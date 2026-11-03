"""海量文件迁移引擎（后端核心）。

设计要点（面向亿级文件）：
- 扫描阶段目录级并行 + 流式统计文件数 / 总大小，不在内存中保留文件清单，内存占用恒定；
  扫描进度实时回写任务统计，进程崩溃可从持久化快照保留部分统计。
- 拷贝阶段使用「有界队列 + 固定线程数」的生产者/消费者模型，避免一次性提交海量任务。
- 写入使用 ``*.part`` 临时文件 + 原子替换，任务中断/取消不会留下半截目标文件。
- 增量模式：目标文件大小与 mtime 一致则跳过。
- 校验阶段：多线程 MD5 比对同名文件（复用拷贝的生产者/消费者模型）；
  拷贝零失败时跳过目标结构扫描，省一轮全量枚举。
- 支持暂停 / 恢复 / 取消 / 限速（MB/s）；速度采用滑动窗口统计。
"""

from __future__ import annotations

import atexit
import glob
import hashlib
import json
import os
import queue
import threading
import time
import uuid
from collections import deque

CHUNK = 4 * 1024 * 1024  # 单次读写块大小，兼顾大文件吞吐与小文件延迟
MTIME_TOLERANCE = 1.0
SAMPLE_WINDOW = 3.0  # 速度统计窗口（秒）
LONG_PREFIX = "\\\\?\\"

# ---- 持久化相关 ----
PERSIST_DEBOUNCE = 0.35    # 毫秒级事件合并写盘
PERSIST_LOG_MAX_BYTES = 10 * 1024 * 1024   # 单份日志文件上限
SCHEMA_VERSION = 1
TASK_LOG_DISPLAY_LIMIT = 300000000

DEFAULT_OPTIONS = {
    "threads": 4,          # 并发拷贝线程数
    "verify": True,        # 是否在拷贝后做哈希校验
    "incremental": True,   # 增量模式（大小 + mtime 一致则跳过）
    "rate_limit_mb": 0,    # 单任务限速，0 表示不限速
    "preserve_mtime": True,
    "follow_symlinks": False,
    "overwrite": True,
    # 目标目录归一化模式：
    #   "auto"  自动把源目录 basename 拼到 dest 后面（默认，贴合直觉）
    #           例：源 /var/log/test + 目标 /home/ningding → 实际拷贝到 /home/ningding/test/
    #   "exact" 用户填什么用什么（兼容旧行为，目标必须是最终目录）
    "dest_mode": "auto",
}


# --------------------------------------------------------------------------- #
# 工具函数
# --------------------------------------------------------------------------- #
def longpath(path: str) -> str:
    r"""Windows 下为超长路径加 \\?\ 前缀，避免 260 字符限制。"""
    path = os.path.abspath(path)
    if os.name != "nt" or path.startswith(LONG_PREFIX):
        return path
    if len(path) < 240:
        return path
    if path.startswith("\\\\"):
        return LONG_PREFIX + "UNC\\" + path[2:]
    return LONG_PREFIX + path


def human_size(num: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB", "PB"):
        if abs(num) < 1024 or unit == "PB":
            return f"{num:.2f} {unit}" if unit != "B" else f"{int(num)} B"
        num /= 1024.0
    return f"{num:.2f} PB"


def human_speed(bps: float) -> str:
    v = human_size(bps)
    return v + "/s"


def makedirs(path: str) -> None:
    os.makedirs(longpath(path), exist_ok=True)


def normalize_dest(source: str, dest: str, mode: str = "auto") -> str:
    """根据 dest_mode 把源路径与目标路径归一化成最终的拷贝目标。

    - auto：将源目录的 basename 拼到 dest 后面，得到最终拷贝目录。
      调用方应在归一化之后再做 s==d / d 在 s 内的安全校验，不要指望此函数兜底。
    - exact：用户填什么用什么。
    """
    source = os.path.abspath(source)
    dest = os.path.abspath(dest)
    if mode != "auto":
        return dest
    src_name = os.path.basename(source.rstrip("/\\"))
    # 目标 basename 已经等于源 basename → 用户显式指定了最终目录，不再补
    if os.path.basename(dest.rstrip("/\\")) == src_name:
        return dest
    # 否则一律拼上 basename（调用方负责校验拼完后的安全边界）
    return os.path.join(dest, src_name)


# --------------------------------------------------------------------------- #
# 限速器（令牌桶，多线程共享）
# --------------------------------------------------------------------------- #
class RateLimiter:
    def __init__(self, mbps: float = 0) -> None:
        self.rate = max(0.0, mbps) * 1024 * 1024
        self.allowance = self.rate
        self.last = time.monotonic()
        self.lock = threading.Lock()

    def consume(self, nbytes: int) -> None:
        """令牌桶：保证长期平均速率不超过 rate，保留欠额不重置。"""
        if self.rate <= 0:
            return
        remaining = nbytes
        while remaining > 0:
            take = min(remaining, self.rate)
            while True:
                with self.lock:
                    now = time.monotonic()
                    self.allowance = min(self.rate, self.allowance + (now - self.last) * self.rate)
                    self.last = now
                    if self.allowance >= take:
                        self.allowance -= take
                        break
                    wait = (take - self.allowance) / self.rate
                time.sleep(min(wait, 0.5))
            remaining -= take


# --------------------------------------------------------------------------- #
# 任务
# --------------------------------------------------------------------------- #
class Task:
    def __init__(self, seq: int, name: str, source: str, dest: str, options: dict | None = None,
                 storage_dir: str | None = None):
        self.id = uuid.uuid4().hex[:10]
        self.seq = seq
        self.options = {**DEFAULT_OPTIONS, **(options or {})}
        self.source = os.path.abspath(source)
        # dest 归一化：根据 options.dest_mode 决定最终目标目录
        self.dest = normalize_dest(self.source, dest, self.options.get("dest_mode", "auto"))
        self.name = name or os.path.basename(self.source.rstrip("/\\")) or source
        self.status = "pending"
        self.created = time.time()
        self.started: float | None = None
        self.finished: float | None = None
        self.files_total = 0
        self.files_done = 0
        self.files_skipped = 0
        self.files_failed = 0
        self.bytes_total = 0
        self.bytes_done = 0
        self.verify_total = 0
        self.verify_done = 0
        self.md5_mismatch = 0
        self.current = ""
        self.message = ""
        self.source_links = 0
        self.dest_links = 0
        self._samples: deque = deque(maxlen=256)
        self._pause = threading.Event()
        self._pause.set()  # set = 可运行
        self._cancel = threading.Event()
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self.limiter = RateLimiter(self.options["rate_limit_mb"])
        # ---- task 级独立日志 ----
        self.task_logs: deque = deque(maxlen=TASK_LOG_DISPLAY_LIMIT)
        self.task_log_seq = 0
        self._task_log_file: object | None = None  # 延迟到 Engine 启动时打开
        self._storage_dir = storage_dir
        # 新建任务没有历史日志，不需要懒加载；重启恢复的 task 在 from_persist 里置为 False
        self._logs_loaded = True
        # ---- hash-on-copy：拷贝时顺手算 MD5，校验阶段零 IO ----
        self.file_md5: dict[str, tuple[str, str]] = {}  # rel → (src_md5, dst_md5) 仅成功拷贝的
        self.skipped_files: set[str] = set()             # 增量模式跳过的 rel（视为信任）

    # -- 状态控制 ---------------------------------------------------------- #
    def add_bytes(self, n: int) -> None:
        with self._lock:
            self.bytes_done += n

    def gate(self) -> bool:
        """暂停时阻塞；返回 False 表示任务已取消。"""
        while not self._pause.wait(0.2):
            if self._cancel.is_set():
                return False
        return not self._cancel.is_set()

    @property
    def cancelled(self) -> bool:
        return self._cancel.is_set()

    def pause(self) -> None:
        if self.status in ("scanning", "running", "verifying"):
            self._pause.clear()
            self.status = "paused"

    def resume(self) -> None:
        if self.status == "paused":
            self._pause.set()
            self.status = "running"

    def cancel(self) -> None:
        self._cancel.set()
        self._pause.set()

    # -- 统计 -------------------------------------------------------------- #
    def speed(self) -> float:
        now = time.time()
        samples = [s for s in self._samples if now - s[0] <= SAMPLE_WINDOW]
        if len(samples) < 2:
            return 0.0
        t0, b0 = samples[0]
        t1, b1 = samples[-1]
        if t1 - t0 <= 0:
            return 0.0
        return max(0.0, (b1 - b0) / (t1 - t0))

    def snapshot(self) -> dict:
        if self.status == "done":
            percent = 100.0
        elif self.bytes_total > 0:
            percent = min(100.0, self.bytes_done / self.bytes_total * 100)
        elif self.files_total:
            percent = self.files_done / self.files_total * 100
        else:
            percent = 0.0
        speed = self.speed()
        eta = -1.0
        if speed > 0 and self.bytes_total > self.bytes_done:
            eta = (self.bytes_total - self.bytes_done) / speed
        return {
            "id": self.id,
            "seq": self.seq,
            "name": self.name,
            "source": self.source,
            "dest": self.dest,
            "status": self.status,
            "percent": round(percent, 2),
            "files_total": self.files_total,
            "files_done": self.files_done,
            "files_skipped": self.files_skipped,
            "files_failed": self.files_failed,
            "bytes_total": self.bytes_total,
            "bytes_done": self.bytes_done,
            "bytes_total_text": human_size(self.bytes_total),
            "bytes_done_text": human_size(self.bytes_done),
            "speed": speed,
            "speed_text": human_speed(speed) if speed > 0 else "0 MB/s",
            "eta": eta,
            "current": self.current,
            "message": self.message,
            "verify_total": self.verify_total,
            "verify_done": self.verify_done,
            "md5_mismatch": self.md5_mismatch,
            "options": self.options,
            "elapsed": (self.finished or time.time()) - (self.started or time.time()),
        }

    # -- 持久化 ------------------------------------------------------------ #
    def to_persist(self) -> dict:
        """只导出可序列化字段，排除线程事件等运行态。"""
        return {
            "id": self.id,
            "seq": self.seq,
            "name": self.name,
            "source": self.source,
            "dest": self.dest,
            "status": self.status,
            "options": self.options,
            "created": self.created,
            "started": self.started,
            "finished": self.finished,
            "files_total": self.files_total,
            "files_done": self.files_done,
            "files_skipped": self.files_skipped,
            "files_failed": self.files_failed,
            "bytes_total": self.bytes_total,
            "bytes_done": self.bytes_done,
            "verify_total": self.verify_total,
            "verify_done": self.verify_done,
            "md5_mismatch": self.md5_mismatch,
            "current": self.current,
            "message": self.message,
            "source_links": self.source_links,
            "dest_links": self.dest_links,
        }

    @classmethod
    def from_persist(cls, data: dict) -> "Task":
        """从持久化快照重建 Task。恢复 running/scanning/verifying → interrupted。"""
        t = cls.__new__(cls)
        restored_fields = {
            "id", "seq", "name", "source", "dest", "status", "options",
            "created", "started", "finished",
            "files_total", "files_done", "files_skipped", "files_failed",
            "bytes_total", "bytes_done", "verify_total", "verify_done",
            "md5_mismatch", "current", "message", "source_links", "dest_links",
        }
        for k in restored_fields:
            setattr(t, k, data.get(k))
        if t.options is None:
            t.options = {**DEFAULT_OPTIONS}
        t._samples = deque(maxlen=256)
        t._pause = threading.Event()
        t._pause.set()
        t._cancel = threading.Event()
        t._lock = threading.Lock()
        t._thread = None
        t.limiter = RateLimiter(t.options.get("rate_limit_mb", 0))
        # 运行态 task 日志（重启后重新打开新文件，不再依赖旧内存 deque）
        t.task_logs = deque(maxlen=TASK_LOG_DISPLAY_LIMIT)
        t.task_log_seq = 0
        t._task_log_file = None
        t._storage_dir = None
        # 重启后 task 日志未加载到内存，首次访问时才从 jsonl 懒加载
        t._logs_loaded = False
        # hash-on-copy 内存映射重启后丢失，retry 时会重新算
        t.file_md5 = {}
        t.skipped_files = set()
        # 进程中断的任务（上次退出前仍在跑），统一标记为 interrupted + 暂停
        if t.status in ("scanning", "running", "verifying"):
            t.status = "interrupted"
            t._pause.clear()  # 保持暂停，等用户点击继续/重试
        return t

    # -- task 级日志 ------------------------------------------------------ #
    def open_log_file(self, storage_dir: str) -> None:
        """打开本任务的专属日志文件（.fsync_data/task_logs/<task_id>.jsonl）。"""
        try:
            d = os.path.join(storage_dir, "task_logs")
            makedirs(d)
            self._task_log_file = open(os.path.join(d, self.id + ".jsonl"), "a",
                                       encoding="utf-8", buffering=1)
        except OSError:
            self._task_log_file = None

    def close_log_file(self) -> None:
        try:
            if self._task_log_file:
                self._task_log_file.flush()
                self._task_log_file.close()
        except OSError:
            pass
        self._task_log_file = None

    def append_task_log(self, rec: dict) -> None:
        with self._lock:
            self.task_log_seq += 1
            rec = dict(rec)
            rec["task_seq_local"] = self.task_log_seq
            self.task_logs.append(rec)
        if self._task_log_file:
            try:
                self._task_log_file.write(json.dumps(rec, ensure_ascii=False) + "\n")
                self._task_log_file.flush()
            except OSError:
                pass

    def load_logs_from_disk(self) -> int:
        """从 .fsync_data/task_logs/<task_id>.jsonl 把历史日志加载进内存。返回加载条数。"""
        if self._logs_loaded or not self._storage_dir:
            return 0
        path = os.path.join(self._storage_dir, "task_logs", self.id + ".jsonl")
        loaded = 0
        try:
            if os.path.exists(longpath(path)):
                with open(longpath(path), "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            rec = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        seq = int(rec.get("task_seq_local", 0))
                        if seq > self.task_log_seq:
                            self.task_log_seq = seq
                        self.task_logs.append(rec)
                        loaded += 1
        except OSError:
            pass
        self._logs_loaded = True
        return loaded


# --------------------------------------------------------------------------- #
# 存储层：COW 原子写 + 追加式日志 + 重启恢复
# --------------------------------------------------------------------------- #
class Storage:
    def __init__(self, data_dir: str) -> None:
        self.dir = os.path.abspath(data_dir)
        makedirs(self.dir)
        self.tasks_file = os.path.join(self.dir, "tasks.json")
        self.logs_file = os.path.join(self.dir, "logs.jsonl")
        self._log_file = open(self.logs_file, "a", encoding="utf-8", buffering=1)
        self._rotate_logs_if_needed()

    # -- COW 原子写 ------------------------------------------------------- #
    def _atomic_write_json(self, path: str, payload: dict) -> None:
        """写 .tmp → rename 覆盖 → 清理 .bak；进程崩溃最多留下 .tmp，不会损坏主文件。"""
        data = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
        tmp = path + ".tmp"
        bak = path + ".bak"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(data)
        if os.path.exists(longpath(path)):
            try:
                os.replace(longpath(path), longpath(bak))
            except OSError:
                pass
        os.replace(longpath(tmp), longpath(path))
        try:
            os.unlink(longpath(bak))
        except OSError:
            pass

    def save_tasks(self, tasks: list[Task], next_seq: int) -> None:
        payload = {
            "version": SCHEMA_VERSION,
            "saved_at": time.time(),
            "next_seq": next_seq,
            "tasks": [t.to_persist() for t in tasks],
        }
        self._atomic_write_json(self.tasks_file, payload)

    def load_tasks(self) -> tuple[list[dict], int]:
        if not os.path.exists(longpath(self.tasks_file)):
            return [], 0
        for candidate in (self.tasks_file, self.tasks_file + ".bak"):
            try:
                with open(longpath(candidate), "r", encoding="utf-8") as f:
                    raw = f.read()
                if not raw:
                    continue
                data = json.loads(raw)
                if data.get("version", 0) != SCHEMA_VERSION:
                    continue
                return data.get("tasks", []), int(data.get("next_seq", 0))
            except (OSError, json.JSONDecodeError):
                continue
        return [], 0

    # -- 追加式日志 ------------------------------------------------------- #
    def append_log(self, rec: dict) -> None:
        try:
            self._log_file.write(json.dumps(rec, ensure_ascii=False) + "\n")
            self._log_file.flush()
            self._rotate_logs_if_needed()
        except OSError:
            pass

    def _rotate_logs_if_needed(self) -> None:
        try:
            size = os.path.getsize(longpath(self.logs_file))
        except OSError:
            return
        if size < PERSIST_LOG_MAX_BYTES:
            return
        self._log_file.close()
        rotate = self.logs_file + ".1"
        if os.path.exists(longpath(rotate)):
            os.unlink(longpath(rotate))
        os.replace(longpath(self.logs_file), longpath(rotate))
        self._log_file = open(self.logs_file, "a", encoding="utf-8", buffering=1)

    def close(self) -> None:
        try:
            self._log_file.flush()
            self._log_file.close()
        except OSError:
            pass


# --------------------------------------------------------------------------- #
# 引擎
# --------------------------------------------------------------------------- #
class Engine:
    def __init__(self, max_logs: int = 4000, storage_dir: str | None = None) -> None:
        # 默认存储目录：优先 FSSYNC_DATA 环境变量，否则用当前工作目录(CWD)下的 .fsync_data
        # 这样 PyInstaller 单文件运行时也不会跑到 sys._MEIPASS 临时目录里
        if storage_dir is None:
            storage_dir = os.environ.get("FSSYNC_DATA") or os.path.join(os.getcwd(), ".fsync_data")
        self.storage = Storage(storage_dir)
        self.storage_dir = storage_dir

        self.tasks: dict[str, Task] = {}
        self.order: list[str] = []
        self.logs: deque = deque(maxlen=max_logs)
        self.log_seq = 0
        self._seq = 0
        self._lock = threading.Lock()
        self._persist_lock = threading.Lock()
        self._persist_timer: threading.Timer | None = None
        self._persist_pending = False

        # 先还原持久化的任务，再启动监控线程
        self._revive()

        # 为所有现存任务打开专属日志文件
        for t in self.tasks.values():
            t.open_log_file(self.storage_dir)

        self._monitor = threading.Thread(target=self._monitor_loop, daemon=True)
        self._monitor.start()

        atexit.register(self.flush)

    # -- 日志 -------------------------------------------------------------- #
    def log(self, level: str, message: str, task: Task | None = None,
            detail: bool = False) -> None:
        """写日志。

        - 无 task：始终写系统日志（engine.logs + logs.jsonl）
        - 有 task + detail=False（概要）：系统日志写一行概要，task 日志写完整 message
        - 有 task + detail=True（细节，如每个文件的拷贝/校验）：只写 task 日志，不入系统流
        """
        if task is None:
            self._append_system_log(level, message, task)
            return
        # 先写 task 专属日志
        task.append_task_log({
            "seq": 0,  # task_log_seq 由 Task 自己维护
            "ts": time.time(),
            "time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            "level": level,
            "task_id": task.id,
            "task_seq": task.seq,
            "message": message,
            "detail": detail,
        })
        # 概要级 → 也入系统日志；细节级不入，避免系统日志被海量文件记录淹没
        if not detail:
            self._append_system_log(level, message, task)

    def _append_system_log(self, level: str, message: str, task: Task | None) -> None:
        with self._lock:
            self.log_seq += 1
            rec = {
                "seq": self.log_seq,
                "ts": time.time(),
                "time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                "level": level,
                "task_id": task.id if task else "",
                "task_seq": task.seq if task else 0,
                "message": message,
            }
            self.logs.append(rec)
        try:
            self.storage.append_log(rec)
        except Exception:  # noqa: BLE001
            pass

    def logs_since(self, seq: int, task_id: str = "", level: str = "", limit: int = 0) -> list[dict]:
        """limit=0 表示不限制（返回全部）。"""
        out = []
        for rec in self.logs:
            if rec["seq"] <= seq:
                continue
            if task_id and rec["task_id"] != task_id:
                continue
            if level and level != "ALL" and rec["level"] != level:
                continue
            out.append(rec)
        return out[-limit:] if limit > 0 else out

    def task_logs_since(self, task_id: str, seq: int = 0, level: str = "",
                        limit: int = 0) -> list[dict]:
        """按 task_id 返回专属日志增量。seq 为该 task 的 task_log_seq 起始，0 表示全部。
        若该 task 的历史日志尚未加载（重启后首次访问），会先从 jsonl 懒加载进内存。
        limit=0 表示不限制（返回全部）。"""
        task = self.tasks.get(task_id)
        if not task:
            return []
        # 懒加载：重启后第一次拉日志时，才把磁盘上的 jsonl 读进内存
        if not getattr(task, "_logs_loaded", True):
            task.load_logs_from_disk()
        out = []
        for rec in task.task_logs:
            local_seq = rec.get("task_seq_local", 0)
            if local_seq <= seq:
                continue
            if level and level != "ALL" and rec["level"] != level:
                continue
            out.append(rec)
        return out[-limit:] if limit > 0 else out

    # -- 恢复 / 持久化 ---------------------------------------------------- #
    def _revive(self) -> None:
        """启动时从 tasks.json 还原任务。running/scanning/verifying → interrupted(暂停)。"""
        rows, seq = self.storage.load_tasks()
        self._seq = seq
        revived = 0
        for data in rows:
            try:
                task = Task.from_persist(data)
                # 关键：恢复的任务要设置 _storage_dir，否则懒加载日志时读不到 jsonl 文件
                task._storage_dir = self.storage_dir
                task.open_log_file(self.storage_dir)
                self.tasks[task.id] = task
                self.order.append(task.id)
                revived += 1
            except Exception as exc:  # noqa: BLE001
                self.log("WARN", f"忽略损坏的持久化任务: {exc}")
        if revived:
            interrupted = [t.name for t in self.tasks.values() if t.status == "interrupted"]
            if interrupted:
                self.log("WARN",
                         f"启动恢复 {revived} 个任务，其中 {len(interrupted)} 个因上次进程中断被标记为 interrupted："
                         + ", ".join(interrupted[:5])
                         + ("..." if len(interrupted) > 5 else ""))
            else:
                self.log("INFO", f"启动恢复 {revived} 个已结束任务")
        # 落一份"已恢复"快照，防止 interrupted 状态被重复叠加
        self._persist_now()

    def _schedule_persist(self) -> None:
        """合并写盘：300ms 内多次调用 → 只写一次。"""
        with self._persist_lock:
            if self._persist_pending:
                return
            self._persist_pending = True
            self._persist_timer = threading.Timer(PERSIST_DEBOUNCE, self._persist_now)
            self._persist_timer.daemon = True
            self._persist_timer.start()

    def _persist_now(self) -> None:
        """立即写盘（COW 原子），供定时器与紧急路径调用。"""
        with self._persist_lock:
            if self._persist_timer:
                self._persist_timer.cancel()
                self._persist_timer = None
            self._persist_pending = False
        try:
            with self._lock:
                snapshot = [self.tasks[tid] for tid in self.order if tid in self.tasks]
                seq = self._seq
            self.storage.save_tasks(snapshot, seq)
        except Exception as exc:  # noqa: BLE001
            self.log("ERROR", f"持久化失败: {exc}")

    def flush(self) -> None:
        """进程退出前 / 生命周期关闭时调用，保证最后一次状态落盘。"""
        try:
            self._persist_now()
            self.storage.close()
            for t in list(self.tasks.values()):
                t.close_log_file()
        except Exception:  # noqa: BLE001
            pass

    # -- 任务管理 ---------------------------------------------------------- #
    def create(self, name: str, source: str, dest: str, options: dict | None = None) -> Task:
        with self._lock:
            self._seq += 1
            task = Task(self._seq, name, source, dest, options, storage_dir=self.storage_dir)
            self.tasks[task.id] = task
            self.order.append(task.id)
        task.open_log_file(self.storage_dir)
        self.log("INFO", f"新建任务: {task.source} → {task.dest}", task)
        self._persist_now()  # 新任务立即落盘
        self.start(task)
        return task

    def start(self, task: Task) -> None:
        if task._thread and task._thread.is_alive():
            return
        task._cancel.clear()
        task._pause.set()
        task.started = task.started or time.time()
        task.finished = None
        task.status = "scanning"
        task._thread = threading.Thread(target=self._run, args=(task,), daemon=True)
        task._thread.start()
        self._schedule_persist()

    def pause(self, task: Task) -> None:
        task.pause()
        self._schedule_persist()

    def resume(self, task: Task) -> None:
        task.resume()
        self._schedule_persist()

    def cancel(self, task: Task) -> None:
        task.cancel()
        self._schedule_persist()

    def retry(self, task: Task) -> None:
        task.bytes_done = 0
        task.files_done = 0
        task.files_skipped = 0
        task.files_failed = 0
        task.verify_done = 0
        task.md5_mismatch = 0
        task.started = None
        task.finished = None
        self.start(task)

    def remove(self, task_id: str) -> None:
        task = self.tasks.pop(task_id, None)
        if task:
            task.cancel()
            task.close_log_file()
            if task_id in self.order:
                self.order.remove(task_id)
            self.log("WARN", f"删除任务: {task.name}", task)
            self._persist_now()  # 删除立即落盘

    def snapshot(self) -> list[dict]:
        return [self.tasks[tid].snapshot() for tid in self.order if tid in self.tasks]

    def aggregate(self) -> dict:
        snap = self.snapshot()
        running = [t for t in snap if t["status"] in ("scanning", "running", "verifying")]
        return {
            "total": len(snap),
            "running": len(running),
            "done": len([t for t in snap if t["status"] == "done"]),
            "speed": sum(t["speed"] for t in running),
            "bytes_total": sum(t["bytes_total"] for t in snap),
            "bytes_done": sum(t["bytes_done"] for t in snap),
        }

    # -- 统计采样 + 定期持久化 --------------------------------------------- #
    def _monitor_loop(self) -> None:
        while True:
            time.sleep(0.5)
            now = time.time()
            any_active = False
            for task in list(self.tasks.values()):
                if task.status in ("scanning", "running", "verifying"):
                    any_active = True
                    task._samples.append((now, task.bytes_done))
                elif len(task._samples) > 1:
                    task._samples.popleft()
            # 活跃任务：每 ~2s 合并落盘一次进度
            if any_active and not self._persist_pending:
                self._schedule_persist()

    # -- 主流程 ------------------------------------------------------------ #
    def _run(self, task: Task) -> None:
        try:
            self.log("INFO", f"校准任务启动: {task.source} → {task.dest}", task)
            self.log("INFO", f"源目录: {task.source}", task)
            self.log("INFO", f"目标目录: {task.dest}", task)

            # 1) 扫描（目录级并行，进度实时回写任务统计，中途崩溃可保留部分统计）
            task.status = "scanning"
            self._schedule_persist()

            def _on_scan_progress(f: int, b: int) -> None:
                task.files_total = f
                task.bytes_total = b
                task.message = f"已统计 {f} 个文件 / {human_size(b)}"

            files, total, links = self._scan(task, task.source, _on_scan_progress)
            task.files_total = files
            task.bytes_total = total
            task.source_links = links
            self.log("INFO", f"扫描完成: 文件数 {files} vs 总大小 {human_size(total)}", task)
            self._schedule_persist()
            if task.options["incremental"]:
                self.log("INFO", "增量模式 mtime 校验: 大小 + mtime 一致则跳过", task)
            if task.cancelled:
                task.status = "cancelled"
                task.finished = time.time()
                self.log("WARN", "任务已取消", task)
                return
            while task.status == "paused" and not task.cancelled:
                time.sleep(0.2)

            makedirs(task.dest)

            # 2) 拷贝
            task.status = "running"
            self._schedule_persist()
            self._copy_tree(task)
            if task.cancelled:
                task.status = "cancelled"
                task.finished = time.time()
                self.log("WARN", f"任务已取消: 已处理 {task.files_done}/{task.files_total} 个文件", task)
                return

            # 3) 校验
            if task.options["verify"]:
                task.status = "verifying"
                self._schedule_persist()
                self._verify(task)
                if task.cancelled:
                    task.status = "cancelled"
                    task.finished = time.time()
                    self.log("WARN", f"任务已取消: 校验进行到 {task.verify_done}/{task.verify_total}", task)
                    return

            task.files_total = task.files_total or task.files_done
            task.bytes_total = task.bytes_total or task.bytes_done
            ok = not task.files_failed and not task.md5_mismatch
            task.status = "done" if ok else "failed"
            task.finished = time.time()
            task.current = ""
            self.log(
                "SUCCESS" if task.status == "done" else "ERROR",
                f"任务结束: 文件 {task.files_done} 成功 / {task.files_skipped} 跳过 / "
                f"{task.files_failed} 失败，耗时 {task.finished - (task.started or task.finished):.1f}s",
                task,
            )
        except Exception as exc:  # noqa: BLE001
            task.status = "failed"
            task.finished = time.time()
            task.message = str(exc)
            self.log("ERROR", f"任务异常: {exc}", task)
        finally:
            # 终端状态一律立即落盘，保证重启后可恢复
            if task.status in ("done", "failed", "cancelled"):
                self._persist_now()

    # -- 扫描 -------------------------------------------------------------- #
    def _scan(self, task: Task, root: str, on_progress=None) -> tuple[int, int, int]:
        """目录级并行扫描：N 个线程共享一个目录队列，流式累计文件数/总大小/软链接数。

        - 不保留文件清单，内存占用恒定（仅目录队列）。
        - on_progress(files, total)：每约 2 万个文件回调一次，供调用方实时更新统计。
        - 取消时尽快收敛，返回已统计的部分结果。
        """
        lock = threading.Lock()
        counts = {"files": 0, "total": 0, "links": 0, "pending": 1, "reported": 0}
        q: queue.Queue = queue.Queue()
        q.put(root)
        stop = threading.Event()
        workers_n = min(16, max(4, (os.cpu_count() or 4) * 2))

        def worker() -> None:
            while not stop.is_set():
                try:
                    cur = q.get(timeout=0.1)
                except queue.Empty:
                    with lock:
                        if counts["pending"] == 0:
                            return
                    continue
                subdirs: list[str] = []
                try:
                    with os.scandir(longpath(cur)) as it:
                        for entry in it:
                            try:
                                if entry.is_symlink():
                                    with lock:
                                        counts["links"] += 1
                                    if not task.options["follow_symlinks"]:
                                        continue
                                if entry.is_dir(follow_symlinks=task.options["follow_symlinks"]):
                                    subdirs.append(entry.path)
                                elif entry.is_file(follow_symlinks=task.options["follow_symlinks"]):
                                    with lock:
                                        counts["files"] += 1
                                        counts["total"] += entry.stat().st_size
                            except OSError:
                                continue
                except OSError as exc:
                    self.log("ERROR", f"扫描失败 {cur}: {exc}", task)
                    subdirs = []
                if subdirs:
                    with lock:
                        counts["pending"] += len(subdirs)
                    for d in subdirs:
                        q.put(d)
                with lock:
                    counts["pending"] -= 1
                    report = None
                    if on_progress and counts["files"] - counts["reported"] >= 20000:
                        counts["reported"] = counts["files"]
                        report = (counts["files"], counts["total"])
                if report:
                    on_progress(*report)

        threads = [threading.Thread(target=worker, daemon=True) for _ in range(workers_n)]
        for t in threads:
            t.start()
        try:
            for t in threads:
                while t.is_alive():
                    t.join(timeout=0.2)
                    if task.cancelled:
                        stop.set()
        finally:
            stop.set()
            for t in threads:
                t.join(timeout=1.0)
        return counts["files"], counts["total"], counts["links"]

    def _walk_files(self, root: str):
        """流式产出 (绝对路径, 相对路径)，不保留清单。"""
        stack = [root]
        while stack:
            cur = stack.pop()
            try:
                with os.scandir(longpath(cur)) as it:
                    for entry in it:
                        try:
                            if entry.is_symlink():
                                continue
                            if entry.is_dir():
                                stack.append(entry.path)
                            elif entry.is_file():
                                rel = os.path.relpath(entry.path, root)
                                yield entry.path, rel
                        except OSError:
                            continue
            except OSError:
                continue

    # -- 拷贝 -------------------------------------------------------------- #
    def _copy_tree(self, task: Task) -> None:
        workers = max(1, int(task.options["threads"]))
        q: queue.Queue = queue.Queue(maxsize=workers * 16)

        def worker() -> None:
            while True:
                item = q.get()
                try:
                    if item is None:
                        return
                    self._copy_one(task, item[0], item[1])
                finally:
                    q.task_done()

        threads = [threading.Thread(target=worker, daemon=True) for _ in range(workers)]
        for t in threads:
            t.start()

        try:
            for src, rel in self._walk_files(task.source):
                if task.cancelled:
                    break
                if not task.gate():
                    break
                q.put((src, rel))
        finally:
            for _ in threads:
                q.put(None)
            for t in threads:
                t.join()

    def _copy_one(self, task: Task, src: str, rel: str) -> None:
        if task.cancelled or not task.gate():
            return
        dst = os.path.join(task.dest, rel)
        tmp = dst + ".part"
        try:
            st = os.stat(longpath(src))
            if task.options["incremental"]:
                try:
                    dt = os.stat(longpath(dst))
                    if dt.st_size == st.st_size and abs(dt.st_mtime - st.st_mtime) <= MTIME_TOLERANCE:
                        task.files_skipped += 1
                        task.files_done += 1
                        task.add_bytes(st.st_size)
                        task.skipped_files.add(rel)
                        self.log("INFO", f"跳过(增量): {rel}", task, detail=True)
                        return
                except OSError:
                    pass
            if not task.options["overwrite"] and os.path.exists(longpath(dst)):
                task.files_skipped += 1
                task.files_done += 1
                task.skipped_files.add(rel)
                self.log("INFO", f"跳过(已存在): {rel}", task, detail=True)
                return

            makedirs(os.path.dirname(dst) or task.dest)
            task.current = rel
            written = 0
            src_h = hashlib.md5()
            dst_h = hashlib.md5()
            with open(longpath(src), "rb", buffering=0) as fin, open(longpath(tmp), "wb", buffering=0) as fout:
                while True:
                    if task.cancelled:
                        break
                    if not task.gate():
                        break
                    chunk = fin.read(CHUNK)
                    if not chunk:
                        break
                    src_h.update(chunk)   # hash-on-copy：读时顺手算源 MD5
                    fout.write(chunk)
                    dst_h.update(chunk)   # 写时顺手算目标 MD5
                    task.limiter.consume(len(chunk))
                    written += len(chunk)
                    task.add_bytes(len(chunk))
                if task.cancelled or not task._pause.is_set():
                    fout.close()
                    self._safe_unlink(tmp)
                    return
            os.replace(longpath(tmp), longpath(dst))
            if task.options["preserve_mtime"]:
                os.utime(longpath(dst), (st.st_atime, st.st_mtime))
            task.files_done += 1
            src_md5, dst_md5 = src_h.hexdigest(), dst_h.hexdigest()
            task.file_md5[rel] = (src_md5, dst_md5)
            self.log("INFO", f"已拷贝: {rel}  {human_size(st.st_size)}  MD5={src_md5}", task, detail=True)
            if written != st.st_size:
                task.files_failed += 1
                self.log("WARN", f"大小不一致: {rel} 源 {st.st_size} vs 写入 {written}", task, detail=True)
        except PermissionError as exc:
            task.files_failed += 1
            task.files_done += 1
            self.log("ERROR", f"访问被拒绝: {rel} ({exc})", task, detail=True)
            self._safe_unlink(tmp)
        except OSError as exc:
            task.files_failed += 1
            task.files_done += 1
            self.log("ERROR", f"拷贝失败: {rel} ({exc})", task, detail=True)
            self._safe_unlink(tmp)

    @staticmethod
    def _safe_unlink(path: str) -> None:
        try:
            os.unlink(longpath(path))
        except OSError:
            pass

    @staticmethod
    def _md5(path: str) -> str:
        h = hashlib.md5()
        with open(longpath(path), "rb", buffering=0) as f:
            while True:
                chunk = f.read(CHUNK)
                if not chunk:
                    break
                h.update(chunk)
        return h.hexdigest()

    # -- 校验 -------------------------------------------------------------- #
    def _verify(self, task: Task) -> None:
        self.log("INFO", "校验结果:", task)
        if task.files_failed == 0:
            self.log("INFO", "拷贝阶段零失败，跳过目标结构扫描，直接进入哈希校验", task)
        else:
            d_files, d_total, d_links = self._scan(task, task.dest)
            task.dest_links = d_links
            self.log("INFO", f"文件数: 源 {task.files_total} vs 目标 {d_files} "
                             f"{'一致' if d_files >= task.files_total else '不一致'}", task)
            self.log("INFO", f"总大小: 源 {human_size(task.bytes_total)} vs 目标 {human_size(d_total)} "
                             f"{'一致' if d_total >= task.bytes_total else '不一致'}", task)
            self.log("INFO", f"软链接数: 源 {task.source_links} vs 目标 {d_links} "
                             f"{'一致' if task.source_links == d_links else '不一致'}", task)
        self.log("INFO", "开始哈希校验（hash-on-copy 缓存优先，必要时现算）...", task)

        task.verify_total = task.files_done or task.files_total
        task.verify_done = 0
        task.md5_mismatch = 0
        # 校验并发：内存对比是 CPU-bound，fallback 现算是 IO-bound，独立线程池
        workers = max(2, int(task.options["threads"]) * 2)
        q: queue.Queue = queue.Queue(maxsize=workers * 16)

        def worker() -> None:
            while True:
                item = q.get()
                try:
                    if item is None:
                        return
                    self._verify_one(task, item[0], item[1])
                finally:
                    q.task_done()

        threads = [threading.Thread(target=worker, daemon=True) for _ in range(workers)]
        for t in threads:
            t.start()

        try:
            for src, rel in self._walk_files(task.source):
                if task.cancelled:
                    return
                if not task.gate():
                    return
                task.current = rel
                q.put((src, rel))
        finally:
            for _ in threads:
                q.put(None)
            for t in threads:
                t.join()

        if task.cancelled:
            return
        task.current = ""
        if task.md5_mismatch:
            self.log("ERROR", f"哈希校验完成: {task.md5_mismatch} 个文件不一致", task)
        else:
            self.log("SUCCESS", f"哈希校验完成: {task.verify_done} 个文件全部一致", task)

    def _verify_one(self, task: Task, src: str, rel: str) -> None:
        """校验单个文件：优先用 hash-on-copy 的内存 hash，否则 fallback 现算。"""
        if not task.gate():
            return
        dst = os.path.join(task.dest, rel)
        try:
            if rel in task.file_md5:
                # 拷贝时已经算过两边 MD5，零 IO 直接对比
                src_md5, dst_md5 = task.file_md5[rel]
                if src_md5 != dst_md5:
                    self._verify_fail(task, rel, f"MD5 不一致 源={src_md5} vs 目标={dst_md5}",
                                      src_md5=src_md5, dst_md5=dst_md5)
                else:
                    self.log("INFO", f"校验通过: {rel}  MD5={src_md5}", task, detail=True)
            elif rel in task.skipped_files:
                # 增量模式跳过的文件：用户已信任 size+mtime 一致，直接通过
                # 仍然记 MD5 为 "skipped"，保持日志完整
                self.log("INFO", f"校验通过(增量信任): {rel}", task, detail=True)
            else:
                # fallback：拷贝失败/漏算，现算 MD5
                if not os.path.exists(longpath(dst)):
                    self._verify_fail(task, rel, "目标缺失", src_md5="", dst_md5="")
                else:
                    st_src = os.stat(longpath(src)).st_size
                    st_dst = os.stat(longpath(dst)).st_size
                    if st_src != st_dst:
                        self._verify_fail(task, rel, f"大小不一致 源={st_src} vs 目标={st_dst}",
                                          src_md5="", dst_md5="")
                    else:
                        src_md5 = self._md5(src)
                        dst_md5 = self._md5(dst)
                        if src_md5 != dst_md5:
                            self._verify_fail(task, rel, f"MD5 不一致 源={src_md5} vs 目标={dst_md5}",
                                              src_md5=src_md5, dst_md5=dst_md5)
                        else:
                            self.log("INFO", f"校验通过: {rel}  MD5={src_md5}", task, detail=True)
            with task._lock:
                task.verify_done += 1
        except OSError as exc:
            self._verify_fail(task, rel, f"校验失败 ({exc})", src_md5="", dst_md5="")
        except Exception as exc:  # noqa: BLE001
            self._verify_fail(task, rel, f"校验异常 ({exc})", src_md5="", dst_md5="")

    def _verify_fail(self, task: Task, rel: str, reason: str,
                     src_md5: str = "", dst_md5: str = "") -> None:
        with task._lock:
            task.md5_mismatch += 1
        msg = f"{rel}  |  {reason}"
        self.log("ERROR", msg, task, detail=True)


engine = Engine()
