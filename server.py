"""智能文件迁移 - 后端服务（FastAPI + WebSocket + 静态资源托管）。

浏览器无法访问本地文件系统，因此真正的扫描 / 拷贝 / 校验全部由本后端完成，
前端只负责展示与下发指令。前后端同源部署，无需处理 CORS。
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from engine import engine, longpath, normalize_dest

# PyInstaller 单文件运行时会把资源解压到 sys._MEIPASS
_BASE_DIR = getattr(sys, "_MEIPASS", os.path.dirname(os.path.abspath(__file__)))
STATIC_DIR = os.path.join(_BASE_DIR, "static")


# --------------------------------------------------------------------------- #
# WebSocket 广播中心
# --------------------------------------------------------------------------- #
class Hub:
    def __init__(self) -> None:
        self.clients: set[WebSocket] = set()

    async def register(self, ws: WebSocket) -> None:
        self.clients.add(ws)

    async def unregister(self, ws: WebSocket) -> None:
        self.clients.discard(ws)

    async def broadcast(self, payload: dict) -> None:
        if not self.clients:
            return
        text = json.dumps(payload, ensure_ascii=False)
        for ws in list(self.clients):
            try:
                await ws.send_text(text)
            except Exception:  # noqa: BLE001
                self.clients.discard(ws)


hub = Hub()
_last_log_seq = engine.log_seq


async def _pusher() -> None:
    """以 ~3Hz 推送任务进度与增量日志，避免高频刷新拖垮前端。"""
    global _last_log_seq
    while True:
        await asyncio.sleep(0.35)
        if not hub.clients:
            _last_log_seq = engine.log_seq
            continue
        # 关键：先取增量，再推进本地 seq；否则 logs_since 会永远返回空
        pending_logs = engine.logs_since(_last_log_seq, limit=200)
        _last_log_seq = engine.log_seq
        await hub.broadcast({
            "type": "update",
            "ts": time.time(),
            "tasks": engine.snapshot(),
            "aggregate": engine.aggregate(),
            "logs": pending_logs,
        })


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(_pusher())
    try:
        yield
    finally:
        task.cancel()
        await asyncio.sleep(0.05)
        engine.flush()


app = FastAPI(title="智能文件迁移", lifespan=lifespan)


@app.middleware("http")
async def no_cache(request, call_next):
    """禁用静态资源缓存，避免前端改动后浏览器仍使用旧文件。"""
    resp = await call_next(request)
    path = request.url.path
    if path == "/" or path.endswith((".js", ".css", ".html")):
        resp.headers["Cache-Control"] = "no-store, must-revalidate"
    return resp


# --------------------------------------------------------------------------- #
# 数据模型
# --------------------------------------------------------------------------- #
class CreateTaskReq(BaseModel):
    name: str = ""
    source: str
    dest: str
    options: dict | None = None


# --------------------------------------------------------------------------- #
# 校验
# --------------------------------------------------------------------------- #
def validate_paths(source: str, dest: str, dest_mode: str = "auto") -> tuple[bool, str, str]:
    """校验路径 + 返回归一化后的最终 dest（基于 dest_mode）。

    返回 (ok, msg, normalized_dest)。调用方应使用 normalized_dest 做后续操作。
    """
    if not source or not dest:
        return False, "源路径与目标路径不能为空", dest
    s = os.path.abspath(source)
    d_raw = os.path.abspath(dest)
    if not os.path.exists(longpath(s)):
        return False, f"源路径不存在: {s}", d_raw
    if not os.path.isdir(longpath(s)):
        return False, f"源路径不是目录: {s}", d_raw
    # 先归一化，再用最终路径做安全校验
    d = normalize_dest(s, d_raw, dest_mode)
    if s == d:
        return False, "目标路径不能与源路径相同（即使 dest_mode=auto 也不允许）", d
    if d.startswith(s.rstrip("\\/") + os.sep):
        return False, "目标路径不能位于源路径内部", d
    return True, "", d


def _get(tid: str):
    task = engine.tasks.get(tid)
    if task is None:
        return None, JSONResponse({"error": "任务不存在"}, status_code=404)
    return task, None


# --------------------------------------------------------------------------- #
# REST 接口
# --------------------------------------------------------------------------- #
@app.get("/api/tasks")
async def list_tasks():
    return {"tasks": engine.snapshot(), "aggregate": engine.aggregate()}


@app.post("/api/tasks")
async def create_task(req: CreateTaskReq):
    dest_mode = (req.options or {}).get("dest_mode", "auto")
    ok, msg, normalized_dest = validate_paths(req.source, req.dest, dest_mode)
    if not ok:
        return JSONResponse({"error": msg}, status_code=400)
    # 把归一化后的 dest 回写到 options，避免 Task.__init__ 再走一遍 auto 逻辑（重复但无害，更关键的是让前端/调用方看到一致结果）
    opts = dict(req.options or {})
    # 直接用归一化后的 dest 创建；同时把实际使用的 dest_mode 确保存在
    opts["dest_mode"] = dest_mode
    task = engine.create(req.name, req.source, normalized_dest, opts)
    return JSONResponse(task.snapshot(), status_code=201)


@app.post("/api/tasks/{tid}/pause")
async def pause_task(tid: str):
    task, err = _get(tid)
    if err:
        return err
    engine.pause(task)
    engine.log("INFO", "任务已暂停", task)
    return task.snapshot()


@app.post("/api/tasks/{tid}/resume")
async def resume_task(tid: str):
    task, err = _get(tid)
    if err:
        return err
    if task.status == "interrupted":
        # 进程中断遗留的任务，resume 实际上是 retry（重跑从 0 开始更安全）
        engine.retry(task)
        engine.log("INFO", "从 interrupted 状态重新启动任务", task)
    else:
        engine.resume(task)
        engine.log("INFO", "任务已恢复", task)
    return task.snapshot()


@app.post("/api/tasks/{tid}/cancel")
async def cancel_task(tid: str):
    task, err = _get(tid)
    if err:
        return err
    engine.cancel(task)
    return task.snapshot()


@app.post("/api/tasks/{tid}/retry")
async def retry_task(tid: str):
    task, err = _get(tid)
    if err:
        return err
    engine.retry(task)
    return task.snapshot()


@app.delete("/api/tasks/{tid}")
async def delete_task(tid: str):
    task, err = _get(tid)
    if err:
        return err
    engine.remove(tid)
    return {"ok": True}


@app.get("/api/logs")
async def get_logs(after: int = 0, task_id: str = "", level: str = "", limit: int = 0):
    """系统级日志（概要）。返回的 seq 为全局 engine.log_seq。limit=0 表示不限制。"""
    return {"logs": engine.logs_since(after, task_id, level, limit), "seq": engine.log_seq}


@app.get("/api/tasks/{tid}/logs")
async def get_task_logs(tid: str, after: int = 0, level: str = "", limit: int = 0):
    """指定任务的专属日志（含每个文件的拷贝/校验细节）。

    - after：该 task 的 task_log_seq 起始（0 表示从头来），返回所有 task_seq_local > after 的记录。
    - limit：返回条数上限，0 表示不限制。
    """
    task = engine.tasks.get(tid)
    if task is None:
        return JSONResponse({"error": "任务不存在"}, status_code=404)
    return {
        "task_id": tid,
        "task_seq": task.task_log_seq,
        "logs": engine.task_logs_since(tid, after, level, limit),
    }


@app.get("/api/list-dirs")
async def list_dirs(path: str = ""):
    """供「新建任务」目录选择器使用。"""
    if not path:
        path = os.path.expanduser("~")
    p = os.path.abspath(path)
    dirs: list[str] = []
    try:
        with os.scandir(longpath(p)) as it:
            for entry in it:
                try:
                    if entry.is_dir(follow_symlinks=False):
                        dirs.append(entry.name)
                except OSError:
                    continue
    except OSError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    dirs.sort(key=str.lower)
    return {"path": p, "parent": os.path.dirname(p.rstrip("\\/")) or p, "dirs": dirs}


# --------------------------------------------------------------------------- #
# WebSocket
# --------------------------------------------------------------------------- #
@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    await hub.register(ws)
    try:
        await ws.send_text(json.dumps({
            "type": "init",
            "tasks": engine.snapshot(),
            "aggregate": engine.aggregate(),
            "logs": engine.logs_since(0, limit=300),
        }, ensure_ascii=False))
        while True:
            await ws.receive_text()  # 心跳
    except WebSocketDisconnect:
        pass
    except Exception:  # noqa: BLE001
        pass
    finally:
        await hub.unregister(ws)


# --------------------------------------------------------------------------- #
# 前端静态资源
# --------------------------------------------------------------------------- #
@app.get("/")
async def index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="static")


if __name__ == "__main__":
    import uvicorn

    host = os.environ.get("FSSYNC_HOST", "127.0.0.1")
    port = int(os.environ.get("FSSYNC_PORT", "8000"))
    print(f"智能文件迁移已启动: http://{host}:{port}")
    uvicorn.run(app, host=host, port=port, log_level="warning")
