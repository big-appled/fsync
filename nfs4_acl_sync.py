#!/usr/bin/env python3
"""按目录层级同步 NFSv4 ACL。

需要安装nfs4-acl-tools包

读取源目录「第 N 层」各目录的 ACL（使用环境中的 nfs4_getfacl），
写入目标目录中与之对应的目录（默认按相同相对路径匹配，可选按同名目录匹配）。

用法示例:
    python3 nfs4_acl_sync.py /data/src /data/dst --depth 1
    python3 nfs4_acl_sync.py /data/src /data/dst --depth 1 --verify --workers 4
    python3 nfs4_acl_sync.py /data/src /data/dst --depth 2 --match auto
    python3 nfs4_acl_sync.py /data/src /data/dst --depth 1 --dry-run

默认命令调用方式（可通过 --get-cmd / --set-cmd / --set-flag / --set-mode 调整）:
    nfs4_getfacl <源目录>
    nfs4_setfacl -f <临时ACL文件> <目标目录>

说明:
    depth=1 表示源目录的直接子目录，depth=0 表示源目录本身；脚本只处理该层目录，
    不会继续递归到更深层。需要多层时，可用不同 --depth 多次执行。
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
from collections import Counter, defaultdict
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, as_completed, wait
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterator, List, Optional


class AclSyncError(Exception):
    """读取 / 写入 ACL 过程中的可预期错误。"""


@dataclass
class Result:
    src_rel: str
    dst_rel: str
    status: str  # ok / failed / skipped / dry-run
    detail: str = ""


LABELS = {
    "ok": "[ OK ]",
    "failed": "[FAIL]",
    "skipped": "[SKIP]",
    "dry-run": "[PLAN]",
}


def eprint(*values: object) -> None:
    print(*values, file=sys.stderr, flush=True)


def iter_dirs_at_depth(root: Path, depth: int) -> Iterator[Path]:
    """流式遍历 root 下正好第 depth 层的目录（depth=0 时返回 root 本身）。"""
    if depth == 0:
        yield root
        return

    stack = [(root, 0)]
    while stack:
        current, level = stack.pop()
        try:
            with os.scandir(current) as entries:
                for entry in entries:
                    try:
                        is_dir = entry.is_dir(follow_symlinks=False)
                    except OSError as exc:
                        eprint("[WARN] 无法判断路径类型 %s: %s" % (entry.path, exc))
                        continue
                    if not is_dir:
                        continue
                    child = Path(entry.path)
                    if level + 1 == depth:
                        yield child
                    else:
                        stack.append((child, level + 1))
        except OSError as exc:
            eprint("[WARN] 无法读取目录 %s: %s" % (current, exc))


def build_name_index(root: Path) -> Dict[str, List[Path]]:
    """构建目标目录树中「目录名 -> 路径列表」的索引（仅 --match auto 需要时构建）。"""

    index: Dict[str, List[Path]] = defaultdict(list)

    def on_error(exc: OSError) -> None:
        eprint("[WARN] 扫描目标目录失败 %s: %s" % (exc.filename or "", exc))

    for dirpath, dirnames, _filenames in os.walk(root, onerror=on_error):
        for name in dirnames:
            index[name].append(Path(dirpath) / name)
    return dict(index)


def parse_acl_entries(text: str) -> List[str]:
    """提取 ACL 条目文本，忽略空行与注释行（如 nfs4_getfacl 的 '# file:' 头）。"""
    entries: List[str] = []
    for line in text.splitlines():
        item = line.strip()
        if not item or item.startswith("#"):
            continue
        entries.append(item)
    return entries


def run_command(argv: List[str], input_text: Optional[str] = None) -> subprocess.CompletedProcess:
    try:
        return subprocess.run(
            argv,
            input=input_text,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
    except FileNotFoundError:
        raise AclSyncError("命令不存在: %s" % argv[0])
    except OSError as exc:
        raise AclSyncError("执行命令失败 (%s): %s" % (" ".join(argv), exc))


def command_error(proc: subprocess.CompletedProcess) -> str:
    output = (proc.stderr or "").strip() or (proc.stdout or "").strip()
    if len(output) > 800:
        output = output[:800] + " ..."
    return "返回码 %d: %s" % (proc.returncode, output or "(无输出)")


def build_set_argv(args: argparse.Namespace, acl_file: Path, dst: Path) -> List[str]:
    argv = [args.set_cmd]
    if args.set_mode == "file":
        if args.set_flag:
            argv.append(args.set_flag)
        argv.append(str(acl_file))
    argv.append(str(dst))
    return argv


def sync_one(
    idx: int,
    src: Path,
    dst: Path,
    src_rel: str,
    dst_rel: str,
    args: argparse.Namespace,
    workdir: str,
) -> Result:
    try:
        if args.dry_run:
            return Result(src_rel, dst_rel, "dry-run")

        proc = run_command([args.get_cmd, str(src)])
        if proc.returncode != 0:
            return Result(src_rel, dst_rel, "failed", "读取 ACL 失败: " + command_error(proc))

        entries = parse_acl_entries(proc.stdout)
        if not entries:
            return Result(src_rel, dst_rel, "failed", "源目录 ACL 为空（nfs4_getfacl 无有效条目）")

        acl_text = "\n".join(entries) + "\n"
        acl_file = Path(workdir) / ("%d.acl" % idx)
        with open(acl_file, "w", encoding="utf-8", newline="\n") as fh:
            fh.write(acl_text)

        proc = run_command(
            build_set_argv(args, acl_file, dst),
            input_text=acl_text if args.set_mode == "stdin" else None,
        )
        if proc.returncode != 0:
            return Result(src_rel, dst_rel, "failed", "写入 ACL 失败: " + command_error(proc))

        if args.verify:
            proc = run_command([args.get_cmd, str(dst)])
            if proc.returncode != 0:
                return Result(src_rel, dst_rel, "failed", "校验读取失败: " + command_error(proc))
            after = parse_acl_entries(proc.stdout)
            # 按条目计数比较，避免底层工具对 ACE 顺序做规范化后误报
            if Counter(after) != Counter(entries):
                missing = [item for item in entries if item not in after]
                extra = [item for item in after if item not in entries]
                detail = "校验不一致（源 %d 条 / 目标 %d 条" % (len(entries), len(after))
                if missing:
                    detail += "，目标缺少 %d 条" % len(missing)
                if extra:
                    detail += "，目标多出 %d 条" % len(extra)
                detail += "）"
                return Result(src_rel, dst_rel, "failed", detail)
            return Result(src_rel, dst_rel, "ok", "校验通过（%d 条）" % len(entries))

        return Result(src_rel, dst_rel, "ok")
    except Exception as exc:  # 单个目录失败不中断整体同步
        if isinstance(exc, AclSyncError):
            message = str(exc)
        else:
            message = "%s: %s" % (type(exc).__name__, exc)
        return Result(src_rel, dst_rel, "failed", message)


def resolve_target(
    src: Path,
    src_root: Path,
    dst_root: Path,
    match_mode: str,
    name_lookup,
) -> tuple[Optional[Path], str]:
    rel = src.relative_to(src_root)
    if str(rel) == ".":
        return dst_root, ""

    direct = dst_root / rel
    if direct.is_dir():
        return direct, ""

    if match_mode == "path":
        return None, "目标路径不存在: %s" % direct

    matches = name_lookup(src.name)
    if len(matches) == 1:
        return matches[0], ""
    if not matches:
        return None, "目标中未找到同名目录: %s" % src.name
    preview = ", ".join(str(path) for path in matches[:3])
    return None, "同名目录不唯一（%d 个）: %s" % (len(matches), preview)


def report_result(res: Result) -> None:
    line = "%s %s" % (LABELS.get(res.status, "[????]"), res.src_rel)
    if res.dst_rel and res.dst_rel != res.src_rel:
        line += "  ->  %s" % res.dst_rel
    if res.detail:
        line += "  |  " + res.detail
    print(line, flush=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="读取源目录第 N 层目录的 NFSv4 ACL，写入目标目录中对应的目录。",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "示例:\n"
            "  python3 nfs4_acl_sync.py /data/src /data/dst --depth 1\n"
            "  python3 nfs4_acl_sync.py /data/src /data/dst --depth 1 --verify --workers 4\n"
            "  python3 nfs4_acl_sync.py /data/src /data/dst --depth 2 --match auto\n"
            "\n"
            "默认命令调用:\n"
            "  nfs4_getfacl <源目录>\n"
            "  nfs4_setfacl -f <临时ACL文件> <目标目录>\n"
        ),
    )
    parser.add_argument("source", help="源目录")
    parser.add_argument("target", help="目标目录")
    parser.add_argument(
        "-n",
        "--depth",
        type=int,
        default=1,
        help="要同步的目录层级：1=源目录的直接子目录，0=源目录本身（默认: 1）",
    )
    parser.add_argument(
        "--match",
        choices=("path", "auto"),
        default="path",
        help="目标匹配方式：path=仅相同相对路径；auto=相对路径不存在时再按同名目录唯一匹配（默认: path）",
    )
    parser.add_argument("--get-cmd", default="nfs4_getfacl", help="读取 ACL 的命令（默认: nfs4_getfacl）")
    parser.add_argument("--set-cmd", default="nfs4_setfacl", help="写入 ACL 的命令（默认: nfs4_setfacl）")
    parser.add_argument(
        "--set-flag",
        default="-f",
        help="set 命令中用于传入 ACL 文件的选项（默认: -f；传空串则省略）",
    )
    parser.add_argument(
        "--set-mode",
        choices=("file", "stdin"),
        default="file",
        help="ACL 传递方式：file=用 --set-flag 指定临时文件；stdin=通过标准输入传入（默认: file）",
    )
    parser.add_argument("--verify", action="store_true", help="写入后重新读取目标 ACL 并与源比较")
    parser.add_argument("--workers", type=int, default=1, help="并发处理目录数（默认: 1）")
    parser.add_argument("--dry-run", action="store_true", help="只打印匹配结果，不读取 / 写入 ACL")
    parser.add_argument("--strict", action="store_true", help="存在跳过的目录时也返回非零退出码")
    parser.add_argument("-q", "--quiet", action="store_true", help="不打印成功 / 计划行")
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.depth < 0:
        parser.error("--depth 必须 >= 0")
    if args.workers < 1:
        parser.error("--workers 必须 >= 1")

    src_root = Path(args.source).expanduser().resolve()
    dst_root = Path(args.target).expanduser().resolve()

    if not src_root.is_dir():
        parser.error("源目录不存在或不是目录: %s" % src_root)
    if not dst_root.is_dir():
        parser.error("目标目录不存在或不是目录: %s" % dst_root)

    try:
        dst_root.relative_to(src_root)
    except ValueError:
        pass
    else:
        parser.error("目标目录不能位于源目录内部: %s" % dst_root)

    if not args.dry_run:
        missing = [cmd for cmd in (args.get_cmd, args.set_cmd) if not shutil.which(cmd)]
        if missing:
            eprint("[ERROR] 找不到命令: %s" % ", ".join(missing))
            eprint("        请确认 nfs4-acl-tools 已安装，或通过 --get-cmd / --set-cmd 指定命令路径。")
            return 2

    name_index: Optional[Dict[str, List[Path]]] = None

    def name_lookup(name: str) -> List[Path]:
        nonlocal name_index
        if name_index is None:
            eprint("[INFO] 正在构建目标目录同名索引 ...")
            name_index = build_name_index(dst_root)
        return name_index.get(name, [])

    counts: Counter = Counter()
    total = 0
    ok_like = ("ok", "dry-run")

    def emit(res: Result) -> None:
        counts[res.status] += 1
        if not (args.quiet and res.status in ok_like):
            report_result(res)

    with tempfile.TemporaryDirectory(prefix="nfs4_acl_sync_") as workdir:
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            pending = set()
            for src in iter_dirs_at_depth(src_root, args.depth):
                total += 1
                src_rel = str(src.relative_to(src_root))
                dst, reason = resolve_target(src, src_root, dst_root, args.match, name_lookup)
                if dst is None:
                    emit(Result(src_rel, "", "skipped", reason))
                    continue
                try:
                    dst_rel = str(dst.relative_to(dst_root))
                except ValueError:
                    dst_rel = str(dst)

                pending.add(
                    pool.submit(sync_one, total, src, dst, src_rel, dst_rel, args, workdir)
                )
                if len(pending) >= args.workers * 4:
                    done, pending = wait(pending, return_when=FIRST_COMPLETED)
                    for future in done:
                        emit(future.result())

            for future in as_completed(pending):
                emit(future.result())

    print()
    print("=" * 64)
    print("源目录       : %s" % src_root)
    print("目标目录     : %s" % dst_root)
    print("目录层级     : %d（0=源目录本身）" % args.depth)
    print("扫描目录数   : %d" % total)
    print("成功         : %d" % counts["ok"])
    print("失败         : %d" % counts["failed"])
    print("跳过         : %d" % counts["skipped"])
    if args.dry_run:
        print("计划(未执行) : %d" % counts["dry-run"])

    if counts["failed"]:
        return 1
    if args.strict and counts["skipped"]:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
