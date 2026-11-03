# 智能文件迁移 (fsync)

面向海量文件的本地 Web 拷贝工具，复刻桌面版"智能文件迁移"界面：左侧任务列表 + 右侧进度矩阵 + 系统日志控制台。

浏览器无法访问本地文件系统，因此 **真正的扫描 / 拷贝 / 校验全部在后端 Python 进程内完成**，前端只是同域 SPA，通过 REST + WebSocket 接收实时进度。

## 功能

- **多任务管理**：新建、搜索、批量选中、开始/暂停/继续/取消/删除/重试
- **并发拷贝**：每任务独立线程数（默认 4），有界队列生产者-消费者模型
- **限速**：令牌桶限速器（MB/s），长期平均速率稳定
- **原子写入**：`*.part` 临时文件 + `os.replace` 原子替换，任务中断/取消不留半截文件
- **增量模式**：目标文件 size + mtime 一致则跳过
- **校验**：目录级结构比对（文件数/总大小/软链接数）+ 文件级 MD5 哈希校验
- **实时进度**：滑动窗口测速（~3 秒样本），预计剩余时间，进度条
- **持久化**：任务快照写 `tasks.json`（COW 原子替换）、日志写 `logs.jsonl`（追加 + 1MB 轮转）
- **重启恢复**：运行中任务恢复为 `interrupted`（暂停态，数据计数保留），点「重启」重跑
- **长路径**：Windows 自动添加 `\\?\` 前缀
- **零构建**：纯 FastAPI + vanilla HTML/CSS/JS，没有 React/Node 依赖

## 快速开始

> **Linux（Debian/Ubuntu）**：系统 Python 被 PEP 668 标记为"外部管理"，必须用虚拟环境，见下方说明。

```bash
pip install -r requirements.txt
python server.py
```

### Linux 虚拟环境（PEP 668）

Debian 12+ / Ubuntu 23.04+ 默认启用 PEP 668，直接 `pip install` 会报 `externally-managed-environment`。解决方式：

```bash
apt install -y python3-venv          # 补齐 ensurepip（部分发行版叫 python3.12-venv）
python3 -m venv .venv                # 创建虚拟环境
source .venv/bin/activate           # 激活（退出用 deactivate）
pip install -r requirements.txt      # 此时 pip 来自 venv，不受限制
python server.py
```

> 不要用 `pipx`（只适用于命令行工具），也不要加 `--break-system-packages`（有破坏系统 Python 风险）。

### 环境变量

默认监听 `127.0.0.1:8000`，通过环境变量覆盖：

```bash
FSSYNC_HOST=0.0.0.0 FSSYNC_PORT=9000 python server.py
```

## 项目结构

```
fsync/
├── engine.py          # 拷贝引擎核心（Task / Engine / Storage / RateLimiter）
├── server.py          # FastAPI 后端（REST + WebSocket + 静态托管）
├── requirements.txt   # fastapi + uvicorn
├── static/
│   ├── index.html     # SPA 结构
│   ├── style.css      # 深色主题
│   └── app.js         # 前端逻辑（WebSocket 接入、任务卡片、日志面板）
├── .fsync_data/       # 运行时持久化目录（git ignore）
│   ├── tasks.json     # 所有任务快照
│   ├── logs.jsonl     # 引擎日志（JSONL，追加写入）
│   └── logs.1.jsonl   # 轮转日志（满 1MB）
└── .gitignore
```

## REST API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/tasks` | 任务快照 + 聚合统计 |
| POST | `/api/tasks` | 新建任务 |
| POST | `/api/tasks/{id}/pause` | 暂停 |
| POST | `/api/tasks/{id}/resume` | 继续（`interrupted` 状态下实际是 retry） |
| POST | `/api/tasks/{id}/cancel` | 取消 |
| POST | `/api/tasks/{id}/retry` | 重跑 |
| DELETE | `/api/tasks/{id}` | 删除 |
| GET | `/api/logs?after=&task_id=&level=&limit=` | 历史日志 |
| GET | `/api/list-dirs?path=` | 目录选择器 |

WebSocket 端点 `/ws`：连接后先推一份完整 init 包，之后每 ~350ms 推一次增量 update 包。

## 面向海量文件的设计

- **流式遍历**：`os.scandir` + 生成器，扫描阶段不物化百万级文件清单，内存恒定
- **有界队列**：`queue.Queue(maxsize=workers*16)`，不会一次性把 100 万个文件塞进内存
- **合并写盘**：状态变更不立即落盘，350ms 内多次 → 只写一次；活跃任务每 ~2s 合并写盘；新任务/删除/终端状态立即落盘
- **COW 原子写**：`tasks.json.tmp` → rename 覆盖 → 清理 `.bak`，进程崩溃最多留下 `.tmp`，主文件不会损坏
- **滑动窗口测速**：只保留最近 ~3 秒样本，平滑抖动同时不会让内存无限增长

## 已知限制

- 真正的断点续传（从某个文件中间继续）未实现，`interrupted` → retry 是从头重跑，靠增量模式避免重复拷贝已完成的文件
- 不做权限升级 / UAC 提权，访问被拒绝的目录会在日志里报错并跳过
- 跨盘符/跨目标存储集群拷贝时速度由 OS 决定，限速只是上限

## License

MIT
