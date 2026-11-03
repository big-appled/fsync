# 打包二进制（PyInstaller）

项目是一个 FastAPI + WebSocket + 静态前端的本地文件迁移工具，可用 **PyInstaller** 打成单文件可执行程序，方便在没有 Python 环境的服务器上直接运行。

代码已做 PyInstaller 兼容（`server.py` 用 `sys._MEIPASS` 定位 `static/`），因此**不需要改任何代码**，只要按下面步骤打包即可。

---

## 一、前置说明（重要）

1. **PyInstaller 不能跨平台交叉编译**
   - 要打 **Windows 的 .exe** → 必须在 Windows 上打
   - 要打 **Linux 的 ELF** → 必须在 Linux 上打

2. **单文件（--onefile）**
   - 启动时会把内置的 Python 运行时和 `static/` 解压到系统临时目录，**第一次启动稍慢**（约 1~3 秒），之后正常。
   - 好处是部署只要拷贝一个文件。

3. **数据目录**
   - 程序运行时仍会在**当前工作目录**下生成 `.fsync_data/`（任务快照、日志 jsonl）。
   - 建议在专门目录里运行，例如 `mkdir -p /opt/fsync && cd /opt/fsync && ./fsync`。

---

## 二、Windows 打包（本机可直接执行）

> 已在开发机（Windows + PowerShell）验证过流程。

### 1. 准备虚拟环境（避免污染全局 Python）

```powershell
cd d:\shdn\github\big-appled\fsync
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 2. 安装 PyInstaller

```powershell
pip install pyinstaller
```

### 3. 打包

```powershell
pyinstaller `
  --onefile `
  --name fsync `
  --add-data "static;static" `
  --hidden-import=uvicorn.logging `
  --hidden-import=uvicorn.loops `
  --hidden-import=uvicorn.loops.auto `
  --hidden-import=uvicorn.protocols `
  --hidden-import=uvicorn.protocols.http `
  --hidden-import=uvicorn.protocols.http.auto `
  --hidden-import=uvicorn.protocols.websockets `
  --hidden-import=uvicorn.protocols.websockets.auto `
  --hidden-import=uvicorn.lifespan `
  --hidden-import=uvicorn.lifespan.on `
  server.py
```

> 如果嫌 hidden-import 太多，可以先把上面命令保存为 `fsync.spec`（见「进阶：用 spec 文件」），之后每次只改版本号即可。

### 4. 产物

- 生成的可执行文件在 `dist\fsync.exe`
- 把它拷贝到任何 Windows 机器，双击或在 PowerShell 运行：

```powershell
# 设置监听端口（默认 8000）
$env:FSSYNC_PORT = "8000"
.\fsync.exe
```

浏览器打开 `http://127.0.0.1:8000` 即可看到界面。

### 5. 冒烟测试

```powershell
# 在 dist 目录下运行，确认能正常启动
cd dist
.\fsync.exe
# 打开浏览器 http://127.0.0.1:8000
```

---

## 三、Linux 打包（在目标服务器上执行）

> **必须在 Linux 上打包**，因为 PyInstaller 生成的 ELF 不能反向运行在 Windows。

下面以 **Debian/Ubuntu** 为例（CentOS/RHEL 把 `apt` 换成 `yum/dnf` 即可）。

### 1. 安装系统依赖

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-dev build-essential
```

> `python3-dev` 和 `build-essential` 是编译某些 Python C 扩展时需要的；本项目只用纯 Python 库（fastapi/uvicorn/websockets），但装上可以避免踩坑。

### 2. 创建虚拟环境并安装依赖

```bash
cd /path/to/fsync          # 把项目源码拷到 Linux 机器上
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

> 如果遇到 `externally-managed-environment`（PEP 668），说明系统 Python 受限，**必须用 venv**（上面已经是 venv 了）。

### 3. 安装 PyInstaller

```bash
pip install pyinstaller
```

### 4. 打包

```bash
pyinstaller \
  --onefile \
  --name fsync \
  --add-data "static:static" \
  --hidden-import=uvicorn.logging \
  --hidden-import=uvicorn.loops \
  --hidden-import=uvicorn.loops.auto \
  --hidden-import=uvicorn.protocols \
  --hidden-import=uvicorn.protocols.http \
  --hidden-import=uvicorn.protocols.http.auto \
  --hidden-import=uvicorn.protocols.websockets \
  --hidden-import=uvicorn.protocols.websockets.auto \
  --hidden-import=uvicorn.lifespan \
  --hidden-import=uvicorn.lifespan.on \
  server.py
```

> **注意**：`--add-data` 在 Linux 用冒号 `:` 分隔，Windows 用分号 `;`。

### 5. 产物 + 运行

```bash
# 产物在 dist/fsync
chmod +x dist/fsync

# 建议在独立目录运行，数据会写在当前目录的 .fsync_data/
mkdir -p /opt/fsync
cp dist/fsync /opt/fsync/
cd /opt/fsync
./fsync
```

浏览器打开 `http://<服务器IP>:8000`。

### 6. 后台运行（可选）

```bash
cd /opt/fsync
nohup ./fsync > fsync.log 2>&1 &
```

或用 systemd：

```ini
# /etc/systemd/system/fsync.service
[Unit]
Description=fsync file migration
After=network.target

[Service]
WorkingDirectory=/opt/fsync
ExecStart=/opt/fsync/fsync
Restart=always

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now fsync
```

---

## 四、进阶：用 spec 文件（推荐）

把上面长长的命令保存为 `fsync.spec`，以后每次只改版本号。

**fsync.spec**（放在项目根目录）

```python
# -*- mode: python ; coding: utf-8 -*-
import sys
from PyInstaller.utils.hooks import collect_submodules

block_cipher = None

a = Analysis(
    ['server.py'],
    pathex=[],
    binaries=[],
    datas=[('static', 'static')],
    hiddenimports=[
        'uvicorn.logging',
        'uvicorn.loops',
        'uvicorn.loops.auto',
        'uvicorn.protocols',
        'uvicorn.protocols.http',
        'uvicorn.protocols.http.auto',
        'uvicorn.protocols.websockets',
        'uvicorn.protocols.websockets.auto',
        'uvicorn.lifespan',
        'uvicorn.lifespan.on',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)
pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='fsync',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,          # 改成 False 可以隐藏 Windows 下的黑色控制台
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
```

**之后打包只需一行：**

```bash
pyinstaller fsync.spec
```

---

## 五、常见问题（FAQ）

### Q1: 运行时提示找不到 `static` 或 404
- 检查打包命令里的 `--add-data` 是否写对分隔符（Windows `;`，Linux `:`）。
- 确认 `static/` 目录在项目根目录存在。

### Q2: 启动很慢 / 第一次打开浏览器空白
- 单文件模式启动时要解压临时文件，第一次稍慢（1~3 秒），属于正常现象。

### Q3: 想改默认端口 / 监听地址
- 通过环境变量：
  ```bash
  export FSSYNC_PORT=9000
  export FSSYNC_HOST=0.0.0.0
  ./fsync
  ```

### Q4: Linux 上提示 `Permission denied`
- 给二进制加执行权限：`chmod +x fsync`

### Q5: 想打成“双击就能跑、不要黑色控制台”的 Windows 程序
- 在 `fsync.spec` 里把 `console=True` 改成 `console=False`，然后重新打包。
- 注意：没有控制台后，日志要看 `.fsync_data/logs.jsonl` 文件。

---

## 六、总结

| 平台   | 是否需要在对应系统打包 | 命令格式 | 产物 |
|--------|----------------------|----------|------|
| Windows | ✅（本机即可） | `pyinstaller --onefile --add-data "static;static" ... server.py` | `dist/fsync.exe` |
| Linux   | ✅（必须在 Linux 上） | `pyinstaller --onefile --add-data "static:static" ... server.py` | `dist/fsync` |

按上面步骤操作即可生成单文件可执行程序，无需在目标机器上安装 Python。
