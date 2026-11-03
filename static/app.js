"use strict";

// --------------------------------------------------------------------------- //
// 状态
// --------------------------------------------------------------------------- //
const state = {
  tasks: [],
  aggregate: {},
  selectedId: null,
  checked: new Set(),
  logs: [],          // 系统日志（概要，来自 WS push）
  logFilter: "ALL",
  logSearch: "",
  search: "",
  // --- 任务专属日志 ---
  taskLogs: [],      // 当前选中 task 的专属日志
  taskLogSeq: 0,     // 上次拉到的 task_log_seq_local
  logPanelMode: "system",  // "system" | "task"
};

const STATUS_TEXT = {
  pending: "待开始",
  scanning: "扫描中",
  running: "运行中",
  paused: "已暂停",
  verifying: "校验中",
  done: "已完成",
  failed: "失败",
  cancelled: "已取消",
  interrupted: "中断",
};
const ACTIVE = ["scanning", "running", "verifying"];
const RESUMABLE = ["paused", "interrupted"];

const $ = (id) => document.getElementById(id);

// --------------------------------------------------------------------------- //
// 格式化
// --------------------------------------------------------------------------- //
function fmtSize(n) {
  n = Number(n) || 0;
  const u = ["B", "KB", "MB", "GB", "TB", "PB"];
  let i = 0;
  while (n >= 1024 && i < u.length - 1) { n /= 1024; i++; }
  return (i === 0 ? `${Math.round(n)} B` : `${n.toFixed(2)} ${u[i]}`);
}
function fmtSpeed(bps) {
  if (!bps || bps <= 0) return "0 MB/s";
  return fmtSize(bps) + "/s";
}
function fmtEta(sec) {
  if (sec == null || sec < 0) return "--";
  sec = Math.round(sec);
  if (sec < 60) return `${sec}s`;
  const h = Math.floor(sec / 3600), m = Math.floor((sec % 3600) / 60), s = sec % 60;
  return h ? `${h}h ${m}m` : `${m}m ${s}s`;
}
function num(n) { return (Number(n) || 0).toLocaleString("en-US"); }
function esc(s) {
  return String(s ?? "").replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
}

// --------------------------------------------------------------------------- //
// 数据接入
// --------------------------------------------------------------------------- //
function connect() {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  const ws = new WebSocket(`${proto}://${location.host}/ws`);
  ws.onopen = () => setConn(true);
  ws.onclose = () => { setConn(false); setTimeout(connect, 1500); };
  ws.onerror = () => ws.close();
  ws.onmessage = (ev) => {
    let data;
    try { data = JSON.parse(ev.data); } catch { return; }
    if (data.tasks) { state.tasks = data.tasks; state.aggregate = data.aggregate || {}; }
    if (data.logs) appendSystemLogs(data.logs);
    if (data.type === "init" && !state.selectedId && state.tasks.length) {
      state.selectedId = state.tasks[0].id;
      loadTaskLogs(state.selectedId, true);
    }
    // WS update 时如果处于 task 日志模式，顺便拉一下该 task 的增量日志
    if (data.type === "update" && state.logPanelMode === "task" && state.selectedId) {
      loadTaskLogs(state.selectedId, false);
    }
    render();
  };
}
function setConn(on) {
  const el = $("conn");
  el.classList.toggle("on", on);
  el.classList.toggle("off", !on);
  $("conn-text").textContent = on ? "已连接" : "连接已断开";
}

function appendSystemLogs(records) {
  if (!records.length) return;
  const box = $("log-list");
  const atBottom = box.scrollHeight - box.scrollTop - box.clientHeight < 40;
  state.logs.push(...records);
  // 只有在显示系统日志时才渲染
  if (state.logPanelMode === "system") {
    renderLogs();
    if (atBottom) box.scrollTop = box.scrollHeight;
  }
}

// --------------------------------------------------------------------------- //
// REST 调用
// --------------------------------------------------------------------------- //
async function api(method, url, body) {
  const opt = { method, headers: {} };
  if (body) { opt.headers["Content-Type"] = "application/json"; opt.body = JSON.stringify(body); }
  const res = await fetch(url, opt);
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || `${res.status}`);
  return data;
}

// --------------------------------------------------------------------------- //
// Task 专属日志拉取（增量）
// --------------------------------------------------------------------------- //
async function loadTaskLogs(taskId, fullReload) {
  if (!taskId) return;
  const after = fullReload ? 0 : state.taskLogSeq;
  try {
    const data = await api("GET", `/api/tasks/${taskId}/logs?after=${after}`);
    const newRecords = data.logs || [];
    const box = $("log-list");
    const atBottom = box.scrollHeight - box.scrollTop - box.clientHeight < 40;

    if (fullReload) {
      state.taskLogs = newRecords;
    } else if (newRecords.length) {
      state.taskLogs.push(...newRecords);
    }
    state.taskLogSeq = data.task_seq || state.taskLogSeq;

    if (state.logPanelMode === "task") {
      renderLogs();
      if (atBottom) box.scrollTop = box.scrollHeight;
    }
  } catch (e) {
    // 任务不存在 / 后端还没启动，忽略
  }
}

// 定时轮询 task 专属日志（仅当显示 task 面板且当前有选中任务时）
let _taskLogPollTimer = null;
function startTaskLogPolling() {
  stopTaskLogPolling();
  _taskLogPollTimer = setInterval(() => {
    if (state.logPanelMode === "task" && state.selectedId) {
      loadTaskLogs(state.selectedId, false);
    }
  }, 1500);
}
function stopTaskLogPolling() {
  if (_taskLogPollTimer) { clearInterval(_taskLogPollTimer); _taskLogPollTimer = null; }
}

// --------------------------------------------------------------------------- //
// 渲染
// --------------------------------------------------------------------------- //
function render() { renderStats(); renderTaskList(); renderDetail(); renderLogs(); }

function renderStats() {
  const a = state.aggregate || {};
  $("stat-running").textContent = a.running || 0;
  $("stat-done").textContent = a.done || 0;
  $("stat-speed").textContent = fmtSpeed(a.speed || 0);
}

function visibleTasks() {
  const q = state.search.trim().toLowerCase();
  if (!q) return state.tasks;
  return state.tasks.filter((t) =>
    t.name.toLowerCase().includes(q) || t.source.toLowerCase().includes(q) || t.dest.toLowerCase().includes(q));
}

const taskDom = new Map();

function cardNode() {
  const el = document.createElement("div");
  el.className = "task-card";
  el.innerHTML = `
    <div class="row1">
      <div class="tname-wrap">
        <div class="task-name"></div>
        <div class="task-path s-path"></div>
      </div>
      <div class="row1-right">
        <input type="checkbox" class="tick" />
        <span class="pill"></span>
      </div>
    </div>
    <div class="task-meta"><span class="m-left"></span><span class="pct"></span></div>
    <div class="d-row"><span class="task-path d-path"></span><span class="rate"></span></div>
    <div class="task-bar"><i></i></div>`;
  return el;
}

function updateCard(el, t) {
  const rate = t.options && t.options.rate_limit_mb > 0 ? `${t.options.rate_limit_mb} MB/s` : "不限速";
  const speed = ACTIVE.includes(t.status) ? ` · ${fmtSpeed(t.speed)}` : "";
  el.classList.toggle("active", t.id === state.selectedId);
  el.classList.toggle("done", t.status === "done");
  el.dataset.id = t.id;
  el.querySelector(".task-name").textContent = t.name;
  el.querySelector(".s-path").textContent = t.source;
  el.querySelector(".d-path").textContent = t.dest;
  const tick = el.querySelector(".tick");
  tick.dataset.id = t.id;
  tick.checked = state.checked.has(t.id);
  const pill = el.querySelector(".pill");
  pill.className = "pill " + t.status;
  pill.textContent = STATUS_TEXT[t.status] || t.status;
  el.querySelector(".m-left").textContent = `${num(t.files_done)}/${num(t.files_total)}${speed}`;
  el.querySelector(".pct").textContent = `${(t.percent || 0).toFixed(0)}%`;
  el.querySelector(".rate").textContent = rate;
  el.querySelector(".task-bar > i").style.width = `${(t.percent || 0).toFixed(1)}%`;
}

function renderTaskList() {
  const list = $("task-list");
  const tasks = visibleTasks();
  if (!tasks.length) {
    taskDom.clear();
    list.innerHTML = `<div class="empty">${state.tasks.length ? "无匹配任务" : "暂无任务，点击「新建任务」开始迁移"}</div>`;
    return;
  }
  const empty = list.querySelector(".empty");
  if (empty) empty.remove();

  const wanted = new Set(tasks.map((t) => t.id));
  for (const [id, el] of [...taskDom]) {
    if (!wanted.has(id)) { el.remove(); taskDom.delete(id); }
  }
  let prev = null;
  for (const t of tasks) {
    let el = taskDom.get(t.id);
    if (!el) { el = cardNode(); taskDom.set(t.id, el); }
    updateCard(el, t);
    const anchor = prev ? prev.nextSibling : list.firstChild;
    if (el !== anchor) list.insertBefore(el, anchor);
    prev = el;
  }
  while (prev && prev.nextSibling) prev.nextSibling.remove();
}

function renderDetail() {
  const t = state.tasks.find((x) => x.id === state.selectedId);
  if (!t) {
    $("d-source").textContent = "--";
    $("d-dest").textContent = "--";
    $("d-speed").textContent = "0 MB/s";
    $("d-eta").textContent = "--";
    $("d-status").textContent = "未选择任务";
    $("d-status").className = "run-state";
    $("d-progress").style.width = "0%";
    $("d-files").textContent = "0 / 0";
    $("d-bytes").textContent = "0 B / 0 B";
    $("d-incremental").textContent = "--";
    $("d-verify").textContent = "--";
    updateActionButtons(null);
    return;
  }
  $("d-source").textContent = t.source;
  $("d-dest").textContent = t.dest;
  $("d-speed").textContent = fmtSpeed(t.speed);
  $("d-eta").textContent = t.status === "done" ? "--" : fmtEta(t.eta);
  const st = $("d-status");
  st.textContent = STATUS_TEXT[t.status] || t.status;
  st.className = "run-state " + t.status;
  $("d-progress").style.width = `${(t.percent || 0).toFixed(1)}%`;
  $("d-files").textContent = `${num(t.files_done)} / ${num(t.files_total)}`;
  $("d-bytes").textContent = `${t.bytes_done_text} / ${t.bytes_total_text}`;
  $("d-incremental").textContent = t.options.incremental ? "mtime" : "关闭";
  const vt = t.verify_total ? `大小: ${num(t.verify_done)}/${num(t.verify_total)}` :
             (t.options.verify ? "MD5 开启" : "关闭");
  $("d-verify").textContent = vt;
  updateActionButtons(t);
}

function updateActionButtons(t) {
  const cancel = $("btn-cancel"), start = $("btn-start");
  if (!t) {
    cancel.disabled = start.disabled = true;
    start.textContent = "开始";
    return;
  }
  cancel.disabled = !ACTIVE.includes(t.status);
  if (ACTIVE.includes(t.status)) { start.disabled = true; start.textContent = "运行中"; return; }
  if (t.status === "paused") { start.disabled = false; start.textContent = "继续"; return; }
  if (t.status === "interrupted") { start.disabled = false; start.textContent = "重启"; return; }
  if (["done", "failed", "cancelled"].includes(t.status)) { start.disabled = false; start.textContent = "重试"; return; }
  start.disabled = false; start.textContent = "开始";
}

function renderLogs() {
  const box = $("log-list");
  const q = state.logSearch.trim().toLowerCase();
  const rows = (state.logPanelMode === "task" ? state.taskLogs : state.logs).filter((r) => {
    if (state.logFilter !== "ALL" && r.level !== state.logFilter) return false;
    if (q && !r.message.toLowerCase().includes(q) && !r.time.includes(q)) return false;
    return true;
  });
  box.innerHTML = rows.map((r) => `
    <div class="log-line">
      <span class="t">[${r.time}]</span>
      <span class="lv ${r.level}">${r.level}</span>
      <span class="msg">${esc(r.message)}</span>
    </div>`).join("") || `<div class="empty" style="padding:30px">${
      state.logPanelMode === "task"
        ? (state.selectedId ? "（暂无任务日志）" : "（未选择任务）")
        : "（暂无系统日志）"
    }</div>`;
  $("log-count").textContent = `${rows.length} / ${state.logPanelMode === "task" ? state.taskLogs.length : state.logs.length} 记录`;
}

// --------------------------------------------------------------------------- //
// 批量操作
// --------------------------------------------------------------------------- //
function targetIds() {
  const ids = [...state.checked];
  if (!ids.length && state.selectedId) ids.push(state.selectedId);
  return ids;
}

async function bulkAction(kind) {
  const ids = targetIds();
  if (!ids.length) return;
  await Promise.all(ids.map((id) => {
    const t = state.tasks.find((x) => x.id === id);
    if (!t) return Promise.resolve();
    if (kind === "start") {
      if (t.status === "paused") return api("POST", `/api/tasks/${id}/resume`);
      if (["pending", "failed", "cancelled", "done"].includes(t.status)) return api("POST", `/api/tasks/${id}/retry`);
      return Promise.resolve();
    }
    if (kind === "pause") return api("POST", `/api/tasks/${id}/pause`);
    if (kind === "delete") return api("DELETE", `/api/tasks/${id}`);
    return Promise.resolve();
  })).catch((e) => alert("操作失败: " + e.message));
}

// --------------------------------------------------------------------------- //
// 新建任务弹窗
// --------------------------------------------------------------------------- //
function openModal() { $("modal-err").textContent = ""; $("modal-mask").classList.add("show"); }
function closeModal() { $("modal-mask").classList.remove("show"); }

async function createTask() {
  const source = $("in-source").value.trim();
  const dest = $("in-dest").value.trim();
  const body = {
    name: $("in-name").value.trim(),
    source, dest,
    options: {
      threads: parseInt($("in-threads").value) || 4,
      rate_limit_mb: parseFloat($("in-rate").value) || 0,
      incremental: $("in-incremental").checked,
      verify: $("in-verify").checked,
      overwrite: $("in-overwrite").checked,
      dest_mode: $("in-dest-mode").value,
    },
  };
  try {
    const t = await api("POST", "/api/tasks", body);
    state.selectedId = t.id;
    // 切换到任务日志面板并拉一次
    switchLogPanel("task");
    loadTaskLogs(t.id, true);
    closeModal();
    $("in-name").value = ""; $("in-source").value = ""; $("in-dest").value = "";
  } catch (e) {
    $("modal-err").textContent = e.message;
  }
}

// --------------------------------------------------------------------------- //
// 日志面板切换
// --------------------------------------------------------------------------- //
function switchLogPanel(mode) {
  state.logPanelMode = mode;
  document.querySelectorAll(".log-tab").forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.tab === mode);
  });
  if (mode === "task" && state.selectedId) {
    // 切换到任务日志时全量拉一次
    loadTaskLogs(state.selectedId, true);
  } else if (mode === "system") {
    // 切回系统日志时，清空 taskLogs 以免内存滞留
    state.taskLogs = [];
    state.taskLogSeq = 0;
  }
  startTaskLogPolling();
  renderLogs();
}

// --------------------------------------------------------------------------- //
// 目录选择器
// --------------------------------------------------------------------------- //
const picker = { targetId: null, path: "" };
async function openPicker(targetId) {
  picker.targetId = targetId;
  const seed = $(targetId).value.trim();
  await loadDirs(seed || "");
  $("picker-mask").classList.add("show");
}
async function loadDirs(path) {
  try {
    const data = await api("GET", `/api/list-dirs?path=${encodeURIComponent(path)}`);
    picker.path = data.path;
    $("picker-path").textContent = data.path;
    const items = [];
    if (data.parent && data.parent !== data.path) {
      items.push(`<div class="picker-item" data-go="${esc(data.parent)}"><span class="ico">↰</span>..</div>`);
    }
    for (const d of data.dirs) {
      items.push(`<div class="picker-item" data-go="${esc(join(data.path, d))}"><span class="ico">▸</span>${esc(d)}</div>`);
    }
    $("picker-list").innerHTML = items.join("") || '<div class="empty">（无子目录）</div>';
  } catch (e) {
    $("picker-list").innerHTML = `<div class="empty">${esc(e.message)}</div>`;
  }
}
function join(base, name) {
  const sep = base.includes("\\") ? "\\" : "/";
  return base.replace(/[\\/]+$/, "") + sep + name;
}
function closePicker() { $("picker-mask").classList.remove("show"); }

// --------------------------------------------------------------------------- //
// 事件绑定
// --------------------------------------------------------------------------- //
$("btn-new").onclick = openModal;
$("modal-close").onclick = closeModal;
$("modal-cancel").onclick = closeModal;
$("modal-create").onclick = createTask;
$("picker-close").onclick = closePicker;
$("picker-cancel").onclick = closePicker;
$("picker-choose").onclick = () => { $(picker.targetId).value = picker.path; closePicker(); };

document.querySelectorAll("[data-browse]").forEach((b) => {
  b.onclick = () => openPicker(b.dataset.browse);
});

document.querySelectorAll(".log-tab").forEach((btn) => {
  btn.onclick = () => switchLogPanel(btn.dataset.tab);
});

$("task-search").oninput = (e) => { state.search = e.target.value; renderTaskList(); };
$("log-search").oninput = (e) => { state.logSearch = e.target.value; renderLogs(); };
$("log-filter").onchange = (e) => { state.logFilter = e.target.value; renderLogs(); };
$("log-clear").onclick = () => {
  if (state.logPanelMode === "system") state.logs = [];
  else { state.taskLogs = []; state.taskLogSeq = 0; }
  renderLogs();
};

$("select-all").onchange = (e) => {
  state.checked = e.target.checked ? new Set(visibleTasks().map((t) => t.id)) : new Set();
  renderTaskList();
};
$("bulk-start").onclick = () => bulkAction("start");
$("bulk-pause").onclick = () => bulkAction("pause");
$("bulk-delete").onclick = () => bulkAction("delete");

$("btn-cancel").onclick = async () => {
  const t = state.tasks.find((x) => x.id === state.selectedId);
  if (!t) return;
  await api("POST", `/api/tasks/${t.id}/cancel`).catch(() => {});
};
$("btn-start").onclick = async () => {
  const t = state.tasks.find((x) => x.id === state.selectedId);
  if (!t) return;
  if (t.status === "paused") await api("POST", `/api/tasks/${t.id}/resume`).catch(() => {});
  else await api("POST", `/api/tasks/${t.id}/retry`).catch(() => {});
};

$("task-list").addEventListener("click", (e) => {
  const tick = e.target.closest(".tick");
  if (tick) {
    e.stopPropagation();
    if (tick.checked) state.checked.add(tick.dataset.id); else state.checked.delete(tick.dataset.id);
    renderTaskList();
    return;
  }
  const card = e.target.closest(".task-card");
  if (card && card.dataset.id !== state.selectedId) {
    state.selectedId = card.dataset.id;
    state.taskLogs = [];
    state.taskLogSeq = 0;
    if (state.logPanelMode === "task") loadTaskLogs(state.selectedId, true);
    render();
  }
});

$("picker-list").addEventListener("click", (e) => {
  const item = e.target.closest("[data-go]");
  if (item) loadDirs(item.dataset.go);
});

document.addEventListener("keydown", (e) => {
  if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === "n") { e.preventDefault(); openModal(); }
  if (e.key === "Escape") { closeModal(); closePicker(); }
});
[$("modal-mask"), $("picker-mask")].forEach((m) => {
  m.addEventListener("click", (e) => { if (e.target === m) m.classList.remove("show"); });
});

// 启动
startTaskLogPolling();
connect();
api("GET", "/api/logs?limit=300").then((d) => {
  state.logs = d.logs || [];
  renderLogs();
}).catch(() => {});
