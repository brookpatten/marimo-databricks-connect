// Cluster Widget Frontend — single cluster operational dashboard
// Supports: status, config view, edit config, events, start/stop/restart

const OPS_STYLES = `
  :host {
    --op-bg: #ffffff; --op-bg-alt: #f8f9fa; --op-bg-hover: #e9ecef;
    --op-border: #dee2e6; --op-text: #212529; --op-text-muted: #6c757d;
    --op-primary: #0d6efd; --op-primary-hover: #0b5ed7;
    --op-success: #198754; --op-danger: #dc3545;
    --op-warning: #ffc107; --op-info: #0dcaf0;
    --op-font: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
    --op-font-mono: "SF Mono", "Cascadia Code", "Fira Code", Menlo, Consolas, monospace;
    --op-radius: 6px;
    display: block; font-family: var(--op-font); font-size: 13px;
    color: var(--op-text); background: var(--op-bg);
    border: 1px solid var(--op-border); border-radius: var(--op-radius); overflow: hidden;
  }
  :host(.dark-theme) { --op-bg: #1e1e1e; --op-bg-alt: #252526; --op-bg-hover: #2d2d30;
      --op-border: #3e3e42; --op-text: #cccccc; --op-text-muted: #888888;
      --op-primary: #4fc3f7; --op-primary-hover: #29b6f6;
      --op-success: #66bb6a; --op-danger: #ef5350;
      --op-warning: #ffca28; --op-info: #4dd0e1; }
  * { box-sizing: border-box; }
  .op-header { display: flex; align-items: center; gap: 8px; padding: 10px 14px; background: var(--op-bg-alt); border-bottom: 1px solid var(--op-border); flex-wrap: wrap; }
  .op-header h2 { margin: 0; font-size: 14px; font-weight: 600; }
  .op-header-actions { margin-left: auto; display: flex; gap: 6px; align-items: center; }
  .op-btn { padding: 4px 10px; border: 1px solid var(--op-border); border-radius: 4px; background: var(--op-bg); color: var(--op-text); cursor: pointer; font-size: 12px; font-family: var(--op-font); transition: background 0.15s; }
  .op-btn:hover { background: var(--op-bg-hover); }
  .op-btn:disabled { opacity: 0.5; cursor: not-allowed; }
  .op-btn-primary { background: var(--op-primary); color: #fff; border-color: var(--op-primary); }
  .op-btn-primary:hover { background: var(--op-primary-hover); }
  .op-btn-success { background: var(--op-success); color: #fff; border-color: var(--op-success); }
  .op-btn-danger { background: var(--op-danger); color: #fff; border-color: var(--op-danger); }
  .op-btn-warning { background: var(--op-warning); color: #000; border-color: var(--op-warning); }
  .op-body { max-height: 650px; overflow: auto; }
  .op-loading { padding: 30px; text-align: center; color: var(--op-text-muted); }
  .op-loading .spinner { display: inline-block; width: 20px; height: 20px; border: 2px solid var(--op-border); border-top-color: var(--op-primary); border-radius: 50%; animation: op-spin 0.6s linear infinite; margin-right: 8px; vertical-align: middle; }
  @keyframes op-spin { to { transform: rotate(360deg); } }
  .op-error { padding: 10px 14px; background: #fef2f2; color: var(--op-danger); border-bottom: 1px solid #fecaca; font-size: 12px; }
  :host(.dark-theme) .op-error { background: #3b1f1f; border-color: #5c2b2b; }
  .op-success-msg { padding: 10px 14px; background: #f0fdf4; color: var(--op-success); border-bottom: 1px solid #bbf7d0; font-size: 12px; }
  :host(.dark-theme) .op-success-msg { background: #1a2e1a; border-color: #2e5c2b; }
  .op-detail { padding: 14px; }
  .op-kv { display: grid; grid-template-columns: 180px 1fr; gap: 4px 12px; margin-bottom: 16px; font-size: 12px; }
  .op-kv dt { color: var(--op-text-muted); font-weight: 500; padding: 3px 0; }
  .op-kv dd { margin: 0; padding: 3px 0; word-break: break-all; }
  .op-section { font-size: 13px; font-weight: 600; margin: 16px 0 8px; padding-bottom: 4px; border-bottom: 1px solid var(--op-border); }
  table.op-table { width: 100%; border-collapse: collapse; }
  .op-table th { text-align: left; padding: 8px 14px; font-size: 11px; font-weight: 600; text-transform: uppercase; color: var(--op-text-muted); background: var(--op-bg-alt); border-bottom: 1px solid var(--op-border); }
  .op-table td { padding: 7px 14px; border-bottom: 1px solid var(--op-border); vertical-align: top; }
  .op-badge { display: inline-block; padding: 2px 7px; border-radius: 10px; font-size: 11px; font-weight: 500; }
  .op-badge-success { background: #d1fae5; color: #065f46; }
  .op-badge-danger { background: #fee2e2; color: #991b1b; }
  .op-badge-info { background: #dbeafe; color: #1e40af; }
  .op-badge-warning { background: #fef3c7; color: #92400e; }
  .op-badge-muted { background: #f3f4f6; color: #6b7280; }
  :host(.dark-theme) .op-badge-success { background: #064e3b; color: #6ee7b7; }
    :host(.dark-theme) .op-badge-danger { background: #7f1d1d; color: #fca5a5; }
    :host(.dark-theme) .op-badge-info { background: #1e3a5f; color: #93c5fd; }
    :host(.dark-theme) .op-badge-warning { background: #78350f; color: #fcd34d; }
    :host(.dark-theme) .op-badge-muted { background: #374151; color: #9ca3af; }
  .op-tabs { display: flex; gap: 0; border-bottom: 2px solid var(--op-border); margin-bottom: 12px; }
  .op-tab { padding: 6px 16px; cursor: pointer; font-size: 12px; font-weight: 500; border: none; background: none; color: var(--op-text-muted); border-bottom: 2px solid transparent; margin-bottom: -2px; font-family: var(--op-font); }
  .op-tab:hover { color: var(--op-text); }
  .op-tab.active { color: var(--op-primary); border-bottom-color: var(--op-primary); }
  .op-mono { font-family: var(--op-font-mono); font-size: 12px; }
  .op-muted { color: var(--op-text-muted); font-size: 12px; }
  .op-tag { display: inline-block; padding: 1px 6px; margin: 1px 3px 1px 0; border-radius: 3px; font-size: 11px; background: var(--op-bg-alt); border: 1px solid var(--op-border); font-family: var(--op-font-mono); }
  .op-auto-refresh { display: flex; align-items: center; gap: 4px; font-size: 11px; color: var(--op-text-muted); }
  .op-auto-refresh .dot { width: 6px; height: 6px; border-radius: 50%; background: var(--op-success); animation: op-pulse 2s ease-in-out infinite; }
  @keyframes op-pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.3; } }
  .op-status-bar { padding: 6px 14px; font-size: 11px; color: var(--op-text-muted); background: var(--op-bg-alt); border-top: 1px solid var(--op-border); display: flex; justify-content: space-between; }
  .op-confirm { padding: 14px; background: var(--op-bg-alt); border-bottom: 1px solid var(--op-border); }
  .op-confirm p { margin: 0 0 10px; font-size: 13px; }
  .op-confirm-actions { display: flex; gap: 8px; }
  .op-empty { padding: 30px; text-align: center; color: var(--op-text-muted); }
  /* State indicator */
  .op-state-indicator { display: inline-flex; align-items: center; gap: 8px; padding: 10px 16px; border-radius: 8px; margin-bottom: 16px; font-size: 14px; font-weight: 600; }
  .op-state-running { background: #d1fae5; color: #065f46; }
  .op-state-terminated { background: #f3f4f6; color: #6b7280; }
  .op-state-pending { background: #dbeafe; color: #1e40af; }
  .op-state-error { background: #fee2e2; color: #991b1b; }
  :host(.dark-theme) .op-state-running { background: #064e3b; color: #6ee7b7; }
    :host(.dark-theme) .op-state-terminated { background: #374151; color: #9ca3af; }
    :host(.dark-theme) .op-state-pending { background: #1e3a5f; color: #93c5fd; }
    :host(.dark-theme) .op-state-error { background: #7f1d1d; color: #fca5a5; }
  /* ---- Edit form ---- */
  .op-form-group { margin-bottom: 14px; }
  .op-form-label { display: block; font-size: 12px; font-weight: 500; color: var(--op-text-muted); margin-bottom: 4px; }
  .op-form-hint { font-size: 11px; color: var(--op-text-muted); margin-top: 2px; }
  .op-input, .op-select {
    width: 100%; padding: 5px 8px; border: 1px solid var(--op-border); border-radius: 4px;
    font-size: 12px; font-family: var(--op-font); background: var(--op-bg); color: var(--op-text);
  }
  .op-input:focus, .op-select:focus { outline: none; border-color: var(--op-primary); }
  .op-select { appearance: auto; }
  .op-input-short { width: 120px; }
  .op-input-mono { font-family: var(--op-font-mono); }
  .op-form-row { display: flex; gap: 12px; align-items: flex-end; flex-wrap: wrap; }
  .op-form-row .op-form-group { flex: 1; min-width: 150px; }
  .op-checkbox-label { display: flex; align-items: center; gap: 6px; font-size: 12px; cursor: pointer; }
  .op-checkbox-label input { margin: 0; }
  /* Key-value editor */
  .op-kv-editor { border: 1px solid var(--op-border); border-radius: 4px; overflow: hidden; }
  .op-kv-editor-row { display: flex; border-bottom: 1px solid var(--op-border); }
  .op-kv-editor-row:last-child { border-bottom: none; }
  .op-kv-editor-row input { flex: 1; border: none; padding: 5px 8px; font-size: 12px; font-family: var(--op-font-mono); background: var(--op-bg); color: var(--op-text); min-width: 0; }
  .op-kv-editor-row input:focus { outline: none; background: var(--op-bg-hover); }
  .op-kv-editor-row .kv-sep { padding: 5px 4px; color: var(--op-text-muted); font-size: 12px; user-select: none; }
  .op-kv-editor-row .kv-del { border: none; background: none; color: var(--op-danger); cursor: pointer; padding: 5px 8px; font-size: 14px; }
  .op-kv-editor-row .kv-del:hover { background: var(--op-bg-hover); }
  .op-kv-add { display: block; width: 100%; border: none; background: var(--op-bg-alt); color: var(--op-primary); padding: 5px; font-size: 12px; cursor: pointer; text-align: center; font-family: var(--op-font); border-top: 1px solid var(--op-border); }
  .op-kv-add:hover { background: var(--op-bg-hover); }
  /* Init script editor */
  .op-init-row { display: flex; gap: 6px; align-items: center; margin-bottom: 6px; }
  .op-init-row select { width: 110px; }
  .op-init-row input { flex: 1; }
  .op-init-row .kv-del { border: none; background: none; color: var(--op-danger); cursor: pointer; font-size: 14px; padding: 4px; }
  .op-form-actions { display: flex; gap: 8px; margin-top: 16px; padding-top: 12px; border-top: 1px solid var(--op-border); }
`;

// ---- Helpers ----
function esc(s) { if (s == null) return ""; const d = document.createElement("div"); d.textContent = String(s); return d.innerHTML; }
function stateClass(s) { if (!s) return "terminated"; const u = s.toUpperCase(); if (u === "RUNNING") return "running"; if (["PENDING","RESTARTING","RESIZING"].includes(u)) return "pending"; if (u === "ERROR") return "error"; return "terminated"; }

const SECURITY_MODES = [
  { value: "", label: "— default —" },
  { value: "SINGLE_USER", label: "Single User" },
  { value: "USER_ISOLATION", label: "User Isolation (Shared)" },
  { value: "DATA_SECURITY_MODE_AUTO", label: "Auto" },
  { value: "DATA_SECURITY_MODE_STANDARD", label: "Standard" },
  { value: "DATA_SECURITY_MODE_DEDICATED", label: "Dedicated" },
  { value: "NONE", label: "None (No isolation)" },
];

const RUNTIME_ENGINES = [
  { value: "", label: "— default —" },
  { value: "PHOTON", label: "Photon" },
  { value: "STANDARD", label: "Standard" },
];

const INIT_SCRIPT_TYPES = ["volumes", "workspace", "dbfs", "file"];

// ---- Main render ----

function _syncTheme(hostEl) {
  function isDark() {
    const attr = document.documentElement.getAttribute("data-app-theme");
    if (attr === "dark") return true;
    if (attr === "light") return false;
    return window.matchMedia("(prefers-color-scheme: dark)").matches;
  }
  function apply() { hostEl.classList.toggle("dark-theme", isDark()); }
  apply();
  const obs = new MutationObserver(apply);
  obs.observe(document.documentElement, { attributes: true, attributeFilter: ["data-app-theme"] });
  window.matchMedia("(prefers-color-scheme: dark)").addEventListener("change", apply);
  return () => { obs.disconnect(); };
}

function render({ model, el }) {
  const shadow = el.attachShadow ? el.attachShadow({ mode: "open" }) : el;
  _syncTheme(el);
  shadow.innerHTML = "";
  const styleEl = document.createElement("style"); styleEl.textContent = OPS_STYLES; shadow.appendChild(styleEl);
  const root = document.createElement("div"); shadow.appendChild(root);

  let currentTab = "status";
  let eventsLoaded = false;
  let editOptionsLoaded = false;
  let autoRefreshEnabled = true;
  let autoTimer = null;
  let confirmAction = null;
  let actionMessage = null;
  let actionIsError = false;

  // Edit form state — initialised from cluster data when user opens the Edit tab
  let editState = null;

  function getCluster() { return JSON.parse(model.get("cluster_data") || "{}"); }
  function getEvents() { return JSON.parse(model.get("events_data") || "[]"); }
  function getNodeTypes() { return JSON.parse(model.get("node_types_data") || "[]"); }
  function getSparkVersions() { return JSON.parse(model.get("spark_versions_data") || "[]"); }
  function sendRequest(req) { model.set("request", JSON.stringify({ ...req, _t: Date.now() })); model.save_changes(); }

  function startAutoRefresh() { stopAutoRefresh(); const c = getCluster(); if (autoRefreshEnabled) autoTimer = setInterval(() => sendRequest({ action: "refresh" }), (c.refresh_seconds||30)*1000); }
  function stopAutoRefresh() { if (autoTimer) { clearInterval(autoTimer); autoTimer = null; } }

  function initEditState() {
    const c = getCluster();
    editState = {
      cluster_name: c.cluster_name || "",
      spark_version: c.spark_version || "",
      node_type_id: c.node_type_id || "",
      driver_node_type_id: c.driver_node_type_id || "",
      // Workers: either num_workers or autoscale
      use_autoscale: c.autoscale != null,
      num_workers: c.num_workers != null ? c.num_workers : 0,
      autoscale_min: c.autoscale_min != null ? c.autoscale_min : 1,
      autoscale_max: c.autoscale_max != null ? c.autoscale_max : 8,
      autotermination_minutes: c.autotermination_minutes != null ? c.autotermination_minutes : 120,
      data_security_mode: c.data_security_mode || "",
      single_user_name: c.single_user_name || "",
      runtime_engine: c.runtime_engine || "",
      is_single_node: c.is_single_node || false,
      enable_elastic_disk: c.enable_elastic_disk || false,
      enable_local_disk_encryption: c.enable_local_disk_encryption || false,
      policy_id: c.policy_id || "",
      instance_pool_id: c.instance_pool_id || "",
      driver_instance_pool_id: "",
      spark_conf: Object.entries(c.spark_conf || {}).map(([k,v]) => ({key:k,value:v})),
      spark_env_vars: Object.entries(c.spark_env_vars || {}).map(([k,v]) => ({key:k,value:v})),
      custom_tags: Object.entries(c.custom_tags || {}).map(([k,v]) => ({key:k,value:v})),
      init_scripts: (c.init_scripts || []).map(s => ({type: s.type||"volumes", destination: s.destination||""})),
    };
  }

  function collectEditConfig() {
    // Read current form values from editState, return the config dict
    const s = editState;
    const config = {
      cluster_name: s.cluster_name,
      spark_version: s.spark_version,
      node_type_id: s.node_type_id || null,
      driver_node_type_id: s.driver_node_type_id || null,
      autotermination_minutes: parseInt(s.autotermination_minutes) || 0,
      data_security_mode: s.data_security_mode || null,
      single_user_name: s.single_user_name || null,
      runtime_engine: s.runtime_engine || null,
      is_single_node: s.is_single_node,
      enable_elastic_disk: s.enable_elastic_disk,
      enable_local_disk_encryption: s.enable_local_disk_encryption,
      policy_id: s.policy_id || null,
      instance_pool_id: s.instance_pool_id || null,
      driver_instance_pool_id: s.driver_instance_pool_id || null,
      spark_conf: Object.fromEntries(s.spark_conf.filter(r=>r.key.trim()).map(r=>[r.key.trim(),r.value])),
      spark_env_vars: Object.fromEntries(s.spark_env_vars.filter(r=>r.key.trim()).map(r=>[r.key.trim(),r.value])),
      custom_tags: Object.fromEntries(s.custom_tags.filter(r=>r.key.trim()).map(r=>[r.key.trim(),r.value])),
      init_scripts: s.init_scripts.filter(r=>r.destination.trim()).map(r=>({type:r.type,destination:r.destination.trim()})),
    };
    if (s.use_autoscale) {
      config.autoscale_min = parseInt(s.autoscale_min) || 1;
      config.autoscale_max = parseInt(s.autoscale_max) || 8;
    } else {
      config.num_workers = parseInt(s.num_workers) || 0;
    }
    return config;
  }

  // ---- Renderers ----

  function fullRender() {
    const c = getCluster();
    const loading = model.get("loading");
    const error = model.get("error_message");
    const isRunning = c.state === "RUNNING";
    const isTerminated = c.state === "TERMINATED";
    const isPending = ["PENDING","RESTARTING","RESIZING","TERMINATING"].includes(c.state);

    let html = `<div class="op-header"><h2>🖥️ ${esc(c.cluster_name || "Cluster")}</h2>`;
    html += `<div class="op-header-actions">`;
    html += `<div class="op-auto-refresh">${autoRefreshEnabled?'<span class="dot"></span> Auto':'Paused'}</div>`;
    html += `<button class="op-btn" data-action="toggle-refresh">${autoRefreshEnabled?'⏸':'▶'}</button>`;
    html += `<button class="op-btn" data-action="refresh">↻</button>`;
    if (isTerminated) html += `<button class="op-btn op-btn-success" data-action="start">▶ Start</button>`;
    if (isRunning) html += `<button class="op-btn op-btn-warning" data-action="restart">🔄 Restart</button>`;
    if (isRunning||isPending) html += `<button class="op-btn op-btn-danger" data-action="stop">⏹ Stop</button>`;
    html += `</div></div>`;

    if (confirmAction) html += `<div class="op-confirm"><p>${confirmAction.message}</p><div class="op-confirm-actions"><button class="op-btn op-btn-${confirmAction.btnClass}" data-action="confirm-yes">${confirmAction.yesLabel}</button><button class="op-btn" data-action="confirm-no">Cancel</button></div></div>`;
    if (actionMessage) html += `<div class="${actionIsError?'op-error':'op-success-msg'}">${esc(actionMessage)}</div>`;
    if (error) html += `<div class="op-error">${esc(error)}</div>`;

    if (loading && currentTab !== "edit") {
      html += `<div class="op-body"><div class="op-loading"><span class="spinner"></span> Loading…</div></div>`;
    } else {
      html += `<div class="op-body"><div class="op-detail">`;

      // State indicator
      html += `<div class="op-state-indicator op-state-${stateClass(c.state)}">${c.state==='RUNNING'?'🟢':c.state==='TERMINATED'?'⭕':'🔵'} ${esc(c.state||'UNKNOWN')}</div>`;
      if (c.state_message) html += `<div class="op-muted" style="margin-bottom:12px">${esc(c.state_message)}</div>`;

      html += `<div class="op-tabs">`;
      for (const [id,label] of [["status","Status"],["config","Config"],["edit","✏️ Edit"],["events","Events"]]) {
        html += `<button class="op-tab${currentTab===id?' active':''}" data-tab="${id}">${label}</button>`;
      }
      html += `</div>`;

      // Status tab
      html += `<div class="op-tab-content" data-tab="status" style="${currentTab!=='status'?'display:none':''}">`;
      html += `<dl class="op-kv">`;
      html += `<dt>Cluster ID</dt><dd class="op-mono">${esc(c.cluster_id)}</dd>`;
      html += `<dt>Workers</dt><dd>${c.autoscale ? 'Auto: '+esc(c.autoscale) : (c.num_workers!=null?c.num_workers:'—')}</dd>`;
      if (c.cluster_cores) html += `<dt>Cores</dt><dd>${c.cluster_cores}</dd>`;
      if (c.cluster_memory_mb) html += `<dt>Memory</dt><dd>${Math.round(c.cluster_memory_mb/1024)} GB</dd>`;
      html += `<dt>Start Time</dt><dd>${esc(c.start_time||'—')}</dd>`;
      if (c.terminated_time) html += `<dt>Terminated</dt><dd>${esc(c.terminated_time)}</dd>`;
      if (c.last_restarted_time) html += `<dt>Last Restart</dt><dd>${esc(c.last_restarted_time)}</dd>`;
      if (c.termination_reason) html += `<dt>Term. Reason</dt><dd>${esc(c.termination_reason)}</dd>`;
      html += `<dt>Auto-terminate</dt><dd>${c.autotermination_minutes ? c.autotermination_minutes+' min' : 'Disabled'}</dd>`;
      html += `</dl></div>`;

      // Config tab (read-only)
      html += `<div class="op-tab-content" data-tab="config" style="${currentTab!=='config'?'display:none':''}">`;
      html += `<dl class="op-kv">`;
      html += `<dt>Spark Version</dt><dd class="op-mono">${esc(c.spark_version)}</dd>`;
      html += `<dt>Node Type</dt><dd class="op-mono">${esc(c.node_type_id)}</dd>`;
      if (c.driver_node_type_id) html += `<dt>Driver Type</dt><dd class="op-mono">${esc(c.driver_node_type_id)}</dd>`;
      html += `<dt>Security Mode</dt><dd>${esc(c.data_security_mode||'—')}</dd>`;
      if (c.single_user_name) html += `<dt>Single User</dt><dd>${esc(c.single_user_name)}</dd>`;
      html += `<dt>Runtime Engine</dt><dd>${esc(c.runtime_engine||'STANDARD')}</dd>`;
      html += `<dt>Creator</dt><dd>${esc(c.creator||'—')}</dd>`;
      html += `<dt>Source</dt><dd>${esc(c.cluster_source||'—')}</dd>`;
      if (c.policy_id) html += `<dt>Policy ID</dt><dd class="op-mono">${esc(c.policy_id)}</dd>`;
      if (c.instance_pool_id) html += `<dt>Pool ID</dt><dd class="op-mono">${esc(c.instance_pool_id)}</dd>`;
      html += `</dl>`;
      if (c.spark_conf && Object.keys(c.spark_conf).length) {
        html += `<div class="op-section">Spark Config</div>`;
        html += Object.entries(c.spark_conf).map(([k,v])=>`<div><span class="op-mono op-muted">${esc(k)}</span> = <span class="op-mono">${esc(v)}</span></div>`).join('');
      }
      if (c.spark_env_vars && Object.keys(c.spark_env_vars).length) {
        html += `<div class="op-section">Environment Variables</div>`;
        html += Object.entries(c.spark_env_vars).map(([k,v])=>`<div><span class="op-mono op-muted">${esc(k)}</span> = <span class="op-mono">${esc(v)}</span></div>`).join('');
      }
      if (c.init_scripts && c.init_scripts.length) {
        html += `<div class="op-section">Init Scripts</div>`;
        for (const s of c.init_scripts) html += `<div><span class="op-badge op-badge-muted">${esc(s.type)}</span> <span class="op-mono">${esc(s.destination)}</span></div>`;
      }
      if (c.custom_tags && Object.keys(c.custom_tags).length) {
        html += `<div class="op-section">Tags</div>`;
        html += Object.entries(c.custom_tags).map(([k,v])=>`<span class="op-tag">${esc(k)}=${esc(v)}</span>`).join(' ');
      }
      html += `</div>`;

      // Edit tab
      html += `<div class="op-tab-content" data-tab="edit" style="${currentTab!=='edit'?'display:none':''}">`;
      if (editState) {
        html += renderEditForm();
      } else {
        html += `<div class="op-empty"><button class="op-btn op-btn-primary" data-action="open-edit">✏️ Edit Cluster Configuration</button></div>`;
      }
      html += `</div>`;

      // Events tab
      html += `<div class="op-tab-content" data-tab="events" style="${currentTab!=='events'?'display:none':''}">`;
      const events = getEvents();
      if (events.length) {
        html += `<table class="op-table"><thead><tr><th>Time</th><th>Type</th><th>Details</th></tr></thead><tbody>`;
        for (const e of events) html += `<tr><td class="op-muted">${esc(e.timestamp)}</td><td class="op-mono">${esc(e.type)}</td><td class="op-muted" style="font-size:11px;max-width:400px;word-break:break-all">${esc(e.details)}</td></tr>`;
        html += `</tbody></table>`;
      } else if (!eventsLoaded) {
        html += `<div class="op-empty"><button class="op-btn" data-action="load-events">Load Events</button></div>`;
      } else { html += `<div class="op-empty">No events.</div>`; }
      html += `</div>`;

      html += `</div></div>`;
    }

    html += `<div class="op-status-bar"><span>Last refresh: ${new Date().toLocaleTimeString()}</span><span>${esc(c.cluster_id||'')}</span></div>`;
    root.innerHTML = html;
    bindEvents();
  }

  function renderEditForm() {
    const s = editState;
    const nodeTypes = getNodeTypes();
    const sparkVersions = getSparkVersions();
    let html = '';

    // ---- Cluster Name ----
    html += `<div class="op-form-group"><label class="op-form-label">Cluster Name</label>`;
    html += `<input class="op-input" data-edit="cluster_name" value="${esc(s.cluster_name)}"></div>`;

    // ---- Runtime ----
    html += `<div class="op-section">Runtime</div>`;
    html += `<div class="op-form-row">`;
    // Spark version
    html += `<div class="op-form-group"><label class="op-form-label">Databricks Runtime</label>`;
    if (sparkVersions.length) {
      html += `<select class="op-select" data-edit="spark_version">`;
      for (const sv of sparkVersions) {
        const sel = sv.key === s.spark_version ? ' selected' : '';
        html += `<option value="${esc(sv.key)}"${sel}>${esc(sv.name)}</option>`;
      }
      html += `</select>`;
    } else {
      html += `<input class="op-input op-input-mono" data-edit="spark_version" value="${esc(s.spark_version)}">`;
    }
    html += `</div>`;
    // Runtime engine
    html += `<div class="op-form-group"><label class="op-form-label">Runtime Engine</label>`;
    html += `<select class="op-select" data-edit="runtime_engine">`;
    for (const re of RUNTIME_ENGINES) {
      const sel = re.value === s.runtime_engine ? ' selected' : '';
      html += `<option value="${esc(re.value)}"${sel}>${esc(re.label)}</option>`;
    }
    html += `</select></div></div>`;

    // ---- Node Types ----
    html += `<div class="op-section">Compute</div>`;
    html += `<div class="op-form-row">`;
    // Worker node type
    html += `<div class="op-form-group"><label class="op-form-label">Worker Node Type</label>`;
    if (nodeTypes.length) {
      html += `<select class="op-select" data-edit="node_type_id">`;
      html += `<option value="">— select —</option>`;
      for (const nt of nodeTypes) {
        const sel = nt.node_type_id === s.node_type_id ? ' selected' : '';
        const dep = nt.is_deprecated ? ' (deprecated)' : '';
        const gpu = nt.num_gpus ? ` ${nt.num_gpus}GPU` : '';
        const label = `${nt.node_type_id} — ${nt.num_cores}c / ${Math.round((nt.memory_mb||0)/1024)}GB${gpu}${dep}`;
        html += `<option value="${esc(nt.node_type_id)}"${sel}>${esc(label)}</option>`;
      }
      html += `</select>`;
    } else {
      html += `<input class="op-input op-input-mono" data-edit="node_type_id" value="${esc(s.node_type_id)}">`;
    }
    html += `</div>`;
    // Driver node type
    html += `<div class="op-form-group"><label class="op-form-label">Driver Node Type</label>`;
    if (nodeTypes.length) {
      html += `<select class="op-select" data-edit="driver_node_type_id">`;
      html += `<option value="">Same as worker</option>`;
      for (const nt of nodeTypes) {
        const sel = nt.node_type_id === s.driver_node_type_id ? ' selected' : '';
        const dep = nt.is_deprecated ? ' (deprecated)' : '';
        const label = `${nt.node_type_id} — ${nt.num_cores}c / ${Math.round((nt.memory_mb||0)/1024)}GB${dep}`;
        html += `<option value="${esc(nt.node_type_id)}"${sel}>${esc(label)}</option>`;
      }
      html += `</select>`;
    } else {
      html += `<input class="op-input op-input-mono" data-edit="driver_node_type_id" value="${esc(s.driver_node_type_id)}" placeholder="Same as worker">`;
    }
    html += `</div></div>`;

    // Workers / Autoscale
    html += `<div class="op-form-group" style="margin-top:8px"><label class="op-checkbox-label"><input type="checkbox" data-edit-check="use_autoscale" ${s.use_autoscale?'checked':''}> Enable autoscaling</label></div>`;
    if (s.use_autoscale) {
      html += `<div class="op-form-row">`;
      html += `<div class="op-form-group"><label class="op-form-label">Min Workers</label><input class="op-input op-input-short" type="number" min="0" data-edit="autoscale_min" value="${s.autoscale_min}"></div>`;
      html += `<div class="op-form-group"><label class="op-form-label">Max Workers</label><input class="op-input op-input-short" type="number" min="1" data-edit="autoscale_max" value="${s.autoscale_max}"></div>`;
      html += `</div>`;
    } else {
      html += `<div class="op-form-group"><label class="op-form-label">Number of Workers</label><input class="op-input op-input-short" type="number" min="0" data-edit="num_workers" value="${s.num_workers}"></div>`;
    }
    html += `<div class="op-form-group" style="margin-top:8px"><label class="op-checkbox-label"><input type="checkbox" data-edit-check="is_single_node" ${s.is_single_node?'checked':''}> Single-node cluster</label></div>`;

    // ---- Auto-termination ----
    html += `<div class="op-form-group"><label class="op-form-label">Auto-termination (minutes, 0 = disabled)</label><input class="op-input op-input-short" type="number" min="0" data-edit="autotermination_minutes" value="${s.autotermination_minutes}"></div>`;

    // ---- Security ----
    html += `<div class="op-section">Security</div>`;
    html += `<div class="op-form-row">`;
    html += `<div class="op-form-group"><label class="op-form-label">Data Security Mode</label>`;
    html += `<select class="op-select" data-edit="data_security_mode">`;
    for (const sm of SECURITY_MODES) {
      const sel = sm.value === s.data_security_mode ? ' selected' : '';
      html += `<option value="${esc(sm.value)}"${sel}>${esc(sm.label)}</option>`;
    }
    html += `</select></div>`;
    html += `<div class="op-form-group"><label class="op-form-label">Single User Name</label><input class="op-input" data-edit="single_user_name" value="${esc(s.single_user_name)}" placeholder="user@example.com"></div>`;
    html += `</div>`;
    html += `<div class="op-form-group"><label class="op-checkbox-label"><input type="checkbox" data-edit-check="enable_elastic_disk" ${s.enable_elastic_disk?'checked':''}> Enable elastic disk</label></div>`;
    html += `<div class="op-form-group"><label class="op-checkbox-label"><input type="checkbox" data-edit-check="enable_local_disk_encryption" ${s.enable_local_disk_encryption?'checked':''}> Enable local disk encryption</label></div>`;

    // ---- Spark Config ----
    html += `<div class="op-section">Spark Config</div>`;
    html += renderKVEditor("spark_conf", s.spark_conf, "spark.key", "value");

    // ---- Environment Variables ----
    html += `<div class="op-section">Environment Variables</div>`;
    html += renderKVEditor("spark_env_vars", s.spark_env_vars, "VAR_NAME", "value");

    // ---- Init Scripts ----
    html += `<div class="op-section">Init Scripts</div>`;
    for (let i = 0; i < s.init_scripts.length; i++) {
      const sc = s.init_scripts[i];
      html += `<div class="op-init-row">`;
      html += `<select class="op-select" data-init-type="${i}">`;
      for (const t of INIT_SCRIPT_TYPES) html += `<option value="${t}"${sc.type===t?' selected':''}>${t}</option>`;
      html += `</select>`;
      html += `<input class="op-input op-input-mono" data-init-dest="${i}" value="${esc(sc.destination)}" placeholder="/Volumes/catalog/schema/vol/init.sh">`;
      html += `<button class="kv-del" data-init-del="${i}">✕</button>`;
      html += `</div>`;
    }
    html += `<button class="op-btn" data-action="add-init" style="margin-top:4px">+ Add Init Script</button>`;

    // ---- Tags ----
    html += `<div class="op-section">Tags</div>`;
    html += renderKVEditor("custom_tags", s.custom_tags, "key", "value");

    // ---- Pools / Policy (advanced) ----
    html += `<div class="op-section">Advanced</div>`;
    html += `<div class="op-form-row">`;
    html += `<div class="op-form-group"><label class="op-form-label">Policy ID</label><input class="op-input op-input-mono" data-edit="policy_id" value="${esc(s.policy_id)}" placeholder="Optional"></div>`;
    html += `<div class="op-form-group"><label class="op-form-label">Instance Pool ID</label><input class="op-input op-input-mono" data-edit="instance_pool_id" value="${esc(s.instance_pool_id)}" placeholder="Optional"></div>`;
    html += `</div>`;

    // ---- Actions ----
    html += `<div class="op-form-actions">`;
    html += `<button class="op-btn op-btn-primary" data-action="save-edit">💾 Save Configuration</button>`;
    html += `<button class="op-btn" data-action="cancel-edit">Cancel</button>`;
    html += `<button class="op-btn" data-action="reset-edit" style="margin-left:auto">↻ Reset to Current</button>`;
    html += `</div>`;

    return html;
  }

  function renderKVEditor(name, entries, keyPlaceholder, valuePlaceholder) {
    let html = `<div class="op-kv-editor" data-kv-name="${name}">`;
    for (let i = 0; i < entries.length; i++) {
      html += `<div class="op-kv-editor-row">`;
      html += `<input data-kv-key="${name}" data-kv-idx="${i}" value="${esc(entries[i].key)}" placeholder="${esc(keyPlaceholder)}">`;
      html += `<span class="kv-sep">=</span>`;
      html += `<input data-kv-val="${name}" data-kv-idx="${i}" value="${esc(entries[i].value)}" placeholder="${esc(valuePlaceholder)}">`;
      html += `<button class="kv-del" data-kv-del="${name}" data-kv-idx="${i}">✕</button>`;
      html += `</div>`;
    }
    html += `<button class="op-kv-add" data-kv-add="${name}">+ Add Row</button>`;
    html += `</div>`;
    return html;
  }

  // ---- Event binding ----

  function bindEvents() {
    // Tabs
    root.querySelectorAll(".op-tab").forEach(tab => tab.addEventListener("click", () => {
      currentTab = tab.dataset.tab;
      if (currentTab === "edit" && !editOptionsLoaded) {
        editOptionsLoaded = true;
        sendRequest({ action: "get_edit_options" });
      }
      fullRender();
    }));

    // Standard actions
    root.querySelector("[data-action='refresh']")?.addEventListener("click", () => sendRequest({ action: "refresh" }));
    root.querySelector("[data-action='toggle-refresh']")?.addEventListener("click", () => { autoRefreshEnabled = !autoRefreshEnabled; if (autoRefreshEnabled) startAutoRefresh(); else stopAutoRefresh(); fullRender(); });
    root.querySelector("[data-action='load-events']")?.addEventListener("click", () => { eventsLoaded = true; sendRequest({ action: "get_events" }); });

    for (const [act,msg,btn,label] of [["start","Start this cluster?","success","▶ Start"],["stop","Stop this cluster?","danger","⏹ Stop"],["restart","Restart this cluster?","warning","🔄 Restart"]]) {
      root.querySelector(`[data-action='${act}']`)?.addEventListener("click", () => { confirmAction = { message: msg, btnClass: btn, yesLabel: label, action: act }; fullRender(); });
    }
    root.querySelector("[data-action='confirm-yes']")?.addEventListener("click", () => { const a = confirmAction.action; confirmAction = null; sendRequest({ action: a }); });
    root.querySelector("[data-action='confirm-no']")?.addEventListener("click", () => { confirmAction = null; fullRender(); });

    // ---- Edit form bindings ----
    root.querySelector("[data-action='open-edit']")?.addEventListener("click", () => {
      initEditState();
      if (!editOptionsLoaded) { editOptionsLoaded = true; sendRequest({ action: "get_edit_options" }); }
      fullRender();
    });
    root.querySelector("[data-action='cancel-edit']")?.addEventListener("click", () => { editState = null; currentTab = "config"; fullRender(); });
    root.querySelector("[data-action='reset-edit']")?.addEventListener("click", () => { initEditState(); fullRender(); });
    root.querySelector("[data-action='save-edit']")?.addEventListener("click", () => {
      readFormIntoState();
      const config = collectEditConfig();
      confirmAction = { message: "Apply these configuration changes? The cluster may need to be restarted for changes to take effect.", btnClass: "primary", yesLabel: "💾 Save", action: "_apply_edit", _config: config };
      fullRender();
    });

    // Intercept the confirm for edit
    if (confirmAction && confirmAction.action === "_apply_edit") {
      root.querySelector("[data-action='confirm-yes']")?.addEventListener("click", () => {
        const config = confirmAction._config;
        confirmAction = null;
        sendRequest({ action: "edit", config });
        editState = null;
        currentTab = "config";
      });
    }

    // Simple edit inputs — read on change into editState
    root.querySelectorAll("[data-edit]").forEach(el => {
      el.addEventListener("input", () => { if (editState) editState[el.dataset.edit] = el.value; });
      el.addEventListener("change", () => { if (editState) editState[el.dataset.edit] = el.value; });
    });
    root.querySelectorAll("[data-edit-check]").forEach(el => {
      el.addEventListener("change", () => {
        if (editState) {
          editState[el.dataset.editCheck] = el.checked;
          if (el.dataset.editCheck === "use_autoscale") fullRender();
        }
      });
    });

    // KV editors
    root.querySelectorAll("[data-kv-key]").forEach(el => {
      el.addEventListener("input", () => { if (editState) editState[el.dataset.kvKey][parseInt(el.dataset.kvIdx)].key = el.value; });
    });
    root.querySelectorAll("[data-kv-val]").forEach(el => {
      el.addEventListener("input", () => { if (editState) editState[el.dataset.kvVal][parseInt(el.dataset.kvIdx)].value = el.value; });
    });
    root.querySelectorAll("[data-kv-del]").forEach(el => {
      el.addEventListener("click", () => { if (editState) { readFormIntoState(); editState[el.dataset.kvDel].splice(parseInt(el.dataset.kvIdx), 1); fullRender(); } });
    });
    root.querySelectorAll("[data-kv-add]").forEach(el => {
      el.addEventListener("click", () => { if (editState) { readFormIntoState(); editState[el.dataset.kvAdd].push({key:"",value:""}); fullRender(); } });
    });

    // Init script editor
    root.querySelectorAll("[data-init-type]").forEach(el => {
      el.addEventListener("change", () => { if (editState) editState.init_scripts[parseInt(el.dataset.initType)].type = el.value; });
    });
    root.querySelectorAll("[data-init-dest]").forEach(el => {
      el.addEventListener("input", () => { if (editState) editState.init_scripts[parseInt(el.dataset.initDest)].destination = el.value; });
    });
    root.querySelectorAll("[data-init-del]").forEach(el => {
      el.addEventListener("click", () => { if (editState) { readFormIntoState(); editState.init_scripts.splice(parseInt(el.dataset.initDel), 1); fullRender(); } });
    });
    root.querySelector("[data-action='add-init']")?.addEventListener("click", () => {
      if (editState) { readFormIntoState(); editState.init_scripts.push({type:"volumes",destination:""}); fullRender(); }
    });
  }

  function readFormIntoState() {
    // Read all current DOM input values into editState before a re-render
    if (!editState) return;
    root.querySelectorAll("[data-edit]").forEach(el => { editState[el.dataset.edit] = el.value; });
    root.querySelectorAll("[data-edit-check]").forEach(el => { editState[el.dataset.editCheck] = el.checked; });
    root.querySelectorAll("[data-kv-key]").forEach(el => { editState[el.dataset.kvKey][parseInt(el.dataset.kvIdx)].key = el.value; });
    root.querySelectorAll("[data-kv-val]").forEach(el => { editState[el.dataset.kvVal][parseInt(el.dataset.kvIdx)].value = el.value; });
    root.querySelectorAll("[data-init-type]").forEach(el => { editState.init_scripts[parseInt(el.dataset.initType)].type = el.value; });
    root.querySelectorAll("[data-init-dest]").forEach(el => { editState.init_scripts[parseInt(el.dataset.initDest)].destination = el.value; });
  }

  // ---- Model listeners ----
  model.on("change:cluster_data", () => { if (currentTab !== "edit") fullRender(); });
  model.on("change:events_data", fullRender);
  model.on("change:node_types_data", () => { if (currentTab === "edit") fullRender(); });
  model.on("change:spark_versions_data", () => { if (currentTab === "edit") fullRender(); });
  model.on("change:loading", () => { if (currentTab !== "edit") fullRender(); });
  model.on("change:error_message", fullRender);
  model.on("change:action_result", () => {
    try {
      const r = JSON.parse(model.get("action_result")||"{}");
      actionMessage = r.message; actionIsError = !r.success;
      fullRender();
      if (r.success) setTimeout(() => { actionMessage = null; fullRender(); }, 5000);
    } catch(e) {}
  });

  fullRender();
  startAutoRefresh();
  return () => stopAutoRefresh();
}

export default { render };
