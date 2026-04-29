// Job Widget Frontend — single-job operational dashboard
// Actions: run now, cancel, repair. Auto-refresh. Task DAG + run details.

// ---- Shared styles (inlined for anywidget self-containment) ----
const OPS_STYLES = `
  :host {
    --op-bg: #ffffff; --op-bg-alt: #f8f9fa; --op-bg-hover: #e9ecef;
    --op-border: #dee2e6; --op-text: #212529; --op-text-muted: #6c757d;
    --op-primary: #0d6efd; --op-primary-hover: #0b5ed7;
    --op-success: #198754; --op-danger: #dc3545; --op-warning: #ffc107; --op-info: #0dcaf0;
    --op-font: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
    --op-font-mono: "SF Mono", "Cascadia Code", "Fira Code", Menlo, Consolas, monospace;
    --op-radius: 6px;
    display: block; font-family: var(--op-font); font-size: 13px;
    color: var(--op-text); background: var(--op-bg);
    border: 1px solid var(--op-border); border-radius: var(--op-radius); overflow: hidden;
  }
  :host(.dark-theme) { --op-bg: #1e1e1e; --op-bg-alt: #252526; --op-bg-hover: #2d2d30; --op-border: #3e3e42; --op-text: #cccccc; --op-text-muted: #888888; --op-primary: #4fc3f7; --op-primary-hover: #29b6f6; --op-success: #66bb6a; --op-danger: #ef5350; --op-warning: #ffca28; --op-info: #4dd0e1; }
  * { box-sizing: border-box; }
  .op-header { display: flex; align-items: center; gap: 8px; padding: 10px 14px; background: var(--op-bg-alt); border-bottom: 1px solid var(--op-border); flex-wrap: wrap; }
  .op-header h2 { margin: 0; font-size: 14px; font-weight: 600; }
  .op-header .op-id { color: var(--op-text-muted); font-size: 11px; font-family: var(--op-font-mono); }
  .op-header-actions { margin-left: auto; display: flex; gap: 6px; align-items: center; }
  .op-btn { padding: 4px 10px; border: 1px solid var(--op-border); border-radius: 4px; background: var(--op-bg); color: var(--op-text); cursor: pointer; font-size: 12px; font-family: var(--op-font); white-space: nowrap; transition: background 0.15s; }
  .op-btn:hover { background: var(--op-bg-hover); }
  .op-btn:disabled { opacity: 0.5; cursor: not-allowed; }
  .op-btn-primary { background: var(--op-primary); color: #fff; border-color: var(--op-primary); }
  .op-btn-primary:hover { background: var(--op-primary-hover); }
  .op-btn-danger { background: var(--op-danger); color: #fff; border-color: var(--op-danger); }
  .op-btn-success { background: var(--op-success); color: #fff; border-color: var(--op-success); }
  .op-btn-warning { background: var(--op-warning); color: #000; border-color: var(--op-warning); }
  .op-body { max-height: 600px; overflow: auto; }
  .op-loading { padding: 30px; text-align: center; color: var(--op-text-muted); }
  .op-loading .spinner { display: inline-block; width: 20px; height: 20px; border: 2px solid var(--op-border); border-top-color: var(--op-primary); border-radius: 50%; animation: op-spin 0.6s linear infinite; margin-right: 8px; vertical-align: middle; }
  @keyframes op-spin { to { transform: rotate(360deg); } }
  .op-error { padding: 10px 14px; background: #fef2f2; color: var(--op-danger); border-bottom: 1px solid #fecaca; font-size: 12px; }
  :host(.dark-theme) .op-error { background: #3b1f1f; border-color: #5c2b2b; }
  .op-success-msg { padding: 10px 14px; background: #f0fdf4; color: var(--op-success); border-bottom: 1px solid #bbf7d0; font-size: 12px; }
  :host(.dark-theme) .op-success-msg { background: #1a2e1a; border-color: #2e5c2b; }
  .op-detail { padding: 14px; }
  .op-kv { display: grid; grid-template-columns: 150px 1fr; gap: 4px 12px; margin-bottom: 16px; font-size: 12px; }
  .op-kv dt { color: var(--op-text-muted); font-weight: 500; padding: 3px 0; }
  .op-kv dd { margin: 0; padding: 3px 0; word-break: break-all; }
  .op-section { font-size: 13px; font-weight: 600; margin: 16px 0 8px; padding-bottom: 4px; border-bottom: 1px solid var(--op-border); }
  table.op-table { width: 100%; border-collapse: collapse; }
  .op-table th { text-align: left; padding: 8px 14px; font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; color: var(--op-text-muted); background: var(--op-bg-alt); border-bottom: 1px solid var(--op-border); position: sticky; top: 0; z-index: 1; }
  .op-table td { padding: 7px 14px; border-bottom: 1px solid var(--op-border); vertical-align: top; }
  .op-table tr:hover td { background: var(--op-bg-hover); }
  .op-table tr.clickable { cursor: pointer; }
  .op-badge { display: inline-block; padding: 2px 7px; border-radius: 10px; font-size: 11px; font-weight: 500; white-space: nowrap; }
  .op-badge-success { background: #d1fae5; color: #065f46; }
  .op-badge-danger { background: #fee2e2; color: #991b1b; }
  .op-badge-warning { background: #fef3c7; color: #92400e; }
  .op-badge-info { background: #dbeafe; color: #1e40af; }
  .op-badge-muted { background: #f3f4f6; color: #6b7280; }
  :host(.dark-theme) .op-badge-success { background: #064e3b; color: #6ee7b7; }
    :host(.dark-theme) .op-badge-danger { background: #7f1d1d; color: #fca5a5; }
    :host(.dark-theme) .op-badge-warning { background: #78350f; color: #fcd34d; }
    :host(.dark-theme) .op-badge-info { background: #1e3a5f; color: #93c5fd; }
    :host(.dark-theme) .op-badge-muted { background: #374151; color: #9ca3af; }
  .op-tabs { display: flex; gap: 0; border-bottom: 2px solid var(--op-border); margin-bottom: 12px; }
  .op-tab { padding: 6px 16px; cursor: pointer; font-size: 12px; font-weight: 500; border: none; background: none; color: var(--op-text-muted); border-bottom: 2px solid transparent; margin-bottom: -2px; font-family: var(--op-font); }
  .op-tab:hover { color: var(--op-text); }
  .op-tab.active { color: var(--op-primary); border-bottom-color: var(--op-primary); }
  .op-link { color: var(--op-primary); cursor: pointer; text-decoration: none; }
  .op-link:hover { text-decoration: underline; }
  .op-mono { font-family: var(--op-font-mono); font-size: 12px; }
  .op-muted { color: var(--op-text-muted); font-size: 12px; }
  .op-tag { display: inline-block; padding: 1px 6px; margin: 1px 3px 1px 0; border-radius: 3px; font-size: 11px; background: var(--op-bg-alt); border: 1px solid var(--op-border); font-family: var(--op-font-mono); }
  .op-auto-refresh { display: flex; align-items: center; gap: 4px; font-size: 11px; color: var(--op-text-muted); }
  .op-auto-refresh .dot { width: 6px; height: 6px; border-radius: 50%; background: var(--op-success); animation: op-pulse 2s ease-in-out infinite; }
  @keyframes op-pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.3; } }
  .op-pre { margin: 0; padding: 10px 14px; font-family: var(--op-font-mono); font-size: 12px; line-height: 1.5; white-space: pre-wrap; word-break: break-all; color: var(--op-text); background: var(--op-bg); }
  .op-pre.error-text { color: var(--op-danger); }
  .op-status-bar { padding: 6px 14px; font-size: 11px; color: var(--op-text-muted); background: var(--op-bg-alt); border-top: 1px solid var(--op-border); display: flex; justify-content: space-between; }
  .op-confirm { padding: 14px; background: var(--op-bg-alt); border-bottom: 1px solid var(--op-border); }
  .op-confirm p { margin: 0 0 10px; font-size: 13px; }
  .op-confirm-actions { display: flex; gap: 8px; }
  .op-empty { padding: 30px; text-align: center; color: var(--op-text-muted); }
  /* DAG */
  .op-dag-container { display: flex; gap: 0; min-height: 200px; }
  .op-dag-canvas { flex: 1; overflow: auto; min-height: 200px; position: relative; }
  .op-dag-canvas svg { display: block; }
  .op-dag-canvas .dag-node { cursor: pointer; }
  .op-dag-canvas .dag-node rect { fill: var(--op-bg); stroke: var(--op-border); stroke-width: 1.5; rx: 6; ry: 6; transition: stroke 0.15s; }
  .op-dag-canvas .dag-node:hover rect { stroke: var(--op-primary); stroke-width: 2; }
  .op-dag-canvas .dag-node.selected rect { stroke: var(--op-primary); stroke-width: 2.5; }
  .op-dag-canvas .dag-node .node-title { font-family: var(--op-font-mono); font-size: 11px; font-weight: 600; fill: var(--op-text); }
  .op-dag-canvas .dag-node .node-type { font-family: var(--op-font); font-size: 10px; fill: var(--op-text-muted); }
  .op-dag-canvas .dag-edge { fill: none; stroke: var(--op-border); stroke-width: 1.5; }
  .op-dag-canvas .dag-arrow { fill: var(--op-text-muted); }
  .op-dag-detail { width: 300px; min-width: 300px; border-left: 1px solid var(--op-border); padding: 14px; overflow-y: auto; background: var(--op-bg-alt); font-size: 12px; }
  .op-dag-detail h3 { margin: 0 0 10px; font-size: 13px; font-weight: 600; font-family: var(--op-font-mono); }
  .op-dag-detail .close-btn { float: right; background: none; border: none; font-size: 16px; cursor: pointer; color: var(--op-text-muted); }
  /* Output panel */
  .op-output-panel { margin-top: 16px; border: 1px solid var(--op-border); border-radius: var(--op-radius); overflow: hidden; }
  .op-output-header { display: flex; align-items: center; justify-content: space-between; padding: 8px 12px; background: var(--op-bg-alt); border-bottom: 1px solid var(--op-border); font-weight: 600; font-size: 12px; }
  .op-output-tabs { display: flex; gap: 0; border-bottom: 1px solid var(--op-border); background: var(--op-bg-alt); }
  .op-output-tab { padding: 5px 14px; cursor: pointer; font-size: 11px; font-weight: 500; border: none; background: none; color: var(--op-text-muted); border-bottom: 2px solid transparent; margin-bottom: -1px; font-family: var(--op-font); }
  .op-output-tab:hover { color: var(--op-text); }
  .op-output-tab.active { color: var(--op-primary); border-bottom-color: var(--op-primary); }
  .op-output-tab.has-error { color: var(--op-danger); }
  .op-output-body { max-height: 320px; overflow: auto; }
  .op-output-content { padding: 0; display: none; }
  .op-output-content.active { display: block; }
  .op-output-empty { padding: 16px; text-align: center; color: var(--op-text-muted); font-size: 12px; }
  .task-selected td { background: color-mix(in srgb, var(--op-primary) 10%, var(--op-bg)) !important; }
  .op-truncated { padding: 4px 14px 8px; font-size: 11px; color: var(--op-warning); font-style: italic; }
`;

function esc(s) { if (s == null) return ""; const d = document.createElement("div"); d.textContent = String(s); return d.innerHTML; }

function stateBadge(state) {
  if (!state) return "";
  const s = String(state).toUpperCase();
  let cls = "muted";
  if (s === "SUCCESS") cls = "success";
  else if (["FAILED", "INTERNAL_ERROR", "TIMEDOUT", "CANCELED"].includes(s)) cls = "danger";
  else if (["RUNNING", "PENDING", "QUEUED", "BLOCKED", "WAITING_FOR_RETRY", "TERMINATING"].includes(s)) cls = "info";
  else if (["EXCLUDED", "SKIPPED", "UPSTREAM_CANCELED", "UPSTREAM_FAILED"].includes(s)) cls = "warning";
  return `<span class="op-badge op-badge-${cls}">${s}</span>`;
}

function typeBadgeColor(type) {
  const m = { notebook:"#6f42c1", sql:"#0d6efd", pipeline:"#198754", dbt:"#fd7e14", spark_python:"#e83e8c", python_wheel:"#e83e8c", run_job:"#20c997", condition:"#6c757d", for_each:"#6c757d" };
  return m[type] || "#6c757d";
}

// ---- DAG ----
function layoutDAG(tasks) {
  const map = new Map();
  for (const t of tasks) map.set(t.task_key, { task: t, children: [], parents: [] });
  for (const t of tasks) for (const dep of t.depends_on || []) {
    const parent = map.get(dep);
    if (parent) { parent.children.push(t.task_key); map.get(t.task_key).parents.push(dep); }
  }
  const layer = new Map();
  function lp(key) {
    if (layer.has(key)) return layer.get(key);
    const n = map.get(key);
    if (!n || !n.parents.length) { layer.set(key, 0); return 0; }
    const d = 1 + Math.max(...n.parents.map(lp));
    layer.set(key, d); return d;
  }
  for (const k of map.keys()) lp(k);
  const layers = [];
  for (const [k, l] of layer) { while (layers.length <= l) layers.push([]); layers[l].push(k); }
  const NODE_W = 160, NODE_H = 48, PAD_X = 60, PAD_Y = 28, MARGIN = 20;
  const positions = new Map();
  const maxPL = Math.max(...layers.map(l => l.length), 1);
  for (let li = 0; li < layers.length; li++) {
    const col = layers[li];
    const totalH = col.length * NODE_H + (col.length - 1) * PAD_Y;
    const startY = MARGIN + (maxPL * NODE_H + (maxPL - 1) * PAD_Y - totalH) / 2;
    for (let ni = 0; ni < col.length; ni++) positions.set(col[ni], { x: MARGIN + li * (NODE_W + PAD_X), y: startY + ni * (NODE_H + PAD_Y) });
  }
  const svgW = MARGIN * 2 + layers.length * NODE_W + (layers.length - 1) * PAD_X;
  const svgH = MARGIN * 2 + maxPL * NODE_H + (maxPL - 1) * PAD_Y;
  const edges = [];
  for (const t of tasks) for (const dep of t.depends_on || []) if (positions.has(dep) && positions.has(t.task_key)) edges.push({ from: dep, to: t.task_key });
  return { positions, edges, svgW: Math.max(svgW, 200), svgH: Math.max(svgH, 100), NODE_W, NODE_H };
}

function renderDAGSvg(tasks, selectedKey) {
  if (!tasks || !tasks.length) return "";
  const { positions, edges, svgW, svgH, NODE_W, NODE_H } = layoutDAG(tasks);
  let svg = `<svg xmlns="http://www.w3.org/2000/svg" width="${svgW}" height="${svgH}">`;
  svg += `<defs><marker id="ah" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto"><polygon points="0 0,8 3,0 6" class="dag-arrow"/></marker></defs>`;
  for (const e of edges) {
    const f = positions.get(e.from), t = positions.get(e.to);
    svg += `<path class="dag-edge" d="M${f.x+NODE_W},${f.y+NODE_H/2} C${f.x+NODE_W+30},${f.y+NODE_H/2} ${t.x-30},${t.y+NODE_H/2} ${t.x},${t.y+NODE_H/2}" marker-end="url(#ah)"/>`;
  }
  for (const t of tasks) {
    const p = positions.get(t.task_key); if (!p) continue;
    const sel = t.task_key === selectedKey ? " selected" : "";
    const label = t.task_key.length > 18 ? t.task_key.slice(0, 17) + "…" : t.task_key;
    svg += `<g class="dag-node${sel}" data-task-key="${esc(t.task_key)}" opacity="${t.disabled?0.45:1}">`;
    svg += `<rect x="${p.x}" y="${p.y}" width="${NODE_W}" height="${NODE_H}"/>`;
    svg += `<text class="node-title" x="${p.x+10}" y="${p.y+19}">${esc(label)}</text>`;
    svg += `<text class="node-type" x="${p.x+10}" y="${p.y+38}" style="fill:${typeBadgeColor(t.type)}">${esc(t.type)}</text>`;
    svg += `</g>`;
  }
  svg += `</svg>`;
  return svg;
}

// ---- Main ----

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
  const styleEl = document.createElement("style"); styleEl.textContent = OPS_STYLES; shadow.appendChild(styleEl);
  const root = document.createElement("div"); shadow.appendChild(root);

  let currentTab = "runs";
  let selectedRunId = null;
  let selectedTaskRunId = null;
  let selectedTaskKey = null;
  let autoRefreshEnabled = true;
  let autoTimer = null;
  let confirmAction = null;
  let actionMessage = null;
  let actionIsError = false;

  function getJob() { return JSON.parse(model.get("job_data") || "{}"); }
  function getRuns() { return JSON.parse(model.get("runs_data") || "[]"); }
  function getRunDetail() { return JSON.parse(model.get("run_detail") || "{}"); }

  function startAutoRefresh() {
    stopAutoRefresh();
    const job = getJob();
    const interval = (job.refresh_seconds || 30) * 1000;
    if (autoRefreshEnabled) {
      autoTimer = setInterval(() => {
        model.set("request", JSON.stringify({ action: "refresh", _t: Date.now() }));
        model.save_changes();
      }, interval);
    }
  }
  function stopAutoRefresh() { if (autoTimer) { clearInterval(autoTimer); autoTimer = null; } }

  function sendRequest(req) {
    model.set("request", JSON.stringify({ ...req, _t: Date.now() }));
    model.save_changes();
  }

  function fullRender() {
    const job = getJob();
    const runs = getRuns();
    const loading = model.get("loading");
    const error = model.get("error_message");

    let html = `<div class="op-header">`;
    html += `<h2>⚡ ${esc(job.name || "Job")}</h2>`;
    if (job.job_id) html += `<span class="op-id">#${esc(job.job_id)}</span>`;
    html += `<div class="op-header-actions">`;
    html += `<div class="op-auto-refresh">${autoRefreshEnabled ? '<span class="dot"></span> Auto' : '<span style="opacity:0.3">◯</span> Paused'}</div>`;
    html += `<button class="op-btn" data-action="toggle-refresh">${autoRefreshEnabled ? '⏸' : '▶'}</button>`;
    html += `<button class="op-btn" data-action="refresh">↻</button>`;
    html += `<button class="op-btn op-btn-success" data-action="run-now">▶ Run Now</button>`;
    html += `</div></div>`;

    // Confirm bar
    if (confirmAction) {
      html += `<div class="op-confirm"><p>${confirmAction.message}</p><div class="op-confirm-actions">`;
      html += `<button class="op-btn op-btn-${confirmAction.btnClass || 'primary'}" data-action="confirm-yes">${confirmAction.yesLabel || 'Confirm'}</button>`;
      html += `<button class="op-btn" data-action="confirm-no">Cancel</button></div></div>`;
    }

    // Action result message
    if (actionMessage) {
      html += `<div class="${actionIsError ? 'op-error' : 'op-success-msg'}">${esc(actionMessage)}</div>`;
    }

    if (error) html += `<div class="op-error">${esc(error)}</div>`;

    if (loading) {
      html += `<div class="op-body"><div class="op-loading"><span class="spinner"></span> Loading…</div></div>`;
    } else if (selectedRunId) {
      html += renderRunView(getRunDetail());
    } else {
      html += `<div class="op-body"><div class="op-detail">`;
      html += `<div class="op-tabs">`;
      html += `<button class="op-tab${currentTab==='runs'?' active':''}" data-tab="runs">Runs (${runs.length})</button>`;
      html += `<button class="op-tab${currentTab==='tasks'?' active':''}" data-tab="tasks">Tasks (${(job.tasks||[]).length})</button>`;
      html += `<button class="op-tab${currentTab==='info'?' active':''}" data-tab="info">Info</button>`;
      html += `</div>`;

      // Runs tab
      html += `<div class="op-tab-content" data-tab="runs" style="${currentTab!=='runs'?'display:none':''}">`;
      if (runs.length) {
        // Show latest run status prominently
        const latest = runs[0];
        html += `<div style="margin-bottom:12px;padding:10px;border:1px solid var(--op-border);border-radius:var(--op-radius);background:var(--op-bg-alt)">`;
        html += `<div style="font-size:12px;color:var(--op-text-muted);margin-bottom:4px">Latest Run</div>`;
        html += `<div style="display:flex;gap:12px;align-items:center;flex-wrap:wrap">`;
        html += `${stateBadge(latest.life_cycle_state)} ${stateBadge(latest.result_state)}`;
        html += `<span class="op-mono">#${esc(latest.run_id)}</span>`;
        html += `<span class="op-muted">${esc(latest.duration || '—')}</span>`;
        html += `<span class="op-muted">${esc(latest.start_time || '—')}</span>`;
        if (latest.life_cycle_state === 'RUNNING' || latest.life_cycle_state === 'PENDING') {
          html += `<button class="op-btn op-btn-danger op-btn-sm" data-action="cancel" data-run-id="${latest.run_id}">✕ Cancel</button>`;
        }
        if (latest.result_state === 'FAILED') {
          html += `<button class="op-btn op-btn-warning op-btn-sm" data-action="repair" data-run-id="${latest.run_id}">🔧 Repair</button>`;
        }
        if (latest.run_page_url) html += `<a class="op-link" href="${esc(latest.run_page_url)}" target="_blank">↗ Databricks</a>`;
        html += `</div></div>`;

        html += `<table class="op-table"><thead><tr><th>Run ID</th><th>State</th><th>Result</th><th>Duration</th><th>Start</th><th>Trigger</th><th></th></tr></thead><tbody>`;
        for (const r of runs) {
          html += `<tr class="clickable" data-run-id="${r.run_id}">`;
          html += `<td class="op-mono"><span class="op-link">${esc(r.run_id)}</span></td>`;
          html += `<td>${stateBadge(r.life_cycle_state)}</td><td>${stateBadge(r.result_state)}</td>`;
          html += `<td class="op-muted">${esc(r.duration||'—')}</td>`;
          html += `<td class="op-muted">${esc(r.start_time||'—')}</td>`;
          html += `<td class="op-muted">${esc(r.trigger||r.run_type||'—')}</td>`;
          html += `<td>`;
          if (r.life_cycle_state==='RUNNING'||r.life_cycle_state==='PENDING') html += `<button class="op-btn op-btn-danger" style="padding:2px 6px;font-size:11px" data-action="cancel" data-run-id="${r.run_id}">Cancel</button> `;
          if (r.result_state==='FAILED') html += `<button class="op-btn op-btn-warning" style="padding:2px 6px;font-size:11px" data-action="repair" data-run-id="${r.run_id}">Repair</button> `;
          if (r.run_page_url) html += `<a class="op-link" href="${esc(r.run_page_url)}" target="_blank">↗</a>`;
          html += `</td></tr>`;
        }
        html += `</tbody></table>`;
      } else {
        html += `<div class="op-empty">No runs found.</div>`;
      }
      html += `</div>`;

      // Tasks tab (DAG)
      html += `<div class="op-tab-content" data-tab="tasks" style="${currentTab!=='tasks'?'display:none':''}">`;
      if (job.tasks && job.tasks.length) {
        html += `<div class="op-dag-canvas">${renderDAGSvg(job.tasks, null)}</div>`;
      } else {
        html += `<div class="op-empty">No tasks defined.</div>`;
      }
      html += `</div>`;

      // Info tab
      html += `<div class="op-tab-content" data-tab="info" style="${currentTab!=='info'?'display:none':''}">`;
      html += `<dl class="op-kv">`;
      html += `<dt>Job ID</dt><dd class="op-mono">${esc(job.job_id)}</dd>`;
      if (job.description) html += `<dt>Description</dt><dd>${esc(job.description)}</dd>`;
      if (job.schedule) html += `<dt>Schedule</dt><dd class="op-mono">${esc(job.schedule)}${job.schedule_tz?' ('+esc(job.schedule_tz)+')':''}${job.schedule_paused==='PAUSED'?' <span class="op-badge op-badge-warning">PAUSED</span>':''}</dd>`;
      html += `<dt>Creator</dt><dd>${esc(job.creator||'—')}</dd>`;
      html += `<dt>Created</dt><dd>${esc(job.created_time||'—')}</dd>`;
      if (job.max_concurrent_runs!=null) html += `<dt>Max concurrent</dt><dd>${job.max_concurrent_runs}</dd>`;
      if (job.timeout_seconds!=null) html += `<dt>Timeout</dt><dd>${job.timeout_seconds}s</dd>`;
      html += `</dl>`;
      if (job.tags && Object.keys(job.tags).length) {
        html += `<div class="op-section">Tags</div>`;
        html += Object.entries(job.tags).map(([k,v])=>`<span class="op-tag">${esc(k)}=${esc(v)}</span>`).join(' ');
      }
      html += `</div>`;

      html += `</div></div>`;
    }

    // Status bar
    html += `<div class="op-status-bar"><span>Last refresh: ${new Date().toLocaleTimeString()}</span>`;
    if (job.schedule) html += `<span>Schedule: ${esc(job.schedule)}</span>`;
    html += `</div>`;

    root.innerHTML = html;
    bindEvents();
  }

  function renderRunView(detail) {
    let html = `<div class="op-body"><div class="op-detail">`;
    html += `<div style="margin-bottom:12px"><button class="op-btn" data-action="back-to-job">← Back to job</button></div>`;
    html += `<dl class="op-kv">`;
    html += `<dt>Run ID</dt><dd class="op-mono">${esc(detail.run_id)}</dd>`;
    html += `<dt>State</dt><dd>${stateBadge(detail.life_cycle_state)} ${stateBadge(detail.result_state)}</dd>`;
    if (detail.state_message) html += `<dt>Message</dt><dd class="op-muted">${esc(detail.state_message)}</dd>`;
    html += `<dt>Start</dt><dd>${esc(detail.start_time||'—')}</dd>`;
    html += `<dt>End</dt><dd>${esc(detail.end_time||'—')}</dd>`;
    html += `<dt>Duration</dt><dd>${esc(detail.duration||'—')}</dd>`;
    if (detail.run_page_url) html += `<dt>Link</dt><dd><a class="op-link" href="${esc(detail.run_page_url)}" target="_blank">Open in Databricks ↗</a></dd>`;
    html += `</dl>`;

    // Actions for this run
    html += `<div style="display:flex;gap:8px;margin-bottom:16px">`;
    if (detail.life_cycle_state==='RUNNING'||detail.life_cycle_state==='PENDING') {
      html += `<button class="op-btn op-btn-danger" data-action="cancel" data-run-id="${detail.run_id}">✕ Cancel Run</button>`;
    }
    if (detail.result_state==='FAILED') {
      html += `<button class="op-btn op-btn-warning" data-action="repair" data-run-id="${detail.run_id}">🔧 Repair Failed Tasks</button>`;
    }
    html += `</div>`;

    if (detail.tasks && detail.tasks.length) {
      html += `<div class="op-section">Tasks (${detail.tasks.length})</div>`;
      html += `<table class="op-table"><thead><tr><th>Task</th><th>Type</th><th>State</th><th>Result</th><th>Duration</th><th></th></tr></thead><tbody>`;
      for (const t of detail.tasks) {
        const st = t.state||{};
        const isSel = t.run_id === selectedTaskRunId;
        html += `<tr class="clickable${isSel?' task-selected':''}" data-task-run-id="${t.run_id||''}" data-task-key="${esc(t.task_key)}">`;
        html += `<td class="op-mono"><span class="op-link">${esc(t.task_key)}</span></td>`;
        html += `<td class="op-muted">${esc(t.type)}</td>`;
        html += `<td>${stateBadge(st.life_cycle_state)}</td><td>${stateBadge(st.result_state)}</td>`;
        html += `<td class="op-muted">${esc(t.duration||'—')}</td>`;
        html += `<td>${t.run_page_url?`<a class="op-link" href="${esc(t.run_page_url)}" target="_blank">↗</a>`:''}</td>`;
        html += `</tr>`;
        if (st.state_message && st.result_state && st.result_state !== 'SUCCESS') {
          html += `<tr><td></td><td colspan="5" class="op-muted" style="font-size:11px;padding-top:0">${esc(st.state_message)}</td></tr>`;
        }
      }
      html += `</tbody></table>`;
    }

    // Task output panel
    if (selectedTaskRunId != null) {
      html += renderOutputPanel();
    }

    html += `</div></div>`;
    return html;
  }

  function renderOutputPanel() {
    const output = JSON.parse(model.get("task_output") || "{}");
    let html = `<div class="op-output-panel">`;
    html += `<div class="op-output-header"><span>Output: ${esc(selectedTaskKey)}</span><button class="op-btn" data-action="close-output" style="border:none;background:none;font-size:16px;cursor:pointer;color:var(--op-text-muted)">✕</button></div>`;

    const tabs = [];
    if (output.error || output.error_trace) tabs.push({ key: "error", label: "Error", isError: true });
    if (output.logs) tabs.push({ key: "logs", label: "Logs" });
    if (output.notebook_result) tabs.push({ key: "notebook", label: "Notebook" });
    if (output.sql_output) tabs.push({ key: "sql", label: "SQL" });
    if (output.dbt_output) tabs.push({ key: "dbt", label: "DBT" });
    if (output.info) tabs.push({ key: "info", label: "Info" });

    if (!tabs.length) {
      html += `<div class="op-output-empty">No output available.</div></div>`;
      return html;
    }

    html += `<div class="op-output-tabs">`;
    tabs.forEach((t, i) => {
      html += `<button class="op-output-tab${i===0?' active':''}${t.isError?' has-error':''}" data-output-tab="${t.key}">${t.label}</button>`;
    });
    html += `</div><div class="op-output-body">`;

    if (output.error || output.error_trace) {
      html += `<div class="op-output-content${tabs[0].key==='error'?' active':''}" data-output-tab="error">`;
      if (output.error) html += `<pre class="op-pre error-text">${esc(output.error)}</pre>`;
      if (output.error_trace) html += `<pre class="op-pre error-text">${esc(output.error_trace)}</pre>`;
      html += `</div>`;
    }
    if (output.logs) {
      html += `<div class="op-output-content${tabs[0].key==='logs'?' active':''}" data-output-tab="logs"><pre class="op-pre">${esc(output.logs)}</pre>`;
      if (output.logs_truncated) html += `<div class="op-truncated">⚠ Logs truncated</div>`;
      html += `</div>`;
    }
    if (output.notebook_result) {
      html += `<div class="op-output-content${tabs[0].key==='notebook'?' active':''}" data-output-tab="notebook"><pre class="op-pre">${esc(output.notebook_result)}</pre></div>`;
    }
    if (output.sql_output) {
      html += `<div class="op-output-content${tabs[0].key==='sql'?' active':''}" data-output-tab="sql"><pre class="op-pre">${esc(output.sql_output)}</pre></div>`;
    }
    if (output.dbt_output) {
      html += `<div class="op-output-content${tabs[0].key==='dbt'?' active':''}" data-output-tab="dbt"><pre class="op-pre">${esc(output.dbt_output)}</pre></div>`;
    }
    if (output.info) {
      html += `<div class="op-output-content${tabs[0].key==='info'?' active':''}" data-output-tab="info"><pre class="op-pre">${esc(output.info)}</pre></div>`;
    }

    html += `</div></div>`;
    return html;
  }

  function bindEvents() {
    // Tabs
    root.querySelectorAll(".op-tab").forEach(tab => {
      tab.addEventListener("click", () => { currentTab = tab.dataset.tab; fullRender(); });
    });
    // Refresh
    root.querySelector("[data-action='refresh']")?.addEventListener("click", () => sendRequest({ action: "refresh" }));
    root.querySelector("[data-action='toggle-refresh']")?.addEventListener("click", () => {
      autoRefreshEnabled = !autoRefreshEnabled;
      if (autoRefreshEnabled) startAutoRefresh(); else stopAutoRefresh();
      fullRender();
    });
    // Run now
    root.querySelector("[data-action='run-now']")?.addEventListener("click", () => {
      confirmAction = { message: "Trigger this job to run now?", btnClass: "success", yesLabel: "▶ Run Now", action: "run_now" };
      fullRender();
    });
    // Cancel buttons
    root.querySelectorAll("[data-action='cancel']").forEach(btn => {
      btn.addEventListener("click", (e) => {
        e.stopPropagation();
        const runId = parseInt(btn.dataset.runId);
        confirmAction = { message: `Cancel run ${runId}?`, btnClass: "danger", yesLabel: "Cancel Run", action: "cancel_run", runId };
        fullRender();
      });
    });
    // Repair buttons
    root.querySelectorAll("[data-action='repair']").forEach(btn => {
      btn.addEventListener("click", (e) => {
        e.stopPropagation();
        const runId = parseInt(btn.dataset.runId);
        confirmAction = { message: `Repair (re-run failed tasks) for run ${runId}?`, btnClass: "warning", yesLabel: "🔧 Repair", action: "repair_run", runId };
        fullRender();
      });
    });
    // Confirm actions
    root.querySelector("[data-action='confirm-yes']")?.addEventListener("click", () => {
      const ca = confirmAction;
      confirmAction = null;
      if (ca.action === "run_now") sendRequest({ action: "run_now" });
      else if (ca.action === "cancel_run") sendRequest({ action: "cancel_run", run_id: ca.runId });
      else if (ca.action === "repair_run") sendRequest({ action: "repair_run", run_id: ca.runId });
    });
    root.querySelector("[data-action='confirm-no']")?.addEventListener("click", () => { confirmAction = null; fullRender(); });
    // Run row clicks
    root.querySelectorAll("tr[data-run-id]").forEach(tr => {
      tr.addEventListener("click", (e) => {
        if (e.target.tagName === 'A' || e.target.tagName === 'BUTTON') return;
        selectedRunId = parseInt(tr.dataset.runId);
        sendRequest({ action: "get_run", run_id: selectedRunId });
      });
    });
    // Back
    root.querySelector("[data-action='back-to-job']")?.addEventListener("click", () => {
      selectedRunId = null; selectedTaskRunId = null; selectedTaskKey = null;
      fullRender();
    });
    // Task run clicks
    root.querySelectorAll("tr[data-task-run-id]").forEach(tr => {
      tr.addEventListener("click", (e) => {
        if (e.target.tagName === 'A') return;
        const rid = tr.dataset.taskRunId ? parseInt(tr.dataset.taskRunId) : null;
        if (!rid) return;
        if (rid === selectedTaskRunId) { selectedTaskRunId = null; selectedTaskKey = null; }
        else { selectedTaskRunId = rid; selectedTaskKey = tr.dataset.taskKey; sendRequest({ action: "get_task_output", run_id: rid }); }
        fullRender();
      });
    });
    // Close output
    root.querySelector("[data-action='close-output']")?.addEventListener("click", () => { selectedTaskRunId = null; selectedTaskKey = null; fullRender(); });
    // Output tabs
    root.querySelectorAll(".op-output-tab").forEach(tab => {
      tab.addEventListener("click", () => {
        root.querySelectorAll(".op-output-tab").forEach(t => t.classList.remove("active"));
        tab.classList.add("active");
        root.querySelectorAll(".op-output-content").forEach(c => c.classList.remove("active"));
        root.querySelector(`.op-output-content[data-output-tab="${tab.dataset.outputTab}"]`)?.classList.add("active");
      });
    });
  }

  // Model change listeners
  model.on("change:job_data", fullRender);
  model.on("change:runs_data", () => { if (!selectedRunId) fullRender(); });
  model.on("change:run_detail", () => { if (selectedRunId) fullRender(); });
  model.on("change:task_output", () => { if (selectedTaskRunId) fullRender(); });
  model.on("change:loading", fullRender);
  model.on("change:error_message", fullRender);
  model.on("change:action_result", () => {
    try {
      const r = JSON.parse(model.get("action_result") || "{}");
      actionMessage = r.message || null;
      actionIsError = !r.success;
      fullRender();
      if (r.success) setTimeout(() => { actionMessage = null; fullRender(); }, 5000);
    } catch(e) {}
  });

  fullRender();
  startAutoRefresh();

  return () => { stopAutoRefresh(); };
}

export default { render };
