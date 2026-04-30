// Pipeline Widget Frontend — single Lakeflow Declarative Pipeline (DLT)
// Auto-refresh, start/stop/full-refresh actions, tabs for Overview / Updates / Events / Spec.

const OPS_STYLES = `
  :host {
    --op-bg: #ffffff; --op-bg-alt: #f8f9fa; --op-bg-hover: #e9ecef;
    --op-border: #dee2e6; --op-text: #212529; --op-text-muted: #6c757d;
    --op-primary: #0d6efd; --op-success: #198754; --op-danger: #dc3545; --op-warning: #ffc107; --op-info: #0dcaf0;
    --op-font: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
    --op-font-mono: "SF Mono", "Cascadia Code", "Fira Code", Menlo, Consolas, monospace;
    --op-radius: 6px;
    display: block; font-family: var(--op-font); font-size: 13px; color: var(--op-text); background: var(--op-bg); border: 1px solid var(--op-border); border-radius: var(--op-radius); overflow: hidden;
  }
  :host(.dark-theme) { --op-bg: #1e1e1e; --op-bg-alt: #252526; --op-bg-hover: #2d2d30; --op-border: #3e3e42; --op-text: #cccccc; --op-text-muted: #888888; --op-primary: #4fc3f7; --op-success: #66bb6a; --op-danger: #ef5350; --op-warning: #ffca28; --op-info: #4dd0e1; }
  * { box-sizing: border-box; }
  .op-header { display: flex; align-items: center; gap: 8px; padding: 10px 14px; background: var(--op-bg-alt); border-bottom: 1px solid var(--op-border); flex-wrap: wrap; }
  .op-header h2 { margin: 0; font-size: 14px; font-weight: 600; }
  .op-header-actions { margin-left: auto; display: flex; gap: 6px; align-items: center; flex-wrap: wrap; }
  .op-btn { padding: 4px 10px; border: 1px solid var(--op-border); border-radius: 4px; background: var(--op-bg); color: var(--op-text); cursor: pointer; font-size: 12px; font-family: var(--op-font); }
  .op-btn:hover { background: var(--op-bg-hover); }
  .op-btn-success { background: var(--op-success); color: #fff; border-color: var(--op-success); }
  .op-btn-danger { background: var(--op-danger); color: #fff; border-color: var(--op-danger); }
  .op-btn-warning { background: var(--op-warning); color: #1a1a1a; border-color: var(--op-warning); }
  .op-body { max-height: 600px; overflow: auto; }
  .op-loading { padding: 30px; text-align: center; color: var(--op-text-muted); }
  .op-loading .spinner { display: inline-block; width: 20px; height: 20px; border: 2px solid var(--op-border); border-top-color: var(--op-primary); border-radius: 50%; animation: op-spin 0.6s linear infinite; margin-right: 8px; vertical-align: middle; }
  @keyframes op-spin { to { transform: rotate(360deg); } }
  .op-error { padding: 10px 14px; background: #fef2f2; color: var(--op-danger); border-bottom: 1px solid #fecaca; font-size: 12px; }
  :host(.dark-theme) .op-error { background: #3b1f1f; border-color: #5c2b2b; }
  .op-success-msg { padding: 10px 14px; background: #f0fdf4; color: var(--op-success); border-bottom: 1px solid #bbf7d0; font-size: 12px; }
  :host(.dark-theme) .op-success-msg { background: #1a2e1a; border-color: #2e5c2b; }
  .op-detail { padding: 14px; }
  .op-kv { display: grid; grid-template-columns: 180px 1fr; gap: 4px 12px; font-size: 12px; }
  .op-kv dt { color: var(--op-text-muted); font-weight: 500; padding: 3px 0; }
  .op-kv dd { margin: 0; padding: 3px 0; word-break: break-word; }
  .op-badge { display: inline-block; padding: 2px 7px; border-radius: 10px; font-size: 11px; font-weight: 500; white-space: nowrap; }
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
  .op-mono { font-family: var(--op-font-mono); font-size: 12px; }
  .op-muted { color: var(--op-text-muted); font-size: 12px; }
  .op-tag { display: inline-block; padding: 1px 6px; margin: 1px 3px 1px 0; border-radius: 3px; font-size: 11px; background: var(--op-bg-alt); border: 1px solid var(--op-border); font-family: var(--op-font-mono); }
  .op-section { font-size: 13px; font-weight: 600; margin: 16px 0 8px; padding-bottom: 4px; border-bottom: 1px solid var(--op-border); }
  .op-auto-refresh { display: flex; align-items: center; gap: 4px; font-size: 11px; color: var(--op-text-muted); }
  .op-auto-refresh .dot { width: 6px; height: 6px; border-radius: 50%; background: var(--op-success); animation: op-pulse 2s ease-in-out infinite; }
  @keyframes op-pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.3; } }
  .op-status-bar { padding: 6px 14px; font-size: 11px; color: var(--op-text-muted); background: var(--op-bg-alt); border-top: 1px solid var(--op-border); display: flex; justify-content: space-between; }
  .op-confirm { padding: 14px; background: var(--op-bg-alt); border-bottom: 1px solid var(--op-border); }
  .op-confirm p { margin: 0 0 10px; }
  .op-confirm-actions { display: flex; gap: 8px; flex-wrap: wrap; }
  .op-confirm-options { margin: 8px 0; display: flex; gap: 12px; flex-wrap: wrap; font-size: 12px; }
  .op-confirm-options label { display: inline-flex; gap: 4px; align-items: center; }
  .op-state-indicator { display: inline-flex; align-items: center; gap: 8px; padding: 10px 16px; border-radius: 8px; margin-bottom: 16px; font-size: 14px; font-weight: 600; }
  .op-state-running { background: #d1fae5; color: #065f46; }
  .op-state-idle { background: #f3f4f6; color: #6b7280; }
  .op-state-starting { background: #dbeafe; color: #1e40af; }
  .op-state-failed { background: #fee2e2; color: #991b1b; }
  :host(.dark-theme) .op-state-running { background: #064e3b; color: #6ee7b7; }
  :host(.dark-theme) .op-state-idle { background: #374151; color: #9ca3af; }
  :host(.dark-theme) .op-state-starting { background: #1e3a5f; color: #93c5fd; }
  :host(.dark-theme) .op-state-failed { background: #7f1d1d; color: #fca5a5; }

  /* Tabs */
  .op-tabs { display: flex; gap: 0; border-bottom: 2px solid var(--op-border); margin: 0 0 12px; }
  .op-tab { padding: 6px 16px; cursor: pointer; font-size: 12px; font-weight: 500; border: none; background: none; color: var(--op-text-muted); border-bottom: 2px solid transparent; margin-bottom: -2px; font-family: var(--op-font); }
  .op-tab:hover { color: var(--op-text); }
  .op-tab.active { color: var(--op-primary); border-bottom-color: var(--op-primary); }

  /* Tables */
  table.op-table { width: 100%; border-collapse: collapse; }
  .op-table th { text-align: left; padding: 6px 10px; font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.4px; color: var(--op-text-muted); background: var(--op-bg-alt); border-bottom: 1px solid var(--op-border); position: sticky; top: 0; }
  .op-table td { padding: 6px 10px; border-bottom: 1px solid var(--op-border); vertical-align: top; font-size: 12px; }
  .op-table tr.clickable { cursor: pointer; }
  .op-table tr.clickable:hover td { background: var(--op-bg-hover); }
  .op-empty { padding: 20px; text-align: center; color: var(--op-text-muted); font-size: 12px; }
  .op-pre { margin: 0; padding: 8px 10px; font-family: var(--op-font-mono); font-size: 11px; line-height: 1.4; white-space: pre-wrap; word-break: break-word; background: var(--op-bg-alt); border-radius: 4px; }

  /* Filter row */
  .op-filter-row { display: flex; gap: 6px; align-items: center; margin-bottom: 8px; }
  .op-filter-row input { flex: 1; padding: 4px 8px; border: 1px solid var(--op-border); border-radius: 4px; font-size: 12px; background: var(--op-bg); color: var(--op-text); font-family: var(--op-font-mono); }
  .op-filter-row input:focus { outline: none; border-color: var(--op-primary); }

  .op-loading-overlay { position: relative; pointer-events: none; opacity: 0.6; }
  .op-loading-overlay::after { content: ''; position: absolute; top: 0; left: 0; right: 0; bottom: 0; background: var(--op-bg); opacity: 0.5; z-index: 10; }
  .op-loading-overlay::before { content: ''; position: absolute; top: 50%; left: 50%; width: 20px; height: 20px; margin: -10px 0 0 -10px; border: 2px solid var(--op-border); border-top-color: var(--op-primary); border-radius: 50%; animation: op-spin 0.6s linear infinite; z-index: 11; }
  /* DAG */
  .dag-canvas { width: 100%; overflow: auto; background: var(--op-bg-alt); border: 1px solid var(--op-border); border-radius: 4px; padding: 8px; }
  .dag-canvas svg { display: block; min-width: 100%; }
  .dag-node rect { fill: var(--op-bg); stroke: var(--op-border); stroke-width: 1.5; rx: 4; }
  .dag-node:hover rect { stroke: var(--op-primary); }
  .dag-node.selected rect { stroke: var(--op-primary); stroke-width: 2.5; }
  .dag-node.source rect { fill: var(--op-bg-alt); stroke-dasharray: 4 3; }
  .dag-node text.title { font-size: 11px; font-family: var(--op-font-mono); fill: var(--op-text); pointer-events: none; }
  .dag-node text.kind { font-size: 9px; fill: var(--op-text-muted); pointer-events: none; }
  .dag-edge { stroke: var(--op-text-muted); stroke-width: 1.2; fill: none; opacity: 0.6; }
  .dag-arrow { fill: var(--op-text-muted); }

  /* Editable cells */
  .ed-input, .ed-select, .ed-textarea { padding: 3px 6px; border: 1px solid var(--op-border); border-radius: 3px; background: var(--op-bg); color: var(--op-text); font-size: 12px; font-family: var(--op-font-mono); width: 100%; box-sizing: border-box; }
  .ed-textarea { font-size: 11px; min-height: 120px; resize: vertical; }
  .ed-input:focus, .ed-select:focus, .ed-textarea:focus { outline: none; border-color: var(--op-primary); }
  .ed-row { display: flex; gap: 6px; align-items: center; margin: 4px 0; flex-wrap: wrap; }
  .ed-checkbox-grp { display: flex; gap: 10px; flex-wrap: wrap; font-size: 12px; }
  .ed-checkbox-grp label { display: inline-flex; gap: 4px; align-items: center; }
  .ed-actions-bar { padding: 8px 0; display: flex; gap: 8px; border-top: 1px solid var(--op-border); margin-top: 12px; }
`;

function esc(s) { if (s == null) return ""; const d = document.createElement("div"); d.textContent = String(s); return d.innerHTML; }

function stateBadge(state) {
  if (!state) return "";
  const s = String(state).toUpperCase();
  let cls = "muted";
  if (s === "COMPLETED" || s === "SUCCESS" || s === "HEALTHY" || s === "IDLE") cls = "success";
  else if (["FAILED", "CANCELED", "UNHEALTHY"].includes(s)) cls = "danger";
  else if (["RUNNING", "STARTING", "INITIALIZING", "QUEUED", "CREATED", "WAITING_FOR_RESOURCES",
            "SETTING_UP_TABLES", "DEPLOYING", "RECOVERING", "RESETTING", "STOPPING"].includes(s)) cls = "info";
  else if (["WARN"].includes(s)) cls = "warning";
  return `<span class="op-badge op-badge-${cls}">${s}</span>`;
}

function levelBadge(level) {
  if (!level) return "";
  const l = String(level).toUpperCase();
  let cls = "muted";
  if (l === "INFO") cls = "info";
  else if (l === "WARN") cls = "warning";
  else if (l === "ERROR") cls = "danger";
  else if (l === "METRICS") cls = "muted";
  return `<span class="op-badge op-badge-${cls}">${l}</span>`;
}

// ===================================================================
// Theme sync (mirrors other widgets)
// ===================================================================
function _syncTheme(hostEl) {
  hostEl.__cleanupThemeSync?.();
  const media = window.matchMedia("(prefers-color-scheme: dark)");
  const themeSelector = "[data-app-theme], [data-theme], .dark, .dark-theme, .light, .light-theme";
  function parseTheme(value) { if (!value) return null; const v = String(value).toLowerCase(); if (v.includes("dark")) return true; if (v.includes("light")) return false; return null; }
  function backgroundLooksDark(el) { if (!el) return null; const bg = getComputedStyle(el).backgroundColor; const m = bg && bg.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/i); if (!m) return null; const [, r, g, b] = m.map(Number); return ((0.2126*r + 0.7152*g + 0.0722*b) / 255) < 0.5; }
  function themeFromElement(el) { if (!el) return null; return parseTheme(el.getAttribute?.("data-app-theme")) ?? parseTheme(el.getAttribute?.("data-theme")) ?? parseTheme(el.className) ?? parseTheme(getComputedStyle(el).colorScheme); }
  function isDark() { const a = hostEl.closest?.(themeSelector); return themeFromElement(a) ?? themeFromElement(hostEl) ?? themeFromElement(hostEl.parentElement) ?? themeFromElement(document.body) ?? themeFromElement(document.documentElement) ?? backgroundLooksDark(hostEl.parentElement) ?? backgroundLooksDark(document.body) ?? media.matches; }
  function apply() { const dark = isDark(); hostEl.classList.toggle("dark-theme", dark); hostEl.style.colorScheme = dark ? "dark" : "light"; }
  apply();
  const obs = new MutationObserver(apply); const observed = new Set();
  function observe(el) { if (!el || observed.has(el)) return; obs.observe(el, { attributes: true, attributeFilter: ["data-app-theme", "data-theme", "class", "style"] }); observed.add(el); }
  observe(document.documentElement); observe(document.body); observe(hostEl.parentElement); observe(hostEl.closest?.(themeSelector));
  media.addEventListener("change", apply);
  const cleanup = () => { obs.disconnect(); media.removeEventListener("change", apply); if (hostEl.__cleanupThemeSync === cleanup) delete hostEl.__cleanupThemeSync; };
  hostEl.__cleanupThemeSync = cleanup;
  return cleanup;
}

// ===================================================================
// Render
// ===================================================================
function render({ model, el }) {
  const shadow = el.attachShadow ? el.attachShadow({ mode: "open" }) : el;
  _syncTheme(el);
  const styleEl = document.createElement("style"); styleEl.textContent = OPS_STYLES; shadow.appendChild(styleEl);
  const root = document.createElement("div"); shadow.appendChild(root);

  let autoRefreshEnabled = true, autoTimer = null, confirmAction = null, actionMessage = null, actionIsError = false;
  let activeTab = "overview";
  let eventFilter = "";
  let hasRendered = false;
  let eventsLoaded = false;
  let permsLoaded = false;
  let graphLoaded = false;
  let selectedNode = null;
  let editedSettings = null; // null = not editing; otherwise {notifications, configuration, channel, development, clusters}

  function getP() { return JSON.parse(model.get("pipeline_data") || "{}"); }
  function getPerms() { return JSON.parse(model.get("permissions_data") || "{}"); }
  function getGraph() { return JSON.parse(model.get("graph_data") || "{}"); }
  function getUpdates() { return JSON.parse(model.get("updates_data") || "[]"); }
  function getEvents() { return JSON.parse(model.get("events_data") || "[]"); }
  function sendRequest(req) { model.set("request", JSON.stringify({ ...req, _t: Date.now() })); model.save_changes(); }
  function startAutoRefresh() {
    stopAutoRefresh();
    const p = getP();
    if (autoRefreshEnabled) autoTimer = setInterval(() => sendRequest({ action: "refresh" }), (p.refresh_seconds || 30) * 1000);
  }
  function stopAutoRefresh() { if (autoTimer) { clearInterval(autoTimer); autoTimer = null; } }

  function isRunningState(s) {
    return ["RUNNING", "STARTING", "INITIALIZING", "QUEUED", "WAITING_FOR_RESOURCES", "SETTING_UP_TABLES", "RESETTING", "RECOVERING"].includes(String(s || "").toUpperCase());
  }

  function renderOverview(p) {
    const spec = p.spec || {};
    let html = `<dl class="op-kv">`;
    html += `<dt>Pipeline ID</dt><dd class="op-mono">${esc(p.pipeline_id)}</dd>`;
    if (p.health) html += `<dt>Health</dt><dd>${stateBadge(p.health)}</dd>`;
    html += `<dt>Mode</dt><dd>${spec.continuous ? '<span class="op-badge op-badge-info">Continuous</span>' : '<span class="op-badge op-badge-muted">Triggered</span>'}${spec.development ? ' <span class="op-badge op-badge-warning">Development</span>' : ' <span class="op-badge op-badge-success">Production</span>'}</dd>`;
    html += `<dt>Compute</dt><dd>${spec.serverless ? '<span class="op-badge op-badge-info">Serverless</span>' : '<span class="op-badge op-badge-muted">Classic</span>'}${spec.photon ? ' <span class="op-badge op-badge-info">Photon</span>' : ''}</dd>`;
    if (spec.channel) html += `<dt>Channel</dt><dd>${esc(spec.channel)}</dd>`;
    if (spec.edition) html += `<dt>Edition</dt><dd>${esc(spec.edition)}</dd>`;
    if (spec.catalog) html += `<dt>Catalog</dt><dd class="op-mono">${esc(spec.catalog)}</dd>`;
    if (spec.schema) html += `<dt>Schema</dt><dd class="op-mono">${esc(spec.schema)}</dd>`;
    if (spec.target && spec.target !== spec.schema) html += `<dt>Target</dt><dd class="op-mono">${esc(spec.target)}</dd>`;
    if (p.cluster_id) html += `<dt>Cluster</dt><dd class="op-mono">${esc(p.cluster_id)}</dd>`;
    html += `<dt>Creator</dt><dd>${esc(p.creator || "—")}</dd>`;
    if (p.run_as && p.run_as !== p.creator) html += `<dt>Run as</dt><dd>${esc(p.run_as)}</dd>`;
    if (p.last_modified) html += `<dt>Last modified</dt><dd>${esc(p.last_modified)}</dd>`;
    if (p.cause) html += `<dt>Cause</dt><dd class="op-muted">${esc(p.cause)}</dd>`;
    html += `</dl>`;

    if (p.latest_updates && p.latest_updates.length) {
      html += `<div class="op-section">Latest updates</div>`;
      html += `<table class="op-table"><thead><tr><th>Update ID</th><th>State</th><th>Created</th></tr></thead><tbody>`;
      for (const u of p.latest_updates.slice(0, 5)) {
        html += `<tr><td class="op-mono">${esc(u.update_id)}</td><td>${stateBadge(u.state)}</td><td class="op-muted">${esc(u.creation_time || "—")}</td></tr>`;
      }
      html += `</tbody></table>`;
    }

    if (spec.tags && Object.keys(spec.tags).length) {
      html += `<div class="op-section">Tags</div>`;
      html += Object.entries(spec.tags).map(([k, v]) => `<span class="op-tag">${esc(k)}=${esc(v)}</span>`).join(" ");
    }
    return html;
  }

  function renderUpdates(updates) {
    if (!updates || !updates.length) return `<div class="op-empty">No updates yet.</div>`;
    let html = `<table class="op-table"><thead><tr>
      <th>Update ID</th><th>State</th><th>Cause</th><th>Created</th><th>Flags</th><th>Cluster</th>
    </tr></thead><tbody>`;
    for (const u of updates) {
      const flags = [];
      if (u.full_refresh) flags.push('<span class="op-badge op-badge-warning">FULL REFRESH</span>');
      if (u.validate_only) flags.push('<span class="op-badge op-badge-info">VALIDATE</span>');
      if (u.full_refresh_selection && u.full_refresh_selection.length) flags.push(`<span class="op-badge op-badge-warning">refresh: ${u.full_refresh_selection.length}</span>`);
      if (u.refresh_selection && u.refresh_selection.length) flags.push(`<span class="op-badge op-badge-info">selective: ${u.refresh_selection.length}</span>`);
      html += `<tr>
        <td class="op-mono">${esc(u.update_id)}</td>
        <td>${stateBadge(u.state)}</td>
        <td class="op-muted">${esc(u.cause || "—")}</td>
        <td class="op-muted">${esc(u.creation_time || "—")}</td>
        <td>${flags.join(" ") || '<span class="op-muted">—</span>'}</td>
        <td class="op-mono op-muted">${esc(u.cluster_id || "—")}</td>
      </tr>`;
    }
    html += `</tbody></table>`;
    return html;
  }

  function renderEvents(events) {
    let html = `<div class="op-filter-row">
      <input type="text" placeholder="Filter (e.g. level='ERROR')" value="${esc(eventFilter)}" data-action="event-filter-input"/>
      <button class="op-btn" data-action="event-filter-apply">Apply</button>
      <button class="op-btn" data-action="event-filter-reload">↻ Reload</button>
    </div>`;
    if (!events || !events.length) {
      html += `<div class="op-empty">${eventsLoaded ? "No events match." : "Loading events…"}</div>`;
      return html;
    }
    html += `<table class="op-table"><thead><tr>
      <th>Time</th><th>Level</th><th>Type</th><th>Update / Flow</th><th>Message</th>
    </tr></thead><tbody>`;
    for (const e of events) {
      const ctx = [];
      if (e.update_id) ctx.push(`<span class="op-tag">u:${esc(e.update_id.slice(0, 8))}</span>`);
      if (e.flow_name) ctx.push(`<span class="op-tag">flow:${esc(e.flow_name)}</span>`);
      if (e.dataset_name) ctx.push(`<span class="op-tag">ds:${esc(e.dataset_name)}</span>`);
      const msgHtml = `${esc(e.message || "")}${e.error ? `<pre class="op-pre" style="margin-top:6px;color:var(--op-danger)">${esc(e.error)}</pre>` : ""}`;
      html += `<tr>
        <td class="op-mono op-muted" style="white-space:nowrap">${esc(e.timestamp || "—")}</td>
        <td>${levelBadge(e.level)}</td>
        <td class="op-muted">${esc(e.event_type || "")}</td>
        <td>${ctx.join(" ") || '<span class="op-muted">—</span>'}</td>
        <td>${msgHtml}</td>
      </tr>`;
    }
    html += `</tbody></table>`;
    return html;
  }

  function renderSpec(p) {
    const spec = p.spec || {};
    let html = `<dl class="op-kv">`;
    if (spec.storage) html += `<dt>Storage</dt><dd class="op-mono">${esc(spec.storage)}</dd>`;
    if (spec.root_path) html += `<dt>Root path</dt><dd class="op-mono">${esc(spec.root_path)}</dd>`;
    if (spec.budget_policy_id) html += `<dt>Budget policy</dt><dd class="op-mono">${esc(spec.budget_policy_id)}</dd>`;
    if (p.effective_publishing_mode) html += `<dt>Publishing mode</dt><dd>${esc(p.effective_publishing_mode)}</dd>`;
    html += `</dl>`;

    if (spec.libraries && spec.libraries.length) {
      html += `<div class="op-section">Libraries (${spec.libraries.length})</div>`;
      html += `<table class="op-table"><thead><tr><th>Type</th><th>Value</th></tr></thead><tbody>`;
      for (const lib of spec.libraries) {
        html += `<tr><td>${esc(lib.type)}</td><td class="op-mono">${esc(lib.value || "—")}</td></tr>`;
      }
      html += `</tbody></table>`;
    }

    if (spec.notifications && spec.notifications.length) {
      html += `<div class="op-section">Notifications</div>`;
      for (const n of spec.notifications) {
        html += `<div style="margin:6px 0">`;
        html += `<div class="op-muted">Alerts: ${n.alerts.map(a => `<span class="op-tag">${esc(a)}</span>`).join(" ") || "—"}</div>`;
        html += `<div class="op-muted">Recipients: ${n.email_recipients.map(a => `<span class="op-tag">${esc(a)}</span>`).join(" ") || "—"}</div>`;
        html += `</div>`;
      }
    }

    if (spec.configuration && Object.keys(spec.configuration).length) {
      html += `<div class="op-section">Configuration (${Object.keys(spec.configuration).length})</div>`;
      html += `<table class="op-table"><thead><tr><th>Key</th><th>Value</th></tr></thead><tbody>`;
      for (const [k, v] of Object.entries(spec.configuration)) {
        html += `<tr><td class="op-mono">${esc(k)}</td><td class="op-mono">${esc(v)}</td></tr>`;
      }
      html += `</tbody></table>`;
    }
    return html;
  }

  // -- DAG layout (longest-path layering) ----------------------------
  function layoutDAG(nodes, edges) {
    const map = new Map();
    for (const n of nodes) map.set(n.name, { node: n, parents: [], children: [] });
    for (const e of edges) {
      if (map.has(e.from) && map.has(e.to)) {
        map.get(e.to).parents.push(e.from);
        map.get(e.from).children.push(e.to);
      }
    }
    const layer = new Map();
    const stack = new Set();
    function depth(k) {
      if (layer.has(k)) return layer.get(k);
      if (stack.has(k)) { layer.set(k, 0); return 0; }
      stack.add(k);
      const node = map.get(k);
      const d = node && node.parents.length ? 1 + Math.max(...node.parents.map(depth)) : 0;
      stack.delete(k);
      layer.set(k, d);
      return d;
    }
    for (const k of map.keys()) depth(k);
    const layers = [];
    for (const [k, l] of layer) { while (layers.length <= l) layers.push([]); layers[l].push(k); }
    for (let i = 1; i < layers.length; i++) {
      const prev = new Map(); layers[i - 1].forEach((k, idx) => prev.set(k, idx));
      layers[i].sort((a, b) => {
        const aa = map.get(a).parents.map(p => prev.get(p) ?? 0);
        const bb = map.get(b).parents.map(p => prev.get(p) ?? 0);
        const av = aa.length ? aa.reduce((x, y) => x + y, 0) / aa.length : 0;
        const bv = bb.length ? bb.reduce((x, y) => x + y, 0) / bb.length : 0;
        return av - bv;
      });
    }
    const NW = 180, NH = 44, PX = 52, PY = 22, M = 16;
    const maxPer = Math.max(1, ...layers.map(l => l.length));
    const positions = new Map();
    for (let li = 0; li < layers.length; li++) {
      const col = layers[li];
      const totalH = col.length * NH + (col.length - 1) * PY;
      const startY = M + (maxPer * NH + (maxPer - 1) * PY - totalH) / 2;
      for (let ni = 0; ni < col.length; ni++) {
        positions.set(col[ni], { x: M + li * (NW + PX), y: startY + ni * (NH + PY) });
      }
    }
    const w = M * 2 + layers.length * NW + Math.max(0, layers.length - 1) * PX;
    const h = M * 2 + maxPer * NH + Math.max(0, maxPer - 1) * PY;
    return { positions, edges, NW, NH, w: Math.max(w, 200), h: Math.max(h, 80) };
  }

  function renderDatasets(graph) {
    if (!graphLoaded) return `<div class="op-empty">Loading dataset graph\u2026</div>`;
    const nodes = (graph && graph.nodes) || [];
    const edges = (graph && graph.edges) || [];
    if (!nodes.length) {
      return `<div class="op-empty">No <code class="op-mono">flow_definition</code> events found yet. Run an update first.</div>`;
    }
    const { positions, NW, NH, w, h } = layoutDAG(nodes, edges);
    let svg = `<svg xmlns="http://www.w3.org/2000/svg" width="${w}" height="${h}" viewBox="0 0 ${w} ${h}">`;
    svg += `<defs><marker id="dag-arrow" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto"><polygon points="0 0, 8 3, 0 6" class="dag-arrow"/></marker></defs>`;
    for (const e of edges) {
      const a = positions.get(e.from), b = positions.get(e.to);
      if (!a || !b) continue;
      const x1 = a.x + NW, y1 = a.y + NH / 2, x2 = b.x, y2 = b.y + NH / 2;
      const cx1 = x1 + (x2 - x1) * 0.4, cx2 = x2 - (x2 - x1) * 0.4;
      svg += `<path class="dag-edge" d="M${x1},${y1} C${cx1},${y1} ${cx2},${y2} ${x2},${y2}" marker-end="url(#dag-arrow)"/>`;
    }
    for (const n of nodes) {
      const p = positions.get(n.name); if (!p) continue;
      const cls = (n.external ? "source" : "") + (n.name === selectedNode ? " selected" : "");
      const lbl = n.name.length > 24 ? n.name.slice(0, 23) + "\u2026" : n.name;
      svg += `<g class="dag-node ${cls}" data-node="${esc(n.name)}">`;
      svg += `<rect x="${p.x}" y="${p.y}" width="${NW}" height="${NH}"/>`;
      svg += `<text class="title" x="${p.x + 8}" y="${p.y + 18}">${esc(lbl)}</text>`;
      svg += `<text class="kind" x="${p.x + 8}" y="${p.y + 33}">${esc(n.type || (n.external ? "external source" : ""))}</text>`;
      svg += `</g>`;
    }
    svg += `</svg>`;
    let html = `<div class="ed-row" style="justify-content:space-between"><span class="op-muted">${nodes.length} datasets, ${edges.length} edges</span>`;
    html += `<button class="op-btn" data-action="reload-graph">\u21bb Reload</button></div>`;
    html += `<div class="dag-canvas">${svg}</div>`;
    if (selectedNode) {
      const n = nodes.find(x => x.name === selectedNode);
      if (n) {
        html += `<div class="op-section">${esc(n.name)}</div>`;
        html += `<dl class="op-kv">`;
        if (n.type) html += `<dt>Type</dt><dd>${esc(n.type)}</dd>`;
        if (n.comment) html += `<dt>Comment</dt><dd>${esc(n.comment)}</dd>`;
        if (n.external) html += `<dt>Source</dt><dd>External (not defined in this pipeline)</dd>`;
        if (n.inputs && n.inputs.length) html += `<dt>Inputs</dt><dd>${n.inputs.map(i => `<span class="op-tag">${esc(i)}</span>`).join(" ")}</dd>`;
        const downstreams = nodes.filter(x => (x.inputs || []).includes(n.name)).map(x => x.name);
        if (downstreams.length) html += `<dt>Downstream</dt><dd>${downstreams.map(d => `<span class="op-tag">${esc(d)}</span>`).join(" ")}</dd>`;
        html += `</dl>`;
      }
    }
    return html;
  }

  function renderPermissions(perms) {
    if (!permsLoaded) return `<div class="op-empty">Loading permissions\u2026</div>`;
    const acl = (perms && perms.acl) || [];
    const editing = !!editedSettings && editedSettings.__perms;
    const rows = editing ? editedSettings.__perms : acl.map(e => ({
      principal: e.principal,
      type: e.type,
      permission_level: (e.permissions && e.permissions[0] && e.permissions[0].level) || "CAN_VIEW",
      inherited: e.permissions && e.permissions.some(p => p.inherited),
    }));
    let html = `<div class="ed-row" style="justify-content:space-between">`;
    html += `<span class="op-muted">${rows.length} principal(s)</span>`;
    if (!editing) {
      html += `<div><button class="op-btn" data-action="perms-edit">\u270e Edit</button>`;
      html += ` <button class="op-btn" data-action="reload-perms">\u21bb Reload</button></div>`;
    }
    html += `</div>`;
    html += `<table class="op-table"><thead><tr>
      <th>Principal</th><th>Type</th><th>Level</th>${editing ? "<th></th>" : "<th>Inherited</th>"}
    </tr></thead><tbody>`;
    for (let i = 0; i < rows.length; i++) {
      const r = rows[i];
      html += `<tr>`;
      if (editing) {
        html += `<td><input class="ed-input" data-perm-field="principal" data-perm-idx="${i}" value="${esc(r.principal || "")}"/></td>`;
        html += `<td><select class="ed-select" data-perm-field="type" data-perm-idx="${i}">
          <option value="user" ${r.type === "user" ? "selected" : ""}>user</option>
          <option value="group" ${r.type === "group" ? "selected" : ""}>group</option>
          <option value="service_principal" ${r.type === "service_principal" ? "selected" : ""}>service principal</option>
        </select></td>`;
        html += `<td><select class="ed-select" data-perm-field="permission_level" data-perm-idx="${i}">
          ${["CAN_VIEW", "CAN_RUN", "CAN_MANAGE", "IS_OWNER"].map(l => `<option value="${l}" ${r.permission_level === l ? "selected" : ""}>${l}</option>`).join("")}
        </select></td>`;
        html += `<td><button class="op-btn op-btn-danger" data-action="perm-del" data-perm-idx="${i}">\u2715</button></td>`;
      } else {
        html += `<td>${esc(r.principal || "")}</td><td class="op-muted">${esc(r.type)}</td><td>${esc(r.permission_level)}</td><td>${r.inherited ? '<span class="op-badge op-badge-muted">inherited</span>' : ""}</td>`;
      }
      html += `</tr>`;
    }
    html += `</tbody></table>`;
    if (editing) {
      html += `<div class="ed-actions-bar">
        <button class="op-btn" data-action="perm-add">+ Add row</button>
        <button class="op-btn op-btn-success" data-action="perms-save">\u2713 Save</button>
        <button class="op-btn" data-action="perms-cancel">Cancel</button>
      </div>`;
      html += `<div class="op-muted" style="margin-top:6px">\u26a0\ufe0f Saving replaces the entire ACL. Inherited rows are not editable here \u2014 they are managed at the workspace level.</div>`;
    }
    return html;
  }

  function renderSettings(p) {
    const spec = p.spec || {};
    const editing = !!editedSettings && editedSettings.__settings;
    const cur = editing ? editedSettings.__settings : {
      notifications: spec.notifications || [],
      configuration: spec.configuration || {},
      clusters: spec.clusters || [],
      channel: spec.channel || "CURRENT",
      development: !!spec.development,
    };
    const ALERTS = ["on-update-success", "on-update-failure", "on-update-fatal-failure", "on-flow-failure"];

    let html = `<div class="ed-row" style="justify-content:space-between">`;
    html += `<span class="op-muted">Edit notifications, cluster overrides, configuration, channel, and development mode.</span>`;
    if (!editing) html += `<button class="op-btn" data-action="settings-edit">\u270e Edit</button>`;
    html += `</div>`;

    // --- Channel + development
    html += `<div class="op-section">Pipeline mode</div>`;
    if (editing) {
      html += `<div class="ed-row"><label>Channel:&nbsp;
        <select class="ed-select" data-set-field="channel" style="width:auto">
          <option value="CURRENT" ${cur.channel === "CURRENT" ? "selected" : ""}>CURRENT</option>
          <option value="PREVIEW" ${cur.channel === "PREVIEW" ? "selected" : ""}>PREVIEW</option>
        </select></label>
        <label>&nbsp;<input type="checkbox" data-set-field="development" ${cur.development ? "checked" : ""}/> Development mode</label></div>`;
    } else {
      html += `<div class="ed-row"><span>Channel: <strong>${esc(cur.channel)}</strong></span><span>Development: <strong>${cur.development ? "yes" : "no"}</strong></span></div>`;
    }

    // --- Notifications
    html += `<div class="op-section">Notifications</div>`;
    if (!cur.notifications.length && !editing) html += `<div class="op-muted">None configured.</div>`;
    for (let i = 0; i < cur.notifications.length; i++) {
      const n = cur.notifications[i];
      html += `<div style="border:1px solid var(--op-border); border-radius:4px; padding:8px; margin:6px 0">`;
      if (editing) {
        html += `<div class="ed-checkbox-grp">${ALERTS.map(a => `<label><input type="checkbox" data-notif-alert="${a}" data-notif-idx="${i}" ${(n.alerts || []).includes(a) ? "checked" : ""}/>${a}</label>`).join("")}</div>`;
        html += `<div class="ed-row" style="margin-top:6px"><input class="ed-input" data-notif-recipients data-notif-idx="${i}" value="${esc((n.email_recipients || []).join(", "))}" placeholder="comma-separated emails"/>
          <button class="op-btn op-btn-danger" data-action="notif-del" data-notif-idx="${i}">\u2715</button></div>`;
      } else {
        html += `<div class="op-muted">Alerts: ${(n.alerts || []).map(a => `<span class="op-tag">${esc(a)}</span>`).join(" ") || "\u2014"}</div>`;
        html += `<div class="op-muted">Recipients: ${(n.email_recipients || []).map(r => `<span class="op-tag">${esc(r)}</span>`).join(" ") || "\u2014"}</div>`;
      }
      html += `</div>`;
    }
    if (editing) html += `<button class="op-btn" data-action="notif-add">+ Add notification</button>`;

    // --- Configuration
    html += `<div class="op-section">Configuration</div>`;
    const confEntries = Object.entries(cur.configuration);
    if (!confEntries.length && !editing) html += `<div class="op-muted">No configuration entries.</div>`;
    if (confEntries.length || editing) {
      html += `<table class="op-table"><thead><tr><th>Key</th><th>Value</th>${editing ? "<th></th>" : ""}</tr></thead><tbody>`;
      for (let i = 0; i < confEntries.length; i++) {
        const [k, v] = confEntries[i];
        if (editing) {
          html += `<tr><td><input class="ed-input" data-conf-key="${i}" value="${esc(k)}"/></td>
            <td><input class="ed-input" data-conf-val="${i}" value="${esc(v)}"/></td>
            <td><button class="op-btn op-btn-danger" data-action="conf-del" data-conf-idx="${i}">\u2715</button></td></tr>`;
        } else {
          html += `<tr><td class="op-mono">${esc(k)}</td><td class="op-mono">${esc(v)}</td></tr>`;
        }
      }
      html += `</tbody></table>`;
      if (editing) html += `<button class="op-btn" data-action="conf-add" style="margin-top:6px">+ Add entry</button>`;
    }

    // --- Clusters (JSON editor)
    html += `<div class="op-section">Cluster overrides</div>`;
    html += `<div class="op-muted" style="margin-bottom:6px">Per-label cluster overrides (e.g. <code class="op-mono">default</code> / <code class="op-mono">maintenance</code>). Edited as JSON \u2014 keys map to SDK <code class="op-mono">PipelineCluster</code> fields.</div>`;
    if (editing) {
      const json = JSON.stringify(cur.clusters, null, 2);
      html += `<textarea class="ed-textarea" data-set-field="clusters" spellcheck="false">${esc(json)}</textarea>`;
    } else if (cur.clusters.length) {
      html += `<pre class="op-pre">${esc(JSON.stringify(cur.clusters, null, 2))}</pre>`;
    } else {
      html += `<div class="op-muted">No cluster overrides defined.</div>`;
    }

    if (editing) {
      html += `<div class="ed-actions-bar">
        <button class="op-btn op-btn-success" data-action="settings-save">\u2713 Save</button>
        <button class="op-btn" data-action="settings-cancel">Cancel</button>
      </div>`;
      html += `<div class="op-muted" style="margin-top:6px">\u26a0\ufe0f Saving sends the entire pipeline spec back to Databricks (PUT-style API). Other unrelated fields are preserved from the cached spec.</div>`;
    }
    return html;
  }

  function fullRender() {
    const p = getP();
    const loading = model.get("loading");
    const error = model.get("error_message");
    const state = (p.state || "").toUpperCase();
    const running = isRunningState(state);
    const failed = state === "FAILED";

    let html = `<div class="op-header"><h2>🪈 ${esc(p.name || "Pipeline")}</h2>`;
    html += `<div class="op-header-actions">`;
    html += `<div class="op-auto-refresh">${autoRefreshEnabled ? '<span class="dot"></span> Auto' : 'Paused'}</div>`;
    html += `<button class="op-btn" data-action="toggle-refresh" title="${autoRefreshEnabled ? "Pause auto-refresh" : "Resume auto-refresh"}">${autoRefreshEnabled ? '⏸' : '▶'}</button>`;
    html += `<button class="op-btn" data-action="refresh" title="Refresh now">↻</button>`;
    if (running) {
      html += `<button class="op-btn op-btn-danger" data-action="stop">⏹ Stop</button>`;
    } else {
      html += `<button class="op-btn op-btn-success" data-action="start">▶ Start</button>`;
      html += `<button class="op-btn op-btn-warning" data-action="full-refresh" title="Full refresh: drop & recompute all tables">↻ Full refresh</button>`;
      html += `<button class="op-btn" data-action="validate" title="Validate-only: parse and plan without executing">✓ Validate</button>`;
    }
    html += `</div></div>`;

    if (confirmAction) {
      html += `<div class="op-confirm"><p>${confirmAction.message}</p>`;
      if (confirmAction.options) html += confirmAction.options;
      html += `<div class="op-confirm-actions">
        <button class="op-btn op-btn-${confirmAction.btnClass}" data-action="confirm-yes">${confirmAction.yesLabel}</button>
        <button class="op-btn" data-action="confirm-no">Cancel</button>
      </div></div>`;
    }
    if (actionMessage) html += `<div class="${actionIsError ? "op-error" : "op-success-msg"}">${esc(actionMessage)}</div>`;
    if (error) html += `<div class="op-error">${esc(error)}</div>`;

    if (loading && !hasRendered) {
      html += `<div class="op-body"><div class="op-loading"><span class="spinner"></span> Loading…</div></div>`;
    } else {
      const stCls = running ? "starting" : failed ? "failed" : (state === "IDLE" ? "idle" : "idle");
      const stIcon = running ? "🔵" : failed ? "🔴" : "⚪";
      html += `<div class="op-body${loading ? " op-loading-overlay" : ""}"><div class="op-detail">`;
      html += `<div class="op-state-indicator op-state-${stCls}">${stIcon} ${esc(state || "UNKNOWN")}</div>`;

      // Tabs
      html += `<div class="op-tabs">
        <button class="op-tab ${activeTab === "overview" ? "active" : ""}" data-tab="overview">Overview</button>
        <button class="op-tab ${activeTab === "updates" ? "active" : ""}" data-tab="updates">Updates (${getUpdates().length})</button>
        <button class="op-tab ${activeTab === "events" ? "active" : ""}" data-tab="events">Events</button>
        <button class="op-tab ${activeTab === "datasets" ? "active" : ""}" data-tab="datasets">Datasets</button>
        <button class="op-tab ${activeTab === "spec" ? "active" : ""}" data-tab="spec">Spec</button>
        <button class="op-tab ${activeTab === "settings" ? "active" : ""}" data-tab="settings">Settings</button>
        <button class="op-tab ${activeTab === "permissions" ? "active" : ""}" data-tab="permissions">Permissions</button>
      </div>`;

      if (activeTab === "overview") html += renderOverview(p);
      else if (activeTab === "updates") html += renderUpdates(getUpdates());
      else if (activeTab === "events") html += renderEvents(getEvents());
      else if (activeTab === "datasets") html += renderDatasets(getGraph());
      else if (activeTab === "spec") html += renderSpec(p);
      else if (activeTab === "settings") html += renderSettings(p);
      else if (activeTab === "permissions") html += renderPermissions(getPerms());

      html += `</div></div>`;
    }

    html += `<div class="op-status-bar"><span>Last refresh: ${new Date().toLocaleTimeString()}</span><span class="op-mono">${esc(p.pipeline_id || "")}</span></div>`;
    root.innerHTML = html;
    hasRendered = true;
    bindEvents();
  }

  function bindEvents() {
    root.querySelector("[data-action='refresh']")?.addEventListener("click", () => sendRequest({ action: "refresh" }));
    root.querySelector("[data-action='toggle-refresh']")?.addEventListener("click", () => {
      autoRefreshEnabled = !autoRefreshEnabled;
      if (autoRefreshEnabled) startAutoRefresh(); else stopAutoRefresh();
      fullRender();
    });

    root.querySelector("[data-action='start']")?.addEventListener("click", () => {
      confirmAction = { message: "Start an update for this pipeline?", btnClass: "success", yesLabel: "▶ Start update", action: "start_update", payload: {} };
      fullRender();
    });
    root.querySelector("[data-action='full-refresh']")?.addEventListener("click", () => {
      confirmAction = {
        message: "<strong>Full refresh</strong> will drop and recompute all materialized tables in this pipeline. This can be expensive and is destructive.",
        btnClass: "warning", yesLabel: "↻ Full refresh", action: "start_update", payload: { full_refresh: true },
      };
      fullRender();
    });
    root.querySelector("[data-action='validate']")?.addEventListener("click", () => {
      confirmAction = { message: "Run a validate-only update? (Parses & plans without executing.)", btnClass: "success", yesLabel: "✓ Validate", action: "start_update", payload: { validate_only: true } };
      fullRender();
    });
    root.querySelector("[data-action='stop']")?.addEventListener("click", () => {
      confirmAction = { message: "Stop this pipeline? Any in-flight update will be canceled.", btnClass: "danger", yesLabel: "⏹ Stop", action: "stop", payload: {} };
      fullRender();
    });
    root.querySelector("[data-action='confirm-yes']")?.addEventListener("click", () => {
      const a = confirmAction.action; const payload = confirmAction.payload || {};
      confirmAction = null;
      sendRequest({ action: a, ...payload });
    });
    root.querySelector("[data-action='confirm-no']")?.addEventListener("click", () => { confirmAction = null; fullRender(); });

    // Tab switching
    root.querySelectorAll(".op-tab").forEach((tab) => {
      tab.addEventListener("click", () => {
        activeTab = tab.dataset.tab;
        if (activeTab === "events" && !eventsLoaded) {
          eventsLoaded = true;
          sendRequest({ action: "get_events", filter: eventFilter || null });
        }
        if (activeTab === "permissions" && !permsLoaded) {
          permsLoaded = true;
          sendRequest({ action: "get_permissions" });
        }
        if (activeTab === "datasets" && !graphLoaded) {
          graphLoaded = true;
          sendRequest({ action: "get_graph" });
        }
        // Cancel any in-progress edit when leaving Settings/Permissions.
        if (activeTab !== "settings" && editedSettings && editedSettings.__settings) editedSettings = null;
        if (activeTab !== "permissions" && editedSettings && editedSettings.__perms) editedSettings = null;
        fullRender();
      });
    });

    // Datasets tab handlers
    root.querySelectorAll(".dag-node").forEach((g) => {
      g.addEventListener("click", () => { selectedNode = g.dataset.node; fullRender(); });
    });
    root.querySelector("[data-action='reload-graph']")?.addEventListener("click", () => {
      sendRequest({ action: "get_graph" });
    });

    // Permissions tab handlers
    root.querySelector("[data-action='reload-perms']")?.addEventListener("click", () => sendRequest({ action: "get_permissions" }));
    root.querySelector("[data-action='perms-edit']")?.addEventListener("click", () => {
      const acl = getPerms().acl || [];
      // Filter out inherited entries (read-only).
      editedSettings = { __perms: acl
        .filter(e => !(e.permissions || []).every(p => p.inherited))
        .map(e => ({
          principal: e.principal, type: e.type,
          permission_level: (e.permissions && e.permissions[0] && e.permissions[0].level) || "CAN_VIEW",
        })) };
      fullRender();
    });
    root.querySelector("[data-action='perms-cancel']")?.addEventListener("click", () => { editedSettings = null; fullRender(); });
    root.querySelector("[data-action='perm-add']")?.addEventListener("click", () => {
      editedSettings.__perms.push({ principal: "", type: "user", permission_level: "CAN_VIEW" });
      fullRender();
    });
    root.querySelectorAll("[data-action='perm-del']").forEach((b) => {
      b.addEventListener("click", () => { editedSettings.__perms.splice(Number(b.dataset.permIdx), 1); fullRender(); });
    });
    root.querySelectorAll("[data-perm-field]").forEach((inp) => {
      inp.addEventListener("change", () => {
        const i = Number(inp.dataset.permIdx);
        editedSettings.__perms[i][inp.dataset.permField] = inp.value;
      });
      if (inp.tagName === "INPUT") inp.addEventListener("input", () => {
        const i = Number(inp.dataset.permIdx);
        editedSettings.__perms[i][inp.dataset.permField] = inp.value;
      });
    });
    root.querySelector("[data-action='perms-save']")?.addEventListener("click", () => {
      const acl = editedSettings.__perms
        .filter(r => r.principal && r.principal.trim())
        .map(r => ({
          [r.type === "group" ? "group_name" : r.type === "service_principal" ? "service_principal_name" : "user_name"]: r.principal.trim(),
          permission_level: r.permission_level,
        }));
      editedSettings = null;
      sendRequest({ action: "update_permissions", acl });
    });

    // Settings tab handlers
    root.querySelector("[data-action='settings-edit']")?.addEventListener("click", () => {
      const spec = getP().spec || {};
      editedSettings = { __settings: {
        notifications: JSON.parse(JSON.stringify(spec.notifications || [])),
        configuration: JSON.parse(JSON.stringify(spec.configuration || {})),
        clusters: JSON.parse(JSON.stringify(spec.clusters || [])),
        channel: spec.channel || "CURRENT",
        development: !!spec.development,
      } };
      fullRender();
    });
    root.querySelector("[data-action='settings-cancel']")?.addEventListener("click", () => { editedSettings = null; fullRender(); });

    if (editedSettings && editedSettings.__settings) {
      const s = editedSettings.__settings;
      root.querySelectorAll("[data-set-field]").forEach((el) => {
        const handler = () => {
          const f = el.dataset.setField;
          if (f === "development") s.development = el.checked;
          else if (f === "channel") s.channel = el.value;
          else if (f === "clusters") s.__clustersText = el.value; // store text; parse on save
        };
        el.addEventListener("change", handler);
        if (el.tagName === "TEXTAREA" || el.tagName === "INPUT") el.addEventListener("input", handler);
      });
      root.querySelectorAll("[data-notif-alert]").forEach((cb) => {
        cb.addEventListener("change", () => {
          const i = Number(cb.dataset.notifIdx);
          const a = cb.dataset.notifAlert;
          const cur = s.notifications[i].alerts || (s.notifications[i].alerts = []);
          const idx = cur.indexOf(a);
          if (cb.checked && idx < 0) cur.push(a); else if (!cb.checked && idx >= 0) cur.splice(idx, 1);
        });
      });
      root.querySelectorAll("[data-notif-recipients]").forEach((inp) => {
        inp.addEventListener("input", () => {
          const i = Number(inp.dataset.notifIdx);
          s.notifications[i].email_recipients = inp.value.split(",").map(x => x.trim()).filter(Boolean);
        });
      });
      root.querySelector("[data-action='notif-add']")?.addEventListener("click", () => {
        s.notifications.push({ alerts: [], email_recipients: [] }); fullRender();
      });
      root.querySelectorAll("[data-action='notif-del']").forEach((b) => {
        b.addEventListener("click", () => { s.notifications.splice(Number(b.dataset.notifIdx), 1); fullRender(); });
      });
      // Configuration: rebuild dict on save from current input values to handle key renames.
      root.querySelector("[data-action='conf-add']")?.addEventListener("click", () => {
        s.configuration[""] = ""; fullRender();
      });
      root.querySelectorAll("[data-action='conf-del']").forEach((b) => {
        b.addEventListener("click", () => {
          const i = Number(b.dataset.confIdx);
          const keys = Object.keys(s.configuration);
          delete s.configuration[keys[i]];
          fullRender();
        });
      });
      root.querySelector("[data-action='settings-save']")?.addEventListener("click", () => {
        // Rebuild configuration from current input values (preserves key renames).
        const newConf = {};
        const keyInputs = root.querySelectorAll("[data-conf-key]");
        const valInputs = root.querySelectorAll("[data-conf-val]");
        for (let i = 0; i < keyInputs.length; i++) {
          const k = keyInputs[i].value.trim();
          if (k) newConf[k] = valInputs[i].value;
        }
        s.configuration = newConf;
        // Parse clusters textarea if edited.
        let clusters = s.clusters;
        if (s.__clustersText !== undefined) {
          try { clusters = JSON.parse(s.__clustersText); }
          catch (e) { actionMessage = "Cluster JSON parse error: " + e.message; actionIsError = true; fullRender(); return; }
          if (!Array.isArray(clusters)) { actionMessage = "Cluster JSON must be an array"; actionIsError = true; fullRender(); return; }
        }
        const payload = {
          notifications: s.notifications,
          configuration: s.configuration,
          clusters,
          channel: s.channel,
          development: s.development,
        };
        editedSettings = null;
        sendRequest({ action: "update_settings", settings: payload });
      });
    }

    // Event filter
    const filterInput = root.querySelector("[data-action='event-filter-input']");
    if (filterInput) {
      filterInput.addEventListener("input", (e) => { eventFilter = e.target.value; });
      filterInput.addEventListener("keydown", (e) => {
        if (e.key === "Enter") { sendRequest({ action: "get_events", filter: eventFilter || null }); }
      });
    }
    root.querySelector("[data-action='event-filter-apply']")?.addEventListener("click", () => {
      sendRequest({ action: "get_events", filter: eventFilter || null });
    });
    root.querySelector("[data-action='event-filter-reload']")?.addEventListener("click", () => {
      sendRequest({ action: "get_events", filter: eventFilter || null });
    });
  }

  model.on("change:pipeline_data", fullRender);
  model.on("change:updates_data", () => { if (activeTab === "updates" || activeTab === "overview") fullRender(); });
  model.on("change:events_data", () => { if (activeTab === "events") fullRender(); });
  model.on("change:permissions_data", () => { if (activeTab === "permissions") fullRender(); });
  model.on("change:graph_data", () => { if (activeTab === "datasets") fullRender(); });
  model.on("change:loading", fullRender);
  model.on("change:error_message", fullRender);
  model.on("change:action_result", () => {
    try {
      const r = JSON.parse(model.get("action_result") || "{}");
      actionMessage = r.message; actionIsError = !r.success;
      fullRender();
      if (r.success) setTimeout(() => { actionMessage = null; fullRender(); }, 5000);
    } catch (e) {}
  });

  fullRender();
  startAutoRefresh();
  return () => stopAutoRefresh();
}

export default { render };
