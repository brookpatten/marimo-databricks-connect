// SQL Warehouse Widget Frontend
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
  .op-header-actions { margin-left: auto; display: flex; gap: 6px; align-items: center; }
  .op-btn { padding: 4px 10px; border: 1px solid var(--op-border); border-radius: 4px; background: var(--op-bg); color: var(--op-text); cursor: pointer; font-size: 12px; font-family: var(--op-font); }
  .op-btn:hover { background: var(--op-bg-hover); }
  .op-btn-success { background: var(--op-success); color: #fff; border-color: var(--op-success); }
  .op-btn-danger { background: var(--op-danger); color: #fff; border-color: var(--op-danger); }
  .op-body { max-height: 500px; overflow: auto; }
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
  .op-kv dd { margin: 0; padding: 3px 0; }
  .op-badge { display: inline-block; padding: 2px 7px; border-radius: 10px; font-size: 11px; font-weight: 500; }
  .op-badge-success { background: #d1fae5; color: #065f46; }
  .op-badge-danger { background: #fee2e2; color: #991b1b; }
  .op-badge-info { background: #dbeafe; color: #1e40af; }
  .op-badge-warning { background: #fef3c7; color: #92400e; }
  .op-badge-muted { background: #f3f4f6; color: #6b7280; }
  :host(.dark-theme) .op-badge-success { background: #064e3b; color: #6ee7b7; } :host(.dark-theme) .op-badge-danger { background: #7f1d1d; color: #fca5a5; } :host(.dark-theme) .op-badge-info { background: #1e3a5f; color: #93c5fd; } :host(.dark-theme) .op-badge-warning { background: #78350f; color: #fcd34d; } :host(.dark-theme) .op-badge-muted { background: #374151; color: #9ca3af; }
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
  .op-confirm-actions { display: flex; gap: 8px; }
  .op-state-indicator { display: inline-flex; align-items: center; gap: 8px; padding: 10px 16px; border-radius: 8px; margin-bottom: 16px; font-size: 14px; font-weight: 600; }
  .op-state-running { background: #d1fae5; color: #065f46; }
  .op-state-stopped { background: #f3f4f6; color: #6b7280; }
  .op-state-starting { background: #dbeafe; color: #1e40af; }
  :host(.dark-theme) .op-state-running { background: #064e3b; color: #6ee7b7; } :host(.dark-theme) .op-state-stopped { background: #374151; color: #9ca3af; } :host(.dark-theme) .op-state-starting { background: #1e3a5f; color: #93c5fd; }
  /* Scaling bar */
  .op-scale-bar { display: flex; align-items: center; gap: 8px; margin: 16px 0; }
  .op-scale-track { flex: 1; height: 8px; background: var(--op-border); border-radius: 4px; position: relative; }
  .op-scale-fill { height: 100%; border-radius: 4px; background: var(--op-primary); transition: width 0.3s; }

  .op-loading-overlay { position: relative; pointer-events: none; opacity: 0.6; }
  .op-loading-overlay::after { content: ''; position: absolute; top: 0; left: 0; right: 0; bottom: 0; background: var(--op-bg); opacity: 0.5; z-index: 10; }
  .op-loading-overlay::before { content: ''; position: absolute; top: 50%; left: 50%; width: 20px; height: 20px; margin: -10px 0 0 -10px; border: 2px solid var(--op-border); border-top-color: var(--op-primary); border-radius: 50%; animation: op-spin 0.6s linear infinite; z-index: 11; }
`;

function esc(s) { if (s == null) return ""; const d = document.createElement("div"); d.textContent = String(s); return d.innerHTML; }


function _syncTheme(hostEl) {
  hostEl.__cleanupThemeSync?.();

  const media = window.matchMedia("(prefers-color-scheme: dark)");
  const themeSelector = "[data-app-theme], [data-theme], .dark, .dark-theme, .light, .light-theme";

  function parseTheme(value) {
    if (!value) return null;
    const v = String(value).toLowerCase();
    if (v.includes("dark")) return true;
    if (v.includes("light")) return false;
    return null;
  }

  function backgroundLooksDark(el) {
    if (!el) return null;
    const bg = getComputedStyle(el).backgroundColor;
    const m = bg && bg.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/i);
    if (!m) return null;
    const [, r, g, b] = m.map(Number);
    const luminance = (0.2126 * r + 0.7152 * g + 0.0722 * b) / 255;
    return luminance < 0.5;
  }

  function themeFromElement(el) {
    if (!el) return null;
    const attrTheme = parseTheme(el.getAttribute?.("data-app-theme"));
    if (attrTheme != null) return attrTheme;
    const dataTheme = parseTheme(el.getAttribute?.("data-theme"));
    if (dataTheme != null) return dataTheme;
    const classTheme = parseTheme(el.className);
    if (classTheme != null) return classTheme;
    const schemeTheme = parseTheme(getComputedStyle(el).colorScheme);
    if (schemeTheme != null) return schemeTheme;
    return null;
  }

  function isDark() {
    const themedAncestor = hostEl.closest?.(themeSelector);
    return themeFromElement(themedAncestor)
      ?? themeFromElement(hostEl)
      ?? themeFromElement(hostEl.parentElement)
      ?? themeFromElement(document.body)
      ?? themeFromElement(document.documentElement)
      ?? backgroundLooksDark(hostEl.parentElement)
      ?? backgroundLooksDark(document.body)
      ?? media.matches;
  }

  function apply() {
    const dark = isDark();
    hostEl.classList.toggle("dark-theme", dark);
    hostEl.style.colorScheme = dark ? "dark" : "light";
  }

  apply();

  const obs = new MutationObserver(apply);
  const observed = new Set();
  function observe(el) {
    if (!el || observed.has(el)) return;
    obs.observe(el, { attributes: true, attributeFilter: ["data-app-theme", "data-theme", "class", "style"] });
    observed.add(el);
  }

  observe(document.documentElement);
  observe(document.body);
  observe(hostEl.parentElement);
  observe(hostEl.closest?.(themeSelector));

  media.addEventListener("change", apply);

  const cleanup = () => {
    obs.disconnect();
    media.removeEventListener("change", apply);
    if (hostEl.__cleanupThemeSync === cleanup) delete hostEl.__cleanupThemeSync;
  };
  hostEl.__cleanupThemeSync = cleanup;
  return cleanup;
}

function render({ model, el }) {
  const shadow = el.attachShadow ? el.attachShadow({ mode: "open" }) : el;
  _syncTheme(el);
  const styleEl = document.createElement("style"); styleEl.textContent = OPS_STYLES; shadow.appendChild(styleEl);
  const root = document.createElement("div"); shadow.appendChild(root);

  let autoRefreshEnabled = true, autoTimer = null, confirmAction = null, actionMessage = null, actionIsError = false;
  let hasRendered = false;

  function getWH() { return JSON.parse(model.get("warehouse_data") || "{}"); }
  function sendRequest(req) { model.set("request", JSON.stringify({ ...req, _t: Date.now() })); model.save_changes(); }
  function startAutoRefresh() { stopAutoRefresh(); const w = getWH(); if (autoRefreshEnabled) autoTimer = setInterval(() => sendRequest({ action: "refresh" }), (w.refresh_seconds||30)*1000); }
  function stopAutoRefresh() { if (autoTimer) { clearInterval(autoTimer); autoTimer = null; } }

  function fullRender() {
    const w = getWH();
    const loading = model.get("loading");
    const error = model.get("error_message");
    const isRunning = w.state === "RUNNING";
    const isStopped = w.state === "STOPPED";
    const isStarting = ["STARTING", "STOPPING"].includes(w.state);

    let html = `<div class="op-header"><h2>🏢 ${esc(w.name || "SQL Warehouse")}</h2>`;
    html += `<div class="op-header-actions">`;
    html += `<div class="op-auto-refresh">${autoRefreshEnabled ? '<span class="dot"></span> Auto' : 'Paused'}</div>`;
    html += `<button class="op-btn" data-action="toggle-refresh">${autoRefreshEnabled ? '⏸' : '▶'}</button>`;
    html += `<button class="op-btn" data-action="refresh">↻</button>`;
    if (isStopped) html += `<button class="op-btn op-btn-success" data-action="start">▶ Start</button>`;
    if (isRunning) html += `<button class="op-btn op-btn-danger" data-action="stop">⏹ Stop</button>`;
    html += `</div></div>`;

    if (confirmAction) html += `<div class="op-confirm"><p>${confirmAction.message}</p><div class="op-confirm-actions"><button class="op-btn op-btn-${confirmAction.btnClass}" data-action="confirm-yes">${confirmAction.yesLabel}</button><button class="op-btn" data-action="confirm-no">Cancel</button></div></div>`;
    if (actionMessage) html += `<div class="${actionIsError ? 'op-error' : 'op-success-msg'}">${esc(actionMessage)}</div>`;
    if (error) html += `<div class="op-error">${esc(error)}</div>`;

    if (loading && !hasRendered) {
      html += `<div class="op-body"><div class="op-loading"><span class="spinner"></span> Loading…</div></div>`;
    } else {
      const stCls = isRunning ? 'running' : isStarting ? 'starting' : 'stopped';
      const stIcon = isRunning ? '🟢' : isStarting ? '🔵' : '⭕';
      html += `<div class="op-body${loading ? ' op-loading-overlay' : ''}"><div class="op-detail">`;
      html += `<div class="op-state-indicator op-state-${stCls}">${stIcon} ${esc(w.state||'UNKNOWN')}</div>`;
      if (w.health_message) html += `<div class="op-muted" style="margin-bottom:12px">${esc(w.health_message)}</div>`;

      // Scaling visualization
      if (w.max_num_clusters && w.num_clusters != null) {
        const pct = Math.round((w.num_clusters / w.max_num_clusters) * 100);
        html += `<div class="op-section">Scaling</div>`;
        html += `<div class="op-scale-bar"><span class="op-muted">0</span><div class="op-scale-track"><div class="op-scale-fill" style="width:${pct}%"></div></div><span class="op-muted">${w.max_num_clusters}</span></div>`;
        html += `<div class="op-muted" style="text-align:center">${w.num_clusters} / ${w.max_num_clusters} clusters active</div>`;
      }

      html += `<div class="op-section">Configuration</div>`;
      html += `<dl class="op-kv">`;
      html += `<dt>Warehouse ID</dt><dd class="op-mono">${esc(w.id)}</dd>`;
      html += `<dt>Type</dt><dd>${esc(w.warehouse_type||'—')}${w.enable_serverless_compute?' <span class="op-badge op-badge-info">Serverless</span>':''}</dd>`;
      html += `<dt>Size</dt><dd>${esc(w.cluster_size||'—')}</dd>`;
      html += `<dt>Clusters (min/max)</dt><dd>${w.min_num_clusters||0} / ${w.max_num_clusters||'—'}</dd>`;
      html += `<dt>Active Clusters</dt><dd>${w.num_clusters != null ? w.num_clusters : '—'}</dd>`;
      html += `<dt>Active Sessions</dt><dd>${w.num_active_sessions != null ? w.num_active_sessions : '—'}</dd>`;
      html += `<dt>Auto-stop</dt><dd>${w.auto_stop_mins ? w.auto_stop_mins + ' min' : 'Disabled'}</dd>`;
      html += `<dt>Photon</dt><dd>${w.enable_photon ? '✓ Enabled' : '✗ Disabled'}</dd>`;
      html += `<dt>Health</dt><dd>${w.health_status ? esc(w.health_status) : '—'}</dd>`;
      html += `<dt>Creator</dt><dd>${esc(w.creator_name||'—')}</dd>`;
      html += `</dl>`;
      if (w.tags && Object.keys(w.tags).length) {
        html += `<div class="op-section">Tags</div>`;
        html += Object.entries(w.tags).map(([k,v])=>`<span class="op-tag">${esc(k)}=${esc(v)}</span>`).join(' ');
      }
      html += `</div></div>`;
    }

    html += `<div class="op-status-bar"><span>Last refresh: ${new Date().toLocaleTimeString()}</span><span>${esc(w.id||'')}</span></div>`;
    root.innerHTML = html;
    hasRendered = true;
    bindEvents();
  }

  function bindEvents() {
    root.querySelector("[data-action='refresh']")?.addEventListener("click", () => sendRequest({ action: "refresh" }));
    root.querySelector("[data-action='toggle-refresh']")?.addEventListener("click", () => { autoRefreshEnabled = !autoRefreshEnabled; if (autoRefreshEnabled) startAutoRefresh(); else stopAutoRefresh(); fullRender(); });
    root.querySelector("[data-action='start']")?.addEventListener("click", () => { confirmAction = { message: "Start this warehouse?", btnClass: "success", yesLabel: "▶ Start", action: "start" }; fullRender(); });
    root.querySelector("[data-action='stop']")?.addEventListener("click", () => { confirmAction = { message: "Stop this warehouse?", btnClass: "danger", yesLabel: "⏹ Stop", action: "stop" }; fullRender(); });
    root.querySelector("[data-action='confirm-yes']")?.addEventListener("click", () => { const a = confirmAction.action; confirmAction = null; sendRequest({ action: a }); });
    root.querySelector("[data-action='confirm-no']")?.addEventListener("click", () => { confirmAction = null; fullRender(); });
  }

  model.on("change:warehouse_data", fullRender);
  model.on("change:loading", fullRender);
  model.on("change:error_message", fullRender);
  model.on("change:action_result", () => {
    try { const r = JSON.parse(model.get("action_result")||"{}"); actionMessage = r.message; actionIsError = !r.success; fullRender(); if (r.success) setTimeout(() => { actionMessage = null; fullRender(); }, 5000); } catch(e) {}
  });

  fullRender();
  startAutoRefresh();
  return () => stopAutoRefresh();
}

export default { render };
