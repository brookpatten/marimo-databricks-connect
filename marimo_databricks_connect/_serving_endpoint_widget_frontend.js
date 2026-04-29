// Serving Endpoint Widget Frontend
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
  .op-btn-primary { background: var(--op-primary); color: #fff; border-color: var(--op-primary); }
  .op-btn-danger { background: var(--op-danger); color: #fff; border-color: var(--op-danger); }
  .op-body { max-height: 600px; overflow: auto; }
  .op-loading { padding: 30px; text-align: center; color: var(--op-text-muted); }
  .op-loading .spinner { display: inline-block; width: 20px; height: 20px; border: 2px solid var(--op-border); border-top-color: var(--op-primary); border-radius: 50%; animation: op-spin 0.6s linear infinite; margin-right: 8px; vertical-align: middle; }
  @keyframes op-spin { to { transform: rotate(360deg); } }
  .op-error { padding: 10px 14px; background: #fef2f2; color: var(--op-danger); border-bottom: 1px solid #fecaca; font-size: 12px; }
  :host(.dark-theme) .op-error { background: #3b1f1f; border-color: #5c2b2b; }
  .op-detail { padding: 14px; }
  .op-kv { display: grid; grid-template-columns: 160px 1fr; gap: 4px 12px; margin-bottom: 16px; font-size: 12px; }
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
  :host(.dark-theme) .op-badge-success { background: #064e3b; color: #6ee7b7; } :host(.dark-theme) .op-badge-danger { background: #7f1d1d; color: #fca5a5; } :host(.dark-theme) .op-badge-info { background: #1e3a5f; color: #93c5fd; } :host(.dark-theme) .op-badge-warning { background: #78350f; color: #fcd34d; } :host(.dark-theme) .op-badge-muted { background: #374151; color: #9ca3af; }
  .op-tabs { display: flex; gap: 0; border-bottom: 2px solid var(--op-border); margin-bottom: 12px; }
  .op-tab { padding: 6px 16px; cursor: pointer; font-size: 12px; font-weight: 500; border: none; background: none; color: var(--op-text-muted); border-bottom: 2px solid transparent; margin-bottom: -2px; font-family: var(--op-font); }
  .op-tab:hover { color: var(--op-text); }
  .op-tab.active { color: var(--op-primary); border-bottom-color: var(--op-primary); }
  .op-mono { font-family: var(--op-font-mono); font-size: 12px; }
  .op-muted { color: var(--op-text-muted); font-size: 12px; }
  .op-auto-refresh { display: flex; align-items: center; gap: 4px; font-size: 11px; color: var(--op-text-muted); }
  .op-auto-refresh .dot { width: 6px; height: 6px; border-radius: 50%; background: var(--op-success); animation: op-pulse 2s ease-in-out infinite; }
  @keyframes op-pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.3; } }
  .op-status-bar { padding: 6px 14px; font-size: 11px; color: var(--op-text-muted); background: var(--op-bg-alt); border-top: 1px solid var(--op-border); display: flex; justify-content: space-between; }
  .op-state-indicator { display: inline-flex; align-items: center; gap: 8px; padding: 10px 16px; border-radius: 8px; margin-bottom: 16px; font-size: 14px; font-weight: 600; }
  .op-state-ready { background: #d1fae5; color: #065f46; }
  .op-state-pending { background: #dbeafe; color: #1e40af; }
  .op-state-notready { background: #fee2e2; color: #991b1b; }
  :host(.dark-theme) .op-state-ready { background: #064e3b; color: #6ee7b7; } :host(.dark-theme) .op-state-pending { background: #1e3a5f; color: #93c5fd; } :host(.dark-theme) .op-state-notready { background: #7f1d1d; color: #fca5a5; }
  /* Query area */
  .op-query-area { margin-top: 12px; }
  .op-query-area textarea { width: 100%; min-height: 100px; padding: 10px; font-family: var(--op-font-mono); font-size: 12px; border: 1px solid var(--op-border); border-radius: 4px; background: var(--op-bg); color: var(--op-text); resize: vertical; }
  .op-query-area textarea:focus { outline: none; border-color: var(--op-primary); }
  .op-query-result { margin-top: 12px; padding: 10px; border: 1px solid var(--op-border); border-radius: 4px; background: var(--op-bg-alt); font-family: var(--op-font-mono); font-size: 12px; max-height: 300px; overflow: auto; white-space: pre-wrap; word-break: break-all; }
  .op-query-result.error { color: var(--op-danger); }
  .op-empty { padding: 30px; text-align: center; color: var(--op-text-muted); }
  /* Traffic bar */
  .op-traffic-bar { display: flex; height: 24px; border-radius: 4px; overflow: hidden; margin: 8px 0; border: 1px solid var(--op-border); }
  .op-traffic-segment { display: flex; align-items: center; justify-content: center; font-size: 10px; font-weight: 600; color: #fff; }

  .op-loading-overlay { position: relative; pointer-events: none; opacity: 0.6; }
  .op-loading-overlay::after { content: ''; position: absolute; top: 0; left: 0; right: 0; bottom: 0; background: var(--op-bg); opacity: 0.5; z-index: 10; }
  .op-loading-overlay::before { content: ''; position: absolute; top: 50%; left: 50%; width: 20px; height: 20px; margin: -10px 0 0 -10px; border: 2px solid var(--op-border); border-top-color: var(--op-primary); border-radius: 50%; animation: op-spin 0.6s linear infinite; z-index: 11; }
`;

function esc(s) { if (s == null) return ""; const d = document.createElement("div"); d.textContent = String(s); return d.innerHTML; }

const COLORS = ['#0d6efd','#198754','#fd7e14','#6f42c1','#d63384','#0dcaf0','#ffc107'];


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

  let currentTab = "overview";
  let autoRefreshEnabled = true, autoTimer = null;
  let hasRendered = false;
  let queryPayload = '{\n  "dataframe_records": [\n    {"feature1": 1.0, "feature2": 2.0}\n  ]\n}';

  function getEP() { return JSON.parse(model.get("endpoint_data") || "{}"); }
  function sendRequest(req) { model.set("request", JSON.stringify({ ...req, _t: Date.now() })); model.save_changes(); }
  function startAutoRefresh() { stopAutoRefresh(); const ep = getEP(); if (autoRefreshEnabled) autoTimer = setInterval(() => sendRequest({ action: "refresh" }), (ep.refresh_seconds||30)*1000); }
  function stopAutoRefresh() { if (autoTimer) { clearInterval(autoTimer); autoTimer = null; } }

  function stateClass(s) { if (!s) return 'notready'; const u = (s+'').toUpperCase(); if (u === 'READY') return 'ready'; if (u.includes('NOT_READY')) return 'notready'; return 'pending'; }
  function stateIcon(s) { const u = (s+'').toUpperCase(); if (u === 'READY') return '🟢'; if (u.includes('NOT_READY')) return '🔴'; return '🔵'; }

  function fullRender() {
    const ep = getEP();
    const loading = model.get("loading");
    const error = model.get("error_message");

    let html = `<div class="op-header"><h2>🤖 ${esc(ep.name || "Serving Endpoint")}</h2>`;
    html += `<div class="op-header-actions">`;
    html += `<div class="op-auto-refresh">${autoRefreshEnabled ? '<span class="dot"></span> Auto' : 'Paused'}</div>`;
    html += `<button class="op-btn" data-action="toggle-refresh">${autoRefreshEnabled ? '⏸' : '▶'}</button>`;
    html += `<button class="op-btn" data-action="refresh">↻</button>`;
    html += `</div></div>`;

    if (error) html += `<div class="op-error">${esc(error)}</div>`;

    if (loading && !hasRendered) {
      html += `<div class="op-body"><div class="op-loading"><span class="spinner"></span> Loading…</div></div>`;
    } else {
      html += `<div class="op-body${loading ? ' op-loading-overlay' : ''}"><div class="op-detail">`;

      html += `<div class="op-state-indicator op-state-${stateClass(ep.state)}">${stateIcon(ep.state)} ${esc(ep.state||'UNKNOWN')}</div>`;
      if (ep.config_update) html += `<div class="op-muted" style="margin-bottom:12px">Config update: ${esc(ep.config_update)}</div>`;

      html += `<div class="op-tabs">`;
      html += `<button class="op-tab${currentTab==='overview'?' active':''}" data-tab="overview">Overview</button>`;
      html += `<button class="op-tab${currentTab==='query'?' active':''}" data-tab="query">Query</button>`;
      html += `</div>`;

      // Overview
      html += `<div class="op-tab-content" data-tab="overview" style="${currentTab!=='overview'?'display:none':''}">`;
      html += `<dl class="op-kv">`;
      html += `<dt>Endpoint Name</dt><dd class="op-mono">${esc(ep.name)}</dd>`;
      if (ep.id) html += `<dt>ID</dt><dd class="op-mono">${esc(ep.id)}</dd>`;
      html += `<dt>Creator</dt><dd>${esc(ep.creator||'—')}</dd>`;
      html += `<dt>Created</dt><dd>${esc(ep.creation_timestamp||'—')}</dd>`;
      html += `<dt>Last Updated</dt><dd>${esc(ep.last_updated_timestamp||'—')}</dd>`;
      if (ep.permission_level) html += `<dt>Permission</dt><dd>${esc(ep.permission_level)}</dd>`;
      html += `</dl>`;

      // Served entities
      if (ep.served_entities && ep.served_entities.length) {
        html += `<div class="op-section">Served Entities</div>`;
        html += `<table class="op-table"><thead><tr><th>Name</th><th>Entity</th><th>Version</th><th>Size</th><th>Scale to 0</th><th>State</th></tr></thead><tbody>`;
        for (const se of ep.served_entities) {
          const stBadge = se.state ? `<span class="op-badge op-badge-${se.state==='DEPLOYMENT_READY'?'success':'info'}">${esc(se.state)}</span>` : '';
          html += `<tr><td class="op-mono">${esc(se.name)}</td><td class="op-mono">${esc(se.entity_name)}</td><td>${esc(se.entity_version||'—')}</td><td>${esc(se.workload_size||'—')}</td><td>${se.scale_to_zero_enabled ? '✓' : '✗'}</td><td>${stBadge}</td></tr>`;
        }
        html += `</tbody></table>`;
      }

      // Traffic
      if (ep.traffic_config && ep.traffic_config.routes && ep.traffic_config.routes.length) {
        html += `<div class="op-section">Traffic Split</div>`;
        html += `<div class="op-traffic-bar">`;
        ep.traffic_config.routes.forEach((r, i) => {
          const color = COLORS[i % COLORS.length];
          html += `<div class="op-traffic-segment" style="width:${r.traffic_percentage}%;background:${color}" title="${esc(r.served_model_name)}: ${r.traffic_percentage}%">${r.traffic_percentage}%</div>`;
        });
        html += `</div>`;
        for (const [i, r] of ep.traffic_config.routes.entries()) {
          html += `<div style="display:inline-flex;align-items:center;gap:6px;margin-right:12px;font-size:12px"><span style="width:10px;height:10px;border-radius:2px;background:${COLORS[i%COLORS.length]};display:inline-block"></span> ${esc(r.served_model_name)} (${r.traffic_percentage}%)</div>`;
        }
      }

      // Pending config
      if (ep.pending_config) {
        html += `<div class="op-section">Pending Config Update</div>`;
        html += `<div class="op-muted">`;
        for (const pe of ep.pending_config.entities || []) {
          html += `<div class="op-mono">${esc(pe.entity_name)} v${esc(pe.entity_version)}</div>`;
        }
        html += `</div>`;
      }
      html += `</div>`;

      // Query tab
      html += `<div class="op-tab-content" data-tab="query" style="${currentTab!=='query'?'display:none':''}">`;
      html += `<div class="op-query-area">`;
      html += `<div style="margin-bottom:8px;font-size:12px;color:var(--op-text-muted)">JSON payload to send to the endpoint:</div>`;
      html += `<textarea data-query-input>${esc(queryPayload)}</textarea>`;
      html += `<div style="margin-top:8px"><button class="op-btn op-btn-primary" data-action="send-query">▶ Send Query</button></div>`;
      html += `</div>`;
      const qr = JSON.parse(model.get("query_result") || "{}");
      if (qr.success === true) {
        html += `<div class="op-query-result">${esc(JSON.stringify(qr.data, null, 2))}</div>`;
      } else if (qr.success === false) {
        html += `<div class="op-query-result error">${esc(qr.error)}</div>`;
      }
      html += `</div>`;

      html += `</div></div>`;
    }

    html += `<div class="op-status-bar"><span>Last refresh: ${new Date().toLocaleTimeString()}</span><span>${esc(ep.name||'')}</span></div>`;
    root.innerHTML = html;
    hasRendered = true;
    bindEvents();
  }

  function bindEvents() {
    root.querySelectorAll(".op-tab").forEach(tab => tab.addEventListener("click", () => { currentTab = tab.dataset.tab; fullRender(); }));
    root.querySelector("[data-action='refresh']")?.addEventListener("click", () => sendRequest({ action: "refresh" }));
    root.querySelector("[data-action='toggle-refresh']")?.addEventListener("click", () => { autoRefreshEnabled = !autoRefreshEnabled; if (autoRefreshEnabled) startAutoRefresh(); else stopAutoRefresh(); fullRender(); });
    const textarea = root.querySelector("[data-query-input]");
    if (textarea) textarea.addEventListener("input", () => { queryPayload = textarea.value; });
    root.querySelector("[data-action='send-query']")?.addEventListener("click", () => {
      const ta = root.querySelector("[data-query-input]");
      if (ta) queryPayload = ta.value;
      sendRequest({ action: "query", payload: queryPayload });
    });
  }

  model.on("change:endpoint_data", fullRender);
  model.on("change:query_result", fullRender);
  model.on("change:loading", fullRender);
  model.on("change:error_message", fullRender);

  fullRender();
  startAutoRefresh();
  return () => stopAutoRefresh();
}

export default { render };
