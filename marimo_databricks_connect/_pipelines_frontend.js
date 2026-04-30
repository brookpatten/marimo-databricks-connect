// Pipelines Browser — list of Lakeflow Declarative Pipelines (DLT)
// Communicates with Python via traits:
//   pipelines_data, pipeline_detail, updates_data, events_data, loading, error_message, request

const STYLES = `
  :host {
    --wf-bg: #ffffff; --wf-bg-alt: #f8f9fa; --wf-bg-hover: #e9ecef;
    --wf-border: #dee2e6; --wf-text: #212529; --wf-text-muted: #6c757d;
    --wf-primary: #0d6efd; --wf-success: #198754; --wf-danger: #dc3545;
    --wf-warning: #ffc107; --wf-info: #0dcaf0;
    --wf-font: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
    --wf-font-mono: "SF Mono", "Cascadia Code", "Fira Code", Menlo, Consolas, monospace;
    --wf-radius: 6px;
    display: block; font-family: var(--wf-font); font-size: 13px; color: var(--wf-text);
    background: var(--wf-bg); border: 1px solid var(--wf-border);
    border-radius: var(--wf-radius); overflow: hidden;
  }
  :host(.dark-theme) {
    --wf-bg: #1e1e1e; --wf-bg-alt: #252526; --wf-bg-hover: #2d2d30;
    --wf-border: #3e3e42; --wf-text: #cccccc; --wf-text-muted: #888888;
    --wf-primary: #4fc3f7; --wf-success: #66bb6a; --wf-danger: #ef5350;
    --wf-warning: #ffca28; --wf-info: #4dd0e1;
  }
  * { box-sizing: border-box; }

  .wf-header { display: flex; align-items: center; gap: 8px; padding: 10px 14px;
    background: var(--wf-bg-alt); border-bottom: 1px solid var(--wf-border); flex-wrap: wrap; }
  .wf-header h2 { margin: 0; font-size: 14px; font-weight: 600; white-space: nowrap; }
  .wf-breadcrumb { display: flex; align-items: center; gap: 4px; font-size: 12px; color: var(--wf-text-muted); flex-wrap: wrap; }
  .wf-breadcrumb button { background: none; border: none; color: var(--wf-primary); cursor: pointer; font-size: 12px; padding: 0; font-family: var(--wf-font); }
  .wf-breadcrumb button:hover { text-decoration: underline; }
  .wf-breadcrumb .sep { color: var(--wf-text-muted); margin: 0 2px; }
  .wf-search { margin-left: auto; display: flex; gap: 6px; align-items: center; }
  .wf-search input { padding: 4px 8px; border: 1px solid var(--wf-border); border-radius: 4px; font-size: 12px; background: var(--wf-bg); color: var(--wf-text); font-family: var(--wf-font); width: 200px; }
  .wf-search input:focus { outline: none; border-color: var(--wf-primary); }
  .wf-btn { padding: 4px 10px; border: 1px solid var(--wf-border); border-radius: 4px; background: var(--wf-bg); color: var(--wf-text); cursor: pointer; font-size: 12px; font-family: var(--wf-font); white-space: nowrap; }
  .wf-btn:hover { background: var(--wf-bg-hover); }

  .wf-body { max-height: 600px; overflow: auto; }
  .wf-loading { padding: 30px; text-align: center; color: var(--wf-text-muted); }
  .wf-loading .spinner { display: inline-block; width: 20px; height: 20px; border: 2px solid var(--wf-border); border-top-color: var(--wf-primary); border-radius: 50%; animation: wf-spin 0.6s linear infinite; margin-right: 8px; vertical-align: middle; }
  @keyframes wf-spin { to { transform: rotate(360deg); } }
  .wf-error { padding: 10px 14px; background: #fef2f2; color: var(--wf-danger); border-bottom: 1px solid #fecaca; font-size: 12px; }
  :host(.dark-theme) .wf-error { background: #3b1f1f; border-color: #5c2b2b; }
  .wf-empty { padding: 30px; text-align: center; color: var(--wf-text-muted); }

  table.wf-table { width: 100%; border-collapse: collapse; }
  .wf-table th { text-align: left; padding: 8px 14px; font-size: 11px; font-weight: 600;
    text-transform: uppercase; letter-spacing: 0.5px; color: var(--wf-text-muted);
    background: var(--wf-bg-alt); border-bottom: 1px solid var(--wf-border); position: sticky; top: 0; z-index: 1; }
  .wf-table td { padding: 7px 14px; border-bottom: 1px solid var(--wf-border); vertical-align: top; }
  .wf-table tr:hover td { background: var(--wf-bg-hover); }
  .wf-table tr.clickable { cursor: pointer; }
  .wf-link { color: var(--wf-primary); cursor: pointer; text-decoration: none; }
  .wf-link:hover { text-decoration: underline; }
  .wf-mono { font-family: var(--wf-font-mono); font-size: 12px; }
  .wf-muted { color: var(--wf-text-muted); font-size: 12px; }

  .wf-badge { display: inline-block; padding: 2px 7px; border-radius: 10px; font-size: 11px; font-weight: 500; white-space: nowrap; }
  .wf-badge-success { background: #d1fae5; color: #065f46; }
  .wf-badge-danger { background: #fee2e2; color: #991b1b; }
  .wf-badge-warning { background: #fef3c7; color: #92400e; }
  .wf-badge-info { background: #dbeafe; color: #1e40af; }
  .wf-badge-muted { background: #f3f4f6; color: #6b7280; }
  :host(.dark-theme) .wf-badge-success { background: #064e3b; color: #6ee7b7; }
  :host(.dark-theme) .wf-badge-danger { background: #7f1d1d; color: #fca5a5; }
  :host(.dark-theme) .wf-badge-warning { background: #78350f; color: #fcd34d; }
  :host(.dark-theme) .wf-badge-info { background: #1e3a5f; color: #93c5fd; }
  :host(.dark-theme) .wf-badge-muted { background: #374151; color: #9ca3af; }

  .wf-detail { padding: 14px; }
  .wf-detail-grid { display: grid; grid-template-columns: 160px 1fr; gap: 4px 12px; margin-bottom: 12px; font-size: 12px; }
  .wf-detail-grid dt { color: var(--wf-text-muted); font-weight: 500; padding: 3px 0; }
  .wf-detail-grid dd { margin: 0; padding: 3px 0; word-break: break-word; }
  .wf-section-title { font-size: 13px; font-weight: 600; margin: 16px 0 8px; padding-bottom: 4px; border-bottom: 1px solid var(--wf-border); }
  .wf-tag { display: inline-block; padding: 1px 6px; margin: 1px 3px 1px 0; border-radius: 3px; font-size: 11px; background: var(--wf-bg-alt); border: 1px solid var(--wf-border); font-family: var(--wf-font-mono); }

  .wf-tabs { display: flex; gap: 0; border-bottom: 2px solid var(--wf-border); margin-bottom: 12px; }
  .wf-tab { padding: 6px 16px; cursor: pointer; font-size: 12px; font-weight: 500; border: none; background: none; color: var(--wf-text-muted); border-bottom: 2px solid transparent; margin-bottom: -2px; font-family: var(--wf-font); }
  .wf-tab:hover { color: var(--wf-text); }
  .wf-tab.active { color: var(--wf-primary); border-bottom-color: var(--wf-primary); }

  .wf-pre { margin: 0; padding: 8px 10px; font-family: var(--wf-font-mono); font-size: 11px; line-height: 1.4; white-space: pre-wrap; word-break: break-word; background: var(--wf-bg-alt); border-radius: 4px; }
  .wf-filter-row { display: flex; gap: 6px; align-items: center; margin-bottom: 8px; }
  .wf-filter-row input { flex: 1; padding: 4px 8px; border: 1px solid var(--wf-border); border-radius: 4px; font-size: 12px; background: var(--wf-bg); color: var(--wf-text); font-family: var(--wf-font-mono); }

  .op-loading-overlay { position: relative; pointer-events: none; opacity: 0.6; }
  .op-loading-overlay::after { content: ''; position: absolute; top: 0; left: 0; right: 0; bottom: 0; background: var(--wf-bg); opacity: 0.5; z-index: 10; }
  .op-loading-overlay::before { content: ''; position: absolute; top: 50%; left: 50%; width: 20px; height: 20px; margin: -10px 0 0 -10px; border: 2px solid var(--wf-border); border-top-color: var(--wf-primary); border-radius: 50%; animation: wf-spin 0.6s linear infinite; z-index: 11; }
`;

function esc(s) { if (s == null) return ""; const d = document.createElement("div"); d.textContent = String(s); return d.innerHTML; }

function stateBadge(state) {
  if (!state) return `<span class="wf-badge wf-badge-muted">—</span>`;
  const s = String(state).toUpperCase();
  let cls = "muted";
  if (["IDLE", "COMPLETED", "HEALTHY", "SUCCESS"].includes(s)) cls = "success";
  else if (["FAILED", "CANCELED", "UNHEALTHY"].includes(s)) cls = "danger";
  else if (["RUNNING", "STARTING", "INITIALIZING", "QUEUED", "WAITING_FOR_RESOURCES",
            "SETTING_UP_TABLES", "DEPLOYING", "RECOVERING", "RESETTING", "STOPPING", "CREATED"].includes(s)) cls = "info";
  else if (["WARN"].includes(s)) cls = "warning";
  return `<span class="wf-badge wf-badge-${cls}">${s}</span>`;
}

function levelBadge(level) {
  if (!level) return "";
  const l = String(level).toUpperCase();
  let cls = "muted";
  if (l === "INFO") cls = "info";
  else if (l === "WARN") cls = "warning";
  else if (l === "ERROR") cls = "danger";
  return `<span class="wf-badge wf-badge-${cls}">${l}</span>`;
}

function _syncTheme(hostEl) {
  hostEl.__cleanupThemeSync?.();
  const media = window.matchMedia("(prefers-color-scheme: dark)");
  const themeSelector = "[data-app-theme], [data-theme], .dark, .dark-theme, .light, .light-theme";
  function parseTheme(value) { if (!value) return null; const v = String(value).toLowerCase(); if (v.includes("dark")) return true; if (v.includes("light")) return false; return null; }
  function bgDark(el) { if (!el) return null; const bg = getComputedStyle(el).backgroundColor; const m = bg && bg.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/i); if (!m) return null; const [, r, g, b] = m.map(Number); return ((0.2126*r + 0.7152*g + 0.0722*b)/255) < 0.5; }
  function fromEl(el) { if (!el) return null; return parseTheme(el.getAttribute?.("data-app-theme")) ?? parseTheme(el.getAttribute?.("data-theme")) ?? parseTheme(el.className) ?? parseTheme(getComputedStyle(el).colorScheme); }
  function isDark() { const a = hostEl.closest?.(themeSelector); return fromEl(a) ?? fromEl(hostEl) ?? fromEl(hostEl.parentElement) ?? fromEl(document.body) ?? fromEl(document.documentElement) ?? bgDark(hostEl.parentElement) ?? bgDark(document.body) ?? media.matches; }
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
// View renderers
// ===================================================================

function renderPipelinesList(root, pipelines, model) {
  if (!pipelines.length) {
    root.querySelector(".wf-body").innerHTML = `<div class="wf-empty">No pipelines found.</div>`;
    return;
  }
  let html = `<table class="wf-table"><thead><tr>
    <th>Name</th><th>State</th><th>Health</th><th>Last update</th><th>When</th><th>Creator</th>
  </tr></thead><tbody>`;
  for (const p of pipelines) {
    html += `<tr class="clickable" data-pipeline-id="${esc(p.pipeline_id)}">
      <td><span class="wf-link">${esc(p.name || "(unnamed)")}</span><br><span class="wf-mono wf-muted">${esc(p.pipeline_id)}</span></td>
      <td>${stateBadge(p.state)}</td>
      <td>${stateBadge(p.health)}</td>
      <td>${stateBadge(p.last_update_state)}</td>
      <td class="wf-muted">${esc(p.last_update_time || "—")}</td>
      <td class="wf-muted">${esc(p.creator || "—")}</td>
    </tr>`;
  }
  html += `</tbody></table>`;
  root.querySelector(".wf-body").innerHTML = html;

  root.querySelectorAll("tr[data-pipeline-id]").forEach((tr) => {
    tr.addEventListener("click", () => {
      const pid = tr.dataset.pipelineId;
      model.set("request", JSON.stringify({ action: "get_pipeline", pipeline_id: pid, _t: Date.now() }));
      model.save_changes();
      setTimeout(() => {
        model.set("request", JSON.stringify({ action: "list_updates", pipeline_id: pid, _t: Date.now() }));
        model.save_changes();
      }, 50);
    });
  });
}

function renderPipelineDetail(root, detail, updates, events, model, state) {
  const body = root.querySelector(".wf-body");
  const spec = detail.spec || {};
  const activeTab = state.activeTab || "overview";

  function renderOverview() {
    let html = `<dl class="wf-detail-grid">`;
    html += `<dt>Pipeline ID</dt><dd class="wf-mono">${esc(detail.pipeline_id)}</dd>`;
    html += `<dt>State</dt><dd>${stateBadge(detail.state)}</dd>`;
    if (detail.health) html += `<dt>Health</dt><dd>${stateBadge(detail.health)}</dd>`;
    html += `<dt>Mode</dt><dd>${spec.continuous ? '<span class="wf-badge wf-badge-info">Continuous</span>' : '<span class="wf-badge wf-badge-muted">Triggered</span>'}${spec.development ? ' <span class="wf-badge wf-badge-warning">Development</span>' : ' <span class="wf-badge wf-badge-success">Production</span>'}</dd>`;
    html += `<dt>Compute</dt><dd>${spec.serverless ? '<span class="wf-badge wf-badge-info">Serverless</span>' : '<span class="wf-badge wf-badge-muted">Classic</span>'}${spec.photon ? ' <span class="wf-badge wf-badge-info">Photon</span>' : ''}</dd>`;
    if (spec.channel) html += `<dt>Channel</dt><dd>${esc(spec.channel)}</dd>`;
    if (spec.edition) html += `<dt>Edition</dt><dd>${esc(spec.edition)}</dd>`;
    if (spec.catalog) html += `<dt>Catalog</dt><dd class="wf-mono">${esc(spec.catalog)}</dd>`;
    if (spec.schema) html += `<dt>Schema</dt><dd class="wf-mono">${esc(spec.schema)}</dd>`;
    if (detail.cluster_id) html += `<dt>Cluster</dt><dd class="wf-mono">${esc(detail.cluster_id)}</dd>`;
    html += `<dt>Creator</dt><dd>${esc(detail.creator || "—")}</dd>`;
    if (detail.run_as) html += `<dt>Run as</dt><dd>${esc(detail.run_as)}</dd>`;
    if (detail.last_modified) html += `<dt>Last modified</dt><dd>${esc(detail.last_modified)}</dd>`;
    html += `</dl>`;

    if (detail.latest_updates && detail.latest_updates.length) {
      html += `<div class="wf-section-title">Latest updates</div>`;
      html += `<table class="wf-table"><thead><tr><th>Update ID</th><th>State</th><th>Created</th></tr></thead><tbody>`;
      for (const u of detail.latest_updates.slice(0, 5)) {
        html += `<tr><td class="wf-mono">${esc(u.update_id)}</td><td>${stateBadge(u.state)}</td><td class="wf-muted">${esc(u.creation_time || "—")}</td></tr>`;
      }
      html += `</tbody></table>`;
    }

    html += `<div class="wf-section-title">Tip</div>`;
    html += `<div class="wf-muted">For start/stop and full-refresh actions, place this pipeline in its own cell with <code class="wf-mono">pipeline_widget("${esc(detail.pipeline_id)}")</code>.</div>`;
    return html;
  }

  function renderUpdates() {
    if (!updates || !updates.length) return `<div class="wf-empty">No updates yet.</div>`;
    let html = `<table class="wf-table"><thead><tr>
      <th>Update ID</th><th>State</th><th>Cause</th><th>Created</th><th>Flags</th>
    </tr></thead><tbody>`;
    for (const u of updates) {
      const flags = [];
      if (u.full_refresh) flags.push('<span class="wf-badge wf-badge-warning">FULL REFRESH</span>');
      if (u.validate_only) flags.push('<span class="wf-badge wf-badge-info">VALIDATE</span>');
      if (u.full_refresh_selection?.length) flags.push(`<span class="wf-badge wf-badge-warning">refresh: ${u.full_refresh_selection.length}</span>`);
      if (u.refresh_selection?.length) flags.push(`<span class="wf-badge wf-badge-info">selective: ${u.refresh_selection.length}</span>`);
      html += `<tr>
        <td class="wf-mono">${esc(u.update_id)}</td>
        <td>${stateBadge(u.state)}</td>
        <td class="wf-muted">${esc(u.cause || "—")}</td>
        <td class="wf-muted">${esc(u.creation_time || "—")}</td>
        <td>${flags.join(" ") || '<span class="wf-muted">—</span>'}</td>
      </tr>`;
    }
    html += `</tbody></table>`;
    return html;
  }

  function renderEvents() {
    let html = `<div class="wf-filter-row">
      <input type="text" placeholder="Filter (e.g. level='ERROR')" value="${esc(state.eventFilter || "")}" data-action="event-filter-input"/>
      <button class="wf-btn" data-action="event-filter-apply">Apply</button>
      <button class="wf-btn" data-action="event-filter-reload">↻ Reload</button>
    </div>`;
    if (!events || !events.length) {
      html += `<div class="wf-empty">${state.eventsLoaded ? "No events match." : "Loading events…"}</div>`;
      return html;
    }
    html += `<table class="wf-table"><thead><tr>
      <th>Time</th><th>Level</th><th>Type</th><th>Update / Flow</th><th>Message</th>
    </tr></thead><tbody>`;
    for (const e of events) {
      const ctx = [];
      if (e.update_id) ctx.push(`<span class="wf-tag">u:${esc(String(e.update_id).slice(0, 8))}</span>`);
      if (e.flow_name) ctx.push(`<span class="wf-tag">flow:${esc(e.flow_name)}</span>`);
      if (e.dataset_name) ctx.push(`<span class="wf-tag">ds:${esc(e.dataset_name)}</span>`);
      const msgHtml = `${esc(e.message || "")}${e.error ? `<pre class="wf-pre" style="margin-top:6px;color:var(--wf-danger)">${esc(e.error)}</pre>` : ""}`;
      html += `<tr>
        <td class="wf-mono wf-muted" style="white-space:nowrap">${esc(e.timestamp || "—")}</td>
        <td>${levelBadge(e.level)}</td>
        <td class="wf-muted">${esc(e.event_type || "")}</td>
        <td>${ctx.join(" ") || '<span class="wf-muted">—</span>'}</td>
        <td>${msgHtml}</td>
      </tr>`;
    }
    html += `</tbody></table>`;
    return html;
  }

  function renderSpec() {
    let html = `<dl class="wf-detail-grid">`;
    if (spec.storage) html += `<dt>Storage</dt><dd class="wf-mono">${esc(spec.storage)}</dd>`;
    if (spec.root_path) html += `<dt>Root path</dt><dd class="wf-mono">${esc(spec.root_path)}</dd>`;
    if (spec.budget_policy_id) html += `<dt>Budget policy</dt><dd class="wf-mono">${esc(spec.budget_policy_id)}</dd>`;
    if (detail.effective_publishing_mode) html += `<dt>Publishing mode</dt><dd>${esc(detail.effective_publishing_mode)}</dd>`;
    html += `</dl>`;
    if (spec.libraries && spec.libraries.length) {
      html += `<div class="wf-section-title">Libraries (${spec.libraries.length})</div>`;
      html += `<table class="wf-table"><thead><tr><th>Type</th><th>Value</th></tr></thead><tbody>`;
      for (const lib of spec.libraries) {
        html += `<tr><td>${esc(lib.type)}</td><td class="wf-mono">${esc(lib.value || "—")}</td></tr>`;
      }
      html += `</tbody></table>`;
    }
    if (spec.configuration && Object.keys(spec.configuration).length) {
      html += `<div class="wf-section-title">Configuration (${Object.keys(spec.configuration).length})</div>`;
      html += `<table class="wf-table"><thead><tr><th>Key</th><th>Value</th></tr></thead><tbody>`;
      for (const [k, v] of Object.entries(spec.configuration)) {
        html += `<tr><td class="wf-mono">${esc(k)}</td><td class="wf-mono">${esc(v)}</td></tr>`;
      }
      html += `</tbody></table>`;
    }
    if (spec.tags && Object.keys(spec.tags).length) {
      html += `<div class="wf-section-title">Tags</div>`;
      html += Object.entries(spec.tags).map(([k, v]) => `<span class="wf-tag">${esc(k)}=${esc(v)}</span>`).join(" ");
    }
    return html;
  }

  let html = `<div class="wf-detail">`;
  html += `<div class="wf-tabs">
    <button class="wf-tab ${activeTab === "overview" ? "active" : ""}" data-tab="overview">Overview</button>
    <button class="wf-tab ${activeTab === "updates" ? "active" : ""}" data-tab="updates">Updates (${(updates || []).length})</button>
    <button class="wf-tab ${activeTab === "events" ? "active" : ""}" data-tab="events">Events</button>
    <button class="wf-tab ${activeTab === "spec" ? "active" : ""}" data-tab="spec">Spec</button>
  </div>`;
  if (activeTab === "overview") html += renderOverview();
  else if (activeTab === "updates") html += renderUpdates();
  else if (activeTab === "events") html += renderEvents();
  else if (activeTab === "spec") html += renderSpec();
  html += `</div>`;
  body.innerHTML = html;

  body.querySelectorAll(".wf-tab").forEach((tab) => {
    tab.addEventListener("click", () => {
      state.activeTab = tab.dataset.tab;
      if (state.activeTab === "events" && !state.eventsLoaded) {
        state.eventsLoaded = true;
        model.set("request", JSON.stringify({ action: "list_events", pipeline_id: detail.pipeline_id, filter: state.eventFilter || null, _t: Date.now() }));
        model.save_changes();
      }
      renderPipelineDetail(root, detail, updates, events, model, state);
    });
  });

  const filterInput = body.querySelector("[data-action='event-filter-input']");
  if (filterInput) {
    filterInput.addEventListener("input", (e) => { state.eventFilter = e.target.value; });
    filterInput.addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        model.set("request", JSON.stringify({ action: "list_events", pipeline_id: detail.pipeline_id, filter: state.eventFilter || null, _t: Date.now() }));
        model.save_changes();
      }
    });
  }
  body.querySelector("[data-action='event-filter-apply']")?.addEventListener("click", () => {
    model.set("request", JSON.stringify({ action: "list_events", pipeline_id: detail.pipeline_id, filter: state.eventFilter || null, _t: Date.now() }));
    model.save_changes();
  });
  body.querySelector("[data-action='event-filter-reload']")?.addEventListener("click", () => {
    model.set("request", JSON.stringify({ action: "list_events", pipeline_id: detail.pipeline_id, filter: state.eventFilter || null, _t: Date.now() }));
    model.save_changes();
  });
}

// ===================================================================
// State machine
// ===================================================================

const VIEW = { LIST: "list", DETAIL: "detail" };

function render({ model, el }) {
  const shadow = el.attachShadow ? el.attachShadow({ mode: "open" }) : el;
  _syncTheme(el);
  const styleEl = document.createElement("style"); styleEl.textContent = STYLES; shadow.appendChild(styleEl);
  const root = document.createElement("div"); shadow.appendChild(root);

  let currentView = VIEW.LIST;
  let currentPipelineId = null;
  let currentPipelineName = "";
  let searchValue = "";
  let hasRendered = false;
  const detailState = { activeTab: "overview", eventFilter: "", eventsLoaded: false };

  function buildHeader() {
    let html = `<div class="wf-header"><h2>🪈 Pipelines</h2><div class="wf-breadcrumb">`;
    if (currentView === VIEW.LIST) html += `<span>All Pipelines</span>`;
    else html += `<button data-nav="list">All Pipelines</button><span class="sep">›</span><span>${esc(currentPipelineName || currentPipelineId)}</span>`;
    html += `</div>`;
    if (currentView === VIEW.LIST) {
      html += `<div class="wf-search">
        <input type="text" placeholder="Filter by name…" value="${esc(searchValue)}" />
        <button class="wf-btn" data-action="search">Search</button>
        <span class="wf-muted" style="font-size:11px">${autoRefreshEnabled ? "● Auto" : "Paused"}</span>
        <button class="wf-btn" data-action="toggle-auto" title="${autoRefreshEnabled ? "Pause auto-refresh" : "Resume auto-refresh"}">${autoRefreshEnabled ? "⏸" : "▶"}</button>
        <button class="wf-btn" data-action="refresh">↻</button>
      </div>`;
    } else {
      html += `<div class="wf-search"><button class="wf-btn" data-action="refresh">↻ Refresh</button></div>`;
    }
    html += `</div>`;
    return html;
  }

  function fullRender() {
    root.innerHTML = buildHeader() + `<div class="wf-body"></div>`;

    const err = model.get("error_message");
    if (err) {
      const errDiv = document.createElement("div");
      errDiv.className = "wf-error";
      errDiv.textContent = err;
      root.querySelector(".wf-body").before(errDiv);
    }

    if (model.get("loading") && !hasRendered) {
      root.querySelector(".wf-body").innerHTML = `<div class="wf-loading"><span class="spinner"></span> Loading…</div>`;
      bindHeaderEvents();
      return;
    }
    if (model.get("loading")) {
      root.querySelector(".wf-body").classList.add("op-loading-overlay");
    }

    if (currentView === VIEW.LIST) {
      const ps = JSON.parse(model.get("pipelines_data") || "[]");
      renderPipelinesList(root, ps, model);
    } else {
      const detail = JSON.parse(model.get("pipeline_detail") || "{}");
      const updates = JSON.parse(model.get("updates_data") || "[]");
      const events = JSON.parse(model.get("events_data") || "[]");
      if (detail.name) currentPipelineName = detail.name;
      renderPipelineDetail(root, detail, updates, events, model, detailState);
    }

    bindHeaderEvents();
    hasRendered = true;
  }

  function bindHeaderEvents() {
    root.querySelectorAll("[data-nav]").forEach((btn) => {
      btn.addEventListener("click", () => {
        if (btn.dataset.nav === "list") {
          currentView = VIEW.LIST; currentPipelineId = null;
          detailState.activeTab = "overview"; detailState.eventsLoaded = false;
          fullRender();
        }
      });
    });

    const searchInput = root.querySelector(".wf-search input");
    if (searchInput) {
      const doSearch = () => {
        searchValue = searchInput.value.trim();
        model.set("request", JSON.stringify({ action: "list_pipelines", name_filter: searchValue || null, _t: Date.now() }));
        model.save_changes();
      };
      searchInput.addEventListener("keydown", (e) => { if (e.key === "Enter") doSearch(); });
      root.querySelector("[data-action='search']")?.addEventListener("click", doSearch);
    }
    root.querySelector("[data-action='refresh']")?.addEventListener("click", () => {
      if (currentView === VIEW.LIST) {
        model.set("request", JSON.stringify({ action: "list_pipelines", name_filter: searchValue || null, _t: Date.now() }));
        model.save_changes();
      } else if (currentPipelineId) {
        model.set("request", JSON.stringify({ action: "get_pipeline", pipeline_id: currentPipelineId, _t: Date.now() }));
        model.save_changes();
        setTimeout(() => {
          model.set("request", JSON.stringify({ action: "list_updates", pipeline_id: currentPipelineId, _t: Date.now() }));
          model.save_changes();
          if (detailState.activeTab === "events") {
            setTimeout(() => {
              model.set("request", JSON.stringify({ action: "list_events", pipeline_id: currentPipelineId, filter: detailState.eventFilter || null, _t: Date.now() }));
              model.save_changes();
            }, 50);
          }
        }, 50);
      }
    });
  }

  model.on("change:pipeline_detail", () => {
    const d = JSON.parse(model.get("pipeline_detail") || "{}");
    if (d.pipeline_id) {
      currentView = VIEW.DETAIL; currentPipelineId = d.pipeline_id; currentPipelineName = d.name || "";
      detailState.activeTab = "overview"; detailState.eventsLoaded = false;
      fullRender();
    }
  });
  model.on("change:pipelines_data", () => { if (currentView === VIEW.LIST) fullRender(); });
  model.on("change:updates_data", () => { if (currentView === VIEW.DETAIL) fullRender(); });
  model.on("change:events_data", () => { if (currentView === VIEW.DETAIL && detailState.activeTab === "events") fullRender(); });
  model.on("change:loading", fullRender);
  model.on("change:error_message", fullRender);

  fullRender();
}

export default { render };
