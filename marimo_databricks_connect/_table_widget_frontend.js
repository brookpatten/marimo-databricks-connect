// Table Widget Frontend — single-table operational dashboard

const OPS_STYLES = `
  :host {
    --op-bg: #ffffff; --op-bg-alt: #f8f9fa; --op-bg-hover: #e9ecef;
    --op-border: #dee2e6; --op-text: #212529; --op-text-muted: #6c757d;
    --op-primary: #0d6efd; --op-success: #198754; --op-danger: #dc3545;
    --op-warning: #ffc107; --op-info: #0dcaf0;
    --op-font: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
    --op-font-mono: "SF Mono", "Cascadia Code", "Fira Code", Menlo, Consolas, monospace;
    --op-radius: 6px;
    display: block; font-family: var(--op-font); font-size: 13px;
    color: var(--op-text); background: var(--op-bg);
    border: 1px solid var(--op-border); border-radius: var(--op-radius); overflow: hidden;
  }
  :host(.dark-theme) { --op-bg: #1e1e1e; --op-bg-alt: #252526; --op-bg-hover: #2d2d30; --op-border: #3e3e42; --op-text: #cccccc; --op-text-muted: #888888; --op-primary: #4fc3f7; --op-success: #66bb6a; --op-danger: #ef5350; --op-warning: #ffca28; --op-info: #4dd0e1; }
  * { box-sizing: border-box; }
  .op-header { display: flex; align-items: center; gap: 8px; padding: 10px 14px; background: var(--op-bg-alt); border-bottom: 1px solid var(--op-border); flex-wrap: wrap; }
  .op-header h2 { margin: 0; font-size: 14px; font-weight: 600; }
  .op-header .op-subtitle { color: var(--op-text-muted); font-size: 11px; font-family: var(--op-font-mono); }
  .op-header-actions { margin-left: auto; display: flex; gap: 6px; }
  .op-btn { padding: 4px 10px; border: 1px solid var(--op-border); border-radius: 4px; background: var(--op-bg); color: var(--op-text); cursor: pointer; font-size: 12px; font-family: var(--op-font); white-space: nowrap; }
  .op-btn:hover { background: var(--op-bg-hover); }
  .op-btn-primary { background: var(--op-primary); color: #fff; border-color: var(--op-primary); }
  .op-body { max-height: 600px; overflow: auto; }
  .op-loading { padding: 30px; text-align: center; color: var(--op-text-muted); }
  .op-loading .spinner { display: inline-block; width: 20px; height: 20px; border: 2px solid var(--op-border); border-top-color: var(--op-primary); border-radius: 50%; animation: op-spin 0.6s linear infinite; margin-right: 8px; vertical-align: middle; }
  @keyframes op-spin { to { transform: rotate(360deg); } }
  .op-error { padding: 10px 14px; background: #fef2f2; color: var(--op-danger); border-bottom: 1px solid #fecaca; font-size: 12px; }
  :host(.dark-theme) .op-error { background: #3b1f1f; border-color: #5c2b2b; }
  .op-detail { padding: 14px; }
  .op-kv { display: grid; grid-template-columns: 150px 1fr; gap: 4px 12px; margin-bottom: 16px; font-size: 12px; }
  .op-kv dt { color: var(--op-text-muted); font-weight: 500; padding: 3px 0; }
  .op-kv dd { margin: 0; padding: 3px 0; word-break: break-all; }
  .op-section { font-size: 13px; font-weight: 600; margin: 16px 0 8px; padding-bottom: 4px; border-bottom: 1px solid var(--op-border); }
  table.op-table { width: 100%; border-collapse: collapse; }
  .op-table th { text-align: left; padding: 8px 14px; font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; color: var(--op-text-muted); background: var(--op-bg-alt); border-bottom: 1px solid var(--op-border); position: sticky; top: 0; z-index: 1; }
  .op-table td { padding: 7px 14px; border-bottom: 1px solid var(--op-border); vertical-align: top; }
  .op-table tr:hover td { background: var(--op-bg-hover); }
  .op-badge { display: inline-block; padding: 2px 7px; border-radius: 10px; font-size: 11px; font-weight: 500; }
  .op-badge-info { background: #dbeafe; color: #1e40af; }
  .op-badge-muted { background: #f3f4f6; color: #6b7280; }
  .op-badge-success { background: #d1fae5; color: #065f46; }
  .op-badge-warning { background: #fef3c7; color: #92400e; }
  :host(.dark-theme) .op-badge-info { background: #1e3a5f; color: #93c5fd; }
    :host(.dark-theme) .op-badge-muted { background: #374151; color: #9ca3af; }
    :host(.dark-theme) .op-badge-success { background: #064e3b; color: #6ee7b7; }
    :host(.dark-theme) .op-badge-warning { background: #78350f; color: #fcd34d; }
  .op-tabs { display: flex; gap: 0; border-bottom: 2px solid var(--op-border); margin-bottom: 12px; }
  .op-tab { padding: 6px 16px; cursor: pointer; font-size: 12px; font-weight: 500; border: none; background: none; color: var(--op-text-muted); border-bottom: 2px solid transparent; margin-bottom: -2px; font-family: var(--op-font); }
  .op-tab:hover { color: var(--op-text); }
  .op-tab.active { color: var(--op-primary); border-bottom-color: var(--op-primary); }
  .op-link { color: var(--op-primary); cursor: pointer; text-decoration: none; }
  .op-link:hover { text-decoration: underline; }
  .op-mono { font-family: var(--op-font-mono); font-size: 12px; }
  .op-muted { color: var(--op-text-muted); font-size: 12px; }
  .op-tag { display: inline-block; padding: 1px 6px; margin: 1px 3px 1px 0; border-radius: 3px; font-size: 11px; background: var(--op-bg-alt); border: 1px solid var(--op-border); font-family: var(--op-font-mono); }
  .op-empty { padding: 30px; text-align: center; color: var(--op-text-muted); }
  /* Sample data table */
  .op-sample-table { font-size: 12px; }
  .op-sample-table th { font-size: 11px; white-space: nowrap; }
  .op-sample-table td { font-family: var(--op-font-mono); font-size: 11px; max-width: 200px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .op-sample-table td:hover { white-space: normal; word-break: break-all; }
  /* Lineage */
  .op-lineage-flow { display: flex; align-items: flex-start; gap: 16px; justify-content: center; flex-wrap: wrap; padding: 12px; }
  .op-lineage-group { min-width: 200px; }
  .op-lineage-group h4 { margin: 0 0 8px; font-size: 12px; color: var(--op-text-muted); text-transform: uppercase; letter-spacing: 0.5px; }
  .op-lineage-item { padding: 6px 10px; margin-bottom: 4px; border: 1px solid var(--op-border); border-radius: 4px; font-family: var(--op-font-mono); font-size: 11px; background: var(--op-bg-alt); }
  .op-lineage-center { min-width: 200px; padding: 12px; border: 2px solid var(--op-primary); border-radius: 6px; text-align: center; font-weight: 600; font-family: var(--op-font-mono); font-size: 12px; align-self: center; }
  .op-lineage-arrow { font-size: 20px; color: var(--op-text-muted); align-self: center; }
  .op-status-bar { padding: 6px 14px; font-size: 11px; color: var(--op-text-muted); background: var(--op-bg-alt); border-top: 1px solid var(--op-border); }
`;

function esc(s) { if (s == null) return ""; const d = document.createElement("div"); d.textContent = String(s); return d.innerHTML; }


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

  let currentTab = "columns";
  let sampleLoaded = false;
  let lineageLoaded = false;
  let permissionsLoaded = false;

  function getTable() { return JSON.parse(model.get("table_data") || "{}"); }
  function getSample() { return JSON.parse(model.get("sample_data") || "{}"); }
  function getLineage() { return JSON.parse(model.get("lineage_data") || "{}"); }
  function getPermissions() { return JSON.parse(model.get("permissions_data") || "{}"); }

  function sendRequest(req) { model.set("request", JSON.stringify({ ...req, _t: Date.now() })); model.save_changes(); }

  function fullRender() {
    const t = getTable();
    const loading = model.get("loading");
    const error = model.get("error_message");

    let html = `<div class="op-header">`;
    html += `<h2>📊 ${esc(t.name || t.full_name || "Table")}</h2>`;
    if (t.table_type) html += `<span class="op-badge op-badge-info">${esc(t.table_type)}</span>`;
    if (t.data_source_format) html += `<span class="op-badge op-badge-muted">${esc(t.data_source_format)}</span>`;
    html += `<div class="op-header-actions"><button class="op-btn" data-action="refresh">↻</button></div>`;
    html += `</div>`;

    if (error) html += `<div class="op-error">${esc(error)}</div>`;

    if (loading) {
      html += `<div class="op-body"><div class="op-loading"><span class="spinner"></span> Loading…</div></div>`;
    } else {
      const cols = t.columns || [];
      html += `<div class="op-body"><div class="op-detail">`;

      html += `<div class="op-tabs">`;
      html += `<button class="op-tab${currentTab==='columns'?' active':''}" data-tab="columns">Columns (${cols.length})</button>`;
      html += `<button class="op-tab${currentTab==='sample'?' active':''}" data-tab="sample">Sample Data</button>`;
      html += `<button class="op-tab${currentTab==='lineage'?' active':''}" data-tab="lineage">Lineage</button>`;
      html += `<button class="op-tab${currentTab==='permissions'?' active':''}" data-tab="permissions">Permissions</button>`;
      html += `<button class="op-tab${currentTab==='info'?' active':''}" data-tab="info">Info</button>`;
      html += `</div>`;

      // Columns tab
      html += `<div class="op-tab-content" data-tab="columns" style="${currentTab!=='columns'?'display:none':''}">`;
      if (cols.length) {
        html += `<table class="op-table"><thead><tr><th>#</th><th>Name</th><th>Type</th><th>Nullable</th><th>Comment</th></tr></thead><tbody>`;
        for (const c of cols) {
          html += `<tr><td class="op-muted">${c.position != null ? c.position : ''}</td>`;
          html += `<td class="op-mono">${esc(c.name)}</td>`;
          html += `<td class="op-mono op-muted">${esc(c.type_text || c.type_name)}</td>`;
          html += `<td>${c.nullable ? '✓' : '✗'}</td>`;
          html += `<td class="op-muted">${esc(c.comment || '')}</td></tr>`;
        }
        html += `</tbody></table>`;
      } else { html += `<div class="op-empty">No columns.</div>`; }
      html += `</div>`;

      // Sample tab
      html += `<div class="op-tab-content" data-tab="sample" style="${currentTab!=='sample'?'display:none':''}">`;
      const sample = getSample();
      if (sample.rows && sample.rows.length) {
        html += `<div style="overflow-x:auto"><table class="op-table op-sample-table"><thead><tr>`;
        for (const col of sample.columns) html += `<th>${esc(col)}</th>`;
        html += `</tr></thead><tbody>`;
        for (const row of sample.rows) {
          html += `<tr>`;
          for (const val of row) html += `<td>${esc(val)}</td>`;
          html += `</tr>`;
        }
        html += `</tbody></table></div>`;
      } else if (sampleLoaded) {
        html += `<div class="op-empty">No sample data available.</div>`;
      } else {
        html += `<div class="op-empty"><button class="op-btn op-btn-primary" data-action="load-sample">Load Sample Data (50 rows)</button></div>`;
      }
      html += `</div>`;

      // Lineage tab
      html += `<div class="op-tab-content" data-tab="lineage" style="${currentTab!=='lineage'?'display:none':''}">`;
      const lineage = getLineage();
      if (lineage.type === 'table') {
        html += `<div class="op-lineage-flow">`;
        html += `<div class="op-lineage-group"><h4>Upstream (${(lineage.upstream||[]).length})</h4>`;
        for (const u of lineage.upstream || []) html += `<div class="op-lineage-item">${esc(u.catalog_name)}.${esc(u.schema_name)}.${esc(u.name)}</div>`;
        if (!(lineage.upstream||[]).length) html += `<div class="op-muted">None</div>`;
        html += `</div>`;
        html += `<div class="op-lineage-arrow">→</div>`;
        html += `<div class="op-lineage-center">${esc(lineage.table)}</div>`;
        html += `<div class="op-lineage-arrow">→</div>`;
        html += `<div class="op-lineage-group"><h4>Downstream (${(lineage.downstream||[]).length})</h4>`;
        for (const d of lineage.downstream || []) html += `<div class="op-lineage-item">${esc(d.catalog_name)}.${esc(d.schema_name)}.${esc(d.name)}</div>`;
        if (!(lineage.downstream||[]).length) html += `<div class="op-muted">None</div>`;
        html += `</div></div>`;
      } else if (!lineageLoaded) {
        html += `<div class="op-empty"><button class="op-btn op-btn-primary" data-action="load-lineage">Load Lineage</button></div>`;
      } else {
        html += `<div class="op-empty">No lineage data.</div>`;
      }
      html += `</div>`;

      // Permissions tab
      html += `<div class="op-tab-content" data-tab="permissions" style="${currentTab!=='permissions'?'display:none':''}">`;
      const perms = getPermissions();
      if (perms.permissions && perms.permissions.length) {
        html += `<table class="op-table"><thead><tr><th>Principal</th><th>Privileges</th></tr></thead><tbody>`;
        for (const p of perms.permissions) {
          const privs = (p.privileges||[]).map(pr => {
            let s = esc(pr.privilege);
            if (pr.inherited_from_name) s += ` <span class="op-muted">(from ${esc(pr.inherited_from_name)})</span>`;
            return s;
          }).join(', ');
          html += `<tr><td class="op-mono">${esc(p.principal)}</td><td>${privs}</td></tr>`;
        }
        html += `</tbody></table>`;
      } else if (!permissionsLoaded) {
        html += `<div class="op-empty"><button class="op-btn op-btn-primary" data-action="load-permissions">Load Permissions</button></div>`;
      } else {
        html += `<div class="op-empty">No permissions data.</div>`;
      }
      html += `</div>`;

      // Info tab
      html += `<div class="op-tab-content" data-tab="info" style="${currentTab!=='info'?'display:none':''}">`;
      html += `<dl class="op-kv">`;
      html += `<dt>Full Name</dt><dd class="op-mono">${esc(t.full_name)}</dd>`;
      html += `<dt>Table Type</dt><dd>${esc(t.table_type)}</dd>`;
      html += `<dt>Format</dt><dd>${esc(t.data_source_format)}</dd>`;
      if (t.comment) html += `<dt>Comment</dt><dd>${esc(t.comment)}</dd>`;
      html += `<dt>Owner</dt><dd>${esc(t.owner||'—')}</dd>`;
      html += `<dt>Created</dt><dd>${esc(t.created_at||'—')} by ${esc(t.created_by||'—')}</dd>`;
      html += `<dt>Updated</dt><dd>${esc(t.updated_at||'—')} by ${esc(t.updated_by||'—')}</dd>`;
      if (t.storage_location) html += `<dt>Storage</dt><dd class="op-mono" style="font-size:11px">${esc(t.storage_location)}</dd>`;
      if (t.table_id) html += `<dt>Table ID</dt><dd class="op-mono">${esc(t.table_id)}</dd>`;
      html += `</dl>`;
      if (t.view_definition) {
        html += `<div class="op-section">View Definition</div>`;
        html += `<pre class="op-mono" style="padding:10px;background:var(--op-bg-alt);border:1px solid var(--op-border);border-radius:4px;white-space:pre-wrap;font-size:12px">${esc(t.view_definition)}</pre>`;
      }
      if (t.properties && Object.keys(t.properties).length) {
        html += `<div class="op-section">Properties</div>`;
        html += Object.entries(t.properties).map(([k,v]) => `<span class="op-tag">${esc(k)}=${esc(v)}</span>`).join(' ');
      }
      html += `</div>`;

      html += `</div></div>`;
    }

    root.innerHTML = html;
    bindEvents();
  }

  function bindEvents() {
    root.querySelectorAll(".op-tab").forEach(tab => {
      tab.addEventListener("click", () => { currentTab = tab.dataset.tab; fullRender(); });
    });
    root.querySelector("[data-action='refresh']")?.addEventListener("click", () => sendRequest({ action: "refresh" }));
    root.querySelector("[data-action='load-sample']")?.addEventListener("click", () => { sampleLoaded = true; sendRequest({ action: "get_sample_data" }); });
    root.querySelector("[data-action='load-lineage']")?.addEventListener("click", () => { lineageLoaded = true; sendRequest({ action: "get_lineage" }); });
    root.querySelector("[data-action='load-permissions']")?.addEventListener("click", () => { permissionsLoaded = true; sendRequest({ action: "get_permissions" }); });
  }

  model.on("change:table_data", fullRender);
  model.on("change:sample_data", fullRender);
  model.on("change:lineage_data", fullRender);
  model.on("change:permissions_data", fullRender);
  model.on("change:loading", fullRender);
  model.on("change:error_message", fullRender);

  fullRender();
}

export default { render };
