// Schema Widget Frontend
const OPS_STYLES = `
  :host {
    --op-bg: #ffffff; --op-bg-alt: #f8f9fa; --op-bg-hover: #e9ecef;
    --op-border: #dee2e6; --op-text: #212529; --op-text-muted: #6c757d;
    --op-primary: #0d6efd; --op-success: #198754; --op-danger: #dc3545;
    --op-font: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
    --op-font-mono: "SF Mono", "Cascadia Code", "Fira Code", Menlo, Consolas, monospace;
    --op-radius: 6px;
    display: block; font-family: var(--op-font); font-size: 13px; color: var(--op-text); background: var(--op-bg); border: 1px solid var(--op-border); border-radius: var(--op-radius); overflow: hidden;
  }
  :host(.dark-theme) { --op-bg: #1e1e1e; --op-bg-alt: #252526; --op-bg-hover: #2d2d30; --op-border: #3e3e42; --op-text: #cccccc; --op-text-muted: #888888; --op-primary: #4fc3f7; --op-success: #66bb6a; --op-danger: #ef5350; }
  * { box-sizing: border-box; }
  .op-header { display: flex; align-items: center; gap: 8px; padding: 10px 14px; background: var(--op-bg-alt); border-bottom: 1px solid var(--op-border); flex-wrap: wrap; }
  .op-header h2 { margin: 0; font-size: 14px; font-weight: 600; }
  .op-header-actions { margin-left: auto; display: flex; gap: 6px; }
  .op-btn { padding: 4px 10px; border: 1px solid var(--op-border); border-radius: 4px; background: var(--op-bg); color: var(--op-text); cursor: pointer; font-size: 12px; font-family: var(--op-font); }
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
  .op-table th { text-align: left; padding: 8px 14px; font-size: 11px; font-weight: 600; text-transform: uppercase; color: var(--op-text-muted); background: var(--op-bg-alt); border-bottom: 1px solid var(--op-border); position: sticky; top: 0; z-index: 1; }
  .op-table td { padding: 7px 14px; border-bottom: 1px solid var(--op-border); vertical-align: top; }
  .op-table tr:hover td { background: var(--op-bg-hover); }
  .op-badge { display: inline-block; padding: 2px 7px; border-radius: 10px; font-size: 11px; font-weight: 500; }
  .op-badge-info { background: #dbeafe; color: #1e40af; }
  .op-badge-muted { background: #f3f4f6; color: #6b7280; }
  :host(.dark-theme) .op-badge-info { background: #1e3a5f; color: #93c5fd; } :host(.dark-theme) .op-badge-muted { background: #374151; color: #9ca3af; }
  .op-tabs { display: flex; gap: 0; border-bottom: 2px solid var(--op-border); margin-bottom: 12px; }
  .op-tab { padding: 6px 16px; cursor: pointer; font-size: 12px; font-weight: 500; border: none; background: none; color: var(--op-text-muted); border-bottom: 2px solid transparent; margin-bottom: -2px; font-family: var(--op-font); }
  .op-tab:hover { color: var(--op-text); }
  .op-tab.active { color: var(--op-primary); border-bottom-color: var(--op-primary); }
  .op-mono { font-family: var(--op-font-mono); font-size: 12px; }
  .op-muted { color: var(--op-text-muted); font-size: 12px; }
  .op-tag { display: inline-block; padding: 1px 6px; margin: 1px 3px 1px 0; border-radius: 3px; font-size: 11px; background: var(--op-bg-alt); border: 1px solid var(--op-border); font-family: var(--op-font-mono); }
  .op-empty { padding: 30px; text-align: center; color: var(--op-text-muted); }
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

  let currentTab = "tables";
  let volumesLoaded = false;
  let permissionsLoaded = false;

  function getSchema() { return JSON.parse(model.get("schema_data") || "{}"); }
  function getTables() { return JSON.parse(model.get("tables_data") || "[]"); }
  function getVolumes() { return JSON.parse(model.get("volumes_data") || "[]"); }
  function getPermissions() { return JSON.parse(model.get("permissions_data") || "{}"); }
  function sendRequest(req) { model.set("request", JSON.stringify({ ...req, _t: Date.now() })); model.save_changes(); }

  function fullRender() {
    const s = getSchema();
    const tables = getTables();
    const loading = model.get("loading");
    const error = model.get("error_message");

    let html = `<div class="op-header"><h2>📁 ${esc(s.catalog_name)}.${esc(s.name || "Schema")}</h2>`;
    html += `<div class="op-header-actions"><button class="op-btn" data-action="refresh">↻</button></div></div>`;
    if (error) html += `<div class="op-error">${esc(error)}</div>`;

    if (loading) {
      html += `<div class="op-body"><div class="op-loading"><span class="spinner"></span> Loading…</div></div>`;
    } else {
      html += `<div class="op-body"><div class="op-detail">`;
      html += `<div class="op-tabs">`;
      html += `<button class="op-tab${currentTab==='tables'?' active':''}" data-tab="tables">Tables (${tables.length})</button>`;
      html += `<button class="op-tab${currentTab==='volumes'?' active':''}" data-tab="volumes">Volumes</button>`;
      html += `<button class="op-tab${currentTab==='permissions'?' active':''}" data-tab="permissions">Permissions</button>`;
      html += `<button class="op-tab${currentTab==='info'?' active':''}" data-tab="info">Info</button>`;
      html += `</div>`;

      // Tables
      html += `<div class="op-tab-content" data-tab="tables" style="${currentTab!=='tables'?'display:none':''}">`;
      if (tables.length) {
        html += `<table class="op-table"><thead><tr><th>Name</th><th>Type</th><th>Format</th><th>Owner</th><th>Updated</th></tr></thead><tbody>`;
        for (const t of tables) {
          html += `<tr><td class="op-mono">${esc(t.name)}</td><td><span class="op-badge op-badge-info">${esc(t.table_type)}</span></td><td class="op-muted">${esc(t.data_source_format||'—')}</td><td class="op-muted">${esc(t.owner||'—')}</td><td class="op-muted">${esc(t.updated_at||'—')}</td></tr>`;
        }
        html += `</tbody></table>`;
      } else { html += `<div class="op-empty">No tables.</div>`; }
      html += `</div>`;

      // Volumes
      html += `<div class="op-tab-content" data-tab="volumes" style="${currentTab!=='volumes'?'display:none':''}">`;
      const volumes = getVolumes();
      if (volumes.length) {
        html += `<table class="op-table"><thead><tr><th>Name</th><th>Type</th><th>Owner</th><th>Storage</th></tr></thead><tbody>`;
        for (const v of volumes) {
          html += `<tr><td class="op-mono">${esc(v.name)}</td><td><span class="op-badge op-badge-muted">${esc(v.volume_type)}</span></td><td class="op-muted">${esc(v.owner||'—')}</td><td class="op-mono op-muted" style="font-size:11px">${esc(v.storage_location||'—')}</td></tr>`;
        }
        html += `</tbody></table>`;
      } else if (!volumesLoaded) {
        html += `<div class="op-empty"><button class="op-btn op-btn-primary" data-action="load-volumes">Load Volumes</button></div>`;
      } else { html += `<div class="op-empty">No volumes.</div>`; }
      html += `</div>`;

      // Permissions
      html += `<div class="op-tab-content" data-tab="permissions" style="${currentTab!=='permissions'?'display:none':''}">`;
      const perms = getPermissions();
      if (perms.permissions && perms.permissions.length) {
        html += `<table class="op-table"><thead><tr><th>Principal</th><th>Privileges</th></tr></thead><tbody>`;
        for (const p of perms.permissions) {
          const privs = (p.privileges||[]).map(pr => { let s = esc(pr.privilege); if (pr.inherited_from_name) s += ` <span class="op-muted">(from ${esc(pr.inherited_from_name)})</span>`; return s; }).join(', ');
          html += `<tr><td class="op-mono">${esc(p.principal)}</td><td>${privs}</td></tr>`;
        }
        html += `</tbody></table>`;
      } else if (!permissionsLoaded) {
        html += `<div class="op-empty"><button class="op-btn op-btn-primary" data-action="load-permissions">Load Permissions</button></div>`;
      } else { html += `<div class="op-empty">No permissions data.</div>`; }
      html += `</div>`;

      // Info
      html += `<div class="op-tab-content" data-tab="info" style="${currentTab!=='info'?'display:none':''}">`;
      html += `<dl class="op-kv">`;
      html += `<dt>Full Name</dt><dd class="op-mono">${esc(s.full_name)}</dd>`;
      if (s.comment) html += `<dt>Comment</dt><dd>${esc(s.comment)}</dd>`;
      html += `<dt>Owner</dt><dd>${esc(s.owner||'—')}</dd>`;
      html += `<dt>Created</dt><dd>${esc(s.created_at||'—')} by ${esc(s.created_by||'—')}</dd>`;
      html += `<dt>Updated</dt><dd>${esc(s.updated_at||'—')} by ${esc(s.updated_by||'—')}</dd>`;
      if (s.storage_location) html += `<dt>Storage</dt><dd class="op-mono" style="font-size:11px">${esc(s.storage_location)}</dd>`;
      if (s.storage_root) html += `<dt>Storage Root</dt><dd class="op-mono" style="font-size:11px">${esc(s.storage_root)}</dd>`;
      html += `</dl>`;
      if (s.properties && Object.keys(s.properties).length) {
        html += `<div class="op-section">Properties</div>`;
        html += Object.entries(s.properties).map(([k,v])=>`<span class="op-tag">${esc(k)}=${esc(v)}</span>`).join(' ');
      }
      html += `</div>`;

      html += `</div></div>`;
    }

    root.innerHTML = html;
    bindEvents();
  }

  function bindEvents() {
    root.querySelectorAll(".op-tab").forEach(tab => tab.addEventListener("click", () => { currentTab = tab.dataset.tab; fullRender(); }));
    root.querySelector("[data-action='refresh']")?.addEventListener("click", () => sendRequest({ action: "refresh" }));
    root.querySelector("[data-action='load-volumes']")?.addEventListener("click", () => { volumesLoaded = true; sendRequest({ action: "list_volumes" }); });
    root.querySelector("[data-action='load-permissions']")?.addEventListener("click", () => { permissionsLoaded = true; sendRequest({ action: "get_permissions" }); });
  }

  model.on("change:schema_data", fullRender);
  model.on("change:tables_data", fullRender);
  model.on("change:volumes_data", fullRender);
  model.on("change:permissions_data", fullRender);
  model.on("change:loading", fullRender);
  model.on("change:error_message", fullRender);

  fullRender();
}

export default { render };
