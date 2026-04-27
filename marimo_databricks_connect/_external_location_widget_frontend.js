// External Location Widget Frontend
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
  @media (prefers-color-scheme: dark) { :host { --op-bg: #1e1e1e; --op-bg-alt: #252526; --op-bg-hover: #2d2d30; --op-border: #3e3e42; --op-text: #cccccc; --op-text-muted: #888888; --op-primary: #4fc3f7; --op-success: #66bb6a; --op-danger: #ef5350; } }
  * { box-sizing: border-box; }
  .op-header { display: flex; align-items: center; gap: 8px; padding: 10px 14px; background: var(--op-bg-alt); border-bottom: 1px solid var(--op-border); flex-wrap: wrap; }
  .op-header h2 { margin: 0; font-size: 14px; font-weight: 600; }
  .op-header .op-subtitle { color: var(--op-text-muted); font-size: 11px; font-family: var(--op-font-mono); max-width: 500px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .op-header-actions { margin-left: auto; display: flex; gap: 6px; }
  .op-btn { padding: 4px 10px; border: 1px solid var(--op-border); border-radius: 4px; background: var(--op-bg); color: var(--op-text); cursor: pointer; font-size: 12px; font-family: var(--op-font); }
  .op-btn:hover { background: var(--op-bg-hover); }
  .op-btn-primary { background: var(--op-primary); color: #fff; border-color: var(--op-primary); }
  .op-btn-success { background: var(--op-success); color: #fff; border-color: var(--op-success); }
  .op-body { max-height: 600px; overflow: auto; }
  .op-loading { padding: 30px; text-align: center; color: var(--op-text-muted); }
  .op-loading .spinner { display: inline-block; width: 20px; height: 20px; border: 2px solid var(--op-border); border-top-color: var(--op-primary); border-radius: 50%; animation: op-spin 0.6s linear infinite; margin-right: 8px; vertical-align: middle; }
  @keyframes op-spin { to { transform: rotate(360deg); } }
  .op-error { padding: 10px 14px; background: #fef2f2; color: var(--op-danger); border-bottom: 1px solid #fecaca; font-size: 12px; }
  @media (prefers-color-scheme: dark) { .op-error { background: #3b1f1f; border-color: #5c2b2b; } }
  .op-detail { padding: 14px; }
  .op-kv { display: grid; grid-template-columns: 150px 1fr; gap: 4px 12px; margin-bottom: 16px; font-size: 12px; }
  .op-kv dt { color: var(--op-text-muted); font-weight: 500; padding: 3px 0; }
  .op-kv dd { margin: 0; padding: 3px 0; word-break: break-all; }
  .op-section { font-size: 13px; font-weight: 600; margin: 16px 0 8px; padding-bottom: 4px; border-bottom: 1px solid var(--op-border); }
  table.op-table { width: 100%; border-collapse: collapse; }
  .op-table th { text-align: left; padding: 8px 14px; font-size: 11px; font-weight: 600; text-transform: uppercase; color: var(--op-text-muted); background: var(--op-bg-alt); border-bottom: 1px solid var(--op-border); position: sticky; top: 0; z-index: 1; }
  .op-table td { padding: 7px 14px; border-bottom: 1px solid var(--op-border); vertical-align: top; }
  .op-table tr:hover td { background: var(--op-bg-hover); }
  .op-table tr.clickable { cursor: pointer; }
  .op-badge { display: inline-block; padding: 2px 7px; border-radius: 10px; font-size: 11px; font-weight: 500; }
  .op-badge-success { background: #d1fae5; color: #065f46; }
  .op-badge-danger { background: #fee2e2; color: #991b1b; }
  .op-badge-info { background: #dbeafe; color: #1e40af; }
  .op-badge-muted { background: #f3f4f6; color: #6b7280; }
  @media (prefers-color-scheme: dark) { .op-badge-success { background: #064e3b; color: #6ee7b7; } .op-badge-danger { background: #7f1d1d; color: #fca5a5; } .op-badge-info { background: #1e3a5f; color: #93c5fd; } .op-badge-muted { background: #374151; color: #9ca3af; } }
  .op-tabs { display: flex; gap: 0; border-bottom: 2px solid var(--op-border); margin-bottom: 12px; }
  .op-tab { padding: 6px 16px; cursor: pointer; font-size: 12px; font-weight: 500; border: none; background: none; color: var(--op-text-muted); border-bottom: 2px solid transparent; margin-bottom: -2px; font-family: var(--op-font); }
  .op-tab:hover { color: var(--op-text); }
  .op-tab.active { color: var(--op-primary); border-bottom-color: var(--op-primary); }
  .op-mono { font-family: var(--op-font-mono); font-size: 12px; }
  .op-muted { color: var(--op-text-muted); font-size: 12px; }
  .op-link { color: var(--op-primary); cursor: pointer; text-decoration: none; }
  .op-link:hover { text-decoration: underline; }
  .op-empty { padding: 30px; text-align: center; color: var(--op-text-muted); }
  .op-breadcrumb { display: flex; align-items: center; gap: 4px; font-size: 12px; color: var(--op-text-muted); margin-bottom: 8px; flex-wrap: wrap; }
  .op-breadcrumb button { background: none; border: none; color: var(--op-primary); cursor: pointer; font-size: 12px; padding: 0; font-family: var(--op-font); }
  .op-breadcrumb button:hover { text-decoration: underline; }
  .op-file-icon { margin-right: 4px; }
`;

function esc(s) { if (s == null) return ""; const d = document.createElement("div"); d.textContent = String(s); return d.innerHTML; }

function formatSize(b) {
  if (!b || b === 0) return '—';
  if (b < 1024) return b + ' B';
  if (b < 1048576) return (b/1024).toFixed(1) + ' KB';
  if (b < 1073741824) return (b/1048576).toFixed(1) + ' MB';
  return (b/1073741824).toFixed(1) + ' GB';
}

function shortName(url) {
  // Extract a human-friendly short name from a URL for display
  if (!url) return "Location";
  // abfss://container@account.dfs.core.windows.net/path → container/path
  const m = url.match(/^abfss:\/\/([^@]+)@[^/]+\/(.*)/);
  if (m) return m[1] + "/" + (m[2] || "");
  // s3://bucket/path → bucket/path
  const s3 = url.match(/^s3a?:\/\/(.+)/);
  if (s3) return s3[1];
  // /Volumes/... → just the path
  if (url.startsWith("/")) return url;
  return url;
}

function render({ model, el }) {
  const shadow = el.attachShadow ? el.attachShadow({ mode: "open" }) : el;
  const styleEl = document.createElement("style"); styleEl.textContent = OPS_STYLES; shadow.appendChild(styleEl);
  const root = document.createElement("div"); shadow.appendChild(root);

  let currentTab = null; // set dynamically based on is_raw_path
  let browseHistory = [];  // [{path, name}]
  let permissionsLoaded = false;
  let validationLoaded = false;

  function getLoc() { return JSON.parse(model.get("location_data") || "{}"); }
  function getContents() { return JSON.parse(model.get("contents_data") || "{}"); }
  function getPermissions() { return JSON.parse(model.get("permissions_data") || "{}"); }
  function getValidation() { return JSON.parse(model.get("validation_data") || "{}"); }
  function sendRequest(req) { model.set("request", JSON.stringify({ ...req, _t: Date.now() })); model.save_changes(); }

  function fullRender() {
    const loc = getLoc();
    const loading = model.get("loading");
    const error = model.get("error_message");
    const isRaw = loc.is_raw_path;

    // Default tab: browse for raw paths, details for UC locations
    if (currentTab === null) currentTab = isRaw ? "browse" : "details";

    // Header
    let displayName = loc.name || loc.url || "External Location";
    if (isRaw) displayName = shortName(loc.url);

    let html = `<div class="op-header"><h2>📂 ${esc(isRaw ? '' : (loc.name || ''))}${isRaw ? '' : ' '}</h2>`;
    if (!isRaw && loc.url) html += `<span class="op-subtitle" title="${esc(loc.url)}">${esc(loc.url)}</span>`;
    if (isRaw) html += `<span class="op-subtitle" title="${esc(loc.url)}">${esc(displayName)}</span>`;
    html += `<div class="op-header-actions"><button class="op-btn" data-action="refresh">↻</button></div></div>`;

    if (error) html += `<div class="op-error">${esc(error)}</div>`;

    if (loading) {
      html += `<div class="op-body"><div class="op-loading"><span class="spinner"></span> Loading…</div></div>`;
    } else {
      html += `<div class="op-body"><div class="op-detail">`;

      // Tabs — hide permissions/validate for raw paths
      html += `<div class="op-tabs">`;
      if (!isRaw) html += `<button class="op-tab${currentTab==='details'?' active':''}" data-tab="details">Details</button>`;
      html += `<button class="op-tab${currentTab==='browse'?' active':''}" data-tab="browse">Browse</button>`;
      if (!isRaw) html += `<button class="op-tab${currentTab==='permissions'?' active':''}" data-tab="permissions">Permissions</button>`;
      if (!isRaw) html += `<button class="op-tab${currentTab==='validate'?' active':''}" data-tab="validate">Validate</button>`;
      html += `</div>`;

      // Details tab (UC locations only)
      if (!isRaw) {
        html += `<div class="op-tab-content" data-tab="details" style="${currentTab!=='details'?'display:none':''}">`;
        html += `<dl class="op-kv">`;
        html += `<dt>Name</dt><dd class="op-mono">${esc(loc.name)}</dd>`;
        html += `<dt>URL</dt><dd class="op-mono" style="font-size:11px">${esc(loc.url)}</dd>`;
        html += `<dt>Credential</dt><dd class="op-mono">${esc(loc.credential_name||'—')}</dd>`;
        html += `<dt>Read Only</dt><dd>${loc.read_only ? '✓ Yes' : '✗ No'}</dd>`;
        if (loc.comment) html += `<dt>Comment</dt><dd>${esc(loc.comment)}</dd>`;
        html += `<dt>Owner</dt><dd>${esc(loc.owner||'—')}</dd>`;
        html += `<dt>Created</dt><dd>${esc(loc.created_at||'—')} by ${esc(loc.created_by||'—')}</dd>`;
        html += `<dt>Updated</dt><dd>${esc(loc.updated_at||'—')} by ${esc(loc.updated_by||'—')}</dd>`;
        if (loc.isolation_mode) html += `<dt>Isolation</dt><dd>${esc(loc.isolation_mode)}</dd>`;
        html += `</dl></div>`;
      }

      // Browse tab
      html += `<div class="op-tab-content" data-tab="browse" style="${currentTab!=='browse'?'display:none':''}">`;
      const contents = getContents();
      if (contents.items) {
        // Breadcrumb navigation
        html += `<div class="op-breadcrumb">`;
        html += `<button data-browse-path="${esc(loc.url)}" data-browse-reset="true">📂 root</button>`;
        for (let i = 0; i < browseHistory.length; i++) {
          const h = browseHistory[i];
          html += ` <span style="color:var(--op-text-muted)">›</span> `;
          html += `<button data-browse-path="${esc(h.path)}" data-browse-idx="${i}">${esc(h.name)}</button>`;
        }
        html += `</div>`;

        if (contents.items.length) {
          html += `<table class="op-table"><thead><tr><th>Name</th><th>Size</th><th>Type</th></tr></thead><tbody>`;
          for (const f of contents.items) {
            const icon = f.is_dir ? '📁' : '📄';
            const displayName = f.name.replace(/\/$/, '');
            if (f.is_dir) {
              html += `<tr class="clickable" data-browse-dir="${esc(f.path)}" data-dir-name="${esc(displayName)}">`;
              html += `<td><span class="op-file-icon">${icon}</span><span class="op-link">${esc(displayName)}</span></td>`;
            } else {
              html += `<tr><td><span class="op-file-icon">${icon}</span>${esc(displayName)}</td>`;
            }
            html += `<td class="op-muted">${formatSize(f.size)}</td>`;
            html += `<td class="op-muted">${f.is_dir ? 'Directory' : 'File'}</td></tr>`;
          }
          html += `</tbody></table>`;
        } else {
          html += `<div class="op-empty">Empty directory.</div>`;
        }
      } else {
        html += `<div class="op-empty"><button class="op-btn op-btn-primary" data-action="browse-root">📂 Browse Files</button></div>`;
      }
      html += `</div>`;

      // Permissions tab (UC locations only)
      if (!isRaw) {
        html += `<div class="op-tab-content" data-tab="permissions" style="${currentTab!=='permissions'?'display:none':''}">`;
        const perms = getPermissions();
        if (perms.permissions && perms.permissions.length) {
          html += `<table class="op-table"><thead><tr><th>Principal</th><th>Privileges</th></tr></thead><tbody>`;
          for (const p of perms.permissions) {
            const privs = (p.privileges||[]).map(pr => esc(pr.privilege)).join(', ');
            html += `<tr><td class="op-mono">${esc(p.principal)}</td><td>${privs}</td></tr>`;
          }
          html += `</tbody></table>`;
        } else if (!permissionsLoaded) {
          html += `<div class="op-empty"><button class="op-btn op-btn-primary" data-action="load-permissions">Load Permissions</button></div>`;
        } else {
          html += `<div class="op-empty">No permissions data.</div>`;
        }
        html += `</div>`;

        // Validate tab
        html += `<div class="op-tab-content" data-tab="validate" style="${currentTab!=='validate'?'display:none':''}">`;
        const val = getValidation();
        if (val.checks && val.checks.length) {
          html += `<table class="op-table"><thead><tr><th>Operation</th><th>Result</th><th>Message</th></tr></thead><tbody>`;
          for (const c of val.checks) {
            const badge = c.result === 'PASS' ? 'success' : c.result === 'FAIL' ? 'danger' : 'muted';
            html += `<tr><td class="op-mono">${esc(c.operation)}</td><td><span class="op-badge op-badge-${badge}">${esc(c.result)}</span></td><td class="op-muted">${esc(c.message||'')}</td></tr>`;
          }
          html += `</tbody></table>`;
        } else if (!validationLoaded) {
          html += `<div class="op-empty"><button class="op-btn op-btn-success" data-action="validate">🔍 Validate Location</button></div>`;
        } else {
          html += `<div class="op-empty">No validation results.</div>`;
        }
        html += `</div>`;
      }

      html += `</div></div>`;
    }

    root.innerHTML = html;
    bindEvents();
  }

  function bindEvents() {
    // Tab switching
    root.querySelectorAll(".op-tab").forEach(tab => {
      tab.addEventListener("click", () => { currentTab = tab.dataset.tab; fullRender(); });
    });

    // Refresh
    root.querySelector("[data-action='refresh']")?.addEventListener("click", () => sendRequest({ action: "refresh" }));

    // Browse root
    root.querySelector("[data-action='browse-root']")?.addEventListener("click", () => {
      browseHistory = [];
      sendRequest({ action: "browse" });
    });

    // Directory click — drill down
    root.querySelectorAll("[data-browse-dir]").forEach(tr => {
      tr.addEventListener("click", () => {
        const path = tr.dataset.browseDir;
        const name = tr.dataset.dirName;
        browseHistory.push({ path, name });
        sendRequest({ action: "browse", path });
      });
    });

    // Breadcrumb click — navigate back to that level
    root.querySelectorAll("[data-browse-path]").forEach(btn => {
      btn.addEventListener("click", () => {
        const path = btn.dataset.browsePath;
        if (btn.dataset.browseReset === "true") {
          browseHistory = [];
        } else if (btn.dataset.browseIdx != null) {
          const idx = parseInt(btn.dataset.browseIdx);
          browseHistory = browseHistory.slice(0, idx + 1);
        }
        sendRequest({ action: "browse", path });
      });
    });

    // Permissions
    root.querySelector("[data-action='load-permissions']")?.addEventListener("click", () => {
      permissionsLoaded = true;
      sendRequest({ action: "get_permissions" });
    });

    // Validate
    root.querySelector("[data-action='validate']")?.addEventListener("click", () => {
      validationLoaded = true;
      sendRequest({ action: "validate" });
    });
  }

  model.on("change:location_data", fullRender);
  model.on("change:contents_data", fullRender);
  model.on("change:permissions_data", fullRender);
  model.on("change:validation_data", fullRender);
  model.on("change:loading", fullRender);
  model.on("change:error_message", fullRender);

  fullRender();
}

export default { render };
