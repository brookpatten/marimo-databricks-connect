// Workspace Browser Widget Frontend
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
  .op-header .op-subtitle { color: var(--op-text-muted); font-size: 11px; font-family: var(--op-font-mono); max-width: 500px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
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
  .op-kv { display: grid; grid-template-columns: 130px 1fr; gap: 4px 12px; margin-bottom: 12px; font-size: 12px; }
  .op-kv dt { color: var(--op-text-muted); font-weight: 500; padding: 3px 0; }
  .op-kv dd { margin: 0; padding: 3px 0; word-break: break-all; }
  .op-section { font-size: 12px; font-weight: 600; margin: 10px 0 6px; color: var(--op-text-muted); text-transform: uppercase; letter-spacing: 0.04em; }
  table.op-table { width: 100%; border-collapse: collapse; }
  .op-table th { text-align: left; padding: 8px 14px; font-size: 11px; font-weight: 600; text-transform: uppercase; color: var(--op-text-muted); background: var(--op-bg-alt); border-bottom: 1px solid var(--op-border); position: sticky; top: 0; z-index: 1; }
  .op-table td { padding: 7px 14px; border-bottom: 1px solid var(--op-border); vertical-align: top; }
  .op-table tr:hover td { background: var(--op-bg-hover); }
  .op-table tr.clickable { cursor: pointer; }
  .op-table tr.selected td { background: var(--op-bg-hover); }
  .op-badge { display: inline-block; padding: 2px 7px; border-radius: 10px; font-size: 11px; font-weight: 500; }
  .op-badge-info { background: #dbeafe; color: #1e40af; }
  .op-badge-muted { background: #f3f4f6; color: #6b7280; }
  .op-badge-success { background: #d1fae5; color: #065f46; }
  :host(.dark-theme) .op-badge-info { background: #1e3a5f; color: #93c5fd; }
  :host(.dark-theme) .op-badge-muted { background: #374151; color: #9ca3af; }
  :host(.dark-theme) .op-badge-success { background: #064e3b; color: #6ee7b7; }
  .op-mono { font-family: var(--op-font-mono); font-size: 12px; }
  .op-muted { color: var(--op-text-muted); font-size: 12px; }
  .op-link { color: var(--op-primary); cursor: pointer; text-decoration: none; }
  .op-link:hover { text-decoration: underline; }
  .op-empty { padding: 30px; text-align: center; color: var(--op-text-muted); }
  .op-breadcrumb { display: flex; align-items: center; gap: 4px; font-size: 12px; color: var(--op-text-muted); margin-bottom: 8px; flex-wrap: wrap; padding: 0 14px; }
  .op-breadcrumb button { background: none; border: none; color: var(--op-primary); cursor: pointer; font-size: 12px; padding: 0; font-family: var(--op-font); }
  .op-breadcrumb button:hover { text-decoration: underline; }
  .op-file-icon { margin-right: 6px; }
  .op-split { display: grid; grid-template-columns: 1fr 320px; gap: 0; }
  .op-split .op-pane-list { border-right: 1px solid var(--op-border); min-width: 0; }
  .op-split .op-pane-detail { padding: 12px 14px; background: var(--op-bg-alt); min-width: 0; }
  @media (max-width: 700px) { .op-split { grid-template-columns: 1fr; } .op-split .op-pane-detail { border-top: 1px solid var(--op-border); } }
  pre.op-preview { background: var(--op-bg); border: 1px solid var(--op-border); border-radius: 4px; padding: 8px; max-height: 320px; overflow: auto; font-family: var(--op-font-mono); font-size: 11px; white-space: pre; margin: 0; }
  .op-loading-overlay { position: relative; pointer-events: none; opacity: 0.6; }
  .op-loading-overlay::after { content: ''; position: absolute; top: 0; left: 0; right: 0; bottom: 0; background: var(--op-bg); opacity: 0.5; z-index: 10; }
  .op-loading-overlay::before { content: ''; position: absolute; top: 50%; left: 50%; width: 20px; height: 20px; margin: -10px 0 0 -10px; border: 2px solid var(--op-border); border-top-color: var(--op-primary); border-radius: 50%; animation: op-spin 0.6s linear infinite; z-index: 11; }
`;

function esc(s) { if (s == null) return ""; const d = document.createElement("div"); d.textContent = String(s); return d.innerHTML; }

function formatSize(b) {
  if (!b || b === 0) return '—';
  if (b < 1024) return b + ' B';
  if (b < 1048576) return (b/1024).toFixed(1) + ' KB';
  if (b < 1073741824) return (b/1048576).toFixed(1) + ' MB';
  return (b/1073741824).toFixed(1) + ' GB';
}

function formatTs(ms) {
  if (!ms) return '—';
  try { return new Date(ms).toISOString().replace('T', ' ').slice(0, 19); } catch (e) { return String(ms); }
}

function objectIcon(o) {
  const t = (o.object_type || "").toUpperCase();
  if (o.is_dir && t === "REPO") return "📦";
  if (o.is_dir) return "📁";
  if (t === "NOTEBOOK") return "📓";
  return "📄";
}

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
    return ((0.2126 * r + 0.7152 * g + 0.0722 * b) / 255) < 0.5;
  }
  function themeFromElement(el) {
    if (!el) return null;
    const a = parseTheme(el.getAttribute?.("data-app-theme")); if (a != null) return a;
    const b = parseTheme(el.getAttribute?.("data-theme")); if (b != null) return b;
    const c = parseTheme(el.className); if (c != null) return c;
    const d = parseTheme(getComputedStyle(el).colorScheme); if (d != null) return d;
    return null;
  }
  function isDark() {
    const t = hostEl.closest?.(themeSelector);
    return themeFromElement(t) ?? themeFromElement(hostEl) ?? themeFromElement(hostEl.parentElement)
      ?? themeFromElement(document.body) ?? themeFromElement(document.documentElement)
      ?? backgroundLooksDark(hostEl.parentElement) ?? backgroundLooksDark(document.body) ?? media.matches;
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
  const cleanup = () => { obs.disconnect(); media.removeEventListener("change", apply); if (hostEl.__cleanupThemeSync === cleanup) delete hostEl.__cleanupThemeSync; };
  hostEl.__cleanupThemeSync = cleanup;
  return cleanup;
}

function pathSegments(path, root) {
  // Build [{name, path}] segments from root → path. Both are absolute workspace paths.
  if (!path) return [];
  root = root || "/";
  const parts = path.split("/").filter(Boolean);
  const out = [];
  let acc = "";
  for (const p of parts) {
    acc = acc + "/" + p;
    out.push({ name: p, path: acc });
  }
  // Strip everything that's a prefix of root — root is rendered separately.
  if (root !== "/") {
    const rootParts = root.split("/").filter(Boolean).length;
    return out.slice(rootParts);
  }
  return out;
}

function render({ model, el }) {
  const shadow = el.attachShadow ? el.attachShadow({ mode: "open" }) : el;
  _syncTheme(el);
  const styleEl = document.createElement("style"); styleEl.textContent = OPS_STYLES; shadow.appendChild(styleEl);
  const root = document.createElement("div"); shadow.appendChild(root);

  let detailTab = "info"; // 'info' | 'permissions' | 'preview'
  let hasRendered = false;

  function getContents() { return JSON.parse(model.get("contents_data") || "{}"); }
  function getSelected() { return JSON.parse(model.get("selected_data") || "{}"); }
  function getPermissions() { return JSON.parse(model.get("permissions_data") || "{}"); }
  function getPreview() { return JSON.parse(model.get("preview_data") || "{}"); }
  function sendRequest(req) { model.set("request", JSON.stringify({ ...req, _t: Date.now() })); model.save_changes(); }

  function renderDetail() {
    const sel = getSelected();
    if (!sel || !sel.path) {
      return `<div class="op-empty" style="padding:40px 14px">Select an item from the list to inspect it.</div>`;
    }
    let html = "";
    html += `<div style="display:flex;align-items:center;gap:8px;margin-bottom:8px;">`;
    html += `<div style="font-size:14px;font-weight:600;word-break:break-all;">${objectIcon(sel)} ${esc(sel.name)}</div>`;
    html += `</div>`;
    html += `<div class="op-mono op-muted" style="margin-bottom:8px;word-break:break-all">${esc(sel.path)}</div>`;

    html += `<div class="op-tabs" style="display:flex;gap:6px;border-bottom:1px solid var(--op-border);margin-bottom:8px;">`;
    for (const t of ["info", "permissions", "preview"]) {
      if (t === "preview" && sel.is_dir) continue;
      const label = t.charAt(0).toUpperCase() + t.slice(1);
      const active = detailTab === t;
      html += `<button class="op-btn" data-detail-tab="${t}" style="border:none;border-bottom:2px solid ${active ? 'var(--op-primary)' : 'transparent'};border-radius:0;background:none;color:${active ? 'var(--op-primary)' : 'var(--op-text-muted)'};font-weight:500;padding:4px 8px;">${label}</button>`;
    }
    html += `</div>`;

    if (detailTab === "info") {
      html += `<dl class="op-kv">`;
      html += `<dt>Type</dt><dd><span class="op-badge op-badge-info">${esc(sel.object_type || (sel.is_dir ? 'DIRECTORY' : 'FILE'))}</span></dd>`;
      if (sel.language) html += `<dt>Language</dt><dd class="op-mono">${esc(sel.language)}</dd>`;
      if (sel.object_id) html += `<dt>Object ID</dt><dd class="op-mono">${esc(sel.object_id)}</dd>`;
      if (!sel.is_dir) html += `<dt>Size</dt><dd>${formatSize(sel.size)}</dd>`;
      if (sel.created_at) html += `<dt>Created</dt><dd class="op-mono">${esc(formatTs(sel.created_at))}</dd>`;
      if (sel.modified_at) html += `<dt>Modified</dt><dd class="op-mono">${esc(formatTs(sel.modified_at))}</dd>`;
      html += `</dl>`;
      if (sel.is_dir) {
        html += `<button class="op-btn op-btn-primary" data-action="enter">Open folder</button>`;
      }
    } else if (detailTab === "permissions") {
      const perms = getPermissions();
      const matches = perms && perms.path === sel.path;
      if (!matches) {
        html += `<button class="op-btn op-btn-primary" data-action="load-permissions">Load Permissions</button>`;
      } else if (!perms.acl || !perms.acl.length) {
        html += `<div class="op-empty">No ACL entries (or no access).</div>`;
      } else {
        html += `<table class="op-table"><thead><tr><th>Principal</th><th>Level</th></tr></thead><tbody>`;
        for (const a of perms.acl) {
          const inh = a.inherited ? ` <span class="op-badge op-badge-muted" title="inherited">inh</span>` : "";
          html += `<tr><td class="op-mono">${esc(a.principal || '—')}</td><td>${esc(a.permission_level || '—')}${inh}</td></tr>`;
        }
        html += `</tbody></table>`;
      }
    } else if (detailTab === "preview") {
      const pv = getPreview();
      const matches = pv && pv.path === sel.path;
      if (!matches) {
        html += `<button class="op-btn op-btn-primary" data-action="load-preview">Load Preview</button>`;
      } else {
        html += `<div class="op-muted" style="margin-bottom:6px">${formatSize(pv.size)}${pv.truncated ? ' (truncated)' : ''}</div>`;
        html += `<pre class="op-preview">${esc(pv.text || '')}</pre>`;
      }
    }
    return html;
  }

  function fullRender() {
    const loading = model.get("loading");
    const error = model.get("error_message");
    const contents = getContents();
    const sel = getSelected();
    const path = contents.path || model.get("root") || "/";
    const rootPath = model.get("root") || "/";

    let html = `<div class="op-header"><h2>🗂️ Workspace</h2>`;
    html += `<span class="op-subtitle" title="${esc(rootPath)}">root: ${esc(rootPath)}</span>`;
    html += `<div class="op-header-actions"><button class="op-btn" data-action="refresh">↻</button></div></div>`;

    if (error) html += `<div class="op-error">${esc(error)}</div>`;

    if (loading && !hasRendered) {
      html += `<div class="op-body"><div class="op-loading"><span class="spinner"></span> Loading…</div></div>`;
    } else {
      html += `<div class="op-body${loading ? ' op-loading-overlay' : ''}">`;

      // Breadcrumbs (always show, anchored at root).
      html += `<div class="op-breadcrumb" style="padding-top:10px">`;
      html += `<button data-browse-path="${esc(rootPath)}">📂 ${esc(rootPath === "/" ? "/" : rootPath.split("/").pop() || rootPath)}</button>`;
      for (const seg of pathSegments(path, rootPath)) {
        html += ` <span style="color:var(--op-text-muted)">›</span> `;
        html += `<button data-browse-path="${esc(seg.path)}">${esc(seg.name)}</button>`;
      }
      html += `</div>`;

      html += `<div class="op-split">`;

      // Listing pane
      html += `<div class="op-pane-list">`;
      if (contents.items && contents.items.length) {
        html += `<table class="op-table"><thead><tr><th>Name</th><th>Type</th><th>Modified</th></tr></thead><tbody>`;
        for (const f of contents.items) {
          const selected = sel && sel.path === f.path ? ' selected' : '';
          html += `<tr class="clickable${selected}" data-select-path="${esc(f.path)}" data-is-dir="${f.is_dir ? '1' : '0'}">`;
          html += `<td><span class="op-file-icon">${objectIcon(f)}</span><span class="op-link">${esc(f.name)}</span></td>`;
          html += `<td class="op-muted">${esc(f.object_type || (f.is_dir ? 'DIRECTORY' : 'FILE'))}${f.language ? ` <span class="op-badge op-badge-muted">${esc(f.language)}</span>` : ''}</td>`;
          html += `<td class="op-muted op-mono" style="font-size:11px">${esc(formatTs(f.modified_at))}</td>`;
          html += `</tr>`;
        }
        html += `</tbody></table>`;
      } else if (contents.items) {
        html += `<div class="op-empty">Empty folder.</div>`;
      } else {
        html += `<div class="op-empty">Loading…</div>`;
      }
      html += `</div>`;

      // Detail pane
      html += `<div class="op-pane-detail">${renderDetail()}</div>`;

      html += `</div></div>`;
    }

    root.innerHTML = html;
    hasRendered = true;
    bindEvents();
  }

  function bindEvents() {
    root.querySelector("[data-action='refresh']")?.addEventListener("click", () => sendRequest({ action: "refresh" }));

    root.querySelectorAll("[data-browse-path]").forEach(btn => {
      btn.addEventListener("click", () => sendRequest({ action: "browse", path: btn.dataset.browsePath }));
    });

    root.querySelectorAll("[data-select-path]").forEach(tr => {
      tr.addEventListener("click", () => {
        const path = tr.dataset.selectPath;
        const isDir = tr.dataset.isDir === "1";
        if (isDir) {
          sendRequest({ action: "browse", path });
        }
        // Always also fetch metadata so the right-hand pane updates.
        detailTab = "info";
        sendRequest({ action: "select", path });
      });
    });

    root.querySelectorAll("[data-detail-tab]").forEach(btn => {
      btn.addEventListener("click", () => { detailTab = btn.dataset.detailTab; fullRender(); });
    });

    root.querySelector("[data-action='enter']")?.addEventListener("click", () => {
      const sel = getSelected();
      if (sel && sel.path) sendRequest({ action: "browse", path: sel.path });
    });

    root.querySelector("[data-action='load-permissions']")?.addEventListener("click", () => {
      const sel = getSelected();
      if (!sel || !sel.path) return;
      sendRequest({ action: "get_permissions", path: sel.path, object_type: sel.object_type, object_id: sel.object_id });
    });

    root.querySelector("[data-action='load-preview']")?.addEventListener("click", () => {
      const sel = getSelected();
      if (!sel || !sel.path) return;
      sendRequest({ action: "preview", path: sel.path });
    });
  }

  model.on("change:contents_data", fullRender);
  model.on("change:selected_data", fullRender);
  model.on("change:permissions_data", fullRender);
  model.on("change:preview_data", fullRender);
  model.on("change:loading", fullRender);
  model.on("change:error_message", fullRender);
  model.on("change:root", fullRender);

  fullRender();
}

export default { render };
