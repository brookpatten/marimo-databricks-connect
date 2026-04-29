// Secret Scope Widget Frontend
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
  .op-header .op-subtitle { color: var(--op-text-muted); font-size: 11px; font-family: var(--op-font-mono); }
  .op-header-actions { margin-left: auto; display: flex; gap: 6px; align-items: center; }
  .op-btn { padding: 4px 10px; border: 1px solid var(--op-border); border-radius: 4px; background: var(--op-bg); color: var(--op-text); cursor: pointer; font-size: 12px; font-family: var(--op-font); white-space: nowrap; }
  .op-btn:hover { background: var(--op-bg-hover); }
  .op-btn-primary { background: var(--op-primary); color: #fff; border-color: var(--op-primary); }
  .op-btn-primary:hover { background: var(--op-primary-hover); }
  .op-btn-danger { background: var(--op-danger); color: #fff; border-color: var(--op-danger); }
  .op-btn-danger:hover { opacity: 0.9; }
  .op-btn-success { background: var(--op-success); color: #fff; border-color: var(--op-success); }
  .op-btn-success:hover { opacity: 0.9; }
  .op-body { position: relative; }
  .op-loading { padding: 30px; text-align: center; color: var(--op-text-muted); }
  .op-loading .spinner { display: inline-block; width: 20px; height: 20px; border: 2px solid var(--op-border); border-top-color: var(--op-primary); border-radius: 50%; animation: op-spin 0.6s linear infinite; margin-right: 8px; vertical-align: middle; }
  @keyframes op-spin { to { transform: rotate(360deg); } }
  .op-error { padding: 10px 14px; background: #fef2f2; color: var(--op-danger); border-bottom: 1px solid #fecaca; font-size: 12px; }
  .op-success-msg { padding: 10px 14px; background: #f0fdf4; color: var(--op-success); border-bottom: 1px solid #bbf7d0; font-size: 12px; }
  :host(.dark-theme) .op-error { background: #3b1f1f; border-color: #5c2b2b; }
  :host(.dark-theme) .op-success-msg { background: #1a2e1a; border-color: #2e5c2b; }
  .op-empty { padding: 24px; text-align: center; color: var(--op-text-muted); }
  .op-kv { display: grid; grid-template-columns: 150px 1fr; gap: 4px 12px; margin-bottom: 16px; font-size: 12px; }
  .op-kv dt { color: var(--op-text-muted); font-weight: 500; padding: 3px 0; }
  .op-kv dd { margin: 0; padding: 3px 0; word-break: break-all; }
  .op-mono { font-family: var(--op-font-mono); font-size: 12px; }
  .op-muted { color: var(--op-text-muted); font-size: 12px; }
  .op-badge { display: inline-block; padding: 2px 7px; border-radius: 10px; font-size: 11px; font-weight: 500; white-space: nowrap; }
  .op-badge-info { background: #dbeafe; color: #1e40af; }
  .op-badge-muted { background: #f3f4f6; color: #6b7280; }
  :host(.dark-theme) .op-badge-info { background: #1e3a5f; color: #93c5fd; }
  :host(.dark-theme) .op-badge-muted { background: #374151; color: #9ca3af; }
  .op-tabs { display: flex; gap: 0; border-bottom: 2px solid var(--op-border); }
  .op-tab { padding: 8px 16px; cursor: pointer; font-size: 12px; font-weight: 500; border: none; background: none; color: var(--op-text-muted); border-bottom: 2px solid transparent; margin-bottom: -2px; font-family: var(--op-font); }
  .op-tab:hover { color: var(--op-text); }
  .op-tab.active { color: var(--op-primary); border-bottom-color: var(--op-primary); }
  .op-tab-panel { padding: 14px; }
  .op-split { display: grid; grid-template-columns: minmax(260px, 360px) minmax(0, 1fr); min-height: 420px; }
  .op-sidebar { border-right: 1px solid var(--op-border); background: var(--op-bg-alt); }
  .op-panel { padding: 14px; min-width: 0; }
  .op-search { padding: 10px 12px; border-bottom: 1px solid var(--op-border); display: flex; gap: 8px; }
  .op-input, .op-textarea, .op-select {
    width: 100%; padding: 7px 10px; border: 1px solid var(--op-border); border-radius: 4px;
    font-size: 12px; font-family: var(--op-font); background: var(--op-bg); color: var(--op-text);
  }
  .op-input:focus, .op-textarea:focus, .op-select:focus { outline: none; border-color: var(--op-primary); }
  .op-textarea { min-height: 120px; resize: vertical; font-family: var(--op-font-mono); }
  .op-secret-list { max-height: 560px; overflow: auto; }
  .op-secret-row { padding: 10px 12px; border-bottom: 1px solid var(--op-border); cursor: pointer; }
  .op-secret-row:hover { background: var(--op-bg-hover); }
  .op-secret-row.active { background: color-mix(in srgb, var(--op-primary) 10%, var(--op-bg-alt)); }
  .op-secret-row .key { font-family: var(--op-font-mono); font-size: 12px; font-weight: 600; margin-bottom: 2px; }
  .op-secret-row .meta { color: var(--op-text-muted); font-size: 11px; }
  .op-card { border: 1px solid var(--op-border); border-radius: 6px; overflow: hidden; margin-bottom: 16px; }
  .op-card-header { display: flex; align-items: center; justify-content: space-between; gap: 8px; padding: 10px 12px; background: var(--op-bg-alt); border-bottom: 1px solid var(--op-border); }
  .op-card-header h3 { margin: 0; font-size: 13px; font-weight: 600; font-family: var(--op-font-mono); }
  .op-card-body { padding: 12px; }
  .op-secret-controls, .op-form-actions { display: flex; gap: 8px; margin-top: 10px; flex-wrap: wrap; }
  .op-secret-value {
    width: 100%; padding: 9px 10px; border: 1px solid var(--op-border); border-radius: 4px;
    background: var(--op-bg); color: var(--op-text); font-family: var(--op-font-mono); font-size: 12px;
  }
  .op-copy-status { margin-top: 8px; min-height: 16px; color: var(--op-text-muted); font-size: 11px; }
  .op-section { font-size: 13px; font-weight: 600; margin: 16px 0 8px; padding-bottom: 4px; border-bottom: 1px solid var(--op-border); }
  .op-form-group { margin-bottom: 12px; }
  .op-form-label { display: block; font-size: 12px; font-weight: 500; color: var(--op-text-muted); margin-bottom: 4px; }
  .op-form-hint { font-size: 11px; color: var(--op-text-muted); margin-top: 4px; }
  .op-table { width: 100%; border-collapse: collapse; font-size: 12px; }
  .op-table th { text-align: left; padding: 0 0 8px 0; color: var(--op-text-muted); font-weight: 600; }
  .op-table td { padding: 8px 6px 8px 0; border-top: 1px solid var(--op-border); vertical-align: top; }
  .op-table td:last-child, .op-table th:last-child { padding-right: 0; }
  .op-loading-overlay { position: relative; pointer-events: none; opacity: 0.6; }
  .op-loading-overlay::after { content: ''; position: absolute; inset: 0; background: var(--op-bg); opacity: 0.5; z-index: 10; }
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
  shadow.innerHTML = "";
  const styleEl = document.createElement("style"); styleEl.textContent = OPS_STYLES; shadow.appendChild(styleEl);
  const root = document.createElement("div"); shadow.appendChild(root);

  let currentTab = "keys";
  let selectedKey = null;
  let revealSecret = false;
  let filterText = "";
  let copyStatus = "";
  let draftKey = "";
  let draftValue = "";
  let draftPrincipal = "";
  let draftPermission = "READ";
  let hasRendered = false;

  function getScope() { return JSON.parse(model.get("scope_data") || "{}"); }
  function getKeys() { return JSON.parse(model.get("keys_data") || "[]"); }
  function getPermissions() { return JSON.parse(model.get("permissions_data") || "[]"); }
  function getSelectedSecret() { return JSON.parse(model.get("selected_secret_data") || "{}"); }
  function getActionResult() { const raw = model.get("action_result"); return raw ? JSON.parse(raw) : null; }
  function sendRequest(req) { model.set("request", JSON.stringify({ ...req, _t: Date.now() })); model.save_changes(); }

  async function copyToClipboard(text) {
    try {
      if (navigator.clipboard?.writeText) {
        await navigator.clipboard.writeText(text);
        return true;
      }
    } catch (_) {}
    try {
      const ta = document.createElement("textarea");
      ta.value = text;
      ta.style.position = "fixed";
      ta.style.opacity = "0";
      document.body.appendChild(ta);
      ta.focus();
      ta.select();
      const ok = document.execCommand("copy");
      document.body.removeChild(ta);
      return ok;
    } catch (_) {
      return false;
    }
  }

  function resetDraft() {
    draftKey = "";
    draftValue = "";
  }

  function resetPermissionDraft() {
    draftPrincipal = "";
    draftPermission = "READ";
  }

  function syncDraftFromSecret() {
    const secret = getSelectedSecret();
    if (!secret.key) return;
    selectedKey = secret.key;
    draftKey = secret.key || "";
    draftValue = secret.value || "";
  }

  function renderScopeSummary(scope) {
    let html = `<dl class="op-kv">`;
    html += `<dt>Scope</dt><dd class="op-mono">${esc(scope.name || '—')}</dd>`;
    html += `<dt>Backend</dt><dd>${esc(scope.backend_type || '—')}</dd>`;
    if (scope.keyvault_dns_name) html += `<dt>Key Vault DNS</dt><dd class="op-mono">${esc(scope.keyvault_dns_name)}</dd>`;
    if (scope.keyvault_resource_id) html += `<dt>Key Vault Resource</dt><dd class="op-mono">${esc(scope.keyvault_resource_id)}</dd>`;
    html += `</dl>`;
    return html;
  }

  function renderKeysTab(scope, keys, secret) {
    const filteredKeys = keys.filter((k) => !filterText || (k.key || "").toLowerCase().includes(filterText.toLowerCase()));
    let html = `<div class="op-split">`;

    html += `<div class="op-sidebar">`;
    html += `<div class="op-search"><input class="op-input" data-role="filter" type="search" placeholder="Filter keys…" value="${esc(filterText)}"></div>`;
    html += `<div class="op-secret-list">`;
    if (filteredKeys.length) {
      for (const item of filteredKeys) {
        html += `<div class="op-secret-row${selectedKey===item.key?' active':''}" data-key="${esc(item.key)}">`;
        html += `<div class="key">${esc(item.key)}</div>`;
        html += `<div class="meta">${esc(item.last_updated_at || 'Updated time unavailable')}</div>`;
        html += `</div>`;
      }
    } else if (keys.length) {
      html += `<div class="op-empty">No keys match the current filter.</div>`;
    } else {
      html += `<div class="op-empty">This scope has no visible keys yet.</div>`;
    }
    html += `</div></div>`;

    html += `<div class="op-panel">`;
    html += renderScopeSummary(scope);

    html += `<div class="op-section">Selected secret</div>`;
    if (secret.key) {
      html += `<div class="op-card">`;
      html += `<div class="op-card-header"><h3>${esc(secret.key)}</h3><span class="op-muted">${esc(secret.last_updated_at || '')}</span></div>`;
      html += `<div class="op-card-body">`;
      html += `<input class="op-secret-value" data-role="secret-value" ${revealSecret ? 'type="text"' : 'type="password"'} readonly>`;
      html += `<div class="op-secret-controls">`;
      html += `<button class="op-btn" data-action="toggle-visibility">${revealSecret ? '🙈 Hide' : '👁 Show'}</button>`;
      html += `<button class="op-btn op-btn-primary" data-action="copy-secret">⧉ Copy</button>`;
      html += `<button class="op-btn" data-action="reload-secret">↻ Reload</button>`;
      html += `<button class="op-btn op-btn-danger" data-action="delete-secret">🗑 Delete</button>`;
      html += `</div>`;
      html += `<div class="op-copy-status">${esc(copyStatus)}</div>`;
      html += `</div></div>`;
    } else if (selectedKey) {
      html += `<div class="op-empty">Loading <span class="op-mono">${esc(selectedKey)}</span>…</div>`;
    } else {
      html += `<div class="op-empty">Select a key to fetch its value.</div>`;
    }

    html += `<div class="op-section">Add / update secret</div>`;
    html += `<div class="op-form-group">`;
    html += `<label class="op-form-label">Key</label>`;
    html += `<input class="op-input op-mono" data-role="draft-key" placeholder="api-token" value="${esc(draftKey)}">`;
    html += `</div>`;
    html += `<div class="op-form-group">`;
    html += `<label class="op-form-label">Value</label>`;
    html += `<textarea class="op-textarea" data-role="draft-value" placeholder="Enter secret value">${esc(draftValue)}</textarea>`;
    html += `<div class="op-form-hint">Saving will create or overwrite the key in this scope.</div>`;
    html += `</div>`;
    html += `<div class="op-form-actions">`;
    html += `<button class="op-btn op-btn-success" data-action="save-secret">💾 Save</button>`;
    html += `<button class="op-btn" data-action="clear-draft">Clear</button>`;
    if (draftKey) html += `<button class="op-btn op-btn-danger" data-action="delete-draft-key">Delete key</button>`;
    html += `</div>`;

    html += `</div></div>`;
    return html;
  }

  function renderPermissionsTab(scope, permissions) {
    let html = `<div class="op-tab-panel">`;
    html += renderScopeSummary(scope);

    html += `<div class="op-section">Current permissions</div>`;
    if (permissions.length) {
      html += `<div class="op-card"><div class="op-card-body">`;
      html += `<table class="op-table">`;
      html += `<thead><tr><th>Principal</th><th>Permission</th><th></th></tr></thead><tbody>`;
      for (const acl of permissions) {
        html += `<tr>`;
        html += `<td class="op-mono">${esc(acl.principal)}</td>`;
        html += `<td><span class="op-badge op-badge-muted">${esc(acl.permission || '—')}</span></td>`;
        html += `<td style="text-align:right"><button class="op-btn" data-action="edit-acl" data-principal="${esc(acl.principal)}" data-permission="${esc(acl.permission)}">Edit</button></td>`;
        html += `</tr>`;
      }
      html += `</tbody></table>`;
      html += `</div></div>`;
    } else {
      html += `<div class="op-empty">No explicit scope permissions found.</div>`;
    }

    html += `<div class="op-section">Add / update permission</div>`;
    html += `<div class="op-form-group">`;
    html += `<label class="op-form-label">Principal</label>`;
    html += `<input class="op-input op-mono" data-role="draft-principal" placeholder="alice@example.com or data-team" value="${esc(draftPrincipal)}">`;
    html += `</div>`;
    html += `<div class="op-form-group">`;
    html += `<label class="op-form-label">Permission</label>`;
    html += `<select class="op-select" data-role="draft-permission">`;
    for (const p of ["READ", "WRITE", "MANAGE"]) {
      html += `<option value="${p}"${draftPermission===p?' selected':''}>${p}</option>`;
    }
    html += `</select>`;
    html += `<div class="op-form-hint">Saving will add or update this principal's permission on the scope.</div>`;
    html += `</div>`;
    html += `<div class="op-form-actions">`;
    html += `<button class="op-btn op-btn-success" data-action="save-acl">💾 Save permission</button>`;
    html += `<button class="op-btn" data-action="clear-acl-draft">Clear</button>`;
    if (draftPrincipal) html += `<button class="op-btn op-btn-danger" data-action="delete-acl">Delete permission</button>`;
    html += `</div>`;

    html += `</div>`;
    return html;
  }

  function fullRender() {
    const scope = getScope();
    const keys = getKeys();
    const permissions = getPermissions();
    const secret = getSelectedSecret();
    const actionResult = getActionResult();
    const loading = model.get("loading");
    const error = model.get("error_message");

    if (secret.key) selectedKey = secret.key;

    let html = `<div class="op-header">`;
    html += `<h2>🔐 ${esc(scope.name || "Secret Scope")}</h2>`;
    html += `<span class="op-subtitle">${esc(scope.backend_type || "UNKNOWN")}</span>`;
    html += `<div class="op-header-actions">`;
    html += `<span class="op-badge op-badge-info">${keys.length} keys</span>`;
    html += `<span class="op-badge op-badge-muted">${permissions.length} ACLs</span>`;
    html += `<button class="op-btn" data-action="refresh">↻</button>`;
    html += `</div></div>`;

    if (actionResult?.message) {
      html += `<div class="${actionResult.success ? 'op-success-msg' : 'op-error'}">${esc(actionResult.message)}</div>`;
    }
    if (error) html += `<div class="op-error">${esc(error)}</div>`;

    if (loading && !hasRendered) {
      html += `<div class="op-body"><div class="op-loading"><span class="spinner"></span> Loading…</div></div>`;
    } else {
      html += `<div class="op-body${loading ? ' op-loading-overlay' : ''}">`;
      html += `<div class="op-tabs">`;
      html += `<button class="op-tab${currentTab==='keys'?' active':''}" data-tab="keys">Keys</button>`;
      html += `<button class="op-tab${currentTab==='permissions'?' active':''}" data-tab="permissions">Permissions</button>`;
      html += `</div>`;
      html += currentTab === "keys" ? renderKeysTab(scope, keys, secret) : renderPermissionsTab(scope, permissions);
      html += `</div>`;
    }

    root.innerHTML = html;
    hasRendered = true;

    const secretInput = root.querySelector('[data-role="secret-value"]');
    if (secretInput) secretInput.value = secret.value || "";

    bindEvents();
  }

  function bindEvents() {
    root.querySelectorAll('[data-tab]').forEach((btn) => {
      btn.addEventListener('click', () => {
        currentTab = btn.dataset.tab || 'keys';
        fullRender();
      });
    });

    root.querySelector('[data-action="refresh"]')?.addEventListener('click', () => {
      selectedKey = null;
      revealSecret = false;
      copyStatus = "";
      resetDraft();
      resetPermissionDraft();
      sendRequest({ action: 'refresh' });
    });

    root.querySelector('[data-role="filter"]')?.addEventListener('input', (e) => {
      filterText = e.target.value || "";
      fullRender();
    });

    root.querySelector('[data-role="draft-key"]')?.addEventListener('input', (e) => {
      draftKey = e.target.value || "";
    });

    root.querySelector('[data-role="draft-value"]')?.addEventListener('input', (e) => {
      draftValue = e.target.value || "";
    });

    root.querySelector('[data-role="draft-principal"]')?.addEventListener('input', (e) => {
      draftPrincipal = e.target.value || "";
    });

    root.querySelector('[data-role="draft-permission"]')?.addEventListener('change', (e) => {
      draftPermission = e.target.value || 'READ';
    });

    root.querySelectorAll('[data-key]').forEach((row) => {
      row.addEventListener('click', () => {
        currentTab = 'keys';
        selectedKey = row.dataset.key;
        revealSecret = false;
        copyStatus = "";
        sendRequest({ action: 'get_secret', key: selectedKey });
        fullRender();
      });
    });

    root.querySelector('[data-action="toggle-visibility"]')?.addEventListener('click', () => {
      revealSecret = !revealSecret;
      fullRender();
    });

    root.querySelector('[data-action="reload-secret"]')?.addEventListener('click', () => {
      if (!selectedKey) return;
      copyStatus = "";
      sendRequest({ action: 'get_secret', key: selectedKey });
    });

    root.querySelector('[data-action="copy-secret"]')?.addEventListener('click', async () => {
      const secret = getSelectedSecret();
      const ok = await copyToClipboard(secret.value || "");
      copyStatus = ok ? 'Copied to clipboard.' : 'Failed to copy.';
      fullRender();
    });

    root.querySelector('[data-action="save-secret"]')?.addEventListener('click', () => {
      currentTab = 'keys';
      copyStatus = "";
      sendRequest({ action: 'put_secret', key: draftKey, value: draftValue });
    });

    root.querySelector('[data-action="clear-draft"]')?.addEventListener('click', () => {
      resetDraft();
      fullRender();
    });

    root.querySelector('[data-action="delete-secret"]')?.addEventListener('click', () => {
      const secret = getSelectedSecret();
      if (!secret.key) return;
      currentTab = 'keys';
      copyStatus = "";
      sendRequest({ action: 'delete_secret', key: secret.key });
    });

    root.querySelector('[data-action="delete-draft-key"]')?.addEventListener('click', () => {
      if (!draftKey) return;
      currentTab = 'keys';
      copyStatus = "";
      sendRequest({ action: 'delete_secret', key: draftKey });
    });

    root.querySelectorAll('[data-action="edit-acl"]').forEach((btn) => {
      btn.addEventListener('click', () => {
        currentTab = 'permissions';
        draftPrincipal = btn.dataset.principal || "";
        draftPermission = btn.dataset.permission || 'READ';
        fullRender();
      });
    });

    root.querySelector('[data-action="save-acl"]')?.addEventListener('click', () => {
      currentTab = 'permissions';
      sendRequest({ action: 'put_acl', principal: draftPrincipal, permission: draftPermission });
    });

    root.querySelector('[data-action="clear-acl-draft"]')?.addEventListener('click', () => {
      resetPermissionDraft();
      fullRender();
    });

    root.querySelector('[data-action="delete-acl"]')?.addEventListener('click', () => {
      if (!draftPrincipal) return;
      currentTab = 'permissions';
      sendRequest({ action: 'delete_acl', principal: draftPrincipal });
    });
  }

  model.on('change:scope_data', fullRender);
  model.on('change:keys_data', fullRender);
  model.on('change:permissions_data', fullRender);
  model.on('change:selected_secret_data', () => { syncDraftFromSecret(); fullRender(); });
  model.on('change:action_result', fullRender);
  model.on('change:loading', fullRender);
  model.on('change:error_message', fullRender);

  fullRender();
}

export default { render };
