// Principal Widget Frontend
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
  :host(.dark-theme) {
    --op-bg: #1e1e1e; --op-bg-alt: #252526; --op-bg-hover: #2d2d30;
    --op-border: #3e3e42; --op-text: #cccccc; --op-text-muted: #888888;
    --op-primary: #4fc3f7; --op-primary-hover: #29b6f6;
    --op-success: #66bb6a; --op-danger: #ef5350; --op-warning: #ffca28; --op-info: #4dd0e1;
  }
  * { box-sizing: border-box; }
  .op-header { display: flex; align-items: center; gap: 10px; padding: 10px 14px; background: var(--op-bg-alt); border-bottom: 1px solid var(--op-border); flex-wrap: wrap; }
  .op-header h2 { margin: 0; font-size: 14px; font-weight: 600; }
  .op-header .op-subtitle { color: var(--op-text-muted); font-size: 11px; font-family: var(--op-font-mono); }
  .op-header-actions { margin-left: auto; display: flex; gap: 6px; align-items: center; }
  .op-btn { padding: 5px 12px; border: 1px solid var(--op-border); border-radius: 4px; background: var(--op-bg); color: var(--op-text); cursor: pointer; font-size: 12px; font-family: var(--op-font); white-space: nowrap; }
  .op-btn:hover { background: var(--op-bg-hover); }
  .op-btn:disabled { opacity: 0.5; cursor: not-allowed; }
  .op-btn-primary { background: var(--op-primary); color: #fff; border-color: var(--op-primary); }
  .op-btn-primary:hover { background: var(--op-primary-hover); }
  .op-btn-success { background: var(--op-success); color: #fff; border-color: var(--op-success); }
  .op-error { padding: 10px 14px; background: #fef2f2; color: var(--op-danger); border-bottom: 1px solid #fecaca; font-size: 12px; }
  :host(.dark-theme) .op-error { background: #3b1f1f; border-color: #5c2b2b; }
  .op-empty { padding: 24px; text-align: center; color: var(--op-text-muted); font-size: 12px; }
  .op-loading { padding: 18px; text-align: center; color: var(--op-text-muted); font-size: 12px; }
  .op-loading .spinner { display: inline-block; width: 14px; height: 14px; border: 2px solid var(--op-border); border-top-color: var(--op-primary); border-radius: 50%; animation: op-spin 0.6s linear infinite; margin-right: 8px; vertical-align: middle; }
  @keyframes op-spin { to { transform: rotate(360deg); } }
  .op-input, .op-select, .op-textarea {
    padding: 7px 10px; border: 1px solid var(--op-border); border-radius: 4px;
    font-size: 12px; font-family: var(--op-font); background: var(--op-bg); color: var(--op-text);
    min-width: 0;
  }
  .op-input:focus, .op-select:focus, .op-textarea:focus { outline: none; border-color: var(--op-primary); }
  .op-textarea { font-family: var(--op-font-mono); width: 100%; min-height: 220px; resize: vertical; }
  .op-mono { font-family: var(--op-font-mono); font-size: 12px; }
  .op-muted { color: var(--op-text-muted); font-size: 12px; }
  .op-section-title { font-size: 12px; font-weight: 600; color: var(--op-text-muted); text-transform: uppercase; letter-spacing: 0.5px; margin: 14px 0 6px; }
  .op-row { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }
  .op-card { border: 1px solid var(--op-border); border-radius: 6px; margin-bottom: 12px; overflow: hidden; background: var(--op-bg); }
  .op-card-header { padding: 8px 12px; background: var(--op-bg-alt); border-bottom: 1px solid var(--op-border); font-weight: 600; font-size: 12px; display: flex; align-items: center; gap: 8px; }
  .op-card-body { padding: 12px; }
  .op-badge { display: inline-block; padding: 2px 7px; border-radius: 10px; font-size: 11px; font-weight: 500; white-space: nowrap; margin-right: 4px; margin-bottom: 2px; }
  .op-badge-primary { background: #dbeafe; color: #1e40af; }
  .op-badge-info { background: #cffafe; color: #155e75; }
  .op-badge-success { background: #d1fae5; color: #065f46; }
  .op-badge-warning { background: #fef3c7; color: #92400e; }
  .op-badge-danger { background: #fee2e2; color: #991b1b; }
  .op-badge-muted { background: #f3f4f6; color: #6b7280; }
  .op-badge-inherited { background: transparent; color: var(--op-text-muted); border: 1px dashed var(--op-border); }
  :host(.dark-theme) .op-badge-primary { background: #1e3a5f; color: #93c5fd; }
  :host(.dark-theme) .op-badge-info { background: #164e63; color: #67e8f9; }
  :host(.dark-theme) .op-badge-success { background: #064e3b; color: #6ee7b7; }
  :host(.dark-theme) .op-badge-warning { background: #78350f; color: #fcd34d; }
  :host(.dark-theme) .op-badge-danger { background: #7f1d1d; color: #fca5a5; }
  :host(.dark-theme) .op-badge-muted { background: #374151; color: #9ca3af; }
  table.op-table { width: 100%; border-collapse: collapse; font-size: 12px; }
  .op-table th { text-align: left; padding: 6px 10px; font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; color: var(--op-text-muted); background: var(--op-bg-alt); border-bottom: 1px solid var(--op-border); }
  .op-table td { padding: 7px 10px; border-bottom: 1px solid var(--op-border); vertical-align: top; }
  .op-table tr:hover td { background: var(--op-bg-hover); }
  .op-categories { display: grid; grid-template-columns: repeat(auto-fill, minmax(220px, 1fr)); gap: 4px 12px; padding: 8px 12px; border: 1px solid var(--op-border); border-radius: 4px; background: var(--op-bg-alt); }
  .op-categories label { display: flex; gap: 6px; align-items: center; font-size: 12px; cursor: pointer; }
  .op-categories label.deep { color: var(--op-warning); }
  .op-summary { padding: 8px 0; color: var(--op-text-muted); font-size: 12px; }
  .op-progress { font-size: 11px; color: var(--op-text-muted); padding: 6px 0; font-family: var(--op-font-mono); }
  .op-form-grid { display: grid; grid-template-columns: 130px 1fr auto; gap: 8px; align-items: center; margin-bottom: 8px; }
  .op-form-grid label { color: var(--op-text-muted); font-size: 12px; }
  .op-kv { display: grid; grid-template-columns: 160px 1fr; gap: 4px 12px; font-size: 12px; }
  .op-kv dt { color: var(--op-text-muted); font-weight: 500; padding: 3px 0; }
  .op-kv dd { margin: 0; padding: 3px 0; word-break: break-all; }
  .op-tag { display: inline-block; padding: 1px 7px; margin: 1px 3px 1px 0; border-radius: 3px; font-size: 11px; background: var(--op-bg-alt); border: 1px solid var(--op-border); font-family: var(--op-font-mono); }
  .op-pill { display: inline-block; padding: 1px 6px; border-radius: 3px; background: var(--op-bg-alt); border: 1px solid var(--op-border); font-family: var(--op-font-mono); font-size: 11px; }
  .op-body { padding: 14px; }
`;

function esc(s) {
  if (s == null) return "";
  const d = document.createElement("div");
  d.textContent = String(s);
  return d.innerHTML;
}

function _syncTheme(hostEl) {
  hostEl.__cleanupThemeSync?.();
  const media = window.matchMedia("(prefers-color-scheme: dark)");
  const themeSelector = "[data-app-theme], [data-theme], .dark, .dark-theme, .light, .light-theme";
  function parseTheme(v) { if (!v) return null; const s = String(v).toLowerCase(); if (s.includes("dark")) return true; if (s.includes("light")) return false; return null; }
  function bgDark(el) { if (!el) return null; const bg = getComputedStyle(el).backgroundColor; const m = bg && bg.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/i); if (!m) return null; const [, r, g, b] = m.map(Number); return ((0.2126*r + 0.7152*g + 0.0722*b)/255) < 0.5; }
  function fromEl(el) { if (!el) return null; return parseTheme(el.getAttribute?.("data-app-theme")) ?? parseTheme(el.getAttribute?.("data-theme")) ?? parseTheme(el.className) ?? parseTheme(getComputedStyle(el).colorScheme); }
  function isDark() {
    return fromEl(hostEl.closest?.(themeSelector)) ?? fromEl(hostEl) ?? fromEl(hostEl.parentElement) ?? fromEl(document.body) ?? fromEl(document.documentElement) ?? bgDark(hostEl.parentElement) ?? bgDark(document.body) ?? media.matches;
  }
  function apply() { const dark = isDark(); hostEl.classList.toggle("dark-theme", dark); hostEl.style.colorScheme = dark ? "dark" : "light"; }
  apply();
  const obs = new MutationObserver(apply);
  const observed = new Set();
  function observe(el) { if (!el || observed.has(el)) return; obs.observe(el, { attributes: true, attributeFilter: ["data-app-theme", "data-theme", "class", "style"] }); observed.add(el); }
  observe(document.documentElement); observe(document.body); observe(hostEl.parentElement); observe(hostEl.closest?.(themeSelector));
  media.addEventListener("change", apply);
  const cleanup = () => { obs.disconnect(); media.removeEventListener("change", apply); if (hostEl.__cleanupThemeSync === cleanup) delete hostEl.__cleanupThemeSync; };
  hostEl.__cleanupThemeSync = cleanup;
  return cleanup;
}

function permissionBadge(p) {
  const lvl = p.level || "?";
  const cls = p.inherited ? "op-badge-inherited" : "op-badge-primary";
  const title = p.inherited && p.inherited_from ? ` title="inherited from ${esc(p.inherited_from)}"` : "";
  return `<span class="op-badge ${cls}"${title}>${esc(lvl)}${p.inherited ? " ↑" : ""}</span>`;
}

const KIND_LABEL = { user: "User", service_principal: "Service Principal", group: "Group" };
const KIND_BADGE = { user: "op-badge-info", service_principal: "op-badge-muted", group: "op-badge-warning" };

function render({ model, el }) {
  const shadow = el.attachShadow ? el.attachShadow({ mode: "open" }) : el;
  _syncTheme(el);
  shadow.innerHTML = "";
  const styleEl = document.createElement("style");
  styleEl.textContent = OPS_STYLES;
  shadow.appendChild(styleEl);
  const root = document.createElement("div");
  shadow.appendChild(root);

  // ---- state ----
  let selectedCats = new Set();
  let selectedDeep = new Set();
  let cloneTo = "";
  let cloneToType = "auto";
  let initializedDefaults = false;

  function getPrincipal() { return JSON.parse(model.get("principal_data") || "{}"); }
  function getCategories() { return JSON.parse(model.get("categories_data") || "[]"); }
  function getScan() { return JSON.parse(model.get("scan_data") || "{}"); }
  function getProgress() { return JSON.parse(model.get("scan_progress") || "{}"); }
  function getCloneScript() { return model.get("clone_script") || ""; }
  function getCloneResult() { const r = model.get("clone_result"); return r ? JSON.parse(r) : null; }
  function isLoading() { return Boolean(model.get("loading")); }
  function getError() { return model.get("error_message") || ""; }
  function getName() { return model.get("principal_name") || ""; }

  function send(req) {
    model.set("request", JSON.stringify({ ...req, _t: Date.now() }));
    model.save_changes();
  }

  function initDefaults() {
    if (initializedDefaults) return;
    const cats = getCategories();
    if (cats.length) {
      cats.forEach((c) => { if (c.default && !c.deep) selectedCats.add(c.id); });
      initializedDefaults = true;
    }
  }

  // ---- principal info card ----
  function renderPrincipalCard() {
    const p = getPrincipal();
    if (!p || !p.kind) {
      if (isLoading()) return `<div class="op-loading"><span class="spinner"></span>Looking up principal...</div>`;
      return `<div class="op-empty">Principal not found.</div>`;
    }
    const kind = p.kind;
    const kindBadge = `<span class="op-badge ${KIND_BADGE[kind] || "op-badge-muted"}">${esc(KIND_LABEL[kind] || kind)}</span>`;
    const active = p.active === true ? `<span class="op-badge op-badge-success">active</span>`
                : p.active === false ? `<span class="op-badge op-badge-danger">disabled</span>` : "";

    const kvRows = [];
    function row(label, value) {
      if (value == null || value === "" || (Array.isArray(value) && !value.length)) return;
      kvRows.push(`<dt>${esc(label)}</dt><dd>${value}</dd>`);
    }

    row("ID", `<span class="op-mono">${esc(p.id)}</span>`);
    row("Display name", esc(p.display_name));
    if (kind === "user") {
      row("Username", `<span class="op-mono">${esc(p.user_name)}</span>`);
      row("Emails", (p.emails || []).map((e) => `<span class="op-tag">${esc(e.value || e.display)}${e.primary ? " (primary)" : ""}</span>`).join(" "));
    } else if (kind === "service_principal") {
      row("Application ID", `<span class="op-mono">${esc(p.application_id)}</span>`);
    } else if (kind === "group") {
      row("Members", p.members && p.members.length ? `<span class="op-pill">${p.members.length}</span>` : "0");
    }
    row("External ID", p.external_id ? `<span class="op-mono">${esc(p.external_id)}</span>` : null);
    row("Entitlements", (p.entitlements || []).filter(Boolean).map((v) => `<span class="op-tag">${esc(v)}</span>`).join(" "));
    row("Workspace groups", (p.groups || []).map((g) => `<span class="op-tag">${esc(g.display || g.value)}</span>`).join(" "));
    row("Account roles", (p.roles || []).map((r) => `<span class="op-tag">${esc(r.value || r.display)}</span>`).join(" "));

    let membersHtml = "";
    if (kind === "group" && p.members && p.members.length) {
      const sample = p.members.slice(0, 50);
      membersHtml = `
        <div class="op-section-title">Members${p.members.length > 50 ? ` (showing 50 of ${p.members.length})` : ""}</div>
        <div>${sample.map((m) => `<span class="op-tag" title="${esc(m.value || "")}">${esc(m.display || m.value)}</span>`).join(" ")}</div>
      `;
    }

    return `
      <div class="op-card">
        <div class="op-card-header">
          ${kindBadge} <span class="op-mono">${esc(p.primary_identifier || getName())}</span> ${active}
          <button class="op-btn" data-act="refresh" style="margin-left:auto;">Refresh</button>
        </div>
        <div class="op-card-body">
          <dl class="op-kv">${kvRows.join("")}</dl>
          ${membersHtml}
        </div>
      </div>
    `;
  }

  // ---- scan controls ----
  function renderCategoryPicker() {
    const cats = getCategories();
    const top = cats.filter((c) => !c.deep);
    const deep = cats.filter((c) => c.deep);
    const topHtml = top.map((c) => `
      <label><input type="checkbox" data-cat="${esc(c.id)}" ${selectedCats.has(c.id) ? "checked" : ""}/> ${esc(c.label)}</label>
    `).join("");
    const deepHtml = deep.map((c) => `
      <label class="deep" title="Requires enumerating catalogs/schemas — slow"><input type="checkbox" data-deep="${esc(c.id)}" ${selectedDeep.has(c.id) ? "checked" : ""}/> ${esc(c.label)}</label>
    `).join("");
    return `
      <div class="op-section-title">Scan scope</div>
      <div class="op-categories">${topHtml}</div>
      <div style="margin-top:6px;">
        <button class="op-btn" data-act="select-all">Select all</button>
        <button class="op-btn" data-act="select-none">Clear</button>
        <button class="op-btn" data-act="select-defaults">Defaults</button>
        <button class="op-btn op-btn-primary" id="scan-btn" style="margin-left:8px;">Scan permissions</button>
      </div>
      ${deep.length ? `<div class="op-section-title">Deep Unity Catalog scan (slow)</div><div class="op-categories">${deepHtml}</div>` : ""}
    `;
  }

  function renderProgress() {
    const p = getProgress();
    if (!p || (!p.category && !p.scanned)) return "";
    const finished = p.finished;
    const elapsed = p.started ? ((finished || Date.now()/1000) - p.started).toFixed(1) : "?";
    return `<div class="op-progress">
      ${finished ? "✓ done" : "⏳ scanning"}
      ${p.category ? `· <strong>${esc(p.category)}</strong>` : ""}
      · scanned ${p.scanned || 0} · matched ${p.matched || 0} · ${elapsed}s
    </div>`;
  }

  function renderResults() {
    const scan = getScan();
    const principal = getPrincipal();
    if (!scan.principal && !isLoading()) {
      if (principal && principal.kind) {
        return `<div class="op-empty">Choose categories above and click <strong>Scan permissions</strong>.</div>`;
      }
      return "";
    }
    if (isLoading() && !scan.rows) {
      return `<div class="op-loading"><span class="spinner"></span>Scanning...</div>`;
    }
    const rows = scan.rows || [];
    if (!rows.length) {
      return `<div class="op-empty">No permissions found for <span class="op-mono">${esc(scan.principal)}</span> in the selected categories.</div>`;
    }
    const categoryCount = new Set(rows.map((r) => r.category_label)).size;
    const sorted = rows.slice().sort((a, b) => {
      const ca = String(a.category_label || "").toLowerCase();
      const cb = String(b.category_label || "").toLowerCase();
      if (ca !== cb) return ca < cb ? -1 : 1;
      return String(a.name || "").toLowerCase().localeCompare(String(b.name || "").toLowerCase());
    });
    return `
      <div class="op-summary">Found <strong>${rows.length}</strong> grants across <strong>${categoryCount}</strong> categor${categoryCount === 1 ? "y" : "ies"}.</div>
      <div class="op-card">
        <table class="op-table"><thead><tr><th>Type</th><th>Name</th><th>Identifier</th><th>Permissions</th></tr></thead><tbody>
          ${sorted.map((r) => `<tr>
            <td><span class="op-badge op-badge-info">${esc(r.category_label)}</span></td>
            <td>${esc(r.name)}</td>
            <td class="op-mono op-muted">${esc(r.item_id)}</td>
            <td>${(r.permissions || []).map(permissionBadge).join(" ")}</td>
          </tr>`).join("")}
        </tbody></table>
      </div>
    `;
  }

  function renderCloneSection() {
    const scan = getScan();
    if (!scan.rows || !scan.rows.length) return "";
    const script = getCloneScript();
    const cloneRes = getCloneResult();
    return `
      <div class="op-section-title">Clone these permissions to another principal</div>
      <div class="op-form-grid">
        <label>Destination</label>
        <input class="op-input" id="clone-to" placeholder="user@example.com  /  group-name  /  sp-application-id" value="${esc(cloneTo)}"/>
        <select class="op-select" id="clone-to-type">
          <option value="auto" ${cloneToType==="auto"?"selected":""}>auto-detect</option>
          <option value="user" ${cloneToType==="user"?"selected":""}>user</option>
          <option value="group" ${cloneToType==="group"?"selected":""}>group</option>
          <option value="service_principal" ${cloneToType==="service_principal"?"selected":""}>service principal</option>
        </select>
      </div>
      <div class="op-row">
        <button class="op-btn op-btn-primary" data-act="generate-script">Generate script</button>
        <button class="op-btn op-btn-success" data-act="apply-clone">Apply now</button>
        ${cloneRes ? `<span class="op-muted">${esc(cloneRes.message || "")}</span>` : ""}
      </div>
      ${cloneRes && cloneRes.errors && cloneRes.errors.length ? `<div class="op-error" style="margin-top:8px;">
        <strong>Errors:</strong><ul style="margin:4px 0 0 18px;padding:0;">${cloneRes.errors.map((e) => `<li>${esc(e)}</li>`).join("")}</ul>
      </div>` : ""}
      ${script ? `<div style="margin-top:8px;"><div class="op-section-title">Generated script</div>
        <textarea class="op-textarea" readonly>${esc(script)}</textarea>
        <div style="margin-top:6px;"><button class="op-btn" data-act="copy-script">Copy to clipboard</button></div>
      </div>` : ""}
    `;
  }

  function renderHeader() {
    const name = getName();
    return `
      <div class="op-header">
        <h2>Principal</h2>
        <span class="op-subtitle">${esc(name)}</span>
      </div>
    `;
  }

  function renderError() {
    const err = getError();
    if (!err) return "";
    return `<div class="op-error">${esc(err)}</div>`;
  }

  function renderAll() {
    initDefaults();
    root.innerHTML =
      renderHeader() +
      renderError() +
      `<div class="op-body">` +
      renderPrincipalCard() +
      renderCategoryPicker() +
      renderProgress() +
      renderResults() +
      renderCloneSection() +
      `</div>`;
    bindEvents();
  }

  function bindEvents() {
    const refresh = root.querySelector('[data-act="refresh"]');
    if (refresh) refresh.addEventListener("click", () => send({ action: "refresh" }));

    root.querySelectorAll('input[data-cat]').forEach((cb) => {
      cb.addEventListener("change", (e) => {
        const cid = e.target.dataset.cat;
        if (e.target.checked) selectedCats.add(cid); else selectedCats.delete(cid);
      });
    });
    root.querySelectorAll('input[data-deep]').forEach((cb) => {
      cb.addEventListener("change", (e) => {
        const cid = e.target.dataset.deep;
        if (e.target.checked) selectedDeep.add(cid); else selectedDeep.delete(cid);
      });
    });

    const selAct = (act) => {
      const cats = getCategories();
      if (act === "select-all") cats.forEach((c) => { if (!c.deep) selectedCats.add(c.id); });
      else if (act === "select-none") { selectedCats.clear(); selectedDeep.clear(); }
      else if (act === "select-defaults") {
        selectedCats.clear(); selectedDeep.clear();
        cats.forEach((c) => { if (c.default && !c.deep) selectedCats.add(c.id); });
      }
      renderAll();
    };
    root.querySelectorAll('[data-act="select-all"], [data-act="select-none"], [data-act="select-defaults"]').forEach((b) => {
      b.addEventListener("click", () => selAct(b.dataset.act));
    });

    const scanBtn = root.querySelector("#scan-btn");
    if (scanBtn) scanBtn.addEventListener("click", () => {
      send({
        action: "scan",
        categories: Array.from(selectedCats),
        deep: Array.from(selectedDeep),
      });
    });

    const cloneToInp = root.querySelector("#clone-to");
    if (cloneToInp) cloneToInp.addEventListener("input", (e) => { cloneTo = e.target.value; });
    const cloneTypeSel = root.querySelector("#clone-to-type");
    if (cloneTypeSel) cloneTypeSel.addEventListener("change", (e) => { cloneToType = e.target.value; });
    const genBtn = root.querySelector('[data-act="generate-script"]');
    if (genBtn) genBtn.addEventListener("click", () => {
      send({ action: "generate_clone_script", to_principal: cloneTo, to_type: cloneToType === "auto" ? null : cloneToType });
    });
    const applyBtn = root.querySelector('[data-act="apply-clone"]');
    if (applyBtn) applyBtn.addEventListener("click", () => {
      const n = (getScan().rows || []).length;
      if (!confirm(`Apply ${n} grants to ${cloneTo}?`)) return;
      send({ action: "apply_clone", to_principal: cloneTo, to_type: cloneToType === "auto" ? null : cloneToType });
    });
    const copyBtn = root.querySelector('[data-act="copy-script"]');
    if (copyBtn) copyBtn.addEventListener("click", async () => {
      try { await navigator.clipboard.writeText(getCloneScript()); copyBtn.textContent = "Copied!"; setTimeout(() => { copyBtn.textContent = "Copy to clipboard"; }, 1500); } catch (e) { copyBtn.textContent = "Copy failed"; }
    });
  }

  const traits = [
    "principal_data",
    "scan_data",
    "scan_progress",
    "clone_script",
    "clone_result",
    "loading",
    "error_message",
    "categories_data",
    "principal_name",
  ];
  traits.forEach((t) => model.on(`change:${t}`, renderAll));

  renderAll();

  return () => {
    el.__cleanupThemeSync?.();
    traits.forEach((t) => model.off(`change:${t}`, renderAll));
  };
}

export default { render };
