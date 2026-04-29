// Shared styles and helpers for operational (single-instance) widgets.
// Each widget frontend imports this via inline concatenation at build time,
// but since anywidget requires self-contained ESM, we export constants/functions.

export const OPS_STYLES = `
  :host {
    --op-bg: #ffffff;
    --op-bg-alt: #f8f9fa;
    --op-bg-hover: #e9ecef;
    --op-border: #dee2e6;
    --op-text: #212529;
    --op-text-muted: #6c757d;
    --op-primary: #0d6efd;
    --op-primary-hover: #0b5ed7;
    --op-success: #198754;
    --op-danger: #dc3545;
    --op-warning: #ffc107;
    --op-info: #0dcaf0;
    --op-font: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
    --op-font-mono: "SF Mono", "Cascadia Code", "Fira Code", Menlo, Consolas, monospace;
    --op-radius: 6px;

    display: block;
    font-family: var(--op-font);
    font-size: 13px;
    color: var(--op-text);
    background: var(--op-bg);
    border: 1px solid var(--op-border);
    border-radius: var(--op-radius);
    overflow: hidden;
  }

  :host(.dark-theme) {
      --op-bg: #1e1e1e;
      --op-bg-alt: #252526;
      --op-bg-hover: #2d2d30;
      --op-border: #3e3e42;
      --op-text: #cccccc;
      --op-text-muted: #888888;
      --op-primary: #4fc3f7;
      --op-primary-hover: #29b6f6;
      --op-success: #66bb6a;
      --op-danger: #ef5350;
      --op-warning: #ffca28;
      --op-info: #4dd0e1;
    }

  * { box-sizing: border-box; }

  .op-header {
    display: flex; align-items: center; gap: 8px;
    padding: 10px 14px; background: var(--op-bg-alt);
    border-bottom: 1px solid var(--op-border); flex-wrap: wrap;
  }
  .op-header h2 { margin: 0; font-size: 14px; font-weight: 600; white-space: nowrap; }
  .op-header .op-subtitle { color: var(--op-text-muted); font-size: 12px; font-family: var(--op-font-mono); }
  .op-header-actions { margin-left: auto; display: flex; gap: 6px; align-items: center; }

  .op-btn {
    padding: 4px 10px; border: 1px solid var(--op-border); border-radius: 4px;
    background: var(--op-bg); color: var(--op-text); cursor: pointer;
    font-size: 12px; font-family: var(--op-font); white-space: nowrap;
    transition: background 0.15s;
  }
  .op-btn:hover { background: var(--op-bg-hover); }
  .op-btn:disabled { opacity: 0.5; cursor: not-allowed; }
  .op-btn-primary { background: var(--op-primary); color: #fff; border-color: var(--op-primary); }
  .op-btn-primary:hover { background: var(--op-primary-hover); }
  .op-btn-danger { background: var(--op-danger); color: #fff; border-color: var(--op-danger); }
  .op-btn-danger:hover { opacity: 0.9; }
  .op-btn-success { background: var(--op-success); color: #fff; border-color: var(--op-success); }
  .op-btn-success:hover { opacity: 0.9; }
  .op-btn-warning { background: var(--op-warning); color: #000; border-color: var(--op-warning); }
  .op-btn-warning:hover { opacity: 0.9; }

  .op-body { max-height: 600px; overflow: auto; }
  .op-loading { padding: 30px; text-align: center; color: var(--op-text-muted); }
  .op-loading .spinner { display: inline-block; width: 20px; height: 20px; border: 2px solid var(--op-border); border-top-color: var(--op-primary); border-radius: 50%; animation: op-spin 0.6s linear infinite; margin-right: 8px; vertical-align: middle; }
  @keyframes op-spin { to { transform: rotate(360deg); } }

  .op-error { padding: 10px 14px; background: #fef2f2; color: var(--op-danger); border-bottom: 1px solid #fecaca; font-size: 12px; }
  :host(.dark-theme) .op-error { background: #3b1f1f; border-color: #5c2b2b; }

  .op-success-msg { padding: 10px 14px; background: #f0fdf4; color: var(--op-success); border-bottom: 1px solid #bbf7d0; font-size: 12px; }
  :host(.dark-theme) .op-success-msg { background: #1a2e1a; border-color: #2e5c2b; }

  .op-empty { padding: 30px; text-align: center; color: var(--op-text-muted); }

  /* Grid for key-value detail */
  .op-detail { padding: 14px; }
  .op-kv { display: grid; grid-template-columns: 150px 1fr; gap: 4px 12px; margin-bottom: 16px; font-size: 12px; }
  .op-kv dt { color: var(--op-text-muted); font-weight: 500; padding: 3px 0; }
  .op-kv dd { margin: 0; padding: 3px 0; word-break: break-all; }

  .op-section { font-size: 13px; font-weight: 600; margin: 16px 0 8px; padding-bottom: 4px; border-bottom: 1px solid var(--op-border); }

  /* Table */
  table.op-table { width: 100%; border-collapse: collapse; }
  .op-table th { text-align: left; padding: 8px 14px; font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; color: var(--op-text-muted); background: var(--op-bg-alt); border-bottom: 1px solid var(--op-border); position: sticky; top: 0; z-index: 1; }
  .op-table td { padding: 7px 14px; border-bottom: 1px solid var(--op-border); vertical-align: top; }
  .op-table tr:hover td { background: var(--op-bg-hover); }
  .op-table tr.clickable { cursor: pointer; }

  /* Badges */
  .op-badge { display: inline-block; padding: 2px 7px; border-radius: 10px; font-size: 11px; font-weight: 500; white-space: nowrap; }
  .op-badge-success { background: #d1fae5; color: #065f46; }
  .op-badge-danger  { background: #fee2e2; color: #991b1b; }
  .op-badge-warning { background: #fef3c7; color: #92400e; }
  .op-badge-info    { background: #dbeafe; color: #1e40af; }
  .op-badge-muted   { background: #f3f4f6; color: #6b7280; }
  :host(.dark-theme) .op-badge-success { background: #064e3b; color: #6ee7b7; }
  :host(.dark-theme) .op-badge-danger { background: #7f1d1d; color: #fca5a5; }
  :host(.dark-theme) .op-badge-warning { background: #78350f; color: #fcd34d; }
  :host(.dark-theme) .op-badge-info { background: #1e3a5f; color: #93c5fd; }
  :host(.dark-theme) .op-badge-muted { background: #374151; color: #9ca3af; }

  /* Tabs */
  .op-tabs { display: flex; gap: 0; border-bottom: 2px solid var(--op-border); margin-bottom: 12px; }
  .op-tab { padding: 6px 16px; cursor: pointer; font-size: 12px; font-weight: 500; border: none; background: none; color: var(--op-text-muted); border-bottom: 2px solid transparent; margin-bottom: -2px; font-family: var(--op-font); }
  .op-tab:hover { color: var(--op-text); }
  .op-tab.active { color: var(--op-primary); border-bottom-color: var(--op-primary); }

  .op-link { color: var(--op-primary); cursor: pointer; text-decoration: none; }
  .op-link:hover { text-decoration: underline; }
  .op-mono { font-family: var(--op-font-mono); font-size: 12px; }
  .op-muted { color: var(--op-text-muted); font-size: 12px; }
  .op-tag { display: inline-block; padding: 1px 6px; margin: 1px 3px 1px 0; border-radius: 3px; font-size: 11px; background: var(--op-bg-alt); border: 1px solid var(--op-border); font-family: var(--op-font-mono); }

  /* Auto-refresh indicator */
  .op-auto-refresh { display: flex; align-items: center; gap: 4px; font-size: 11px; color: var(--op-text-muted); }
  .op-auto-refresh .dot { width: 6px; height: 6px; border-radius: 50%; background: var(--op-success); animation: op-pulse 2s ease-in-out infinite; }
  @keyframes op-pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.3; } }

  /* Logs/pre */
  .op-pre { margin: 0; padding: 10px 14px; font-family: var(--op-font-mono); font-size: 12px; line-height: 1.5; white-space: pre-wrap; word-break: break-all; color: var(--op-text); background: var(--op-bg); }
  .op-pre.error-text { color: var(--op-danger); }

  /* Confirm dialog */
  .op-confirm { padding: 14px; background: var(--op-bg-alt); border-bottom: 1px solid var(--op-border); }
  .op-confirm p { margin: 0 0 10px; font-size: 13px; }
  .op-confirm-actions { display: flex; gap: 8px; }

  /* Status bar at bottom */
  .op-status-bar { padding: 6px 14px; font-size: 11px; color: var(--op-text-muted); background: var(--op-bg-alt); border-top: 1px solid var(--op-border); display: flex; justify-content: space-between; }

  .op-loading-overlay { position: relative; pointer-events: none; opacity: 0.6; }
  .op-loading-overlay::after { content: ''; position: absolute; top: 0; left: 0; right: 0; bottom: 0; background: var(--op-bg); opacity: 0.5; z-index: 10; }
  .op-loading-overlay::before { content: ''; position: absolute; top: 50%; left: 50%; width: 20px; height: 20px; margin: -10px 0 0 -10px; border: 2px solid var(--op-border); border-top-color: var(--op-primary); border-radius: 50%; animation: op-spin 0.6s linear infinite; z-index: 11; }
`;

export function esc(s) {
  if (s == null) return "";
  const d = document.createElement("div");
  d.textContent = String(s);
  return d.innerHTML;
}

export function stateBadge(state) {
  if (!state) return "";
  const s = String(state).toUpperCase();
  let cls = "muted";
  if (["SUCCESS", "RUNNING", "ACTIVE", "ONLINE", "READY"].includes(s)) cls = "success";
  else if (["FAILED", "INTERNAL_ERROR", "TIMEDOUT", "CANCELED", "ERROR", "TERMINATED", "OFFLINE", "DELETING"].includes(s)) cls = "danger";
  else if (["PENDING", "QUEUED", "STARTING", "RESTARTING", "RESIZING", "PROVISIONING", "NOT_READY", "UPDATING"].includes(s)) cls = "info";
  else if (["SKIPPED", "EXCLUDED", "STOPPING", "TERMINATING", "DEGRADED", "WARNING"].includes(s)) cls = "warning";
  return `<span class="op-badge op-badge-${cls}">${esc(s)}</span>`;
}

export function setupTabs(container) {
  container.querySelectorAll(".op-tab").forEach((tab) => {
    tab.addEventListener("click", () => {
      container.querySelectorAll(".op-tab").forEach((t) => t.classList.remove("active"));
      tab.classList.add("active");
      container.querySelectorAll(".op-tab-content").forEach((c) => (c.style.display = "none"));
      const target = container.querySelector(`.op-tab-content[data-tab="${tab.dataset.tab}"]`);
      if (target) target.style.display = "";
    });
  });
}

export function autoRefresh(model, callback, intervalMs = 30000) {
  let timer = null;
  let enabled = true;

  function start() {
    stop();
    if (enabled) {
      timer = setInterval(callback, intervalMs);
    }
  }

  function stop() {
    if (timer) { clearInterval(timer); timer = null; }
  }

  function toggle() {
    enabled = !enabled;
    if (enabled) start(); else stop();
    return enabled;
  }

  start();
  return { start, stop, toggle, isEnabled: () => enabled };
}

export function _syncTheme(hostEl) {
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
