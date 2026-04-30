// Shared frontend for marimo_databricks_connect.ui.* selector widgets.
// Renders a labeled, searchable single-select dropdown with a refresh button.
// Synchronized traitlets:
//   label, placeholder, value, options (JSON list of {value,label,sublabel?}),
//   loading, error_message, request (string – widget-side action protocol).

const STYLES = `
:host {
  --sel-bg: #ffffff;
  --sel-bg-alt: #f8f9fa;
  --sel-bg-hover: #eef2f7;
  --sel-border: #dee2e6;
  --sel-text: #212529;
  --sel-text-muted: #6c757d;
  --sel-primary: #0d6efd;
  --sel-danger: #dc3545;
  --sel-radius: 6px;
  --sel-font: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
  --sel-font-mono: "SF Mono", "Cascadia Code", Menlo, Consolas, monospace;
  display: block;
  font-family: var(--sel-font);
  font-size: 13px;
  color: var(--sel-text);
}
:host(.dark-theme) {
  --sel-bg: #1e1e1e; --sel-bg-alt: #252526; --sel-bg-hover: #2d2d30;
  --sel-border: #3e3e42; --sel-text: #cccccc; --sel-text-muted: #888888;
  --sel-primary: #4fc3f7;
}
* { box-sizing: border-box; }
.sel-root {
  background: var(--sel-bg);
  border: 1px solid var(--sel-border);
  border-radius: var(--sel-radius);
  padding: 8px 10px;
  display: flex; flex-direction: column; gap: 6px;
  min-width: 280px;
}
.sel-header { display: flex; align-items: center; gap: 8px; }
.sel-label { font-weight: 600; font-size: 12px; color: var(--sel-text-muted); flex: 1; }
.sel-value { font-family: var(--sel-font-mono); font-size: 11px; color: var(--sel-primary); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; max-width: 60%; }
.sel-row { display: flex; gap: 6px; align-items: stretch; }
.sel-input {
  flex: 1; padding: 5px 8px; border: 1px solid var(--sel-border);
  border-radius: 4px; background: var(--sel-bg); color: var(--sel-text);
  font-family: var(--sel-font); font-size: 12px;
}
.sel-input:focus { outline: 1px solid var(--sel-primary); border-color: var(--sel-primary); }
.sel-btn {
  padding: 4px 10px; border: 1px solid var(--sel-border); border-radius: 4px;
  background: var(--sel-bg-alt); color: var(--sel-text); cursor: pointer; font-size: 12px;
}
.sel-btn:hover { background: var(--sel-bg-hover); }
.sel-list {
  max-height: 220px; overflow-y: auto;
  border: 1px solid var(--sel-border); border-radius: 4px;
  background: var(--sel-bg);
}
.sel-item {
  padding: 5px 8px; cursor: pointer; border-bottom: 1px solid var(--sel-border);
  display: flex; flex-direction: column; gap: 2px;
}
.sel-item:last-child { border-bottom: none; }
.sel-item:hover { background: var(--sel-bg-hover); }
.sel-item.selected { background: var(--sel-bg-hover); border-left: 3px solid var(--sel-primary); }
.sel-item-main { font-family: var(--sel-font-mono); font-size: 12px; color: var(--sel-text); }
.sel-item-sub { font-size: 10px; color: var(--sel-text-muted); }
.sel-empty { padding: 12px; text-align: center; color: var(--sel-text-muted); font-size: 12px; }
.sel-error { color: var(--sel-danger); font-size: 11px; padding: 4px 0; }
.sel-loading { color: var(--sel-text-muted); font-size: 11px; }
.spinner { display: inline-block; width: 10px; height: 10px; border: 2px solid var(--sel-border); border-top-color: var(--sel-primary); border-radius: 50%; animation: sel-spin 0.6s linear infinite; vertical-align: middle; margin-right: 4px; }
@keyframes sel-spin { to { transform: rotate(360deg); } }
`;

function render({ model, el }) {
  // Theme detection
  try {
    const dark = window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches;
    if (dark) el.classList.add("dark-theme");
  } catch (e) {}

  const style = document.createElement("style");
  style.textContent = STYLES;
  el.appendChild(style);

  const root = document.createElement("div");
  root.className = "sel-root";
  el.appendChild(root);

  const header = document.createElement("div");
  header.className = "sel-header";
  const labelEl = document.createElement("div");
  labelEl.className = "sel-label";
  const valueEl = document.createElement("div");
  valueEl.className = "sel-value";
  header.appendChild(labelEl);
  header.appendChild(valueEl);
  root.appendChild(header);

  const row = document.createElement("div");
  row.className = "sel-row";
  const filter = document.createElement("input");
  filter.type = "text";
  filter.className = "sel-input";
  const refresh = document.createElement("button");
  refresh.className = "sel-btn";
  refresh.textContent = "↻";
  refresh.title = "Refresh options";
  row.appendChild(filter);
  row.appendChild(refresh);
  root.appendChild(row);

  const status = document.createElement("div");
  root.appendChild(status);

  const list = document.createElement("div");
  list.className = "sel-list";
  root.appendChild(list);

  let filterText = "";

  function renderStatus() {
    const loading = model.get("loading");
    const err = model.get("error_message");
    if (err) {
      status.className = "sel-error";
      status.textContent = err;
    } else if (loading) {
      status.className = "sel-loading";
      status.innerHTML = '<span class="spinner"></span> Loading…';
    } else {
      status.className = "";
      status.textContent = "";
    }
  }

  function renderHeader() {
    labelEl.textContent = model.get("label") || "";
    const v = model.get("value") || "";
    valueEl.textContent = v;
    filter.placeholder = model.get("placeholder") || "Filter…";
  }

  function renderList() {
    let opts = [];
    try { opts = JSON.parse(model.get("options") || "[]"); } catch (e) { opts = []; }
    const selected = model.get("value") || "";
    const q = (filterText || "").toLowerCase();
    const filtered = q
      ? opts.filter(o => (o.label || o.value || "").toLowerCase().includes(q) ||
                         (o.sublabel || "").toLowerCase().includes(q) ||
                         (o.value || "").toLowerCase().includes(q))
      : opts;
    list.innerHTML = "";
    if (!filtered.length) {
      const empty = document.createElement("div");
      empty.className = "sel-empty";
      empty.textContent = opts.length ? "No matches." : (model.get("loading") ? "" : "No options.");
      list.appendChild(empty);
      return;
    }
    for (const opt of filtered) {
      const item = document.createElement("div");
      item.className = "sel-item" + (opt.value === selected ? " selected" : "");
      const main = document.createElement("div");
      main.className = "sel-item-main";
      main.textContent = opt.label || opt.value || "";
      item.appendChild(main);
      if (opt.sublabel) {
        const sub = document.createElement("div");
        sub.className = "sel-item-sub";
        sub.textContent = opt.sublabel;
        item.appendChild(sub);
      }
      item.addEventListener("click", () => {
        model.set("value", opt.value);
        model.save_changes();
      });
      list.appendChild(item);
    }
  }

  filter.addEventListener("input", (e) => {
    filterText = e.target.value || "";
    renderList();
  });

  refresh.addEventListener("click", () => {
    model.set("request", JSON.stringify({ action: "refresh", t: Date.now() }));
    model.save_changes();
  });

  model.on("change:options", renderList);
  model.on("change:value", () => { renderHeader(); renderList(); });
  model.on("change:label", renderHeader);
  model.on("change:placeholder", renderHeader);
  model.on("change:loading", () => { renderStatus(); renderList(); });
  model.on("change:error_message", renderStatus);

  renderHeader();
  renderStatus();
  renderList();
}

export default { render };
