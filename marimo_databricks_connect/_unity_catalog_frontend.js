// Databricks Unity Catalog Browser — anywidget ESM frontend
// Traits: catalogs_data, schemas_data, tables_data, table_detail, volumes_data,
//         sample_data, permissions_data, lineage_data, external_locations_data,
//         location_contents_data, storage_credentials_data, connections_data,
//         external_metadata_data, loading, error_message, request

const STYLES = `
  :host {
    --uc-bg: #ffffff;
    --uc-bg-alt: #f8f9fa;
    --uc-bg-hover: #e9ecef;
    --uc-border: #dee2e6;
    --uc-text: #212529;
    --uc-text-muted: #6c757d;
    --uc-primary: #0d6efd;
    --uc-success: #198754;
    --uc-danger: #dc3545;
    --uc-warning: #ffc107;
    --uc-info: #0dcaf0;
    --uc-font: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
    --uc-mono: "SF Mono", "Cascadia Code", "Fira Code", Menlo, Consolas, monospace;
    --uc-radius: 6px;
    display: block;
    font-family: var(--uc-font);
    font-size: 13px;
    color: var(--uc-text);
    background: var(--uc-bg);
    border: 1px solid var(--uc-border);
    border-radius: var(--uc-radius);
    overflow: hidden;
  }
  :host(.dark-theme) {
      --uc-bg: #1e1e1e; --uc-bg-alt: #252526; --uc-bg-hover: #2d2d30;
      --uc-border: #3e3e42; --uc-text: #cccccc; --uc-text-muted: #888888;
      --uc-primary: #4fc3f7; --uc-success: #66bb6a; --uc-danger: #ef5350;
      --uc-warning: #ffca28; --uc-info: #4dd0e1;
    }
  * { box-sizing: border-box; }

  .uc-header {
    display: flex; align-items: center; gap: 8px;
    padding: 10px 14px; background: var(--uc-bg-alt);
    border-bottom: 1px solid var(--uc-border); flex-wrap: wrap;
  }
  .uc-header h2 { margin: 0; font-size: 14px; font-weight: 600; }
  .uc-header-right { margin-left: auto; display: flex; gap: 6px; }

  .uc-btn {
    padding: 4px 10px; border: 1px solid var(--uc-border); border-radius: 4px;
    background: var(--uc-bg); color: var(--uc-text); cursor: pointer;
    font-size: 12px; font-family: var(--uc-font);
  }
  .uc-btn:hover { background: var(--uc-bg-hover); }
  .uc-btn-primary {
    background: var(--uc-primary); color: #fff; border-color: var(--uc-primary);
  }
  .uc-btn-primary:hover { opacity: 0.9; }
  .uc-btn-sm { padding: 2px 8px; font-size: 11px; }

  /* Tabs */
  .uc-tabs {
    display: flex; gap: 0; background: var(--uc-bg-alt);
    border-bottom: 2px solid var(--uc-border); padding: 0 14px;
    overflow-x: auto;
  }
  .uc-tab {
    padding: 8px 16px; cursor: pointer; font-size: 12px; font-weight: 500;
    border: none; background: none; color: var(--uc-text-muted);
    border-bottom: 2px solid transparent; margin-bottom: -2px;
    font-family: var(--uc-font); white-space: nowrap;
  }
  .uc-tab:hover { color: var(--uc-text); }
  .uc-tab.active { color: var(--uc-primary); border-bottom-color: var(--uc-primary); }

  .uc-body { max-height: 600px; overflow: auto; }

  .uc-loading {
    padding: 30px; text-align: center; color: var(--uc-text-muted);
  }
  .uc-loading .spinner {
    display: inline-block; width: 20px; height: 20px;
    border: 2px solid var(--uc-border); border-top-color: var(--uc-primary);
    border-radius: 50%; animation: uc-spin 0.6s linear infinite;
    margin-right: 8px; vertical-align: middle;
  }
  @keyframes uc-spin { to { transform: rotate(360deg); } }

  .uc-error {
    padding: 10px 14px; background: #fef2f2; color: var(--uc-danger);
    border-bottom: 1px solid #fecaca; font-size: 12px;
  }
  :host(.dark-theme) .uc-error { background: #3b1f1f; border-color: #5c2b2b; }

  .uc-empty { padding: 30px; text-align: center; color: var(--uc-text-muted); }

  /* Breadcrumb */
  .uc-breadcrumb {
    display: flex; align-items: center; gap: 4px;
    padding: 8px 14px; font-size: 12px; color: var(--uc-text-muted);
    border-bottom: 1px solid var(--uc-border); background: var(--uc-bg);
    flex-wrap: wrap;
  }
  .uc-breadcrumb-item {
    cursor: pointer; color: var(--uc-primary);
  }
  .uc-breadcrumb-item:hover { text-decoration: underline; }
  .uc-breadcrumb-sep { color: var(--uc-text-muted); }
  .uc-breadcrumb-current { color: var(--uc-text); font-weight: 500; }

  /* Table */
  table.uc-table { width: 100%; border-collapse: collapse; }
  .uc-table th {
    text-align: left; padding: 8px 14px; font-size: 11px; font-weight: 600;
    text-transform: uppercase; letter-spacing: 0.5px; color: var(--uc-text-muted);
    background: var(--uc-bg-alt); border-bottom: 1px solid var(--uc-border);
    position: sticky; top: 0; z-index: 1;
  }
  .uc-table td {
    padding: 7px 14px; border-bottom: 1px solid var(--uc-border); vertical-align: top;
  }
  .uc-table tr:hover td { background: var(--uc-bg-hover); }
  .uc-table tr.clickable { cursor: pointer; }

  .uc-link { color: var(--uc-primary); cursor: pointer; text-decoration: none; }
  .uc-link:hover { text-decoration: underline; }
  .uc-mono { font-family: var(--uc-mono); font-size: 12px; }
  .uc-muted { color: var(--uc-text-muted); font-size: 12px; }

  /* Badges */
  .uc-badge {
    display: inline-block; padding: 2px 7px; border-radius: 10px;
    font-size: 11px; font-weight: 500; white-space: nowrap;
  }
  .uc-badge-success { background: #d1fae5; color: #065f46; }
  .uc-badge-danger  { background: #fee2e2; color: #991b1b; }
  .uc-badge-warning { background: #fef3c7; color: #92400e; }
  .uc-badge-info    { background: #dbeafe; color: #1e40af; }
  .uc-badge-muted   { background: #f3f4f6; color: #6b7280; }
  .uc-badge-table   { background: #ede9fe; color: #5b21b6; }
  .uc-badge-view    { background: #fce7f3; color: #9d174d; }
  :host(.dark-theme) .uc-badge-success { background: #064e3b; color: #6ee7b7; }
  :host(.dark-theme) .uc-badge-danger { background: #7f1d1d; color: #fca5a5; }
  :host(.dark-theme) .uc-badge-warning { background: #78350f; color: #fcd34d; }
  :host(.dark-theme) .uc-badge-info { background: #1e3a5f; color: #93c5fd; }
  :host(.dark-theme) .uc-badge-muted { background: #374151; color: #9ca3af; }
  :host(.dark-theme) .uc-badge-table { background: #4c1d95; color: #c4b5fd; }
  :host(.dark-theme) .uc-badge-view { background: #831843; color: #f9a8d4; }

  /* Detail panel */
  .uc-detail-panel {
    border-top: 1px solid var(--uc-border);
    padding: 14px;
    background: var(--uc-bg-alt);
  }
  .uc-detail-panel h3 {
    margin: 0 0 10px; font-size: 13px; font-weight: 600;
    display: flex; align-items: center; justify-content: space-between;
  }
  .uc-detail-panel .close-btn {
    background: none; border: none; font-size: 16px; cursor: pointer;
    color: var(--uc-text-muted); padding: 0 4px; line-height: 1;
  }
  .uc-detail-panel .close-btn:hover { color: var(--uc-text); }

  .uc-detail-grid {
    display: grid; grid-template-columns: 160px 1fr;
    gap: 4px 12px; font-size: 12px;
  }
  .uc-detail-grid dt { color: var(--uc-text-muted); font-weight: 500; padding: 3px 0; }
  .uc-detail-grid dd { margin: 0; padding: 3px 0; word-break: break-all; }

  .uc-tag {
    display: inline-block; padding: 1px 6px; margin: 1px 3px 1px 0;
    border-radius: 3px; font-size: 11px; background: var(--uc-bg);
    border: 1px solid var(--uc-border); font-family: var(--uc-mono);
  }

  .uc-section-title {
    font-size: 12px; font-weight: 600; margin: 12px 0 6px;
    padding-bottom: 3px; border-bottom: 1px solid var(--uc-border);
  }

  /* Sub-tabs inside detail panel */
  .uc-sub-tabs {
    display: flex; gap: 0; margin-bottom: 10px;
    border-bottom: 1px solid var(--uc-border);
  }
  .uc-sub-tab {
    padding: 6px 12px; cursor: pointer; font-size: 11px; font-weight: 500;
    border: none; background: none; color: var(--uc-text-muted);
    border-bottom: 2px solid transparent; margin-bottom: -1px;
    font-family: var(--uc-font);
  }
  .uc-sub-tab:hover { color: var(--uc-text); }
  .uc-sub-tab.active { color: var(--uc-primary); border-bottom-color: var(--uc-primary); }

  /* Sample data table */
  .uc-sample-wrap {
    max-height: 300px; overflow: auto; border: 1px solid var(--uc-border);
    border-radius: 4px; margin-top: 6px;
  }
  .uc-sample-table { width: 100%; border-collapse: collapse; font-size: 11px; }
  .uc-sample-table th {
    position: sticky; top: 0; background: var(--uc-bg-alt);
    padding: 5px 8px; font-weight: 600; text-align: left;
    border-bottom: 1px solid var(--uc-border); font-size: 10px;
    text-transform: uppercase; letter-spacing: 0.3px;
    color: var(--uc-text-muted);
  }
  .uc-sample-table td {
    padding: 4px 8px; border-bottom: 1px solid var(--uc-border);
    font-family: var(--uc-mono); max-width: 200px; overflow: hidden;
    text-overflow: ellipsis; white-space: nowrap;
  }

  /* Lineage */
  .uc-lineage-section { margin: 8px 0; }
  .uc-lineage-label { font-size: 11px; font-weight: 600; color: var(--uc-text-muted); margin-bottom: 4px; }
  .uc-lineage-item {
    display: inline-block; padding: 4px 10px; margin: 2px 4px 2px 0;
    border-radius: 4px; font-size: 12px; font-family: var(--uc-mono);
    background: var(--uc-bg); border: 1px solid var(--uc-border);
    cursor: pointer;
  }
  .uc-lineage-item:hover { background: var(--uc-bg-hover); border-color: var(--uc-primary); }
  .uc-lineage-arrow { color: var(--uc-text-muted); margin: 0 4px; }

  /* Permissions */
  .uc-perm-table { width: 100%; border-collapse: collapse; margin-top: 6px; }
  .uc-perm-table th {
    text-align: left; padding: 5px 8px; font-size: 10px; font-weight: 600;
    text-transform: uppercase; letter-spacing: 0.3px;
    color: var(--uc-text-muted); border-bottom: 1px solid var(--uc-border);
    background: var(--uc-bg-alt);
  }
  .uc-perm-table td {
    padding: 4px 8px; border-bottom: 1px solid var(--uc-border);
    font-size: 12px; vertical-align: top;
  }

  /* Pre/code block */
  .uc-code-block {
    margin: 0; padding: 8px 12px; font-family: var(--uc-mono);
    font-size: 11px; line-height: 1.5; white-space: pre-wrap;
    word-break: break-all; background: var(--uc-bg); border-radius: 4px;
    border: 1px solid var(--uc-border); max-height: 260px; overflow: auto;
  }

  /* File browser */
  .uc-file-item {
    display: flex; align-items: center; gap: 8px;
    padding: 6px 14px; border-bottom: 1px solid var(--uc-border);
    cursor: pointer;
  }
  .uc-file-item:hover { background: var(--uc-bg-hover); }
  .uc-file-icon { font-size: 14px; width: 20px; text-align: center; }
  .uc-file-name { flex: 1; font-size: 12px; }
  .uc-file-size { font-size: 11px; color: var(--uc-text-muted); font-family: var(--uc-mono); }

  .uc-action-bar {
    display: flex; gap: 4px; padding: 8px 14px;
    border-bottom: 1px solid var(--uc-border);
    background: var(--uc-bg);
    flex-wrap: wrap;
  }

  /* Filter bar */
  .uc-filter-bar {
    display: flex; align-items: center; gap: 8px;
    padding: 6px 14px; border-bottom: 1px solid var(--uc-border);
    background: var(--uc-bg);
  }
  .uc-filter-input {
    flex: 1; padding: 4px 8px; border: 1px solid var(--uc-border);
    border-radius: 4px; font-size: 12px; font-family: var(--uc-font);
    background: var(--uc-bg); color: var(--uc-text); outline: none;
    min-width: 120px; max-width: 360px;
  }
  .uc-filter-input:focus { border-color: var(--uc-primary); }
  .uc-filter-input::placeholder { color: var(--uc-text-muted); }
  .uc-filter-count { font-size: 11px; color: var(--uc-text-muted); white-space: nowrap; }
  .uc-filter-clear {
    background: none; border: none; color: var(--uc-text-muted); cursor: pointer;
    font-size: 14px; padding: 0 4px; line-height: 1; font-family: var(--uc-font);
  }
  .uc-filter-clear:hover { color: var(--uc-text); }

  /* Sortable headers */
  .uc-table th.sortable { cursor: pointer; user-select: none; }
  .uc-table th.sortable:hover { color: var(--uc-text); }
  .uc-sort-indicator {
    display: inline-block; margin-left: 4px; font-size: 10px;
    color: var(--uc-primary); vertical-align: middle;
  }
`;

// ---- Helpers ----

function esc(s) {
  if (s == null) return "";
  const d = document.createElement("div");
  d.textContent = String(s);
  return d.innerHTML;
}

function typeBadge(tableType) {
  if (!tableType) return "";
  const t = String(tableType).toUpperCase();
  if (t === "MANAGED" || t === "TABLE") return '<span class="uc-badge uc-badge-table">' + esc(t) + "</span>";
  if (t === "VIEW") return '<span class="uc-badge uc-badge-view">VIEW</span>';
  if (t === "EXTERNAL") return '<span class="uc-badge uc-badge-info">EXTERNAL</span>';
  if (t === "MATERIALIZED_VIEW") return '<span class="uc-badge uc-badge-warning">MAT VIEW</span>';
  if (t === "STREAMING_TABLE") return '<span class="uc-badge uc-badge-success">STREAMING</span>';
  return '<span class="uc-badge uc-badge-muted">' + esc(t) + "</span>";
}

function formatBadge(fmt) {
  if (!fmt) return "";
  return '<span class="uc-badge uc-badge-muted">' + esc(fmt) + "</span>";
}

function tableHeader(cols) {
  let h = '<table class="uc-table"><thead><tr>';
  for (const c of cols) h += "<th>" + esc(c) + "</th>";
  h += "</tr></thead><tbody>";
  return h;
}

// Sortable table header: cols = [{key, label}, ...], viewKey = state key
function sortableTableHeader(cols, vs) {
  let h = '<table class="uc-table"><thead><tr>';
  for (const c of cols) {
    const active = vs.sortCol === c.key;
    const arrow = active ? (vs.sortDir === "asc" ? "\u25B2" : "\u25BC") : "";
    h += '<th class="sortable" data-sort-col="' + esc(c.key) + '">' + esc(c.label);
    if (arrow) h += ' <span class="uc-sort-indicator">' + arrow + '</span>';
    h += '</th>';
  }
  h += '</tr></thead><tbody>';
  return h;
}

// Filter bar HTML
function filterBar(viewKey, vs, totalCount, filteredCount) {
  const hasFilter = vs.filter.length > 0;
  let h = '<div class="uc-filter-bar">';
  h += '<input class="uc-filter-input" data-filter-view="' + esc(viewKey) + '" type="text" placeholder="\uD83D\uDD0D Filter\u2026" value="' + esc(vs.filter) + '">';
  if (hasFilter) {
    h += '<button class="uc-filter-clear" data-filter-clear="' + esc(viewKey) + '" title="Clear filter">\u2715</button>';
    h += '<span class="uc-filter-count">' + filteredCount + ' / ' + totalCount + '</span>';
  } else {
    h += '<span class="uc-filter-count">' + totalCount + ' items</span>';
  }
  h += '</div>';
  return h;
}

// Generic filter: returns items where any field value matches the query (case-insensitive)
function filterItems(items, fields, query) {
  if (!query) return items;
  const q = query.toLowerCase();
  return items.filter(item =>
    fields.some(f => {
      const v = item[f];
      if (v == null) return false;
      return String(v).toLowerCase().includes(q);
    })
  );
}

// Generic sort: returns a sorted copy
function sortItems(items, col, dir) {
  if (!col) return items;
  const sorted = [...items];
  const mult = dir === "desc" ? -1 : 1;
  sorted.sort((a, b) => {
    let va = a[col], vb = b[col];
    if (va == null && vb == null) return 0;
    if (va == null) return 1;
    if (vb == null) return -1;
    va = String(va).toLowerCase();
    vb = String(vb).toLowerCase();
    if (va < vb) return -1 * mult;
    if (va > vb) return 1 * mult;
    return 0;
  });
  return sorted;
}

// Toggle sort on a column
function toggleSort(vs, col) {
  if (vs.sortCol === col) {
    vs.sortDir = vs.sortDir === "asc" ? "desc" : "asc";
  } else {
    vs.sortCol = col;
    vs.sortDir = "asc";
  }
}

// Default view state factory
function defaultVS() { return { filter: "", sortCol: null, sortDir: "asc" }; }

function dt(label) { return "<dt>" + esc(label) + "</dt>"; }
function dd(val, mono) {
  const cls = mono ? ' class="uc-mono"' : "";
  return "<dd" + cls + ">" + esc(val != null ? val : "\u2014") + "</dd>";
}

function propsHtml(props) {
  if (!props || !Object.keys(props).length) return "";
  return Object.entries(props).map(([k, v]) =>
    '<span class="uc-tag">' + esc(k) + "=" + esc(v) + "</span>"
  ).join(" ");
}

function formatBytes(bytes) {
  if (bytes == null || bytes === 0) return "\u2014";
  if (bytes < 1024) return bytes + " B";
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + " KB";
  if (bytes < 1024 * 1024 * 1024) return (bytes / (1024 * 1024)).toFixed(1) + " MB";
  return (bytes / (1024 * 1024 * 1024)).toFixed(2) + " GB";
}

// ---- Tab definitions ----

const TABS = [
  { key: "catalog_browser", label: "\uD83D\uDCC1 Catalog Browser" },
  { key: "external_locations", label: "\uD83D\uDCCD External Locations", action: "list_external_locations", dataKey: "external_locations_data" },
  { key: "storage_credentials", label: "\uD83D\uDD11 Credentials", action: "list_storage_credentials", dataKey: "storage_credentials_data" },
  { key: "connections", label: "\uD83D\uDD17 Connections", action: "list_connections", dataKey: "connections_data" },
  { key: "external_metadata", label: "\uD83D\uDCCB External Metadata", action: "list_external_metadata", dataKey: "external_metadata_data" },
];

// ===================================================================
// Main render
// ===================================================================


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
  const styleEl = document.createElement("style");
  styleEl.textContent = STYLES;
  shadow.appendChild(styleEl);

  const root = document.createElement("div");
  shadow.appendChild(root);

  // ---- State ----
  let activeTab = "catalog_browser";
  let loadedTabs = {};

  // Catalog browser state
  let browserPath = { catalog: null, schema: null };
  let selectedTable = null;
  let tableSubTab = "columns";

  // External locations state
  let selectedLocation = null;
  let browseStack = [];

  // Other tab selected items
  let selectedCred = null;
  let selectedConn = null;
  let selectedMeta = null;

  // Per-view filter/sort state
  const vs = {
    catalogs: defaultVS(),
    schemas: defaultVS(),
    tables: defaultVS(),
    columns: defaultVS(),
    ext_locations: defaultVS(),
    credentials: defaultVS(),
    connections: defaultVS(),
    ext_metadata: defaultVS(),
  };

  // Column definitions per view (key must match data field name)
  const VIEW_COLS = {
    catalogs:       [{key:"name",label:"Name"},{key:"catalog_type",label:"Type"},{key:"owner",label:"Owner"},{key:"comment",label:"Comment"},{key:"created_at",label:"Created"}],
    schemas:        [{key:"name",label:"Name"},{key:"owner",label:"Owner"},{key:"comment",label:"Comment"},{key:"created_at",label:"Created"}],
    tables:         [{key:"name",label:"Name"},{key:"table_type",label:"Type"},{key:"data_source_format",label:"Format"},{key:"owner",label:"Owner"},{key:"comment",label:"Comment"}],
    columns:        [{key:"position",label:"#"},{key:"name",label:"Name"},{key:"type_text",label:"Type"},{key:"nullable",label:"Nullable"},{key:"partition_index",label:"Partition"},{key:"comment",label:"Comment"}],
    ext_locations:  [{key:"name",label:"Name"},{key:"url",label:"URL"},{key:"credential_name",label:"Credential"},{key:"owner",label:"Owner"},{key:"read_only",label:"Read-only"}],
    credentials:    [{key:"name",label:"Name"},{key:"credential_type",label:"Type"},{key:"owner",label:"Owner"},{key:"read_only",label:"Read-only"},{key:"used_for_managed_storage",label:"Managed Storage"},{key:"created_at",label:"Created"}],
    connections:    [{key:"name",label:"Name"},{key:"connection_type",label:"Type"},{key:"credential_type",label:"Credential"},{key:"owner",label:"Owner"},{key:"url",label:"URL"}],
    ext_metadata:   [{key:"name",label:"Name"},{key:"entity_type",label:"Entity Type"},{key:"system_type",label:"System Type"},{key:"owner",label:"Owner"},{key:"url",label:"URL"}],
  };

  // ---- Send request to Python ----
  function sendRequest(req) {
    model.set("request", JSON.stringify({ ...req, _ts: Date.now() }));
    model.save_changes();
  }

  // ---- Full render ----
  function fullRender() {
    let html = "";

    // Header
    html += '<div class="uc-header"><h2>\uD83C\uDFDB Unity Catalog</h2>';
    html += '<div class="uc-header-right">';
    html += '<button class="uc-btn" data-action="refresh">\u21BB Refresh</button>';
    html += '</div></div>';

    // Tabs
    html += '<div class="uc-tabs">';
    for (const t of TABS) {
      const act = t.key === activeTab ? " active" : "";
      html += '<button class="uc-tab' + act + '" data-tab="' + t.key + '">' + t.label + "</button>";
    }
    html += "</div>";

    // Error
    const err = model.get("error_message");
    if (err) html += '<div class="uc-error">' + esc(err) + "</div>";

    // Body
    html += '<div class="uc-body">';

    if (model.get("loading")) {
      html += '<div class="uc-loading"><span class="spinner"></span> Loading\u2026</div>';
    } else {
      if (activeTab === "catalog_browser") {
        html += renderCatalogBrowser();
      } else if (activeTab === "external_locations") {
        html += renderExternalLocations();
      } else if (activeTab === "storage_credentials") {
        html += renderStorageCredentials();
      } else if (activeTab === "connections") {
        html += renderConnections();
      } else if (activeTab === "external_metadata") {
        html += renderExternalMetadata();
      }
    }

    html += "</div>"; // body
    root.innerHTML = html;
    bindEvents();
  }

  // ================ CATALOG BROWSER ================

  function renderCatalogBrowser() {
    let html = "";

    // Breadcrumb
    html += '<div class="uc-breadcrumb">';
    html += '<span class="uc-breadcrumb-item" data-nav="root">\uD83C\uDFDB Catalogs</span>';
    if (browserPath.catalog) {
      html += '<span class="uc-breadcrumb-sep">\u203A</span>';
      if (browserPath.schema) {
        html += '<span class="uc-breadcrumb-item" data-nav="catalog">' + esc(browserPath.catalog) + '</span>';
        html += '<span class="uc-breadcrumb-sep">\u203A</span>';
        html += '<span class="uc-breadcrumb-current">' + esc(browserPath.schema) + '</span>';
      } else {
        html += '<span class="uc-breadcrumb-current">' + esc(browserPath.catalog) + '</span>';
      }
    }
    html += '</div>';

    if (!browserPath.catalog) {
      // List catalogs
      html += renderCatalogsList();
    } else if (!browserPath.schema) {
      // List schemas
      html += renderSchemasList();
    } else {
      // List tables + detail
      html += renderTablesList();
    }

    return html;
  }

  function renderCatalogsList() {
    const allCatalogs = JSON.parse(model.get("catalogs_data") || "[]");
    if (!allCatalogs.length) return '<div class="uc-empty">No catalogs found.</div>';

    const v = vs.catalogs;
    const colDefs = VIEW_COLS.catalogs;
    const fields = colDefs.map(c => c.key);
    const filtered = filterItems(allCatalogs, fields, v.filter);
    const catalogs = sortItems(filtered, v.sortCol, v.sortDir);

    let html = filterBar("catalogs", v, allCatalogs.length, catalogs.length);
    html += sortableTableHeader(colDefs, v);
    for (const c of catalogs) {
      html += '<tr class="clickable" data-catalog="' + esc(c.name) + '">';
      html += '<td><span class="uc-link">\uD83D\uDCC1 ' + esc(c.name) + '</span></td>';
      html += '<td>' + (c.catalog_type ? '<span class="uc-badge uc-badge-muted">' + esc(c.catalog_type) + '</span>' : '') + '</td>';
      html += '<td class="uc-muted">' + esc(c.owner || "\u2014") + '</td>';
      html += '<td class="uc-muted">' + esc(c.comment ? (c.comment.length > 50 ? c.comment.slice(0, 47) + "\u2026" : c.comment) : "\u2014") + '</td>';
      html += '<td class="uc-muted">' + esc(c.created_at || "\u2014") + '</td>';
      html += '</tr>';
    }
    html += '</tbody></table>';
    return html;
  }

  function renderSchemasList() {
    const allSchemas = JSON.parse(model.get("schemas_data") || "[]");
    if (!allSchemas.length) return '<div class="uc-empty">No schemas found in ' + esc(browserPath.catalog) + '.</div>';

    const v = vs.schemas;
    const colDefs = VIEW_COLS.schemas;
    const fields = colDefs.map(c => c.key);
    const filtered = filterItems(allSchemas, fields, v.filter);
    const schemas = sortItems(filtered, v.sortCol, v.sortDir);

    let html = filterBar("schemas", v, allSchemas.length, schemas.length);
    html += sortableTableHeader(colDefs, v);
    for (const s of schemas) {
      html += '<tr class="clickable" data-schema="' + esc(s.name) + '">';
      html += '<td><span class="uc-link">\uD83D\uDCC2 ' + esc(s.name) + '</span></td>';
      html += '<td class="uc-muted">' + esc(s.owner || "\u2014") + '</td>';
      html += '<td class="uc-muted">' + esc(s.comment ? (s.comment.length > 50 ? s.comment.slice(0, 47) + "\u2026" : s.comment) : "\u2014") + '</td>';
      html += '<td class="uc-muted">' + esc(s.created_at || "\u2014") + '</td>';
      html += '</tr>';
    }
    html += '</tbody></table>';

    // Permissions button
    html += '<div class="uc-action-bar">';
    html += '<button class="uc-btn uc-btn-sm" data-action="catalog-perms">\uD83D\uDD12 Catalog Permissions</button>';
    html += '</div>';

    return html;
  }

  function renderTablesList() {
    const allTables = JSON.parse(model.get("tables_data") || "[]");
    const volumes = JSON.parse(model.get("volumes_data") || "[]");

    let html = '';

    // Action bar
    html += '<div class="uc-action-bar">';
    html += '<button class="uc-btn uc-btn-sm" data-action="schema-perms">\uD83D\uDD12 Schema Permissions</button>';
    html += '<button class="uc-btn uc-btn-sm" data-action="list-volumes">\uD83D\uDCC0 Volumes</button>';
    html += '</div>';

    if (!allTables.length && !volumes.length) return html + '<div class="uc-empty">No tables found in ' + esc(browserPath.catalog + '.' + browserPath.schema) + '.</div>';

    // Tables list
    if (allTables.length) {
      const v = vs.tables;
      const colDefs = VIEW_COLS.tables;
      const fields = colDefs.map(c => c.key);
      const filtered = filterItems(allTables, fields, v.filter);
      const tables = sortItems(filtered, v.sortCol, v.sortDir);

      html += filterBar("tables", v, allTables.length, tables.length);
      html += sortableTableHeader(colDefs, v);
      for (const t of tables) {
        const fn = t.full_name || (browserPath.catalog + "." + browserPath.schema + "." + t.name);
        html += '<tr class="clickable" data-table="' + esc(fn) + '">';
        html += '<td><span class="uc-link">' + esc(t.name) + '</span></td>';
        html += '<td>' + typeBadge(t.table_type) + '</td>';
        html += '<td>' + formatBadge(t.data_source_format) + '</td>';
        html += '<td class="uc-muted">' + esc(t.owner || "\u2014") + '</td>';
        html += '<td class="uc-muted">' + esc(t.comment ? (t.comment.length > 40 ? t.comment.slice(0, 37) + "\u2026" : t.comment) : "\u2014") + '</td>';
        html += '</tr>';
      }
      html += '</tbody></table>';
    }

    // Volumes list
    if (volumes.length) {
      html += '<div class="uc-section-title">\uD83D\uDCC0 Volumes</div>';
      const cols = ["Name", "Type", "Owner", "Storage Location"];
      html += tableHeader(cols);
      for (const v of volumes) {
        html += '<tr>';
        html += '<td>' + esc(v.name) + '</td>';
        html += '<td>' + (v.volume_type ? '<span class="uc-badge uc-badge-muted">' + esc(v.volume_type) + '</span>' : '') + '</td>';
        html += '<td class="uc-muted">' + esc(v.owner || "\u2014") + '</td>';
        html += '<td class="uc-mono uc-muted">' + esc(v.storage_location || "\u2014") + '</td>';
        html += '</tr>';
      }
      html += '</tbody></table>';
    }

    // Table detail
    if (selectedTable) {
      html += renderTableDetail();
    }

    // Permissions overlay
    const permsData = JSON.parse(model.get("permissions_data") || "[]");
    if (permsData && permsData.permissions) {
      html += renderPermissions(permsData);
    }

    return html;
  }

  function renderTableDetail() {
    const detail = JSON.parse(model.get("table_detail") || "{}");
    if (!detail.name) return "";

    let html = '<div class="uc-detail-panel">';
    html += '<h3>' + esc(detail.full_name || detail.name);
    html += '<button class="close-btn" data-action="close-table-detail">\u2715</button></h3>';

    // Sub-tabs
    html += '<div class="uc-sub-tabs">';
    for (const [key, label] of [["columns", "Columns"], ["details", "Details"], ["sample", "Sample Data"], ["lineage", "Lineage"], ["permissions", "Permissions"]]) {
      const act = key === tableSubTab ? " active" : "";
      html += '<button class="uc-sub-tab' + act + '" data-subtab="' + key + '">' + label + '</button>';
    }
    html += '</div>';

    if (tableSubTab === "columns") {
      html += renderColumnsTab(detail);
    } else if (tableSubTab === "details") {
      html += renderDetailsTab(detail);
    } else if (tableSubTab === "sample") {
      html += renderSampleTab();
    } else if (tableSubTab === "lineage") {
      html += renderLineageTab();
    } else if (tableSubTab === "permissions") {
      html += renderTablePermissions();
    }

    html += '</div>';
    return html;
  }

  function renderColumnsTab(detail) {
    if (!detail.columns || !detail.columns.length) return '<div class="uc-empty">No column information available.</div>';

    const v = vs.columns;
    const colDefs = VIEW_COLS.columns;
    const allCols = detail.columns.map(c => ({
      ...c,
      type_text: c.type_text || c.type_name || null,
    }));
    const fields = colDefs.map(c => c.key);
    const filtered = filterItems(allCols, fields, v.filter);
    const cols = sortItems(filtered, v.sortCol, v.sortDir);

    let html = filterBar("columns", v, allCols.length, cols.length);
    html += sortableTableHeader(colDefs, v);
    for (const col of cols) {
      html += '<tr class="clickable" data-col-lineage="' + esc(col.name) + '">';
      html += '<td class="uc-muted">' + esc(col.position != null ? col.position : "") + '</td>';
      html += '<td class="uc-mono">' + esc(col.name) + '</td>';
      html += '<td class="uc-mono uc-muted">' + esc(col.type_text || "\u2014") + '</td>';
      html += '<td>' + (col.nullable === false ? '<span class="uc-badge uc-badge-danger">NOT NULL</span>' : '<span class="uc-badge uc-badge-muted">nullable</span>') + '</td>';
      html += '<td>' + (col.partition_index != null ? '<span class="uc-badge uc-badge-info">P' + esc(col.partition_index) + '</span>' : '') + '</td>';
      html += '<td class="uc-muted">' + esc(col.comment || "") + '</td>';
      html += '</tr>';
    }
    html += '</tbody></table>';
    html += '<div class="uc-muted" style="padding:6px 14px;font-size:11px;">\uD83D\uDCA1 Click a column to view its lineage</div>';
    return html;
  }

  function renderDetailsTab(detail) {
    let html = '<dl class="uc-detail-grid">';
    html += dt("Full Name") + dd(detail.full_name, true);
    html += dt("Table Type") + '<dd>' + typeBadge(detail.table_type) + '</dd>';
    if (detail.data_source_format) html += dt("Format") + dd(detail.data_source_format, true);
    html += dt("Owner") + dd(detail.owner);
    if (detail.comment) html += dt("Comment") + dd(detail.comment);
    if (detail.storage_location) html += dt("Storage Location") + dd(detail.storage_location, true);
    if (detail.storage_credential_name) html += dt("Storage Credential") + dd(detail.storage_credential_name);
    if (detail.view_definition) html += dt("View Definition") + dd(detail.view_definition, true);
    if (detail.sql_path) html += dt("SQL Path") + dd(detail.sql_path, true);
    html += dt("Table ID") + dd(detail.table_id, true);
    html += dt("Created") + dd(detail.created_at);
    html += dt("Created By") + dd(detail.created_by);
    if (detail.updated_at) html += dt("Updated") + dd(detail.updated_at);
    if (detail.updated_by) html += dt("Updated By") + dd(detail.updated_by);
    html += '</dl>';

    if (detail.properties && Object.keys(detail.properties).length) {
      html += '<div class="uc-section-title">Properties</div>';
      html += '<div class="uc-code-block">' + esc(JSON.stringify(detail.properties, null, 2)) + '</div>';
    }

    if (detail.view_definition) {
      html += '<div class="uc-section-title">View Definition</div>';
      html += '<div class="uc-code-block">' + esc(detail.view_definition) + '</div>';
    }

    return html;
  }

  function renderSampleTab() {
    const sd = JSON.parse(model.get("sample_data") || "{}");
    if (!sd.columns) {
      return '<div class="uc-empty">' +
        '<button class="uc-btn uc-btn-primary" data-action="load-sample">Load Sample Data (50 rows)</button>' +
        '</div>';
    }
    if (sd.table !== selectedTable) {
      return '<div class="uc-empty">' +
        '<button class="uc-btn uc-btn-primary" data-action="load-sample">Load Sample Data (50 rows)</button>' +
        '</div>';
    }

    let html = '<div class="uc-sample-wrap"><table class="uc-sample-table"><thead><tr>';
    for (const col of sd.columns) html += '<th>' + esc(col) + '</th>';
    html += '</tr></thead><tbody>';
    for (const row of sd.rows) {
      html += '<tr>';
      for (const cell of row) html += '<td title="' + esc(cell) + '">' + esc(cell != null ? cell : "NULL") + '</td>';
      html += '</tr>';
    }
    html += '</tbody></table></div>';
    html += '<div class="uc-muted" style="padding:4px 0;font-size:11px;">Showing ' + sd.rows.length + ' rows</div>';
    return html;
  }

  function renderLineageTab() {
    const ld = JSON.parse(model.get("lineage_data") || "{}");

    let html = '<div style="margin-bottom:8px">';
    html += '<button class="uc-btn uc-btn-sm" data-action="load-table-lineage">\uD83D\uDD04 Load Table Lineage</button>';
    html += '</div>';

    if (!ld.table || ld.table !== selectedTable) {
      return html + '<div class="uc-empty uc-muted">Click the button above to load lineage data.</div>';
    }

    if (ld.type === "table") {
      // Table lineage
      html += '<div class="uc-lineage-section">';
      html += '<div class="uc-lineage-label">\u2B06 Upstream Tables (' + (ld.upstream || []).length + ')</div>';
      if (ld.upstream && ld.upstream.length) {
        for (const t of ld.upstream) {
          const fn = [t.catalog_name, t.schema_name, t.name].filter(Boolean).join(".");
          html += '<span class="uc-lineage-item" data-nav-table="' + esc(fn) + '">' + esc(fn || t.name) + '</span>';
        }
      } else {
        html += '<div class="uc-muted">No upstream tables found.</div>';
      }
      html += '</div>';

      html += '<div class="uc-lineage-section">';
      html += '<div class="uc-lineage-label">\u2B07 Downstream Tables (' + (ld.downstream || []).length + ')</div>';
      if (ld.downstream && ld.downstream.length) {
        for (const t of ld.downstream) {
          const fn = [t.catalog_name, t.schema_name, t.name].filter(Boolean).join(".");
          html += '<span class="uc-lineage-item" data-nav-table="' + esc(fn) + '">' + esc(fn || t.name) + '</span>';
        }
      } else {
        html += '<div class="uc-muted">No downstream tables found.</div>';
      }
      html += '</div>';
    } else if (ld.type === "column") {
      // Column lineage
      html += '<div class="uc-lineage-section">';
      html += '<div class="uc-lineage-label">\uD83D\uDD0D Column Lineage for: <span class="uc-mono">' + esc(ld.column) + '</span></div>';

      html += '<div class="uc-lineage-label" style="margin-top:8px">\u2B06 Upstream Columns (' + (ld.upstream_cols || []).length + ')</div>';
      if (ld.upstream_cols && ld.upstream_cols.length) {
        for (const c of ld.upstream_cols) {
          const fn = [c.catalog_name, c.schema_name, c.table_name, c.name].filter(Boolean).join(".");
          html += '<span class="uc-lineage-item">' + esc(fn) + '</span>';
        }
      } else {
        html += '<div class="uc-muted">No upstream columns found.</div>';
      }

      html += '<div class="uc-lineage-label" style="margin-top:8px">\u2B07 Downstream Columns (' + (ld.downstream_cols || []).length + ')</div>';
      if (ld.downstream_cols && ld.downstream_cols.length) {
        for (const c of ld.downstream_cols) {
          const fn = [c.catalog_name, c.schema_name, c.table_name, c.name].filter(Boolean).join(".");
          html += '<span class="uc-lineage-item">' + esc(fn) + '</span>';
        }
      } else {
        html += '<div class="uc-muted">No downstream columns found.</div>';
      }
      html += '</div>';
    }

    return html;
  }

  function renderTablePermissions() {
    const pd = JSON.parse(model.get("permissions_data") || "{}");

    let html = '<div style="margin-bottom:8px">';
    html += '<button class="uc-btn uc-btn-sm" data-action="load-table-perms">\uD83D\uDD04 Load Permissions</button>';
    html += '</div>';

    if (!pd.permissions || pd.full_name !== selectedTable) {
      return html + '<div class="uc-empty uc-muted">Click the button above to load permissions.</div>';
    }

    return html + renderPermissionsTable(pd.permissions);
  }

  function renderPermissions(permsData) {
    let html = '<div class="uc-detail-panel">';
    html += '<h3>\uD83D\uDD12 Permissions: ' + esc(permsData.securable_type) + ' ' + esc(permsData.full_name);
    html += '<button class="close-btn" data-action="close-perms">\u2715</button></h3>';
    html += renderPermissionsTable(permsData.permissions);
    html += '</div>';
    return html;
  }

  function renderPermissionsTable(permissions) {
    if (!permissions || !permissions.length) return '<div class="uc-muted">No permissions found.</div>';

    let html = '<table class="uc-perm-table"><thead><tr>';
    html += '<th>Principal</th><th>Privileges</th>';
    html += '</tr></thead><tbody>';
    for (const pa of permissions) {
      html += '<tr>';
      html += '<td><strong>' + esc(pa.principal) + '</strong></td>';
      html += '<td>';
      for (const p of (pa.privileges || [])) {
        let badge = '<span class="uc-badge uc-badge-info">' + esc(p.privilege) + '</span> ';
        if (p.inherited_from_name) {
          badge += '<span class="uc-muted">(from ' + esc(p.inherited_from_type) + ': ' + esc(p.inherited_from_name) + ')</span> ';
        }
        html += badge;
      }
      html += '</td>';
      html += '</tr>';
    }
    html += '</tbody></table>';
    return html;
  }

  // ================ EXTERNAL LOCATIONS ================

  function renderExternalLocations() {
    const allLocs = JSON.parse(model.get("external_locations_data") || "[]");
    if (!allLocs.length) return '<div class="uc-empty">No external locations found.</div>';

    const v = vs.ext_locations;
    const colDefs = VIEW_COLS.ext_locations;
    const fields = colDefs.map(c => c.key);
    const filtered = filterItems(allLocs, fields, v.filter);
    const locs = sortItems(filtered, v.sortCol, v.sortDir);

    let html = filterBar("ext_locations", v, allLocs.length, locs.length);
    html += sortableTableHeader(colDefs, v);
    for (const loc of locs) {
      const sel = selectedLocation === loc.name ? ' style="background:color-mix(in srgb,var(--uc-primary) 10%,var(--uc-bg))"' : '';
      html += '<tr class="clickable" data-ext-loc="' + esc(loc.name) + '"' + sel + '>';
      html += '<td><span class="uc-link">' + esc(loc.name) + '</span></td>';
      html += '<td class="uc-mono uc-muted" style="max-width:250px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="' + esc(loc.url) + '">' + esc(loc.url || "\u2014") + '</td>';
      html += '<td class="uc-muted">' + esc(loc.credential_name || "\u2014") + '</td>';
      html += '<td class="uc-muted">' + esc(loc.owner || "\u2014") + '</td>';
      html += '<td>' + (loc.read_only ? '<span class="uc-badge uc-badge-warning">RO</span>' : '') + '</td>';
      html += '</tr>';
    }
    html += '</tbody></table>';

    // Detail + browse panel
    if (selectedLocation) {
      const loc = allLocs.find(l => l.name === selectedLocation);
      if (loc) {
        html += '<div class="uc-detail-panel">';
        html += '<h3>' + esc(loc.name);
        html += '<span><button class="uc-btn uc-btn-sm" data-action="browse-loc" data-url="' + esc(loc.url) + '">\uD83D\uDCC2 Browse</button> ';
        html += '<button class="uc-btn uc-btn-sm" data-action="loc-perms">\uD83D\uDD12 Permissions</button> ';
        html += '<button class="close-btn" data-action="close-ext-loc">\u2715</button></span></h3>';

        html += '<dl class="uc-detail-grid">';
        html += dt("URL") + dd(loc.url, true);
        html += dt("Credential") + dd(loc.credential_name);
        html += dt("Owner") + dd(loc.owner);
        if (loc.read_only != null) html += dt("Read-only") + dd(loc.read_only ? "Yes" : "No");
        if (loc.isolation_mode) html += dt("Isolation") + dd(loc.isolation_mode);
        if (loc.fallback != null) html += dt("Fallback") + dd(loc.fallback ? "Yes" : "No");
        html += dt("Created") + dd(loc.created_at);
        html += dt("Created By") + dd(loc.created_by);
        if (loc.updated_at) html += dt("Updated") + dd(loc.updated_at);
        if (loc.comment) html += dt("Comment") + dd(loc.comment);
        html += '</dl>';

        // File browser
        html += renderFileBrowser();

        html += '</div>';
      }
    }

    // Permissions overlay
    const permsData = JSON.parse(model.get("permissions_data") || "{}");
    if (permsData && permsData.permissions && permsData.securable_type === "external_location") {
      html += renderPermissions(permsData);
    }

    return html;
  }

  function renderFileBrowser() {
    const contents = JSON.parse(model.get("location_contents_data") || "{}");
    if (!contents.items) return "";

    let html = '<div class="uc-section-title">\uD83D\uDCC2 Contents of ' + esc(contents.url) + '</div>';

    // Back button if we have a browse stack
    if (browseStack.length > 1) {
      html += '<div style="padding:4px 0"><button class="uc-btn uc-btn-sm" data-action="browse-back">\u2190 Back</button></div>';
    }

    if (!contents.items.length) {
      html += '<div class="uc-muted" style="padding:8px 0">Empty directory.</div>';
      return html;
    }

    for (const item of contents.items) {
      html += '<div class="uc-file-item" data-browse-path="' + esc(item.path) + '" data-is-dir="' + (item.is_dir ? "1" : "0") + '">';
      html += '<span class="uc-file-icon">' + (item.is_dir ? "\uD83D\uDCC1" : "\uD83D\uDCC4") + '</span>';
      html += '<span class="uc-file-name">' + esc(item.name) + '</span>';
      html += '<span class="uc-file-size">' + (item.is_dir ? "" : formatBytes(item.size)) + '</span>';
      html += '</div>';
    }

    return html;
  }

  // ================ STORAGE CREDENTIALS ================

  function renderStorageCredentials() {
    const allCreds = JSON.parse(model.get("storage_credentials_data") || "[]");
    if (!allCreds.length) return '<div class="uc-empty">No storage credentials found.</div>';

    const v = vs.credentials;
    const colDefs = VIEW_COLS.credentials;
    const fields = colDefs.map(c => c.key);
    const filtered = filterItems(allCreds, fields, v.filter);
    const creds = sortItems(filtered, v.sortCol, v.sortDir);

    let html = filterBar("credentials", v, allCreds.length, creds.length);
    html += sortableTableHeader(colDefs, v);
    for (const c of creds) {
      const sel = selectedCred === c.name ? ' style="background:color-mix(in srgb,var(--uc-primary) 10%,var(--uc-bg))"' : '';
      html += '<tr class="clickable" data-cred="' + esc(c.name) + '"' + sel + '>';
      html += '<td><span class="uc-link">' + esc(c.name) + '</span></td>';
      html += '<td><span class="uc-badge uc-badge-muted">' + esc(c.credential_type) + '</span></td>';
      html += '<td class="uc-muted">' + esc(c.owner || "\u2014") + '</td>';
      html += '<td>' + (c.read_only ? '<span class="uc-badge uc-badge-warning">RO</span>' : '') + '</td>';
      html += '<td>' + (c.used_for_managed_storage ? '<span class="uc-badge uc-badge-info">YES</span>' : '') + '</td>';
      html += '<td class="uc-muted">' + esc(c.created_at || "\u2014") + '</td>';
      html += '</tr>';
    }
    html += '</tbody></table>';

    // Detail
    if (selectedCred) {
      const c = allCreds.find(x => x.name === selectedCred);
      if (c) {
        html += '<div class="uc-detail-panel">';
        html += '<h3>' + esc(c.name) + '<button class="close-btn" data-action="close-cred">\u2715</button></h3>';
        html += '<dl class="uc-detail-grid">';
        html += dt("ID") + dd(c.id, true);
        html += dt("Type") + dd(c.credential_type);
        if (c.credential_detail) html += dt("Detail") + dd(c.credential_detail, true);
        html += dt("Owner") + dd(c.owner);
        if (c.read_only != null) html += dt("Read-only") + dd(c.read_only ? "Yes" : "No");
        if (c.used_for_managed_storage != null) html += dt("Managed Storage") + dd(c.used_for_managed_storage ? "Yes" : "No");
        if (c.isolation_mode) html += dt("Isolation") + dd(c.isolation_mode);
        html += dt("Created") + dd(c.created_at);
        html += dt("Created By") + dd(c.created_by);
        if (c.updated_at) html += dt("Updated") + dd(c.updated_at);
        if (c.comment) html += dt("Comment") + dd(c.comment);
        html += '</dl>';
        html += '</div>';
      }
    }

    return html;
  }

  // ================ CONNECTIONS ================

  function renderConnections() {
    const allConns = JSON.parse(model.get("connections_data") || "[]");
    if (!allConns.length) return '<div class="uc-empty">No connections found.</div>';

    const v = vs.connections;
    const colDefs = VIEW_COLS.connections;
    const fields = colDefs.map(c => c.key);
    const filtered = filterItems(allConns, fields, v.filter);
    const conns = sortItems(filtered, v.sortCol, v.sortDir);

    let html = filterBar("connections", v, allConns.length, conns.length);
    html += sortableTableHeader(colDefs, v);
    for (const c of conns) {
      const sel = selectedConn === c.name ? ' style="background:color-mix(in srgb,var(--uc-primary) 10%,var(--uc-bg))"' : '';
      html += '<tr class="clickable" data-conn="' + esc(c.name) + '"' + sel + '>';
      html += '<td><span class="uc-link">' + esc(c.name) + '</span></td>';
      html += '<td><span class="uc-badge uc-badge-muted">' + esc(c.connection_type || "\u2014") + '</span></td>';
      html += '<td class="uc-muted">' + esc(c.credential_type || "\u2014") + '</td>';
      html += '<td class="uc-muted">' + esc(c.owner || "\u2014") + '</td>';
      html += '<td class="uc-mono uc-muted" style="max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">' + esc(c.url || "\u2014") + '</td>';
      html += '</tr>';
    }
    html += '</tbody></table>';

    // Detail
    if (selectedConn) {
      const c = allConns.find(x => x.name === selectedConn);
      if (c) {
        html += '<div class="uc-detail-panel">';
        html += '<h3>' + esc(c.name) + '<button class="close-btn" data-action="close-conn">\u2715</button></h3>';
        html += '<dl class="uc-detail-grid">';
        html += dt("Connection ID") + dd(c.connection_id, true);
        html += dt("Type") + dd(c.connection_type);
        html += dt("Credential Type") + dd(c.credential_type);
        html += dt("Owner") + dd(c.owner);
        if (c.url) html += dt("URL") + dd(c.url, true);
        if (c.read_only != null) html += dt("Read-only") + dd(c.read_only ? "Yes" : "No");
        html += dt("Created") + dd(c.created_at);
        html += dt("Created By") + dd(c.created_by);
        if (c.updated_at) html += dt("Updated") + dd(c.updated_at);
        if (c.comment) html += dt("Comment") + dd(c.comment);
        html += '</dl>';
        if (c.properties && Object.keys(c.properties).length) {
          html += '<div class="uc-section-title">Properties</div>';
          html += '<div class="uc-code-block">' + esc(JSON.stringify(c.properties, null, 2)) + '</div>';
        }
        html += '</div>';
      }
    }

    return html;
  }

  // ================ EXTERNAL METADATA ================

  function renderExternalMetadata() {
    const allItems = JSON.parse(model.get("external_metadata_data") || "[]");
    if (!allItems.length) return '<div class="uc-empty">No external metadata found.</div>';

    const v = vs.ext_metadata;
    const colDefs = VIEW_COLS.ext_metadata;
    const fields = colDefs.map(c => c.key);
    const filtered = filterItems(allItems, fields, v.filter);
    const items = sortItems(filtered, v.sortCol, v.sortDir);

    let html = filterBar("ext_metadata", v, allItems.length, items.length);
    html += sortableTableHeader(colDefs, v);
    for (const em of items) {
      const sel = selectedMeta === em.name ? ' style="background:color-mix(in srgb,var(--uc-primary) 10%,var(--uc-bg))"' : '';
      html += '<tr class="clickable" data-meta="' + esc(em.name) + '"' + sel + '>';
      html += '<td><span class="uc-link">' + esc(em.name) + '</span></td>';
      html += '<td><span class="uc-badge uc-badge-muted">' + esc(em.entity_type || "\u2014") + '</span></td>';
      html += '<td class="uc-muted">' + esc(em.system_type || "\u2014") + '</td>';
      html += '<td class="uc-muted">' + esc(em.owner || "\u2014") + '</td>';
      html += '<td class="uc-mono uc-muted" style="max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">' + esc(em.url || "\u2014") + '</td>';
      html += '</tr>';
    }
    html += '</tbody></table>';

    // Detail
    if (selectedMeta) {
      const em = allItems.find(x => x.name === selectedMeta);
      if (em) {
        html += '<div class="uc-detail-panel">';
        html += '<h3>' + esc(em.name) + '<button class="close-btn" data-action="close-meta">\u2715</button></h3>';
        html += '<dl class="uc-detail-grid">';
        html += dt("ID") + dd(em.id, true);
        html += dt("Entity Type") + dd(em.entity_type);
        html += dt("System Type") + dd(em.system_type);
        html += dt("Owner") + dd(em.owner);
        if (em.url) html += dt("URL") + dd(em.url, true);
        if (em.description) html += dt("Description") + dd(em.description);
        html += dt("Created By") + dd(em.created_by);
        if (em.create_time) html += dt("Created") + dd(em.create_time);
        if (em.updated_by) html += dt("Updated By") + dd(em.updated_by);
        if (em.update_time) html += dt("Updated") + dd(em.update_time);
        html += '</dl>';

        if (em.columns && em.columns.length) {
          html += '<div class="uc-section-title">Columns</div>';
          html += '<table class="uc-table"><thead><tr><th>Name</th><th>Type</th><th>Comment</th></tr></thead><tbody>';
          for (const col of em.columns) {
            html += '<tr>';
            html += '<td class="uc-mono">' + esc(col.name) + '</td>';
            html += '<td class="uc-mono uc-muted">' + esc(col.type_text || "\u2014") + '</td>';
            html += '<td class="uc-muted">' + esc(col.comment || "") + '</td>';
            html += '</tr>';
          }
          html += '</tbody></table>';
        }

        if (em.properties && Object.keys(em.properties).length) {
          html += '<div class="uc-section-title">Properties</div>';
          html += '<div class="uc-code-block">' + esc(JSON.stringify(em.properties, null, 2)) + '</div>';
        }
        html += '</div>';
      }
    }

    return html;
  }

  // ================ EVENT BINDING ================

  function bindEvents() {
    // ---- Filter inputs ----
    root.querySelectorAll(".uc-filter-input").forEach(input => {
      // Restore cursor position after re-render
      const viewKey = input.dataset.filterView;
      if (viewKey && vs[viewKey] && vs[viewKey].filter) {
        // Move cursor to end
        input.setSelectionRange(input.value.length, input.value.length);
      }
      input.addEventListener("input", (e) => {
        const vk = e.target.dataset.filterView;
        if (vk && vs[vk]) {
          vs[vk].filter = e.target.value;
          fullRender();
          // Re-focus the input after render
          const newInput = root.querySelector('.uc-filter-input[data-filter-view="' + vk + '"]');
          if (newInput) { newInput.focus(); newInput.setSelectionRange(newInput.value.length, newInput.value.length); }
        }
      });
    });

    // ---- Filter clear buttons ----
    root.querySelectorAll(".uc-filter-clear").forEach(btn => {
      btn.addEventListener("click", () => {
        const vk = btn.dataset.filterClear;
        if (vk && vs[vk]) {
          vs[vk].filter = "";
          fullRender();
        }
      });
    });

    // ---- Sortable column header clicks ----
    root.querySelectorAll("th.sortable[data-sort-col]").forEach(th => {
      th.addEventListener("click", () => {
        const col = th.dataset.sortCol;
        // Determine which view this header belongs to by finding the closest table context
        // We walk up to find the view key from the filter bar sibling
        const table = th.closest("table");
        if (!table) return;
        const prev = table.previousElementSibling;
        if (prev && prev.classList.contains("uc-filter-bar")) {
          const input = prev.querySelector(".uc-filter-input");
          if (input) {
            const vk = input.dataset.filterView;
            if (vk && vs[vk]) {
              toggleSort(vs[vk], col);
              fullRender();
              return;
            }
          }
        }
        // Fallback: try to figure out view from active context
        const vk = activeTab === "catalog_browser"
          ? (!browserPath.catalog ? "catalogs" : !browserPath.schema ? "schemas" : (th.closest(".uc-detail-panel") ? "columns" : "tables"))
          : activeTab === "external_locations" ? "ext_locations"
          : activeTab === "storage_credentials" ? "credentials"
          : activeTab === "connections" ? "connections"
          : activeTab === "external_metadata" ? "ext_metadata"
          : null;
        if (vk && vs[vk]) {
          toggleSort(vs[vk], col);
          fullRender();
        }
      });
    });

    // Tab clicks
    root.querySelectorAll(".uc-tab").forEach(tab => {
      tab.addEventListener("click", () => {
        activeTab = tab.dataset.tab;
        const tabDef = TABS.find(t => t.key === activeTab);
        if (tabDef && tabDef.action && !loadedTabs[activeTab]) {
          loadedTabs[activeTab] = true;
          sendRequest({ action: tabDef.action });
        }
        fullRender();
      });
    });

    // Refresh
    root.querySelector("[data-action='refresh']")?.addEventListener("click", () => {
      if (activeTab === "catalog_browser") {
        if (!browserPath.catalog) {
          sendRequest({ action: "list_catalogs" });
        } else if (!browserPath.schema) {
          sendRequest({ action: "list_schemas", catalog_name: browserPath.catalog });
        } else {
          sendRequest({ action: "list_tables", catalog_name: browserPath.catalog, schema_name: browserPath.schema });
        }
      } else {
        const tabDef = TABS.find(t => t.key === activeTab);
        if (tabDef && tabDef.action) sendRequest({ action: tabDef.action });
      }
    });

    // Breadcrumb navigation
    root.querySelectorAll("[data-nav]").forEach(el => {
      el.addEventListener("click", () => {
        const nav = el.dataset.nav;
        if (nav === "root") {
          browserPath = { catalog: null, schema: null };
          selectedTable = null;
          model.set("permissions_data", "{}");
          model.save_changes();
          sendRequest({ action: "list_catalogs" });
        } else if (nav === "catalog") {
          browserPath.schema = null;
          selectedTable = null;
          model.set("permissions_data", "{}");
          model.save_changes();
          sendRequest({ action: "list_schemas", catalog_name: browserPath.catalog });
        }
        fullRender();
      });
    });

    // Catalog clicks
    root.querySelectorAll("[data-catalog]").forEach(tr => {
      tr.addEventListener("click", () => {
        browserPath.catalog = tr.dataset.catalog;
        browserPath.schema = null;
        selectedTable = null;
        sendRequest({ action: "list_schemas", catalog_name: browserPath.catalog });
      });
    });

    // Schema clicks
    root.querySelectorAll("[data-schema]").forEach(tr => {
      tr.addEventListener("click", () => {
        browserPath.schema = tr.dataset.schema;
        selectedTable = null;
        sendRequest({ action: "list_tables", catalog_name: browserPath.catalog, schema_name: browserPath.schema });
      });
    });

    // Table clicks
    root.querySelectorAll("[data-table]").forEach(tr => {
      tr.addEventListener("click", () => {
        const fn = tr.dataset.table;
        if (selectedTable === fn) {
          selectedTable = null;
          fullRender();
        } else {
          selectedTable = fn;
          tableSubTab = "columns";
          // Clear stale data
          model.set("permissions_data", "{}");
          model.set("lineage_data", "{}");
          model.set("sample_data", "{}");
          model.save_changes();
          sendRequest({ action: "get_table", full_name: fn });
        }
      });
    });

    // Close table detail
    root.querySelector("[data-action='close-table-detail']")?.addEventListener("click", () => {
      selectedTable = null;
      fullRender();
    });

    // Sub-tab clicks
    root.querySelectorAll("[data-subtab]").forEach(btn => {
      btn.addEventListener("click", () => {
        tableSubTab = btn.dataset.subtab;
        fullRender();
      });
    });

    // Column lineage click
    root.querySelectorAll("[data-col-lineage]").forEach(tr => {
      tr.addEventListener("click", () => {
        const colName = tr.dataset.colLineage;
        sendRequest({ action: "get_column_lineage", full_name: selectedTable, column_name: colName });
        tableSubTab = "lineage";
      });
    });

    // Load sample data
    root.querySelector("[data-action='load-sample']")?.addEventListener("click", () => {
      if (selectedTable) sendRequest({ action: "get_sample_data", full_name: selectedTable });
    });

    // Load table lineage
    root.querySelector("[data-action='load-table-lineage']")?.addEventListener("click", () => {
      if (selectedTable) sendRequest({ action: "get_table_lineage", full_name: selectedTable });
    });

    // Load table permissions
    root.querySelector("[data-action='load-table-perms']")?.addEventListener("click", () => {
      if (selectedTable) sendRequest({ action: "get_permissions", securable_type: "table", full_name: selectedTable });
    });

    // Catalog permissions
    root.querySelector("[data-action='catalog-perms']")?.addEventListener("click", () => {
      if (browserPath.catalog) sendRequest({ action: "get_permissions", securable_type: "catalog", full_name: browserPath.catalog });
    });

    // Schema permissions
    root.querySelector("[data-action='schema-perms']")?.addEventListener("click", () => {
      if (browserPath.catalog && browserPath.schema) {
        sendRequest({ action: "get_permissions", securable_type: "schema", full_name: browserPath.catalog + "." + browserPath.schema });
      }
    });

    // Close permissions
    root.querySelector("[data-action='close-perms']")?.addEventListener("click", () => {
      model.set("permissions_data", "{}");
      model.save_changes();
      fullRender();
    });

    // List volumes
    root.querySelector("[data-action='list-volumes']")?.addEventListener("click", () => {
      if (browserPath.catalog && browserPath.schema) {
        sendRequest({ action: "list_volumes", catalog_name: browserPath.catalog, schema_name: browserPath.schema });
      }
    });

    // Navigate to table from lineage
    root.querySelectorAll("[data-nav-table]").forEach(el => {
      el.addEventListener("click", () => {
        const fn = el.dataset.navTable;
        const parts = fn.split(".");
        if (parts.length >= 3) {
          browserPath.catalog = parts[0];
          browserPath.schema = parts[1];
          selectedTable = fn;
          tableSubTab = "columns";
          sendRequest({ action: "list_tables", catalog_name: parts[0], schema_name: parts[1] });
          sendRequest({ action: "get_table", full_name: fn });
        }
      });
    });

    // External location clicks
    root.querySelectorAll("[data-ext-loc]").forEach(tr => {
      tr.addEventListener("click", () => {
        const name = tr.dataset.extLoc;
        selectedLocation = selectedLocation === name ? null : name;
        browseStack = [];
        model.set("location_contents_data", "{}");
        model.set("permissions_data", "{}");
        model.save_changes();
        fullRender();
      });
    });

    root.querySelector("[data-action='close-ext-loc']")?.addEventListener("click", () => {
      selectedLocation = null;
      browseStack = [];
      fullRender();
    });

    // Browse external location
    root.querySelector("[data-action='browse-loc']")?.addEventListener("click", (e) => {
      const url = e.currentTarget.dataset.url;
      if (url) {
        browseStack = [url];
        sendRequest({ action: "browse_location", url });
      }
    });

    // Location permissions
    root.querySelector("[data-action='loc-perms']")?.addEventListener("click", () => {
      if (selectedLocation) {
        sendRequest({ action: "get_permissions", securable_type: "external_location", full_name: selectedLocation });
      }
    });

    // File browser navigation
    root.querySelectorAll("[data-browse-path]").forEach(el => {
      el.addEventListener("click", () => {
        if (el.dataset.isDir === "1") {
          const path = el.dataset.browsePath;
          browseStack.push(path);
          sendRequest({ action: "browse_location", url: path });
        }
      });
    });

    // Browse back
    root.querySelector("[data-action='browse-back']")?.addEventListener("click", () => {
      if (browseStack.length > 1) {
        browseStack.pop();
        const prev = browseStack[browseStack.length - 1];
        sendRequest({ action: "browse_location", url: prev });
      }
    });

    // Storage credential clicks
    root.querySelectorAll("[data-cred]").forEach(tr => {
      tr.addEventListener("click", () => {
        const name = tr.dataset.cred;
        selectedCred = selectedCred === name ? null : name;
        fullRender();
      });
    });

    root.querySelector("[data-action='close-cred']")?.addEventListener("click", () => {
      selectedCred = null;
      fullRender();
    });

    // Connection clicks
    root.querySelectorAll("[data-conn]").forEach(tr => {
      tr.addEventListener("click", () => {
        const name = tr.dataset.conn;
        selectedConn = selectedConn === name ? null : name;
        fullRender();
      });
    });

    root.querySelector("[data-action='close-conn']")?.addEventListener("click", () => {
      selectedConn = null;
      fullRender();
    });

    // External metadata clicks
    root.querySelectorAll("[data-meta]").forEach(tr => {
      tr.addEventListener("click", () => {
        const name = tr.dataset.meta;
        selectedMeta = selectedMeta === name ? null : name;
        fullRender();
      });
    });

    root.querySelector("[data-action='close-meta']")?.addEventListener("click", () => {
      selectedMeta = null;
      fullRender();
    });
  }

  // ---- Data change listeners ----
  const dataTraits = [
    "catalogs_data", "schemas_data", "tables_data", "table_detail",
    "volumes_data", "sample_data", "permissions_data", "lineage_data",
    "external_locations_data", "location_contents_data",
    "storage_credentials_data", "connections_data", "external_metadata_data",
  ];
  for (const trait of dataTraits) {
    model.on("change:" + trait, fullRender);
  }
  model.on("change:loading", fullRender);
  model.on("change:error_message", fullRender);

  loadedTabs["catalog_browser"] = true;
  fullRender();
}

export default { render };
