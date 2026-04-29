// Databricks Compute Browser — anywidget ESM frontend
// Traits: clusters_data, warehouses_data, vs_endpoints_data, pools_data,
//         policies_data, loading, error_message, request

const STYLES = `
  :host {
    --cb-bg: #ffffff;
    --cb-bg-alt: #f8f9fa;
    --cb-bg-hover: #e9ecef;
    --cb-border: #dee2e6;
    --cb-text: #212529;
    --cb-text-muted: #6c757d;
    --cb-primary: #0d6efd;
    --cb-success: #198754;
    --cb-danger: #dc3545;
    --cb-warning: #ffc107;
    --cb-info: #0dcaf0;
    --cb-font: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
    --cb-mono: "SF Mono", "Cascadia Code", "Fira Code", Menlo, Consolas, monospace;
    --cb-radius: 6px;
    display: block;
    font-family: var(--cb-font);
    font-size: 13px;
    color: var(--cb-text);
    background: var(--cb-bg);
    border: 1px solid var(--cb-border);
    border-radius: var(--cb-radius);
    overflow: hidden;
  }
  :host(.dark-theme) {
      --cb-bg: #1e1e1e; --cb-bg-alt: #252526; --cb-bg-hover: #2d2d30;
      --cb-border: #3e3e42; --cb-text: #cccccc; --cb-text-muted: #888888;
      --cb-primary: #4fc3f7; --cb-success: #66bb6a; --cb-danger: #ef5350;
      --cb-warning: #ffca28; --cb-info: #4dd0e1;
    }
  * { box-sizing: border-box; }

  .cb-header {
    display: flex; align-items: center; gap: 8px;
    padding: 10px 14px; background: var(--cb-bg-alt);
    border-bottom: 1px solid var(--cb-border); flex-wrap: wrap;
  }
  .cb-header h2 { margin: 0; font-size: 14px; font-weight: 600; }
  .cb-header-right { margin-left: auto; }

  .cb-btn {
    padding: 4px 10px; border: 1px solid var(--cb-border); border-radius: 4px;
    background: var(--cb-bg); color: var(--cb-text); cursor: pointer;
    font-size: 12px; font-family: var(--cb-font);
  }
  .cb-btn:hover { background: var(--cb-bg-hover); }

  /* Tabs */
  .cb-tabs {
    display: flex; gap: 0; background: var(--cb-bg-alt);
    border-bottom: 2px solid var(--cb-border); padding: 0 14px;
  }
  .cb-tab {
    padding: 8px 16px; cursor: pointer; font-size: 12px; font-weight: 500;
    border: none; background: none; color: var(--cb-text-muted);
    border-bottom: 2px solid transparent; margin-bottom: -2px;
    font-family: var(--cb-font); white-space: nowrap;
  }
  .cb-tab:hover { color: var(--cb-text); }
  .cb-tab.active { color: var(--cb-primary); border-bottom-color: var(--cb-primary); }

  .cb-body { max-height: 560px; overflow: auto; }

  .cb-loading {
    padding: 30px; text-align: center; color: var(--cb-text-muted);
  }
  .cb-loading .spinner {
    display: inline-block; width: 20px; height: 20px;
    border: 2px solid var(--cb-border); border-top-color: var(--cb-primary);
    border-radius: 50%; animation: cb-spin 0.6s linear infinite;
    margin-right: 8px; vertical-align: middle;
  }
  @keyframes cb-spin { to { transform: rotate(360deg); } }

  .cb-error {
    padding: 10px 14px; background: #fef2f2; color: var(--cb-danger);
    border-bottom: 1px solid #fecaca; font-size: 12px;
  }
  :host(.dark-theme) .cb-error { background: #3b1f1f; border-color: #5c2b2b; }

  .cb-empty { padding: 30px; text-align: center; color: var(--cb-text-muted); }

  /* Table */
  table.cb-table { width: 100%; border-collapse: collapse; }
  .cb-table th {
    text-align: left; padding: 8px 14px; font-size: 11px; font-weight: 600;
    text-transform: uppercase; letter-spacing: 0.5px; color: var(--cb-text-muted);
    background: var(--cb-bg-alt); border-bottom: 1px solid var(--cb-border);
    position: sticky; top: 0; z-index: 1;
  }
  .cb-table td {
    padding: 7px 14px; border-bottom: 1px solid var(--cb-border); vertical-align: top;
  }
  .cb-table tr:hover td { background: var(--cb-bg-hover); }
  .cb-table tr.clickable { cursor: pointer; }
  .cb-table tr.selected td {
    background: color-mix(in srgb, var(--cb-primary) 10%, var(--cb-bg));
  }

  .cb-link { color: var(--cb-primary); cursor: pointer; text-decoration: none; }
  .cb-mono { font-family: var(--cb-mono); font-size: 12px; }
  .cb-muted { color: var(--cb-text-muted); font-size: 12px; }

  /* Badges */
  .cb-badge {
    display: inline-block; padding: 2px 7px; border-radius: 10px;
    font-size: 11px; font-weight: 500; white-space: nowrap;
  }
  .cb-badge-success { background: #d1fae5; color: #065f46; }
  .cb-badge-danger  { background: #fee2e2; color: #991b1b; }
  .cb-badge-warning { background: #fef3c7; color: #92400e; }
  .cb-badge-info    { background: #dbeafe; color: #1e40af; }
  .cb-badge-muted   { background: #f3f4f6; color: #6b7280; }
  :host(.dark-theme) .cb-badge-success { background: #064e3b; color: #6ee7b7; }
  :host(.dark-theme) .cb-badge-danger { background: #7f1d1d; color: #fca5a5; }
  :host(.dark-theme) .cb-badge-warning { background: #78350f; color: #fcd34d; }
  :host(.dark-theme) .cb-badge-info { background: #1e3a5f; color: #93c5fd; }
  :host(.dark-theme) .cb-badge-muted { background: #374151; color: #9ca3af; }

  /* Detail panel */
  .cb-detail-panel {
    border-top: 1px solid var(--cb-border);
    padding: 14px;
    background: var(--cb-bg-alt);
  }
  .cb-detail-panel h3 {
    margin: 0 0 10px; font-size: 13px; font-weight: 600;
    display: flex; align-items: center; justify-content: space-between;
  }
  .cb-detail-panel .close-btn {
    background: none; border: none; font-size: 16px; cursor: pointer;
    color: var(--cb-text-muted); padding: 0 4px; line-height: 1;
  }
  .cb-detail-panel .close-btn:hover { color: var(--cb-text); }

  .cb-detail-grid {
    display: grid; grid-template-columns: 160px 1fr;
    gap: 4px 12px; font-size: 12px;
  }
  .cb-detail-grid dt { color: var(--cb-text-muted); font-weight: 500; padding: 3px 0; }
  .cb-detail-grid dd { margin: 0; padding: 3px 0; word-break: break-all; }

  .cb-tag {
    display: inline-block; padding: 1px 6px; margin: 1px 3px 1px 0;
    border-radius: 3px; font-size: 11px; background: var(--cb-bg);
    border: 1px solid var(--cb-border); font-family: var(--cb-mono);
  }

  .cb-section-title {
    font-size: 12px; font-weight: 600; margin: 12px 0 6px;
    padding-bottom: 3px; border-bottom: 1px solid var(--cb-border);
  }

  .cb-policy-def {
    margin: 0; padding: 8px 12px; font-family: var(--cb-mono);
    font-size: 11px; line-height: 1.5; white-space: pre-wrap;
    word-break: break-all; background: var(--cb-bg); border-radius: 4px;
    border: 1px solid var(--cb-border); max-height: 260px; overflow: auto;
  }

  .op-loading-overlay { position: relative; pointer-events: none; opacity: 0.6; }
  .op-loading-overlay::after { content: ''; position: absolute; top: 0; left: 0; right: 0; bottom: 0; background: var(--cb-bg); opacity: 0.5; z-index: 10; }
  .op-loading-overlay::before { content: ''; position: absolute; top: 50%; left: 50%; width: 20px; height: 20px; margin: -10px 0 0 -10px; border: 2px solid var(--cb-border); border-top-color: var(--cb-primary); border-radius: 50%; animation: cb-spin 0.6s linear infinite; z-index: 11; }
`;

// ---- Helpers ----

function esc(s) {
  if (s == null) return "";
  const d = document.createElement("div");
  d.textContent = String(s);
  return d.innerHTML;
}

function stateBadge(state) {
  if (!state) return "";
  const s = String(state).toUpperCase();
  let cls = "muted";
  if (["RUNNING", "ACTIVE", "ONLINE"].includes(s)) cls = "success";
  else if (["ERROR", "DELETED", "RED_STATE"].includes(s)) cls = "danger";
  else if (["PENDING", "STARTING", "RESTARTING", "RESIZING", "PROVISIONING", "STOPPING", "TERMINATING", "YELLOW_STATE"].includes(s)) cls = "info";
  else if (["TERMINATED", "STOPPED", "OFFLINE", "UNKNOWN"].includes(s)) cls = "muted";
  return '<span class="cb-badge cb-badge-' + cls + '">' + esc(s) + "</span>";
}

function tagsHtml(tags) {
  if (!tags || !Object.keys(tags).length) return "";
  return Object.entries(tags).map(([k, v]) =>
    '<span class="cb-tag">' + esc(k) + "=" + esc(v) + "</span>"
  ).join(" ");
}

// ---- Tab definitions ----

const TABS = [
  { key: "clusters", label: "Clusters", action: "list_clusters", dataKey: "clusters_data" },
  { key: "warehouses", label: "SQL Warehouses", action: "list_warehouses", dataKey: "warehouses_data" },
  { key: "vs_endpoints", label: "Vector Search", action: "list_vs_endpoints", dataKey: "vs_endpoints_data" },
  { key: "pools", label: "Instance Pools", action: "list_pools", dataKey: "pools_data" },
  { key: "policies", label: "Cluster Policies", action: "list_policies", dataKey: "policies_data" },
];

// ---- Render functions per tab ----

function renderClusters(items, selectedId) {
  const cols = ["Name", "State", "Node Type", "Workers", "Runtime", "Creator"];
  let html = tableHeader(cols);
  for (const c of items) {
    const sel = c.cluster_id === selectedId ? " selected" : "";
    const workers = c.autoscale ? c.autoscale + " (auto)" : (c.num_workers != null ? c.num_workers : "—");
    html += '<tr class="clickable' + sel + '" data-id="' + esc(c.cluster_id) + '">';
    html += "<td><span class=\"cb-link\">" + esc(c.cluster_name || "(unnamed)") + "</span></td>";
    html += "<td>" + stateBadge(c.state) + "</td>";
    html += '<td class="cb-mono cb-muted">' + esc(c.node_type_id || "—") + "</td>";
    html += "<td>" + esc(workers) + "</td>";
    html += '<td class="cb-mono cb-muted">' + esc(c.spark_version || "—") + "</td>";
    html += '<td class="cb-muted">' + esc(c.creator || "—") + "</td>";
    html += "</tr>";
  }
  html += "</tbody></table>";
  return html;
}

function renderClusterDetail(c) {
  let html = "<dl class=\"cb-detail-grid\">";
  html += dt("Cluster ID") + dd(c.cluster_id, true);
  html += dt("State") + "<dd>" + stateBadge(c.state) + "</dd>";
  if (c.state_message) html += dt("State Message") + dd(c.state_message);
  html += dt("Node Type") + dd(c.node_type_id, true);
  if (c.driver_node_type_id && c.driver_node_type_id !== c.node_type_id)
    html += dt("Driver Node Type") + dd(c.driver_node_type_id, true);
  html += dt("Workers") + dd(c.autoscale ? c.autoscale + " (autoscale)" : c.num_workers);
  if (c.is_single_node) html += dt("Single Node") + dd("Yes");
  html += dt("Runtime") + dd(c.spark_version, true);
  if (c.runtime_engine) html += dt("Engine") + dd(c.runtime_engine);
  if (c.data_security_mode) html += dt("Security Mode") + dd(c.data_security_mode);
  if (c.single_user_name) html += dt("Single User") + dd(c.single_user_name);
  if (c.cluster_source) html += dt("Source") + dd(c.cluster_source);
  if (c.policy_id) html += dt("Policy ID") + dd(c.policy_id, true);
  if (c.instance_pool_id) html += dt("Instance Pool") + dd(c.instance_pool_id, true);
  if (c.autotermination_minutes) html += dt("Auto-terminate") + dd(c.autotermination_minutes + " min");
  if (c.cluster_cores) html += dt("Total Cores") + dd(c.cluster_cores);
  if (c.cluster_memory_mb) html += dt("Total Memory") + dd(Math.round(c.cluster_memory_mb / 1024) + " GB");
  html += dt("Creator") + dd(c.creator);
  if (c.start_time) html += dt("Started") + dd(c.start_time);
  if (c.terminated_time) html += dt("Terminated") + dd(c.terminated_time);
  if (c.last_restarted_time) html += dt("Last Restarted") + dd(c.last_restarted_time);
  if (c.termination_reason) html += dt("Termination Reason") + dd(c.termination_reason);
  html += "</dl>";
  if (c.custom_tags && Object.keys(c.custom_tags).length) {
    html += '<div class="cb-section-title">Tags</div>' + tagsHtml(c.custom_tags);
  }
  return html;
}

function renderWarehouses(items, selectedId) {
  const cols = ["Name", "State", "Size", "Clusters", "Sessions", "Type", "Creator"];
  let html = tableHeader(cols);
  for (const w of items) {
    const sel = w.id === selectedId ? " selected" : "";
    const clusters = w.num_clusters != null ? w.num_clusters + " / " + (w.max_num_clusters || "—") : "—";
    html += '<tr class="clickable' + sel + '" data-id="' + esc(w.id) + '">';
    html += "<td><span class=\"cb-link\">" + esc(w.name || "(unnamed)") + "</span></td>";
    html += "<td>" + stateBadge(w.state) + "</td>";
    html += '<td class="cb-mono">' + esc(w.cluster_size || "—") + "</td>";
    html += "<td>" + esc(clusters) + "</td>";
    html += "<td>" + esc(w.num_active_sessions != null ? w.num_active_sessions : "—") + "</td>";
    html += '<td class="cb-muted">' + esc(w.warehouse_type || "—") + "</td>";
    html += '<td class="cb-muted">' + esc(w.creator_name || "—") + "</td>";
    html += "</tr>";
  }
  html += "</tbody></table>";
  return html;
}

function renderWarehouseDetail(w) {
  let html = "<dl class=\"cb-detail-grid\">";
  html += dt("Warehouse ID") + dd(w.id, true);
  html += dt("State") + "<dd>" + stateBadge(w.state) + "</dd>";
  html += dt("Size") + dd(w.cluster_size, true);
  html += dt("Min Clusters") + dd(w.min_num_clusters);
  html += dt("Max Clusters") + dd(w.max_num_clusters);
  html += dt("Active Clusters") + dd(w.num_clusters);
  html += dt("Active Sessions") + dd(w.num_active_sessions);
  html += dt("Auto-stop") + dd(w.auto_stop_mins != null ? w.auto_stop_mins + " min" : "—");
  html += dt("Type") + dd(w.warehouse_type);
  html += dt("Photon") + dd(w.enable_photon ? "Yes" : "No");
  html += dt("Serverless") + dd(w.enable_serverless_compute ? "Yes" : "No");
  html += dt("Creator") + dd(w.creator_name);
  if (w.health_status) html += dt("Health") + "<dd>" + stateBadge(w.health_status) + "</dd>";
  if (w.health_message) html += dt("Health Message") + dd(w.health_message);
  html += "</dl>";
  if (w.tags && Object.keys(w.tags).length) {
    html += '<div class="cb-section-title">Tags</div>' + tagsHtml(w.tags);
  }
  return html;
}

function renderVsEndpoints(items, selectedId) {
  const cols = ["Name", "State", "Type", "Indexes", "Creator", "Created"];
  let html = tableHeader(cols);
  for (const e of items) {
    const sel = e.id === selectedId ? " selected" : "";
    html += '<tr class="clickable' + sel + '" data-id="' + esc(e.id) + '">';
    html += "<td><span class=\"cb-link\">" + esc(e.name || "(unnamed)") + "</span></td>";
    html += "<td>" + stateBadge(e.state) + "</td>";
    html += '<td class="cb-muted">' + esc(e.endpoint_type || "—") + "</td>";
    html += "<td>" + esc(e.num_indexes != null ? e.num_indexes : "—") + "</td>";
    html += '<td class="cb-muted">' + esc(e.creator || "—") + "</td>";
    html += '<td class="cb-muted">' + esc(e.creation_timestamp || "—") + "</td>";
    html += "</tr>";
  }
  html += "</tbody></table>";
  return html;
}

function renderVsEndpointDetail(e) {
  let html = "<dl class=\"cb-detail-grid\">";
  html += dt("Endpoint ID") + dd(e.id, true);
  html += dt("State") + "<dd>" + stateBadge(e.state) + "</dd>";
  if (e.state_message) html += dt("Message") + dd(e.state_message);
  html += dt("Type") + dd(e.endpoint_type);
  html += dt("Indexes") + dd(e.num_indexes);
  html += dt("Creator") + dd(e.creator);
  if (e.creation_timestamp) html += dt("Created") + dd(e.creation_timestamp);
  if (e.last_updated_timestamp) html += dt("Last Updated") + dd(e.last_updated_timestamp);
  if (e.last_updated_user) html += dt("Updated By") + dd(e.last_updated_user);
  html += "</dl>";
  return html;
}

function renderPools(items, selectedId) {
  const cols = ["Name", "State", "Node Type", "Idle / Used", "Min Idle", "Max Capacity"];
  let html = tableHeader(cols);
  for (const p of items) {
    const sel = p.instance_pool_id === selectedId ? " selected" : "";
    const usage = (p.idle_count != null ? p.idle_count : "?") + " / " + (p.used_count != null ? p.used_count : "?");
    html += '<tr class="clickable' + sel + '" data-id="' + esc(p.instance_pool_id) + '">';
    html += "<td><span class=\"cb-link\">" + esc(p.instance_pool_name || "(unnamed)") + "</span></td>";
    html += "<td>" + stateBadge(p.state) + "</td>";
    html += '<td class="cb-mono cb-muted">' + esc(p.node_type_id || "—") + "</td>";
    html += "<td>" + esc(usage) + "</td>";
    html += "<td>" + esc(p.min_idle_instances != null ? p.min_idle_instances : "—") + "</td>";
    html += "<td>" + esc(p.max_capacity != null ? p.max_capacity : "—") + "</td>";
    html += "</tr>";
  }
  html += "</tbody></table>";
  return html;
}

function renderPoolDetail(p) {
  let html = "<dl class=\"cb-detail-grid\">";
  html += dt("Pool ID") + dd(p.instance_pool_id, true);
  html += dt("State") + "<dd>" + stateBadge(p.state) + "</dd>";
  html += dt("Node Type") + dd(p.node_type_id, true);
  html += dt("Idle Instances") + dd(p.idle_count);
  html += dt("Used Instances") + dd(p.used_count);
  html += dt("Pending Idle") + dd(p.pending_idle_count);
  html += dt("Pending Used") + dd(p.pending_used_count);
  html += dt("Min Idle") + dd(p.min_idle_instances);
  html += dt("Max Capacity") + dd(p.max_capacity);
  html += dt("Auto-terminate") + dd(p.idle_instance_autotermination_minutes != null ? p.idle_instance_autotermination_minutes + " min" : "—");
  if (p.preloaded_spark_versions && p.preloaded_spark_versions.length)
    html += dt("Preloaded Runtimes") + '<dd class="cb-mono">' + p.preloaded_spark_versions.map(v => esc(v)).join(", ") + "</dd>";
  html += "</dl>";
  if (p.custom_tags && Object.keys(p.custom_tags).length) {
    html += '<div class="cb-section-title">Tags</div>' + tagsHtml(p.custom_tags);
  }
  return html;
}

function renderPolicies(items, selectedId) {
  const cols = ["Name", "Default", "Description", "Max Clusters/User", "Creator"];
  let html = tableHeader(cols);
  for (const p of items) {
    const sel = p.policy_id === selectedId ? " selected" : "";
    html += '<tr class="clickable' + sel + '" data-id="' + esc(p.policy_id) + '">';
    html += "<td><span class=\"cb-link\">" + esc(p.name || "(unnamed)") + "</span></td>";
    html += "<td>" + (p.is_default ? '<span class="cb-badge cb-badge-info">DEFAULT</span>' : "") + "</td>";
    const desc = p.description ? (p.description.length > 60 ? p.description.slice(0, 57) + "…" : p.description) : "—";
    html += '<td class="cb-muted">' + esc(desc) + "</td>";
    html += "<td>" + esc(p.max_clusters_per_user != null ? p.max_clusters_per_user : "—") + "</td>";
    html += '<td class="cb-muted">' + esc(p.creator || "—") + "</td>";
    html += "</tr>";
  }
  html += "</tbody></table>";
  return html;
}

function renderPolicyDetail(p) {
  let html = "<dl class=\"cb-detail-grid\">";
  html += dt("Policy ID") + dd(p.policy_id, true);
  if (p.is_default) html += dt("Default") + "<dd>" + '<span class="cb-badge cb-badge-info">DEFAULT</span></dd>';
  if (p.description) html += dt("Description") + dd(p.description);
  html += dt("Creator") + dd(p.creator);
  if (p.created_at) html += dt("Created") + dd(p.created_at);
  if (p.max_clusters_per_user != null) html += dt("Max Clusters/User") + dd(p.max_clusters_per_user);
  if (p.policy_family_id) html += dt("Policy Family") + dd(p.policy_family_id, true);
  html += "</dl>";
  if (p.definition) {
    html += '<div class="cb-section-title">Definition (JSON)</div>';
    let formatted = p.definition;
    try { formatted = JSON.stringify(JSON.parse(p.definition), null, 2); } catch {}
    html += '<pre class="cb-policy-def">' + esc(formatted) + "</pre>";
  }
  return html;
}

// ---- Shared helpers ----

function tableHeader(cols) {
  let h = '<table class="cb-table"><thead><tr>';
  for (const c of cols) h += "<th>" + esc(c) + "</th>";
  h += "</tr></thead><tbody>";
  return h;
}

function dt(label) { return "<dt>" + esc(label) + "</dt>"; }
function dd(val, mono) {
  const cls = mono ? ' class="cb-mono"' : "";
  return "<dd" + cls + ">" + esc(val != null ? val : "—") + "</dd>";
}

// Lookup table: tab key -> { renderList, renderDetail, idField }
const TAB_RENDERERS = {
  clusters: { renderList: renderClusters, renderDetail: renderClusterDetail, idField: "cluster_id", nameField: "cluster_name" },
  warehouses: { renderList: renderWarehouses, renderDetail: renderWarehouseDetail, idField: "id", nameField: "name" },
  vs_endpoints: { renderList: renderVsEndpoints, renderDetail: renderVsEndpointDetail, idField: "id", nameField: "name" },
  pools: { renderList: renderPools, renderDetail: renderPoolDetail, idField: "instance_pool_id", nameField: "instance_pool_name" },
  policies: { renderList: renderPolicies, renderDetail: renderPolicyDetail, idField: "policy_id", nameField: "name" },
};

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

  let activeTab = "clusters";
  let selectedIds = {};    // per-tab selected item id
  let loadedTabs = {};     // track which tabs have been loaded
  let hasRendered = false;

  function fullRender() {
    // Header
    let html = '<div class="cb-header"><h2>\u2699 Compute</h2>';
    html += '<div class="cb-header-right"><button class="cb-btn" data-action="refresh">\u21bb Refresh</button></div></div>';

    // Tabs
    html += '<div class="cb-tabs">';
    for (const t of TABS) {
      const act = t.key === activeTab ? " active" : "";
      html += '<button class="cb-tab' + act + '" data-tab="' + t.key + '">' + esc(t.label) + "</button>";
    }
    html += "</div>";

    // Error
    const err = model.get("error_message");
    if (err) html += '<div class="cb-error">' + esc(err) + "</div>";

    // Body
    html += '<div class="cb-body' + (model.get("loading") && hasRendered ? ' op-loading-overlay' : '') + '">';

    if (model.get("loading") && !hasRendered) {
      html += '<div class="cb-loading"><span class="spinner"></span> Loading\u2026</div>';
    } else {
      const tabDef = TABS.find(t => t.key === activeTab);
      const renderer = TAB_RENDERERS[activeTab];
      if (tabDef && renderer) {
        const data = JSON.parse(model.get(tabDef.dataKey) || "[]");
        const selId = selectedIds[activeTab] || null;

        if (!data.length) {
          html += '<div class="cb-empty">No ' + esc(tabDef.label.toLowerCase()) + ' found.</div>';
        } else {
          html += renderer.renderList(data, selId);

          // Detail panel
          if (selId) {
            const item = data.find(d => d[renderer.idField] === selId);
            if (item) {
              html += '<div class="cb-detail-panel">';
              html += '<h3>' + esc(item[renderer.nameField] || selId) + '<button class="close-btn" data-action="close-detail">\u2715</button></h3>';
              html += renderer.renderDetail(item);
              html += "</div>";
            }
          }
        }
      }
    }

    html += "</div>";
    root.innerHTML = html;
    hasRendered = true;
    bindEvents();
  }

  function bindEvents() {
    // Tab clicks
    root.querySelectorAll(".cb-tab").forEach(tab => {
      tab.addEventListener("click", () => {
        activeTab = tab.dataset.tab;
        // Load tab data if not yet loaded
        const tabDef = TABS.find(t => t.key === activeTab);
        if (tabDef && !loadedTabs[activeTab]) {
          loadedTabs[activeTab] = true;
          model.set("request", JSON.stringify({ action: tabDef.action }));
          model.save_changes();
        }
        fullRender();
      });
    });

    // Row clicks
    root.querySelectorAll("tr[data-id]").forEach(tr => {
      tr.addEventListener("click", () => {
        const id = tr.dataset.id;
        selectedIds[activeTab] = selectedIds[activeTab] === id ? null : id;
        fullRender();
      });
    });

    // Close detail
    root.querySelector("[data-action='close-detail']")?.addEventListener("click", () => {
      selectedIds[activeTab] = null;
      fullRender();
    });

    // Refresh
    root.querySelector("[data-action='refresh']")?.addEventListener("click", () => {
      const tabDef = TABS.find(t => t.key === activeTab);
      if (tabDef) {
        model.set("request", JSON.stringify({ action: tabDef.action }));
        model.save_changes();
      }
    });
  }

  // Listen for data changes
  for (const t of TABS) {
    model.on("change:" + t.dataKey, () => { if (activeTab === t.key) fullRender(); });
  }
  model.on("change:loading", fullRender);
  model.on("change:error_message", fullRender);

  // Mark clusters as loaded (loaded on init)
  loadedTabs["clusters"] = true;
  fullRender();
}

export default { render };
