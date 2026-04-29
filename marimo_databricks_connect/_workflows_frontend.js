// Databricks Workflows Browser — anywidget ESM frontend
// Communicates with Python backend via model traits:
//   jobs_data, job_detail, runs_data, run_detail, loading, error_message, request

// ===================================================================
// Styles
// ===================================================================

const STYLES = `
  :host {
    --wf-bg: #ffffff;
    --wf-bg-alt: #f8f9fa;
    --wf-bg-hover: #e9ecef;
    --wf-border: #dee2e6;
    --wf-text: #212529;
    --wf-text-muted: #6c757d;
    --wf-primary: #0d6efd;
    --wf-primary-hover: #0b5ed7;
    --wf-success: #198754;
    --wf-danger: #dc3545;
    --wf-warning: #ffc107;
    --wf-info: #0dcaf0;
    --wf-font: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
    --wf-font-mono: "SF Mono", "Cascadia Code", "Fira Code", Menlo, Consolas, monospace;
    --wf-radius: 6px;

    --dag-node-bg: #ffffff;
    --dag-node-border: #c8ccd0;
    --dag-node-selected-border: var(--wf-primary);
    --dag-edge: #adb5bd;
    --dag-edge-arrow: #868e96;

    display: block;
    font-family: var(--wf-font);
    font-size: 13px;
    color: var(--wf-text);
    background: var(--wf-bg);
    border: 1px solid var(--wf-border);
    border-radius: var(--wf-radius);
    overflow: hidden;
  }

  :host(.dark-theme) {
      --wf-bg: #1e1e1e;
      --wf-bg-alt: #252526;
      --wf-bg-hover: #2d2d30;
      --wf-border: #3e3e42;
      --wf-text: #cccccc;
      --wf-text-muted: #888888;
      --wf-primary: #4fc3f7;
      --wf-primary-hover: #29b6f6;
      --wf-success: #66bb6a;
      --wf-danger: #ef5350;
      --wf-warning: #ffca28;
      --wf-info: #4dd0e1;

      --dag-node-bg: #2d2d30;
      --dag-node-border: #555;
      --dag-edge: #555;
      --dag-edge-arrow: #888;
    }

  * { box-sizing: border-box; }

  .wf-header {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 10px 14px;
    background: var(--wf-bg-alt);
    border-bottom: 1px solid var(--wf-border);
    flex-wrap: wrap;
  }
  .wf-header h2 { margin: 0; font-size: 14px; font-weight: 600; white-space: nowrap; }

  .wf-breadcrumb { display: flex; align-items: center; gap: 4px; font-size: 12px; color: var(--wf-text-muted); flex-wrap: wrap; }
  .wf-breadcrumb button { background: none; border: none; color: var(--wf-primary); cursor: pointer; font-size: 12px; padding: 0; font-family: var(--wf-font); }
  .wf-breadcrumb button:hover { text-decoration: underline; }
  .wf-breadcrumb .sep { color: var(--wf-text-muted); margin: 0 2px; }

  .wf-search { margin-left: auto; display: flex; gap: 6px; align-items: center; }
  .wf-search input { padding: 4px 8px; border: 1px solid var(--wf-border); border-radius: 4px; font-size: 12px; background: var(--wf-bg); color: var(--wf-text); font-family: var(--wf-font); width: 200px; }
  .wf-search input:focus { outline: none; border-color: var(--wf-primary); }

  .wf-btn { padding: 4px 10px; border: 1px solid var(--wf-border); border-radius: 4px; background: var(--wf-bg); color: var(--wf-text); cursor: pointer; font-size: 12px; font-family: var(--wf-font); white-space: nowrap; }
  .wf-btn:hover { background: var(--wf-bg-hover); }

  .wf-body { max-height: 600px; overflow: auto; }

  .wf-loading { padding: 30px; text-align: center; color: var(--wf-text-muted); }
  .wf-loading .spinner { display: inline-block; width: 20px; height: 20px; border: 2px solid var(--wf-border); border-top-color: var(--wf-primary); border-radius: 50%; animation: wf-spin 0.6s linear infinite; margin-right: 8px; vertical-align: middle; }
  @keyframes wf-spin { to { transform: rotate(360deg); } }

  .wf-error { padding: 10px 14px; background: #fef2f2; color: var(--wf-danger); border-bottom: 1px solid #fecaca; font-size: 12px; }
  :host(.dark-theme) .wf-error { background: #3b1f1f; border-color: #5c2b2b; }

  .wf-empty { padding: 30px; text-align: center; color: var(--wf-text-muted); }

  /* ---- Table ---- */
  table.wf-table { width: 100%; border-collapse: collapse; }
  .wf-table th { text-align: left; padding: 8px 14px; font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; color: var(--wf-text-muted); background: var(--wf-bg-alt); border-bottom: 1px solid var(--wf-border); position: sticky; top: 0; z-index: 1; }
  .wf-table td { padding: 7px 14px; border-bottom: 1px solid var(--wf-border); vertical-align: top; }
  .wf-table tr:hover td { background: var(--wf-bg-hover); }
  .wf-table tr.clickable { cursor: pointer; }

  .wf-link { color: var(--wf-primary); cursor: pointer; text-decoration: none; }
  .wf-link:hover { text-decoration: underline; }
  .wf-mono { font-family: var(--wf-font-mono); font-size: 12px; }
  .wf-muted { color: var(--wf-text-muted); font-size: 12px; }

  /* ---- Badges ---- */
  .wf-badge { display: inline-block; padding: 2px 7px; border-radius: 10px; font-size: 11px; font-weight: 500; white-space: nowrap; }
  .wf-badge-success { background: #d1fae5; color: #065f46; }
  .wf-badge-danger  { background: #fee2e2; color: #991b1b; }
  .wf-badge-warning { background: #fef3c7; color: #92400e; }
  .wf-badge-info    { background: #dbeafe; color: #1e40af; }
  .wf-badge-muted   { background: #f3f4f6; color: #6b7280; }
  :host(.dark-theme) .wf-badge-success { background: #064e3b; color: #6ee7b7; }
    :host(.dark-theme) .wf-badge-danger { background: #7f1d1d; color: #fca5a5; }
    :host(.dark-theme) .wf-badge-warning { background: #78350f; color: #fcd34d; }
    :host(.dark-theme) .wf-badge-info { background: #1e3a5f; color: #93c5fd; }
    :host(.dark-theme) .wf-badge-muted { background: #374151; color: #9ca3af; }

  /* ---- Detail panels ---- */
  .wf-detail { padding: 14px; }
  .wf-detail-grid { display: grid; grid-template-columns: 140px 1fr; gap: 4px 12px; margin-bottom: 16px; font-size: 12px; }
  .wf-detail-grid dt { color: var(--wf-text-muted); font-weight: 500; padding: 3px 0; }
  .wf-detail-grid dd { margin: 0; padding: 3px 0; word-break: break-all; }

  .wf-section-title { font-size: 13px; font-weight: 600; margin: 16px 0 8px; padding-bottom: 4px; border-bottom: 1px solid var(--wf-border); }
  .wf-tag { display: inline-block; padding: 1px 6px; margin: 1px 3px 1px 0; border-radius: 3px; font-size: 11px; background: var(--wf-bg-alt); border: 1px solid var(--wf-border); font-family: var(--wf-font-mono); }

  .wf-tabs { display: flex; gap: 0; border-bottom: 2px solid var(--wf-border); margin-bottom: 12px; }
  .wf-tab { padding: 6px 16px; cursor: pointer; font-size: 12px; font-weight: 500; border: none; background: none; color: var(--wf-text-muted); border-bottom: 2px solid transparent; margin-bottom: -2px; font-family: var(--wf-font); }
  .wf-tab:hover { color: var(--wf-text); }
  .wf-tab.active { color: var(--wf-primary); border-bottom-color: var(--wf-primary); }

  /* ---- DAG ---- */
  .wf-dag-container {
    display: flex;
    gap: 0;
    min-height: 200px;
  }
  .wf-dag-canvas {
    flex: 1;
    overflow: auto;
    min-height: 200px;
    position: relative;
  }
  .wf-dag-canvas svg {
    display: block;
  }
  .wf-dag-canvas .dag-node {
    cursor: pointer;
  }
  .wf-dag-canvas .dag-node rect {
    fill: var(--dag-node-bg);
    stroke: var(--dag-node-border);
    stroke-width: 1.5;
    rx: 6;
    ry: 6;
    transition: stroke 0.15s, stroke-width 0.15s;
  }
  .wf-dag-canvas .dag-node:hover rect {
    stroke: var(--wf-primary);
    stroke-width: 2;
  }
  .wf-dag-canvas .dag-node.selected rect {
    stroke: var(--wf-primary);
    stroke-width: 2.5;
    filter: drop-shadow(0 0 4px rgba(13, 110, 253, 0.35));
  }
  .wf-dag-canvas .dag-node .node-title {
    font-family: var(--wf-font-mono);
    font-size: 11px;
    font-weight: 600;
    fill: var(--wf-text);
  }
  .wf-dag-canvas .dag-node .node-type {
    font-family: var(--wf-font);
    font-size: 10px;
    fill: var(--wf-text-muted);
  }
  .wf-dag-canvas .dag-node .node-type-badge {
    rx: 3; ry: 3;
    fill: var(--wf-bg-alt);
    stroke: var(--wf-border);
    stroke-width: 0.5;
  }
  .wf-dag-canvas .dag-edge {
    fill: none;
    stroke: var(--dag-edge);
    stroke-width: 1.5;
  }
  .wf-dag-canvas .dag-arrow {
    fill: var(--dag-edge-arrow);
  }

  .wf-dag-detail {
    width: 300px;
    min-width: 300px;
    border-left: 1px solid var(--wf-border);
    padding: 14px;
    overflow-y: auto;
    background: var(--wf-bg-alt);
    font-size: 12px;
  }
  .wf-dag-detail h3 {
    margin: 0 0 10px;
    font-size: 13px;
    font-weight: 600;
    font-family: var(--wf-font-mono);
    word-break: break-all;
  }
  .wf-dag-detail .close-btn {
    float: right;
    background: none;
    border: none;
    font-size: 16px;
    cursor: pointer;
    color: var(--wf-text-muted);
    padding: 0 4px;
    line-height: 1;
  }
  .wf-dag-detail .close-btn:hover { color: var(--wf-text); }

  /* ---- Task output panel ---- */
  .wf-output-panel {
    margin-top: 16px;
    border: 1px solid var(--wf-border);
    border-radius: var(--wf-radius);
    overflow: hidden;
  }
  .wf-output-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 8px 12px;
    background: var(--wf-bg-alt);
    border-bottom: 1px solid var(--wf-border);
    font-weight: 600;
    font-size: 12px;
  }
  .wf-output-header .close-btn {
    background: none; border: none; font-size: 16px; cursor: pointer;
    color: var(--wf-text-muted); padding: 0 4px; line-height: 1;
  }
  .wf-output-header .close-btn:hover { color: var(--wf-text); }

  .wf-output-tabs {
    display: flex; gap: 0;
    border-bottom: 1px solid var(--wf-border);
    background: var(--wf-bg-alt);
  }
  .wf-output-tab {
    padding: 5px 14px; cursor: pointer; font-size: 11px; font-weight: 500;
    border: none; background: none; color: var(--wf-text-muted);
    border-bottom: 2px solid transparent; margin-bottom: -1px;
    font-family: var(--wf-font);
  }
  .wf-output-tab:hover { color: var(--wf-text); }
  .wf-output-tab.active { color: var(--wf-primary); border-bottom-color: var(--wf-primary); }
  .wf-output-tab.has-content { font-weight: 600; }
  .wf-output-tab.has-error { color: var(--wf-danger); }

  .wf-output-body {
    max-height: 320px;
    overflow: auto;
  }
  .wf-output-content {
    padding: 0;
    display: none;
  }
  .wf-output-content.active {
    display: block;
  }
  .wf-output-pre {
    margin: 0;
    padding: 10px 14px;
    font-family: var(--wf-font-mono);
    font-size: 12px;
    line-height: 1.5;
    white-space: pre-wrap;
    word-break: break-all;
    color: var(--wf-text);
    background: var(--wf-bg);
  }
  .wf-output-pre.error-text {
    color: var(--wf-danger);
  }
  .wf-output-empty {
    padding: 16px;
    text-align: center;
    color: var(--wf-text-muted);
    font-size: 12px;
  }
  .wf-output-loading {
    padding: 16px;
    text-align: center;
    color: var(--wf-text-muted);
    font-size: 12px;
  }
  .wf-output-loading .spinner {
    display: inline-block; width: 14px; height: 14px;
    border: 2px solid var(--wf-border); border-top-color: var(--wf-primary);
    border-radius: 50%; animation: wf-spin 0.6s linear infinite;
    margin-right: 6px; vertical-align: middle;
  }
  .wf-truncated {
    padding: 4px 14px 8px;
    font-size: 11px;
    color: var(--wf-warning);
    font-style: italic;
  }

  .wf-table tr.task-selected td {
    background: color-mix(in srgb, var(--wf-primary) 10%, var(--wf-bg));
  }
`;

// ===================================================================
// Helpers
// ===================================================================

function stateBadge(state) {
  if (!state) return "";
  const s = String(state).toUpperCase();
  let cls = "muted";
  if (s === "SUCCESS") cls = "success";
  else if (["FAILED", "INTERNAL_ERROR", "TIMEDOUT", "CANCELED"].includes(s)) cls = "danger";
  else if (["RUNNING", "PENDING", "QUEUED", "BLOCKED", "WAITING_FOR_RETRY", "TERMINATING"].includes(s)) cls = "info";
  else if (["EXCLUDED", "SKIPPED", "UPSTREAM_CANCELED", "UPSTREAM_FAILED", "DISABLED", "SUCCESS_WITH_FAILURES"].includes(s)) cls = "warning";
  return `<span class="wf-badge wf-badge-${cls}">${s}</span>`;
}

function typeBadgeColor(type) {
  const m = {
    notebook: "#6f42c1", sql: "#0d6efd", pipeline: "#198754", dbt: "#fd7e14",
    spark_python: "#e83e8c", python_wheel: "#e83e8c", spark_jar: "#d63384",
    run_job: "#20c997", condition: "#6c757d", for_each: "#6c757d", dashboard: "#0dcaf0",
  };
  return m[type] || "#6c757d";
}

function esc(s) {
  if (s == null) return "";
  const d = document.createElement("div");
  d.textContent = String(s);
  return d.innerHTML;
}

// ===================================================================
// DAG Layout Engine
// ===================================================================

function layoutDAG(tasks) {
  // Build adjacency: task_key -> { task, children[], parents[] }
  const map = new Map();
  for (const t of tasks) {
    map.set(t.task_key, { task: t, children: [], parents: [] });
  }
  for (const t of tasks) {
    for (const dep of t.depends_on || []) {
      const parent = map.get(dep);
      if (parent) {
        parent.children.push(t.task_key);
        map.get(t.task_key).parents.push(dep);
      }
    }
  }

  // Assign layers via longest-path from roots
  const layer = new Map();
  function longestPath(key) {
    if (layer.has(key)) return layer.get(key);
    const node = map.get(key);
    if (!node || node.parents.length === 0) { layer.set(key, 0); return 0; }
    const d = 1 + Math.max(...node.parents.map(longestPath));
    layer.set(key, d);
    return d;
  }
  for (const key of map.keys()) longestPath(key);

  // Group by layer
  const layers = [];
  for (const [key, l] of layer) {
    while (layers.length <= l) layers.push([]);
    layers[l].push(key);
  }

  // Sort within each layer to reduce crossings: order by average parent position
  for (let li = 1; li < layers.length; li++) {
    const prevOrder = new Map();
    layers[li - 1].forEach((k, i) => prevOrder.set(k, i));
    layers[li].sort((a, b) => {
      const avgA = avg(map.get(a).parents.map((p) => prevOrder.get(p) ?? 0));
      const avgB = avg(map.get(b).parents.map((p) => prevOrder.get(p) ?? 0));
      return avgA - avgB;
    });
  }

  // Compute node positions
  const NODE_W = 160, NODE_H = 48, PAD_X = 60, PAD_Y = 28, MARGIN = 20;
  const positions = new Map();
  const maxPerLayer = Math.max(...layers.map((l) => l.length), 1);

  for (let li = 0; li < layers.length; li++) {
    const col = layers[li];
    const totalH = col.length * NODE_H + (col.length - 1) * PAD_Y;
    const startY = MARGIN + (maxPerLayer * NODE_H + (maxPerLayer - 1) * PAD_Y - totalH) / 2;
    for (let ni = 0; ni < col.length; ni++) {
      positions.set(col[ni], {
        x: MARGIN + li * (NODE_W + PAD_X),
        y: startY + ni * (NODE_H + PAD_Y),
      });
    }
  }

  const svgW = MARGIN * 2 + layers.length * NODE_W + (layers.length - 1) * PAD_X;
  const svgH = MARGIN * 2 + maxPerLayer * NODE_H + (maxPerLayer - 1) * PAD_Y;

  // Build edges
  const edges = [];
  for (const t of tasks) {
    for (const dep of t.depends_on || []) {
      if (positions.has(dep) && positions.has(t.task_key)) {
        edges.push({ from: dep, to: t.task_key });
      }
    }
  }

  return { positions, edges, svgW: Math.max(svgW, 200), svgH: Math.max(svgH, 100), NODE_W, NODE_H, map };
}

function avg(arr) { return arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : 0; }

// ===================================================================
// DAG Renderer (pure SVG)
// ===================================================================

function renderDAGSvg(tasks, selectedKey) {
  if (!tasks || !tasks.length) return { svg: "", layout: null };

  const layout = layoutDAG(tasks);
  const { positions, edges, svgW, svgH, NODE_W, NODE_H } = layout;

  let svg = `<svg xmlns="http://www.w3.org/2000/svg" width="${svgW}" height="${svgH}" viewBox="0 0 ${svgW} ${svgH}">`;

  // Arrowhead marker
  svg += `<defs><marker id="dag-arrowhead" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto"><polygon points="0 0, 8 3, 0 6" class="dag-arrow"/></marker></defs>`;

  // Edges
  for (const e of edges) {
    const from = positions.get(e.from);
    const to = positions.get(e.to);
    const x1 = from.x + NODE_W;
    const y1 = from.y + NODE_H / 2;
    const x2 = to.x;
    const y2 = to.y + NODE_H / 2;
    const cx1 = x1 + (x2 - x1) * 0.4;
    const cx2 = x2 - (x2 - x1) * 0.4;
    svg += `<path class="dag-edge" d="M${x1},${y1} C${cx1},${y1} ${cx2},${y2} ${x2},${y2}" marker-end="url(#dag-arrowhead)"/>`;
  }

  // Nodes
  for (const t of tasks) {
    const pos = positions.get(t.task_key);
    if (!pos) continue;
    const sel = t.task_key === selectedKey ? " selected" : "";
    const dis = t.disabled ? 0.45 : 1;
    const tc = typeBadgeColor(t.type);

    svg += `<g class="dag-node${sel}" data-task-key="${esc(t.task_key)}" opacity="${dis}">`;
    svg += `<rect x="${pos.x}" y="${pos.y}" width="${NODE_W}" height="${NODE_H}"/>`;

    // Task key label
    const label = t.task_key.length > 18 ? t.task_key.slice(0, 17) + "…" : t.task_key;
    svg += `<text class="node-title" x="${pos.x + 10}" y="${pos.y + 19}" textLength="${Math.min(label.length * 6.6, NODE_W - 20)}" lengthAdjust="spacingAndGlyphs">${esc(label)}</text>`;

    // Type badge
    const typeLabel = t.type;
    const tw = typeLabel.length * 6 + 10;
    svg += `<rect class="node-type-badge" x="${pos.x + 8}" y="${pos.y + 27}" width="${tw}" height="${15}" style="fill:${tc}15;stroke:${tc}60"/>`;
    svg += `<text class="node-type" x="${pos.x + 13}" y="${pos.y + 38}" style="fill:${tc}">${esc(typeLabel)}</text>`;

    svg += `</g>`;
  }

  svg += `</svg>`;
  return { svg, layout };
}

// ===================================================================
// Task detail panel (right side)
// ===================================================================

function renderTaskDetailPanel(task) {
  if (!task) return "";
  let html = `<button class="close-btn" data-action="close-detail">✕</button>`;
  html += `<h3>${esc(task.task_key)}</h3>`;
  html += `<dl class="wf-detail-grid">`;
  html += `<dt>Type</dt><dd>${stateBadge(task.type) || esc(task.type)}</dd>`;
  if (task.detail) html += `<dt>Detail</dt><dd class="wf-mono">${esc(task.detail)}</dd>`;
  if (task.description) html += `<dt>Description</dt><dd>${esc(task.description)}</dd>`;
  html += `<dt>Disabled</dt><dd>${task.disabled ? "Yes" : "No"}</dd>`;

  if (task.depends_on && task.depends_on.length) {
    html += `<dt>Upstream</dt><dd>`;
    html += task.depends_on.map((d) => `<span class="wf-tag" data-nav-task="${esc(d)}">${esc(d)}</span>`).join(" ");
    html += `</dd>`;
  } else {
    html += `<dt>Upstream</dt><dd class="wf-muted">— (root task)</dd>`;
  }

  if (task.downstream && task.downstream.length) {
    html += `<dt>Downstream</dt><dd>`;
    html += task.downstream.map((d) => `<span class="wf-tag" data-nav-task="${esc(d)}">${esc(d)}</span>`).join(" ");
    html += `</dd>`;
  } else {
    html += `<dt>Downstream</dt><dd class="wf-muted">— (leaf task)</dd>`;
  }

  html += `</dl>`;
  return html;
}

// ===================================================================
// View renderers
// ===================================================================

function renderJobsList(root, jobs, model) {
  if (!jobs.length) {
    root.querySelector(".wf-body").innerHTML = `<div class="wf-empty">No workflows found.</div>`;
    return;
  }
  let html = `<table class="wf-table"><thead><tr>
    <th>Name</th><th>Job ID</th><th>Tasks</th><th>Schedule</th><th>Creator</th><th>Created</th>
  </tr></thead><tbody>`;
  for (const j of jobs) {
    const tags = j.tags && Object.keys(j.tags).length
      ? Object.entries(j.tags).map(([k, v]) => `<span class="wf-tag">${esc(k)}=${esc(v)}</span>`).join("")
      : "";
    html += `<tr class="clickable" data-job-id="${j.job_id}">
      <td><span class="wf-link">${esc(j.name || "(unnamed)")}</span>${tags ? "<br>" + tags : ""}</td>
      <td class="wf-mono">${esc(j.job_id)}</td>
      <td>${j.task_count || 0}</td>
      <td class="wf-mono wf-muted">${esc(j.schedule || "—")}</td>
      <td class="wf-muted">${esc(j.creator || "—")}</td>
      <td class="wf-muted">${esc(j.created_time || "—")}</td>
    </tr>`;
  }
  html += `</tbody></table>`;
  root.querySelector(".wf-body").innerHTML = html;

  root.querySelectorAll("tr[data-job-id]").forEach((tr) => {
    tr.addEventListener("click", () => {
      const jobId = parseInt(tr.dataset.jobId);
      model.set("request", JSON.stringify({ action: "get_job", job_id: jobId }));
      model.save_changes();
      setTimeout(() => {
        model.set("request", JSON.stringify({ action: "list_runs", job_id: jobId }));
        model.save_changes();
      }, 50);
    });
  });
}

function renderJobDetail(root, detail, runs, model) {
  const body = root.querySelector(".wf-body");
  let selectedTaskKey = null;

  function renderTabs() {
    let html = `<div class="wf-detail">`;
    html += `<div class="wf-tabs">
      <button class="wf-tab active" data-tab="tasks">Tasks (${(detail.tasks || []).length})</button>
      <button class="wf-tab" data-tab="runs">Runs</button>
      <button class="wf-tab" data-tab="info">Info</button>
    </div>`;

    // ---- Tasks tab (DAG) ----
    html += `<div class="wf-tab-content" data-tab="tasks">`;
    if (detail.tasks && detail.tasks.length) {
      const { svg } = renderDAGSvg(detail.tasks, selectedTaskKey);
      const selectedTask = selectedTaskKey ? detail.tasks.find((t) => t.task_key === selectedTaskKey) : null;

      html += `<div class="wf-dag-container">`;
      html += `<div class="wf-dag-canvas">${svg}</div>`;
      if (selectedTask) {
        html += `<div class="wf-dag-detail">${renderTaskDetailPanel(selectedTask)}</div>`;
      }
      html += `</div>`;
    } else {
      html += `<div class="wf-empty">No tasks defined.</div>`;
    }
    html += `</div>`;

    // ---- Runs tab ----
    html += `<div class="wf-tab-content" data-tab="runs" style="display:none">`;
    if (runs && runs.length) {
      html += `<table class="wf-table"><thead><tr>
        <th>Run ID</th><th>State</th><th>Result</th><th>Duration</th><th>Start</th><th>Trigger</th><th></th>
      </tr></thead><tbody>`;
      for (const r of runs) {
        html += `<tr class="clickable" data-run-id="${r.run_id}">
          <td class="wf-mono"><span class="wf-link">${esc(r.run_id)}</span></td>
          <td>${stateBadge(r.life_cycle_state)}</td>
          <td>${stateBadge(r.result_state)}</td>
          <td class="wf-muted">${esc(r.duration || "—")}</td>
          <td class="wf-muted">${esc(r.start_time || "—")}</td>
          <td class="wf-muted">${esc(r.trigger || r.run_type || "—")}</td>
          <td>${r.run_page_url ? `<a class="wf-link" href="${esc(r.run_page_url)}" target="_blank" title="Open in Databricks">↗</a>` : ""}</td>
        </tr>`;
      }
      html += `</tbody></table>`;
    } else {
      html += `<div class="wf-empty">No runs found.</div>`;
    }
    html += `</div>`;

    // ---- Info tab ----
    html += `<div class="wf-tab-content" data-tab="info" style="display:none">`;
    html += `<dl class="wf-detail-grid">`;
    html += `<dt>Job ID</dt><dd class="wf-mono">${esc(detail.job_id)}</dd>`;
    if (detail.description) html += `<dt>Description</dt><dd>${esc(detail.description)}</dd>`;
    if (detail.schedule)
      html += `<dt>Schedule</dt><dd class="wf-mono">${esc(detail.schedule)}${detail.schedule_tz ? ` (${esc(detail.schedule_tz)})` : ""}</dd>`;
    html += `<dt>Creator</dt><dd>${esc(detail.creator || "—")}</dd>`;
    html += `<dt>Created</dt><dd>${esc(detail.created_time || "—")}</dd>`;
    if (detail.max_concurrent_runs != null)
      html += `<dt>Max concurrent</dt><dd>${detail.max_concurrent_runs}</dd>`;
    if (detail.timeout_seconds != null)
      html += `<dt>Timeout</dt><dd>${detail.timeout_seconds}s</dd>`;
    html += `</dl>`;
    if (detail.tags && Object.keys(detail.tags).length) {
      html += `<div class="wf-section-title">Tags</div>`;
      html += Object.entries(detail.tags).map(([k, v]) => `<span class="wf-tag">${esc(k)}=${esc(v)}</span>`).join(" ");
    }
    html += `</div>`;

    html += `</div>`;
    return html;
  }

  function renderAndBind(activeTab) {
    body.innerHTML = renderTabs();

    // Restore active tab
    if (activeTab && activeTab !== "tasks") {
      body.querySelectorAll(".wf-tab").forEach((t) => t.classList.remove("active"));
      body.querySelectorAll(".wf-tab-content").forEach((c) => (c.style.display = "none"));
      const tab = body.querySelector(`.wf-tab[data-tab="${activeTab}"]`);
      const content = body.querySelector(`.wf-tab-content[data-tab="${activeTab}"]`);
      if (tab) tab.classList.add("active");
      if (content) content.style.display = "";
    }

    // Tab switching
    body.querySelectorAll(".wf-tab").forEach((tab) => {
      tab.addEventListener("click", () => {
        body.querySelectorAll(".wf-tab").forEach((t) => t.classList.remove("active"));
        tab.classList.add("active");
        body.querySelectorAll(".wf-tab-content").forEach((c) => (c.style.display = "none"));
        const target = body.querySelector(`.wf-tab-content[data-tab="${tab.dataset.tab}"]`);
        if (target) target.style.display = "";
      });
    });

    // DAG node click
    body.querySelectorAll(".dag-node").forEach((node) => {
      node.addEventListener("click", () => {
        const key = node.dataset.taskKey;
        selectedTaskKey = selectedTaskKey === key ? null : key;
        const activeTabEl = body.querySelector(".wf-tab.active");
        renderAndBind(activeTabEl ? activeTabEl.dataset.tab : "tasks");
      });
    });

    // Detail panel close
    const closeBtn = body.querySelector("[data-action='close-detail']");
    if (closeBtn) {
      closeBtn.addEventListener("click", () => {
        selectedTaskKey = null;
        const activeTabEl = body.querySelector(".wf-tab.active");
        renderAndBind(activeTabEl ? activeTabEl.dataset.tab : "tasks");
      });
    }

    // Dep/downstream tag click → select that task in the DAG
    body.querySelectorAll(".wf-dag-detail [data-nav-task]").forEach((tag) => {
      tag.style.cursor = "pointer";
      tag.addEventListener("click", () => {
        const depKey = tag.dataset.navTask;
        if (detail.tasks.find((t) => t.task_key === depKey)) {
          selectedTaskKey = depKey;
          renderAndBind("tasks");
        }
      });
    });

    // Run click handlers
    body.querySelectorAll("tr[data-run-id]").forEach((tr) => {
      tr.addEventListener("click", (e) => {
        if (e.target.tagName === "A") return;
        const runId = parseInt(tr.dataset.runId);
        model.set("request", JSON.stringify({ action: "get_run", run_id: runId }));
        model.save_changes();
      });
    });
  }

  renderAndBind("tasks");
}

function renderRunDetail(root, detail, model) {
  const body = root.querySelector(".wf-body");
  let selectedTaskRunId = null;
  let selectedTaskKey = null;
  let outputLoading = false;

  // Listen for task_output changes while this view is active
  function onTaskOutput() {
    outputLoading = false;
    rerender();
  }
  model.on("change:task_output", onTaskOutput);

  function rerender() {
    let html = `<div class="wf-detail">`;

    html += `<dl class="wf-detail-grid">`;
    html += `<dt>Run ID</dt><dd class="wf-mono">${esc(detail.run_id)}</dd>`;
    html += `<dt>State</dt><dd>${stateBadge(detail.life_cycle_state)} ${stateBadge(detail.result_state)}</dd>`;
    if (detail.state_message) html += `<dt>Message</dt><dd class="wf-muted">${esc(detail.state_message)}</dd>`;
    html += `<dt>Start</dt><dd>${esc(detail.start_time || "—")}</dd>`;
    html += `<dt>End</dt><dd>${esc(detail.end_time || "—")}</dd>`;
    html += `<dt>Duration</dt><dd>${esc(detail.duration || "—")}</dd>`;
    if (detail.setup_duration) html += `<dt>Setup</dt><dd>${esc(detail.setup_duration)}</dd>`;
    if (detail.run_page_url) html += `<dt>Link</dt><dd><a class="wf-link" href="${esc(detail.run_page_url)}" target="_blank">Open in Databricks ↗</a></dd>`;
    html += `</dl>`;

    if (detail.tasks && detail.tasks.length) {
      html += `<div class="wf-section-title">Tasks (${detail.tasks.length})</div>`;
      html += `<table class="wf-table"><thead><tr>
        <th>Task Key</th><th>Type</th><th>State</th><th>Result</th><th>Duration</th><th>Start</th><th></th>
      </tr></thead><tbody>`;
      for (const t of detail.tasks) {
        const st = t.state || {};
        const isSel = t.run_id != null && t.run_id === selectedTaskRunId;
        const selCls = isSel ? " task-selected" : "";
        html += `<tr class="clickable${selCls}" data-task-run-id="${t.run_id || ""}" data-task-key="${esc(t.task_key)}">
          <td class="wf-mono"><span class="wf-link">${esc(t.task_key)}</span></td>
          <td class="wf-muted">${esc(t.type)}</td>
          <td>${stateBadge(st.life_cycle_state)}</td>
          <td>${stateBadge(st.result_state)}</td>
          <td class="wf-muted">${esc(t.duration || "—")}</td>
          <td class="wf-muted">${esc(t.start_time || "—")}</td>
          <td>${t.run_page_url ? `<a class="wf-link" href="${esc(t.run_page_url)}" target="_blank">↗</a>` : ""}</td>
        </tr>`;
        if (st.state_message && st.result_state && st.result_state !== "SUCCESS") {
          html += `<tr><td></td><td colspan="6" class="wf-muted" style="font-size:11px;padding-top:0">${esc(st.state_message)}</td></tr>`;
        }
      }
      html += `</tbody></table>`;
    }

    // Output panel
    if (selectedTaskRunId != null) {
      html += renderOutputPanel(selectedTaskKey, outputLoading, model);
    }

    html += `</div>`;
    body.innerHTML = html;

    // Bind task row clicks
    body.querySelectorAll("tr[data-task-run-id]").forEach((tr) => {
      tr.addEventListener("click", (e) => {
        if (e.target.tagName === "A") return;
        const runId = tr.dataset.taskRunId ? parseInt(tr.dataset.taskRunId) : null;
        const key = tr.dataset.taskKey;
        if (runId == null) return;
        if (runId === selectedTaskRunId) {
          // Toggle off
          selectedTaskRunId = null;
          selectedTaskKey = null;
          rerender();
        } else {
          selectedTaskRunId = runId;
          selectedTaskKey = key;
          outputLoading = true;
          rerender();
          model.set("request", JSON.stringify({ action: "get_task_output", run_id: runId }));
          model.save_changes();
        }
      });
    });

    // Output panel tab switching
    body.querySelectorAll(".wf-output-tab").forEach((tab) => {
      tab.addEventListener("click", () => {
        body.querySelectorAll(".wf-output-tab").forEach((t) => t.classList.remove("active"));
        tab.classList.add("active");
        body.querySelectorAll(".wf-output-content").forEach((c) => c.classList.remove("active"));
        const target = body.querySelector(`.wf-output-content[data-output-tab="${tab.dataset.outputTab}"]`);
        if (target) target.classList.add("active");
      });
    });

    // Output panel close
    body.querySelector(".wf-output-header .close-btn")?.addEventListener("click", () => {
      selectedTaskRunId = null;
      selectedTaskKey = null;
      rerender();
    });
  }

  rerender();

  // Return cleanup function to remove listener when view changes
  return () => { model.off("change:task_output", onTaskOutput); };
}

function renderOutputPanel(taskKey, isLoading, model) {
  let html = `<div class="wf-output-panel">`;
  html += `<div class="wf-output-header">`;
  html += `<span>Output: ${esc(taskKey)}</span>`;
  html += `<button class="close-btn">✕</button>`;
  html += `</div>`;

  if (isLoading) {
    html += `<div class="wf-output-loading"><span class="spinner"></span> Loading output…</div>`;
    html += `</div>`;
    return html;
  }

  const output = JSON.parse(model.get("task_output") || "{}");

  // Build tabs based on available content
  const tabs = [];
  if (output.error || output.error_trace) tabs.push({ key: "error", label: "Error", isError: true });
  if (output.logs) tabs.push({ key: "logs", label: "Logs" });
  if (output.notebook_result) tabs.push({ key: "notebook", label: "Notebook Result" });
  if (output.sql_output) tabs.push({ key: "sql", label: "SQL Output" });
  if (output.dbt_output) tabs.push({ key: "dbt", label: "DBT Output" });
  if (output.info) tabs.push({ key: "info", label: "Info" });

  if (tabs.length === 0) {
    html += `<div class="wf-output-empty">No output available for this task.</div>`;
    html += `</div>`;
    return html;
  }

  // Default to first tab (error if present, then logs, etc.)
  html += `<div class="wf-output-tabs">`;
  for (let i = 0; i < tabs.length; i++) {
    const t = tabs[i];
    const active = i === 0 ? " active" : "";
    const errorCls = t.isError ? " has-error" : "";
    html += `<button class="wf-output-tab has-content${active}${errorCls}" data-output-tab="${t.key}">${t.label}</button>`;
  }
  html += `</div>`;

  html += `<div class="wf-output-body">`;

  // Error tab
  if (output.error || output.error_trace) {
    const active = tabs[0].key === "error" ? " active" : "";
    html += `<div class="wf-output-content${active}" data-output-tab="error">`;
    if (output.error) html += `<pre class="wf-output-pre error-text">${esc(output.error)}</pre>`;
    if (output.error_trace) html += `<pre class="wf-output-pre error-text">${esc(output.error_trace)}</pre>`;
    html += `</div>`;
  }

  // Logs tab
  if (output.logs) {
    const active = tabs[0].key === "logs" ? " active" : "";
    html += `<div class="wf-output-content${active}" data-output-tab="logs">`;
    html += `<pre class="wf-output-pre">${esc(output.logs)}</pre>`;
    if (output.logs_truncated) html += `<div class="wf-truncated">⚠ Logs truncated — view full output in Databricks</div>`;
    html += `</div>`;
  }

  // Notebook result tab
  if (output.notebook_result) {
    const active = tabs[0].key === "notebook" ? " active" : "";
    html += `<div class="wf-output-content${active}" data-output-tab="notebook">`;
    html += `<pre class="wf-output-pre">${esc(output.notebook_result)}</pre>`;
    if (output.notebook_result_truncated) html += `<div class="wf-truncated">⚠ Result truncated</div>`;
    html += `</div>`;
  }

  // SQL output tab
  if (output.sql_output) {
    const active = tabs[0].key === "sql" ? " active" : "";
    html += `<div class="wf-output-content${active}" data-output-tab="sql">`;
    html += `<pre class="wf-output-pre">${esc(output.sql_output)}</pre>`;
    html += `</div>`;
  }

  // DBT output tab
  if (output.dbt_output) {
    const active = tabs[0].key === "dbt" ? " active" : "";
    html += `<div class="wf-output-content${active}" data-output-tab="dbt">`;
    html += `<pre class="wf-output-pre">${esc(output.dbt_output)}</pre>`;
    html += `</div>`;
  }

  // Info tab
  if (output.info) {
    const active = tabs[0].key === "info" ? " active" : "";
    html += `<div class="wf-output-content${active}" data-output-tab="info">`;
    html += `<pre class="wf-output-pre">${esc(output.info)}</pre>`;
    html += `</div>`;
  }

  html += `</div></div>`;
  return html;
}

// ===================================================================
// Main render / state machine
// ===================================================================

const VIEW = { JOBS: "jobs", JOB: "job", RUN: "run" };


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

  let currentView = VIEW.JOBS;
  let currentJobName = "";
  let currentJobId = null;
  let currentRunId = null;
  let searchValue = "";
  let cleanupRunView = null;

  function buildHeader() {
    let html = `<div class="wf-header"><h2>⚡ Workflows</h2><div class="wf-breadcrumb">`;
    if (currentView === VIEW.JOBS) {
      html += `<span>All Jobs</span>`;
    } else if (currentView === VIEW.JOB) {
      html += `<button data-nav="jobs">All Jobs</button><span class="sep">›</span><span>${esc(currentJobName || currentJobId)}</span>`;
    } else if (currentView === VIEW.RUN) {
      html += `<button data-nav="jobs">All Jobs</button><span class="sep">›</span><button data-nav="job">${esc(currentJobName || currentJobId)}</button><span class="sep">›</span><span>Run ${esc(currentRunId)}</span>`;
    }
    html += `</div>`;
    if (currentView === VIEW.JOBS) {
      html += `<div class="wf-search"><input type="text" placeholder="Filter by name…" value="${esc(searchValue)}" /><button class="wf-btn" data-action="search">Search</button><button class="wf-btn" data-action="refresh">↻</button></div>`;
    } else {
      html += `<div class="wf-search"><button class="wf-btn" data-action="refresh">↻ Refresh</button></div>`;
    }
    html += `</div>`;
    return html;
  }

  function fullRender() {
    root.innerHTML = buildHeader() + `<div class="wf-body"></div>`;

    const err = model.get("error_message");
    if (err) {
      const errDiv = document.createElement("div");
      errDiv.className = "wf-error";
      errDiv.textContent = err;
      root.querySelector(".wf-body").before(errDiv);
    }

    if (model.get("loading")) {
      root.querySelector(".wf-body").innerHTML = `<div class="wf-loading"><span class="spinner"></span> Loading…</div>`;
      bindHeaderEvents();
      return;
    }

    if (currentView === VIEW.JOBS) {
      const jobs = JSON.parse(model.get("jobs_data") || "[]");
      renderJobsList(root, jobs, model);
    } else if (currentView === VIEW.JOB) {
      const detail = JSON.parse(model.get("job_detail") || "{}");
      const runs_list = JSON.parse(model.get("runs_data") || "[]");
      if (detail.name) currentJobName = detail.name;
      renderJobDetail(root, detail, runs_list, model);
    } else if (currentView === VIEW.RUN) {
      const detail = JSON.parse(model.get("run_detail") || "{}");
      if (cleanupRunView) { cleanupRunView(); cleanupRunView = null; }
      cleanupRunView = renderRunDetail(root, detail, model);
    }

    bindHeaderEvents();
  }

  function bindHeaderEvents() {
    root.querySelectorAll("[data-nav]").forEach((btn) => {
      btn.addEventListener("click", () => {
        const target = btn.dataset.nav;
        if (target === "jobs") {
          if (cleanupRunView) { cleanupRunView(); cleanupRunView = null; }
          currentView = VIEW.JOBS; currentJobId = null; currentRunId = null;
          fullRender();
        } else if (target === "job" && currentJobId) {
          if (cleanupRunView) { cleanupRunView(); cleanupRunView = null; }
          currentView = VIEW.JOB; currentRunId = null;
          model.set("request", JSON.stringify({ action: "get_job", job_id: currentJobId }));
          model.save_changes();
          setTimeout(() => { model.set("request", JSON.stringify({ action: "list_runs", job_id: currentJobId })); model.save_changes(); }, 50);
        }
      });
    });

    const searchInput = root.querySelector(".wf-search input");
    if (searchInput) {
      const doSearch = () => {
        searchValue = searchInput.value.trim();
        model.set("request", JSON.stringify({ action: "list_jobs", name_filter: searchValue || null }));
        model.save_changes();
      };
      searchInput.addEventListener("keydown", (e) => { if (e.key === "Enter") doSearch(); });
      root.querySelector("[data-action='search']")?.addEventListener("click", doSearch);
    }

    root.querySelector("[data-action='refresh']")?.addEventListener("click", () => {
      if (currentView === VIEW.JOBS) {
        model.set("request", JSON.stringify({ action: "list_jobs", name_filter: searchValue || null }));
        model.save_changes();
      } else if (currentView === VIEW.JOB && currentJobId) {
        model.set("request", JSON.stringify({ action: "get_job", job_id: currentJobId }));
        model.save_changes();
        setTimeout(() => { model.set("request", JSON.stringify({ action: "list_runs", job_id: currentJobId })); model.save_changes(); }, 50);
      } else if (currentView === VIEW.RUN && currentRunId) {
        model.set("request", JSON.stringify({ action: "get_run", run_id: currentRunId }));
        model.save_changes();
      }
    });
  }

  model.on("change:job_detail", () => {
    const detail = JSON.parse(model.get("job_detail") || "{}");
    if (detail.job_id != null) {
      currentView = VIEW.JOB; currentJobId = detail.job_id; currentJobName = detail.name || ""; currentRunId = null;
      fullRender();
    }
  });

  model.on("change:run_detail", () => {
    const detail = JSON.parse(model.get("run_detail") || "{}");
    if (detail.run_id != null) { currentView = VIEW.RUN; currentRunId = detail.run_id; fullRender(); }
  });

  model.on("change:jobs_data", () => { if (currentView === VIEW.JOBS) fullRender(); });
  model.on("change:runs_data", () => { if (currentView === VIEW.JOB) fullRender(); });
  model.on("change:loading", fullRender);
  model.on("change:error_message", fullRender);

  fullRender();
}

export default { render };
