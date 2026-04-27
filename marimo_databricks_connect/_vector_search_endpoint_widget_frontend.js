// Vector Search Endpoint Widget Frontend
const S = `
  :host { --op-bg:#ffffff;--op-bg-alt:#f8f9fa;--op-bg-hover:#e9ecef;--op-border:#dee2e6;--op-text:#212529;--op-text-muted:#6c757d;--op-primary:#0d6efd;--op-success:#198754;--op-danger:#dc3545;--op-warning:#ffc107;--op-info:#0dcaf0;--op-font:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"Helvetica Neue",Arial,sans-serif;--op-font-mono:"SF Mono","Cascadia Code","Fira Code",Menlo,Consolas,monospace;--op-radius:6px;display:block;font-family:var(--op-font);font-size:13px;color:var(--op-text);background:var(--op-bg);border:1px solid var(--op-border);border-radius:var(--op-radius);overflow:hidden; }
  @media(prefers-color-scheme:dark){:host{--op-bg:#1e1e1e;--op-bg-alt:#252526;--op-bg-hover:#2d2d30;--op-border:#3e3e42;--op-text:#ccc;--op-text-muted:#888;--op-primary:#4fc3f7;--op-success:#66bb6a;--op-danger:#ef5350;--op-warning:#ffca28;--op-info:#4dd0e1;}}
  *{box-sizing:border-box}
  .op-header{display:flex;align-items:center;gap:8px;padding:10px 14px;background:var(--op-bg-alt);border-bottom:1px solid var(--op-border);flex-wrap:wrap}
  .op-header h2{margin:0;font-size:14px;font-weight:600}
  .op-header-actions{margin-left:auto;display:flex;gap:6px;align-items:center}
  .op-btn{padding:4px 10px;border:1px solid var(--op-border);border-radius:4px;background:var(--op-bg);color:var(--op-text);cursor:pointer;font-size:12px;font-family:var(--op-font)}
  .op-btn:hover{background:var(--op-bg-hover)}
  .op-btn-primary{background:var(--op-primary);color:#fff;border-color:var(--op-primary)}
  .op-body{max-height:600px;overflow:auto}
  .op-loading{padding:30px;text-align:center;color:var(--op-text-muted)}
  .op-loading .spinner{display:inline-block;width:20px;height:20px;border:2px solid var(--op-border);border-top-color:var(--op-primary);border-radius:50%;animation:op-spin .6s linear infinite;margin-right:8px;vertical-align:middle}
  @keyframes op-spin{to{transform:rotate(360deg)}}
  .op-error{padding:10px 14px;background:#fef2f2;color:var(--op-danger);border-bottom:1px solid #fecaca;font-size:12px}
  @media(prefers-color-scheme:dark){.op-error{background:#3b1f1f;border-color:#5c2b2b}}
  .op-detail{padding:14px}
  .op-kv{display:grid;grid-template-columns:180px 1fr;gap:4px 12px;margin-bottom:16px;font-size:12px}
  .op-kv dt{color:var(--op-text-muted);font-weight:500;padding:3px 0}
  .op-kv dd{margin:0;padding:3px 0;word-break:break-all}
  .op-section{font-size:13px;font-weight:600;margin:16px 0 8px;padding-bottom:4px;border-bottom:1px solid var(--op-border)}
  table.op-table{width:100%;border-collapse:collapse}
  .op-table th{text-align:left;padding:8px 14px;font-size:11px;font-weight:600;text-transform:uppercase;color:var(--op-text-muted);background:var(--op-bg-alt);border-bottom:1px solid var(--op-border);position:sticky;top:0;z-index:1}
  .op-table td{padding:7px 14px;border-bottom:1px solid var(--op-border);vertical-align:top}
  .op-table tr:hover td{background:var(--op-bg-hover)}
  .op-badge{display:inline-block;padding:2px 7px;border-radius:10px;font-size:11px;font-weight:500}
  .op-badge-success{background:#d1fae5;color:#065f46}.op-badge-danger{background:#fee2e2;color:#991b1b}.op-badge-info{background:#dbeafe;color:#1e40af}.op-badge-warning{background:#fef3c7;color:#92400e}.op-badge-muted{background:#f3f4f6;color:#6b7280}
  @media(prefers-color-scheme:dark){.op-badge-success{background:#064e3b;color:#6ee7b7}.op-badge-danger{background:#7f1d1d;color:#fca5a5}.op-badge-info{background:#1e3a5f;color:#93c5fd}.op-badge-warning{background:#78350f;color:#fcd34d}.op-badge-muted{background:#374151;color:#9ca3af}}
  .op-tabs{display:flex;gap:0;border-bottom:2px solid var(--op-border);margin-bottom:12px}
  .op-tab{padding:6px 16px;cursor:pointer;font-size:12px;font-weight:500;border:none;background:none;color:var(--op-text-muted);border-bottom:2px solid transparent;margin-bottom:-2px;font-family:var(--op-font)}
  .op-tab:hover{color:var(--op-text)}.op-tab.active{color:var(--op-primary);border-bottom-color:var(--op-primary)}
  .op-mono{font-family:var(--op-font-mono);font-size:12px}
  .op-muted{color:var(--op-text-muted);font-size:12px}
  .op-tag{display:inline-block;padding:1px 6px;margin:1px 3px 1px 0;border-radius:3px;font-size:11px;background:var(--op-bg-alt);border:1px solid var(--op-border);font-family:var(--op-font-mono)}
  .op-empty{padding:30px;text-align:center;color:var(--op-text-muted)}
  .op-auto-refresh{display:flex;align-items:center;gap:4px;font-size:11px;color:var(--op-text-muted)}
  .op-auto-refresh .dot{width:6px;height:6px;border-radius:50%;background:var(--op-success);animation:op-pulse 2s ease-in-out infinite}
  @keyframes op-pulse{0%,100%{opacity:1}50%{opacity:.3}}
  .op-status-bar{padding:6px 14px;font-size:11px;color:var(--op-text-muted);background:var(--op-bg-alt);border-top:1px solid var(--op-border);display:flex;justify-content:space-between}
  .op-state-indicator{display:inline-flex;align-items:center;gap:8px;padding:10px 16px;border-radius:8px;margin-bottom:16px;font-size:14px;font-weight:600}
  .op-state-online{background:#d1fae5;color:#065f46}
  .op-state-provisioning{background:#dbeafe;color:#1e40af}
  .op-state-offline{background:#f3f4f6;color:#6b7280}
  @media(prefers-color-scheme:dark){.op-state-online{background:#064e3b;color:#6ee7b7}.op-state-provisioning{background:#1e3a5f;color:#93c5fd}.op-state-offline{background:#374151;color:#9ca3af}}
  .op-metric-card{border:1px solid var(--op-border);border-radius:var(--op-radius);padding:10px 14px;margin-bottom:8px}
  .op-metric-name{font-size:12px;font-weight:600;margin-bottom:4px}
  .op-metric-labels{font-size:11px;color:var(--op-text-muted);margin-bottom:4px}
  .op-metric-value{font-size:20px;font-weight:700;color:var(--op-primary)}
  .op-metric-ts{font-size:10px;color:var(--op-text-muted)}
  .op-metrics-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:8px}
`;

function esc(s){if(s==null)return"";const d=document.createElement("div");d.textContent=String(s);return d.innerHTML}
function stateCls(s){if(!s)return"offline";const u=s.toUpperCase();if(u==="ONLINE")return"online";if(["PROVISIONING","SCALING"].includes(u))return"provisioning";return"offline"}
function stateIcon(s){const u=(s||"").toUpperCase();if(u==="ONLINE")return"🟢";if(["PROVISIONING","SCALING"].includes(u))return"🔵";return"⭕"}
function stateBadge(s){if(!s)return"";const u=s.toUpperCase();let c="muted";if(u==="ONLINE")c="success";else if(["PROVISIONING","SCALING"].includes(u))c="info";else if(["OFFLINE"].includes(u))c="muted";return`<span class="op-badge op-badge-${c}">${esc(u)}</span>`}

function render({model,el}){
  const shadow=el.attachShadow?el.attachShadow({mode:"open"}):el;
  shadow.innerHTML="";
  const styleEl=document.createElement("style");styleEl.textContent=S;shadow.appendChild(styleEl);
  const root=document.createElement("div");shadow.appendChild(root);

  let currentTab="details",indexesLoaded=false,metricsLoaded=false;
  let autoRefreshEnabled=true,autoTimer=null;

  function getEP(){return JSON.parse(model.get("endpoint_data")||"{}")}
  function getIndexes(){return JSON.parse(model.get("indexes_data")||"[]")}
  function getMetrics(){return JSON.parse(model.get("metrics_data")||"[]")}
  function send(req){model.set("request",JSON.stringify({...req,_t:Date.now()}));model.save_changes()}
  function startAR(){stopAR();const ep=getEP();if(autoRefreshEnabled)autoTimer=setInterval(()=>send({action:"refresh"}),(ep.refresh_seconds||30)*1000)}
  function stopAR(){if(autoTimer){clearInterval(autoTimer);autoTimer=null}}

  function fullRender(){
    const ep=getEP(),loading=model.get("loading"),error=model.get("error_message");

    let h=`<div class="op-header"><h2>🔍 ${esc(ep.name||"Vector Search Endpoint")}</h2>`;
    h+=`<div class="op-header-actions">`;
    h+=`<div class="op-auto-refresh">${autoRefreshEnabled?'<span class="dot"></span> Auto':'Paused'}</div>`;
    h+=`<button class="op-btn" data-action="toggle-refresh">${autoRefreshEnabled?'⏸':'▶'}</button>`;
    h+=`<button class="op-btn" data-action="refresh">↻</button>`;
    h+=`</div></div>`;
    if(error)h+=`<div class="op-error">${esc(error)}</div>`;

    if(loading){
      h+=`<div class="op-body"><div class="op-loading"><span class="spinner"></span> Loading…</div></div>`;
    }else{
      h+=`<div class="op-body"><div class="op-detail">`;
      h+=`<div class="op-state-indicator op-state-${stateCls(ep.state)}">${stateIcon(ep.state)} ${esc(ep.state||'UNKNOWN')}</div>`;
      if(ep.state_message)h+=`<div class="op-muted" style="margin-bottom:12px">${esc(ep.state_message)}</div>`;

      h+=`<div class="op-tabs">`;
      h+=`<button class="op-tab${currentTab==='details'?' active':''}" data-tab="details">Details</button>`;
      h+=`<button class="op-tab${currentTab==='indexes'?' active':''}" data-tab="indexes">Indexes${ep.num_indexes!=null?' ('+ep.num_indexes+')':''}</button>`;
      h+=`<button class="op-tab${currentTab==='metrics'?' active':''}" data-tab="metrics">Metrics</button>`;
      h+=`</div>`;

      // Details
      h+=`<div class="op-tab-content" data-tab="details" style="${currentTab!=='details'?'display:none':''}">`;
      h+=`<dl class="op-kv">`;
      h+=`<dt>Endpoint Name</dt><dd class="op-mono">${esc(ep.name)}</dd>`;
      if(ep.id)h+=`<dt>ID</dt><dd class="op-mono">${esc(ep.id)}</dd>`;
      h+=`<dt>Type</dt><dd>${esc(ep.endpoint_type||'—')}</dd>`;
      h+=`<dt>Indexes</dt><dd>${ep.num_indexes!=null?ep.num_indexes:'—'}</dd>`;
      h+=`<dt>Creator</dt><dd>${esc(ep.creator||'—')}</dd>`;
      h+=`<dt>Created</dt><dd>${esc(ep.created_at||'—')}</dd>`;
      h+=`<dt>Last Updated</dt><dd>${esc(ep.last_updated||'—')} by ${esc(ep.last_updated_user||'—')}</dd>`;
      if(ep.scaling_requested_min_qps!=null)h+=`<dt>Min QPS</dt><dd>${ep.scaling_requested_min_qps}</dd>`;
      if(ep.scaling_state)h+=`<dt>Scaling State</dt><dd>${stateBadge(ep.scaling_state)}</dd>`;
      if(ep.budget_policy_id)h+=`<dt>Budget Policy</dt><dd class="op-mono">${esc(ep.budget_policy_id)}</dd>`;
      h+=`</dl>`;
      if(ep.custom_tags&&ep.custom_tags.length){
        h+=`<div class="op-section">Tags</div>`;
        h+=ep.custom_tags.map(t=>`<span class="op-tag">${esc(t.key)}=${esc(t.value)}</span>`).join(' ');
      }
      h+=`</div>`;

      // Indexes
      h+=`<div class="op-tab-content" data-tab="indexes" style="${currentTab!=='indexes'?'display:none':''}">`;
      const indexes=getIndexes();
      if(indexes.length){
        h+=`<table class="op-table"><thead><tr><th>Name</th><th>Type</th><th>Subtype</th><th>Primary Key</th><th>Creator</th></tr></thead><tbody>`;
        for(const i of indexes){
          h+=`<tr><td class="op-mono">${esc(i.name)}</td><td><span class="op-badge op-badge-info">${esc(i.index_type)}</span></td><td>${esc(i.index_subtype||'—')}</td><td class="op-mono">${esc(i.primary_key)}</td><td class="op-muted">${esc(i.creator||'—')}</td></tr>`;
        }
        h+=`</tbody></table>`;
      }else if(!indexesLoaded){
        h+=`<div class="op-empty"><button class="op-btn op-btn-primary" data-action="load-indexes">Load Indexes</button></div>`;
      }else{h+=`<div class="op-empty">No indexes on this endpoint.</div>`}
      h+=`</div>`;

      // Metrics
      h+=`<div class="op-tab-content" data-tab="metrics" style="${currentTab!=='metrics'?'display:none':''}">`;
      const metrics=getMetrics();
      if(metrics.length){
        h+=`<div class="op-metrics-grid">`;
        for(const m of metrics){
          const latest=m.values&&m.values.length?m.values[m.values.length-1]:null;
          h+=`<div class="op-metric-card">`;
          h+=`<div class="op-metric-name">${esc(m.name)}</div>`;
          if(m.labels&&m.labels.length)h+=`<div class="op-metric-labels">${m.labels.map(l=>esc(l.key)+'='+esc(l.value)).join(', ')}</div>`;
          if(latest){
            h+=`<div class="op-metric-value">${latest.value!=null?latest.value.toFixed(2):'—'}</div>`;
            h+=`<div class="op-metric-ts">${esc(latest.timestamp||'')}</div>`;
          }else{h+=`<div class="op-metric-value op-muted">No data</div>`}
          h+=`</div>`;
        }
        h+=`</div>`;
      }else if(!metricsLoaded){
        h+=`<div class="op-empty"><button class="op-btn op-btn-primary" data-action="load-metrics">Load Metrics</button></div>`;
      }else{h+=`<div class="op-empty">No metrics available.</div>`}
      h+=`</div>`;

      h+=`</div></div>`;
    }
    h+=`<div class="op-status-bar"><span>Last refresh: ${new Date().toLocaleTimeString()}</span><span>${esc(ep.name||'')}</span></div>`;
    root.innerHTML=h;
    bind();
  }

  function bind(){
    root.querySelectorAll(".op-tab").forEach(t=>t.addEventListener("click",()=>{currentTab=t.dataset.tab;fullRender()}));
    root.querySelector("[data-action='refresh']")?.addEventListener("click",()=>send({action:"refresh"}));
    root.querySelector("[data-action='toggle-refresh']")?.addEventListener("click",()=>{autoRefreshEnabled=!autoRefreshEnabled;if(autoRefreshEnabled)startAR();else stopAR();fullRender()});
    root.querySelector("[data-action='load-indexes']")?.addEventListener("click",()=>{indexesLoaded=true;send({action:"list_indexes"})});
    root.querySelector("[data-action='load-metrics']")?.addEventListener("click",()=>{metricsLoaded=true;send({action:"get_metrics"})});
  }

  model.on("change:endpoint_data",fullRender);
  model.on("change:indexes_data",fullRender);
  model.on("change:metrics_data",fullRender);
  model.on("change:loading",fullRender);
  model.on("change:error_message",fullRender);

  fullRender();startAR();
  return()=>stopAR();
}

export default{render};
