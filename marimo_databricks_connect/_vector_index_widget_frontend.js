// Vector Index Widget Frontend — single index operational dashboard
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
  .op-btn-success{background:var(--op-success);color:#fff;border-color:var(--op-success)}
  .op-btn-warning{background:var(--op-warning);color:#000;border-color:var(--op-warning)}
  .op-body{max-height:600px;overflow:auto}
  .op-loading{padding:30px;text-align:center;color:var(--op-text-muted)}
  .op-loading .spinner{display:inline-block;width:20px;height:20px;border:2px solid var(--op-border);border-top-color:var(--op-primary);border-radius:50%;animation:op-spin .6s linear infinite;margin-right:8px;vertical-align:middle}
  @keyframes op-spin{to{transform:rotate(360deg)}}
  .op-error{padding:10px 14px;background:#fef2f2;color:var(--op-danger);border-bottom:1px solid #fecaca;font-size:12px}
  @media(prefers-color-scheme:dark){.op-error{background:#3b1f1f;border-color:#5c2b2b}}
  .op-success-msg{padding:10px 14px;background:#f0fdf4;color:var(--op-success);border-bottom:1px solid #bbf7d0;font-size:12px}
  @media(prefers-color-scheme:dark){.op-success-msg{background:#1a2e1a;border-color:#2e5c2b}}
  .op-detail{padding:14px}
  .op-kv{display:grid;grid-template-columns:180px 1fr;gap:4px 12px;margin-bottom:16px;font-size:12px}
  .op-kv dt{color:var(--op-text-muted);font-weight:500;padding:3px 0}
  .op-kv dd{margin:0;padding:3px 0;word-break:break-all}
  .op-section{font-size:13px;font-weight:600;margin:16px 0 8px;padding-bottom:4px;border-bottom:1px solid var(--op-border)}
  table.op-table{width:100%;border-collapse:collapse}
  .op-table th{text-align:left;padding:8px 14px;font-size:11px;font-weight:600;text-transform:uppercase;color:var(--op-text-muted);background:var(--op-bg-alt);border-bottom:1px solid var(--op-border);position:sticky;top:0;z-index:1}
  .op-table td{padding:7px 14px;border-bottom:1px solid var(--op-border);vertical-align:top;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
  .op-table td:hover{white-space:normal;word-break:break-all}
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
  .op-confirm{padding:14px;background:var(--op-bg-alt);border-bottom:1px solid var(--op-border)}
  .op-confirm p{margin:0 0 10px;font-size:13px}
  .op-confirm-actions{display:flex;gap:8px}
  .op-state-indicator{display:inline-flex;align-items:center;gap:8px;padding:10px 16px;border-radius:8px;margin-bottom:16px;font-size:14px;font-weight:600}
  .op-state-ready{background:#d1fae5;color:#065f46}
  .op-state-notready{background:#fef3c7;color:#92400e}
  .op-state-unknown{background:#f3f4f6;color:#6b7280}
  @media(prefers-color-scheme:dark){.op-state-ready{background:#064e3b;color:#6ee7b7}.op-state-notready{background:#78350f;color:#fcd34d}.op-state-unknown{background:#374151;color:#9ca3af}}
  .op-lineage-flow{display:flex;align-items:flex-start;gap:16px;justify-content:center;flex-wrap:wrap;padding:12px}
  .op-lineage-group{min-width:200px}
  .op-lineage-group h4{margin:0 0 8px;font-size:12px;color:var(--op-text-muted);text-transform:uppercase;letter-spacing:.5px}
  .op-lineage-item{padding:6px 10px;margin-bottom:4px;border:1px solid var(--op-border);border-radius:4px;font-family:var(--op-font-mono);font-size:11px;background:var(--op-bg-alt)}
  .op-lineage-center{min-width:200px;padding:12px;border:2px solid var(--op-primary);border-radius:6px;text-align:center;font-weight:600;font-family:var(--op-font-mono);font-size:12px;align-self:center}
  .op-lineage-arrow{font-size:20px;color:var(--op-text-muted);align-self:center}
  /* Embedding cards */
  .op-embed-card{border:1px solid var(--op-border);border-radius:var(--op-radius);padding:10px 14px;margin-bottom:8px;background:var(--op-bg-alt)}
  .op-embed-card-title{font-weight:600;font-size:12px;margin-bottom:4px;font-family:var(--op-font-mono)}
  .op-embed-card-detail{font-size:11px;color:var(--op-text-muted)}
  .op-row-count{font-size:24px;font-weight:700;color:var(--op-primary)}
`;

function esc(s){if(s==null)return"";const d=document.createElement("div");d.textContent=String(s);return d.innerHTML}
function fmtNum(n){if(n==null)return"—";return n.toLocaleString()}

function render({model,el}){
  const shadow=el.attachShadow?el.attachShadow({mode:"open"}):el;
  shadow.innerHTML="";
  const styleEl=document.createElement("style");styleEl.textContent=S;shadow.appendChild(styleEl);
  const root=document.createElement("div");shadow.appendChild(root);

  let currentTab="status",sampleLoaded=false,permissionsLoaded=false,lineageLoaded=false;
  let autoRefreshEnabled=true,autoTimer=null;
  let confirmAction=null,actionMessage=null,actionIsError=false;

  function getIdx(){return JSON.parse(model.get("index_data")||"{}")}
  function getSample(){return JSON.parse(model.get("sample_data")||"{}")}
  function getPerms(){return JSON.parse(model.get("permissions_data")||"{}")}
  function getLineage(){return JSON.parse(model.get("lineage_data")||"{}")}
  function send(req){model.set("request",JSON.stringify({...req,_t:Date.now()}));model.save_changes()}
  function startAR(){stopAR();const i=getIdx();if(autoRefreshEnabled)autoTimer=setInterval(()=>send({action:"refresh"}),(i.refresh_seconds||30)*1000)}
  function stopAR(){if(autoTimer){clearInterval(autoTimer);autoTimer=null}}

  function fullRender(){
    const i=getIdx(),loading=model.get("loading"),error=model.get("error_message");
    const shortName=i.name?(i.name.split(".").pop()):("Index");
    const isReady=i.status_ready===true;
    const isDeltaSync=i.index_type==="DELTA_SYNC";

    let h=`<div class="op-header"><h2>📐 ${esc(shortName)}</h2>`;
    h+=`<div class="op-header-actions">`;
    h+=`<div class="op-auto-refresh">${autoRefreshEnabled?'<span class="dot"></span> Auto':'Paused'}</div>`;
    h+=`<button class="op-btn" data-action="toggle-refresh">${autoRefreshEnabled?'⏸':'▶'}</button>`;
    h+=`<button class="op-btn" data-action="refresh">↻</button>`;
    if(isDeltaSync)h+=`<button class="op-btn op-btn-warning" data-action="sync">🔄 Sync</button>`;
    h+=`</div></div>`;

    if(confirmAction)h+=`<div class="op-confirm"><p>${confirmAction.message}</p><div class="op-confirm-actions"><button class="op-btn op-btn-${confirmAction.btnClass}" data-action="confirm-yes">${confirmAction.yesLabel}</button><button class="op-btn" data-action="confirm-no">Cancel</button></div></div>`;
    if(actionMessage)h+=`<div class="${actionIsError?'op-error':'op-success-msg'}">${esc(actionMessage)}</div>`;
    if(error)h+=`<div class="op-error">${esc(error)}</div>`;

    if(loading){
      h+=`<div class="op-body"><div class="op-loading"><span class="spinner"></span> Loading…</div></div>`;
    }else{
      h+=`<div class="op-body"><div class="op-detail">`;

      // State
      const stCls=isReady?"ready":(i.status_ready===false?"notready":"unknown");
      const stIcon=isReady?"🟢":(i.status_ready===false?"🟡":"⭕");
      const stText=isReady?"READY":(i.status_ready===false?"NOT READY":"UNKNOWN");
      h+=`<div class="op-state-indicator op-state-${stCls}">${stIcon} ${stText}</div>`;
      if(i.indexed_row_count!=null)h+=` <span class="op-row-count">${fmtNum(i.indexed_row_count)}</span><span class="op-muted"> indexed rows</span>`;
      if(i.status_message)h+=`<div class="op-muted" style="margin-top:8px;margin-bottom:12px">${esc(i.status_message)}</div>`;

      h+=`<div class="op-tabs">`;
      h+=`<button class="op-tab${currentTab==='status'?' active':''}" data-tab="status">Details</button>`;
      h+=`<button class="op-tab${currentTab==='embedding'?' active':''}" data-tab="embedding">Embedding</button>`;
      h+=`<button class="op-tab${currentTab==='sample'?' active':''}" data-tab="sample">Sample Data</button>`;
      h+=`<button class="op-tab${currentTab==='lineage'?' active':''}" data-tab="lineage">Lineage</button>`;
      h+=`<button class="op-tab${currentTab==='permissions'?' active':''}" data-tab="permissions">Permissions</button>`;
      h+=`</div>`;

      // Details tab
      h+=`<div class="op-tab-content" data-tab="status" style="${currentTab!=='status'?'display:none':''}">`;
      h+=`<dl class="op-kv">`;
      h+=`<dt>Full Name</dt><dd class="op-mono">${esc(i.name)}</dd>`;
      h+=`<dt>Endpoint</dt><dd class="op-mono">${esc(i.endpoint_name)}</dd>`;
      h+=`<dt>Index Type</dt><dd><span class="op-badge op-badge-info">${esc(i.index_type)}</span></dd>`;
      if(i.index_subtype)h+=`<dt>Subtype</dt><dd><span class="op-badge op-badge-muted">${esc(i.index_subtype)}</span></dd>`;
      h+=`<dt>Primary Key</dt><dd class="op-mono">${esc(i.primary_key)}</dd>`;
      h+=`<dt>Creator</dt><dd>${esc(i.creator||'—')}</dd>`;
      if(i.index_url)h+=`<dt>Index URL</dt><dd class="op-mono" style="font-size:11px">${esc(i.index_url)}</dd>`;
      h+=`</dl>`;
      if(isDeltaSync){
        h+=`<div class="op-section">Delta Sync Configuration</div>`;
        h+=`<dl class="op-kv">`;
        h+=`<dt>Source Table</dt><dd class="op-mono">${esc(i.source_table||'—')}</dd>`;
        if(i.pipeline_id)h+=`<dt>Pipeline ID</dt><dd class="op-mono">${esc(i.pipeline_id)}</dd>`;
        if(i.pipeline_type)h+=`<dt>Pipeline Type</dt><dd>${esc(i.pipeline_type)}</dd>`;
        if(i.embedding_writeback_table)h+=`<dt>Writeback Table</dt><dd class="op-mono">${esc(i.embedding_writeback_table)}</dd>`;
        h+=`</dl>`;
        if(i.columns_to_sync&&i.columns_to_sync.length){
          h+=`<div style="margin-top:4px"><span class="op-muted">Synced columns:</span> `;
          h+=i.columns_to_sync.map(c=>`<span class="op-tag">${esc(c)}</span>`).join(' ');
          h+=`</div>`;
        }
      }
      if(i.schema_json){
        h+=`<div class="op-section">Schema</div>`;
        h+=`<pre class="op-mono" style="padding:10px;background:var(--op-bg-alt);border:1px solid var(--op-border);border-radius:4px;white-space:pre-wrap;font-size:11px;max-height:200px;overflow:auto">${esc(i.schema_json)}</pre>`;
      }
      h+=`</div>`;

      // Embedding tab
      h+=`<div class="op-tab-content" data-tab="embedding" style="${currentTab!=='embedding'?'display:none':''}">`;
      if(i.embedding_sources&&i.embedding_sources.length){
        h+=`<div class="op-section">Embedding Source Columns</div>`;
        for(const s of i.embedding_sources){
          h+=`<div class="op-embed-card">`;
          h+=`<div class="op-embed-card-title">${esc(s.name)}</div>`;
          if(s.embedding_model_endpoint)h+=`<div class="op-embed-card-detail">Model endpoint: <span class="op-mono">${esc(s.embedding_model_endpoint)}</span></div>`;
          if(s.query_model_endpoint)h+=`<div class="op-embed-card-detail">Query endpoint: <span class="op-mono">${esc(s.query_model_endpoint)}</span></div>`;
          h+=`</div>`;
        }
      }
      if(i.embedding_vectors&&i.embedding_vectors.length){
        h+=`<div class="op-section">Embedding Vector Columns</div>`;
        for(const v of i.embedding_vectors){
          h+=`<div class="op-embed-card">`;
          h+=`<div class="op-embed-card-title">${esc(v.name)}</div>`;
          if(v.dimension)h+=`<div class="op-embed-card-detail">Dimension: <strong>${v.dimension}</strong></div>`;
          h+=`</div>`;
        }
      }
      if((!i.embedding_sources||!i.embedding_sources.length)&&(!i.embedding_vectors||!i.embedding_vectors.length)){
        h+=`<div class="op-empty">No embedding configuration found.</div>`;
      }
      h+=`</div>`;

      // Sample data tab
      h+=`<div class="op-tab-content" data-tab="sample" style="${currentTab!=='sample'?'display:none':''}">`;
      const sample=getSample();
      if(sample.rows&&sample.rows.length){
        h+=`<div style="overflow-x:auto"><table class="op-table"><thead><tr>`;
        for(const col of sample.columns)h+=`<th>${esc(col)}</th>`;
        h+=`</tr></thead><tbody>`;
        for(const row of sample.rows){
          h+=`<tr>`;for(const val of row)h+=`<td class="op-mono" style="font-size:11px">${esc(val)}</td>`;h+=`</tr>`;
        }
        h+=`</tbody></table></div>`;
      }else if(!sampleLoaded){
        h+=`<div class="op-empty"><button class="op-btn op-btn-primary" data-action="load-sample">Scan Index (50 rows)</button></div>`;
      }else{h+=`<div class="op-empty">No data found.</div>`}
      h+=`</div>`;

      // Lineage tab
      h+=`<div class="op-tab-content" data-tab="lineage" style="${currentTab!=='lineage'?'display:none':''}">`;
      const lineage=getLineage();
      if(lineage.upstream||lineage.downstream){
        h+=`<div class="op-lineage-flow">`;
        h+=`<div class="op-lineage-group"><h4>Upstream (${(lineage.upstream||[]).length})</h4>`;
        for(const u of lineage.upstream||[])h+=`<div class="op-lineage-item">${esc(u.catalog_name)}.${esc(u.schema_name)}.${esc(u.name)}</div>`;
        if(!(lineage.upstream||[]).length)h+=`<div class="op-muted">None</div>`;
        h+=`</div><div class="op-lineage-arrow">→</div>`;
        h+=`<div class="op-lineage-center">${esc(i.name)}</div>`;
        h+=`<div class="op-lineage-arrow">→</div>`;
        h+=`<div class="op-lineage-group"><h4>Downstream (${(lineage.downstream||[]).length})</h4>`;
        for(const d of lineage.downstream||[])h+=`<div class="op-lineage-item">${esc(d.catalog_name)}.${esc(d.schema_name)}.${esc(d.name)}</div>`;
        if(!(lineage.downstream||[]).length)h+=`<div class="op-muted">None</div>`;
        h+=`</div></div>`;
      }else if(!lineageLoaded){
        h+=`<div class="op-empty"><button class="op-btn op-btn-primary" data-action="load-lineage">Load Lineage</button></div>`;
      }else{h+=`<div class="op-empty">No lineage data.</div>`}
      h+=`</div>`;

      // Permissions tab
      h+=`<div class="op-tab-content" data-tab="permissions" style="${currentTab!=='permissions'?'display:none':''}">`;
      const perms=getPerms();
      if(perms.permissions&&perms.permissions.length){
        h+=`<table class="op-table"><thead><tr><th>Principal</th><th>Privileges</th></tr></thead><tbody>`;
        for(const p of perms.permissions){
          const privs=(p.privileges||[]).map(pr=>{let s=esc(pr.privilege);if(pr.inherited_from_name)s+=` <span class="op-muted">(from ${esc(pr.inherited_from_name)})</span>`;return s}).join(', ');
          h+=`<tr><td class="op-mono">${esc(p.principal)}</td><td>${privs}</td></tr>`;
        }
        h+=`</tbody></table>`;
      }else if(!permissionsLoaded){
        h+=`<div class="op-empty"><button class="op-btn op-btn-primary" data-action="load-permissions">Load Permissions</button></div>`;
      }else{h+=`<div class="op-empty">No permissions data.</div>`}
      h+=`</div>`;

      h+=`</div></div>`;
    }
    h+=`<div class="op-status-bar"><span>Last refresh: ${new Date().toLocaleTimeString()}</span><span>${esc(i.name||'')}</span></div>`;
    root.innerHTML=h;bind();
  }

  function bind(){
    root.querySelectorAll(".op-tab").forEach(t=>t.addEventListener("click",()=>{currentTab=t.dataset.tab;fullRender()}));
    root.querySelector("[data-action='refresh']")?.addEventListener("click",()=>send({action:"refresh"}));
    root.querySelector("[data-action='toggle-refresh']")?.addEventListener("click",()=>{autoRefreshEnabled=!autoRefreshEnabled;if(autoRefreshEnabled)startAR();else stopAR();fullRender()});
    root.querySelector("[data-action='sync']")?.addEventListener("click",()=>{confirmAction={message:"Trigger a sync for this index?",btnClass:"warning",yesLabel:"🔄 Sync Now",action:"sync"};fullRender()});
    root.querySelector("[data-action='confirm-yes']")?.addEventListener("click",()=>{const a=confirmAction.action;confirmAction=null;send({action:a})});
    root.querySelector("[data-action='confirm-no']")?.addEventListener("click",()=>{confirmAction=null;fullRender()});
    root.querySelector("[data-action='load-sample']")?.addEventListener("click",()=>{sampleLoaded=true;send({action:"scan"})});
    root.querySelector("[data-action='load-permissions']")?.addEventListener("click",()=>{permissionsLoaded=true;send({action:"get_permissions"})});
    root.querySelector("[data-action='load-lineage']")?.addEventListener("click",()=>{lineageLoaded=true;send({action:"get_lineage"})});
  }

  model.on("change:index_data",fullRender);
  model.on("change:sample_data",fullRender);
  model.on("change:permissions_data",fullRender);
  model.on("change:lineage_data",fullRender);
  model.on("change:loading",fullRender);
  model.on("change:error_message",fullRender);
  model.on("change:action_result",()=>{
    try{const r=JSON.parse(model.get("action_result")||"{}");actionMessage=r.message;actionIsError=!r.success;fullRender();if(r.success)setTimeout(()=>{actionMessage=null;fullRender()},5000)}catch(e){}
  });

  fullRender();startAR();
  return()=>stopAR();
}

export default{render};
