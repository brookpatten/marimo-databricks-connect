// Databricks App Widget Frontend
const S=`
:host{--op-bg:#fff;--op-bg-alt:#f8f9fa;--op-bg-hover:#e9ecef;--op-border:#dee2e6;--op-text:#212529;--op-text-muted:#6c757d;--op-primary:#0d6efd;--op-primary-hover:#0b5ed7;--op-success:#198754;--op-danger:#dc3545;--op-warning:#ffc107;--op-info:#0dcaf0;--op-font:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"Helvetica Neue",Arial,sans-serif;--op-font-mono:"SF Mono","Cascadia Code","Fira Code",Menlo,Consolas,monospace;--op-radius:6px;display:block;font-family:var(--op-font);font-size:13px;color:var(--op-text);background:var(--op-bg);border:1px solid var(--op-border);border-radius:var(--op-radius);overflow:hidden}
:host(.dark-theme) {--op-bg:#1e1e1e;--op-bg-alt:#252526;--op-bg-hover:#2d2d30;--op-border:#3e3e42;--op-text:#ccc;--op-text-muted:#888;--op-primary:#4fc3f7;--op-primary-hover:#29b6f6;--op-success:#66bb6a;--op-danger:#ef5350;--op-warning:#ffca28;--op-info:#4dd0e1}
*{box-sizing:border-box}
.op-header{display:flex;align-items:center;gap:8px;padding:10px 14px;background:var(--op-bg-alt);border-bottom:1px solid var(--op-border);flex-wrap:wrap}
.op-header h2{margin:0;font-size:14px;font-weight:600}
.op-header-actions{margin-left:auto;display:flex;gap:6px;align-items:center}
.op-btn{padding:4px 10px;border:1px solid var(--op-border);border-radius:4px;background:var(--op-bg);color:var(--op-text);cursor:pointer;font-size:12px;font-family:var(--op-font);transition:background .15s}
.op-btn:hover{background:var(--op-bg-hover)}.op-btn:disabled{opacity:.5;cursor:not-allowed}
.op-btn-primary{background:var(--op-primary);color:#fff;border-color:var(--op-primary)}.op-btn-primary:hover{background:var(--op-primary-hover)}
.op-btn-success{background:var(--op-success);color:#fff;border-color:var(--op-success)}
.op-btn-danger{background:var(--op-danger);color:#fff;border-color:var(--op-danger)}
.op-btn-warning{background:var(--op-warning);color:#000;border-color:var(--op-warning)}
.op-body{max-height:650px;overflow:auto}
.op-loading{padding:30px;text-align:center;color:var(--op-text-muted)}
.op-loading .spinner{display:inline-block;width:20px;height:20px;border:2px solid var(--op-border);border-top-color:var(--op-primary);border-radius:50%;animation:op-spin .6s linear infinite;margin-right:8px;vertical-align:middle}
@keyframes op-spin{to{transform:rotate(360deg)}}
.op-error{padding:10px 14px;background:#fef2f2;color:var(--op-danger);border-bottom:1px solid #fecaca;font-size:12px}
:host(.dark-theme) .op-error {background:#3b1f1f;border-color:#5c2b2b}
.op-success-msg{padding:10px 14px;background:#f0fdf4;color:var(--op-success);border-bottom:1px solid #bbf7d0;font-size:12px}
:host(.dark-theme) .op-success-msg {background:#1a2e1a;border-color:#2e5c2b}
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
:host(.dark-theme) .op-badge-success {background:#064e3b;color:#6ee7b7}:host(.dark-theme) .op-badge-danger {background:#7f1d1d;color:#fca5a5}:host(.dark-theme) .op-badge-info {background:#1e3a5f;color:#93c5fd}:host(.dark-theme) .op-badge-warning {background:#78350f;color:#fcd34d}:host(.dark-theme) .op-badge-muted {background:#374151;color:#9ca3af}
.op-tabs{display:flex;gap:0;border-bottom:2px solid var(--op-border);margin-bottom:12px}
.op-tab{padding:6px 16px;cursor:pointer;font-size:12px;font-weight:500;border:none;background:none;color:var(--op-text-muted);border-bottom:2px solid transparent;margin-bottom:-2px;font-family:var(--op-font)}
.op-tab:hover{color:var(--op-text)}.op-tab.active{color:var(--op-primary);border-bottom-color:var(--op-primary)}
.op-mono{font-family:var(--op-font-mono);font-size:12px}
.op-muted{color:var(--op-text-muted);font-size:12px}
.op-tag{display:inline-block;padding:1px 6px;margin:1px 3px 1px 0;border-radius:3px;font-size:11px;background:var(--op-bg-alt);border:1px solid var(--op-border);font-family:var(--op-font-mono)}
.op-link{color:var(--op-primary);cursor:pointer;text-decoration:none}.op-link:hover{text-decoration:underline}
.op-empty{padding:30px;text-align:center;color:var(--op-text-muted)}
.op-auto-refresh{display:flex;align-items:center;gap:4px;font-size:11px;color:var(--op-text-muted)}
.op-auto-refresh .dot{width:6px;height:6px;border-radius:50%;background:var(--op-success);animation:op-pulse 2s ease-in-out infinite}
@keyframes op-pulse{0%,100%{opacity:1}50%{opacity:.3}}
.op-status-bar{padding:6px 14px;font-size:11px;color:var(--op-text-muted);background:var(--op-bg-alt);border-top:1px solid var(--op-border);display:flex;justify-content:space-between}
.op-confirm{padding:14px;background:var(--op-bg-alt);border-bottom:1px solid var(--op-border)}
.op-confirm p{margin:0 0 10px;font-size:13px}
.op-confirm-actions{display:flex;gap:8px}
.op-state-indicator{display:inline-flex;align-items:center;gap:8px;padding:10px 16px;border-radius:8px;margin-bottom:16px;font-size:14px;font-weight:600}
.op-state-running{background:#d1fae5;color:#065f46}
.op-state-deploying{background:#dbeafe;color:#1e40af}
.op-state-stopped{background:#f3f4f6;color:#6b7280}
.op-state-crashed{background:#fee2e2;color:#991b1b}
:host(.dark-theme) .op-state-running {background:#064e3b;color:#6ee7b7}:host(.dark-theme) .op-state-deploying {background:#1e3a5f;color:#93c5fd}:host(.dark-theme) .op-state-stopped {background:#374151;color:#9ca3af}:host(.dark-theme) .op-state-crashed {background:#7f1d1d;color:#fca5a5}
/* Form */
.op-form-group{margin-bottom:12px}
.op-form-label{display:block;font-size:12px;font-weight:500;color:var(--op-text-muted);margin-bottom:4px}
.op-input,.op-select{width:100%;padding:5px 8px;border:1px solid var(--op-border);border-radius:4px;font-size:12px;font-family:var(--op-font);background:var(--op-bg);color:var(--op-text)}
.op-input:focus,.op-select:focus{outline:none;border-color:var(--op-primary)}
.op-form-row{display:flex;gap:12px;align-items:flex-end;flex-wrap:wrap}
.op-form-row .op-form-group{flex:1;min-width:150px}
.op-form-actions{display:flex;gap:8px;margin-top:12px}
/* Deployment card */
.op-deploy-card{border:1px solid var(--op-border);border-radius:var(--op-radius);padding:12px 14px;margin-bottom:8px;background:var(--op-bg-alt)}
.op-deploy-card.active{border-color:var(--op-success);border-width:2px}
.op-deploy-card-header{display:flex;align-items:center;gap:8px;margin-bottom:6px}
.op-deploy-card-header .dep-id{font-family:var(--op-font-mono);font-size:12px;font-weight:600}
/* Perm editor row */
.op-perm-row{display:flex;gap:8px;align-items:center;margin-bottom:6px}
.op-perm-row input{flex:1}
.op-perm-row select{width:140px}
.op-perm-row .kv-del{border:none;background:none;color:var(--op-danger);cursor:pointer;font-size:14px;padding:4px}
/* Thumbnail */
.op-thumb-preview{max-width:200px;max-height:200px;border:1px solid var(--op-border);border-radius:var(--op-radius);margin:8px 0}
.op-thumb-sm{width:48px;height:48px;object-fit:cover;border-radius:var(--op-radius);border:1px solid var(--op-border)}
.op-thumb-current{max-width:300px;max-height:300px;border:1px solid var(--op-border);border-radius:var(--op-radius);margin:8px 0}
.op-file-input{margin:8px 0}
`;

function esc(s){if(s==null)return"";const d=document.createElement("div");d.textContent=String(s);return d.innerHTML}
function stateBadge(s){if(!s)return"";const u=s.toUpperCase();let c="muted";if(["RUNNING","SUCCEEDED","ACTIVE"].includes(u))c="success";else if(["FAILED","CRASHED","ERROR"].includes(u))c="danger";else if(["DEPLOYING","IN_PROGRESS","STARTING","STOPPING","UPDATING"].includes(u))c="info";else if(["CANCELLED","UNAVAILABLE","STOPPED"].includes(u))c="warning";return`<span class="op-badge op-badge-${c}">${esc(u)}</span>`}
function appStateCls(s){if(!s)return"stopped";const u=s.toUpperCase();if(u==="RUNNING")return"running";if(u==="DEPLOYING")return"deploying";if(u==="CRASHED")return"crashed";return"stopped"}
function appStateIcon(s){const u=(s||"").toUpperCase();if(u==="RUNNING")return"🟢";if(u==="DEPLOYING")return"🔵";if(u==="CRASHED")return"🔴";return"⭕"}

const PERMISSION_LEVELS=["CAN_USE","CAN_MANAGE"];

function _syncTheme(h){function d(){const a=document.documentElement.getAttribute("data-app-theme");if(a==="dark")return true;if(a==="light")return false;return window.matchMedia("(prefers-color-scheme: dark)").matches}function apply(){h.classList.toggle("dark-theme",d())}apply();const obs=new MutationObserver(apply);obs.observe(document.documentElement,{attributes:true,attributeFilter:["data-app-theme"]});window.matchMedia("(prefers-color-scheme: dark)").addEventListener("change",apply);return()=>{obs.disconnect()}}
function render({model,el}){
  const shadow=el.attachShadow?el.attachShadow({mode:"open"}):el;
  _syncTheme(el);
  shadow.innerHTML="";const sty=document.createElement("style");sty.textContent=S;shadow.appendChild(sty);
  const root=document.createElement("div");shadow.appendChild(root);

  let currentTab="details",deploymentsLoaded=false,permissionsLoaded=false;
  let autoRefreshEnabled=true,autoTimer=null;
  let confirmAction=null,actionMessage=null,actionIsError=false;
  let permEditState=null; // [{principal,type,level}]
  let deployForm=null; // {source_code_path,mode}
  let thumbnailFile=null; // base64 string

  function getApp(){return JSON.parse(model.get("app_data")||"{}")}
  function getThumb(){try{return JSON.parse(model.get("thumbnail_data")||"{}")}catch(e){return{}}}
  function getDeps(){return JSON.parse(model.get("deployments_data")||"[]")}
  function getPerms(){return JSON.parse(model.get("permissions_data")||"{}")}
  function send(req){model.set("request",JSON.stringify({...req,_t:Date.now()}));model.save_changes()}
  function startAR(){stopAR();const a=getApp();if(autoRefreshEnabled)autoTimer=setInterval(()=>send({action:"refresh"}),(a.refresh_seconds||30)*1000)}
  function stopAR(){if(autoTimer){clearInterval(autoTimer);autoTimer=null}}

  function fullRender(){
    const a=getApp(),loading=model.get("loading"),error=model.get("error_message");
    const isRunning=a.app_state==="RUNNING";
    const isStopped=["UNAVAILABLE","CRASHED"].includes(a.app_state)||a.compute_state==="STOPPED";

    let h=`<div class="op-header"><h2>📱 ${esc(a.name||"App")}</h2>`;
    if(a.url)h+=`<a class="op-link" href="${esc(a.url)}" target="_blank" style="font-size:12px">↗ Open App</a>`;
    h+=`<div class="op-header-actions">`;
    h+=`<div class="op-auto-refresh">${autoRefreshEnabled?'<span class="dot"></span> Auto':'Paused'}</div>`;
    h+=`<button class="op-btn" data-action="toggle-refresh">${autoRefreshEnabled?'⏸':'▶'}</button>`;
    h+=`<button class="op-btn" data-action="refresh">↻</button>`;
    if(isStopped)h+=`<button class="op-btn op-btn-success" data-action="start">▶ Start</button>`;
    if(isRunning)h+=`<button class="op-btn op-btn-danger" data-action="stop">⏹ Stop</button>`;
    h+=`</div></div>`;

    if(confirmAction)h+=`<div class="op-confirm"><p>${confirmAction.message}</p><div class="op-confirm-actions"><button class="op-btn op-btn-${confirmAction.btnClass}" data-action="confirm-yes">${confirmAction.yesLabel}</button><button class="op-btn" data-action="confirm-no">Cancel</button></div></div>`;
    if(actionMessage)h+=`<div class="${actionIsError?'op-error':'op-success-msg'}">${esc(actionMessage)}</div>`;
    if(error)h+=`<div class="op-error">${esc(error)}</div>`;

    if(loading){h+=`<div class="op-body"><div class="op-loading"><span class="spinner"></span> Loading…</div></div>`}
    else{
      h+=`<div class="op-body"><div class="op-detail">`;
      // State + thumbnail
      h+=`<div style="display:flex;gap:12px;align-items:center;flex-wrap:wrap;margin-bottom:12px">`;
      const thumb=getThumb();
      if(thumb.thumbnail)h+=`<img class="op-thumb-sm" src="data:image/png;base64,${esc(thumb.thumbnail)}" alt="App thumbnail">`;
      h+=`<div class="op-state-indicator op-state-${appStateCls(a.app_state)}">${appStateIcon(a.app_state)} ${esc(a.app_state||'UNKNOWN')}</div>`;
      if(a.compute_state)h+=`<div>${stateBadge(a.compute_state)}<span class="op-muted" style="margin-left:4px">compute</span></div>`;
      if(a.compute_active_instances!=null)h+=`<div class="op-muted">${a.compute_active_instances} instance(s)</div>`;
      h+=`</div>`;
      if(a.app_state_message)h+=`<div class="op-muted" style="margin-bottom:8px">${esc(a.app_state_message)}</div>`;

      h+=`<div class="op-tabs">`;
      for(const[id,label]of[["details","Details"],["deployments","Deployments"],["permissions","Permissions"],["thumbnail","Thumbnail"]])
        h+=`<button class="op-tab${currentTab===id?' active':''}" data-tab="${id}">${label}</button>`;
      h+=`</div>`;

      // Details
      h+=`<div class="op-tab-content" data-tab="details" style="${currentTab!=='details'?'display:none':''}">`;
      h+=`<dl class="op-kv">`;
      h+=`<dt>Name</dt><dd class="op-mono">${esc(a.name)}</dd>`;
      if(a.id)h+=`<dt>ID</dt><dd class="op-mono">${esc(a.id)}</dd>`;
      if(a.description)h+=`<dt>Description</dt><dd>${esc(a.description)}</dd>`;
      if(a.url)h+=`<dt>URL</dt><dd><a class="op-link" href="${esc(a.url)}" target="_blank">${esc(a.url)}</a></dd>`;
      h+=`<dt>Creator</dt><dd>${esc(a.creator||'—')}</dd>`;
      h+=`<dt>Created</dt><dd>${esc(a.create_time||'—')}</dd>`;
      h+=`<dt>Updated</dt><dd>${esc(a.update_time||'—')} by ${esc(a.updater||'—')}</dd>`;
      if(a.compute_size)h+=`<dt>Compute Size</dt><dd>${esc(a.compute_size)}</dd>`;
      if(a.default_source_code_path)h+=`<dt>Source Path</dt><dd class="op-mono">${esc(a.default_source_code_path)}</dd>`;
      if(a.service_principal_name)h+=`<dt>Service Principal</dt><dd class="op-mono">${esc(a.service_principal_name)}</dd>`;
      if(a.space)h+=`<dt>Space</dt><dd>${esc(a.space)}</dd>`;
      h+=`</dl>`;
      // Active deployment summary
      if(a.active_deployment){
        const ad=a.active_deployment;
        h+=`<div class="op-section">Active Deployment</div>`;
        h+=`<dl class="op-kv"><dt>ID</dt><dd class="op-mono">${esc(ad.deployment_id)}</dd>`;
        h+=`<dt>State</dt><dd>${stateBadge(ad.state)}</dd>`;
        h+=`<dt>Mode</dt><dd>${esc(ad.mode||'—')}</dd>`;
        if(ad.source_code_path)h+=`<dt>Source</dt><dd class="op-mono">${esc(ad.source_code_path)}</dd>`;
        h+=`<dt>Created</dt><dd>${esc(ad.create_time||'—')}</dd>`;
        h+=`</dl>`;
      }
      if(a.pending_deployment){
        const pd=a.pending_deployment;
        h+=`<div class="op-section">Pending Deployment</div>`;
        h+=`<dl class="op-kv"><dt>ID</dt><dd class="op-mono">${esc(pd.deployment_id)}</dd><dt>State</dt><dd>${stateBadge(pd.state)}</dd></dl>`;
      }
      if(a.resources&&a.resources.length){
        h+=`<div class="op-section">Resources</div>`;
        h+=`<table class="op-table"><thead><tr><th>Name</th><th>Type</th><th>Detail</th></tr></thead><tbody>`;
        for(const r of a.resources)h+=`<tr><td class="op-mono">${esc(r.name)}</td><td><span class="op-badge op-badge-muted">${esc(r.type)}</span></td><td class="op-muted">${esc(r.detail||r.description||'')}</td></tr>`;
        h+=`</tbody></table>`;
      }
      h+=`</div>`;

      // Deployments
      h+=`<div class="op-tab-content" data-tab="deployments" style="${currentTab!=='deployments'?'display:none':''}">`;
      // New deployment form
      if(deployForm){
        h+=`<div style="border:1px solid var(--op-border);border-radius:var(--op-radius);padding:14px;margin-bottom:16px;background:var(--op-bg-alt)">`;
        h+=`<div style="font-weight:600;margin-bottom:10px">New Deployment</div>`;
        h+=`<div class="op-form-row">`;
        h+=`<div class="op-form-group"><label class="op-form-label">Source Code Path</label><input class="op-input op-mono" data-deploy-field="source_code_path" value="${esc(deployForm.source_code_path)}" placeholder="/Workspace/Users/..."></div>`;
        h+=`<div class="op-form-group"><label class="op-form-label">Mode</label><select class="op-select" data-deploy-field="mode"><option value="SNAPSHOT"${deployForm.mode==='SNAPSHOT'?' selected':''}>Snapshot</option><option value="AUTO_SYNC"${deployForm.mode==='AUTO_SYNC'?' selected':''}>Auto Sync</option></select></div>`;
        h+=`</div>`;
        h+=`<div class="op-form-actions"><button class="op-btn op-btn-primary" data-action="submit-deploy">🚀 Deploy</button><button class="op-btn" data-action="cancel-deploy">Cancel</button></div>`;
        h+=`</div>`;
      }else{
        h+=`<div style="margin-bottom:12px"><button class="op-btn op-btn-primary" data-action="new-deploy">+ New Deployment</button></div>`;
      }
      const deps=getDeps();
      if(deps.length){
        for(const d of deps){
          const isActive=a.active_deployment&&a.active_deployment.deployment_id===d.deployment_id;
          h+=`<div class="op-deploy-card${isActive?' active':''}">`;
          h+=`<div class="op-deploy-card-header"><span class="dep-id">${esc(d.deployment_id)}</span>${stateBadge(d.state)}${isActive?' <span class="op-badge op-badge-success">ACTIVE</span>':''}`;
          if(d.mode)h+=`<span class="op-badge op-badge-muted">${esc(d.mode)}</span>`;
          h+=`</div>`;
          if(d.source_code_path)h+=`<div class="op-muted">Source: <span class="op-mono">${esc(d.source_code_path)}</span></div>`;
          if(d.state_message)h+=`<div class="op-muted" style="margin-top:2px">${esc(d.state_message)}</div>`;
          h+=`<div class="op-muted" style="margin-top:4px">Created: ${esc(d.create_time||'—')} by ${esc(d.creator||'—')}</div>`;
          if(d.env_vars&&d.env_vars.length)h+=`<div class="op-muted" style="margin-top:2px">Env: ${d.env_vars.map(e=>esc(e.name)+'='+esc(e.value)).join(', ')}</div>`;
          h+=`</div>`;
        }
      }else if(!deploymentsLoaded){
        h+=`<div class="op-empty"><button class="op-btn op-btn-primary" data-action="load-deployments">Load Deployment History</button></div>`;
      }else{h+=`<div class="op-empty">No deployments.</div>`}
      h+=`</div>`;

      // Permissions
      h+=`<div class="op-tab-content" data-tab="permissions" style="${currentTab!=='permissions'?'display:none':''}">`;
      const perms=getPerms();
      if(permEditState){
        h+=`<div style="margin-bottom:12px;font-weight:600">Edit Permissions</div>`;
        for(let i=0;i<permEditState.length;i++){
          const p=permEditState[i];
          h+=`<div class="op-perm-row">`;
          h+=`<select class="op-select" data-perm-type="${i}" style="width:130px"><option value="user"${p.type==='user'?' selected':''}>User</option><option value="group"${p.type==='group'?' selected':''}>Group</option><option value="service_principal"${p.type==='service_principal'?' selected':''}>SP</option></select>`;
          h+=`<input class="op-input" data-perm-principal="${i}" value="${esc(p.principal)}" placeholder="name@example.com">`;
          h+=`<select class="op-select" data-perm-level="${i}" style="width:140px">`;
          for(const lv of PERMISSION_LEVELS)h+=`<option value="${lv}"${p.level===lv?' selected':''}>${lv}</option>`;
          h+=`</select>`;
          h+=`<button class="kv-del" data-perm-del="${i}">✕</button></div>`;
        }
        h+=`<button class="op-btn" data-action="add-perm" style="margin-top:4px">+ Add</button>`;
        h+=`<div class="op-form-actions"><button class="op-btn op-btn-primary" data-action="save-perms">💾 Save Permissions</button><button class="op-btn" data-action="cancel-perms">Cancel</button></div>`;
      }else if(perms.acl&&perms.acl.length){
        h+=`<div style="margin-bottom:8px"><button class="op-btn" data-action="edit-perms">✏️ Edit</button></div>`;
        h+=`<table class="op-table"><thead><tr><th>Principal</th><th>Type</th><th>Permissions</th></tr></thead><tbody>`;
        for(const e of perms.acl){
          const levels=(e.permissions||[]).map(p=>{let s=esc(p.level);if(p.inherited)s+=' <span class="op-muted">(inherited)</span>';return s}).join(', ');
          h+=`<tr><td class="op-mono">${esc(e.principal)}</td><td class="op-muted">${esc(e.type)}</td><td>${levels}</td></tr>`;
        }
        h+=`</tbody></table>`;
      }else if(!permissionsLoaded){
        h+=`<div class="op-empty"><button class="op-btn op-btn-primary" data-action="load-permissions">Load Permissions</button></div>`;
      }else{
        h+=`<div style="margin-bottom:8px"><button class="op-btn" data-action="edit-perms">✏️ Edit Permissions</button></div>`;
        h+=`<div class="op-empty">No permissions configured.</div>`;
      }
      h+=`</div>`;

      // Thumbnail
      h+=`<div class="op-tab-content" data-tab="thumbnail" style="${currentTab!=='thumbnail'?'display:none':''}">`;
      // Current thumbnail
      const thumbCurrent=getThumb();
      if(thumbCurrent.thumbnail){
        h+=`<div class="op-section">Current Thumbnail</div>`;
        h+=`<img class="op-thumb-current" src="data:image/png;base64,${esc(thumbCurrent.thumbnail)}" alt="Current thumbnail">`;
      }else if(thumbCurrent.loaded){
        h+=`<div class="op-muted" style="margin-bottom:12px">No thumbnail set for this app.</div>`;
      }
      h+=`<div class="op-section">${thumbCurrent.thumbnail?'Replace':'Set'} Thumbnail</div>`;
      h+=`<div class="op-muted" style="margin-bottom:8px">Select an image file to use as the app thumbnail.</div>`;
      h+=`<div class="op-file-input"><input type="file" accept="image/*" data-action="thumb-file"></div>`;
      if(thumbnailFile){
        h+=`<div style="margin-top:8px;font-size:12px;color:var(--op-text-muted)">Preview:</div>`;
        h+=`<img class="op-thumb-preview" src="data:image/png;base64,${esc(thumbnailFile)}">`;
        h+=`<div class="op-form-actions"><button class="op-btn op-btn-primary" data-action="upload-thumb">📤 Upload Thumbnail</button><button class="op-btn" data-action="clear-thumb">Clear</button></div>`;
      }
      if(thumbCurrent.thumbnail)h+=`<div style="margin-top:16px"><button class="op-btn op-btn-danger" data-action="delete-thumb">🗑️ Delete Current Thumbnail</button></div>`;
      h+=`</div>`;

      h+=`</div></div>`;
    }
    h+=`<div class="op-status-bar"><span>Last refresh: ${new Date().toLocaleTimeString()}</span><span>${esc(a.name||'')}</span></div>`;
    root.innerHTML=h;bindEvents();
  }

  function bindEvents(){
    root.querySelectorAll(".op-tab").forEach(t=>t.addEventListener("click",()=>{currentTab=t.dataset.tab;fullRender()}));
    root.querySelector("[data-action='refresh']")?.addEventListener("click",()=>send({action:"refresh"}));
    root.querySelector("[data-action='toggle-refresh']")?.addEventListener("click",()=>{autoRefreshEnabled=!autoRefreshEnabled;if(autoRefreshEnabled)startAR();else stopAR();fullRender()});

    // Start/Stop
    root.querySelector("[data-action='start']")?.addEventListener("click",()=>{confirmAction={message:"Start this app?",btnClass:"success",yesLabel:"▶ Start",action:"start"};fullRender()});
    root.querySelector("[data-action='stop']")?.addEventListener("click",()=>{confirmAction={message:"Stop this app?",btnClass:"danger",yesLabel:"⏹ Stop",action:"stop"};fullRender()});
    root.querySelector("[data-action='confirm-yes']")?.addEventListener("click",()=>{const a=confirmAction.action;confirmAction=null;send({action:a})});
    root.querySelector("[data-action='confirm-no']")?.addEventListener("click",()=>{confirmAction=null;fullRender()});

    // Deployments
    root.querySelector("[data-action='load-deployments']")?.addEventListener("click",()=>{deploymentsLoaded=true;send({action:"list_deployments"})});
    root.querySelector("[data-action='new-deploy']")?.addEventListener("click",()=>{const a=getApp();deployForm={source_code_path:a.default_source_code_path||"",mode:"SNAPSHOT"};fullRender()});
    root.querySelector("[data-action='cancel-deploy']")?.addEventListener("click",()=>{deployForm=null;fullRender()});
    root.querySelector("[data-action='submit-deploy']")?.addEventListener("click",()=>{
      root.querySelectorAll("[data-deploy-field]").forEach(el=>{if(deployForm)deployForm[el.dataset.deployField]=el.value});
      send({action:"deploy",config:deployForm});deployForm=null;
    });

    // Permissions
    root.querySelector("[data-action='load-permissions']")?.addEventListener("click",()=>{permissionsLoaded=true;send({action:"get_permissions"})});
    root.querySelector("[data-action='edit-perms']")?.addEventListener("click",()=>{
      const perms=getPerms();
      permEditState=(perms.acl||[]).map(e=>({principal:e.principal||"",type:e.type||"user",level:(e.permissions&&e.permissions.length?e.permissions[0].level:"CAN_USE")||"CAN_USE"}));
      fullRender();
    });
    root.querySelector("[data-action='cancel-perms']")?.addEventListener("click",()=>{permEditState=null;fullRender()});
    root.querySelector("[data-action='add-perm']")?.addEventListener("click",()=>{readPermState();permEditState.push({principal:"",type:"user",level:"CAN_USE"});fullRender()});
    root.querySelectorAll("[data-perm-del]").forEach(el=>el.addEventListener("click",()=>{readPermState();permEditState.splice(parseInt(el.dataset.permDel),1);fullRender()}));
    root.querySelector("[data-action='save-perms']")?.addEventListener("click",()=>{
      readPermState();
      const acl=permEditState.filter(p=>p.principal.trim()).map(p=>{
        const r={permission_level:p.level};
        if(p.type==='user')r.user_name=p.principal;
        else if(p.type==='group')r.group_name=p.principal;
        else r.service_principal_name=p.principal;
        return r;
      });
      send({action:"update_permissions",acl});permEditState=null;
    });

    // Thumbnail
    root.querySelector("[data-action='thumb-file']")?.addEventListener("change",(e)=>{
      const file=e.target.files[0];if(!file)return;
      const reader=new FileReader();
      reader.onload=()=>{thumbnailFile=reader.result.split(",")[1];fullRender()};
      reader.readAsDataURL(file);
    });
    root.querySelector("[data-action='upload-thumb']")?.addEventListener("click",()=>{if(thumbnailFile)send({action:"update_thumbnail",thumbnail_base64:thumbnailFile})});
    root.querySelector("[data-action='clear-thumb']")?.addEventListener("click",()=>{thumbnailFile=null;fullRender()});
    root.querySelector("[data-action='delete-thumb']")?.addEventListener("click",()=>{confirmAction={message:"Delete the app thumbnail?",btnClass:"danger",yesLabel:"🗑️ Delete",action:"delete_thumbnail"};fullRender()});
  }

  function readPermState(){
    if(!permEditState)return;
    root.querySelectorAll("[data-perm-type]").forEach(el=>{permEditState[parseInt(el.dataset.permType)].type=el.value});
    root.querySelectorAll("[data-perm-principal]").forEach(el=>{permEditState[parseInt(el.dataset.permPrincipal)].principal=el.value});
    root.querySelectorAll("[data-perm-level]").forEach(el=>{permEditState[parseInt(el.dataset.permLevel)].level=el.value});
  }

  model.on("change:app_data",fullRender);
  model.on("change:deployments_data",fullRender);
  model.on("change:permissions_data",()=>{permEditState=null;fullRender()});
  model.on("change:thumbnail_data",()=>{thumbnailFile=null;fullRender()});
  model.on("change:loading",fullRender);
  model.on("change:error_message",fullRender);
  model.on("change:action_result",()=>{
    try{const r=JSON.parse(model.get("action_result")||"{}");actionMessage=r.message;actionIsError=!r.success;fullRender();if(r.success)setTimeout(()=>{actionMessage=null;fullRender()},5000)}catch(e){}
  });

  fullRender();startAR();
  return()=>stopAR();
}
export default{render};
