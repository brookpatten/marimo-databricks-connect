// Genie Widget Frontend
const OPS_STYLES = `
  :host {
    --op-bg: #ffffff; --op-bg-alt: #f8f9fa; --op-bg-hover: #e9ecef;
    --op-border: #dee2e6; --op-text: #212529; --op-text-muted: #6c757d;
    --op-primary: #0d6efd; --op-success: #198754; --op-danger: #dc3545; --op-warning: #ffc107; --op-info: #0dcaf0;
    --op-user-bubble: #e7f1ff; --op-asst-bubble: #f8f9fa;
    --op-font: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
    --op-font-mono: "SF Mono", "Cascadia Code", "Fira Code", Menlo, Consolas, monospace;
    --op-radius: 6px;
    display: block; font-family: var(--op-font); font-size: 13px; color: var(--op-text); background: var(--op-bg); border: 1px solid var(--op-border); border-radius: var(--op-radius); overflow: hidden;
  }
  :host(.dark-theme) {
    --op-bg: #1e1e1e; --op-bg-alt: #252526; --op-bg-hover: #2d2d30;
    --op-border: #3e3e42; --op-text: #cccccc; --op-text-muted: #888888;
    --op-primary: #4fc3f7; --op-success: #66bb6a; --op-danger: #ef5350; --op-warning: #ffca28; --op-info: #4dd0e1;
    --op-user-bubble: #1e3a5f; --op-asst-bubble: #252526;
  }
  * { box-sizing: border-box; }
  .op-header { display: flex; align-items: center; gap: 8px; padding: 10px 14px; background: var(--op-bg-alt); border-bottom: 1px solid var(--op-border); flex-wrap: wrap; }
  .op-header h2 { margin: 0; font-size: 14px; font-weight: 600; }
  .op-header-sub { font-size: 11px; color: var(--op-text-muted); margin-left: 4px; }
  .op-header-actions { margin-left: auto; display: flex; gap: 6px; align-items: center; }
  .op-btn { padding: 4px 10px; border: 1px solid var(--op-border); border-radius: 4px; background: var(--op-bg); color: var(--op-text); cursor: pointer; font-size: 12px; font-family: var(--op-font); }
  .op-btn:hover:not(:disabled) { background: var(--op-bg-hover); }
  .op-btn:disabled { opacity: 0.5; cursor: not-allowed; }
  .op-btn-primary { background: var(--op-primary); color: #fff; border-color: var(--op-primary); }
  .op-btn-primary:hover:not(:disabled) { filter: brightness(1.1); background: var(--op-primary); }
  .op-error { padding: 10px 14px; background: #fef2f2; color: var(--op-danger); border-bottom: 1px solid #fecaca; font-size: 12px; }
  :host(.dark-theme) .op-error { background: #3b1f1f; border-color: #5c2b2b; }

  .op-chat { max-height: 600px; min-height: 200px; overflow-y: auto; padding: 14px; display: flex; flex-direction: column; gap: 12px; }
  .op-empty { text-align: center; color: var(--op-text-muted); padding: 30px 14px; }
  .op-empty .hint { font-size: 12px; margin-top: 8px; }
  .op-msg { display: flex; flex-direction: column; gap: 6px; max-width: 100%; }
  .op-msg-user { align-items: flex-end; }
  .op-msg-asst { align-items: flex-start; }
  .op-bubble { padding: 10px 12px; border-radius: 10px; max-width: 90%; word-wrap: break-word; white-space: pre-wrap; line-height: 1.45; font-size: 13px; }
  .op-msg-user .op-bubble { background: var(--op-user-bubble); border: 1px solid var(--op-border); }
  .op-msg-asst .op-bubble { background: var(--op-asst-bubble); border: 1px solid var(--op-border); }
  .op-msg-meta { font-size: 10px; color: var(--op-text-muted); }
  .op-msg-status { font-size: 11px; color: var(--op-text-muted); font-style: italic; }
  .op-msg-error { color: var(--op-danger); font-size: 12px; padding: 6px 10px; background: var(--op-bg-alt); border-left: 3px solid var(--op-danger); border-radius: 4px; }

  .op-attachment { width: 100%; max-width: 100%; border: 1px solid var(--op-border); border-radius: 6px; overflow: hidden; }
  .op-attachment-header { padding: 6px 10px; background: var(--op-bg-alt); border-bottom: 1px solid var(--op-border); display: flex; align-items: center; gap: 8px; font-size: 11px; color: var(--op-text-muted); font-weight: 600; text-transform: uppercase; letter-spacing: 0.04em; }
  .op-attachment-header .title { color: var(--op-text); text-transform: none; letter-spacing: 0; font-weight: 500; }
  .op-attachment-header .actions { margin-left: auto; }
  .op-attachment-body { padding: 10px; }
  .op-sql { font-family: var(--op-font-mono); font-size: 12px; white-space: pre; overflow-x: auto; padding: 10px; background: var(--op-bg-alt); border-radius: 4px; margin: 0; line-height: 1.5; }
  .op-desc { font-size: 12px; color: var(--op-text-muted); margin-bottom: 8px; }

  .op-suggested { display: flex; flex-wrap: wrap; gap: 6px; }
  .op-pill { padding: 5px 10px; border: 1px solid var(--op-primary); color: var(--op-primary); background: transparent; border-radius: 16px; font-size: 12px; cursor: pointer; font-family: var(--op-font); }
  .op-pill:hover { background: var(--op-primary); color: #fff; }

  .op-result-table-wrap { max-height: 320px; overflow: auto; border: 1px solid var(--op-border); border-radius: 4px; margin-top: 8px; }
  table.op-table { width: 100%; border-collapse: collapse; font-size: 12px; }
  .op-table th { position: sticky; top: 0; text-align: left; padding: 6px 10px; font-size: 11px; font-weight: 600; text-transform: uppercase; color: var(--op-text-muted); background: var(--op-bg-alt); border-bottom: 1px solid var(--op-border); white-space: nowrap; }
  .op-table td { padding: 5px 10px; border-bottom: 1px solid var(--op-border); vertical-align: top; font-family: var(--op-font-mono); }
  .op-result-meta { font-size: 11px; color: var(--op-text-muted); margin-top: 6px; }
  .op-result-meta .truncated { color: var(--op-warning); margin-left: 8px; }

  .op-spinner { display: inline-block; width: 14px; height: 14px; border: 2px solid var(--op-border); border-top-color: var(--op-primary); border-radius: 50%; animation: op-spin 0.6s linear infinite; vertical-align: middle; margin-right: 6px; }
  @keyframes op-spin { to { transform: rotate(360deg); } }

  .op-input-bar { display: flex; gap: 6px; padding: 10px 14px; border-top: 1px solid var(--op-border); background: var(--op-bg-alt); }
  .op-input-bar textarea { flex: 1; resize: none; min-height: 38px; max-height: 160px; padding: 8px 10px; border: 1px solid var(--op-border); border-radius: 6px; background: var(--op-bg); color: var(--op-text); font-family: var(--op-font); font-size: 13px; line-height: 1.4; }
  .op-input-bar textarea:focus { outline: none; border-color: var(--op-primary); }
  .op-input-bar button { align-self: stretch; min-width: 64px; }

  .op-conv-list { position: absolute; right: 14px; top: 44px; background: var(--op-bg); border: 1px solid var(--op-border); border-radius: 6px; box-shadow: 0 4px 12px rgba(0,0,0,0.12); max-height: 300px; overflow: auto; min-width: 280px; z-index: 100; }
  .op-conv-item { padding: 8px 12px; cursor: pointer; border-bottom: 1px solid var(--op-border); font-size: 12px; }
  .op-conv-item:last-child { border-bottom: none; }
  .op-conv-item:hover { background: var(--op-bg-hover); }
  .op-conv-item .ts { font-size: 10px; color: var(--op-text-muted); margin-top: 2px; }
`;

function esc(s) { if (s == null) return ""; const d = document.createElement("div"); d.textContent = String(s); return d.innerHTML; }

function _syncTheme(hostEl) {
  hostEl.__cleanupThemeSync?.();
  const media = window.matchMedia("(prefers-color-scheme: dark)");
  const themeSelector = "[data-app-theme], [data-theme], .dark, .dark-theme, .light, .light-theme";
  function parseTheme(value) { if (!value) return null; const v = String(value).toLowerCase(); if (v.includes("dark")) return true; if (v.includes("light")) return false; return null; }
  function backgroundLooksDark(el) { if (!el) return null; const bg = getComputedStyle(el).backgroundColor; const m = bg && bg.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/i); if (!m) return null; const [, r, g, b] = m.map(Number); return ((0.2126*r + 0.7152*g + 0.0722*b)/255) < 0.5; }
  function themeFromElement(el) { if (!el) return null; const a = parseTheme(el.getAttribute?.("data-app-theme")); if (a != null) return a; const d = parseTheme(el.getAttribute?.("data-theme")); if (d != null) return d; const c = parseTheme(el.className); if (c != null) return c; const s = parseTheme(getComputedStyle(el).colorScheme); if (s != null) return s; return null; }
  function isDark() { const t = hostEl.closest?.(themeSelector); return themeFromElement(t) ?? themeFromElement(hostEl) ?? themeFromElement(hostEl.parentElement) ?? themeFromElement(document.body) ?? themeFromElement(document.documentElement) ?? backgroundLooksDark(hostEl.parentElement) ?? backgroundLooksDark(document.body) ?? media.matches; }
  function apply() { const dark = isDark(); hostEl.classList.toggle("dark-theme", dark); hostEl.style.colorScheme = dark ? "dark" : "light"; }
  apply();
  const obs = new MutationObserver(apply);
  const observed = new Set();
  function observe(el) { if (!el || observed.has(el)) return; obs.observe(el, { attributes: true, attributeFilter: ["data-app-theme","data-theme","class","style"] }); observed.add(el); }
  observe(document.documentElement); observe(document.body); observe(hostEl.parentElement); observe(hostEl.closest?.(themeSelector));
  media.addEventListener("change", apply);
  const cleanup = () => { obs.disconnect(); media.removeEventListener("change", apply); if (hostEl.__cleanupThemeSync === cleanup) delete hostEl.__cleanupThemeSync; };
  hostEl.__cleanupThemeSync = cleanup;
  return cleanup;
}

function fmtCell(v) {
  if (v == null) return '<span style="color:var(--op-text-muted)">NULL</span>';
  const s = String(v);
  if (s.length > 200) return esc(s.slice(0, 200)) + '…';
  return esc(s);
}

function render({ model, el }) {
  const shadow = el.attachShadow ? el.attachShadow({ mode: "open" }) : el;
  _syncTheme(el);
  const styleEl = document.createElement("style"); styleEl.textContent = OPS_STYLES; shadow.appendChild(styleEl);
  const root = document.createElement("div"); shadow.appendChild(root);

  let inputDraft = "";
  let showConvList = false;
  // Local optimistic state: while the kernel is blocked on a Genie request,
  // we don't get a `busy` traitlet update until it returns. Track it here so
  // the UI can show the user's pending question + a spinner immediately.
  let pendingAsk = null;  // { content } | null

  function getSpace() { try { return JSON.parse(model.get("space_data") || "{}"); } catch { return {}; } }
  function getMessages() { try { return JSON.parse(model.get("messages") || "[]"); } catch { return []; } }
  function getConversations() { try { return JSON.parse(model.get("conversations") || "[]"); } catch { return []; } }
  function getResults() { try { return JSON.parse(model.get("query_results") || "{}"); } catch { return {}; } }

  function sendRequest(req) { model.set("request", JSON.stringify({ ...req, _t: Date.now() })); model.save_changes(); }

  function renderAttachment(att, msg) {
    const results = getResults();
    let html = '';
    if (att.text) {
      html += `<div class="op-attachment">`;
      html += `<div class="op-attachment-header">💬 Answer</div>`;
      html += `<div class="op-attachment-body" style="white-space:pre-wrap">${esc(att.text.content || '')}</div>`;
      html += `</div>`;
    }
    if (att.query) {
      const r = results[att.attachment_id];
      html += `<div class="op-attachment">`;
      html += `<div class="op-attachment-header">🔎 SQL${att.query.title ? ` · <span class="title">${esc(att.query.title)}</span>` : ''}`;
      const isRunning = r && r.state === 'RUNNING';
      const btnLabel = isRunning ? '<span class="op-spinner"></span>Running…' : (r ? '↻ Re-run' : '▶ Run query');
      html += `<span class="actions"><button class="op-btn op-btn-primary" data-run-att="${esc(att.attachment_id)}" data-run-msg="${esc(msg.message_id)}" ${isRunning ? 'disabled' : ''}>${btnLabel}</button></span>`;
      html += `</div>`;
      html += `<div class="op-attachment-body">`;
      if (att.query.description) html += `<div class="op-desc">${esc(att.query.description)}</div>`;
      html += `<pre class="op-sql">${esc(att.query.query || '')}</pre>`;
      if (r && r.state === 'FAILED') {
        html += `<div class="op-msg-error" style="margin-top:8px">${esc(r.error || 'Query failed')}</div>`;
      } else if (r && r.columns) {
        html += renderResultTable(r);
      }
      html += `</div></div>`;
    }
    if (att.suggested_questions && att.suggested_questions.length) {
      html += `<div class="op-attachment">`;
      html += `<div class="op-attachment-header">💡 Suggested questions</div>`;
      html += `<div class="op-attachment-body"><div class="op-suggested">`;
      for (const q of att.suggested_questions) {
        html += `<button class="op-pill" data-suggested="${esc(q)}">${esc(q)}</button>`;
      }
      html += `</div></div></div>`;
    }
    return html;
  }

  function renderResultTable(r) {
    if (!r.columns || !r.columns.length) {
      return `<div class="op-result-meta">No columns returned.</div>`;
    }
    let html = `<div class="op-result-table-wrap"><table class="op-table"><thead><tr>`;
    for (const c of r.columns) html += `<th title="${esc(c.type||'')}">${esc(c.name||'')}</th>`;
    html += `</tr></thead><tbody>`;
    if (!r.rows || !r.rows.length) {
      html += `<tr><td colspan="${r.columns.length}" style="color:var(--op-text-muted);text-align:center">(no rows)</td></tr>`;
    } else {
      for (const row of r.rows) {
        html += `<tr>`;
        for (const v of row) html += `<td>${fmtCell(v)}</td>`;
        html += `</tr>`;
      }
    }
    html += `</tbody></table></div>`;
    const rc = r.row_count != null ? r.row_count : (r.rows ? r.rows.length : 0);
    html += `<div class="op-result-meta">${rc} row${rc === 1 ? '' : 's'}${r.truncated ? '<span class="truncated">⚠ truncated</span>' : ''}</div>`;
    return html;
  }

  function renderMessage(m) {
    let html = '';
    // User question bubble
    if (m.content) {
      html += `<div class="op-msg op-msg-user">`;
      html += `<div class="op-bubble">${esc(m.content)}</div>`;
      if (m.created_timestamp) html += `<div class="op-msg-meta">${esc(m.created_timestamp)}</div>`;
      html += `</div>`;
    }
    // Assistant response
    const isPending = m.message_id === '_pending_';
    const status = (m.status || '').toUpperCase();
    const inFlight = isPending || ['SUBMITTED','ASKING_AI','FILTERING_CONTEXT','FETCHING_METADATA','EXECUTING_QUERY','PENDING_WAREHOUSE'].includes(status);
    const hasContent = (m.attachments && m.attachments.length) || m.error || status === 'FAILED' || status === 'CANCELLED';
    if (inFlight && !hasContent) {
      html += `<div class="op-msg op-msg-asst"><div class="op-msg-status"><span class="op-spinner"></span>${esc(status || 'Thinking…')}</div></div>`;
    }
    if (m.error || status === 'FAILED') {
      html += `<div class="op-msg op-msg-asst"><div class="op-msg-error">${esc(m.error || 'Genie returned an error')}</div></div>`;
    }
    if (m.attachments && m.attachments.length) {
      html += `<div class="op-msg op-msg-asst" style="width:100%">`;
      for (const att of m.attachments) html += renderAttachment(att, m);
      html += `</div>`;
    }
    return html;
  }

  function fullRender() {
    const space = getSpace();
    const messages = getMessages().slice();
    const busy = model.get("busy") || !!pendingAsk;
    const error = model.get("error_message");
    const cid = model.get("conversation_id");

    // Inject the optimistic pending message at the end so the user sees their
    // question + a thinking spinner immediately, before the kernel returns.
    if (pendingAsk) {
      messages.push({
        message_id: "_pending_",
        content: pendingAsk.content,
        status: "SUBMITTED",
        attachments: [],
      });
    }

    let html = `<div style="position:relative">`;
    html += `<div class="op-header"><h2>🧞 ${esc(space.title || 'Genie')}</h2>`;
    if (space.description) html += `<span class="op-header-sub">${esc(space.description)}</span>`;
    html += `<div class="op-header-actions">`;
    if (cid) html += `<span class="op-header-sub" title="${esc(cid)}">conv: ${esc(cid.slice(0,8))}…</span>`;
    html += `<button class="op-btn" data-action="conversations" title="Past conversations">🗂</button>`;
    html += `<button class="op-btn" data-action="new-conv" title="New conversation">＋ New</button>`;
    html += `<button class="op-btn" data-action="refresh" title="Refresh space">↻</button>`;
    html += `</div></div>`;

    if (error) html += `<div class="op-error">${esc(error)}</div>`;

    if (showConvList) {
      const convs = getConversations();
      html += `<div class="op-conv-list">`;
      if (!convs.length) {
        html += `<div class="op-conv-item" style="color:var(--op-text-muted);cursor:default">No conversations</div>`;
      } else {
        for (const c of convs) {
          html += `<div class="op-conv-item" data-conv="${esc(c.conversation_id)}">${esc(c.title || '(untitled)')}<div class="ts">${esc(c.last_updated_timestamp || c.created_timestamp || '')}</div></div>`;
        }
      }
      html += `</div>`;
    }

    html += `<div class="op-chat">`;
    if (!messages.length) {
      html += `<div class="op-empty">Ask Genie a question about your data.<div class="hint">e.g. <em>"What were total sales last quarter by region?"</em></div></div>`;
    } else {
      for (const m of messages) html += renderMessage(m);
    }
    html += `</div>`;

    html += `<div class="op-input-bar">`;
    html += `<textarea data-input placeholder="Ask Genie…" ${busy ? 'disabled' : ''}>${esc(inputDraft)}</textarea>`;
    html += `<button class="op-btn op-btn-primary" data-action="send" ${busy ? 'disabled' : ''}>${busy ? 'Asking…' : 'Send'}</button>`;
    html += `</div>`;
    html += `</div>`;

    root.innerHTML = html;
    bindEvents();
    // Auto-scroll chat to bottom
    const chat = root.querySelector(".op-chat");
    if (chat) chat.scrollTop = chat.scrollHeight;
    // Keep textarea focus & cursor position after re-render
    const ta = root.querySelector("[data-input]");
    if (ta && document.activeElement !== el) {
      // best effort: focus only if user just typed (inputDraft non-empty) and not busy
    }
  }

  function bindEvents() {
    root.querySelector("[data-action='refresh']")?.addEventListener("click", () => sendRequest({ action: "refresh" }));
    root.querySelector("[data-action='new-conv']")?.addEventListener("click", () => { showConvList = false; sendRequest({ action: "new_conversation" }); });
    root.querySelector("[data-action='conversations']")?.addEventListener("click", () => {
      showConvList = !showConvList;
      if (showConvList) sendRequest({ action: "list_conversations" });
      fullRender();
    });
    root.querySelectorAll("[data-conv]").forEach(el => {
      el.addEventListener("click", () => {
        const cid = el.dataset.conv;
        showConvList = false;
        sendRequest({ action: "select_conversation", conversation_id: cid });
      });
    });
    root.querySelectorAll("[data-suggested]").forEach(b => {
      b.addEventListener("click", () => {
        const q = b.dataset.suggested;
        inputDraft = "";
        sendRequest({ action: "ask", content: q });
      });
    });
    root.querySelectorAll("[data-run-att]").forEach(b => {
      b.addEventListener("click", () => sendRequest({ action: "run_query", attachment_id: b.dataset.runAtt, message_id: b.dataset.runMsg }));
    });

    const ta = root.querySelector("[data-input]");
    if (ta) {
      ta.addEventListener("input", () => { inputDraft = ta.value; });
      ta.addEventListener("keydown", (e) => {
        if (e.key === "Enter" && !e.shiftKey) {
          e.preventDefault();
          submit();
        }
      });
    }
    root.querySelector("[data-action='send']")?.addEventListener("click", submit);
  }

  function submit() {
    const ta = root.querySelector("[data-input]");
    const content = (ta ? ta.value : inputDraft).trim();
    if (!content) return;
    if (model.get("busy") || pendingAsk) return;
    inputDraft = "";
    pendingAsk = { content };
    fullRender();           // optimistic UI: show user bubble + spinner now
    sendRequest({ action: "ask", content });
  }

  function clearPendingIfDone() {
    // When the kernel finishes the ask, `busy` flips back to false and a new
    // assistant message has been appended; drop the optimistic placeholder.
    if (pendingAsk && model.get("busy") === false) {
      pendingAsk = null;
    }
  }

  model.on("change:space_data", fullRender);
  model.on("change:messages", () => { clearPendingIfDone(); fullRender(); });
  model.on("change:conversations", fullRender);
  model.on("change:conversation_id", fullRender);
  model.on("change:query_results", fullRender);
  model.on("change:busy", () => { clearPendingIfDone(); fullRender(); });
  model.on("change:loading", fullRender);
  model.on("change:error_message", () => { clearPendingIfDone(); fullRender(); });

  fullRender();
}

export default { render };
