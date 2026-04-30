"""Conversational widget for a single Databricks Genie space.

Lets you chat with an AI/BI Genie space from a marimo cell — ask questions
in natural language, get back text answers and/or generated SQL, run the
generated query, and view tabular results inline.

Usage::

    from marimo_databricks_connect import genie_widget
    widget = genie_widget("01ef...space_id...")
    widget
"""

from __future__ import annotations

import json
import logging
import pathlib
from typing import Any

import anywidget
import traitlets

from ._ops_common import enum_val, ms_to_iso

LOGGER = logging.getLogger(__name__)

_ESM_PATH = pathlib.Path(__file__).parent / "_genie_widget_frontend.js"


# --------------------------------------------------------------------------- #
# Serialization helpers                                                        #
# --------------------------------------------------------------------------- #


def _serialize_space(space: Any) -> dict:
    return {
        "space_id": getattr(space, "space_id", None),
        "title": getattr(space, "title", None),
        "description": getattr(space, "description", None),
        "warehouse_id": getattr(space, "warehouse_id", None),
        "parent_path": getattr(space, "parent_path", None),
    }


def _serialize_attachment(att: Any) -> dict:
    out: dict[str, Any] = {"attachment_id": getattr(att, "attachment_id", None)}
    text = getattr(att, "text", None)
    if text is not None:
        out["text"] = {
            "content": getattr(text, "content", None),
            "purpose": enum_val(getattr(text, "purpose", None)),
        }
    query = getattr(att, "query", None)
    if query is not None:
        out["query"] = {
            "title": getattr(query, "title", None),
            "description": getattr(query, "description", None),
            "query": getattr(query, "query", None),
            "statement_id": getattr(query, "statement_id", None),
        }
    suggested = getattr(att, "suggested_questions", None)
    if suggested is not None:
        out["suggested_questions"] = list(getattr(suggested, "questions", None) or [])
    return out


def _serialize_message(msg: Any) -> dict:
    status = enum_val(getattr(msg, "status", None))
    err = getattr(msg, "error", None)
    return {
        "message_id": getattr(msg, "message_id", None) or getattr(msg, "id", None),
        "conversation_id": getattr(msg, "conversation_id", None),
        "content": getattr(msg, "content", None),
        "status": status,
        "created_timestamp": ms_to_iso(getattr(msg, "created_timestamp", None)),
        "user_id": getattr(msg, "user_id", None),
        "error": getattr(err, "error", None) if err else None,
        "attachments": [_serialize_attachment(a) for a in (getattr(msg, "attachments", None) or [])],
    }


def _serialize_conversation(conv: Any) -> dict:
    return {
        "conversation_id": getattr(conv, "conversation_id", None) or getattr(conv, "id", None),
        "title": getattr(conv, "title", None),
        "created_timestamp": ms_to_iso(getattr(conv, "created_timestamp", None)),
        "last_updated_timestamp": ms_to_iso(getattr(conv, "last_updated_timestamp", None)),
    }


def _serialize_query_result(resp: Any) -> dict:
    """Pull a flat ``{columns, rows, truncated, row_count, error}`` dict
    from a ``GenieGetMessageQueryResultResponse``."""
    sr = getattr(resp, "statement_response", None)
    if sr is None:
        return {"columns": [], "rows": [], "row_count": 0}
    status = getattr(sr, "status", None)
    state = enum_val(getattr(status, "state", None)) if status else None
    err = None
    if status and getattr(status, "error", None):
        err = getattr(status.error, "message", None) or str(status.error)

    manifest = getattr(sr, "manifest", None)
    schema = getattr(manifest, "schema", None) if manifest else None
    cols = []
    if schema:
        for c in getattr(schema, "columns", None) or []:
            cols.append(
                {
                    "name": getattr(c, "name", None),
                    "type": getattr(c, "type_text", None) or enum_val(getattr(c, "type_name", None)),
                }
            )

    result = getattr(sr, "result", None)
    rows = list(getattr(result, "data_array", None) or []) if result else []
    truncated = bool(getattr(manifest, "truncated", False)) if manifest else False
    total_rows = getattr(manifest, "total_row_count", None) if manifest else None

    return {
        "state": state,
        "error": err,
        "columns": cols,
        "rows": rows,
        "row_count": total_rows if total_rows is not None else len(rows),
        "truncated": truncated,
        "statement_id": getattr(sr, "statement_id", None),
    }


# --------------------------------------------------------------------------- #
# Widget                                                                       #
# --------------------------------------------------------------------------- #


class GenieWidget(anywidget.AnyWidget):
    """Chat with a Databricks Genie space from a marimo cell."""

    _esm = traitlets.Unicode().tag(sync=True)
    _css = traitlets.Unicode("").tag(sync=True)

    space_data = traitlets.Unicode("{}").tag(sync=True)
    # current conversation: list of message dicts (oldest first)
    messages = traitlets.Unicode("[]").tag(sync=True)
    # list of conversations (lazy-loaded on demand)
    conversations = traitlets.Unicode("[]").tag(sync=True)
    # mapping of attachment_id -> serialized query result dict (json-encoded)
    query_results = traitlets.Unicode("{}").tag(sync=True)
    conversation_id = traitlets.Unicode("").tag(sync=True)
    busy = traitlets.Bool(False).tag(sync=True)
    loading = traitlets.Bool(False).tag(sync=True)
    error_message = traitlets.Unicode("").tag(sync=True)
    request = traitlets.Unicode("").tag(sync=True)

    def __init__(
        self,
        space_id: str,
        workspace_client: Any = None,
        conversation_id: str | None = None,
        **kwargs: Any,
    ) -> None:
        esm_content = _ESM_PATH.read_text()
        super().__init__(_esm=esm_content, **kwargs)
        self._ws = workspace_client
        self._space_id = space_id
        self._results: dict[str, dict] = {}
        self.observe(self._handle_request, names=["request"])
        self._load_space()
        if conversation_id:
            self.conversation_id = conversation_id
            self._load_conversation_messages(conversation_id)

    # -- client helpers -----------------------------------------------------

    def _get_client(self) -> Any:
        if self._ws is not None:
            return self._ws
        from databricks.sdk import WorkspaceClient

        self._ws = WorkspaceClient()
        return self._ws

    def _genie(self) -> Any:
        return self._get_client().genie

    # -- request dispatch ---------------------------------------------------

    def _handle_request(self, change: Any) -> None:
        raw = change.get("new", "")
        if not raw:
            return
        try:
            req = json.loads(raw)
        except json.JSONDecodeError:
            return
        action = req.get("action")
        if action == "refresh":
            self._load_space()
        elif action == "ask":
            content = (req.get("content") or "").strip()
            if content:
                self._ask(content)
        elif action == "new_conversation":
            self.conversation_id = ""
            self.messages = "[]"
            self._results.clear()
            self.query_results = "{}"
            self.error_message = ""
        elif action == "list_conversations":
            self._list_conversations()
        elif action == "select_conversation":
            cid = req.get("conversation_id") or ""
            if cid:
                self._load_conversation_messages(cid)
        elif action == "run_query":
            att_id = req.get("attachment_id")
            msg_id = req.get("message_id")
            if att_id and msg_id:
                self._run_query(msg_id, att_id)

    # -- space + conversations ---------------------------------------------

    def _load_space(self) -> None:
        self.loading = True
        self.error_message = ""
        try:
            space = self._genie().get_space(self._space_id)
            self.space_data = json.dumps(_serialize_space(space))
        except Exception as exc:
            LOGGER.debug("Failed to get genie space %s", self._space_id, exc_info=True)
            self.error_message = f"Failed to load space: {exc}"
        finally:
            self.loading = False

    def _list_conversations(self) -> None:
        try:
            resp = self._genie().list_conversations(self._space_id, page_size=50)
            convs = list(getattr(resp, "conversations", None) or [])
            self.conversations = json.dumps([_serialize_conversation(c) for c in convs])
        except Exception as exc:
            LOGGER.debug("Failed to list conversations", exc_info=True)
            self.error_message = f"Failed to list conversations: {exc}"

    def _load_conversation_messages(self, cid: str) -> None:
        try:
            resp = self._genie().list_conversation_messages(self._space_id, cid)
            msgs = list(getattr(resp, "messages", None) or [])
            # API returns newest first; show oldest first in UI
            msgs_sorted = sorted(msgs, key=lambda m: getattr(m, "created_timestamp", 0) or 0)
            self.conversation_id = cid
            self.messages = json.dumps([_serialize_message(m) for m in msgs_sorted])
            self._results.clear()
            self.query_results = "{}"
            self.error_message = ""
        except Exception as exc:
            LOGGER.debug("Failed to load conversation %s", cid, exc_info=True)
            self.error_message = f"Failed to load conversation: {exc}"

    # -- chat ---------------------------------------------------------------

    def _current_messages(self) -> list[dict]:
        try:
            return json.loads(self.messages or "[]")
        except json.JSONDecodeError:
            return []

    def _ask(self, content: str) -> None:
        """Run a Genie request synchronously.

        The frontend handles the optimistic "asking…" UI state while this
        blocks; we just need to append the resulting message and (for a
        brand-new conversation) update ``conversation_id``.
        """
        self.busy = True
        self.error_message = ""
        try:
            genie = self._genie()
            if self.conversation_id:
                msg = genie.create_message_and_wait(
                    space_id=self._space_id,
                    conversation_id=self.conversation_id,
                    content=content,
                )
            else:
                msg = genie.start_conversation_and_wait(space_id=self._space_id, content=content)
                cid = getattr(msg, "conversation_id", None) or ""
                if cid:
                    self.conversation_id = cid
            msgs = self._current_messages()
            msgs.append(_serialize_message(msg))
            self.messages = json.dumps(msgs)
        except Exception as exc:
            LOGGER.debug("Genie ask failed", exc_info=True)
            self.error_message = f"Genie request failed: {exc}"
        finally:
            self.busy = False

    # -- query results ------------------------------------------------------

    def _run_query(self, message_id: str, attachment_id: str) -> None:
        try:
            # Mark pending in the results map so the UI can show a spinner
            self._results[attachment_id] = {"state": "RUNNING"}
            self.query_results = json.dumps(self._results)

            genie = self._genie()
            cid = self.conversation_id
            if not cid:
                raise RuntimeError("No active conversation")
            try:
                resp = genie.get_message_attachment_query_result(
                    space_id=self._space_id,
                    conversation_id=cid,
                    message_id=message_id,
                    attachment_id=attachment_id,
                )
            except Exception:
                # If the cached result has expired, re-execute the query.
                resp = genie.execute_message_attachment_query(
                    space_id=self._space_id,
                    conversation_id=cid,
                    message_id=message_id,
                    attachment_id=attachment_id,
                )
            self._results[attachment_id] = _serialize_query_result(resp)
        except Exception as exc:
            LOGGER.debug("Genie run_query failed", exc_info=True)
            self._results[attachment_id] = {"state": "FAILED", "error": str(exc)}
        finally:
            self.query_results = json.dumps(self._results)
