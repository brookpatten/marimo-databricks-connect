"""Anywidget for browsing Databricks Lakeflow Declarative Pipelines (DLT).

Lists pipelines in the workspace with drill-down into each pipeline's spec,
recent updates, and event log.

Usage in a marimo notebook::

    from marimo_databricks_connect import pipelines_widget
    widget = pipelines_widget()
    widget  # display in cell output
"""

from __future__ import annotations

import json
import logging
import pathlib
from typing import Any

import anywidget
import traitlets

from ._ops_common import enum_val, ms_to_iso
from ._pipeline_widget import (
    _serialize_event,
    _serialize_pipeline,
    _serialize_update,
)

LOGGER = logging.getLogger(__name__)

_ESM_PATH = pathlib.Path(__file__).parent / "_pipelines_frontend.js"


def _serialize_pipeline_summary(p: Any) -> dict:
    """Lightweight pipeline serialization for the list view."""
    latest = list(getattr(p, "latest_updates", None) or [])
    last_update_state = None
    last_update_time = None
    if latest:
        last_update_state = enum_val(getattr(latest[0], "state", None))
        ct = getattr(latest[0], "creation_time", None)
        last_update_time = ms_to_iso(ct) if isinstance(ct, (int, float)) else ct
    return {
        "pipeline_id": getattr(p, "pipeline_id", None),
        "name": getattr(p, "name", None),
        "state": enum_val(getattr(p, "state", None)),
        "health": enum_val(getattr(p, "health", None)),
        "creator": getattr(p, "creator_user_name", None),
        "run_as": getattr(p, "run_as_user_name", None),
        "cluster_id": getattr(p, "cluster_id", None),
        "last_update_state": last_update_state,
        "last_update_time": last_update_time,
    }


class PipelinesWidget(anywidget.AnyWidget):
    """Browser for Databricks Lakeflow Declarative Pipelines (DLT)."""

    _esm = traitlets.Unicode().tag(sync=True)
    _css = traitlets.Unicode("").tag(sync=True)

    pipelines_data = traitlets.Unicode("[]").tag(sync=True)
    refresh_seconds = traitlets.Int(60).tag(sync=True)
    pipeline_detail = traitlets.Unicode("{}").tag(sync=True)
    updates_data = traitlets.Unicode("[]").tag(sync=True)
    events_data = traitlets.Unicode("[]").tag(sync=True)
    loading = traitlets.Bool(False).tag(sync=True)
    error_message = traitlets.Unicode("").tag(sync=True)

    request = traitlets.Unicode("").tag(sync=True)

    def __init__(self, workspace_client: Any = None, refresh_seconds: int = 60, **kwargs: Any) -> None:
        esm_content = _ESM_PATH.read_text()
        super().__init__(_esm=esm_content, **kwargs)
        self._ws = workspace_client
        self._refresh_seconds = refresh_seconds
        self.refresh_seconds = refresh_seconds
        self.observe(self._handle_request, names=["request"])
        self._load_pipelines()

    def _get_client(self) -> Any:
        if self._ws is not None:
            return self._ws
        from databricks.sdk import WorkspaceClient

        self._ws = WorkspaceClient()
        return self._ws

    def _handle_request(self, change: Any) -> None:
        raw = change.get("new", "")
        if not raw:
            return
        try:
            req = json.loads(raw)
        except json.JSONDecodeError:
            return
        action = req.get("action")
        if action == "list_pipelines":
            self._load_pipelines(name_filter=req.get("name_filter"))
        elif action == "get_pipeline":
            self._load_pipeline_detail(req["pipeline_id"])
        elif action == "list_updates":
            self._load_updates(req["pipeline_id"])
        elif action == "list_events":
            self._load_events(req["pipeline_id"], filter_text=req.get("filter"))

    # -- loaders ----------------------------------------------------------

    def _load_pipelines(self, name_filter: str | None = None) -> None:
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            kwargs: dict[str, Any] = {"max_results": 100}
            if name_filter:
                escaped = name_filter.replace("'", "''")
                kwargs["filter"] = f"name LIKE '%{escaped}%'"
            pipelines = list(ws.pipelines.list_pipelines(**kwargs))
            self.pipelines_data = json.dumps([_serialize_pipeline_summary(p) for p in pipelines])
        except Exception as exc:
            LOGGER.debug("Failed to list pipelines", exc_info=True)
            self.error_message = f"Failed to list pipelines: {exc}"
        finally:
            self.loading = False

    def _load_pipeline_detail(self, pipeline_id: str) -> None:
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            p = ws.pipelines.get(pipeline_id)
            self.pipeline_detail = json.dumps(_serialize_pipeline(p))
        except Exception as exc:
            LOGGER.debug("Failed to get pipeline %s", pipeline_id, exc_info=True)
            self.error_message = f"Failed to get pipeline: {exc}"
        finally:
            self.loading = False

    def _load_updates(self, pipeline_id: str) -> None:
        self.error_message = ""
        try:
            ws = self._get_client()
            resp = ws.pipelines.list_updates(pipeline_id, max_results=25)
            updates = list(getattr(resp, "updates", None) or [])
            self.updates_data = json.dumps([_serialize_update(u) for u in updates])
        except Exception as exc:
            LOGGER.debug("Failed to list updates for %s", pipeline_id, exc_info=True)
            self.error_message = f"Failed to list updates: {exc}"

    def _load_events(self, pipeline_id: str, filter_text: str | None = None) -> None:
        self.error_message = ""
        try:
            ws = self._get_client()
            kwargs: dict[str, Any] = {"max_results": 100, "order_by": ["timestamp desc"]}
            if filter_text:
                kwargs["filter"] = filter_text
            events = list(ws.pipelines.list_pipeline_events(pipeline_id, **kwargs))
            self.events_data = json.dumps([_serialize_event(e) for e in events])
        except Exception as exc:
            LOGGER.debug("Failed to list events for %s", pipeline_id, exc_info=True)
            self.error_message = f"Failed to list events: {exc}"
