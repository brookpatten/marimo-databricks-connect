"""Operational widget for a single Databricks SQL warehouse.

Usage::

    from marimo_databricks_connect import warehouse_widget
    widget = warehouse_widget(warehouse_id="abc123def456")
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

_ESM_PATH = pathlib.Path(__file__).parent / "_warehouse_widget_frontend.js"


def _serialize_warehouse(w: Any) -> dict:
    health = getattr(w, "health", None)
    tags = getattr(w, "tags", None)
    custom_tags = {}
    if tags:
        for t in getattr(tags, "custom_tags", None) or []:
            k, v = getattr(t, "key", None), getattr(t, "value", None)
            if k:
                custom_tags[k] = v or ""
    return {
        "id": getattr(w, "id", None),
        "name": getattr(w, "name", None),
        "state": enum_val(getattr(w, "state", None)),
        "cluster_size": getattr(w, "cluster_size", None),
        "min_num_clusters": getattr(w, "min_num_clusters", None),
        "max_num_clusters": getattr(w, "max_num_clusters", None),
        "num_clusters": getattr(w, "num_clusters", None),
        "num_active_sessions": getattr(w, "num_active_sessions", None),
        "auto_stop_mins": getattr(w, "auto_stop_mins", None),
        "enable_photon": bool(getattr(w, "enable_photon", False)),
        "enable_serverless_compute": bool(getattr(w, "enable_serverless_compute", False)),
        "warehouse_type": enum_val(getattr(w, "warehouse_type", None)),
        "creator_name": getattr(w, "creator_name", None),
        "health_status": enum_val(getattr(health, "status", None)) if health else None,
        "health_message": getattr(health, "message", None) if health else None,
        "tags": custom_tags,
    }


class WarehouseWidget(anywidget.AnyWidget):
    """Operational widget for a single Databricks SQL warehouse."""

    _esm = traitlets.Unicode().tag(sync=True)
    _css = traitlets.Unicode("").tag(sync=True)

    warehouse_data = traitlets.Unicode("{}").tag(sync=True)
    action_result = traitlets.Unicode("").tag(sync=True)
    loading = traitlets.Bool(False).tag(sync=True)
    error_message = traitlets.Unicode("").tag(sync=True)
    request = traitlets.Unicode("").tag(sync=True)

    def __init__(
        self, warehouse_id: str, workspace_client: Any = None, refresh_seconds: int = 30, **kwargs: Any
    ) -> None:
        esm_content = _ESM_PATH.read_text()
        super().__init__(_esm=esm_content, **kwargs)
        self._ws = workspace_client
        self._warehouse_id = warehouse_id
        self._refresh_seconds = refresh_seconds
        self.observe(self._handle_request, names=["request"])
        self._load_warehouse()

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
        if action == "refresh":
            self._load_warehouse()
        elif action == "start":
            self._start()
        elif action == "stop":
            self._stop()

    def _load_warehouse(self) -> None:
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            wh = ws.warehouses.get(self._warehouse_id)
            data = _serialize_warehouse(wh)
            data["refresh_seconds"] = self._refresh_seconds
            self.warehouse_data = json.dumps(data)
        except Exception as exc:
            LOGGER.debug("Failed to get warehouse %s", self._warehouse_id, exc_info=True)
            self.error_message = f"Failed to get warehouse: {exc}"
        finally:
            self.loading = False

    def _start(self) -> None:
        self.action_result = ""
        try:
            ws = self._get_client()
            ws.warehouses.start(self._warehouse_id)
            self.action_result = json.dumps(
                {"action": "start", "success": True, "message": "Warehouse start requested."}
            )
            self._load_warehouse()
        except Exception as exc:
            self.action_result = json.dumps(
                {"action": "start", "success": False, "message": f"Failed to start: {exc}"}
            )

    def _stop(self) -> None:
        self.action_result = ""
        try:
            ws = self._get_client()
            ws.warehouses.stop(self._warehouse_id)
            self.action_result = json.dumps(
                {"action": "stop", "success": True, "message": "Warehouse stop requested."}
            )
            self._load_warehouse()
        except Exception as exc:
            self.action_result = json.dumps({"action": "stop", "success": False, "message": f"Failed to stop: {exc}"})
