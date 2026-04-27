"""Operational widget for a single Databricks model serving endpoint.

Usage::

    from marimo_databricks_connect import serving_endpoint_widget
    widget = serving_endpoint_widget("my-model-endpoint")
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

_ESM_PATH = pathlib.Path(__file__).parent / "_serving_endpoint_widget_frontend.js"


def _serialize_endpoint(ep: Any) -> dict:
    state = getattr(ep, "state", None)
    config = getattr(ep, "config", None)
    served_entities = []
    if config:
        for se in getattr(config, "served_entities", None) or getattr(config, "served_models", None) or []:
            served_entities.append(
                {
                    "name": getattr(se, "name", None) or getattr(se, "model_name", None),
                    "entity_name": getattr(se, "entity_name", None) or getattr(se, "model_name", None),
                    "entity_version": getattr(se, "entity_version", None) or getattr(se, "model_version", None),
                    "workload_size": getattr(se, "workload_size", None),
                    "scale_to_zero_enabled": getattr(se, "scale_to_zero_enabled", None),
                    "workload_type": getattr(se, "workload_type", None),
                    "state": enum_val(getattr(getattr(se, "state", None), "deployment", None))
                    if getattr(se, "state", None)
                    else None,
                }
            )
    traffic_config = None
    if config and getattr(config, "traffic_config", None):
        tc = config.traffic_config
        routes = []
        for r in getattr(tc, "routes", None) or []:
            routes.append(
                {
                    "served_model_name": getattr(r, "served_model_name", None),
                    "traffic_percentage": getattr(r, "traffic_percentage", None),
                }
            )
        traffic_config = {"routes": routes}
    pending_config = None
    pc = getattr(ep, "pending_config", None)
    if pc:
        pending_entities = []
        for se in getattr(pc, "served_entities", None) or getattr(pc, "served_models", None) or []:
            pending_entities.append(
                {
                    "name": getattr(se, "name", None) or getattr(se, "model_name", None),
                    "entity_name": getattr(se, "entity_name", None) or getattr(se, "model_name", None),
                    "entity_version": getattr(se, "entity_version", None) or getattr(se, "model_version", None),
                }
            )
        pending_config = {"entities": pending_entities}

    return {
        "name": getattr(ep, "name", None),
        "creator": getattr(ep, "creator", None),
        "creation_timestamp": ms_to_iso(getattr(ep, "creation_timestamp", None)),
        "last_updated_timestamp": ms_to_iso(getattr(ep, "last_updated_timestamp", None)),
        "state": enum_val(getattr(state, "ready", None)) if state else None,
        "config_update": enum_val(getattr(state, "config_update", None)) if state else None,
        "served_entities": served_entities,
        "traffic_config": traffic_config,
        "pending_config": pending_config,
        "permission_level": enum_val(getattr(ep, "permission_level", None)),
        "id": getattr(ep, "id", None),
    }


class ServingEndpointWidget(anywidget.AnyWidget):
    """Operational widget for a single model serving endpoint."""

    _esm = traitlets.Unicode().tag(sync=True)
    _css = traitlets.Unicode("").tag(sync=True)

    endpoint_data = traitlets.Unicode("{}").tag(sync=True)
    query_result = traitlets.Unicode("{}").tag(sync=True)
    action_result = traitlets.Unicode("").tag(sync=True)
    loading = traitlets.Bool(False).tag(sync=True)
    error_message = traitlets.Unicode("").tag(sync=True)
    request = traitlets.Unicode("").tag(sync=True)

    def __init__(
        self, endpoint_name: str, workspace_client: Any = None, refresh_seconds: int = 30, **kwargs: Any
    ) -> None:
        esm_content = _ESM_PATH.read_text()
        super().__init__(_esm=esm_content, **kwargs)
        self._ws = workspace_client
        self._endpoint_name = endpoint_name
        self._refresh_seconds = refresh_seconds
        self.observe(self._handle_request, names=["request"])
        self._load_endpoint()

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
            self._load_endpoint()
        elif action == "query":
            self._query_endpoint(req.get("payload", "{}"))

    def _load_endpoint(self) -> None:
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            ep = ws.serving_endpoints.get(self._endpoint_name)
            data = _serialize_endpoint(ep)
            data["refresh_seconds"] = self._refresh_seconds
            self.endpoint_data = json.dumps(data)
        except Exception as exc:
            LOGGER.debug("Failed to get endpoint %s", self._endpoint_name, exc_info=True)
            self.error_message = f"Failed to get endpoint: {exc}"
        finally:
            self.loading = False

    def _query_endpoint(self, payload_str: str) -> None:
        """Send a query to the serving endpoint."""
        self.error_message = ""
        try:
            payload = json.loads(payload_str)
            ws = self._get_client()
            resp = ws.serving_endpoints.query(name=self._endpoint_name, **payload)
            # resp could be various types; serialize to dict
            if hasattr(resp, "as_dict"):
                result = resp.as_dict()
            elif hasattr(resp, "__dict__"):
                result = {k: v for k, v in resp.__dict__.items() if not k.startswith("_")}
            else:
                result = {"response": str(resp)}
            self.query_result = json.dumps({"success": True, "data": result})
        except json.JSONDecodeError as exc:
            self.query_result = json.dumps({"success": False, "error": f"Invalid JSON payload: {exc}"})
        except Exception as exc:
            LOGGER.debug("Failed to query endpoint %s", self._endpoint_name, exc_info=True)
            self.query_result = json.dumps({"success": False, "error": f"Query failed: {exc}"})
