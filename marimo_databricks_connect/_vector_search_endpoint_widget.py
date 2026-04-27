"""Operational widget for a single Databricks Vector Search endpoint.

Shows endpoint details, status, scaling, indices hosted on it, and metrics.

Usage::

    from marimo_databricks_connect import vector_search_endpoint_widget
    widget = vector_search_endpoint_widget("my-vs-endpoint")
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

_ESM_PATH = pathlib.Path(__file__).parent / "_vector_search_endpoint_widget_frontend.js"


def _serialize_endpoint(ep: Any) -> dict:
    status = getattr(ep, "endpoint_status", None)
    scaling = getattr(ep, "scaling_info", None)
    custom_tags = []
    for t in getattr(ep, "custom_tags", None) or []:
        custom_tags.append({"key": getattr(t, "key", ""), "value": getattr(t, "value", "")})
    return {
        "name": getattr(ep, "name", None),
        "id": getattr(ep, "id", None),
        "endpoint_type": enum_val(getattr(ep, "endpoint_type", None)),
        "state": enum_val(getattr(status, "state", None)) if status else None,
        "state_message": getattr(status, "message", None) if status else None,
        "num_indexes": getattr(ep, "num_indexes", None),
        "creator": getattr(ep, "creator", None),
        "created_at": ms_to_iso(getattr(ep, "creation_timestamp", None)),
        "last_updated": ms_to_iso(getattr(ep, "last_updated_timestamp", None)),
        "last_updated_user": getattr(ep, "last_updated_user", None),
        "scaling_requested_min_qps": getattr(scaling, "requested_min_qps", None) if scaling else None,
        "scaling_state": enum_val(getattr(scaling, "state", None)) if scaling else None,
        "budget_policy_id": getattr(ep, "budget_policy_id", None),
        "custom_tags": custom_tags,
    }


def _serialize_mini_index(idx: Any) -> dict:
    return {
        "name": getattr(idx, "name", None),
        "endpoint_name": getattr(idx, "endpoint_name", None),
        "index_type": enum_val(getattr(idx, "index_type", None)),
        "index_subtype": enum_val(getattr(idx, "index_subtype", None)),
        "primary_key": getattr(idx, "primary_key", None),
        "creator": getattr(idx, "creator", None),
    }


def _serialize_metrics(resp: Any) -> list[dict]:
    result = []
    for mv in getattr(resp, "metric_values", None) or []:
        metric = getattr(mv, "metric", None)
        metric_name = getattr(metric, "name", None) if metric else None
        labels = []
        for lb in getattr(metric, "labels", None) or []:
            labels.append({"key": getattr(lb, "key", ""), "value": getattr(lb, "value", "")})
        values = []
        for v in getattr(mv, "values", None) or []:
            values.append({"timestamp": ms_to_iso(getattr(v, "timestamp", None)), "value": getattr(v, "value", None)})
        result.append({"name": metric_name, "labels": labels, "values": values})
    return result


class VectorSearchEndpointWidget(anywidget.AnyWidget):
    """Operational widget for a single Vector Search endpoint."""

    _esm = traitlets.Unicode().tag(sync=True)
    _css = traitlets.Unicode("").tag(sync=True)

    endpoint_data = traitlets.Unicode("{}").tag(sync=True)
    indexes_data = traitlets.Unicode("[]").tag(sync=True)
    metrics_data = traitlets.Unicode("[]").tag(sync=True)
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
        elif action == "list_indexes":
            self._load_indexes()
        elif action == "get_metrics":
            self._load_metrics()

    def _load_endpoint(self) -> None:
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            ep = ws.vector_search_endpoints.get_endpoint(self._endpoint_name)
            data = _serialize_endpoint(ep)
            data["refresh_seconds"] = self._refresh_seconds
            self.endpoint_data = json.dumps(data)
        except Exception as exc:
            LOGGER.debug("Failed to get VS endpoint %s", self._endpoint_name, exc_info=True)
            self.error_message = f"Failed to get endpoint: {exc}"
        finally:
            self.loading = False

    def _load_indexes(self) -> None:
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            indexes = list(ws.vector_search_indexes.list_indexes(endpoint_name=self._endpoint_name))
            self.indexes_data = json.dumps([_serialize_mini_index(i) for i in indexes])
        except Exception as exc:
            LOGGER.debug("Failed to list indexes for %s", self._endpoint_name, exc_info=True)
            self.error_message = f"Failed to list indexes: {exc}"
        finally:
            self.loading = False

    def _load_metrics(self) -> None:
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            resp = ws.vector_search_endpoints.retrieve_user_visible_metrics(name=self._endpoint_name)
            self.metrics_data = json.dumps(_serialize_metrics(resp))
        except Exception as exc:
            LOGGER.debug("Failed to get metrics for %s", self._endpoint_name, exc_info=True)
            self.error_message = f"Failed to get metrics: {exc}"
        finally:
            self.loading = False
