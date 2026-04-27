"""Anywidget for browsing Databricks compute resources.

Displays clusters, SQL warehouses, vector search endpoints,
instance pools, and cluster policies in a unified tabbed interface.

Usage in a marimo notebook::

    from marimo_databricks_connect import compute_widget
    widget = compute_widget()
    widget  # display in cell output
"""

from __future__ import annotations

import json
import logging
import pathlib
import time
from typing import Any

import anywidget
import traitlets

LOGGER = logging.getLogger(__name__)

_ESM_PATH = pathlib.Path(__file__).parent / "_compute_frontend.js"


# --------------------------------------------------------------------------- #
# Helpers                                                                      #
# --------------------------------------------------------------------------- #


def _ms_to_iso(ms: int | None) -> str | None:
    if ms is None:
        return None
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ms / 1000))
    except Exception:
        return None


def _enum_val(obj: Any) -> str | None:
    if obj is None:
        return None
    return obj.value if hasattr(obj, "value") else str(obj)


def _autoscale_str(autoscale: Any) -> str | None:
    if autoscale is None:
        return None
    mn = getattr(autoscale, "min_workers", None)
    mx = getattr(autoscale, "max_workers", None)
    if mn is not None and mx is not None:
        return f"{mn}–{mx}"
    return None


# --------------------------------------------------------------------------- #
# Serializers                                                                  #
# --------------------------------------------------------------------------- #


def _serialize_cluster(c: Any) -> dict:
    autoscale = getattr(c, "autoscale", None)
    state = _enum_val(getattr(c, "state", None))
    workers = getattr(c, "num_workers", None)
    return {
        "cluster_id": getattr(c, "cluster_id", None),
        "cluster_name": getattr(c, "cluster_name", None),
        "state": state,
        "state_message": getattr(c, "state_message", None),
        "creator": getattr(c, "creator_user_name", None),
        "spark_version": getattr(c, "spark_version", None),
        "node_type_id": getattr(c, "node_type_id", None),
        "driver_node_type_id": getattr(c, "driver_node_type_id", None),
        "num_workers": workers,
        "autoscale": _autoscale_str(autoscale),
        "cluster_cores": getattr(c, "cluster_cores", None),
        "cluster_memory_mb": getattr(c, "cluster_memory_mb", None),
        "autotermination_minutes": getattr(c, "autotermination_minutes", None),
        "start_time": _ms_to_iso(getattr(c, "start_time", None)),
        "terminated_time": _ms_to_iso(getattr(c, "terminated_time", None)),
        "last_restarted_time": _ms_to_iso(getattr(c, "last_restarted_time", None)),
        "data_security_mode": _enum_val(getattr(c, "data_security_mode", None)),
        "single_user_name": getattr(c, "single_user_name", None),
        "cluster_source": _enum_val(getattr(c, "cluster_source", None)),
        "policy_id": getattr(c, "policy_id", None),
        "instance_pool_id": getattr(c, "instance_pool_id", None),
        "is_single_node": bool(getattr(c, "is_single_node", False)),
        "custom_tags": dict(getattr(c, "custom_tags", None) or {}),
        "runtime_engine": _enum_val(getattr(c, "runtime_engine", None)),
        "termination_reason": _serialize_termination(getattr(c, "termination_reason", None)),
    }


def _serialize_termination(reason: Any) -> str | None:
    if reason is None:
        return None
    code = _enum_val(getattr(reason, "code", None))
    params = getattr(reason, "parameters", None)
    msg = code or ""
    if params:
        inactivity = getattr(params, "inactivity_duration_min", None)
        if inactivity:
            msg += f" (inactive {inactivity}min)"
    return msg or None


def _serialize_warehouse(w: Any) -> dict:
    health = getattr(w, "health", None)
    tags = getattr(w, "tags", None)
    custom_tags = {}
    if tags:
        pairs = getattr(tags, "custom_tags", None) or []
        for t in pairs:
            k = getattr(t, "key", None)
            v = getattr(t, "value", None)
            if k:
                custom_tags[k] = v or ""
    return {
        "id": getattr(w, "id", None),
        "name": getattr(w, "name", None),
        "state": _enum_val(getattr(w, "state", None)),
        "cluster_size": getattr(w, "cluster_size", None),
        "min_num_clusters": getattr(w, "min_num_clusters", None),
        "max_num_clusters": getattr(w, "max_num_clusters", None),
        "num_clusters": getattr(w, "num_clusters", None),
        "num_active_sessions": getattr(w, "num_active_sessions", None),
        "auto_stop_mins": getattr(w, "auto_stop_mins", None),
        "enable_photon": bool(getattr(w, "enable_photon", False)),
        "enable_serverless_compute": bool(getattr(w, "enable_serverless_compute", False)),
        "warehouse_type": _enum_val(getattr(w, "warehouse_type", None)),
        "creator_name": getattr(w, "creator_name", None),
        "health_status": _enum_val(getattr(health, "status", None)) if health else None,
        "health_message": getattr(health, "message", None) if health else None,
        "tags": custom_tags,
    }


def _serialize_vs_endpoint(e: Any) -> dict:
    status = getattr(e, "endpoint_status", None)
    return {
        "id": getattr(e, "id", None),
        "name": getattr(e, "name", None),
        "state": _enum_val(getattr(status, "state", None)) if status else None,
        "state_message": getattr(status, "message", None) if status else None,
        "endpoint_type": _enum_val(getattr(e, "endpoint_type", None)),
        "num_indexes": getattr(e, "num_indexes", None),
        "creator": getattr(e, "creator", None),
        "creation_timestamp": _ms_to_iso(getattr(e, "creation_timestamp", None)),
        "last_updated_timestamp": _ms_to_iso(getattr(e, "last_updated_timestamp", None)),
        "last_updated_user": getattr(e, "last_updated_user", None),
    }


def _serialize_pool(p: Any) -> dict:
    stats = getattr(p, "stats", None)
    return {
        "instance_pool_id": getattr(p, "instance_pool_id", None),
        "instance_pool_name": getattr(p, "instance_pool_name", None),
        "state": _enum_val(getattr(p, "state", None)),
        "node_type_id": getattr(p, "node_type_id", None),
        "min_idle_instances": getattr(p, "min_idle_instances", None),
        "max_capacity": getattr(p, "max_capacity", None),
        "idle_instance_autotermination_minutes": getattr(p, "idle_instance_autotermination_minutes", None),
        "preloaded_spark_versions": getattr(p, "preloaded_spark_versions", None) or [],
        "idle_count": getattr(stats, "idle_count", None) if stats else None,
        "used_count": getattr(stats, "used_count", None) if stats else None,
        "pending_idle_count": getattr(stats, "pending_idle_count", None) if stats else None,
        "pending_used_count": getattr(stats, "pending_used_count", None) if stats else None,
        "custom_tags": dict(getattr(p, "custom_tags", None) or {}),
    }


def _serialize_policy(p: Any) -> dict:
    return {
        "policy_id": getattr(p, "policy_id", None),
        "name": getattr(p, "name", None),
        "description": getattr(p, "description", None),
        "creator": getattr(p, "creator_user_name", None),
        "is_default": bool(getattr(p, "is_default", False)),
        "max_clusters_per_user": getattr(p, "max_clusters_per_user", None),
        "policy_family_id": getattr(p, "policy_family_id", None),
        "created_at": _ms_to_iso(getattr(p, "created_at_timestamp", None)),
        "definition": getattr(p, "definition", None),
    }


# --------------------------------------------------------------------------- #
# Widget                                                                       #
# --------------------------------------------------------------------------- #


class ComputeWidget(anywidget.AnyWidget):
    """Anywidget for browsing Databricks compute resources."""

    _esm = traitlets.Unicode().tag(sync=True)
    _css = traitlets.Unicode("").tag(sync=True)

    # Data traits
    clusters_data = traitlets.Unicode("[]").tag(sync=True)
    warehouses_data = traitlets.Unicode("[]").tag(sync=True)
    vs_endpoints_data = traitlets.Unicode("[]").tag(sync=True)
    pools_data = traitlets.Unicode("[]").tag(sync=True)
    policies_data = traitlets.Unicode("[]").tag(sync=True)
    loading = traitlets.Bool(False).tag(sync=True)
    error_message = traitlets.Unicode("").tag(sync=True)

    # Request from frontend
    request = traitlets.Unicode("").tag(sync=True)

    def __init__(self, workspace_client: Any = None, **kwargs: Any) -> None:
        esm_content = _ESM_PATH.read_text()
        super().__init__(_esm=esm_content, **kwargs)
        self._ws = workspace_client
        self.observe(self._handle_request, names=["request"])
        # Load clusters by default
        self._load_clusters()

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
        if action == "list_clusters":
            self._load_clusters()
        elif action == "list_warehouses":
            self._load_warehouses()
        elif action == "list_vs_endpoints":
            self._load_vs_endpoints()
        elif action == "list_pools":
            self._load_pools()
        elif action == "list_policies":
            self._load_policies()

    def _load_clusters(self) -> None:
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            clusters = list(ws.clusters.list())
            self.clusters_data = json.dumps([_serialize_cluster(c) for c in clusters])
        except Exception as exc:
            LOGGER.debug("Failed to list clusters", exc_info=True)
            self.error_message = f"Failed to list clusters: {exc}"
        finally:
            self.loading = False

    def _load_warehouses(self) -> None:
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            warehouses = list(ws.warehouses.list())
            self.warehouses_data = json.dumps([_serialize_warehouse(w) for w in warehouses])
        except Exception as exc:
            LOGGER.debug("Failed to list warehouses", exc_info=True)
            self.error_message = f"Failed to list warehouses: {exc}"
        finally:
            self.loading = False

    def _load_vs_endpoints(self) -> None:
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            endpoints = list(ws.vector_search_endpoints.list_endpoints())
            self.vs_endpoints_data = json.dumps([_serialize_vs_endpoint(e) for e in endpoints])
        except Exception as exc:
            LOGGER.debug("Failed to list vector search endpoints", exc_info=True)
            self.error_message = f"Failed to list vector search endpoints: {exc}"
        finally:
            self.loading = False

    def _load_pools(self) -> None:
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            pools = list(ws.instance_pools.list())
            self.pools_data = json.dumps([_serialize_pool(p) for p in pools])
        except Exception as exc:
            LOGGER.debug("Failed to list instance pools", exc_info=True)
            self.error_message = f"Failed to list instance pools: {exc}"
        finally:
            self.loading = False

    def _load_policies(self) -> None:
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            policies = list(ws.cluster_policies.list())
            self.policies_data = json.dumps([_serialize_policy(p) for p in policies])
        except Exception as exc:
            LOGGER.debug("Failed to list cluster policies", exc_info=True)
            self.error_message = f"Failed to list cluster policies: {exc}"
        finally:
            self.loading = False
