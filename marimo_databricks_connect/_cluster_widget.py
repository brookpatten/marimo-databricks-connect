"""Operational widget for a single Databricks cluster.

Displays cluster status, config, events. Supports start/stop/restart
and full configuration editing (node types, runtime, Spark conf,
environment variables, init scripts, security mode, etc.).

Usage::

    from marimo_databricks_connect import cluster_widget
    widget = cluster_widget(cluster_id="0123-456789-abcdef")
"""

from __future__ import annotations

import json
import logging
import pathlib
from typing import Any

import anywidget
import traitlets

from ._ops_common import duration_str, enum_val, ms_to_iso, safe_dict

LOGGER = logging.getLogger(__name__)

_ESM_PATH = pathlib.Path(__file__).parent / "_cluster_widget_frontend.js"


# --------------------------------------------------------------------------- #
# Serializers                                                                  #
# --------------------------------------------------------------------------- #


def _serialize_init_script(script: Any) -> dict:
    """Flatten an InitScriptInfo to {type, destination}."""
    for attr, stype in (
        ("workspace", "workspace"),
        ("volumes", "volumes"),
        ("dbfs", "dbfs"),
        ("abfss", "abfss"),
        ("file", "file"),
        ("s3", "s3"),
        ("gcs", "gcs"),
    ):
        info = getattr(script, attr, None)
        if info is not None:
            return {"type": stype, "destination": getattr(info, "destination", None)}
    return {"type": "unknown", "destination": None}


def _serialize_cluster(c: Any) -> dict:
    autoscale = getattr(c, "autoscale", None)
    autoscale_str = None
    autoscale_min = None
    autoscale_max = None
    if autoscale:
        autoscale_min = getattr(autoscale, "min_workers", None)
        autoscale_max = getattr(autoscale, "max_workers", None)
        if autoscale_min is not None and autoscale_max is not None:
            autoscale_str = f"{autoscale_min}–{autoscale_max}"

    state = enum_val(getattr(c, "state", None))
    term = getattr(c, "termination_reason", None)
    term_str = None
    if term:
        code = enum_val(getattr(term, "code", None))
        term_str = code or None

    init_scripts = []
    for s in getattr(c, "init_scripts", None) or []:
        init_scripts.append(_serialize_init_script(s))

    return {
        "cluster_id": getattr(c, "cluster_id", None),
        "cluster_name": getattr(c, "cluster_name", None),
        "state": state,
        "state_message": getattr(c, "state_message", None),
        "creator": getattr(c, "creator_user_name", None),
        "spark_version": getattr(c, "spark_version", None),
        "node_type_id": getattr(c, "node_type_id", None),
        "driver_node_type_id": getattr(c, "driver_node_type_id", None),
        "num_workers": getattr(c, "num_workers", None),
        "autoscale": autoscale_str,
        "autoscale_min": autoscale_min,
        "autoscale_max": autoscale_max,
        "cluster_cores": getattr(c, "cluster_cores", None),
        "cluster_memory_mb": getattr(c, "cluster_memory_mb", None),
        "autotermination_minutes": getattr(c, "autotermination_minutes", None),
        "start_time": ms_to_iso(getattr(c, "start_time", None)),
        "terminated_time": ms_to_iso(getattr(c, "terminated_time", None)),
        "last_restarted_time": ms_to_iso(getattr(c, "last_restarted_time", None)),
        "data_security_mode": enum_val(getattr(c, "data_security_mode", None)),
        "single_user_name": getattr(c, "single_user_name", None),
        "cluster_source": enum_val(getattr(c, "cluster_source", None)),
        "policy_id": getattr(c, "policy_id", None),
        "instance_pool_id": getattr(c, "instance_pool_id", None),
        "custom_tags": dict(getattr(c, "custom_tags", None) or {}),
        "runtime_engine": enum_val(getattr(c, "runtime_engine", None)),
        "termination_reason": term_str,
        "spark_conf": dict(getattr(c, "spark_conf", None) or {}),
        "spark_env_vars": dict(getattr(c, "spark_env_vars", None) or {}),
        "init_scripts": init_scripts,
        "is_single_node": bool(getattr(c, "is_single_node", False)),
        "enable_elastic_disk": bool(getattr(c, "enable_elastic_disk", False)),
        "enable_local_disk_encryption": bool(getattr(c, "enable_local_disk_encryption", False)),
    }


def _serialize_event(e: Any) -> dict:
    return {
        "type": enum_val(getattr(e, "type", None)),
        "timestamp": ms_to_iso(getattr(e, "timestamp", None)),
        "details": str(getattr(e, "details", None) or ""),
    }


# --------------------------------------------------------------------------- #
# Edit helper                                                                  #
# --------------------------------------------------------------------------- #


def _build_init_scripts(raw: list[dict]) -> list[Any]:
    """Convert [{type, destination}, …] dicts back to SDK InitScriptInfo objects."""
    from databricks.sdk.service.compute import (
        DbfsStorageInfo,
        InitScriptInfo,
        LocalFileInfo,
        VolumesStorageInfo,
        WorkspaceStorageInfo,
    )

    result = []
    for item in raw:
        dest = (item.get("destination") or "").strip()
        if not dest:
            continue
        stype = item.get("type", "").lower()
        if stype == "volumes":
            result.append(InitScriptInfo(volumes=VolumesStorageInfo(destination=dest)))
        elif stype == "workspace":
            result.append(InitScriptInfo(workspace=WorkspaceStorageInfo(destination=dest)))
        elif stype == "dbfs":
            result.append(InitScriptInfo(dbfs=DbfsStorageInfo(destination=dest)))
        elif stype == "file":
            result.append(InitScriptInfo(file=LocalFileInfo(destination=dest)))
        # abfss/s3/gcs require their own info objects; for simplicity
        # we fall back to the most common patterns above.
    return result


# --------------------------------------------------------------------------- #
# Widget                                                                       #
# --------------------------------------------------------------------------- #


class ClusterWidget(anywidget.AnyWidget):
    """Operational widget for a single Databricks cluster."""

    _esm = traitlets.Unicode().tag(sync=True)
    _css = traitlets.Unicode("").tag(sync=True)

    cluster_data = traitlets.Unicode("{}").tag(sync=True)
    events_data = traitlets.Unicode("[]").tag(sync=True)
    # Dropdown option lists for the edit form
    node_types_data = traitlets.Unicode("[]").tag(sync=True)
    spark_versions_data = traitlets.Unicode("[]").tag(sync=True)
    action_result = traitlets.Unicode("").tag(sync=True)
    loading = traitlets.Bool(False).tag(sync=True)
    error_message = traitlets.Unicode("").tag(sync=True)
    request = traitlets.Unicode("").tag(sync=True)

    def __init__(
        self,
        cluster_id: str,
        workspace_client: Any = None,
        refresh_seconds: int = 30,
        **kwargs: Any,
    ) -> None:
        esm_content = _ESM_PATH.read_text()
        super().__init__(_esm=esm_content, **kwargs)
        self._ws = workspace_client
        self._cluster_id = cluster_id
        self._refresh_seconds = refresh_seconds
        self.observe(self._handle_request, names=["request"])
        self._load_cluster()

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
            self._load_cluster()
        elif action == "get_events":
            self._load_events()
        elif action == "start":
            self._start_cluster()
        elif action == "stop":
            self._stop_cluster()
        elif action == "restart":
            self._restart_cluster()
        elif action == "get_edit_options":
            self._load_edit_options()
        elif action == "edit":
            self._edit_cluster(req.get("config", {}))

    # -- data loading -----------------------------------------------------

    def _load_cluster(self) -> None:
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            cluster = ws.clusters.get(self._cluster_id)
            data = _serialize_cluster(cluster)
            data["refresh_seconds"] = self._refresh_seconds
            self.cluster_data = json.dumps(data)
        except Exception as exc:
            LOGGER.debug("Failed to get cluster %s", self._cluster_id, exc_info=True)
            self.error_message = f"Failed to get cluster: {exc}"
        finally:
            self.loading = False

    def _load_events(self) -> None:
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            events = list(ws.clusters.events(cluster_id=self._cluster_id, limit=50))
            self.events_data = json.dumps([_serialize_event(e) for e in events])
        except Exception as exc:
            LOGGER.debug("Failed to get events for %s", self._cluster_id, exc_info=True)
            self.error_message = f"Failed to get events: {exc}"
        finally:
            self.loading = False

    def _load_edit_options(self) -> None:
        """Fetch node types and Spark versions for the edit form dropdowns."""
        self.error_message = ""
        try:
            ws = self._get_client()
            # Node types
            nt_resp = ws.clusters.list_node_types()
            node_types = []
            for nt in getattr(nt_resp, "node_types", None) or []:
                node_types.append(
                    {
                        "node_type_id": getattr(nt, "node_type_id", None),
                        "description": getattr(nt, "description", None),
                        "memory_mb": getattr(nt, "memory_mb", None),
                        "num_cores": getattr(nt, "num_cores", None),
                        "num_gpus": getattr(nt, "num_gpus", 0) or 0,
                        "is_deprecated": bool(getattr(nt, "is_deprecated", False)),
                    }
                )
            # Sort: non-deprecated first, then by description
            node_types.sort(key=lambda x: (x["is_deprecated"], x["description"] or ""))
            self.node_types_data = json.dumps(node_types)

            # Spark versions
            sv_resp = ws.clusters.spark_versions()
            spark_versions = []
            for sv in getattr(sv_resp, "versions", None) or []:
                spark_versions.append(
                    {
                        "key": getattr(sv, "key", None),
                        "name": getattr(sv, "name", None),
                    }
                )
            self.spark_versions_data = json.dumps(spark_versions)
        except Exception as exc:
            LOGGER.debug("Failed to load edit options", exc_info=True)
            self.error_message = f"Failed to load edit options: {exc}"

    # -- actions ----------------------------------------------------------

    def _start_cluster(self) -> None:
        self.action_result = ""
        try:
            ws = self._get_client()
            ws.clusters.start(self._cluster_id)
            self.action_result = json.dumps(
                {"action": "start", "success": True, "message": "Cluster start requested."}
            )
            self._load_cluster()
        except Exception as exc:
            self.action_result = json.dumps(
                {"action": "start", "success": False, "message": f"Failed to start: {exc}"}
            )

    def _stop_cluster(self) -> None:
        self.action_result = ""
        try:
            ws = self._get_client()
            ws.clusters.delete(self._cluster_id)
            self.action_result = json.dumps({"action": "stop", "success": True, "message": "Cluster stop requested."})
            self._load_cluster()
        except Exception as exc:
            self.action_result = json.dumps({"action": "stop", "success": False, "message": f"Failed to stop: {exc}"})

    def _restart_cluster(self) -> None:
        self.action_result = ""
        try:
            ws = self._get_client()
            ws.clusters.restart(self._cluster_id)
            self.action_result = json.dumps(
                {"action": "restart", "success": True, "message": "Cluster restart requested."}
            )
            self._load_cluster()
        except Exception as exc:
            self.action_result = json.dumps(
                {"action": "restart", "success": False, "message": f"Failed to restart: {exc}"}
            )

    def _edit_cluster(self, config: dict) -> None:
        """Apply a configuration change to the cluster.

        *config* is a dict of only the fields the user changed.  We read
        the current cluster state and merge changes on top so that the
        ``clusters.edit`` call always receives the required ``spark_version``
        field alongside any optional updates.
        """
        self.action_result = ""
        self.error_message = ""
        try:
            ws = self._get_client()

            # Read current state as the baseline
            current = ws.clusters.get(self._cluster_id)
            cur_settings = getattr(current, "settings", current)  # compat

            # Required fields — always forwarded from current state
            spark_version = config.get(
                "spark_version",
                getattr(cur_settings, "spark_version", None) or getattr(current, "spark_version", None),
            )

            kwargs: dict[str, Any] = {
                "cluster_id": self._cluster_id,
                "spark_version": spark_version,
            }

            # ---- Simple scalar fields ----
            _SIMPLE = {
                "cluster_name": str,
                "node_type_id": str,
                "driver_node_type_id": str,
                "num_workers": int,
                "autotermination_minutes": int,
                "single_user_name": str,
                "instance_pool_id": str,
                "driver_instance_pool_id": str,
                "policy_id": str,
                "is_single_node": bool,
                "enable_elastic_disk": bool,
                "enable_local_disk_encryption": bool,
            }
            for key, typ in _SIMPLE.items():
                if key in config:
                    val = config[key]
                    if val is None or val == "":
                        kwargs[key] = None
                    else:
                        kwargs[key] = typ(val)

            # ---- Autoscale ----
            if "autoscale_min" in config and "autoscale_max" in config:
                from databricks.sdk.service.compute import AutoScale

                mn = config["autoscale_min"]
                mx = config["autoscale_max"]
                if mn is not None and mx is not None:
                    kwargs["autoscale"] = AutoScale(min_workers=int(mn), max_workers=int(mx))
                    kwargs.pop("num_workers", None)  # mutually exclusive
            elif "num_workers" in config:
                kwargs.pop("autoscale", None)

            # ---- Data security mode ----
            if "data_security_mode" in config:
                from databricks.sdk.service.compute import DataSecurityMode

                val = config["data_security_mode"]
                if val:
                    kwargs["data_security_mode"] = DataSecurityMode(val)

            # ---- Runtime engine (Photon) ----
            if "runtime_engine" in config:
                from databricks.sdk.service.compute import RuntimeEngine

                val = config["runtime_engine"]
                if val:
                    kwargs["runtime_engine"] = RuntimeEngine(val)

            # ---- Dict fields ----
            if "spark_conf" in config:
                kwargs["spark_conf"] = config["spark_conf"] or {}
            if "spark_env_vars" in config:
                kwargs["spark_env_vars"] = config["spark_env_vars"] or {}
            if "custom_tags" in config:
                kwargs["custom_tags"] = config["custom_tags"] or {}

            # ---- Init scripts ----
            if "init_scripts" in config:
                kwargs["init_scripts"] = _build_init_scripts(config["init_scripts"] or [])

            ws.clusters.edit(**kwargs)

            self.action_result = json.dumps(
                {"action": "edit", "success": True, "message": "Cluster configuration updated."}
            )
            self._load_cluster()

        except Exception as exc:
            LOGGER.debug("Failed to edit cluster %s", self._cluster_id, exc_info=True)
            self.action_result = json.dumps(
                {
                    "action": "edit",
                    "success": False,
                    "message": f"Failed to update cluster: {exc}",
                }
            )
