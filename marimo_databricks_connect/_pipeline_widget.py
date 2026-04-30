"""Operational widget for a single Databricks Lakeflow Declarative Pipeline (DLT).

Displays pipeline status, configuration, recent updates, and the pipeline event
log.  Supports starting an update (with optional full refresh / refresh
selection / validate-only), stopping the pipeline, and triggering a refresh of
the displayed state.  Auto-refreshes periodically.

Usage::

    from marimo_databricks_connect import pipeline_widget
    widget = pipeline_widget(pipeline_id="abc-123-def-456")
    widget  # display in cell

    # Or by name (resolved against ``pipelines.list_pipelines``):
    widget = pipeline_widget(pipeline_name="bronze_etl")
"""

from __future__ import annotations

import json
import logging
import pathlib
import time
from typing import Any

import anywidget
import traitlets

from ._ops_common import enum_val, ms_to_iso, safe_dict

# Allowed values for Notifications.alerts (per Databricks REST docs).
_ALERT_OPTIONS = (
    "on-update-success",
    "on-update-failure",
    "on-update-fatal-failure",
    "on-flow-failure",
)

LOGGER = logging.getLogger(__name__)

_ESM_PATH = pathlib.Path(__file__).parent / "_pipeline_widget_frontend.js"


# --------------------------------------------------------------------------- #
# Serializers                                                                  #
# --------------------------------------------------------------------------- #


def _serialize_library(lib: Any) -> dict:
    """Flatten a PipelineLibrary into a {type, value} dict."""
    nb = getattr(lib, "notebook", None)
    if nb is not None:
        return {"type": "notebook", "value": getattr(nb, "path", None)}
    fil = getattr(lib, "file", None)
    if fil is not None:
        return {"type": "file", "value": getattr(fil, "path", None)}
    glob = getattr(lib, "glob", None)
    if glob is not None:
        return {"type": "glob", "value": getattr(glob, "include", None)}
    jar = getattr(lib, "jar", None)
    if jar is not None:
        return {"type": "jar", "value": jar}
    whl = getattr(lib, "whl", None)
    if whl is not None:
        return {"type": "whl", "value": whl}
    maven = getattr(lib, "maven", None)
    if maven is not None:
        return {"type": "maven", "value": getattr(maven, "coordinates", None)}
    return {"type": "unknown", "value": None}


def _serialize_notification(n: Any) -> dict:
    return {
        "alerts": list(getattr(n, "alerts", None) or []),
        "email_recipients": list(getattr(n, "email_recipients", None) or []),
    }


def _serialize_latest_update(u: Any) -> dict:
    """Serialize the lightweight UpdateStateInfo objects on PipelineStateInfo."""
    return {
        "update_id": getattr(u, "update_id", None),
        "state": enum_val(getattr(u, "state", None)),
        "creation_time": ms_to_iso(getattr(u, "creation_time", None))
        if isinstance(getattr(u, "creation_time", None), (int, float))
        else getattr(u, "creation_time", None),
    }


def _serialize_update(u: Any) -> dict:
    """Serialize a full UpdateInfo object (from list_updates / get_update)."""
    return {
        "update_id": getattr(u, "update_id", None),
        "state": enum_val(getattr(u, "state", None)),
        "creation_time": ms_to_iso(getattr(u, "creation_time", None))
        if isinstance(getattr(u, "creation_time", None), (int, float))
        else getattr(u, "creation_time", None),
        "cause": enum_val(getattr(u, "cause", None)),
        "cluster_id": getattr(u, "cluster_id", None),
        "full_refresh": bool(getattr(u, "full_refresh", False)),
        "full_refresh_selection": list(getattr(u, "full_refresh_selection", None) or []),
        "refresh_selection": list(getattr(u, "refresh_selection", None) or []),
        "validate_only": bool(getattr(u, "validate_only", False)),
    }


def _serialize_event(e: Any) -> dict:
    origin = getattr(e, "origin", None)
    err = getattr(e, "error", None)
    err_msgs: list[str] = []
    if err is not None:
        for ex in getattr(err, "exceptions", None) or []:
            msg = getattr(ex, "message", None)
            if msg:
                err_msgs.append(msg)
    return {
        "id": getattr(e, "id", None),
        "timestamp": getattr(e, "timestamp", None),  # already an ISO string for events
        "level": enum_val(getattr(e, "level", None)),
        "event_type": getattr(e, "event_type", None),
        "maturity_level": enum_val(getattr(e, "maturity_level", None)),
        "message": getattr(e, "message", None),
        "update_id": getattr(origin, "update_id", None) if origin else None,
        "flow_name": getattr(origin, "flow_name", None) if origin else None,
        "dataset_name": getattr(origin, "dataset_name", None) if origin else None,
        "error": "\n".join(err_msgs) if err_msgs else None,
    }


def _serialize_pipeline_cluster(c: Any) -> dict:
    """Lightweight projection of PipelineCluster for display + round-trip editing.

    Only the most commonly tweaked fields are surfaced; the full object can be
    edited through the JSON cluster editor in the Settings tab, which feeds
    straight into the SDK ``PipelineCluster`` constructor.
    """
    autoscale = getattr(c, "autoscale", None)
    return {
        "label": getattr(c, "label", None),
        "node_type_id": getattr(c, "node_type_id", None),
        "driver_node_type_id": getattr(c, "driver_node_type_id", None),
        "num_workers": getattr(c, "num_workers", None),
        "autoscale": (
            {
                "min_workers": getattr(autoscale, "min_workers", None),
                "max_workers": getattr(autoscale, "max_workers", None),
                "mode": enum_val(getattr(autoscale, "mode", None)),
            }
            if autoscale
            else None
        ),
        "instance_pool_id": getattr(c, "instance_pool_id", None),
        "driver_instance_pool_id": getattr(c, "driver_instance_pool_id", None),
        "policy_id": getattr(c, "policy_id", None),
        "apply_policy_default_values": getattr(c, "apply_policy_default_values", None),
        "custom_tags": safe_dict(getattr(c, "custom_tags", None)),
        "spark_conf": safe_dict(getattr(c, "spark_conf", None)),
        "spark_env_vars": safe_dict(getattr(c, "spark_env_vars", None)),
    }


def _serialize_permissions(perms: Any) -> dict:
    """Serialize an ObjectPermissions response into a JSON-safe shape."""
    acl = []
    for entry in getattr(perms, "access_control_list", None) or []:
        principal = (
            getattr(entry, "user_name", None)
            or getattr(entry, "group_name", None)
            or getattr(entry, "service_principal_name", None)
        )
        ptype = (
            "user"
            if getattr(entry, "user_name", None)
            else ("group" if getattr(entry, "group_name", None) else "service_principal")
        )
        all_perms = []
        for p in getattr(entry, "all_permissions", None) or []:
            all_perms.append(
                {
                    "level": enum_val(getattr(p, "permission_level", None)),
                    "inherited": bool(getattr(p, "inherited", False)),
                }
            )
        acl.append(
            {
                "principal": principal,
                "display_name": getattr(entry, "display_name", None),
                "type": ptype,
                "permissions": all_perms,
            }
        )
    return {
        "object_id": getattr(perms, "object_id", None),
        "object_type": getattr(perms, "object_type", None),
        "acl": acl,
    }


def _serialize_spec(spec: Any) -> dict:
    """Serialize the PipelineSpec into a JSON-safe dict (subset of fields)."""
    if spec is None:
        return {}
    libraries = [_serialize_library(lib) for lib in (getattr(spec, "libraries", None) or [])]
    notifications = [_serialize_notification(n) for n in (getattr(spec, "notifications", None) or [])]
    clusters = [_serialize_pipeline_cluster(c) for c in (getattr(spec, "clusters", None) or [])]
    return {
        "clusters": clusters,
        "catalog": getattr(spec, "catalog", None),
        "schema": getattr(spec, "schema", None),
        "target": getattr(spec, "target", None),
        "channel": getattr(spec, "channel", None),
        "edition": getattr(spec, "edition", None),
        "continuous": bool(getattr(spec, "continuous", False)),
        "development": bool(getattr(spec, "development", False)),
        "photon": bool(getattr(spec, "photon", False)),
        "serverless": bool(getattr(spec, "serverless", False)),
        "storage": getattr(spec, "storage", None),
        "root_path": getattr(spec, "root_path", None),
        "budget_policy_id": getattr(spec, "budget_policy_id", None),
        "configuration": safe_dict(getattr(spec, "configuration", None)),
        "tags": safe_dict(getattr(spec, "tags", None)),
        "libraries": libraries,
        "notifications": notifications,
    }


def _serialize_pipeline(p: Any) -> dict:
    """Serialize a GetPipelineResponse (preferred) or PipelineStateInfo."""
    spec = getattr(p, "spec", None)
    latest = [_serialize_latest_update(u) for u in (getattr(p, "latest_updates", None) or [])]
    health = enum_val(getattr(p, "health", None))
    return {
        "pipeline_id": getattr(p, "pipeline_id", None),
        "name": getattr(p, "name", None),
        "state": enum_val(getattr(p, "state", None)),
        "health": health,
        "creator": getattr(p, "creator_user_name", None),
        "run_as": getattr(p, "run_as_user_name", None),
        "cluster_id": getattr(p, "cluster_id", None),
        "last_modified": ms_to_iso(getattr(p, "last_modified", None))
        if isinstance(getattr(p, "last_modified", None), (int, float))
        else getattr(p, "last_modified", None),
        "cause": getattr(p, "cause", None),
        "effective_publishing_mode": enum_val(getattr(p, "effective_publishing_mode", None)),
        "latest_updates": latest,
        "spec": _serialize_spec(spec),
    }


# --------------------------------------------------------------------------- #
# Widget                                                                       #
# --------------------------------------------------------------------------- #


class PipelineWidget(anywidget.AnyWidget):
    """Operational widget for a single Lakeflow Declarative Pipeline (DLT)."""

    _esm = traitlets.Unicode().tag(sync=True)
    _css = traitlets.Unicode("").tag(sync=True)

    pipeline_data = traitlets.Unicode("{}").tag(sync=True)
    updates_data = traitlets.Unicode("[]").tag(sync=True)
    events_data = traitlets.Unicode("[]").tag(sync=True)
    update_detail = traitlets.Unicode("{}").tag(sync=True)
    permissions_data = traitlets.Unicode("{}").tag(sync=True)
    graph_data = traitlets.Unicode("{}").tag(sync=True)
    action_result = traitlets.Unicode("").tag(sync=True)
    loading = traitlets.Bool(False).tag(sync=True)
    error_message = traitlets.Unicode("").tag(sync=True)
    request = traitlets.Unicode("").tag(sync=True)

    def __init__(
        self,
        pipeline_id: str | None = None,
        pipeline_name: str | None = None,
        workspace_client: Any = None,
        refresh_seconds: int = 30,
        **kwargs: Any,
    ) -> None:
        esm_content = _ESM_PATH.read_text()
        super().__init__(_esm=esm_content, **kwargs)
        self._ws = workspace_client
        self._pipeline_id = pipeline_id
        self._pipeline_name = pipeline_name
        self._refresh_seconds = refresh_seconds
        # Cached SDK objects for partial-update round-trips.
        self._raw_spec: Any = None
        self._last_modified: int | None = None
        self.observe(self._handle_request, names=["request"])
        self._resolve_and_load()

    # -- client / request dispatch ---------------------------------------

    def _get_client(self) -> Any:
        if self._ws is not None:
            return self._ws
        from databricks.sdk import WorkspaceClient

        self._ws = WorkspaceClient()
        return self._ws

    def _resolve_and_load(self) -> None:
        if self._pipeline_id is None and self._pipeline_name:
            self.loading = True
            self.error_message = ""
            try:
                ws = self._get_client()
                # filter syntax used by the SDK is `name LIKE '%foo%'`
                escaped = self._pipeline_name.replace("'", "''")
                candidates = list(ws.pipelines.list_pipelines(filter=f"name LIKE '%{escaped}%'", max_results=50))
                exact = [p for p in candidates if getattr(p, "name", None) == self._pipeline_name]
                pick = exact[0] if exact else (candidates[0] if candidates else None)
                if pick is None:
                    self.error_message = f"Pipeline '{self._pipeline_name}' not found"
                    self.loading = False
                    return
                self._pipeline_id = getattr(pick, "pipeline_id", None)
            except Exception as exc:
                LOGGER.debug("Failed to resolve pipeline name %s", self._pipeline_name, exc_info=True)
                self.error_message = f"Failed to find pipeline: {exc}"
                self.loading = False
                return
        if self._pipeline_id is None:
            self.error_message = "No pipeline_id or pipeline_name provided"
            return
        self._load_pipeline()
        self._load_updates()

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
            self._load_pipeline()
            self._load_updates()
        elif action == "get_updates":
            self._load_updates()
        elif action == "get_events":
            self._load_events(filter_text=req.get("filter"))
        elif action == "get_update_detail":
            self._load_update_detail(req["update_id"])
        elif action == "start_update":
            self._start_update(
                full_refresh=bool(req.get("full_refresh", False)),
                full_refresh_selection=req.get("full_refresh_selection") or None,
                refresh_selection=req.get("refresh_selection") or None,
                validate_only=bool(req.get("validate_only", False)),
            )
        elif action == "stop":
            self._stop()
        elif action == "get_permissions":
            self._load_permissions()
        elif action == "update_permissions":
            self._update_permissions(req.get("acl", []))
        elif action == "get_graph":
            self._load_graph(update_id=req.get("update_id"))
        elif action == "update_settings":
            self._update_settings(req.get("settings") or {})

    # -- loaders ----------------------------------------------------------

    def _load_pipeline(self) -> None:
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            p = ws.pipelines.get(self._pipeline_id)
            # Cache the raw spec + last-modified token for partial-update round-trips.
            self._raw_spec = getattr(p, "spec", None)
            lm = getattr(p, "last_modified", None)
            self._last_modified = int(lm) if isinstance(lm, (int, float)) else None
            data = _serialize_pipeline(p)
            data["refresh_seconds"] = self._refresh_seconds
            self.pipeline_data = json.dumps(data)
        except Exception as exc:
            LOGGER.debug("Failed to get pipeline %s", self._pipeline_id, exc_info=True)
            self.error_message = f"Failed to get pipeline: {exc}"
        finally:
            self.loading = False

    def _load_updates(self) -> None:
        self.error_message = ""
        try:
            ws = self._get_client()
            resp = ws.pipelines.list_updates(self._pipeline_id, max_results=25)
            updates = list(getattr(resp, "updates", None) or [])
            self.updates_data = json.dumps([_serialize_update(u) for u in updates])
        except Exception as exc:
            LOGGER.debug("Failed to list updates for %s", self._pipeline_id, exc_info=True)
            self.error_message = f"Failed to list updates: {exc}"

    def _load_events(self, filter_text: str | None = None) -> None:
        self.error_message = ""
        try:
            ws = self._get_client()
            kwargs: dict[str, Any] = {"max_results": 100, "order_by": ["timestamp desc"]}
            if filter_text:
                kwargs["filter"] = filter_text
            events = list(ws.pipelines.list_pipeline_events(self._pipeline_id, **kwargs))
            self.events_data = json.dumps([_serialize_event(e) for e in events])
        except Exception as exc:
            LOGGER.debug("Failed to list events for %s", self._pipeline_id, exc_info=True)
            self.error_message = f"Failed to list events: {exc}"

    def _load_update_detail(self, update_id: str) -> None:
        self.error_message = ""
        try:
            ws = self._get_client()
            resp = ws.pipelines.get_update(self._pipeline_id, update_id)
            update = getattr(resp, "update", None) or resp
            self.update_detail = json.dumps(_serialize_update(update))
        except Exception as exc:
            LOGGER.debug("Failed to get update %s", update_id, exc_info=True)
            self.error_message = f"Failed to get update: {exc}"

    # -- actions ----------------------------------------------------------

    def _start_update(
        self,
        full_refresh: bool = False,
        full_refresh_selection: list[str] | None = None,
        refresh_selection: list[str] | None = None,
        validate_only: bool = False,
    ) -> None:
        self.action_result = ""
        try:
            ws = self._get_client()
            kwargs: dict[str, Any] = {}
            if full_refresh:
                kwargs["full_refresh"] = True
            if full_refresh_selection:
                kwargs["full_refresh_selection"] = list(full_refresh_selection)
            if refresh_selection:
                kwargs["refresh_selection"] = list(refresh_selection)
            if validate_only:
                kwargs["validate_only"] = True
            resp = ws.pipelines.start_update(self._pipeline_id, **kwargs)
            update_id = getattr(resp, "update_id", None)
            label = "validation" if validate_only else ("full refresh" if full_refresh else "update")
            msg = f"Pipeline {label} started."
            if update_id:
                msg += f" Update ID: {update_id}"
            self.action_result = json.dumps(
                {"action": "start_update", "success": True, "message": msg, "update_id": update_id}
            )
            time.sleep(0.5)
            self._load_pipeline()
            self._load_updates()
        except Exception as exc:
            LOGGER.debug("Failed to start update for %s", self._pipeline_id, exc_info=True)
            self.action_result = json.dumps(
                {
                    "action": "start_update",
                    "success": False,
                    "message": f"Failed to start update: {exc}",
                }
            )

    def _stop(self) -> None:
        self.action_result = ""
        try:
            ws = self._get_client()
            ws.pipelines.stop(self._pipeline_id)
            self.action_result = json.dumps({"action": "stop", "success": True, "message": "Pipeline stop requested."})
            time.sleep(0.5)
            self._load_pipeline()
            self._load_updates()
        except Exception as exc:
            LOGGER.debug("Failed to stop pipeline %s", self._pipeline_id, exc_info=True)
            self.action_result = json.dumps({"action": "stop", "success": False, "message": f"Failed to stop: {exc}"})

    # -- permissions ------------------------------------------------------

    def _load_permissions(self) -> None:
        self.error_message = ""
        try:
            ws = self._get_client()
            perms = ws.pipelines.get_permissions(self._pipeline_id)
            self.permissions_data = json.dumps(_serialize_permissions(perms))
        except Exception as exc:
            LOGGER.debug("Failed to get permissions for %s", self._pipeline_id, exc_info=True)
            self.error_message = f"Failed to get permissions: {exc}"

    def _update_permissions(self, acl: list[dict]) -> None:
        self.action_result = ""
        try:
            from databricks.sdk.service.iam import (
                AccessControlRequest,
                PermissionLevel,
            )

            requests = []
            for entry in acl:
                kwargs: dict[str, Any] = {}
                if entry.get("user_name"):
                    kwargs["user_name"] = entry["user_name"]
                elif entry.get("group_name"):
                    kwargs["group_name"] = entry["group_name"]
                elif entry.get("service_principal_name"):
                    kwargs["service_principal_name"] = entry["service_principal_name"]
                else:
                    continue
                level = entry.get("permission_level", "CAN_VIEW")
                kwargs["permission_level"] = PermissionLevel(level)
                requests.append(AccessControlRequest(**kwargs))

            ws = self._get_client()
            ws.pipelines.set_permissions(self._pipeline_id, access_control_list=requests)
            self.action_result = json.dumps(
                {"action": "update_permissions", "success": True, "message": "Permissions updated."}
            )
            self._load_permissions()
        except Exception as exc:
            LOGGER.debug("Failed to update permissions for %s", self._pipeline_id, exc_info=True)
            self.action_result = json.dumps(
                {
                    "action": "update_permissions",
                    "success": False,
                    "message": f"Failed to update permissions: {exc}",
                }
            )

    # -- dataset graph ----------------------------------------------------

    def _load_graph(self, update_id: str | None = None) -> None:
        """Build a node/edge graph from ``flow_definition`` events.

        The Databricks SDK's typed ``PipelineEvent`` strips the ``details``
        field, so we hit the raw REST endpoint to recover
        ``details.flow_definition.{output_dataset, input_datasets[]}`` which
        encodes the dataset DAG.  When an ``update_id`` is supplied we
        restrict to flow_definitions emitted during that update; otherwise we
        take the most recent definition per output dataset across all events.
        """
        self.error_message = ""
        try:
            ws = self._get_client()
            query: dict[str, Any] = {
                "max_results": 250,
                "filter": "event_type='flow_definition'",
                "order_by": "timestamp desc",
            }
            if update_id:
                # SDK filter syntax supports AND/OR combinations on origin fields.
                query["filter"] = f"event_type='flow_definition' AND update_id='{update_id}'"
            resp = ws.api_client.do(
                "GET",
                f"/api/2.0/pipelines/{self._pipeline_id}/events",
                query=query,
            )
            events = resp.get("events") or []

            # Most-recent-wins per dataset: events come back newest-first.
            seen: dict[str, dict] = {}
            for ev in events:
                details = (ev.get("details") or {}).get("flow_definition") or {}
                output = details.get("output_dataset")
                if not output or output in seen:
                    continue
                inputs = []
                for src in details.get("input_datasets") or []:
                    if isinstance(src, dict):
                        name = src.get("name") or src.get("table_name") or src.get("path")
                    else:
                        name = str(src)
                    if name:
                        inputs.append(name)
                origin = ev.get("origin") or {}
                seen[output] = {
                    "name": output,
                    "type": details.get("flow_type") or details.get("type"),
                    "comment": details.get("comment"),
                    "inputs": inputs,
                    "flow_id": origin.get("flow_id"),
                    "update_id": origin.get("update_id"),
                }

            nodes = list(seen.values())
            # Add upstream-only nodes (sources that aren't themselves DLT flows).
            known = {n["name"] for n in nodes}
            for n in list(nodes):
                for src in n["inputs"]:
                    if src not in known:
                        known.add(src)
                        nodes.append({"name": src, "type": "source", "inputs": [], "external": True})

            edges = []
            for n in nodes:
                for src in n.get("inputs") or []:
                    edges.append({"from": src, "to": n["name"]})

            self.graph_data = json.dumps(
                {
                    "nodes": nodes,
                    "edges": edges,
                    "update_id": update_id,
                    "event_count": len(events),
                }
            )
        except Exception as exc:
            LOGGER.debug("Failed to load graph for %s", self._pipeline_id, exc_info=True)
            self.error_message = f"Failed to load dataset graph: {exc}"

    # -- settings (notifications / clusters / config / channel / dev) ----

    def _build_notifications(self, items: list[dict]) -> list:
        from databricks.sdk.service.pipelines import Notifications

        out = []
        for item in items or []:
            alerts = [a for a in (item.get("alerts") or []) if a]
            recipients = [r for r in (item.get("email_recipients") or []) if r]
            if not alerts and not recipients:
                continue
            out.append(Notifications(alerts=alerts, email_recipients=recipients))
        return out

    def _build_clusters(self, items: list[dict]) -> list:
        from databricks.sdk.service.pipelines import (
            PipelineCluster,
            PipelineClusterAutoscale,
            PipelineClusterAutoscaleMode,
        )

        out = []
        valid_fields = {f for f in PipelineCluster.__dataclass_fields__}
        for raw in items or []:
            if not isinstance(raw, dict):
                continue
            kwargs: dict[str, Any] = {}
            for k, v in raw.items():
                if k not in valid_fields or v is None:
                    continue
                if k == "autoscale" and isinstance(v, dict):
                    mode_val = v.get("mode")
                    kwargs[k] = PipelineClusterAutoscale(
                        min_workers=v.get("min_workers"),
                        max_workers=v.get("max_workers"),
                        mode=PipelineClusterAutoscaleMode(mode_val) if mode_val else None,
                    )
                else:
                    kwargs[k] = v
            if kwargs:
                out.append(PipelineCluster(**kwargs))
        return out

    def _update_settings(self, settings: dict) -> None:
        """Partial update over a small whitelist of pipeline-spec fields.

        Because ``pipelines.update`` is PUT-style, we re-send the cached spec
        verbatim and overlay only the fields the user changed.
        """
        self.action_result = ""
        try:
            ws = self._get_client()
            spec = self._raw_spec
            if spec is None:
                # Force a fresh fetch if we never cached it.
                self._load_pipeline()
                spec = self._raw_spec
            if spec is None:
                raise RuntimeError("Could not resolve current pipeline spec")

            # Start from the current spec; SDK update accepts None for "leave
            # alone", but the REST endpoint is a full PUT, so we pass every
            # field explicitly.
            kwargs: dict[str, Any] = {
                "name": getattr(spec, "name", None),
                "catalog": getattr(spec, "catalog", None),
                "schema": getattr(spec, "schema", None),
                "target": getattr(spec, "target", None),
                "channel": getattr(spec, "channel", None),
                "edition": getattr(spec, "edition", None),
                "continuous": getattr(spec, "continuous", None),
                "development": getattr(spec, "development", None),
                "photon": getattr(spec, "photon", None),
                "serverless": getattr(spec, "serverless", None),
                "storage": getattr(spec, "storage", None),
                "root_path": getattr(spec, "root_path", None),
                "budget_policy_id": getattr(spec, "budget_policy_id", None),
                "configuration": dict(getattr(spec, "configuration", None) or {}),
                "tags": dict(getattr(spec, "tags", None) or {}),
                "libraries": list(getattr(spec, "libraries", None) or []),
                "notifications": list(getattr(spec, "notifications", None) or []),
                "clusters": list(getattr(spec, "clusters", None) or []),
            }
            if self._last_modified is not None:
                kwargs["expected_last_modified"] = self._last_modified

            # Whitelist the fields we let the user override.
            changed = []
            if "notifications" in settings:
                kwargs["notifications"] = self._build_notifications(settings["notifications"])
                changed.append("notifications")
            if "clusters" in settings:
                kwargs["clusters"] = self._build_clusters(settings["clusters"])
                changed.append("clusters")
            if "configuration" in settings and isinstance(settings["configuration"], dict):
                kwargs["configuration"] = {str(k): str(v) for k, v in settings["configuration"].items()}
                changed.append("configuration")
            if "channel" in settings and settings["channel"]:
                kwargs["channel"] = settings["channel"]
                changed.append("channel")
            if "development" in settings:
                kwargs["development"] = bool(settings["development"])
                changed.append("development")

            if not changed:
                self.action_result = json.dumps(
                    {"action": "update_settings", "success": False, "message": "No changes to apply."}
                )
                return

            ws.pipelines.update(self._pipeline_id, **kwargs)
            self.action_result = json.dumps(
                {
                    "action": "update_settings",
                    "success": True,
                    "message": f"Updated: {', '.join(changed)}.",
                }
            )
            self._load_pipeline()
        except Exception as exc:
            LOGGER.debug("Failed to update settings for %s", self._pipeline_id, exc_info=True)
            self.action_result = json.dumps(
                {
                    "action": "update_settings",
                    "success": False,
                    "message": f"Failed to update settings: {exc}",
                }
            )
