"""Operational widget for a single Databricks App.

Shows app details, deployments, permissions, and thumbnail.  Supports
start / stop, creating & deleting deployments, updating permissions, and
managing the app thumbnail.

Usage::

    from marimo_databricks_connect import app_widget
    widget = app_widget("my-app")
"""

from __future__ import annotations

import json
import logging
import pathlib
from typing import Any

import anywidget
import traitlets

from ._ops_common import enum_val

LOGGER = logging.getLogger(__name__)

_ESM_PATH = pathlib.Path(__file__).parent / "_app_widget_frontend.js"


# --------------------------------------------------------------------------- #
# Serializers                                                                  #
# --------------------------------------------------------------------------- #


def _serialize_deployment(d: Any) -> dict:
    status = getattr(d, "status", None)
    env_vars = []
    for ev in getattr(d, "env_vars", None) or []:
        env_vars.append({"name": getattr(ev, "name", ""), "value": getattr(ev, "value", "")})
    git_source = getattr(d, "git_source", None)
    return {
        "deployment_id": getattr(d, "deployment_id", None),
        "source_code_path": getattr(d, "source_code_path", None),
        "mode": enum_val(getattr(d, "mode", None)),
        "state": enum_val(getattr(status, "state", None)) if status else None,
        "state_message": getattr(status, "message", None) if status else None,
        "create_time": getattr(d, "create_time", None),
        "update_time": getattr(d, "update_time", None),
        "creator": getattr(d, "creator", None),
        "command": list(getattr(d, "command", None) or []),
        "env_vars": env_vars,
        "git_branch": getattr(git_source, "branch", None) if git_source else None,
        "git_commit": getattr(git_source, "commit", None) if git_source else None,
    }


def _serialize_resource(r: Any) -> dict:
    rtype = "unknown"
    detail = None
    for attr, label in (
        ("serving_endpoint", "serving_endpoint"),
        ("sql_warehouse", "sql_warehouse"),
        ("job", "job"),
        ("secret", "secret"),
        ("database", "database"),
        ("experiment", "experiment"),
        ("uc_securable", "uc_securable"),
        ("genie_space", "genie_space"),
        ("postgres", "postgres"),
        ("app", "app"),
    ):
        obj = getattr(r, attr, None)
        if obj is not None:
            rtype = label
            # Try to pull a name/id/permission out of the nested object
            detail = getattr(obj, "name", None) or getattr(obj, "id", None) or getattr(obj, "key", None) or str(obj)
            break
    return {
        "name": getattr(r, "name", None),
        "description": getattr(r, "description", None),
        "type": rtype,
        "detail": detail,
    }


def _serialize_app(app: Any) -> dict:
    app_status = getattr(app, "app_status", None)
    compute_status = getattr(app, "compute_status", None)
    active = getattr(app, "active_deployment", None)
    pending = getattr(app, "pending_deployment", None)
    resources = [_serialize_resource(r) for r in getattr(app, "resources", None) or []]

    return {
        "name": getattr(app, "name", None),
        "id": getattr(app, "id", None),
        "description": getattr(app, "description", None),
        "url": getattr(app, "url", None),
        "creator": getattr(app, "creator", None),
        "create_time": getattr(app, "create_time", None),
        "update_time": getattr(app, "update_time", None),
        "updater": getattr(app, "updater", None),
        "app_state": enum_val(getattr(app_status, "state", None)) if app_status else None,
        "app_state_message": getattr(app_status, "message", None) if app_status else None,
        "compute_state": enum_val(getattr(compute_status, "state", None)) if compute_status else None,
        "compute_message": getattr(compute_status, "message", None) if compute_status else None,
        "compute_active_instances": getattr(compute_status, "active_instances", None) if compute_status else None,
        "compute_size": enum_val(getattr(app, "compute_size", None)),
        "default_source_code_path": getattr(app, "default_source_code_path", None),
        "service_principal_name": getattr(app, "service_principal_name", None),
        "space": getattr(app, "space", None),
        "active_deployment": _serialize_deployment(active) if active else None,
        "pending_deployment": _serialize_deployment(pending) if pending else None,
        "resources": resources,
    }


def _serialize_permissions(perms: Any) -> dict:
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


# --------------------------------------------------------------------------- #
# Widget                                                                       #
# --------------------------------------------------------------------------- #


class AppWidget(anywidget.AnyWidget):
    """Operational widget for a single Databricks App."""

    _esm = traitlets.Unicode().tag(sync=True)
    _css = traitlets.Unicode("").tag(sync=True)

    app_data = traitlets.Unicode("{}").tag(sync=True)
    deployments_data = traitlets.Unicode("[]").tag(sync=True)
    permissions_data = traitlets.Unicode("{}").tag(sync=True)
    thumbnail_data = traitlets.Unicode("{}").tag(sync=True)
    action_result = traitlets.Unicode("").tag(sync=True)
    loading = traitlets.Bool(False).tag(sync=True)
    error_message = traitlets.Unicode("").tag(sync=True)
    request = traitlets.Unicode("").tag(sync=True)

    def __init__(self, app_name: str, workspace_client: Any = None, refresh_seconds: int = 30, **kwargs: Any) -> None:
        esm_content = _ESM_PATH.read_text()
        super().__init__(_esm=esm_content, **kwargs)
        self._ws = workspace_client
        self._app_name = app_name
        self._refresh_seconds = refresh_seconds
        self.observe(self._handle_request, names=["request"])
        self._load_app()
        self._load_thumbnail()

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
            self._load_app()
        elif action == "list_deployments":
            self._load_deployments()
        elif action == "get_permissions":
            self._load_permissions()
        elif action == "get_thumbnail":
            self._load_thumbnail()
        elif action == "start":
            self._start_app()
        elif action == "stop":
            self._stop_app()
        elif action == "deploy":
            self._create_deployment(req.get("config", {}))
        elif action == "delete_deployment":
            # The SDK doesn't expose a delete-deployment; but we pass through
            self._action_error(
                "delete_deployment", "Deployment deletion is not supported via SDK. Use the Databricks UI."
            )
        elif action == "update_permissions":
            self._update_permissions(req.get("acl", []))
        elif action == "update_thumbnail":
            self._update_thumbnail(req.get("thumbnail_base64"))
        elif action == "delete_thumbnail":
            self._delete_thumbnail()

    # -- helpers --

    def _action_ok(self, action: str, message: str) -> None:
        self.action_result = json.dumps({"action": action, "success": True, "message": message})

    def _action_error(self, action: str, message: str) -> None:
        self.action_result = json.dumps({"action": action, "success": False, "message": message})

    # -- data loading --

    def _load_app(self) -> None:
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            app = ws.apps.get(self._app_name)
            data = _serialize_app(app)
            data["refresh_seconds"] = self._refresh_seconds
            self.app_data = json.dumps(data)
        except Exception as exc:
            LOGGER.debug("Failed to get app %s", self._app_name, exc_info=True)
            self.error_message = f"Failed to get app: {exc}"
        finally:
            self.loading = False

    def _load_deployments(self) -> None:
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            deps = list(ws.apps.list_deployments(app_name=self._app_name))
            self.deployments_data = json.dumps([_serialize_deployment(d) for d in deps])
        except Exception as exc:
            LOGGER.debug("Failed to list deployments for %s", self._app_name, exc_info=True)
            self.error_message = f"Failed to list deployments: {exc}"
        finally:
            self.loading = False

    def _load_permissions(self) -> None:
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            perms = ws.apps.get_permissions(self._app_name)
            self.permissions_data = json.dumps(_serialize_permissions(perms))
        except Exception as exc:
            LOGGER.debug("Failed to get permissions for %s", self._app_name, exc_info=True)
            self.error_message = f"Failed to get permissions: {exc}"
        finally:
            self.loading = False

    def _load_thumbnail(self) -> None:
        self.error_message = ""
        try:
            ws = self._get_client()
            resp = ws.api_client.do(
                "GET",
                f"/api/2.0/apps/{self._app_name}/thumbnail",
            )
            thumb_b64 = None
            if isinstance(resp, dict):
                thumb_b64 = resp.get("thumbnail")
                # If nested under app_thumbnail key
                if not thumb_b64 and resp.get("app_thumbnail"):
                    thumb_b64 = resp["app_thumbnail"].get("thumbnail")
            self.thumbnail_data = json.dumps({"loaded": True, "thumbnail": thumb_b64})
        except Exception:
            LOGGER.debug("Failed to get thumbnail for %s", self._app_name, exc_info=True)
            # Not all apps have thumbnails — don't treat as hard error
            self.thumbnail_data = json.dumps({"loaded": True, "thumbnail": None})

    # -- actions --

    def _start_app(self) -> None:
        self.action_result = ""
        try:
            ws = self._get_client()
            ws.apps.start(self._app_name)
            self._action_ok("start", "App start requested.")
            self._load_app()
        except Exception as exc:
            self._action_error("start", f"Failed to start: {exc}")

    def _stop_app(self) -> None:
        self.action_result = ""
        try:
            ws = self._get_client()
            ws.apps.stop(self._app_name)
            self._action_ok("stop", "App stop requested.")
            self._load_app()
        except Exception as exc:
            self._action_error("stop", f"Failed to stop: {exc}")

    def _create_deployment(self, config: dict) -> None:
        self.action_result = ""
        try:
            from databricks.sdk.service.apps import AppDeployment, AppDeploymentMode

            kwargs: dict[str, Any] = {}
            if config.get("source_code_path"):
                kwargs["source_code_path"] = config["source_code_path"]
            mode_str = config.get("mode")
            if mode_str:
                kwargs["mode"] = AppDeploymentMode(mode_str)

            dep = AppDeployment(**kwargs)
            ws = self._get_client()
            result = ws.apps.deploy(self._app_name, dep)
            dep_id = getattr(result, "deployment_id", None) or getattr(
                getattr(result, "response", None), "deployment_id", None
            )
            self._action_ok("deploy", f"Deployment created. ID: {dep_id}")
            self._load_app()
        except Exception as exc:
            LOGGER.debug("Failed to deploy %s", self._app_name, exc_info=True)
            self._action_error("deploy", f"Failed to create deployment: {exc}")

    def _update_permissions(self, acl: list[dict]) -> None:
        self.action_result = ""
        try:
            from databricks.sdk.service.apps import AppAccessControlRequest, AppPermissionLevel

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
                kwargs["permission_level"] = AppPermissionLevel(entry.get("permission_level", "CAN_USE"))
                requests.append(AppAccessControlRequest(**kwargs))

            ws = self._get_client()
            ws.apps.set_permissions(self._app_name, access_control_list=requests)
            self._action_ok("update_permissions", "Permissions updated.")
            self._load_permissions()
        except Exception as exc:
            LOGGER.debug("Failed to update permissions for %s", self._app_name, exc_info=True)
            self._action_error("update_permissions", f"Failed to update permissions: {exc}")

    def _update_thumbnail(self, thumbnail_base64: str | None) -> None:
        self.action_result = ""
        try:
            from databricks.sdk.service.apps import AppThumbnail

            ws = self._get_client()
            ws.apps.update_app_thumbnail(
                self._app_name,
                app_thumbnail=AppThumbnail(thumbnail=thumbnail_base64),
            )
            self._action_ok("update_thumbnail", "Thumbnail updated.")
            # Refresh the thumbnail display
            self.thumbnail_data = json.dumps({"loaded": True, "thumbnail": thumbnail_base64})
        except Exception as exc:
            LOGGER.debug("Failed to update thumbnail for %s", self._app_name, exc_info=True)
            self._action_error("update_thumbnail", f"Failed to update thumbnail: {exc}")

    def _delete_thumbnail(self) -> None:
        self.action_result = ""
        try:
            ws = self._get_client()
            ws.apps.delete_app_thumbnail(self._app_name)
            self._action_ok("delete_thumbnail", "Thumbnail deleted.")
            # Clear the thumbnail display
            self.thumbnail_data = json.dumps({"loaded": True, "thumbnail": None})
        except Exception as exc:
            LOGGER.debug("Failed to delete thumbnail for %s", self._app_name, exc_info=True)
            self._action_error("delete_thumbnail", f"Failed to delete thumbnail: {exc}")
