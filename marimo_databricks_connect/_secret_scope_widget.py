"""Operational widget for a single Databricks secret scope.

Usage::

    from marimo_databricks_connect import secret_scope_widget
    widget = secret_scope_widget("my-scope")
"""

from __future__ import annotations

import base64
import json
import logging
import pathlib
from typing import Any

import anywidget
import traitlets

from ._ops_common import enum_val, ms_to_iso

LOGGER = logging.getLogger(__name__)

_ESM_PATH = pathlib.Path(__file__).parent / "_secret_scope_widget_frontend.js"


def _serialize_scope(scope: Any) -> dict:
    keyvault = getattr(scope, "keyvault_metadata", None)
    return {
        "name": getattr(scope, "name", None),
        "backend_type": enum_val(getattr(scope, "backend_type", None)),
        "keyvault_dns_name": getattr(keyvault, "dns_name", None) if keyvault else None,
        "keyvault_resource_id": getattr(keyvault, "resource_id", None) if keyvault else None,
    }


def _serialize_key(secret: Any) -> dict:
    return {
        "key": getattr(secret, "key", None),
        "last_updated_at": ms_to_iso(getattr(secret, "last_updated_timestamp", None)),
    }


def _serialize_acl(acl: Any) -> dict:
    return {
        "principal": getattr(acl, "principal", None),
        "permission": enum_val(getattr(acl, "permission", None)),
    }


def _decode_secret_value(value: str | None) -> str:
    if value is None:
        return ""
    try:
        decoded = base64.b64decode(value, validate=True)
        return decoded.decode("utf-8")
    except Exception:
        return value


class SecretScopeWidget(anywidget.AnyWidget):
    """Operational widget for a single Databricks secret scope."""

    _esm = traitlets.Unicode().tag(sync=True)
    _css = traitlets.Unicode("").tag(sync=True)

    scope_data = traitlets.Unicode("{}").tag(sync=True)
    keys_data = traitlets.Unicode("[]").tag(sync=True)
    permissions_data = traitlets.Unicode("[]").tag(sync=True)
    selected_secret_data = traitlets.Unicode("{}").tag(sync=True)
    action_result = traitlets.Unicode("").tag(sync=True)
    loading = traitlets.Bool(False).tag(sync=True)
    error_message = traitlets.Unicode("").tag(sync=True)
    request = traitlets.Unicode("").tag(sync=True)

    def __init__(self, scope_name: str, workspace_client: Any = None, **kwargs: Any) -> None:
        esm_content = _ESM_PATH.read_text()
        super().__init__(_esm=esm_content, **kwargs)
        self._ws = workspace_client
        self._spark = None
        self._dbutils = None
        self._scope_name = scope_name
        self._keys_by_name: dict[str, dict[str, Any]] = {}
        self.observe(self._handle_request, names=["request"])
        self._refresh()

    def _get_client(self) -> Any:
        if self._ws is not None:
            return self._ws
        from databricks.sdk import WorkspaceClient

        self._ws = WorkspaceClient()
        return self._ws

    def _get_spark(self) -> Any:
        if self._spark is not None:
            return self._spark
        try:
            from databricks.connect import DatabricksSession

            self._spark = DatabricksSession.builder.serverless().getOrCreate()
        except Exception:
            LOGGER.debug("Could not create DatabricksSession", exc_info=True)
        return self._spark

    def _get_dbutils(self) -> Any:
        if self._dbutils is not None:
            return self._dbutils
        spark = self._get_spark()
        if spark is None:
            return None
        try:
            from pyspark.dbutils import DBUtils  # type: ignore[import-untyped]

            self._dbutils = DBUtils(spark)
        except Exception:
            LOGGER.debug("Could not create DBUtils", exc_info=True)
        return self._dbutils

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
            self._refresh()
        elif action == "get_secret":
            self._load_secret(req.get("key"))
        elif action == "put_secret":
            self._put_secret(req.get("key"), req.get("value"))
        elif action == "delete_secret":
            self._delete_secret(req.get("key"))
        elif action == "get_permissions":
            self._load_permissions()
        elif action == "put_acl":
            self._put_acl(req.get("principal"), req.get("permission"))
        elif action == "delete_acl":
            self._delete_acl(req.get("principal"))

    def _refresh(self) -> None:
        self.selected_secret_data = "{}"
        self.action_result = ""
        ok = self._load_scope()
        if ok:
            self._load_keys()
            self._load_permissions()
        else:
            self.keys_data = "[]"
            self.permissions_data = "[]"
            self._keys_by_name = {}

    def _load_scope(self) -> bool:
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            scope = next(
                (s for s in ws.secrets.list_scopes() if getattr(s, "name", None) == self._scope_name),
                None,
            )
            if scope is None:
                raise ValueError(f"Secret scope {self._scope_name!r} not found.")
            self.scope_data = json.dumps(_serialize_scope(scope))
            return True
        except Exception as exc:
            LOGGER.debug("Failed to get secret scope %s", self._scope_name, exc_info=True)
            self.error_message = f"Failed to get secret scope: {exc}"
            return False
        finally:
            self.loading = False

    def _load_keys(self) -> None:
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            keys = sorted(
                list(ws.secrets.list_secrets(self._scope_name)),
                key=lambda s: (getattr(s, "key", None) or "").lower(),
            )
            serialized = [_serialize_key(s) for s in keys]
            self._keys_by_name = {k["key"]: k for k in serialized if k.get("key")}
            self.keys_data = json.dumps(serialized)
        except Exception as exc:
            LOGGER.debug("Failed to list secrets for scope %s", self._scope_name, exc_info=True)
            self.error_message = f"Failed to list secrets: {exc}"
        finally:
            self.loading = False

    def _load_permissions(self) -> None:
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            acls = sorted(
                list(ws.secrets.list_acls(self._scope_name)),
                key=lambda a: (getattr(a, "principal", None) or "").lower(),
            )
            self.permissions_data = json.dumps([_serialize_acl(a) for a in acls])
        except Exception as exc:
            LOGGER.debug("Failed to list ACLs for scope %s", self._scope_name, exc_info=True)
            self.error_message = f"Failed to list permissions: {exc}"
        finally:
            self.loading = False

    def _get_secret_value(self, key: str) -> str:
        dbutils = self._get_dbutils()
        if dbutils is not None:
            try:
                return dbutils.secrets.get(self._scope_name, key)
            except Exception:
                LOGGER.debug("dbutils.secrets.get failed for %s/%s", self._scope_name, key, exc_info=True)

        ws = self._get_client()
        resp = ws.secrets.get_secret(scope=self._scope_name, key=key)
        return _decode_secret_value(getattr(resp, "value", None))

    def _load_secret(self, key: str | None) -> None:
        if not key:
            self.error_message = "Missing secret key."
            return
        self.loading = True
        self.error_message = ""
        self.action_result = ""
        try:
            value = self._get_secret_value(key)
            self.selected_secret_data = json.dumps(
                {
                    "scope": self._scope_name,
                    "key": key,
                    "value": value,
                    "last_updated_at": self._keys_by_name.get(key, {}).get("last_updated_at"),
                }
            )
        except Exception as exc:
            LOGGER.debug("Failed to get secret %s/%s", self._scope_name, key, exc_info=True)
            self.error_message = f"Failed to get secret value: {exc}"
        finally:
            self.loading = False

    def _put_secret(self, key: Any, value: Any) -> None:
        key_text = str(key or "").strip()
        if not key_text:
            self.error_message = "Missing secret key."
            return
        if value is None:
            self.error_message = "Missing secret value."
            return

        self.loading = True
        self.error_message = ""
        self.action_result = ""
        try:
            ws = self._get_client()
            ws.secrets.put_secret(scope=self._scope_name, key=key_text, string_value=str(value))
            self._load_keys()
            self.selected_secret_data = json.dumps(
                {
                    "scope": self._scope_name,
                    "key": key_text,
                    "value": str(value),
                    "last_updated_at": self._keys_by_name.get(key_text, {}).get("last_updated_at"),
                }
            )
            self.action_result = json.dumps(
                {
                    "action": "put_secret",
                    "success": True,
                    "message": f"Saved secret {key_text!r}.",
                    "key": key_text,
                }
            )
        except Exception as exc:
            LOGGER.debug("Failed to put secret %s/%s", self._scope_name, key_text, exc_info=True)
            self.action_result = json.dumps(
                {
                    "action": "put_secret",
                    "success": False,
                    "message": f"Failed to save secret: {exc}",
                    "key": key_text,
                }
            )
        finally:
            self.loading = False

    def _delete_secret(self, key: Any) -> None:
        key_text = str(key or "").strip()
        if not key_text:
            self.error_message = "Missing secret key."
            return

        self.loading = True
        self.error_message = ""
        self.action_result = ""
        try:
            ws = self._get_client()
            ws.secrets.delete_secret(scope=self._scope_name, key=key_text)
            self._load_keys()
            current = json.loads(self.selected_secret_data or "{}")
            if current.get("key") == key_text:
                self.selected_secret_data = "{}"
            self.action_result = json.dumps(
                {
                    "action": "delete_secret",
                    "success": True,
                    "message": f"Deleted secret {key_text!r}.",
                    "key": key_text,
                }
            )
        except Exception as exc:
            LOGGER.debug("Failed to delete secret %s/%s", self._scope_name, key_text, exc_info=True)
            self.action_result = json.dumps(
                {
                    "action": "delete_secret",
                    "success": False,
                    "message": f"Failed to delete secret: {exc}",
                    "key": key_text,
                }
            )
        finally:
            self.loading = False

    def _put_acl(self, principal: Any, permission: Any) -> None:
        principal_text = str(principal or "").strip()
        permission_text = str(permission or "").strip().upper()
        if not principal_text:
            self.error_message = "Missing principal."
            return
        if not permission_text:
            self.error_message = "Missing permission."
            return

        self.loading = True
        self.error_message = ""
        self.action_result = ""
        try:
            from databricks.sdk.service.workspace import AclPermission

            ws = self._get_client()
            ws.secrets.put_acl(scope=self._scope_name, principal=principal_text, permission=AclPermission(permission_text))
            self._load_permissions()
            self.action_result = json.dumps(
                {
                    "action": "put_acl",
                    "success": True,
                    "message": f"Updated permissions for {principal_text!r}.",
                    "principal": principal_text,
                }
            )
        except Exception as exc:
            LOGGER.debug("Failed to put ACL %s/%s", self._scope_name, principal_text, exc_info=True)
            self.action_result = json.dumps(
                {
                    "action": "put_acl",
                    "success": False,
                    "message": f"Failed to update permissions: {exc}",
                    "principal": principal_text,
                }
            )
        finally:
            self.loading = False

    def _delete_acl(self, principal: Any) -> None:
        principal_text = str(principal or "").strip()
        if not principal_text:
            self.error_message = "Missing principal."
            return

        self.loading = True
        self.error_message = ""
        self.action_result = ""
        try:
            ws = self._get_client()
            ws.secrets.delete_acl(scope=self._scope_name, principal=principal_text)
            self._load_permissions()
            self.action_result = json.dumps(
                {
                    "action": "delete_acl",
                    "success": True,
                    "message": f"Removed permissions for {principal_text!r}.",
                    "principal": principal_text,
                }
            )
        except Exception as exc:
            LOGGER.debug("Failed to delete ACL %s/%s", self._scope_name, principal_text, exc_info=True)
            self.action_result = json.dumps(
                {
                    "action": "delete_acl",
                    "success": False,
                    "message": f"Failed to remove permissions: {exc}",
                    "principal": principal_text,
                }
            )
        finally:
            self.loading = False
