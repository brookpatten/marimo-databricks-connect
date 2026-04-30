"""Browser widget for the Databricks Workspace tree.

Lets users navigate notebooks, files, folders, and Repos under any starting
path, preview file/notebook content, and inspect per-object permissions.

Usage::

    from marimo_databricks_connect import workspace_widget

    widget = workspace_widget()                # browse from "/"
    widget = workspace_widget(root="/Users/me@example.com")
"""

from __future__ import annotations

import base64
import json
import logging
import pathlib
from typing import Any

import anywidget
import traitlets

from ._ops_common import enum_val
from ._workspace_fs import _is_dir_object, _object_type

LOGGER = logging.getLogger(__name__)

_ESM_PATH = pathlib.Path(__file__).parent / "_workspace_widget_frontend.js"

# Map workspace ObjectType -> permissions API resource path component.
# (See WorkspaceClient.permissions.get(request_object_type=..., request_object_id=...))
_PERMISSIONS_OBJECT_TYPE = {
    "NOTEBOOK": "notebooks",
    "DIRECTORY": "directories",
    "REPO": "repos",
    "FILE": "files",
}

# Max bytes of file/notebook content to surface to the frontend in a preview.
_PREVIEW_MAX_BYTES = 256 * 1024


# --------------------------------------------------------------------------- #
# Serializers                                                                  #
# --------------------------------------------------------------------------- #


def _serialize_object(entry: Any) -> dict:
    """Flatten a Workspace ``ObjectInfo`` into a JSON-friendly dict."""
    path = getattr(entry, "path", "") or ""
    name = path.rsplit("/", 1)[-1] or path
    obj_type = _object_type(entry)
    language = getattr(entry, "language", None)
    return {
        "name": name,
        "path": path,
        "object_type": obj_type,
        "is_dir": _is_dir_object(entry),
        "language": (
            getattr(language, "value", None)
            or getattr(language, "name", None)
            or (str(language) if language else None)
        ),
        "object_id": int(getattr(entry, "object_id", 0) or 0) or None,
        "size": int(getattr(entry, "size", 0) or 0),
        "modified_at": getattr(entry, "modified_at", None),
        "created_at": getattr(entry, "created_at", None),
    }


def _serialize_acl(resp: Any) -> list[dict]:
    """Flatten ``ObjectPermissions`` into ``[{principal, level, inherited}]``."""
    out: list[dict] = []
    acl = getattr(resp, "access_control_list", None) or []
    for entry in acl:
        principal = (
            getattr(entry, "user_name", None)
            or getattr(entry, "group_name", None)
            or getattr(entry, "service_principal_name", None)
        )
        for perm in getattr(entry, "all_permissions", None) or []:
            out.append(
                {
                    "principal": principal,
                    "permission_level": enum_val(getattr(perm, "permission_level", None)),
                    "inherited": bool(getattr(perm, "inherited", False)),
                    "inherited_from": list(getattr(perm, "inherited_from_object", None) or []),
                }
            )
    return out


# --------------------------------------------------------------------------- #
# Widget                                                                       #
# --------------------------------------------------------------------------- #


class WorkspaceWidget(anywidget.AnyWidget):
    """Workspace browser widget.

    Args:
        root: Starting path in the workspace tree (default ``"/"``).
        workspace_client: Optional ``databricks.sdk.WorkspaceClient``.
    """

    _esm = traitlets.Unicode().tag(sync=True)
    _css = traitlets.Unicode("").tag(sync=True)

    root = traitlets.Unicode("/").tag(sync=True)
    contents_data = traitlets.Unicode("{}").tag(sync=True)
    selected_data = traitlets.Unicode("{}").tag(sync=True)
    permissions_data = traitlets.Unicode("{}").tag(sync=True)
    preview_data = traitlets.Unicode("{}").tag(sync=True)
    loading = traitlets.Bool(False).tag(sync=True)
    error_message = traitlets.Unicode("").tag(sync=True)
    request = traitlets.Unicode("").tag(sync=True)

    def __init__(
        self,
        root: str = "/",
        workspace_client: Any = None,
        **kwargs: Any,
    ) -> None:
        esm_content = _ESM_PATH.read_text()
        super().__init__(_esm=esm_content, root=root or "/", **kwargs)
        self._ws = workspace_client
        self.observe(self._handle_request, names=["request"])
        # Eagerly list the root so something appears immediately.
        self._browse(self.root)

    # -- client helpers ---------------------------------------------------

    def _get_client(self) -> Any:
        if self._ws is not None:
            return self._ws
        from databricks.sdk import WorkspaceClient

        self._ws = WorkspaceClient()
        return self._ws

    # -- request routing --------------------------------------------------

    def _handle_request(self, change: Any) -> None:
        raw = change.get("new", "")
        if not raw:
            return
        try:
            req = json.loads(raw)
        except json.JSONDecodeError:
            return
        action = req.get("action")
        if action == "browse":
            self._browse(req.get("path") or self.root)
        elif action == "select":
            self._select(req.get("path"))
        elif action == "get_permissions":
            self._load_permissions(req.get("path"), req.get("object_type"), req.get("object_id"))
        elif action == "preview":
            self._preview(req.get("path"))
        elif action == "refresh":
            self._browse(self.root)

    # -- data loading -----------------------------------------------------

    def _browse(self, path: str) -> None:
        path = path or "/"
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            entries = list(ws.workspace.list(path))
            items = [_serialize_object(e) for e in entries]
            # Stable ordering: dirs first, then alphabetical.
            items.sort(key=lambda d: (not d["is_dir"], d["name"].lower()))
            self.contents_data = json.dumps({"path": path, "items": items})
        except Exception as exc:
            LOGGER.debug("Failed to list workspace path %s", path, exc_info=True)
            self.error_message = f"Failed to list {path!r}: {exc}"
        finally:
            self.loading = False

    def _select(self, path: str | None) -> None:
        if not path:
            return
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            entry = ws.workspace.get_status(path)
            self.selected_data = json.dumps(_serialize_object(entry))
        except Exception as exc:
            LOGGER.debug("Failed to get workspace status %s", path, exc_info=True)
            self.error_message = f"Failed to inspect {path!r}: {exc}"
        finally:
            self.loading = False

    def _load_permissions(
        self,
        path: str | None,
        object_type: str | None,
        object_id: int | None,
    ) -> None:
        if not object_id and path:
            # Resolve object_id from path
            try:
                ws = self._get_client()
                entry = ws.workspace.get_status(path)
                object_id = int(getattr(entry, "object_id", 0) or 0) or None
                object_type = _object_type(entry)
            except Exception as exc:
                self.error_message = f"Failed to resolve object id for {path!r}: {exc}"
                return
        if not object_id:
            self.error_message = "No object id available for permissions lookup."
            return
        resource = _PERMISSIONS_OBJECT_TYPE.get((object_type or "").upper())
        if not resource:
            self.error_message = f"Permissions not supported for object type {object_type!r}."
            return
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            resp = ws.permissions.get(request_object_type=resource, request_object_id=str(object_id))
            self.permissions_data = json.dumps(
                {
                    "path": path,
                    "object_type": object_type,
                    "object_id": object_id,
                    "acl": _serialize_acl(resp),
                }
            )
        except Exception as exc:
            LOGGER.debug("Failed to get permissions for %s/%s", object_type, object_id, exc_info=True)
            self.error_message = f"Failed to get permissions: {exc}"
        finally:
            self.loading = False

    def _preview(self, path: str | None) -> None:
        if not path:
            return
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            try:
                from databricks.sdk.service.workspace import ExportFormat

                fmt = ExportFormat.SOURCE
            except Exception:  # pragma: no cover
                fmt = "SOURCE"
            resp = ws.workspace.export(path, format=fmt)
            content_b64 = getattr(resp, "content", "") or ""
            try:
                raw = base64.b64decode(content_b64)
            except Exception:
                raw = (
                    (content_b64 or "").encode("utf-8", errors="replace")
                    if isinstance(content_b64, str)
                    else bytes(content_b64)
                )
            truncated = len(raw) > _PREVIEW_MAX_BYTES
            raw = raw[:_PREVIEW_MAX_BYTES]
            try:
                text = raw.decode("utf-8")
                is_text = True
            except UnicodeDecodeError:
                text = f"<binary content, {len(raw)} bytes>"
                is_text = False
            self.preview_data = json.dumps(
                {
                    "path": path,
                    "text": text,
                    "is_text": is_text,
                    "truncated": truncated,
                    "size": len(raw),
                }
            )
        except Exception as exc:
            LOGGER.debug("Failed to preview %s", path, exc_info=True)
            self.error_message = f"Failed to preview: {exc}"
        finally:
            self.loading = False
