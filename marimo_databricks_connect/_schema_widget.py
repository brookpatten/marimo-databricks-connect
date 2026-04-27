"""Operational widget for a single Databricks Unity Catalog schema.

Usage::

    from marimo_databricks_connect import schema_widget
    widget = schema_widget("catalog_name", "schema_name")
"""

from __future__ import annotations

import json
import logging
import pathlib
from typing import Any

import anywidget
import traitlets

from ._ops_common import enum_val, ms_to_iso, safe_dict

LOGGER = logging.getLogger(__name__)

_ESM_PATH = pathlib.Path(__file__).parent / "_schema_widget_frontend.js"


def _serialize_schema(s: Any) -> dict:
    return {
        "name": getattr(s, "name", None),
        "catalog_name": getattr(s, "catalog_name", None),
        "full_name": getattr(s, "full_name", None),
        "comment": getattr(s, "comment", None),
        "owner": getattr(s, "owner", None),
        "created_at": ms_to_iso(getattr(s, "created_at", None)),
        "created_by": getattr(s, "created_by", None),
        "updated_at": ms_to_iso(getattr(s, "updated_at", None)),
        "updated_by": getattr(s, "updated_by", None),
        "storage_location": getattr(s, "storage_location", None),
        "storage_root": getattr(s, "storage_root", None),
        "properties": safe_dict(getattr(s, "properties", None)),
    }


def _serialize_table_summary(t: Any) -> dict:
    return {
        "name": getattr(t, "name", None),
        "full_name": getattr(t, "full_name", None),
        "table_type": enum_val(getattr(t, "table_type", None)),
        "data_source_format": enum_val(getattr(t, "data_source_format", None)),
        "comment": getattr(t, "comment", None),
        "owner": getattr(t, "owner", None),
        "created_at": ms_to_iso(getattr(t, "created_at", None)),
        "updated_at": ms_to_iso(getattr(t, "updated_at", None)),
    }


def _serialize_volume(v: Any) -> dict:
    return {
        "name": getattr(v, "name", None),
        "full_name": getattr(v, "full_name", None),
        "volume_type": enum_val(getattr(v, "volume_type", None)),
        "comment": getattr(v, "comment", None),
        "owner": getattr(v, "owner", None),
        "storage_location": getattr(v, "storage_location", None),
        "created_at": ms_to_iso(getattr(v, "created_at", None)),
        "updated_at": ms_to_iso(getattr(v, "updated_at", None)),
    }


def _serialize_permissions(resp: Any) -> list[dict]:
    result = []
    for pa in getattr(resp, "privilege_assignments", None) or []:
        privs = [
            {
                "privilege": enum_val(getattr(p, "privilege", None)),
                "inherited_from_name": getattr(p, "inherited_from_name", None),
            }
            for p in getattr(pa, "privileges", None) or []
        ]
        result.append({"principal": getattr(pa, "principal", None), "privileges": privs})
    return result


class SchemaWidget(anywidget.AnyWidget):
    """Operational widget for a single Unity Catalog schema."""

    _esm = traitlets.Unicode().tag(sync=True)
    _css = traitlets.Unicode("").tag(sync=True)

    schema_data = traitlets.Unicode("{}").tag(sync=True)
    tables_data = traitlets.Unicode("[]").tag(sync=True)
    volumes_data = traitlets.Unicode("[]").tag(sync=True)
    permissions_data = traitlets.Unicode("{}").tag(sync=True)
    loading = traitlets.Bool(False).tag(sync=True)
    error_message = traitlets.Unicode("").tag(sync=True)
    request = traitlets.Unicode("").tag(sync=True)

    def __init__(self, catalog_name: str, schema_name: str, workspace_client: Any = None, **kwargs: Any) -> None:
        esm_content = _ESM_PATH.read_text()
        super().__init__(_esm=esm_content, **kwargs)
        self._ws = workspace_client
        self._catalog = catalog_name
        self._schema = schema_name
        self.observe(self._handle_request, names=["request"])
        self._load_schema()
        self._load_tables()

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
            self._load_schema()
            self._load_tables()
        elif action == "list_volumes":
            self._load_volumes()
        elif action == "get_permissions":
            self._load_permissions()

    def _load_schema(self) -> None:
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            schema = ws.schemas.get(f"{self._catalog}.{self._schema}")
            self.schema_data = json.dumps(_serialize_schema(schema))
        except Exception as exc:
            LOGGER.debug("Failed to get schema", exc_info=True)
            self.error_message = f"Failed to get schema: {exc}"
        finally:
            self.loading = False

    def _load_tables(self) -> None:
        self.error_message = ""
        try:
            ws = self._get_client()
            tables = list(ws.tables.list(catalog_name=self._catalog, schema_name=self._schema, max_results=200))
            self.tables_data = json.dumps([_serialize_table_summary(t) for t in tables])
        except Exception as exc:
            LOGGER.debug("Failed to list tables", exc_info=True)
            self.error_message = f"Failed to list tables: {exc}"

    def _load_volumes(self) -> None:
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            volumes = list(ws.volumes.list(catalog_name=self._catalog, schema_name=self._schema))
            self.volumes_data = json.dumps([_serialize_volume(v) for v in volumes])
        except Exception as exc:
            LOGGER.debug("Failed to list volumes", exc_info=True)
            self.error_message = f"Failed to list volumes: {exc}"
        finally:
            self.loading = False

    def _load_permissions(self) -> None:
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            resp = ws.grants.get_effective(securable_type="SCHEMA", full_name=f"{self._catalog}.{self._schema}")
            self.permissions_data = json.dumps({"permissions": _serialize_permissions(resp)})
        except Exception as exc:
            LOGGER.debug("Failed to get permissions", exc_info=True)
            self.error_message = f"Failed to get permissions: {exc}"
        finally:
            self.loading = False
