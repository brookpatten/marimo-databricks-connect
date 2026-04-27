"""Operational widget for a single Databricks Vector Search index.

Shows index status, embedding config, source table, sample data,
permissions, lineage, and supports triggering a sync.

Usage::

    from marimo_databricks_connect import vector_index_widget
    widget = vector_index_widget("catalog.schema.my_index")
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

_ESM_PATH = pathlib.Path(__file__).parent / "_vector_index_widget_frontend.js"


def _serialize_index(idx: Any) -> dict:
    status = getattr(idx, "status", None)
    delta_spec = getattr(idx, "delta_sync_index_spec", None)
    direct_spec = getattr(idx, "direct_access_index_spec", None)

    embedding_sources = []
    embedding_vectors = []

    spec = delta_spec or direct_spec
    if spec:
        for col in getattr(spec, "embedding_source_columns", None) or []:
            embedding_sources.append(
                {
                    "name": getattr(col, "name", None),
                    "embedding_model_endpoint": getattr(col, "embedding_model_endpoint_name", None),
                    "query_model_endpoint": getattr(col, "model_endpoint_name_for_query", None),
                }
            )
        for col in getattr(spec, "embedding_vector_columns", None) or []:
            embedding_vectors.append(
                {
                    "name": getattr(col, "name", None),
                    "dimension": getattr(col, "embedding_dimension", None),
                }
            )

    result = {
        "name": getattr(idx, "name", None),
        "endpoint_name": getattr(idx, "endpoint_name", None),
        "index_type": enum_val(getattr(idx, "index_type", None)),
        "index_subtype": enum_val(getattr(idx, "index_subtype", None)),
        "primary_key": getattr(idx, "primary_key", None),
        "creator": getattr(idx, "creator", None),
        "status_ready": getattr(status, "ready", None) if status else None,
        "status_message": getattr(status, "message", None) if status else None,
        "index_url": getattr(status, "index_url", None) if status else None,
        "indexed_row_count": getattr(status, "indexed_row_count", None) if status else None,
        "embedding_sources": embedding_sources,
        "embedding_vectors": embedding_vectors,
    }

    # Delta sync specific
    if delta_spec:
        result["source_table"] = getattr(delta_spec, "source_table", None)
        result["pipeline_id"] = getattr(delta_spec, "pipeline_id", None)
        result["pipeline_type"] = enum_val(getattr(delta_spec, "pipeline_type", None))
        result["embedding_writeback_table"] = getattr(delta_spec, "embedding_writeback_table", None)
        result["columns_to_sync"] = list(getattr(delta_spec, "columns_to_sync", None) or [])

    # Direct access specific
    if direct_spec:
        result["schema_json"] = getattr(direct_spec, "schema_json", None)

    return result


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


class VectorIndexWidget(anywidget.AnyWidget):
    """Operational widget for a single Vector Search index."""

    _esm = traitlets.Unicode().tag(sync=True)
    _css = traitlets.Unicode("").tag(sync=True)

    index_data = traitlets.Unicode("{}").tag(sync=True)
    sample_data = traitlets.Unicode("{}").tag(sync=True)
    permissions_data = traitlets.Unicode("{}").tag(sync=True)
    lineage_data = traitlets.Unicode("{}").tag(sync=True)
    action_result = traitlets.Unicode("").tag(sync=True)
    loading = traitlets.Bool(False).tag(sync=True)
    error_message = traitlets.Unicode("").tag(sync=True)
    request = traitlets.Unicode("").tag(sync=True)

    def __init__(
        self, index_name: str, workspace_client: Any = None, refresh_seconds: int = 30, **kwargs: Any
    ) -> None:
        esm_content = _ESM_PATH.read_text()
        super().__init__(_esm=esm_content, **kwargs)
        self._ws = workspace_client
        self._index_name = index_name
        self._refresh_seconds = refresh_seconds
        self.observe(self._handle_request, names=["request"])
        self._load_index()

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
            self._load_index()
        elif action == "sync":
            self._sync_index()
        elif action == "scan":
            self._scan_index()
        elif action == "get_permissions":
            self._load_permissions()
        elif action == "get_lineage":
            self._load_lineage()

    def _load_index(self) -> None:
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            idx = ws.vector_search_indexes.get_index(self._index_name)
            data = _serialize_index(idx)
            data["refresh_seconds"] = self._refresh_seconds
            self.index_data = json.dumps(data)
        except Exception as exc:
            LOGGER.debug("Failed to get index %s", self._index_name, exc_info=True)
            self.error_message = f"Failed to get index: {exc}"
        finally:
            self.loading = False

    def _sync_index(self) -> None:
        self.action_result = ""
        self.error_message = ""
        try:
            ws = self._get_client()
            ws.vector_search_indexes.sync_index(self._index_name)
            self.action_result = json.dumps(
                {
                    "action": "sync",
                    "success": True,
                    "message": "Index sync triggered.",
                }
            )
            self._load_index()
        except Exception as exc:
            LOGGER.debug("Failed to sync index %s", self._index_name, exc_info=True)
            self.action_result = json.dumps(
                {
                    "action": "sync",
                    "success": False,
                    "message": f"Failed to sync: {exc}",
                }
            )

    def _scan_index(self) -> None:
        """Scan first N rows from the index for preview."""
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            resp = ws.vector_search_indexes.scan_index(
                index_name=self._index_name,
                num_results=50,
            )
            rows: list[dict] = []
            columns: list[str] = []
            for struct in getattr(resp, "data", None) or []:
                row: dict[str, Any] = {}
                for field in getattr(struct, "fields", None) or []:
                    key = getattr(field, "key", None)
                    val_obj = getattr(field, "value", None)
                    # Value is a protobuf-like wrapper; extract the actual value
                    val = None
                    if val_obj is not None:
                        for attr in ("string_value", "float_value", "bool_value", "null_value", "list_value"):
                            v = getattr(val_obj, attr, None)
                            if v is not None:
                                val = v
                                break
                        if val is None:
                            val = str(val_obj)
                    if key and key not in columns:
                        columns.append(key)
                    row[key] = val
                rows.append(row)
            # Convert to columnar for frontend
            table_rows = []
            for row in rows:
                table_rows.append([str(row.get(c, "")) if row.get(c) is not None else None for c in columns])
            self.sample_data = json.dumps(
                {
                    "index": self._index_name,
                    "columns": columns,
                    "rows": table_rows,
                }
            )
        except Exception as exc:
            LOGGER.debug("Failed to scan index %s", self._index_name, exc_info=True)
            self.error_message = f"Failed to scan index: {exc}"
        finally:
            self.loading = False

    def _load_permissions(self) -> None:
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            # Vector search indexes are securable as TABLE type in Unity Catalog
            resp = ws.grants.get_effective(securable_type="TABLE", full_name=self._index_name)
            self.permissions_data = json.dumps(
                {
                    "full_name": self._index_name,
                    "permissions": _serialize_permissions(resp),
                }
            )
        except Exception as exc:
            LOGGER.debug("Failed to get permissions for %s", self._index_name, exc_info=True)
            self.error_message = f"Failed to get permissions: {exc}"
        finally:
            self.loading = False

    def _load_lineage(self) -> None:
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            resp = ws.api_client.do(
                "GET",
                "/api/2.0/lineage-tracking/table-lineage",
                query={"table_name": self._index_name},
            )
            upstream = []
            for entry in resp.get("upstreams") or []:
                ti = entry.get("tableInfo") or {}
                upstream.append(
                    {
                        "name": ti.get("name"),
                        "catalog_name": ti.get("catalog_name"),
                        "schema_name": ti.get("schema_name"),
                        "table_type": ti.get("table_type"),
                    }
                )
            downstream = []
            for entry in resp.get("downstreams") or []:
                ti = entry.get("tableInfo") or {}
                downstream.append(
                    {
                        "name": ti.get("name"),
                        "catalog_name": ti.get("catalog_name"),
                        "schema_name": ti.get("schema_name"),
                        "table_type": ti.get("table_type"),
                    }
                )
            self.lineage_data = json.dumps(
                {
                    "table": self._index_name,
                    "upstream": upstream,
                    "downstream": downstream,
                }
            )
        except Exception as exc:
            LOGGER.debug("Failed to get lineage for %s", self._index_name, exc_info=True)
            self.error_message = f"Failed to get lineage: {exc}"
        finally:
            self.loading = False
