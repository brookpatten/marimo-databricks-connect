"""Operational widget for a single Databricks Unity Catalog table.

Displays schema/columns, sample data, lineage, properties, and permissions.

Usage::

    from marimo_databricks_connect import table_widget
    widget = table_widget("catalog.schema.table_name")
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

_ESM_PATH = pathlib.Path(__file__).parent / "_table_widget_frontend.js"


def _serialize_column(c: Any) -> dict:
    return {
        "name": getattr(c, "name", None),
        "type_text": getattr(c, "type_text", None),
        "type_name": enum_val(getattr(c, "type_name", None)),
        "comment": getattr(c, "comment", None),
        "nullable": getattr(c, "nullable", None),
        "position": getattr(c, "position", None),
        "partition_index": getattr(c, "partition_index", None),
    }


def _serialize_table(t: Any) -> dict:
    cols = [_serialize_column(c) for c in (getattr(t, "columns", None) or [])]
    return {
        "name": getattr(t, "name", None),
        "catalog_name": getattr(t, "catalog_name", None),
        "schema_name": getattr(t, "schema_name", None),
        "full_name": getattr(t, "full_name", None),
        "table_type": enum_val(getattr(t, "table_type", None)),
        "data_source_format": enum_val(getattr(t, "data_source_format", None)),
        "comment": getattr(t, "comment", None),
        "owner": getattr(t, "owner", None),
        "created_at": ms_to_iso(getattr(t, "created_at", None)),
        "created_by": getattr(t, "created_by", None),
        "updated_at": ms_to_iso(getattr(t, "updated_at", None)),
        "updated_by": getattr(t, "updated_by", None),
        "storage_location": getattr(t, "storage_location", None),
        "view_definition": getattr(t, "view_definition", None),
        "table_id": getattr(t, "table_id", None),
        "columns": cols,
        "properties": safe_dict(getattr(t, "properties", None)),
    }


def _serialize_permissions(resp: Any) -> list[dict]:
    assignments = getattr(resp, "privilege_assignments", None) or []
    result = []
    for pa in assignments:
        privileges = []
        for p in getattr(pa, "privileges", None) or []:
            privileges.append(
                {
                    "privilege": enum_val(getattr(p, "privilege", None)),
                    "inherited_from_name": getattr(p, "inherited_from_name", None),
                    "inherited_from_type": enum_val(getattr(p, "inherited_from_type", None)),
                }
            )
        result.append({"principal": getattr(pa, "principal", None), "privileges": privileges})
    return result


class TableWidget(anywidget.AnyWidget):
    """Operational widget for a single Unity Catalog table."""

    _esm = traitlets.Unicode().tag(sync=True)
    _css = traitlets.Unicode("").tag(sync=True)

    table_data = traitlets.Unicode("{}").tag(sync=True)
    sample_data = traitlets.Unicode("{}").tag(sync=True)
    history_data = traitlets.Unicode("{}").tag(sync=True)
    lineage_data = traitlets.Unicode("{}").tag(sync=True)
    permissions_data = traitlets.Unicode("{}").tag(sync=True)
    loading = traitlets.Bool(False).tag(sync=True)
    error_message = traitlets.Unicode("").tag(sync=True)
    request = traitlets.Unicode("").tag(sync=True)

    def __init__(self, full_name: str, workspace_client: Any = None, **kwargs: Any) -> None:
        esm_content = _ESM_PATH.read_text()
        super().__init__(_esm=esm_content, **kwargs)
        self._ws = workspace_client
        self._spark = None
        self._full_name = full_name
        self.observe(self._handle_request, names=["request"])
        self._load_table()

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
            self._load_table()
        elif action == "get_sample_data":
            self._load_sample_data(
                limit=req.get("limit"),
                sort_column=req.get("sort_column"),
                sort_order=req.get("sort_order"),
                mode=req.get("mode"),
                filter_expr=req.get("filter"),
            )
        elif action == "get_history":
            self._load_history(limit=req.get("limit"))
        elif action == "get_permissions":
            self._load_permissions()
        elif action == "get_lineage":
            self._load_lineage()
        elif action == "get_column_lineage":
            self._load_column_lineage(req.get("column_name"))

    def _load_table(self) -> None:
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            table = ws.tables.get(self._full_name)
            self.table_data = json.dumps(_serialize_table(table))
        except Exception as exc:
            LOGGER.debug("Failed to get table %s", self._full_name, exc_info=True)
            self.error_message = f"Failed to get table: {exc}"
        finally:
            self.loading = False

    def _load_sample_data(
        self,
        limit: int | None = None,
        sort_column: str | None = None,
        sort_order: str | None = None,
        mode: str | None = None,
        filter_expr: str | None = None,
    ) -> None:
        self.loading = True
        self.error_message = ""
        try:
            spark = self._get_spark()
            if spark is None:
                self.error_message = "Spark session not available for sample data."
                self.loading = False
                return
            parts = self._full_name.split(".")
            quoted = ".".join(f"`{p}`" for p in parts)

            try:
                n = int(limit) if limit is not None else 50
            except (TypeError, ValueError):
                n = 50
            n = max(1, min(n, 10000))

            order = (sort_order or "asc").lower()
            if order not in ("asc", "desc"):
                order = "asc"
            # "last N" is just the inverse sort order
            if (mode or "first").lower() == "last":
                order = "desc" if order == "asc" else "asc"

            sql = f"SELECT * FROM {quoted}"
            if filter_expr and filter_expr.strip():
                sql += f" WHERE {filter_expr.strip()}"
            if sort_column:
                sql += f" ORDER BY `{sort_column}` {order.upper()}"
            sql += f" LIMIT {n}"

            df = spark.sql(sql)
            col_names = df.columns
            rows = [[str(v) if v is not None else None for v in row] for row in df.collect()]
            # If "last N" with a sort column, reverse so user sees ascending again
            if sort_column and (mode or "first").lower() == "last":
                rows.reverse()
            self.sample_data = json.dumps(
                {
                    "table": self._full_name,
                    "columns": col_names,
                    "rows": rows,
                    "limit": n,
                    "sort_column": sort_column,
                    "sort_order": (sort_order or "asc").lower(),
                    "mode": (mode or "first").lower(),
                    "filter": filter_expr or "",
                    "sql": sql,
                }
            )
        except Exception as exc:
            LOGGER.debug("Failed to get sample data for %s", self._full_name, exc_info=True)
            self.error_message = f"Failed to get sample data: {exc}"
        finally:
            self.loading = False

    def _load_history(self, limit: int | None = None) -> None:
        self.loading = True
        self.error_message = ""
        try:
            spark = self._get_spark()
            if spark is None:
                self.error_message = "Spark session not available for table history."
                self.loading = False
                return
            parts = self._full_name.split(".")
            quoted = ".".join(f"`{p}`" for p in parts)
            try:
                n = int(limit) if limit is not None else 50
            except (TypeError, ValueError):
                n = 50
            n = max(1, min(n, 1000))
            df = spark.sql(f"DESCRIBE HISTORY {quoted} LIMIT {n}")
            col_names = df.columns
            rows = []
            for row in df.collect():
                out = []
                for v in row:
                    if v is None:
                        out.append(None)
                    elif isinstance(v, (str, int, float, bool)):
                        out.append(v)
                    elif isinstance(v, dict):
                        out.append({str(k): (str(vv) if vv is not None else None) for k, vv in v.items()})
                    elif isinstance(v, list):
                        out.append([str(x) if x is not None else None for x in v])
                    else:
                        out.append(str(v))
                rows.append(out)
            self.history_data = json.dumps({"table": self._full_name, "columns": col_names, "rows": rows, "limit": n})
        except Exception as exc:
            LOGGER.debug("Failed to get history for %s", self._full_name, exc_info=True)
            self.error_message = f"Failed to get history: {exc}"
        finally:
            self.loading = False

    def _load_permissions(self) -> None:
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            resp = ws.grants.get_effective(securable_type="TABLE", full_name=self._full_name)
            self.permissions_data = json.dumps(
                {
                    "full_name": self._full_name,
                    "permissions": _serialize_permissions(resp),
                }
            )
        except Exception as exc:
            LOGGER.debug("Failed to get permissions for %s", self._full_name, exc_info=True)
            self.error_message = f"Failed to get permissions: {exc}"
        finally:
            self.loading = False

    def _load_lineage(self) -> None:
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            resp = ws.api_client.do(
                "GET", "/api/2.0/lineage-tracking/table-lineage", query={"table_name": self._full_name}
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
                {"type": "table", "table": self._full_name, "upstream": upstream, "downstream": downstream}
            )
        except Exception as exc:
            LOGGER.debug("Failed to get lineage for %s", self._full_name, exc_info=True)
            self.error_message = f"Failed to get lineage: {exc}"
        finally:
            self.loading = False

    def _load_column_lineage(self, column_name: str | None = None) -> None:
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            query: dict[str, Any] = {"table_name": self._full_name}
            if column_name:
                query["column_name"] = column_name
            resp = ws.api_client.do("GET", "/api/2.0/lineage-tracking/column-lineage", query=query)
            upstream_cols = [
                {
                    "name": e.get("name"),
                    "catalog_name": e.get("catalog_name"),
                    "schema_name": e.get("schema_name"),
                    "table_name": e.get("table_name"),
                }
                for e in resp.get("upstream_cols") or []
            ]
            downstream_cols = [
                {
                    "name": e.get("name"),
                    "catalog_name": e.get("catalog_name"),
                    "schema_name": e.get("schema_name"),
                    "table_name": e.get("table_name"),
                }
                for e in resp.get("downstream_cols") or []
            ]
            self.lineage_data = json.dumps(
                {
                    "type": "column",
                    "table": self._full_name,
                    "column": column_name,
                    "upstream_cols": upstream_cols,
                    "downstream_cols": downstream_cols,
                }
            )
        except Exception as exc:
            LOGGER.debug("Failed to get column lineage", exc_info=True)
            self.error_message = f"Failed to get column lineage: {exc}"
        finally:
            self.loading = False
