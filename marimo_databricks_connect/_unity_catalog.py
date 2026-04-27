"""Anywidget for browsing Databricks Unity Catalog.

Browse catalogs, schemas, tables, columns, lineage, permissions,
external locations, storage credentials, connections, volumes,
and external metadata in a unified tabbed interface.

Usage in a marimo notebook::

    from marimo_databricks_connect import unity_catalog_widget
    widget = unity_catalog_widget()
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

_ESM_PATH = pathlib.Path(__file__).parent / "_unity_catalog_frontend.js"

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


def _safe_dict(obj: Any) -> dict:
    """Convert a mapping-like object to a plain dict, or return {}."""
    if obj is None:
        return {}
    try:
        return dict(obj)
    except Exception:
        return {}


# --------------------------------------------------------------------------- #
# Serializers                                                                  #
# --------------------------------------------------------------------------- #


def _serialize_catalog(c: Any) -> dict:
    return {
        "name": getattr(c, "name", None),
        "comment": getattr(c, "comment", None),
        "owner": getattr(c, "owner", None),
        "catalog_type": _enum_val(getattr(c, "catalog_type", None)),
        "isolation_mode": _enum_val(getattr(c, "isolation_mode", None)),
        "created_at": _ms_to_iso(getattr(c, "created_at", None)),
        "created_by": getattr(c, "created_by", None),
        "updated_at": _ms_to_iso(getattr(c, "updated_at", None)),
        "updated_by": getattr(c, "updated_by", None),
        "connection_name": getattr(c, "connection_name", None),
        "storage_location": getattr(c, "storage_location", None),
        "storage_root": getattr(c, "storage_root", None),
        "provider_name": getattr(c, "provider_name", None),
        "share_name": getattr(c, "share_name", None),
        "properties": _safe_dict(getattr(c, "properties", None)),
    }


def _serialize_schema(s: Any) -> dict:
    return {
        "name": getattr(s, "name", None),
        "catalog_name": getattr(s, "catalog_name", None),
        "full_name": getattr(s, "full_name", None),
        "comment": getattr(s, "comment", None),
        "owner": getattr(s, "owner", None),
        "created_at": _ms_to_iso(getattr(s, "created_at", None)),
        "created_by": getattr(s, "created_by", None),
        "updated_at": _ms_to_iso(getattr(s, "updated_at", None)),
        "updated_by": getattr(s, "updated_by", None),
        "storage_location": getattr(s, "storage_location", None),
        "storage_root": getattr(s, "storage_root", None),
        "properties": _safe_dict(getattr(s, "properties", None)),
    }


def _serialize_table(t: Any) -> dict:
    cols = []
    if getattr(t, "columns", None):
        for c in t.columns:
            cols.append(
                {
                    "name": getattr(c, "name", None),
                    "type_text": getattr(c, "type_text", None),
                    "type_name": _enum_val(getattr(c, "type_name", None)),
                    "comment": getattr(c, "comment", None),
                    "nullable": getattr(c, "nullable", None),
                    "position": getattr(c, "position", None),
                    "partition_index": getattr(c, "partition_index", None),
                }
            )
    return {
        "name": getattr(t, "name", None),
        "catalog_name": getattr(t, "catalog_name", None),
        "schema_name": getattr(t, "schema_name", None),
        "full_name": getattr(t, "full_name", None),
        "table_type": _enum_val(getattr(t, "table_type", None)),
        "data_source_format": _enum_val(getattr(t, "data_source_format", None)),
        "comment": getattr(t, "comment", None),
        "owner": getattr(t, "owner", None),
        "created_at": _ms_to_iso(getattr(t, "created_at", None)),
        "created_by": getattr(t, "created_by", None),
        "updated_at": _ms_to_iso(getattr(t, "updated_at", None)),
        "updated_by": getattr(t, "updated_by", None),
        "storage_location": getattr(t, "storage_location", None),
        "storage_credential_name": getattr(t, "storage_credential_name", None),
        "view_definition": getattr(t, "view_definition", None),
        "sql_path": getattr(t, "sql_path", None),
        "table_id": getattr(t, "table_id", None),
        "columns": cols,
        "properties": _safe_dict(getattr(t, "properties", None)),
    }


def _serialize_table_summary(t: Any) -> dict:
    """Lightweight table serialization for list views (no columns)."""
    return {
        "name": getattr(t, "name", None),
        "catalog_name": getattr(t, "catalog_name", None),
        "schema_name": getattr(t, "schema_name", None),
        "full_name": getattr(t, "full_name", None),
        "table_type": _enum_val(getattr(t, "table_type", None)),
        "data_source_format": _enum_val(getattr(t, "data_source_format", None)),
        "comment": getattr(t, "comment", None),
        "owner": getattr(t, "owner", None),
        "created_at": _ms_to_iso(getattr(t, "created_at", None)),
        "created_by": getattr(t, "created_by", None),
        "updated_at": _ms_to_iso(getattr(t, "updated_at", None)),
    }


def _serialize_volume(v: Any) -> dict:
    return {
        "name": getattr(v, "name", None),
        "catalog_name": getattr(v, "catalog_name", None),
        "schema_name": getattr(v, "schema_name", None),
        "full_name": getattr(v, "full_name", None),
        "volume_type": _enum_val(getattr(v, "volume_type", None)),
        "comment": getattr(v, "comment", None),
        "owner": getattr(v, "owner", None),
        "storage_location": getattr(v, "storage_location", None),
        "created_at": _ms_to_iso(getattr(v, "created_at", None)),
        "created_by": getattr(v, "created_by", None),
        "updated_at": _ms_to_iso(getattr(v, "updated_at", None)),
        "updated_by": getattr(v, "updated_by", None),
    }


def _serialize_external_location(loc: Any) -> dict:
    return {
        "name": getattr(loc, "name", None),
        "url": getattr(loc, "url", None),
        "credential_name": getattr(loc, "credential_name", None),
        "comment": getattr(loc, "comment", None),
        "owner": getattr(loc, "owner", None),
        "read_only": getattr(loc, "read_only", None),
        "created_at": _ms_to_iso(getattr(loc, "created_at", None)),
        "created_by": getattr(loc, "created_by", None),
        "updated_at": _ms_to_iso(getattr(loc, "updated_at", None)),
        "updated_by": getattr(loc, "updated_by", None),
        "isolation_mode": _enum_val(getattr(loc, "isolation_mode", None)),
        "fallback": getattr(loc, "fallback", None),
    }


def _serialize_storage_credential(cred: Any) -> dict:
    # Determine credential type
    cred_type = "unknown"
    cred_detail = None
    if getattr(cred, "azure_managed_identity", None):
        cred_type = "Azure Managed Identity"
        ami = cred.azure_managed_identity
        cred_detail = getattr(ami, "access_connector_id", None) or getattr(ami, "credential_id", None)
    elif getattr(cred, "azure_service_principal", None):
        cred_type = "Azure Service Principal"
        sp = cred.azure_service_principal
        cred_detail = getattr(sp, "application_id", None)
    elif getattr(cred, "aws_iam_role", None):
        cred_type = "AWS IAM Role"
        role = cred.aws_iam_role
        cred_detail = getattr(role, "role_arn", None)
    elif getattr(cred, "databricks_gcp_service_account", None):
        cred_type = "GCP Service Account"
        gcp = cred.databricks_gcp_service_account
        cred_detail = getattr(gcp, "email", None)
    return {
        "name": getattr(cred, "name", None),
        "id": getattr(cred, "id", None),
        "comment": getattr(cred, "comment", None),
        "owner": getattr(cred, "owner", None),
        "read_only": getattr(cred, "read_only", None),
        "used_for_managed_storage": getattr(cred, "used_for_managed_storage", None),
        "credential_type": cred_type,
        "credential_detail": cred_detail,
        "isolation_mode": _enum_val(getattr(cred, "isolation_mode", None)),
        "created_at": _ms_to_iso(getattr(cred, "created_at", None)),
        "created_by": getattr(cred, "created_by", None),
        "updated_at": _ms_to_iso(getattr(cred, "updated_at", None)),
        "updated_by": getattr(cred, "updated_by", None),
    }


def _serialize_connection(conn: Any) -> dict:
    return {
        "name": getattr(conn, "name", None),
        "connection_id": getattr(conn, "connection_id", None),
        "connection_type": _enum_val(getattr(conn, "connection_type", None)),
        "comment": getattr(conn, "comment", None),
        "owner": getattr(conn, "owner", None),
        "url": getattr(conn, "url", None),
        "read_only": getattr(conn, "read_only", None),
        "credential_type": _enum_val(getattr(conn, "credential_type", None)),
        "created_at": _ms_to_iso(getattr(conn, "created_at", None)),
        "created_by": getattr(conn, "created_by", None),
        "updated_at": _ms_to_iso(getattr(conn, "updated_at", None)),
        "updated_by": getattr(conn, "updated_by", None),
        "properties": _safe_dict(getattr(conn, "properties", None)),
    }


def _serialize_external_metadata(em: Any) -> dict:
    cols = []
    if getattr(em, "columns", None):
        for c in em.columns:
            cols.append(
                {
                    "name": getattr(c, "name", None),
                    "type_text": getattr(c, "type_text", None) or getattr(c, "type_name", None),
                    "comment": getattr(c, "comment", None),
                }
            )
    return {
        "name": getattr(em, "name", None),
        "id": getattr(em, "id", None),
        "entity_type": _enum_val(getattr(em, "entity_type", None)),
        "system_type": _enum_val(getattr(em, "system_type", None)),
        "description": getattr(em, "description", None),
        "url": getattr(em, "url", None),
        "owner": getattr(em, "owner", None),
        "created_by": getattr(em, "created_by", None),
        "create_time": _ms_to_iso(getattr(em, "create_time", None)),
        "updated_by": getattr(em, "updated_by", None),
        "update_time": _ms_to_iso(getattr(em, "update_time", None)),
        "columns": cols,
        "properties": _safe_dict(getattr(em, "properties", None)),
    }


def _serialize_permissions(resp: Any) -> list[dict]:
    assignments = getattr(resp, "privilege_assignments", None) or []
    result = []
    for pa in assignments:
        privileges = []
        for p in getattr(pa, "privileges", None) or []:
            privileges.append(
                {
                    "privilege": _enum_val(getattr(p, "privilege", None)),
                    "inherited_from_name": getattr(p, "inherited_from_name", None),
                    "inherited_from_type": _enum_val(getattr(p, "inherited_from_type", None)),
                }
            )
        result.append(
            {
                "principal": getattr(pa, "principal", None),
                "privileges": privileges,
            }
        )
    return result


# --------------------------------------------------------------------------- #
# Widget                                                                       #
# --------------------------------------------------------------------------- #


class UnityCatalogWidget(anywidget.AnyWidget):
    """Anywidget for browsing Databricks Unity Catalog."""

    _esm = traitlets.Unicode().tag(sync=True)
    _css = traitlets.Unicode("").tag(sync=True)

    # Data traits — JSON strings synced to the frontend
    catalogs_data = traitlets.Unicode("[]").tag(sync=True)
    schemas_data = traitlets.Unicode("[]").tag(sync=True)
    tables_data = traitlets.Unicode("[]").tag(sync=True)
    table_detail = traitlets.Unicode("{}").tag(sync=True)
    volumes_data = traitlets.Unicode("[]").tag(sync=True)
    sample_data = traitlets.Unicode("{}").tag(sync=True)
    permissions_data = traitlets.Unicode("[]").tag(sync=True)
    lineage_data = traitlets.Unicode("{}").tag(sync=True)
    external_locations_data = traitlets.Unicode("[]").tag(sync=True)
    location_contents_data = traitlets.Unicode("[]").tag(sync=True)
    storage_credentials_data = traitlets.Unicode("[]").tag(sync=True)
    connections_data = traitlets.Unicode("[]").tag(sync=True)
    external_metadata_data = traitlets.Unicode("[]").tag(sync=True)
    loading = traitlets.Bool(False).tag(sync=True)
    error_message = traitlets.Unicode("").tag(sync=True)

    # Requests from the frontend
    request = traitlets.Unicode("").tag(sync=True)

    def __init__(self, workspace_client: Any = None, **kwargs: Any) -> None:
        esm_content = _ESM_PATH.read_text()
        super().__init__(_esm=esm_content, **kwargs)
        self._ws = workspace_client
        self._spark = None
        self.observe(self._handle_request, names=["request"])
        self._load_catalogs()

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
            LOGGER.debug("Could not create DatabricksSession for sample data", exc_info=True)
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
        try:
            if action == "list_catalogs":
                self._load_catalogs()
            elif action == "list_schemas":
                self._load_schemas(req["catalog_name"])
            elif action == "list_tables":
                self._load_tables(req["catalog_name"], req["schema_name"])
            elif action == "get_table":
                self._load_table_detail(req["full_name"])
            elif action == "list_volumes":
                self._load_volumes(req["catalog_name"], req["schema_name"])
            elif action == "get_sample_data":
                self._load_sample_data(req["full_name"])
            elif action == "get_permissions":
                self._load_permissions(req["securable_type"], req["full_name"])
            elif action == "get_table_lineage":
                self._load_table_lineage(req["full_name"])
            elif action == "get_column_lineage":
                self._load_column_lineage(req["full_name"], req.get("column_name"))
            elif action == "list_external_locations":
                self._load_external_locations()
            elif action == "browse_location":
                self._browse_location(req["url"])
            elif action == "list_storage_credentials":
                self._load_storage_credentials()
            elif action == "list_connections":
                self._load_connections()
            elif action == "list_external_metadata":
                self._load_external_metadata()
        except Exception as exc:
            LOGGER.debug("Error handling request %s", action, exc_info=True)
            self.error_message = f"Error: {exc}"
            self.loading = False

    # ---- Catalogs / Schemas / Tables ---- #

    def _load_catalogs(self) -> None:
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            catalogs = list(ws.catalogs.list())
            self.catalogs_data = json.dumps([_serialize_catalog(c) for c in catalogs])
        except Exception as exc:
            LOGGER.debug("Failed to list catalogs", exc_info=True)
            self.error_message = f"Failed to list catalogs: {exc}"
        finally:
            self.loading = False

    def _load_schemas(self, catalog_name: str) -> None:
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            schemas = list(ws.schemas.list(catalog_name=catalog_name))
            self.schemas_data = json.dumps([_serialize_schema(s) for s in schemas])
        except Exception as exc:
            LOGGER.debug("Failed to list schemas for %s", catalog_name, exc_info=True)
            self.error_message = f"Failed to list schemas: {exc}"
        finally:
            self.loading = False

    def _load_tables(self, catalog_name: str, schema_name: str) -> None:
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            tables = list(
                ws.tables.list(
                    catalog_name=catalog_name,
                    schema_name=schema_name,
                    max_results=200,
                )
            )
            self.tables_data = json.dumps([_serialize_table_summary(t) for t in tables])
        except Exception as exc:
            LOGGER.debug("Failed to list tables for %s.%s", catalog_name, schema_name, exc_info=True)
            self.error_message = f"Failed to list tables: {exc}"
        finally:
            self.loading = False

    def _load_table_detail(self, full_name: str) -> None:
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            table = ws.tables.get(full_name)
            self.table_detail = json.dumps(_serialize_table(table))
        except Exception as exc:
            LOGGER.debug("Failed to get table %s", full_name, exc_info=True)
            self.error_message = f"Failed to get table: {exc}"
        finally:
            self.loading = False

    def _load_volumes(self, catalog_name: str, schema_name: str) -> None:
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            volumes = list(
                ws.volumes.list(
                    catalog_name=catalog_name,
                    schema_name=schema_name,
                )
            )
            self.volumes_data = json.dumps([_serialize_volume(v) for v in volumes])
        except Exception as exc:
            LOGGER.debug("Failed to list volumes", exc_info=True)
            self.error_message = f"Failed to list volumes: {exc}"
        finally:
            self.loading = False

    # ---- Sample data ---- #

    def _load_sample_data(self, full_name: str) -> None:
        self.loading = True
        self.error_message = ""
        try:
            spark = self._get_spark()
            if spark is None:
                # Use a statement execution if spark isn't available
                self.error_message = "Spark session not available for sample data. Use databricks-connect."
                self.loading = False
                return

            # Use backtick quoting for the full name
            parts = full_name.split(".")
            quoted = ".".join(f"`{p}`" for p in parts)
            df = spark.sql(f"SELECT * FROM {quoted} LIMIT 50")
            col_names = df.columns
            rows = []
            for row in df.collect():
                rows.append([str(v) if v is not None else None for v in row])
            self.sample_data = json.dumps(
                {
                    "table": full_name,
                    "columns": col_names,
                    "rows": rows,
                }
            )
        except Exception as exc:
            LOGGER.debug("Failed to get sample data for %s", full_name, exc_info=True)
            self.error_message = f"Failed to get sample data: {exc}"
        finally:
            self.loading = False

    # ---- Permissions ---- #

    def _load_permissions(self, securable_type: str, full_name: str) -> None:
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            resp = ws.grants.get_effective(
                securable_type=securable_type,
                full_name=full_name,
            )
            self.permissions_data = json.dumps(
                {
                    "securable_type": securable_type,
                    "full_name": full_name,
                    "permissions": _serialize_permissions(resp),
                }
            )
        except Exception as exc:
            LOGGER.debug("Failed to get permissions for %s %s", securable_type, full_name, exc_info=True)
            self.error_message = f"Failed to get permissions: {exc}"
        finally:
            self.loading = False

    # ---- Lineage ---- #

    def _load_table_lineage(self, full_name: str) -> None:
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            resp = ws.api_client.do(
                "GET",
                "/api/2.0/lineage-tracking/table-lineage",
                query={"table_name": full_name},
            )
            # resp is a dict with upstream_tables and downstream_tables
            upstream = []
            for entry in resp.get("upstreams") or []:
                tinfo = entry.get("tableInfo") or {}
                upstream.append(
                    {
                        "name": tinfo.get("name"),
                        "catalog_name": tinfo.get("catalog_name"),
                        "schema_name": tinfo.get("schema_name"),
                        "table_type": tinfo.get("table_type"),
                    }
                )
            downstream = []
            for entry in resp.get("downstreams") or []:
                tinfo = entry.get("tableInfo") or {}
                downstream.append(
                    {
                        "name": tinfo.get("name"),
                        "catalog_name": tinfo.get("catalog_name"),
                        "schema_name": tinfo.get("schema_name"),
                        "table_type": tinfo.get("table_type"),
                    }
                )
            self.lineage_data = json.dumps(
                {
                    "type": "table",
                    "table": full_name,
                    "upstream": upstream,
                    "downstream": downstream,
                }
            )
        except Exception as exc:
            LOGGER.debug("Failed to get table lineage for %s", full_name, exc_info=True)
            self.error_message = f"Failed to get table lineage: {exc}"
        finally:
            self.loading = False

    def _load_column_lineage(self, full_name: str, column_name: str | None = None) -> None:
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            query: dict[str, Any] = {"table_name": full_name}
            if column_name:
                query["column_name"] = column_name
            resp = ws.api_client.do(
                "GET",
                "/api/2.0/lineage-tracking/column-lineage",
                query=query,
            )
            upstream_cols = []
            for entry in resp.get("upstream_cols") or []:
                upstream_cols.append(
                    {
                        "name": entry.get("name"),
                        "catalog_name": entry.get("catalog_name"),
                        "schema_name": entry.get("schema_name"),
                        "table_name": entry.get("table_name"),
                    }
                )
            downstream_cols = []
            for entry in resp.get("downstream_cols") or []:
                downstream_cols.append(
                    {
                        "name": entry.get("name"),
                        "catalog_name": entry.get("catalog_name"),
                        "schema_name": entry.get("schema_name"),
                        "table_name": entry.get("table_name"),
                    }
                )
            self.lineage_data = json.dumps(
                {
                    "type": "column",
                    "table": full_name,
                    "column": column_name,
                    "upstream_cols": upstream_cols,
                    "downstream_cols": downstream_cols,
                }
            )
        except Exception as exc:
            LOGGER.debug("Failed to get column lineage for %s", full_name, exc_info=True)
            self.error_message = f"Failed to get column lineage: {exc}"
        finally:
            self.loading = False

    # ---- External locations ---- #

    def _load_external_locations(self) -> None:
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            locs = list(ws.external_locations.list())
            self.external_locations_data = json.dumps([_serialize_external_location(loc) for loc in locs])
        except Exception as exc:
            LOGGER.debug("Failed to list external locations", exc_info=True)
            self.error_message = f"Failed to list external locations: {exc}"
        finally:
            self.loading = False

    def _browse_location(self, url: str) -> None:
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            # Use the files API or dbutils to list
            items: list[dict] = []
            try:
                # Try listing via dbutils (works for abfss/s3/volumes)
                from databricks.connect import DatabricksSession

                spark = self._get_spark()
                if spark:
                    from pyspark.dbutils import DBUtils

                    dbu = DBUtils(spark)
                    file_list = dbu.fs.ls(url)
                    for f in file_list:
                        items.append(
                            {
                                "name": getattr(f, "name", str(f)),
                                "path": getattr(f, "path", ""),
                                "size": getattr(f, "size", 0),
                                "is_dir": getattr(f, "isDir", getattr(f, "is_dir", False)),
                                "modification_time": getattr(f, "modificationTime", None),
                            }
                        )
            except Exception:
                LOGGER.debug("dbutils listing failed for %s, trying files API", url, exc_info=True)
                # Fallback: use workspace REST API for /Volumes paths
                if url.startswith("/Volumes"):
                    resp = ws.api_client.do(
                        "GET",
                        f"/api/2.0/fs/directories{url}",
                    )
                    for entry in resp.get("contents", []):
                        items.append(
                            {
                                "name": entry.get("name", ""),
                                "path": entry.get("path", ""),
                                "size": entry.get("file_size", 0),
                                "is_dir": entry.get("is_directory", False),
                                "modification_time": entry.get("last_modified", None),
                            }
                        )
                else:
                    self.error_message = f"Cannot browse {url}: dbutils unavailable and path is not a /Volumes path."
            self.location_contents_data = json.dumps(
                {
                    "url": url,
                    "items": items,
                }
            )
        except Exception as exc:
            LOGGER.debug("Failed to browse %s", url, exc_info=True)
            self.error_message = f"Failed to browse location: {exc}"
        finally:
            self.loading = False

    # ---- Storage credentials ---- #

    def _load_storage_credentials(self) -> None:
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            creds = list(ws.storage_credentials.list())
            self.storage_credentials_data = json.dumps([_serialize_storage_credential(c) for c in creds])
        except Exception as exc:
            LOGGER.debug("Failed to list storage credentials", exc_info=True)
            self.error_message = f"Failed to list storage credentials: {exc}"
        finally:
            self.loading = False

    # ---- Connections ---- #

    def _load_connections(self) -> None:
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            conns = list(ws.connections.list())
            self.connections_data = json.dumps([_serialize_connection(c) for c in conns])
        except Exception as exc:
            LOGGER.debug("Failed to list connections", exc_info=True)
            self.error_message = f"Failed to list connections: {exc}"
        finally:
            self.loading = False

    # ---- External metadata ---- #

    def _load_external_metadata(self) -> None:
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            items = list(ws.external_metadata.list_external_metadata())
            self.external_metadata_data = json.dumps([_serialize_external_metadata(em) for em in items])
        except Exception as exc:
            LOGGER.debug("Failed to list external metadata", exc_info=True)
            self.error_message = f"Failed to list external metadata: {exc}"
        finally:
            self.loading = False
