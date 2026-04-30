"""Spark Connect SQL engine adapter for marimo.

In addition to running SQL, this engine feeds marimo's data sources panel and
in-cell SQL **autocomplete** (catalog / schema / table / column suggestions).
Discovery is done in bulk via Unity Catalog's ``<catalog>.information_schema``
views (one query per catalog instead of N ``SHOW`` / ``DESCRIBE`` round trips)
and cached in-process with a TTL.

Use :meth:`SparkConnectEngine.prefetch` (or the package-level
:func:`marimo_databricks_connect.prefetch`) to warm the cache eagerly so SQL
completion is fully populated before you start typing.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Literal

from marimo._data.models import Database, DataTable, DataTableColumn, Schema
from marimo._sql.engines.types import InferenceConfig, SQLConnection
from marimo._sql.utils import sql_type_to_data_type
from marimo._types.ids import VariableName

LOGGER = logging.getLogger(__name__)

# In-process cache TTL for catalog metadata.  UC schemas don't change often;
# 10 minutes keeps the panel snappy while still picking up new tables in a
# typical session.  Bypass with ``refresh()``.
_CACHE_TTL_SECONDS = 600


class _CatalogCache:
    """Thread-safe TTL cache for one catalog's ``{schema: {table: [cols]}}``.

    Each entry is fetched once via a single ``information_schema.columns`` query
    (or a SHOW/DESCRIBE fallback) and reused for the TTL window.
    """

    def __init__(self, ttl: float = _CACHE_TTL_SECONDS) -> None:
        self._ttl = ttl
        self._lock = threading.Lock()
        # catalog -> (timestamp, {schema: {table: [DataTableColumn]}})
        self._data: dict[str, tuple[float, dict[str, dict[str, list[DataTableColumn]]]]] = {}

    def get(self, catalog: str) -> dict[str, dict[str, list[DataTableColumn]]] | None:
        with self._lock:
            entry = self._data.get(catalog)
            if entry is None:
                return None
            ts, value = entry
            if time.time() - ts > self._ttl:
                del self._data[catalog]
                return None
            return value

    def set(self, catalog: str, value: dict[str, dict[str, list[DataTableColumn]]]) -> None:
        with self._lock:
            self._data[catalog] = (time.time(), value)

    def invalidate(self, catalog: str | None = None) -> None:
        with self._lock:
            if catalog is None:
                self._data.clear()
            else:
                self._data.pop(catalog, None)


class SparkConnectEngine(SQLConnection[Any]):
    """A marimo SQL engine backed by a ``pyspark.sql.SparkSession``.

    Compatible with both classic ``SparkSession`` (Databricks runtime) and
    ``DatabricksSession`` from ``databricks-connect`` — the latter is a subclass
    of ``SparkSession``.

    ``execute()`` returns the lazy Spark ``DataFrame`` produced by ``spark.sql``;
    marimo's built-in formatter renders it without materialising rows.  We do
    not inject a ``LIMIT`` — Spark / the user is responsible for shaping the
    result.
    """

    def __init__(self, connection: Any, engine_name: VariableName | None = None) -> None:
        super().__init__(connection, engine_name)
        self._catalog_cache = _CatalogCache()

    # -- BaseEngine -------------------------------------------------------

    @property
    def source(self) -> str:
        return "databricks-connect"

    @property
    def dialect(self) -> str:
        return "databricks"

    @staticmethod
    def is_compatible(var: Any) -> bool:
        # databricks-connect returns ``pyspark.sql.connect.session.SparkSession``
        # which is a *separate* class from the classic ``pyspark.sql.SparkSession``
        # (they share an API but not an inheritance chain). Match either.
        candidates: list[type] = []
        try:
            from pyspark.sql import SparkSession as ClassicSparkSession

            candidates.append(ClassicSparkSession)
        except ImportError:  # pragma: no cover
            pass
        try:
            from pyspark.sql.connect.session import (
                SparkSession as ConnectSparkSession,
            )

            candidates.append(ConnectSparkSession)
        except ImportError:  # pragma: no cover
            pass
        if not candidates:
            return False
        return isinstance(var, tuple(candidates))

    # -- EngineCatalog ----------------------------------------------------

    @property
    def inference_config(self) -> InferenceConfig:
        # We resolve ``"auto"`` ourselves in :meth:`_resolve_should_auto_discover`
        # below.  By default we eagerly surface schemas + tables (cheap with
        # bulk ``information_schema`` queries + cache) so SQL completion has
        # something to chew on, and lazy-load columns unless the user has
        # narrowed scope with ``include_catalogs(...)``.
        return InferenceConfig(
            auto_discover_schemas="auto",
            auto_discover_tables="auto",
            auto_discover_columns="auto",
        )

    def get_default_database(self) -> str | None:
        return self._scalar("SELECT current_catalog()")

    def get_default_schema(self) -> str | None:
        return self._scalar("SELECT current_schema()")

    # -- "auto" resolution -----------------------------------------------

    def _scope_is_narrow(self) -> bool:
        """``True`` if the user has narrowed catalog visibility (via
        ``include_catalogs(...)``), in which case it's cheap & desirable to
        eagerly fetch column metadata for completion.
        """
        from ._filter import _filter

        return bool(_filter.includes) and not _filter.show_all

    def _resolve_should_auto_discover(
        self,
        value: bool | Literal["auto"],
        *,
        kind: Literal["schemas", "tables", "columns"],
    ) -> bool:
        if value is True:
            return True
        if value is False:
            return False
        # value == "auto"
        if kind in ("schemas", "tables"):
            # Cheap with a single ``information_schema`` query + cache.
            return True
        # columns: only eagerly load when the user has narrowed scope; otherwise
        # defer to per-table DESCRIBE on demand to avoid a multi-second probe of
        # every column in the workspace.
        return self._scope_is_narrow()

    # -- Discovery (catalogs / schemas / tables / columns) ---------------

    def get_databases(
        self,
        *,
        include_schemas: bool | str,
        include_tables: bool | str,
        include_table_details: bool | str,
    ) -> list[Database]:
        want_schemas = self._resolve_should_auto_discover(include_schemas, kind="schemas")
        want_tables = self._resolve_should_auto_discover(include_tables, kind="tables")
        want_cols = self._resolve_should_auto_discover(include_table_details, kind="columns")

        databases: list[Database] = []
        for catalog in self._list_catalogs():
            schemas: list[Schema] = []
            if want_schemas:
                schemas = self._build_schemas(
                    catalog,
                    include_tables=want_tables,
                    include_table_details=want_cols,
                )
            databases.append(
                Database(
                    name=catalog,
                    dialect=self.dialect,
                    schemas=schemas,
                    engine=self._engine_name,
                )
            )
        return databases

    def get_schemas(
        self,
        *,
        database: str | None,
        include_tables: bool,
        include_table_details: bool,
    ) -> list[Schema]:
        database = database or self.get_default_database()
        if not database:
            return []
        return self._build_schemas(
            database,
            include_tables=include_tables,
            include_table_details=include_table_details,
        )

    def get_tables_in_schema(
        self,
        *,
        schema: str,
        database: str,
        include_table_details: bool,
    ) -> list[DataTable]:
        catalog_meta = self._catalog_metadata(database)
        if catalog_meta is not None:
            schema_meta = catalog_meta.get(schema, {})
            return [
                self._make_table(database, schema, name, cols if include_table_details else [])
                for name, cols in schema_meta.items()
            ]
        # Fallback path (information_schema unavailable).
        out: list[DataTable] = []
        for name in self._list_tables_legacy(database, schema):
            cols: list[DataTableColumn] = []
            if include_table_details:
                detail = self.get_table_details(table_name=name, schema_name=schema, database_name=database)
                if detail is not None:
                    out.append(detail)
                    continue
            out.append(self._make_table(database, schema, name, cols))
        return out

    def get_table_details(
        self,
        *,
        table_name: str,
        schema_name: str,
        database_name: str,
    ) -> DataTable | None:
        # Prefer the cached bulk fetch when available.
        catalog_meta = self._catalog_metadata(database_name)
        if catalog_meta is not None:
            cols = catalog_meta.get(schema_name, {}).get(table_name)
            if cols is not None:
                return self._make_table(database_name, schema_name, table_name, cols)

        # Fallback: per-table DESCRIBE.
        fqn = f"`{database_name}`.`{schema_name}`.`{table_name}`"
        try:
            rows = self._connection.sql(f"DESCRIBE TABLE {fqn}").collect()
        except Exception:
            LOGGER.debug("DESCRIBE TABLE %s failed", fqn, exc_info=True)
            return None

        columns: list[DataTableColumn] = []
        for row in rows:
            d = row.asDict()
            name = (d.get("col_name") or "").strip()
            dtype = (d.get("data_type") or "").strip()
            # DESCRIBE output ends with a blank row then partition / detailed
            # metadata sections (rows whose col_name starts with "#" or is empty).
            if not name or name.startswith("#"):
                break
            columns.append(
                DataTableColumn(
                    name=name,
                    type=sql_type_to_data_type(dtype),
                    external_type=dtype,
                    sample_values=[],
                )
            )
        return self._make_table(database_name, schema_name, table_name, columns)

    # -- Cache control (public API) --------------------------------------

    def prefetch(self, *catalogs: str) -> dict[str, int]:
        """Eagerly populate the metadata cache for one or more catalogs.

        Useful at notebook startup so SQL completion is fully populated before
        the first keystroke.  If no catalogs are passed, prefetches every
        catalog visible under the current ``include_catalogs`` filter (or just
        the current catalog if no filter is set).

        Returns a ``{catalog: table_count}`` summary.
        """
        targets = list(catalogs) if catalogs else self._list_catalogs()
        summary: dict[str, int] = {}
        for cat in targets:
            meta = self._catalog_metadata(cat, force=True) or {}
            summary[cat] = sum(len(tables) for tables in meta.values())
        return summary

    def refresh(self, catalog: str | None = None) -> None:
        """Invalidate the metadata cache (one catalog, or all if ``None``)."""
        self._catalog_cache.invalidate(catalog)

    # -- QueryEngine ------------------------------------------------------

    def execute(self, query: str) -> Any:
        return self._connection.sql(query)

    # -- internals --------------------------------------------------------

    def _make_table(
        self,
        database: str,
        schema: str,
        name: str,
        columns: list[DataTableColumn],
    ) -> DataTable:
        return DataTable(
            source_type="connection",
            source=self.dialect,
            name=name,
            num_rows=None,
            num_columns=len(columns) or None,
            variable_name=None,
            columns=columns,
            engine=self._engine_name,
        )

    def _build_schemas(
        self,
        catalog: str,
        *,
        include_tables: bool,
        include_table_details: bool,
    ) -> list[Schema]:
        from ._filter import _filter

        catalog_meta = self._catalog_metadata(catalog) if include_tables else None
        if catalog_meta is not None:
            # Bulk path: schemas, tables, and (optionally) columns came from one
            # round-trip and live in cache.
            schemas: list[Schema] = []
            for schema_name in _filter.filter_schemas(catalog, sorted(catalog_meta.keys())):
                tables = [
                    self._make_table(
                        catalog,
                        schema_name,
                        tname,
                        cols if include_table_details else [],
                    )
                    for tname, cols in catalog_meta[schema_name].items()
                ]
                schemas.append(Schema(name=schema_name, tables=tables))
            return schemas

        # Fallback path: SHOW SCHEMAS + (optional) SHOW TABLES + DESCRIBE.
        result: list[Schema] = []
        for schema_name in self._list_schemas(catalog):
            tables: list[DataTable] = []
            if include_tables:
                tables = self.get_tables_in_schema(
                    schema=schema_name,
                    database=catalog,
                    include_table_details=include_table_details,
                )
            result.append(Schema(name=schema_name, tables=tables))
        return result

    # -- Bulk catalog metadata via information_schema --------------------

    def _catalog_metadata(
        self, catalog: str, *, force: bool = False
    ) -> dict[str, dict[str, list[DataTableColumn]]] | None:
        """Return ``{schema: {table: [columns]}}`` for a catalog, or ``None``
        if information_schema isn't usable (legacy ``hive_metastore``,
        permission-denied, ``samples`` demo catalog, etc.).

        Cached per-catalog with a TTL.
        """
        if not force:
            cached = self._catalog_cache.get(catalog)
            if cached is not None:
                return cached

        meta = self._fetch_catalog_metadata(catalog)
        if meta:
            self._catalog_cache.set(catalog, meta)
            return meta
        # Empty result — either the catalog truly has no tables, or
        # information_schema isn't usable here (e.g. ``samples``,
        # ``hive_metastore``, or insufficient privileges).  Either way fall
        # through to the SHOW/DESCRIBE path so we still surface anything
        # there is to surface.
        return None

    def _fetch_catalog_metadata(
        self,
        catalog: str,
    ) -> dict[str, dict[str, list[DataTableColumn]]] | None:
        # One query returns every (schema, table, column) for the catalog,
        # ordered so we preserve column position. ``ordinal_position`` is
        # standard ANSI; ``data_type`` is the human-readable type string.
        query = (
            f"SELECT table_schema, table_name, column_name, data_type, ordinal_position "
            f"FROM `{catalog}`.information_schema.columns "
            f"ORDER BY table_schema, table_name, ordinal_position"
        )
        try:
            rows = self._connection.sql(query).collect()
        except Exception:
            LOGGER.debug(
                "information_schema.columns query failed for catalog %s; falling back to SHOW/DESCRIBE",
                catalog,
                exc_info=True,
            )
            return None

        out: dict[str, dict[str, list[DataTableColumn]]] = {}
        for row in rows:
            try:
                # Tolerate either positional or named access (Spark Row).
                schema = row[0]
                table = row[1]
                colname = row[2]
                dtype = row[3] or ""
            except Exception:
                d = row.asDict() if hasattr(row, "asDict") else {}
                schema = d.get("table_schema")
                table = d.get("table_name")
                colname = d.get("column_name")
                dtype = d.get("data_type") or ""
            if not schema or not table or not colname:
                continue
            out.setdefault(schema, {}).setdefault(table, []).append(
                DataTableColumn(
                    name=colname,
                    type=sql_type_to_data_type(str(dtype)),
                    external_type=str(dtype),
                    sample_values=[],
                )
            )

        # information_schema.columns only returns schemas with at least one
        # table; pick up empty schemas via a cheap second query so the panel
        # still shows them.  Best-effort: ignore errors.
        try:
            schema_rows = self._connection.sql(
                f"SELECT schema_name FROM `{catalog}`.information_schema.schemata"
            ).collect()
            for row in schema_rows:
                name = row[0] if len(row) else None
                if name and name not in out:
                    out[name] = {}
        except Exception:
            LOGGER.debug("information_schema.schemata query failed for %s", catalog, exc_info=True)

        return out

    # -- helpers ----------------------------------------------------------

    def _scalar(self, query: str) -> Any:
        try:
            rows = self._connection.sql(query).collect()
            return rows[0][0] if rows else None
        except Exception:
            LOGGER.debug("scalar query %r failed", query, exc_info=True)
            return None

    def _list_catalogs(self) -> list[str]:
        from ._filter import _filter

        if _filter.show_all or _filter.includes:
            try:
                rows = self._connection.sql("SHOW CATALOGS").collect()
            except Exception:
                LOGGER.debug("SHOW CATALOGS failed", exc_info=True)
                return []
            all_catalogs = [r[0] for r in rows if r and r[0]]
            return _filter.filter_catalogs(all_catalogs, self.get_default_database())

        # Default fast path: don't enumerate all catalogs (UC may have 1000+);
        # surface only the current one.
        return _filter.filter_catalogs(
            [self.get_default_database()] if self.get_default_database() else [],
            self.get_default_database(),
        )

    def _list_schemas(self, catalog: str) -> list[str]:
        try:
            rows = self._connection.sql(f"SHOW SCHEMAS IN `{catalog}`").collect()
        except Exception:
            LOGGER.debug("SHOW SCHEMAS IN %s failed", catalog, exc_info=True)
            return []
        # SHOW SCHEMAS returns a single column historically named "databaseName"
        # or "namespace" depending on runtime version.
        schemas = [r[0] for r in rows if r and r[0]]
        from ._filter import _filter

        return _filter.filter_schemas(catalog, schemas)

    def _list_tables_legacy(self, catalog: str, schema: str) -> list[str]:
        try:
            rows = self._connection.sql(f"SHOW TABLES IN `{catalog}`.`{schema}`").collect()
        except Exception:
            LOGGER.debug("SHOW TABLES IN %s.%s failed", catalog, schema, exc_info=True)
            return []
        out: list[str] = []
        for row in rows:
            d = row.asDict()
            name = d.get("tableName") or (row[1] if len(row) > 1 else None)
            if name:
                out.append(name)
        return out

    # Backwards-compat alias used by older tests.
    _list_tables = _list_tables_legacy
