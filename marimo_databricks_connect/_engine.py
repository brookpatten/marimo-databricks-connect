"""Spark Connect SQL engine adapter for marimo."""

from __future__ import annotations

import logging
from typing import Any

from marimo._data.models import Database, DataTable, DataTableColumn, Schema
from marimo._sql.engines.types import InferenceConfig, SQLConnection
from marimo._sql.utils import sql_type_to_data_type
from marimo._types.ids import VariableName

LOGGER = logging.getLogger(__name__)


class SparkConnectEngine(SQLConnection[Any]):
    """A marimo SQL engine backed by a ``pyspark.sql.SparkSession``.

    Compatible with both classic ``SparkSession`` (Databricks runtime) and
    ``DatabricksSession`` from ``databricks-connect`` — the latter is a subclass
    of ``SparkSession``.

    ``execute()`` returns the lazy Spark ``DataFrame`` produced by ``spark.sql``;
    marimo's built-in formatter renders it without materialising rows. We do not
    inject a ``LIMIT`` — Spark / the user is responsible for shaping the result.
    """

    def __init__(self, connection: Any, engine_name: VariableName | None = None) -> None:
        super().__init__(connection, engine_name)

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
        # Lazy everything — UC catalogs can be large; let the UI drive expansion.
        return InferenceConfig(
            auto_discover_schemas="auto",
            auto_discover_tables="auto",
            auto_discover_columns="auto",
        )

    def get_default_database(self) -> str | None:
        return self._scalar("SELECT current_catalog()")

    def get_default_schema(self) -> str | None:
        return self._scalar("SELECT current_schema()")

    def get_databases(
        self,
        *,
        include_schemas: bool | str,
        include_tables: bool | str,
        include_table_details: bool | str,
    ) -> list[Database]:
        databases: list[Database] = []
        for catalog in self._list_catalogs():
            schemas: list[Schema] = []
            if include_schemas is True:
                schemas = self.get_schemas(
                    database=catalog,
                    include_tables=include_tables is True,
                    include_table_details=include_table_details is True,
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
        result: list[Schema] = []
        for schema_name in self._list_schemas(database):
            tables: list[DataTable] = []
            if include_tables:
                tables = self.get_tables_in_schema(
                    schema=schema_name,
                    database=database,
                    include_table_details=include_table_details,
                )
            result.append(Schema(name=schema_name, tables=tables))
        return result

    def get_tables_in_schema(
        self,
        *,
        schema: str,
        database: str,
        include_table_details: bool,
    ) -> list[DataTable]:
        result: list[DataTable] = []
        for tbl in self._list_tables(database, schema):
            if include_table_details:
                detail = self.get_table_details(
                    table_name=tbl, schema_name=schema, database_name=database
                )
                if detail is not None:
                    result.append(detail)
                    continue
            result.append(
                DataTable(
                    source_type="connection",
                    source=self.dialect,
                    name=tbl,
                    num_rows=None,
                    num_columns=None,
                    variable_name=None,
                    columns=[],
                    engine=self._engine_name,
                )
            )
        return result

    def get_table_details(
        self,
        *,
        table_name: str,
        schema_name: str,
        database_name: str,
    ) -> DataTable | None:
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

        return DataTable(
            source_type="connection",
            source=self.dialect,
            name=table_name,
            num_rows=None,
            num_columns=len(columns) or None,
            variable_name=None,
            columns=columns,
            engine=self._engine_name,
        )

    # -- QueryEngine ------------------------------------------------------

    def execute(self, query: str) -> Any:
        return self._connection.sql(query)

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

    def _list_tables(self, catalog: str, schema: str) -> list[str]:
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
