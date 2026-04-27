"""marimo + databricks-connect integration.

Importing this package registers a Spark Connect SQL engine with marimo so that
``mo.sql(query, engine=spark)`` executes against Databricks via Spark, and exposes
helpers for the marimo storage browser (Unity Catalog volumes + external locations).

Typical notebook usage::

    import marimo as mo
    from marimo_databricks_connect import spark, dbutils, dbfs

That single import:

* builds (or reuses) a serverless ``DatabricksSession``,
* creates a ``DBUtils`` bound to that session,
* exposes an fsspec-compatible filesystem rooted at ``/Volumes`` for the marimo
  storage browser,
* registers a ``SparkConnectEngine`` so marimo's data source panel and SQL cells
  light up automatically.

To browse a Unity Catalog external location, add a cell::

    finops = external_location("finops_landing")          # by UC name
    raw    = external_location("abfss://c@acct.dfs.core.windows.net/data")  # by path

Each variable becomes its own root in the marimo storage browser.
"""

from __future__ import annotations

from typing import Any

__all__ = [
    "spark",
    "dbutils",
    "dbfs",
    "engine",
    "external_location",
    "mount",
    "include_catalogs",
    "exclude_catalogs",
    "show_all_catalogs",
    "reset_catalog_filter",
    "catalog_filter",
    "workflows_widget",
    "compute_widget",
]

_cache: dict[str, Any] = {}


def _build_spark() -> Any:
    from databricks.connect import DatabricksSession

    # Host + OAuth credentials are inferred from the Databricks unified auth chain
    # (env vars, ~/.databrickscfg, az login → ARM token). Serverless = all-purpose
    # serverless compute, no SQL warehouse.
    return DatabricksSession.builder.serverless().getOrCreate()


def _build_dbutils(spark: Any) -> Any:
    from pyspark.dbutils import DBUtils  # type: ignore[import-untyped]

    return DBUtils(spark)


def _build_dbfs(spark: Any, dbu: Any) -> Any:
    from ._fs import DbutilsFileSystem

    return DbutilsFileSystem(dbutils=dbu, spark=spark, root="/Volumes")


def _build_engine(spark: Any) -> Any:
    from ._engine import SparkConnectEngine

    return SparkConnectEngine(spark)


def __getattr__(name: str) -> Any:
    """Lazily build singletons on first attribute access."""
    if name in _cache:
        return _cache[name]
    if name == "spark":
        value = _build_spark()
    elif name == "dbutils":
        value = _build_dbutils(__getattr__("spark"))
    elif name == "dbfs":
        value = _build_dbfs(__getattr__("spark"), __getattr__("dbutils"))
    elif name == "engine":
        value = _build_engine(__getattr__("spark"))
    else:
        raise AttributeError(f"module 'marimo_databricks_connect' has no attribute {name!r}")
    _cache[name] = value
    return value


def external_location(name_or_path: str) -> Any:
    """Return an fsspec filesystem rooted at a Unity Catalog external location.

    ``name_or_path`` may be either:

    * the *name* of a UC external location — looked up via ``DESCRIBE EXTERNAL
      LOCATION`` to resolve its underlying URL, or
    * an explicit ``abfss://`` / ``s3://`` / ``/Volumes/...`` path used as-is.

    Assign the result to a notebook variable to make it appear as a separate
    root in the marimo storage browser::

        landing = external_location("finops_landing")
    """
    from ._fs import DbutilsFileSystem

    spark = __getattr__("spark")
    dbu = __getattr__("dbutils")

    if "://" in name_or_path or name_or_path.startswith("/"):
        root = name_or_path
    else:
        # ``DESCRIBE EXTERNAL LOCATION <name>`` returns a single row with
        # named columns: name, url, credential_name, read_only, comment, ...
        try:
            rows = spark.sql(f"DESCRIBE EXTERNAL LOCATION `{name_or_path}`").collect()
        except Exception as exc:
            raise ValueError(
                f"Could not resolve external location {name_or_path!r}: "
                f"DESCRIBE failed ({exc}). Pass an explicit abfss://, s3://, "
                f"or /Volumes/... path instead."
            ) from exc
        if not rows:
            raise ValueError(f"External location {name_or_path!r} not found in Unity Catalog.")
        row = rows[0].asDict()
        # Column name has been ``url`` since the feature shipped, but be
        # defensive about casing / future renames.
        root = row.get("url") or row.get("URL") or row.get("location") or row.get("storage_location")
        if not root:
            raise ValueError(f"DESCRIBE EXTERNAL LOCATION returned no URL column; got {list(row)}")

    return DbutilsFileSystem(dbutils=dbu, spark=spark, root=root)


# Alias for users who prefer this name.
mount = external_location


# --------------------------------------------------------------------------- #
# Catalog/schema visibility filter (data sources panel only)                  #
# --------------------------------------------------------------------------- #


def include_catalogs(*patterns: str) -> None:
    """Set catalog (or ``catalog.schema``) glob patterns for the allow-list.

    Each call **replaces** any previously set include patterns.  The marimo data
    sources panel will surface only catalogs / schemas matching one of the
    include patterns (minus anything matched by ``exclude_catalogs``).  SQL
    execution is unaffected — every catalog the workspace has access to remains
    queryable.

    Examples::

        include_catalogs("main", "samples")          # exact catalog names
        include_catalogs("dev_*", "*_prod")          # fnmatch globs
        include_catalogs("main.bronze_*", "*_dev.silver")  # narrow to schemas
    """
    from ._filter import _filter

    _filter.set_includes(patterns)


def exclude_catalogs(*patterns: str) -> None:
    """Set catalog (or ``catalog.schema``) glob patterns for the deny-list.

    Each call **replaces** any previously set exclude patterns.  Excludes always
    take precedence over includes. A catalog-only pattern (``"system"``) hides
    the entire catalog; a scoped pattern (``"main.__internal_*"``) hides only
    matching schemas.
    """
    from ._filter import _filter

    _filter.set_excludes(patterns)


def show_all_catalogs() -> None:
    """Disable include-list filtering and surface every catalog the workspace exposes.

    Excludes still apply.
    """
    from ._filter import _filter

    _filter.show_all = True


def reset_catalog_filter() -> None:
    """Clear all include / exclude patterns and the show-all flag."""
    from ._filter import _filter

    _filter.reset()


def catalog_filter() -> Any:
    """Return the live ``CatalogFilter`` singleton (for inspection / testing)."""
    from ._filter import _filter

    return _filter


def _register_with_marimo() -> None:
    """Append our SQL engine to marimo's registry (idempotent).

    Marimo's ``FsspecFilesystem`` adapter already accepts any
    ``fsspec.AbstractFileSystem`` instance, so the storage backend is picked up
    automatically — no separate storage registration needed.
    """
    try:
        from marimo._sql.get_engines import SUPPORTED_ENGINES
    except ImportError:  # pragma: no cover - marimo not installed
        return

    from ._engine import SparkConnectEngine

    if SparkConnectEngine not in SUPPORTED_ENGINES:
        SUPPORTED_ENGINES.insert(0, SparkConnectEngine)


_register_with_marimo()


def workflows_widget(workspace_client: Any = None) -> Any:
    """Create an interactive widget for browsing Databricks workflows.

    The widget displays jobs in the workspace with drill-down into tasks
    and run history. Requires ``anywidget`` (installed as a dependency).

    Args:
        workspace_client: An optional ``databricks.sdk.WorkspaceClient``.
            If not provided, one is created using the default auth chain.

    Returns:
        A ``WorkflowsWidget`` anywidget instance. Display it by placing it
        as the last expression in a marimo cell.

    Example::

        from marimo_databricks_connect import workflows_widget
        widget = workflows_widget()
        widget
    """
    from ._workflows import WorkflowsWidget

    return WorkflowsWidget(workspace_client=workspace_client)


def compute_widget(workspace_client: Any = None) -> Any:
    """Create an interactive widget for browsing Databricks compute resources.

    Displays clusters, SQL warehouses, vector search endpoints, instance pools,
    and cluster policies in a tabbed interface with click-to-inspect detail panels.

    Args:
        workspace_client: An optional ``databricks.sdk.WorkspaceClient``.
            If not provided, one is created using the default auth chain.

    Returns:
        A ``ComputeWidget`` anywidget instance. Display it by placing it
        as the last expression in a marimo cell.

    Example::

        from marimo_databricks_connect import compute_widget
        widget = compute_widget()
        widget
    """
    from ._compute import ComputeWidget

    return ComputeWidget(workspace_client=workspace_client)
