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
    "prefetch",
    "refresh_metadata",
    "external_location",
    "mount",
    "include_catalogs",
    "exclude_catalogs",
    "show_all_catalogs",
    "reset_catalog_filter",
    "catalog_filter",
    "workflows_widget",
    "compute_widget",
    "unity_catalog_widget",
    "pipelines_widget",
    # Single-instance operational widgets
    "job_widget",
    "pipeline_widget",
    "table_widget",
    "schema_widget",
    "cluster_widget",
    "warehouse_widget",
    "serving_endpoint_widget",
    "external_location_widget",
    "secret_scope_widget",
    "vector_search_endpoint_widget",
    "vector_index_widget",
    "app_widget",
    "genie_widget",
    "acl_widget",
    "permissions_widget",
    "principal_widget",
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


def prefetch(*catalogs: str) -> dict[str, int]:
    """Eagerly populate the SQL metadata cache so completion lights up immediately.

    Without this, marimo's data sources panel and SQL autocomplete only
    discover catalogs / schemas / tables / columns lazily as you expand
    nodes (or as the engine is asked for them), which can mean the first
    completion in a fresh notebook hits a cold cache.

    Calling ``prefetch()`` runs the bulk ``information_schema`` queries up
    front and stashes the results in an in-process TTL cache shared with
    the engine.  Pass explicit catalog names to scope the prefetch::

        prefetch()                         # everything visible under your filter
        prefetch("main", "samples")        # just these two

    Returns a ``{catalog: table_count}`` summary.
    """
    eng = __getattr__("engine")
    return eng.prefetch(*catalogs)


def refresh_metadata(catalog: str | None = None) -> None:
    """Drop cached SQL metadata so the next completion / panel expansion
    re-fetches it.  Pass a catalog name to invalidate just one catalog,
    or no argument to invalidate everything."""
    eng = __getattr__("engine")
    eng.refresh(catalog)


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


def _register_streaming_formatter() -> None:
    """Register streaming DataFrame formatter (idempotent)."""
    try:
        from ._streaming_formatter import register_streaming_formatter

        register_streaming_formatter()
    except Exception:  # pragma: no cover
        pass


_register_streaming_formatter()


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


def unity_catalog_widget(workspace_client: Any = None) -> Any:
    """Create an interactive widget for browsing Databricks Unity Catalog.

    The widget displays catalogs, schemas, tables, columns, lineage,
    permissions, external locations, storage credentials, connections,
    volumes, and external metadata in a tabbed interface.

    Args:
        workspace_client: An optional ``databricks.sdk.WorkspaceClient``.
            If not provided, one is created using the default auth chain.

    Returns:
        A ``UnityCatalogWidget`` anywidget instance. Display it by placing it
        as the last expression in a marimo cell.

    Example::

        from marimo_databricks_connect import unity_catalog_widget
        widget = unity_catalog_widget()
        widget
    """
    from ._unity_catalog import UnityCatalogWidget

    return UnityCatalogWidget(workspace_client=workspace_client)


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


# --------------------------------------------------------------------------- #
# Single-instance operational widgets                                          #
# --------------------------------------------------------------------------- #


def job_widget(
    job_id: int | None = None,
    job_name: str | None = None,
    workspace_client: Any = None,
    refresh_seconds: int = 30,
) -> Any:
    """Create an operational widget for a single Databricks job/workflow.

    Displays job details, recent runs, task DAG, and logs. Supports
    actions: Run Now, Cancel, and Repair. Auto-refreshes periodically.

    Args:
        job_id: The numeric job ID. Provide either this or ``job_name``.
        job_name: The exact job name. Resolved to job_id on init.
        workspace_client: Optional ``WorkspaceClient``.
        refresh_seconds: Auto-refresh interval (default 30s).

    Example::

        from marimo_databricks_connect import job_widget
        widget = job_widget(job_id=123456)
        widget
    """
    from ._job_widget import JobWidget

    return JobWidget(
        job_id=job_id,
        job_name=job_name,
        workspace_client=workspace_client,
        refresh_seconds=refresh_seconds,
    )


def table_widget(full_name: str, workspace_client: Any = None) -> Any:
    """Create an operational widget for a single Unity Catalog table.

    Displays columns, sample data, lineage, permissions, and properties.

    Args:
        full_name: Three-part table name (``catalog.schema.table``).
        workspace_client: Optional ``WorkspaceClient``.

    Example::

        from marimo_databricks_connect import table_widget
        widget = table_widget("main.bronze.events")
        widget
    """
    from ._table_widget import TableWidget

    return TableWidget(full_name=full_name, workspace_client=workspace_client)


def schema_widget(catalog_name: str, schema_name: str, workspace_client: Any = None) -> Any:
    """Create an operational widget for a single Unity Catalog schema.

    Displays tables, volumes, permissions, and properties.

    Args:
        catalog_name: The catalog name.
        schema_name: The schema name.
        workspace_client: Optional ``WorkspaceClient``.

    Example::

        from marimo_databricks_connect import schema_widget
        widget = schema_widget("main", "bronze")
        widget
    """
    from ._schema_widget import SchemaWidget

    return SchemaWidget(
        catalog_name=catalog_name,
        schema_name=schema_name,
        workspace_client=workspace_client,
    )


def cluster_widget(cluster_id: str, workspace_client: Any = None, refresh_seconds: int = 30) -> Any:
    """Create an operational widget for a single Databricks cluster.

    Displays cluster status, config, and events. Supports Start, Stop,
    and Restart actions. Auto-refreshes periodically.

    Args:
        cluster_id: The cluster ID string.
        workspace_client: Optional ``WorkspaceClient``.
        refresh_seconds: Auto-refresh interval (default 30s).

    Example::

        from marimo_databricks_connect import cluster_widget
        widget = cluster_widget("0123-456789-abcdef")
        widget
    """
    from ._cluster_widget import ClusterWidget

    return ClusterWidget(
        cluster_id=cluster_id,
        workspace_client=workspace_client,
        refresh_seconds=refresh_seconds,
    )


def warehouse_widget(warehouse_id: str, workspace_client: Any = None, refresh_seconds: int = 30) -> Any:
    """Create an operational widget for a single Databricks SQL warehouse.

    Displays warehouse status, scaling, and config. Supports Start and
    Stop actions. Auto-refreshes periodically.

    Args:
        warehouse_id: The SQL warehouse ID string.
        workspace_client: Optional ``WorkspaceClient``.
        refresh_seconds: Auto-refresh interval (default 30s).

    Example::

        from marimo_databricks_connect import warehouse_widget
        widget = warehouse_widget("abc123def456")
        widget
    """
    from ._warehouse_widget import WarehouseWidget

    return WarehouseWidget(
        warehouse_id=warehouse_id,
        workspace_client=workspace_client,
        refresh_seconds=refresh_seconds,
    )


def serving_endpoint_widget(endpoint_name: str, workspace_client: Any = None, refresh_seconds: int = 30) -> Any:
    """Create an operational widget for a single model serving endpoint.

    Displays endpoint status, served entities, traffic config, and
    provides an interactive query interface. Auto-refreshes periodically.

    Args:
        endpoint_name: The serving endpoint name.
        workspace_client: Optional ``WorkspaceClient``.
        refresh_seconds: Auto-refresh interval (default 30s).

    Example::

        from marimo_databricks_connect import serving_endpoint_widget
        widget = serving_endpoint_widget("my-model-endpoint")
        widget
    """
    from ._serving_endpoint_widget import ServingEndpointWidget

    return ServingEndpointWidget(
        endpoint_name=endpoint_name,
        workspace_client=workspace_client,
        refresh_seconds=refresh_seconds,
    )


def external_location_widget(location_name: str, workspace_client: Any = None) -> Any:
    """Create an operational widget for a single external location.

    Displays location details, file browser, permissions, and validation.

    Args:
        location_name: The UC external location name.
        workspace_client: Optional ``WorkspaceClient``.

    Example::

        from marimo_databricks_connect import external_location_widget
        widget = external_location_widget("finops_landing")
        widget
    """
    from ._external_location_widget import ExternalLocationWidget

    return ExternalLocationWidget(location_name=location_name, workspace_client=workspace_client)


def secret_scope_widget(scope_name: str, workspace_client: Any = None) -> Any:
    """Create an operational widget for a single Databricks secret scope.

    Displays scope metadata, the list of keys in the scope, and lets
    users fetch a selected secret into a masked textbox with copy/show
    controls.

    Args:
        scope_name: The Databricks secret scope name.
        workspace_client: Optional ``WorkspaceClient``.

    Example::

        from marimo_databricks_connect import secret_scope_widget
        widget = secret_scope_widget("my-scope")
        widget
    """
    from ._secret_scope_widget import SecretScopeWidget

    return SecretScopeWidget(scope_name=scope_name, workspace_client=workspace_client)


def vector_search_endpoint_widget(endpoint_name: str, workspace_client: Any = None, refresh_seconds: int = 30) -> Any:
    """Create an operational widget for a single Vector Search endpoint.

    Displays endpoint status, scaling info, hosted indexes, and metrics.
    Auto-refreshes periodically.

    Args:
        endpoint_name: The Vector Search endpoint name.
        workspace_client: Optional ``WorkspaceClient``.
        refresh_seconds: Auto-refresh interval (default 30s).

    Example::

        from marimo_databricks_connect import vector_search_endpoint_widget
        widget = vector_search_endpoint_widget("my-vs-endpoint")
        widget
    """
    from ._vector_search_endpoint_widget import VectorSearchEndpointWidget

    return VectorSearchEndpointWidget(
        endpoint_name=endpoint_name,
        workspace_client=workspace_client,
        refresh_seconds=refresh_seconds,
    )


def vector_index_widget(index_name: str, workspace_client: Any = None, refresh_seconds: int = 30) -> Any:
    """Create an operational widget for a single Vector Search index.

    Displays index status, embedding configuration, sample data,
    lineage, and permissions.  Supports triggering a sync for
    Delta Sync indexes.  Auto-refreshes periodically.

    Args:
        index_name: Three-part index name (``catalog.schema.index``).
        workspace_client: Optional ``WorkspaceClient``.
        refresh_seconds: Auto-refresh interval (default 30s).

    Example::

        from marimo_databricks_connect import vector_index_widget
        widget = vector_index_widget("main.rag.doc_index")
        widget
    """
    from ._vector_index_widget import VectorIndexWidget

    return VectorIndexWidget(
        index_name=index_name,
        workspace_client=workspace_client,
        refresh_seconds=refresh_seconds,
    )


def pipelines_widget(workspace_client: Any = None, refresh_seconds: int = 60) -> Any:
    """Create an interactive widget for browsing Databricks Lakeflow Declarative Pipelines (DLT).

    Lists pipelines in the workspace with drill-down into spec, recent
    updates, and the pipeline event log. For start/stop and full-refresh
    actions on a specific pipeline, use :func:`pipeline_widget` instead.

    Args:
        workspace_client: An optional ``databricks.sdk.WorkspaceClient``.
            If not provided, one is created using the default auth chain.
        refresh_seconds: Auto-refresh interval for the pipeline list view
            (default 60s). The browser includes a pause/resume toggle.

    Returns:
        A ``PipelinesWidget`` anywidget instance.

    Example::

        from marimo_databricks_connect import pipelines_widget
        widget = pipelines_widget()
        widget
    """
    from ._pipelines import PipelinesWidget

    return PipelinesWidget(workspace_client=workspace_client, refresh_seconds=refresh_seconds)


def pipeline_widget(
    pipeline_id: str | None = None,
    pipeline_name: str | None = None,
    workspace_client: Any = None,
    refresh_seconds: int = 30,
) -> Any:
    """Create an operational widget for a single Lakeflow Declarative Pipeline (DLT).

    Displays pipeline status, configuration, recent updates, and event log.
    Supports start, stop, full-refresh, and validate actions.  Auto-refreshes
    periodically.

    Args:
        pipeline_id: The DLT pipeline ID (UUID).
        pipeline_name: Alternative to ``pipeline_id`` — resolved by listing
            pipelines whose name matches; the first exact match is used.
        workspace_client: Optional ``WorkspaceClient``.
        refresh_seconds: Auto-refresh interval (default 30s).

    Example::

        from marimo_databricks_connect import pipeline_widget
        widget = pipeline_widget("abc-123-def-456")
        widget

        # Or by name:
        widget = pipeline_widget(pipeline_name="bronze_etl")
    """
    from ._pipeline_widget import PipelineWidget

    return PipelineWidget(
        pipeline_id=pipeline_id,
        pipeline_name=pipeline_name,
        workspace_client=workspace_client,
        refresh_seconds=refresh_seconds,
    )


def acl_widget(workspace_client: Any = None) -> Any:
    """Create a cross-cutting permissions / ACL explorer widget.

    Two tabs:

    * **By Principal** — pick a user, group, or service principal and scan the
      workspace + Unity Catalog for everything they have permissions on.
      Optionally clone those grants to another principal, applied immediately
      or emitted as a Python script.

    * **By Securable** — pick any securable (cluster, job, warehouse, app,
      secret scope, UC catalog/schema/table/volume/external-location/...) and
      see every principal that has permissions on it.

    Args:
        workspace_client: Optional ``WorkspaceClient``.

    Example::

        from marimo_databricks_connect import acl_widget
        widget = acl_widget()
        widget
    """
    from ._acl_widget import AclWidget

    return AclWidget(workspace_client=workspace_client)


# Friendly alias
permissions_widget = acl_widget


def principal_widget(
    principal: str,
    principal_type: str | None = None,
    workspace_client: Any = None,
    auto_scan: bool = False,
) -> Any:
    """Create a widget for a single principal (user, group, or service principal).

    Resolves the principal via SCIM (``users.list`` / ``service_principals.list``
    / ``groups.list``) and shows their identity details (id, displayName,
    active, entitlements, group memberships, roles, members for groups), plus
    the same cross-cutting permission scan offered by :func:`acl_widget`.

    Args:
        principal: A user's ``userName`` (email), a service principal's
            ``applicationId`` or ``displayName``, or a group's ``displayName``.
        principal_type: Optional hint to skip resolution attempts. One of
            ``"user"``, ``"service_principal"``, ``"group"``.
        workspace_client: Optional ``WorkspaceClient``.
        auto_scan: If ``True``, immediately run a scan over the default
            (cheap) categories after resolving the principal.

    Example::

        from marimo_databricks_connect import principal_widget
        widget = principal_widget("alice@example.com")
        widget
    """
    from ._principal_widget import PrincipalWidget

    return PrincipalWidget(
        principal=principal,
        principal_type=principal_type,
        workspace_client=workspace_client,
        auto_scan=auto_scan,
    )


def app_widget(app_name: str, workspace_client: Any = None, refresh_seconds: int = 30) -> Any:
    """Create an operational widget for a single Databricks App.

    Displays app details, deployments, permissions, and thumbnail.
    Supports start/stop, creating deployments, updating permissions,
    and managing the app thumbnail.  Auto-refreshes periodically.

    Args:
        app_name: The Databricks App name.
        workspace_client: Optional ``WorkspaceClient``.
        refresh_seconds: Auto-refresh interval (default 30s).

    Example::

        from marimo_databricks_connect import app_widget
        widget = app_widget("my-dashboard-app")
        widget
    """
    from ._app_widget import AppWidget

    return AppWidget(
        app_name=app_name,
        workspace_client=workspace_client,
        refresh_seconds=refresh_seconds,
    )


def genie_widget(
    space_id: str,
    workspace_client: Any = None,
    conversation_id: str | None = None,
) -> Any:
    """Create a chat widget for a Databricks AI/BI Genie space.

    Lets you ask questions in natural language and see Genie's text answers,
    generated SQL, and tabular query results inline. Supports browsing past
    conversations, starting a new one, clicking suggested follow-ups, and
    re-running cached queries.

    Args:
        space_id: The Genie space ID (UUID).
        workspace_client: Optional ``WorkspaceClient``.
        conversation_id: Optionally resume an existing conversation.

    Example::

        from marimo_databricks_connect import genie_widget
        widget = genie_widget("01ef...")
        widget
    """
    from ._genie_widget import GenieWidget

    return GenieWidget(
        space_id=space_id,
        workspace_client=workspace_client,
        conversation_id=conversation_id,
    )
