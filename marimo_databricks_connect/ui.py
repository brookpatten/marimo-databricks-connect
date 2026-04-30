"""Friendly factories for ``mo.ui``-style Databricks selector widgets.

Usage in a marimo notebook::

    import marimo_databricks_connect as mdc

    catalog = mdc.ui.catalog()
    catalog                                    # cell output: dropdown

    schema  = mdc.ui.schema(catalog=catalog)   # auto-refreshes when ``catalog`` changes
    schema

    table   = mdc.ui.table(schema=schema)
    column  = mdc.ui.column(table=table)

In a downstream cell::

    table.value      # e.g. "main.bronze.events" (reactive)
    column.value     # e.g. "user_id"
"""

from __future__ import annotations

from typing import Any

from ._selectors import (
    AppSelector,
    CatalogSelector,
    ClusterSelector,
    ColumnSelector,
    GenieSpaceSelector,
    PipelineSelector,
    PrincipalSelector,
    SchemaSelector,
    SecretScopeSelector,
    SecretSelector,
    ServingEndpointSelector,
    TableSelector,
    VectorIndexSelector,
    VectorSearchEndpointSelector,
    WarehouseSelector,
    WorkflowSelector,
)

__all__ = [
    "catalog",
    "schema",
    "table",
    "column",
    "secret_scope",
    "secret",
    "cluster",
    "warehouse",
    "workflow",
    "pipeline",
    "app",
    "serving_endpoint",
    "vector_search",
    "vector_search_endpoint",
    "vector_index",
    "genie_space",
    "principal",
]


def catalog(**kwargs: Any) -> CatalogSelector:
    """Selector for Unity Catalog catalogs. ``value`` = catalog name."""
    return CatalogSelector(**kwargs)


def schema(catalog: Any = None, **kwargs: Any) -> SchemaSelector:
    """Selector for UC schemas under a catalog. ``value`` = ``catalog.schema``.

    ``catalog`` may be a catalog-name string or a :func:`catalog` selector.
    """
    return SchemaSelector(catalog=catalog, **kwargs)


def table(catalog: Any = None, schema: Any = None, **kwargs: Any) -> TableSelector:
    """Selector for UC tables. ``value`` = ``catalog.schema.table``.

    Either pass ``schema=schema_selector`` (which already encodes the catalog)
    or pass both ``catalog=...`` and ``schema=...`` as strings.
    """
    return TableSelector(catalog=catalog, schema=schema, **kwargs)


def column(table: Any = None, **kwargs: Any) -> ColumnSelector:
    """Selector for columns of a table. ``value`` = column name.

    ``table`` may be a three-part ``catalog.schema.table`` string or a
    :func:`table` selector.
    """
    return ColumnSelector(table=table, **kwargs)


def secret_scope(**kwargs: Any) -> SecretScopeSelector:
    """Selector for Databricks secret scopes. ``value`` = scope name."""
    return SecretScopeSelector(**kwargs)


def secret(scope: Any = None, **kwargs: Any) -> SecretSelector:
    """Selector for keys within a secret scope. ``value`` = key name.

    ``scope`` may be a scope-name string or a :func:`secret_scope` selector.
    The ``selected_meta`` dict includes ``{scope, key, ref}`` where ``ref`` is
    the ``{{secrets/scope/key}}`` interpolation Databricks uses in cluster /
    job specs.
    """
    return SecretSelector(scope=scope, **kwargs)


def cluster(**kwargs: Any) -> ClusterSelector:
    """Selector for all-purpose clusters. ``value`` = cluster_id."""
    return ClusterSelector(**kwargs)


def warehouse(**kwargs: Any) -> WarehouseSelector:
    """Selector for SQL warehouses. ``value`` = warehouse id."""
    return WarehouseSelector(**kwargs)


def workflow(**kwargs: Any) -> WorkflowSelector:
    """Selector for Databricks Jobs / Workflows. ``value`` = job_id (str)."""
    return WorkflowSelector(**kwargs)


def pipeline(**kwargs: Any) -> PipelineSelector:
    """Selector for Lakeflow Declarative Pipelines (DLT). ``value`` = pipeline_id."""
    return PipelineSelector(**kwargs)


def app(**kwargs: Any) -> AppSelector:
    """Selector for Databricks Apps. ``value`` = app name."""
    return AppSelector(**kwargs)


def serving_endpoint(**kwargs: Any) -> ServingEndpointSelector:
    """Selector for model serving endpoints. ``value`` = endpoint name."""
    return ServingEndpointSelector(**kwargs)


def vector_search_endpoint(**kwargs: Any) -> VectorSearchEndpointSelector:
    """Selector for Vector Search endpoints. ``value`` = endpoint name."""
    return VectorSearchEndpointSelector(**kwargs)


# Friendly alias.
vector_search = vector_search_endpoint


def vector_index(endpoint: Any = None, **kwargs: Any) -> VectorIndexSelector:
    """Selector for Vector Search indexes. ``value`` = three-part index name.

    ``endpoint`` may be an endpoint-name string or a
    :func:`vector_search_endpoint` selector.
    """
    return VectorIndexSelector(endpoint=endpoint, **kwargs)


def genie_space(**kwargs: Any) -> GenieSpaceSelector:
    """Selector for Genie spaces. ``value`` = space_id."""
    return GenieSpaceSelector(**kwargs)


def principal(
    kinds: tuple[str, ...] | list[str] = ("user", "service_principal", "group"),
    **kwargs: Any,
) -> PrincipalSelector:
    """Selector for principals (users, service principals, groups).

    ``value`` is the natural identifier per kind: userName, applicationId,
    or group displayName respectively.  Restrict via ``kinds=("user",)`` etc.
    """
    return PrincipalSelector(kinds=kinds, **kwargs)
