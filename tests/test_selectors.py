"""Tests for the ``mo.ui``-style selector widgets (no live Databricks)."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from marimo_databricks_connect import ui as mui
from marimo_databricks_connect import _selectors as sel


# --------------------------------------------------------------------------- #
# Helpers                                                                      #
# --------------------------------------------------------------------------- #


def _ns(**kw):
    return SimpleNamespace(**kw)


def _enum(v):
    return SimpleNamespace(value=v)


def _ws():
    ws = MagicMock()
    # catalogs
    ws.catalogs.list.return_value = [_ns(name="main", comment="primary"), _ns(name="dev", comment="")]
    # schemas
    ws.schemas.list.return_value = [_ns(name="bronze", comment="raw"), _ns(name="silver", comment="")]
    # tables
    ws.tables.list.return_value = [
        _ns(name="events", table_type=_enum("MANAGED")),
        _ns(name="users", table_type=_enum("EXTERNAL")),
    ]
    ws.tables.get.return_value = _ns(
        columns=[
            _ns(name="id", type_text="bigint", type_name="LONG"),
            _ns(name="ts", type_text="timestamp", type_name="TIMESTAMP"),
        ]
    )
    # secrets
    ws.secrets.list_scopes.return_value = [
        _ns(name="prod", backend_type=_enum("DATABRICKS")),
        _ns(name="kv", backend_type=_enum("AZURE_KEYVAULT")),
    ]
    ws.secrets.list_secrets.return_value = [
        _ns(key="api_key"),
        _ns(key="db_pwd"),
    ]
    # clusters
    ws.clusters.list.return_value = [
        _ns(cluster_id="c1", cluster_name="alpha", state=_enum("RUNNING")),
        _ns(cluster_id="c2", cluster_name="beta", state=_enum("TERMINATED")),
    ]
    # warehouses
    ws.warehouses.list.return_value = [
        _ns(id="w1", name="dev-wh", state=_enum("RUNNING"), cluster_size="2X-Small"),
    ]
    # jobs
    ws.jobs.list.return_value = [
        _ns(job_id=1, settings=_ns(name="etl_one")),
        _ns(job_id=2, settings=_ns(name="etl_two")),
    ]
    # pipelines
    ws.pipelines.list_pipelines.return_value = [
        _ns(pipeline_id="p1", name="bronze_etl", state=_enum("RUNNING")),
    ]
    # apps
    ws.apps.list.return_value = [
        _ns(name="dash", compute_status=_ns(state=_enum("ACTIVE")), url="https://x"),
    ]
    # serving endpoints
    ws.serving_endpoints.list.return_value = [
        _ns(name="model-a", state=_ns(ready=_enum("READY"), config_update=None)),
    ]
    # vector search
    ws.vector_search_endpoints.list_endpoints.return_value = [
        _ns(name="vs1", endpoint_status=_ns(state=_enum("ONLINE")), endpoint_type="STANDARD"),
    ]
    ws.vector_search_indexes.list_indexes.return_value = [
        _ns(name="main.rag.docs", index_type=_enum("DELTA_SYNC")),
    ]
    # genie
    ws.genie.list_spaces.return_value = [
        _ns(space_id="sp1", title="Sales", description="sales bot"),
    ]
    # principals
    ws.users.list.return_value = [_ns(user_name="alice@x.com", display_name="Alice", id="u1")]
    ws.service_principals.list.return_value = [_ns(application_id="app-1", display_name="bot", id="sp1")]
    ws.groups.list.return_value = [_ns(display_name="admins", id="g1")]
    return ws


# --------------------------------------------------------------------------- #
# Base behaviour                                                               #
# --------------------------------------------------------------------------- #


class TestBase:
    def test_loads_options_and_value_default(self):
        s = mui.catalog(workspace_client=_ws(), default="main")
        opts = json.loads(s.options)
        assert {"main", "dev"} <= {o["value"] for o in opts}
        assert s.value == "main"
        meta = s.selected_meta
        assert meta["value"] == "main"
        assert meta["label"] == "main"

    def test_label_default_capitalized(self):
        s = mui.catalog(workspace_client=_ws())
        assert s.label == "Catalog"

    def test_refresh_via_request_traitlet(self):
        ws = _ws()
        s = mui.catalog(workspace_client=ws)
        ws.catalogs.list.return_value = [_ns(name="extra", comment="")]
        s.request = json.dumps({"action": "refresh"})
        opts = json.loads(s.options)
        assert {o["value"] for o in opts} == {"extra"}

    def test_invalid_value_cleared_on_refresh(self):
        ws = _ws()
        s = mui.catalog(workspace_client=ws, default="main")
        assert s.value == "main"
        ws.catalogs.list.return_value = [_ns(name="dev", comment="")]
        s.refresh()
        assert s.value == ""

    def test_error_message_on_failure(self):
        ws = MagicMock()
        ws.catalogs.list.side_effect = RuntimeError("boom")
        s = mui.catalog(workspace_client=ws)
        assert "boom" in s.error_message
        assert json.loads(s.options) == []


# --------------------------------------------------------------------------- #
# Unity Catalog hierarchy                                                      #
# --------------------------------------------------------------------------- #


class TestUcHierarchy:
    def test_schema_requires_catalog(self):
        s = mui.schema(workspace_client=_ws())
        assert json.loads(s.options) == []

    def test_schema_with_string_catalog(self):
        ws = _ws()
        s = mui.schema(catalog="main", workspace_client=ws)
        opts = json.loads(s.options)
        assert {o["value"] for o in opts} == {"main.bronze", "main.silver"}
        ws.schemas.list.assert_called_with(catalog_name="main")

    def test_schema_follows_catalog_selector(self):
        ws = _ws()
        cat = mui.catalog(workspace_client=ws)
        sch = mui.schema(catalog=cat, workspace_client=ws)
        assert json.loads(sch.options) == []  # no catalog selected
        cat.value = "dev"
        opts = json.loads(sch.options)
        assert all(o["value"].startswith("dev.") for o in opts)

    def test_table_via_schema_selector(self):
        ws = _ws()
        cat = mui.catalog(workspace_client=ws, default="main")
        sch = mui.schema(catalog=cat, workspace_client=ws, default="main.bronze")
        tab = mui.table(schema=sch, workspace_client=ws)
        opts = json.loads(tab.options)
        assert "main.bronze.events" in {o["value"] for o in opts}
        ws.tables.list.assert_called_with(catalog_name="main", schema_name="bronze")

    def test_table_via_strings(self):
        ws = _ws()
        tab = mui.table(catalog="main", schema="bronze", workspace_client=ws)
        assert "main.bronze.events" in {o["value"] for o in json.loads(tab.options)}

    def test_column_from_table_selector(self):
        ws = _ws()
        tab = mui.table(catalog="main", schema="bronze", workspace_client=ws, default="main.bronze.events")
        col = mui.column(table=tab, workspace_client=ws)
        ws.tables.get.assert_called_with("main.bronze.events")
        opts = json.loads(col.options)
        assert {o["value"] for o in opts} == {"id", "ts"}

    def test_column_string_table(self):
        ws = _ws()
        col = mui.column(table="main.bronze.events", workspace_client=ws)
        assert {o["value"] for o in json.loads(col.options)} == {"id", "ts"}


# --------------------------------------------------------------------------- #
# Secrets                                                                      #
# --------------------------------------------------------------------------- #


class TestSecrets:
    def test_secret_scope(self):
        s = mui.secret_scope(workspace_client=_ws())
        assert {o["value"] for o in json.loads(s.options)} == {"prod", "kv"}

    def test_secret_with_scope(self):
        ws = _ws()
        scope = mui.secret_scope(workspace_client=ws, default="prod")
        sec = mui.secret(scope=scope, workspace_client=ws, default="api_key")
        ws.secrets.list_secrets.assert_called_with("prod")
        meta = sec.selected_meta
        assert meta["meta"]["ref"] == "{{secrets/prod/api_key}}"


# --------------------------------------------------------------------------- #
# Compute / workflows / pipelines                                              #
# --------------------------------------------------------------------------- #


class TestComputeWorkflowsPipelines:
    def test_cluster(self):
        s = mui.cluster(workspace_client=_ws())
        opts = json.loads(s.options)
        assert {o["value"] for o in opts} == {"c1", "c2"}

    def test_warehouse(self):
        s = mui.warehouse(workspace_client=_ws())
        assert {o["value"] for o in json.loads(s.options)} == {"w1"}

    def test_workflow_string_ids(self):
        s = mui.workflow(workspace_client=_ws())
        opts = json.loads(s.options)
        assert {o["value"] for o in opts} == {"1", "2"}
        s.value = "1"
        assert s.selected_meta["label"] == "etl_one"

    def test_pipeline(self):
        s = mui.pipeline(workspace_client=_ws())
        assert {o["value"] for o in json.loads(s.options)} == {"p1"}


# --------------------------------------------------------------------------- #
# Apps / serving / vector / genie                                              #
# --------------------------------------------------------------------------- #


class TestAppsServingVectorGenie:
    def test_app(self):
        s = mui.app(workspace_client=_ws())
        assert {o["value"] for o in json.loads(s.options)} == {"dash"}

    def test_serving_endpoint(self):
        s = mui.serving_endpoint(workspace_client=_ws())
        assert {o["value"] for o in json.loads(s.options)} == {"model-a"}

    def test_vector_search(self):
        s = mui.vector_search(workspace_client=_ws())
        assert {o["value"] for o in json.loads(s.options)} == {"vs1"}

    def test_vector_search_alias(self):
        # `vector_search` should be an alias of `vector_search_endpoint`.
        assert mui.vector_search is mui.vector_search_endpoint

    def test_vector_index_with_endpoint(self):
        ws = _ws()
        ep = mui.vector_search(workspace_client=ws, default="vs1")
        idx = mui.vector_index(endpoint=ep, workspace_client=ws)
        ws.vector_search_indexes.list_indexes.assert_called_with(endpoint_name="vs1")
        assert {o["value"] for o in json.loads(idx.options)} == {"main.rag.docs"}

    def test_vector_index_endpoint_change_refreshes(self):
        ws = _ws()
        ep = mui.vector_search(workspace_client=ws)
        idx = mui.vector_index(endpoint=ep, workspace_client=ws)
        ws.vector_search_indexes.list_indexes.reset_mock()
        ep.value = "vs1"
        ws.vector_search_indexes.list_indexes.assert_called_with(endpoint_name="vs1")

    def test_genie_space(self):
        s = mui.genie_space(workspace_client=_ws())
        opts = json.loads(s.options)
        assert opts and opts[0]["value"] == "sp1"
        assert opts[0]["label"] == "Sales"

    def test_genie_space_unsupported_sdk(self):
        ws = MagicMock()
        del ws.genie  # force AttributeError
        s = mui.genie_space(workspace_client=ws)
        assert "databricks-sdk" in s.error_message.lower()


# --------------------------------------------------------------------------- #
# Principals                                                                   #
# --------------------------------------------------------------------------- #


class TestPrincipal:
    def test_all_kinds(self):
        s = mui.principal(workspace_client=_ws())
        vals = {o["value"] for o in json.loads(s.options)}
        assert vals == {"alice@x.com", "app-1", "admins"}

    def test_filter_kinds(self):
        s = mui.principal(kinds=("user",), workspace_client=_ws())
        assert {o["value"] for o in json.loads(s.options)} == {"alice@x.com"}

    def test_meta_kind(self):
        s = mui.principal(workspace_client=_ws(), default="alice@x.com")
        meta = s.selected_meta["meta"]
        assert meta["kind"] == "user"
        assert meta["display_name"] == "Alice"


# --------------------------------------------------------------------------- #
# ui module surface                                                            #
# --------------------------------------------------------------------------- #


def test_ui_module_exposes_all_selectors():
    expected = {
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
    }
    missing = expected - set(mui.__all__)
    assert not missing, f"ui module missing exports: {missing}"


def test_ui_accessible_from_top_level():
    import marimo_databricks_connect as mdc

    assert mdc.ui is mui


def test_selector_repr():
    s = mui.catalog(workspace_client=_ws(), default="main")
    r = repr(s)
    assert "CatalogSelector" in r and "main" in r
