"""Tests for the compute widget (no live Databricks required)."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest


# ---- Mock builders ----


def _make_cluster(
    cluster_id="abc-123",
    name="my-cluster",
    state="RUNNING",
    node_type="i3.xlarge",
    num_workers=4,
    spark_version="14.3.x-scala2.12",
    creator="user@example.com",
    autoscale=None,
    tags=None,
):
    return SimpleNamespace(
        cluster_id=cluster_id,
        cluster_name=name,
        state=SimpleNamespace(value=state),
        state_message=None,
        creator_user_name=creator,
        spark_version=spark_version,
        node_type_id=node_type,
        driver_node_type_id=node_type,
        num_workers=num_workers,
        autoscale=autoscale,
        cluster_cores=16.0,
        cluster_memory_mb=65536,
        autotermination_minutes=120,
        start_time=1700000000000,
        terminated_time=None,
        last_restarted_time=None,
        data_security_mode=SimpleNamespace(value="SINGLE_USER"),
        single_user_name=creator,
        cluster_source=SimpleNamespace(value="UI"),
        policy_id=None,
        instance_pool_id=None,
        is_single_node=False,
        custom_tags=tags or {},
        runtime_engine=SimpleNamespace(value="PHOTON"),
        termination_reason=None,
    )


def _make_warehouse(wh_id="wh-001", name="my-warehouse", state="RUNNING", size="Small", creator="user@example.com"):
    return SimpleNamespace(
        id=wh_id,
        name=name,
        state=SimpleNamespace(value=state),
        cluster_size=size,
        min_num_clusters=1,
        max_num_clusters=4,
        num_clusters=2,
        num_active_sessions=5,
        auto_stop_mins=10,
        enable_photon=True,
        enable_serverless_compute=False,
        warehouse_type=SimpleNamespace(value="PRO"),
        creator_name=creator,
        health=SimpleNamespace(status=SimpleNamespace(value="HEALTHY"), message=None),
        tags=SimpleNamespace(
            custom_tags=[
                SimpleNamespace(key="env", value="prod"),
            ]
        ),
    )


def _make_vs_endpoint(
    ep_id="vs-001", name="my-vs-endpoint", state="ONLINE", num_indexes=3, creator="user@example.com"
):
    return SimpleNamespace(
        id=ep_id,
        name=name,
        endpoint_status=SimpleNamespace(
            state=SimpleNamespace(value=state),
            message=None,
        ),
        endpoint_type=SimpleNamespace(value="STANDARD"),
        num_indexes=num_indexes,
        creator=creator,
        creation_timestamp=1700000000000,
        last_updated_timestamp=1700003600000,
        last_updated_user=creator,
    )


def _make_pool(pool_id="pool-001", name="my-pool", state="ACTIVE", node_type="i3.xlarge", min_idle=2, max_cap=10):
    return SimpleNamespace(
        instance_pool_id=pool_id,
        instance_pool_name=name,
        state=SimpleNamespace(value=state),
        node_type_id=node_type,
        min_idle_instances=min_idle,
        max_capacity=max_cap,
        idle_instance_autotermination_minutes=30,
        preloaded_spark_versions=["14.3.x-scala2.12"],
        stats=SimpleNamespace(idle_count=2, used_count=3, pending_idle_count=0, pending_used_count=1),
        custom_tags={"team": "data"},
    )


def _make_policy(policy_id="pol-001", name="default-policy", is_default=True, creator="admin@example.com"):
    return SimpleNamespace(
        policy_id=policy_id,
        name=name,
        description="Standard policy for all users",
        creator_user_name=creator,
        is_default=is_default,
        max_clusters_per_user=5,
        policy_family_id="personal-vm",
        created_at_timestamp=1700000000000,
        definition='{"spark_version":{"type":"fixed","value":"14.3.x-scala2.12"}}',
    )


def _mock_workspace_client(clusters=None, warehouses=None, vs_endpoints=None, pools=None, policies=None):
    ws = MagicMock()
    ws.clusters.list.return_value = clusters or []
    ws.warehouses.list.return_value = warehouses or []
    ws.vector_search_endpoints.list_endpoints.return_value = vs_endpoints or []
    ws.instance_pools.list.return_value = pools or []
    ws.cluster_policies.list.return_value = policies or []
    return ws


# ---- Serialization tests ----


def test_serialize_cluster():
    from marimo_databricks_connect._compute import _serialize_cluster

    c = _make_cluster(tags={"env": "prod"})
    result = _serialize_cluster(c)
    assert result["cluster_id"] == "abc-123"
    assert result["cluster_name"] == "my-cluster"
    assert result["state"] == "RUNNING"
    assert result["node_type_id"] == "i3.xlarge"
    assert result["num_workers"] == 4
    assert result["runtime_engine"] == "PHOTON"
    assert result["data_security_mode"] == "SINGLE_USER"
    assert result["custom_tags"] == {"env": "prod"}
    assert result["autoscale"] is None


def test_serialize_cluster_with_autoscale():
    from marimo_databricks_connect._compute import _serialize_cluster

    autoscale = SimpleNamespace(min_workers=2, max_workers=8)
    c = _make_cluster(autoscale=autoscale)
    result = _serialize_cluster(c)
    assert result["autoscale"] == "2–8"


def test_serialize_warehouse():
    from marimo_databricks_connect._compute import _serialize_warehouse

    w = _make_warehouse()
    result = _serialize_warehouse(w)
    assert result["id"] == "wh-001"
    assert result["name"] == "my-warehouse"
    assert result["state"] == "RUNNING"
    assert result["cluster_size"] == "Small"
    assert result["enable_photon"] is True
    assert result["warehouse_type"] == "PRO"
    assert result["tags"] == {"env": "prod"}
    assert result["health_status"] == "HEALTHY"


def test_serialize_vs_endpoint():
    from marimo_databricks_connect._compute import _serialize_vs_endpoint

    e = _make_vs_endpoint()
    result = _serialize_vs_endpoint(e)
    assert result["id"] == "vs-001"
    assert result["state"] == "ONLINE"
    assert result["num_indexes"] == 3
    assert result["endpoint_type"] == "STANDARD"


def test_serialize_pool():
    from marimo_databricks_connect._compute import _serialize_pool

    p = _make_pool()
    result = _serialize_pool(p)
    assert result["instance_pool_id"] == "pool-001"
    assert result["state"] == "ACTIVE"
    assert result["idle_count"] == 2
    assert result["used_count"] == 3
    assert result["custom_tags"] == {"team": "data"}


def test_serialize_policy():
    from marimo_databricks_connect._compute import _serialize_policy

    p = _make_policy()
    result = _serialize_policy(p)
    assert result["policy_id"] == "pol-001"
    assert result["is_default"] is True
    assert result["max_clusters_per_user"] == 5
    assert "spark_version" in result["definition"]


# ---- Widget tests ----


def test_widget_loads_clusters_on_init():
    from marimo_databricks_connect._compute import ComputeWidget

    clusters = [_make_cluster("c1", "cluster-a"), _make_cluster("c2", "cluster-b")]
    ws = _mock_workspace_client(clusters=clusters)
    w = ComputeWidget(workspace_client=ws)
    data = json.loads(w.clusters_data)
    assert len(data) == 2
    assert data[0]["cluster_name"] == "cluster-a"
    assert w.loading is False


def test_widget_handles_cluster_error():
    from marimo_databricks_connect._compute import ComputeWidget

    ws = MagicMock()
    ws.clusters.list.side_effect = RuntimeError("auth failed")
    w = ComputeWidget(workspace_client=ws)
    assert "auth failed" in w.error_message
    assert w.loading is False


def test_widget_request_list_warehouses():
    from marimo_databricks_connect._compute import ComputeWidget

    ws = _mock_workspace_client(warehouses=[_make_warehouse()])
    w = ComputeWidget(workspace_client=ws)
    w.request = json.dumps({"action": "list_warehouses"})
    data = json.loads(w.warehouses_data)
    assert len(data) == 1
    assert data[0]["name"] == "my-warehouse"


def test_widget_request_list_vs_endpoints():
    from marimo_databricks_connect._compute import ComputeWidget

    ws = _mock_workspace_client(vs_endpoints=[_make_vs_endpoint()])
    w = ComputeWidget(workspace_client=ws)
    w.request = json.dumps({"action": "list_vs_endpoints"})
    data = json.loads(w.vs_endpoints_data)
    assert len(data) == 1
    assert data[0]["state"] == "ONLINE"


def test_widget_request_list_pools():
    from marimo_databricks_connect._compute import ComputeWidget

    ws = _mock_workspace_client(pools=[_make_pool()])
    w = ComputeWidget(workspace_client=ws)
    w.request = json.dumps({"action": "list_pools"})
    data = json.loads(w.pools_data)
    assert len(data) == 1
    assert data[0]["instance_pool_name"] == "my-pool"


def test_widget_request_list_policies():
    from marimo_databricks_connect._compute import ComputeWidget

    ws = _mock_workspace_client(policies=[_make_policy()])
    w = ComputeWidget(workspace_client=ws)
    w.request = json.dumps({"action": "list_policies"})
    data = json.loads(w.policies_data)
    assert len(data) == 1
    assert data[0]["is_default"] is True


def test_widget_refresh_reloads():
    from marimo_databricks_connect._compute import ComputeWidget

    ws = _mock_workspace_client(clusters=[_make_cluster()])
    w = ComputeWidget(workspace_client=ws)
    assert ws.clusters.list.call_count == 1
    w.request = json.dumps({"action": "list_clusters"})
    assert ws.clusters.list.call_count == 2


def test_compute_widget_factory():
    """Test the public API factory function."""
    ws = _mock_workspace_client()
    from marimo_databricks_connect import compute_widget

    w = compute_widget(workspace_client=ws)
    from marimo_databricks_connect._compute import ComputeWidget

    assert isinstance(w, ComputeWidget)


def test_serialize_termination_reason():
    from marimo_databricks_connect._compute import _serialize_cluster

    reason = SimpleNamespace(
        code=SimpleNamespace(value="INACTIVITY"),
        parameters=SimpleNamespace(inactivity_duration_min="60"),
    )
    c = _make_cluster()
    c.termination_reason = reason
    result = _serialize_cluster(c)
    assert "INACTIVITY" in result["termination_reason"]
    assert "60" in result["termination_reason"]
