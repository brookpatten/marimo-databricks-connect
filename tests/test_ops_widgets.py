"""Tests for the single-instance operational widgets (no live Databricks required)."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest


# ===================================================================
# Helpers
# ===================================================================


def _make_run_state(life_cycle="TERMINATED", result="SUCCESS", message=None):
    return SimpleNamespace(
        life_cycle_state=SimpleNamespace(value=life_cycle),
        result_state=SimpleNamespace(value=result) if result else None,
        state_message=message,
    )


def _make_run(run_id, job_id=1, life_cycle="TERMINATED", result="SUCCESS"):
    return SimpleNamespace(
        run_id=run_id,
        run_name=f"run_{run_id}",
        job_id=job_id,
        start_time=1700000000000,
        end_time=1700003600000,
        run_duration=3600000,
        setup_duration=10000,
        state=_make_run_state(life_cycle, result),
        run_page_url=f"https://databricks.com/run/{run_id}",
        trigger=SimpleNamespace(value="PERIODIC"),
        run_type=SimpleNamespace(value="JOB_RUN"),
        tasks=None,
    )


def _make_task(task_key, notebook_path=None, depends_on=None, disabled=False):
    return SimpleNamespace(
        task_key=task_key,
        notebook_task=SimpleNamespace(notebook_path=notebook_path) if notebook_path else None,
        spark_python_task=None,
        python_wheel_task=None,
        spark_jar_task=None,
        spark_submit_task=None,
        pipeline_task=None,
        sql_task=None,
        dbt_task=None,
        run_job_task=None,
        condition_task=None,
        for_each_task=None,
        dashboard_task=None,
        depends_on=[SimpleNamespace(task_key=d) for d in depends_on] if depends_on else None,
        description=None,
        disabled=disabled,
    )


def _make_job_settings(name="test_job", tasks=None, schedule=None, tags=None):
    return SimpleNamespace(
        name=name,
        description="A test job",
        tasks=tasks or [],
        job_clusters=None,
        schedule=SimpleNamespace(quartz_cron_expression=schedule, timezone_id="UTC", pause_status=None)
        if schedule
        else None,
        tags=tags or {},
        max_concurrent_runs=1,
        timeout_seconds=3600,
    )


def _mock_ws():
    ws = MagicMock()
    ws.jobs.list.return_value = []
    ws.jobs.get.return_value = SimpleNamespace(
        job_id=1,
        created_time=1700000000000,
        creator_user_name="user@test.com",
        settings=_make_job_settings(tasks=[_make_task("t1", "/nb1")]),
    )
    ws.jobs.list_runs.return_value = [_make_run(10), _make_run(11, result="FAILED")]
    ws.jobs.run_now.return_value = SimpleNamespace(run_id=99)
    ws.jobs.cancel_run.return_value = None
    ws.jobs.repair_run.return_value = None
    return ws


# ===================================================================
# JobWidget tests
# ===================================================================


class TestJobWidget:
    def test_init_loads_job_and_runs(self):
        from marimo_databricks_connect._job_widget import JobWidget

        ws = _mock_ws()
        w = JobWidget(job_id=1, workspace_client=ws)
        data = json.loads(w.job_data)
        assert data["job_id"] == 1
        assert data["name"] == "test_job"
        assert len(data["tasks"]) == 1
        runs = json.loads(w.runs_data)
        assert len(runs) == 2
        assert w.loading is False

    def test_init_by_name(self):
        from marimo_databricks_connect._job_widget import JobWidget

        ws = _mock_ws()
        ws.jobs.list.return_value = [
            SimpleNamespace(job_id=42, settings=SimpleNamespace(name="my_pipeline")),
        ]
        w = JobWidget(job_name="my_pipeline", workspace_client=ws)
        ws.jobs.get.assert_called_with(42)

    def test_run_now_action(self):
        from marimo_databricks_connect._job_widget import JobWidget

        ws = _mock_ws()
        w = JobWidget(job_id=1, workspace_client=ws)
        w.request = json.dumps({"action": "run_now"})
        ws.jobs.run_now.assert_called_once_with(1)
        result = json.loads(w.action_result)
        assert result["success"] is True
        assert "99" in result["message"]

    def test_cancel_run_action(self):
        from marimo_databricks_connect._job_widget import JobWidget

        ws = _mock_ws()
        w = JobWidget(job_id=1, workspace_client=ws)
        w.request = json.dumps({"action": "cancel_run", "run_id": 10})
        ws.jobs.cancel_run.assert_called_once_with(10)
        result = json.loads(w.action_result)
        assert result["success"] is True

    def test_repair_run_action(self):
        from marimo_databricks_connect._job_widget import JobWidget

        ws = _mock_ws()
        w = JobWidget(job_id=1, workspace_client=ws)
        w.request = json.dumps({"action": "repair_run", "run_id": 11})
        ws.jobs.repair_run.assert_called_once_with(run_id=11, rerun_all_failed_tasks=True)
        result = json.loads(w.action_result)
        assert result["success"] is True

    def test_repair_specific_tasks(self):
        from marimo_databricks_connect._job_widget import JobWidget

        ws = _mock_ws()
        w = JobWidget(job_id=1, workspace_client=ws)
        w.request = json.dumps({"action": "repair_run", "run_id": 11, "rerun_tasks": ["t1"]})
        ws.jobs.repair_run.assert_called_once_with(run_id=11, rerun_tasks=["t1"])

    def test_error_handling(self):
        from marimo_databricks_connect._job_widget import JobWidget

        ws = _mock_ws()
        ws.jobs.get.side_effect = RuntimeError("forbidden")
        ws.jobs.list_runs.side_effect = RuntimeError("forbidden")
        w = JobWidget(job_id=1, workspace_client=ws)
        assert "forbidden" in w.error_message

    def test_factory_function(self):
        from marimo_databricks_connect import job_widget

        ws = _mock_ws()
        w = job_widget(job_id=1, workspace_client=ws)
        from marimo_databricks_connect._job_widget import JobWidget

        assert isinstance(w, JobWidget)


# ===================================================================
# TableWidget tests
# ===================================================================


def _make_table():
    cols = [
        SimpleNamespace(
            name="id",
            type_text="INT",
            type_name=SimpleNamespace(value="INT"),
            comment=None,
            nullable=False,
            position=0,
            partition_index=None,
        ),
        SimpleNamespace(
            name="name",
            type_text="STRING",
            type_name=SimpleNamespace(value="STRING"),
            comment="User name",
            nullable=True,
            position=1,
            partition_index=None,
        ),
    ]
    return SimpleNamespace(
        name="events",
        catalog_name="main",
        schema_name="bronze",
        full_name="main.bronze.events",
        table_type=SimpleNamespace(value="MANAGED"),
        data_source_format=SimpleNamespace(value="DELTA"),
        comment="Event log",
        owner="admin",
        created_at=1700000000000,
        created_by="admin",
        updated_at=1700000000000,
        updated_by="admin",
        storage_location="dbfs:/user/hive/warehouse/events",
        storage_credential_name=None,
        view_definition=None,
        sql_path=None,
        table_id="abc123",
        columns=cols,
        properties={"delta.minReaderVersion": "1"},
    )


class TestTableWidget:
    def test_init_loads_table(self):
        from marimo_databricks_connect._table_widget import TableWidget

        ws = MagicMock()
        ws.tables.get.return_value = _make_table()
        w = TableWidget(full_name="main.bronze.events", workspace_client=ws)
        data = json.loads(w.table_data)
        assert data["name"] == "events"
        assert data["full_name"] == "main.bronze.events"
        assert len(data["columns"]) == 2
        assert data["columns"][0]["name"] == "id"
        assert data["properties"]["delta.minReaderVersion"] == "1"

    def test_factory_function(self):
        from marimo_databricks_connect import table_widget

        ws = MagicMock()
        ws.tables.get.return_value = _make_table()
        w = table_widget("main.bronze.events", workspace_client=ws)
        from marimo_databricks_connect._table_widget import TableWidget

        assert isinstance(w, TableWidget)

    def test_load_sample_data_with_options(self):
        from marimo_databricks_connect._table_widget import TableWidget

        ws = MagicMock()
        ws.tables.get.return_value = _make_table()
        w = TableWidget(full_name="main.bronze.events", workspace_client=ws)

        # Mock spark session
        mock_df = MagicMock()
        mock_df.columns = ["id", "name"]
        mock_df.collect.return_value = [(1, "a"), (2, "b")]
        spark = MagicMock()
        spark.sql.return_value = mock_df
        w._spark = spark

        w.request = json.dumps(
            {
                "action": "get_sample_data",
                "limit": 10,
                "mode": "first",
                "sort_column": "id",
                "sort_order": "desc",
                "filter": "id > 0",
            }
        )
        sql = spark.sql.call_args[0][0]
        assert "FROM `main`.`bronze`.`events`" in sql
        assert "WHERE id > 0" in sql
        assert "ORDER BY `id` DESC" in sql
        assert "LIMIT 10" in sql
        sample = json.loads(w.sample_data)
        assert sample["limit"] == 10
        assert sample["sort_column"] == "id"
        assert sample["mode"] == "first"
        assert sample["filter"] == "id > 0"
        assert len(sample["rows"]) == 2

    def test_load_sample_data_last_n_inverts_order(self):
        from marimo_databricks_connect._table_widget import TableWidget

        ws = MagicMock()
        ws.tables.get.return_value = _make_table()
        w = TableWidget(full_name="main.bronze.events", workspace_client=ws)
        mock_df = MagicMock()
        mock_df.columns = ["id"]
        mock_df.collect.return_value = [(3,), (2,), (1,)]
        spark = MagicMock()
        spark.sql.return_value = mock_df
        w._spark = spark

        w.request = json.dumps(
            {
                "action": "get_sample_data",
                "limit": 5,
                "mode": "last",
                "sort_column": "id",
                "sort_order": "asc",
            }
        )
        sql = spark.sql.call_args[0][0]
        # last + asc => fetch DESC then reverse client-side
        assert "ORDER BY `id` DESC" in sql
        sample = json.loads(w.sample_data)
        assert sample["mode"] == "last"
        # Rows reversed back to ascending for display
        assert [r[0] for r in sample["rows"]] == ["1", "2", "3"]

    def test_load_sample_data_defaults(self):
        from marimo_databricks_connect._table_widget import TableWidget

        ws = MagicMock()
        ws.tables.get.return_value = _make_table()
        w = TableWidget(full_name="main.bronze.events", workspace_client=ws)
        mock_df = MagicMock()
        mock_df.columns = ["id"]
        mock_df.collect.return_value = []
        spark = MagicMock()
        spark.sql.return_value = mock_df
        w._spark = spark

        w.request = json.dumps({"action": "get_sample_data"})
        sql = spark.sql.call_args[0][0]
        assert "LIMIT 50" in sql
        assert "ORDER BY" not in sql
        assert "WHERE" not in sql

    def test_load_history(self):
        from marimo_databricks_connect._table_widget import TableWidget

        ws = MagicMock()
        ws.tables.get.return_value = _make_table()
        w = TableWidget(full_name="main.bronze.events", workspace_client=ws)
        mock_df = MagicMock()
        mock_df.columns = ["version", "timestamp", "operation", "operationParameters"]
        mock_df.collect.return_value = [
            (5, "2024-01-01", "WRITE", {"mode": "Append"}),
            (4, "2023-12-31", "OPTIMIZE", {}),
        ]
        spark = MagicMock()
        spark.sql.return_value = mock_df
        w._spark = spark

        w.request = json.dumps({"action": "get_history", "limit": 25})
        sql = spark.sql.call_args[0][0]
        assert sql.startswith("DESCRIBE HISTORY `main`.`bronze`.`events`")
        assert "LIMIT 25" in sql
        history = json.loads(w.history_data)
        assert history["limit"] == 25
        assert history["columns"][0] == "version"
        assert len(history["rows"]) == 2
        # dict serialized as object
        assert history["rows"][0][3] == {"mode": "Append"}
        assert history["rows"][0][0] == 5

    def test_load_history_no_spark(self):
        from marimo_databricks_connect._table_widget import TableWidget

        ws = MagicMock()
        ws.tables.get.return_value = _make_table()
        w = TableWidget(full_name="main.bronze.events", workspace_client=ws)
        # Force spark unavailable
        w._spark = None
        w._get_spark = lambda: None
        w.request = json.dumps({"action": "get_history"})
        assert "Spark session not available" in w.error_message


# ===================================================================
# SchemaWidget tests
# ===================================================================


class TestSchemaWidget:
    def test_init_loads_schema_and_tables(self):
        from marimo_databricks_connect._schema_widget import SchemaWidget

        ws = MagicMock()
        ws.schemas.get.return_value = SimpleNamespace(
            name="bronze",
            catalog_name="main",
            full_name="main.bronze",
            comment="Bronze layer",
            owner="admin",
            created_at=1700000000000,
            created_by="admin",
            updated_at=None,
            updated_by=None,
            storage_location=None,
            storage_root=None,
            properties={},
        )
        ws.tables.list.return_value = [
            SimpleNamespace(
                name="events",
                full_name="main.bronze.events",
                table_type=SimpleNamespace(value="MANAGED"),
                data_source_format=SimpleNamespace(value="DELTA"),
                comment=None,
                owner="admin",
                created_at=None,
                created_by=None,
                updated_at=None,
            ),
        ]
        w = SchemaWidget(catalog_name="main", schema_name="bronze", workspace_client=ws)
        schema = json.loads(w.schema_data)
        assert schema["name"] == "bronze"
        tables = json.loads(w.tables_data)
        assert len(tables) == 1
        assert tables[0]["name"] == "events"

    def test_factory_function(self):
        from marimo_databricks_connect import schema_widget

        ws = MagicMock()
        ws.schemas.get.return_value = SimpleNamespace(
            name="b",
            catalog_name="c",
            full_name="c.b",
            comment=None,
            owner=None,
            created_at=None,
            created_by=None,
            updated_at=None,
            updated_by=None,
            storage_location=None,
            storage_root=None,
            properties={},
        )
        ws.tables.list.return_value = []
        w = schema_widget("c", "b", workspace_client=ws)
        from marimo_databricks_connect._schema_widget import SchemaWidget

        assert isinstance(w, SchemaWidget)


# ===================================================================
# ClusterWidget tests
# ===================================================================


def _make_cluster(state="RUNNING"):
    return SimpleNamespace(
        cluster_id="abc-123",
        cluster_name="my-cluster",
        state=SimpleNamespace(value=state),
        state_message=None,
        creator_user_name="admin",
        spark_version="14.3.x-scala2.12",
        node_type_id="Standard_DS3_v2",
        driver_node_type_id="Standard_DS3_v2",
        num_workers=4,
        autoscale=None,
        cluster_cores=16,
        cluster_memory_mb=65536,
        autotermination_minutes=120,
        start_time=1700000000000,
        terminated_time=None,
        last_restarted_time=None,
        data_security_mode=SimpleNamespace(value="SINGLE_USER"),
        single_user_name="admin@test.com",
        cluster_source=SimpleNamespace(value="UI"),
        policy_id=None,
        instance_pool_id=None,
        is_single_node=False,
        custom_tags={"env": "dev"},
        runtime_engine=SimpleNamespace(value="PHOTON"),
        termination_reason=None,
        spark_conf={"spark.speculation": "true"},
        spark_env_vars={"PYSPARK_PYTHON": "/usr/bin/python3"},
        init_scripts=[
            SimpleNamespace(
                workspace=SimpleNamespace(destination="/Users/admin/init.sh"),
                volumes=None,
                dbfs=None,
                abfss=None,
                file=None,
                s3=None,
                gcs=None,
            ),
        ],
        enable_elastic_disk=True,
        enable_local_disk_encryption=False,
    )


class TestClusterWidget:
    def test_init_loads_cluster(self):
        from marimo_databricks_connect._cluster_widget import ClusterWidget

        ws = MagicMock()
        ws.clusters.get.return_value = _make_cluster()
        w = ClusterWidget(cluster_id="abc-123", workspace_client=ws)
        data = json.loads(w.cluster_data)
        assert data["cluster_name"] == "my-cluster"
        assert data["state"] == "RUNNING"
        assert data["cluster_cores"] == 16

    def test_init_serializes_spark_conf_and_env_vars(self):
        from marimo_databricks_connect._cluster_widget import ClusterWidget

        ws = MagicMock()
        ws.clusters.get.return_value = _make_cluster()
        w = ClusterWidget(cluster_id="abc-123", workspace_client=ws)
        data = json.loads(w.cluster_data)
        assert data["spark_conf"] == {"spark.speculation": "true"}
        assert data["spark_env_vars"] == {"PYSPARK_PYTHON": "/usr/bin/python3"}
        assert len(data["init_scripts"]) == 1
        assert data["init_scripts"][0]["type"] == "workspace"
        assert data["init_scripts"][0]["destination"] == "/Users/admin/init.sh"
        assert data["enable_elastic_disk"] is True
        assert data["enable_local_disk_encryption"] is False

    def test_start_action(self):
        from marimo_databricks_connect._cluster_widget import ClusterWidget

        ws = MagicMock()
        ws.clusters.get.return_value = _make_cluster("TERMINATED")
        w = ClusterWidget(cluster_id="abc-123", workspace_client=ws)
        w.request = json.dumps({"action": "start"})
        ws.clusters.start.assert_called_once_with("abc-123")
        result = json.loads(w.action_result)
        assert result["success"] is True

    def test_stop_action(self):
        from marimo_databricks_connect._cluster_widget import ClusterWidget

        ws = MagicMock()
        ws.clusters.get.return_value = _make_cluster("RUNNING")
        w = ClusterWidget(cluster_id="abc-123", workspace_client=ws)
        w.request = json.dumps({"action": "stop"})
        ws.clusters.delete.assert_called_once_with("abc-123")

    def test_restart_action(self):
        from marimo_databricks_connect._cluster_widget import ClusterWidget

        ws = MagicMock()
        ws.clusters.get.return_value = _make_cluster("RUNNING")
        w = ClusterWidget(cluster_id="abc-123", workspace_client=ws)
        w.request = json.dumps({"action": "restart"})
        ws.clusters.restart.assert_called_once_with("abc-123")

    def test_edit_cluster(self):
        from marimo_databricks_connect._cluster_widget import ClusterWidget

        ws = MagicMock()
        ws.clusters.get.return_value = _make_cluster()
        w = ClusterWidget(cluster_id="abc-123", workspace_client=ws)
        w.request = json.dumps(
            {
                "action": "edit",
                "config": {
                    "node_type_id": "Standard_DS4_v2",
                    "num_workers": 8,
                    "runtime_engine": "PHOTON",
                    "spark_conf": {"spark.speculation": "false"},
                    "spark_env_vars": {"MY_VAR": "hello"},
                    "custom_tags": {"team": "data"},
                    "init_scripts": [{"type": "volumes", "destination": "/Volumes/c/s/v/init.sh"}],
                },
            }
        )
        ws.clusters.edit.assert_called_once()
        call_kwargs = ws.clusters.edit.call_args
        assert call_kwargs.kwargs["cluster_id"] == "abc-123"
        assert call_kwargs.kwargs["node_type_id"] == "Standard_DS4_v2"
        assert call_kwargs.kwargs["num_workers"] == 8
        assert call_kwargs.kwargs["spark_conf"] == {"spark.speculation": "false"}
        assert call_kwargs.kwargs["spark_env_vars"] == {"MY_VAR": "hello"}
        assert call_kwargs.kwargs["custom_tags"] == {"team": "data"}
        result = json.loads(w.action_result)
        assert result["success"] is True

    def test_edit_cluster_autoscale(self):
        from marimo_databricks_connect._cluster_widget import ClusterWidget

        ws = MagicMock()
        ws.clusters.get.return_value = _make_cluster()
        w = ClusterWidget(cluster_id="abc-123", workspace_client=ws)
        w.request = json.dumps(
            {
                "action": "edit",
                "config": {
                    "autoscale_min": 2,
                    "autoscale_max": 10,
                },
            }
        )
        ws.clusters.edit.assert_called_once()
        call_kwargs = ws.clusters.edit.call_args.kwargs
        assert call_kwargs["autoscale"].min_workers == 2
        assert call_kwargs["autoscale"].max_workers == 10
        assert "num_workers" not in call_kwargs

    def test_edit_cluster_error_handling(self):
        from marimo_databricks_connect._cluster_widget import ClusterWidget

        ws = MagicMock()
        ws.clusters.get.return_value = _make_cluster()
        ws.clusters.edit.side_effect = RuntimeError("forbidden")
        w = ClusterWidget(cluster_id="abc-123", workspace_client=ws)
        w.request = json.dumps({"action": "edit", "config": {"num_workers": 99}})
        result = json.loads(w.action_result)
        assert result["success"] is False
        assert "forbidden" in result["message"]

    def test_load_edit_options(self):
        from marimo_databricks_connect._cluster_widget import ClusterWidget

        ws = MagicMock()
        ws.clusters.get.return_value = _make_cluster()
        ws.clusters.list_node_types.return_value = SimpleNamespace(
            node_types=[
                SimpleNamespace(
                    node_type_id="Standard_DS3_v2",
                    description="DS3 v2",
                    memory_mb=14336,
                    num_cores=4,
                    num_gpus=0,
                    is_deprecated=False,
                ),
            ]
        )
        ws.clusters.spark_versions.return_value = SimpleNamespace(
            versions=[
                SimpleNamespace(key="14.3.x-scala2.12", name="14.3 LTS (Scala 2.12)"),
            ]
        )
        w = ClusterWidget(cluster_id="abc-123", workspace_client=ws)
        w.request = json.dumps({"action": "get_edit_options"})
        node_types = json.loads(w.node_types_data)
        assert len(node_types) == 1
        assert node_types[0]["node_type_id"] == "Standard_DS3_v2"
        spark_versions = json.loads(w.spark_versions_data)
        assert len(spark_versions) == 1
        assert spark_versions[0]["key"] == "14.3.x-scala2.12"

    def test_factory_function(self):
        from marimo_databricks_connect import cluster_widget

        ws = MagicMock()
        ws.clusters.get.return_value = _make_cluster()
        w = cluster_widget("abc-123", workspace_client=ws)
        from marimo_databricks_connect._cluster_widget import ClusterWidget

        assert isinstance(w, ClusterWidget)


# ===================================================================
# WarehouseWidget tests
# ===================================================================


def _make_warehouse(state="RUNNING"):
    return SimpleNamespace(
        id="wh-123",
        name="my-warehouse",
        state=SimpleNamespace(value=state),
        cluster_size="Small",
        min_num_clusters=1,
        max_num_clusters=4,
        num_clusters=2,
        num_active_sessions=5,
        auto_stop_mins=15,
        enable_photon=True,
        enable_serverless_compute=False,
        warehouse_type=SimpleNamespace(value="PRO"),
        creator_name="admin",
        health=SimpleNamespace(status=SimpleNamespace(value="HEALTHY"), message=None),
        tags=SimpleNamespace(
            custom_tags=[
                SimpleNamespace(key="env", value="prod"),
            ]
        ),
    )


class TestWarehouseWidget:
    def test_init_loads_warehouse(self):
        from marimo_databricks_connect._warehouse_widget import WarehouseWidget

        ws = MagicMock()
        ws.warehouses.get.return_value = _make_warehouse()
        w = WarehouseWidget(warehouse_id="wh-123", workspace_client=ws)
        data = json.loads(w.warehouse_data)
        assert data["name"] == "my-warehouse"
        assert data["state"] == "RUNNING"
        assert data["num_clusters"] == 2
        assert data["enable_photon"] is True
        assert data["tags"] == {"env": "prod"}

    def test_start_action(self):
        from marimo_databricks_connect._warehouse_widget import WarehouseWidget

        ws = MagicMock()
        ws.warehouses.get.return_value = _make_warehouse("STOPPED")
        w = WarehouseWidget(warehouse_id="wh-123", workspace_client=ws)
        w.request = json.dumps({"action": "start"})
        ws.warehouses.start.assert_called_once_with("wh-123")

    def test_stop_action(self):
        from marimo_databricks_connect._warehouse_widget import WarehouseWidget

        ws = MagicMock()
        ws.warehouses.get.return_value = _make_warehouse("RUNNING")
        w = WarehouseWidget(warehouse_id="wh-123", workspace_client=ws)
        w.request = json.dumps({"action": "stop"})
        ws.warehouses.stop.assert_called_once_with("wh-123")

    def test_factory_function(self):
        from marimo_databricks_connect import warehouse_widget

        ws = MagicMock()
        ws.warehouses.get.return_value = _make_warehouse()
        w = warehouse_widget("wh-123", workspace_client=ws)
        from marimo_databricks_connect._warehouse_widget import WarehouseWidget

        assert isinstance(w, WarehouseWidget)


# ===================================================================
# ServingEndpointWidget tests
# ===================================================================


def _make_serving_endpoint():
    return SimpleNamespace(
        name="my-model",
        id="ep-123",
        creator="admin",
        creation_timestamp=1700000000000,
        last_updated_timestamp=1700000000000,
        state=SimpleNamespace(ready=SimpleNamespace(value="READY"), config_update=None),
        config=SimpleNamespace(
            served_entities=[
                SimpleNamespace(
                    name="model-v1",
                    entity_name="my_model",
                    entity_version="1",
                    model_name=None,
                    model_version=None,
                    workload_size="Small",
                    scale_to_zero_enabled=True,
                    workload_type=None,
                    state=SimpleNamespace(deployment=SimpleNamespace(value="DEPLOYMENT_READY")),
                ),
            ],
            served_models=None,
            traffic_config=SimpleNamespace(
                routes=[
                    SimpleNamespace(served_model_name="model-v1", traffic_percentage=100),
                ]
            ),
        ),
        pending_config=None,
        permission_level=SimpleNamespace(value="CAN_MANAGE"),
    )


class TestServingEndpointWidget:
    def test_init_loads_endpoint(self):
        from marimo_databricks_connect._serving_endpoint_widget import ServingEndpointWidget

        ws = MagicMock()
        ws.serving_endpoints.get.return_value = _make_serving_endpoint()
        w = ServingEndpointWidget(endpoint_name="my-model", workspace_client=ws)
        data = json.loads(w.endpoint_data)
        assert data["name"] == "my-model"
        assert data["state"] == "READY"
        assert len(data["served_entities"]) == 1
        assert data["served_entities"][0]["entity_name"] == "my_model"
        assert data["traffic_config"]["routes"][0]["traffic_percentage"] == 100

    def test_query_action(self):
        from marimo_databricks_connect._serving_endpoint_widget import ServingEndpointWidget

        ws = MagicMock()
        ws.serving_endpoints.get.return_value = _make_serving_endpoint()
        ws.serving_endpoints.query.return_value = SimpleNamespace(predictions=[0.5])
        w = ServingEndpointWidget(endpoint_name="my-model", workspace_client=ws)
        w.request = json.dumps({"action": "query", "payload": '{"dataframe_records": [{"x": 1}]}'})
        result = json.loads(w.query_result)
        assert result["success"] is True

    def test_factory_function(self):
        from marimo_databricks_connect import serving_endpoint_widget

        ws = MagicMock()
        ws.serving_endpoints.get.return_value = _make_serving_endpoint()
        w = serving_endpoint_widget("my-model", workspace_client=ws)
        from marimo_databricks_connect._serving_endpoint_widget import ServingEndpointWidget

        assert isinstance(w, ServingEndpointWidget)


# ===================================================================
# ExternalLocationWidget tests
# ===================================================================


def _make_ext_location():
    return SimpleNamespace(
        name="finops_landing",
        url="abfss://container@account.dfs.core.windows.net/data",
        credential_name="my_cred",
        comment="Landing zone",
        owner="admin",
        read_only=False,
        created_at=1700000000000,
        created_by="admin",
        updated_at=1700000000000,
        updated_by="admin",
        isolation_mode=SimpleNamespace(value="OPEN"),
        fallback=False,
    )


class TestExternalLocationWidget:
    def test_init_loads_location_by_name(self):
        from marimo_databricks_connect._external_location_widget import ExternalLocationWidget

        ws = MagicMock()
        ws.external_locations.get.return_value = _make_ext_location()
        w = ExternalLocationWidget(location_name="finops_landing", workspace_client=ws)
        data = json.loads(w.location_data)
        assert data["name"] == "finops_landing"
        assert "abfss://" in data["url"]
        assert data["credential_name"] == "my_cred"
        assert data["read_only"] is False
        assert data["is_raw_path"] is False
        ws.external_locations.get.assert_called_once_with("finops_landing")

    def test_init_by_raw_abfss_path(self):
        from marimo_databricks_connect._external_location_widget import ExternalLocationWidget

        ws = MagicMock()  # should NOT be called for metadata
        w = ExternalLocationWidget(
            location_name="abfss://container@account.dfs.core.windows.net/data",
            workspace_client=ws,
        )
        data = json.loads(w.location_data)
        assert data["is_raw_path"] is True
        assert data["url"] == "abfss://container@account.dfs.core.windows.net/data"
        assert data["name"] == "abfss://container@account.dfs.core.windows.net/data"
        # Should NOT have tried to call external_locations.get
        ws.external_locations.get.assert_not_called()

    def test_init_by_raw_s3_path(self):
        from marimo_databricks_connect._external_location_widget import ExternalLocationWidget

        ws = MagicMock()
        w = ExternalLocationWidget(
            location_name="s3://my-bucket/prefix",
            workspace_client=ws,
        )
        data = json.loads(w.location_data)
        assert data["is_raw_path"] is True
        assert data["url"] == "s3://my-bucket/prefix"
        ws.external_locations.get.assert_not_called()

    def test_init_by_raw_volumes_path(self):
        from marimo_databricks_connect._external_location_widget import ExternalLocationWidget

        ws = MagicMock()
        w = ExternalLocationWidget(
            location_name="/Volumes/catalog/schema/vol",
            workspace_client=ws,
        )
        data = json.loads(w.location_data)
        assert data["is_raw_path"] is True
        assert data["url"] == "/Volumes/catalog/schema/vol"
        ws.external_locations.get.assert_not_called()

    def test_permissions_blocked_for_raw_path(self):
        from marimo_databricks_connect._external_location_widget import ExternalLocationWidget

        ws = MagicMock()
        w = ExternalLocationWidget(
            location_name="abfss://c@a.dfs.core.windows.net/d",
            workspace_client=ws,
        )
        w.request = json.dumps({"action": "get_permissions"})
        assert "raw path" in w.error_message.lower()

    def test_validate_blocked_for_raw_path(self):
        from marimo_databricks_connect._external_location_widget import ExternalLocationWidget

        ws = MagicMock()
        w = ExternalLocationWidget(
            location_name="abfss://c@a.dfs.core.windows.net/d",
            workspace_client=ws,
        )
        w.request = json.dumps({"action": "validate"})
        assert "raw path" in w.error_message.lower()

    def test_needs_spark_listing_helper(self):
        from marimo_databricks_connect._external_location_widget import _needs_spark_listing

        assert _needs_spark_listing("abfss://c@a.dfs.core.windows.net/d") is True
        assert _needs_spark_listing("s3://bucket/prefix") is True
        assert _needs_spark_listing("s3a://bucket/prefix") is True
        assert _needs_spark_listing("gs://bucket/prefix") is True
        assert _needs_spark_listing("dbfs:/mnt/data") is False
        assert _needs_spark_listing("file:/tmp/data") is False
        assert _needs_spark_listing("/Volumes/cat/sch/vol") is False

    def test_is_raw_path_helper(self):
        from marimo_databricks_connect._external_location_widget import _is_raw_path

        assert _is_raw_path("abfss://c@a.dfs.core.windows.net/d") is True
        assert _is_raw_path("s3://bucket/prefix") is True
        assert _is_raw_path("/Volumes/cat/sch/vol") is True
        assert _is_raw_path("finops_landing") is False
        assert _is_raw_path("my_location_name") is False

    def test_factory_function_with_name(self):
        from marimo_databricks_connect import external_location_widget

        ws = MagicMock()
        ws.external_locations.get.return_value = _make_ext_location()
        w = external_location_widget("finops_landing", workspace_client=ws)
        from marimo_databricks_connect._external_location_widget import ExternalLocationWidget

        assert isinstance(w, ExternalLocationWidget)

    def test_factory_function_with_path(self):
        from marimo_databricks_connect import external_location_widget

        ws = MagicMock()
        w = external_location_widget(
            "abfss://container@account.dfs.core.windows.net/data",
            workspace_client=ws,
        )
        from marimo_databricks_connect._external_location_widget import ExternalLocationWidget

        assert isinstance(w, ExternalLocationWidget)
        data = json.loads(w.location_data)
        assert data["is_raw_path"] is True


# ===================================================================
# SecretScopeWidget tests
# ===================================================================


def _make_secret_scope():
    return SimpleNamespace(
        name="demo-scope",
        backend_type=SimpleNamespace(value="DATABRICKS"),
        keyvault_metadata=None,
    )


def _make_secret_key(name: str, ts: int = 1700000000000):
    return SimpleNamespace(key=name, last_updated_timestamp=ts)


def _make_secret_acl(principal: str, permission: str = "READ"):
    return SimpleNamespace(principal=principal, permission=SimpleNamespace(value=permission))


class TestSecretScopeWidget:
    def test_init_loads_scope_and_keys(self):
        from marimo_databricks_connect._secret_scope_widget import SecretScopeWidget

        ws = MagicMock()
        ws.secrets.list_scopes.return_value = [_make_secret_scope()]
        ws.secrets.list_secrets.return_value = [
            _make_secret_key("zeta"),
            _make_secret_key("alpha"),
        ]
        ws.secrets.list_acls.return_value = [_make_secret_acl("alice@example.com", "READ")]
        w = SecretScopeWidget(scope_name="demo-scope", workspace_client=ws)
        scope = json.loads(w.scope_data)
        keys = json.loads(w.keys_data)
        assert scope["name"] == "demo-scope"
        assert scope["backend_type"] == "DATABRICKS"
        assert [k["key"] for k in keys] == ["alpha", "zeta"]
        perms = json.loads(w.permissions_data)
        assert perms[0]["principal"] == "alice@example.com"
        assert perms[0]["permission"] == "READ"

    def test_get_secret_action_uses_sdk_and_decodes_value(self):
        from marimo_databricks_connect._secret_scope_widget import SecretScopeWidget

        ws = MagicMock()
        ws.secrets.list_scopes.return_value = [_make_secret_scope()]
        ws.secrets.list_secrets.return_value = [_make_secret_key("api-token")]
        ws.secrets.list_acls.return_value = []
        ws.secrets.get_secret.return_value = SimpleNamespace(
            key="api-token",
            value="c3VwZXJzZWNyZXQ=",  # base64("supersecret")
        )
        w = SecretScopeWidget(scope_name="demo-scope", workspace_client=ws)
        w._get_dbutils = lambda: None
        w.request = json.dumps({"action": "get_secret", "key": "api-token"})
        secret = json.loads(w.selected_secret_data)
        assert secret["key"] == "api-token"
        assert secret["value"] == "supersecret"
        ws.secrets.get_secret.assert_called_once_with(scope="demo-scope", key="api-token")

    def test_put_secret_action(self):
        from marimo_databricks_connect._secret_scope_widget import SecretScopeWidget

        ws = MagicMock()
        ws.secrets.list_scopes.return_value = [_make_secret_scope()]
        ws.secrets.list_secrets.side_effect = [
            [_make_secret_key("api-token")],
            [_make_secret_key("api-token", 1700000100000)],
        ]
        ws.secrets.list_acls.return_value = []
        w = SecretScopeWidget(scope_name="demo-scope", workspace_client=ws)
        w.request = json.dumps({"action": "put_secret", "key": "api-token", "value": "new-value"})
        ws.secrets.put_secret.assert_called_once_with(scope="demo-scope", key="api-token", string_value="new-value")
        secret = json.loads(w.selected_secret_data)
        result = json.loads(w.action_result)
        assert secret["key"] == "api-token"
        assert secret["value"] == "new-value"
        assert result["success"] is True

    def test_delete_secret_action(self):
        from marimo_databricks_connect._secret_scope_widget import SecretScopeWidget

        ws = MagicMock()
        ws.secrets.list_scopes.return_value = [_make_secret_scope()]
        ws.secrets.list_secrets.side_effect = [
            [_make_secret_key("api-token")],
            [],
        ]
        ws.secrets.list_acls.return_value = []
        w = SecretScopeWidget(scope_name="demo-scope", workspace_client=ws)
        w.selected_secret_data = json.dumps({"scope": "demo-scope", "key": "api-token", "value": "secret"})
        w.request = json.dumps({"action": "delete_secret", "key": "api-token"})
        ws.secrets.delete_secret.assert_called_once_with(scope="demo-scope", key="api-token")
        result = json.loads(w.action_result)
        assert json.loads(w.selected_secret_data or "{}") == {}
        assert result["success"] is True

    def test_put_acl_action(self):
        from marimo_databricks_connect._secret_scope_widget import SecretScopeWidget

        ws = MagicMock()
        ws.secrets.list_scopes.return_value = [_make_secret_scope()]
        ws.secrets.list_secrets.return_value = []
        ws.secrets.list_acls.side_effect = [
            [_make_secret_acl("alice@example.com", "READ")],
            [_make_secret_acl("alice@example.com", "MANAGE")],
        ]
        w = SecretScopeWidget(scope_name="demo-scope", workspace_client=ws)
        w.request = json.dumps({"action": "put_acl", "principal": "alice@example.com", "permission": "MANAGE"})
        ws.secrets.put_acl.assert_called_once()
        result = json.loads(w.action_result)
        perms = json.loads(w.permissions_data)
        assert result["success"] is True
        assert perms[0]["permission"] == "MANAGE"

    def test_delete_acl_action(self):
        from marimo_databricks_connect._secret_scope_widget import SecretScopeWidget

        ws = MagicMock()
        ws.secrets.list_scopes.return_value = [_make_secret_scope()]
        ws.secrets.list_secrets.return_value = []
        ws.secrets.list_acls.side_effect = [
            [_make_secret_acl("alice@example.com", "READ")],
            [],
        ]
        w = SecretScopeWidget(scope_name="demo-scope", workspace_client=ws)
        w.request = json.dumps({"action": "delete_acl", "principal": "alice@example.com"})
        ws.secrets.delete_acl.assert_called_once_with(scope="demo-scope", principal="alice@example.com")
        result = json.loads(w.action_result)
        perms = json.loads(w.permissions_data)
        assert result["success"] is True
        assert perms == []

    def test_scope_not_found(self):
        from marimo_databricks_connect._secret_scope_widget import SecretScopeWidget

        ws = MagicMock()
        ws.secrets.list_scopes.return_value = []
        ws.secrets.list_acls.return_value = []
        w = SecretScopeWidget(scope_name="missing-scope", workspace_client=ws)
        assert "not found" in w.error_message.lower()

    def test_factory_function(self):
        from marimo_databricks_connect import secret_scope_widget

        ws = MagicMock()
        ws.secrets.list_scopes.return_value = [_make_secret_scope()]
        ws.secrets.list_secrets.return_value = []
        ws.secrets.list_acls.return_value = []
        w = secret_scope_widget("demo-scope", workspace_client=ws)
        from marimo_databricks_connect._secret_scope_widget import SecretScopeWidget

        assert isinstance(w, SecretScopeWidget)


# ===================================================================
# VectorSearchEndpointWidget tests
# ===================================================================


def _make_vs_endpoint():
    return SimpleNamespace(
        name="my-vs-endpoint",
        id="vse-123",
        endpoint_type=SimpleNamespace(value="STANDARD"),
        endpoint_status=SimpleNamespace(
            state=SimpleNamespace(value="ONLINE"),
            message=None,
        ),
        num_indexes=3,
        creator="admin",
        creation_timestamp=1700000000000,
        last_updated_timestamp=1700000000000,
        last_updated_user="admin",
        scaling_info=SimpleNamespace(
            requested_min_qps=10,
            state=SimpleNamespace(value="STABLE"),
        ),
        budget_policy_id=None,
        custom_tags=[
            SimpleNamespace(key="env", value="prod"),
        ],
    )


class TestVectorSearchEndpointWidget:
    def test_init_loads_endpoint(self):
        from marimo_databricks_connect._vector_search_endpoint_widget import VectorSearchEndpointWidget

        ws = MagicMock()
        ws.vector_search_endpoints.get_endpoint.return_value = _make_vs_endpoint()
        w = VectorSearchEndpointWidget(endpoint_name="my-vs-endpoint", workspace_client=ws)
        data = json.loads(w.endpoint_data)
        assert data["name"] == "my-vs-endpoint"
        assert data["state"] == "ONLINE"
        assert data["num_indexes"] == 3
        assert data["endpoint_type"] == "STANDARD"
        assert data["scaling_requested_min_qps"] == 10
        assert data["custom_tags"] == [{"key": "env", "value": "prod"}]

    def test_list_indexes(self):
        from marimo_databricks_connect._vector_search_endpoint_widget import VectorSearchEndpointWidget

        ws = MagicMock()
        ws.vector_search_endpoints.get_endpoint.return_value = _make_vs_endpoint()
        ws.vector_search_indexes.list_indexes.return_value = [
            SimpleNamespace(
                name="cat.sch.idx1",
                endpoint_name="my-vs-endpoint",
                index_type=SimpleNamespace(value="DELTA_SYNC"),
                index_subtype=SimpleNamespace(value="VECTOR"),
                primary_key="id",
                creator="admin",
            ),
        ]
        w = VectorSearchEndpointWidget(endpoint_name="my-vs-endpoint", workspace_client=ws)
        w.request = json.dumps({"action": "list_indexes"})
        indexes = json.loads(w.indexes_data)
        assert len(indexes) == 1
        assert indexes[0]["name"] == "cat.sch.idx1"
        assert indexes[0]["index_type"] == "DELTA_SYNC"

    def test_factory_function(self):
        from marimo_databricks_connect import vector_search_endpoint_widget

        ws = MagicMock()
        ws.vector_search_endpoints.get_endpoint.return_value = _make_vs_endpoint()
        w = vector_search_endpoint_widget("my-vs-endpoint", workspace_client=ws)
        from marimo_databricks_connect._vector_search_endpoint_widget import VectorSearchEndpointWidget

        assert isinstance(w, VectorSearchEndpointWidget)


# ===================================================================
# VectorIndexWidget tests
# ===================================================================


def _make_vector_index():
    return SimpleNamespace(
        name="main.rag.doc_index",
        endpoint_name="my-vs-endpoint",
        index_type=SimpleNamespace(value="DELTA_SYNC"),
        index_subtype=SimpleNamespace(value="VECTOR"),
        primary_key="doc_id",
        creator="admin",
        status=SimpleNamespace(
            ready=True,
            message=None,
            index_url="https://my-workspace.databricks.com/vs/main.rag.doc_index",
            indexed_row_count=15000,
        ),
        delta_sync_index_spec=SimpleNamespace(
            source_table="main.rag.documents",
            pipeline_id="pipe-abc",
            pipeline_type=SimpleNamespace(value="TRIGGERED"),
            embedding_writeback_table="main.rag.doc_index_writeback",
            columns_to_sync=["doc_id", "content", "title"],
            embedding_source_columns=[
                SimpleNamespace(
                    name="content",
                    embedding_model_endpoint_name="databricks-bge-large-en",
                    model_endpoint_name_for_query=None,
                ),
            ],
            embedding_vector_columns=[
                SimpleNamespace(name="content_vector", embedding_dimension=1024),
            ],
        ),
        direct_access_index_spec=None,
    )


class TestVectorIndexWidget:
    def test_init_loads_index(self):
        from marimo_databricks_connect._vector_index_widget import VectorIndexWidget

        ws = MagicMock()
        ws.vector_search_indexes.get_index.return_value = _make_vector_index()
        w = VectorIndexWidget(index_name="main.rag.doc_index", workspace_client=ws)
        data = json.loads(w.index_data)
        assert data["name"] == "main.rag.doc_index"
        assert data["index_type"] == "DELTA_SYNC"
        assert data["status_ready"] is True
        assert data["indexed_row_count"] == 15000
        assert data["source_table"] == "main.rag.documents"
        assert data["pipeline_id"] == "pipe-abc"
        assert len(data["embedding_sources"]) == 1
        assert data["embedding_sources"][0]["name"] == "content"
        assert data["embedding_sources"][0]["embedding_model_endpoint"] == "databricks-bge-large-en"
        assert len(data["embedding_vectors"]) == 1
        assert data["embedding_vectors"][0]["dimension"] == 1024
        assert data["columns_to_sync"] == ["doc_id", "content", "title"]

    def test_sync_action(self):
        from marimo_databricks_connect._vector_index_widget import VectorIndexWidget

        ws = MagicMock()
        ws.vector_search_indexes.get_index.return_value = _make_vector_index()
        w = VectorIndexWidget(index_name="main.rag.doc_index", workspace_client=ws)
        w.request = json.dumps({"action": "sync"})
        ws.vector_search_indexes.sync_index.assert_called_once_with("main.rag.doc_index")
        result = json.loads(w.action_result)
        assert result["success"] is True

    def test_sync_error(self):
        from marimo_databricks_connect._vector_index_widget import VectorIndexWidget

        ws = MagicMock()
        ws.vector_search_indexes.get_index.return_value = _make_vector_index()
        ws.vector_search_indexes.sync_index.side_effect = RuntimeError("forbidden")
        w = VectorIndexWidget(index_name="main.rag.doc_index", workspace_client=ws)
        w.request = json.dumps({"action": "sync"})
        result = json.loads(w.action_result)
        assert result["success"] is False
        assert "forbidden" in result["message"]

    def test_factory_function(self):
        from marimo_databricks_connect import vector_index_widget

        ws = MagicMock()
        ws.vector_search_indexes.get_index.return_value = _make_vector_index()
        w = vector_index_widget("main.rag.doc_index", workspace_client=ws)
        from marimo_databricks_connect._vector_index_widget import VectorIndexWidget

        assert isinstance(w, VectorIndexWidget)


# ===================================================================
# AppWidget tests
# ===================================================================


def _make_app():
    return SimpleNamespace(
        name="my-app",
        id="app-123",
        description="My dashboard app",
        url="https://my-workspace.databricks.com/apps/my-app",
        creator="admin",
        create_time="2024-01-01T00:00:00Z",
        update_time="2024-01-02T00:00:00Z",
        updater="admin",
        app_status=SimpleNamespace(
            state=SimpleNamespace(value="RUNNING"),
            message=None,
        ),
        compute_status=SimpleNamespace(
            state=SimpleNamespace(value="ACTIVE"),
            message=None,
            active_instances=1,
        ),
        compute_size=SimpleNamespace(value="MEDIUM"),
        default_source_code_path="/Workspace/Users/admin/my-app",
        service_principal_name="my-app-sp",
        service_principal_client_id="sp-client-123",
        service_principal_id=12345,
        space=None,
        active_deployment=SimpleNamespace(
            deployment_id="dep-001",
            source_code_path="/Workspace/Users/admin/my-app",
            mode=SimpleNamespace(value="SNAPSHOT"),
            status=SimpleNamespace(
                state=SimpleNamespace(value="SUCCEEDED"),
                message=None,
            ),
            create_time="2024-01-02T00:00:00Z",
            update_time="2024-01-02T00:00:00Z",
            creator="admin",
            command=["python", "app.py"],
            env_vars=[SimpleNamespace(name="DEBUG", value="true")],
            git_source=None,
            deployment_artifacts=None,
        ),
        pending_deployment=None,
        resources=[
            SimpleNamespace(
                name="my-endpoint",
                description="Serving endpoint",
                serving_endpoint=SimpleNamespace(name="fraud-model", permission="CAN_QUERY"),
                sql_warehouse=None,
                job=None,
                secret=None,
                database=None,
                experiment=None,
                uc_securable=None,
                genie_space=None,
                postgres=None,
                app=None,
            ),
        ],
        budget_policy_id=None,
        effective_budget_policy_id=None,
        effective_usage_policy_id=None,
        effective_user_api_scopes=None,
        git_repository=None,
        id_="app-123",
        oauth2_app_client_id=None,
        oauth2_app_integration_id=None,
        telemetry_export_destinations=None,
        usage_policy_id=None,
        user_api_scopes=None,
    )


class TestAppWidget:
    def test_init_loads_app(self):
        from marimo_databricks_connect._app_widget import AppWidget

        ws = MagicMock()
        ws.apps.get.return_value = _make_app()
        ws.api_client.do.return_value = {"thumbnail": "iVBORbase64data"}
        w = AppWidget(app_name="my-app", workspace_client=ws)
        data = json.loads(w.app_data)
        assert data["name"] == "my-app"
        assert data["app_state"] == "RUNNING"
        assert data["compute_state"] == "ACTIVE"
        assert data["compute_active_instances"] == 1
        assert data["url"] == "https://my-workspace.databricks.com/apps/my-app"
        assert data["active_deployment"]["deployment_id"] == "dep-001"
        assert data["active_deployment"]["state"] == "SUCCEEDED"
        assert len(data["resources"]) == 1
        assert data["resources"][0]["type"] == "serving_endpoint"
        # Thumbnail should be loaded on init
        thumb = json.loads(w.thumbnail_data)
        assert thumb["loaded"] is True
        assert thumb["thumbnail"] == "iVBORbase64data"

    def test_init_thumbnail_missing(self):
        """When the app has no thumbnail, it should gracefully return None."""
        from marimo_databricks_connect._app_widget import AppWidget

        ws = MagicMock()
        ws.apps.get.return_value = _make_app()
        ws.api_client.do.side_effect = RuntimeError("NOT_FOUND")
        w = AppWidget(app_name="my-app", workspace_client=ws)
        thumb = json.loads(w.thumbnail_data)
        assert thumb["loaded"] is True
        assert thumb["thumbnail"] is None

    def test_start_action(self):
        from marimo_databricks_connect._app_widget import AppWidget

        ws = MagicMock()
        ws.apps.get.return_value = _make_app()
        w = AppWidget(app_name="my-app", workspace_client=ws)
        w.request = json.dumps({"action": "start"})
        ws.apps.start.assert_called_once_with("my-app")
        result = json.loads(w.action_result)
        assert result["success"] is True

    def test_stop_action(self):
        from marimo_databricks_connect._app_widget import AppWidget

        ws = MagicMock()
        ws.apps.get.return_value = _make_app()
        w = AppWidget(app_name="my-app", workspace_client=ws)
        w.request = json.dumps({"action": "stop"})
        ws.apps.stop.assert_called_once_with("my-app")
        result = json.loads(w.action_result)
        assert result["success"] is True

    def test_deploy_action(self):
        from marimo_databricks_connect._app_widget import AppWidget

        ws = MagicMock()
        ws.apps.get.return_value = _make_app()
        ws.apps.deploy.return_value = SimpleNamespace(deployment_id="dep-002")
        w = AppWidget(app_name="my-app", workspace_client=ws)
        w.request = json.dumps(
            {
                "action": "deploy",
                "config": {
                    "source_code_path": "/Workspace/Users/admin/v2",
                    "mode": "SNAPSHOT",
                },
            }
        )
        ws.apps.deploy.assert_called_once()
        result = json.loads(w.action_result)
        assert result["success"] is True

    def test_update_permissions(self):
        from marimo_databricks_connect._app_widget import AppWidget

        ws = MagicMock()
        ws.apps.get.return_value = _make_app()
        w = AppWidget(app_name="my-app", workspace_client=ws)
        w.request = json.dumps(
            {
                "action": "update_permissions",
                "acl": [
                    {"user_name": "alice@example.com", "permission_level": "CAN_MANAGE"},
                    {"group_name": "data-team", "permission_level": "CAN_USE"},
                ],
            }
        )
        ws.apps.set_permissions.assert_called_once()
        call_args = ws.apps.set_permissions.call_args
        # app_name is the first positional arg
        assert call_args.args[0] == "my-app" or call_args.kwargs.get("app_name") == "my-app"
        acl = call_args.kwargs["access_control_list"]
        assert len(acl) == 2
        result = json.loads(w.action_result)
        assert result["success"] is True

    def test_delete_thumbnail(self):
        from marimo_databricks_connect._app_widget import AppWidget

        ws = MagicMock()
        ws.apps.get.return_value = _make_app()
        w = AppWidget(app_name="my-app", workspace_client=ws)
        w.request = json.dumps({"action": "delete_thumbnail"})
        ws.apps.delete_app_thumbnail.assert_called_once_with("my-app")
        result = json.loads(w.action_result)
        assert result["success"] is True

    def test_update_thumbnail(self):
        from marimo_databricks_connect._app_widget import AppWidget

        ws = MagicMock()
        ws.apps.get.return_value = _make_app()
        w = AppWidget(app_name="my-app", workspace_client=ws)
        w.request = json.dumps({"action": "update_thumbnail", "thumbnail_base64": "iVBOR..."})
        ws.apps.update_app_thumbnail.assert_called_once()
        result = json.loads(w.action_result)
        assert result["success"] is True

    def test_update_thumbnail_refreshes_display(self):
        from marimo_databricks_connect._app_widget import AppWidget

        ws = MagicMock()
        ws.apps.get.return_value = _make_app()
        ws.api_client.do.return_value = {"thumbnail": "old_data"}
        w = AppWidget(app_name="my-app", workspace_client=ws)
        w.request = json.dumps({"action": "update_thumbnail", "thumbnail_base64": "new_data"})
        ws.apps.update_app_thumbnail.assert_called_once()
        result = json.loads(w.action_result)
        assert result["success"] is True
        # thumbnail_data should now show the new image
        thumb = json.loads(w.thumbnail_data)
        assert thumb["thumbnail"] == "new_data"

    def test_delete_thumbnail_clears_display(self):
        from marimo_databricks_connect._app_widget import AppWidget

        ws = MagicMock()
        ws.apps.get.return_value = _make_app()
        ws.api_client.do.return_value = {"thumbnail": "some_data"}
        w = AppWidget(app_name="my-app", workspace_client=ws)
        w.request = json.dumps({"action": "delete_thumbnail"})
        ws.apps.delete_app_thumbnail.assert_called_once_with("my-app")
        result = json.loads(w.action_result)
        assert result["success"] is True
        # thumbnail_data should now be cleared
        thumb = json.loads(w.thumbnail_data)
        assert thumb["thumbnail"] is None

    def test_factory_function(self):
        from marimo_databricks_connect import app_widget

        ws = MagicMock()
        ws.apps.get.return_value = _make_app()
        w = app_widget("my-app", workspace_client=ws)
        from marimo_databricks_connect._app_widget import AppWidget

        assert isinstance(w, AppWidget)


# ===================================================================
# PipelineWidget tests
# ===================================================================


def _make_latest_update(update_id="upd-001", state="COMPLETED", creation_time=1700000000000):
    return SimpleNamespace(
        update_id=update_id,
        state=SimpleNamespace(value=state),
        creation_time=creation_time,
    )


def _make_full_update(update_id="upd-001", state="COMPLETED", full_refresh=False):
    return SimpleNamespace(
        update_id=update_id,
        state=SimpleNamespace(value=state),
        creation_time=1700000000000,
        cause=SimpleNamespace(value="API_CALL"),
        cluster_id="cluster-xyz",
        full_refresh=full_refresh,
        full_refresh_selection=[],
        refresh_selection=[],
        validate_only=False,
    )


def _make_pipeline_spec():
    lib_nb = SimpleNamespace(
        notebook=SimpleNamespace(path="/Users/me/etl"),
        file=None,
        glob=None,
        jar=None,
        whl=None,
        maven=None,
    )
    return SimpleNamespace(
        catalog="main",
        schema="bronze",
        target=None,
        channel="CURRENT",
        edition="ADVANCED",
        continuous=False,
        development=True,
        photon=True,
        serverless=True,
        storage=None,
        root_path=None,
        budget_policy_id=None,
        configuration={"spark.sql.shuffle.partitions": "200"},
        tags={"team": "data"},
        libraries=[lib_nb],
        notifications=[],
    )


def _make_pipeline(state="IDLE", health="HEALTHY"):
    return SimpleNamespace(
        pipeline_id="pl-abc",
        name="bronze_etl",
        state=SimpleNamespace(value=state),
        health=SimpleNamespace(value=health),
        creator_user_name="user@test.com",
        run_as_user_name="user@test.com",
        cluster_id="cluster-xyz",
        last_modified=1700000000000,
        cause=None,
        effective_publishing_mode=SimpleNamespace(value="DIRECT_PUBLISHING"),
        latest_updates=[_make_latest_update()],
        spec=_make_pipeline_spec(),
    )


def _make_pipeline_event(level="INFO", event_type="update_progress", message="started"):
    return SimpleNamespace(
        id="evt-1",
        timestamp="2024-01-01T00:00:00Z",
        level=SimpleNamespace(value=level),
        event_type=event_type,
        maturity_level=SimpleNamespace(value="STABLE"),
        message=message,
        origin=SimpleNamespace(update_id="upd-001", flow_name="bronze", dataset_name="raw_events"),
        error=None,
    )


def _mock_pipeline_ws():
    ws = MagicMock()
    ws.pipelines.get.return_value = _make_pipeline()
    ws.pipelines.list_updates.return_value = SimpleNamespace(updates=[_make_full_update()])
    ws.pipelines.list_pipeline_events.return_value = [_make_pipeline_event()]
    ws.pipelines.start_update.return_value = SimpleNamespace(update_id="upd-002")
    ws.pipelines.stop.return_value = None
    ws.pipelines.list_pipelines.return_value = []
    return ws


class TestPipelineWidget:
    def test_init_loads_pipeline_and_updates(self):
        from marimo_databricks_connect._pipeline_widget import PipelineWidget

        ws = _mock_pipeline_ws()
        w = PipelineWidget(pipeline_id="pl-abc", workspace_client=ws)
        data = json.loads(w.pipeline_data)
        assert data["pipeline_id"] == "pl-abc"
        assert data["name"] == "bronze_etl"
        assert data["state"] == "IDLE"
        assert data["health"] == "HEALTHY"
        assert data["spec"]["catalog"] == "main"
        assert data["spec"]["schema"] == "bronze"
        assert data["spec"]["serverless"] is True
        assert data["spec"]["development"] is True
        assert data["spec"]["libraries"] == [{"type": "notebook", "value": "/Users/me/etl"}]
        assert data["spec"]["configuration"] == {"spark.sql.shuffle.partitions": "200"}
        assert data["latest_updates"][0]["update_id"] == "upd-001"
        updates = json.loads(w.updates_data)
        assert len(updates) == 1
        assert updates[0]["state"] == "COMPLETED"
        assert w.loading is False

    def test_init_by_name_resolves_id(self):
        from marimo_databricks_connect._pipeline_widget import PipelineWidget

        ws = _mock_pipeline_ws()
        ws.pipelines.list_pipelines.return_value = [
            SimpleNamespace(pipeline_id="pl-found", name="bronze_etl"),
        ]
        ws.pipelines.get.return_value = _make_pipeline()
        w = PipelineWidget(pipeline_name="bronze_etl", workspace_client=ws)
        ws.pipelines.list_pipelines.assert_called_once()
        ws.pipelines.get.assert_called_with("pl-found")
        assert w._pipeline_id == "pl-found"

    def test_init_by_name_not_found(self):
        from marimo_databricks_connect._pipeline_widget import PipelineWidget

        ws = _mock_pipeline_ws()
        ws.pipelines.list_pipelines.return_value = []
        w = PipelineWidget(pipeline_name="missing", workspace_client=ws)
        assert "not found" in w.error_message

    def test_start_update_action(self):
        from marimo_databricks_connect._pipeline_widget import PipelineWidget

        ws = _mock_pipeline_ws()
        w = PipelineWidget(pipeline_id="pl-abc", workspace_client=ws)
        w.request = json.dumps({"action": "start_update"})
        ws.pipelines.start_update.assert_called_with("pl-abc")
        result = json.loads(w.action_result)
        assert result["success"] is True
        assert result["update_id"] == "upd-002"

    def test_full_refresh_action(self):
        from marimo_databricks_connect._pipeline_widget import PipelineWidget

        ws = _mock_pipeline_ws()
        w = PipelineWidget(pipeline_id="pl-abc", workspace_client=ws)
        w.request = json.dumps({"action": "start_update", "full_refresh": True})
        ws.pipelines.start_update.assert_called_with("pl-abc", full_refresh=True)
        result = json.loads(w.action_result)
        assert result["success"] is True
        assert "full refresh" in result["message"].lower()

    def test_validate_only_action(self):
        from marimo_databricks_connect._pipeline_widget import PipelineWidget

        ws = _mock_pipeline_ws()
        w = PipelineWidget(pipeline_id="pl-abc", workspace_client=ws)
        w.request = json.dumps({"action": "start_update", "validate_only": True})
        ws.pipelines.start_update.assert_called_with("pl-abc", validate_only=True)
        result = json.loads(w.action_result)
        assert result["success"] is True
        assert "valid" in result["message"].lower()

    def test_selective_refresh(self):
        from marimo_databricks_connect._pipeline_widget import PipelineWidget

        ws = _mock_pipeline_ws()
        w = PipelineWidget(pipeline_id="pl-abc", workspace_client=ws)
        w.request = json.dumps(
            {
                "action": "start_update",
                "refresh_selection": ["bronze.events"],
                "full_refresh_selection": ["bronze.dim"],
            }
        )
        ws.pipelines.start_update.assert_called_with(
            "pl-abc", full_refresh_selection=["bronze.dim"], refresh_selection=["bronze.events"]
        )

    def test_stop_action(self):
        from marimo_databricks_connect._pipeline_widget import PipelineWidget

        ws = _mock_pipeline_ws()
        w = PipelineWidget(pipeline_id="pl-abc", workspace_client=ws)
        w.request = json.dumps({"action": "stop"})
        ws.pipelines.stop.assert_called_once_with("pl-abc")
        result = json.loads(w.action_result)
        assert result["success"] is True

    def test_get_events_lazy_load(self):
        from marimo_databricks_connect._pipeline_widget import PipelineWidget

        ws = _mock_pipeline_ws()
        w = PipelineWidget(pipeline_id="pl-abc", workspace_client=ws)
        # events not loaded on init
        assert w.events_data == "[]"
        w.request = json.dumps({"action": "get_events"})
        events = json.loads(w.events_data)
        assert len(events) == 1
        assert events[0]["level"] == "INFO"
        assert events[0]["event_type"] == "update_progress"
        assert events[0]["update_id"] == "upd-001"

    def test_get_events_with_filter(self):
        from marimo_databricks_connect._pipeline_widget import PipelineWidget

        ws = _mock_pipeline_ws()
        w = PipelineWidget(pipeline_id="pl-abc", workspace_client=ws)
        w.request = json.dumps({"action": "get_events", "filter": "level='ERROR'"})
        ws.pipelines.list_pipeline_events.assert_called_with(
            "pl-abc", max_results=100, order_by=["timestamp desc"], filter="level='ERROR'"
        )

    def test_error_handling_on_get(self):
        from marimo_databricks_connect._pipeline_widget import PipelineWidget

        ws = _mock_pipeline_ws()
        ws.pipelines.get.side_effect = RuntimeError("forbidden")
        ws.pipelines.list_updates.side_effect = RuntimeError("forbidden")
        w = PipelineWidget(pipeline_id="pl-abc", workspace_client=ws)
        assert "forbidden" in w.error_message

    def test_action_failure_reports_error(self):
        from marimo_databricks_connect._pipeline_widget import PipelineWidget

        ws = _mock_pipeline_ws()
        w = PipelineWidget(pipeline_id="pl-abc", workspace_client=ws)
        ws.pipelines.start_update.side_effect = RuntimeError("permission denied")
        w.request = json.dumps({"action": "start_update"})
        result = json.loads(w.action_result)
        assert result["success"] is False
        assert "permission denied" in result["message"]

    def test_factory_function(self):
        from marimo_databricks_connect import pipeline_widget

        ws = _mock_pipeline_ws()
        w = pipeline_widget("pl-abc", workspace_client=ws)
        from marimo_databricks_connect._pipeline_widget import PipelineWidget

        assert isinstance(w, PipelineWidget)

    # -- new: permissions, graph, settings -----------------------------

    def _mk_perms(self):
        from databricks.sdk.service import iam

        return SimpleNamespace(
            object_id="/pipelines/pl-abc",
            object_type="pipelines",
            access_control_list=[
                SimpleNamespace(
                    user_name="alice@test.com",
                    group_name=None,
                    service_principal_name=None,
                    display_name="Alice",
                    all_permissions=[
                        SimpleNamespace(
                            permission_level=iam.PermissionLevel("CAN_MANAGE"),
                            inherited=False,
                        )
                    ],
                ),
                SimpleNamespace(
                    user_name=None,
                    group_name="data-eng",
                    service_principal_name=None,
                    display_name="data-eng",
                    all_permissions=[
                        SimpleNamespace(
                            permission_level=iam.PermissionLevel("CAN_VIEW"),
                            inherited=True,
                        )
                    ],
                ),
            ],
        )

    def test_get_permissions(self):
        from marimo_databricks_connect._pipeline_widget import PipelineWidget

        ws = _mock_pipeline_ws()
        ws.pipelines.get_permissions.return_value = self._mk_perms()
        w = PipelineWidget(pipeline_id="pl-abc", workspace_client=ws)
        w.request = json.dumps({"action": "get_permissions"})
        ws.pipelines.get_permissions.assert_called_once_with("pl-abc")
        data = json.loads(w.permissions_data)
        assert data["object_type"] == "pipelines"
        assert len(data["acl"]) == 2
        assert data["acl"][0]["principal"] == "alice@test.com"
        assert data["acl"][0]["type"] == "user"
        assert data["acl"][0]["permissions"][0]["level"] == "CAN_MANAGE"
        assert data["acl"][1]["type"] == "group"
        assert data["acl"][1]["permissions"][0]["inherited"] is True

    def test_update_permissions(self):
        from marimo_databricks_connect._pipeline_widget import PipelineWidget

        ws = _mock_pipeline_ws()
        ws.pipelines.get_permissions.return_value = self._mk_perms()
        w = PipelineWidget(pipeline_id="pl-abc", workspace_client=ws)
        w.request = json.dumps(
            {
                "action": "update_permissions",
                "acl": [
                    {"user_name": "bob@test.com", "permission_level": "CAN_RUN"},
                    {"group_name": "data-eng", "permission_level": "CAN_MANAGE"},
                ],
            }
        )
        result = json.loads(w.action_result)
        assert result["success"] is True
        ws.pipelines.set_permissions.assert_called_once()
        kwargs = ws.pipelines.set_permissions.call_args.kwargs
        acl = kwargs["access_control_list"]
        assert len(acl) == 2
        assert acl[0].user_name == "bob@test.com"
        assert acl[0].permission_level.value == "CAN_RUN"
        assert acl[1].group_name == "data-eng"
        assert acl[1].permission_level.value == "CAN_MANAGE"

    def test_get_graph(self):
        from marimo_databricks_connect._pipeline_widget import PipelineWidget

        ws = _mock_pipeline_ws()
        ws.api_client.do.return_value = {
            "events": [
                {
                    "details": {
                        "flow_definition": {
                            "output_dataset": "silver_orders",
                            "flow_type": "MATERIALIZED_VIEW",
                            "input_datasets": [
                                {"name": "bronze_orders"},
                                {"name": "lookup_customers"},
                            ],
                        }
                    },
                    "origin": {"flow_id": "f-2", "update_id": "upd-001"},
                },
                {
                    "details": {
                        "flow_definition": {
                            "output_dataset": "bronze_orders",
                            "flow_type": "STREAMING_TABLE",
                            "input_datasets": [{"name": "external.raw_orders"}],
                        }
                    },
                    "origin": {"flow_id": "f-1", "update_id": "upd-001"},
                },
                # Older duplicate of silver_orders should be ignored.
                {
                    "details": {
                        "flow_definition": {
                            "output_dataset": "silver_orders",
                            "input_datasets": [{"name": "stale"}],
                        }
                    },
                    "origin": {},
                },
            ]
        }
        w = PipelineWidget(pipeline_id="pl-abc", workspace_client=ws)
        w.request = json.dumps({"action": "get_graph"})

        # API path & filter
        ws.api_client.do.assert_called_with(
            "GET",
            "/api/2.0/pipelines/pl-abc/events",
            query={
                "max_results": 250,
                "filter": "event_type='flow_definition'",
                "order_by": "timestamp desc",
            },
        )
        graph = json.loads(w.graph_data)
        names = {n["name"] for n in graph["nodes"]}
        assert names == {"silver_orders", "bronze_orders", "lookup_customers", "external.raw_orders"}
        # External sources marked
        ext = next(n for n in graph["nodes"] if n["name"] == "external.raw_orders")
        assert ext["external"] is True
        # Edges
        edge_pairs = {(e["from"], e["to"]) for e in graph["edges"]}
        assert ("bronze_orders", "silver_orders") in edge_pairs
        assert ("lookup_customers", "silver_orders") in edge_pairs
        assert ("external.raw_orders", "bronze_orders") in edge_pairs

    def test_get_graph_for_specific_update(self):
        from marimo_databricks_connect._pipeline_widget import PipelineWidget

        ws = _mock_pipeline_ws()
        ws.api_client.do.return_value = {"events": []}
        w = PipelineWidget(pipeline_id="pl-abc", workspace_client=ws)
        w.request = json.dumps({"action": "get_graph", "update_id": "upd-001"})
        kwargs = ws.api_client.do.call_args.kwargs
        assert "update_id='upd-001'" in kwargs["query"]["filter"]

    def test_update_settings_notifications(self):
        from marimo_databricks_connect._pipeline_widget import PipelineWidget

        ws = _mock_pipeline_ws()
        w = PipelineWidget(pipeline_id="pl-abc", workspace_client=ws)
        w.request = json.dumps(
            {
                "action": "update_settings",
                "settings": {
                    "notifications": [
                        {
                            "alerts": ["on-update-failure"],
                            "email_recipients": ["oncall@test.com"],
                        }
                    ]
                },
            }
        )
        result = json.loads(w.action_result)
        assert result["success"] is True, result
        ws.pipelines.update.assert_called_once()
        kwargs = ws.pipelines.update.call_args.kwargs
        # The full spec must be re-sent (PUT-style).
        assert kwargs["catalog"] == "main"
        assert kwargs["schema"] == "bronze"
        assert kwargs["serverless"] is True
        # Notifications replaced.
        notifs = kwargs["notifications"]
        assert len(notifs) == 1
        assert notifs[0].alerts == ["on-update-failure"]
        assert notifs[0].email_recipients == ["oncall@test.com"]

    def test_update_settings_clusters_and_config(self):
        from marimo_databricks_connect._pipeline_widget import PipelineWidget

        ws = _mock_pipeline_ws()
        w = PipelineWidget(pipeline_id="pl-abc", workspace_client=ws)
        w.request = json.dumps(
            {
                "action": "update_settings",
                "settings": {
                    "configuration": {"spark.sql.shuffle.partitions": "400"},
                    "clusters": [
                        {
                            "label": "default",
                            "node_type_id": "i3.xlarge",
                            "autoscale": {
                                "min_workers": 1,
                                "max_workers": 5,
                                "mode": "ENHANCED",
                            },
                            "custom_tags": {"team": "data"},
                        }
                    ],
                    "channel": "PREVIEW",
                    "development": False,
                },
            }
        )
        result = json.loads(w.action_result)
        assert result["success"] is True, result
        kwargs = ws.pipelines.update.call_args.kwargs
        assert kwargs["configuration"] == {"spark.sql.shuffle.partitions": "400"}
        assert kwargs["channel"] == "PREVIEW"
        assert kwargs["development"] is False
        clusters = kwargs["clusters"]
        assert len(clusters) == 1
        assert clusters[0].label == "default"
        assert clusters[0].node_type_id == "i3.xlarge"
        assert clusters[0].autoscale.min_workers == 1
        assert clusters[0].autoscale.max_workers == 5
        assert clusters[0].autoscale.mode.value == "ENHANCED"
        assert clusters[0].custom_tags == {"team": "data"}

    def test_update_settings_no_changes(self):
        from marimo_databricks_connect._pipeline_widget import PipelineWidget

        ws = _mock_pipeline_ws()
        w = PipelineWidget(pipeline_id="pl-abc", workspace_client=ws)
        w.request = json.dumps({"action": "update_settings", "settings": {}})
        result = json.loads(w.action_result)
        assert result["success"] is False
        assert "no changes" in result["message"].lower()
        ws.pipelines.update.assert_not_called()

    def test_update_settings_failure(self):
        from marimo_databricks_connect._pipeline_widget import PipelineWidget

        ws = _mock_pipeline_ws()
        w = PipelineWidget(pipeline_id="pl-abc", workspace_client=ws)
        ws.pipelines.update.side_effect = RuntimeError("forbidden")
        w.request = json.dumps({"action": "update_settings", "settings": {"channel": "PREVIEW"}})
        result = json.loads(w.action_result)
        assert result["success"] is False
        assert "forbidden" in result["message"]
