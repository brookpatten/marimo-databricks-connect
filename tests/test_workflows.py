"""Tests for the workflows widget (no live Databricks required)."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest


# ---- Helpers ----

def _make_task(task_key, notebook_path=None, depends_on=None, disabled=False):
    t = SimpleNamespace(
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
    return t


def _make_job_settings(name="test_job", tasks=None, schedule=None, tags=None):
    return SimpleNamespace(
        name=name,
        description="A test job",
        tasks=tasks or [],
        schedule=SimpleNamespace(quartz_cron_expression=schedule, timezone_id="UTC") if schedule else None,
        tags=tags or {},
        max_concurrent_runs=1,
        timeout_seconds=3600,
    )


def _make_base_job(job_id, name="test_job", task_count=2, schedule=None, tags=None):
    tasks = [_make_task(f"task_{i}") for i in range(task_count)]
    return SimpleNamespace(
        job_id=job_id,
        created_time=1700000000000,
        creator_user_name="user@example.com",
        settings=_make_job_settings(name=name, tasks=tasks, schedule=schedule, tags=tags),
    )


def _make_run_state(life_cycle="TERMINATED", result="SUCCESS", message=None):
    lcs = SimpleNamespace(value=life_cycle)
    rs = SimpleNamespace(value=result) if result else None
    return SimpleNamespace(
        life_cycle_state=lcs,
        result_state=rs,
        state_message=message,
        user_cancelled_or_timedout=False,
    )


def _make_base_run(run_id, job_id=1, state=None):
    return SimpleNamespace(
        run_id=run_id,
        run_name=f"run_{run_id}",
        job_id=job_id,
        start_time=1700000000000,
        end_time=1700003600000,
        run_duration=3600000,
        setup_duration=10000,
        state=state or _make_run_state(),
        run_page_url=f"https://databricks.com/run/{run_id}",
        trigger=SimpleNamespace(value="PERIODIC"),
        run_type=SimpleNamespace(value="JOB_RUN"),
        tasks=None,
    )


def _make_run_task(task_key, life_cycle="TERMINATED", result="SUCCESS", run_id=None):
    return SimpleNamespace(
        task_key=task_key,
        run_id=run_id or 100,
        notebook_task=None,
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
        start_time=1700000000000,
        end_time=1700000060000,
        execution_duration=60000,
        setup_duration=5000,
        state=_make_run_state(life_cycle, result),
        run_page_url=f"https://databricks.com/task/{task_key}",
        attempt_number=0,
    )


# ---- Serialization tests ----

def test_task_type_detection():
    from marimo_databricks_connect._workflows import _task_type
    t = _make_task("t1", notebook_path="/path/to/nb")
    assert _task_type(t) == "notebook"

    t2 = SimpleNamespace(**{
        k: None for k in [
            "notebook_task", "spark_python_task", "python_wheel_task",
            "spark_jar_task", "spark_submit_task", "pipeline_task",
            "sql_task", "dbt_task", "run_job_task", "condition_task",
            "for_each_task", "dashboard_task",
        ]
    })
    assert _task_type(t2) == "unknown"


def test_task_detail_extracts_notebook_path():
    from marimo_databricks_connect._workflows import _task_detail
    t = _make_task("t1", notebook_path="/Repos/user/project/nb")
    assert _task_detail(t) == "/Repos/user/project/nb"


def test_serialize_tasks():
    from marimo_databricks_connect._workflows import _serialize_tasks
    tasks = [
        _make_task("extract", notebook_path="/nb1"),
        _make_task("etl", notebook_path="/nb2", depends_on=["extract"]),
        _make_task("load", depends_on=["etl"]),
    ]
    result = _serialize_tasks(tasks)
    assert len(result) == 3

    extract = result[0]
    assert extract["task_key"] == "extract"
    assert extract["depends_on"] == []
    assert extract["downstream"] == ["etl"]

    etl = result[1]
    assert etl["task_key"] == "etl"
    assert etl["type"] == "notebook"
    assert etl["detail"] == "/nb2"
    assert etl["depends_on"] == ["extract"]
    assert etl["downstream"] == ["load"]
    assert etl["disabled"] is False

    load = result[2]
    assert load["depends_on"] == ["etl"]
    assert load["downstream"] == []


def test_serialize_run():
    from marimo_databricks_connect._workflows import _serialize_run
    r = _make_base_run(42)
    result = _serialize_run(r)
    assert result["run_id"] == 42
    assert result["life_cycle_state"] == "TERMINATED"
    assert result["result_state"] == "SUCCESS"
    assert result["duration"] == "1h 0m 0s"
    assert "databricks.com" in result["run_page_url"]


def test_serialize_run_task_includes_run_id():
    from marimo_databricks_connect._workflows import _serialize_run_task
    rt = _make_run_task("load", result="FAILED", run_id=42)
    result = _serialize_run_task(rt)
    assert result["task_key"] == "load"
    assert result["run_id"] == 42
    assert result["state"]["result_state"] == "FAILED"
    assert result["duration"] == "1m 0s"


def test_duration_str():
    from marimo_databricks_connect._workflows import _duration_str
    assert _duration_str(None) is None
    assert _duration_str(0) is None
    assert _duration_str(5000) == "5s"
    assert _duration_str(90000) == "1m 30s"
    assert _duration_str(3661000) == "1h 1m 1s"


def test_ms_to_iso():
    from marimo_databricks_connect._workflows import _ms_to_iso
    assert _ms_to_iso(None) is None
    result = _ms_to_iso(1700000000000)
    assert result is not None
    assert "2023" in result  # Nov 2023


# ---- Widget integration tests ----

def _mock_workspace_client(jobs=None, job_detail=None, runs=None, run_detail=None):
    ws = MagicMock()
    ws.jobs.list.return_value = jobs or []
    ws.jobs.get.return_value = job_detail or SimpleNamespace(
        job_id=1, created_time=None, creator_user_name=None,
        settings=_make_job_settings(tasks=[]),
    )
    ws.jobs.list_runs.return_value = runs or []
    ws.jobs.get_run.return_value = run_detail or SimpleNamespace(
        run_id=1, run_name="r", job_id=1, start_time=None, end_time=None,
        run_duration=None, setup_duration=None, state=None,
        run_page_url=None, tasks=[],
    )
    return ws


def test_widget_loads_jobs_on_init():
    from marimo_databricks_connect._workflows import WorkflowsWidget

    jobs = [_make_base_job(1, "job_a"), _make_base_job(2, "job_b")]
    ws = _mock_workspace_client(jobs=jobs)

    w = WorkflowsWidget(workspace_client=ws)
    data = json.loads(w.jobs_data)
    assert len(data) == 2
    assert data[0]["name"] == "job_a"
    assert data[1]["job_id"] == 2
    assert w.loading is False


def test_widget_handles_list_jobs_error():
    from marimo_databricks_connect._workflows import WorkflowsWidget

    ws = MagicMock()
    ws.jobs.list.side_effect = RuntimeError("auth failed")

    w = WorkflowsWidget(workspace_client=ws)
    assert "auth failed" in w.error_message
    assert w.loading is False


def test_widget_request_get_job():
    from marimo_databricks_connect._workflows import WorkflowsWidget

    tasks = [_make_task("extract", notebook_path="/nb1"), _make_task("load", depends_on=["extract"])]
    job = SimpleNamespace(
        job_id=99, created_time=1700000000000, creator_user_name="me",
        settings=_make_job_settings(name="my_pipeline", tasks=tasks, schedule="0 0 * * *"),
    )
    ws = _mock_workspace_client(job_detail=job)

    w = WorkflowsWidget(workspace_client=ws)
    w.request = json.dumps({"action": "get_job", "job_id": 99})

    detail = json.loads(w.job_detail)
    assert detail["job_id"] == 99
    assert detail["name"] == "my_pipeline"
    assert len(detail["tasks"]) == 2
    assert detail["tasks"][0]["task_key"] == "extract"
    assert detail["tasks"][0]["detail"] == "/nb1"
    assert detail["tasks"][0]["downstream"] == ["load"]
    assert detail["tasks"][1]["depends_on"] == ["extract"]
    assert detail["tasks"][1]["downstream"] == []


def test_widget_request_list_runs():
    from marimo_databricks_connect._workflows import WorkflowsWidget

    runs = [
        _make_base_run(10, state=_make_run_state("TERMINATED", "SUCCESS")),
        _make_base_run(11, state=_make_run_state("TERMINATED", "FAILED", "OOM")),
    ]
    ws = _mock_workspace_client(runs=runs)

    w = WorkflowsWidget(workspace_client=ws)
    w.request = json.dumps({"action": "list_runs", "job_id": 1})

    data = json.loads(w.runs_data)
    assert len(data) == 2
    assert data[0]["result_state"] == "SUCCESS"
    assert data[1]["result_state"] == "FAILED"


def test_widget_request_get_run():
    from marimo_databricks_connect._workflows import WorkflowsWidget

    run_tasks = [
        _make_run_task("extract", result="SUCCESS"),
        _make_run_task("load", result="FAILED"),
    ]
    run = SimpleNamespace(
        run_id=55, run_name="run_55", job_id=1,
        start_time=1700000000000, end_time=1700003600000,
        run_duration=3600000, setup_duration=10000,
        state=_make_run_state("TERMINATED", "FAILED"),
        run_page_url="https://databricks.com/run/55",
        tasks=run_tasks,
    )
    ws = _mock_workspace_client(run_detail=run)

    w = WorkflowsWidget(workspace_client=ws)
    w.request = json.dumps({"action": "get_run", "run_id": 55})

    detail = json.loads(w.run_detail)
    assert detail["run_id"] == 55
    assert detail["result_state"] == "FAILED"
    assert len(detail["tasks"]) == 2
    assert detail["tasks"][0]["state"]["result_state"] == "SUCCESS"
    assert detail["tasks"][1]["state"]["result_state"] == "FAILED"


def test_widget_search_filter():
    from marimo_databricks_connect._workflows import WorkflowsWidget

    ws = _mock_workspace_client(jobs=[_make_base_job(1, "etl_daily")])

    w = WorkflowsWidget(workspace_client=ws)
    w.request = json.dumps({"action": "list_jobs", "name_filter": "etl"})

    ws.jobs.list.assert_called_with(limit=100, name="etl")


def test_widget_job_tags_serialized():
    from marimo_databricks_connect._workflows import WorkflowsWidget

    jobs = [_make_base_job(1, "tagged_job", tags={"env": "prod", "team": "data"})]
    ws = _mock_workspace_client(jobs=jobs)

    w = WorkflowsWidget(workspace_client=ws)
    data = json.loads(w.jobs_data)
    assert data[0]["tags"] == {"env": "prod", "team": "data"}


def test_serialize_tasks_diamond_dag():
    """Test downstream computation with a diamond DAG: A -> B, A -> C, B -> D, C -> D."""
    from marimo_databricks_connect._workflows import _serialize_tasks
    tasks = [
        _make_task("A"),
        _make_task("B", depends_on=["A"]),
        _make_task("C", depends_on=["A"]),
        _make_task("D", depends_on=["B", "C"]),
    ]
    result = _serialize_tasks(tasks)
    by_key = {t["task_key"]: t for t in result}
    assert sorted(by_key["A"]["downstream"]) == ["B", "C"]
    assert by_key["B"]["downstream"] == ["D"]
    assert by_key["C"]["downstream"] == ["D"]
    assert by_key["D"]["downstream"] == []
    assert by_key["D"]["depends_on"] == ["B", "C"]


# ---- Run output tests ----


def test_serialize_run_output_with_error():
    from marimo_databricks_connect._workflows import _serialize_run_output
    output = SimpleNamespace(
        error="Task failed: division by zero",
        error_trace="Traceback (most recent call last):\n  File ...\nZeroDivisionError",
        logs="Starting task\nLoading data\n",
        logs_truncated=False,
        info=None,
        notebook_output=None,
        sql_output=None,
        dbt_output=None,
        metadata=None,
    )
    result = _serialize_run_output(output)
    assert result["error"] == "Task failed: division by zero"
    assert "ZeroDivisionError" in result["error_trace"]
    assert result["logs"] == "Starting task\nLoading data\n"
    assert result["logs_truncated"] is False
    assert result["notebook_result"] is None


def test_serialize_run_output_with_notebook_result():
    from marimo_databricks_connect._workflows import _serialize_run_output
    output = SimpleNamespace(
        error=None,
        error_trace=None,
        logs="All good\n",
        logs_truncated=True,
        info="Completed successfully",
        notebook_output=SimpleNamespace(result="{\"count\": 42}", truncated=False),
        sql_output=None,
        dbt_output=None,
        metadata=None,
    )
    result = _serialize_run_output(output)
    assert result["error"] is None
    assert result["notebook_result"] == '{"count": 42}'
    assert result["notebook_result_truncated"] is False
    assert result["logs_truncated"] is True
    assert result["info"] == "Completed successfully"


def test_serialize_run_output_with_sql_output():
    from marimo_databricks_connect._workflows import _serialize_run_output
    query_out = SimpleNamespace(
        query_text="SELECT count(*) FROM t",
        output_link="https://databricks.com/sql/results/123",
    )
    output = SimpleNamespace(
        error=None, error_trace=None, logs=None, logs_truncated=False,
        info=None, notebook_output=None,
        sql_output=SimpleNamespace(query_output=query_out, dashboard_output=None, alert_output=None),
        dbt_output=None, metadata=None,
    )
    result = _serialize_run_output(output)
    assert "SELECT count" in result["sql_output"]
    assert "results/123" in result["sql_output"]


def test_serialize_run_output_with_dbt_output():
    from marimo_databricks_connect._workflows import _serialize_run_output
    output = SimpleNamespace(
        error=None, error_trace=None, logs=None, logs_truncated=False,
        info=None, notebook_output=None, sql_output=None,
        dbt_output=SimpleNamespace(artifacts_link="https://link", artifacts_headers=None),
        metadata=None,
    )
    result = _serialize_run_output(output)
    assert result["dbt_output"] == "https://link"


def test_serialize_run_output_empty():
    from marimo_databricks_connect._workflows import _serialize_run_output
    output = SimpleNamespace(
        error=None, error_trace=None, logs=None, logs_truncated=False,
        info=None, notebook_output=None, sql_output=None, dbt_output=None,
        metadata=None,
    )
    result = _serialize_run_output(output)
    assert result["error"] is None
    assert result["logs"] is None
    assert result["notebook_result"] is None


def test_serialize_run_output_extracts_metadata_state_message():
    from marimo_databricks_connect._workflows import _serialize_run_output
    meta_state = SimpleNamespace(state_message="Run terminated: MAX_RUN_DURATION_EXCEEDED")
    output = SimpleNamespace(
        error=None, error_trace=None, logs="some logs", logs_truncated=False,
        info=None, notebook_output=None, sql_output=None, dbt_output=None,
        metadata=SimpleNamespace(state=meta_state),
    )
    result = _serialize_run_output(output)
    assert "MAX_RUN_DURATION_EXCEEDED" in result["info"]


def test_widget_request_get_task_output():
    from marimo_databricks_connect._workflows import WorkflowsWidget

    run_output = SimpleNamespace(
        error="OOM killed",
        error_trace="java.lang.OutOfMemoryError\n  at ...",
        logs="2024-01-01 Starting\n2024-01-01 Processing\n",
        logs_truncated=False,
        info=None,
        notebook_output=None,
        sql_output=None,
        dbt_output=None,
        metadata=None,
    )
    ws = _mock_workspace_client()
    ws.jobs.get_run_output.return_value = run_output

    w = WorkflowsWidget(workspace_client=ws)
    w.request = json.dumps({"action": "get_task_output", "run_id": 555})

    ws.jobs.get_run_output.assert_called_once_with(555)
    output = json.loads(w.task_output)
    assert output["error"] == "OOM killed"
    assert "OutOfMemoryError" in output["error_trace"]
    assert "Starting" in output["logs"]


def test_widget_get_task_output_handles_error():
    from marimo_databricks_connect._workflows import WorkflowsWidget

    ws = _mock_workspace_client()
    ws.jobs.get_run_output.side_effect = RuntimeError("permission denied")

    w = WorkflowsWidget(workspace_client=ws)
    w.request = json.dumps({"action": "get_task_output", "run_id": 999})

    output = json.loads(w.task_output)
    assert "permission denied" in output["error"]
    assert output["logs"] is None


def test_workflows_widget_factory():
    """Test the public API factory function."""
    ws = _mock_workspace_client()
    from marimo_databricks_connect import workflows_widget
    w = workflows_widget(workspace_client=ws)
    from marimo_databricks_connect._workflows import WorkflowsWidget
    assert isinstance(w, WorkflowsWidget)
