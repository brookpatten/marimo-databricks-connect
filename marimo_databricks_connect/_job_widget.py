"""Operational widget for a single Databricks job/workflow.

Displays job details, recent runs, task DAG, logs/output, and supports
actions: run now, cancel run, repair run. Auto-refreshes periodically.

Usage::

    from marimo_databricks_connect import job_widget
    widget = job_widget(job_id=123456)
    widget  # display in cell

    # Or by name:
    widget = job_widget(job_name="my_etl_pipeline")
"""

from __future__ import annotations

import json
import logging
import pathlib
import time
from typing import Any

import anywidget
import traitlets

from ._ops_common import duration_str, enum_val, ms_to_iso, safe_dict

LOGGER = logging.getLogger(__name__)

_ESM_PATH = pathlib.Path(__file__).parent / "_job_widget_frontend.js"


# --------------------------------------------------------------------------- #
# Serializers (reuse patterns from _workflows.py but focused on single job)   #
# --------------------------------------------------------------------------- #


def _task_type(task: Any) -> str:
    for attr, label in (
        ("notebook_task", "notebook"),
        ("spark_python_task", "spark_python"),
        ("python_wheel_task", "python_wheel"),
        ("spark_jar_task", "spark_jar"),
        ("spark_submit_task", "spark_submit"),
        ("pipeline_task", "pipeline"),
        ("sql_task", "sql"),
        ("dbt_task", "dbt"),
        ("run_job_task", "run_job"),
        ("condition_task", "condition"),
        ("for_each_task", "for_each"),
        ("dashboard_task", "dashboard"),
    ):
        if getattr(task, attr, None) is not None:
            return label
    return "unknown"


def _task_detail(task: Any) -> str | None:
    nb = getattr(task, "notebook_task", None)
    if nb and getattr(nb, "notebook_path", None):
        return nb.notebook_path
    sp = getattr(task, "spark_python_task", None)
    if sp and getattr(sp, "python_file", None):
        return sp.python_file
    pl = getattr(task, "pipeline_task", None)
    if pl and getattr(pl, "pipeline_id", None):
        return f"pipeline:{pl.pipeline_id}"
    sq = getattr(task, "sql_task", None)
    if sq:
        qid = getattr(sq, "query", None)
        if qid and getattr(qid, "query_id", None):
            return f"query:{qid.query_id}"
    rj = getattr(task, "run_job_task", None)
    if rj and getattr(rj, "job_id", None):
        return f"job:{rj.job_id}"
    return None


def _serialize_tasks(tasks: list[Any]) -> list[dict]:
    serialized = []
    for task in tasks:
        deps = []
        if getattr(task, "depends_on", None):
            deps = [getattr(d, "task_key", "") for d in task.depends_on]
        serialized.append(
            {
                "task_key": getattr(task, "task_key", ""),
                "type": _task_type(task),
                "detail": _task_detail(task),
                "depends_on": deps,
                "downstream": [],
                "description": getattr(task, "description", None) or None,
                "disabled": bool(getattr(task, "disabled", False)),
            }
        )
    by_key = {t["task_key"]: t for t in serialized}
    for t in serialized:
        for dep_key in t["depends_on"]:
            parent = by_key.get(dep_key)
            if parent is not None:
                parent["downstream"].append(t["task_key"])
    return serialized


def _run_task_state(task: Any) -> dict:
    state = getattr(task, "state", None)
    return {
        "life_cycle_state": getattr(state, "life_cycle_state", None).value
        if state and getattr(state, "life_cycle_state", None)
        else None,
        "result_state": getattr(state, "result_state", None).value
        if state and getattr(state, "result_state", None)
        else None,
        "state_message": getattr(state, "state_message", None) if state else None,
    }


def _serialize_run(run: Any) -> dict:
    state = getattr(run, "state", None)
    return {
        "run_id": getattr(run, "run_id", None),
        "run_name": getattr(run, "run_name", None),
        "start_time": ms_to_iso(getattr(run, "start_time", None)),
        "end_time": ms_to_iso(getattr(run, "end_time", None)),
        "duration": duration_str(getattr(run, "run_duration", None)),
        "life_cycle_state": getattr(state, "life_cycle_state", None).value
        if state and getattr(state, "life_cycle_state", None)
        else None,
        "result_state": getattr(state, "result_state", None).value
        if state and getattr(state, "result_state", None)
        else None,
        "state_message": getattr(state, "state_message", None) if state else None,
        "run_page_url": getattr(run, "run_page_url", None),
        "trigger": getattr(run, "trigger", None).value if getattr(run, "trigger", None) else None,
        "run_type": getattr(run, "run_type", None).value if getattr(run, "run_type", None) else None,
    }


def _serialize_run_task(task: Any) -> dict:
    return {
        "task_key": getattr(task, "task_key", ""),
        "type": _task_type(task),
        "run_id": getattr(task, "run_id", None),
        "start_time": ms_to_iso(getattr(task, "start_time", None)),
        "end_time": ms_to_iso(getattr(task, "end_time", None)),
        "duration": duration_str(getattr(task, "execution_duration", None)),
        "setup_duration": duration_str(getattr(task, "setup_duration", None)),
        "state": _run_task_state(task),
        "run_page_url": getattr(task, "run_page_url", None),
        "attempt_number": getattr(task, "attempt_number", None),
    }


def _serialize_run_output(output: Any) -> dict:
    result: dict[str, Any] = {
        "error": getattr(output, "error", None),
        "error_trace": getattr(output, "error_trace", None),
        "logs": getattr(output, "logs", None),
        "logs_truncated": bool(getattr(output, "logs_truncated", False)),
        "info": getattr(output, "info", None),
        "notebook_result": None,
        "notebook_result_truncated": False,
        "sql_output": None,
        "dbt_output": None,
    }
    nb_out = getattr(output, "notebook_output", None)
    if nb_out:
        result["notebook_result"] = getattr(nb_out, "result", None)
        result["notebook_result_truncated"] = bool(getattr(nb_out, "truncated", False))
    sql_out = getattr(output, "sql_output", None)
    if sql_out:
        pieces = []
        query_out = getattr(sql_out, "query_output", None)
        if query_out:
            qt = getattr(query_out, "query_text", None)
            if qt:
                pieces.append(f"Query: {qt}")
            ol = getattr(query_out, "output_link", None)
            if ol:
                pieces.append(f"Output: {ol}")
        if pieces:
            result["sql_output"] = "\n".join(pieces)
    dbt_out = getattr(output, "dbt_output", None)
    if dbt_out:
        link = getattr(dbt_out, "artifacts_link", None)
        if link:
            result["dbt_output"] = link
    meta = getattr(output, "metadata", None)
    if meta and not result["error"]:
        state = getattr(meta, "state", None)
        if state:
            msg = getattr(state, "state_message", None)
            if msg:
                result["info"] = (result["info"] or "") + ("\n" if result["info"] else "") + msg
    return result


# --------------------------------------------------------------------------- #
# Widget                                                                       #
# --------------------------------------------------------------------------- #


class JobWidget(anywidget.AnyWidget):
    """Operational widget for a single Databricks job."""

    _esm = traitlets.Unicode().tag(sync=True)
    _css = traitlets.Unicode("").tag(sync=True)

    # Data
    job_data = traitlets.Unicode("{}").tag(sync=True)
    runs_data = traitlets.Unicode("[]").tag(sync=True)
    run_detail = traitlets.Unicode("{}").tag(sync=True)
    task_output = traitlets.Unicode("{}").tag(sync=True)
    action_result = traitlets.Unicode("").tag(sync=True)
    loading = traitlets.Bool(False).tag(sync=True)
    error_message = traitlets.Unicode("").tag(sync=True)

    # Request from frontend
    request = traitlets.Unicode("").tag(sync=True)

    def __init__(
        self,
        job_id: int | None = None,
        job_name: str | None = None,
        workspace_client: Any = None,
        refresh_seconds: int = 30,
        **kwargs: Any,
    ) -> None:
        esm_content = _ESM_PATH.read_text()
        super().__init__(_esm=esm_content, **kwargs)
        self._ws = workspace_client
        self._job_id = job_id
        self._job_name = job_name
        self._refresh_seconds = refresh_seconds
        self.observe(self._handle_request, names=["request"])
        self._resolve_and_load()

    def _get_client(self) -> Any:
        if self._ws is not None:
            return self._ws
        from databricks.sdk import WorkspaceClient

        self._ws = WorkspaceClient()
        return self._ws

    def _resolve_and_load(self) -> None:
        """Resolve job_name to job_id if needed, then load everything."""
        if self._job_id is None and self._job_name:
            self.loading = True
            self.error_message = ""
            try:
                ws = self._get_client()
                jobs = list(ws.jobs.list(name=self._job_name, limit=10))
                matches = [j for j in jobs if getattr(getattr(j, "settings", None), "name", None) == self._job_name]
                if not matches:
                    self.error_message = f"Job '{self._job_name}' not found"
                    self.loading = False
                    return
                self._job_id = getattr(matches[0], "job_id", None)
            except Exception as exc:
                LOGGER.debug("Failed to resolve job name %s", self._job_name, exc_info=True)
                self.error_message = f"Failed to find job: {exc}"
                self.loading = False
                return
        if self._job_id is None:
            self.error_message = "No job_id or job_name provided"
            return
        self._load_job()
        self._load_runs()

    def _handle_request(self, change: Any) -> None:
        raw = change.get("new", "")
        if not raw:
            return
        try:
            req = json.loads(raw)
        except json.JSONDecodeError:
            return
        action = req.get("action")
        if action == "refresh":
            self._load_job()
            self._load_runs()
        elif action == "get_run":
            self._load_run_detail(req["run_id"])
        elif action == "get_task_output":
            self._load_task_output(req["run_id"])
        elif action == "run_now":
            self._run_now()
        elif action == "cancel_run":
            self._cancel_run(req["run_id"])
        elif action == "repair_run":
            self._repair_run(req["run_id"], req.get("rerun_tasks"))

    def _load_job(self) -> None:
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            job = ws.jobs.get(self._job_id)
            settings = getattr(job, "settings", None)
            tasks = []
            if settings and getattr(settings, "tasks", None):
                tasks = _serialize_tasks(settings.tasks)
            schedule = getattr(settings, "schedule", None) if settings else None
            tags = getattr(settings, "tags", None) if settings else None

            # Compute cluster info
            compute = []
            if settings and getattr(settings, "job_clusters", None):
                for jc in settings.job_clusters:
                    cluster_spec = getattr(jc, "new_cluster", None)
                    compute.append(
                        {
                            "job_cluster_key": getattr(jc, "job_cluster_key", None),
                            "node_type": getattr(cluster_spec, "node_type_id", None) if cluster_spec else None,
                            "num_workers": getattr(cluster_spec, "num_workers", None) if cluster_spec else None,
                            "spark_version": getattr(cluster_spec, "spark_version", None) if cluster_spec else None,
                        }
                    )

            detail = {
                "job_id": getattr(job, "job_id", None),
                "name": getattr(settings, "name", None) if settings else None,
                "description": getattr(settings, "description", None) if settings else None,
                "created_time": ms_to_iso(getattr(job, "created_time", None)),
                "creator": getattr(job, "creator_user_name", None),
                "schedule": getattr(schedule, "quartz_cron_expression", None) if schedule else None,
                "schedule_tz": getattr(schedule, "timezone_id", None) if schedule else None,
                "schedule_paused": enum_val(getattr(schedule, "pause_status", None)) if schedule else None,
                "tags": dict(tags) if tags else {},
                "max_concurrent_runs": getattr(settings, "max_concurrent_runs", None) if settings else None,
                "timeout_seconds": getattr(settings, "timeout_seconds", None) if settings else None,
                "tasks": tasks,
                "compute": compute,
                "refresh_seconds": self._refresh_seconds,
            }
            self.job_data = json.dumps(detail)
        except Exception as exc:
            LOGGER.debug("Failed to get job %s", self._job_id, exc_info=True)
            self.error_message = f"Failed to get job: {exc}"
        finally:
            self.loading = False

    def _load_runs(self) -> None:
        self.error_message = ""
        try:
            ws = self._get_client()
            runs = list(ws.jobs.list_runs(job_id=self._job_id, limit=25))
            self.runs_data = json.dumps([_serialize_run(r) for r in runs])
        except Exception as exc:
            LOGGER.debug("Failed to list runs for job %s", self._job_id, exc_info=True)
            self.error_message = f"Failed to list runs: {exc}"

    def _load_run_detail(self, run_id: int) -> None:
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            run = ws.jobs.get_run(run_id)
            state = getattr(run, "state", None)
            tasks = []
            if getattr(run, "tasks", None):
                tasks = [_serialize_run_task(t) for t in run.tasks]
            detail = {
                "run_id": getattr(run, "run_id", None),
                "run_name": getattr(run, "run_name", None),
                "job_id": getattr(run, "job_id", None),
                "start_time": ms_to_iso(getattr(run, "start_time", None)),
                "end_time": ms_to_iso(getattr(run, "end_time", None)),
                "duration": duration_str(getattr(run, "run_duration", None)),
                "setup_duration": duration_str(getattr(run, "setup_duration", None)),
                "life_cycle_state": getattr(state, "life_cycle_state", None).value
                if state and getattr(state, "life_cycle_state", None)
                else None,
                "result_state": getattr(state, "result_state", None).value
                if state and getattr(state, "result_state", None)
                else None,
                "state_message": getattr(state, "state_message", None) if state else None,
                "run_page_url": getattr(run, "run_page_url", None),
                "tasks": tasks,
            }
            self.run_detail = json.dumps(detail)
        except Exception as exc:
            LOGGER.debug("Failed to get run %s", run_id, exc_info=True)
            self.error_message = f"Failed to get run: {exc}"
        finally:
            self.loading = False

    def _load_task_output(self, run_id: int) -> None:
        self.error_message = ""
        try:
            ws = self._get_client()
            output = ws.jobs.get_run_output(run_id)
            self.task_output = json.dumps(_serialize_run_output(output))
        except Exception as exc:
            LOGGER.debug("Failed to get output for run %s", run_id, exc_info=True)
            self.task_output = json.dumps(
                {
                    "error": f"Failed to fetch output: {exc}",
                    "error_trace": None,
                    "logs": None,
                    "logs_truncated": False,
                    "info": None,
                    "notebook_result": None,
                    "notebook_result_truncated": False,
                    "sql_output": None,
                    "dbt_output": None,
                }
            )

    def _run_now(self) -> None:
        """Trigger the job to run immediately."""
        self.action_result = ""
        self.error_message = ""
        try:
            ws = self._get_client()
            result = ws.jobs.run_now(self._job_id)
            run_id = getattr(result, "run_id", None)
            self.action_result = json.dumps(
                {
                    "action": "run_now",
                    "success": True,
                    "message": f"Job triggered successfully. Run ID: {run_id}",
                    "run_id": run_id,
                }
            )
            # Refresh runs list
            time.sleep(1)
            self._load_runs()
        except Exception as exc:
            LOGGER.debug("Failed to run job %s", self._job_id, exc_info=True)
            self.action_result = json.dumps(
                {
                    "action": "run_now",
                    "success": False,
                    "message": f"Failed to trigger job: {exc}",
                }
            )

    def _cancel_run(self, run_id: int) -> None:
        """Cancel a specific run."""
        self.action_result = ""
        self.error_message = ""
        try:
            ws = self._get_client()
            ws.jobs.cancel_run(run_id)
            self.action_result = json.dumps(
                {
                    "action": "cancel_run",
                    "success": True,
                    "message": f"Run {run_id} cancellation requested.",
                }
            )
            time.sleep(1)
            self._load_runs()
        except Exception as exc:
            LOGGER.debug("Failed to cancel run %s", run_id, exc_info=True)
            self.action_result = json.dumps(
                {
                    "action": "cancel_run",
                    "success": False,
                    "message": f"Failed to cancel run: {exc}",
                }
            )

    def _repair_run(self, run_id: int, rerun_tasks: list[str] | None = None) -> None:
        """Repair (re-run failed tasks of) a specific run."""
        self.action_result = ""
        self.error_message = ""
        try:
            ws = self._get_client()
            kwargs: dict[str, Any] = {"run_id": run_id}
            if rerun_tasks:
                kwargs["rerun_tasks"] = rerun_tasks
            else:
                kwargs["rerun_all_failed_tasks"] = True
            ws.jobs.repair_run(**kwargs)
            self.action_result = json.dumps(
                {
                    "action": "repair_run",
                    "success": True,
                    "message": f"Repair requested for run {run_id}.",
                }
            )
            time.sleep(1)
            self._load_runs()
        except Exception as exc:
            LOGGER.debug("Failed to repair run %s", run_id, exc_info=True)
            self.action_result = json.dumps(
                {
                    "action": "repair_run",
                    "success": False,
                    "message": f"Failed to repair run: {exc}",
                }
            )
