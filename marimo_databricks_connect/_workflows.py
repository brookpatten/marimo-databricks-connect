"""Anywidget for browsing Databricks workflows (jobs), tasks, and runs.

Usage in a marimo notebook::

    from marimo_databricks_connect import workflows_widget
    widget = workflows_widget()
    widget  # display in cell output

Or with an explicit WorkspaceClient::

    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient(...)
    widget = workflows_widget(workspace_client=w)
"""

from __future__ import annotations

import json
import logging
import pathlib
import time
from typing import Any

import anywidget
import traitlets

LOGGER = logging.getLogger(__name__)

_ESM_PATH = pathlib.Path(__file__).parent / "_workflows_frontend.js"


def _ms_to_iso(ms: int | None) -> str | None:
    """Convert epoch-millis to ISO 8601 string, or None."""
    if ms is None:
        return None
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ms / 1000))
    except Exception:
        return None


def _duration_str(ms: int | None) -> str | None:
    """Human-readable duration from milliseconds."""
    if ms is None or ms <= 0:
        return None
    secs = ms // 1000
    if secs < 60:
        return f"{secs}s"
    mins, secs = divmod(secs, 60)
    if mins < 60:
        return f"{mins}m {secs}s"
    hrs, mins = divmod(mins, 60)
    return f"{hrs}h {mins}m {secs}s"


def _task_type(task: Any) -> str:
    """Return a short human-readable task type string."""
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
    """Extract a short detail string (e.g. notebook path) from a task."""
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
    """Serialize a list of tasks, computing both upstream and downstream edges."""
    serialized = []
    for task in tasks:
        deps = []
        if getattr(task, "depends_on", None):
            deps = [getattr(d, "task_key", "") for d in task.depends_on]
        serialized.append({
            "task_key": getattr(task, "task_key", ""),
            "type": _task_type(task),
            "detail": _task_detail(task),
            "depends_on": deps,
            "downstream": [],  # filled below
            "description": getattr(task, "description", None) or None,
            "disabled": bool(getattr(task, "disabled", False)),
        })

    # Build downstream adjacency
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
        "life_cycle_state": getattr(state, "life_cycle_state", None).value if state and getattr(state, "life_cycle_state", None) else None,
        "result_state": getattr(state, "result_state", None).value if state and getattr(state, "result_state", None) else None,
        "state_message": getattr(state, "state_message", None) if state else None,
    }


def _serialize_run_task(task: Any) -> dict:
    return {
        "task_key": getattr(task, "task_key", ""),
        "type": _task_type(task),
        "run_id": getattr(task, "run_id", None),
        "start_time": _ms_to_iso(getattr(task, "start_time", None)),
        "end_time": _ms_to_iso(getattr(task, "end_time", None)),
        "duration": _duration_str(getattr(task, "execution_duration", None)),
        "setup_duration": _duration_str(getattr(task, "setup_duration", None)),
        "state": _run_task_state(task),
        "run_page_url": getattr(task, "run_page_url", None),
        "attempt_number": getattr(task, "attempt_number", None),
    }


def _serialize_run_output(output: Any) -> dict:
    """Serialize a ``RunOutput`` object into a JSON-safe dict."""
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

    # Notebook output
    nb_out = getattr(output, "notebook_output", None)
    if nb_out:
        result["notebook_result"] = getattr(nb_out, "result", None)
        result["notebook_result_truncated"] = bool(getattr(nb_out, "truncated", False))

    # SQL output
    sql_out = getattr(output, "sql_output", None)
    if sql_out:
        pieces = []
        query_out = getattr(sql_out, "query_output", None)
        if query_out:
            query_text = getattr(query_out, "query_text", None)
            if query_text:
                pieces.append(f"Query: {query_text}")
            output_link = getattr(query_out, "output_link", None)
            if output_link:
                pieces.append(f"Output: {output_link}")
        if pieces:
            result["sql_output"] = "\n".join(pieces)

    # DBT output
    dbt_out = getattr(output, "dbt_output", None)
    if dbt_out:
        link = getattr(dbt_out, "artifacts_link", None)
        if link:
            result["dbt_output"] = link

    # Metadata — extract state message from the embedded Run if present
    meta = getattr(output, "metadata", None)
    if meta and not result["error"]:
        state = getattr(meta, "state", None)
        if state:
            msg = getattr(state, "state_message", None)
            if msg:
                result["info"] = (result["info"] or "") + ("\n" if result["info"] else "") + msg

    return result


def _serialize_run(run: Any) -> dict:
    state = getattr(run, "state", None)
    return {
        "run_id": getattr(run, "run_id", None),
        "run_name": getattr(run, "run_name", None),
        "start_time": _ms_to_iso(getattr(run, "start_time", None)),
        "end_time": _ms_to_iso(getattr(run, "end_time", None)),
        "duration": _duration_str(getattr(run, "run_duration", None)),
        "life_cycle_state": getattr(state, "life_cycle_state", None).value if state and getattr(state, "life_cycle_state", None) else None,
        "result_state": getattr(state, "result_state", None).value if state and getattr(state, "result_state", None) else None,
        "state_message": getattr(state, "state_message", None) if state else None,
        "run_page_url": getattr(run, "run_page_url", None),
        "trigger": getattr(run, "trigger", None).value if getattr(run, "trigger", None) else None,
        "run_type": getattr(run, "run_type", None).value if getattr(run, "run_type", None) else None,
    }


class WorkflowsWidget(anywidget.AnyWidget):
    """Anywidget that displays Databricks workflows with drill-down into tasks and runs."""

    _esm = traitlets.Unicode().tag(sync=True)
    _css = traitlets.Unicode("").tag(sync=True)

    # Data sent to the frontend (read-only from JS side)
    jobs_data = traitlets.Unicode("[]").tag(sync=True)
    job_detail = traitlets.Unicode("{}").tag(sync=True)
    runs_data = traitlets.Unicode("[]").tag(sync=True)
    run_detail = traitlets.Unicode("{}").tag(sync=True)
    task_output = traitlets.Unicode("{}").tag(sync=True)
    loading = traitlets.Bool(False).tag(sync=True)
    error_message = traitlets.Unicode("").tag(sync=True)

    # Requests from the frontend (JS writes these, Python observes)
    request = traitlets.Unicode("").tag(sync=True)

    def __init__(self, workspace_client: Any = None, **kwargs: Any) -> None:
        esm_content = _ESM_PATH.read_text()
        super().__init__(_esm=esm_content, **kwargs)
        self._ws = workspace_client
        self.observe(self._handle_request, names=["request"])
        # Load initial job list
        self._load_jobs()

    def _get_client(self) -> Any:
        if self._ws is not None:
            return self._ws
        from databricks.sdk import WorkspaceClient
        self._ws = WorkspaceClient()
        return self._ws

    def _handle_request(self, change: Any) -> None:
        raw = change.get("new", "")
        if not raw:
            return
        try:
            req = json.loads(raw)
        except json.JSONDecodeError:
            return
        action = req.get("action")
        if action == "list_jobs":
            self._load_jobs(name_filter=req.get("name_filter"))
        elif action == "get_job":
            self._load_job_detail(req["job_id"])
        elif action == "list_runs":
            self._load_runs(req["job_id"])
        elif action == "get_run":
            self._load_run_detail(req["run_id"])
        elif action == "get_task_output":
            self._load_task_output(req["run_id"])

    def _load_jobs(self, name_filter: str | None = None) -> None:
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            kwargs: dict[str, Any] = {"limit": 100}
            if name_filter:
                kwargs["name"] = name_filter
            jobs = list(ws.jobs.list(**kwargs))
            result = []
            for j in jobs:
                settings = getattr(j, "settings", None)
                schedule = getattr(settings, "schedule", None) if settings else None
                tags = getattr(settings, "tags", None) if settings else None
                task_count = len(settings.tasks) if settings and getattr(settings, "tasks", None) else 0
                result.append({
                    "job_id": getattr(j, "job_id", None),
                    "name": getattr(settings, "name", None) if settings else None,
                    "created_time": _ms_to_iso(getattr(j, "created_time", None)),
                    "creator": getattr(j, "creator_user_name", None),
                    "schedule": getattr(schedule, "quartz_cron_expression", None) if schedule else None,
                    "tags": dict(tags) if tags else {},
                    "task_count": task_count,
                })
            self.jobs_data = json.dumps(result)
        except Exception as exc:
            LOGGER.debug("Failed to list jobs", exc_info=True)
            self.error_message = f"Failed to list jobs: {exc}"
        finally:
            self.loading = False

    def _load_job_detail(self, job_id: int) -> None:
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            job = ws.jobs.get(job_id)
            settings = getattr(job, "settings", None)
            tasks = []
            if settings and getattr(settings, "tasks", None):
                tasks = _serialize_tasks(settings.tasks)

            schedule = getattr(settings, "schedule", None) if settings else None
            tags = getattr(settings, "tags", None) if settings else None

            detail = {
                "job_id": getattr(job, "job_id", None),
                "name": getattr(settings, "name", None) if settings else None,
                "description": getattr(settings, "description", None) if settings else None,
                "created_time": _ms_to_iso(getattr(job, "created_time", None)),
                "creator": getattr(job, "creator_user_name", None),
                "schedule": getattr(schedule, "quartz_cron_expression", None) if schedule else None,
                "schedule_tz": getattr(schedule, "timezone_id", None) if schedule else None,
                "tags": dict(tags) if tags else {},
                "max_concurrent_runs": getattr(settings, "max_concurrent_runs", None) if settings else None,
                "timeout_seconds": getattr(settings, "timeout_seconds", None) if settings else None,
                "tasks": tasks,
            }
            self.job_detail = json.dumps(detail)
        except Exception as exc:
            LOGGER.debug("Failed to get job %s", job_id, exc_info=True)
            self.error_message = f"Failed to get job {job_id}: {exc}"
        finally:
            self.loading = False

    def _load_runs(self, job_id: int) -> None:
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            runs = list(ws.jobs.list_runs(job_id=job_id, limit=25))
            result = [_serialize_run(r) for r in runs]
            self.runs_data = json.dumps(result)
        except Exception as exc:
            LOGGER.debug("Failed to list runs for job %s", job_id, exc_info=True)
            self.error_message = f"Failed to list runs: {exc}"
        finally:
            self.loading = False

    def _load_task_output(self, run_id: int) -> None:
        """Fetch logs/output for a specific task run via ``get_run_output``."""
        self.error_message = ""
        try:
            ws = self._get_client()
            output = ws.jobs.get_run_output(run_id)
            self.task_output = json.dumps(_serialize_run_output(output))
        except Exception as exc:
            LOGGER.debug("Failed to get output for run %s", run_id, exc_info=True)
            self.task_output = json.dumps({
                "error": f"Failed to fetch output: {exc}",
                "error_trace": None,
                "logs": None,
                "logs_truncated": False,
                "info": None,
                "notebook_result": None,
                "notebook_result_truncated": False,
                "sql_output": None,
                "dbt_output": None,
            })

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
                "start_time": _ms_to_iso(getattr(run, "start_time", None)),
                "end_time": _ms_to_iso(getattr(run, "end_time", None)),
                "duration": _duration_str(getattr(run, "run_duration", None)),
                "setup_duration": _duration_str(getattr(run, "setup_duration", None)),
                "life_cycle_state": getattr(state, "life_cycle_state", None).value if state and getattr(state, "life_cycle_state", None) else None,
                "result_state": getattr(state, "result_state", None).value if state and getattr(state, "result_state", None) else None,
                "state_message": getattr(state, "state_message", None) if state else None,
                "run_page_url": getattr(run, "run_page_url", None),
                "tasks": tasks,
            }
            self.run_detail = json.dumps(detail)
        except Exception as exc:
            LOGGER.debug("Failed to get run %s", run_id, exc_info=True)
            self.error_message = f"Failed to get run {run_id}: {exc}"
        finally:
            self.loading = False
