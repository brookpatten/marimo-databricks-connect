"""Tests for the pipelines browser widget (no live Databricks required)."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock


# Reuse the rich pipeline fixtures from test_ops_widgets via re-import
from tests.test_ops_widgets import (  # type: ignore
    _make_full_update,
    _make_latest_update,
    _make_pipeline,
    _make_pipeline_event,
)


def _make_pipeline_summary(pipeline_id="pl-1", name="bronze", state="IDLE", health="HEALTHY"):
    return SimpleNamespace(
        pipeline_id=pipeline_id,
        name=name,
        state=SimpleNamespace(value=state),
        health=SimpleNamespace(value=health),
        creator_user_name="user@test.com",
        run_as_user_name="user@test.com",
        cluster_id=None,
        latest_updates=[_make_latest_update("upd-x", "COMPLETED")],
    )


def _mock_ws():
    ws = MagicMock()
    ws.pipelines.list_pipelines.return_value = [
        _make_pipeline_summary("pl-1", "bronze", "IDLE"),
        _make_pipeline_summary("pl-2", "silver", "RUNNING"),
    ]
    ws.pipelines.get.return_value = _make_pipeline()
    ws.pipelines.list_updates.return_value = SimpleNamespace(updates=[_make_full_update()])
    ws.pipelines.list_pipeline_events.return_value = [_make_pipeline_event()]
    return ws


class TestPipelinesWidget:
    def test_init_loads_pipelines(self):
        from marimo_databricks_connect._pipelines import PipelinesWidget

        ws = _mock_ws()
        w = PipelinesWidget(workspace_client=ws)
        ws.pipelines.list_pipelines.assert_called_once()
        data = json.loads(w.pipelines_data)
        assert len(data) == 2
        assert data[0]["pipeline_id"] == "pl-1"
        assert data[0]["name"] == "bronze"
        assert data[0]["state"] == "IDLE"
        assert data[0]["health"] == "HEALTHY"
        assert data[0]["last_update_state"] == "COMPLETED"
        assert data[1]["state"] == "RUNNING"
        assert w.loading is False

    def test_search_by_name(self):
        from marimo_databricks_connect._pipelines import PipelinesWidget

        ws = _mock_ws()
        w = PipelinesWidget(workspace_client=ws)
        w.request = json.dumps({"action": "list_pipelines", "name_filter": "bronze"})
        # Should pass through a LIKE filter
        kwargs = ws.pipelines.list_pipelines.call_args_list[-1].kwargs
        assert "filter" in kwargs and "bronze" in kwargs["filter"]
        assert kwargs.get("max_results") == 100

    def test_get_pipeline_loads_detail(self):
        from marimo_databricks_connect._pipelines import PipelinesWidget

        ws = _mock_ws()
        w = PipelinesWidget(workspace_client=ws)
        w.request = json.dumps({"action": "get_pipeline", "pipeline_id": "pl-1"})
        ws.pipelines.get.assert_called_with("pl-1")
        detail = json.loads(w.pipeline_detail)
        assert detail["pipeline_id"] == "pl-abc"  # from _make_pipeline()
        assert detail["spec"]["catalog"] == "main"

    def test_list_updates(self):
        from marimo_databricks_connect._pipelines import PipelinesWidget

        ws = _mock_ws()
        w = PipelinesWidget(workspace_client=ws)
        w.request = json.dumps({"action": "list_updates", "pipeline_id": "pl-1"})
        ws.pipelines.list_updates.assert_called_with("pl-1", max_results=25)
        updates = json.loads(w.updates_data)
        assert len(updates) == 1
        assert updates[0]["state"] == "COMPLETED"

    def test_list_events_with_filter(self):
        from marimo_databricks_connect._pipelines import PipelinesWidget

        ws = _mock_ws()
        w = PipelinesWidget(workspace_client=ws)
        w.request = json.dumps({"action": "list_events", "pipeline_id": "pl-1", "filter": "level='WARN'"})
        ws.pipelines.list_pipeline_events.assert_called_with(
            "pl-1", max_results=100, order_by=["timestamp desc"], filter="level='WARN'"
        )
        events = json.loads(w.events_data)
        assert len(events) == 1

    def test_error_on_list(self):
        from marimo_databricks_connect._pipelines import PipelinesWidget

        ws = _mock_ws()
        ws.pipelines.list_pipelines.side_effect = RuntimeError("permission denied")
        w = PipelinesWidget(workspace_client=ws)
        assert "permission denied" in w.error_message

    def test_factory_function(self):
        from marimo_databricks_connect import pipelines_widget

        ws = _mock_ws()
        w = pipelines_widget(workspace_client=ws)
        from marimo_databricks_connect._pipelines import PipelinesWidget

        assert isinstance(w, PipelinesWidget)

    def test_refresh_seconds_passthrough(self):
        from marimo_databricks_connect import pipelines_widget

        ws = _mock_ws()
        w = pipelines_widget(workspace_client=ws, refresh_seconds=15)
        assert w.refresh_seconds == 15
