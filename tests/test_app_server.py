"""Tests for the Databricks App server (workspace browser + OBO middleware)."""

from __future__ import annotations

import base64
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")

from starlette.testclient import TestClient

from marimo_databricks_connect import _obo
from marimo_databricks_connect.app import asgi


class _Obj:
    def __init__(self, path, object_type, language=None):
        self.path = path
        self.object_type = MagicMock(value=object_type)
        self.language = MagicMock(value=language) if language else None


@pytest.fixture
def client():
    return TestClient(asgi)


def test_healthz(client):
    r = client.get("/healthz")
    assert r.status_code == 200
    assert r.json() == {"ok": True}


def test_index_uses_obo_token(client):
    seen = {}

    def fake_ws(*_a, **kw):
        seen["kw"] = kw
        seen["obo"] = _obo.get_credentials()
        m = MagicMock()
        m.workspace.list.return_value = [
            _Obj("/Users/me/proj", "DIRECTORY"),
            _Obj("/Users/me/nb.py", "NOTEBOOK", "PYTHON"),
            _Obj("/Users/me/sql_nb", "NOTEBOOK", "SQL"),
        ]
        return m

    with patch("databricks.sdk.WorkspaceClient", side_effect=fake_ws):
        r = client.get(
            "/?path=/Users/me",
            headers={
                "X-Forwarded-Access-Token": "user-tok",
                "X-Forwarded-Email": "alice@example.com",
                "X-Forwarded-Host": "adb-123.azuredatabricks.net",
            },
        )
    assert r.status_code == 200
    assert "alice@example.com" in r.text
    assert "nb.py" in r.text
    assert "sql_nb" not in r.text  # non-python notebooks filtered out
    # WorkspaceClient was constructed with the user's token, not the SP's.
    assert seen["kw"]["token"] == "user-tok"
    assert seen["kw"]["host"] == "https://adb-123.azuredatabricks.net"
    # OBO contextvar was active during the call but cleared after.
    assert seen["obo"] == ("https://adb-123.azuredatabricks.net", "user-tok")
    assert _obo.get_credentials() == (None, None)


def test_edit_exports_notebook_and_redirects(client, tmp_path, monkeypatch):
    monkeypatch.setattr("marimo_databricks_connect.app.server.NOTEBOOK_CACHE", tmp_path)

    body = b"import marimo as mo\n"
    fake = MagicMock()
    fake.workspace.export.return_value = MagicMock(content=base64.b64encode(body).decode())

    with patch("databricks.sdk.WorkspaceClient", return_value=fake):
        r = client.get(
            "/edit",
            params={"path": "/Users/me/nb.py"},
            headers={"X-Forwarded-Access-Token": "user-tok"},
            follow_redirects=False,
        )
    assert r.status_code == 303
    assert r.headers["location"].startswith("/m/")
    # File was materialised on disk for marimo to serve.
    files = list(tmp_path.glob("*.py"))
    assert len(files) == 1
    assert files[0].read_bytes() == body


def test_edit_rejects_relative_path(client):
    r = client.get(
        "/edit",
        params={"path": "relative"},
        headers={"X-Forwarded-Access-Token": "tok"},
    )
    assert r.status_code == 400


def test_no_obo_header_falls_back(client):
    """Without OBO headers we fall back to the unified auth chain (no token kw)."""
    seen = {}

    def fake_ws(*_a, **kw):
        seen["kw"] = kw
        m = MagicMock()
        m.workspace.list.return_value = []
        return m

    with patch("databricks.sdk.WorkspaceClient", side_effect=fake_ws):
        r = client.get("/")
    assert r.status_code == 200
    # WorkspaceClient() called with no overrides \u2014 default chain takes over.
    assert seen["kw"] == {}
