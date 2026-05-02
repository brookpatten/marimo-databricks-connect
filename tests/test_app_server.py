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


def test_index_uses_obo_token(client, monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", "https://adb-123.azuredatabricks.net")
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
                # X-Forwarded-Host is intentionally ignored — in Databricks
                # Apps it carries the public app URL, not the workspace API.
                "X-Forwarded-Host": "app-1441745333137516.16.azure.databricksapps.com",
            },
        )
    assert r.status_code == 200
    assert "alice@example.com" in r.text
    assert "nb.py" in r.text
    assert "sql_nb" not in r.text  # non-python notebooks filtered out
    # WorkspaceClient was constructed with the user's token + the workspace
    # host from DATABRICKS_HOST (NOT the X-Forwarded-Host header), and
    # forced to PAT auth so the App SP's OAuth env vars don't conflict.
    assert seen["kw"]["token"] == "user-tok"
    assert seen["kw"]["host"] == "https://adb-123.azuredatabricks.net"
    assert seen["kw"]["auth_type"] == "pat"
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
    # File was materialised on disk for marimo to serve, in a per-slug dir.
    files = list(tmp_path.rglob("*.py"))
    assert len(files) == 1
    assert files[0].read_bytes() == body
    # Basename matches the workspace path leaf so marimo's UI shows it.
    assert files[0].name == "nb.py"


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


# ---- new / save / drafts -------------------------------------------------


def test_new_notebook_local_only(client, tmp_path, monkeypatch):
    """POST /new with no workspace_path writes to cache + skips workspace import."""
    monkeypatch.setattr("marimo_databricks_connect.app.server.NOTEBOOK_CACHE", tmp_path)
    fake = MagicMock()
    with patch("databricks.sdk.WorkspaceClient", return_value=fake):
        r = client.post(
            "/new",
            data={"workspace_path": ""},
            headers={"X-Forwarded-Access-Token": "tok", "X-Forwarded-Email": "alice@example.com"},
            follow_redirects=False,
        )
    assert r.status_code == 303
    assert r.headers["location"].startswith("/m/untitled-")
    # Notebook + sidecar metadata written, no import_ call to workspace.
    pys = list(tmp_path.rglob("*.py"))
    metas = list(tmp_path.rglob("*.meta.json"))
    assert len(pys) == 1 and len(metas) == 1
    assert "@app.cell" in pys[0].read_text()
    fake.workspace.import_.assert_not_called()


def test_new_notebook_imports_to_workspace(client, tmp_path, monkeypatch):
    monkeypatch.setattr("marimo_databricks_connect.app.server.NOTEBOOK_CACHE", tmp_path)
    fake = MagicMock()
    with patch("databricks.sdk.WorkspaceClient", return_value=fake):
        r = client.post(
            "/new",
            data={"workspace_path": "/Users/alice@example.com/marimo/x.py"},
            headers={"X-Forwarded-Access-Token": "tok"},
            follow_redirects=False,
        )
    assert r.status_code == 303
    fake.workspace.import_.assert_called_once()
    kwargs = fake.workspace.import_.call_args.kwargs
    assert kwargs["path"] == "/Users/alice@example.com/marimo/x.py"
    assert kwargs["overwrite"] is True
    # Sidecar tracks the workspace target.
    import json
    meta_files = list(tmp_path.rglob("*.meta.json"))
    assert len(meta_files) == 1
    meta = json.loads(meta_files[0].read_text())
    assert meta["workspace_path"] == "/Users/alice@example.com/marimo/x.py"
    assert "last_uploaded_mtime" in meta


def test_new_notebook_rejects_relative_path(client):
    r = client.post(
        "/new",
        data={"workspace_path": "no-leading-slash.py"},
        headers={"X-Forwarded-Access-Token": "tok"},
    )
    assert r.status_code == 400


def test_save_uses_tracked_workspace_path(client, tmp_path, monkeypatch):
    """/save POSTs the cached file content back to the workspace path in meta."""
    import json
    monkeypatch.setattr("marimo_databricks_connect.app.server.NOTEBOOK_CACHE", tmp_path)
    slug = "my_draft"
    d = tmp_path / slug
    d.mkdir()
    (d / "draft.py").write_text("import marimo as mo\n# edits\n")
    (d / ".meta.json").write_text(
        json.dumps(
            {
                "workspace_path": "/Users/me/marimo/draft.py",
                "filename": "draft.py",
                "last_uploaded_mtime": 0,
            }
        )
    )
    fake = MagicMock()
    with patch("databricks.sdk.WorkspaceClient", return_value=fake):
        r = client.post(
            "/save",
            data={"slug": slug},
            headers={"X-Forwarded-Access-Token": "tok"},
            follow_redirects=False,
        )
    assert r.status_code == 303
    fake.workspace.import_.assert_called_once()
    kwargs = fake.workspace.import_.call_args.kwargs
    assert kwargs["path"] == "/Users/me/marimo/draft.py"
    # base64-decoded content matches the file on disk.
    assert base64.b64decode(kwargs["content"]) == b"import marimo as mo\n# edits\n"
    # Meta updated with new mtime.
    meta = json.loads((d / ".meta.json").read_text())
    assert meta["last_uploaded_mtime"] > 0


def test_save_as_retargets_workspace_path(client, tmp_path, monkeypatch):
    import json
    monkeypatch.setattr("marimo_databricks_connect.app.server.NOTEBOOK_CACHE", tmp_path)
    slug = "draft2"
    d = tmp_path / slug
    d.mkdir()
    (d / "draft2.py").write_text("x = 1\n")
    (d / ".meta.json").write_text(
        json.dumps({"workspace_path": "/old/path.py", "filename": "draft2.py"})
    )
    fake = MagicMock()
    with patch("databricks.sdk.WorkspaceClient", return_value=fake):
        r = client.post(
            "/save",
            data={"slug": slug, "workspace_path": "/new/path.py"},
            headers={"X-Forwarded-Access-Token": "tok"},
            follow_redirects=False,
        )
    assert r.status_code == 303
    assert fake.workspace.import_.call_args.kwargs["path"] == "/new/path.py"
    meta = json.loads((d / ".meta.json").read_text())
    assert meta["workspace_path"] == "/new/path.py"


def test_save_without_target_returns_400(client, tmp_path, monkeypatch):
    monkeypatch.setattr("marimo_databricks_connect.app.server.NOTEBOOK_CACHE", tmp_path)
    slug = "orphan"
    d = tmp_path / slug
    d.mkdir()
    (d / "orphan.py").write_text("x = 1\n")
    # No meta sidecar -> no workspace_path tracked.
    r = client.post(
        "/save",
        data={"slug": slug},
        headers={"X-Forwarded-Access-Token": "tok"},
    )
    assert r.status_code == 400


def test_index_renders_drafts_section(client, tmp_path, monkeypatch):
    """Index page should show a Drafts row for each cached notebook."""
    import json
    monkeypatch.setattr("marimo_databricks_connect.app.server.NOTEBOOK_CACHE", tmp_path)
    d = tmp_path / "draftA"
    d.mkdir()
    (d / "a.py").write_text("# a\n")
    (d / ".meta.json").write_text(
        json.dumps({"workspace_path": "/Users/me/a.py", "filename": "a.py"})
    )
    fake = MagicMock()
    fake.workspace.list.return_value = []
    with patch("databricks.sdk.WorkspaceClient", return_value=fake):
        r = client.get("/", headers={"X-Forwarded-Access-Token": "tok"})
    assert r.status_code == 200
    assert "Drafts" in r.text
    assert "draftA" in r.text
    assert "/Users/me/a.py" in r.text


def test_workspace_save_middleware_pushes_to_workspace(tmp_path, monkeypatch):
    """A successful POST to ``.../api/kernel/save`` re-uploads to workspace."""
    import json
    from marimo_databricks_connect.app import server as server_mod
    from marimo_databricks_connect.app.auth import UserIdentity

    monkeypatch.setattr(server_mod, "NOTEBOOK_CACHE", tmp_path)
    slug = "saveme"
    d = tmp_path / slug
    d.mkdir()
    cache_file = d / "saveme.py"
    cache_file.write_text("# fresh edits from marimo\n")
    (d / ".meta.json").write_text(
        json.dumps(
            {"workspace_path": "/Users/me/saveme.py", "filename": "saveme.py"}
        )
    )

    # Build a fake inner ASGI app that returns 200 (mimicking marimo's save).
    async def inner_app(scope, receive, send):
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    mw = server_mod.workspace_save_middleware_factory(inner_app)

    user = UserIdentity(user="me", email="me@x", token="tok", host="https://x")
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/api/kernel/save",
        "headers": [],
        "state": {"user": user},
        "marimo_app_file": str(cache_file),
    }

    fake = MagicMock()
    sent = []

    async def send(msg):
        sent.append(msg)

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    import asyncio
    with patch("databricks.sdk.WorkspaceClient", return_value=fake):
        asyncio.run(mw(scope, receive, send))

    fake.workspace.import_.assert_called_once()
    kwargs = fake.workspace.import_.call_args.kwargs
    assert kwargs["path"] == "/Users/me/saveme.py"
    assert base64.b64decode(kwargs["content"]) == b"# fresh edits from marimo\n"
    # Sidecar updated with the new upload time.
    meta = json.loads((d / ".meta.json").read_text())
    assert meta["last_uploaded_mtime"] > 0
