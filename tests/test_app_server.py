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


# ---- per-user filtering / access control ---------------------------------


def _write_draft(tmp_path, slug, *, user_key, ws_path="/Users/x/n.py", filename="n.py"):
    import json
    d = tmp_path / slug
    d.mkdir(exist_ok=True)
    (d / filename).write_text("# code\n")
    meta = {"workspace_path": ws_path, "filename": filename, "user_key": user_key}
    (d / ".meta.json").write_text(json.dumps(meta))
    return d


def test_drafts_are_filtered_by_user(client, tmp_path, monkeypatch):
    """alice's index page must not show bob's drafts."""
    monkeypatch.setattr("marimo_databricks_connect.app.server.NOTEBOOK_CACHE", tmp_path)
    _write_draft(tmp_path, "alice_draft", user_key="alice@x", ws_path="/Users/alice/a.py")
    _write_draft(tmp_path, "bob_draft", user_key="bob@x", ws_path="/Users/bob/b.py")
    fake = MagicMock()
    fake.workspace.list.return_value = []
    with patch("databricks.sdk.WorkspaceClient", return_value=fake):
        r = client.get(
            "/",
            headers={
                "X-Forwarded-Access-Token": "tok",
                "X-Forwarded-Email": "alice@x",
            },
        )
    assert r.status_code == 200
    assert "alice_draft" in r.text
    assert "/Users/alice/a.py" in r.text
    assert "bob_draft" not in r.text
    assert "/Users/bob/b.py" not in r.text


def test_save_rejects_non_owner(client, tmp_path, monkeypatch):
    """bob can't /save (or /save-as) alice's draft — reported as 404."""
    monkeypatch.setattr("marimo_databricks_connect.app.server.NOTEBOOK_CACHE", tmp_path)
    _write_draft(tmp_path, "alices", user_key="alice@x")
    fake = MagicMock()
    with patch("databricks.sdk.WorkspaceClient", return_value=fake):
        r = client.post(
            "/save",
            data={"slug": "alices"},
            headers={"X-Forwarded-Access-Token": "tok", "X-Forwarded-Email": "bob@x"},
        )
    assert r.status_code == 404
    fake.workspace.import_.assert_not_called()


def test_delete_rejects_non_owner(client, tmp_path, monkeypatch):
    monkeypatch.setattr("marimo_databricks_connect.app.server.NOTEBOOK_CACHE", tmp_path)
    d = _write_draft(tmp_path, "alices2", user_key="alice@x")
    r = client.post(
        "/delete-draft",
        data={"slug": "alices2"},
        headers={"X-Forwarded-Access-Token": "tok", "X-Forwarded-Email": "bob@x"},
    )
    assert r.status_code == 404
    # Files untouched.
    assert (d / "n.py").exists()


def test_open_slug_rejects_non_owner(client, tmp_path, monkeypatch):
    monkeypatch.setattr("marimo_databricks_connect.app.server.NOTEBOOK_CACHE", tmp_path)
    _write_draft(tmp_path, "alices3", user_key="alice@x")
    r = client.get(
        "/open/alices3",
        headers={"X-Forwarded-Access-Token": "tok", "X-Forwarded-Email": "bob@x"},
        follow_redirects=False,
    )
    assert r.status_code == 404


def test_new_records_user_key(client, tmp_path, monkeypatch):
    """POST /new writes the caller's user_key into the sidecar."""
    import json
    monkeypatch.setattr("marimo_databricks_connect.app.server.NOTEBOOK_CACHE", tmp_path)
    fake = MagicMock()
    with patch("databricks.sdk.WorkspaceClient", return_value=fake):
        r = client.post(
            "/new",
            data={"workspace_path": "/Users/alice@x/marimo/x.py"},
            headers={"X-Forwarded-Access-Token": "tok", "X-Forwarded-Email": "alice@x"},
            follow_redirects=False,
        )
    assert r.status_code == 303
    metas = list(tmp_path.rglob("*.meta.json"))
    assert len(metas) == 1
    meta = json.loads(metas[0].read_text())
    assert meta["user_key"] == "alice@x"


# ---- save-error capture --------------------------------------------------


def test_workspace_save_middleware_records_error_on_failure(tmp_path, monkeypatch):
    """When the workspace import_ raises, the meta sidecar gets last_save_error."""
    import json
    from marimo_databricks_connect.app import server as server_mod
    from marimo_databricks_connect.app.auth import UserIdentity

    monkeypatch.setattr(server_mod, "NOTEBOOK_CACHE", tmp_path)
    slug = "failsave"
    d = tmp_path / slug
    d.mkdir()
    (d / "failsave.py").write_text("# x\n")
    (d / ".meta.json").write_text(
        json.dumps(
            {
                "workspace_path": "/Users/me/x.py",
                "filename": "failsave.py",
                "user_key": "me@x",
            }
        )
    )

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
        "marimo_app_file": str(d / "failsave.py"),
    }

    fake = MagicMock()
    fake.workspace.import_.side_effect = RuntimeError("workspace boom")

    async def send(msg):
        pass

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    import asyncio
    with patch("databricks.sdk.WorkspaceClient", return_value=fake):
        asyncio.run(mw(scope, receive, send))

    meta = json.loads((d / ".meta.json").read_text())
    assert "workspace boom" in meta["last_save_error"]
    assert meta["last_save_error_at"]
    # And the index page surfaces the error + a retry button.
    fake2 = MagicMock()
    fake2.workspace.list.return_value = []
    client = TestClient(asgi)
    with patch("databricks.sdk.WorkspaceClient", return_value=fake2):
        r = client.get(
            "/",
            headers={"X-Forwarded-Access-Token": "tok", "X-Forwarded-Email": "me@x"},
        )
    assert "Auto-save failed" in r.text
    assert "workspace boom" in r.text
    assert "Retry save" in r.text


def test_workspace_save_middleware_clears_error_on_success(tmp_path, monkeypatch):
    """A subsequent successful save wipes the previous error fields."""
    import json
    from marimo_databricks_connect.app import server as server_mod
    from marimo_databricks_connect.app.auth import UserIdentity

    monkeypatch.setattr(server_mod, "NOTEBOOK_CACHE", tmp_path)
    slug = "recover"
    d = tmp_path / slug
    d.mkdir()
    (d / "recover.py").write_text("# x\n")
    (d / ".meta.json").write_text(
        json.dumps(
            {
                "workspace_path": "/Users/me/x.py",
                "filename": "recover.py",
                "user_key": "me@x",
                "last_save_error": "old failure",
                "last_save_error_at": "2020-01-01T00:00:00+00:00",
            }
        )
    )

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
        "marimo_app_file": str(d / "recover.py"),
    }

    fake = MagicMock()

    async def send(msg):
        pass

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    import asyncio
    with patch("databricks.sdk.WorkspaceClient", return_value=fake):
        asyncio.run(mw(scope, receive, send))

    meta = json.loads((d / ".meta.json").read_text())
    assert "last_save_error" not in meta
    assert "last_save_error_at" not in meta


def test_workspace_save_middleware_refuses_cross_user_push(tmp_path, monkeypatch):
    """If a save fires for a slug owned by alice while bob is the OBO user,
    the middleware must NOT push bob's edits to alice's workspace path.
    """
    import json
    from marimo_databricks_connect.app import server as server_mod
    from marimo_databricks_connect.app.auth import UserIdentity

    monkeypatch.setattr(server_mod, "NOTEBOOK_CACHE", tmp_path)
    slug = "alices_secret"
    d = tmp_path / slug
    d.mkdir()
    (d / "alices_secret.py").write_text("# alice's code\n")
    (d / ".meta.json").write_text(
        json.dumps(
            {
                "workspace_path": "/Users/alice/secret.py",
                "filename": "alices_secret.py",
                "user_key": "alice@x",
            }
        )
    )

    async def inner_app(scope, receive, send):
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    mw = server_mod.workspace_save_middleware_factory(inner_app)
    bob = UserIdentity(user="bob", email="bob@x", token="tok", host="https://x")
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/api/kernel/save",
        "headers": [],
        "state": {"user": bob},
        "marimo_app_file": str(d / "alices_secret.py"),
    }
    fake = MagicMock()

    async def send(msg):
        pass

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    import asyncio
    with patch("databricks.sdk.WorkspaceClient", return_value=fake):
        asyncio.run(mw(scope, receive, send))

    fake.workspace.import_.assert_not_called()


def test_drafts_section_renders_move_button_not_save(client, tmp_path, monkeypatch):
    """The per-row UI should expose Move (and Delete) but no inline Save."""
    monkeypatch.setattr("marimo_databricks_connect.app.server.NOTEBOOK_CACHE", tmp_path)
    _write_draft(tmp_path, "only", user_key="me@x", ws_path="/Users/me/n.py")
    fake = MagicMock()
    fake.workspace.list.return_value = []
    with patch("databricks.sdk.WorkspaceClient", return_value=fake):
        r = client.get(
            "/",
            headers={"X-Forwarded-Access-Token": "tok", "X-Forwarded-Email": "me@x"},
        )
    assert r.status_code == 200
    # Move button present, original "Save" button removed from the row.
    assert ">Move<" in r.text
    assert ">Delete<" in r.text
    assert ">Save<" not in r.text
