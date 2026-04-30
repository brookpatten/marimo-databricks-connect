"""Unit tests for the workspace fsspec filesystem and workspace widget."""

from __future__ import annotations

import base64
import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest


def _obj(path, object_type="NOTEBOOK", language="PYTHON", size=0, object_id=1, modified_at=1700000000000):
    return SimpleNamespace(
        path=path,
        object_type=SimpleNamespace(value=object_type, name=object_type),
        language=SimpleNamespace(value=language, name=language) if language else None,
        size=size,
        object_id=object_id,
        modified_at=modified_at,
        created_at=modified_at - 1000,
    )


# ---------- WorkspaceFileSystem -------------------------------------------


def test_ws_fs_is_fsspec_subclass():
    from fsspec.spec import AbstractFileSystem

    from marimo_databricks_connect._workspace_fs import WorkspaceFileSystem

    fs = WorkspaceFileSystem(workspace_client=MagicMock())
    assert isinstance(fs, AbstractFileSystem)
    assert fs.root_marker == "/"


def test_ws_fs_ls_returns_dicts():
    from marimo_databricks_connect._workspace_fs import WorkspaceFileSystem

    ws = MagicMock()
    ws.workspace.list.return_value = [
        _obj("/Users/me/notebook", object_type="NOTEBOOK", language="PYTHON", size=123),
        _obj("/Users/me/sub", object_type="DIRECTORY", language=None, size=0),
        _obj("/Users/me/repo", object_type="REPO", language=None, size=0),
    ]
    fs = WorkspaceFileSystem(workspace_client=ws)
    out = fs.ls("/Users/me", detail=True)
    by_name = {e["name"]: e for e in out}
    assert by_name["/Users/me/notebook"]["type"] == "file"
    assert by_name["/Users/me/notebook"]["language"] == "PYTHON"
    assert by_name["/Users/me/notebook"]["size"] == 123
    assert by_name["/Users/me/sub"]["type"] == "directory"
    # REPO is treated as a folder.
    assert by_name["/Users/me/repo"]["type"] == "directory"


def test_ws_fs_ls_relative_path_uses_root():
    from marimo_databricks_connect._workspace_fs import WorkspaceFileSystem

    ws = MagicMock()
    ws.workspace.list.return_value = []
    fs = WorkspaceFileSystem(workspace_client=ws, root="/Users/alice")
    fs.ls("notebooks")
    ws.workspace.list.assert_called_once_with("/Users/alice/notebooks")


def test_ws_fs_ls_root_default():
    from marimo_databricks_connect._workspace_fs import WorkspaceFileSystem

    ws = MagicMock()
    ws.workspace.list.return_value = []
    fs = WorkspaceFileSystem(workspace_client=ws)
    fs.ls("/")
    ws.workspace.list.assert_called_once_with("/")


def test_ws_fs_ls_missing_raises_filenotfound():
    from marimo_databricks_connect._workspace_fs import WorkspaceFileSystem

    ws = MagicMock()
    ws.workspace.list.side_effect = RuntimeError("RESOURCE_DOES_NOT_EXIST")
    fs = WorkspaceFileSystem(workspace_client=ws)
    with pytest.raises(FileNotFoundError):
        fs.ls("/missing")


def test_ws_fs_info_uses_get_status():
    from marimo_databricks_connect._workspace_fs import WorkspaceFileSystem

    ws = MagicMock()
    ws.workspace.get_status.return_value = _obj("/Users/me/x", object_type="FILE", language=None, size=42)
    fs = WorkspaceFileSystem(workspace_client=ws)
    info = fs.info("/Users/me/x")
    assert info["type"] == "file"
    assert info["size"] == 42
    ws.workspace.get_status.assert_called_once_with("/Users/me/x")


def test_ws_fs_read_bytes_decodes_base64():
    from marimo_databricks_connect._workspace_fs import WorkspaceFileSystem

    payload = b"# hello world\nprint('hi')\n"
    ws = MagicMock()
    ws.workspace.export.return_value = SimpleNamespace(content=base64.b64encode(payload).decode())
    fs = WorkspaceFileSystem(workspace_client=ws)
    assert fs.cat_file("/Users/me/n") == payload
    # range slicing
    assert fs.cat_file("/Users/me/n", start=0, end=5) == payload[:5]


def test_ws_fs_open_returns_bytesio():
    from marimo_databricks_connect._workspace_fs import WorkspaceFileSystem

    payload = b"abc"
    ws = MagicMock()
    ws.workspace.export.return_value = SimpleNamespace(content=base64.b64encode(payload).decode())
    fs = WorkspaceFileSystem(workspace_client=ws)
    with fs._open("/Users/me/n") as fh:
        assert fh.read() == payload


# ---------- workspace lazy singleton --------------------------------------


def test_workspace_singleton_lazily_built(monkeypatch):
    """Accessing ``pkg.workspace`` must build a fsspec filesystem on demand."""
    import marimo_databricks_connect as pkg

    fake_ws = MagicMock()

    class _FakeWC:
        def __init__(self, *a, **kw):  # noqa: D401, ARG002
            pass

    # Patch WorkspaceClient before access.
    import databricks.sdk as sdk

    monkeypatch.setattr(sdk, "WorkspaceClient", _FakeWC)

    pkg._cache.pop("workspace", None)
    try:
        ws_fs = pkg.workspace
        from fsspec.spec import AbstractFileSystem

        from marimo_databricks_connect._workspace_fs import WorkspaceFileSystem

        assert isinstance(ws_fs, WorkspaceFileSystem)
        assert isinstance(ws_fs, AbstractFileSystem)
        assert ws_fs.root_marker == "/"
        # Cached on subsequent access.
        assert pkg.workspace is ws_fs
    finally:
        pkg._cache.pop("workspace", None)
    _ = fake_ws  # silence unused


# ---------- WorkspaceWidget -----------------------------------------------


def _make_widget(ws):
    """Construct a WorkspaceWidget with browse pre-stubbed."""
    from marimo_databricks_connect._workspace_widget import WorkspaceWidget

    return WorkspaceWidget(root="/", workspace_client=ws)


def test_widget_initial_browse_populates_contents():
    ws = MagicMock()
    ws.workspace.list.return_value = [
        _obj("/notebooks", object_type="DIRECTORY", language=None),
        _obj("/readme", object_type="FILE", language=None, size=10),
    ]
    w = _make_widget(ws)
    data = json.loads(w.contents_data)
    assert data["path"] == "/"
    names = [i["name"] for i in data["items"]]
    # Directories sorted before files.
    assert names == ["notebooks", "readme"]


def test_widget_browse_request_lists_subdir():
    ws = MagicMock()
    ws.workspace.list.return_value = []
    w = _make_widget(ws)
    ws.workspace.list.reset_mock()
    ws.workspace.list.return_value = [_obj("/Users/me/n", object_type="NOTEBOOK")]
    w.request = json.dumps({"action": "browse", "path": "/Users/me"})
    ws.workspace.list.assert_called_with("/Users/me")
    data = json.loads(w.contents_data)
    assert data["path"] == "/Users/me"
    assert data["items"][0]["path"] == "/Users/me/n"


def test_widget_select_calls_get_status():
    ws = MagicMock()
    ws.workspace.list.return_value = []
    w = _make_widget(ws)
    ws.workspace.get_status.return_value = _obj(
        "/Users/me/n", object_type="NOTEBOOK", language="PYTHON", size=99, object_id=42
    )
    w.request = json.dumps({"action": "select", "path": "/Users/me/n"})
    sel = json.loads(w.selected_data)
    assert sel["path"] == "/Users/me/n"
    assert sel["object_type"] == "NOTEBOOK"
    assert sel["language"] == "PYTHON"
    assert sel["object_id"] == 42
    assert sel["is_dir"] is False


def test_widget_get_permissions_uses_resource_mapping():
    ws = MagicMock()
    ws.workspace.list.return_value = []
    w = _make_widget(ws)

    # Permissions call returns one user with one permission level.
    perm = SimpleNamespace(
        permission_level=SimpleNamespace(value="CAN_READ"), inherited=False, inherited_from_object=None
    )
    acl_entry = SimpleNamespace(
        user_name="alice@example.com",
        group_name=None,
        service_principal_name=None,
        all_permissions=[perm],
    )
    ws.permissions.get.return_value = SimpleNamespace(access_control_list=[acl_entry])

    w.request = json.dumps({"action": "get_permissions", "path": "/n", "object_type": "NOTEBOOK", "object_id": 7})
    ws.permissions.get.assert_called_with(request_object_type="notebooks", request_object_id="7")
    perms = json.loads(w.permissions_data)
    assert perms["object_type"] == "NOTEBOOK"
    assert perms["acl"] == [
        {"principal": "alice@example.com", "permission_level": "CAN_READ", "inherited": False, "inherited_from": []}
    ]


def test_widget_get_permissions_resolves_object_id_when_missing():
    ws = MagicMock()
    ws.workspace.list.return_value = []
    w = _make_widget(ws)
    ws.workspace.get_status.return_value = _obj("/n", object_type="NOTEBOOK", object_id=11)
    ws.permissions.get.return_value = SimpleNamespace(access_control_list=[])
    w.request = json.dumps({"action": "get_permissions", "path": "/n"})
    ws.permissions.get.assert_called_with(request_object_type="notebooks", request_object_id="11")


def test_widget_get_permissions_unknown_object_type_sets_error():
    ws = MagicMock()
    ws.workspace.list.return_value = []
    w = _make_widget(ws)
    w.request = json.dumps({"action": "get_permissions", "path": "/x", "object_type": "LIBRARY", "object_id": 1})
    assert "not supported" in w.error_message.lower()


def test_widget_preview_decodes_base64():
    ws = MagicMock()
    ws.workspace.list.return_value = []
    w = _make_widget(ws)
    payload = b"# hi\nprint('x')\n"
    ws.workspace.export.return_value = SimpleNamespace(content=base64.b64encode(payload).decode())
    w.request = json.dumps({"action": "preview", "path": "/n"})
    pv = json.loads(w.preview_data)
    assert pv["path"] == "/n"
    assert pv["text"] == payload.decode()
    assert pv["is_text"] is True
    assert pv["truncated"] is False


def test_widget_browse_failure_sets_error():
    ws = MagicMock()
    ws.workspace.list.side_effect = RuntimeError("boom")
    w = _make_widget(ws)
    assert "Failed to list" in w.error_message


def test_workspace_widget_factory_returns_anywidget():
    from marimo_databricks_connect import workspace_widget

    ws = MagicMock()
    ws.workspace.list.return_value = []
    w = workspace_widget(root="/Users/me", workspace_client=ws)
    from marimo_databricks_connect._workspace_widget import WorkspaceWidget

    assert isinstance(w, WorkspaceWidget)
    assert w.root == "/Users/me"
