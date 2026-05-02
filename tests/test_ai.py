"""Tests for AI provider registration and the auth-refreshing proxy."""

from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import requests
import tomlkit

from marimo_databricks_connect import _ai, _ai_proxy


# --------------------------------------------------------------------------- #
# Fakes                                                                        #
# --------------------------------------------------------------------------- #


def _ep(name, task=None, ready=True, fm=False):
    """Build a fake serving-endpoint object."""
    state = SimpleNamespace(ready="READY" if ready else "NOT_READY")
    served = []
    if fm:
        served.append(SimpleNamespace(foundation_model=SimpleNamespace(name=name)))
    cfg = SimpleNamespace(served_entities=served)
    return SimpleNamespace(name=name, task=task, state=state, config=cfg, pending_config=None)


def _fake_ws(endpoints, host="https://example.cloud.databricks.com"):
    ws = MagicMock()
    ws.serving_endpoints.list.return_value = list(endpoints)
    ws.config.host = host
    ws.config.authenticate.return_value = {"Authorization": "Bearer fresh-token"}
    return ws


# --------------------------------------------------------------------------- #
# list_serving_endpoints                                                      #
# --------------------------------------------------------------------------- #


def test_list_filters_by_glob_and_task():
    ws = _fake_ws(
        [
            _ep("databricks-claude-3-7-sonnet", task="llm/v1/chat"),
            _ep("databricks-meta-llama", fm=True),  # task inferred from FM
            _ep("my-custom-classifier", task="llm/v1/embeddings"),
            _ep("databricks-not-ready", task="llm/v1/chat", ready=False),
            _ep("databricks-old-completions", task="llm/v1/completions"),
        ]
    )
    # Default tasks=() -> no task filter; just name + ready.
    names = _ai.list_serving_endpoints(workspace_client=ws, include=["databricks-*"])
    assert names == [
        "databricks-claude-3-7-sonnet",
        "databricks-meta-llama",
        "databricks-old-completions",
    ]
    # Strict task filter when explicitly requested.
    names = _ai.list_serving_endpoints(
        workspace_client=ws, include=["databricks-*"], tasks=("llm/v1/chat",)
    )
    assert names == ["databricks-claude-3-7-sonnet", "databricks-meta-llama"]


def test_list_excludes_take_priority():
    ws = _fake_ws(
        [
            _ep("databricks-a", task="llm/v1/chat"),
            _ep("databricks-b", task="llm/v1/chat"),
        ]
    )
    names = _ai.list_serving_endpoints(
        workspace_client=ws, include=["databricks-*"], exclude=["*-b"]
    )
    assert names == ["databricks-a"]


def test_endpoint_ready_handles_sdk_enum():
    """Regression: ``state.ready`` is an EndpointStateReady enum, not a str.

    ``str(EndpointStateReady.READY)`` is ``"EndpointStateReady.READY"``, so
    a naive ``str(ready).upper() in {"READY"}`` membership check would mark
    every real endpoint as not ready. We must use ``.value`` / ``.name``.
    """
    from databricks.sdk.service.serving import EndpointStateReady

    ready_ep = SimpleNamespace(state=SimpleNamespace(ready=EndpointStateReady.READY))
    not_ready_ep = SimpleNamespace(state=SimpleNamespace(ready=EndpointStateReady.NOT_READY))
    assert _ai._endpoint_ready(ready_ep) is True
    assert _ai._endpoint_ready(not_ready_ep) is False
    # And the list path actually keeps the ready one.
    ws = MagicMock()
    ws.serving_endpoints.list.return_value = [
        SimpleNamespace(
            name="databricks-claude",
            task="llm/v1/chat",
            state=SimpleNamespace(ready=EndpointStateReady.READY),
            config=None,
            pending_config=None,
        ),
        SimpleNamespace(
            name="databricks-not-ready",
            task="llm/v1/chat",
            state=SimpleNamespace(ready=EndpointStateReady.NOT_READY),
            config=None,
            pending_config=None,
        ),
    ]
    names = _ai.list_serving_endpoints(workspace_client=ws, include=["databricks-*"])
    assert names == ["databricks-claude"]


def test_list_empty_tasks_disables_task_filter():
    ws = _fake_ws([_ep("custom", task=None)])
    names = _ai.list_serving_endpoints(workspace_client=ws, include=["*"], tasks=())
    assert names == ["custom"]


# --------------------------------------------------------------------------- #
# marimo.toml writer                                                           #
# --------------------------------------------------------------------------- #


def test_write_marimo_toml_creates_provider_and_models(tmp_path):
    cfg = tmp_path / "marimo.toml"
    _ai._write_marimo_toml(
        cfg,
        provider_name="databricks",
        base_url="http://127.0.0.1:9999",
        api_key="sentinel",
        model_ids=["databricks/foo", "databricks/bar"],
        default_chat="databricks/foo",
        default_edit=None,
        default_autocomplete=None,
    )
    doc = tomlkit.parse(cfg.read_text())
    assert doc["ai"]["custom_providers"]["databricks"]["base_url"] == "http://127.0.0.1:9999"
    assert doc["ai"]["custom_providers"]["databricks"]["api_key"] == "sentinel"
    assert list(doc["ai"]["models"]["custom_models"]) == ["databricks/bar", "databricks/foo"]
    assert doc["ai"]["models"]["chat_model"] == "databricks/foo"


def test_write_marimo_toml_preserves_unrelated_keys_and_merges(tmp_path):
    cfg = tmp_path / "marimo.toml"
    cfg.write_text(
        """
[display]
theme = "dark"

[ai.models]
custom_models = ["other/keep-me"]
chat_model = "other/keep-me"

[ai.custom_providers.other]
base_url = "https://other.example.com"
api_key = "secret"
"""
    )
    _ai._write_marimo_toml(
        cfg,
        provider_name="databricks",
        base_url="http://127.0.0.1:1",
        api_key="sentinel",
        model_ids=["databricks/foo"],
        default_chat=None,
        default_edit=None,
        default_autocomplete=None,
    )
    doc = tomlkit.parse(cfg.read_text())
    # unrelated keys preserved
    assert doc["display"]["theme"] == "dark"
    assert doc["ai"]["custom_providers"]["other"]["api_key"] == "secret"
    # default_chat=None did NOT clobber the existing chat_model
    assert doc["ai"]["models"]["chat_model"] == "other/keep-me"
    # models merged + sorted, no duplicates
    assert list(doc["ai"]["models"]["custom_models"]) == ["databricks/foo", "other/keep-me"]


# --------------------------------------------------------------------------- #
# register_serving_endpoints_as_ai_providers                                  #
# --------------------------------------------------------------------------- #


def test_register_writes_config_and_returns_proxy(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _ai_proxy._reset_proxy_for_tests()
    try:
        ws = _fake_ws([_ep("databricks-claude", task="llm/v1/chat")])
        result = _ai.register_serving_endpoints_as_ai_providers(
            workspace_client=ws,
            include=["databricks-*"],
            default_chat="databricks-claude",
            scope="project",
            verbose=False,
        )
        assert result["models"] == ["databricks/databricks-claude"]
        assert result["base_url"].startswith("http://127.0.0.1:")
        cfg_path = tmp_path / "marimo.toml"
        assert cfg_path.exists()
        doc = tomlkit.parse(cfg_path.read_text())
        assert doc["ai"]["models"]["chat_model"] == "databricks/databricks-claude"
        assert doc["ai"]["custom_providers"]["databricks"]["api_key"] == _ai._PROXY_API_KEY_SENTINEL
    finally:
        _ai_proxy._reset_proxy_for_tests()


def test_register_dry_run_does_not_write(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _ai_proxy._reset_proxy_for_tests()
    try:
        ws = _fake_ws([_ep("databricks-claude", task="llm/v1/chat")])
        result = _ai.register_serving_endpoints_as_ai_providers(
            workspace_client=ws, write=False, verbose=False
        )
        assert result["config_path"] is None
        assert not (tmp_path / "marimo.toml").exists()
    finally:
        _ai_proxy._reset_proxy_for_tests()


def test_register_memory_scope_is_per_user_and_patches_marimo(monkeypatch):
    """scope='memory' keeps providers in-process, keyed by OBO user identity.

    The patched ``UserConfigManager._load_config`` must merge each user's
    runtime registry into the config it returns, leaving on-disk config
    untouched and other users' entries hidden.
    """
    from marimo._config.manager import UserConfigManager

    from marimo_databricks_connect import _ai as ai_mod
    from marimo_databricks_connect import _obo

    # Reset registry between runs; the monkeypatch on _load_config is
    # idempotent so we don't bother undoing it.
    ai_mod._reset_runtime_registry_for_tests()
    _ai_proxy._reset_proxy_for_tests()

    try:
        # Pretend we're inside an OBO request for alice@example.com.
        state = _obo.set_credentials(
            host="https://example.cloud.databricks.com",
            token="alice-token",
            user_key="alice@example.com",
        )
        try:
            ws = _fake_ws([_ep("databricks-claude", task="llm/v1/chat")])
            result = ai_mod.register_serving_endpoints_as_ai_providers(
                workspace_client=ws,
                include=["databricks-*"],
                scope="memory",
                verbose=False,
            )
            assert result["config_path"] is None
            assert result["models"] == ["databricks/databricks-claude"]

            cfg = UserConfigManager().get_config(hide_secrets=False)
            assert (
                cfg["ai"]["custom_providers"]["databricks"]["api_key"]
                == ai_mod._PROXY_API_KEY_SENTINEL
            )
            assert "databricks/databricks-claude" in cfg["ai"]["models"]["custom_models"]
            alice_base_url = cfg["ai"]["custom_providers"]["databricks"]["base_url"]
            assert alice_base_url == result["base_url"]
        finally:
            _obo.reset_credentials(state)

        # Outside any OBO request: alice's entry must NOT leak to '*'.
        cfg = UserConfigManager().get_config(hide_secrets=False)
        assert "databricks" not in (cfg.get("ai", {}).get("custom_providers") or {})
        assert "databricks/databricks-claude" not in (
            cfg.get("ai", {}).get("models", {}).get("custom_models") or []
        )

        # A different user (bob) starts with no providers — alice's bucket
        # must not be visible to him either.
        state = _obo.set_credentials(
            host="https://example.cloud.databricks.com",
            token="bob-token",
            user_key="bob@example.com",
        )
        try:
            cfg = UserConfigManager().get_config(hide_secrets=False)
            assert "databricks" not in (cfg.get("ai", {}).get("custom_providers") or {})
        finally:
            _obo.reset_credentials(state)
    finally:
        ai_mod._reset_runtime_registry_for_tests()
        _ai_proxy._reset_proxy_for_tests()


def test_register_writes_pyproject_when_present(tmp_path, monkeypatch):
    """With scope='project', a sibling pyproject.toml gets [tool.marimo.ai...] keys.

    Marimo doesn't read a project-level marimo.toml, so we must target
    pyproject.toml when one is reachable.
    """
    monkeypatch.chdir(tmp_path)
    (tmp_path / "pyproject.toml").write_text('[project]\nname = "demo"\n')
    _ai_proxy._reset_proxy_for_tests()
    try:
        ws = _fake_ws([_ep("databricks-claude", task="llm/v1/chat")])
        result = _ai.register_serving_endpoints_as_ai_providers(
            workspace_client=ws,
            include=["databricks-*"],
            scope="project",
            verbose=False,
        )
        assert result["config_path"] == str(tmp_path / "pyproject.toml")
        assert not (tmp_path / "marimo.toml").exists()
        doc = tomlkit.parse((tmp_path / "pyproject.toml").read_text())
        assert doc["project"]["name"] == "demo"
        prov = doc["tool"]["marimo"]["ai"]["custom_providers"]["databricks"]
        assert prov["api_key"] == _ai._PROXY_API_KEY_SENTINEL
        assert list(doc["tool"]["marimo"]["ai"]["models"]["custom_models"]) == [
            "databricks/databricks-claude"
        ]
    finally:
        _ai_proxy._reset_proxy_for_tests()


def test_register_user_scope_writes_to_xdg(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    _ai_proxy._reset_proxy_for_tests()
    try:
        ws = _fake_ws([_ep("databricks-claude", task="llm/v1/chat")])
        result = _ai.register_serving_endpoints_as_ai_providers(
            workspace_client=ws,
            include=["databricks-*"],
            scope="user",
            verbose=False,
        )
        cfg = tmp_path / "marimo" / "marimo.toml"
        assert result["config_path"] == str(cfg)
        assert cfg.exists()
        doc = tomlkit.parse(cfg.read_text())
        assert "tool" not in doc
        assert doc["ai"]["custom_providers"]["databricks"]["api_key"] == _ai._PROXY_API_KEY_SENTINEL
    finally:
        _ai_proxy._reset_proxy_for_tests()


def test_register_diagnostics_lists_available_when_no_match(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    _ai_proxy._reset_proxy_for_tests()
    try:
        ws = _fake_ws(
            [
                _ep("prod-llama", task="llm/v1/chat"),
                _ep("my-classifier", task=None),
            ]
        )
        result = _ai.register_serving_endpoints_as_ai_providers(
            workspace_client=ws,
            include=["databricks-*"],  # matches nothing
            write=False,
            verbose=True,
        )
        out = capsys.readouterr().out
        assert "workspace returned 2" in out
        assert "prod-llama" in out
        assert "my-classifier" in out
        assert "hint:" in out
        assert result["models"] == []
        # all_endpoints surfaced for programmatic inspection
        assert {e["name"] for e in result["all_endpoints"]} == {"prod-llama", "my-classifier"}
    finally:
        _ai_proxy._reset_proxy_for_tests()


# --------------------------------------------------------------------------- #
# Auth-refreshing proxy: end-to-end against a fake upstream                    #
# --------------------------------------------------------------------------- #


class _UpstreamHandler(BaseHTTPRequestHandler):
    """Records inbound requests so the test can assert on them."""

    received: list[dict] = []

    def log_message(self, *a, **k):  # silence
        pass

    def do_POST(self):  # noqa: N802
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length) if length else b""
        type(self).received.append(
            {
                "path": self.path,
                "auth": self.headers.get("Authorization"),
                "ct": self.headers.get("Content-Type"),
                "body": body,
            }
        )
        # Respond with a tiny streamed payload, using SSE-style framing.
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.end_headers()
        self.wfile.write(b"data: {\"chunk\": 1}\n\n")
        self.wfile.flush()
        self.wfile.write(b"data: {\"chunk\": 2}\n\n")
        self.wfile.flush()


@pytest.fixture
def upstream():
    _UpstreamHandler.received = []
    server = ThreadingHTTPServer(("127.0.0.1", 0), _UpstreamHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()


def test_proxy_forwards_with_fresh_auth_header(upstream):
    tokens = iter(["tok-1", "tok-2", "tok-3"])
    ws = MagicMock()
    ws.config.host = upstream  # upstream stands in for the workspace
    # Each call returns a different token, simulating SDK refresh.
    ws.config.authenticate.side_effect = lambda: {"Authorization": f"Bearer {next(tokens)}"}

    _ai_proxy._reset_proxy_for_tests()
    try:
        proxy = _ai_proxy.get_or_create_proxy(lambda: ws)
        # OpenAI client would POST to <base_url>/chat/completions
        url = proxy.base_url + "/chat/completions"

        # Stale client-supplied Authorization should be replaced.
        r1 = requests.post(
            url,
            headers={"Authorization": "Bearer STALE", "Content-Type": "application/json"},
            data=json.dumps({"model": "databricks-claude", "messages": []}),
            timeout=10,
            stream=True,
        )
        assert r1.status_code == 200
        # streamed content fully readable
        chunks = b"".join(r1.iter_content(chunk_size=None))
        assert b"chunk" in chunks

        r2 = requests.post(url, json={"model": "databricks-claude"}, timeout=10)
        assert r2.status_code == 200

        # Upstream saw forwarded path and *fresh* auth headers, not "STALE".
        assert len(_UpstreamHandler.received) == 2
        first, second = _UpstreamHandler.received
        assert first["path"] == "/serving-endpoints/chat/completions"
        assert first["auth"] == "Bearer tok-1"
        assert second["auth"] == "Bearer tok-2"
        assert b"databricks-claude" in first["body"]
    finally:
        _ai_proxy._reset_proxy_for_tests()


def test_proxy_returns_502_on_upstream_failure():
    ws = MagicMock()
    ws.config.host = "http://127.0.0.1:1"  # nothing listening
    ws.config.authenticate.return_value = {"Authorization": "Bearer x"}

    _ai_proxy._reset_proxy_for_tests()
    try:
        proxy = _ai_proxy.get_or_create_proxy(lambda: ws)
        r = requests.post(proxy.base_url + "/chat/completions", json={}, timeout=10)
        assert r.status_code == 502
    finally:
        _ai_proxy._reset_proxy_for_tests()


def test_get_or_create_proxy_is_singleton():
    _ai_proxy._reset_proxy_for_tests()
    try:
        ws = MagicMock()
        a = _ai_proxy.get_or_create_proxy(lambda: ws)
        b = _ai_proxy.get_or_create_proxy(lambda: ws)
        assert a is b
    finally:
        _ai_proxy._reset_proxy_for_tests()
        # tiny grace period for the listener socket to release
        time.sleep(0.01)
