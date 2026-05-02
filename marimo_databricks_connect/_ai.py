"""Wire Databricks Model Serving endpoints into marimo's AI features.

Public entry point: :func:`register_serving_endpoints_as_ai_providers`.

This:

1. Enumerates serving endpoints via the Databricks SDK.
2. Starts a localhost auth-refreshing proxy (see :mod:`._ai_proxy`) that
   forwards OpenAI-compatible requests to ``<workspace>/serving-endpoints/*``
   with freshly-minted bearer tokens — so OAuth/CLI/OBO tokens never expire
   mid-session and no token is ever written to ``marimo.toml``.
3. Patches the user's ``marimo.toml`` so the endpoints appear in marimo's
   AI model picker / chat / autocomplete UI under a ``databricks/`` prefix.

Marimo loads ``marimo.toml`` at server start, so changes here typically
require restarting ``marimo edit`` to take effect (the function prints a
hint to that effect).
"""

from __future__ import annotations

import fnmatch
import logging
import os
import pathlib
from typing import Any, Iterable, Optional

LOG = logging.getLogger(__name__)

# Task strings that Databricks Model Serving advertises for endpoints.
_CHAT_TASKS = frozenset({"llm/v1/chat", "chat"})
_COMPLETION_TASKS = frozenset({"llm/v1/completions", "completions"})
_EMBEDDING_TASKS = frozenset({"llm/v1/embeddings", "embeddings"})

# Sentinel value written to marimo.toml as the api_key for the proxy provider.
# The proxy ignores it and substitutes a real bearer token per request.
_PROXY_API_KEY_SENTINEL = "databricks-mdc-proxy"  # noqa: S105


def _endpoint_task(ep: Any) -> Optional[str]:
    """Best-effort extraction of an endpoint's task type."""
    task = getattr(ep, "task", None)
    if task:
        return str(task)
    # Foundation Model API endpoints don't always expose ``task``; if they have
    # a ``foundation_model`` served entity, treat them as chat.
    for attr in ("config", "pending_config"):
        cfg = getattr(ep, attr, None)
        if not cfg:
            continue
        for entity in getattr(cfg, "served_entities", None) or []:
            fm = getattr(entity, "foundation_model", None)
            if fm and getattr(fm, "name", None):
                return "llm/v1/chat"
    return None


def _endpoint_ready(ep: Any) -> bool:
    state = getattr(ep, "state", None)
    if state is None:
        return True
    ready = getattr(state, "ready", None)
    if ready is None:
        return True
    return str(ready).upper() in {"READY", "STATE_READY"}


def list_serving_endpoints(
    workspace_client: Any = None,
    include: Iterable[str] = ("*",),
    exclude: Iterable[str] = (),
    only_ready: bool = True,
    tasks: Iterable[str] = (),
) -> list[str]:
    """Return endpoint names matching the include/exclude globs and task filter.

    Args:
        workspace_client: Optional ``WorkspaceClient`` (uses default auth chain).
        include: glob patterns (matched against endpoint name) to include.
        exclude: glob patterns to exclude.
        only_ready: if True, skip endpoints not in a ready state.
        tasks: endpoint task strings to keep. **Empty by default** because
            Databricks doesn't normalize the ``task`` field consistently
            (Foundation Model endpoints often leave it unset). Pass e.g.
            ``tasks=("llm/v1/chat",)`` if you want strict filtering.
    """
    if workspace_client is None:
        from . import _build_workspace_client

        workspace_client = _build_workspace_client()

    inc = list(include) or ["*"]
    exc = list(exclude)
    task_set = {t.lower() for t in tasks}

    out: list[str] = []
    for ep in workspace_client.serving_endpoints.list():
        name = getattr(ep, "name", None)
        if not name:
            continue
        if not any(fnmatch.fnmatchcase(name, p) for p in inc):
            continue
        if any(fnmatch.fnmatchcase(name, p) for p in exc):
            continue
        if only_ready and not _endpoint_ready(ep):
            continue
        if task_set:
            t = (_endpoint_task(ep) or "").lower()
            if t and t not in task_set:
                continue
        out.append(name)
    return sorted(out)


def _resolve_config_path(scope: str) -> pathlib.Path:
    """Resolve a marimo.toml path from a scope name or explicit path."""
    if scope == "user":
        cfg = os.environ.get("XDG_CONFIG_HOME") or os.path.expanduser("~/.config")
        return pathlib.Path(cfg) / "marimo" / "marimo.toml"
    if scope == "project":
        return pathlib.Path.cwd() / "marimo.toml"
    return pathlib.Path(scope).expanduser()


def _load_doc(path: pathlib.Path) -> Any:
    import tomlkit

    if path.exists():
        return tomlkit.parse(path.read_text())
    return tomlkit.document()


def _ensure_table(doc: Any, *parts: str) -> Any:
    import tomlkit

    cur = doc
    for p in parts:
        if p not in cur or not hasattr(cur[p], "items"):
            cur[p] = tomlkit.table()
        cur = cur[p]
    return cur


def _write_marimo_toml(
    path: pathlib.Path,
    *,
    provider_name: str,
    base_url: str,
    api_key: str,
    model_ids: list[str],
    default_chat: Optional[str],
    default_edit: Optional[str],
    default_autocomplete: Optional[str],
) -> None:
    """Idempotently merge our AI provider config into ``marimo.toml``."""
    import tomlkit

    doc = _load_doc(path)

    prov = _ensure_table(doc, "ai", "custom_providers", provider_name)
    prov["base_url"] = base_url
    prov["api_key"] = api_key

    models = _ensure_table(doc, "ai", "models")
    existing = list(models.get("custom_models", []) or [])
    merged = sorted(set(existing) | set(model_ids))
    models["custom_models"] = merged
    if default_chat:
        models["chat_model"] = default_chat
    if default_edit:
        models["edit_model"] = default_edit
    if default_autocomplete:
        models["autocomplete_model"] = default_autocomplete

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(tomlkit.dumps(doc))


def register_serving_endpoints_as_ai_providers(
    *,
    workspace_client: Any = None,
    include: Iterable[str] = ("databricks-*",),
    exclude: Iterable[str] = (),
    tasks: Iterable[str] = (),
    provider_name: str = "databricks",
    default_chat: Optional[str] = None,
    default_edit: Optional[str] = None,
    default_autocomplete: Optional[str] = None,
    scope: str = "project",
    write: bool = True,
    proxy_port: int = 0,
    verbose: bool = True,
) -> dict[str, Any]:
    """Discover Databricks Model Serving endpoints and wire them into marimo's AI features.

    Starts a localhost auth-refreshing proxy that forwards OpenAI-compatible
    requests to ``<workspace>/serving-endpoints/*`` with a freshly-minted
    bearer token on every request, then writes a ``[ai.custom_providers.<provider_name>]``
    block to ``marimo.toml`` pointing marimo at that proxy.

    The ``base_url`` written to ``marimo.toml`` is the **proxy's** localhost
    URL (e.g. ``http://127.0.0.1:54321``), *not* the workspace URL. The proxy
    then forwards to ``<workspace>/serving-endpoints/*`` using credentials
    obtained from the Databricks SDK (same auth chain as ``spark`` /
    ``workspace``). You don't need to pass the workspace URL.

    Args:
        workspace_client: Optional ``WorkspaceClient`` (uses default auth chain).
        include: glob patterns (matched against endpoint name) to include.
            Defaults to ``("databricks-*",)`` which captures the Foundation
            Model API endpoints (``databricks-claude-*``, ``databricks-meta-*``,
            ``databricks-gte-*``, ...). Pass ``("*",)`` to include everything.
        exclude: glob patterns to exclude.
        tasks: optional task-string allow-list (e.g. ``("llm/v1/chat",)``).
            Empty by default — Databricks doesn't normalize ``task`` across
            endpoints, so filtering by it tends to drop too much.
        provider_name: marimo namespace for these models. Models appear in the
            marimo UI as ``<provider_name>/<endpoint-name>`` and marimo routes
            calls to ``[ai.custom_providers.<provider_name>]`` to find the
            ``base_url``/``api_key``. The default ``"databricks"`` is fine
            unless you want to register multiple workspaces side-by-side under
            different prefixes.
        default_chat: bare endpoint name to set as marimo's default chat model.
        default_edit: bare endpoint name to set as marimo's default edit model.
        default_autocomplete: bare endpoint name to set as marimo's default
            autocomplete model.
        scope: where to write ``marimo.toml`` — ``"project"`` (CWD),
            ``"user"`` (``~/.config/marimo/marimo.toml``), or an explicit path.
        write: if False, don't touch ``marimo.toml``; just start the proxy and
            return what *would* be written.
        proxy_port: port for the localhost proxy. ``0`` picks a free port.
        verbose: if True (default), print a short diagnostic summary of
            what was discovered, what got filtered out, and where to look
            next if nothing matched.

    Returns:
        Dict with ``provider``, ``base_url``, ``models``, ``endpoints``,
        ``config_path``, ``proxy``, and ``all_endpoints`` (every endpoint the
        workspace returned, before filtering — useful for debugging).
    """
    if workspace_client is None:
        from . import _build_workspace_client

        workspace_client = _build_workspace_client()

    raw = list(workspace_client.serving_endpoints.list())
    all_summary = [
        {
            "name": getattr(ep, "name", None),
            "task": _endpoint_task(ep),
            "ready": _endpoint_ready(ep),
        }
        for ep in raw
    ]

    endpoints = list_serving_endpoints(
        workspace_client=workspace_client,
        include=include,
        exclude=exclude,
        tasks=tasks,
    )

    if verbose:
        _print_diagnostics(all_summary, endpoints, include, exclude, tasks)

    if not endpoints:
        LOG.warning(
            "mdc: no serving endpoints matched include=%r exclude=%r tasks=%r "
            "(workspace returned %d endpoint(s) total)",
            list(include),
            list(exclude),
            list(tasks),
            len(raw),
        )

    from ._ai_proxy import get_or_create_proxy

    proxy = get_or_create_proxy(lambda: workspace_client, port=proxy_port)
    base_url = proxy.base_url

    model_ids = [f"{provider_name}/{n}" for n in endpoints]

    def _qualify(name: Optional[str]) -> Optional[str]:
        if not name:
            return None
        return name if "/" in name else f"{provider_name}/{name}"

    cfg_path: Optional[pathlib.Path] = None
    if write:
        cfg_path = _resolve_config_path(scope)
        _write_marimo_toml(
            cfg_path,
            provider_name=provider_name,
            base_url=base_url,
            api_key=_PROXY_API_KEY_SENTINEL,
            model_ids=model_ids,
            default_chat=_qualify(default_chat),
            default_edit=_qualify(default_edit),
            default_autocomplete=_qualify(default_autocomplete),
        )
        LOG.info(
            "mdc: wrote %d Databricks model(s) to %s; restart marimo to pick up changes.",
            len(model_ids),
            cfg_path,
        )

    return {
        "provider": provider_name,
        "base_url": base_url,
        "models": model_ids,
        "endpoints": endpoints,
        "all_endpoints": all_summary,
        "config_path": str(cfg_path) if cfg_path else None,
        "proxy": proxy,
    }


def _print_diagnostics(
    all_summary: list[dict],
    matched: list[str],
    include: Iterable[str],
    exclude: Iterable[str],
    tasks: Iterable[str],
) -> None:
    """Emit a short, human-readable summary of endpoint discovery + filtering."""
    inc = list(include)
    exc = list(exclude)
    tsk = list(tasks)
    print(
        f"mdc: workspace returned {len(all_summary)} serving endpoint(s); "
        f"{len(matched)} matched include={inc!r} exclude={exc!r} tasks={tsk!r}."
    )
    if matched:
        print("  matched:")
        for n in matched:
            print(f"    - {n}")
    if not matched and all_summary:
        print("  available endpoints (showing up to 20):")
        for ep in all_summary[:20]:
            print(f"    - {ep['name']}  (task={ep['task']!r}, ready={ep['ready']})")
        print(
            "  hint: nothing matched. Try include=('*',) to register every "
            "endpoint, or adjust the glob to match the names above."
        )
