"""Wire Databricks Model Serving endpoints into marimo's AI features.

Public entry point: :func:`register_serving_endpoints_as_ai_providers`.

This:

1. Enumerates serving endpoints via the Databricks SDK.
2. Starts a localhost auth-refreshing proxy (see :mod:`._ai_proxy`) that
   forwards OpenAI-compatible requests to ``<workspace>/serving-endpoints/*``
   with freshly-minted bearer tokens — so OAuth/CLI/OBO tokens never expire
   mid-session and no token is ever written to ``marimo.toml``.
3. Patches the user's ``marimo.toml`` (or the project's pyproject.toml's
   ``[tool.marimo]`` block) so the endpoints appear in marimo's AI model
   picker / chat / autocomplete UI under a ``databricks/`` prefix.

Marimo re-reads its TOML config on every request, so calling this from
inside a running notebook is fine — just **refresh the marimo browser tab**
afterwards so the AI panel re-fetches the (now-extended) provider list.
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
    # ``ready`` is typically an ``EndpointStateReady`` enum, whose ``str()``
    # is ``"EndpointStateReady.READY"`` — useless for a membership check.
    # Prefer the enum's ``.value`` / ``.name`` (both are ``"READY"``), and
    # fall back to ``str()`` for plain-string payloads.
    token = getattr(ready, "value", None) or getattr(ready, "name", None) or str(ready)
    return str(token).upper() in {"READY", "STATE_READY"}


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


def _find_pyproject_toml(start: pathlib.Path) -> Optional[pathlib.Path]:
    """Walk up from ``start`` looking for a ``pyproject.toml``."""
    p = start.resolve()
    root = pathlib.Path(p.anchor)
    while True:
        cand = p / "pyproject.toml"
        if cand.exists():
            return cand
        if p == root or p.parent == p:
            return None
        p = p.parent


def _resolve_target(scope: str) -> tuple[pathlib.Path, tuple[str, ...]]:
    """Resolve where to write AI config.

    Returns ``(path, key_prefix)`` where ``key_prefix`` is the table prefix
    that the AI keys are nested under inside that file. For a standalone
    ``marimo.toml`` the prefix is empty (``[ai...]`` lives at the top); for
    ``pyproject.toml`` it is ``("tool", "marimo")`` so the keys land at
    ``[tool.marimo.ai...]`` (which is what marimo actually reads — marimo
    does **not** load a project-level ``marimo.toml``, only the user one and
    the pyproject ``[tool.marimo]`` block).
    """
    if scope == "user":
        cfg = os.environ.get("XDG_CONFIG_HOME") or os.path.expanduser("~/.config")
        return pathlib.Path(cfg) / "marimo" / "marimo.toml", ()
    if scope == "pyproject":
        return pathlib.Path.cwd() / "pyproject.toml", ("tool", "marimo")
    if scope == "project":
        # Marimo only honours project config under pyproject.toml's
        # [tool.marimo] table. If a pyproject.toml is reachable, target it;
        # otherwise fall back to a standalone marimo.toml (preserved for
        # backwards compatibility, but marimo itself will ignore it).
        py = _find_pyproject_toml(pathlib.Path.cwd())
        if py is not None:
            return py, ("tool", "marimo")
        return pathlib.Path.cwd() / "marimo.toml", ()
    p = pathlib.Path(scope).expanduser()
    prefix: tuple[str, ...] = ("tool", "marimo") if p.name == "pyproject.toml" else ()
    return p, prefix


def _resolve_config_path(scope: str) -> pathlib.Path:
    """Back-compat shim returning just the path (drops the key prefix)."""
    return _resolve_target(scope)[0]


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
    key_prefix: tuple[str, ...] = (),
) -> None:
    """Idempotently merge our AI provider config into a TOML config file.

    ``key_prefix`` is prepended to every key path so the same writer works
    for both standalone ``marimo.toml`` (prefix ``()``) and pyproject's
    ``[tool.marimo]`` block (prefix ``("tool", "marimo")``).
    """
    import tomlkit

    doc = _load_doc(path)

    prov = _ensure_table(doc, *key_prefix, "ai", "custom_providers", provider_name)
    prov["base_url"] = base_url
    prov["api_key"] = api_key

    models = _ensure_table(doc, *key_prefix, "ai", "models")
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


# ---- runtime (in-memory) AI config registry ------------------------------ #
#
# When marimo is hosted as a Databricks App, multiple end users share a single
# server process and a single on-disk marimo config. Writing AI providers to
# that shared config would either (a) leak one user's discovered endpoints to
# everyone else or (b) require coordinated writes / restarts. Instead we keep
# the config in-process and merge it into marimo's view of the user config on
# the fly, keyed by the OBO user identity attached to each request.
#
# Layout: ``{user_key_or_'*': {"providers": {provider_name: {...}},
#                              "models": {"custom_models": [...],
#                                         "chat_model": ..., ...}}}``
# ``"*"`` is the fallback bucket used when no OBO user is in scope (local
# dev) or when no per-user entry exists.

_RUNTIME_AI_REGISTRY: dict[str, dict[str, Any]] = {}
_RUNTIME_PATCH_INSTALLED = False


def _runtime_user_key() -> str:
    """Return the per-user registry key for the current request, or ``"*"``."""
    try:
        from . import _obo

        return _obo.get_user_key() or "*"
    except Exception:  # noqa: BLE001 -- never let config lookup fail open
        return "*"


def _merge_runtime_into_config(base: Any) -> Any:
    """Merge the runtime AI registry into a loaded marimo user config dict.

    Operates on plain dicts (returned by ``UserConfigManager._load_config``)
    rather than tomlkit documents, so we can treat keys as ordinary mapping
    entries. Per-user entries take priority over the shared ``"*"`` bucket;
    on-disk config is preserved unless the runtime registry explicitly sets
    a key.
    """
    star = _RUNTIME_AI_REGISTRY.get("*") or {}
    user = _RUNTIME_AI_REGISTRY.get(_runtime_user_key()) or {}
    if not star and not user:
        return base

    merged = dict(base) if isinstance(base, dict) else {}
    ai = dict(merged.get("ai") or {})

    cps = dict(ai.get("custom_providers") or {})
    for src in (star, user):
        for name, cfg in (src.get("providers") or {}).items():
            cps[name] = dict(cfg)
    ai["custom_providers"] = cps

    models = dict(ai.get("models") or {})
    existing = list(models.get("custom_models") or [])
    extra: list[str] = []
    for src in (star, user):
        extra.extend((src.get("models") or {}).get("custom_models") or [])
    models["custom_models"] = sorted(set(existing) | set(extra))
    # User-level defaults beat the star-level defaults beat whatever's on disk.
    for src in (star, user):
        for k in ("chat_model", "edit_model", "autocomplete_model"):
            v = (src.get("models") or {}).get(k)
            if v:
                models[k] = v
    ai["models"] = models

    merged["ai"] = ai
    return merged


def _install_runtime_config_patch() -> None:
    """Monkeypatch :meth:`marimo._config.manager.UserConfigManager._load_config`.

    Idempotent. After the patch, every call to marimo's user-config loader
    returns the on-disk config merged with our in-process AI registry,
    keyed by the current OBO user (via ``_obo.get_user_key()``).

    ``UserConfigManager._load_config`` is invoked by every
    ``MarimoConfigManager.get_config()`` call, which in turn powers the
    AI endpoint handlers — so providers registered at runtime show up
    on the next browser refresh without restarting the server.
    """
    global _RUNTIME_PATCH_INSTALLED
    if _RUNTIME_PATCH_INSTALLED:
        return
    try:
        from marimo._config.manager import UserConfigManager
    except Exception as exc:  # noqa: BLE001
        LOG.warning("mdc: could not patch marimo UserConfigManager: %s", exc)
        return

    original = UserConfigManager._load_config

    def patched(self):  # type: ignore[no-untyped-def]
        base = original(self)
        try:
            return _merge_runtime_into_config(base)
        except Exception:  # noqa: BLE001
            LOG.exception("mdc: failed to merge runtime AI config; falling back to disk-only")
            return base

    patched._mdc_runtime_patched = True  # type: ignore[attr-defined]
    UserConfigManager._load_config = patched  # type: ignore[method-assign]
    _RUNTIME_PATCH_INSTALLED = True
    LOG.info("mdc: installed runtime AI-config patch on marimo UserConfigManager")


def _store_runtime_ai_config(
    *,
    user_key: str,
    provider_name: str,
    base_url: str,
    api_key: str,
    model_ids: list[str],
    default_chat: Optional[str],
    default_edit: Optional[str],
    default_autocomplete: Optional[str],
) -> None:
    """Update the in-process registry for ``user_key`` with the given provider."""
    bucket = _RUNTIME_AI_REGISTRY.setdefault(user_key, {"providers": {}, "models": {}})
    bucket["providers"][provider_name] = {"base_url": base_url, "api_key": api_key}
    models = bucket.setdefault("models", {})
    existing = list(models.get("custom_models") or [])
    models["custom_models"] = sorted(set(existing) | set(model_ids))
    if default_chat:
        models["chat_model"] = default_chat
    if default_edit:
        models["edit_model"] = default_edit
    if default_autocomplete:
        models["autocomplete_model"] = default_autocomplete


def _reset_runtime_registry_for_tests() -> None:
    """Test helper: clear the in-memory registry. Does not undo the patch."""
    _RUNTIME_AI_REGISTRY.clear()


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
        scope: where to put the AI config. One of:

            * ``"memory"`` — do **not** touch any file. Store the providers
              in an in-process registry keyed by the current OBO user (via
              ``X-Forwarded-User`` / ``X-Forwarded-Email``) and patch
              marimo's user-config loader to merge that registry into the
              config it returns. Use this when running inside the
              :mod:`marimo_databricks_connect.app` Databricks App, where a
              single server process is shared across multiple end users and
              writing to disk would either leak one user's endpoints to
              everyone or require coordinated restarts.
            * ``"user"`` — ``~/.config/marimo/marimo.toml`` (or
              ``$XDG_CONFIG_HOME``). Always read by marimo; safest pick
              for single-user local development.
            * ``"project"`` (default) — the nearest ``pyproject.toml`` in
              CWD or its parents (writes the ``[tool.marimo.ai...]`` block).
              Falls back to ``./marimo.toml`` when no pyproject.toml is
              reachable, but **note** marimo does not load a project-level
              ``marimo.toml`` — only the user one and pyproject's
              ``[tool.marimo]`` table.
            * ``"pyproject"`` — force ``./pyproject.toml``
              (creating the file if needed).
            * Any other string is treated as an explicit path. If the path
              ends in ``pyproject.toml`` we nest under ``[tool.marimo]``
              automatically.
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
    if write and scope == "memory":
        # In-process registry: works for multi-user app deployments where
        # writing to a shared marimo.toml would either leak config across
        # users or require restarting the server. The patch installed below
        # makes marimo's config loader merge this registry into the user
        # config it returns to the AI panel.
        _install_runtime_config_patch()
        _store_runtime_ai_config(
            user_key=_runtime_user_key(),
            provider_name=provider_name,
            base_url=base_url,
            api_key=_PROXY_API_KEY_SENTINEL,
            model_ids=model_ids,
            default_chat=_qualify(default_chat),
            default_edit=_qualify(default_edit),
            default_autocomplete=_qualify(default_autocomplete),
        )
        msg = (
            f"mdc: registered {len(model_ids)} Databricks model(s) in-process for "
            f"user={_runtime_user_key()!r}. Refresh the marimo browser tab so the "
            "AI panel picks up the new providers."
        )
        if verbose:
            print(msg)
        LOG.info(msg)
    elif write:
        cfg_path, key_prefix = _resolve_target(scope)
        _write_marimo_toml(
            cfg_path,
            provider_name=provider_name,
            base_url=base_url,
            api_key=_PROXY_API_KEY_SENTINEL,
            model_ids=model_ids,
            default_chat=_qualify(default_chat),
            default_edit=_qualify(default_edit),
            default_autocomplete=_qualify(default_autocomplete),
            key_prefix=key_prefix,
        )
        # Marimo re-reads the user marimo.toml and the project pyproject.toml
        # on every config request, so the new providers will appear after
        # the next refresh of the marimo UI (the AI panel only fetches the
        # provider list when it opens). When called from inside a notebook,
        # tell the user to refresh the browser tab.
        msg = (
            f"mdc: wrote {len(model_ids)} Databricks model(s) to {cfg_path}. "
            "Refresh the marimo browser tab so the AI panel picks up the new providers."
        )
        if verbose:
            print(msg)
        LOG.info(msg)
        if cfg_path.name == "marimo.toml" and key_prefix == () and scope in ("project",):
            warn = (
                "mdc: wrote a standalone marimo.toml, but marimo only reads project "
                "config from pyproject.toml's [tool.marimo] block. Use scope='user' "
                "or scope='pyproject' (or run from a directory containing a "
                "pyproject.toml) so the new providers actually show up in the UI."
            )
            if verbose:
                print(warn)
            LOG.warning(warn)

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
