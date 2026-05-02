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


# ---- runtime (cross-process) AI config sidecars -------------------------- #
#
# When marimo is hosted as a Databricks App, multiple end users share a single
# server process and a single on-disk marimo config. Writing AI providers to
# that shared config would either (a) leak one user's discovered endpoints to
# everyone else or (b) require coordinated writes / restarts.
#
# Critically, marimo runs notebooks in **separate kernel subprocesses**
# (multiprocessing) in EDIT mode -- so any in-memory registry populated from
# the notebook is invisible to the marimo server process that renders the AI
# panel. We therefore persist the runtime config to per-user JSON sidecar
# files in a shared directory, and install a UserConfigManager monkeypatch in
# the *server* process that reads the right sidecar based on the current
# request's OBO identity.
#
# Sidecar layout (one file per user, written atomically):
#     <RUNTIME_DIR>/<sha256(user_key)[:16]>.json
#     {"providers": {provider_name: {"base_url": ..., "api_key": ...}, ...},
#      "models":    {"custom_models": [...],
#                    "chat_model": ..., "edit_model": ..., "autocomplete_model": ...}}
#
# The user_key ``"_default"`` is used when no OBO user is in scope (local
# ``marimo edit`` -- single-user assumption).

_DEFAULT_USER_KEY = "_default"


def _runtime_dir() -> pathlib.Path:
    """Directory shared between the marimo server and kernel subprocesses.

    Override via ``MDC_AI_RUNTIME_DIR`` if the default temp location isn't
    suitable (e.g. on systems where ``/tmp`` is per-process).
    """
    import tempfile

    env = os.environ.get("MDC_AI_RUNTIME_DIR")
    if env:
        return pathlib.Path(env)
    uid = getattr(os, "getuid", lambda: "x")()
    return pathlib.Path(tempfile.gettempdir()) / f"mdc-ai-runtime-{uid}"


def _runtime_user_key() -> str:
    """Return the per-user sidecar key for the current request."""
    try:
        from . import _obo

        return _obo.get_user_key() or _DEFAULT_USER_KEY
    except Exception:  # noqa: BLE001 -- never let config lookup fail open
        return _DEFAULT_USER_KEY


def _sidecar_path(user_key: str) -> pathlib.Path:
    import hashlib

    safe = hashlib.sha256(user_key.encode("utf-8")).hexdigest()[:16]
    return _runtime_dir() / f"{safe}.json"


def _read_sidecar(user_key: str) -> dict[str, Any]:
    import json

    p = _sidecar_path(user_key)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        LOG.warning("mdc: discarding unreadable runtime AI sidecar at %s", p)
        return {}


def _write_sidecar(user_key: str, payload: dict[str, Any]) -> pathlib.Path:
    """Atomic write of ``payload`` to the per-user sidecar (tmp + rename)."""
    import json

    p = _sidecar_path(user_key)
    p.parent.mkdir(parents=True, exist_ok=True)
    # Lock down: only the OS user running this process should read the dir;
    # in Databricks Apps each end user shares the app's OS uid, but on local
    # multi-user machines this prevents cross-account leakage.
    try:
        os.chmod(p.parent, 0o700)
    except OSError:
        pass
    tmp = p.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    try:
        os.chmod(tmp, 0o600)
    except OSError:
        pass
    os.replace(tmp, p)
    return p


def _merge_into_sidecar(
    user_key: str,
    *,
    provider_name: str,
    base_url: str,
    api_key: str,
    model_ids: list[str],
    default_chat: Optional[str],
    default_edit: Optional[str],
    default_autocomplete: Optional[str],
) -> pathlib.Path:
    """Merge a single provider into the per-user sidecar and persist it.

    Existing entries (other providers, other models) are preserved so that
    repeated calls accumulate rather than clobber.
    """
    payload = _read_sidecar(user_key) or {}
    providers = dict(payload.get("providers") or {})
    providers[provider_name] = {"base_url": base_url, "api_key": api_key}
    payload["providers"] = providers

    models = dict(payload.get("models") or {})
    existing = list(models.get("custom_models") or [])
    models["custom_models"] = sorted(set(existing) | set(model_ids))
    if default_chat:
        models["chat_model"] = default_chat
    if default_edit:
        models["edit_model"] = default_edit
    if default_autocomplete:
        models["autocomplete_model"] = default_autocomplete
    payload["models"] = models

    return _write_sidecar(user_key, payload)


def _merge_sidecar_into_config(base: Any, user_key: str) -> Any:
    """Merge the per-user sidecar (if any) into a loaded marimo user config."""
    payload = _read_sidecar(user_key)
    if not payload:
        # Fall through to the default bucket so a single shared registration
        # (no OBO) is still surfaced when an OBO request comes in for an
        # unregistered user. Removing this would force every Apps user to
        # run the cell themselves before chat works at all.
        if user_key != _DEFAULT_USER_KEY:
            payload = _read_sidecar(_DEFAULT_USER_KEY)
        if not payload:
            return base

    merged = dict(base) if isinstance(base, dict) else {}
    ai = dict(merged.get("ai") or {})

    cps = dict(ai.get("custom_providers") or {})
    for name, cfg in (payload.get("providers") or {}).items():
        cps[name] = dict(cfg)
    ai["custom_providers"] = cps

    models = dict(ai.get("models") or {})
    existing = list(models.get("custom_models") or [])
    extra = list((payload.get("models") or {}).get("custom_models") or [])
    models["custom_models"] = sorted(set(existing) | set(extra))
    for k in ("chat_model", "edit_model", "autocomplete_model"):
        v = (payload.get("models") or {}).get(k)
        if v:
            models[k] = v
    ai["models"] = models

    merged["ai"] = ai
    return merged


_RUNTIME_PATCH_INSTALLED = False


def install_runtime_config_patch() -> None:
    """Patch ``UserConfigManager._load_config`` to merge per-user sidecars.

    Call this once in the **marimo server process** (the parent that renders
    the AI panel) -- typically from your ASGI app builder. Idempotent.

    After the patch, every call to marimo's user-config loader merges in the
    sidecar for the current request's OBO user (or ``_default`` when no OBO
    is active), so providers registered via ``scope="memory"`` show up on
    the next browser refresh without restarting the server.

    For ``marimo_databricks_connect.app`` deployments this is installed
    automatically when :mod:`.app.server` is imported.
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
            return _merge_sidecar_into_config(base, _runtime_user_key())
        except Exception:  # noqa: BLE001
            LOG.exception("mdc: failed to merge runtime AI sidecar; falling back to disk-only")
            return base

    patched._mdc_runtime_patched = True  # type: ignore[attr-defined]
    UserConfigManager._load_config = patched  # type: ignore[method-assign]
    _RUNTIME_PATCH_INSTALLED = True
    LOG.info("mdc: installed runtime AI-config patch on marimo UserConfigManager")


# Back-compat alias.
_install_runtime_config_patch = install_runtime_config_patch


def _reset_runtime_registry_for_tests() -> None:
    """Test helper: wipe every sidecar in the runtime dir."""
    d = _runtime_dir()
    if d.exists():
        for p in d.glob("*.json"):
            try:
                p.unlink()
            except OSError:
                pass


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
) -> pathlib.Path:
    """Persist a provider into the per-user sidecar (cross-process safe)."""
    return _merge_into_sidecar(
        user_key,
        provider_name=provider_name,
        base_url=base_url,
        api_key=api_key,
        model_ids=model_ids,
        default_chat=default_chat,
        default_edit=default_edit,
        default_autocomplete=default_autocomplete,
    )


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
        # Cross-process per-user sidecar: works for multi-tenant app
        # deployments where the marimo server (parent process) and the
        # notebook kernel (subprocess) cannot share Python state, and where
        # writing to a shared marimo.toml would either leak config across
        # users or require restarting the server. The sidecar is read by
        # the UserConfigManager monkeypatch installed in the marimo server
        # process (auto-installed by ``marimo_databricks_connect.app.server``;
        # call ``install_runtime_config_patch()`` yourself when embedding
        # marimo into a different ASGI app).
        user_key = _runtime_user_key()
        cfg_path = _store_runtime_ai_config(
            user_key=user_key,
            provider_name=provider_name,
            base_url=base_url,
            api_key=_PROXY_API_KEY_SENTINEL,
            model_ids=model_ids,
            default_chat=_qualify(default_chat),
            default_edit=_qualify(default_edit),
            default_autocomplete=_qualify(default_autocomplete),
        )
        # Also install the patch in *this* process. For Databricks Apps the
        # server already installed it at import time; for environments where
        # the kernel was forked from the patched server this is a no-op. We
        # do it again here so a developer who imports this module from a
        # standalone script still ends up with a working setup.
        install_runtime_config_patch()
        msg = (
            f"mdc: registered {len(model_ids)} Databricks model(s) in sidecar "
            f"{cfg_path} for user={user_key!r}. Refresh the marimo browser tab "
            "so the AI panel picks up the new providers."
        )
        if verbose:
            print(msg)
        LOG.info(msg)
        if user_key == _DEFAULT_USER_KEY:
            # Local marimo edit: the marimo server process did not import
            # us, so the sidecar will be ignored. Tell the developer to use
            # scope='user' (or pyproject) instead.
            warn = (
                "mdc: scope='memory' relies on a UserConfigManager monkeypatch "
                "installed in the marimo server process. When running locally "
                "under `marimo edit` we don't control that process, so the "
                "sidecar is ignored. Use scope='user' (writes "
                "~/.config/marimo/marimo.toml) for local dev, or run via "
                "`python -m marimo_databricks_connect.app` so the patch is "
                "installed automatically."
            )
            if verbose:
                print(warn)
            LOG.warning(warn)
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
