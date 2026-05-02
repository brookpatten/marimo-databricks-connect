"""FastAPI/Starlette server: workspace browser + dynamic marimo mounts.

Routes:
    ``GET /``               browse workspace folders (filter to ``.py`` notebooks)
    ``GET /edit?path=...``  open the chosen notebook in a marimo session
    ``/m/<sanitised>/...``  marimo app mount (created on demand)

The marimo apps are mounted via :func:`marimo.create_asgi_app().with_dynamic_directory`
pointing at a per-process cache directory.  When the user opens a workspace
notebook for the first time, we ``workspace.export`` it (using *their* token)
into that cache and then redirect to the dynamic mount.

Every request flows through :class:`OboMiddleware`, which pushes the user's
OAuth token into a contextvar; both the FastAPI handlers and the marimo
notebook code read from there to act on behalf of the user.
"""

from __future__ import annotations

import json
import logging
import os
import re
import tempfile
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from starlette.requests import HTTPConnection

from .. import _obo
from .._workspace_fs import _is_dir_object, _object_type
from .auth import OboMiddleware, UserIdentity, get_request_user, identity_from_request, obo_middleware_factory
from .templates import (
    render_drafts_section,
    render_error,
    render_listing,
    render_page,
    render_starter_notebook,
)

LOGGER = logging.getLogger(__name__)

# ---- configuration --------------------------------------------------------


def _default_notebook_cache() -> Path:
    """Pick a writable cache directory **outside** ``/tmp``.

    The marimo frontend treats any notebook path under ``/tmp/`` (or
    ``/var/folders``, or Windows ``AppData\\Local\\Temp``) as a
    non-persistent scratch file and pops up a Save-As dialog every time the
    user hits Save — even when the file is happily round-tripping to the
    workspace. We therefore avoid those prefixes by default.

    Resolution order:
      1. ``$MDC_APP_NOTEBOOK_CACHE`` (honoured even if it's under ``/tmp``;
         a warning is logged so operators know save UX will be degraded).
      2. ``$HOME/.cache/mdc-notebooks`` when ``HOME`` exists and is writable.
      3. ``/var/tmp/mdc-notebooks`` (writable in Databricks Apps containers
         and outside marimo's tmp denylist).
      4. As a last resort, a fresh ``mkdtemp`` under the system temp dir
         (will trigger the Save-As dialog, but at least the app starts).
    """
    explicit = os.environ.get("MDC_APP_NOTEBOOK_CACHE")
    if explicit:
        p = Path(explicit)
        if str(p).startswith("/tmp/") or str(p).startswith("/var/folders"):
            LOGGER.warning(
                "MDC_APP_NOTEBOOK_CACHE=%s is under a path marimo treats as"
                " non-persistent (/tmp/, /var/folders/). The marimo Save"
                " button will keep prompting for a filename. Move the cache"
                " to e.g. /var/tmp/mdc-notebooks or $HOME/.cache/mdc-notebooks"
                " to fix.",
                explicit,
            )
        return p

    candidates: list[Path] = []
    home = os.environ.get("HOME")
    if home:
        candidates.append(Path(home) / ".cache" / "mdc-notebooks")
    candidates.append(Path("/var/tmp/mdc-notebooks"))

    for c in candidates:
        try:
            c.mkdir(parents=True, exist_ok=True)
            # Sanity check: write a probe file to confirm the path is writable.
            probe = c / ".write-probe"
            probe.write_text("ok")
            probe.unlink()
            return c
        except OSError:
            continue

    fallback = Path(tempfile.mkdtemp(prefix="mdc-app-"))
    LOGGER.warning(
        "Falling back to %s for notebook cache; marimo will treat saved"
        " notebooks as scratch files and prompt for a filename on Save."
        " Set MDC_APP_NOTEBOOK_CACHE to a path outside /tmp to fix.",
        fallback,
    )
    return fallback


# Where exported notebooks land before being served by marimo.
NOTEBOOK_CACHE = _default_notebook_cache()
NOTEBOOK_CACHE.mkdir(parents=True, exist_ok=True)

# URL prefix the dynamic marimo mount lives at.  Each ``.py`` file inside
# NOTEBOOK_CACHE becomes ``/m/<filename-without-ext>``.
MARIMO_MOUNT = "/m"


# ---- workspace listing ----------------------------------------------------


def _build_workspace_client(user: UserIdentity):
    """Build a WorkspaceClient acting as the request's end user."""
    from databricks.sdk import WorkspaceClient

    if user.token:
        # Force PAT auth so the SDK ignores DATABRICKS_CLIENT_ID /
        # DATABRICKS_CLIENT_SECRET env vars that the Databricks Apps runtime
        # injects for the app's service principal -- otherwise the unified
        # auth validator sees both PAT and OAuth configured and refuses to
        # pick one.
        return WorkspaceClient(host=user.host, token=user.token, auth_type="pat")
    # Local dev: fall back to unified auth chain.
    return WorkspaceClient()


def _list_workspace(user: UserIdentity, path: str) -> list[dict]:
    ws = _build_workspace_client(user)
    entries: list[dict] = []
    for obj in ws.workspace.list(path or "/"):
        full = getattr(obj, "path", "") or ""
        name = full.rsplit("/", 1)[-1] or full
        ot = _object_type(obj).upper()
        if _is_dir_object(obj):
            entries.append({"kind": "dir", "name": name, "path": full})
            continue
        if ot == "NOTEBOOK":
            language = getattr(obj, "language", None)
            lang = getattr(language, "value", None) or getattr(language, "name", None) or str(language or "")
            # Only surface python notebooks; skip SCALA/SQL/R for now.
            if str(lang).upper() != "PYTHON":
                continue
            entries.append({"kind": "notebook", "name": name, "path": full, "language": lang})
        # Plain ``FILE`` objects are shown for context but aren't openable.
        elif ot == "FILE" and full.endswith(".py"):
            entries.append({"kind": "notebook", "name": name, "path": full, "language": "PY"})
    entries.sort(key=lambda e: (0 if e["kind"] == "dir" else 1, e["name"].lower()))
    return entries


# ---- notebook export ------------------------------------------------------


def _default_new_dir(user: UserIdentity) -> str:
    """Suggested workspace directory new notebooks land in.

    ``/Users/<email>/marimo`` when we know the user; otherwise empty (the
    handler will fall back to the currently browsed path).
    """
    who = user.email or user.user
    if not who:
        return ""
    return f"/Users/{who}/marimo"


def _user_home(user: UserIdentity) -> str:
    """Workspace home folder for the signed-in user, or ``/`` if unknown."""
    who = user.email or user.user
    return f"/Users/{who}" if who else "/"


_SLUG_RE = re.compile(r"[^a-zA-Z0-9_-]+")


def _slug_for(path: str) -> str:
    """Stable filesystem-safe id for a workspace path.

    The slug names the per-notebook cache *directory* under
    ``NOTEBOOK_CACHE/<slug>/<basename>.py`` and becomes the first URL
    segment under ``/m/<slug>/<basename>``. It MUST NOT contain a ``.``
    — marimo's ``DynamicDirectoryMiddleware`` skips its ``<slug>.py``
    lookup when the URL segment already has a suffix.
    """
    s = _SLUG_RE.sub("_", path.strip("/")).strip("_")
    return s or "notebook"


def _basename_for(ws_path: str) -> str:
    """Pick a filesystem-safe ``.py`` basename for a workspace path.

    Databricks notebook paths have no extension; plain workspace files keep
    theirs. Always normalise to ``<name>.py`` so marimo's UI shows it as a
    Python notebook and the dynamic-directory router can find it.

    The returned name is also guarded against shadowing top-level Python
    packages: marimo's kernel subprocess prepends the notebook's directory
    to ``sys.path``, so a file called e.g. ``marimo.py`` or ``pyspark.py``
    would break ``import marimo`` from inside the notebook and crash the
    kernel before it can connect. We prefix any such name with ``nb_``.
    """
    import importlib.util

    leaf = (ws_path.rsplit("/", 1)[-1] or "notebook").strip()
    leaf = _SLUG_RE.sub("_", leaf).strip("_") or "notebook"
    if leaf.endswith("_py"):
        leaf = leaf[: -len("_py")] + ".py"
    if not leaf.endswith(".py"):
        leaf = leaf + ".py"
    if leaf.startswith("_"):
        leaf = "nb" + leaf  # DynamicDirectoryMiddleware skips ``_*.py``
    stem = leaf[:-3]
    # Avoid shadowing importable top-level modules (most importantly
    # ``marimo`` itself, but also ``pyspark``, ``databricks``, ...).
    try:
        shadows = importlib.util.find_spec(stem) is not None
    except (ImportError, ValueError):
        shadows = False
    if shadows or stem in _RESERVED_BASENAMES:
        leaf = f"nb_{leaf}"
    return leaf


# Names that aren't necessarily importable in the current process but that
# the notebook code is virtually guaranteed to import — keep them out of
# the basename slot regardless of what's installed at app build time.
_RESERVED_BASENAMES = frozenset(
    {
        "marimo",
        "pyspark",
        "databricks",
        "fastapi",
        "starlette",
        "uvicorn",
        "pandas",
        "numpy",
        "sklearn",
        "scipy",
        "matplotlib",
    }
)


# ---- per-slug cache layout / metadata sidecar ----------------------------
#
# Each cached notebook lives in its own directory:
#
#     <NOTEBOOK_CACHE>/<slug>/<basename>.py
#     <NOTEBOOK_CACHE>/<slug>/.meta.json
#
# The directory wrapper lets us pick a *human-friendly* basename (matching
# the workspace path's leaf, e.g. ``demo.py``) so marimo's UI shows the
# real notebook name instead of the URL slug, while still keeping the slug
# unique across the cache. The dynamic-directory mount then routes
# ``/m/<slug>/<basename_no_ext>`` to the file.

_META_NAME = ".meta.json"


def _cache_dir(slug: str) -> Path:
    return NOTEBOOK_CACHE / slug


def _meta_path(slug: str) -> Path:
    return _cache_dir(slug) / _META_NAME


def _cache_path(slug: str) -> Optional[Path]:
    """Resolve the on-disk ``.py`` file for ``slug``, or ``None`` if absent."""
    d = _cache_dir(slug)
    if not d.is_dir():
        return None
    meta = _load_meta(slug)
    fname = meta.get("filename")
    if fname:
        candidate = d / fname
        if candidate.exists():
            return candidate
    # Fall back to the first non-underscore .py file in the directory.
    for p in sorted(d.glob("*.py")):
        if not p.name.startswith("_"):
            return p
    return None


def _slug_for_cache_file(p: Path) -> Optional[str]:
    """Reverse of ``_cache_path``: given an absolute file in the cache.

    Returns the slug (parent dir name) or ``None`` if not under NOTEBOOK_CACHE.
    """
    try:
        rel = p.resolve().relative_to(NOTEBOOK_CACHE.resolve())
    except (ValueError, OSError):
        return None
    parts = rel.parts
    if not parts:
        return None
    return parts[0]


def _open_url(slug: str, filename: Optional[str] = None) -> str:
    """Build the marimo edit URL for a slug.

    Falls back to a redirect-only ``/open/<slug>`` route when we don't yet
    know the basename (e.g. legacy callers that only have the slug).
    """
    if not filename:
        cache = _cache_path(slug)
        filename = cache.name if cache else None
    if not filename:
        return f"/open/{slug}"
    stem = filename[:-3] if filename.endswith(".py") else filename
    return f"{MARIMO_MOUNT}/{slug}/{stem}"


def _load_meta(slug: str) -> dict:
    p = _meta_path(slug)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        LOGGER.warning("Discarding unreadable metadata for slug %r", slug)
        return {}


def _save_meta(slug: str, meta: dict) -> None:
    d = _cache_dir(slug)
    d.mkdir(parents=True, exist_ok=True)
    _meta_path(slug).write_text(json.dumps(meta, indent=2, sort_keys=True), encoding="utf-8")


def _list_drafts() -> list[dict]:
    """Return cached notebooks (newest first) with save state for the index page."""
    out: list[dict] = []
    for d in NOTEBOOK_CACHE.iterdir():
        if not d.is_dir():
            continue
        slug = d.name
        cache = _cache_path(slug)
        if cache is None:
            continue
        meta = _load_meta(slug)
        try:
            mtime = cache.stat().st_mtime
        except OSError:
            continue
        uploaded = float(meta.get("last_uploaded_mtime") or 0.0)
        out.append(
            {
                "slug": slug,
                "filename": cache.name,
                "open_url": _open_url(slug, cache.name),
                "workspace_path": meta.get("workspace_path"),
                "mtime": mtime,
                "last_uploaded_mtime": uploaded,
                "dirty": mtime > uploaded + 0.5,  # 0.5s slack for fs timestamp jitter
            }
        )
    out.sort(key=lambda d: d["mtime"], reverse=True)
    return out


def _import_to_workspace(user: UserIdentity, ws_path: str, source: bytes) -> None:
    """Upload ``source`` to ``ws_path`` as a Python notebook (overwrite).

    We import as ``format=SOURCE`` + ``language=PYTHON`` so the file shows up
    as a Databricks notebook (round-trips perfectly through ``workspace.export``
    even though the Databricks notebook UI will render the marimo source as a
    single cell). Parent directories are created on demand.
    """
    import base64

    from databricks.sdk.service.workspace import ImportFormat, Language

    ws = _build_workspace_client(user)
    parent = ws_path.rsplit("/", 1)[0]
    if parent and parent != "":
        try:
            ws.workspace.mkdirs(parent)
        except Exception:  # noqa: BLE001 -- mkdirs is idempotent; ignore errors
            LOGGER.debug("mkdirs(%r) failed (likely already exists)", parent, exc_info=True)
    ws.workspace.import_(
        path=ws_path,
        content=base64.b64encode(source).decode("ascii"),
        format=ImportFormat.SOURCE,
        language=Language.PYTHON,
        overwrite=True,
    )


def _save_slug_to_workspace(user: UserIdentity, slug: str) -> str:
    """Push the cached notebook at ``slug`` back to its tracked workspace path.

    Returns the workspace path written. Raises ``HTTPException`` on usage errors.
    """
    cache = _cache_path(slug)
    if cache is None:
        raise HTTPException(404, f"No cached notebook for slug {slug!r}")
    meta = _load_meta(slug)
    ws_path = meta.get("workspace_path")
    if not ws_path:
        raise HTTPException(
            400,
            f"Notebook {slug!r} has no workspace target. Use 'Save as' to choose one.",
        )
    source = cache.read_bytes()
    _import_to_workspace(user, ws_path, source)
    meta["last_uploaded_mtime"] = cache.stat().st_mtime
    _save_meta(slug, meta)
    return ws_path


def _export_notebook(user: UserIdentity, ws_path: str) -> Path:
    """Download ``ws_path`` from the workspace into NOTEBOOK_CACHE.

    Always re-exports on each open so notebook edits in the workspace are
    picked up. Records the workspace origin in a sidecar so subsequent
    Save buttons know where to push edits back to.
    """
    import base64

    from databricks.sdk.service.workspace import ExportFormat

    ws = _build_workspace_client(user)
    resp = ws.workspace.export(path=ws_path, format=ExportFormat.SOURCE)
    content_b64 = getattr(resp, "content", None)
    if content_b64 is None:
        raise HTTPException(500, f"Workspace export returned no content for {ws_path!r}")
    raw = base64.b64decode(content_b64)
    slug = _slug_for(ws_path)
    filename = _basename_for(ws_path)
    d = _cache_dir(slug)
    d.mkdir(parents=True, exist_ok=True)
    # Clean up stale .py files from a previous export under the same slug
    # (e.g. if a workspace path was renamed). Keep the metadata sidecar.
    for old in d.glob("*.py"):
        if old.name != filename:
            try:
                old.unlink()
            except OSError:
                pass
    target = d / filename
    target.write_bytes(raw)
    _save_meta(
        slug,
        {
            "workspace_path": ws_path,
            "filename": filename,
            "last_uploaded_mtime": target.stat().st_mtime,
            "origin": "workspace",
        },
    )
    return target


# ---- save-back middleware -------------------------------------------------


# marimo's save endpoint relative URL inside each per-notebook sub-app.
_MARIMO_SAVE_PATH = "/api/kernel/save"


def _push_cache_file_to_workspace(file_path: str, user: Optional[UserIdentity]) -> None:
    """Best-effort upload: read ``file_path`` and import it back to its tracked workspace path.

    Logged-and-swallowed on error so save UX in marimo isn't broken by transient API issues
    — the user can retry from the index page "Save" button.
    """
    if user is None or not user.token:
        LOGGER.warning(
            "Skipping workspace save for %s: no OBO user/token in scope",
            file_path,
        )
        return
    try:
        p = Path(file_path)
        slug = _slug_for_cache_file(p)
        if slug is None:
            LOGGER.debug("Save: %s is outside NOTEBOOK_CACHE, skipping", file_path)
            return
        meta = _load_meta(slug)
        ws_path = meta.get("workspace_path")
        if not ws_path:
            LOGGER.info("Save: slug %r has no workspace_path — keeping local-only", slug)
            return
        # Reflect the actual on-disk filename in metadata so future saves
        # use the right basename if marimo ever rewrote it.
        meta["filename"] = p.name
        source = p.read_bytes()
        _import_to_workspace(user, ws_path, source)
        meta["last_uploaded_mtime"] = p.stat().st_mtime
        _save_meta(slug, meta)
        LOGGER.info("Saved %s back to workspace path %s", p, ws_path)
    except Exception:  # noqa: BLE001
        LOGGER.exception("Failed to push %s back to workspace", file_path)


def workspace_save_middleware_factory(app):
    """Wrap a marimo per-notebook sub-app, mirroring saves to the workspace.

    The middleware fires after a successful ``POST .../api/kernel/save`` and
    re-uploads the freshly written cache file to the workspace path tracked
    in the slug's ``.meta.json``.
    """

    class WorkspaceSaveMiddleware:
        def __init__(self, inner):
            self.app = inner

        async def __call__(self, scope, receive, send):
            if scope.get("type") != "http":
                await self.app(scope, receive, send)
                return
            method = scope.get("method", "").upper()
            path = scope.get("path", "") or ""
            is_save = method == "POST" and path.endswith(_MARIMO_SAVE_PATH)
            if not is_save:
                await self.app(scope, receive, send)
                return

            # Capture the user identity *before* awaiting the inner app so
            # we don't depend on contextvar propagation across the call.
            user: Optional[UserIdentity] = None
            state = scope.get("state") or {}
            cand = state.get("user")
            if isinstance(cand, UserIdentity):
                user = cand
            else:
                try:
                    user = identity_from_request(HTTPConnection(scope))
                except Exception:  # noqa: BLE001
                    user = None
            marimo_file = scope.get("marimo_app_file")

            status_holder = {"status": 500}

            async def send_wrapper(msg):
                if msg.get("type") == "http.response.start":
                    status_holder["status"] = msg.get("status", 500)
                await send(msg)

            await self.app(scope, receive, send_wrapper)

            if 200 <= status_holder["status"] < 300 and marimo_file:
                _push_cache_file_to_workspace(marimo_file, user)

    return WorkspaceSaveMiddleware(app)


# ---- ASGI app -------------------------------------------------------------


def _force_marimo_edit_mode() -> None:
    """Patch marimo so its ASGI builder serves notebooks in **edit** mode.

    ``marimo.create_asgi_app().with_dynamic_directory`` hard-codes
    ``SessionMode.RUN`` (read-only/app view). We want the full editor, so we
    wrap :class:`~marimo._server.session_manager.SessionManager` to coerce
    ``mode=RUN`` → ``mode=EDIT`` *before* the rest of ``__init__`` runs (so
    the token-manager / resume-strategy are built for the right mode).

    Idempotent and safe to call at import time — this process serves only
    marimo notebooks, so the patch has no other consumers.
    """
    from marimo._server.session_manager import SessionManager
    from marimo._session.model import SessionMode

    if getattr(SessionManager.__init__, "_mdc_edit_patched", False):
        return
    original_init = SessionManager.__init__

    def patched_init(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        if kwargs.get("mode") is SessionMode.RUN:
            kwargs["mode"] = SessionMode.EDIT
        return original_init(self, *args, **kwargs)

    patched_init._mdc_edit_patched = True  # type: ignore[attr-defined]
    SessionManager.__init__ = patched_init  # type: ignore[method-assign]


def _build_marimo_asgi():
    """Build the marimo ASGI app that serves the cache directory dynamically.

    ``with_dynamic_directory`` re-scans on each request, so notebooks freshly
    exported into NOTEBOOK_CACHE become routable immediately.
    """
    _force_marimo_edit_mode()
    # Install the runtime AI-provider sidecar patch in the *server* process
    # before marimo touches its config. The patch makes UserConfigManager
    # merge each request's per-user sidecar (written by
    # ``register_serving_endpoints_as_ai_providers(scope="memory")`` in the
    # kernel subprocess) into the config it serves to the AI panel. Without
    # this, AI providers registered from inside a notebook are invisible
    # to the marimo frontend.
    from .._ai import install_runtime_config_patch

    install_runtime_config_patch()

    import marimo

    builder = marimo.create_asgi_app(quiet=True, include_code=True)
    builder = builder.with_dynamic_directory(
        path=MARIMO_MOUNT,
        directory=str(NOTEBOOK_CACHE),
        # Marimo applies these middlewares to every sub-app, so the OBO
        # contextvar is populated for code running inside the notebook (i.e.
        # ``from marimo_databricks_connect import spark`` will see the user's
        # token and build a per-user DatabricksSession).
        #
        # ``workspace_save_middleware_factory`` watches for marimo's
        # ``POST .../api/kernel/save`` and pushes the freshly written file
        # back to the user's workspace. It runs *inside* the OBO middleware
        # so the contextvar is set; the explicit user lookup below is a
        # belt-and-suspenders for environments where the contextvar copy
        # doesn't propagate across the awaited subapp call.
        middleware=[obo_middleware_factory, workspace_save_middleware_factory],
    )
    return builder.build()


def build_app() -> FastAPI:
    """Construct the FastAPI ``ASGIApp``."""
    app = FastAPI(title="marimo \u00b7 Databricks")

    marimo_asgi = _build_marimo_asgi()
    # Mount marimo at /m \u2014 the dynamic-directory builder owns everything
    # under that path.
    app.mount(MARIMO_MOUNT, marimo_asgi)

    @app.get("/healthz")
    async def healthz() -> dict:
        return {"ok": True}

    @app.get("/", response_class=HTMLResponse)
    async def index(request: Request, path: Optional[str] = None) -> HTMLResponse:
        user = get_request_user(request)
        if not path:
            path = _user_home(user)
        try:
            entries = _list_workspace(user, path)
            listing = render_listing(entries, path, default_new_dir=path)
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Failed to list workspace path %r", path)
            listing = render_error(f"Failed to list {path!r}: {exc}")
        body = render_drafts_section(_list_drafts()) + listing
        return HTMLResponse(
            render_page(
                title=f"Workspace: {path}",
                user=user.display_name,
                body=body,
            )
        )

    @app.post("/new")
    async def new_notebook(request: Request) -> RedirectResponse:
        """Create a fresh starter notebook in NOTEBOOK_CACHE and open it.

        If the form supplies a ``workspace_path`` we also import the starter
        into the user's workspace immediately, so a subsequent Save just
        overwrites that path. Without one, the notebook lives only in the
        local app cache (and the Save button will require a path).
        """
        from datetime import datetime, timezone

        user = get_request_user(request)
        form = await request.form()
        ws_path = (form.get("workspace_path") or "").strip() or None
        if ws_path is None:
            # New form: ``directory`` + ``name`` (joined here so the slug is
            # stable across revisits of the same workspace path).
            directory = (form.get("directory") or "").strip()
            name = (form.get("name") or "").strip()
            if directory and name:
                if "/" in name:
                    raise HTTPException(400, "`name` must not contain '/'")
                if not name.endswith(".py"):
                    name = name + ".py"
                ws_path = directory.rstrip("/") + "/" + name
        if ws_path is not None and not ws_path.startswith("/"):
            raise HTTPException(400, "`workspace_path` must be an absolute workspace path (start with /)")

        if ws_path:
            # Reuse the workspace path's slug so re-opening from the index
            # listing maps to the same cached file.
            slug = _slug_for(ws_path)
        else:
            stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
            whom = _slug_for(user.email or user.user or "anon") or "anon"
            slug = f"untitled-{whom}-{stamp}"

        if ws_path:
            filename = _basename_for(ws_path)
        else:
            # Local-only notebooks: use the supplied name (or a default).
            raw_name = (form.get("name") or "untitled.py").strip()
            filename = _basename_for(raw_name)

        d = _cache_dir(slug)
        d.mkdir(parents=True, exist_ok=True)
        target = d / filename
        try:
            target.write_text(render_starter_notebook(), encoding="utf-8")
        except OSError as exc:
            LOGGER.exception("Failed to write starter notebook to %s", target)
            raise HTTPException(500, f"Could not create starter notebook: {exc}") from exc

        meta: dict = {"origin": "new", "filename": filename}
        if ws_path:
            try:
                _import_to_workspace(user, ws_path, target.read_bytes())
            except Exception as exc:  # noqa: BLE001
                LOGGER.exception("Failed to import new notebook to workspace at %r", ws_path)
                raise HTTPException(500, f"Could not save new notebook to {ws_path!r}: {exc}") from exc
            meta["workspace_path"] = ws_path
            meta["last_uploaded_mtime"] = target.stat().st_mtime
        _save_meta(slug, meta)
        return RedirectResponse(url=_open_url(slug, filename), status_code=303)

    @app.post("/save")
    async def save_notebook(request: Request) -> RedirectResponse:
        """Push a cached notebook back to its tracked workspace path.

        Form fields:
            slug (required): cache slug as shown in the Drafts section.
            workspace_path (optional): when supplied, retargets the notebook
                ("Save as") and updates the sidecar metadata.
            return_to (optional): URL to redirect to (defaults to /).
        """
        user = get_request_user(request)
        form = await request.form()
        slug = (form.get("slug") or "").strip()
        if not slug:
            raise HTTPException(400, "`slug` is required")
        new_target = (form.get("workspace_path") or "").strip()
        if new_target:
            if not new_target.startswith("/"):
                raise HTTPException(400, "`workspace_path` must be absolute")
            meta = _load_meta(slug)
            meta["workspace_path"] = new_target
            _save_meta(slug, meta)
        try:
            _save_slug_to_workspace(user, slug)
        except HTTPException:
            raise
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Failed to save slug %r to workspace", slug)
            raise HTTPException(500, f"Could not save notebook {slug!r}: {exc}") from exc
        return RedirectResponse(url=form.get("return_to") or "/", status_code=303)

    @app.post("/delete-draft")
    async def delete_draft(request: Request) -> RedirectResponse:
        """Remove a cached notebook (and its sidecar) from NOTEBOOK_CACHE.

        The workspace copy, if any, is left untouched.
        """
        form = await request.form()
        slug = (form.get("slug") or "").strip()
        if not slug or "/" in slug or slug.startswith("."):
            raise HTTPException(400, "invalid `slug`")
        d = _cache_dir(slug)
        if d.exists():
            try:
                # Only delete files we created (.py + meta) to avoid wiping
                # arbitrary content if NOTEBOOK_CACHE was misconfigured.
                for p in list(d.glob("*.py")) + [_meta_path(slug)]:
                    try:
                        p.unlink()
                    except FileNotFoundError:
                        pass
                d.rmdir()
            except OSError as exc:
                LOGGER.exception("Failed to delete cache dir %s", d)
                raise HTTPException(500, f"Could not delete draft: {exc}") from exc
        return RedirectResponse(url=form.get("return_to") or "/", status_code=303)

    @app.get("/edit")
    async def edit(request: Request, path: str) -> RedirectResponse:
        user = get_request_user(request)
        if not path or not path.startswith("/"):
            raise HTTPException(400, "`path` must be an absolute workspace path")
        try:
            _export_notebook(user, path)
        except HTTPException:
            raise
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Failed to export notebook %r", path)
            raise HTTPException(500, f"Could not export notebook {path!r}: {exc}") from exc
        slug = _slug_for(path)
        return RedirectResponse(url=_open_url(slug), status_code=303)

    @app.get("/open/{slug}")
    async def open_slug(slug: str) -> RedirectResponse:
        """Resolve a slug to its current ``/m/<slug>/<basename>`` URL.

        Used by older bookmarks / templates that only know the slug.
        """
        cache = _cache_path(slug)
        if cache is None:
            raise HTTPException(404, f"No cached notebook for slug {slug!r}")
        return RedirectResponse(url=_open_url(slug, cache.name), status_code=303)

    # Outermost middleware so marimo sub-apps see the contextvar too.
    app.add_middleware(OboMiddleware)
    return app


# Module-level instance for ``uvicorn marimo_databricks_connect.app:asgi``.
asgi: Optional[FastAPI] = None


def _get_asgi() -> FastAPI:
    global asgi
    if asgi is None:
        asgi = build_app()
    return asgi


# When imported as ``marimo_databricks_connect.app:asgi`` (the form documented
# in app.yaml), uvicorn will look up the module attribute at startup; build
# eagerly so it's ready.
asgi = _get_asgi()


# ---- CLI ------------------------------------------------------------------


def main() -> None:
    """Run the server with uvicorn (used by ``python -m`` and Databricks Apps)."""
    import uvicorn

    host = os.environ.get("HOST", "0.0.0.0")  # noqa: S104 \u2014 bound by Apps router
    # Databricks Apps inject the listen port via DATABRICKS_APP_PORT.
    port = int(os.environ.get("DATABRICKS_APP_PORT") or os.environ.get("PORT") or 8000)
    LOGGER.info("Starting marimo-databricks app on %s:%d", host, port)
    uvicorn.run(
        "marimo_databricks_connect.app.server:asgi",
        host=host,
        port=port,
        log_level=os.environ.get("LOG_LEVEL", "info"),
    )
