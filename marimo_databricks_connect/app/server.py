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

import logging
import os
import re
import tempfile
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from .. import _obo
from .._workspace_fs import _is_dir_object, _object_type
from .auth import OboMiddleware, UserIdentity, get_request_user, obo_middleware_factory
from .templates import render_error, render_listing, render_page

LOGGER = logging.getLogger(__name__)

# ---- configuration --------------------------------------------------------

# Where exported notebooks land before being served by marimo.  Defaults to a
# per-process tmp dir; override with MDC_APP_NOTEBOOK_CACHE for a writable
# persistent volume in production deploys.
NOTEBOOK_CACHE = Path(os.environ.get("MDC_APP_NOTEBOOK_CACHE") or tempfile.mkdtemp(prefix="mdc-app-"))
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

_SLUG_RE = re.compile(r"[^a-zA-Z0-9._-]+")


def _slug_for(path: str) -> str:
    """Stable filesystem-safe id for a workspace path."""
    s = _SLUG_RE.sub("_", path.strip("/")).strip("_")
    return s or "notebook"


def _export_notebook(user: UserIdentity, ws_path: str) -> Path:
    """Download ``ws_path`` from the workspace into NOTEBOOK_CACHE.

    Always re-exports on each open so notebook edits in the workspace are
    picked up.  (Save-back from marimo to the workspace is intentionally out
    of scope for this initial cut.)
    """
    import base64

    ws = _build_workspace_client(user)
    resp = ws.workspace.export(path=ws_path, format="SOURCE")
    content_b64 = getattr(resp, "content", None)
    if content_b64 is None:
        raise HTTPException(500, f"Workspace export returned no content for {ws_path!r}")
    raw = base64.b64decode(content_b64)
    target = NOTEBOOK_CACHE / f"{_slug_for(ws_path)}.py"
    target.write_bytes(raw)
    return target


# ---- ASGI app -------------------------------------------------------------


def _build_marimo_asgi():
    """Build the marimo ASGI app that serves the cache directory dynamically.

    ``with_dynamic_directory`` re-scans on each request, so notebooks freshly
    exported into NOTEBOOK_CACHE become routable immediately.
    """
    import marimo

    builder = marimo.create_asgi_app(quiet=True, include_code=True)
    builder = builder.with_dynamic_directory(
        path=MARIMO_MOUNT,
        directory=str(NOTEBOOK_CACHE),
        # Marimo applies these middlewares to every sub-app, so the OBO
        # contextvar is populated for code running inside the notebook (i.e.
        # ``from marimo_databricks_connect import spark`` will see the user's
        # token and build a per-user DatabricksSession).
        middleware=[obo_middleware_factory],
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
    async def index(request: Request, path: str = "/") -> HTMLResponse:
        user = get_request_user(request)
        try:
            entries = _list_workspace(user, path)
            body = render_listing(entries, path)
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Failed to list workspace path %r", path)
            body = render_error(f"Failed to list {path!r}: {exc}")
        return HTMLResponse(
            render_page(
                title=f"Workspace: {path}",
                user=user.display_name,
                body=body,
            )
        )

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
        return RedirectResponse(url=f"{MARIMO_MOUNT}/{slug}", status_code=303)

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
