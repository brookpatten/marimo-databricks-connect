"""OBO middleware + helpers for the app server.

Databricks Apps inject identity headers into every request when the app is
configured with **user authorization scopes**.  We extract them per request and
bind them into a contextvar so that any code running in the same async task
\u2014 including the marimo notebook session that's mounted further down the
ASGI tree \u2014 can ask :mod:`marimo_databricks_connect._obo` for the active
user's credentials.

References:
    https://learn.microsoft.com/en-us/azure/databricks/dev-tools/databricks-apps/auth
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Awaitable, Callable, Optional

from starlette.requests import HTTPConnection, Request
from starlette.types import ASGIApp, Receive, Scope, Send

from .. import _obo

LOGGER = logging.getLogger(__name__)

# Headers documented by Databricks Apps for OBO auth.
HEADER_TOKEN = "x-forwarded-access-token"
HEADER_USER = "x-forwarded-user"
HEADER_EMAIL = "x-forwarded-email"
# Note: ``X-Forwarded-Host`` is intentionally *not* used to derive the
# workspace API host. In Databricks Apps that header carries the public app
# URL (e.g. ``app-xxx.azure.databricksapps.com``), which is **not** a
# Databricks REST API endpoint -- pointing the SDK at it causes every call
# to hang and eventually time out. The workspace URL is provided by the
# ``DATABRICKS_HOST`` env var that the Apps runtime injects.


@dataclass(frozen=True)
class UserIdentity:
    """The end user the current request is acting on behalf of."""

    user: Optional[str]
    email: Optional[str]
    token: Optional[str]
    host: Optional[str]

    @property
    def display_name(self) -> str:
        """Return a display name for the user.

        Returns:
            str: The display name, preferring email, then username, then "unknown".
        """
        return self.email or self.user or "unknown"


def _databricks_host() -> Optional[str]:
    """Return the workspace URL the app is running in.

    Databricks Apps set ``DATABRICKS_HOST`` in the runtime environment.  When
    developing locally we fall back to the user's CLI config via the SDK.
    """
    host = os.environ.get("DATABRICKS_HOST")
    if host:
        return host if host.startswith("http") else f"https://{host}"
    try:  # pragma: no cover - convenience for local dev
        from databricks.sdk.config import Config

        cfg = Config()
        return cfg.host
    except Exception:
        return None


def identity_from_request(request: HTTPConnection) -> UserIdentity:
    """Extract OBO identity from incoming request headers.

    Accepts any :class:`starlette.requests.HTTPConnection` (covers both
    ``Request`` for HTTP and ``WebSocket`` for the marimo kernel socket).
    """
    h = request.headers
    token = h.get(HEADER_TOKEN)
    if not token:
        # Local-dev fallback: use whatever the unified auth chain finds.  The
        # contextvar stays unset so the package keeps its existing behaviour.
        return UserIdentity(user=None, email=None, token=None, host=_databricks_host())
    # Always use the workspace URL from the app runtime env; never the
    # request's Host/X-Forwarded-Host (those are the app's public URL).
    host = _databricks_host()
    return UserIdentity(
        user=h.get(HEADER_USER),
        email=h.get(HEADER_EMAIL),
        token=token,
        host=host,
    )


class OboMiddleware:
    """ASGI middleware that pushes OBO credentials into a contextvar.

    Applied at the *outer* ASGI layer so that every downstream handler \u2014
    FastAPI routes *and* the dynamically-mounted marimo apps \u2014 can read
    the active user's credentials.
    """

    def __init__(self, app: ASGIApp) -> None:
        """Initialize the OBO middleware.

        Args:
            app (ASGIApp): The ASGI application to wrap.
        """
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Handle an incoming ASGI request, injecting OBO credentials into the context.

        Args:
            scope (Scope): The ASGI scope for the request.
            receive (Receive): The ASGI receive callable.
            send (Send): The ASGI send callable.
        """
        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return
        # NB: use HTTPConnection (not Request) -- ``Request(scope)`` asserts
        # ``scope['type'] == 'http'`` and would raise AssertionError on every
        # WebSocket upgrade, killing the marimo kernel socket and surfacing
        # to the user as a misleading "kernel not found" error.
        ident = identity_from_request(HTTPConnection(scope))
        user_key = ident.email or ident.user
        if ident.token:
            state = _obo.set_credentials(ident.host, ident.token, user_key=user_key)
            scope.setdefault("state", {})
            scope["state"]["user"] = ident
            try:
                await self.app(scope, receive, send)
            finally:
                _obo.reset_credentials(state)
        else:
            scope.setdefault("state", {})
            scope["state"]["user"] = ident
            if user_key:
                # Even without an OBO token we want downstream code to know
                # which user is calling — e.g. for per-user runtime AI config.
                state = _obo.set_credentials(ident.host, None, user_key=user_key)
                try:
                    await self.app(scope, receive, send)
                finally:
                    _obo.reset_credentials(state)
            else:
                await self.app(scope, receive, send)


def get_request_user(request: Request) -> UserIdentity:
    """FastAPI dependency-style accessor for the current user."""
    user = request.scope.get("state", {}).get("user")
    if isinstance(user, UserIdentity):
        return user
    return identity_from_request(request)


# Convenience for ``with_dynamic_directory(middleware=[obo_middleware_factory])``
def obo_middleware_factory(app: ASGIApp) -> ASGIApp:
    """Return :class:`OboMiddleware` \u2014 marimo's middleware factory shape."""
    return OboMiddleware(app)


# Re-export for callers that want a Callable signature for documentation.
MiddlewareCallable = Callable[[ASGIApp], Awaitable[None]]
