"""On-behalf-of-user (OBO) credential plumbing for Databricks Apps.

When ``marimo_databricks_connect`` is imported inside a notebook that is being
served by the Databricks App in :mod:`marimo_databricks_connect.app`, we want
``spark`` / ``workspace`` / etc. to act *as the end user*, not as the app's
service principal.

Databricks Apps surface the calling user's OAuth token in the
``X-Forwarded-Access-Token`` request header (see the OBO docs). The app server
extracts that header in middleware and stashes it here via :func:`set_credentials`.
The cached singletons in :mod:`marimo_databricks_connect.__init__` then build a
``WorkspaceClient`` / ``DatabricksSession`` per (host, token) pair instead of
falling back to the unified-auth chain.

If no credentials have been pushed (i.e. the package is being used outside of
the app — local ``marimo edit`` etc.), :func:`get_credentials` returns
``(None, None)`` and the existing behaviour is preserved.
"""

from __future__ import annotations

from contextvars import ContextVar
from typing import Optional

_user_token: ContextVar[Optional[str]] = ContextVar("mdc_obo_token", default=None)
_user_host: ContextVar[Optional[str]] = ContextVar("mdc_obo_host", default=None)
_user_key_var: ContextVar[Optional[str]] = ContextVar("mdc_obo_user_key", default=None)


def set_credentials(
    host: Optional[str],
    token: Optional[str],
    user_key: Optional[str] = None,
) -> object:
    """Push (host, token, user_key) into the current async/thread context.

    ``user_key`` is a stable per-user identifier (e.g. email) used by code
    that wants to keep per-user in-process state — see :mod:`._ai` for the
    runtime AI-provider registry. It is purely advisory; auth itself only
    needs ``host`` + ``token``.

    Returns an opaque token usable with :func:`reset_credentials`.
    """
    t1 = _user_token.set(token)
    t2 = _user_host.set(host)
    t3 = _user_key_var.set(user_key)
    return (t1, t2, t3)


def reset_credentials(state: object) -> None:
    """Restore the credentials state returned by :func:`set_credentials`."""
    t1, t2, t3 = state  # type: ignore[misc]
    _user_token.reset(t1)
    _user_host.reset(t2)
    _user_key_var.reset(t3)


def get_credentials() -> tuple[Optional[str], Optional[str]]:
    """Return ``(host, token)`` for the current request, or ``(None, None)``."""
    return _user_host.get(), _user_token.get()


def get_user_key() -> Optional[str]:
    """Return the current request's user identifier (e.g. email), if known."""
    return _user_key_var.get()


def is_obo_active() -> bool:
    """True when an OBO user token is currently in scope."""
    return _user_token.get() is not None
