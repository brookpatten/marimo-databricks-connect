"""Shared helpers for operational (single-instance) widgets."""

from __future__ import annotations

import time
from typing import Any


def ms_to_iso(ms: int | None) -> str | None:
    """Convert epoch-millis to ISO 8601 string, or None."""
    if ms is None:
        return None
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ms / 1000))
    except Exception:
        return None


def enum_val(obj: Any) -> str | None:
    """Extract .value from an enum, or stringify, or None."""
    if obj is None:
        return None
    return obj.value if hasattr(obj, "value") else str(obj)


def safe_dict(obj: Any) -> dict:
    """Convert a mapping-like object to a plain dict, or return {}."""
    if obj is None:
        return {}
    try:
        return dict(obj)
    except Exception:
        return {}


def duration_str(ms: int | None) -> str | None:
    """Human-readable duration from milliseconds."""
    if ms is None or ms <= 0:
        return None
    secs = ms // 1000
    if secs < 60:
        return f"{secs}s"
    mins, secs = divmod(secs, 60)
    if mins < 60:
        return f"{mins}m {secs}s"
    hrs, mins = divmod(mins, 60)
    return f"{hrs}h {mins}m {secs}s"


def get_workspace_client(ws: Any | None) -> Any:
    """Return the given client or create a default one."""
    if ws is not None:
        return ws
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient()
