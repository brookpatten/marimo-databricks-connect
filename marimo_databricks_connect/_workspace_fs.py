"""fsspec filesystem backed by the Databricks Workspace API.

Marimo's storage browser auto-detects any ``fsspec.AbstractFileSystem`` instance
in the notebook globals and renders it as a browseable tree. By exposing a
``WorkspaceFileSystem`` instance (e.g. ``workspace``) we get a Databricks
Workspace browser (notebooks, files, repos, folders) for free, alongside the
existing UC volumes / external-location browsers.

All path access flows through ``WorkspaceClient.workspace`` so it honours
workspace ACLs.
"""

from __future__ import annotations

import base64
import io
import logging
from typing import Any

from fsspec.spec import AbstractFileSystem  # type: ignore[import-untyped]

LOGGER = logging.getLogger(__name__)


def _object_type(entry: Any) -> str:
    """Return the ``object_type`` of a Workspace ``ObjectInfo`` as a string."""
    ot = getattr(entry, "object_type", None)
    if ot is None:
        return ""
    # SDK enum exposes ``.value`` and a useful ``str(...)``
    return getattr(ot, "value", None) or getattr(ot, "name", None) or str(ot)


def _is_dir_object(entry: Any) -> bool:
    """True for workspace ObjectInfo that should be treated as a folder."""
    return _object_type(entry).upper() in ("DIRECTORY", "REPO")


class WorkspaceFileSystem(AbstractFileSystem):
    """Read-only fsspec view over the Databricks Workspace API.

    Args:
        workspace_client: A ``databricks.sdk.WorkspaceClient``.
        root: Default root for relative paths (defaults to ``"/"``).
            Absolute workspace paths (``/Users/...``, ``/Workspace/...``,
            ``/Repos/...``, ``/Shared/...``) are passed through unchanged.
        export_format: Format passed to ``workspace.export`` when reading
            file/notebook content.  ``"SOURCE"`` returns the raw source
            (``.py``/``.sql``/``.scala``/``.r``).  Other valid values:
            ``"HTML"``, ``"JUPYTER"``, ``"DBC"``, ``"AUTO"``.
    """

    protocol = "workspace"
    sep = "/"

    def __init__(
        self,
        workspace_client: Any,
        root: str = "/",
        export_format: str = "SOURCE",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._ws = workspace_client
        self._root = root.rstrip("/") or "/"
        self._export_format = export_format

    # ``root_marker`` is read by marimo's FsspecFilesystem adapter to populate
    # the StorageNamespace's ``root_path``.
    @property
    def root_marker(self) -> str:  # type: ignore[override]
        return self._root

    # -- path handling ----------------------------------------------------

    def _resolve(self, path: str) -> str:
        if not path or path in (".", "/"):
            return self._root
        if path.startswith("/"):
            return path
        # Strip our protocol prefix if a fully-qualified URL was passed.
        if path.startswith("workspace://"):
            return "/" + path[len("workspace://") :].lstrip("/")
        base = self._root.rstrip("/") if self._root != "/" else ""
        return f"{base}/{path.lstrip('/')}"

    @staticmethod
    def _entry_to_dict(entry: Any) -> dict[str, Any]:
        path = getattr(entry, "path", "") or ""
        size = getattr(entry, "size", 0) or 0
        is_dir = _is_dir_object(entry)
        d: dict[str, Any] = {
            "name": path,
            "size": int(size),
            "type": "directory" if is_dir else "file",
            "object_type": _object_type(entry),
        }
        language = getattr(entry, "language", None)
        if language is not None:
            d["language"] = getattr(language, "value", None) or getattr(language, "name", None) or str(language)
        oid = getattr(entry, "object_id", None)
        if oid is not None:
            d["object_id"] = int(oid)
        mtime = getattr(entry, "modified_at", None)
        if mtime is not None:
            # fsspec convention: seconds since epoch.
            d["mtime"] = float(mtime) / 1000.0
        return d

    # -- AbstractFileSystem API ------------------------------------------

    def ls(self, path: str, detail: bool = True, **kwargs: Any) -> list[Any]:
        resolved = self._resolve(path)
        try:
            entries = list(self._ws.workspace.list(resolved))
        except Exception as exc:
            raise FileNotFoundError(resolved) from exc
        out = [self._entry_to_dict(e) for e in entries]
        return out if detail else [d["name"] for d in out]

    def info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        resolved = self._resolve(path)
        try:
            entry = self._ws.workspace.get_status(resolved)
        except Exception as exc:
            raise FileNotFoundError(resolved) from exc
        return self._entry_to_dict(entry)

    def _open(  # type: ignore[override]
        self,
        path: str,
        mode: str = "rb",
        block_size: int | None = None,  # noqa: ARG002
        autocommit: bool = True,  # noqa: ARG002
        cache_options: Any = None,  # noqa: ARG002
        **kwargs: Any,
    ) -> io.BytesIO:
        if "r" not in mode:
            raise NotImplementedError("WorkspaceFileSystem is read-only")
        resolved = self._resolve(path)
        return io.BytesIO(self._read_bytes(resolved))

    def cat_file(self, path: str, start: int | None = None, end: int | None = None, **kwargs: Any) -> bytes:
        data = self._read_bytes(self._resolve(path))
        if start is None and end is None:
            return data
        return data[(start or 0) : end]

    # -- internals --------------------------------------------------------

    def _read_bytes(self, path: str) -> bytes:
        try:
            from databricks.sdk.service.workspace import ExportFormat
        except Exception:  # pragma: no cover - SDK shape
            ExportFormat = None  # type: ignore[assignment]

        fmt: Any = self._export_format
        if ExportFormat is not None:
            fmt = getattr(ExportFormat, str(self._export_format).upper(), self._export_format)
        try:
            resp = self._ws.workspace.export(path, format=fmt)
        except Exception as exc:
            raise FileNotFoundError(path) from exc

        content = getattr(resp, "content", None) or ""
        if not content:
            return b""
        try:
            return base64.b64decode(content)
        except Exception:
            # Some SDK versions may already return decoded bytes/str.
            if isinstance(content, bytes):
                return content
            return str(content).encode("utf-8", errors="replace")
