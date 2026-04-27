"""fsspec filesystem backed by Databricks ``dbutils.fs``.

Marimo's storage browser auto-detects any ``fsspec.AbstractFileSystem`` instance
in the notebook globals and renders it as a browseable tree. By exposing this
class as a notebook variable (e.g. ``dbfs`` or one returned from
``external_location()``) we get UC-aware browsing for free.

All path access flows through ``dbutils.fs`` so it honours Unity Catalog
permissions: ``/Volumes/<cat>/<schema>/<vol>/...`` for managed/external
volumes, and ``abfss://...`` for UC external locations the principal can read.
"""

from __future__ import annotations

import io
import logging
from typing import Any

from fsspec.spec import AbstractFileSystem  # type: ignore[import-untyped]

LOGGER = logging.getLogger(__name__)


class DbutilsFileSystem(AbstractFileSystem):
    """Read-only fsspec view over ``dbutils.fs``.

    Args:
        dbutils: A ``DBUtils`` instance bound to an active Spark session.
        spark: Optional Spark session, used to read file content via the
            ``binaryFile`` source. Required for ``open()`` / ``cat_file()``.
        root: Default root for relative paths. Absolute paths
            (``/Volumes/...``, ``abfss://...``, ``dbfs:/...``) are passed
            through unchanged.
    """

    protocol = ("dbfs", "abfss")
    sep = "/"

    def __init__(
        self,
        dbutils: Any,
        spark: Any = None,
        root: str = "/Volumes",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._dbutils = dbutils
        self._spark = spark
        self._root = root.rstrip("/") or "/"

    # ``root_marker`` is read by marimo's FsspecFilesystem adapter to populate
    # the StorageNamespace's ``root_path``.
    @property
    def root_marker(self) -> str:  # type: ignore[override]
        return self._root

    # -- path handling ----------------------------------------------------

    def _resolve(self, path: str) -> str:
        if not path or path in (".", "/"):
            return self._root
        if "://" in path or path.startswith(("/", "dbfs:")):
            return path
        return f"{self._root.rstrip('/')}/{path.lstrip('/')}"

    @staticmethod
    def _entry_to_dict(entry: Any) -> dict[str, Any]:
        # ``dbutils.fs.ls`` returns FileInfo objects with ``.path``, ``.name``,
        # ``.size``, and (newer runtimes) ``.modificationTime``.
        path = getattr(entry, "path", "")
        name = getattr(entry, "name", "")
        size = getattr(entry, "size", 0) or 0
        is_dir = bool(name) and name.endswith("/")
        mtime = getattr(entry, "modificationTime", None)
        d: dict[str, Any] = {
            "name": path.rstrip("/") if is_dir else path,
            "size": int(size),
            "type": "directory" if is_dir else "file",
        }
        if mtime is not None:
            # fsspec convention: seconds since epoch.
            d["mtime"] = float(mtime) / 1000.0
        return d

    # -- AbstractFileSystem API ------------------------------------------

    def ls(self, path: str, detail: bool = True, **kwargs: Any) -> list[Any]:
        resolved = self._resolve(path)
        if self._needs_spark_listing(resolved):
            out = self._spark_ls(resolved)
        else:
            try:
                entries = self._dbutils.fs.ls(resolved)
            except Exception as exc:
                raise FileNotFoundError(resolved) from exc
            out = [self._entry_to_dict(e) for e in entries]
        return out if detail else [d["name"] for d in out]

    def info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        resolved = self._resolve(path)

        # Try listing the parent and locating ``resolved``. Works for both
        # dbutils-backed paths and Spark-backed (abfss://, s3://, ...) paths
        # because ``ls`` itself dispatches on scheme.
        parent, _, leaf = resolved.rpartition("/")
        # Preserve the scheme separator: "abfss://host/foo".rpartition("/") gives
        # ("abfss:/", "", "host") on the *first* call, which is wrong. Detect that.
        if parent.endswith(":") or not leaf:
            parent = ""
        if parent and leaf:
            try:
                for entry in self.ls(parent, detail=True):
                    if entry["name"].rstrip("/") == resolved.rstrip("/"):
                        return entry
            except Exception:
                LOGGER.debug("parent ls failed for %s", resolved, exc_info=True)

        # Fall back: try listing ``resolved`` itself — only meaningful for
        # directories.
        try:
            self.ls(resolved, detail=False)
        except Exception as exc:
            raise FileNotFoundError(resolved) from exc
        return {"name": resolved, "size": 0, "type": "directory"}

    @staticmethod
    def _needs_spark_listing(path: str) -> bool:
        """True for URIs that ``dbutils.fs`` (SDK build) refuses to handle.

        The SDK-backed DBUtils only accepts ``file:``, ``dbfs:`` and unscheme'd
        paths (``/Volumes/...``, ``/tmp/...``). Cloud schemes like ``abfss``,
        ``s3``, ``gs`` must be listed via Spark instead.
        """
        if "://" not in path:
            return False
        return path.split("://", 1)[0].lower() not in ("file", "dbfs")

    def _spark_ls(self, path: str, max_files: int = 5000) -> list[dict[str, Any]]:
        """List one directory level at an arbitrary URI using Spark.

        Uses the ``binaryFile`` reader with ``recursiveFileLookup=true`` to
        enumerate files, then groups by the first path segment under ``path``
        so we can surface both files and intermediate subdirectories. Capped at
        ``max_files`` rows to bound cost on large external locations.
        """
        if self._spark is None:
            raise FileNotFoundError(f"No Spark session available to list {path}")

        base = path.rstrip("/") + "/"
        try:
            df = (
                self._spark.read.format("binaryFile")
                .option("recursiveFileLookup", "true")
                .option("pathGlobFilter", "*")
                .load(path)
                .select("path", "length", "modificationTime")
                .limit(max_files)
            )
            rows = df.collect()
        except Exception as exc:
            raise FileNotFoundError(path) from exc

        entries: dict[str, dict[str, Any]] = {}
        for row in rows:
            full = row["path"] or ""
            if not full.startswith(base):
                continue
            rest = full[len(base) :]
            head, sep, _ = rest.partition("/")
            if not head:
                continue
            name = base + head
            if sep:
                # Subdirectory at this level — only record once.
                entries.setdefault(name, {"name": name, "size": 0, "type": "directory"})
            else:
                mtime = row["modificationTime"]
                entries[name] = {
                    "name": name,
                    "size": int(row["length"] or 0),
                    "type": "file",
                    "mtime": mtime.timestamp() if mtime is not None else None,
                }
        return list(entries.values())

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
            raise NotImplementedError("DbutilsFileSystem is read-only")
        resolved = self._resolve(path)
        return io.BytesIO(self._read_bytes(resolved))

    def cat_file(self, path: str, start: int | None = None, end: int | None = None, **kwargs: Any) -> bytes:
        data = self._read_bytes(self._resolve(path))
        if start is None and end is None:
            return data
        return data[(start or 0) : end]

    # -- internals --------------------------------------------------------

    def _read_bytes(self, path: str) -> bytes:
        if self._spark is not None:
            try:
                df = self._spark.read.format("binaryFile").load(path)
                rows = df.select("content").limit(1).collect()
                if rows:
                    blob = rows[0][0]
                    return bytes(blob) if blob is not None else b""
            except Exception:
                LOGGER.debug("binaryFile read failed for %s", path, exc_info=True)

        # Fallback: dbutils.fs.head reads up to ~maxBytes; bytes-safe on recent
        # runtimes, otherwise returns str.
        max_bytes = 100 * 1024 * 1024
        data = self._dbutils.fs.head(path, max_bytes)
        if isinstance(data, str):
            return data.encode("utf-8", errors="replace")
        return bytes(data)
