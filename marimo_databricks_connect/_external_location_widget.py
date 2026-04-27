"""Operational widget for a single Databricks external location.

Accepts either a UC external location *name* (resolved via ``DESCRIBE
EXTERNAL LOCATION``) or a raw cloud path (``abfss://``, ``s3://``,
``/Volumes/...``).  File browsing mirrors the storage browser logic in
``_fs.py`` — ``dbutils.fs`` for local/dbfs paths, Spark ``binaryFile``
for cloud URIs.

Usage::

    from marimo_databricks_connect import external_location_widget

    # By UC name — resolved to its underlying URL:
    widget = external_location_widget("finops_landing")

    # By raw path:
    widget = external_location_widget("abfss://container@account.dfs.core.windows.net/data")
"""

from __future__ import annotations

import json
import logging
import pathlib
from typing import Any

import anywidget
import traitlets

from ._ops_common import enum_val, ms_to_iso

LOGGER = logging.getLogger(__name__)

_ESM_PATH = pathlib.Path(__file__).parent / "_external_location_widget_frontend.js"


# --------------------------------------------------------------------------- #
# Helpers                                                                      #
# --------------------------------------------------------------------------- #


def _is_raw_path(name_or_path: str) -> bool:
    """True when the string is a cloud/volume path rather than a UC name."""
    return "://" in name_or_path or name_or_path.startswith("/")


def _needs_spark_listing(path: str) -> bool:
    """True for URIs that ``dbutils.fs`` cannot list.

    The SDK-backed DBUtils only accepts ``file:``, ``dbfs:`` and unschemed
    paths.  Cloud schemes (``abfss``, ``s3``, ``gs``, …) must go via Spark.
    """
    if "://" not in path:
        return False
    return path.split("://", 1)[0].lower() not in ("file", "dbfs")


def _spark_ls(spark: Any, path: str, max_files: int = 5000) -> list[dict]:
    """List one directory level at a cloud URI using Spark ``binaryFile``.

    Groups results by the first path segment under *path* so that both
    immediate files and subdirectories are surfaced.
    """
    base = path.rstrip("/") + "/"
    df = (
        spark.read.format("binaryFile")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*")
        .load(path)
        .select("path", "length", "modificationTime")
        .limit(max_files)
    )
    rows = df.collect()

    entries: dict[str, dict] = {}
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
            # Subdirectory — record once
            entries.setdefault(
                name,
                {
                    "name": head + "/",
                    "path": name,
                    "size": 0,
                    "is_dir": True,
                },
            )
        else:
            entries[name] = {
                "name": head,
                "path": name,
                "size": int(row["length"] or 0),
                "is_dir": False,
            }
    return list(entries.values())


def _dbutils_ls(dbutils: Any, path: str) -> list[dict]:
    """List a directory via ``dbutils.fs.ls`` (for local/dbfs/Volumes paths)."""
    items = []
    for f in dbutils.fs.ls(path):
        fname = getattr(f, "name", str(f))
        items.append(
            {
                "name": fname,
                "path": getattr(f, "path", ""),
                "size": getattr(f, "size", 0) or 0,
                "is_dir": bool(fname and fname.endswith("/")),
            }
        )
    return items


# --------------------------------------------------------------------------- #
# Serializers                                                                  #
# --------------------------------------------------------------------------- #


def _serialize_location(loc: Any) -> dict:
    return {
        "name": getattr(loc, "name", None),
        "url": getattr(loc, "url", None),
        "credential_name": getattr(loc, "credential_name", None),
        "comment": getattr(loc, "comment", None),
        "owner": getattr(loc, "owner", None),
        "read_only": getattr(loc, "read_only", None),
        "created_at": ms_to_iso(getattr(loc, "created_at", None)),
        "created_by": getattr(loc, "created_by", None),
        "updated_at": ms_to_iso(getattr(loc, "updated_at", None)),
        "updated_by": getattr(loc, "updated_by", None),
        "isolation_mode": enum_val(getattr(loc, "isolation_mode", None)),
        "fallback": getattr(loc, "fallback", None),
    }


def _serialize_permissions(resp: Any) -> list[dict]:
    result = []
    for pa in getattr(resp, "privilege_assignments", None) or []:
        privs = [
            {
                "privilege": enum_val(getattr(p, "privilege", None)),
                "inherited_from_name": getattr(p, "inherited_from_name", None),
            }
            for p in getattr(pa, "privileges", None) or []
        ]
        result.append({"principal": getattr(pa, "principal", None), "privileges": privs})
    return result


# --------------------------------------------------------------------------- #
# Widget                                                                       #
# --------------------------------------------------------------------------- #


class ExternalLocationWidget(anywidget.AnyWidget):
    """Operational widget for a single external location.

    Args:
        location_name: Either a UC external location name (e.g.
            ``"finops_landing"``) or a raw path (``abfss://…``,
            ``s3://…``, ``/Volumes/…``).  Raw paths skip the
            ``DESCRIBE EXTERNAL LOCATION`` lookup and use the path
            directly for browsing.
        workspace_client: Optional ``WorkspaceClient``.
    """

    _esm = traitlets.Unicode().tag(sync=True)
    _css = traitlets.Unicode("").tag(sync=True)

    location_data = traitlets.Unicode("{}").tag(sync=True)
    contents_data = traitlets.Unicode("{}").tag(sync=True)
    permissions_data = traitlets.Unicode("{}").tag(sync=True)
    validation_data = traitlets.Unicode("{}").tag(sync=True)
    loading = traitlets.Bool(False).tag(sync=True)
    error_message = traitlets.Unicode("").tag(sync=True)
    request = traitlets.Unicode("").tag(sync=True)

    def __init__(
        self,
        location_name: str,
        workspace_client: Any = None,
        **kwargs: Any,
    ) -> None:
        esm_content = _ESM_PATH.read_text()
        super().__init__(_esm=esm_content, **kwargs)
        self._ws = workspace_client
        self._spark = None
        self._dbutils = None
        self._is_raw = _is_raw_path(location_name)
        # For raw paths, we store the path directly; for names we'll
        # resolve later.
        self._location_name = location_name if not self._is_raw else None
        self._root_url = location_name if self._is_raw else None
        self.observe(self._handle_request, names=["request"])
        self._load_location()

    # -- client helpers ---------------------------------------------------

    def _get_client(self) -> Any:
        if self._ws is not None:
            return self._ws
        from databricks.sdk import WorkspaceClient

        self._ws = WorkspaceClient()
        return self._ws

    def _get_spark(self) -> Any:
        if self._spark is not None:
            return self._spark
        try:
            from databricks.connect import DatabricksSession

            self._spark = DatabricksSession.builder.serverless().getOrCreate()
        except Exception:
            LOGGER.debug("Could not create DatabricksSession", exc_info=True)
        return self._spark

    def _get_dbutils(self) -> Any:
        if self._dbutils is not None:
            return self._dbutils
        spark = self._get_spark()
        if spark is None:
            return None
        try:
            from pyspark.dbutils import DBUtils  # type: ignore[import-untyped]

            self._dbutils = DBUtils(spark)
        except Exception:
            LOGGER.debug("Could not create DBUtils", exc_info=True)
        return self._dbutils

    # -- request routing --------------------------------------------------

    def _handle_request(self, change: Any) -> None:
        raw = change.get("new", "")
        if not raw:
            return
        try:
            req = json.loads(raw)
        except json.JSONDecodeError:
            return
        action = req.get("action")
        if action == "refresh":
            self._load_location()
        elif action == "browse":
            self._browse(req.get("path"))
        elif action == "get_permissions":
            self._load_permissions()
        elif action == "validate":
            self._validate()

    # -- data loading -----------------------------------------------------

    def _load_location(self) -> None:
        self.loading = True
        self.error_message = ""
        try:
            if self._is_raw:
                # No UC metadata — just expose the path
                self.location_data = json.dumps(
                    {
                        "name": self._root_url,
                        "url": self._root_url,
                        "credential_name": None,
                        "comment": None,
                        "owner": None,
                        "read_only": None,
                        "created_at": None,
                        "created_by": None,
                        "updated_at": None,
                        "updated_by": None,
                        "isolation_mode": None,
                        "fallback": None,
                        "is_raw_path": True,
                    }
                )
            else:
                ws = self._get_client()
                loc = ws.external_locations.get(self._location_name)
                data = _serialize_location(loc)
                data["is_raw_path"] = False
                self._root_url = data.get("url")
                self.location_data = json.dumps(data)
        except Exception as exc:
            LOGGER.debug("Failed to get external location %s", self._location_name or self._root_url, exc_info=True)
            self.error_message = f"Failed to get external location: {exc}"
        finally:
            self.loading = False

    def _browse(self, path: str | None = None) -> None:
        """List directory contents, dispatching to Spark or dbutils as needed."""
        self.loading = True
        self.error_message = ""
        url = path or self._root_url
        if not url:
            self.error_message = "No URL available to browse."
            self.loading = False
            return
        try:
            if _needs_spark_listing(url):
                spark = self._get_spark()
                if spark is None:
                    self.error_message = (
                        "Spark session required to browse cloud paths. "
                        "Install databricks-connect and ensure auth is configured."
                    )
                    self.loading = False
                    return
                items = _spark_ls(spark, url)
            else:
                dbutils = self._get_dbutils()
                if dbutils is None:
                    self.error_message = "DBUtils not available for browsing."
                    self.loading = False
                    return
                items = _dbutils_ls(dbutils, url)
            self.contents_data = json.dumps({"url": url, "items": items})
        except Exception as exc:
            LOGGER.debug("Failed to browse %s", url, exc_info=True)
            self.error_message = f"Failed to browse: {exc}"
        finally:
            self.loading = False

    def _load_permissions(self) -> None:
        if self._is_raw:
            self.error_message = "Permissions not available for raw paths — use a UC external location name."
            return
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            resp = ws.grants.get_effective(
                securable_type="EXTERNAL_LOCATION",
                full_name=self._location_name,
            )
            self.permissions_data = json.dumps({"permissions": _serialize_permissions(resp)})
        except Exception as exc:
            LOGGER.debug("Failed to get permissions", exc_info=True)
            self.error_message = f"Failed to get permissions: {exc}"
        finally:
            self.loading = False

    def _validate(self) -> None:
        if self._is_raw:
            self.error_message = "Validation not available for raw paths — use a UC external location name."
            return
        self.loading = True
        self.error_message = ""
        try:
            ws = self._get_client()
            resp = ws.external_locations.validate(external_location_name=self._location_name)
            checks = []
            if hasattr(resp, "results"):
                for r in resp.results or []:
                    checks.append(
                        {
                            "operation": enum_val(getattr(r, "operation", None)),
                            "result": enum_val(getattr(r, "result", None)),
                            "message": getattr(r, "message", None),
                        }
                    )
            self.validation_data = json.dumps({"checks": checks})
        except Exception as exc:
            LOGGER.debug("Failed to validate %s", self._location_name, exc_info=True)
            self.error_message = f"Failed to validate: {exc}"
        finally:
            self.loading = False
