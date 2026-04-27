"""Catalog/schema visibility filter for the marimo data sources panel.

This module manages an *allow / deny list* applied when our SQL engine reports
catalogs and schemas to marimo. It only affects what shows up in the data
sources panel — every catalog and schema the workspace has access to remains
queryable via ``mo.sql(..., engine=spark)`` and ``spark.sql(...)``.

Pattern syntax
--------------

Each pattern is either:

* ``"<catalog_glob>"``        — catalog-only, e.g. ``"main"`` or ``"dev_*"``
* ``"<catalog_glob>.<schema_glob>"`` — narrow to specific schemas inside
  matching catalog(s), e.g. ``"main.bronze_*"`` or ``"*_dev.bronze_*"``

Globs use ``fnmatch`` semantics (``*``, ``?``, ``[abc]``). Matching is
case-sensitive (Unity Catalog identifiers are lowercased on store).

Precedence
----------

Excludes always win over includes. Defaults (in order of precedence):

1. ``MARIMO_DBC_SHOW_ALL_CATALOGS=1`` env var → show every catalog.
2. ``MARIMO_DBC_INCLUDE_CATALOGS`` / ``MARIMO_DBC_EXCLUDE_CATALOGS`` env vars
   (comma-separated patterns).
3. ``[tool.marimo_databricks_connect]`` table in the nearest ``pyproject.toml``::

       [tool.marimo_databricks_connect]
       include_catalogs = ["main", "dev_*"]
       exclude_catalogs = ["system", "__databricks_internal"]
       show_all_catalogs = false

4. Otherwise the panel shows only the **current catalog** (``SELECT
   current_catalog()``).
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from fnmatch import fnmatchcase
from pathlib import Path
from typing import Iterable

LOGGER = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Pattern                                                                     #
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class _Pattern:
    """A single catalog (and optionally schema) glob pattern."""

    catalog: str
    schema: str | None  # ``None`` => catalog-wide pattern (all schemas)

    @classmethod
    def parse(cls, raw: str) -> _Pattern:
        raw = raw.strip()
        if not raw:
            raise ValueError("empty pattern")
        if "." in raw:
            cat, _, sch = raw.partition(".")
            cat, sch = cat.strip(), sch.strip()
            if not cat or not sch:
                raise ValueError(f"invalid pattern {raw!r}: both sides of '.' must be non-empty")
            return cls(catalog=cat, schema=sch)
        return cls(catalog=raw, schema=None)

    def matches_catalog(self, catalog: str) -> bool:
        return fnmatchcase(catalog, self.catalog)

    def matches_schema(self, catalog: str, schema: str) -> bool:
        if not fnmatchcase(catalog, self.catalog):
            return False
        if self.schema is None:
            return True
        return fnmatchcase(schema, self.schema)


# --------------------------------------------------------------------------- #
# CatalogFilter                                                               #
# --------------------------------------------------------------------------- #


@dataclass
class CatalogFilter:
    """Mutable include/exclude list applied to catalog + schema discovery."""

    includes: list[_Pattern] = field(default_factory=list)
    excludes: list[_Pattern] = field(default_factory=list)
    show_all: bool = False

    # -- mutation --------------------------------------------------------- #

    def add_includes(self, patterns: Iterable[str]) -> None:
        for raw in patterns:
            p = _Pattern.parse(raw)
            if p not in self.includes:
                self.includes.append(p)

    def add_excludes(self, patterns: Iterable[str]) -> None:
        for raw in patterns:
            p = _Pattern.parse(raw)
            if p not in self.excludes:
                self.excludes.append(p)

    def reset(self) -> None:
        self.includes.clear()
        self.excludes.clear()
        self.show_all = False

    # -- application ------------------------------------------------------ #

    def filter_catalogs(
        self,
        all_catalogs: list[str],
        default_catalog: str | None,
    ) -> list[str]:
        """Return the subset of ``all_catalogs`` to surface in the panel.

        ``default_catalog`` is used only when no includes / show-all are set.
        """
        if self.show_all:
            candidates = list(all_catalogs)
        elif self.includes:
            candidates = [c for c in all_catalogs if any(p.matches_catalog(c) for p in self.includes)]
        else:
            candidates = [default_catalog] if default_catalog else []

        # Only catalog-wide excludes (no schema part) drop a catalog from the
        # listing entirely — schema-scoped excludes don't hide the parent.
        cat_wide_excludes = [p for p in self.excludes if p.schema is None]
        return [c for c in candidates if not any(p.matches_catalog(c) for p in cat_wide_excludes)]

    def filter_schemas(self, catalog: str, all_schemas: list[str]) -> list[str]:
        """Return the subset of ``all_schemas`` to surface inside ``catalog``."""
        if self.show_all or not self.includes:
            included = list(all_schemas)
        else:
            cat_wide_includes = [p for p in self.includes if p.schema is None and p.matches_catalog(catalog)]
            if cat_wide_includes:
                included = list(all_schemas)
            else:
                included = [
                    s
                    for s in all_schemas
                    if any(p.matches_schema(catalog, s) for p in self.includes if p.schema is not None)
                ]

        return [s for s in included if not any(p.matches_schema(catalog, s) for p in self.excludes)]


# --------------------------------------------------------------------------- #
# Module-level singleton + env / pyproject loading                            #
# --------------------------------------------------------------------------- #

_filter = CatalogFilter()


def _split_env_list(value: str | None) -> list[str]:
    if not value:
        return []
    return [p for p in (s.strip() for s in value.split(",")) if p]


def _load_pyproject_defaults(start: Path | None = None) -> dict[str, object]:
    """Walk up from ``start`` (default: cwd) looking for ``pyproject.toml``."""
    try:
        import tomllib  # Python 3.11+
    except ImportError:  # pragma: no cover
        return {}

    cur = (start or Path.cwd()).resolve()
    for candidate in [cur, *cur.parents]:
        path = candidate / "pyproject.toml"
        if path.is_file():
            try:
                with path.open("rb") as fh:
                    data = tomllib.load(fh)
            except Exception:
                LOGGER.debug("failed to parse %s", path, exc_info=True)
                return {}
            return data.get("tool", {}).get("marimo_databricks_connect", {}) or {}
    return {}


def _load_initial_defaults() -> None:
    """Populate the singleton from pyproject + env (env wins)."""
    cfg = _load_pyproject_defaults()
    if isinstance(cfg.get("include_catalogs"), list):
        _filter.add_includes(str(p) for p in cfg["include_catalogs"])
    if isinstance(cfg.get("exclude_catalogs"), list):
        _filter.add_excludes(str(p) for p in cfg["exclude_catalogs"])
    if cfg.get("show_all_catalogs") is True:
        _filter.show_all = True

    # Env overrides
    _filter.add_includes(_split_env_list(os.environ.get("MARIMO_DBC_INCLUDE_CATALOGS")))
    _filter.add_excludes(_split_env_list(os.environ.get("MARIMO_DBC_EXCLUDE_CATALOGS")))
    if os.environ.get("MARIMO_DBC_SHOW_ALL_CATALOGS", "").lower() in ("1", "true", "yes"):
        _filter.show_all = True


_load_initial_defaults()
