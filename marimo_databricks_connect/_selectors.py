"""``mo.ui``-style selector widgets for Databricks resources.

These widgets are lightweight searchable dropdowns whose ``.value`` traitlet is
the selected resource identifier (a catalog name, cluster id, full table name,
etc.).  Because ``anywidget``'s synced traitlets participate in marimo's
reactive graph, a downstream cell that reads ``selector.value`` will re-execute
when the user picks a different option — exactly like ``mo.ui.dropdown``.

Every selector exposes:

* ``value``      — selected identifier (str), the primary reactive output
* ``selected``   — JSON dict with extra metadata about the selection
* ``options``    — JSON list of ``{value, label, sublabel?}`` (synced)
* ``label`` / ``placeholder`` / ``loading`` / ``error_message`` — UI state

Dependent selectors (``schema`` needs a catalog, ``table`` needs catalog +
schema, ``column`` needs a table, ``secret`` needs a scope, ``vector_index``
needs an endpoint) accept either a literal string or another selector.  When
given a selector, they ``observe`` its ``value`` and refetch automatically.
"""

from __future__ import annotations

import json
import logging
import pathlib
from typing import Any, Callable, Iterable

import anywidget
import traitlets

from ._ops_common import enum_val, get_workspace_client

LOGGER = logging.getLogger(__name__)

_ESM_PATH = pathlib.Path(__file__).parent / "_selectors_frontend.js"
_ESM = _ESM_PATH.read_text() if _ESM_PATH.exists() else ""


# --------------------------------------------------------------------------- #
# Base                                                                         #
# --------------------------------------------------------------------------- #


def _resolve_parent(parent: Any) -> str | None:
    """Accept a string, a selector instance, or None and return the current value."""
    if parent is None:
        return None
    if isinstance(parent, _Selector):
        v = parent.value
        return v or None
    if isinstance(parent, str):
        return parent or None
    # last-ditch: object with .value
    v = getattr(parent, "value", None)
    return v if isinstance(v, str) and v else None


class _Selector(anywidget.AnyWidget):
    """Base class for all ``mdc.ui.*`` selectors."""

    _esm = traitlets.Unicode(_ESM).tag(sync=True)
    _css = traitlets.Unicode("").tag(sync=True)

    label = traitlets.Unicode("").tag(sync=True)
    placeholder = traitlets.Unicode("Filter…").tag(sync=True)
    options = traitlets.Unicode("[]").tag(sync=True)
    value = traitlets.Unicode("").tag(sync=True)
    selected = traitlets.Unicode("{}").tag(sync=True)
    loading = traitlets.Bool(False).tag(sync=True)
    error_message = traitlets.Unicode("").tag(sync=True)
    request = traitlets.Unicode("").tag(sync=True)

    # Subclasses override
    resource_label: str = "item"

    def __init__(
        self,
        *,
        workspace_client: Any = None,
        label: str | None = None,
        placeholder: str | None = None,
        default: str | None = None,
        auto_load: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._ws = workspace_client
        if label is not None:
            self.label = label
        elif not self.label:
            self.label = self.resource_label.capitalize()
        if placeholder is not None:
            self.placeholder = placeholder
        self._options_cache: list[dict] = []
        self._parents_observed: list[_Selector] = []
        self.observe(self._handle_request, names=["request"])
        self.observe(self._on_value_change, names=["value"])
        if default:
            self.value = default
        if auto_load:
            self._refresh()

    # -- subclass API ------------------------------------------------------ #

    def _fetch_options(self) -> list[dict]:
        """Return the full list of options. Override in subclasses.

        Each entry must be a dict with at least ``value`` (str). ``label`` and
        ``sublabel`` are shown in the UI.
        """
        return []

    # -- helpers ----------------------------------------------------------- #

    def _get_client(self) -> Any:
        self._ws = get_workspace_client(self._ws)
        return self._ws

    def _observe_parent(self, parent: Any) -> None:
        if isinstance(parent, _Selector):
            parent.observe(lambda *_: self._refresh(), names=["value"])
            self._parents_observed.append(parent)

    def _refresh(self) -> None:
        self.loading = True
        self.error_message = ""
        try:
            opts = list(self._fetch_options() or [])
        except Exception as exc:
            LOGGER.debug("Failed to load %s options", self.resource_label, exc_info=True)
            self.error_message = f"Failed to load {self.resource_label}s: {exc}"
            opts = []
        finally:
            self.loading = False
        # Normalise
        clean: list[dict] = []
        for o in opts:
            if not isinstance(o, dict):
                continue
            v = o.get("value")
            if v is None:
                continue
            entry = {"value": str(v)}
            if "label" in o and o["label"] is not None:
                entry["label"] = str(o["label"])
            if "sublabel" in o and o["sublabel"] is not None:
                entry["sublabel"] = str(o["sublabel"])
            if "meta" in o:
                entry["meta"] = o["meta"]
            clean.append(entry)
        self._options_cache = clean
        # Strip ``meta`` before sending to the frontend (kept for ``selected``).
        self.options = json.dumps([{k: v for k, v in o.items() if k != "meta"} for o in clean])
        # If current value disappeared, clear it
        if self.value and not any(o["value"] == self.value for o in clean):
            self.value = ""
        else:
            # Re-emit selected to refresh metadata
            self._on_value_change({"new": self.value})

    def _handle_request(self, change: Any) -> None:
        raw = change.get("new", "")
        if not raw:
            return
        try:
            req = json.loads(raw)
        except json.JSONDecodeError:
            return
        if req.get("action") == "refresh":
            self._refresh()

    def _on_value_change(self, change: Any) -> None:
        v = change.get("new") if isinstance(change, dict) else change.new
        if not v:
            self.selected = "{}"
            return
        for o in self._options_cache:
            if o.get("value") == v:
                self.selected = json.dumps(o)
                return
        self.selected = json.dumps({"value": v})

    # Public convenience
    def refresh(self) -> None:
        """Re-fetch options from Databricks."""
        self._refresh()

    @property
    def selected_meta(self) -> dict:
        """Parsed ``selected`` traitlet."""
        try:
            return json.loads(self.selected or "{}")
        except json.JSONDecodeError:
            return {}

    def __repr__(self) -> str:
        return f"<{type(self).__name__} value={self.value!r} options={len(self._options_cache)}>"


# --------------------------------------------------------------------------- #
# Helpers                                                                      #
# --------------------------------------------------------------------------- #


def _by_attr(items: Iterable[Any], attr: str) -> list[Any]:
    return sorted(items or [], key=lambda x: (getattr(x, attr, "") or "").lower())


# --------------------------------------------------------------------------- #
# Unity Catalog selectors                                                      #
# --------------------------------------------------------------------------- #


class CatalogSelector(_Selector):
    resource_label = "catalog"

    def _fetch_options(self) -> list[dict]:
        ws = self._get_client()
        out = []
        for c in _by_attr(ws.catalogs.list(), "name"):
            name = getattr(c, "name", None)
            if not name:
                continue
            out.append(
                {
                    "value": name,
                    "label": name,
                    "sublabel": (getattr(c, "comment", None) or enum_val(getattr(c, "catalog_type", None)) or ""),
                }
            )
        return out


class SchemaSelector(_Selector):
    resource_label = "schema"

    def __init__(self, catalog: Any = None, **kwargs: Any) -> None:
        self._catalog = catalog
        super().__init__(**kwargs)
        self._observe_parent(catalog)

    def _fetch_options(self) -> list[dict]:
        cat = _resolve_parent(self._catalog)
        if not cat:
            return []
        ws = self._get_client()
        out = []
        for s in _by_attr(ws.schemas.list(catalog_name=cat), "name"):
            name = getattr(s, "name", None)
            if not name:
                continue
            out.append(
                {
                    "value": f"{cat}.{name}",
                    "label": name,
                    "sublabel": getattr(s, "comment", None) or cat,
                }
            )
        return out


class TableSelector(_Selector):
    resource_label = "table"

    def __init__(self, catalog: Any = None, schema: Any = None, **kwargs: Any) -> None:
        self._catalog = catalog
        self._schema = schema  # may be a SchemaSelector (whose value is "cat.schema") or a string schema name
        super().__init__(**kwargs)
        self._observe_parent(catalog)
        self._observe_parent(schema)

    def _resolve_cat_schema(self) -> tuple[str | None, str | None]:
        s = _resolve_parent(self._schema)
        if s and "." in s:
            cat, sch = s.split(".", 1)
            return cat, sch
        cat = _resolve_parent(self._catalog)
        return cat, s

    def _fetch_options(self) -> list[dict]:
        cat, sch = self._resolve_cat_schema()
        if not cat or not sch:
            return []
        ws = self._get_client()
        out = []
        for t in _by_attr(ws.tables.list(catalog_name=cat, schema_name=sch), "name"):
            name = getattr(t, "name", None)
            if not name:
                continue
            out.append(
                {
                    "value": f"{cat}.{sch}.{name}",
                    "label": name,
                    "sublabel": (enum_val(getattr(t, "table_type", None)) or "") + (f" · {sch}" if sch else ""),
                }
            )
        return out


class ColumnSelector(_Selector):
    resource_label = "column"

    def __init__(self, table: Any = None, **kwargs: Any) -> None:
        self._table = table
        super().__init__(**kwargs)
        self._observe_parent(table)

    def _fetch_options(self) -> list[dict]:
        full = _resolve_parent(self._table)
        if not full or full.count(".") != 2:
            return []
        ws = self._get_client()
        t = ws.tables.get(full)
        out = []
        for c in getattr(t, "columns", None) or []:
            name = getattr(c, "name", None)
            if not name:
                continue
            out.append(
                {
                    "value": name,
                    "label": name,
                    "sublabel": getattr(c, "type_text", None) or getattr(c, "type_name", None) or "",
                }
            )
        return out


# --------------------------------------------------------------------------- #
# Secrets                                                                      #
# --------------------------------------------------------------------------- #


class SecretScopeSelector(_Selector):
    resource_label = "secret scope"

    def _fetch_options(self) -> list[dict]:
        ws = self._get_client()
        out = []
        for s in _by_attr(ws.secrets.list_scopes(), "name"):
            name = getattr(s, "name", None)
            if not name:
                continue
            out.append(
                {
                    "value": name,
                    "label": name,
                    "sublabel": enum_val(getattr(s, "backend_type", None)),
                }
            )
        return out


class SecretSelector(_Selector):
    """Selects a *secret key* within a scope. ``value`` is the key name.

    The full ``scope/key`` reference is available on ``selected_meta['ref']``.
    """

    resource_label = "secret"

    def __init__(self, scope: Any = None, **kwargs: Any) -> None:
        self._scope = scope
        super().__init__(**kwargs)
        self._observe_parent(scope)

    def _fetch_options(self) -> list[dict]:
        scope = _resolve_parent(self._scope)
        if not scope:
            return []
        ws = self._get_client()
        out = []
        for s in _by_attr(ws.secrets.list_secrets(scope), "key"):
            key = getattr(s, "key", None)
            if not key:
                continue
            out.append(
                {
                    "value": key,
                    "label": key,
                    "sublabel": f"{scope}/{key}",
                    "meta": {"scope": scope, "key": key, "ref": f"{{{{secrets/{scope}/{key}}}}}"},
                }
            )
        return out


# --------------------------------------------------------------------------- #
# Compute                                                                      #
# --------------------------------------------------------------------------- #


class ClusterSelector(_Selector):
    resource_label = "cluster"

    def _fetch_options(self) -> list[dict]:
        ws = self._get_client()
        out = []
        clusters = sorted(
            list(ws.clusters.list() or []),
            key=lambda c: (getattr(c, "cluster_name", "") or "").lower(),
        )
        for c in clusters:
            cid = getattr(c, "cluster_id", None)
            if not cid:
                continue
            out.append(
                {
                    "value": cid,
                    "label": getattr(c, "cluster_name", None) or cid,
                    "sublabel": f"{enum_val(getattr(c, 'state', None)) or ''} · {cid}",
                }
            )
        return out


class WarehouseSelector(_Selector):
    resource_label = "warehouse"

    def _fetch_options(self) -> list[dict]:
        ws = self._get_client()
        out = []
        whs = sorted(
            list(ws.warehouses.list() or []),
            key=lambda w: (getattr(w, "name", "") or "").lower(),
        )
        for w in whs:
            wid = getattr(w, "id", None)
            if not wid:
                continue
            out.append(
                {
                    "value": wid,
                    "label": getattr(w, "name", None) or wid,
                    "sublabel": (
                        f"{enum_val(getattr(w, 'state', None)) or ''} · {getattr(w, 'cluster_size', '') or ''}"
                    ),
                }
            )
        return out


# --------------------------------------------------------------------------- #
# Workflows / Pipelines                                                        #
# --------------------------------------------------------------------------- #


class WorkflowSelector(_Selector):
    """Pick a Databricks Job (a.k.a. Workflow). ``value`` is the job_id (str)."""

    resource_label = "workflow"

    def _fetch_options(self) -> list[dict]:
        ws = self._get_client()
        out = []
        jobs = list(ws.jobs.list() or [])

        # job.settings.name on jobs.list, but jobs.list returns BaseJob which
        # also exposes .settings; tolerate both shapes.
        def _name(j: Any) -> str:
            n = getattr(j, "settings", None)
            n = getattr(n, "name", None) if n is not None else None
            return n or getattr(j, "name", None) or ""

        for j in sorted(jobs, key=lambda j: _name(j).lower()):
            jid = getattr(j, "job_id", None)
            if jid is None:
                continue
            out.append(
                {
                    "value": str(jid),
                    "label": _name(j) or str(jid),
                    "sublabel": f"job_id={jid}",
                }
            )
        return out


class PipelineSelector(_Selector):
    """Pick a Lakeflow Declarative Pipeline (DLT). ``value`` is the pipeline_id."""

    resource_label = "pipeline"

    def _fetch_options(self) -> list[dict]:
        ws = self._get_client()
        out = []
        pipes = list(ws.pipelines.list_pipelines() or [])
        for p in sorted(pipes, key=lambda p: (getattr(p, "name", "") or "").lower()):
            pid = getattr(p, "pipeline_id", None)
            if not pid:
                continue
            out.append(
                {
                    "value": pid,
                    "label": getattr(p, "name", None) or pid,
                    "sublabel": f"{enum_val(getattr(p, 'state', None)) or ''} · {pid}",
                }
            )
        return out


# --------------------------------------------------------------------------- #
# Apps / Serving / Vector Search / Genie                                       #
# --------------------------------------------------------------------------- #


class AppSelector(_Selector):
    resource_label = "app"

    def _fetch_options(self) -> list[dict]:
        ws = self._get_client()
        out = []
        for a in sorted(list(ws.apps.list() or []), key=lambda a: (getattr(a, "name", "") or "").lower()):
            name = getattr(a, "name", None)
            if not name:
                continue
            status = getattr(a, "compute_status", None) or getattr(a, "app_status", None)
            out.append(
                {
                    "value": name,
                    "label": name,
                    "sublabel": (enum_val(getattr(status, "state", None)) if status else None)
                    or getattr(a, "url", None)
                    or "",
                }
            )
        return out


class ServingEndpointSelector(_Selector):
    resource_label = "serving endpoint"

    def _fetch_options(self) -> list[dict]:
        ws = self._get_client()
        out = []
        for e in sorted(list(ws.serving_endpoints.list() or []), key=lambda e: (getattr(e, "name", "") or "").lower()):
            name = getattr(e, "name", None)
            if not name:
                continue
            state = getattr(e, "state", None)
            ready = enum_val(getattr(state, "ready", None)) if state else None
            out.append(
                {
                    "value": name,
                    "label": name,
                    "sublabel": ready or enum_val(getattr(state, "config_update", None) if state else None) or "",
                }
            )
        return out


class VectorSearchEndpointSelector(_Selector):
    """Pick a Vector Search endpoint. ``value`` is the endpoint name."""

    resource_label = "vector search endpoint"

    def _fetch_options(self) -> list[dict]:
        ws = self._get_client()
        out = []
        endpoints = list(ws.vector_search_endpoints.list_endpoints() or [])
        for e in sorted(endpoints, key=lambda e: (getattr(e, "name", "") or "").lower()):
            name = getattr(e, "name", None)
            if not name:
                continue
            state = getattr(e, "endpoint_status", None) or getattr(e, "state", None)
            out.append(
                {
                    "value": name,
                    "label": name,
                    "sublabel": enum_val(getattr(state, "state", None))
                    if state
                    else (enum_val(getattr(e, "endpoint_type", None)) or ""),
                }
            )
        return out


class VectorIndexSelector(_Selector):
    """Pick a Vector Search index. ``value`` is the three-part index name."""

    resource_label = "vector index"

    def __init__(self, endpoint: Any = None, **kwargs: Any) -> None:
        self._endpoint = endpoint
        super().__init__(**kwargs)
        self._observe_parent(endpoint)

    def _fetch_options(self) -> list[dict]:
        ws = self._get_client()
        ep = _resolve_parent(self._endpoint)
        out = []
        # The SDK returns an iterable of VectorIndex objects.
        try:
            if ep:
                indexes = list(ws.vector_search_indexes.list_indexes(endpoint_name=ep) or [])
            else:
                # Without an endpoint we still try: some SDK versions accept None.
                indexes = list(ws.vector_search_indexes.list_indexes() or [])
        except TypeError:
            indexes = list(ws.vector_search_indexes.list_indexes() or [])
        for i in sorted(indexes, key=lambda i: (getattr(i, "name", "") or "").lower()):
            name = getattr(i, "name", None)
            if not name:
                continue
            out.append(
                {
                    "value": name,
                    "label": name.split(".")[-1],
                    "sublabel": f"{enum_val(getattr(i, 'index_type', None)) or ''}" + (f" · {ep}" if ep else ""),
                }
            )
        return out


class GenieSpaceSelector(_Selector):
    resource_label = "genie space"

    def _fetch_options(self) -> list[dict]:
        ws = self._get_client()
        try:
            spaces = list(ws.genie.list_spaces() or [])
        except AttributeError as exc:
            raise RuntimeError(
                "This databricks-sdk version does not expose ws.genie.list_spaces(); "
                "upgrade databricks-sdk or pass the space_id directly."
            ) from exc
        # The SDK may return a Pageable wrapper exposing `.spaces` on each page.
        flat: list[Any] = []
        for s in spaces:
            inner = getattr(s, "spaces", None)
            if inner is not None:
                flat.extend(inner)
            else:
                flat.append(s)
        out = []
        for s in sorted(flat, key=lambda s: (getattr(s, "title", "") or getattr(s, "name", "") or "").lower()):
            sid = getattr(s, "space_id", None) or getattr(s, "id", None)
            if not sid:
                continue
            out.append(
                {
                    "value": sid,
                    "label": getattr(s, "title", None) or getattr(s, "name", None) or sid,
                    "sublabel": getattr(s, "description", None) or sid,
                }
            )
        return out


# --------------------------------------------------------------------------- #
# Principals                                                                   #
# --------------------------------------------------------------------------- #


class PrincipalSelector(_Selector):
    """Pick a workspace user, service principal, or group.

    ``value`` is the principal's identifying string:

    * users → ``userName`` (e.g. an email)
    * service principals → ``applicationId``
    * groups → ``displayName``

    The ``selected_meta`` dict carries ``{kind, id, display_name}``.
    """

    resource_label = "principal"

    def __init__(
        self, kinds: tuple[str, ...] | list[str] = ("user", "service_principal", "group"), **kwargs: Any
    ) -> None:
        self._kinds = tuple(kinds)
        super().__init__(**kwargs)

    def _fetch_options(self) -> list[dict]:
        ws = self._get_client()
        out: list[dict] = []
        if "user" in self._kinds:
            for u in list(ws.users.list() or []):
                un = getattr(u, "user_name", None)
                if not un:
                    continue
                out.append(
                    {
                        "value": un,
                        "label": getattr(u, "display_name", None) or un,
                        "sublabel": f"user · {un}",
                        "meta": {
                            "kind": "user",
                            "id": getattr(u, "id", None),
                            "display_name": getattr(u, "display_name", None),
                        },
                    }
                )
        if "service_principal" in self._kinds:
            for sp in list(ws.service_principals.list() or []):
                appid = getattr(sp, "application_id", None)
                if not appid:
                    continue
                out.append(
                    {
                        "value": appid,
                        "label": getattr(sp, "display_name", None) or appid,
                        "sublabel": f"service principal · {appid}",
                        "meta": {
                            "kind": "service_principal",
                            "id": getattr(sp, "id", None),
                            "display_name": getattr(sp, "display_name", None),
                        },
                    }
                )
        if "group" in self._kinds:
            for g in list(ws.groups.list() or []):
                dn = getattr(g, "display_name", None)
                if not dn:
                    continue
                out.append(
                    {
                        "value": dn,
                        "label": dn,
                        "sublabel": "group",
                        "meta": {"kind": "group", "id": getattr(g, "id", None), "display_name": dn},
                    }
                )
        out.sort(key=lambda o: o["label"].lower())
        return out


__all__ = [
    "_Selector",
    "CatalogSelector",
    "SchemaSelector",
    "TableSelector",
    "ColumnSelector",
    "SecretScopeSelector",
    "SecretSelector",
    "ClusterSelector",
    "WarehouseSelector",
    "WorkflowSelector",
    "PipelineSelector",
    "AppSelector",
    "ServingEndpointSelector",
    "VectorSearchEndpointSelector",
    "VectorIndexSelector",
    "GenieSpaceSelector",
    "PrincipalSelector",
]
