"""Cross-cutting permissions / ACL explorer widget.

Two modes (tabs):

* **By Principal** — pick a user, group, or service principal and scan the
  workspace + Unity Catalog for everything they have permissions on.  Optionally
  clone those grants to another principal, either applied immediately or
  emitted as a Python script for review.

* **By Securable** — pick any securable (cluster, job, warehouse, app, secret
  scope, UC catalog/schema/table/volume/external-location/...) and see every
  principal that has permissions on it.

Usage::

    from marimo_databricks_connect import acl_widget
    widget = acl_widget()
    widget
"""

from __future__ import annotations

import json
import logging
import pathlib
import time
from typing import Any, Callable, Iterable

import anywidget
import traitlets

from ._ops_common import enum_val, get_workspace_client

LOGGER = logging.getLogger(__name__)

_ESM_PATH = pathlib.Path(__file__).parent / "_acl_widget_frontend.js"


# --------------------------------------------------------------------------- #
# Category registry                                                           #
# --------------------------------------------------------------------------- #
#
# Every securable type we know about is a row in CATEGORIES.  Each entry
# describes:
#
#   * label            – display name shown in the UI
#   * kind             – one of "workspace", "uc", "secret_scope", "apps".
#                        Determines which API family to use.
#   * list             – callable(ws) returning an iterable of items
#   * id               – callable(item) returning the identifier used by the
#                        permissions API for this object type
#   * name             – callable(item) returning a human-readable name
#   * request_object_type – workspace permissions API object type (kind == "workspace")
#   * securable_type   – UC SecurableType enum value (kind == "uc")
#
# To keep "scan everything" reasonable we group categories into:
#   - default_scan: cheap / 1-list-call categories
#   - deep_scan:    requires drilling into catalogs/schemas
#

# --- accessors that tolerate missing optional SDK methods -------------------


def _safe_list(fn: Callable[[], Iterable[Any]]) -> list[Any]:
    try:
        return list(fn())
    except Exception as exc:  # pragma: no cover - depends on workspace
        LOGGER.debug("list call failed: %s", exc, exc_info=True)
        return []


def _list_clusters(ws: Any) -> Iterable[Any]:
    return ws.clusters.list()


def _list_cluster_policies(ws: Any) -> Iterable[Any]:
    return ws.cluster_policies.list()


def _list_instance_pools(ws: Any) -> Iterable[Any]:
    return ws.instance_pools.list()


def _list_jobs(ws: Any) -> Iterable[Any]:
    return ws.jobs.list()


def _list_pipelines(ws: Any) -> Iterable[Any]:
    return ws.pipelines.list_pipelines()


def _list_warehouses(ws: Any) -> Iterable[Any]:
    return ws.warehouses.list()


def _list_serving_endpoints(ws: Any) -> Iterable[Any]:
    return ws.serving_endpoints.list()


def _list_apps(ws: Any) -> Iterable[Any]:
    return ws.apps.list()


def _list_dashboards(ws: Any) -> Iterable[Any]:
    # Lakeview dashboards
    try:
        return ws.lakeview.list()
    except Exception:
        return []


def _list_queries(ws: Any) -> Iterable[Any]:
    try:
        return ws.queries.list()
    except Exception:
        return []


def _list_alerts(ws: Any) -> Iterable[Any]:
    try:
        return ws.alerts.list()
    except Exception:
        return []


def _list_experiments(ws: Any) -> Iterable[Any]:
    try:
        return ws.experiments.list_experiments()
    except Exception:
        return []


def _list_registered_models_legacy(ws: Any) -> Iterable[Any]:
    """MLflow legacy registered models (workspace-scoped)."""
    try:
        return ws.model_registry.list_models()
    except Exception:
        return []


def _list_secret_scopes(ws: Any) -> Iterable[Any]:
    return ws.secrets.list_scopes()


def _list_uc_catalogs(ws: Any) -> Iterable[Any]:
    return ws.catalogs.list()


def _list_uc_external_locations(ws: Any) -> Iterable[Any]:
    return ws.external_locations.list()


def _list_uc_storage_credentials(ws: Any) -> Iterable[Any]:
    return ws.storage_credentials.list()


def _list_uc_connections(ws: Any) -> Iterable[Any]:
    return ws.connections.list()


def _list_uc_metastores(ws: Any) -> Iterable[Any]:
    try:
        m = ws.metastores.summary()
        return [m] if m else []
    except Exception:
        return []


# --- registry ---------------------------------------------------------------


CATEGORIES: dict[str, dict[str, Any]] = {
    # --- workspace permissions API ---
    "clusters": {
        "label": "Clusters",
        "kind": "workspace",
        "default_scan": True,
        "request_object_type": "clusters",
        "list": _list_clusters,
        "id": lambda x: getattr(x, "cluster_id", None),
        "name": lambda x: getattr(x, "cluster_name", None) or getattr(x, "cluster_id", None),
    },
    "cluster-policies": {
        "label": "Cluster Policies",
        "kind": "workspace",
        "default_scan": True,
        "request_object_type": "cluster-policies",
        "list": _list_cluster_policies,
        "id": lambda x: getattr(x, "policy_id", None),
        "name": lambda x: getattr(x, "name", None),
    },
    "instance-pools": {
        "label": "Instance Pools",
        "kind": "workspace",
        "default_scan": True,
        "request_object_type": "instance-pools",
        "list": _list_instance_pools,
        "id": lambda x: getattr(x, "instance_pool_id", None),
        "name": lambda x: getattr(x, "instance_pool_name", None),
    },
    "jobs": {
        "label": "Jobs / Workflows",
        "kind": "workspace",
        "default_scan": True,
        "request_object_type": "jobs",
        "list": _list_jobs,
        "id": lambda x: getattr(x, "job_id", None),
        "name": lambda x: getattr(getattr(x, "settings", None), "name", None) or str(getattr(x, "job_id", "")),
    },
    "pipelines": {
        "label": "DLT Pipelines",
        "kind": "workspace",
        "default_scan": True,
        "request_object_type": "pipelines",
        "list": _list_pipelines,
        "id": lambda x: getattr(x, "pipeline_id", None),
        "name": lambda x: getattr(x, "name", None),
    },
    "warehouses": {
        "label": "SQL Warehouses",
        "kind": "workspace",
        "default_scan": True,
        "request_object_type": "sql/warehouses",
        "list": _list_warehouses,
        "id": lambda x: getattr(x, "id", None),
        "name": lambda x: getattr(x, "name", None),
    },
    "serving-endpoints": {
        "label": "Model Serving Endpoints",
        "kind": "workspace",
        "default_scan": True,
        "request_object_type": "serving-endpoints",
        "list": _list_serving_endpoints,
        "id": lambda x: getattr(x, "id", None) or getattr(x, "name", None),
        "name": lambda x: getattr(x, "name", None),
    },
    "dashboards": {
        "label": "Lakeview Dashboards",
        "kind": "workspace",
        "default_scan": False,
        "request_object_type": "dashboards",
        "list": _list_dashboards,
        "id": lambda x: getattr(x, "dashboard_id", None),
        "name": lambda x: getattr(x, "display_name", None) or getattr(x, "dashboard_id", None),
    },
    "sql-queries": {
        "label": "SQL Queries",
        "kind": "workspace",
        "default_scan": False,
        "request_object_type": "sql/queries",
        "list": _list_queries,
        "id": lambda x: getattr(x, "id", None),
        "name": lambda x: getattr(x, "display_name", None) or getattr(x, "id", None),
    },
    "sql-alerts": {
        "label": "SQL Alerts",
        "kind": "workspace",
        "default_scan": False,
        "request_object_type": "sql/alerts",
        "list": _list_alerts,
        "id": lambda x: getattr(x, "id", None),
        "name": lambda x: getattr(x, "display_name", None) or getattr(x, "id", None),
    },
    "experiments": {
        "label": "MLflow Experiments",
        "kind": "workspace",
        "default_scan": False,
        "request_object_type": "experiments",
        "list": _list_experiments,
        "id": lambda x: getattr(x, "experiment_id", None),
        "name": lambda x: getattr(x, "name", None),
    },
    "registered-models": {
        "label": "MLflow Registered Models (legacy)",
        "kind": "workspace",
        "default_scan": False,
        "request_object_type": "registered-models",
        "list": _list_registered_models_legacy,
        "id": lambda x: getattr(x, "id", None) or getattr(x, "name", None),
        "name": lambda x: getattr(x, "name", None),
    },
    # --- secret scopes (separate ACL API) ---
    "secret-scopes": {
        "label": "Secret Scopes",
        "kind": "secret_scope",
        "default_scan": True,
        "list": _list_secret_scopes,
        "id": lambda x: getattr(x, "name", None),
        "name": lambda x: getattr(x, "name", None),
    },
    # --- apps (separate API: ws.apps.get_permissions(name)) ---
    "apps": {
        "label": "Databricks Apps",
        "kind": "apps",
        "default_scan": True,
        "list": _list_apps,
        "id": lambda x: getattr(x, "name", None),
        "name": lambda x: getattr(x, "name", None),
    },
    # --- Unity Catalog ---
    "uc-metastore": {
        "label": "UC Metastore",
        "kind": "uc",
        "default_scan": True,
        "securable_type": "METASTORE",
        "list": _list_uc_metastores,
        "id": lambda x: getattr(x, "metastore_id", None),
        "name": lambda x: getattr(x, "name", None) or getattr(x, "metastore_id", None),
    },
    "uc-catalog": {
        "label": "UC Catalogs",
        "kind": "uc",
        "default_scan": True,
        "securable_type": "CATALOG",
        "list": _list_uc_catalogs,
        "id": lambda x: getattr(x, "name", None),
        "name": lambda x: getattr(x, "name", None),
    },
    "uc-external-location": {
        "label": "UC External Locations",
        "kind": "uc",
        "default_scan": True,
        "securable_type": "EXTERNAL_LOCATION",
        "list": _list_uc_external_locations,
        "id": lambda x: getattr(x, "name", None),
        "name": lambda x: getattr(x, "name", None),
    },
    "uc-storage-credential": {
        "label": "UC Storage Credentials",
        "kind": "uc",
        "default_scan": True,
        "securable_type": "STORAGE_CREDENTIAL",
        "list": _list_uc_storage_credentials,
        "id": lambda x: getattr(x, "name", None),
        "name": lambda x: getattr(x, "name", None),
    },
    "uc-connection": {
        "label": "UC Connections",
        "kind": "uc",
        "default_scan": True,
        "securable_type": "CONNECTION",
        "list": _list_uc_connections,
        "id": lambda x: getattr(x, "name", None),
        "name": lambda x: getattr(x, "name", None),
    },
    # Schemas / tables / volumes / functions / models / vector-indexes are
    # enumerated lazily per-catalog (deep scan). Their entries are still listed
    # here so the by-securable picker can target them, but `list` returns
    # nothing on its own.
    "uc-schema": {
        "label": "UC Schemas (deep scan)",
        "kind": "uc",
        "default_scan": False,
        "deep": True,
        "securable_type": "SCHEMA",
        "list": lambda ws: [],
        "id": lambda x: getattr(x, "full_name", None),
        "name": lambda x: getattr(x, "full_name", None),
    },
    "uc-table": {
        "label": "UC Tables/Views (deep scan)",
        "kind": "uc",
        "default_scan": False,
        "deep": True,
        "securable_type": "TABLE",
        "list": lambda ws: [],
        "id": lambda x: getattr(x, "full_name", None),
        "name": lambda x: getattr(x, "full_name", None),
    },
    "uc-volume": {
        "label": "UC Volumes (deep scan)",
        "kind": "uc",
        "default_scan": False,
        "deep": True,
        "securable_type": "VOLUME",
        "list": lambda ws: [],
        "id": lambda x: getattr(x, "full_name", None),
        "name": lambda x: getattr(x, "full_name", None),
    },
    "uc-function": {
        "label": "UC Functions (deep scan)",
        "kind": "uc",
        "default_scan": False,
        "deep": True,
        "securable_type": "FUNCTION",
        "list": lambda ws: [],
        "id": lambda x: getattr(x, "full_name", None),
        "name": lambda x: getattr(x, "full_name", None),
    },
}


# --------------------------------------------------------------------------- #
# Principal / ACL helpers                                                     #
# --------------------------------------------------------------------------- #


def _principal_of(acr: Any) -> tuple[str | None, str | None, str | None]:
    """Return (principal, type, display_name) for a workspace AccessControlResponse."""
    principal = getattr(acr, "user_name", None)
    if principal:
        ptype = "user"
    else:
        principal = getattr(acr, "group_name", None)
        if principal:
            ptype = "group"
        else:
            principal = getattr(acr, "service_principal_name", None)
            ptype = "service_principal" if principal else None
    return principal, ptype, getattr(acr, "display_name", None)


def _principal_matches(target: str, principal: str | None) -> bool:
    if not principal or not target:
        return False
    return principal.strip().lower() == target.strip().lower()


def _serialize_workspace_perms(perms: Any) -> list[dict]:
    """Flatten ObjectPermissions.access_control_list into rows."""
    rows = []
    for entry in getattr(perms, "access_control_list", None) or []:
        principal, ptype, display = _principal_of(entry)
        levels = []
        for p in getattr(entry, "all_permissions", None) or []:
            levels.append(
                {
                    "level": enum_val(getattr(p, "permission_level", None)),
                    "inherited": bool(getattr(p, "inherited", False)),
                    "inherited_from": getattr(p, "inherited_from_object", None),
                }
            )
        rows.append({"principal": principal, "type": ptype, "display_name": display, "permissions": levels})
    return rows


def _serialize_uc_grants(resp: Any) -> list[dict]:
    """Flatten PermissionsList.privilege_assignments into rows."""
    rows = []
    for pa in getattr(resp, "privilege_assignments", None) or []:
        privs = []
        for p in getattr(pa, "privileges", None) or []:
            # PermissionsList.get_effective returns EffectivePrivilege which has inherited_from*
            privs.append(
                {
                    "privilege": enum_val(getattr(p, "privilege", None)) or str(p),
                    "inherited_from_name": getattr(p, "inherited_from_name", None),
                    "inherited_from_type": enum_val(getattr(p, "inherited_from_type", None)),
                }
            )
        rows.append(
            {
                "principal": getattr(pa, "principal", None),
                "type": None,  # UC doesn't distinguish in the response
                "display_name": None,
                "permissions": [
                    {
                        "level": pr["privilege"],
                        "inherited": pr["inherited_from_name"] is not None,
                        "inherited_from": (
                            f"{pr['inherited_from_type']} {pr['inherited_from_name']}"
                            if pr["inherited_from_name"]
                            else None
                        ),
                    }
                    for pr in privs
                ],
            }
        )
    return rows


def _serialize_secret_acls(acls: Any) -> list[dict]:
    rows = []
    for a in acls or []:
        rows.append(
            {
                "principal": getattr(a, "principal", None),
                "type": None,
                "display_name": None,
                "permissions": [
                    {
                        "level": enum_val(getattr(a, "permission", None)),
                        "inherited": False,
                        "inherited_from": None,
                    }
                ],
            }
        )
    return rows


def _fetch_acl(ws: Any, category_id: str, item_id: str) -> tuple[list[dict], str]:
    """Return (rows, error) for the ACL of a single securable."""
    cat = CATEGORIES[category_id]
    kind = cat["kind"]
    try:
        if kind == "workspace":
            perms = ws.permissions.get(cat["request_object_type"], item_id)
            return _serialize_workspace_perms(perms), ""
        if kind == "uc":
            from databricks.sdk.service.catalog import SecurableType

            st = SecurableType[cat["securable_type"]]
            try:
                resp = ws.grants.get_effective(st, item_id)
            except Exception:
                resp = ws.grants.get(st, item_id)
            return _serialize_uc_grants(resp), ""
        if kind == "secret_scope":
            acls = list(ws.secrets.list_acls(item_id))
            return _serialize_secret_acls(acls), ""
        if kind == "apps":
            perms = ws.apps.get_permissions(item_id)
            return _serialize_workspace_perms(perms), ""
    except Exception as exc:
        return [], f"{exc}"
    return [], "Unknown category kind"


# --------------------------------------------------------------------------- #
# Deep UC enumeration                                                         #
# --------------------------------------------------------------------------- #


def _iter_deep_uc(ws: Any, deep: list[str]) -> Iterable[tuple[str, Any]]:
    """Yield (category_id, item) for deep-scan UC securables.

    ``deep`` is a list of category ids among uc-schema / uc-table / uc-volume /
    uc-function. We always enumerate catalogs → schemas first, then optionally
    enumerate the contents of each schema for tables/volumes/functions.
    """
    if not deep:
        return
    want_schemas = "uc-schema" in deep
    want_tables = "uc-table" in deep
    want_volumes = "uc-volume" in deep
    want_funcs = "uc-function" in deep
    for catalog in _safe_list(lambda: ws.catalogs.list()):
        cname = getattr(catalog, "name", None)
        if not cname:
            continue
        for schema in _safe_list(lambda c=cname: ws.schemas.list(catalog_name=c)):
            sname = getattr(schema, "name", "") or ""
            if want_schemas:
                yield "uc-schema", schema
            if want_tables:
                for t in _safe_list(lambda c=cname, s=sname: ws.tables.list(catalog_name=c, schema_name=s)):
                    yield "uc-table", t
            if want_volumes:
                for v in _safe_list(lambda c=cname, s=sname: ws.volumes.list(catalog_name=c, schema_name=s)):
                    yield "uc-volume", v
            if want_funcs:
                for f in _safe_list(lambda c=cname, s=sname: ws.functions.list(catalog_name=c, schema_name=s)):
                    yield "uc-function", f


# --------------------------------------------------------------------------- #
# Clone script generation                                                     #
# --------------------------------------------------------------------------- #


def _principal_kwargs(principal: str, ptype: str | None) -> dict:
    """Return the kwarg dict for AccessControlRequest given a principal + type."""
    if ptype == "user" or (ptype is None and "@" in principal):
        return {"user_name": principal}
    if ptype == "group":
        return {"group_name": principal}
    # default to service_principal for non-email, non-group
    return {"service_principal_name": principal}


def _quote(s: Any) -> str:
    return json.dumps(s)


def _generate_clone_script(rows: list[dict], from_principal: str, to_principal: str, to_type: str | None) -> str:
    """Generate a Python script that re-applies all `rows` to `to_principal`."""
    lines = [
        "# Generated by marimo_databricks_connect.acl_widget",
        f"# Clone permissions from {from_principal!r} to {to_principal!r}",
        "from databricks.sdk import WorkspaceClient",
        "from databricks.sdk.service.iam import AccessControlRequest, PermissionLevel",
        "from databricks.sdk.service.catalog import (",
        "    PermissionsChange, SecurableType, Privilege,",
        ")",
        "from databricks.sdk.service.workspace import AclPermission",
        "",
        "ws = WorkspaceClient()",
        "",
    ]
    pkw = _principal_kwargs(to_principal, to_type)
    pkw_repr = ", ".join(f"{k}={_quote(v)}" for k, v in pkw.items())

    for r in rows:
        cat = CATEGORIES.get(r["category_id"])
        if not cat:
            continue
        levels = [p["level"] for p in r.get("permissions", []) if p.get("level") and not p.get("inherited")]
        if not levels:
            lines.append(f"# (skipped — only inherited permissions on {r['name']!r})")
            continue
        if cat["kind"] in ("workspace", "apps"):
            obj_id = r["item_id"]
            api = "permissions.update" if cat["kind"] == "workspace" else "apps.set_permissions"
            obj_arg = (
                f"{_quote(cat['request_object_type'])}, {_quote(str(obj_id))}"
                if cat["kind"] == "workspace"
                else f"{_quote(str(obj_id))}"
            )
            acl_items = [f"AccessControlRequest({pkw_repr}, permission_level=PermissionLevel.{lvl})" for lvl in levels]
            lines.append(f"# {cat['label']}: {r['name']}")
            lines.append(f"ws.{api}(")
            lines.append(f"    {obj_arg},")
            lines.append("    access_control_list=[")
            for item in acl_items:
                lines.append(f"        {item},")
            lines.append("    ],")
            lines.append(")")
            lines.append("")
        elif cat["kind"] == "uc":
            full = r["item_id"]
            privs = ", ".join(f"Privilege.{lvl}" for lvl in levels)
            lines.append(f"# {cat['label']}: {full}")
            lines.append("ws.grants.update(")
            lines.append(f"    SecurableType.{cat['securable_type']},")
            lines.append(f"    {_quote(full)},")
            lines.append("    changes=[PermissionsChange(")
            lines.append(f"        principal={_quote(to_principal)},")
            lines.append(f"        add=[{privs}],")
            lines.append("    )],")
            lines.append(")")
            lines.append("")
        elif cat["kind"] == "secret_scope":
            for lvl in levels:
                lines.append(f"# Secret scope: {r['name']}")
                lines.append(
                    f"ws.secrets.put_acl(scope={_quote(r['item_id'])}, "
                    f"principal={_quote(to_principal)}, permission=AclPermission.{lvl})"
                )
            lines.append("")
    return "\n".join(lines)


def _apply_clone(
    ws: Any,
    rows: list[dict],
    to_principal: str,
    to_type: str | None,
) -> tuple[int, list[str]]:
    """Apply each row's permissions to ``to_principal``. Returns (success_count, errors)."""
    errors: list[str] = []
    success = 0
    pkw = _principal_kwargs(to_principal, to_type)
    for r in rows:
        cat = CATEGORIES.get(r["category_id"])
        if not cat:
            continue
        levels = [p["level"] for p in r.get("permissions", []) if p.get("level") and not p.get("inherited")]
        if not levels:
            continue
        try:
            if cat["kind"] in ("workspace", "apps"):
                from databricks.sdk.service.iam import AccessControlRequest, PermissionLevel

                acl = [AccessControlRequest(**pkw, permission_level=PermissionLevel(lvl)) for lvl in levels]
                if cat["kind"] == "workspace":
                    ws.permissions.update(cat["request_object_type"], str(r["item_id"]), access_control_list=acl)
                else:
                    ws.apps.set_permissions(str(r["item_id"]), access_control_list=acl)
            elif cat["kind"] == "uc":
                from databricks.sdk.service.catalog import (
                    PermissionsChange,
                    Privilege,
                    SecurableType,
                )

                ws.grants.update(
                    SecurableType[cat["securable_type"]],
                    r["item_id"],
                    changes=[PermissionsChange(principal=to_principal, add=[Privilege(lvl) for lvl in levels])],
                )
            elif cat["kind"] == "secret_scope":
                from databricks.sdk.service.workspace import AclPermission

                for lvl in levels:
                    ws.secrets.put_acl(scope=r["item_id"], principal=to_principal, permission=AclPermission(lvl))
            success += 1
        except Exception as exc:
            errors.append(f"{cat['label']} {r['name']}: {exc}")
    return success, errors


# --------------------------------------------------------------------------- #
# Widget                                                                       #
# --------------------------------------------------------------------------- #


class AclWidget(anywidget.AnyWidget):
    """Cross-cutting permissions / ACL explorer widget."""

    _esm = traitlets.Unicode().tag(sync=True)
    _css = traitlets.Unicode("").tag(sync=True)

    # Static metadata pushed to the frontend
    categories_data = traitlets.Unicode("[]").tag(sync=True)

    # By-principal results
    principal_scan_data = traitlets.Unicode("{}").tag(sync=True)
    scan_progress = traitlets.Unicode("{}").tag(sync=True)
    clone_script = traitlets.Unicode("").tag(sync=True)
    clone_result = traitlets.Unicode("").tag(sync=True)

    # By-securable results
    securable_list_data = traitlets.Unicode("{}").tag(sync=True)
    securable_acl_data = traitlets.Unicode("{}").tag(sync=True)

    loading = traitlets.Bool(False).tag(sync=True)
    error_message = traitlets.Unicode("").tag(sync=True)
    request = traitlets.Unicode("").tag(sync=True)

    def __init__(self, workspace_client: Any = None, **kwargs: Any) -> None:
        esm_content = _ESM_PATH.read_text()
        super().__init__(_esm=esm_content, **kwargs)
        self._ws = workspace_client
        self._last_scan_rows: list[dict] = []
        self._last_scan_principal: str = ""
        self.categories_data = json.dumps(
            [
                {
                    "id": cid,
                    "label": cat["label"],
                    "kind": cat["kind"],
                    "default": bool(cat.get("default_scan")),
                    "deep": bool(cat.get("deep")),
                }
                for cid, cat in CATEGORIES.items()
            ]
        )
        self.observe(self._handle_request, names=["request"])

    # ---- glue ---- #

    def _ws_client(self) -> Any:
        if self._ws is None:
            self._ws = get_workspace_client(None)
        return self._ws

    def _handle_request(self, change: Any) -> None:
        raw = change.get("new", "")
        if not raw:
            return
        try:
            req = json.loads(raw)
        except json.JSONDecodeError:
            return
        action = req.get("action")
        try:
            if action == "scan_principal":
                self._scan_principal(
                    req.get("principal", ""),
                    req.get("categories") or [],
                    req.get("deep") or [],
                )
            elif action == "list_securables":
                self._list_securables(req.get("category_id", ""), req.get("filter", ""))
            elif action == "get_securable_acl":
                self._get_securable_acl(req.get("category_id", ""), req.get("item_id", ""), req.get("name"))
            elif action == "generate_clone_script":
                self._gen_clone_script(req.get("to_principal", ""), req.get("to_type"))
            elif action == "apply_clone":
                self._do_apply_clone(req.get("to_principal", ""), req.get("to_type"))
            elif action == "deep_scan_uc":
                # Used for by-securable picker to enumerate schemas/tables under a catalog.
                self._deep_uc_for_picker(req.get("category_id", ""), req.get("catalog"), req.get("schema"))
        except Exception as exc:
            LOGGER.exception("acl widget action failed")
            self.error_message = f"Action {action!r} failed: {exc}"

    # ---- by-principal scan ---- #

    def _scan_principal(self, principal: str, category_ids: list[str], deep_ids: list[str]) -> None:
        principal = (principal or "").strip()
        if not principal:
            self.error_message = "Enter a principal (user, group, or service principal name)."
            return
        ws = self._ws_client()
        self.loading = True
        self.error_message = ""
        self.clone_script = ""
        self.clone_result = ""

        rows: list[dict] = []
        per_category_errors: dict[str, str] = {}
        progress = {"started": time.time(), "category": None, "scanned": 0, "matched": 0}

        def push_progress() -> None:
            self.scan_progress = json.dumps(progress)

        def scan_one(category_id: str, item: Any) -> None:
            cat = CATEGORIES[category_id]
            try:
                item_id = cat["id"](item)
                name = cat["name"](item) or str(item_id)
                if item_id is None:
                    return
                acl_rows, err = _fetch_acl(ws, category_id, str(item_id))
                if err:
                    return
                for ar in acl_rows:
                    if _principal_matches(principal, ar.get("principal")):
                        rows.append(
                            {
                                "category_id": category_id,
                                "category_label": cat["label"],
                                "kind": cat["kind"],
                                "item_id": item_id,
                                "name": name,
                                "principal": ar.get("principal"),
                                "principal_type": ar.get("type"),
                                "permissions": ar.get("permissions") or [],
                            }
                        )
                        progress["matched"] += 1
            except Exception as exc:
                LOGGER.debug("scan error for %s: %s", category_id, exc, exc_info=True)
            progress["scanned"] += 1

        # 1. Scan top-level (non-deep) categories
        for cid in category_ids:
            if cid not in CATEGORIES:
                continue
            cat = CATEGORIES[cid]
            if cat.get("deep"):
                continue
            progress["category"] = cat["label"]
            push_progress()
            try:
                items = _safe_list(lambda c=cat: c["list"](ws))
            except Exception as exc:
                per_category_errors[cid] = str(exc)
                continue
            for item in items:
                scan_one(cid, item)
            push_progress()

        # 2. Deep UC walk (catalog -> schema -> table/volume/function)
        if deep_ids:
            progress["category"] = "Deep UC scan"
            push_progress()
            for cid, item in _iter_deep_uc(ws, deep_ids):
                scan_one(cid, item)
            push_progress()

        progress["category"] = None
        progress["finished"] = time.time()
        push_progress()

        self._last_scan_rows = rows
        self._last_scan_principal = principal
        self.principal_scan_data = json.dumps(
            {
                "principal": principal,
                "rows": rows,
                "errors": per_category_errors,
                "scanned": progress["scanned"],
            }
        )
        self.loading = False

    # ---- clone helpers ---- #

    def _gen_clone_script(self, to_principal: str, to_type: str | None) -> None:
        to_principal = (to_principal or "").strip()
        if not to_principal:
            self.error_message = "Enter a destination principal."
            return
        if not self._last_scan_rows:
            self.error_message = "Run a principal scan first."
            return
        self.clone_script = _generate_clone_script(
            self._last_scan_rows, self._last_scan_principal, to_principal, to_type
        )
        self.clone_result = json.dumps(
            {"action": "generate_clone_script", "success": True, "message": f"Generated script for {to_principal!r}."}
        )

    def _do_apply_clone(self, to_principal: str, to_type: str | None) -> None:
        to_principal = (to_principal or "").strip()
        if not to_principal:
            self.error_message = "Enter a destination principal."
            return
        if not self._last_scan_rows:
            self.error_message = "Run a principal scan first."
            return
        ws = self._ws_client()
        self.loading = True
        try:
            ok, errs = _apply_clone(ws, self._last_scan_rows, to_principal, to_type)
            self.clone_result = json.dumps(
                {
                    "action": "apply_clone",
                    "success": not errs,
                    "message": (
                        f"Applied {ok} grants to {to_principal!r}." + (f" {len(errs)} failed." if errs else "")
                    ),
                    "errors": errs,
                }
            )
        finally:
            self.loading = False

    # ---- by-securable ---- #

    def _list_securables(self, category_id: str, filter_text: str) -> None:
        if category_id not in CATEGORIES:
            self.error_message = f"Unknown category {category_id!r}."
            return
        cat = CATEGORIES[category_id]
        ws = self._ws_client()
        self.loading = True
        self.error_message = ""
        try:
            if cat.get("deep"):
                # For deep UC, ask for catalog/schema first via deep_scan_uc.
                self.securable_list_data = json.dumps(
                    {
                        "category_id": category_id,
                        "needs_drill": True,
                        "items": [],
                    }
                )
                return
            try:
                raw = _safe_list(lambda: cat["list"](ws))
            except Exception as exc:
                raise RuntimeError(f"Failed to list {cat['label']}: {exc}") from exc
            ft = (filter_text or "").lower()
            items = []
            for it in raw:
                iid = cat["id"](it)
                name = cat["name"](it) or str(iid)
                if iid is None:
                    continue
                if ft and ft not in str(name).lower() and ft not in str(iid).lower():
                    continue
                items.append({"item_id": iid, "name": name})
            items.sort(key=lambda x: str(x["name"]).lower())
            self.securable_list_data = json.dumps({"category_id": category_id, "needs_drill": False, "items": items})
        except Exception as exc:
            self.error_message = str(exc)
        finally:
            self.loading = False

    def _deep_uc_for_picker(self, category_id: str, catalog: str | None, schema: str | None) -> None:
        """Enumerate UC items under a catalog/schema for the by-securable picker."""
        if category_id not in CATEGORIES:
            self.error_message = f"Unknown category {category_id!r}."
            return
        cat = CATEGORIES[category_id]
        ws = self._ws_client()
        self.loading = True
        self.error_message = ""
        items = []
        try:
            if category_id == "uc-schema":
                if not catalog:
                    raise ValueError("Need a catalog to list schemas.")
                for s in _safe_list(lambda: ws.schemas.list(catalog_name=catalog)):
                    items.append({"item_id": getattr(s, "full_name", None), "name": getattr(s, "full_name", None)})
            elif category_id in ("uc-table", "uc-volume", "uc-function"):
                if not catalog or not schema:
                    raise ValueError("Need a catalog and schema.")
                listfn = {
                    "uc-table": ws.tables.list,
                    "uc-volume": ws.volumes.list,
                    "uc-function": ws.functions.list,
                }[category_id]
                for it in _safe_list(lambda: listfn(catalog_name=catalog, schema_name=schema)):
                    fn = getattr(it, "full_name", None) or f"{catalog}.{schema}.{getattr(it, 'name', '')}"
                    items.append({"item_id": fn, "name": fn})
            items.sort(key=lambda x: str(x["name"]).lower())
            self.securable_list_data = json.dumps(
                {
                    "category_id": category_id,
                    "needs_drill": False,
                    "items": items,
                    "catalog": catalog,
                    "schema": schema,
                }
            )
        except Exception as exc:
            self.error_message = str(exc)
        finally:
            self.loading = False
        _ = cat  # silence

    def _get_securable_acl(self, category_id: str, item_id: str, name: str | None) -> None:
        if category_id not in CATEGORIES:
            self.error_message = f"Unknown category {category_id!r}."
            return
        ws = self._ws_client()
        self.loading = True
        self.error_message = ""
        try:
            rows, err = _fetch_acl(ws, category_id, str(item_id))
            self.securable_acl_data = json.dumps(
                {
                    "category_id": category_id,
                    "category_label": CATEGORIES[category_id]["label"],
                    "item_id": item_id,
                    "name": name or str(item_id),
                    "rows": rows,
                    "error": err,
                }
            )
            if err:
                self.error_message = err
        finally:
            self.loading = False
