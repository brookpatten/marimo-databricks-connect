"""Operational widget for a single principal (user, group, or service principal).

Shows identity details from the SCIM APIs plus the same cross-cutting
permission scan as :func:`acl_widget` (by-principal tab), pre-bound to the
selected principal.

Usage::

    from marimo_databricks_connect import principal_widget
    widget = principal_widget("alice@example.com")
    widget
"""

from __future__ import annotations

import json
import logging
import pathlib
import time
from typing import Any

import anywidget
import traitlets

from ._acl_widget import (
    CATEGORIES,
    _apply_clone,
    _fetch_acl,
    _generate_clone_script,
    _iter_deep_uc,
    _principal_matches,
    _safe_list,
)
from ._ops_common import enum_val, get_workspace_client

LOGGER = logging.getLogger(__name__)

_ESM_PATH = pathlib.Path(__file__).parent / "_principal_widget_frontend.js"


# --------------------------------------------------------------------------- #
# Principal resolution & serialization                                         #
# --------------------------------------------------------------------------- #


def _scim_ref_list(values: Any) -> list[dict]:
    """Serialize a list of SCIM ComplexValue / ResourceReference objects."""
    out = []
    for v in values or []:
        out.append(
            {
                "value": getattr(v, "value", None),
                "display": getattr(v, "display", None),
                "ref": getattr(v, "ref", None),
                "type": getattr(v, "type", None),
                "primary": bool(getattr(v, "primary", False)) if hasattr(v, "primary") else None,
            }
        )
    return out


def _serialize_user(u: Any) -> dict:
    return {
        "kind": "user",
        "id": getattr(u, "id", None),
        "user_name": getattr(u, "user_name", None),
        "display_name": getattr(u, "display_name", None),
        "active": bool(getattr(u, "active", False)) if getattr(u, "active", None) is not None else None,
        "external_id": getattr(u, "external_id", None),
        "emails": _scim_ref_list(getattr(u, "emails", None)),
        "entitlements": [getattr(e, "value", None) for e in (getattr(u, "entitlements", None) or [])],
        "groups": _scim_ref_list(getattr(u, "groups", None)),
        "roles": _scim_ref_list(getattr(u, "roles", None)),
        "primary_identifier": getattr(u, "user_name", None) or getattr(u, "display_name", None),
    }


def _serialize_sp(sp: Any) -> dict:
    return {
        "kind": "service_principal",
        "id": getattr(sp, "id", None),
        "application_id": getattr(sp, "application_id", None),
        "display_name": getattr(sp, "display_name", None),
        "active": bool(getattr(sp, "active", False)) if getattr(sp, "active", None) is not None else None,
        "external_id": getattr(sp, "external_id", None),
        "entitlements": [getattr(e, "value", None) for e in (getattr(sp, "entitlements", None) or [])],
        "groups": _scim_ref_list(getattr(sp, "groups", None)),
        "roles": _scim_ref_list(getattr(sp, "roles", None)),
        "primary_identifier": getattr(sp, "application_id", None) or getattr(sp, "display_name", None),
    }


def _serialize_group(g: Any) -> dict:
    return {
        "kind": "group",
        "id": getattr(g, "id", None),
        "display_name": getattr(g, "display_name", None),
        "external_id": getattr(g, "external_id", None),
        "entitlements": [getattr(e, "value", None) for e in (getattr(g, "entitlements", None) or [])],
        "groups": _scim_ref_list(getattr(g, "groups", None)),
        "roles": _scim_ref_list(getattr(g, "roles", None)),
        "members": _scim_ref_list(getattr(g, "members", None)),
        "meta": {
            "resource_type": getattr(getattr(g, "meta", None), "resource_type", None),
        }
        if getattr(g, "meta", None)
        else None,
        "primary_identifier": getattr(g, "display_name", None),
    }


def _resolve_principal(ws: Any, name: str, hint: str | None = None) -> tuple[dict | None, str | None]:
    """Look up a principal by free-form name.

    Tries (in order, unless ``hint`` narrows the search):

    1. user with matching ``userName``
    2. service principal with matching ``applicationId``
    3. service principal with matching ``displayName``
    4. group with matching ``displayName``

    Returns ``(serialized_principal | None, error_message | None)``.
    """
    name = (name or "").strip()
    if not name:
        return None, "Empty principal name."
    safe = name.replace('"', '\\"')

    def try_user() -> dict | None:
        try:
            users = list(ws.users.list(filter=f'userName eq "{safe}"'))
            if users:
                # Re-fetch by id to populate full details (entitlements, groups)
                u = users[0]
                uid = getattr(u, "id", None)
                if uid:
                    try:
                        u = ws.users.get(uid)
                    except Exception:
                        pass
                return _serialize_user(u)
        except Exception as exc:
            LOGGER.debug("user lookup failed for %s: %s", name, exc, exc_info=True)
        return None

    def try_sp() -> dict | None:
        # SPs may be matched by applicationId (UUID) or displayName.
        for field in ("applicationId", "displayName"):
            try:
                sps = list(ws.service_principals.list(filter=f'{field} eq "{safe}"'))
                if sps:
                    sp = sps[0]
                    sid = getattr(sp, "id", None)
                    if sid:
                        try:
                            sp = ws.service_principals.get(sid)
                        except Exception:
                            pass
                    return _serialize_sp(sp)
            except Exception as exc:
                LOGGER.debug("sp lookup (%s) failed for %s: %s", field, name, exc, exc_info=True)
        return None

    def try_group() -> dict | None:
        try:
            groups = list(ws.groups.list(filter=f'displayName eq "{safe}"'))
            if groups:
                g = groups[0]
                gid = getattr(g, "id", None)
                if gid:
                    try:
                        g = ws.groups.get(gid)
                    except Exception:
                        pass
                return _serialize_group(g)
        except Exception as exc:
            LOGGER.debug("group lookup failed for %s: %s", name, exc, exc_info=True)
        return None

    order = {
        "user": [try_user],
        "service_principal": [try_sp],
        "group": [try_group],
        None: [try_user, try_sp, try_group],
    }.get(hint, [try_user, try_sp, try_group])

    for fn in order:
        result = fn()
        if result is not None:
            return result, None

    return None, f"Could not find a user, service principal, or group named {name!r}."


def _scan_id_for(principal: dict) -> str:
    """Return the identifier used to match this principal in ACLs."""
    if principal["kind"] == "user":
        return principal.get("user_name") or principal.get("display_name") or ""
    if principal["kind"] == "service_principal":
        return principal.get("application_id") or principal.get("display_name") or ""
    if principal["kind"] == "group":
        return principal.get("display_name") or ""
    return ""


# --------------------------------------------------------------------------- #
# Widget                                                                       #
# --------------------------------------------------------------------------- #


class PrincipalWidget(anywidget.AnyWidget):
    """Widget showing a single principal's details and all their permissions."""

    _esm = traitlets.Unicode().tag(sync=True)
    _css = traitlets.Unicode("").tag(sync=True)

    principal_name = traitlets.Unicode("").tag(sync=True)
    principal_data = traitlets.Unicode("{}").tag(sync=True)
    categories_data = traitlets.Unicode("[]").tag(sync=True)
    scan_data = traitlets.Unicode("{}").tag(sync=True)
    scan_progress = traitlets.Unicode("{}").tag(sync=True)
    clone_script = traitlets.Unicode("").tag(sync=True)
    clone_result = traitlets.Unicode("").tag(sync=True)
    loading = traitlets.Bool(False).tag(sync=True)
    error_message = traitlets.Unicode("").tag(sync=True)
    request = traitlets.Unicode("").tag(sync=True)

    def __init__(
        self,
        principal: str,
        principal_type: str | None = None,
        workspace_client: Any = None,
        auto_scan: bool = False,
        **kwargs: Any,
    ) -> None:
        esm_content = _ESM_PATH.read_text()
        super().__init__(_esm=esm_content, **kwargs)
        self._ws = workspace_client
        self._principal_input = principal
        self._principal_type_hint = principal_type
        self._resolved: dict | None = None
        self._last_scan_rows: list[dict] = []
        self.principal_name = principal
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
        self._resolve()
        if auto_scan and self._resolved is not None:
            default_cats = [cid for cid, cat in CATEGORIES.items() if cat.get("default_scan") and not cat.get("deep")]
            self._scan(default_cats, [])

    # ---- helpers ---- #

    def _ws_client(self) -> Any:
        if self._ws is None:
            self._ws = get_workspace_client(None)
        return self._ws

    def _resolve(self) -> None:
        self.loading = True
        self.error_message = ""
        try:
            data, err = _resolve_principal(self._ws_client(), self._principal_input, self._principal_type_hint)
            if err:
                self.error_message = err
                self._resolved = None
                self.principal_data = "{}"
            else:
                self._resolved = data
                self.principal_data = json.dumps(data)
        finally:
            self.loading = False

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
            if action == "refresh":
                self._resolve()
            elif action == "scan":
                self._scan(req.get("categories") or [], req.get("deep") or [])
            elif action == "generate_clone_script":
                self._gen_clone_script(req.get("to_principal", ""), req.get("to_type"))
            elif action == "apply_clone":
                self._do_apply_clone(req.get("to_principal", ""), req.get("to_type"))
        except Exception as exc:
            LOGGER.exception("principal widget action failed")
            self.error_message = f"Action {action!r} failed: {exc}"

    # ---- scan ---- #

    def _scan(self, category_ids: list[str], deep_ids: list[str]) -> None:
        if self._resolved is None:
            self.error_message = "Principal not resolved."
            return
        principal_id = _scan_id_for(self._resolved)
        if not principal_id:
            self.error_message = "Could not determine a usable identifier for the principal."
            return
        ws = self._ws_client()
        self.loading = True
        self.error_message = ""
        self.clone_script = ""
        self.clone_result = ""

        rows: list[dict] = []
        per_category_errors: dict[str, str] = {}
        progress = {"started": time.time(), "category": None, "scanned": 0, "matched": 0}

        def push() -> None:
            self.scan_progress = json.dumps(progress)

        def scan_one(cid: str, item: Any) -> None:
            cat = CATEGORIES[cid]
            try:
                item_id = cat["id"](item)
                name = cat["name"](item) or str(item_id)
                if item_id is None:
                    return
                acl_rows, err = _fetch_acl(ws, cid, str(item_id))
                if err:
                    return
                for ar in acl_rows:
                    if _principal_matches(principal_id, ar.get("principal")):
                        rows.append(
                            {
                                "category_id": cid,
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
                LOGGER.debug("scan error for %s: %s", cid, exc, exc_info=True)
            progress["scanned"] += 1

        for cid in category_ids:
            if cid not in CATEGORIES or CATEGORIES[cid].get("deep"):
                continue
            cat = CATEGORIES[cid]
            progress["category"] = cat["label"]
            push()
            try:
                items = _safe_list(lambda c=cat: c["list"](ws))
            except Exception as exc:
                per_category_errors[cid] = str(exc)
                continue
            for item in items:
                scan_one(cid, item)
            push()

        if deep_ids:
            progress["category"] = "Deep UC scan"
            push()
            for cid, item in _iter_deep_uc(ws, deep_ids):
                scan_one(cid, item)
            push()

        progress["category"] = None
        progress["finished"] = time.time()
        push()

        self._last_scan_rows = rows
        self.scan_data = json.dumps(
            {
                "principal": principal_id,
                "rows": rows,
                "errors": per_category_errors,
                "scanned": progress["scanned"],
            }
        )
        self.loading = False

    # ---- clone ---- #

    def _gen_clone_script(self, to_principal: str, to_type: str | None) -> None:
        to_principal = (to_principal or "").strip()
        if not to_principal:
            self.error_message = "Enter a destination principal."
            return
        if not self._last_scan_rows:
            self.error_message = "Run a scan first."
            return
        from_id = _scan_id_for(self._resolved or {})
        self.clone_script = _generate_clone_script(self._last_scan_rows, from_id, to_principal, to_type)
        self.clone_result = json.dumps(
            {"action": "generate_clone_script", "success": True, "message": f"Generated script for {to_principal!r}."}
        )

    def _do_apply_clone(self, to_principal: str, to_type: str | None) -> None:
        to_principal = (to_principal or "").strip()
        if not to_principal:
            self.error_message = "Enter a destination principal."
            return
        if not self._last_scan_rows:
            self.error_message = "Run a scan first."
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


# Re-export to silence "unused import" linters; callers may use it directly.
__all__ = ["PrincipalWidget", "_resolve_principal", "_scan_id_for", "enum_val"]
