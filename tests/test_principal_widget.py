"""Tests for the principal widget."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock


# ---------- helpers ---------- #


def _scim_ref(value=None, display=None, type_=None, primary=False):
    return SimpleNamespace(value=value, display=display, ref=None, type=type_, primary=primary)


def _entitlement(v):
    return SimpleNamespace(value=v)


def _make_user(
    user_name="alice@example.com",
    uid="111",
    display="Alice",
    active=True,
    groups=None,
    entitlements=None,
    emails=None,
    roles=None,
):
    return SimpleNamespace(
        id=uid,
        user_name=user_name,
        display_name=display,
        active=active,
        external_id=None,
        emails=emails or [_scim_ref(value=user_name, primary=True)],
        entitlements=[_entitlement(e) for e in (entitlements or ["workspace-access"])],
        groups=groups or [_scim_ref(value="g1", display="data-engineers")],
        roles=roles or [],
    )


def _make_sp(app_id="app-uuid-123", sid="222", display="my-sp", active=True):
    return SimpleNamespace(
        id=sid,
        application_id=app_id,
        display_name=display,
        active=active,
        external_id=None,
        entitlements=[_entitlement("workspace-access")],
        groups=[],
        roles=[],
    )


def _make_group(display="data-engineers", gid="333", members=None):
    return SimpleNamespace(
        id=gid,
        display_name=display,
        external_id=None,
        entitlements=[],
        groups=[],
        roles=[],
        members=members or [_scim_ref(value="111", display="Alice"), _scim_ref(value="112", display="Bob")],
        meta=SimpleNamespace(resource_type="WorkspaceGroup"),
    )


def _acr(*, user=None, group=None, sp=None, levels=("CAN_VIEW",), inherited=False):
    return SimpleNamespace(
        user_name=user,
        group_name=group,
        service_principal_name=sp,
        display_name=user or group or sp,
        all_permissions=[
            SimpleNamespace(
                permission_level=SimpleNamespace(value=lvl), inherited=inherited, inherited_from_object=None
            )
            for lvl in levels
        ],
    )


def _obj_perms(entries):
    return SimpleNamespace(access_control_list=entries, object_id="x", object_type="t")


def _ws_with_user(user=None):
    """Build a workspace mock that resolves a user via SCIM filter."""
    ws = MagicMock()
    user = user or _make_user()
    ws.users.list.return_value = [user]
    ws.users.get.return_value = user
    ws.service_principals.list.return_value = []
    ws.groups.list.return_value = []
    # Empty listings for scan categories so by default scans don't blow up
    for attr in (
        "clusters",
        "cluster_policies",
        "instance_pools",
        "warehouses",
        "serving_endpoints",
        "lakeview",
        "queries",
        "alerts",
        "experiments",
        "external_locations",
        "storage_credentials",
        "connections",
    ):
        getattr(ws, attr).list.return_value = []
    ws.jobs.list.return_value = []
    ws.pipelines.list_pipelines.return_value = []
    ws.model_registry.list_models.return_value = []
    ws.secrets.list_scopes.return_value = []
    ws.secrets.list_acls.return_value = []
    ws.apps.list.return_value = []
    ws.metastores.summary.return_value = None
    ws.catalogs.list.return_value = []
    return ws


# ---------- Tests ---------- #


class TestPrincipalResolution:
    def test_resolve_user(self):
        from marimo_databricks_connect._principal_widget import _resolve_principal

        ws = _ws_with_user()
        data, err = _resolve_principal(ws, "alice@example.com")
        assert err is None
        assert data["kind"] == "user"
        assert data["user_name"] == "alice@example.com"
        assert data["display_name"] == "Alice"
        # groups serialized
        assert data["groups"][0]["display"] == "data-engineers"
        # filter passed correctly
        assert 'userName eq "alice@example.com"' in ws.users.list.call_args.kwargs["filter"]

    def test_resolve_service_principal_by_app_id(self):
        from marimo_databricks_connect._principal_widget import _resolve_principal

        ws = MagicMock()
        ws.users.list.return_value = []
        sp = _make_sp(app_id="app-uuid-123")
        # First call: applicationId filter returns SP
        ws.service_principals.list.side_effect = [[sp], []]
        ws.service_principals.get.return_value = sp
        ws.groups.list.return_value = []
        data, err = _resolve_principal(ws, "app-uuid-123")
        assert err is None
        assert data["kind"] == "service_principal"
        assert data["application_id"] == "app-uuid-123"

    def test_resolve_sp_by_display_name_falls_through(self):
        from marimo_databricks_connect._principal_widget import _resolve_principal

        ws = MagicMock()
        ws.users.list.return_value = []
        sp = _make_sp(display="my-sp")
        # applicationId filter returns nothing → displayName filter returns SP
        ws.service_principals.list.side_effect = [[], [sp]]
        ws.service_principals.get.return_value = sp
        ws.groups.list.return_value = []
        data, err = _resolve_principal(ws, "my-sp")
        assert err is None
        assert data["display_name"] == "my-sp"

    def test_resolve_group(self):
        from marimo_databricks_connect._principal_widget import _resolve_principal

        ws = MagicMock()
        ws.users.list.return_value = []
        ws.service_principals.list.return_value = []
        g = _make_group()
        ws.groups.list.return_value = [g]
        ws.groups.get.return_value = g
        data, err = _resolve_principal(ws, "data-engineers")
        assert err is None
        assert data["kind"] == "group"
        assert len(data["members"]) == 2

    def test_resolve_not_found(self):
        from marimo_databricks_connect._principal_widget import _resolve_principal

        ws = MagicMock()
        ws.users.list.return_value = []
        ws.service_principals.list.return_value = []
        ws.groups.list.return_value = []
        data, err = _resolve_principal(ws, "nobody")
        assert data is None
        assert "Could not find" in err

    def test_resolve_hint_skips_other_lookups(self):
        from marimo_databricks_connect._principal_widget import _resolve_principal

        ws = MagicMock()
        g = _make_group()
        ws.groups.list.return_value = [g]
        ws.groups.get.return_value = g
        # With hint=group, users.list and service_principals.list must NOT be called.
        data, err = _resolve_principal(ws, "data-engineers", hint="group")
        assert err is None
        assert data["kind"] == "group"
        ws.users.list.assert_not_called()
        ws.service_principals.list.assert_not_called()

    def test_resolve_empty_input(self):
        from marimo_databricks_connect._principal_widget import _resolve_principal

        ws = MagicMock()
        data, err = _resolve_principal(ws, "")
        assert data is None
        assert "Empty" in err


class TestPrincipalWidgetInit:
    def test_init_resolves_and_publishes_categories(self):
        from marimo_databricks_connect._principal_widget import PrincipalWidget

        ws = _ws_with_user()
        w = PrincipalWidget(principal="alice@example.com", workspace_client=ws)
        assert w.principal_name == "alice@example.com"
        p = json.loads(w.principal_data)
        assert p["kind"] == "user"
        assert p["user_name"] == "alice@example.com"
        cats = json.loads(w.categories_data)
        ids = {c["id"] for c in cats}
        assert "clusters" in ids and "uc-catalog" in ids and "secret-scopes" in ids
        # No scan run yet
        assert w.scan_data == "{}"

    def test_init_missing_principal_sets_error(self):
        from marimo_databricks_connect._principal_widget import PrincipalWidget

        ws = MagicMock()
        ws.users.list.return_value = []
        ws.service_principals.list.return_value = []
        ws.groups.list.return_value = []
        w = PrincipalWidget(principal="ghost@example.com", workspace_client=ws)
        assert "Could not find" in w.error_message
        assert w.principal_data == "{}"

    def test_factory_function(self):
        from marimo_databricks_connect import principal_widget
        from marimo_databricks_connect._principal_widget import PrincipalWidget

        ws = _ws_with_user()
        w = principal_widget("alice@example.com", workspace_client=ws)
        assert isinstance(w, PrincipalWidget)

    def test_auto_scan_runs_default_categories(self):
        from marimo_databricks_connect._principal_widget import PrincipalWidget

        ws = _ws_with_user()
        ws.clusters.list.return_value = [SimpleNamespace(cluster_id="c1", cluster_name="alpha")]
        ws.permissions.get.return_value = _obj_perms([_acr(user="alice@example.com", levels=("CAN_RESTART",))])
        w = PrincipalWidget(principal="alice@example.com", workspace_client=ws, auto_scan=True)
        scan = json.loads(w.scan_data)
        assert scan["principal"] == "alice@example.com"
        assert any(r["category_id"] == "clusters" and r["item_id"] == "c1" for r in scan["rows"])


class TestPrincipalWidgetScan:
    def test_scan_finds_sp_grants_by_application_id(self):
        from marimo_databricks_connect._principal_widget import PrincipalWidget

        ws = MagicMock()
        ws.users.list.return_value = []
        sp = _make_sp(app_id="app-uuid-123", display="my-sp")
        ws.service_principals.list.side_effect = [[sp], []]
        ws.service_principals.get.return_value = sp
        ws.groups.list.return_value = []
        # one job ACL'd to the SP by applicationId
        ws.jobs.list.return_value = [SimpleNamespace(job_id=42, settings=SimpleNamespace(name="ml-train"))]
        ws.permissions.get.return_value = _obj_perms([_acr(sp="app-uuid-123", levels=("CAN_MANAGE_RUN",))])
        # Empty everything else
        for attr in (
            "clusters",
            "cluster_policies",
            "instance_pools",
            "warehouses",
            "serving_endpoints",
            "lakeview",
            "queries",
            "alerts",
            "experiments",
            "external_locations",
            "storage_credentials",
            "connections",
        ):
            getattr(ws, attr).list.return_value = []
        ws.pipelines.list_pipelines.return_value = []
        ws.model_registry.list_models.return_value = []
        ws.secrets.list_scopes.return_value = []
        ws.apps.list.return_value = []
        ws.metastores.summary.return_value = None
        ws.catalogs.list.return_value = []

        w = PrincipalWidget(principal="my-sp", workspace_client=ws)
        w.request = json.dumps({"action": "scan", "categories": ["jobs"], "deep": []})
        scan = json.loads(w.scan_data)
        assert scan["principal"] == "app-uuid-123"
        assert any(r["category_id"] == "jobs" and r["principal"] == "app-uuid-123" for r in scan["rows"])

    def test_scan_finds_group_grants_by_display_name(self):
        from marimo_databricks_connect._principal_widget import PrincipalWidget

        ws = MagicMock()
        ws.users.list.return_value = []
        ws.service_principals.list.return_value = []
        g = _make_group()
        ws.groups.list.return_value = [g]
        ws.groups.get.return_value = g
        ws.warehouses.list.return_value = [SimpleNamespace(id="w1", name="wh-prod")]
        ws.permissions.get.return_value = _obj_perms([_acr(group="data-engineers", levels=("CAN_USE",))])
        for attr in (
            "clusters",
            "cluster_policies",
            "instance_pools",
            "serving_endpoints",
            "lakeview",
            "queries",
            "alerts",
            "experiments",
            "external_locations",
            "storage_credentials",
            "connections",
        ):
            getattr(ws, attr).list.return_value = []
        ws.jobs.list.return_value = []
        ws.pipelines.list_pipelines.return_value = []
        ws.model_registry.list_models.return_value = []
        ws.secrets.list_scopes.return_value = []
        ws.apps.list.return_value = []
        ws.metastores.summary.return_value = None
        ws.catalogs.list.return_value = []

        w = PrincipalWidget(principal="data-engineers", workspace_client=ws)
        w.request = json.dumps({"action": "scan", "categories": ["warehouses"], "deep": []})
        scan = json.loads(w.scan_data)
        assert any(r["category_id"] == "warehouses" and r["principal"] == "data-engineers" for r in scan["rows"])

    def test_refresh_re_resolves(self):
        from marimo_databricks_connect._principal_widget import PrincipalWidget

        ws = _ws_with_user()
        w = PrincipalWidget(principal="alice@example.com", workspace_client=ws)
        ws.users.list.reset_mock()
        updated = _make_user(display="Alice Updated")
        ws.users.list.return_value = [updated]
        ws.users.get.return_value = updated
        w.request = json.dumps({"action": "refresh"})
        ws.users.list.assert_called()
        assert json.loads(w.principal_data)["display_name"] == "Alice Updated"


class TestPrincipalWidgetClone:
    def test_clone_script_uses_resolved_identifier(self):
        from marimo_databricks_connect._principal_widget import PrincipalWidget

        ws = _ws_with_user()
        ws.clusters.list.return_value = [SimpleNamespace(cluster_id="c1", cluster_name="alpha")]
        ws.permissions.get.return_value = _obj_perms([_acr(user="alice@example.com", levels=("CAN_RESTART",))])
        w = PrincipalWidget(principal="alice@example.com", workspace_client=ws)
        w.request = json.dumps({"action": "scan", "categories": ["clusters"], "deep": []})
        w.request = json.dumps(
            {"action": "generate_clone_script", "to_principal": "carol@example.com", "to_type": "user"}
        )
        script = w.clone_script
        assert "carol@example.com" in script
        assert "PermissionLevel.CAN_RESTART" in script
        assert "user_name=" in script

    def test_clone_requires_scan(self):
        from marimo_databricks_connect._principal_widget import PrincipalWidget

        ws = _ws_with_user()
        w = PrincipalWidget(principal="alice@example.com", workspace_client=ws)
        w.request = json.dumps({"action": "generate_clone_script", "to_principal": "x@y.com"})
        assert "scan" in w.error_message.lower()

    def test_apply_clone_calls_apis(self):
        from marimo_databricks_connect._principal_widget import PrincipalWidget

        ws = _ws_with_user()
        ws.clusters.list.return_value = [SimpleNamespace(cluster_id="c1", cluster_name="alpha")]
        ws.permissions.get.return_value = _obj_perms([_acr(user="alice@example.com", levels=("CAN_RESTART",))])
        w = PrincipalWidget(principal="alice@example.com", workspace_client=ws)
        w.request = json.dumps({"action": "scan", "categories": ["clusters"], "deep": []})
        w.request = json.dumps({"action": "apply_clone", "to_principal": "carol@example.com", "to_type": "user"})
        ws.permissions.update.assert_called()
        result = json.loads(w.clone_result)
        assert result["success"] is True

    def test_invalid_request_silently_handled(self):
        from marimo_databricks_connect._principal_widget import PrincipalWidget

        ws = _ws_with_user()
        w = PrincipalWidget(principal="alice@example.com", workspace_client=ws)
        w.request = "not-json"
        w.request = json.dumps({"action": "bogus"})
