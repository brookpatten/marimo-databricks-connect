"""Tests for the cross-cutting permissions / ACL widget."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock


# ---------- helpers ---------- #


def _acr(*, user=None, group=None, sp=None, levels=("CAN_VIEW",), inherited=False):
    return SimpleNamespace(
        user_name=user,
        group_name=group,
        service_principal_name=sp,
        display_name=user or group or sp,
        all_permissions=[
            SimpleNamespace(
                permission_level=SimpleNamespace(value=lvl),
                inherited=inherited,
                inherited_from_object=None,
            )
            for lvl in levels
        ],
    )


def _obj_perms(acl_entries):
    return SimpleNamespace(access_control_list=acl_entries, object_id="x", object_type="t")


def _uc_priv_assignment(principal, privileges, inherited_from=None):
    return SimpleNamespace(
        principal=principal,
        privileges=[
            SimpleNamespace(
                privilege=SimpleNamespace(value=p),
                inherited_from_name=inherited_from,
                inherited_from_type=SimpleNamespace(value="CATALOG") if inherited_from else None,
            )
            for p in privileges
        ],
    )


def _uc_perms(assignments):
    return SimpleNamespace(privilege_assignments=assignments)


def _make_ws_for_scan():
    """Build a mock workspace client with a few items per category so we can
    verify the by-principal scan walks the right APIs."""
    ws = MagicMock()
    # clusters
    ws.clusters.list.return_value = [
        SimpleNamespace(cluster_id="c1", cluster_name="alpha"),
        SimpleNamespace(cluster_id="c2", cluster_name="beta"),
    ]
    # jobs
    ws.jobs.list.return_value = [
        SimpleNamespace(job_id=1, settings=SimpleNamespace(name="ingest")),
    ]
    # warehouses
    ws.warehouses.list.return_value = [SimpleNamespace(id="w1", name="wh-prod")]
    # cluster policies / instance pools / pipelines / serving / lakeview / queries / alerts / experiments / models
    ws.cluster_policies.list.return_value = []
    ws.instance_pools.list.return_value = []
    ws.pipelines.list_pipelines.return_value = []
    ws.serving_endpoints.list.return_value = []
    ws.lakeview.list.return_value = []
    ws.queries.list.return_value = []
    ws.alerts.list.return_value = []
    ws.experiments.list_experiments.return_value = []
    ws.model_registry.list_models.return_value = []

    # secret scopes
    ws.secrets.list_scopes.return_value = [SimpleNamespace(name="prod-secrets")]
    ws.secrets.list_acls.return_value = [
        SimpleNamespace(principal="alice@example.com", permission=SimpleNamespace(value="READ")),
        SimpleNamespace(principal="bob@example.com", permission=SimpleNamespace(value="MANAGE")),
    ]

    # apps
    ws.apps.list.return_value = [SimpleNamespace(name="my-app")]
    ws.apps.get_permissions.return_value = _obj_perms([_acr(user="alice@example.com", levels=("CAN_MANAGE",))])

    # UC
    ws.metastores.summary.return_value = SimpleNamespace(metastore_id="ms-1", name="main-ms")
    ws.catalogs.list.return_value = [SimpleNamespace(name="main"), SimpleNamespace(name="dev")]
    ws.external_locations.list.return_value = [SimpleNamespace(name="finops")]
    ws.storage_credentials.list.return_value = []
    ws.connections.list.return_value = []

    # workspace permissions API: route by request_object_type
    def perms_get(rot, oid):
        # Give alice CAN_RESTART on cluster c1, no permissions on c2
        if rot == "clusters" and oid == "c1":
            return _obj_perms([_acr(user="alice@example.com", levels=("CAN_RESTART",))])
        if rot == "clusters" and oid == "c2":
            return _obj_perms([_acr(user="bob@example.com", levels=("CAN_ATTACH_TO",))])
        if rot == "jobs":
            return _obj_perms([_acr(user="alice@example.com", levels=("CAN_MANAGE_RUN",))])
        if rot == "sql/warehouses":
            return _obj_perms([_acr(group="data-eng", levels=("CAN_USE",))])
        return _obj_perms([])

    ws.permissions.get.side_effect = perms_get

    # UC grants.get_effective: alice has SELECT on main, USE_CATALOG inherited from metastore on dev
    def grants_get_effective(securable_type, full_name):
        st = getattr(securable_type, "name", str(securable_type))
        if st == "CATALOG" and full_name == "main":
            return _uc_perms([_uc_priv_assignment("alice@example.com", ["USE_CATALOG", "SELECT"])])
        if st == "CATALOG" and full_name == "dev":
            return _uc_perms([_uc_priv_assignment("alice@example.com", ["USE_CATALOG"], inherited_from="metastore")])
        if st == "EXTERNAL_LOCATION" and full_name == "finops":
            return _uc_perms([_uc_priv_assignment("data-eng", ["READ_FILES"])])
        if st == "METASTORE":
            return _uc_perms([])
        return _uc_perms([])

    ws.grants.get_effective.side_effect = grants_get_effective
    return ws


# ---------- Tests ---------- #


class TestAclWidget:
    def test_initialization_publishes_categories(self):
        from marimo_databricks_connect._acl_widget import AclWidget

        ws = MagicMock()
        w = AclWidget(workspace_client=ws)
        cats = json.loads(w.categories_data)
        ids = {c["id"] for c in cats}
        # must include the major workspace + UC + secret + apps categories
        for required in (
            "clusters",
            "jobs",
            "warehouses",
            "secret-scopes",
            "apps",
            "uc-catalog",
            "uc-external-location",
            "uc-storage-credential",
            "uc-table",
            "uc-volume",
        ):
            assert required in ids
        # default flag is set on cheap categories
        defaults = {c["id"] for c in cats if c["default"]}
        assert "clusters" in defaults
        assert "uc-catalog" in defaults
        assert "uc-table" not in defaults  # deep
        # deep flag
        assert any(c["deep"] for c in cats if c["id"] == "uc-table")

    def test_factory_function(self):
        from marimo_databricks_connect import acl_widget, permissions_widget
        from marimo_databricks_connect._acl_widget import AclWidget

        ws = MagicMock()
        w = acl_widget(workspace_client=ws)
        assert isinstance(w, AclWidget)
        assert permissions_widget is acl_widget

    def test_scan_principal_finds_matches_across_categories(self):
        from marimo_databricks_connect._acl_widget import AclWidget

        ws = _make_ws_for_scan()
        w = AclWidget(workspace_client=ws)
        w.request = json.dumps(
            {
                "action": "scan_principal",
                "principal": "alice@example.com",
                "categories": [
                    "clusters",
                    "jobs",
                    "warehouses",
                    "secret-scopes",
                    "apps",
                    "uc-metastore",
                    "uc-catalog",
                    "uc-external-location",
                ],
                "deep": [],
            }
        )
        scan = json.loads(w.principal_scan_data)
        assert scan["principal"] == "alice@example.com"
        rows = scan["rows"]
        cats_seen = {r["category_id"] for r in rows}
        # Alice should be matched on clusters (c1), jobs, secret scope, app, both catalogs
        assert "clusters" in cats_seen
        assert "jobs" in cats_seen
        assert "secret-scopes" in cats_seen
        assert "apps" in cats_seen
        assert "uc-catalog" in cats_seen
        # warehouse acl is for group, not alice
        assert "warehouses" not in cats_seen
        # We should NOT have matched bob's grants on c2
        cluster_rows = [r for r in rows if r["category_id"] == "clusters"]
        assert len(cluster_rows) == 1
        assert cluster_rows[0]["item_id"] == "c1"
        # UC matched both catalogs (one direct, one inherited)
        uc_rows = [r for r in rows if r["category_id"] == "uc-catalog"]
        assert {r["item_id"] for r in uc_rows} == {"main", "dev"}
        # inherited flag is preserved
        dev_row = next(r for r in uc_rows if r["item_id"] == "dev")
        assert any(p["inherited"] for p in dev_row["permissions"])

    def test_scan_principal_requires_principal(self):
        from marimo_databricks_connect._acl_widget import AclWidget

        ws = MagicMock()
        w = AclWidget(workspace_client=ws)
        w.request = json.dumps({"action": "scan_principal", "principal": "  "})
        assert "principal" in w.error_message.lower()

    def test_scan_is_case_insensitive(self):
        from marimo_databricks_connect._acl_widget import AclWidget

        ws = _make_ws_for_scan()
        w = AclWidget(workspace_client=ws)
        w.request = json.dumps(
            {
                "action": "scan_principal",
                "principal": "ALICE@example.com",
                "categories": ["clusters"],
                "deep": [],
            }
        )
        scan = json.loads(w.principal_scan_data)
        assert any(r["category_id"] == "clusters" for r in scan["rows"])

    def test_list_securables_for_workspace_category(self):
        from marimo_databricks_connect._acl_widget import AclWidget

        ws = _make_ws_for_scan()
        w = AclWidget(workspace_client=ws)
        w.request = json.dumps({"action": "list_securables", "category_id": "clusters", "filter": ""})
        data = json.loads(w.securable_list_data)
        assert data["category_id"] == "clusters"
        assert data["needs_drill"] is False
        names = [it["name"] for it in data["items"]]
        assert "alpha" in names and "beta" in names

    def test_list_securables_filter(self):
        from marimo_databricks_connect._acl_widget import AclWidget

        ws = _make_ws_for_scan()
        w = AclWidget(workspace_client=ws)
        w.request = json.dumps({"action": "list_securables", "category_id": "clusters", "filter": "alpha"})
        data = json.loads(w.securable_list_data)
        assert [it["name"] for it in data["items"]] == ["alpha"]

    def test_list_securables_deep_signals_drill(self):
        from marimo_databricks_connect._acl_widget import AclWidget

        ws = _make_ws_for_scan()
        w = AclWidget(workspace_client=ws)
        w.request = json.dumps({"action": "list_securables", "category_id": "uc-table"})
        data = json.loads(w.securable_list_data)
        assert data["needs_drill"] is True

    def test_get_securable_acl_workspace(self):
        from marimo_databricks_connect._acl_widget import AclWidget

        ws = _make_ws_for_scan()
        w = AclWidget(workspace_client=ws)
        w.request = json.dumps(
            {"action": "get_securable_acl", "category_id": "clusters", "item_id": "c1", "name": "alpha"}
        )
        data = json.loads(w.securable_acl_data)
        assert data["item_id"] == "c1"
        assert data["category_id"] == "clusters"
        principals = {r["principal"] for r in data["rows"]}
        assert principals == {"alice@example.com"}

    def test_get_securable_acl_uc_catalog(self):
        from marimo_databricks_connect._acl_widget import AclWidget

        ws = _make_ws_for_scan()
        w = AclWidget(workspace_client=ws)
        w.request = json.dumps({"action": "get_securable_acl", "category_id": "uc-catalog", "item_id": "main"})
        data = json.loads(w.securable_acl_data)
        assert data["item_id"] == "main"
        levels = {p["level"] for r in data["rows"] for p in r["permissions"]}
        assert "SELECT" in levels and "USE_CATALOG" in levels

    def test_get_securable_acl_secret_scope(self):
        from marimo_databricks_connect._acl_widget import AclWidget

        ws = _make_ws_for_scan()
        w = AclWidget(workspace_client=ws)
        w.request = json.dumps(
            {"action": "get_securable_acl", "category_id": "secret-scopes", "item_id": "prod-secrets"}
        )
        data = json.loads(w.securable_acl_data)
        principals = {r["principal"] for r in data["rows"]}
        assert principals == {"alice@example.com", "bob@example.com"}

    def test_get_securable_acl_apps(self):
        from marimo_databricks_connect._acl_widget import AclWidget

        ws = _make_ws_for_scan()
        w = AclWidget(workspace_client=ws)
        w.request = json.dumps({"action": "get_securable_acl", "category_id": "apps", "item_id": "my-app"})
        data = json.loads(w.securable_acl_data)
        assert data["rows"][0]["principal"] == "alice@example.com"

    def test_generate_clone_script_requires_scan(self):
        from marimo_databricks_connect._acl_widget import AclWidget

        ws = MagicMock()
        w = AclWidget(workspace_client=ws)
        w.request = json.dumps({"action": "generate_clone_script", "to_principal": "x@y.com"})
        assert "scan" in w.error_message.lower()

    def test_generate_clone_script_after_scan(self):
        from marimo_databricks_connect._acl_widget import AclWidget

        ws = _make_ws_for_scan()
        w = AclWidget(workspace_client=ws)
        w.request = json.dumps(
            {
                "action": "scan_principal",
                "principal": "alice@example.com",
                "categories": ["clusters", "jobs", "secret-scopes", "apps", "uc-catalog"],
                "deep": [],
            }
        )
        w.request = json.dumps(
            {"action": "generate_clone_script", "to_principal": "carol@example.com", "to_type": "user"}
        )
        script = w.clone_script
        assert "carol@example.com" in script
        assert "ws.permissions.update" in script
        assert "AccessControlRequest" in script
        assert "PermissionLevel.CAN_RESTART" in script
        assert "PermissionLevel.CAN_MANAGE_RUN" in script
        assert "ws.grants.update" in script
        assert "SecurableType.CATALOG" in script
        assert "Privilege.SELECT" in script
        assert "ws.secrets.put_acl" in script
        assert "AclPermission.READ" in script
        # Inherited-only grants are skipped (the dev catalog grant for alice is inherited)
        assert "# (skipped" in script

    def test_generate_clone_script_auto_detects_principal_type(self):
        from marimo_databricks_connect._acl_widget import AclWidget

        ws = _make_ws_for_scan()
        w = AclWidget(workspace_client=ws)
        w.request = json.dumps(
            {"action": "scan_principal", "principal": "alice@example.com", "categories": ["clusters"], "deep": []}
        )
        # No to_type → email is detected as user
        w.request = json.dumps({"action": "generate_clone_script", "to_principal": "newuser@x.com"})
        assert "user_name=" in w.clone_script
        # Group name (no @) → service_principal by default unless explicit
        w.request = json.dumps(
            {"action": "generate_clone_script", "to_principal": "data-engineers", "to_type": "group"}
        )
        assert "group_name=" in w.clone_script

    def test_apply_clone_calls_apis(self):
        from marimo_databricks_connect._acl_widget import AclWidget

        ws = _make_ws_for_scan()
        w = AclWidget(workspace_client=ws)
        w.request = json.dumps(
            {
                "action": "scan_principal",
                "principal": "alice@example.com",
                "categories": ["clusters", "secret-scopes"],
                "deep": [],
            }
        )
        w.request = json.dumps({"action": "apply_clone", "to_principal": "carol@example.com", "to_type": "user"})
        # We applied to: 1 cluster + 1 secret scope = 2 items
        result = json.loads(w.clone_result)
        assert result["action"] == "apply_clone"
        assert result["success"] is True
        # workspace permissions.update was called for the cluster
        ws.permissions.update.assert_called()
        rot, oid = ws.permissions.update.call_args.args[:2]
        assert rot == "clusters"
        assert oid == "c1"
        # secret scope put_acl was called
        ws.secrets.put_acl.assert_called_once()
        kwargs = ws.secrets.put_acl.call_args.kwargs
        assert kwargs["scope"] == "prod-secrets"
        assert kwargs["principal"] == "carol@example.com"

    def test_apply_clone_collects_errors(self):
        from marimo_databricks_connect._acl_widget import AclWidget

        ws = _make_ws_for_scan()
        ws.permissions.update.side_effect = RuntimeError("forbidden")
        w = AclWidget(workspace_client=ws)
        w.request = json.dumps(
            {
                "action": "scan_principal",
                "principal": "alice@example.com",
                "categories": ["clusters"],
                "deep": [],
            }
        )
        w.request = json.dumps({"action": "apply_clone", "to_principal": "carol@example.com"})
        result = json.loads(w.clone_result)
        assert result["success"] is False
        assert any("forbidden" in e for e in result["errors"])

    def test_deep_uc_for_picker_lists_schemas(self):
        from marimo_databricks_connect._acl_widget import AclWidget

        ws = _make_ws_for_scan()
        ws.schemas.list.return_value = [
            SimpleNamespace(name="bronze", full_name="main.bronze"),
            SimpleNamespace(name="silver", full_name="main.silver"),
        ]
        w = AclWidget(workspace_client=ws)
        w.request = json.dumps({"action": "deep_scan_uc", "category_id": "uc-schema", "catalog": "main"})
        data = json.loads(w.securable_list_data)
        assert {it["item_id"] for it in data["items"]} == {"main.bronze", "main.silver"}

    def test_deep_uc_for_picker_lists_tables(self):
        from marimo_databricks_connect._acl_widget import AclWidget

        ws = _make_ws_for_scan()
        ws.tables.list.return_value = [
            SimpleNamespace(name="events", full_name="main.bronze.events"),
        ]
        w = AclWidget(workspace_client=ws)
        w.request = json.dumps(
            {"action": "deep_scan_uc", "category_id": "uc-table", "catalog": "main", "schema": "bronze"}
        )
        data = json.loads(w.securable_list_data)
        assert data["items"][0]["item_id"] == "main.bronze.events"

    def test_deep_principal_scan_walks_catalogs_and_schemas(self):
        from marimo_databricks_connect._acl_widget import AclWidget

        ws = _make_ws_for_scan()
        # Set up catalogs -> schemas -> tables
        ws.schemas.list.side_effect = lambda catalog_name: (
            [SimpleNamespace(name="bronze", full_name=f"{catalog_name}.bronze")] if catalog_name == "main" else []
        )
        ws.tables.list.return_value = [
            SimpleNamespace(name="events", full_name="main.bronze.events"),
        ]
        ws.volumes.list.return_value = []
        ws.functions.list.return_value = []

        # alice has SELECT on main.bronze.events
        original = ws.grants.get_effective.side_effect

        def grants_with_table(st, fn):
            stn = getattr(st, "name", str(st))
            if stn == "TABLE" and fn == "main.bronze.events":
                return _uc_perms([_uc_priv_assignment("alice@example.com", ["SELECT", "MODIFY"])])
            if stn == "SCHEMA":
                return _uc_perms([])
            return original(st, fn)

        ws.grants.get_effective.side_effect = grants_with_table

        w = AclWidget(workspace_client=ws)
        w.request = json.dumps(
            {
                "action": "scan_principal",
                "principal": "alice@example.com",
                "categories": [],
                "deep": ["uc-table"],
            }
        )
        scan = json.loads(w.principal_scan_data)
        rows = scan["rows"]
        assert any(r["category_id"] == "uc-table" and r["item_id"] == "main.bronze.events" for r in rows)

    def test_invalid_action_does_not_raise(self):
        from marimo_databricks_connect._acl_widget import AclWidget

        ws = MagicMock()
        w = AclWidget(workspace_client=ws)
        # Garbage JSON → ignored
        w.request = "not-json"
        # Unknown action → ignored silently
        w.request = json.dumps({"action": "bogus"})
