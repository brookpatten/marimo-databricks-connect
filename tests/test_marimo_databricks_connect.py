"""Unit tests for marimo_databricks_connect (no live Databricks required)."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

# ---------- SparkConnectEngine ---------------------------------------------


def _fake_row(values, names=None):
    """Return an object that behaves like a pyspark Row for our engine code."""
    row = MagicMock()
    row.__getitem__.side_effect = lambda i: values[i]
    row.__len__.return_value = len(values)
    if names is not None:
        row.asDict.return_value = dict(zip(names, values))
    else:
        row.asDict.return_value = {}
    return row


def _fake_spark(sql_responses):
    """Build a fake SparkSession whose .sql(q) returns a fake DataFrame.

    sql_responses: dict mapping exact SQL string -> list of (values, names) tuples.
    """
    spark = MagicMock()

    def fake_sql(q):
        rows = [_fake_row(values, names) for (values, names) in sql_responses.get(q, [])]
        df = MagicMock()
        df.collect.return_value = rows
        return df

    spark.sql.side_effect = fake_sql
    return spark


def test_engine_is_compatible_with_classic_spark_session():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    from marimo_databricks_connect._engine import SparkConnectEngine

    s = MagicMock(spec=SparkSession)
    assert SparkConnectEngine.is_compatible(s) is True
    assert SparkConnectEngine.is_compatible("not a spark") is False


def test_engine_is_compatible_with_spark_connect_session():
    """databricks-connect returns a Spark *Connect* session, a separate class."""
    pytest.importorskip("pyspark")
    from pyspark.sql.connect.session import SparkSession as ConnectSparkSession

    from marimo_databricks_connect._engine import SparkConnectEngine

    s = MagicMock(spec=ConnectSparkSession)
    assert SparkConnectEngine.is_compatible(s) is True


def test_engine_source_and_dialect():
    from marimo_databricks_connect._engine import SparkConnectEngine

    eng = SparkConnectEngine(MagicMock())
    assert eng.source == "databricks-connect"
    assert eng.dialect == "databricks"


def test_engine_execute_delegates_to_spark_sql():
    from marimo_databricks_connect._engine import SparkConnectEngine

    spark = MagicMock()
    eng = SparkConnectEngine(spark)
    result = eng.execute("SELECT 1")
    spark.sql.assert_called_once_with("SELECT 1")
    assert result is spark.sql.return_value


def test_engine_get_databases_show_all(clean_filter):
    """With show_all_catalogs(), every catalog from SHOW CATALOGS is surfaced."""
    from marimo_databricks_connect import show_all_catalogs
    from marimo_databricks_connect._engine import SparkConnectEngine

    show_all_catalogs()
    spark = _fake_spark(
        {
            "SHOW CATALOGS": [(["main"], None), (["samples"], None)],
            "SELECT current_catalog()": [(["main"], None)],
        }
    )
    eng = SparkConnectEngine(spark)
    dbs = eng.get_databases(
        include_schemas=False,
        include_tables=False,
        include_table_details=False,
    )
    assert [d.name for d in dbs] == ["main", "samples"]
    assert all(d.dialect == "databricks" for d in dbs)
    assert all(d.schemas == [] for d in dbs)


def test_engine_get_tables_in_schema():
    from marimo_databricks_connect._engine import SparkConnectEngine

    spark = _fake_spark(
        {
            "SHOW TABLES IN `samples`.`nyctaxi`": [
                (["nyctaxi", "trips", False], ["database", "tableName", "isTemporary"]),
                (["nyctaxi", "fares", False], ["database", "tableName", "isTemporary"]),
            ],
        }
    )
    eng = SparkConnectEngine(spark)
    tables = eng.get_tables_in_schema(schema="nyctaxi", database="samples", include_table_details=False)
    assert [t.name for t in tables] == ["trips", "fares"]
    assert all(t.source_type == "connection" for t in tables)


def test_engine_get_table_details_parses_describe():
    from marimo_databricks_connect._engine import SparkConnectEngine

    cols = ["col_name", "data_type", "comment"]
    spark = _fake_spark(
        {
            "DESCRIBE TABLE `samples`.`nyctaxi`.`trips`": [
                (["pickup_zip", "int", ""], cols),
                (["fare_amount", "double", ""], cols),
                (["", "", ""], cols),
                (["# Partition Information", "", ""], cols),
            ],
        }
    )
    eng = SparkConnectEngine(spark)
    detail = eng.get_table_details(table_name="trips", schema_name="nyctaxi", database_name="samples")
    assert detail is not None
    assert detail.name == "trips"
    assert [c.name for c in detail.columns] == ["pickup_zip", "fare_amount"]
    assert [c.external_type for c in detail.columns] == ["int", "double"]


# ---------- DbutilsFileSystem ----------------------------------------------


def _file_info(path, name, size, is_dir=False):
    return SimpleNamespace(
        path=path,
        name=name + ("/" if is_dir else ""),
        size=size,
        modificationTime=1700000000000,
    )


def test_fs_ls_returns_fsspec_dicts():
    from marimo_databricks_connect._fs import DbutilsFileSystem

    dbu = MagicMock()
    dbu.fs.ls.return_value = [
        _file_info("/Volumes/main/raw/x.parquet", "x.parquet", 100),
        _file_info("/Volumes/main/raw/sub", "sub", 0, is_dir=True),
    ]
    fs = DbutilsFileSystem(dbutils=dbu, spark=None, root="/Volumes")

    out = fs.ls("/Volumes/main/raw", detail=True)
    assert out[0]["name"] == "/Volumes/main/raw/x.parquet"
    assert out[0]["type"] == "file"
    assert out[0]["size"] == 100
    assert out[1]["type"] == "directory"


def test_fs_ls_relative_path_uses_root():
    from marimo_databricks_connect._fs import DbutilsFileSystem

    dbu = MagicMock()
    dbu.fs.ls.return_value = []
    fs = DbutilsFileSystem(dbutils=dbu, root="/Volumes/main")
    fs.ls("raw")
    dbu.fs.ls.assert_called_once_with("/Volumes/main/raw")


def test_fs_ls_abfss_does_not_call_dbutils():
    """Abfss paths must bypass dbutils (which the SDK rejects with abfss)."""
    from marimo_databricks_connect._fs import DbutilsFileSystem

    dbu = MagicMock()
    dbu.fs.ls.side_effect = AssertionError("dbutils must not be called for abfss")
    spark = MagicMock()
    chain = spark.read.format.return_value.option.return_value.option.return_value.load.return_value.select.return_value.limit.return_value
    chain.collect.return_value = []
    fs = DbutilsFileSystem(dbutils=dbu, spark=spark, root="/Volumes")
    fs.ls("abfss://c@acct.dfs.core.windows.net/data")
    spark.read.format.assert_called_with("binaryFile")


def test_fs_ls_missing_raises_filenotfound():
    from marimo_databricks_connect._fs import DbutilsFileSystem

    dbu = MagicMock()
    dbu.fs.ls.side_effect = RuntimeError("path does not exist")
    fs = DbutilsFileSystem(dbutils=dbu, root="/Volumes")
    with pytest.raises(FileNotFoundError):
        fs.ls("/Volumes/missing")


def test_fs_root_marker():
    from marimo_databricks_connect._fs import DbutilsFileSystem

    fs = DbutilsFileSystem(dbutils=MagicMock(), root="/Volumes/main")
    assert fs.root_marker == "/Volumes/main"


def test_fs_is_recognised_by_marimo_fsspec_adapter():
    """Marimo's storage browser auto-detects any fsspec.AbstractFileSystem."""
    from fsspec.spec import AbstractFileSystem

    from marimo_databricks_connect._fs import DbutilsFileSystem

    fs = DbutilsFileSystem(dbutils=MagicMock())
    assert isinstance(fs, AbstractFileSystem)


def test_fs_needs_spark_listing_detection():
    from marimo_databricks_connect._fs import DbutilsFileSystem

    needs = DbutilsFileSystem._needs_spark_listing
    assert needs("abfss://c@acct.dfs.core.windows.net/data") is True
    assert needs("s3://bucket/key") is True
    assert needs("gs://bucket/key") is True
    assert needs("/Volumes/main/raw") is False
    assert needs("dbfs:/tmp/x") is False
    assert needs("file:///tmp/x") is False


def test_fs_ls_abfss_uses_spark_and_groups_subdirs():
    """Listing an abfss path goes through Spark, not dbutils."""
    from datetime import datetime, timezone

    from marimo_databricks_connect._fs import DbutilsFileSystem

    base = "abfss://devops@acct.dfs.core.windows.net/root"
    mtime = datetime(2024, 1, 1, tzinfo=timezone.utc)

    def _row(path, length):
        r = MagicMock()
        r.__getitem__.side_effect = lambda k: {
            "path": path,
            "length": length,
            "modificationTime": mtime,
        }[k]
        return r

    spark = MagicMock()
    chain = spark.read.format.return_value.option.return_value.option.return_value.load.return_value.select.return_value.limit.return_value
    chain.collect.return_value = [
        _row(f"{base}/a.parquet", 100),
        _row(f"{base}/b.parquet", 200),
        _row(f"{base}/sub/c.parquet", 300),  # implies 'sub/' directory
        _row(f"{base}/sub/d.parquet", 400),  # dedup -> still one 'sub/' entry
    ]

    dbu = MagicMock()
    dbu.fs.ls.side_effect = AssertionError("dbutils must NOT be used for abfss paths")

    fs = DbutilsFileSystem(dbutils=dbu, spark=spark, root=base)
    out = fs.ls(base, detail=True)

    by_name = {e["name"]: e for e in out}
    assert set(by_name) == {
        f"{base}/a.parquet",
        f"{base}/b.parquet",
        f"{base}/sub",
    }
    assert by_name[f"{base}/a.parquet"]["type"] == "file"
    assert by_name[f"{base}/a.parquet"]["size"] == 100
    assert by_name[f"{base}/sub"]["type"] == "directory"
    spark.read.format.assert_called_with("binaryFile")


def test_fs_ls_abfss_failure_raises_filenotfound():
    from marimo_databricks_connect._fs import DbutilsFileSystem

    spark = MagicMock()
    spark.read.format.side_effect = RuntimeError("path not found")
    fs = DbutilsFileSystem(dbutils=MagicMock(), spark=spark, root="abfss://x/y")
    with pytest.raises(FileNotFoundError):
        fs.ls("abfss://x/y")


# ---------- catalog filter -------------------------------------------------


@pytest.fixture
def clean_filter():
    from marimo_databricks_connect._filter import _filter

    _filter.reset()
    yield _filter
    _filter.reset()


def test_pattern_parse_catalog_only():
    from marimo_databricks_connect._filter import _Pattern

    p = _Pattern.parse("main")
    assert p.catalog == "main" and p.schema is None


def test_pattern_parse_with_schema():
    from marimo_databricks_connect._filter import _Pattern

    p = _Pattern.parse("*_dev.bronze_*")
    assert p.catalog == "*_dev" and p.schema == "bronze_*"


def test_pattern_parse_invalid():
    from marimo_databricks_connect._filter import _Pattern

    with pytest.raises(ValueError):
        _Pattern.parse("main.")
    with pytest.raises(ValueError):
        _Pattern.parse(".bronze")
    with pytest.raises(ValueError):
        _Pattern.parse("")


def test_filter_default_returns_only_current_catalog(clean_filter):
    f = clean_filter
    out = f.filter_catalogs(["a", "b", "main", "system"], default_catalog="main")
    assert out == ["main"]


def test_filter_show_all_returns_everything(clean_filter):
    f = clean_filter
    f.show_all = True
    assert f.filter_catalogs(["a", "b", "c"], default_catalog="a") == ["a", "b", "c"]


def test_filter_includes_with_glob(clean_filter):
    f = clean_filter
    f.add_includes(["dev_*", "main"])
    out = f.filter_catalogs(["main", "dev_alice", "dev_bob", "prod", "system"], default_catalog="main")
    assert out == ["main", "dev_alice", "dev_bob"]


def test_filter_excludes_take_precedence_over_includes(clean_filter):
    f = clean_filter
    f.add_includes(["dev_*"])
    f.add_excludes(["dev_internal_*"])
    out = f.filter_catalogs(
        ["dev_alice", "dev_internal_x", "dev_internal_y", "dev_bob"],
        default_catalog="x",
    )
    assert out == ["dev_alice", "dev_bob"]


def test_filter_excludes_when_show_all(clean_filter):
    f = clean_filter
    f.show_all = True
    f.add_excludes(["system", "__databricks_*"])
    out = f.filter_catalogs(["main", "system", "__databricks_internal", "dev"], default_catalog="main")
    assert out == ["main", "dev"]


def test_filter_schema_includes(clean_filter):
    f = clean_filter
    f.add_includes(["main.bronze_*", "main.silver"])
    out = f.filter_schemas("main", ["bronze_a", "bronze_b", "silver", "gold"])
    assert out == ["bronze_a", "bronze_b", "silver"]


def test_filter_schema_includes_with_catalog_glob(clean_filter):
    f = clean_filter
    f.add_includes(["*_dev.bronze_*"])
    out = f.filter_schemas("alice_dev", ["bronze_raw", "bronze_clean", "silver"])
    assert out == ["bronze_raw", "bronze_clean"]
    # Different catalog — no schema-pattern matches.
    assert f.filter_schemas("alice_prod", ["bronze_raw"]) == []


def test_filter_catalog_wide_include_keeps_all_schemas(clean_filter):
    f = clean_filter
    f.add_includes(["main"])  # no schema part
    out = f.filter_schemas("main", ["a", "b", "c"])
    assert out == ["a", "b", "c"]


def test_filter_schema_excludes(clean_filter):
    f = clean_filter
    f.add_includes(["main"])
    f.add_excludes(["main.__internal_*"])
    out = f.filter_schemas("main", ["bronze", "silver", "__internal_a", "__internal_b"])
    assert out == ["bronze", "silver"]


def test_filter_schema_scoped_exclude_does_not_hide_catalog(clean_filter):
    f = clean_filter
    f.add_includes(["main"])
    f.add_excludes(["main.system"])  # only schema-scoped
    assert f.filter_catalogs(["main", "other"], default_catalog="main") == ["main"]


def test_filter_catalog_wide_exclude_hides_catalog(clean_filter):
    f = clean_filter
    f.add_excludes(["system"])
    f.show_all = True
    assert f.filter_catalogs(["main", "system"], default_catalog="main") == ["main"]


def test_filter_set_includes_replaces(clean_filter):
    f = clean_filter
    f.set_includes(["main", "dev"])
    assert [p.catalog for p in f.includes] == ["main", "dev"]
    f.set_includes(["prod"])
    assert [p.catalog for p in f.includes] == ["prod"]


def test_filter_set_excludes_replaces(clean_filter):
    f = clean_filter
    f.set_excludes(["system"])
    assert [p.catalog for p in f.excludes] == ["system"]
    f.set_excludes(["__internal"])
    assert [p.catalog for p in f.excludes] == ["__internal"]


def test_include_catalogs_replaces_previous_call(clean_filter):
    """Calling include_catalogs() a second time replaces, not appends."""
    from marimo_databricks_connect import include_catalogs

    include_catalogs("main")
    include_catalogs("dev_*")
    f = clean_filter
    # Only "dev_*" should remain.
    assert len(f.includes) == 1
    assert f.includes[0].catalog == "dev_*"


def test_exclude_catalogs_replaces_previous_call(clean_filter):
    from marimo_databricks_connect import exclude_catalogs

    exclude_catalogs("system")
    exclude_catalogs("__internal")
    f = clean_filter
    assert len(f.excludes) == 1
    assert f.excludes[0].catalog == "__internal"


def test_narrowing_from_catalog_wide_to_schema_scoped(clean_filter):
    """Replacing a catalog-wide include with schema-scoped one filters schemas."""
    from marimo_databricks_connect import include_catalogs

    include_catalogs("main")  # catalog-wide: all schemas visible
    assert clean_filter.filter_schemas("main", ["a", "b", "c"]) == ["a", "b", "c"]

    include_catalogs("main.a")  # narrow to schema "a" only
    assert clean_filter.filter_schemas("main", ["a", "b", "c"]) == ["a"]


# ---------- engine + filter integration ------------------------------------


def test_engine_list_catalogs_default_uses_only_current(clean_filter):
    from marimo_databricks_connect._engine import SparkConnectEngine

    spark = _fake_spark(
        {
            "SELECT current_catalog()": [(["my_default"], None)],
            # SHOW CATALOGS deliberately not stubbed: must not be called by default.
        }
    )
    eng = SparkConnectEngine(spark)
    dbs = eng.get_databases(include_schemas=False, include_tables=False, include_table_details=False)
    assert [d.name for d in dbs] == ["my_default"]


def test_engine_list_catalogs_with_includes_calls_show_catalogs(clean_filter):
    from marimo_databricks_connect import include_catalogs
    from marimo_databricks_connect._engine import SparkConnectEngine

    include_catalogs("dev_*", "main")
    spark = _fake_spark(
        {
            "SHOW CATALOGS": [
                (["main"], None),
                (["dev_a"], None),
                (["dev_b"], None),
                (["prod"], None),
                (["system"], None),
            ],
            "SELECT current_catalog()": [(["main"], None)],
        }
    )
    eng = SparkConnectEngine(spark)
    names = [
        d.name
        for d in eng.get_databases(
            include_schemas=False,
            include_tables=False,
            include_table_details=False,
        )
    ]
    assert names == ["main", "dev_a", "dev_b"]


def test_engine_list_schemas_applies_filter(clean_filter):
    from marimo_databricks_connect import include_catalogs
    from marimo_databricks_connect._engine import SparkConnectEngine

    include_catalogs("main.bronze_*")
    spark = _fake_spark(
        {
            "SHOW SCHEMAS IN `main`": [
                (["bronze_raw"], None),
                (["bronze_clean"], None),
                (["silver"], None),
                (["gold"], None),
            ],
        }
    )
    eng = SparkConnectEngine(spark)
    schemas = eng._list_schemas("main")
    assert schemas == ["bronze_raw", "bronze_clean"]


def test_env_var_loads_initial_includes(monkeypatch):
    monkeypatch.setenv("MARIMO_DBC_INCLUDE_CATALOGS", "main, dev_*")
    monkeypatch.setenv("MARIMO_DBC_EXCLUDE_CATALOGS", "system")
    monkeypatch.setenv("MARIMO_DBC_SHOW_ALL_CATALOGS", "")
    from marimo_databricks_connect import _filter as f_mod

    f_mod._filter.reset()
    f_mod._load_initial_defaults()
    try:
        assert [p.catalog for p in f_mod._filter.includes] == ["main", "dev_*"]
        assert [p.catalog for p in f_mod._filter.excludes] == ["system"]
        assert f_mod._filter.show_all is False
    finally:
        f_mod._filter.reset()


def test_env_var_show_all(monkeypatch):
    monkeypatch.setenv("MARIMO_DBC_SHOW_ALL_CATALOGS", "true")
    from marimo_databricks_connect import _filter as f_mod

    f_mod._filter.reset()
    f_mod._load_initial_defaults()
    try:
        assert f_mod._filter.show_all is True
    finally:
        f_mod._filter.reset()


# ---------- registration ---------------------------------------------------


def test_external_location_passes_through_abfss_path():
    import marimo_databricks_connect as pkg

    pkg._cache["spark"] = MagicMock()
    pkg._cache["dbutils"] = MagicMock()
    try:
        fs = pkg.external_location("abfss://c@acct.dfs.core.windows.net/data")
        assert fs.root_marker == "abfss://c@acct.dfs.core.windows.net/data"
    finally:
        pkg._cache.clear()


def test_external_location_passes_through_volumes_path():
    import marimo_databricks_connect as pkg

    pkg._cache["spark"] = MagicMock()
    pkg._cache["dbutils"] = MagicMock()
    try:
        fs = pkg.external_location("/Volumes/main/raw/landing")
        assert fs.root_marker == "/Volumes/main/raw/landing"
    finally:
        pkg._cache.clear()


def test_external_location_resolves_uc_name_via_describe():
    import marimo_databricks_connect as pkg

    url = "abfss://devops@sa01flexflowdppc01dev.dfs.core.windows.net/"
    expected_root = url.rstrip("/")  # DbutilsFileSystem strips trailing slash
    row = MagicMock()
    row.asDict.return_value = {
        "name": "my_loc",
        "url": url,
        "credential_name": "cred",
        "read_only": False,
    }
    spark = MagicMock()
    spark.sql.return_value.collect.return_value = [row]

    pkg._cache["spark"] = spark
    pkg._cache["dbutils"] = MagicMock()
    try:
        fs = pkg.external_location("my_loc")
        spark.sql.assert_called_once_with("DESCRIBE EXTERNAL LOCATION `my_loc`")
        assert fs.root_marker == expected_root
    finally:
        pkg._cache.clear()


def test_external_location_raises_when_describe_fails():
    import marimo_databricks_connect as pkg

    spark = MagicMock()
    spark.sql.side_effect = RuntimeError("location not found")
    pkg._cache["spark"] = spark
    pkg._cache["dbutils"] = MagicMock()
    try:
        with pytest.raises(ValueError, match="Could not resolve external location"):
            pkg.external_location("nope")
    finally:
        pkg._cache.clear()


def test_engine_registered_with_marimo_on_import():
    from marimo._sql.get_engines import SUPPORTED_ENGINES

    import marimo_databricks_connect  # noqa: F401  (triggers registration)
    from marimo_databricks_connect._engine import SparkConnectEngine

    assert SparkConnectEngine in SUPPORTED_ENGINES
    # Idempotent: importing again does not duplicate.
    import importlib

    import marimo_databricks_connect as pkg

    importlib.reload(pkg)
    assert SUPPORTED_ENGINES.count(SparkConnectEngine) == 1


# ---------- Bulk metadata discovery (information_schema) -------------------


def _info_schema_query(catalog):
    return (
        f"SELECT table_schema, table_name, column_name, data_type, ordinal_position "
        f"FROM `{catalog}`.information_schema.columns "
        f"ORDER BY table_schema, table_name, ordinal_position"
    )


def _schemata_query(catalog):
    return f"SELECT schema_name FROM `{catalog}`.information_schema.schemata"


def _bulk_spark(catalog="main", *, columns_rows=None, schemata_rows=None, fail_columns=False):
    """Build a fake spark with a populated information_schema."""
    cols = ["table_schema", "table_name", "column_name", "data_type", "ordinal_position"]
    responses = {
        "SELECT current_catalog()": [([catalog], None)],
    }
    if not fail_columns:
        responses[_info_schema_query(catalog)] = [(list(r), cols) for r in (columns_rows or [])]
    if schemata_rows is not None:
        responses[_schemata_query(catalog)] = [([s], None) for s in schemata_rows]

    spark = MagicMock()

    def fake_sql(q):
        if q not in responses:
            if fail_columns and q == _info_schema_query(catalog):
                raise RuntimeError("info_schema unavailable")
            # Default: empty result for unknown queries.
            df = MagicMock()
            df.collect.return_value = []
            return df
        df = MagicMock()
        df.collect.return_value = [_fake_row(values, names) for (values, names) in responses[q]]
        return df

    spark.sql.side_effect = fake_sql
    return spark


def test_bulk_metadata_populates_schemas_tables_columns(clean_filter):
    """A single information_schema.columns query feeds the whole catalog."""
    from marimo_databricks_connect._engine import SparkConnectEngine

    spark = _bulk_spark(
        "main",
        columns_rows=[
            ("bronze", "events", "id", "BIGINT", 1),
            ("bronze", "events", "ts", "TIMESTAMP", 2),
            ("bronze", "users", "id", "BIGINT", 1),
            ("bronze", "users", "email", "STRING", 2),
            ("silver", "agg", "day", "DATE", 1),
        ],
    )
    eng = SparkConnectEngine(spark)
    dbs = eng.get_databases(include_schemas=True, include_tables=True, include_table_details=True)

    assert [d.name for d in dbs] == ["main"]
    schemas = {s.name: s for s in dbs[0].schemas}
    assert set(schemas) == {"bronze", "silver"}
    bronze_tables = {t.name: t for t in schemas["bronze"].tables}
    assert set(bronze_tables) == {"events", "users"}
    events_cols = bronze_tables["events"].columns
    assert [c.name for c in events_cols] == ["id", "ts"]
    assert [c.external_type for c in events_cols] == ["BIGINT", "TIMESTAMP"]


def test_bulk_metadata_includes_empty_schemas(clean_filter):
    """Schemas with no tables show up via the schemata fallback query."""
    from marimo_databricks_connect._engine import SparkConnectEngine

    spark = _bulk_spark(
        "main",
        columns_rows=[("bronze", "events", "id", "BIGINT", 1)],
        schemata_rows=["bronze", "silver", "empty_schema"],
    )
    eng = SparkConnectEngine(spark)
    dbs = eng.get_databases(include_schemas=True, include_tables=True, include_table_details=False)
    schema_names = {s.name for s in dbs[0].schemas}
    assert "empty_schema" in schema_names


def test_bulk_metadata_falls_back_to_show_when_info_schema_fails(clean_filter):
    """Catalogs without information_schema (e.g. ``samples``) use SHOW/DESCRIBE."""
    from marimo_databricks_connect._engine import SparkConnectEngine

    spark = _fake_spark(
        {
            "SELECT current_catalog()": [(["samples"], None)],
            "SHOW SCHEMAS IN `samples`": [(["nyctaxi"], None)],
            "SHOW TABLES IN `samples`.`nyctaxi`": [
                (["nyctaxi", "trips", False], ["database", "tableName", "isTemporary"]),
            ],
        }
    )
    # Make the info_schema columns query throw to simulate samples.
    real_sql = spark.sql.side_effect

    def sql_with_info_schema_failure(q):
        if "information_schema.columns" in q:
            raise RuntimeError("not found")
        return real_sql(q)

    spark.sql.side_effect = sql_with_info_schema_failure

    eng = SparkConnectEngine(spark)
    dbs = eng.get_databases(include_schemas=True, include_tables=True, include_table_details=False)
    schemas = {s.name: s for s in dbs[0].schemas}
    assert "nyctaxi" in schemas
    assert [t.name for t in schemas["nyctaxi"].tables] == ["trips"]


def test_metadata_cache_avoids_repeat_information_schema_calls(clean_filter):
    """Repeated discovery calls hit the in-process TTL cache, not Spark."""
    from marimo_databricks_connect._engine import SparkConnectEngine

    spark = _bulk_spark(
        "main",
        columns_rows=[("bronze", "events", "id", "BIGINT", 1)],
    )
    eng = SparkConnectEngine(spark)
    eng.get_databases(include_schemas=True, include_tables=True, include_table_details=True)
    eng.get_databases(include_schemas=True, include_tables=True, include_table_details=True)
    eng.get_databases(include_schemas=True, include_tables=True, include_table_details=True)

    info_schema_calls = sum(1 for c in spark.sql.call_args_list if "information_schema.columns" in c.args[0])
    assert info_schema_calls == 1


def test_refresh_invalidates_cache(clean_filter):
    from marimo_databricks_connect._engine import SparkConnectEngine

    spark = _bulk_spark(
        "main",
        columns_rows=[("bronze", "events", "id", "BIGINT", 1)],
    )
    eng = SparkConnectEngine(spark)
    eng.get_databases(include_schemas=True, include_tables=True, include_table_details=True)
    eng.refresh("main")
    eng.get_databases(include_schemas=True, include_tables=True, include_table_details=True)

    info_schema_calls = sum(1 for c in spark.sql.call_args_list if "information_schema.columns" in c.args[0])
    assert info_schema_calls == 2


def test_prefetch_returns_table_count(clean_filter):
    from marimo_databricks_connect._engine import SparkConnectEngine

    spark = _bulk_spark(
        "main",
        columns_rows=[
            ("bronze", "events", "id", "BIGINT", 1),
            ("bronze", "users", "id", "BIGINT", 1),
            ("silver", "agg", "day", "DATE", 1),
        ],
    )
    eng = SparkConnectEngine(spark)
    summary = eng.prefetch("main")
    assert summary == {"main": 3}


def test_get_table_details_uses_cached_columns(clean_filter):
    """``get_table_details`` should serve from cache when available — no
    extra DESCRIBE round-trip."""
    from marimo_databricks_connect._engine import SparkConnectEngine

    spark = _bulk_spark(
        "main",
        columns_rows=[
            ("bronze", "events", "id", "BIGINT", 1),
            ("bronze", "events", "ts", "TIMESTAMP", 2),
        ],
    )
    eng = SparkConnectEngine(spark)
    eng.prefetch("main")
    pre_calls = len(spark.sql.call_args_list)
    detail = eng.get_table_details(table_name="events", schema_name="bronze", database_name="main")
    assert detail is not None
    assert [c.name for c in detail.columns] == ["id", "ts"]
    # No additional Spark queries should have happened.
    assert len(spark.sql.call_args_list) == pre_calls


def test_auto_resolves_schemas_and_tables_eagerly_columns_lazily(clean_filter):
    """Default behavior: schemas+tables surface for completion; columns
    lazy-load unless include_catalogs has narrowed scope."""
    from marimo_databricks_connect._engine import SparkConnectEngine

    spark = _bulk_spark(
        "main",
        columns_rows=[
            ("bronze", "events", "id", "BIGINT", 1),
            ("bronze", "events", "ts", "TIMESTAMP", 2),
        ],
    )
    eng = SparkConnectEngine(spark)
    # All three flags = "auto" mimics what marimo passes from the default config.
    dbs = eng.get_databases(
        include_schemas="auto",
        include_tables="auto",
        include_table_details="auto",
    )
    bronze = next(s for s in dbs[0].schemas if s.name == "bronze")
    events = next(t for t in bronze.tables if t.name == "events")
    # Schemas + tables present...
    assert [t.name for t in bronze.tables] == ["events"]
    # ...but columns deferred (no narrow include_catalogs scope set).
    assert events.columns == []


def test_auto_resolves_columns_eagerly_when_include_catalogs_narrows_scope(clean_filter):
    from marimo_databricks_connect import include_catalogs
    from marimo_databricks_connect._engine import SparkConnectEngine

    include_catalogs("main")
    spark = _bulk_spark(
        "main",
        columns_rows=[("bronze", "events", "id", "BIGINT", 1)],
    )
    # _list_catalogs uses SHOW CATALOGS when includes is set; stub it.
    base_sql = spark.sql.side_effect

    def sql_with_show(q):
        if q == "SHOW CATALOGS":
            df = MagicMock()
            df.collect.return_value = [_fake_row(["main"], None)]
            return df
        return base_sql(q)

    spark.sql.side_effect = sql_with_show

    eng = SparkConnectEngine(spark)
    dbs = eng.get_databases(
        include_schemas="auto",
        include_tables="auto",
        include_table_details="auto",
    )
    events = dbs[0].schemas[0].tables[0]
    assert [c.name for c in events.columns] == ["id"]


def test_prefetch_helper_at_package_level(clean_filter):
    import marimo_databricks_connect as pkg

    spark = _bulk_spark(
        "main",
        columns_rows=[("bronze", "events", "id", "BIGINT", 1)],
    )
    pkg._cache.clear()
    pkg._cache["spark"] = spark
    try:
        # Also warm the engine singleton.
        eng = pkg.engine  # noqa: F841
        summary = pkg.prefetch("main")
        assert summary == {"main": 1}
        pkg.refresh_metadata("main")
    finally:
        pkg._cache.clear()
