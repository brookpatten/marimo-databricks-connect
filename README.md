# marimo-databricks-connect

This package provides compatibility for marimo notebooks to use databricks all purpose serverless compute.  It was designed with the following priorities.

- Connect to databricks using databricks-connect & spark (not sql warehouse)
- Authenticate/configure spark using the default databricks-connect process (env vars, .databrickscfg etc)
- Allow execution of both python & sql
- Allow browsing of catalogs/schemas/tables/columns in the marimo data sources view
- Allow browsing of external locations, volumes, and dbfs in the storage browser


## Quickstart

Authenticate once on your machine:

```bash
az login
```

Then in any notebook in this folder:

```python
import marimo as mo
from marimo_databricks_connect import dbfs, dbutils, external_location, spark, exclude_catalogs, include_catalogs, show_all_catalogs
```

That single import gives you:

- `spark` — a `DatabricksSession` on serverless compute (OAuth, no host/token config).
- `dbutils` — bound to that session.
- external_location - Add external locations to browse in the UI
- include/exclude_catalogs - Show/Hide catalogs in the datasource UI
- `dbfs` — an fsspec filesystem rooted at `/Volumes` that powers the marimo
  **storage browser** via Unity Catalog (no direct ADLS access).
- A registered `SparkConnectEngine` so marimo's **data sources** panel browses
  catalogs / schemas / tables, and SQL cells run on Spark when you pass
  `engine=spark`:
  

  ```python
  mo.sql("SELECT * FROM samples.nyctaxi.trips LIMIT 100", engine=spark)
  ```

## Browsing UC external locations

Add a cell to expose another root in the storage browser:

```python
from marimo_databricks_connect import external_location

landing = external_location("finops_landing")                  # by UC name
raw     = external_location("abfss://c@acct.dfs.core.windows.net/data")  # by path
```

Each variable shows up as its own tree in the storage panel.

## Filtering the data sources panel (catalogs / schemas)

With 1000+ UC catalogs the panel becomes unusable. By **default** only the
*current catalog* (`SELECT current_catalog()`) is surfaced. Add catalogs (or
specific schemas) explicitly with fnmatch globs:

```python
from marimo_databricks_connect import (
    include_catalogs, exclude_catalogs, show_all_catalogs, reset_catalog_filter,
)

include_catalogs("main", "samples")            # exact names
include_catalogs("dev_*", "*_prod")             # globs
include_catalogs("main.bronze_*", "*_dev.silver")  # narrow to specific schemas

exclude_catalogs("system", "__databricks_*")    # always wins over includes

show_all_catalogs()                             # opt out of the allow-list
reset_catalog_filter()                          # back to defaults
```

Filtering only affects the **data sources panel** — `mo.sql(..., engine=spark)`
and `spark.sql(...)` can still query any catalog you have UC permission for.

### Persistent defaults

Set once per project in `pyproject.toml`:

```toml
[tool.marimo_databricks_connect]
include_catalogs = ["main", "dev_*"]
exclude_catalogs = ["system", "__databricks_internal"]
# show_all_catalogs = true
```

…or per shell with environment variables (these *override* `pyproject.toml`):

```bash
export MARIMO_DBC_INCLUDE_CATALOGS="main,dev_*"
export MARIMO_DBC_EXCLUDE_CATALOGS="system"
export MARIMO_DBC_SHOW_ALL_CATALOGS=1
```

## Running

```bash
marimo edit scratch/m.py
```
