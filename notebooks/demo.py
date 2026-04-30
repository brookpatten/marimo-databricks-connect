import marimo

__generated_with = "0.23.3"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    from marimo_databricks_connect import dbfs, dbutils, external_location, spark, exclude_catalogs, show_all_catalogs, external_location_widget, secret_scope_widget, table_widget, acl_widget, principal_widget, genie_widget, include_catalogs, prefetch, workspace_widget, workspace


    return (
        acl_widget,
        external_location,
        external_location_widget,
        genie_widget,
        include_catalogs,
        mo,
        prefetch,
        principal_widget,
        secret_scope_widget,
        spark,
        table_widget,
        workspace_widget,
    )


@app.cell
def _(acl_widget):
    acl_widget()
    return


@app.cell
def _(include_catalogs, prefetch):
    include_catalogs("samples")                                                              
    prefetch()
    return


@app.cell
def _(genie_widget):
    genie_widget("01f048b8787017b5b7e96c0961f66281")
    return


@app.cell
def _(principal_widget):
    principal_widget("foo@bar.com")
    return


@app.cell
def _(workspace_widget):
    workspace_widget()
    return


@app.cell
def _(external_location):
    es = external_location("abfss://devops@foo.dfs.core.windows.net")
    return


@app.cell
def _(secret_scope_widget):
    widget = secret_scope_widget("secrets")
    widget
    return


@app.cell
def _(table_widget):
    table_widget("samples.nyctaxi.trips")
    return


@app.cell
def _(spark):
    # Lazy Spark DataFrame — marimo renders it without materialising the whole table.
    trips = spark.read.table("samples.nyctaxi.trips").limit(25)
    trips
    return


@app.cell
def _(external_location, include_catalogs):
    # Filter the Data sources panel. Default = current catalog only; add more
    # by name or fnmatch glob. Excludes always win. Filtering is panel-only;
    # SQL execution remains workspace-wide.


    include_catalogs("system")
    # exclude_catalogs("system", "__databricks_*")

    # landing = external_location("landing_zone")
    _ = external_location  # keeps the import "used" without doing anything yet
    return


@app.cell
def _():
    from marimo_databricks_connect import compute_widget                                                                            

    compute_widget()  
    return


@app.cell
def _():
    from marimo_databricks_connect import workflows_widget                                                                          

    workflows_widget()   
    return


@app.cell
def _():
    from marimo_databricks_connect import unity_catalog_widget

    unity_catalog_widget()
    return


@app.cell
def _(external_location_widget):
    external_location_widget("__databricks_managed_storage_location")
    return


@app.cell
def _(mo, spark):
    fares = mo.sql(
        f"""
        SELECT
            pickup_zip,
            count(*)              AS trip_count,
            round(avg(fare_amount), 2) AS avg_fare
        FROM samples.nyctaxi.trips
        GROUP BY pickup_zip
        ORDER BY trip_count DESC
        LIMIT 10
        """,
        engine=spark
    )
    return


if __name__ == "__main__":
    app.run()
