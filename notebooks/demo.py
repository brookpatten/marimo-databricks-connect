import marimo

__generated_with = "0.23.3"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    from marimo_databricks_connect import dbfs, dbutils, external_location, spark, exclude_catalogs, include_catalogs, show_all_catalogs


    return exclude_catalogs, external_location, include_catalogs, mo, spark


@app.cell
def _(exclude_catalogs, external_location, include_catalogs):
    # Filter the Data sources panel. Default = current catalog only; add more
    # by name or fnmatch glob. Excludes always win. Filtering is panel-only;
    # SQL execution remains workspace-wide.


    include_catalogs("system")
    # exclude_catalogs("system", "__databricks_*")

    # landing = external_location("landing_zone")
    _ = external_location  # keeps the import "used" without doing anything yet
    return


@app.cell
def _(spark):
    # Lazy Spark DataFrame — marimo renders it without materialising the whole table.
    trips = spark.read.table("samples.nyctaxi.trips").limit(25)
    trips
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
