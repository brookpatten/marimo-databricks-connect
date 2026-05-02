"""Tiny HTML rendering helpers \u2014 no Jinja dep needed for two pages."""

from __future__ import annotations

import html
from typing import Iterable

_PAGE = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>{title}</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
            max-width: 900px; margin: 2rem auto; padding: 0 1rem; color: #222; }}
    h1 {{ margin-bottom: 0.25rem; }}
    .who {{ color: #666; margin-bottom: 1.5rem; font-size: 0.9rem; }}
    .crumbs a {{ color: #06c; text-decoration: none; }}
    .crumbs {{ margin: 1rem 0; }}
    ul.entries {{ list-style: none; padding: 0; }}
    ul.entries li {{ padding: 0.4rem 0.5rem; border-bottom: 1px solid #eee;
                     display: flex; gap: 0.6rem; align-items: center; }}
    ul.entries li:hover {{ background: #f6f8fa; }}
    .icon {{ width: 1.2rem; text-align: center; }}
    a.entry {{ color: #06c; text-decoration: none; flex: 1; }}
    .badge {{ font-size: 0.7rem; color: #888; padding: 0.1rem 0.4rem;
              border: 1px solid #ddd; border-radius: 3px; }}
    .empty {{ color: #888; font-style: italic; }}
    .err {{ background: #fee; border: 1px solid #c33; padding: 1rem;
            border-radius: 4px; color: #900; }}
    .toolbar {{ margin: 1rem 0; }}
    .toolbar .hint {{ color: #666; font-size: 0.8rem; margin-top: 0.3rem; }}
    .path-input {{ width: 22rem; padding: 0.4rem 0.5rem; font-family: inherit;
                   font-size: 0.85rem; border: 1px solid #ccc; border-radius: 3px;
                   margin-right: 0.4rem; }}
    .btn {{ display: inline-block; padding: 0.45rem 0.9rem; border-radius: 4px;
            background: #06c; color: white; text-decoration: none;
            font-size: 0.9rem; border: none; cursor: pointer; }}
    .btn.secondary {{ background: #eef2f6; color: #06c; border: 1px solid #cbd5e1; }}
    .btn:hover {{ background: #048; }}
    .btn.secondary:hover {{ background: #dde6ef; }}
    .btn[disabled] {{ background: #ccc; cursor: not-allowed; }}
    form.inline {{ display: inline; }}
    .drafts {{ margin: 1.5rem 0 1rem; padding: 1rem; background: #fafbfc;
               border: 1px solid #e1e4e8; border-radius: 4px; }}
    .drafts h2 {{ margin: 0 0 0.5rem; font-size: 1rem; }}
    table.drafts-table {{ width: 100%; border-collapse: collapse; font-size: 0.9rem; }}
    table.drafts-table td {{ padding: 0.4rem 0.5rem; border-bottom: 1px solid #eee;
                             vertical-align: middle; }}
    table.drafts-table tr:last-child td {{ border-bottom: none; }}
    .draft-path {{ color: #555; font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
                   font-size: 0.85rem; }}
    .pill {{ display: inline-block; font-size: 0.7rem; padding: 0.1rem 0.45rem;
             border-radius: 10px; }}
    .pill.dirty {{ background: #fff3bf; color: #7c5800; border: 1px solid #f5d76e; }}
    .pill.clean {{ background: #e6ffed; color: #196c2e; border: 1px solid #b6e8c4; }}
  </style>
</head>
<body>
  <h1>{title}</h1>
  <div class="who">Signed in as <strong>{user}</strong></div>
  {body}
</body>
</html>
"""


def render_page(*, title: str, user: str, body: str) -> str:
    """Render a full HTML page.

    Args:
        title (str): The title of the page.
        user (str): The name of the signed-in user.
        body (str): The HTML content of the page body.

    Returns:
        str: The rendered HTML page.
    """
    return _PAGE.format(title=html.escape(title), user=html.escape(user), body=body)


def render_breadcrumbs(path: str) -> str:
    """Render breadcrumb navigation for a given path.

    Args:
        path (str): The directory path to render breadcrumbs for.

    Returns:
        str: HTML breadcrumb navigation.
    """
    parts = [p for p in path.split("/") if p]
    crumbs = ['<a href="/">/</a>']
    accum = ""
    for p in parts:
        accum += "/" + p
        crumbs.append(f'<a href="/?path={html.escape(accum)}">{html.escape(p)}</a>')
    return '<div class="crumbs">' + " / ".join(crumbs) + "</div>"


def render_listing(entries: Iterable[dict], current_path: str, *, default_new_dir: str = "") -> str:
    """Render a directory listing.

    Args:
        entries (Iterable[dict]): List of entry items to render.
        current_path (str): The current directory path.
        default_new_dir (str): Workspace directory new notebooks land in.
            Defaults to ``current_path`` when empty.

    Returns:
        str: HTML listing of directory contents.
    """
    items: list[str] = []
    has_any = False
    for e in entries:
        has_any = True
        kind = e["kind"]  # "dir" | "notebook" | "file"
        name = html.escape(e["name"])
        path = html.escape(e["path"])
        if kind == "dir":
            icon = "\U0001f4c1"
            link = f'<a class="entry" href="/?path={path}">{name}/</a>'
            badge = ""
        elif kind == "notebook":
            icon = "\U0001f4d3"
            link = f'<a class="entry" href="/edit?path={path}">{name}</a>'
            lang = e.get("language") or "PY"
            badge = f'<span class="badge">{html.escape(lang)}</span>'
        else:
            icon = "\U0001f4c4"
            link = f'<span class="entry" style="color:#888">{name}</span>'
            badge = '<span class="badge">file</span>'
        items.append(f'<li><span class="icon">{icon}</span>{link}{badge}</li>')

    if not has_any:
        body = '<p class="empty">No items here.</p>'
    else:
        body = '<ul class="entries">' + "".join(items) + "</ul>"
    target_dir = default_new_dir or current_path or "/"
    target_dir_disp = html.escape(target_dir.rstrip("/") or "/")
    target_dir_val = html.escape(target_dir)
    toolbar = (
        '<div class="toolbar">'
        '<button class="btn" type="button" id="new-nb-btn" '
        "onclick=\"document.getElementById('new-nb-form').style.display='inline';"
        "this.style.display='none';"
        "document.getElementById('new-nb-name').focus();\">+ New notebook</button>"
        '<form class="inline" id="new-nb-form" method="post" action="/new" '
        'style="display:none">'
        f'<input type="hidden" name="directory" value="{target_dir_val}">'
        f'<span class="hint" style="margin-right:0.4rem">{target_dir_disp}/</span>'
        '<input class="path-input" type="text" id="new-nb-name" name="name" '
        'required pattern="[^/]+" placeholder="new-notebook.py" '
        'style="width:14rem">'
        '<button class="btn" type="submit">OK</button>'
        '<button class="btn secondary" type="button" '
        "onclick=\"document.getElementById('new-nb-form').style.display='none';"
        "document.getElementById('new-nb-btn').style.display='inline-block';\">Cancel</button>"
        "</form>"
        f'<div class="hint">Will be created in <code>{target_dir_disp}/</code>.</div>'
        "</div>"
    )
    return render_breadcrumbs(current_path) + toolbar + body


def render_error(message: str) -> str:
    """Render an error message.

    Args:
        message (str): The error message to render.

    Returns:
        str: HTML error message.
    """
    return f'<div class="err">{html.escape(message)}</div>'


def _format_age(seconds: float) -> str:
    """Human-friendly relative timestamp (“3m ago”, “2h ago”, “just now”)."""
    if seconds < 5:
        return "just now"
    if seconds < 60:
        return f"{int(seconds)}s ago"
    if seconds < 3600:
        return f"{int(seconds // 60)}m ago"
    if seconds < 86400:
        return f"{int(seconds // 3600)}h ago"
    return f"{int(seconds // 86400)}d ago"


def render_drafts_section(drafts: Iterable[dict]) -> str:
    """Render the “Drafts” (cached notebook) panel for the index page.

    Each draft dict carries: ``slug``, ``workspace_path`` (or None), ``mtime``,
    ``last_uploaded_mtime``, ``dirty``. Rendered as a small table with Open
    and Save buttons (Save is a POST form so it survives page refresh).
    """
    import time

    drafts = list(drafts)
    if not drafts:
        return ""
    now = time.time()
    rows: list[str] = []
    for d in drafts:
        slug = html.escape(d["slug"])
        ws_path = d.get("workspace_path") or ""
        ws_disp = html.escape(ws_path) if ws_path else '<span class="empty">(local only)</span>'
        age = _format_age(max(0.0, now - d["mtime"]))
        if d["dirty"]:
            status = '<span class="pill dirty">unsaved</span>'
        elif ws_path:
            status = '<span class="pill clean">saved</span>'
        else:
            status = '<span class="pill dirty">local only</span>'
        # Save form: ``slug`` plus an inline ``workspace_path`` input that
        # doubles as Save-As when the user edits the path before clicking.
        save_form = (
            '<form class="inline" method="post" action="/save">'
            f'<input type="hidden" name="slug" value="{slug}">'
            '<input type="hidden" name="return_to" value="/">'
            '<input class="path-input" type="text" name="workspace_path" '
            f'value="{html.escape(ws_path)}" placeholder="/Users/...">'
            '<button class="btn secondary" type="submit">Save</button>'
            "</form>"
        )
        open_btn = f'<a class="btn secondary" href="/m/{slug}">Open</a>'
        delete_form = (
            '<form class="inline" method="post" action="/delete-draft" '
            "onsubmit=\"return confirm('Delete this draft from the local cache? "
            "Workspace copy is not affected.');\">"
            f'<input type="hidden" name="slug" value="{slug}">'
            '<button class="btn secondary" type="submit" '
            'style="color:#a00;border-color:#f3c2c2">Delete</button>'
            "</form>"
        )
        rows.append(
            f"<tr><td>{open_btn}</td>"
            f'<td><div class="draft-path">{ws_disp}</div>'
            f'<div style="color:#888;font-size:0.75rem;margin-top:0.15rem">slug: {slug} · edited {age}</div></td>'
            f"<td>{status}</td>"
            f"<td>{save_form}</td>"
            f"<td>{delete_form}</td></tr>"
        )
    return (
        '<div class="drafts">'
        "<h2>Drafts (this app session)</h2>"
        '<table class="drafts-table">' + "".join(rows) + "</table>"
        "</div>"
    )


# ---- Starter notebook -----------------------------------------------------
#
# Source for the notebook created by the "+ New notebook" button. Must be a
# valid ``marimo`` script (the dynamic-directory mount loads it as an app).
# Keep cells short, well-commented, and side-effect-free so the notebook is
# safe to open against any workspace.

STARTER_NOTEBOOK = '''import marimo

__generated_with = "0.23.3"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def _intro():
    import marimo as mo
    mo.md(
        """
        # \U0001f44b New marimo \u00b7 Databricks notebook

        This notebook was created by the **marimo \u00b7 Databricks** app and is
        running **on-behalf-of** you \u2014 every Spark / Workspace call below
        uses your OAuth token, not the app's service principal.

        Each section is a self-contained cell. Edit, delete, or duplicate them
        as you like. Marimo re-runs only the cells whose inputs changed.

        > **Note:** if you provided a workspace path when creating this
        > notebook, the **Save** button on the app’s home page writes
        > your edits back to that path. Otherwise it lives only in the
        > app’s local cache until you Save-As somewhere in the workspace.
        """
    )
    return (mo,)


@app.cell
def _imports():
    # Everything ships from the package root. Each name is a lazily-built
    # singleton that's bound to the *current user* inside the app.
    from marimo_databricks_connect import (
        spark,             # databricks.connect DatabricksSession (serverless)
        dbutils,           # pyspark.dbutils.DBUtils bound to ``spark``
        dbfs,              # fsspec FS rooted at /Volumes
        workspace,         # fsspec FS rooted at the Workspace tree (/Users, /Repos, ...)
        external_location, # fn(name_or_uri) -> fsspec FS for a UC external location
        ui,                # marimo UI helpers (selectors, etc.)
        # Catalog / Data-sources panel filtering
        include_catalogs, exclude_catalogs, show_all_catalogs, prefetch,
        # Widgets
        table_widget, schema_widget, workspace_widget, acl_widget,
        principal_widget, genie_widget, secret_scope_widget,
        external_location_widget, cluster_widget, warehouse_widget,
        serving_endpoint_widget, app_widget, pipeline_widget, job_widget,
        compute_widget, pipelines_widget, workflows_widget,
        unity_catalog_widget, vector_search_endpoint_widget,
        vector_index_widget,
    )
    return (
        acl_widget, app_widget, cluster_widget, compute_widget, dbfs,
        dbutils, exclude_catalogs, external_location,
        external_location_widget, genie_widget, include_catalogs,
        job_widget, pipeline_widget, pipelines_widget, prefetch,
        principal_widget, schema_widget, secret_scope_widget,
        serving_endpoint_widget, show_all_catalogs, spark, table_widget,
        ui, unity_catalog_widget, vector_index_widget,
        vector_search_endpoint_widget, warehouse_widget, workflows_widget,
        workspace, workspace_widget,
    )


@app.cell(hide_code=True)
def _spark_intro(mo):
    mo.md(
        """
        ## \u26a1 Spark & SQL

        ``spark`` is a regular ``DatabricksSession`` from
        ``databricks-connect``. Use it like any Spark session. DataFrames
        render lazily \u2014 marimo only materialises what's visible.
        """
    )
    return


@app.cell
def _df(spark):
    # A tiny lazy DataFrame against the public sample dataset.
    df = spark.read.table("samples.nyctaxi.trips").limit(25)
    df
    return (df,)


@app.cell
def _sql(mo, spark):
    # Inline SQL via marimo's mo.sql. ``engine=spark`` runs through
    # databricks-connect, so anything your user can query from a notebook
    # cluster works here too.
    result = mo.sql("SELECT count(*) AS n FROM samples.nyctaxi.trips", engine=spark)
    result
    return (result,)


@app.cell(hide_code=True)
def _sources_intro(mo):
    mo.md(
        """
        ## \U0001f5c2\ufe0f Data sources panel & autocomplete

        ``include_catalogs`` / ``exclude_catalogs`` filter what shows up in
        marimo's **Data sources** panel (left sidebar). They do *not* limit
        what SQL can query \u2014 they just keep the panel manageable in
        large workspaces. ``prefetch()`` warms the schema cache so
        autocomplete on table/column names works without waiting.
        """
    )
    return


@app.cell
def _filter(include_catalogs, prefetch):
    # Show ``samples`` (and the active catalog) in the Data sources panel,
    # then prefetch table metadata for fast autocomplete.
    include_catalogs("samples")
    prefetch()
    return


@app.cell(hide_code=True)
def _storage_intro(mo):
    mo.md(
        """
        ## \U0001f4c1 Storage browsers

        Each of these is an ``fsspec.AbstractFileSystem`` and is automatically
        picked up by marimo's **Storage** panel:

        * ``workspace`` \u2014 your Workspace tree (notebooks, files, Repos).
        * ``dbfs``       \u2014 ``/Volumes`` via ``dbutils.fs``.
        * ``external_location("name_or_abfss_uri")`` \u2014 a UC external
          location, returned as a fresh fsspec FS rooted at that volume.
        """
    )
    return


@app.cell
def _ls(workspace):
    # List the root of the Workspace tree using the OBO token.
    workspace.ls("/")[:10]
    return


@app.cell
def _ext(external_location):
    # Bind an external-location FS \u2014 mark it `_` so the import counts
    # as "used" without doing any IO if you don't need it.
    _ = external_location  # e.g. landing = external_location("landing_zone")
    return


@app.cell(hide_code=True)
def _widgets_intro(mo):
    mo.md(
        """
        ## \U0001f9e9 Widgets

        Drop-in anywidgets backed by the Databricks SDK. Uncomment one and
        replace the placeholder id/name with something from your workspace.
        """
    )
    return


@app.cell
def _widget_examples(
    table_widget, workspace_widget, acl_widget, principal_widget,
    genie_widget, cluster_widget, warehouse_widget, serving_endpoint_widget,
    secret_scope_widget, external_location_widget, app_widget,
    pipeline_widget, job_widget, schema_widget,
):
    # workspace_widget()                                # Workspace browser
    # acl_widget()                                      # Permissions explorer
    # principal_widget("you@example.com")               # User / SP details
    # table_widget("samples.nyctaxi.trips")             # UC table summary
    # schema_widget("samples", "nyctaxi")               # UC schema summary
    # genie_widget("<genie-space-id>")                  # Genie Q&A
    # cluster_widget("<cluster-id>")                    # All-purpose cluster
    # warehouse_widget("<warehouse-id>")                # SQL warehouse
    # serving_endpoint_widget("<endpoint-name>")        # Model-serving endpoint
    # secret_scope_widget("<scope-name>")               # Secret scope
    # external_location_widget("<location-name>")       # UC external location
    # app_widget("<app-name>")                          # Databricks App
    # pipeline_widget("<pipeline-id>")                  # DLT pipeline
    # job_widget("<job-id>")                            # Workflow job
    return


@app.cell(hide_code=True)
def _selectors_intro(mo):
    mo.md(
        """
        ## \U0001f3af Selectors

        ``ui`` exposes Databricks-aware ``marimo`` selectors. They render as
        regular ``mo.ui`` elements but populate themselves from the SDK using
        the OBO token.
        """
    )
    return


@app.cell
def _selector(ui):
    # A dropdown of catalogs the current user can see. ``ui.catalog/schema/
    # table/column/cluster/warehouse/...`` are all live SDK-backed selectors.
    catalog = ui.catalog()
    catalog
    return (catalog,)


@app.cell
def _selected(catalog, mo):
    mo.md(f"You picked: **{catalog.value or '(none)'}**")
    return


if __name__ == "__main__":
    app.run()
'''


def render_starter_notebook() -> str:
    """Return the source code for a fresh starter notebook."""
    return STARTER_NOTEBOOK
