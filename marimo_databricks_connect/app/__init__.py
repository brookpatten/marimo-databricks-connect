"""Databricks App that lists workspace ``.py`` notebooks and edits them with marimo.

This subpackage is meant to be deployed as a `Databricks App
<https://learn.microsoft.com/en-us/azure/databricks/dev-tools/databricks-apps/>`_
configured for **on-behalf-of-user** authentication.  Each request that hits the
app carries the calling user's OAuth token in the ``X-Forwarded-Access-Token``
header; we use that token to:

1. List the user's workspace notebooks (only what *they* can see), and
2. Run the chosen ``.py`` notebook with marimo, with
   :mod:`marimo_databricks_connect`'s ``spark`` / ``workspace`` singletons
   bound to the *user's* identity (not the app's service principal).

Run locally for development::

    uv run python -m marimo_databricks_connect.app

Deploy as a Databricks App by pointing ``app.yaml`` at
``marimo_databricks_connect.app:asgi`` (see ``databricks_app/`` at the repo
root).
"""

from .server import asgi, build_app, main

__all__ = ["asgi", "build_app", "main"]
