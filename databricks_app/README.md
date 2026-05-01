# marimo-databricks-connect Databricks App

A [Databricks App](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/databricks-apps/)
that lets a user browse their workspace, pick any `.py` notebook, and edit it
with [marimo](https://marimo.io) — using *their own* identity for both the
workspace listing **and** the Spark Connect / SDK calls inside the notebook.

## How it works

1. The app is deployed with `user_authorization` enabled (see `app.yaml`),
   so every request carries the calling user's OAuth token in the
   `X-Forwarded-Access-Token` header. See the
   [Databricks Apps auth docs](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/databricks-apps/auth).
2. `OboMiddleware` (in `marimo_databricks_connect.app.auth`) extracts that
   token on every request and stashes it into a `contextvars.ContextVar`.
3. The workspace browser at `/` calls `WorkspaceClient(host=..., token=user_token).workspace.list(...)`
   so the user only sees what they have access to.
4. When a notebook is selected, the app exports it via the Workspace API
   (still as the user) into a local cache directory.
5. A marimo ASGI app is mounted at `/m` via
   `marimo.create_asgi_app().with_dynamic_directory(...)`. The same OBO
   middleware is attached to every sub-app, so when the notebook does
   `from marimo_databricks_connect import spark`, the cached singleton is
   built per-user (`DatabricksSession.builder.host(...).token(user_token).serverless()`).

The result: each user gets their own `DatabricksSession` and `WorkspaceClient`
inside the marimo notebook, automatically scoped to their identity.

## Deploy

```bash
# 1. Sync this repo into the workspace.
databricks sync --watch . /Workspace/Users/$USER/marimo-databricks-app

# 2. Create / update the app.
databricks apps deploy marimo-databricks \
  --source-code-path /Workspace/Users/$USER/marimo-databricks-app/databricks_app
```

Then open the app URL from the Databricks UI.

## Run locally

```bash
uv sync --extra app
uv run python -m marimo_databricks_connect.app
# → http://localhost:8000
```

When run locally, no `X-Forwarded-Access-Token` is present, so the app falls
back to the unified-auth chain (env vars, `~/.databrickscfg`, `az login`) for
both the workspace listing and the notebook's Spark/SDK clients — matching
the behaviour of `marimo edit` on your laptop.
