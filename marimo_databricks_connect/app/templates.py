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


def render_listing(entries: Iterable[dict], current_path: str) -> str:
    """Render a directory listing.

    Args:
        entries (Iterable[dict]): List of entry items to render.
        current_path (str): The current directory path.

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
    return render_breadcrumbs(current_path) + body


def render_error(message: str) -> str:
    """Render an error message.

    Args:
        message (str): The error message to render.

    Returns:
        str: HTML error message.
    """
    return f'<div class="err">{html.escape(message)}</div>'
