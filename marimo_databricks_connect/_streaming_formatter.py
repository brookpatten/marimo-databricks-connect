"""Custom marimo formatters for PySpark streaming DataFrames and queries.

Streaming DataFrames cannot be materialized (no ``.collect()``, ``.toArrow()``,
etc.), so marimo's built-in PySpark formatter either silently errors or shows
nothing.  This module registers formatters that render useful displays:

* **Streaming DataFrames** — schema summary with column names and types.
* **StreamingQuery** — live status, progress, and query metadata.

For non-streaming DataFrames, we delegate to marimo's built-in PySpark
formatter (looked up at call time so import-order doesn't matter).
"""

from __future__ import annotations

import logging
from typing import Any

LOGGER = logging.getLogger(__name__)

# We must hold strong references to marimo UI elements so they are not
# garbage-collected (the kernel's UIElementRegistry only stores weakrefs).
# Keyed by streaming-query id so we reuse / replace buttons across renders.
_STOP_BUTTONS: dict[str, Any] = {}


# ---------------------------------------------------------------------------
# CSS animation for active streaming indicators
# ---------------------------------------------------------------------------

_PULSE_CSS = """
<style>
@keyframes marimo-stream-pulse {
  0%, 100% { transform: scale(1); opacity: 1; }
  50% { transform: scale(0.85); opacity: 0.5; }
}
@keyframes marimo-stream-glow {
  0%, 100% { box-shadow: 0 0 4px 1px rgba(5,150,105,0.4); }
  50% { box-shadow: 0 0 8px 3px rgba(5,150,105,0.7); }
}
@keyframes marimo-stream-bar {
  0% { transform: translateX(-100%); }
  100% { transform: translateX(200%); }
}
@keyframes marimo-stream-border-glow {
  0%, 100% { border-color: #a7f3d0; }
  50% { border-color: #059669; }
}
.marimo-stream-active-dot {
  display: inline-block;
  width: 10px; height: 10px;
  border-radius: 50%;
  background: #059669;
  animation:
    marimo-stream-pulse 1.5s ease-in-out infinite,
    marimo-stream-glow 1.5s ease-in-out infinite;
  flex-shrink: 0;
}
.marimo-stream-bar-track {
  height: 4px;
  overflow: hidden;
  background: #ecfdf5;
  position: relative;
}
.marimo-stream-bar-fill {
  position: absolute;
  top: 0;
  left: 0;
  height: 100%;
  width: 50%;
  border-radius: 2px;
  background: linear-gradient(90deg, transparent, #059669 40%, #34d399 60%, transparent);
  animation: marimo-stream-bar 1.8s ease-in-out infinite;
}
.marimo-stream-card-active {
  border: 1.5px solid #a7f3d0;
  animation: marimo-stream-border-glow 3s ease-in-out infinite;
}
.marimo-stream-badge-active {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  font-size: 11px;
  padding: 2px 10px;
  border-radius: 10px;
  background: #d1fae5;
  color: #065f46;
  font-weight: 600;
  letter-spacing: 0.3px;
}
</style>
"""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _is_streaming_dataframe(df: Any) -> bool:
    """Return True if *df* is a PySpark streaming DataFrame."""
    try:
        return bool(getattr(df, "isStreaming", False))
    except Exception:
        return False


def _esc(s: Any) -> str:
    """HTML-escape a string."""
    if s is None:
        return ""
    return (
        str(s)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _detail_row(label: str, value: str) -> str:
    return (
        '<div style="display:flex;gap:12px;padding:3px 0;font-size:12px;">'
        f'<span style="min-width:140px;color:#6c757d;font-weight:500;">{label}</span>'
        f"<span>{value}</span>"
        "</div>"
    )


def _progress_label(key: str) -> str:
    """Convert camelCase progress keys to readable labels."""
    labels = {
        "id": "Query ID",
        "runId": "Run ID",
        "batchId": "Batch ID",
        "timestamp": "Timestamp",
        "numInputRows": "Input Rows",
        "inputRowsPerSecond": "Input Rows/sec",
        "processedRowsPerSecond": "Processed Rows/sec",
    }
    return labels.get(key, key)


def _safe_attr(obj: Any, name: str) -> Any:
    """Safely get an attribute, returning None on any error."""
    try:
        return getattr(obj, name, None)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Streaming DataFrame formatter
# ---------------------------------------------------------------------------


def _format_streaming_schema(df: Any) -> tuple[str, str]:
    """Build an HTML summary of a streaming DataFrame and return (mime, data)."""
    try:
        dtypes = df.dtypes  # list of (name, type_string)
    except Exception:
        dtypes = []

    num_cols = len(dtypes)

    rows_html = ""
    for i, (col_name, col_type) in enumerate(dtypes):
        rows_html += (
            "<tr>"
            f'<td style="padding:4px 10px;border-bottom:1px solid #dee2e6;'
            f'color:#6c757d;font-size:12px;">{i}</td>'
            f'<td style="padding:4px 10px;border-bottom:1px solid #dee2e6;'
            f'font-family:monospace;font-size:12px;">{_esc(col_name)}</td>'
            f'<td style="padding:4px 10px;border-bottom:1px solid #dee2e6;'
            f'font-family:monospace;font-size:12px;color:#6c757d;">{_esc(col_type)}</td>'
            "</tr>"
        )

    html = (
        _PULSE_CSS
        + '<div class="marimo-stream-card-active" style="border-radius:6px;overflow:hidden;'
        "font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;"
        'font-size:13px;max-width:700px;">'
        # Header
        '<div style="padding:10px 14px;background:linear-gradient(135deg,#ecfdf5,#f0fdf4);'
        'border-bottom:1px solid #d1fae5;'
        'display:flex;align-items:center;gap:8px;">'
        '<span class="marimo-stream-active-dot"></span>'
        '<span style="font-weight:600;font-size:14px;">Streaming DataFrame</span>'
        '<span style="margin-left:auto;font-size:11px;padding:2px 10px;border-radius:10px;'
        'background:#fee2e2;color:#991b1b;font-weight:600;letter-spacing:0.3px;">'
        'STREAMING</span>'
        "</div>"
        # Animated bar
        '<div class="marimo-stream-bar-track">'
        '<div class="marimo-stream-bar-fill"></div></div>'
        # Info bar
        '<div style="padding:10px 14px;font-size:12px;color:#6c757d;'
        'border-bottom:1px solid #dee2e6;">'
        f"This is a streaming DataFrame with <strong>{num_cols}</strong> "
        f'column{"s" if num_cols != 1 else ""}. '
        "Streaming DataFrames cannot be displayed as tables &mdash; use "
        "<code>.writeStream</code> to start a query, or call "
        "<code>spark.sql(&quot;SELECT * FROM table LIMIT 100&quot;)</code> "
        "for a static preview."
        "</div>"
        # Schema table
        '<div style="max-height:350px;overflow:auto;">'
        '<table style="width:100%;border-collapse:collapse;">'
        "<thead><tr>"
        '<th style="text-align:left;padding:6px 10px;font-size:10px;font-weight:600;'
        "text-transform:uppercase;letter-spacing:0.5px;color:#6c757d;background:#f8f9fa;"
        'border-bottom:1px solid #dee2e6;position:sticky;top:0;">#</th>'
        '<th style="text-align:left;padding:6px 10px;font-size:10px;font-weight:600;'
        "text-transform:uppercase;letter-spacing:0.5px;color:#6c757d;background:#f8f9fa;"
        'border-bottom:1px solid #dee2e6;position:sticky;top:0;">Column</th>'
        '<th style="text-align:left;padding:6px 10px;font-size:10px;font-weight:600;'
        "text-transform:uppercase;letter-spacing:0.5px;color:#6c757d;background:#f8f9fa;"
        'border-bottom:1px solid #dee2e6;position:sticky;top:0;">Type</th>'
        "</tr></thead><tbody>"
        f"{rows_html}"
        "</tbody></table></div>"
        # Footer hint
        '<div style="padding:8px 14px;background:#f8f9fa;border-top:1px solid #dee2e6;'
        'font-size:11px;color:#6c757d;">'
        "\U0001F4A1 Tip: use <code>.writeStream.format(&quot;memory&quot;)"
        ".queryName(&quot;preview&quot;).start()</code> then "
        "<code>spark.table(&quot;preview&quot;)</code> to view data."
        "</div>"
        "</div>"
    )
    return ("text/html", html)


# ---------------------------------------------------------------------------
# StreamingQuery formatter
# ---------------------------------------------------------------------------


def _format_streaming_query(query: Any) -> tuple[str, str]:
    """Render a StreamingQuery as an HTML status card."""
    query_name = _safe_attr(query, "name") or "(unnamed)"
    query_id = _safe_attr(query, "id") or "\u2014"
    run_id = _safe_attr(query, "runId") or "\u2014"
    is_active = _safe_attr(query, "isActive")

    # Status dict
    status: dict[str, Any] = {}
    try:
        raw_status = query.status
        if isinstance(raw_status, dict):
            status = raw_status
        elif hasattr(raw_status, "message"):
            status = {
                "message": getattr(raw_status, "message", None),
                "isDataAvailable": getattr(raw_status, "isDataAvailable", None),
                "isTriggerActive": getattr(raw_status, "isTriggerActive", None),
            }
    except Exception:
        pass

    # Last progress
    last_progress: dict[str, Any] = {}
    try:
        lp = query.lastProgress
        if isinstance(lp, dict):
            last_progress = lp
        elif hasattr(lp, "asDict"):
            last_progress = lp.asDict()
    except Exception:
        pass

    # Exception
    exception_msg = None
    try:
        exc = query.exception()
        if exc is not None:
            exception_msg = str(exc)
    except Exception:
        pass

    # Active badge
    if is_active is True:
        badge = (
            '<span class="marimo-stream-badge-active">'
            '<span class="marimo-stream-active-dot"></span> ACTIVE</span>'
        )
    elif is_active is False:
        badge = (
            '<span style="font-size:11px;padding:2px 8px;border-radius:10px;'
            'background:#f3f4f6;color:#6b7280;font-weight:500;">\u25CB STOPPED</span>'
        )
    else:
        badge = (
            '<span style="font-size:11px;padding:2px 8px;border-radius:10px;'
            'background:#fef3c7;color:#92400e;font-weight:500;">? UNKNOWN</span>'
        )

    # Stop button (only when active) — built as a real marimo UI element
    # so the frontend properly wires up click handling.
    stop_element: Any = None  # Will be a marimo Html/layout element or None
    if is_active is True:
        try:
            import marimo as mo

            q_key = str(query_id)

            def _on_stop_click(_: Any) -> str:
                """Call query.stop() and return a status string."""
                try:
                    query.stop()
                except Exception:
                    pass
                return "stopped"

            def _on_stop_change(_value: Any) -> None:
                """Re-render the card after the stream has been stopped."""
                try:
                    # Re-run the formatter now that isActive should be False.
                    # Use mo.output.replace to push the updated card to the
                    # cell output (execution context is available during
                    # on_change processing).
                    mime, data = _format_streaming_query(query)
                    mo.output.replace(mo.Html(data))
                except Exception:
                    pass
                # Clean up the button reference
                _STOP_BUTTONS.pop(q_key, None)

            stop_button = mo.ui.button(
                on_click=_on_stop_click,
                label="\u23F9 Stop Stream",
                kind="danger",
                tooltip="Stop this streaming query",
                on_change=_on_stop_change,
            )
            # Keep a strong reference so the kernel doesn't lose the
            # element before the user has a chance to click it.
            _STOP_BUTTONS[q_key] = stop_button

            stop_element = mo.hstack(
                [
                    stop_button,
                    mo.Html(
                        '<span style="font-size:11px;color:#6c757d;">'
                        "Click to stop this streaming query</span>"
                    ),
                ],
                align="center",
                gap=0.5,
            )
        except Exception:
            pass
    else:
        # Query is no longer active — drop the cached button (if any)
        _STOP_BUTTONS.pop(str(query_id), None)

    # Build detail rows
    detail_rows = ""
    detail_rows += _detail_row("Name", _esc(query_name))
    detail_rows += _detail_row(
        "Query ID",
        f'<code style="font-size:11px;">{_esc(str(query_id))}</code>',
    )
    detail_rows += _detail_row(
        "Run ID",
        f'<code style="font-size:11px;">{_esc(str(run_id))}</code>',
    )

    if status.get("message"):
        detail_rows += _detail_row("Status", _esc(status["message"]))
    if status.get("isDataAvailable") is not None:
        detail_rows += _detail_row(
            "Data Available", "Yes" if status["isDataAvailable"] else "No"
        )
    if status.get("isTriggerActive") is not None:
        detail_rows += _detail_row(
            "Trigger Active", "Yes" if status["isTriggerActive"] else "No"
        )

    # Progress details
    progress_html = ""
    if last_progress:
        progress_rows = ""
        for key in [
            "batchId",
            "timestamp",
            "numInputRows",
            "inputRowsPerSecond",
            "processedRowsPerSecond",
        ]:
            if key in last_progress and last_progress[key] is not None:
                progress_rows += _detail_row(
                    _progress_label(key),
                    _esc(str(last_progress[key])),
                )

        # Sources summary
        sources = last_progress.get("sources") or []
        for i, src in enumerate(sources):
            desc = (
                src.get("description")
                or src.get("startOffset")
                or f"source {i}"
            )
            n_input = src.get("numInputRows", "\u2014")
            progress_rows += _detail_row(
                f"Source {i}",
                f"{_esc(str(desc))} ({_esc(str(n_input))} rows)",
            )

        # Sink summary
        sink = last_progress.get("sink") or {}
        if sink:
            sink_desc = sink.get("description") or str(sink)
            progress_rows += _detail_row("Sink", _esc(str(sink_desc)))

        if progress_rows:
            progress_html = (
                '<div style="margin-top:8px;padding-top:8px;border-top:1px solid #dee2e6;">'
                '<div style="font-size:11px;font-weight:600;color:#6c757d;'
                'text-transform:uppercase;letter-spacing:0.5px;margin-bottom:4px;">'
                "Last Progress</div>"
                f"{progress_rows}"
                "</div>"
            )

    # Exception display
    exception_html = ""
    if exception_msg:
        exception_html = (
            '<div style="margin-top:8px;padding:8px 10px;background:#fef2f2;'
            'border:1px solid #fecaca;border-radius:4px;font-size:12px;color:#991b1b;">'
            f"<strong>Exception:</strong> {_esc(exception_msg)}"
            "</div>"
        )

    # Animated bar for active queries
    active_bar = (
        '<div class="marimo-stream-bar-track">'
        '<div class="marimo-stream-bar-fill"></div></div>'
        if is_active is True
        else ""
    )

    # Card border style
    card_class = ' class="marimo-stream-card-active"' if is_active is True else ""
    card_border = "" if is_active is True else "border:1px solid #dee2e6;"
    header_bg = (
        "background:linear-gradient(135deg,#ecfdf5,#f0fdf4);"
        "border-bottom:1px solid #d1fae5;"
        if is_active is True
        else "background:#f8f9fa;border-bottom:1px solid #dee2e6;"
    )

    card_html = (
        _PULSE_CSS
        + f'<div{card_class} style="{card_border}border-radius:6px;'
        'overflow:hidden;'
        "font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;"
        'font-size:13px;max-width:700px;">'
        # Header
        f'<div style="padding:10px 14px;{header_bg}'
        'display:flex;align-items:center;gap:8px;">'
        '<span style="font-size:16px;">\u25B6\uFE0F</span>'
        '<span style="font-weight:600;font-size:14px;">Streaming Query</span>'
        f'<span style="margin-left:auto;">{badge}</span>'
        "</div>"
        # Animated progress bar
        f"{active_bar}"
        # Detail grid
        f'<div style="padding:10px 14px;">{detail_rows}{progress_html}{exception_html}</div>'
        # Footer
        '<div style="padding:8px 14px;background:#f8f9fa;border-top:1px solid #dee2e6;'
        'font-size:11px;color:#6c757d;">'
        "\U0001F4A1 Use <code>query.stop()</code> to stop, "
        "<code>query.awaitTermination()</code> to block, or "
        "<code>query.lastProgress</code> to inspect progress."
        "</div>"
        "</div>"
    )

    # If we have a stop button element, compose it with the card using
    # marimo's layout so the button is a first-class interactive element.
    if stop_element is not None:
        try:
            import marimo as mo

            composed = mo.vstack(
                [mo.Html(card_html), stop_element],
                gap=0,
            )
            return composed._mime_()
        except Exception:
            pass

    return ("text/html", card_html)


# ---------------------------------------------------------------------------
# Combined DataFrame formatter (streaming + non-streaming)
# ---------------------------------------------------------------------------


def _make_dataframe_formatter(cls: type) -> Any:
    """Create a formatter for a PySpark DataFrame class.

    For streaming DataFrames: renders schema summary.
    For non-streaming: delegates to marimo's original formatter (looked up
    at call time so import order doesn't matter).
    """

    def _formatter(df: Any) -> tuple[str, str]:
        if _is_streaming_dataframe(df):
            return _format_streaming_schema(df)

        # Delegate to marimo's original PySpark formatter.
        # We look it up at call time because marimo registers formatters
        # lazily via import hooks, so the original might not exist yet at
        # registration time.
        try:
            from marimo._output.formatters.df_formatters import (
                include_opinionated,
            )
            from marimo._plugins.ui._impl.table import (
                get_default_table_page_size,
                table,
            )

            if not include_opinionated():
                return ("text/plain", repr(df))

            import narwhals.stable.v2 as nw

            if hasattr(nw.dependencies, "is_pyspark_connect_dataframe"):
                return table.lazy(df)._mime_()

            return table(
                df.limit(get_default_table_page_size()).toArrow(),
                selection=None,
                pagination=False,
                _internal_lazy=True,
                _internal_preload=True,
            )._mime_()
        except Exception as exc:
            LOGGER.debug(
                "Falling back to repr for PySpark DataFrame: %s", exc
            )
            return ("text/plain", repr(df))

    return _formatter


# ---------------------------------------------------------------------------
# Public registration
# ---------------------------------------------------------------------------


def register_streaming_formatter() -> None:
    """Register marimo formatters for streaming DataFrames and StreamingQuery.

    * PySpark DataFrame classes get a combined formatter that shows a schema
      card for streaming DataFrames and delegates to marimo's table widget
      for regular DataFrames.
    * StreamingQuery classes get a status/progress card formatter.
    """
    try:
        from marimo._output.formatting import OPINIONATED_FORMATTERS
    except ImportError:
        LOGGER.debug("marimo formatting not available", exc_info=True)
        return

    # --- DataFrame formatters ---
    df_classes: list[type] = []
    try:
        from pyspark.sql.connect.dataframe import (
            DataFrame as ConnectDataFrame,
        )

        df_classes.append(ConnectDataFrame)
    except ImportError:
        pass
    try:
        from pyspark.sql.dataframe import DataFrame as ClassicDataFrame

        df_classes.append(ClassicDataFrame)
    except ImportError:
        pass

    for cls in df_classes:
        OPINIONATED_FORMATTERS.add_formatter(cls, _make_dataframe_formatter(cls))

    # --- StreamingQuery formatters ---
    query_classes: list[type] = []
    try:
        from pyspark.sql.connect.streaming.query import (
            StreamingQuery as ConnectStreamingQuery,
        )

        query_classes.append(ConnectStreamingQuery)
    except ImportError:
        pass
    try:
        from pyspark.sql.streaming.query import (
            StreamingQuery as ClassicStreamingQuery,
        )

        query_classes.append(ClassicStreamingQuery)
    except ImportError:
        pass

    for cls in query_classes:
        OPINIONATED_FORMATTERS.add_formatter(cls, _format_streaming_query)

    LOGGER.debug(
        "Registered streaming formatters for DataFrames=%s, Queries=%s",
        df_classes,
        query_classes,
    )
