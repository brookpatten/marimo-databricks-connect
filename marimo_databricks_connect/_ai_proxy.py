"""Localhost auth-refreshing proxy for Databricks Model Serving.

Marimo (and any OpenAI-compatible client) is pointed at this proxy as its
``base_url``.  Each incoming request is forwarded to
``<workspace-host>/serving-endpoints/<path>`` after the proxy injects a fresh
``Authorization: Bearer <token>`` produced by the Databricks SDK auth chain
(PAT, OAuth U2M, OAuth M2M, Azure CLI, OBO, ...).

This means:

* Tokens never expire mid-session — the SDK refreshes OAuth tokens on demand
  and the proxy reads them per-request.
* No bearer token is ever written to ``marimo.toml``; the value stored there is
  an opaque sentinel string the proxy ignores.
* The same proxy works for ``chat/completions``, ``completions``, ``embeddings``
  and any other path under ``/serving-endpoints/``.

Streaming responses (SSE) are supported by streaming the upstream response
back to the caller chunk-by-chunk.
"""

from __future__ import annotations

import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Optional

LOG = logging.getLogger(__name__)

# Headers that must not be forwarded verbatim. ``authorization`` is dropped
# because we replace it with a fresh SDK-minted token; the rest are hop-by-hop
# or recomputed by the upstream HTTP library.
_HOP_BY_HOP = frozenset(
    {
        "host",
        "content-length",
        "connection",
        "authorization",
        "accept-encoding",
        "transfer-encoding",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "upgrade",
    }
)

_RESPONSE_SKIP = frozenset({"transfer-encoding", "content-encoding", "content-length", "connection"})


class AuthRefreshingProxy:
    """Threaded localhost HTTP proxy that injects fresh Databricks auth headers."""

    def __init__(
        self,
        workspace_client_factory: Callable[[], Any],
        host: str = "127.0.0.1",
        port: int = 0,
    ) -> None:
        self._factory = workspace_client_factory
        self._ws_lock = threading.Lock()
        self._ws: Any = None
        self.host = host

        proxy = self

        class _Handler(BaseHTTPRequestHandler):
            def log_message(self, fmt: str, *args: Any) -> None:  # noqa: D401
                LOG.debug("mdc-ai-proxy: " + fmt, *args)

            def do_OPTIONS(self) -> None:  # noqa: N802
                proxy._handle(self)

            def do_GET(self) -> None:  # noqa: N802
                proxy._handle(self)

            def do_POST(self) -> None:  # noqa: N802
                proxy._handle(self)

            def do_PUT(self) -> None:  # noqa: N802
                proxy._handle(self)

            def do_DELETE(self) -> None:  # noqa: N802
                proxy._handle(self)

            def do_PATCH(self) -> None:  # noqa: N802
                proxy._handle(self)

        self._server = ThreadingHTTPServer((host, port), _Handler)
        self.port = self._server.server_address[1]
        self._thread = threading.Thread(target=self._server.serve_forever, name="mdc-ai-proxy", daemon=True)

    @property
    def base_url(self) -> str:
        """Return the URL marimo / OpenAI clients should use as ``base_url``."""
        return f"http://{self.host}:{self.port}"

    def start(self) -> None:
        """Start the proxy thread (idempotent)."""
        if not self._thread.is_alive():
            self._thread.start()

    def stop(self) -> None:
        """Shut the proxy down."""
        try:
            self._server.shutdown()
        finally:
            self._server.server_close()

    def _get_ws(self) -> Any:
        with self._ws_lock:
            if self._ws is None:
                self._ws = self._factory()
            return self._ws

    def _handle(self, h: BaseHTTPRequestHandler) -> None:  # noqa: C901
        try:
            import requests  # transitively available via databricks-sdk
        except ImportError:  # pragma: no cover - extremely unlikely
            self._fail(h, 500, "requests not installed")
            return

        try:
            ws = self._get_ws()
            target_host = str(ws.config.host).rstrip("/")
            path = h.path if h.path.startswith("/") else "/" + h.path
            target_url = f"{target_host}/serving-endpoints{path}"

            # SDK refreshes expired OAuth tokens automatically on each call.
            auth_headers = ws.config.authenticate() or {}

            fwd_headers: dict[str, str] = {}
            for k, v in h.headers.items():
                if k.lower() in _HOP_BY_HOP:
                    continue
                fwd_headers[k] = v
            fwd_headers.update(auth_headers)

            body: Optional[bytes] = None
            length_hdr = h.headers.get("Content-Length")
            if length_hdr is not None:
                length = int(length_hdr)
                body = h.rfile.read(length) if length > 0 else b""

            resp = requests.request(
                method=h.command,
                url=target_url,
                headers=fwd_headers,
                data=body,
                stream=True,
                timeout=300,
            )

            try:
                h.send_response(resp.status_code)
                for k, v in resp.headers.items():
                    if k.lower() in _RESPONSE_SKIP:
                        continue
                    h.send_header(k, v)
                h.end_headers()
                for chunk in resp.iter_content(chunk_size=8192):
                    if not chunk:
                        continue
                    try:
                        h.wfile.write(chunk)
                        h.wfile.flush()
                    except (BrokenPipeError, ConnectionResetError):
                        break
            finally:
                resp.close()
        except Exception as exc:  # pragma: no cover - defensive
            LOG.warning("mdc-ai-proxy error: %s", exc, exc_info=True)
            self._fail(h, 502, f"proxy_error: {exc}")

    @staticmethod
    def _fail(h: BaseHTTPRequestHandler, status: int, msg: str) -> None:
        try:
            h.send_response(status)
            h.send_header("Content-Type", "application/json")
            h.end_headers()
            h.wfile.write(b'{"error": ' + repr(msg).encode() + b"}")
        except Exception:
            pass


_PROXY_SINGLETON: Optional[AuthRefreshingProxy] = None
_PROXY_LOCK = threading.Lock()


def get_or_create_proxy(
    workspace_client_factory: Callable[[], Any],
    *,
    host: str = "127.0.0.1",
    port: int = 0,
) -> AuthRefreshingProxy:
    """Return a process-wide singleton proxy, starting it on first call."""
    global _PROXY_SINGLETON
    with _PROXY_LOCK:
        if _PROXY_SINGLETON is None:
            _PROXY_SINGLETON = AuthRefreshingProxy(workspace_client_factory, host=host, port=port)
            _PROXY_SINGLETON.start()
        return _PROXY_SINGLETON


def _reset_proxy_for_tests() -> None:
    """Tear down the singleton proxy. Test-only."""
    global _PROXY_SINGLETON
    with _PROXY_LOCK:
        if _PROXY_SINGLETON is not None:
            _PROXY_SINGLETON.stop()
            _PROXY_SINGLETON = None
