"""
Local Fixed-Port Reverse Proxy API Gateway for EasyColab Windows Client.

Provides a stable localhost endpoint (`http://127.0.0.1:8787`) using pure Python
standard library (`ThreadingHTTPServer`, `http.client`, `ssl`, `socket`, `threading`)
so external AI tools (Magpie, Claude Code, Codex, Cursor, Cherry Studio, etc.)
never need reconfiguration when Colab tunnel URLs, API keys, or loaded models change.
"""
from __future__ import annotations

import http.client
import json
import socket
import ssl
import threading
import time
import urllib.parse
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Dict, List, Optional, Tuple

VIRTUAL_MODEL_ID = "easycolab-auto"
DEFAULT_PORT = 8787

_FALLBACK_BUILTIN_REPOS: Tuple[str, ...] = (
    "unsloth/Qwen3.8-27B-GGUF",
    "Qwen/Qwen3.8-27B",
    "MiniMaxAI/MiniMax-H3",
    "Comfy-Org/MiniMax-H3-Image",
    "Qwen/Qwen-Image-2.1",
    "Tongyi-MAI/Z-Image-Turbo",
)

_MODEL_REWRITE_ALIASES = frozenset({
    VIRTUAL_MODEL_ID,
    "auto",
    "default",
    "easycolab",
})

_STRIP_REQ_HEADERS = frozenset({
    "host",
    "connection",
    "keep-alive",
    "proxy-connection",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "content-length",
    "accept-encoding",
    "origin",
    "referer",
    "expect",
})

_STRIP_RESP_HEADERS = frozenset({
    "connection",
    "keep-alive",
    "proxy-connection",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "content-length",
    "access-control-allow-origin",
    "access-control-allow-methods",
    "access-control-allow-headers",
    "access-control-max-age",
})


@dataclass
class LocalGatewaySnapshot:
    upstream_base_url: str = ""
    api_access_key: str = ""
    is_connected: bool = False
    active_model_repo: str = "Qwen/Qwen3.8-27B"
    active_model_name: str = "Qwen3.8-27B"
    custom_repo_ids: List[str] = field(default_factory=list)
    context_length_tokens: int = 262144


class _GatewayHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self,
        server_address: Tuple[str, int],
        request_handler_class: type,
        gateway: "LocalAPIGatewayServer",
    ) -> None:
        self.gateway = gateway
        super().__init__(server_address, request_handler_class)


def _read_chunked_request_body(rfile) -> bytes:
    chunks: List[bytes] = []
    while True:
        size_line = rfile.readline(65536)
        if not size_line:
            break
        semi = size_line.find(b";")
        hex_part = size_line[:semi] if semi >= 0 else size_line
        hex_str = hex_part.strip()
        if not hex_str:
            continue
        chunk_len = int(hex_str, 16)
        if chunk_len == 0:
            # Consume trailing CRLF / trailer headers up to blank line
            while True:
                trailer = rfile.readline(65536)
                if not trailer or trailer in (b"\r\n", b"\n"):
                    break
            break
        data = rfile.read(chunk_len)
        chunks.append(data)
        # Consume trailing CRLF after chunk payload
        rfile.readline(16)
    return b"".join(chunks)


def _open_upstream_connection(
    parsed_url: urllib.parse.SplitResult,
    connect_timeout: float,
    read_timeout: float,
) -> http.client.HTTPConnection:
    scheme = (parsed_url.scheme or "http").lower()
    host = parsed_url.hostname or "127.0.0.1"
    if scheme == "https":
        port = parsed_url.port or 443
        ctx = ssl.create_default_context()
        conn: http.client.HTTPConnection = http.client.HTTPSConnection(
            host, port, timeout=connect_timeout, context=ctx
        )
    else:
        port = parsed_url.port or 80
        conn = http.client.HTTPConnection(host, port, timeout=connect_timeout)
    conn.connect()
    if conn.sock is not None:
        conn.sock.settimeout(read_timeout)
        try:
            conn.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except OSError:
            pass
    return conn


class _GatewayRequestHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: Any) -> None:
        # Suppress console stderr logging (sys.stderr may be None in GUI EXE builds).
        return

    def log_error(self, format: str, *args: Any) -> None:
        return

    def _enable_nodelay(self) -> None:
        try:
            self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except OSError:
            pass

    @property
    def _gateway(self) -> "LocalAPIGatewayServer":
        return self.server.gateway  # type: ignore[attr-defined]

    def _send_cors_headers(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.send_header("Access-Control-Max-Age", "86400")

    def _send_json_bytes(self, status: int, body: bytes) -> None:
        self.close_connection = True
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self._send_cors_headers()
        self.send_header("Connection", "close")
        self.end_headers()
        if self.command != "HEAD" and body:
            self.wfile.write(body)
        self.wfile.flush()

    def _send_json(self, status: int, payload: Dict[str, Any]) -> None:
        raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self._send_json_bytes(status, raw)

    def _send_offline_error(self, is_anthropic: bool, detail: Optional[str] = None) -> None:
        msg = detail or (
            "EasyColab 本地网关已就绪，但当前尚未连接到云端 Colab GPU 实例。"
            "请在 EasyColab 客户端启动或连接云端工作台后重试（无需修改外部工具配置）。 / "
            "EasyColab local gateway is running, but no Colab GPU backend is currently connected."
        )
        if is_anthropic:
            payload: Dict[str, Any] = {
                "type": "error",
                "error": {
                    "type": "overloaded_error",
                    "message": msg,
                },
            }
        else:
            payload = {
                "error": {
                    "message": msg,
                    "type": "service_unavailable",
                    "code": "colab_runtime_offline",
                }
            }
        self._send_json(503, payload)

    def _read_request_body(self) -> bytes:
        te = (self.headers.get("Transfer-Encoding") or "").lower()
        if "chunked" in te:
            return _read_chunked_request_body(self.rfile)
        cl = self.headers.get("Content-Length")
        if cl:
            try:
                length = int(cl.strip())
                if length > 0:
                    return self.rfile.read(length)
            except ValueError:
                pass
        return b""

    def do_OPTIONS(self) -> None:
        self._enable_nodelay()
        self.close_connection = True
        self.send_response(204)
        self._send_cors_headers()
        self.send_header("Content-Length", "0")
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.flush()

    def do_GET(self) -> None:
        self._dispatch()

    def do_HEAD(self) -> None:
        self._dispatch()

    def do_POST(self) -> None:
        self._dispatch()

    def do_PUT(self) -> None:
        self._dispatch()

    def do_PATCH(self) -> None:
        self._dispatch()

    def do_DELETE(self) -> None:
        self._dispatch()

    def _dispatch(self) -> None:
        self._enable_nodelay()
        gw = self._gateway
        snap = gw.get_snapshot()

        parsed = urllib.parse.urlsplit(self.path)
        raw_path = parsed.path or "/"
        clean_path = raw_path.rstrip("/") or "/"

        if self.command in ("GET", "HEAD") and clean_path in ("/", "/health", "/v1/health"):
            port = gw.bound_port or DEFAULT_PORT
            up_cfg = bool(snap.upstream_base_url.strip())
            self._send_json(
                200,
                {
                    "status": "ok",
                    "service": "EasyColab Local API Gateway",
                    "proxy_url": f"http://127.0.0.1:{port}",
                    "openai_base_url": f"http://127.0.0.1:{port}/v1",
                    "anthropic_base_url": f"http://127.0.0.1:{port}",
                    "recommended_model": VIRTUAL_MODEL_ID,
                    "upstream_configured": up_cfg,
                    "upstream_connected": bool(snap.is_connected and up_cfg),
                    "active_model_name": snap.active_model_name,
                    "active_model_repo": snap.active_model_repo,
                },
            )
            return

        if self.command in ("GET", "HEAD") and clean_path in ("/v1/models", "/models"):
            self._handle_models(snap)
            return

        self._handle_proxy_forward(snap, parsed, raw_path, clean_path)

    def _handle_models(self, snap: LocalGatewaySnapshot) -> None:
        now_ts = int(time.time())
        ctx_tokens = max(4096, int(snap.context_length_tokens or 262144))
        up_base = snap.upstream_base_url.strip().rstrip("/")

        virtual_entry: Dict[str, Any] = {
            "id": VIRTUAL_MODEL_ID,
            "object": "model",
            "created": now_ts,
            "owned_by": "easycolab-local-gateway",
            "loaded": bool(snap.is_connected and up_base),
            "context_length": ctx_tokens,
            "max_input_tokens": ctx_tokens,
        }

        if up_base and snap.is_connected:
            try:
                headers: Dict[str, str] = {
                    "Accept": "application/json",
                    "Accept-Encoding": "identity",
                    "Connection": "close",
                }
                key = snap.api_access_key.strip()
                if key:
                    headers["Authorization"] = f"Bearer {key}"
                    headers["x-api-key"] = key

                conn, resp = self._request_upstream_with_redirects(
                    method="GET",
                    url=f"{up_base}/v1/models",
                    headers=headers,
                    body=b"",
                    connect_timeout=5.0,
                    read_timeout=5.0,
                )
                try:
                    if 200 <= resp.status < 300:
                        raw_data = resp.read()
                        obj = json.loads(raw_data.decode("utf-8"))
                        if isinstance(obj, dict) and isinstance(obj.get("data"), list):
                            items: List[Dict[str, Any]] = []
                            has_virtual = False
                            for item in obj["data"]:
                                if not isinstance(item, dict):
                                    continue
                                copied = dict(item)
                                mid = str(copied.get("id") or "")
                                if mid == VIRTUAL_MODEL_ID:
                                    has_virtual = True
                                if "context_length" not in copied:
                                    copied["context_length"] = ctx_tokens
                                if "max_input_tokens" not in copied:
                                    copied["max_input_tokens"] = int(copied.get("context_length") or ctx_tokens)
                                items.append(copied)
                            if not has_virtual:
                                items.insert(0, virtual_entry)
                            obj["data"] = items
                            self._send_json(200, obj)
                            return
                finally:
                    conn.close()
            except Exception:
                pass

        seen = {VIRTUAL_MODEL_ID}
        ordered_repos: List[str] = []
        candidates = (
            [snap.active_model_repo]
            + list(snap.custom_repo_ids)
            + list(_FALLBACK_BUILTIN_REPOS)
        )
        for repo in candidates:
            r = (repo or "").strip()
            if r and r not in seen:
                seen.add(r)
                ordered_repos.append(r)

        fallback_items: List[Dict[str, Any]] = [virtual_entry]
        for repo in ordered_repos:
            fallback_items.append(
                {
                    "id": repo,
                    "object": "model",
                    "created": now_ts,
                    "owned_by": "easycolab",
                    "loaded": bool(snap.is_connected and up_base and repo == snap.active_model_repo),
                    "context_length": ctx_tokens,
                    "max_input_tokens": ctx_tokens,
                }
            )
        self._send_json(200, {"object": "list", "data": fallback_items})

    def _request_upstream_with_redirects(
        self,
        method: str,
        url: str,
        headers: Dict[str, str],
        body: bytes,
        connect_timeout: float = 15.0,
        read_timeout: float = 600.0,
        max_redirects: int = 3,
    ) -> Tuple[http.client.HTTPConnection, http.client.HTTPResponse]:
        current_url = url
        current_method = method
        current_body = body
        req_headers = dict(headers)

        for _ in range(max_redirects + 1):
            parsed_target = urllib.parse.urlsplit(current_url)
            host_hdr = parsed_target.hostname or "127.0.0.1"
            if parsed_target.port:
                default_port = 443 if parsed_target.scheme.lower() == "https" else 80
                if parsed_target.port != default_port:
                    host_hdr = f"{host_hdr}:{parsed_target.port}"
            req_headers["Host"] = host_hdr
            if current_body:
                req_headers["Content-Length"] = str(len(current_body))
            elif current_method in ("POST", "PUT", "PATCH"):
                req_headers["Content-Length"] = "0"
            else:
                req_headers.pop("Content-Length", None)

            path_and_query = parsed_target.path or "/"
            if parsed_target.query:
                path_and_query = f"{path_and_query}?{parsed_target.query}"

            conn = _open_upstream_connection(parsed_target, connect_timeout, read_timeout)
            try:
                conn.request(
                    current_method,
                    path_and_query,
                    body=current_body if current_body else None,
                    headers=req_headers,
                )
                resp = conn.getresponse()
            except Exception:
                conn.close()
                raise

            if resp.status in (301, 302, 303, 307, 308):
                loc = resp.getheader("Location")
                if loc:
                    resp.read()
                    conn.close()
                    current_url = urllib.parse.urljoin(current_url, loc)
                    if resp.status == 303:
                        current_method = "GET"
                        current_body = b""
                    continue
            return conn, resp

        raise RuntimeError("Too many upstream HTTP redirects")

    def _handle_proxy_forward(
        self,
        snap: LocalGatewaySnapshot,
        parsed_req: urllib.parse.SplitResult,
        raw_path: str,
        clean_path: str,
    ) -> None:
        try:
            body = self._read_request_body()
        except Exception as exc:
            self._send_json(400, {"error": {"message": f"Invalid request body: {exc}", "type": "invalid_request_error"}})
            return

        is_anthropic = clean_path.startswith("/v1/messages") or clean_path.startswith("/messages")
        up_base = snap.upstream_base_url.strip().rstrip("/")
        if not up_base:
            self._send_offline_error(is_anthropic)
            return

        # Normalize bare Anthropic SDK paths (/messages -> /v1/messages)
        target_path = raw_path
        if clean_path == "/messages":
            target_path = "/v1/messages"
        elif clean_path.startswith("/messages/"):
            target_path = "/v1" + raw_path

        # Rewrite virtual / missing "model" field in JSON request bodies
        req_ctype = self.headers.get("Content-Type") or ""
        if body and ("json" in req_ctype.lower() or body.lstrip()[:1] == b"{"):
            try:
                decoded = json.loads(body.decode("utf-8"))
                if isinstance(decoded, dict):
                    raw_model = decoded.get("model")
                    norm_model = raw_model.strip().lower() if isinstance(raw_model, str) else ""
                    if (not norm_model or norm_model in _MODEL_REWRITE_ALIASES) and snap.active_model_repo.strip():
                        decoded["model"] = snap.active_model_repo.strip()
                        body = json.dumps(decoded, ensure_ascii=False).encode("utf-8")
            except Exception:
                pass

        out_headers: Dict[str, str] = {}
        for k, v in self.headers.items():
            kl = k.lower()
            if kl in _STRIP_REQ_HEADERS or kl.startswith("sec-fetch-"):
                continue
            out_headers[k] = v

        out_headers["Accept-Encoding"] = "identity"
        out_headers["Connection"] = "close"
        if body and not any(k.lower() == "content-type" for k in out_headers):
            out_headers["Content-Type"] = "application/json"

        key = snap.api_access_key.strip()
        if key:
            # Remove any existing casing variants before injecting canonical auth headers
            for existing_k in [k for k in out_headers if k.lower() in ("authorization", "x-api-key")]:
                out_headers.pop(existing_k, None)
            out_headers["Authorization"] = f"Bearer {key}"
            out_headers["x-api-key"] = key

        full_upstream_url = f"{up_base}{target_path}"
        if parsed_req.query:
            full_upstream_url = f"{full_upstream_url}?{parsed_req.query}"

        self._gateway._notify_request_forwarded()

        conn: Optional[http.client.HTTPConnection] = None
        try:
            conn, resp = self._request_upstream_with_redirects(
                method=self.command,
                url=full_upstream_url,
                headers=out_headers,
                body=body,
                connect_timeout=15.0,
                read_timeout=600.0,
            )
            status = resp.status
            resp_ctype = (resp.getheader("Content-Type") or "").lower()

            # Detect dead Gradio FRP / Cloudflare HTML error pages and convert to structured JSON 503
            if status in (404, 502, 503, 504) and "json" not in resp_ctype and "event-stream" not in resp_ctype:
                try:
                    resp.read(4096)
                except Exception:
                    pass
                self._send_offline_error(
                    is_anthropic,
                    f"云端 Colab 隧道响应不可用 (HTTP {status})，请检查云端实例运行状态。"
                    f" / Upstream Colab tunnel unavailable (HTTP {status}).",
                )
                return

            is_stream = "text/event-stream" in resp_ctype or "ndjson" in resp_ctype
            if is_stream:
                self.close_connection = True
                self.send_response(status)
                has_ctype = False
                for hk, hv in resp.getheaders():
                    hkl = hk.lower()
                    if hkl in _STRIP_RESP_HEADERS or hkl in ("cache-control", "x-accel-buffering"):
                        continue
                    if hkl == "content-type":
                        has_ctype = True
                    self.send_header(hk, hv)
                if not has_ctype:
                    self.send_header("Content-Type", "text/event-stream; charset=utf-8")
                self.send_header("Cache-Control", "no-cache, no-transform")
                self.send_header("X-Accel-Buffering", "no")
                self._send_cors_headers()
                self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.flush()

                if self.command != "HEAD":
                    read_chunk_fn = getattr(resp, "read1", None)
                    while True:
                        chunk = read_chunk_fn(4096) if callable(read_chunk_fn) else resp.read(256)
                        if not chunk:
                            break
                        self.wfile.write(chunk)
                        self.wfile.flush()
                return

            # Non-streaming response (including HTTP 400 context_length_exceeded JSON, 200 JSON, etc.)
            resp_body = resp.read()
            self.close_connection = True
            self.send_response(status)
            has_ctype = False
            for hk, hv in resp.getheaders():
                hkl = hk.lower()
                if hkl in _STRIP_RESP_HEADERS:
                    continue
                if hkl == "content-type":
                    has_ctype = True
                self.send_header(hk, hv)
            if not has_ctype:
                self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(resp_body)))
            self._send_cors_headers()
            self.send_header("Connection", "close")
            self.end_headers()
            if self.command != "HEAD" and resp_body:
                self.wfile.write(resp_body)
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            # Client disconnected mid-stream — closing `conn` in `finally` aborts upstream generation.
            return
        except Exception as exc:
            try:
                self._send_offline_error(
                    is_anthropic,
                    f"无法连接云端 Colab 网关 ({exc}) / Failed to reach upstream Colab gateway ({exc})",
                )
            except Exception:
                pass
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass


class LocalAPIGatewayServer:
    """Thread-safe localhost HTTP/1.1 reverse proxy gateway (`127.0.0.1:<port>`)."""

    VIRTUAL_MODEL_ID = VIRTUAL_MODEL_ID
    DEFAULT_PORT = DEFAULT_PORT

    def __init__(
        self,
        on_request_forwarded: Optional[Callable[[], None]] = None,
    ) -> None:
        self._lock = threading.Lock()
        self._snapshot = LocalGatewaySnapshot()
        self._httpd: Optional[_GatewayHTTPServer] = None
        self._thread: Optional[threading.Thread] = None
        self._running: bool = False
        self._bound_port: int = 0
        self._last_error: str = ""
        self.on_request_forwarded: Optional[Callable[[], None]] = on_request_forwarded

    @property
    def is_running(self) -> bool:
        with self._lock:
            return self._running and self._httpd is not None

    @property
    def bound_port(self) -> int:
        with self._lock:
            return self._bound_port

    @property
    def last_error(self) -> str:
        with self._lock:
            return self._last_error

    def update_snapshot(self, snapshot: LocalGatewaySnapshot) -> None:
        copied = LocalGatewaySnapshot(
            upstream_base_url=str(snapshot.upstream_base_url or ""),
            api_access_key=str(snapshot.api_access_key or ""),
            is_connected=bool(snapshot.is_connected),
            active_model_repo=str(snapshot.active_model_repo or "Qwen/Qwen3.8-27B"),
            active_model_name=str(snapshot.active_model_name or "Qwen3.8-27B"),
            custom_repo_ids=list(snapshot.custom_repo_ids or []),
            context_length_tokens=max(4096, int(snapshot.context_length_tokens or 262144)),
        )
        with self._lock:
            self._snapshot = copied

    def get_snapshot(self) -> LocalGatewaySnapshot:
        with self._lock:
            s = self._snapshot
            return LocalGatewaySnapshot(
                upstream_base_url=s.upstream_base_url,
                api_access_key=s.api_access_key,
                is_connected=s.is_connected,
                active_model_repo=s.active_model_repo,
                active_model_name=s.active_model_name,
                custom_repo_ids=list(s.custom_repo_ids),
                context_length_tokens=s.context_length_tokens,
            )

    def _notify_request_forwarded(self) -> None:
        cb = self.on_request_forwarded
        if callable(cb):
            try:
                cb()
            except Exception:
                pass

    def start(self, port: int = DEFAULT_PORT) -> Tuple[bool, str]:
        """Bind `127.0.0.1:<port>` and start background daemon server thread.

        Pass `port=0` in tests to bind an OS-assigned ephemeral port; otherwise
        ports are clamped to `1024..65535`. Never raises on bind errors.
        """
        try:
            req_port = int(port)
        except (TypeError, ValueError):
            req_port = DEFAULT_PORT
        safe_port = 0 if req_port == 0 else max(1024, min(65535, req_port))

        with self._lock:
            if self._running and self._httpd is not None and safe_port != 0 and self._bound_port == safe_port:
                return True, ""

        self.stop()

        try:
            httpd = _GatewayHTTPServer(("127.0.0.1", safe_port), _GatewayRequestHandler, self)
            actual_port = int(httpd.server_address[1])
        except OSError as exc:
            err_msg = f"端口 {safe_port} 被占用或无法绑定 ({exc})"
            with self._lock:
                self._httpd = None
                self._thread = None
                self._running = False
                self._bound_port = 0
                self._last_error = err_msg
            return False, err_msg
        except Exception as exc:
            err_msg = str(exc)
            with self._lock:
                self._httpd = None
                self._thread = None
                self._running = False
                self._bound_port = 0
                self._last_error = err_msg
            return False, err_msg

        srv_thread = threading.Thread(
            target=self._serve_loop,
            args=(httpd,),
            name=f"EasyColabLocalGateway-{actual_port}",
            daemon=True,
        )
        with self._lock:
            self._httpd = httpd
            self._thread = srv_thread
            self._running = True
            self._bound_port = actual_port
            self._last_error = ""
        srv_thread.start()
        return True, ""

    @staticmethod
    def _serve_loop(httpd: _GatewayHTTPServer) -> None:
        try:
            httpd.serve_forever(poll_interval=0.2)
        except Exception:
            pass

    def stop(self) -> None:
        with self._lock:
            httpd = self._httpd
            srv_thread = self._thread
            self._httpd = None
            self._thread = None
            self._running = False
            self._bound_port = 0

        if httpd is not None:
            try:
                httpd.shutdown()
            except Exception:
                pass
            try:
                httpd.server_close()
            except Exception:
                pass

        if srv_thread is not None and srv_thread.is_alive() and srv_thread is not threading.current_thread():
            srv_thread.join(timeout=1.5)
