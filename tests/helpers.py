"""Black-box test helpers.

Nothing here imports the service's internal modules: the engine is exercised
exclusively over HTTP, as a subprocess, exactly like an external client would.
"""

from __future__ import annotations

import http.client
import json
import os
import socket
import subprocess
import tempfile
import threading
import time
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SERVE = os.path.join(REPO_ROOT, "serve")

DEFAULT_ENV = {
    "WEBHOOK_ALLOW_LOCALHOST": "1",
    "WEBHOOK_BACKOFF_BASE_MS": "50",
    "WEBHOOK_MAX_ATTEMPTS": "3",
}


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@dataclass
class Response:
    status: int
    headers: dict
    raw: bytes
    json: object


def request(
    method: str,
    port: int,
    path: str,
    body: object = None,
    raw_body: bytes | None = None,
    headers: dict | None = None,
    host: str = "127.0.0.1",
    timeout: float = 15.0,
) -> Response:
    """Perform an HTTP request, returning status, headers, raw and parsed body."""
    conn = http.client.HTTPConnection(host, port, timeout=timeout)
    payload = (
        raw_body
        if raw_body is not None
        else (json.dumps(body).encode("utf-8") if body is not None else None)
    )
    try:
        conn.request(method, path, body=payload, headers=headers or {})
        resp = conn.getresponse()
        raw = resp.read()
        status = resp.status
        hdrs = {k.lower(): v for k, v in resp.getheaders()}
    finally:
        conn.close()
    try:
        parsed = json.loads(raw) if raw else None
    except json.JSONDecodeError:
        parsed = None
    return Response(status, hdrs, raw, parsed)


def json_post(port: int, path: str, body: object) -> Response:
    return request("POST", port, path, body=body, headers={"Content-Type": "application/json"})


def wait_for(predicate, timeout: float = 5.0, interval: float = 0.02):
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    return None


class Engine:
    """A ``./serve`` subprocess with its own temp DATA_DIR and ephemeral port."""

    def __init__(
        self, data_dir: str | None = None, port: int | None = None, env: dict | None = None
    ):
        self.data_dir = data_dir or tempfile.mkdtemp(prefix="wh-engine-")
        self.port = port or free_port()
        self.env = os.environ.copy()
        self.env.update(DEFAULT_ENV)
        self.env["DATA_DIR"] = self.data_dir
        if env:
            self.env.update(env)
        self.proc: subprocess.Popen | None = None

    def start(self) -> Engine:
        started = time.time()
        self.proc = subprocess.Popen(
            [SERVE, "--port", str(self.port)],
            env=self.env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        deadline = time.time() + 5.0
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError("serve exited before becoming ready")
            try:
                if request("GET", self.port, "/health", timeout=1.0).status == 200:
                    self.ready_seconds = time.time() - started
                    return self
            except OSError:
                time.sleep(0.05)
        raise RuntimeError("serve did not become ready within 5s")

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()

    def kill(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait()

    def restart(self) -> Engine:
        self.start()
        return self

    def __enter__(self) -> Engine:
        return self.start()

    def __exit__(self, *exc) -> None:
        self.stop()


class Receiver:
    """A scriptable HTTP receiver that records every POST it sees."""

    def __init__(self, default_status: int = 200, default_delay: float = 0.0):
        self.records: list[dict] = []
        self._plan: list[dict] = []
        self._default = {"status": default_status, "delay": default_delay}
        self._lock = threading.Lock()
        receiver = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args):  # noqa: D102 - silence
                pass

            def do_POST(self):  # noqa: N802
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length)
                with receiver._lock:
                    step = receiver._plan.pop(0) if receiver._plan else dict(receiver._default)
                delay = step.get("delay", 0.0)
                record = {
                    "arrived": time.monotonic(),
                    "headers": {k.lower(): v for k, v in self.headers.items()},
                    "body": body,
                }
                try:
                    record["json"] = json.loads(body)
                except json.JSONDecodeError:
                    record["json"] = None
                receiver.records.append(record)
                if delay:
                    time.sleep(delay)
                status = step.get("status", 200)
                self.send_response(status)
                self.send_header("Content-Length", "0")
                self.end_headers()

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.port = self._server.server_address[1]
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def url(self, path: str = "/hook") -> str:
        return f"http://127.0.0.1:{self.port}{path}"

    def plan(self, *steps: dict) -> None:
        with self._lock:
            self._plan.extend(steps)

    def set_default(self, status: int, delay: float = 0.0) -> None:
        with self._lock:
            self._default = {"status": status, "delay": delay}

    def count(self) -> int:
        return len(self.records)

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()


class StallingReceiver:
    """Accepts TCP connections and never sends a response."""

    def __init__(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(16)
        self.port = self._sock.getsockname()[1]
        self._conns: list[socket.socket] = []
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self) -> None:
        while True:
            try:
                conn, _ = self._sock.accept()
            except OSError:
                return
            self._conns.append(conn)

    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}/hook"

    def close(self) -> None:
        self._sock.close()
        for conn in self._conns:
            try:
                conn.close()
            except OSError:
                pass


def register_endpoint(port: int, url: str, events: list) -> Response:
    return json_post(port, "/endpoints", {"url": url, "events": events})


def publish(port: int, event_type: str, payload: dict) -> Response:
    return json_post(port, "/events", {"type": event_type, "payload": payload})


def get_deliveries(port: int, path: str) -> list:
    resp = request("GET", port, path)
    assert resp.status == 200, f"unexpected {resp.status}: {resp.raw!r}"
    return resp.json["deliveries"]


def one_delivery(port: int, path: str) -> dict:
    deliveries = get_deliveries(port, path)
    assert len(deliveries) == 1, f"expected one delivery, got {len(deliveries)}"
    return deliveries[0]
