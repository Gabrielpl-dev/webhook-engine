"""HTTP API: request routing, validation and JSON serialization."""

from __future__ import annotations

import json
import sqlite3
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

from . import delivery as delivery_mod
from . import ids, util
from .config import Config
from .db import TERMINAL_STATUSES, VALID_STATUSES, Database
from .worker import Worker

MAX_PAYLOAD_BYTES = 262144
MAX_TYPE_LENGTH = 128
DEFAULT_LIMIT = 50
MAX_LIMIT = 200


class HttpError(Exception):
    def __init__(self, status: int, code: str, message: str, details: dict | None = None) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message
        self.details = details or {}


def endpoint_public(row: sqlite3.Row) -> dict:
    return {
        "id": row["id"],
        "url": row["url"],
        "events": json.loads(row["events"]),
        "created_at": row["created_at"],
    }


def event_public(row: sqlite3.Row) -> dict:
    return {
        "id": row["id"],
        "type": row["type"],
        "payload": json.loads(row["payload"]),
        "created_at": row["created_at"],
    }


def delivery_public(row: sqlite3.Row) -> dict:
    return {
        "id": row["id"],
        "endpoint_id": row["endpoint_id"],
        "event_id": row["event_id"],
        "status": row["status"],
        "attempts": row["attempts"],
        "last_http_status": row["last_http_status"],
        "last_error": row["last_error"],
        "next_retry_at": row["next_retry_at"],
        "replayed_from": row["replayed_from"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _clamp_limit(raw: str | None) -> int:
    if raw is None:
        return DEFAULT_LIMIT
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return DEFAULT_LIMIT
    if value <= 0:
        return DEFAULT_LIMIT
    if value > MAX_LIMIT:
        return MAX_LIMIT
    return value


def _clamp_offset(raw: str | None) -> int:
    if raw is None:
        return 0
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return 0
    return value if value > 0 else 0


def _dedupe(events: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in events:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "WebhookEngine/0.1"
    sys_version = ""

    # -- plumbing ----------------------------------------------------------

    def log_message(self, fmt: str, *args: object) -> None:  # noqa: A003 - stdlib signature
        util.log("http", client=self.client_address[0], message=fmt % args)

    def _json(self, status: int, payload: dict) -> None:
        data = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _no_content(self) -> None:
        self.send_response(204)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _error(self, status: int, code: str, message: str, details: dict | None = None) -> None:
        self._json(status, {"error": {"code": code, "message": message, "details": details or {}}})

    def _read_body(self) -> bytes:
        """Read and cache the request body.

        The body is always drained (even on paths that ignore it) so a client
        reusing a keep-alive connection never desyncs on leftover bytes.
        """
        cached = getattr(self, "_body_cache", None)
        if cached is not None:
            return cached
        raw_length = self.headers.get("Content-Length")
        data = b""
        if raw_length:
            try:
                length = int(raw_length)
            except (TypeError, ValueError):
                length = 0
            if length > 0:
                data = self.rfile.read(length)
        self._body_cache = data
        return data

    def _require_json_content_type(self) -> None:
        content_type = self.headers.get("Content-Type", "")
        if "application/json" not in content_type.lower():
            raise HttpError(400, "invalid_json", "Content-Type must be application/json")

    def _parse_json(self, raw: bytes) -> object:
        try:
            return json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise HttpError(400, "invalid_json", "request body is not valid JSON") from exc

    # -- dispatch ----------------------------------------------------------

    def do_GET(self) -> None:  # noqa: N802 - stdlib naming
        self._handle("GET")

    def do_POST(self) -> None:  # noqa: N802
        self._handle("POST")

    def do_DELETE(self) -> None:  # noqa: N802
        self._handle("DELETE")

    def do_PUT(self) -> None:  # noqa: N802
        self._handle("PUT")

    def do_PATCH(self) -> None:  # noqa: N802
        self._handle("PATCH")

    def do_OPTIONS(self) -> None:  # noqa: N802
        self._handle("OPTIONS")

    def do_HEAD(self) -> None:  # noqa: N802
        self._handle("HEAD")

    def _handle(self, method: str) -> None:
        try:
            self._read_body()
            parsed = urlsplit(self.path)
            segments = [s for s in parsed.path.split("/") if s != ""]
            query = parse_qs(parsed.query)
            handler = self._route(method, segments)
            handler(segments, query)
        except HttpError as err:
            self._error(err.status, err.code, err.message, err.details)
        except Exception as exc:  # noqa: BLE001 - convert unexpected failures to the envelope
            util.log("http.internal_error", error=repr(exc))
            self._error(500, "internal_error", "internal server error")

    def _route(self, method: str, segments: list[str]):
        if segments == ["health"]:
            return self._only(method, {"GET"}, self._h_health)
        if segments == ["endpoints"]:
            return self._only(method, {"GET", "POST"}, self._dispatch_endpoints)
        if len(segments) == 2 and segments[0] == "endpoints":
            return self._only(method, {"GET", "DELETE"}, self._dispatch_endpoint)
        if len(segments) == 3 and segments[0] == "endpoints" and segments[2] == "deliveries":
            return self._only(method, {"GET"}, self._h_endpoint_deliveries)
        if segments == ["events"]:
            return self._only(method, {"POST"}, self._h_create_event)
        if len(segments) == 2 and segments[0] == "events":
            return self._only(method, {"GET"}, self._h_get_event)
        if len(segments) == 3 and segments[0] == "events" and segments[2] == "deliveries":
            return self._only(method, {"GET"}, self._h_event_deliveries)
        if len(segments) == 3 and segments[0] == "deliveries" and segments[2] == "replay":
            return self._only(method, {"POST"}, self._h_replay)
        raise HttpError(404, "not_found", "resource not found")

    @staticmethod
    def _only(method: str, allowed: set[str], handler):
        if method not in allowed:
            raise HttpError(405, "method_not_allowed", f"method {method} not allowed")
        return handler

    def _dispatch_endpoints(self, segments: list[str], query: dict):
        if self.command == "POST":
            return self._h_create_endpoint(segments, query)
        return self._h_list_endpoints(segments, query)

    def _dispatch_endpoint(self, segments: list[str], query: dict):
        if self.command == "DELETE":
            return self._h_delete_endpoint(segments, query)
        return self._h_get_endpoint(segments, query)

    # -- handlers ----------------------------------------------------------

    def _h_health(self, segments: list[str], query: dict) -> None:
        self._json(200, {"status": "ok"})

    def _h_create_endpoint(self, segments: list[str], query: dict) -> None:
        self._require_json_content_type()
        data = self._parse_json(self._read_body())
        if not isinstance(data, dict):
            raise HttpError(400, "invalid_json", "request body must be a JSON object")
        try:
            url = delivery_mod.validate_url(data.get("url"))
        except delivery_mod.InvalidUrlError as exc:
            raise HttpError(422, "invalid_url", str(exc)) from exc

        if not self.server.config.allow_localhost and delivery_mod.url_is_loopback(url):
            raise HttpError(
                422, "ssrf_blocked", "endpoint host resolves to loopback and is not allowed"
            )

        raw_events = data.get("events")
        if not isinstance(raw_events, list) or len(raw_events) == 0:
            raise HttpError(422, "invalid_events", "events must be a non-empty array of strings")
        for item in raw_events:
            if not isinstance(item, str):
                raise HttpError(
                    422, "invalid_events", "events must be a non-empty array of strings"
                )
        events = _dedupe(raw_events)

        endpoint = self.server.db.insert_endpoint(url, events, ids.new_secret())
        util.log("endpoint.created", endpoint_id=endpoint["id"])
        self._json(
            201,
            {
                "id": endpoint["id"],
                "url": endpoint["url"],
                "events": events,
                "secret": endpoint["secret"],
                "created_at": endpoint["created_at"],
            },
        )

    def _h_list_endpoints(self, segments: list[str], query: dict) -> None:
        rows = self.server.db.list_endpoints()
        self._json(200, {"endpoints": [endpoint_public(r) for r in rows]})

    def _h_get_endpoint(self, segments: list[str], query: dict) -> None:
        row = self.server.db.get_endpoint(segments[1])
        if row is None:
            raise HttpError(404, "not_found", "endpoint not found")
        self._json(200, endpoint_public(row))

    def _h_delete_endpoint(self, segments: list[str], query: dict) -> None:
        if not self.server.db.delete_endpoint(segments[1]):
            raise HttpError(404, "not_found", "endpoint not found")
        util.log("endpoint.deleted", endpoint_id=segments[1])
        self._no_content()

    def _h_create_event(self, segments: list[str], query: dict) -> None:
        self._require_json_content_type()
        data = self._parse_json(self._read_body())
        if not isinstance(data, dict):
            raise HttpError(400, "invalid_json", "request body must be a JSON object")

        event_type = data.get("type")
        if not isinstance(event_type, str) or event_type == "" or len(event_type) > MAX_TYPE_LENGTH:
            raise HttpError(
                422, "invalid_type", "type must be a non-empty string of at most 128 characters"
            )

        payload = data.get("payload")
        if not isinstance(payload, dict):
            raise HttpError(422, "invalid_payload", "payload must be a JSON object")

        payload_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        if len(payload_json.encode("utf-8")) > MAX_PAYLOAD_BYTES:
            raise HttpError(413, "payload_too_large", "payload exceeds 262144 bytes")

        endpoint_ids = self._matching_endpoints(event_type)
        event = self.server.db.insert_event(event_type, payload_json)
        created = self.server.db.create_deliveries(event["id"], endpoint_ids)
        if created:
            self.server.worker.wake()
        util.log("event.published", event_id=event["id"], deliveries=created)
        self._json(
            202,
            {
                "id": event["id"],
                "type": event["type"],
                "payload": payload,
                "created_at": event["created_at"],
                "deliveries_created": created,
            },
        )

    def _matching_endpoints(self, event_type: str) -> list[str]:
        matched: list[str] = []
        for row in self.server.db.list_endpoints():
            subscriptions = json.loads(row["events"])
            if "*" in subscriptions or event_type in subscriptions:
                matched.append(row["id"])
        return matched

    def _h_get_event(self, segments: list[str], query: dict) -> None:
        row = self.server.db.get_event(segments[1])
        if row is None:
            raise HttpError(404, "not_found", "event not found")
        self._json(200, event_public(row))

    def _h_endpoint_deliveries(self, segments: list[str], query: dict) -> None:
        if self.server.db.get_endpoint(segments[1]) is None:
            raise HttpError(404, "not_found", "endpoint not found")
        self._list_deliveries(query, endpoint_id=segments[1], event_id=None)

    def _h_event_deliveries(self, segments: list[str], query: dict) -> None:
        if self.server.db.get_event(segments[1]) is None:
            raise HttpError(404, "not_found", "event not found")
        self._list_deliveries(query, endpoint_id=None, event_id=segments[1])

    def _list_deliveries(self, query: dict, endpoint_id: str | None, event_id: str | None) -> None:
        status = query.get("status", [None])[0]
        if status is not None and status not in VALID_STATUSES:
            status = None
        limit = _clamp_limit(query.get("limit", [None])[0])
        offset = _clamp_offset(query.get("offset", [None])[0])
        rows, total = self.server.db.list_deliveries(endpoint_id, event_id, status, limit, offset)
        self._json(200, {"deliveries": [delivery_public(r) for r in rows], "total": total})

    def _h_replay(self, segments: list[str], query: dict) -> None:
        original = self.server.db.get_delivery(segments[1])
        if original is None:
            raise HttpError(404, "not_found", "delivery not found")
        if original["status"] not in TERMINAL_STATUSES:
            raise HttpError(409, "conflict", "delivery is not in a terminal state")
        new_row = self.server.db.create_replay(original)
        self.server.worker.wake()
        util.log("delivery.replayed", delivery_id=new_row["id"], replayed_from=original["id"])
        self._json(202, delivery_public(new_row))


class Server(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self, address: tuple[str, int], db: Database, worker: Worker, config: Config
    ) -> None:
        super().__init__(address, Handler)
        self.db = db
        self.worker = worker
        self.config = config


def create_server(config: Config, db: Database, worker: Worker) -> Server:
    return Server(("0.0.0.0", config.port), db, worker, config)
