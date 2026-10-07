"""Delivery primitives: URL validation, SSRF guard, signing and the HTTP attempt."""

from __future__ import annotations

import hashlib
import hmac
import http.client
import ipaddress
import json
import socket
import ssl
import time
from urllib.parse import urlsplit

RESPONSE_BODY_LIMIT = 64 * 1024


class InvalidUrlError(ValueError):
    """The supplied URL is absent, relative, or uses a non-http(s) scheme."""


def validate_url(url: object) -> str:
    """Validate and return a webhook destination URL.

    Raises :class:`InvalidUrlError` when the value is not an absolute http(s) URL.
    """
    if not isinstance(url, str) or url == "":
        raise InvalidUrlError("endpoint url must be http(s) and absolute")
    try:
        parts = urlsplit(url)
        # Accessing ``port`` validates that it is numeric and in range.
        _ = parts.port
    except ValueError as exc:  # malformed netloc / bad port
        raise InvalidUrlError("endpoint url must be http(s) and absolute") from exc
    if parts.scheme.lower() not in ("http", "https"):
        raise InvalidUrlError("endpoint url must be http(s) and absolute")
    if not parts.netloc or not parts.hostname:
        raise InvalidUrlError("endpoint url must be http(s) and absolute")
    return url


def host_is_loopback(host: str) -> bool:
    """Return True when ``host`` resolves to at least one loopback address."""
    if not host:
        return False
    try:
        infos = socket.getaddrinfo(host, None)
    except socket.gaierror:
        return False
    for info in infos:
        addr = info[4][0]
        try:
            if ipaddress.ip_address(addr).is_loopback:
                return True
        except ValueError:
            continue
    return False


def url_is_loopback(url: str) -> bool:
    """Return True when the URL host resolves to a loopback address."""
    try:
        host = urlsplit(url).hostname
    except ValueError:
        return False
    return host_is_loopback(host or "")


def build_body(event_id: str, event_type: str, payload_json: str, published_at: str) -> bytes:
    """Compose the canonical delivery body, reusing the stored payload bytes verbatim."""
    return (
        '{"event_id":'
        + json.dumps(event_id, ensure_ascii=False)
        + ',"type":'
        + json.dumps(event_type, ensure_ascii=False)
        + ',"payload":'
        + payload_json
        + ',"published_at":'
        + json.dumps(published_at, ensure_ascii=False)
        + "}"
    ).encode("utf-8")


def sign(secret: str, timestamp: str, body: bytes) -> str:
    """Compute the ``X-Webhook-Signature`` header value for a delivery attempt."""
    message = timestamp.encode("utf-8") + b"." + body
    digest = hmac.new(secret.encode("utf-8"), message, hashlib.sha256).hexdigest()
    return "sha256=" + digest


def attempt(url: str, body: bytes, headers: dict, timeout_ms: int) -> tuple[int | None, str | None]:
    """Perform one delivery attempt.

    Returns ``(last_http_status, last_error)``. A successful response yields
    ``(status, None)``; every failure yields either a non-null status with
    ``"non_2xx"`` or a null status with ``"timeout"`` / ``"connection_error"``.
    """
    parts = urlsplit(url)
    scheme = parts.scheme.lower()
    host = parts.hostname or ""
    port = parts.port or (443 if scheme == "https" else 80)
    path = parts.path or "/"
    if parts.query:
        path = path + "?" + parts.query

    conn_cls = http.client.HTTPSConnection if scheme == "https" else http.client.HTTPConnection
    conn = conn_cls(host, port, timeout=timeout_ms / 1000.0)
    try:
        conn.request("POST", path, body=body, headers=headers)
        response = conn.getresponse()
        status = response.status
        try:
            response.read(RESPONSE_BODY_LIMIT)
        except Exception:  # noqa: BLE001 - body read must never fail a delivery
            pass
        if 200 <= status <= 299:
            return status, None
        return status, "non_2xx"
    except TimeoutError:
        return None, "timeout"
    except (ssl.SSLError, ConnectionError, socket.gaierror, http.client.HTTPException, OSError):
        return None, "connection_error"
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001 - best-effort socket teardown
            pass


def unix_seconds() -> str:
    """Current wall-clock time as integer epoch seconds, rendered as a string."""
    return str(int(time.time()))
