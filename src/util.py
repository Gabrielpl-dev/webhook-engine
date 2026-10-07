"""Small time and logging helpers shared across the service."""

from __future__ import annotations

import json
import sys
import threading
from datetime import UTC, datetime, timedelta

_log_lock = threading.Lock()


def format_ms(dt: datetime) -> str:
    """Render a datetime as RFC 3339 UTC with millisecond precision and a ``Z``."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def now_ms() -> str:
    """Current UTC time as an RFC 3339 string with millisecond precision."""
    return format_ms(datetime.now(UTC))


def future_ms(delay_ms: int) -> str:
    """UTC time ``delay_ms`` milliseconds in the future, RFC 3339 with millis."""
    dt = datetime.now(UTC) + timedelta(milliseconds=delay_ms)
    return format_ms(dt)


def log(event: str, **fields: object) -> None:
    """Emit a single structured JSON line to stdout."""
    record = {"ts": now_ms(), "event": event}
    record.update(fields)
    line = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
    with _log_lock:
        sys.stdout.write(line + "\n")
        sys.stdout.flush()
