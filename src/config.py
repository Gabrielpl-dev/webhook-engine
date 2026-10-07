"""Process configuration, read once at start-up."""

from __future__ import annotations

import os
from dataclasses import dataclass


class ConfigError(Exception):
    """Raised when an environment variable or CLI value is invalid."""


@dataclass(frozen=True)
class Config:
    port: int
    data_dir: str
    backoff_base_ms: int
    max_attempts: int
    allow_localhost: bool
    timeout_ms: int
    worker_concurrency: int

    @property
    def db_path(self) -> str:
        return os.path.join(self.data_dir, "webhooks.db")


def _int_env(name: str, default: int, minimum: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        raise ConfigError(f"{name} must be an integer, got {raw!r}") from None
    if value < minimum:
        raise ConfigError(f"{name} must be >= {minimum}, got {value}")
    return value


def load_config(port: int | None, data_dir: str | None) -> Config:
    """Build a :class:`Config` from CLI overrides and environment variables."""
    env_data_dir = os.environ.get("DATA_DIR") or "./data"
    resolved_data_dir = data_dir if data_dir else env_data_dir
    return Config(
        port=8080 if port is None else port,
        data_dir=resolved_data_dir,
        backoff_base_ms=_int_env("WEBHOOK_BACKOFF_BASE_MS", 1000, 1),
        max_attempts=_int_env("WEBHOOK_MAX_ATTEMPTS", 5, 1),
        allow_localhost=os.environ.get("WEBHOOK_ALLOW_LOCALHOST") == "1",
        timeout_ms=_int_env("WEBHOOK_TIMEOUT_MS", 10000, 1),
        worker_concurrency=_int_env("WEBHOOK_WORKER_CONCURRENCY", 4, 1),
    )
