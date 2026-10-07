"""Entrypoint: argument parsing, start-up validation and process lifecycle."""

from __future__ import annotations

import os
import signal
import sys
import threading
from dataclasses import dataclass

from . import util
from .config import Config, ConfigError, load_config
from .db import Database
from .http_server import create_server
from .worker import Worker

USAGE = """usage: serve [--port N] [--data-dir PATH]

Options:
  --port N         TCP port to listen on (1..65535, default 8080)
  --data-dir PATH  directory holding webhooks.db (overrides DATA_DIR)
  --help, -h       show this help and exit
"""


class ArgError(Exception):
    pass


@dataclass
class Args:
    port: int | None = None
    data_dir: str | None = None
    help: bool = False


def _parse_int(value: str, label: str, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        raise ArgError(f"{label} must be an integer, got {value!r}") from None
    if parsed < minimum or parsed > maximum:
        raise ArgError(f"{label} must be between {minimum} and {maximum}, got {parsed}")
    return parsed


def parse_args(argv: list[str]) -> Args:
    args = Args()
    index = 0
    while index < len(argv):
        token = argv[index]
        if token in ("--help", "-h"):
            args.help = True
            return args
        if token == "--port" or token.startswith("--port="):
            value = token.split("=", 1)[1] if "=" in token else _next(argv, index)
            if "=" not in token:
                index += 1
            args.port = _parse_int(value, "port", 1, 65535)
        elif token == "--data-dir" or token.startswith("--data-dir="):
            value = token.split("=", 1)[1] if "=" in token else _next(argv, index)
            if "=" not in token:
                index += 1
            args.data_dir = value
        else:
            raise ArgError(f"unknown argument: {token}")
        index += 1
    return args


def _next(argv: list[str], index: int) -> str:
    if index + 1 >= len(argv):
        raise ArgError(f"{argv[index]} requires a value")
    return argv[index + 1]


def _prepare_data_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)
    probe = os.path.join(path, ".write-test")
    with open(probe, "w", encoding="utf-8") as handle:
        handle.write("ok")
    os.remove(probe)


def run(config: Config) -> int:
    try:
        _prepare_data_dir(config.data_dir)
    except OSError as exc:
        print(f"error: DATA_DIR {config.data_dir!r} is not writable: {exc}", file=sys.stderr)
        return 1

    try:
        db = Database(config.db_path)
    except Exception as exc:  # noqa: BLE001 - any storage failure is fatal
        print(f"error: could not open database: {exc}", file=sys.stderr)
        return 1

    worker = Worker(db, config)
    worker.recover()
    worker.start()

    try:
        server = create_server(config, db, worker)
    except OSError as exc:
        print(f"error: could not bind to port {config.port}: {exc}", file=sys.stderr)
        worker.stop()
        db.close()
        return 1

    util.log("server.started", port=config.port, data_dir=config.data_dir)

    stop = threading.Event()

    def _shutdown(signum: int, frame: object) -> None:
        stop.set()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    thread = threading.Thread(
        target=server.serve_forever, kwargs={"poll_interval": 0.2}, daemon=True
    )
    thread.start()
    stop.wait()
    server.shutdown()
    server.server_close()
    worker.stop()
    db.close()
    util.log("server.stopped")
    return 0


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    try:
        args = parse_args(argv)
    except ArgError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.help:
        sys.stdout.write(USAGE)
        return 0

    try:
        config = load_config(args.port, args.data_dir)
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    return run(config)


if __name__ == "__main__":
    raise SystemExit(main())
