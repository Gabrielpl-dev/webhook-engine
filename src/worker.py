"""Background delivery worker.

A single dispatcher thread selects eligible deliveries and hands them to a
thread pool. At most one delivery per endpoint is ever in flight, which
preserves FIFO ordering at the cost of intentional head-of-line blocking.
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

from . import delivery as delivery_mod
from . import util
from .config import Config
from .db import Database

POLL_INTERVAL_S = 0.02


class Worker:
    def __init__(self, db: Database, config: Config) -> None:
        self._db = db
        self._config = config
        self._executor = ThreadPoolExecutor(max_workers=config.worker_concurrency)
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._in_flight: set[str] = set()
        self._in_flight_endpoints: set[str] = set()
        self._thread: threading.Thread | None = None

    # -- lifecycle ---------------------------------------------------------

    def recover(self) -> None:
        """Transition in-flight deliveries and re-apply the loopback guard."""
        recovered = self._db.recover_delivering()
        blocked = self._sweep_ssrf()
        util.log("worker.recover", recovered=recovered, ssrf_blocked=blocked)

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, name="dispatcher", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()
        self._executor.shutdown(wait=False)

    def wake(self) -> None:
        self._wake.set()

    # -- internals ---------------------------------------------------------

    def _sweep_ssrf(self) -> int:
        if self._config.allow_localhost:
            return 0
        blocked = 0
        for row in self._db.pending_deliveries_with_endpoint():
            if delivery_mod.url_is_loopback(row["url"]):
                self._block_ssrf(row["delivery_id"])
                blocked += 1
        return blocked

    def _block_ssrf(self, delivery_id: str) -> None:
        self._db.finalize_delivery(delivery_id, "failed", None, "ssrf_blocked", None)

    def _run(self) -> None:
        while not self._stop.is_set():
            dispatched = False
            with self._lock:
                if len(self._in_flight) < self._config.worker_concurrency:
                    for row in self._db.select_eligible(util.now_ms(), self._config.max_attempts):
                        if len(self._in_flight) >= self._config.worker_concurrency:
                            break
                        if row["endpoint_id"] in self._in_flight_endpoints:
                            continue
                        attempt_no = self._db.claim_delivery(row["id"])
                        if attempt_no is None:
                            continue
                        self._in_flight.add(row["id"])
                        self._in_flight_endpoints.add(row["endpoint_id"])
                        self._executor.submit(
                            self._deliver, row["id"], row["endpoint_id"], attempt_no
                        )
                        dispatched = True
            if not dispatched:
                self._wake.wait(POLL_INTERVAL_S)
            self._wake.clear()

    def _deliver(self, delivery_id: str, endpoint_id: str, attempt_no: int) -> None:
        try:
            self._attempt(delivery_id, endpoint_id, attempt_no)
        except Exception as exc:  # noqa: BLE001 - never let a task kill the pool
            util.log("worker.error", delivery_id=delivery_id, error=repr(exc))
            self._db.finalize_delivery(delivery_id, "failed", None, "internal_error", None)
        finally:
            with self._lock:
                self._in_flight.discard(delivery_id)
                self._in_flight_endpoints.discard(endpoint_id)
            self._wake.set()

    def _attempt(self, delivery_id: str, endpoint_id: str, attempt_no: int) -> None:
        endpoint = self._db.get_endpoint(endpoint_id)
        row = self._db.get_delivery(delivery_id)
        if endpoint is None or row is None:
            return
        event = self._db.get_event(row["event_id"])
        if event is None:
            return

        if not self._config.allow_localhost and delivery_mod.url_is_loopback(endpoint["url"]):
            self._block_ssrf(delivery_id)
            return

        body = delivery_mod.build_body(
            event["id"], event["type"], event["payload"], event["created_at"]
        )
        timestamp = delivery_mod.unix_seconds()
        headers = {
            "Content-Type": "application/json",
            "X-Webhook-Id": event["id"],
            "X-Webhook-Timestamp": timestamp,
            "X-Webhook-Signature": delivery_mod.sign(endpoint["secret"], timestamp, body),
        }
        http_status, last_error = delivery_mod.attempt(
            endpoint["url"], body, headers, self._config.timeout_ms
        )
        self._finalize(delivery_id, attempt_no, http_status, last_error)

    def _finalize(
        self, delivery_id: str, attempt_no: int, http_status: int | None, last_error: str | None
    ) -> None:
        if last_error is None and http_status is not None and 200 <= http_status <= 299:
            self._db.finalize_delivery(delivery_id, "succeeded", http_status, None, None)
            util.log("delivery.succeeded", delivery_id=delivery_id, attempts=attempt_no)
            return
        if attempt_no >= self._config.max_attempts:
            self._db.finalize_delivery(delivery_id, "failed", http_status, last_error, None)
            util.log(
                "delivery.failed",
                delivery_id=delivery_id,
                attempts=attempt_no,
                http_status=http_status,
                error=last_error,
            )
            return
        delay_ms = self._config.backoff_base_ms * (2 ** (attempt_no - 1))
        next_retry_at = util.future_ms(delay_ms)
        self._db.finalize_delivery(delivery_id, "retrying", http_status, last_error, next_retry_at)
        util.log(
            "delivery.retrying",
            delivery_id=delivery_id,
            attempts=attempt_no,
            http_status=http_status,
            error=last_error,
            delay_ms=delay_ms,
        )
