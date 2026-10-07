"""SQLite persistence layer.

A single connection guarded by a re-entrant lock keeps the storage model simple:
throughput is low, and every write is serialized by the lock anyway.
"""

from __future__ import annotations

import json
import sqlite3
import threading

from . import ids, util

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS endpoints (
    id         TEXT PRIMARY KEY,
    url        TEXT NOT NULL,
    events     TEXT NOT NULL,
    secret     TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    id         TEXT PRIMARY KEY,
    type       TEXT NOT NULL,
    payload    TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS deliveries (
    id               TEXT PRIMARY KEY,
    endpoint_id      TEXT NOT NULL REFERENCES endpoints(id) ON DELETE CASCADE,
    event_id         TEXT NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    seq              INTEGER NOT NULL,
    status           TEXT NOT NULL,
    attempts         INTEGER NOT NULL DEFAULT 0,
    last_http_status INTEGER,
    last_error       TEXT,
    next_retry_at    TEXT,
    replayed_from    TEXT,
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_deliveries_endpoint_seq ON deliveries(endpoint_id, seq);
CREATE INDEX IF NOT EXISTS idx_deliveries_status_next ON deliveries(status, next_retry_at);
CREATE INDEX IF NOT EXISTS idx_deliveries_event ON deliveries(event_id);
"""

TERMINAL_STATUSES = ("succeeded", "failed")
VALID_STATUSES = ("pending", "delivering", "retrying", "succeeded", "failed")


class Database:
    def __init__(self, path: str) -> None:
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(path, check_same_thread=False, timeout=5.0)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.executescript(SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # -- low level ---------------------------------------------------------

    def execute(self, sql: str, params: tuple = ()) -> None:
        with self._lock:
            self._conn.execute(sql, params)
            self._conn.commit()

    def query(self, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(sql, params).fetchall()

    def query_one(self, sql: str, params: tuple = ()) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(sql, params).fetchone()

    def _next_seq_locked(self) -> int:
        row = self._conn.execute("SELECT value FROM meta WHERE key='seq'").fetchone()
        if row is None:
            seq = 1
            self._conn.execute("INSERT INTO meta(key, value) VALUES('seq', ?)", (str(seq),))
        else:
            seq = int(row["value"]) + 1
            self._conn.execute("UPDATE meta SET value=? WHERE key='seq'", (str(seq),))
        return seq

    # -- endpoints ---------------------------------------------------------

    def insert_endpoint(self, url: str, events: list[str], secret: str) -> dict:
        endpoint = {
            "id": ids.new_id("ep_"),
            "url": url,
            "events": json.dumps(events, ensure_ascii=False),
            "secret": secret,
            "created_at": util.now_ms(),
        }
        self.execute(
            "INSERT INTO endpoints(id, url, events, secret, created_at) VALUES(?,?,?,?,?)",
            (
                endpoint["id"],
                endpoint["url"],
                endpoint["events"],
                endpoint["secret"],
                endpoint["created_at"],
            ),
        )
        return endpoint

    def get_endpoint(self, endpoint_id: str) -> sqlite3.Row | None:
        return self.query_one("SELECT * FROM endpoints WHERE id=?", (endpoint_id,))

    def list_endpoints(self) -> list[sqlite3.Row]:
        return self.query("SELECT * FROM endpoints ORDER BY created_at ASC, id ASC")

    def delete_endpoint(self, endpoint_id: str) -> bool:
        with self._lock:
            cur = self._conn.execute("DELETE FROM endpoints WHERE id=?", (endpoint_id,))
            self._conn.commit()
            return cur.rowcount > 0

    # -- events ------------------------------------------------------------

    def insert_event(self, event_type: str, payload_json: str) -> dict:
        event = {
            "id": ids.new_id("evt_"),
            "type": event_type,
            "payload": payload_json,
            "created_at": util.now_ms(),
        }
        self.execute(
            "INSERT INTO events(id, type, payload, created_at) VALUES(?,?,?,?)",
            (event["id"], event["type"], event["payload"], event["created_at"]),
        )
        return event

    def get_event(self, event_id: str) -> sqlite3.Row | None:
        return self.query_one("SELECT * FROM events WHERE id=?", (event_id,))

    # -- deliveries --------------------------------------------------------

    def _insert_delivery_locked(self, row: dict) -> None:
        self._conn.execute(
            """
            INSERT INTO deliveries(
                id, endpoint_id, event_id, seq, status, attempts,
                last_http_status, last_error, next_retry_at, replayed_from,
                created_at, updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                row["id"],
                row["endpoint_id"],
                row["event_id"],
                row["seq"],
                row["status"],
                row["attempts"],
                row["last_http_status"],
                row["last_error"],
                row["next_retry_at"],
                row["replayed_from"],
                row["created_at"],
                row["updated_at"],
            ),
        )

    def create_deliveries(self, event_id: str, endpoint_ids: list[str]) -> int:
        if not endpoint_ids:
            return 0
        now = util.now_ms()
        with self._lock:
            try:
                for endpoint_id in endpoint_ids:
                    self._insert_delivery_locked(
                        {
                            "id": ids.new_id("dlv_"),
                            "endpoint_id": endpoint_id,
                            "event_id": event_id,
                            "seq": self._next_seq_locked(),
                            "status": "pending",
                            "attempts": 0,
                            "last_http_status": None,
                            "last_error": None,
                            "next_retry_at": None,
                            "replayed_from": None,
                            "created_at": now,
                            "updated_at": now,
                        }
                    )
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
        return len(endpoint_ids)

    def get_delivery(self, delivery_id: str) -> sqlite3.Row | None:
        return self.query_one("SELECT * FROM deliveries WHERE id=?", (delivery_id,))

    def create_replay(self, original: sqlite3.Row) -> dict:
        now = util.now_ms()
        new_id = ids.new_id("dlv_")
        with self._lock:
            seq = self._next_seq_locked()
            self._insert_delivery_locked(
                {
                    "id": new_id,
                    "endpoint_id": original["endpoint_id"],
                    "event_id": original["event_id"],
                    "seq": seq,
                    "status": "pending",
                    "attempts": 0,
                    "last_http_status": None,
                    "last_error": None,
                    "next_retry_at": None,
                    "replayed_from": original["id"],
                    "created_at": now,
                    "updated_at": now,
                }
            )
            self._conn.commit()
        return self.get_delivery(new_id)

    def list_deliveries(
        self,
        endpoint_id: str | None,
        event_id: str | None,
        status: str | None,
        limit: int,
        offset: int,
    ) -> tuple[list[sqlite3.Row], int]:
        clauses: list[str] = []
        params: list = []
        if endpoint_id is not None:
            clauses.append("endpoint_id = ?")
            params.append(endpoint_id)
        if event_id is not None:
            clauses.append("event_id = ?")
            params.append(event_id)
        if status is not None:
            clauses.append("status = ?")
            params.append(status)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        total_row = self.query_one(f"SELECT COUNT(*) AS n FROM deliveries{where}", tuple(params))
        total = int(total_row["n"]) if total_row else 0
        rows = self.query(
            f"SELECT * FROM deliveries{where} ORDER BY created_at ASC, id ASC LIMIT ? OFFSET ?",
            tuple(params) + (limit, offset),
        )
        return rows, total

    # -- worker support ----------------------------------------------------

    def select_eligible(self, now: str, max_attempts: int) -> list[sqlite3.Row]:
        return self.query(
            """
            SELECT * FROM deliveries d
            WHERE d.status IN ('pending', 'retrying')
              AND (d.next_retry_at IS NULL OR d.next_retry_at <= ?)
              AND d.attempts < ?
              AND d.seq = (
                  SELECT MIN(d2.seq) FROM deliveries d2
                  WHERE d2.endpoint_id = d.endpoint_id
                    AND d2.status NOT IN ('succeeded', 'failed')
              )
            ORDER BY d.seq ASC
            """,
            (now, max_attempts),
        )

    def claim_delivery(self, delivery_id: str) -> int | None:
        """Move a delivery to ``delivering`` and increment ``attempts``.

        Returns the new attempt number, or ``None`` if the row was no longer eligible.
        """
        now = util.now_ms()
        with self._lock:
            row = self._conn.execute(
                "SELECT attempts FROM deliveries WHERE id=? AND status IN ('pending','retrying')",
                (delivery_id,),
            ).fetchone()
            if row is None:
                return None
            attempt_no = int(row["attempts"]) + 1
            self._conn.execute(
                """
                UPDATE deliveries
                SET status='delivering', attempts=?, next_retry_at=NULL, updated_at=?
                WHERE id=?
                """,
                (attempt_no, now, delivery_id),
            )
            self._conn.commit()
            return attempt_no

    def finalize_delivery(
        self,
        delivery_id: str,
        status: str,
        http_status: int | None,
        last_error: str | None,
        next_retry_at: str | None,
    ) -> None:
        self.execute(
            """
            UPDATE deliveries
            SET status=?, last_http_status=?, last_error=?, next_retry_at=?, updated_at=?
            WHERE id=?
            """,
            (status, http_status, last_error, next_retry_at, util.now_ms(), delivery_id),
        )

    def recover_delivering(self, max_attempts: int) -> int:
        now = util.now_ms()
        with self._lock:
            # An interrupted final attempt has no retries left: mark it failed
            # instead of retrying, otherwise it would block its endpoint's
            # queue forever (selection requires attempts < max_attempts).
            cur = self._conn.execute(
                """
                UPDATE deliveries
                SET status='failed', next_retry_at=NULL, updated_at=?
                WHERE status='delivering' AND attempts >= ?
                """,
                (now, max_attempts),
            )
            recovered = cur.rowcount
            cur = self._conn.execute(
                """
                UPDATE deliveries
                SET status='retrying', next_retry_at=?, updated_at=?
                WHERE status='delivering'
                """,
                (now, now),
            )
            recovered += cur.rowcount
            self._conn.commit()
            return recovered

    def pending_deliveries_with_endpoint(self) -> list[sqlite3.Row]:
        return self.query(
            """
            SELECT d.id AS delivery_id, e.url AS url
            FROM deliveries d JOIN endpoints e ON e.id = d.endpoint_id
            WHERE d.status IN ('pending', 'retrying')
            """
        )
