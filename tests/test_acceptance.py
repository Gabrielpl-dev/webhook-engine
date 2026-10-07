"""Black-box acceptance tests mapped to AC-1..AC-20 of the specification.

Every test starts the real ``./serve`` binary in a subprocess with a fresh
temporary DATA_DIR and an ephemeral port, then talks to it only over HTTP.
"""

from __future__ import annotations

import hashlib
import hmac
import os
import re
import socket
import subprocess
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from helpers import (  # noqa: E402
    SERVE,
    Engine,
    Receiver,
    StallingReceiver,
    free_port,
    get_deliveries,
    json_post,
    one_delivery,
    publish,
    register_endpoint,
    request,
    wait_for,
)

SECRET_RE = re.compile(r"^whsec_[0-9a-f]{64}$")
ID_RE = re.compile(r"^(ep|evt|dlv)_[0-9a-f]{24}$")


def tmp_dir(prefix: str) -> str:
    return tempfile.mkdtemp(prefix=prefix)


class AcceptanceTest(unittest.TestCase):
    def engine(self, env: dict | None = None) -> Engine:
        instance = Engine(env=env)
        instance.start()
        self.addCleanup(instance.stop)
        return instance

    def receiver(self, default_status: int = 200) -> Receiver:
        instance = Receiver(default_status=default_status)
        self.addCleanup(instance.close)
        return instance

    # -- AC-1 --------------------------------------------------------------

    def test_ac01_starts_and_health_within_5s(self) -> None:
        engine = self.engine()
        self.assertLess(engine.ready_seconds, 5.0)
        resp = request("GET", engine.port, "/health")
        self.assertEqual(resp.status, 200)
        self.assertEqual(resp.json, {"status": "ok"})
        self.assertTrue(resp.headers["content-type"].startswith("application/json"))
        self.assertTrue(os.path.exists(os.path.join(engine.data_dir, "webhooks.db")))

    # -- AC-2 --------------------------------------------------------------

    def test_ac02_register_and_deliver_once(self) -> None:
        engine = self.engine()
        receiver = self.receiver()
        created = register_endpoint(engine.port, receiver.url(), ["order.created"])
        self.assertEqual(created.status, 201)
        self.assertRegex(created.json["id"], ID_RE)
        self.assertRegex(created.json["secret"], SECRET_RE)
        self.assertEqual(created.json["events"], ["order.created"])

        published = publish(engine.port, "order.created", {"order_id": "A-1"})
        self.assertEqual(published.status, 202)
        self.assertEqual(published.json["deliveries_created"], 1)
        self.assertTrue(wait_for(lambda: receiver.count() == 1, timeout=2.0))
        self.assertEqual(receiver.count(), 1)

    # -- AC-3 --------------------------------------------------------------

    def test_ac03_signature_verifies(self) -> None:
        engine = self.engine()
        receiver = self.receiver()
        secret = register_endpoint(engine.port, receiver.url(), ["order.created"]).json["secret"]
        publish(engine.port, "order.created", {"order_id": "A-1", "total": 12990})
        self.assertTrue(wait_for(lambda: receiver.count() == 1))
        record = receiver.records[0]
        timestamp = record["headers"]["x-webhook-timestamp"]
        raw_body = record["body"]
        expected = (
            "sha256="
            + hmac.new(
                secret.encode(), timestamp.encode() + b"." + raw_body, hashlib.sha256
            ).hexdigest()
        )
        self.assertEqual(record["headers"]["x-webhook-signature"], expected)
        self.assertRegex(expected, r"^sha256=[0-9a-f]{64}$")

    # -- AC-4 --------------------------------------------------------------

    def test_ac04_body_shape_and_event_id_header(self) -> None:
        engine = self.engine()
        receiver = self.receiver()
        register_endpoint(engine.port, receiver.url(), ["order.created"])
        published = publish(engine.port, "order.created", {"order_id": "A-1"})
        self.assertTrue(wait_for(lambda: receiver.count() == 1))
        record = receiver.records[0]
        body = record["json"]
        self.assertEqual(set(body.keys()), {"event_id", "type", "payload", "published_at"})
        self.assertEqual(body["event_id"], published.json["id"])
        self.assertEqual(body["payload"], {"order_id": "A-1"})
        self.assertEqual(record["headers"]["x-webhook-id"], body["event_id"])

    # -- AC-5 --------------------------------------------------------------

    def test_ac05_secret_not_exposed(self) -> None:
        engine = self.engine()
        receiver = self.receiver()
        created = register_endpoint(engine.port, receiver.url(), ["order.created"]).json
        listed = request("GET", engine.port, "/endpoints")
        self.assertEqual(listed.status, 200)
        self.assertNotIn("secret", listed.json["endpoints"][0])
        fetched = request("GET", engine.port, f"/endpoints/{created['id']}")
        self.assertEqual(fetched.status, 200)
        self.assertNotIn("secret", fetched.json)

    # -- AC-6 --------------------------------------------------------------

    def test_ac06_retries_with_exponential_backoff(self) -> None:
        engine = self.engine({"WEBHOOK_BACKOFF_BASE_MS": "100"})
        receiver = self.receiver()
        receiver.plan({"status": 500}, {"status": 500}, {"status": 200})
        endpoint = register_endpoint(engine.port, receiver.url(), ["order.created"]).json
        publish(engine.port, "order.created", {"n": 1})
        path = f"/endpoints/{endpoint['id']}/deliveries"
        delivery = wait_for(
            lambda: (lambda d: d if d["status"] == "succeeded" else None)(
                one_delivery(engine.port, path)
            )
        )
        self.assertIsNotNone(delivery)
        self.assertEqual(delivery["attempts"], 3)
        self.assertEqual(delivery["last_http_status"], 200)
        self.assertIsNone(delivery["next_retry_at"])
        self.assertEqual(receiver.count(), 3)
        intervals = [
            receiver.records[i]["arrived"] - receiver.records[i - 1]["arrived"] for i in range(1, 3)
        ]
        self.assertAlmostEqual(intervals[0], 0.1, delta=0.05)
        self.assertAlmostEqual(intervals[1], 0.2, delta=0.1)

    # -- AC-7 --------------------------------------------------------------

    def test_ac07_gives_up_after_max_attempts(self) -> None:
        engine = self.engine()
        receiver = self.receiver(default_status=500)
        endpoint = register_endpoint(engine.port, receiver.url(), ["order.created"]).json
        publish(engine.port, "order.created", {"n": 1})
        path = f"/endpoints/{endpoint['id']}/deliveries"
        delivery = wait_for(
            lambda: (lambda d: d if d["status"] == "failed" else None)(
                one_delivery(engine.port, path)
            )
        )
        self.assertIsNotNone(delivery)
        self.assertEqual(delivery["attempts"], 3)
        self.assertEqual(delivery["last_http_status"], 500)
        self.assertIsNone(delivery["next_retry_at"])
        time.sleep(1.0)
        self.assertEqual(receiver.count(), 3)

    # -- AC-8 --------------------------------------------------------------

    def test_ac08_connection_error_is_not_timeout(self) -> None:
        engine = self.engine()
        closed_port = free_port()
        endpoint = register_endpoint(
            engine.port, f"http://127.0.0.1:{closed_port}/hook", ["x"]
        ).json
        publish(engine.port, "x", {"n": 1})
        path = f"/endpoints/{endpoint['id']}/deliveries"
        delivery = wait_for(
            lambda: (lambda d: d if d["status"] == "failed" else None)(
                one_delivery(engine.port, path)
            ),
            timeout=5.0,
        )
        self.assertIsNotNone(delivery)
        self.assertEqual(delivery["last_error"], "connection_error")
        self.assertIsNone(delivery["last_http_status"])
        self.assertEqual(delivery["attempts"], 3)

    # -- AC-9 --------------------------------------------------------------

    def test_ac09_timeout_on_stalling_receiver(self) -> None:
        engine = self.engine({"WEBHOOK_TIMEOUT_MS": "300"})
        stall = StallingReceiver()
        self.addCleanup(stall.close)
        endpoint = register_endpoint(engine.port, stall.url(), ["x"]).json
        started = time.time()
        publish(engine.port, "x", {"n": 1})
        path = f"/endpoints/{endpoint['id']}/deliveries"
        delivery = wait_for(
            lambda: (lambda d: d if d["last_error"] == "timeout" else None)(
                one_delivery(engine.port, path)
            ),
            timeout=2.0,
        )
        self.assertIsNotNone(delivery)
        self.assertLess(time.time() - started, 2.0)
        self.assertIsNone(delivery["last_http_status"])

    # -- AC-10 -------------------------------------------------------------

    def test_ac10_replay_failed_delivery(self) -> None:
        engine = self.engine({"WEBHOOK_BACKOFF_BASE_MS": "20"})
        receiver = self.receiver(default_status=500)
        endpoint = register_endpoint(engine.port, receiver.url(), ["order.created"]).json
        published = publish(engine.port, "order.created", {"n": 1})
        path = f"/endpoints/{endpoint['id']}/deliveries"
        delivery = wait_for(
            lambda: (lambda d: d if d["status"] == "failed" else None)(
                one_delivery(engine.port, path)
            )
        )
        self.assertIsNotNone(delivery)
        receiver.set_default(200)
        replay = request("POST", engine.port, f"/deliveries/{delivery['id']}/replay")
        self.assertEqual(replay.status, 202)
        self.assertEqual(replay.json["status"], "pending")
        self.assertEqual(replay.json["attempts"], 0)
        self.assertEqual(replay.json["replayed_from"], delivery["id"])
        self.assertTrue(wait_for(lambda: receiver.count() == 4))
        self.assertEqual(receiver.records[-1]["json"]["event_id"], published.json["id"])

    # -- AC-11 -------------------------------------------------------------

    def test_ac11_replay_conflict_while_retrying(self) -> None:
        engine = self.engine({"WEBHOOK_BACKOFF_BASE_MS": "5000"})
        receiver = self.receiver(default_status=500)
        endpoint = register_endpoint(engine.port, receiver.url(), ["order.created"]).json
        publish(engine.port, "order.created", {"n": 1})
        path = f"/endpoints/{endpoint['id']}/deliveries"
        delivery = wait_for(
            lambda: (lambda d: d if d["status"] == "retrying" else None)(
                one_delivery(engine.port, path)
            )
        )
        self.assertIsNotNone(delivery)
        replay = request("POST", engine.port, f"/deliveries/{delivery['id']}/replay")
        self.assertEqual(replay.status, 409)
        self.assertEqual(replay.json["error"]["code"], "conflict")

    # -- AC-12 -------------------------------------------------------------

    def test_ac12_ssrf_blocked_without_allow_localhost(self) -> None:
        engine = self.engine({"WEBHOOK_ALLOW_LOCALHOST": "0"})
        resp = register_endpoint(engine.port, "http://127.0.0.1:9000/hook", ["x"])
        self.assertEqual(resp.status, 422)
        self.assertEqual(resp.json["error"]["code"], "ssrf_blocked")
        self.assertEqual(request("GET", engine.port, "/endpoints").json["endpoints"], [])

    # -- AC-13 -------------------------------------------------------------

    def test_ac13_payload_validation(self) -> None:
        engine = self.engine()
        bad_payload = json_post(engine.port, "/events", {"type": "x", "payload": [1, 2]})
        self.assertEqual(bad_payload.status, 422)
        self.assertEqual(bad_payload.json["error"]["code"], "invalid_payload")

        malformed = request(
            "POST",
            engine.port,
            "/events",
            raw_body=b"{",
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(malformed.status, 400)
        self.assertEqual(malformed.json["error"]["code"], "invalid_json")

        oversized = json_post(
            engine.port, "/events", {"type": "x", "payload": {"blob": "a" * 300000}}
        )
        self.assertEqual(oversized.status, 413)
        self.assertEqual(oversized.json["error"]["code"], "payload_too_large")

    # -- AC-14 -------------------------------------------------------------

    def test_ac14_fifo_ordering_per_endpoint(self) -> None:
        engine = self.engine()
        receiver = self.receiver()
        receiver.plan({"status": 200, "delay": 0.5})
        register_endpoint(engine.port, receiver.url(), ["x"])
        first = publish(engine.port, "x", {"n": 1}).json["id"]
        second = publish(engine.port, "x", {"n": 2}).json["id"]
        self.assertTrue(wait_for(lambda: receiver.count() >= 2, timeout=5.0))
        self.assertEqual(receiver.records[0]["json"]["event_id"], first)
        self.assertEqual(receiver.records[1]["json"]["event_id"], second)
        gap = receiver.records[1]["arrived"] - receiver.records[0]["arrived"]
        self.assertGreaterEqual(gap, 0.3)

    # -- AC-15 -------------------------------------------------------------

    def test_ac15_attempts_survive_sigkill(self) -> None:
        engine = Engine(env={"WEBHOOK_BACKOFF_BASE_MS": "500"})
        engine.start()
        self.addCleanup(engine.stop)
        receiver = self.receiver(default_status=500)
        endpoint = register_endpoint(engine.port, receiver.url(), ["x"]).json
        publish(engine.port, "x", {"n": 1})
        path = f"/endpoints/{endpoint['id']}/deliveries"
        delivery = wait_for(
            lambda: (lambda d: d if d["status"] == "retrying" else None)(
                one_delivery(engine.port, path)
            )
        )
        self.assertIsNotNone(delivery)
        self.assertEqual(delivery["attempts"], 1)
        engine.kill()
        engine.restart()
        final = wait_for(
            lambda: (lambda d: d if d["status"] == "failed" else None)(
                one_delivery(engine.port, path)
            ),
            timeout=5.0,
        )
        self.assertIsNotNone(final)
        self.assertEqual(final["attempts"], 3)
        self.assertEqual(receiver.count(), 3)

    # -- AC-16 -------------------------------------------------------------

    def test_ac16_delete_endpoint_cascades(self) -> None:
        engine = self.engine()
        receiver = self.receiver()
        endpoint = register_endpoint(engine.port, receiver.url(), ["x"]).json
        event = publish(engine.port, "x", {"n": 1})
        self.assertTrue(wait_for(lambda: receiver.count() == 1))
        deleted = request("DELETE", engine.port, f"/endpoints/{endpoint['id']}")
        self.assertEqual(deleted.status, 204)
        self.assertEqual(deleted.raw, b"")
        missing = request("GET", engine.port, f"/endpoints/{endpoint['id']}/deliveries")
        self.assertEqual(missing.status, 404)
        event_deliveries = request("GET", engine.port, f"/events/{event.json['id']}/deliveries")
        self.assertEqual(event_deliveries.status, 200)
        self.assertEqual(event_deliveries.json["deliveries"], [])
        self.assertEqual(event_deliveries.json["total"], 0)

    # -- AC-17 -------------------------------------------------------------

    def test_ac17_no_subscribers(self) -> None:
        engine = self.engine()
        receiver = self.receiver()
        register_endpoint(engine.port, receiver.url(), ["other.type"])
        published = publish(engine.port, "unsubscribed.type", {"n": 1})
        self.assertEqual(published.status, 202)
        self.assertEqual(published.json["deliveries_created"], 0)
        fetched = request("GET", engine.port, f"/events/{published.json['id']}")
        self.assertEqual(fetched.status, 200)
        time.sleep(0.3)
        self.assertEqual(receiver.count(), 0)

    # -- AC-18 -------------------------------------------------------------

    def test_ac18_filter_and_limit(self) -> None:
        engine = self.engine({"WEBHOOK_BACKOFF_BASE_MS": "20"})
        receiver = self.receiver(default_status=500)
        endpoint = register_endpoint(engine.port, receiver.url(), ["x"]).json
        publish(engine.port, "x", {"n": 1})
        publish(engine.port, "x", {"n": 2})
        path = f"/endpoints/{endpoint['id']}/deliveries"

        def two_failed():
            deliveries = get_deliveries(engine.port, path)
            return (
                deliveries if sum(1 for d in deliveries if d["status"] == "failed") == 2 else None
            )

        self.assertIsNotNone(wait_for(two_failed, timeout=5.0))
        filtered = request("GET", engine.port, f"{path}?status=failed&limit=1")
        self.assertEqual(filtered.status, 200)
        self.assertEqual(len(filtered.json["deliveries"]), 1)
        self.assertEqual(filtered.json["total"], 2)
        self.assertEqual(filtered.json["deliveries"][0]["status"], "failed")

    # -- AC-19 -------------------------------------------------------------

    def test_ac19_error_envelope_everywhere(self) -> None:
        engine = self.engine()
        cases = [
            request("GET", engine.port, "/nope"),
            request("PUT", engine.port, "/endpoints"),
            request(
                "POST",
                engine.port,
                "/endpoints",
                raw_body=b"{}",
                headers={"Content-Type": "text/plain"},
            ),
            json_post(engine.port, "/endpoints", {"events": ["x"]}),
            json_post(engine.port, "/events", {"type": "x", "payload": []}),
        ]
        expected = [
            (404, "not_found"),
            (405, "method_not_allowed"),
            (400, "invalid_json"),
            (422, "invalid_url"),
            (422, "invalid_payload"),
        ]
        for resp, (status, code) in zip(cases, expected, strict=True):
            with self.subTest(code=code):
                self.assertEqual(resp.status, status)
                self.assertTrue(resp.headers["content-type"].startswith("application/json"))
                self.assertEqual(set(resp.json.keys()), {"error"})
                self.assertEqual(resp.json["error"]["code"], code)
                self.assertIsInstance(resp.json["error"]["message"], str)
                self.assertIsInstance(resp.json["error"]["details"], dict)

    # -- AC-20 -------------------------------------------------------------

    def test_ac20_startup_failures_exit_nonzero(self) -> None:
        env = os.environ.copy()
        env.update(
            {"DATA_DIR": os.path.join(tmp_dir("wh-ac20-"), "d"), "WEBHOOK_ALLOW_LOCALHOST": "1"}
        )

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as occupied:
            occupied.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            occupied.bind(("0.0.0.0", 0))
            occupied.listen(1)
            port = occupied.getsockname()[1]
            busy = subprocess.run(
                [SERVE, "--port", str(port)], env=env, capture_output=True, timeout=10
            )
        self.assertEqual(busy.returncode, 1)
        self.assertTrue(busy.stderr.strip())

        bad_env = os.environ.copy()
        bad_env.update(
            {"DATA_DIR": os.path.join(tmp_dir("wh-ac20-"), "d2"), "WEBHOOK_MAX_ATTEMPTS": "0"}
        )
        invalid = subprocess.run(
            [SERVE, "--port", str(free_port())], env=bad_env, capture_output=True, timeout=10
        )
        self.assertEqual(invalid.returncode, 1)
        self.assertTrue(invalid.stderr.strip())


if __name__ == "__main__":
    unittest.main(verbosity=2)
