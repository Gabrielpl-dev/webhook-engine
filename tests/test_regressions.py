"""Regressions for SSRF resolution and interrupted final attempts."""

import io
import socket
import unittest
from unittest.mock import Mock, patch

from src import delivery, util
from src.config import Config
from src.db import Database
from src.worker import Worker


def address(ip):
    family = socket.AF_INET6 if ":" in ip else socket.AF_INET
    sockaddr = (ip, 80, 0, 0) if family == socket.AF_INET6 else (ip, 80)
    return (family, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", sockaddr)


class SSRFRegressionTest(unittest.TestCase):
    def test_mapped_loopback_is_blocked(self):
        with patch("src.delivery.socket.getaddrinfo", return_value=[address("::ffff:127.0.0.1")]):
            self.assertTrue(delivery.url_is_loopback("http://[::ffff:127.0.0.1]/"))

    def test_connection_uses_validated_address(self):
        connected = []
        resolutions = []
        sock = Mock()
        sock.makefile.return_value = io.BytesIO(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
        sock.connect.side_effect = lambda target: connected.append(target[0])

        def resolve(host, *args, **kwargs):
            resolutions.append(host)
            return [address("93.184.216.34" if len(resolutions) == 1 else "127.0.0.1")]

        with (
            patch("src.delivery.socket.getaddrinfo", side_effect=resolve),
            patch("src.delivery.socket.socket", return_value=sock),
        ):
            self.assertEqual(delivery.attempt("http://example.test/", b"{}", {}, 1000), (200, None))
        self.assertEqual(connected, ["93.184.216.34"])
        self.assertEqual(resolutions, ["example.test"])
        self.assertIn(b"Host: example.test\r\n", sock.sendall.call_args_list[0].args[0])

    def test_attempt_blocks_mixed_dns_answers(self):
        with (
            patch(
                "src.delivery.socket.getaddrinfo",
                return_value=[address("93.184.216.34"), address("::ffff:127.0.0.1")],
            ),
            patch("src.delivery.socket.socket") as sock,
        ):
            self.assertEqual(
                delivery.attempt("http://example.test/", b"{}", {}, 1000), (None, "ssrf_blocked")
            )
            sock.assert_not_called()


class RecoveryRegressionTest(unittest.TestCase):
    def test_final_attempt_recovery_releases_endpoint_queue(self):
        db = Database(":memory:")
        self.addCleanup(db.close)
        endpoint = db.insert_endpoint("http://example.test/", ["event"], "secret")
        for _ in range(2):
            event = db.insert_event("event", "{}")
            db.create_deliveries(event["id"], [endpoint["id"]])
        first, second = db.query("SELECT * FROM deliveries ORDER BY seq")
        self.assertEqual(db.claim_delivery(first["id"]), 1)
        worker = Worker(db, Config(8080, ".", 50, 1, True, 1000, 1))
        self.addCleanup(worker.stop)
        worker.recover()
        recovered = db.get_delivery(first["id"])
        self.assertEqual(recovered["status"], "failed")
        self.assertEqual(recovered["attempts"], 1)
        self.assertIsNone(recovered["next_retry_at"])
        self.assertEqual(
            [row["id"] for row in db.select_eligible(util.now_ms(), 1)], [second["id"]]
        )
