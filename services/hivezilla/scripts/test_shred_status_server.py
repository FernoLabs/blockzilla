#!/usr/bin/env python3

import importlib.util
from contextlib import contextmanager
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import stat
import sys
import tempfile
import threading
import unittest
from unittest import mock


SCRIPT = Path(__file__).with_name("shred_status_server.py")
SPEC = importlib.util.spec_from_file_location("shred_status_server", SCRIPT)
assert SPEC and SPEC.loader
status_server = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = status_server
SPEC.loader.exec_module(status_server)


def hivezilla_status(now=2_000, **overrides):
    value = {
        "schema_version": 1,
        "updated_unix_secs": now,
        "started_unix_secs": 1_900,
        "state": "receiving",
        "accepted_total": 500,
        "invalid_total": 2,
        "bytes_total": 614_400,
        "durable_through_sequence": 7_499,
        "latest_slot": 433_735_944,
        "shred_version": 50_093,
        "last_durable_unix_secs": now,
        "spool_bytes": 1_048_576,
        "spool_max_bytes": 21_474_836_480,
        "filesystem_free_bytes": 42_949_672_960,
        "filesystem_total_bytes": 64_424_509_440,
        "reserve_free_bytes": 2_147_483_648,
    }
    value.update(overrides)
    return value


def receiver_metrics(**overrides):
    value = {
        "identity": "secret-public-node-id",
        "advertised_ip": "203.0.113.25",
        "gossip_port": 18_001,
        "tvu_port": 18_002,
        "shred_version": 50_093,
        "uptime_seconds": 900,
        "gossip_peers": 2_000,
        "recent_gossip_peers": 37,
        "tvu_peers": 1_500,
        "packets_total": 1_000,
        "bytes_total": 1_228_800,
        "parsed_total": 990,
        "invalid_total": 3,
        "version_mismatch_total": 7,
        "unique_total": 800,
        "duplicates_total": 190,
        "data_total": 700,
        "code_total": 290,
        "forward_targets": 1,
        "forwarded_datagrams_total": 985,
        "forward_errors_total": 5,
        "tracked_sources": 22,
        "latest_slot": 433_735_944,
        "seconds_since_last_packet": 2,
    }
    value.update(overrides)
    return value


class MetricsHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    body = b"{}"
    status = 200
    content_type = "application/json"
    declared_length = None

    def do_GET(self):
        if self.path != "/metrics":
            self.send_error(404)
            return
        self.send_response(self.status)
        self.send_header("Content-Type", self.content_type)
        length = len(self.body) if self.declared_length is None else self.declared_length
        self.send_header("Content-Length", str(length))
        self.end_headers()
        self.wfile.write(self.body)

    def log_message(self, _format, *_args):
        return


@contextmanager
def metrics_service(body, *, status=200, content_type="application/json", declared_length=None):
    handler = type(
        "ConfiguredMetricsHandler",
        (MetricsHandler,),
        {
            "body": body,
            "status": status,
            "content_type": content_type,
            "declared_length": declared_length,
        },
    )
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    try:
        yield server.server_address[1]
    finally:
        server.shutdown()
        server.server_close()
        worker.join(timeout=2)


@contextmanager
def public_service(cache, cors_origin=None):
    handler = type(
        "ConfiguredShredStatusHandler",
        (status_server.ShredStatusHandler,),
        {"cache": cache, "cors_origin": cors_origin},
    )
    server = status_server.ShredStatusServer(("127.0.0.1", 0), handler)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    try:
        yield server.server_address[1]
    finally:
        server.shutdown()
        server.server_close()
        worker.join(timeout=2)


class ShredStatusTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.hivezilla_file = self.root / "hivezilla.json"
        self.endpoint = status_server.ReceiverEndpoint("127.0.0.1", 9)
        self.config = status_server.StatusConfig(
            hivezilla_status_file=self.hivezilla_file,
            receiver_endpoint=self.endpoint,
        )

    def tearDown(self):
        self.directory.cleanup()

    def write_hivezilla(self, value=None):
        self.hivezilla_file.write_text(
            json.dumps(hivezilla_status() if value is None else value),
            encoding="utf-8",
        )

    def build(self, receiver=None, hivezilla=None, now=2_000):
        receiver = receiver_metrics() if receiver is None else receiver
        hivezilla = hivezilla_status(now) if hivezilla is None else hivezilla
        return status_server.build_status(
            self.config,
            now_unix_secs=now,
            receiver_loader=lambda: receiver,
            hivezilla_loader=lambda: hivezilla,
        )

    def test_builds_independent_public_truths_and_selects_away_secrets(self):
        receiver = receiver_metrics(
            token="receiver-token-must-not-leak",
            peers=["198.51.100.2:8001"],
            config_path="/data/receiver/config.json",
        )
        hivezilla = hivezilla_status(
            journal_id="deadbeef",
            peer="127.0.0.1:44444",
            spool_path="/data/shred-ingest",
            api_secret="hivezilla-secret-must-not-leak",
        )
        public = self.build(receiver, hivezilla)

        self.assertEqual(public["schema_version"], 1)
        self.assertEqual(public["gossip"]["state"], "observed")
        self.assertEqual(public["gossip"]["recent_peer_count"], 37)
        self.assertEqual(public["tvu"]["state"], "receiving")
        self.assertEqual(public["tvu"]["latest_slot"], 433_735_944)
        self.assertEqual(public["forwarding"]["state"], "sending")
        self.assertEqual(public["forwarding"]["attempts_total"], 990)
        self.assertEqual(public["hivezilla"]["availability"], "available")
        self.assertTrue(public["hivezilla"]["status_fresh"])
        self.assertEqual(public["hivezilla"]["durable_through_sequence"], 7_499)

        encoded = status_server.encode_public_status(public).decode("ascii")
        for forbidden in (
            "secret-public-node-id",
            "203.0.113.25",
            "198.51.100.2",
            "receiver-token",
            "/data/receiver",
            "deadbeef",
            "127.0.0.1:44444",
            "/data/shred-ingest",
            "hivezilla-secret",
        ):
            self.assertNotIn(forbidden, encoded)

    def test_receiver_and_hivezilla_fail_independently(self):
        def unavailable():
            raise OSError("private source path or URL")

        receiver_missing = status_server.build_status(
            self.config,
            now_unix_secs=2_000,
            receiver_loader=unavailable,
            hivezilla_loader=lambda: hivezilla_status(),
        )
        self.assertEqual(receiver_missing["gossip"]["state"], "unavailable")
        self.assertEqual(receiver_missing["tvu"]["state"], "unavailable")
        self.assertEqual(receiver_missing["forwarding"]["state"], "unavailable")
        self.assertEqual(receiver_missing["hivezilla"]["availability"], "available")

        hivezilla_missing = status_server.build_status(
            self.config,
            now_unix_secs=2_000,
            receiver_loader=lambda: receiver_metrics(),
            hivezilla_loader=unavailable,
        )
        self.assertEqual(hivezilla_missing["gossip"]["state"], "observed")
        self.assertEqual(hivezilla_missing["hivezilla"]["state"], "unavailable")
        self.assertFalse(hivezilla_missing["hivezilla"]["status_fresh"])

        recursively_invalid = status_server.build_status(
            self.config,
            now_unix_secs=2_000,
            receiver_loader=lambda: (_ for _ in ()).throw(RecursionError()),
            hivezilla_loader=lambda: hivezilla_status(),
        )
        self.assertEqual(recursively_invalid["gossip"]["state"], "unavailable")
        self.assertEqual(recursively_invalid["hivezilla"]["availability"], "available")

    def test_hivezilla_freshness_is_explicit_and_does_not_rewrite_producer_state(self):
        public = self.build(hivezilla=hivezilla_status(1_979), now=2_000)
        self.assertEqual(public["hivezilla"]["state"], "receiving")
        self.assertFalse(public["hivezilla"]["status_fresh"])

        boundary = self.build(hivezilla=hivezilla_status(1_980), now=2_000)
        self.assertTrue(boundary["hivezilla"]["status_fresh"])

    def test_recovered_durable_tail_is_available_before_this_process_receives(self):
        recovered = hivezilla_status(
            state="waiting",
            accepted_total=0,
            bytes_total=0,
            last_durable_unix_secs=None,
        )
        public = self.build(hivezilla=recovered)
        self.assertEqual(public["hivezilla"]["availability"], "available")
        self.assertEqual(public["hivezilla"]["state"], "waiting")
        self.assertEqual(public["hivezilla"]["durable_through_sequence"], 7_499)
        self.assertIsNone(public["hivezilla"]["last_durable_unix_secs"])

    def test_tvu_waiting_idle_and_receiving_are_not_connection_claims(self):
        waiting = self.build(receiver=receiver_metrics(packets_total=0, parsed_total=0, latest_slot=0, seconds_since_last_packet=None))
        self.assertEqual(waiting["tvu"]["state"], "waiting")
        self.assertIsNone(waiting["tvu"]["latest_slot"])

        idle = self.build(receiver=receiver_metrics(seconds_since_last_packet=31))
        self.assertEqual(idle["tvu"]["state"], "idle")

        receiving = self.build(receiver=receiver_metrics(seconds_since_last_packet=30))
        self.assertEqual(receiving["tvu"]["state"], "receiving")

    def test_gossip_and_forwarding_states_are_explicit(self):
        waiting = self.build(receiver=receiver_metrics(recent_gossip_peers=0, forward_targets=1, forwarded_datagrams_total=0, forward_errors_total=0))
        self.assertEqual(waiting["gossip"]["state"], "waiting")
        self.assertEqual(waiting["forwarding"]["state"], "waiting")

        disabled = self.build(receiver=receiver_metrics(forward_targets=0, forwarded_datagrams_total=0, forward_errors_total=0))
        self.assertEqual(disabled["forwarding"]["state"], "disabled")

        sending = self.build(receiver=receiver_metrics(forward_errors_total=0))
        self.assertEqual(sending["forwarding"]["state"], "sending")

        recovered_after_errors = self.build(receiver=receiver_metrics(forward_errors_total=5))
        self.assertEqual(recovered_after_errors["forwarding"]["state"], "sending")
        self.assertEqual(recovered_after_errors["forwarding"]["errors_total"], 5)

        only_errors = self.build(receiver=receiver_metrics(forwarded_datagrams_total=0, forward_errors_total=5))
        self.assertEqual(only_errors["forwarding"]["state"], "errors")

    def test_invalid_source_shapes_are_marked_unavailable(self):
        cases = [
            hivezilla_status(schema_version=2),
            hivezilla_status(state="connected"),
            hivezilla_status(accepted_total=True),
            hivezilla_status(updated_unix_secs=2_006),
            hivezilla_status(spool_bytes=30_000_000_000),
            hivezilla_status(filesystem_free_bytes=70_000_000_000),
            hivezilla_status(last_durable_unix_secs=None),
        ]
        for value in cases:
            with self.subTest(value=value):
                public = self.build(hivezilla=value)
                self.assertEqual(public["hivezilla"]["availability"], "unavailable")

        invalid_receiver = self.build(receiver=receiver_metrics(recent_gossip_peers=2_001))
        self.assertEqual(invalid_receiver["gossip"]["state"], "unavailable")
        invalid_forwarding = self.build(receiver=receiver_metrics(forward_targets=0))
        self.assertEqual(invalid_forwarding["forwarding"]["state"], "unavailable")

    def test_unsafe_or_duplicate_json_is_rejected(self):
        duplicate = b'{"schema_version":1,"schema_version":1}'
        with self.assertRaises(ValueError):
            status_server._decode_json(duplicate)
        with self.assertRaises(ValueError):
            status_server._decode_json(b"")
        with self.assertRaises(ValueError):
            status_server._decode_json(b"x" * (status_server.MAX_SOURCE_JSON_BYTES + 1))

    def test_status_file_reader_does_not_follow_symlinks(self):
        target = self.root / "target.json"
        target.write_text("{}", encoding="utf-8")
        self.hivezilla_file.symlink_to(target)
        with self.assertRaises(OSError):
            status_server.read_bounded_regular(self.hivezilla_file)

    def test_receiver_endpoint_is_loopback_and_exact(self):
        endpoint = status_server.parse_receiver_endpoint("http://127.0.0.1:19090/metrics")
        self.assertEqual(endpoint, status_server.ReceiverEndpoint("127.0.0.1", 19090))
        endpoint_v6 = status_server.parse_receiver_endpoint("http://[::1]:19090/metrics")
        self.assertEqual(endpoint_v6.host, "::1")
        for value in (
            "https://127.0.0.1:19090/metrics",
            "http://10.0.0.1:19090/metrics",
            "http://localhost:19090/metrics",
            "http://user:pass@127.0.0.1:19090/metrics",
            "http://127.0.0.1:19090/other",
            "http://127.0.0.1:19090/metrics?token=secret",
            "http://127.0.0.1:99999/metrics",
        ):
            with self.subTest(value=value), self.assertRaises(Exception):
                status_server.parse_receiver_endpoint(value)

    def test_metrics_fetch_requires_bounded_json_success(self):
        body = json.dumps(receiver_metrics()).encode("utf-8")
        with metrics_service(body) as port:
            config = status_server.StatusConfig(
                self.hivezilla_file,
                status_server.ReceiverEndpoint("127.0.0.1", port),
            )
            self.assertEqual(status_server.fetch_receiver_metrics(config)["packets_total"], 1_000)

        with metrics_service(body, status=503) as port:
            config = status_server.StatusConfig(self.hivezilla_file, status_server.ReceiverEndpoint("127.0.0.1", port))
            with self.assertRaises(ValueError):
                status_server.fetch_receiver_metrics(config)

        with metrics_service(body, content_type="text/plain") as port:
            config = status_server.StatusConfig(self.hivezilla_file, status_server.ReceiverEndpoint("127.0.0.1", port))
            with self.assertRaises(ValueError):
                status_server.fetch_receiver_metrics(config)

        with metrics_service(body, declared_length=status_server.MAX_SOURCE_JSON_BYTES + 1) as port:
            config = status_server.StatusConfig(self.hivezilla_file, status_server.ReceiverEndpoint("127.0.0.1", port))
            with self.assertRaises(ValueError):
                status_server.fetch_receiver_metrics(config)

    def test_atomic_output_matches_the_canonical_http_payload(self):
        self.write_hivezilla()
        output = self.root / "public.json"
        body = json.dumps(receiver_metrics()).encode("utf-8")
        with metrics_service(body) as port:
            config = status_server.StatusConfig(
                self.hivezilla_file,
                status_server.ReceiverEndpoint("127.0.0.1", port),
                output_file=output,
            )
            cache = status_server.StatusCache(config)
            with mock.patch.object(status_server.time, "time", return_value=2_000):
                cache.refresh()

        self.assertEqual(output.read_bytes(), cache.status())
        self.assertEqual(stat.S_IMODE(output.stat().st_mode), 0o644)
        public = json.loads(output.read_bytes())
        self.assertEqual(public["hivezilla"]["accepted_total"], 500)
        self.assertNotIn("identity", output.read_text(encoding="ascii"))

    def test_atomic_output_rejects_symlink_without_touching_target(self):
        target = self.root / "private-target"
        target.write_text("do-not-change", encoding="utf-8")
        output = self.root / "public.json"
        output.symlink_to(target)
        with self.assertRaises(ValueError):
            status_server.write_atomic_public(output, b"{}")
        self.assertEqual(target.read_text(encoding="utf-8"), "do-not-change")

    def test_input_and_output_must_not_alias(self):
        with self.assertRaises(ValueError):
            status_server.validate_distinct_files(self.hivezilla_file, self.hivezilla_file)
        self.write_hivezilla()
        alias = self.root / "alias.json"
        os.link(self.hivezilla_file, alias)
        with self.assertRaises(ValueError):
            status_server.validate_distinct_files(self.hivezilla_file, alias)

    def test_cache_health_stays_ok_when_sources_are_unavailable(self):
        cache = status_server.StatusCache(self.config)
        with mock.patch.object(status_server.time, "time", return_value=2_000):
            cache.refresh()
        healthy, body = cache.health()
        self.assertTrue(healthy)
        health = json.loads(body)
        self.assertEqual(health["receiver"], "unavailable")
        self.assertEqual(health["hivezilla"], "unavailable")

    def ready_cache(self):
        self.write_hivezilla()
        body = json.dumps(receiver_metrics()).encode("utf-8")
        metrics = metrics_service(body)
        port = metrics.__enter__()
        self.addCleanup(lambda: metrics.__exit__(None, None, None))
        config = status_server.StatusConfig(
            self.hivezilla_file,
            status_server.ReceiverEndpoint("127.0.0.1", port),
            cors_origin="https://watch.example",
        )
        cache = status_server.StatusCache(config)
        with mock.patch.object(status_server.time, "time", return_value=2_000):
            cache.refresh()
        return cache

    def request(self, port, method, path, headers=None):
        connection = HTTPConnection("127.0.0.1", port, timeout=2)
        try:
            connection.request(method, path, headers=headers or {})
            response = connection.getresponse()
            body = response.read()
            return response.status, dict(response.getheaders()), body
        finally:
            connection.close()

    def test_http_surface_is_bounded_read_only_and_no_store(self):
        cache = self.ready_cache()
        with public_service(cache, "https://watch.example") as port:
            code, headers, body = self.request(port, "GET", status_server.STATUS_PATH)
            self.assertEqual(code, 200)
            self.assertEqual(headers["Cache-Control"], "no-store")
            self.assertEqual(headers["Content-Type"], "application/json; charset=utf-8")
            self.assertEqual(json.loads(body)["schema_version"], 1)

            code, headers, body = self.request(port, "HEAD", status_server.STATUS_PATH)
            self.assertEqual(code, 200)
            self.assertEqual(body, b"")
            self.assertEqual(int(headers["Content-Length"]), len(cache.status()))

            code, _, body = self.request(port, "GET", status_server.HEALTH_PATH)
            self.assertEqual(code, 200)
            self.assertTrue(json.loads(body)["ok"])

            for method in ("POST", "PUT", "PATCH", "DELETE"):
                code, headers, _ = self.request(port, method, status_server.STATUS_PATH)
                self.assertEqual(code, 405)
                self.assertEqual(headers["Allow"], "GET, HEAD, OPTIONS")

            self.assertEqual(
                self.request(port, "PROPFIND", status_server.STATUS_PATH)[0],
                405,
            )

            self.assertEqual(self.request(port, "GET", "/unknown")[0], 404)
            self.assertEqual(self.request(port, "GET", status_server.STATUS_PATH + "?secret=x")[0], 400)
            code, _, body = self.request(port, "HEAD", status_server.STATUS_PATH + "?secret=x")
            self.assertEqual(code, 400)
            self.assertEqual(body, b"")

    def test_cors_allows_only_the_exact_configured_origin(self):
        cache = self.ready_cache()
        with public_service(cache, "https://watch.example") as port:
            code, headers, _ = self.request(
                port,
                "GET",
                status_server.STATUS_PATH,
                {"Origin": "https://watch.example"},
            )
            self.assertEqual(code, 200)
            self.assertEqual(headers["Access-Control-Allow-Origin"], "https://watch.example")
            self.assertEqual(headers["Vary"], "Origin")
            self.assertNotIn("Access-Control-Allow-Credentials", headers)

            code, headers, _ = self.request(
                port,
                "GET",
                status_server.STATUS_PATH,
                {"Origin": "https://evil.example"},
            )
            self.assertEqual(code, 403)

            connection = HTTPConnection("127.0.0.1", port, timeout=2)
            try:
                connection.putrequest("GET", status_server.STATUS_PATH)
                connection.putheader("Origin", "https://watch.example")
                connection.putheader("Origin", "https://watch.example")
                connection.endheaders()
                response = connection.getresponse()
                response.read()
                duplicate_headers = dict(response.getheaders())
                self.assertEqual(response.status, 403)
                self.assertNotIn("Access-Control-Allow-Origin", duplicate_headers)
            finally:
                connection.close()
            self.assertNotIn("Access-Control-Allow-Origin", headers)

            code, headers, _ = self.request(port, "GET", status_server.STATUS_PATH)
            self.assertEqual(code, 200)
            self.assertNotIn("Access-Control-Allow-Origin", headers)

            code, headers, body = self.request(
                port,
                "OPTIONS",
                status_server.STATUS_PATH,
                {
                    "Origin": "https://watch.example",
                    "Access-Control-Request-Method": "GET",
                    "Access-Control-Request-Headers": "Accept",
                },
            )
            self.assertEqual(code, 204)
            self.assertEqual(body, b"")
            self.assertEqual(headers["Access-Control-Allow-Methods"], "GET, HEAD, OPTIONS")

            code, _, _ = self.request(
                port,
                "OPTIONS",
                status_server.STATUS_PATH,
                {
                    "Origin": "https://watch.example",
                    "Access-Control-Request-Method": "GET",
                    "Access-Control-Request-Headers": "Authorization",
                },
            )
            self.assertEqual(code, 403)

    def test_cors_and_listener_parsers_reject_unsafe_values(self):
        self.assertEqual(status_server.parse_listener("0.0.0.0:8790"), ("0.0.0.0", 8790))
        self.assertEqual(status_server.parse_listener("localhost:8790"), ("127.0.0.1", 8790))
        self.assertEqual(
            status_server.parse_cors_origin("https://watch.example:8443"),
            "https://watch.example:8443",
        )
        for value in (
            "*",
            "https://watch.example/",
            "https://user:pass@watch.example",
            "https://watch.example?token=x",
            "javascript:alert(1)",
            "https://watch.example\r\nInjected: yes",
        ):
            with self.subTest(value=value), self.assertRaises(Exception):
                status_server.parse_cors_origin(value)
        with self.assertRaises(Exception):
            status_server.parse_listener("127.0.0.1:99999")
        with self.assertRaises(Exception):
            status_server.parse_listener("8.8.8.8:8790")

    def test_environment_defaults_are_parsed_without_exposing_values(self):
        environment = {
            "SHRED_STATUS_HIVEZILLA_FILE": str(self.hivezilla_file),
            "SHRED_STATUS_LISTEN": "0.0.0.0:8790",
            "SHRED_STATUS_RECEIVER_METRICS_URL": "http://127.0.0.1:19090/metrics",
            "SHRED_STATUS_OUTPUT_FILE": str(self.root / "public.json"),
            "SHRED_STATUS_CORS_ORIGIN": "https://watch.example",
            "SHRED_STATUS_INTERVAL_SECS": "7",
        }
        with mock.patch.dict(os.environ, environment, clear=False):
            args = status_server.argument_parser().parse_args([])
        self.assertEqual(args.listen, ("0.0.0.0", 8790))
        self.assertEqual(args.hivezilla_status_file, self.hivezilla_file)
        self.assertEqual(args.output_file, self.root / "public.json")
        self.assertEqual(args.cors_origin, "https://watch.example")
        self.assertEqual(args.interval_secs, 7)


if __name__ == "__main__":
    unittest.main()
