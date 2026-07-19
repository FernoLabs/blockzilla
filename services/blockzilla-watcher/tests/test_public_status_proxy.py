import copy
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib.util
import json
from pathlib import Path
import threading
import unittest


SCRIPT = Path(__file__).parents[1] / "scripts" / "public-status-proxy.py"
SPEC = importlib.util.spec_from_file_location("public_status_proxy", SCRIPT)
proxy = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(proxy)


def valid_ingest_status():
    return {
        "schema_version": 1,
        "updated_unix_secs": 100,
        "overall_state": "healthy",
        "upstream": {
            "state": "connected",
            "updated_unix_secs": 100,
            "reconnects_1h": None,
        },
        "recorder": {
            "state": "recording",
            "durable_slot": 1000,
            "updated_unix_secs": 100,
            "active_bytes": 1024,
            "sealed_generations": 0,
            "unacknowledged_bytes": 0,
            "disk_free_bytes": 2048,
            "disk_total_bytes": 4096,
        },
        "replication": {
            "state": "caught_up",
            "ack_through_sequence": 1000,
            "ack_slot": 1000,
            "updated_unix_secs": 100,
            "lag_records": 0,
        },
        "indexer": {
            "state": "unavailable",
            "last_slot": None,
            "updated_unix_secs": None,
            "lag_slots": None,
        },
        "object_store": {
            "state": "unavailable",
            "provider": "r2",
            "committed_bytes": None,
            "pending_bytes": 0,
            "updated_unix_secs": None,
        },
        "fallback": {
            "state": "unavailable",
            "last_slot": None,
            "updated_unix_secs": None,
            "lag_slots": None,
        },
        "gaps": [],
        "gaps_truncated": False,
        "incidents": [],
    }


def request_through_proxy(
    status, content_type, body, method="GET",
    path="/api/v1/sidecars/ingest-pipeline/status.json",
    ingest_response=None,
):
    def handler_for(response_status, response_content_type, response_body):
        class UpstreamHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(response_status)
                self.send_header("Content-Type", response_content_type)
                self.send_header("Content-Length", str(len(response_body)))
                self.send_header("X-Internal-Diagnostic", "must-not-leak")
                self.end_headers()
                self.wfile.write(response_body)

            do_HEAD = do_GET

            def log_message(self, message, *args):
                pass

        return UpstreamHandler

    upstream = ThreadingHTTPServer(
        ("127.0.0.1", 0), handler_for(status, content_type, body)
    )
    upstream_thread = threading.Thread(target=upstream.serve_forever, daemon=True)
    upstream_thread.start()

    if ingest_response is None:
        ingest_upstream = upstream
        ingest_upstream_thread = None
    else:
        ingest_status, ingest_content_type, ingest_body = ingest_response
        ingest_upstream = ThreadingHTTPServer(
            ("127.0.0.1", 0),
            handler_for(ingest_status, ingest_content_type, ingest_body),
        )
        ingest_upstream_thread = threading.Thread(
            target=ingest_upstream.serve_forever, daemon=True
        )
        ingest_upstream_thread.start()

    class TestProxyHandler(proxy.PublicWatcherProxyHandler):
        upstream_host = "127.0.0.1"
        upstream_port = upstream.server_address[1]
        ingest_upstream_host = "127.0.0.1"
        ingest_upstream_port = ingest_upstream.server_address[1]
        upstream_timeout_secs = 2.0

    public = proxy.PublicWatcherProxyServer(
        ("127.0.0.1", 0), TestProxyHandler, client_timeout_secs=2.0
    )
    public_thread = threading.Thread(target=public.serve_forever, daemon=True)
    public_thread.start()
    try:
        connection = HTTPConnection("127.0.0.1", public.server_address[1], timeout=2.0)
        connection.request(method, path)
        response = connection.getresponse()
        result = response.status, dict(response.getheaders()), response.read()
        connection.close()
        return result
    finally:
        public.shutdown()
        public.server_close()
        upstream.shutdown()
        upstream.server_close()
        if ingest_upstream is not upstream:
            ingest_upstream.shutdown()
            ingest_upstream.server_close()
        public_thread.join(timeout=2.0)
        upstream_thread.join(timeout=2.0)
        if ingest_upstream_thread is not None:
            ingest_upstream_thread.join(timeout=2.0)


class PublicStatusProxyTests(unittest.TestCase):
    def test_ingest_sidecar_is_an_explicit_public_get(self):
        self.assertIn(
            "/api/v1/sidecars/ingest-pipeline/status.json",
            proxy.PUBLIC_API_GETS,
        )

    def test_ingest_sidecar_drops_every_non_contract_field(self):
        private = valid_ingest_status()
        private["overall_state"] = "failed"
        private["upstream"].update({
            "endpoint": "https://private.invalid",
            "token": "must-not-leak",
        })
        private["replication"]["journal_id"] = "private-journal"
        private["object_store"]["remote_key"] = "private-object"
        private["gaps"] = [{
                "from_slot": 1,
                "to_slot": 2,
                "produced_blocks": 2,
                "coverage": "unproven",
                "blockhash": "private-hash",
        }]
        private["dokploy_token"] = "must-not-leak"
        public = json.loads(proxy.public_ingest_json_bytes(json.dumps(private).encode()))
        serialized = json.dumps(public)
        self.assertNotIn("endpoint", serialized)
        self.assertNotIn("token", serialized)
        self.assertNotIn("journal_id", serialized)
        self.assertNotIn("remote_key", serialized)
        self.assertNotIn("blockhash", serialized)
        self.assertEqual(public["gaps"][0]["coverage"], "unproven")

    def test_ingest_sidecar_rejects_nested_leaf_values(self):
        private = valid_ingest_status()
        private["upstream"]["reconnects_1h"] = {"token": "must-not-leak"}
        with self.assertRaises(ValueError):
            proxy.public_ingest_json_bytes(json.dumps(private).encode())

    def test_ingest_sidecar_rejects_unknown_incident_ids(self):
        private = valid_ingest_status()
        private["incidents"] = [{
            "id": "secret-api-token-value",
            "severity": "critical",
            "started_unix_secs": 99,
            "resolved_unix_secs": None,
        }]
        with self.assertRaises(ValueError):
            proxy.public_ingest_json_bytes(json.dumps(private).encode())

    def test_ingest_sidecar_rejects_invalid_types_enums_and_cardinality(self):
        cases = []

        wrong_type = valid_ingest_status()
        wrong_type["recorder"]["active_bytes"] = True
        cases.append(wrong_type)

        wrong_enum = valid_ingest_status()
        wrong_enum["upstream"]["state"] = "secret-value"
        cases.append(wrong_enum)

        too_many_gaps = valid_ingest_status()
        too_many_gaps["gaps"] = [{
            "from_slot": index,
            "to_slot": index,
            "produced_blocks": 1,
            "coverage": "raw",
        } for index in range(proxy.MAX_PUBLIC_GAPS + 1)]
        cases.append(too_many_gaps)

        duplicate_incident = {
            "id": "grpc_stale",
            "severity": "error",
            "started_unix_secs": 99,
            "resolved_unix_secs": None,
        }
        duplicate_incidents = valid_ingest_status()
        duplicate_incidents["incidents"] = [
            copy.deepcopy(duplicate_incident),
            copy.deepcopy(duplicate_incident),
        ]
        cases.append(duplicate_incidents)

        duplicate_gaps = valid_ingest_status()
        duplicate_gap = {
            "from_slot": 1,
            "to_slot": 2,
            "produced_blocks": 2,
            "coverage": "raw",
        }
        duplicate_gaps["gaps"] = [copy.deepcopy(duplicate_gap), copy.deepcopy(duplicate_gap)]
        cases.append(duplicate_gaps)

        contradictory_ack = valid_ingest_status()
        contradictory_ack["replication"]["lag_records"] = 2
        cases.append(contradictory_ack)

        contradictory_indexer = valid_ingest_status()
        contradictory_indexer["indexer"]["state"] = "indexing"
        cases.append(contradictory_indexer)

        for value in cases:
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    proxy.public_ingest_json_bytes(json.dumps(value).encode())

    def test_ingest_sidecar_has_a_dedicated_one_megabyte_limit(self):
        private = valid_ingest_status()
        private["ignored"] = "x" * proxy.MAX_INGEST_JSON_BYTES
        with self.assertRaises(ValueError):
            proxy.public_ingest_json_bytes(json.dumps(private).encode())

    def test_ingest_proxy_never_passes_through_non_json_body(self):
        status, headers, body = request_through_proxy(
            200, "text/plain", b"private token in diagnostic"
        )
        self.assertEqual(status, 502)
        self.assertNotIn(b"private token", body)
        self.assertNotIn("X-Internal-Diagnostic", headers)

    def test_ingest_proxy_never_passes_through_upstream_error_body(self):
        status, headers, body = request_through_proxy(
            500, "application/json", b'{"token":"private token"}'
        )
        self.assertEqual(status, 502)
        self.assertEqual(json.loads(body), {"error": "watcher upstream unavailable"})
        self.assertNotIn(b"private token", body)
        self.assertNotIn("X-Internal-Diagnostic", headers)

    def test_ingest_proxy_returns_only_validated_json(self):
        private = valid_ingest_status()
        private["secret"] = "must-not-leak"
        status, headers, body = request_through_proxy(
            200, "application/json; charset=utf-8", json.dumps(private).encode()
        )
        self.assertEqual(status, 200)
        self.assertNotIn(b"secret", body)
        self.assertEqual(headers["Content-Type"], "application/json; charset=utf-8")
        self.assertEqual(headers["Cache-Control"], "no-store")
        self.assertNotIn("X-Internal-Diagnostic", headers)

    def test_ingest_proxy_uses_its_dedicated_upstream(self):
        ingest = valid_ingest_status()
        status, _, body = request_through_proxy(
            200,
            "application/json",
            b'{"watcher":"wrong upstream"}',
            ingest_response=(
                200,
                "application/json",
                json.dumps(ingest).encode(),
            ),
        )
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body), ingest)

        status, _, body = request_through_proxy(
            200,
            "application/json",
            b'{"watcher":true}',
            path="/api/v1/status",
            ingest_response=(
                200,
                "application/json",
                b'{"ingest":true}',
            ),
        )
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body), {"watcher": True})

    def test_all_public_api_errors_and_content_type_mismatches_fail_closed(self):
        status, headers, body = request_through_proxy(
            500,
            "application/json",
            b'{"token":"private token"}',
            path="/api/v1/status",
        )
        self.assertEqual(status, 502)
        self.assertEqual(json.loads(body), {"error": "watcher upstream unavailable"})
        self.assertNotIn("X-Internal-Diagnostic", headers)

        status, headers, body = request_through_proxy(
            200,
            "text/plain",
            b"private diagnostic",
            path="/api/v1/status",
        )
        self.assertEqual(status, 502)
        self.assertNotIn(b"private diagnostic", body)
        self.assertNotIn("X-Internal-Diagnostic", headers)

    def test_generic_public_json_drops_private_response_headers(self):
        status, headers, body = request_through_proxy(
            200,
            "application/json",
            b'{"ok":true}',
            path="/api/v1/status",
        )
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body), {"ok": True})
        self.assertNotIn("X-Internal-Diagnostic", headers)

    def test_snapshot_paths_are_reduced_to_basenames(self):
        private = {
            "epochs": [{
                "input_path": "/volume1/archive/cars/epoch-341.car.zst",
                "output_path": "/volume1/archive/v2/epoch-341",
            }],
            "live": [{
                "capture_dir": "/volume1/private/live/capture-1",
                "output_path": "/volume1/archive/v2/epoch-1004",
            }],
            "errors": [
                {"message": "open /home/operator/private/state failed"},
                {"message": "read /mnt/archive/private failed"},
            ],
        }
        public = json.loads(proxy.public_json_bytes(json.dumps(private).encode()))
        self.assertEqual(public["epochs"][0]["input_path"], "epoch-341.car.zst")
        self.assertEqual(public["epochs"][0]["output_path"], "epoch-341")
        self.assertEqual(public["live"][0]["capture_dir"], "capture-1")
        self.assertEqual(public["live"][0]["output_path"], "epoch-1004")
        serialized = json.dumps(public)
        self.assertNotIn("/volume1", serialized)
        self.assertNotIn("/home/operator", serialized)
        self.assertNotIn("/mnt/archive", serialized)
        self.assertIn("<redacted-path>", serialized)

    def test_snapshot_patch_sse_is_sanitized(self):
        line = b'data: {"data":{"live":[{"capture_dir":"/tmp/private/live"}]}}\n'
        public = proxy.public_sse_line(line)
        self.assertTrue(public.startswith(b"data:"))
        self.assertNotIn(b"/tmp/private", public)
        self.assertIn(b'"capture_dir":"live"', public)

    def test_invalid_sse_data_fails_closed(self):
        self.assertIsNone(proxy.public_sse_line(b"data: not-json\n"))

    def test_sse_non_data_fields_are_allowlisted_and_canonicalized(self):
        self.assertEqual(
            proxy.public_sse_line(b"event: snapshot_patch\r\n"),
            b"event:snapshot_patch\n",
        )
        self.assertEqual(proxy.public_sse_line(b"id: 00042\n"), b"id:42\n")
        self.assertEqual(proxy.public_sse_line(b"retry: 1500\n"), b"retry:1500\n")
        self.assertEqual(proxy.public_sse_line(b": private heartbeat\n"), b"")
        self.assertIsNone(proxy.public_sse_line(b"event: private_debug\n"))
        self.assertIsNone(proxy.public_sse_line(b"x-internal: must-not-leak\n"))
        self.assertIsNone(proxy.public_sse_line(b"id: secret-value\n"))

    def test_mixed_case_sse_content_type_still_uses_sanitized_stream(self):
        event = (
            b"event: snapshot_patch\n"
            b'data: {"message":"Bearer TOPSECRET","path":"/tmp/private"}\n\n'
        )
        status, headers, body = request_through_proxy(
            200,
            "Text/Event-Stream; Charset=UTF-8",
            event,
            path="/api/v1/events",
        )
        self.assertEqual(status, 200)
        self.assertEqual(headers["Content-Type"], "text/event-stream")
        self.assertIn(b"event:snapshot_patch", body)
        self.assertIn(b"Bearer <redacted>", body)
        self.assertNotIn(b"TOPSECRET", body)
        self.assertNotIn(b"/tmp/private", body)

    def test_unknown_sse_field_closes_without_forwarding_it(self):
        status, _, body = request_through_proxy(
            200,
            "text/event-stream",
            b"x-internal: TOPSECRET\ndata: {\"ok\":true}\n\n",
            path="/api/v1/events",
        )
        self.assertEqual(status, 200)
        self.assertEqual(body, b"")

    def test_urls_and_non_path_fields_are_unchanged(self):
        value = {
            "status_url": "/api/v1/status",
            "message": "see https://example.invalid/path",
            "capture_id": "epoch-1004-capture",
        }
        self.assertEqual(proxy.sanitize_public_value(value), value)

    def test_generic_json_omits_secret_keys_and_redacts_secret_strings(self):
        value = {
            "ok": True,
            "token": "TOPSECRET",
            "nested": {
                "dokploy_token": "TOPSECRET",
                "apiKey": "TOPSECRET",
                "receiverTokenValue": "TOPSECRET",
                "endpoint": "https://internal.invalid",
            },
            "message": "request used Bearer TOPSECRET",
            "basic_message": "request used Basic dXNlcjpUT1BTRUNSRVQ=",
            "credentialed_url": (
                "https://" + "operator:p@ss" + "@example.invalid/status"
            ),
            "query_url": (
                "https://example.invalid/status?token=TOPSECRET&mode=public"
            ),
            "diagnostic": "api_key=TOPSECRET",
        }
        public = json.loads(proxy.public_json_bytes(json.dumps(value).encode()))
        serialized = json.dumps(public)
        self.assertEqual(public["ok"], True)
        self.assertNotIn("token", public)
        self.assertNotIn("dokploy_token", public["nested"])
        self.assertNotIn("apiKey", public["nested"])
        self.assertNotIn("receiverTokenValue", public["nested"])
        self.assertNotIn("endpoint", public["nested"])
        self.assertNotIn("TOPSECRET", serialized)
        self.assertIn("Bearer <redacted>", public["message"])
        self.assertIn("Basic <redacted>", public["basic_message"])
        self.assertIn("https://<redacted>@example.invalid", public["credentialed_url"])
        self.assertIn("token=<redacted>", public["query_url"])
        self.assertEqual(public["diagnostic"], "api_key=<redacted>")

    def test_bind_addresses_must_be_loopback(self):
        self.assertEqual(
            proxy.network_address("127.0.0.1:8787", "--listen", True),
            ("127.0.0.1", 8787),
        )
        self.assertEqual(
            proxy.network_address("192.168.1.10:8787", "--listen", True),
            ("192.168.1.10", 8787),
        )
        with self.assertRaises(proxy.argparse.ArgumentTypeError):
            proxy.network_address("0.0.0.0:8787", "--listen", True)
        with self.assertRaises(proxy.argparse.ArgumentTypeError):
            proxy.network_address("192.168.1.10:8786", "--upstream", False)
        with self.assertRaises(proxy.argparse.ArgumentTypeError):
            proxy.network_address(
                "192.168.1.10:8788", "--ingest-upstream", False
            )
        with self.assertRaises(proxy.argparse.ArgumentTypeError):
            proxy.network_address("203.0.113.10:8787", "--listen", True)


if __name__ == "__main__":
    unittest.main()
