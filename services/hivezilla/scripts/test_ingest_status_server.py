#!/usr/bin/env python3

from http.client import HTTPConnection
import importlib.util
import json
import os
from pathlib import Path
import socket
import sys
import tempfile
import threading
import unittest
from unittest import mock


SCRIPT = Path(__file__).with_name("ingest_status_server.py")
SPEC = importlib.util.spec_from_file_location("ingest_status_server", SCRIPT)
status_server = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = status_server
SPEC.loader.exec_module(status_server)


class Fixture:
    def __init__(self, root: Path, now: int = 2_000):
        self.root = root
        self.now = now
        self.cache = root / "cache"
        self.active = self.cache / "active"
        self.ack = root / "pull-ack-status.json"
        self.gaps = root / "known-gaps.json"
        self.active.mkdir(parents=True)
        (self.cache / "sealed").mkdir()
        (self.cache / "receipts").mkdir()
        (self.cache / ".rotation.lock").write_bytes(b"")
        (self.active / "identity.json").write_text(json.dumps({
            "schema_version": 1,
            "endpoint": "https://must-not-leak.invalid",
            "cluster_id": "solana-mainnet",
            "origin_node_id": "hetzner-recorder",
            "source_id": "triton-blocks",
            "journal_id": [1] * 16,
            "replication_journal_id": [2] * 16,
            "replication_sequence_base": 100,
        }))
        self.journal = self.active / "raw-blocks.jsonl"
        self.journal.write_text(
            json.dumps({
                "schema_version": 1,
                "frame_id": 4,
                "slot": 433_000_004,
                "blockhash": "must-not-leak",
            }) + "\n" +
            json.dumps({
                "schema_version": 1,
                "frame_id": 5,
                "slot": 433_000_005,
                "blockhash": "must-not-leak",
            }) + "\n"
        )
        (self.active / "wal").mkdir()
        (self.active / "wal" / "segment-0").write_bytes(b"durable-payload")
        self.ack.write_text(json.dumps({
            "schema_version": 1,
            "cluster_id": "solana-mainnet",
            "origin_node_id": "hetzner-recorder",
            "source_id": "triton-blocks",
            "journal_id": "02" * 16,
            "through_sequence": 105,
            "updated_unix_secs": now,
        }))
        self.gaps.write_text(json.dumps([{
            "from_slot": 432_900_000,
            "to_slot": 432_900_010,
            "produced_blocks": 10,
            "coverage": "rpc_recoverable",
            "private_note": "must-not-leak",
        }]))
        receipt = self.cache / "receipts" / "slot-1.json"
        receipt.write_text(json.dumps({
            "total_bytes": 1234,
            "remote_key": "must-not-leak",
            "sha256": "must-not-leak",
        }))
        alerts = self.cache / "monitoring" / "telegram-alerts"
        alerts.mkdir(parents=True)
        (alerts / "disk_space.active").write_text(
            "Blockzilla backup - WARNING\nprivate alert body\n"
        )
        (alerts / "disk_space.delivered").write_text("1900 WARNING\n")
        (alerts / "disk_space.level").write_text("CRITICAL\n")
        for path in (self.journal, self.ack, receipt, alerts / "disk_space.active"):
            os.utime(path, (now, now))

    def config(self, **overrides):
        values = {
            "cache_root": self.cache,
            "ack_status_file": self.ack,
            "known_gaps_file": self.gaps,
            # The production defaults mirror the recorder's 20/30 GiB gates.
            # Unit fixtures use byte-sized gates so the host test disk does not
            # determine nominal status.
            "disk_critical_free_bytes": 1,
            "disk_warning_free_bytes": 2,
        }
        values.update(overrides)
        return status_server.StatusConfig(
            **values,
        )


class IngestStatusTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.fixture = Fixture(Path(self.temporary.name))

    def tearDown(self):
        self.temporary.cleanup()

    def test_mainnet_seed_preserves_the_audited_historical_gap(self):
        seed = json.loads(
            (SCRIPT.parents[1] / "config" / "known-ingest-gaps.mainnet.json").read_text()
        )
        self.assertEqual(seed, [{
            "from_slot": 433_728_271,
            "to_slot": 433_731_796,
            "produced_blocks": 3_526,
            "coverage": "rpc_recoverable",
        }])

    def test_builds_bounded_secret_free_status(self):
        status = status_server.build_status(self.fixture.config(), now=self.fixture.now)
        self.assertEqual(status["schema_version"], 1)
        self.assertEqual(status["overall_state"], "degraded")
        self.assertEqual(status["recorder"]["durable_slot"], 433_000_005)
        self.assertEqual(status["replication"]["ack_through_sequence"], 105)
        self.assertEqual(status["replication"]["lag_records"], 0)
        self.assertEqual(status["recorder"]["unacknowledged_bytes"], 0)
        self.assertEqual(status["object_store"]["state"], "unavailable")
        self.assertIsNone(status["object_store"]["committed_bytes"])
        self.assertIsNone(status["object_store"]["updated_unix_secs"])
        self.assertEqual(status["indexer"]["state"], "unavailable")
        self.assertEqual(status["fallback"]["state"], "unavailable")
        self.assertEqual(status["gaps"], [{
            "from_slot": 432_900_000,
            "to_slot": 432_900_010,
            "produced_blocks": 10,
            "coverage": "rpc_recoverable",
        }])
        self.assertFalse(status["gaps_truncated"])
        self.assertEqual(status["incidents"][0]["id"], "disk_space")
        self.assertEqual(status["incidents"][0]["severity"], "warning")
        self.assertEqual(status["incidents"][0]["started_unix_secs"], 1900)
        serialized = json.dumps(status)
        for private in ("endpoint", "journal_id", "blockhash", "remote_key", "sha256", "private alert"):
            self.assertNotIn(private, serialized)

    def test_stale_capture_and_ack_fail_visible(self):
        os.utime(self.fixture.journal, (1_000, 1_000))
        ack = json.loads(self.fixture.ack.read_text())
        ack["updated_unix_secs"] = 1_000
        self.fixture.ack.write_text(json.dumps(ack))
        status = status_server.build_status(self.fixture.config(), now=2_000)
        self.assertEqual(status["overall_state"], "failed")
        self.assertEqual(status["upstream"]["state"], "stalled")
        self.assertEqual(status["recorder"]["state"], "stalled")
        self.assertEqual(status["replication"]["state"], "stalled")

    def test_future_capture_or_ack_timestamp_fails_closed(self):
        os.utime(self.fixture.journal, (2_006, 2_006))
        with self.assertRaisesRegex(ValueError, "journal timestamp is in the future"):
            status_server.build_status(self.fixture.config(), now=2_000)

        os.utime(self.fixture.journal, (2_000, 2_000))
        ack = json.loads(self.fixture.ack.read_text())
        ack["updated_unix_secs"] = 2_006
        self.fixture.ack.write_text(json.dumps(ack))
        with self.assertRaisesRegex(ValueError, "ACK status timestamp is in the future"):
            status_server.build_status(self.fixture.config(), now=2_000)

    def test_only_an_incomplete_non_newline_tail_fragment_is_ignored(self):
        tracker = status_server.CaptureProgressTracker()
        first = status_server.build_status(
            self.fixture.config(), now=self.fixture.now, progress_tracker=tracker
        )
        self.assertEqual(first["recorder"]["updated_unix_secs"], self.fixture.now)

        with self.fixture.journal.open("a") as journal:
            journal.write('{"schema_version":1,"frame_id":6')
        os.utime(self.fixture.journal, (2_061, 2_061))

        status = status_server.build_status(
            self.fixture.config(), now=2_061, progress_tracker=tracker
        )
        self.assertEqual(status["recorder"]["durable_slot"], 433_000_005)
        self.assertEqual(status["replication"]["lag_records"], 0)
        self.assertEqual(status["recorder"]["updated_unix_secs"], self.fixture.now)
        self.assertEqual(status["recorder"]["state"], "stalled")

    def test_malformed_newline_committed_final_row_fails_closed(self):
        with self.fixture.journal.open("a") as journal:
            journal.write("not-json\n")
        os.utime(self.fixture.journal, (self.fixture.now, self.fixture.now))

        with self.assertRaisesRegex(ValueError, "final committed row is invalid JSON"):
            status_server.build_status(self.fixture.config(), now=self.fixture.now)

    def test_final_row_schema_and_counters_are_required(self):
        cases = [
            {"schema_version": 2, "frame_id": 6, "slot": 433_000_006},
            {"schema_version": 1, "frame_id": True, "slot": 433_000_006},
            {"schema_version": 1, "frame_id": 6, "slot": -1},
        ]
        for row in cases:
            with self.subTest(row=row):
                self.fixture.journal.write_text(json.dumps(row) + "\n")
                os.utime(self.fixture.journal, (self.fixture.now, self.fixture.now))
                with self.assertRaises(ValueError):
                    status_server.build_status(self.fixture.config(), now=self.fixture.now)

    def test_unacknowledged_record_lag_is_explicit(self):
        ack = json.loads(self.fixture.ack.read_text())
        ack["through_sequence"] = 103
        self.fixture.ack.write_text(json.dumps(ack))
        status = status_server.build_status(self.fixture.config(), now=self.fixture.now)
        self.assertEqual(status["overall_state"], "degraded")
        self.assertEqual(status["replication"]["state"], "syncing")
        self.assertEqual(status["replication"]["lag_records"], 2)
        self.assertIsNone(status["replication"]["ack_slot"])
        self.assertGreater(status["recorder"]["unacknowledged_bytes"], 0)

    def test_ack_must_match_the_active_logical_replication_stream(self):
        ack = json.loads(self.fixture.ack.read_text())
        ack["journal_id"] = "01" * 16
        self.fixture.ack.write_text(json.dumps(ack))
        with self.assertRaisesRegex(ValueError, "different replication stream"):
            status_server.build_status(self.fixture.config(), now=self.fixture.now)

    def test_capture_is_reread_once_when_ack_wins_a_rotation_race(self):
        ack = json.loads(self.fixture.ack.read_text())
        ack["through_sequence"] = 106
        self.fixture.ack.write_text(json.dumps(ack))
        original_read_capture = status_server._read_capture
        calls = 0

        def rotating_read(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 2:
                with self.fixture.journal.open("a") as journal:
                    journal.write(json.dumps({
                        "schema_version": 1,
                        "frame_id": 6,
                        "slot": 433_000_006,
                    }) + "\n")
                os.utime(self.fixture.journal, (self.fixture.now, self.fixture.now))
            return original_read_capture(*args, **kwargs)

        with mock.patch.object(status_server, "_read_capture", side_effect=rotating_read):
            status = status_server.build_status(self.fixture.config(), now=self.fixture.now)
        self.assertEqual(calls, 2)
        self.assertEqual(status["recorder"]["durable_slot"], 433_000_006)
        self.assertEqual(status["replication"]["lag_records"], 0)

    def test_actual_replay_gap_schemas_become_unproven_intervals(self):
        replay = self.fixture.active / "replay-gaps"
        replay.mkdir()
        (replay / "schema-1.json").write_text(json.dumps({
            "schema_version": 1,
            "anchor_slot": 100,
            "requested_slot": 100,
            "available_slot": 104,
            "cluster_id": "must-not-leak",
        }))
        (replay / "schema-2.json").write_text(json.dumps({
            "schema_version": 2,
            "anchor_slot": 200,
            "requested_slot": 201,
            "provider_available_slot": 204,
            "selected_resume_slot": 206,
            "source_id": "must-not-leak",
        }))
        status = status_server.build_status(self.fixture.config(), now=self.fixture.now)
        self.assertIn({
            "from_slot": 101,
            "to_slot": 103,
            "produced_blocks": None,
            "coverage": "unproven",
        }, status["gaps"])
        self.assertIn({
            "from_slot": 201,
            "to_slot": 205,
            "produced_blocks": None,
            "coverage": "unproven",
        }, status["gaps"])
        self.assertNotIn("must-not-leak", json.dumps(status))
        self.assertEqual(status["overall_state"], "failed")

    def test_persistent_replay_registry_survives_generation_retirement(self):
        self.fixture.gaps.write_text("[]")
        registry = self.fixture.cache / "monitoring" / "replay-gaps"
        registry.mkdir()
        (registry / "retired-generation.json").write_text(json.dumps({
            "schema_version": 2,
            "anchor_slot": 300,
            "requested_slot": 300,
            "provider_available_slot": 304,
            "selected_resume_slot": 305,
        }))
        for path in sorted(self.fixture.active.rglob("*"), reverse=True):
            if path.is_dir():
                path.rmdir()
            else:
                path.unlink()
        self.fixture.active.rmdir()
        (self.fixture.cache / "sealed").rmdir()

        gaps, truncated, has_unproven, has_partial = status_server._read_gaps(
            self.fixture.config()
        )

        self.assertIn({
            "from_slot": 301,
            "to_slot": 304,
            "produced_blocks": None,
            "coverage": "unproven",
        }, gaps)
        self.assertFalse(truncated)
        self.assertTrue(has_unproven)
        self.assertFalse(has_partial)

    def test_malformed_replay_gap_records_fail_closed(self):
        replay = self.fixture.active / "replay-gaps"
        replay.mkdir()
        invalid_records = [
            {},
            {
                "schema_version": True,
                "anchor_slot": 100,
                "requested_slot": 100,
                "available_slot": 104,
            },
            {
                "schema_version": 1,
                "anchor_slot": 100,
                "requested_slot": 99,
                "available_slot": 104,
            },
            {
                "schema_version": 1,
                "anchor_slot": 100,
                "requested_slot": 100,
                "available_slot": 100,
            },
            {
                "schema_version": 2,
                "anchor_slot": 100,
                "requested_slot": 100,
                "provider_available_slot": 100,
                "selected_resume_slot": 104,
            },
            {
                "schema_version": 2,
                "anchor_slot": 100,
                "requested_slot": 100,
                "provider_available_slot": 104,
                "selected_resume_slot": 103,
            },
            {
                "schema_version": 2,
                "anchor_slot": 100,
                "requested_slot": 100,
                "provider_available_slot": 104,
                "selected_resume_slot": 104 + status_server.MAX_REPLAY_RESUME_HEADROOM_SLOTS + 1,
            },
        ]
        evidence = replay / "invalid.json"
        for value in invalid_records:
            with self.subTest(value=value):
                evidence.write_text(json.dumps(value))
                with self.assertRaisesRegex(ValueError, "replay gap"):
                    status_server._read_gaps(self.fixture.config())

    def test_gap_coverage_changes_overall_health(self):
        alerts = self.fixture.cache / "monitoring" / "telegram-alerts"
        for active in alerts.glob("*.active"):
            active.unlink()
        expectations = {
            "raw": "healthy",
            "normalized": "degraded",
            "rpc_recoverable": "degraded",
            "unproven": "failed",
        }
        for coverage, expected in expectations.items():
            with self.subTest(coverage=coverage):
                self.fixture.gaps.write_text(json.dumps([{
                    "from_slot": 10,
                    "to_slot": 11,
                    "produced_blocks": 1,
                    "coverage": coverage,
                }]))
                status = status_server.build_status(
                    self.fixture.config(), now=self.fixture.now
                )
                self.assertEqual(status["overall_state"], expected)

    def test_disk_free_space_changes_overall_health_without_alert_files(self):
        alerts = self.fixture.cache / "monitoring" / "telegram-alerts"
        for active in alerts.glob("*.active"):
            active.unlink()
        self.fixture.gaps.write_text("[]")
        config = self.fixture.config(
            disk_critical_free_bytes=20,
            disk_warning_free_bytes=30,
        )
        expectations = {19: "failed", 20: "degraded", 29: "degraded", 30: "healthy"}
        for free_bytes, expected in expectations.items():
            disk = mock.Mock(f_bavail=free_bytes, f_frsize=1, f_blocks=100)
            with self.subTest(free_bytes=free_bytes), mock.patch.object(
                status_server.os, "statvfs", return_value=disk
            ):
                status = status_server.build_status(config, now=self.fixture.now)
                self.assertEqual(status["overall_state"], expected)

    def test_gap_feed_deduplicates_and_caps_without_taking_status_offline(self):
        gaps = [{
            "from_slot": 10,
            "to_slot": 11,
            "produced_blocks": None,
            "coverage": "unproven",
        }]
        gaps.extend({
            "from_slot": 100 + index * 2,
            "to_slot": 101 + index * 2,
            "produced_blocks": 1,
            "coverage": "normalized",
        } for index in range(40))
        gaps.append({
            "from_slot": 10,
            "to_slot": 11,
            "produced_blocks": 1,
            "coverage": "raw",
        })
        self.fixture.gaps.write_text(json.dumps(gaps))
        status = status_server.build_status(self.fixture.config(), now=self.fixture.now)
        self.assertEqual(len(status["gaps"]), status_server.MAX_GAPS)
        self.assertTrue(status["gaps_truncated"])
        self.assertEqual(status["gaps"][0], {
            "from_slot": 10,
            "to_slot": 11,
            "produced_blocks": 1,
            "coverage": "raw",
        })

    def test_gap_feed_cap_cannot_hide_a_later_unproven_gap(self):
        gaps = [{
            "from_slot": index * 2,
            "to_slot": index * 2 + 1,
            "produced_blocks": 1,
            "coverage": "raw",
        } for index in range(status_server.MAX_GAPS)]
        gaps.append({
            "from_slot": 10_000,
            "to_slot": 10_001,
            "produced_blocks": None,
            "coverage": "unproven",
        })
        self.fixture.gaps.write_text(json.dumps(gaps))
        alerts = self.fixture.cache / "monitoring" / "telegram-alerts"
        for active in alerts.glob("*.active"):
            active.unlink()

        status = status_server.build_status(self.fixture.config(), now=self.fixture.now)

        self.assertEqual(len(status["gaps"]), status_server.MAX_GAPS)
        self.assertEqual(status["overall_state"], "failed")

    def test_unbounded_replay_gap_tree_fails_closed(self):
        replay = self.fixture.active / "replay-gaps"
        replay.mkdir()
        for index in range(3):
            (replay / f"{index}.json").write_text(json.dumps({
                "schema_version": 1,
                "anchor_slot": index * 10,
                "requested_slot": index * 10,
                "available_slot": index * 10 + 2,
            }))
        with mock.patch.object(status_server, "MAX_TREE_ENTRIES", 2):
            with self.assertRaisesRegex(ValueError, "replay gap tree exceeds"):
                status_server._read_gaps(self.fixture.config())

    def test_malformed_gap_coverage_is_a_caught_validation_error(self):
        self.fixture.gaps.write_text(json.dumps([{
            "from_slot": 10,
            "to_slot": 11,
            "produced_blocks": 1,
            "coverage": [],
        }]))
        with self.assertRaisesRegex(ValueError, "known gap entry has invalid fields"):
            status_server.build_status(self.fixture.config(), now=self.fixture.now)

    def test_configured_known_gap_file_must_exist(self):
        self.fixture.gaps.unlink()
        with self.assertRaisesRegex(ValueError, "known gap file is missing"):
            status_server.build_status(self.fixture.config(), now=self.fixture.now)

    def test_refresh_loop_survives_a_bounded_validation_type_error(self):
        cache = status_server.StatusCache(self.fixture.config(), interval_secs=0)
        calls = 0

        def flaky_refresh():
            nonlocal calls
            calls += 1
            if calls == 1:
                raise TypeError("malformed bounded status input")
            cache.stop()

        cache.refresh = flaky_refresh
        cache.run()
        self.assertEqual(calls, 2)

    def test_incidents_use_active_severity_and_merge_shared_public_ids(self):
        alerts = self.fixture.cache / "monitoring" / "telegram-alerts"
        (alerts / "r2_usage.active").write_text("Blockzilla backup - ERROR\nprivate\n")
        (alerts / "r2_usage.delivered").write_text("1800 ERROR\n")
        (alerts / "b2_usage.active").write_text("Blockzilla backup - CRITICAL\nprivate\n")
        (alerts / "b2_usage.delivered").write_text("1700 CRITICAL\n")
        status = status_server.build_status(self.fixture.config(), now=self.fixture.now)
        object_store = [item for item in status["incidents"] if item["id"] == "object_store"]
        self.assertEqual(object_store, [{
            "id": "object_store",
            "severity": "critical",
            "started_unix_secs": 1700,
            "resolved_unix_secs": None,
        }])
        self.assertEqual(status["overall_state"], "failed")

    def test_unknown_alert_keys_are_not_public_incident_ids(self):
        alerts = self.fixture.cache / "monitoring" / "telegram-alerts"
        (alerts / "secret_token_value.active").write_text(
            "Blockzilla backup - CRITICAL\nprivate\n"
        )
        (alerts / "secret_token_value.delivered").write_text("1700 CRITICAL\n")
        status = status_server.build_status(self.fixture.config(), now=self.fixture.now)
        self.assertNotIn("secret_token_value", json.dumps(status))

    def test_symlinked_input_fails_closed(self):
        identity = self.fixture.active / "identity.json"
        target = self.fixture.root / "private-identity.json"
        target.write_text(identity.read_text())
        identity.unlink()
        identity.symlink_to(target)
        with self.assertRaises(OSError):
            status_server.build_status(self.fixture.config(), now=self.fixture.now)

    def test_listener_rejects_wildcard_and_public_addresses(self):
        self.assertEqual(status_server.listener("127.0.0.1:8790"), ("127.0.0.1", 8790))
        self.assertEqual(status_server.listener("192.168.1.10:8790"), ("192.168.1.10", 8790))
        with self.assertRaises(status_server.argparse.ArgumentTypeError):
            status_server.listener("0.0.0.0:8790")
        with self.assertRaises(status_server.argparse.ArgumentTypeError):
            status_server.listener("203.0.113.10:8790")

    def test_http_surface_is_read_only_and_has_no_cors(self):
        body = json.dumps(status_server.build_status(self.fixture.config(), now=self.fixture.now)).encode()

        class StaticCache:
            def get(self):
                return body

            def healthy(self):
                return True

        handler = type(
            "TestIngestStatusHandler",
            (status_server.IngestStatusHandler,),
            {"cache": StaticCache()},
        )
        server = status_server.IngestStatusServer(("127.0.0.1", 0), handler)
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        try:
            connection = HTTPConnection("127.0.0.1", server.server_port, timeout=2)
            connection.request("GET", status_server.STATUS_PATH)
            response = connection.getresponse()
            self.assertEqual(response.status, 200)
            self.assertEqual(json.loads(response.read()), json.loads(body))
            self.assertEqual(response.getheader("Cache-Control"), "no-store")
            self.assertIsNone(response.getheader("Access-Control-Allow-Origin"))
            connection.close()

            connection = HTTPConnection("127.0.0.1", server.server_port, timeout=2)
            connection.request("POST", status_server.STATUS_PATH)
            response = connection.getresponse()
            self.assertEqual(response.status, 405)
            response.read()
            connection.close()
        finally:
            server.shutdown()
            server.server_close()
            worker.join(timeout=2)

    def test_health_fails_after_refresh_error_while_status_keeps_last_snapshot(self):
        cache = status_server.StatusCache(self.fixture.config(), interval_secs=10)
        cache.refresh()
        retained = cache.get()
        identity = self.fixture.active / "identity.json"
        identity.write_text("not-json")
        with self.assertRaises(json.JSONDecodeError):
            cache.refresh()

        handler = type(
            "FailedRefreshIngestStatusHandler",
            (status_server.IngestStatusHandler,),
            {"cache": cache},
        )
        server = status_server.IngestStatusServer(("127.0.0.1", 0), handler)
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        try:
            connection = HTTPConnection("127.0.0.1", server.server_port, timeout=2)
            connection.request("GET", "/healthz")
            response = connection.getresponse()
            self.assertEqual(response.status, 503)
            health_body = response.read()
            self.assertEqual(json.loads(health_body), {"ok": False})
            self.assertNotIn(b"identity", health_body)
            self.assertNotIn(str(self.fixture.root).encode(), health_body)
            connection.close()

            connection = HTTPConnection("127.0.0.1", server.server_port, timeout=2)
            connection.request("GET", status_server.STATUS_PATH)
            response = connection.getresponse()
            self.assertEqual(response.status, 200)
            self.assertEqual(response.read(), retained)
            connection.close()
        finally:
            server.shutdown()
            server.server_close()
            worker.join(timeout=2)

    def test_accepted_connections_receive_a_header_timeout(self):
        server = status_server.IngestStatusServer(
            ("127.0.0.1", 0), status_server.IngestStatusHandler
        )
        client = socket.create_connection(server.server_address, timeout=2)
        accepted = None
        try:
            accepted, _ = server.get_request()
            self.assertEqual(accepted.gettimeout(), 5.0)
        finally:
            if accepted is not None:
                accepted.close()
            client.close()
            server.server_close()


if __name__ == "__main__":
    unittest.main()
