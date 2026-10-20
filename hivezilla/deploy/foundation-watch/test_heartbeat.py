import hashlib
from contextlib import closing
from http.client import HTTPMessage
import importlib.util
import io
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import urllib.error
import urllib.request
from urllib.response import addinfourl

spec = importlib.util.spec_from_file_location("heartbeat", Path(__file__).with_name("heartbeat.py"))
heartbeat = importlib.util.module_from_spec(spec)
spec.loader.exec_module(heartbeat)

SESSION = "000000000000000018ddd9a6bb6fc50e-282459"
OTHER = "000000000000000018dd84835b6e7677-282459"
NEWER = "000000000000000018ddd9a6bb6fc50f-282460"
NOW = 1791844000000
TOKEN = "private-test-token-" + "x" * 40


class FakeHTTPS(urllib.request.HTTPSHandler):
    """Use urllib's real redirect handling without an external network call."""
    def __init__(self, code, body, headers=None):
        super().__init__()
        self.code, self.body, self.extra_headers = code, body, headers or {}
        self.requests = []

    def https_open(self, request):
        self.requests.append(request)
        headers = HTTPMessage()
        for key, value in self.extra_headers.items():
            headers[key] = value
        reply = addinfourl(io.BytesIO(self.body), headers, request.full_url, self.code)
        reply.msg = "Response"
        return reply


class HeartbeatTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name) / "root"
        self.proc = Path(self.temporary.name) / "proc"
        self.proc.mkdir()
        (self.proc / "locks").write_text("")
        boot = self.proc / "sys/kernel/random/boot_id"
        boot.parent.mkdir(parents=True)
        boot.write_text("0b041e69-1558-4c8f-ab03-423b7c67b451\n")
        (self.proc / "meminfo").write_text("MemTotal: 1024 kB\nMemAvailable: 512 kB\n")
        self.capture = self.root / "captures/direct-agave-live" / SESSION
        self.capture.mkdir(parents=True)
        (self.capture / "LIVE.lock").touch()
        self.output = self.root / "compact/direct-agave-processed-live" / SESSION
        self.publisher = self.root / "reports/native-processed-r2-bank-v2" / SESSION
        self.json(self.root / "reports/bank-v2-recovery-20261012/RECOVERY.json", {"source": {"session_id": SESSION}})
        self.json(self.capture / "SESSION.json", {"session_id": SESSION})
        with closing(sqlite3.connect(self.capture / "events.sqlite")) as db:
            db.execute("CREATE TABLE status(id INTEGER,state TEXT,durable_sequence INTEGER,latest_rooted_slot INTEGER)")
            db.execute("INSERT INTO status VALUES(1,'CAPTURING',1900,456208200)")
            db.commit()
        os.utime(self.capture / "events.sqlite", (NOW / 1000, NOW / 1000))
        self.json(self.output / "FEED.json", {"session_id": SESSION})
        self.json(self.output / "STATUS.json", {"state": "RUNNING", "last_compact_slot": 456208198,
            "event_sequence": 900, "source_sequence": 1899, "updated_unix_ms": NOW - 1000})
        self.json(self.publisher / "CHECKPOINT.json", {"session": SESSION, "source": str(self.output),
            "prefix": "native-stream/processed/direct-agave-live-v1/" + SESSION, "sequence": 890})
        self.json(self.publisher / "STATUS.json", {"state": "RUNNING", "updated_unix_ms": NOW - 2000})
        self.json(self.output / "cold-storage/STATUS.json", {"state": "RUNNING", "retired_banks": 42,
            "updated_unix_ms": NOW - 3000})
        self.service_calls = []

    def tearDown(self):
        self.temporary.cleanup()

    @staticmethod
    def json(path, value):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value))

    def services(self, unit):
        self.service_calls.append(unit)
        return "active", "binary --source " + str(self.capture)

    def collect(self, **kwargs):
        return heartbeat.sample(self.root, now_ms=kwargs.pop("now_ms", NOW), proc_root=self.proc,
            service_reader=kwargs.pop("service_reader", self.services),
            rpc_reader=kwargs.pop("rpc_reader", lambda: {"healthy": True, "finalized_slot": 456208201}),
            bank_reader=kwargs.pop("bank_reader", lambda root, source, now: {
                'status': 'live', 'confirmed_slot': 456208201,
                'source_observed_at_unix_ms': now - 1000, 'sampled_at_unix_ms': now,
                'source_lag_ms': 1000}),
            disk_reader=kwargs.pop("disk_reader", lambda path: SimpleNamespace(free=900 * 1024**3)), **kwargs)

    def payload(self):
        return self.collect()

    def opener(self, code=202, value=None, headers=None):
        if value is None:
            value = {"accepted": True, "schema": heartbeat.ACCEPTED_SCHEMA, "sampled_unix_ms": NOW}
        body = value if isinstance(value, bytes) else json.dumps(value).encode()
        transport = FakeHTTPS(code, body, headers)
        return urllib.request.build_opener(urllib.request.ProxyHandler({}), transport, heartbeat.NoRedirect()), transport

    def storage_status(self, phase, waiting, committed, **changes):
        value = {"state": "RUNNING", "retired_banks": 42, "updated_unix_ms": NOW,
                 "storage_progress": phase, "progress_stall_seconds": 900,
                 "eligible_wait_since_unix_ms": waiting,
                 "last_archive_progress_unix_ms": committed, **changes}
        self.json(self.output / "cold-storage/STATUS.json", value)

    def test_private_v2_head_uses_only_local_key_and_bounded_response(self):
        key = self.root / 'config/stream-20261004/client-api.key'
        key.parent.mkdir(parents=True); key.write_text(TOKEN + '\n')
        source = self.source_pin()
        head = dict(schema='blockzilla-confirmed-head-v1', profile='native-bank-compact-v2',
                    source=source, status='live', sampled_at_unix_ms=NOW,
                    source_observed_at_unix_ms=NOW - 1000, source_lag_ms=1000,
                    latest_confirmed={'bank': {'slot': 456208201}, 'observed_at_unix_ms': NOW - 1000})
        requests = []
        def open_local(request, timeout):
            requests.append(request)
            self.assertEqual(request.full_url, 'http://127.0.0.1:8871/v2/banks/head')
            self.assertEqual(request.get_header('Authorization'), 'Bearer ' + TOKEN)
            self.assertEqual(timeout, 2)
            return addinfourl(io.BytesIO(json.dumps(head).encode()), HTTPMessage(), request.full_url, 200)
        with patch.object(heartbeat, 'direct_opener', return_value=SimpleNamespace(open=open_local)):
            value = heartbeat.read_bank_head(self.root, source, NOW)
            self.assertEqual(value['confirmed_slot'], 456208201)
            self.assertNotIn(TOKEN, json.dumps(value))
            head['source'] = {**source, 'session_id': OTHER}
            with self.assertRaises(heartbeat.SafeError): heartbeat.read_bank_head(self.root, source, NOW)
            head['source'] = source
            head['source_lag_ms'] = 1
            with self.assertRaises(heartbeat.SafeError): heartbeat.read_bank_head(self.root, source, NOW)
        self.assertEqual(len(requests), 3)

    def test_private_v2_failures_do_not_suppress_the_machine_heartbeat(self):
        def unavailable(*args): raise RuntimeError('do not log private-test-token')
        value = self.collect(bank_reader=unavailable)
        self.assertIn('bank_v2_unavailable', value['errors'])
        self.assertEqual(value['capture']['state'], 'CAPTURING')
        self.assertNotIn('private-test-token', json.dumps(value))
        for phase, observed, expected in [('failed', NOW - 1000, 'bank_v2_not_live'),
                                           ('stale', NOW - 5001, 'bank_v2_stale'),
                                           ('live', NOW - 5001, 'bank_v2_stale')]:
            with self.subTest(phase=phase):
                value = self.collect(bank_reader=lambda *args: {'status': phase,
                    'confirmed_slot': 456208201, 'source_observed_at_unix_ms': observed})
                self.assertIn(expected, value['errors'])

    def test_private_v2_reader_rejects_oversize_data_and_key_symlinks(self):
        key = self.root / 'config/stream-20261004/client-api.key'
        key.parent.mkdir(parents=True); key.write_text(TOKEN)
        def oversized(request, timeout):
            return addinfourl(io.BytesIO(b'x' * 65537), HTTPMessage(), request.full_url, 200)
        with patch.object(heartbeat, 'direct_opener', return_value=SimpleNamespace(open=oversized)):
            with self.assertRaises(heartbeat.SafeError):
                heartbeat.read_bank_head(self.root, self.source_pin(), NOW)
        original = key.with_name('original.key'); key.rename(original); key.symlink_to(original.name)
        with self.assertRaises(heartbeat.SafeError):
            heartbeat.read_bank_head(self.root, self.source_pin(), NOW)

    def test_fresh_storage_status_cannot_hide_no_archive_progress(self):
        # Fresh helper timestamps and a RUNNING process are not a durable commit.
        for phase in ("STALLED", "TRANSFERRING", "WAITING"):
            with self.subTest(phase=phase):
                self.storage_status(phase, NOW - 900000, None)
                value = self.collect()
                self.assertIn("cold_storage_no_archive_progress", value["errors"])
                self.assertNotIn("cold_storage_stale", value["errors"])

    def test_waiting_gates_and_slow_transfer_do_not_claim_a_stall(self):
        for phase, waiting, committed in (("WAITING", None, None),
                                          ("ADVANCING", None, NOW - 1000),
                                          ("TRANSFERRING", NOW - 899999, None),
                                          ("TRANSFERRING", NOW - 1800000, NOW - 1000)):
            with self.subTest(phase=phase, waiting=waiting, committed=committed):
                self.storage_status(phase, waiting, committed)
                value = self.collect()
                self.assertEqual(value["errors"], [])
                self.assertEqual(value["cold_storage"]["storage_progress"], phase)

    def test_invalid_storage_progress_remains_a_visible_collection_fault(self):
        for changes in ({"storage_progress": "unexpected"},
                        {"progress_stall_seconds": True},
                        {"progress_stall_seconds": 0},
                        {"eligible_wait_since_unix_ms": NOW + 30001},
                        {"last_archive_progress_unix_ms": NOW + 30001}):
            with self.subTest(changes=changes):
                self.storage_status("TRANSFERRING", NOW - 1000, None, **changes)
                self.assertIn("cold_storage_unavailable", self.collect()["errors"])
        self.storage_status("STALLED", NOW - 1000, None)
        self.assertIn("cold_storage_unavailable", self.collect()["errors"])
        self.storage_status("STALLED", None, None)
        self.assertIn("cold_storage_unavailable", self.collect()["errors"])

    def test_collector_is_read_only_and_sends_expected_schema(self):
        paths = list(self.root.rglob("*"))
        before = {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in paths if p.is_file()}
        value = self.collect()
        after = {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in self.root.rglob("*") if p.is_file()}
        self.assertEqual(before, after)
        self.assertEqual(value["errors"], [])
        self.assertEqual(value["schema"], heartbeat.SCHEMA)
        self.assertEqual(value["machine_id"], "foundation")
        self.assertIs(type(value["monotonic_ms"]), int)
        self.assertEqual(value["capture"]["durable_sequence"], 1900)
        self.assertIsNone(value["capture"]["updated_unix_ms"])
        self.assertEqual(value["compact"]["event_sequence"], 900)
        self.assertEqual(value["publisher"]["sequence"], 890)
        self.assertEqual(value["capture_reserve_bytes"], 429496729600)
        self.assertEqual(value["mem_available_bytes"], 512 * 1024)
        self.assertEqual(set(self.service_calls), set(heartbeat.SERVICES))
        with self.assertRaises(sqlite3.OperationalError):
            heartbeat.read_capture(self.capture / "does-not-exist.sqlite")
        self.assertFalse((self.capture / "does-not-exist.sqlite").exists())

    def test_boot_local_monotonic_sequence_progresses_when_wall_clock_moves_backwards(self):
        with patch.object(heartbeat.time, "monotonic_ns", side_effect=[123000000000, 153000000000]):
            first = self.collect(now_ms=NOW)
            second = self.collect(now_ms=NOW - 5000)
        self.assertEqual(first["boot_id"], second["boot_id"])
        self.assertIs(type(first["monotonic_ms"]), int)
        self.assertIs(type(second["monotonic_ms"]), int)
        self.assertEqual(first["monotonic_ms"], 123000)
        self.assertEqual(second["monotonic_ms"], 153000)
        self.assertGreater(second["monotonic_ms"], first["monotonic_ms"])
        self.assertLess(second["sampled_unix_ms"], first["sampled_unix_ms"])

    def test_partial_collection_still_sends_machine_alive_without_exception_text(self):
        def broken(*args):
            raise RuntimeError("https://secret.example/?token=do-not-log-this")
        (self.output / "STATUS.json").unlink()
        value = self.collect(service_reader=broken, rpc_reader=broken, disk_reader=broken)
        self.assertEqual(value["capture"]["state"], "CAPTURING")
        self.assertIn("compact_unavailable", value["errors"])
        self.assertIn("rpc_unavailable", value["errors"])
        sent = []
        report = self.root / "reports/foundation-watch/STATUS.json"
        def sender(payload, url, token):
            sent.append(payload)
        self.assertEqual(heartbeat.run_once(value, report, "https://watch.example", TOKEN, sender=sender), 0)
        self.assertEqual(len(sent), 1)
        receipt = json.loads(report.read_text())
        self.assertEqual(receipt["state"], "SENT")
        self.assertNotIn("secret.example", report.read_text())
        self.assertNotIn("do-not-log-this", json.dumps(sent))
        self.assertEqual(report.stat().st_mode & 0o777, 0o600)

    def test_invalid_and_stale_source_are_explicit_and_bounded(self):
        self.json(self.root / "reports/bank-v2-recovery-20261012/RECOVERY.json",
                  {"source": {"session_id": "../../outside"}})
        value = self.collect()
        self.assertIn("source_invalid", value["errors"])
        self.assertIsNone(value["capture"]["session"])
        self.assertTrue(value["rpc"]["healthy"])
        self.json(self.root / "reports/bank-v2-recovery-20261012/RECOVERY.json", {"source": {"session_id": SESSION}})
        self.json(self.capture / "SESSION.json", {"session_id": OTHER})
        self.json(self.output / "FEED.json", {"session_id": OTHER})
        self.json(self.publisher / "CHECKPOINT.json", {"session": OTHER, "source": "wrong", "prefix": "wrong", "sequence": 3})
        os.utime(self.capture / "events.sqlite", ((NOW - 121000) / 1000,) * 2)
        self.json(self.output / "STATUS.json", {"state": "RUNNING", "last_compact_slot": None,
            "event_sequence": 0, "source_sequence": 0, "updated_unix_ms": NOW - 121000})
        self.json(self.publisher / "STATUS.json", {"state": "RUNNING", "updated_unix_ms": NOW + 31000})
        value = self.collect(service_reader=lambda unit: ("active", "binary --source " + OTHER))
        for code in ("capture_source_mismatch", "compact_source_mismatch", "publisher_source_mismatch",
                     "capture_stale", "compact_stale", "publisher_stale", "service_worker_source_mismatch"):
            self.assertIn(code, value["errors"])
        (self.root / "reports/bank-v2-recovery-20261012/RECOVERY.json").write_bytes(b"x" * (heartbeat.MAX_JSON_BYTES + 1))
        self.assertIn("source_invalid", self.collect()["errors"])

    def source_pin(self, session=SESSION):
        return dict(genesis_hash="genesis", node_identity="identity", agave_revision="revision", session_id=session)

    def raw_fixture(self, phase="ACTIVE", stamp=NOW):
        source = self.source_pin()
        self.json(self.root / "config/native-active-source.json", source)
        output = self.root / heartbeat.RAW_OUTPUT
        capture_id, pid = "a" * 32, int(SESSION.rsplit("-", 1)[1], 16)
        capture = output / capture_id
        self.json(output / "CURRENT.json", dict(capture_id=capture_id, path=str(capture), producer_pid=pid))
        self.json(capture / "SESSION.json", dict(capture_id=capture_id, producer_pid=pid,
            genesis_hash=source["genesis_hash"], validator_identity=source["node_identity"]))
        self.json(capture / "STATUS.json", dict(schema="hivezilla-shred-hook-status-v1", state=phase,
            updated_unix_ms=stamp, durable_next_sequence=100, journal_bytes=12345, counts=dict(packets=99),
            reason="private path and secret must not be copied"))
        return output, capture

    def require_raw(self):
        self.json(self.root / "config/raw-capture-monitor.json",
                  dict(schema=heartbeat.RAW_POLICY_SCHEMA, required=True))

    def test_optional_failed_raw_capture_is_separate_from_native_feed_faults(self):
        self.raw_fixture("FAILED", NOW - 900000)
        value = self.collect()
        self.assertEqual(value["errors"], [])
        self.assertEqual(value["capture"]["state"], "CAPTURING")
        self.assertEqual(value["raw_capture"]["state"], "FAILED")
        self.assertEqual(value["raw_capture"]["recorder_state"], "active")
        self.assertEqual(value["raw_capture"]["faults"], ["raw_capture_failed"])
        self.assertFalse(value["raw_capture"]["required"])
        self.assertNotIn("private path", json.dumps(value))

    def test_local_sender_report_keeps_optional_raw_failure_without_native_faults(self):
        self.raw_fixture("FAILED", NOW - 900000)
        value = self.collect()
        sent = []
        report = self.root / "reports/foundation-watch/STATUS.json"
        self.assertEqual(heartbeat.run_once(value, report, "https://watch.example", TOKEN,
            sender=lambda payload, url, token: sent.append(payload)), 0)
        receipt = json.loads(report.read_text())
        self.assertEqual(receipt["state"], "SENT")
        self.assertEqual(receipt["errors"], [])
        self.assertEqual(receipt["raw_capture"], value["raw_capture"])
        self.assertEqual(receipt["raw_capture"]["state"], "FAILED")
        self.assertFalse(receipt["raw_capture"]["required"])
        self.assertEqual(sent, [value])
        for secret in (TOKEN, "watch.example", "private path", "secret must not be copied"):
            self.assertNotIn(secret, report.read_text())

    def test_required_raw_failure_stale_source_and_missing_capture_are_explicit(self):
        self.require_raw()
        self.assertIn("raw_capture_unavailable", self.collect()["errors"])
        output, capture = self.raw_fixture("FAILED")
        self.assertIn("raw_capture_failed", self.collect()["errors"])
        self.json(capture / "STATUS.json", dict(schema="hivezilla-shred-hook-status-v1", state="ACTIVE",
            updated_unix_ms=NOW - 120001, durable_next_sequence=100, journal_bytes=12345, counts=dict(packets=99)))
        self.assertIn("raw_capture_stale", self.collect()["errors"])
        self.json(output / "CURRENT.json", dict(capture_id="../outside", path="/private", producer_pid=123))
        self.assertIn("raw_capture_source_mismatch", self.collect()["errors"])

    def test_raw_capture_never_infers_health_from_service_or_historical_capture(self):
        output, capture = self.raw_fixture()
        self.require_raw()
        self.assertEqual(self.collect()["raw_capture"]["state"], "ACTIVE")
        self.json(output / ("b" * 32) / "STATUS.json", dict(state="FAILED"))
        self.assertEqual(self.collect()["errors"], [])
        (capture / "STATUS.json").unlink()
        value = self.collect()
        self.assertEqual(value["raw_capture"]["state"], "UNAVAILABLE")
        self.assertIn("raw_capture_unavailable", value["errors"])
        self.json(self.root / "config/raw-capture-monitor.json", dict(required=False))
        self.assertIn("raw_capture_policy_invalid", self.collect()["errors"])

    def capacity_fixture(self, free=750 * 1024**3, stamp=NOW):
        source = self.source_pin()
        self.json(self.root / "config/native-active-source.json", source)
        reserve, recovery, budget = 400 * 1024**3, 500 * 1024**3, 128 * 1024**3
        value = dict(schema="blockzilla-native-capacity-v1", state="RUNNING", source=source,
            updated_unix_ms=stamp, free_disk_bytes=free, capture_reserve_bytes=reserve,
            recovery_minimum_free_bytes=recovery, next_archive_budget_bytes=budget,
            snapshot_headroom_bytes=free - reserve - budget, recovery_headroom_bytes=free - recovery - budget,
            snapshot_capture_reserve_at_risk=free < reserve + budget,
            snapshot_headroom_low=free < recovery + budget)
        path = self.root / "reports/native-capacity/STATUS.json"
        self.json(path, value)
        return path, value

    def test_snapshot_capacity_warns_before_capture_reserve_is_at_risk(self):
        self.assertEqual(self.collect()["errors"], [])  # Legacy host has no installed collector.
        self.capacity_fixture()
        self.assertEqual(self.collect()["errors"], [])
        self.capacity_fixture(free=600 * 1024**3)
        value = self.collect()
        self.assertIn("snapshot_capacity_headroom_low", value["errors"])
        self.assertNotIn("snapshot_capacity_reserve_at_risk", value["errors"])
        self.capacity_fixture(free=520 * 1024**3)
        value = self.collect()
        self.assertIn("snapshot_capacity_reserve_at_risk", value["errors"])
        self.assertLess(value["capacity"]["snapshot_headroom_bytes"], 0)

    def test_installed_capacity_telemetry_fails_closed_on_stale_missing_wrong_source_or_math(self):
        path, value = self.capacity_fixture(stamp=NOW - 90001)
        self.assertIn("snapshot_capacity_stale", self.collect()["errors"])
        path.unlink()
        self.assertIn("snapshot_capacity_unavailable", self.collect()["errors"])
        path, value = self.capacity_fixture()
        self.json(path, dict(value, source=self.source_pin(OTHER)))
        self.assertIn("snapshot_capacity_source_mismatch", self.collect()["errors"])
        self.json(path, dict(value, snapshot_headroom_bytes=0))
        self.assertIn("snapshot_capacity_invalid", self.collect()["errors"])
        self.json(path, dict(value, state="FAILED", error="private details"))
        sample = self.collect()
        self.assertIn("snapshot_capacity_failed", sample["errors"])
        self.assertNotIn("private details", json.dumps(sample))

    def test_terminal_cold_storage_failure_keeps_safe_state_without_progress_unavailable(self):
        self.json(self.output / "cold-storage/STATUS.json", dict(state="FAILED", storage_progress="FAILED",
            updated_unix_ms=NOW, error="private details"))
        value = self.collect()
        self.assertEqual(value["cold_storage"]["state"], "FAILED")
        self.assertIsNone(value["cold_storage"]["retired_banks"])
        self.assertEqual(value["cold_storage"]["storage_progress"], "FAILED")
        self.assertIn("cold_storage_failed", value["errors"])
        self.assertNotIn("cold_storage_unavailable", value["errors"])
        self.assertNotIn("private details", json.dumps(value))

    def storage_jobs_fixture(self, *, failed=False, stamp=NOW):
        source = dict(self.source_pin(), agave_revision="a" * 40)
        self.json(self.root / "config/native-active-source.json", source)
        old = dict(source, session_id=OTHER)
        unit = "hivezilla-native-cold-storage-20261016-source-18de229d.service"
        approved = dict(unit=unit, source=old, max_archived_banks=785546)
        registry = self.root / "config/storage-jobs.json"
        self.json(registry, dict(schema="blockzilla-storage-jobs-registry-v1", root=str(self.root), jobs=[approved]))
        registry.chmod(0o600)
        faults = ["storage_job_failed"] if failed else []
        value = dict(schema="blockzilla-storage-jobs-status-v1", state="DEGRADED" if faults else "HEALTHY",
            active_source=source, registry_sha256=hashlib.sha256(registry.read_bytes()).hexdigest(),
            updated_unix_ms=stamp, issues=faults,
            jobs=[dict(approved, state="FAILED" if failed else "RUNNING", issues=faults,
                       archived_banks=756106, retired_banks=756106, remaining_banks=29440)])
        path = self.root / "reports/storage-jobs/STATUS.json"
        self.json(path, value)
        return registry, path, value

    def test_registered_old_storage_failure_is_visible_separate_from_active_live_storage(self):
        _, _, _ = self.storage_jobs_fixture(failed=True)
        value = self.collect()
        self.assertEqual(value["errors"], ["storage_jobs_degraded"])
        self.assertEqual(value["cold_storage"]["state"], "RUNNING")
        self.assertEqual(value["storage_jobs"]["jobs"][0]["remaining_banks"], 29440)
        self.assertEqual(value["storage_jobs"]["jobs"][0]["source"]["session_id"], OTHER)

    def test_local_sender_keeps_capacity_and_storage_details_after_success_and_failure(self):
        capacity_path, capacity = self.capacity_fixture(free=600 * 1024**3)
        _, jobs_path, jobs = self.storage_jobs_fixture(failed=True)
        private_url = "https://private.example/diagnostics?key=secret"
        capacity.update(source=jobs["active_source"], error=TOKEN, url=private_url)
        self.json(capacity_path, capacity)
        jobs["jobs"][0]["private_details"] = "private storage exception body"
        self.json(jobs_path, jobs)
        with patch.dict(os.environ, {"PRIVATE_TEST_SETTING": "private environment value"}):
            value = self.collect()
            before = json.dumps(value, sort_keys=True)
            for code in (202, 500):
                with self.subTest(http_status=code):
                    opener, transport = self.opener(code, b"private response body" if code == 500 else None)
                    def sender(payload, url, token):
                        heartbeat.send(payload, url, token, opener=opener)
                    report = self.root / "reports/foundation-watch/STATUS.json"
                    self.assertEqual(heartbeat.run_once(value, report, "https://watch.example", TOKEN,
                        sender=sender), 0 if code == 202 else 1)
                    receipt = json.loads(report.read_text())
                    self.assertEqual(receipt["state"], "SENT" if code == 202 else "FAILED")
                    self.assertEqual(receipt["capacity"], value["capacity"])
                    self.assertEqual(receipt["storage_jobs"], value["storage_jobs"])
                    self.assertEqual(receipt["storage_jobs"]["jobs"][0]["remaining_banks"], 29440)
                    self.assertTrue(receipt["capacity"]["snapshot_headroom_low"])
                    self.assertEqual(receipt["errors"], value["errors"])
                    self.assertEqual(json.loads(transport.requests[0].data), value)
                    self.assertEqual(json.dumps(value, sort_keys=True), before)
                    self.assertEqual(report.stat().st_mode & 0o777, 0o600)
                    for secret in (TOKEN, private_url, "watch.example", "private environment value",
                                   "private storage exception body", "private response body"):
                        self.assertNotIn(secret, report.read_text())

    def test_storage_registry_enables_strict_fresh_source_cap_and_registry_checks(self):
        registry, path, data = self.storage_jobs_fixture()
        self.assertEqual(self.collect()["errors"], [])
        self.json(path, dict(data, updated_unix_ms=NOW - 90001))
        self.assertIn("storage_jobs_stale", self.collect()["errors"])
        path.unlink()
        self.assertIn("storage_jobs_unavailable", self.collect()["errors"])
        for mutation in ("active_source", "registry", "job_source", "cap", "unit", "duplicate", "false_healthy", "unsafe_issue"):
            changed = json.loads(json.dumps(data))
            if mutation == "active_source": changed["active_source"]["session_id"] = OTHER
            elif mutation == "registry": changed["registry_sha256"] = "0" * 64
            elif mutation == "job_source": changed["jobs"][0]["source"]["session_id"] = SESSION
            elif mutation == "cap": changed["jobs"][0]["max_archived_banks"] += 1
            elif mutation == "unit": changed["jobs"][0]["unit"] = "unapproved.service"
            elif mutation == "duplicate": changed["jobs"].append(changed["jobs"][0])
            elif mutation == "false_healthy": changed["jobs"][0]["state"] = "FAILED"
            else: changed["issues"] = ["private credential/?token=secret"]
            with self.subTest(mutation=mutation):
                self.json(path, changed)
                value = self.collect()
                self.assertIn("storage_jobs_unavailable", value["errors"])
                self.assertNotIn("token=secret", json.dumps(value))
        registry.unlink()  # Reports cannot authorize jobs without registration.
        self.assertNotIn("storage_jobs", self.collect())
        self.assertEqual(self.collect()["errors"], [])

    def test_only_current_live_storage_registration_may_omit_absolute_cap(self):
        registry, path, data = self.storage_jobs_fixture()
        registered = json.loads(registry.read_text())
        registered["jobs"][0]["max_archived_banks"] = None
        self.json(registry, registered)
        data["registry_sha256"] = hashlib.sha256(registry.read_bytes()).hexdigest()
        data["jobs"][0]["max_archived_banks"] = None
        self.json(path, data)
        self.assertIn("storage_jobs_unavailable", self.collect()["errors"])
        registered["jobs"][0].update(unit="hivezilla-native-cold-storage-20261012-live.service",
                                     source=data["active_source"])
        self.json(registry, registered)
        data["registry_sha256"] = hashlib.sha256(registry.read_bytes()).hexdigest()
        data["jobs"][0].update(registered["jobs"][0])
        self.json(path, data)
        self.assertEqual(self.collect()["errors"], [])

    def test_storage_registry_permissions_are_explicit_and_group_writable_input_is_rejected(self):
        previous = os.umask(0o002)
        try:
            registry, _, _ = self.storage_jobs_fixture()
        finally:
            os.umask(previous)
        self.assertEqual(registry.stat().st_mode & 0o777, 0o600)
        self.assertEqual(self.collect()["errors"], [])
        registry.chmod(0o660)
        self.assertIn("storage_jobs_unavailable", self.collect()["errors"])
        registry.chmod(0o600)
        self.assertEqual(self.collect()["errors"], [])

    def test_explicit_current_pin_wins_without_changing_historical_recovery(self):
        historical = self.root / "reports/bank-v2-recovery-20261012/RECOVERY.json"
        self.json(historical, {"source": {"session_id": OTHER}})
        before = historical.read_bytes()
        pin = self.root / "config/native-active-source.json"
        self.json(pin, self.source_pin())
        value = self.collect()
        self.assertEqual(value["capture"]["session"], SESSION)
        self.assertEqual(value["capture"]["state"], "CAPTURING")
        self.assertEqual(value["errors"], [])
        self.assertEqual(historical.read_bytes(), before)

    def test_absent_pin_keeps_legacy_source_and_existing_invalid_pin_fails_closed(self):
        self.assertEqual(heartbeat.select_source(self.root)["session_id"], SESSION)
        pin = self.root / "config/native-active-source.json"
        for invalid in ({}, {"source": self.source_pin()}, self.source_pin("../../outside"),
                        {**self.source_pin(), "node_identity": None},
                        {**self.source_pin(), "extra": "ignored"}):
            with self.subTest(value=invalid):
                self.json(pin, invalid)
                value = self.collect()
                self.assertIn("source_invalid", value["errors"])
                self.assertIsNone(value["capture"]["session"])
        pin.write_text("invalid JSON")
        self.assertIn("source_invalid", self.collect()["errors"])
        pin.unlink()
        pin.symlink_to(pin.with_name("missing.json"))
        self.assertIn("source_invalid", self.collect()["errors"])

    def test_pin_change_is_observed_without_selecting_newest_capture(self):
        pin = self.root / "config/native-active-source.json"
        self.json(pin, self.source_pin())
        self.create_capture(NEWER)
        self.assertEqual(heartbeat.select_source(self.root)["session_id"], SESSION)
        self.json(pin, self.source_pin(NEWER))
        self.assertEqual(heartbeat.select_source(self.root)["session_id"], NEWER)

    def test_terminal_compact_failure_retains_state_and_nullable_counters(self):
        raw_error = "https://private.example/?token=do-not-copy"
        self.json(self.output / "STATUS.json", {"state": "FAILED", "profile": "native-processed-compact-v1",
            "error": raw_error, "source_preserved": True})
        value = self.collect()
        self.assertEqual(value["compact"], dict(state="FAILED", last_compact_slot=None,
            event_sequence=None, source_sequence=None, updated_unix_ms=None))
        self.assertIn("compact_failed", value["errors"])
        self.assertIn("compact_stale", value["errors"])
        self.assertNotIn("compact_unavailable", value["errors"])
        self.assertNotIn(raw_error, json.dumps(value))

    def test_running_compact_status_still_requires_counters(self):
        self.json(self.output / "STATUS.json", {"state": "RUNNING"})
        value = self.collect()
        self.assertIn("compact_unavailable", value["errors"])

    def test_read_capture_uses_new_timestamp_if_schema_supports_it(self):
        with closing(sqlite3.connect(self.capture / "events.sqlite")) as db:
            db.execute("ALTER TABLE status ADD COLUMN updated_unix_ms INTEGER")
            db.execute("UPDATE status SET updated_unix_ms=?", (NOW - 121000,))
            db.commit()
        value = self.collect()
        self.assertEqual(value["capture"]["updated_unix_ms"], NOW - 121000)
        self.assertIn("capture_stale", value["errors"])

    def create_capture(self, session, active=False):
        path = self.root / "captures/direct-agave-live" / session
        self.json(path / "SESSION.json", {"session_id": session})
        lock = path / "LIVE.lock"
        lock.touch()
        if active:
            stamp = lock.stat()
            with (self.proc / "locks").open("a") as stream:
                stream.write("1: FLOCK ADVISORY WRITE 123 "
                    + f"{os.major(stamp.st_dev):02x}:{os.minor(stamp.st_dev):02x}:{stamp.st_ino} 0 EOF\n")

    def test_new_capture_rejects_old_worker_and_producer_bindings_but_sends_heartbeat(self):
        # Old telemetry is still fresh and all units claim active after restart.
        # The recovery record and downstream units remain on the old session.
        self.create_capture(NEWER, active=True)
        value = self.collect()
        self.assertEqual(value["capture"]["session"], SESSION)
        for code in ("source_newer_capture", "source_active_capture_mismatch",
                     "service_worker_source_mismatch", "service_producer_source_mismatch",
                     "service_publisher_source_mismatch", "service_cold_storage_source_mismatch"):
            self.assertIn(code, value["errors"])
        self.assertEqual(set(value["services"]), set(heartbeat.SERVICES))
        self.assertEqual(value["services"]["hivezilla-live-priority-20261012.service"], "active")
        sent = []
        status = heartbeat.run_once(value, self.root / "reports/foundation-watch/STATUS.json",
            "https://watch.example", TOKEN, sender=lambda payload, url, token: sent.append(payload))
        self.assertEqual(status, 0)
        self.assertEqual(sent[0]["errors"], value["errors"])

    def test_newer_inactive_capture_and_different_older_active_writer_are_detected(self):
        self.create_capture(NEWER)
        value = self.collect()
        self.assertIn("source_newer_capture", value["errors"])
        self.assertNotIn("source_active_capture_mismatch", value["errors"])
        self.assertIn("service_producer_source_mismatch", value["errors"])
        self.create_capture(OTHER, active=True)
        value = self.collect()
        self.assertIn("source_active_capture_mismatch", value["errors"])
        self.assertIn("service_worker_source_mismatch", value["errors"])
        self.assertIn("service_producer_source_mismatch", value["errors"])

    def test_priority_service_failure_and_capture_inventory_failure_remain_observable(self):
        def services(unit):
            return ("inactive" if unit == "hivezilla-live-priority-20261012.service" else "active",
                    "binary --source " + SESSION)
        (self.proc / "locks").unlink()
        value = self.collect(service_reader=services)
        self.assertIn("service_priority_inactive", value["errors"])
        self.assertIn("capture_inventory_unavailable", value["errors"])
        self.assertEqual(value["capture"]["state"], "CAPTURING")

    def worker_launcher(self):
        return ("{ path=/usr/bin/python3 ; argv[]=/usr/bin/python3 "
            + str(self.root / "deploy/processed-stream-20261010/worker_launcher.py")
            + " --processed --capture-root " + str(self.root / "captures/direct-agave-live")
            + " --output-root " + str(self.root / "compact/direct-agave-processed-live")
            + " --engine " + str(self.root / "releases/native-cloud-retention-fast-r3-20261012/hivezilla-direct-agave")
            + " --blockzilla " + str(self.root / "releases/bank-stream-v2-20261010/blockzilla")
            + " ; ignore_errors=no ; start_time=[Tue 2026-10-13] ; pid=123 ; code=(null) ; status=0/0 }")

    def test_real_worker_launcher_without_literal_session_is_checked_without_false_alarm(self):
        command = self.worker_launcher()
        def services(unit):
            return "active", command if unit == "hivezilla-direct-worker.service" else "binary --source " + SESSION
        self.assertTrue(heartbeat.recognized_worker_launcher(command, self.root))
        value = self.collect(service_reader=services)
        self.assertEqual(value["errors"], [])
        self.create_capture(NEWER, active=True)
        value = self.collect(service_reader=services)
        self.assertIn("source_newer_capture", value["errors"])
        self.assertIn("service_worker_source_mismatch", value["errors"])

    def test_generic_commands_with_wrong_script_roots_or_engine_are_not_exempt(self):
        valid = self.worker_launcher()
        invalid = ["binary --capture-root anywhere --output-root anywhere",
                   valid.replace("worker_launcher.py", "other_launcher.py"),
                   valid.replace("/captures/direct-agave-live", "/captures/other"),
                   valid.replace("/compact/direct-agave-processed-live", "/compact/other"),
                   valid.replace("--processed", "--other"),
                   valid.replace("/releases/native-cloud-retention-fast-r3-20261012/hivezilla-direct-agave", "/tmp/hivezilla-direct-agave"),
                   valid.replace("--processed", "--processed --processed")]
        for command in invalid:
            with self.subTest(command_type=command[:40]):
                self.assertFalse(heartbeat.recognized_worker_launcher(command, self.root))
                value = self.collect(service_reader=lambda unit: ("active", command))
                self.assertIn("service_worker_source_mismatch", value["errors"])

    def test_versioned_agave_release_worker_launcher_is_recognized(self):
        release = "agave-4.3.2-20261016-87167f3906bdf511"
        command = self.worker_launcher().replace(
            "native-cloud-retention-fast-r3-20261012", release).replace(
            "bank-stream-v2-20261010/blockzilla", release + "/blockzilla")
        self.assertTrue(heartbeat.recognized_worker_launcher(command, self.root))
        value = self.collect(service_reader=lambda unit: (
            "active", command if unit == "hivezilla-direct-worker.service" else "binary --source " + SESSION))
        self.assertEqual(value["errors"], [])

    def test_versioned_release_paths_and_malformed_launchers_remain_rejected(self):
        release = "agave-4.3.2-20261016-87167f3906bdf511"
        valid = self.worker_launcher().replace(
            "native-cloud-retention-fast-r3-20261012", release).replace(
            "bank-stream-v2-20261010/blockzilla", release + "/blockzilla")
        invalid = []
        for binary in ("hivezilla-direct-agave", "blockzilla"):
            for directory in (".", "..", "../" + release, release + "/../outside",
                              release + "/nested", "." + release, "_" + release,
                              release + "_other", "a" * 97):
                invalid.append(valid.replace(
                    "/releases/" + release + "/" + binary,
                    "/releases/" + directory + "/" + binary))
        invalid.extend((
            valid.replace("argv[]=", "argv[]=ignored argv[]="),
            valid.replace(" --processed ", " --processed unexpected "),
            valid.replace(" --processed ", " --processed --processed "),
            valid.replace("--engine ", '--engine "'),
            valid.replace("--engine ", "--unknown "),
            valid.replace(" --processed ", " --output-root /wrong "),
        ))
        for command in invalid:
            with self.subTest(command=command):
                self.assertFalse(heartbeat.recognized_worker_launcher(command, self.root))
                value = self.collect(service_reader=lambda unit: ("active", command))
                self.assertIn("service_worker_source_mismatch", value["errors"])

    @unittest.skipIf(os.geteuid() == 0, "Root bypasses filesystem permission checks")
    def test_existing_wal_and_shm_are_read_without_write_permissions(self):
        directory = Path(self.temporary.name) / "read-only-capture"
        directory.mkdir()
        path = directory / "events.sqlite"
        writer = sqlite3.connect(path)
        try:
            self.assertEqual(writer.execute("PRAGMA journal_mode=WAL").fetchone()[0], "wal")
            writer.execute("CREATE TABLE status(id INTEGER,state TEXT,durable_sequence INTEGER,latest_rooted_slot INTEGER)")
            writer.execute("INSERT INTO status VALUES(1,'CAPTURING',0,0)")
            writer.commit()
            writer.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            writer.execute("UPDATE status SET durable_sequence=1900,latest_rooted_slot=456208200 WHERE id=1")
            writer.commit()
            # The latest row is committed only to WAL. immutable=1 would miss it.
            with closing(sqlite3.connect(path.as_uri() + "?immutable=1", uri=True)) as immutable:
                self.assertEqual(immutable.execute("SELECT durable_sequence FROM status WHERE id=1").fetchone()[0], 0)
            files = [path, Path(str(path) + "-wal"), Path(str(path) + "-shm")]
            self.assertTrue(all(p.is_file() for p in files))
            before = {p.name: p.read_bytes() for p in files}
            for file in files:
                file.chmod(0o400)
                self.assertFalse(os.access(file, os.W_OK))
            directory.chmod(0o500)
            self.assertFalse(os.access(directory, os.W_OK))
            # Use a different process, as the systemd sender does. Otherwise
            # SQLite can reuse this writer's existing writable SHM mapping.
            code = """import importlib.util,json,sqlite3,sys
from contextlib import closing
from pathlib import Path
spec=importlib.util.spec_from_file_location('heartbeat',sys.argv[1])
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
path=Path(sys.argv[2]);value=module.read_capture(path)
with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True)) as reader:
    reader.execute('PRAGMA query_only=ON')
    try:
        reader.execute('UPDATE status SET durable_sequence=0')
    except sqlite3.OperationalError:
        value['write_rejected']=True
print(json.dumps(value))
"""
            result = subprocess.run([sys.executable, "-B", "-c", code,
                str(Path(__file__).with_name("heartbeat.py")), str(path)],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=5, check=True)
            value = json.loads(result.stdout)
            self.assertEqual(value["durable_sequence"], 1900)
            self.assertEqual(value["latest_rooted_slot"], 456208200)
            self.assertTrue(value["write_rejected"])
            self.assertEqual(before, {p.name: p.read_bytes() for p in files})
            self.assertEqual(set(before), {p.name for p in directory.iterdir()})
        finally:
            directory.chmod(0o700)
            for file in directory.iterdir():
                file.chmod(0o600)
            writer.close()

    def test_redirect_is_rejected_before_second_request_or_token_forward(self):
        opener, transport = self.opener(302, b"", {"Location": "https://evil.example/?private=secret"})
        with self.assertRaisesRegex(heartbeat.SafeError, "^http_redirect_rejected$"):
            heartbeat.send(self.payload(), "https://watch.example", TOKEN, opener=opener)
        self.assertEqual(len(transport.requests), 1)
        self.assertEqual(transport.requests[0].full_url, "https://watch.example/heartbeat")
        self.assertEqual(transport.requests[0].get_header("Authorization"), "Bearer " + TOKEN)

    def test_transport_rejects_url_credentials_and_invalid_outgoing_schema(self):
        opener, transport = self.opener()
        for url in ("http://watch.example", "https://user:secret@watch.example", "https://watch.example?token=secret",
                    "https://watch.example/#secret", "https://watch.example/else", "https://watch.example:8443",
                    "https://watch.example\n.invalid"):
            with self.subTest(url=url), self.assertRaises(heartbeat.SafeError):
                heartbeat.send(self.payload(), url, TOKEN, opener=opener)
        value = self.payload()
        for field, bad in (("schema", "other-v1"), ("machine_id", "other"), ("sampled_unix_ms", True),
                           ("monotonic_ms", None), ("monotonic_ms", True)):
            wrong = {**value, field: bad}
            with self.subTest(field=field), self.assertRaises(heartbeat.SafeError):
                heartbeat.send(wrong, "https://watch.example", TOKEN, opener=opener)
        self.assertEqual(transport.requests, [])

    def test_response_must_acknowledge_the_exact_sample(self):
        invalid = [b"not-json", [], {"accepted": False},
            {"accepted": True, "schema": "other", "sampled_unix_ms": NOW},
            {"accepted": True, "schema": heartbeat.ACCEPTED_SCHEMA, "sampled_unix_ms": NOW - 1},
            {"accepted": True, "schema": heartbeat.ACCEPTED_SCHEMA, "sampled_unix_ms": True},
            b" " * (heartbeat.MAX_REPLY_BYTES + 1)]
        for reply in invalid:
            opener, _ = self.opener(value=reply)
            with self.subTest(reply_type=type(reply).__name__), self.assertRaises(heartbeat.SafeError):
                heartbeat.send(self.payload(), "https://watch.example/heartbeat", TOKEN, opener=opener)
        opener, transport = self.opener()
        heartbeat.send(self.payload(), "https://watch.example/", TOKEN, opener=opener)
        sent = json.loads(transport.requests[0].data)
        self.assertEqual(sent["sampled_unix_ms"], NOW)

    def test_remote_failure_report_has_only_fixed_error_code(self):
        opener, _ = self.opener(500, b"private body with token and URL")
        def sender(payload, url, token):
            heartbeat.send(payload, url, token, opener=opener)
        report = self.root / "reports/foundation-watch/STATUS.json"
        self.assertEqual(heartbeat.run_once(self.payload(), report, "https://watch.example", TOKEN, sender=sender), 1)
        receipt = json.loads(report.read_text())
        self.assertEqual(receipt["send_error"], "watch_send_failed")
        for secret in (TOKEN, "private body", "watch.example"):
            self.assertNotIn(secret, report.read_text())


if __name__ == "__main__":
    unittest.main()
