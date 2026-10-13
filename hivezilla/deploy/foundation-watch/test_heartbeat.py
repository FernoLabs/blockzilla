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
            disk_reader=kwargs.pop("disk_reader", lambda path: SimpleNamespace(free=900 * 1024**3)), **kwargs)

    def payload(self):
        return self.collect()

    def opener(self, code=202, value=None, headers=None):
        if value is None:
            value = {"accepted": True, "schema": heartbeat.ACCEPTED_SCHEMA, "sampled_unix_ms": NOW}
        body = value if isinstance(value, bytes) else json.dumps(value).encode()
        transport = FakeHTTPS(code, body, headers)
        return urllib.request.build_opener(urllib.request.ProxyHandler({}), transport, heartbeat.NoRedirect()), transport

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
