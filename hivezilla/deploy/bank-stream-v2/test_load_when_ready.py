from contextlib import closing
import importlib.util
import json
import os
from pathlib import Path
import shutil
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location("loader", Path(__file__).with_name("load_when_ready.py"))
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)

SESSION = "000000000000000018ddd9a6bb6fc50e-123"


class LoaderTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.capture_root = self.root / "captures"
        self.capture_root.mkdir()
        self.binary = self.root / "validator"
        self.config = self.root / "config.json"
        self.config.write_text(json.dumps(dict(output_root=str(self.capture_root), reserve_disk_bytes=400 * m.GIB)))
        self.report = self.root / "reports/bank-v2-native-load.json"
        self.report.parent.mkdir()
        self.report.write_text(json.dumps(dict(state="LOADED", validator_pid=99, session_id="old")))
        self.pin = self.root / "config/native-active-source.json"
        self.pin.parent.mkdir()
        self.pin.write_bytes(b"retained source pin\n")
        self.pin_before = self.pin.read_bytes()
        self.clock = 0
        self.advance_capture = False

    def tearDown(self):
        self.assertEqual(self.pin.read_bytes(), self.pin_before)
        self.temporary.cleanup()

    def sleep(self, seconds):
        self.clock += seconds
        if self.advance_capture and (self.capture_root / SESSION / "events.sqlite").exists():
            with closing(sqlite3.connect(self.capture_root / SESSION / "events.sqlite")) as db:
                db.execute("UPDATE status SET durable_sequence=durable_sequence+1,latest_rooted_slot=latest_rooted_slot+1")
                db.commit()

    def start_capture(self, command, **kwargs):
        self.assertEqual(command[-3:], ["plugin", "load", str(self.config)])
        path = self.capture_root / SESSION
        path.mkdir()
        (path / "SESSION.json").write_text(json.dumps(dict(
            session_id=SESSION, bank_observations_v2=True, agave_revision="revision")))
        (path / "FAILURE.json").write_text("{}")
        (path / "LIVE.lock").touch()
        with closing(sqlite3.connect(path / "events.sqlite")) as db:
            db.execute("CREATE TABLE status(id INTEGER PRIMARY KEY,state TEXT,durable_sequence INTEGER,latest_rooted_slot INTEGER)")
            db.execute("INSERT INTO status VALUES(1,'CAPTURING',100,40)")
            db.commit()
        return SimpleNamespace(returncode=0)

    def run_loader(self, *, disk=None, rpc=None, host=None, load=None, writer=True):
        with mock.patch.object(m, "verify_host", side_effect=host, return_value=123), \
             mock.patch.object(m.shutil, "disk_usage", side_effect=disk, return_value=SimpleNamespace(free=600 * m.GIB)), \
             mock.patch.object(m, "rpc", side_effect=rpc or ["ok", 102, 100]), \
             mock.patch.object(m.subprocess, "run", side_effect=load or self.start_capture) as child, \
             mock.patch.object(m, "writer_present", return_value=writer), \
             mock.patch.object(m.time, "monotonic", side_effect=lambda: self.clock), \
             mock.patch.object(m.time, "sleep", side_effect=self.sleep):
            result = m.run_loader(self.root, self.binary, self.config)
        return result, child

    def status(self):
        return json.loads(self.report.read_text())

    def test_low_disk_blocks_load_and_keeps_validator_and_source_pin(self):
        result, child = self.run_loader(disk=lambda path: SimpleNamespace(free=499 * m.GIB))
        self.assertEqual(result, 0)
        child.assert_not_called()
        self.assertEqual(self.status()["state"], "CAPTURE_BLOCKED_STORAGE")
        self.assertEqual(self.status()["validator_pid"], 123)
        self.assertEqual(self.status()["capture_reserve_bytes"], 400 * m.GIB)
        self.assertEqual(self.report.stat().st_mode & 0o777, 0o600)

    def test_disk_is_rechecked_after_catch_up_immediately_before_load(self):
        result, child = self.run_loader(disk=[SimpleNamespace(free=600 * m.GIB),
            SimpleNamespace(free=600 * m.GIB), SimpleNamespace(free=499 * m.GIB)])
        self.assertEqual(result, 0)
        child.assert_not_called()
        self.assertEqual(self.status()["state"], "CAPTURE_BLOCKED_STORAGE")

    def test_failed_capture_has_one_load_attempt_and_no_false_loaded_receipt(self):
        def fail(command, **kwargs):
            result = self.start_capture(command, **kwargs)
            (self.capture_root / SESSION / "FAILURE.json").write_text(json.dumps(
                dict(state="FAILED", error="private error text")))
            return result
        result, child = self.run_loader(load=fail)
        self.assertEqual(result, 0)
        self.assertEqual(child.call_count, 1)
        self.assertEqual(self.status()["state"], "CAPTURE_FAILED")
        self.assertEqual(self.status()["reason"], "new_capture_failed")
        self.assertTrue(self.status()["operator_recovery_required"])
        self.assertNotIn("private error text", self.report.read_text())

    def test_healthy_load_requires_real_durable_and_rooted_progress(self):
        self.advance_capture = True
        result, child = self.run_loader()
        self.assertEqual(result, 0)
        self.assertEqual(child.call_count, 1)
        status = self.status()
        self.assertEqual(status["state"], "LOADED")
        self.assertEqual(status["session_id"], SESSION)
        self.assertEqual(status["durable_sequence"], 101)
        self.assertEqual(status["latest_rooted_slot"], 41)
        self.assertTrue(status["writer_lock_verified"])

    def test_missing_writer_or_stalled_capture_cannot_be_loaded(self):
        for writer, expected in [(False, "new_capture_writer_missing"), (True, "new_capture_progress_not_observed")]:
            with self.subTest(writer=writer):
                existing = self.capture_root / SESSION
                if existing.exists():
                    shutil.rmtree(existing)
                with mock.patch.object(m, "PROGRESS_SECONDS", 1):
                    result, child = self.run_loader(writer=writer)
                self.assertEqual(result, 0)
                self.assertEqual(child.call_count, 1)
                self.assertEqual(self.status()["state"], "CAPTURE_FAILED")
                self.assertEqual(self.status()["reason"], expected)

    def test_pid_change_prevents_plugin_load(self):
        with self.assertRaisesRegex(RuntimeError, "validator_pid_changed"), \
             mock.patch.object(m.subprocess, "run") as child, \
             mock.patch.object(m, "verify_host", side_effect=[123, 123, RuntimeError("validator_pid_changed")]), \
             mock.patch.object(m.shutil, "disk_usage", return_value=SimpleNamespace(free=600 * m.GIB)):
            m.run_loader(self.root, self.binary, self.config)
        child.assert_not_called()
        self.assertNotEqual(self.status()["state"], "LOADED")

    def test_false_health_and_catch_up_never_load_capture(self):
        for healthy in (False, True):
            self.clock = 0
            def unavailable(url, method, params):
                if method == "getHealth":
                    return "ok" if healthy else "unhealthy"
                return 10 if url.startswith("http://127") else 100
            with self.subTest(healthy=healthy), mock.patch.object(m, "BOOTSTRAP_SECONDS", 3):
                result, child = self.run_loader(rpc=unavailable)
            self.assertEqual(result, 1)
            child.assert_not_called()
            self.assertEqual(self.status()["state"], "CAPTURE_NOT_READY")

    def test_reserve_must_be_a_nonnegative_bounded_integer(self):
        for invalid in (True, -1, 4.5, 2**53):
            with self.subTest(value=invalid):
                self.config.write_text(json.dumps(dict(output_root=str(self.capture_root), reserve_disk_bytes=invalid)))
                with self.assertRaisesRegex(RuntimeError, "invalid_capture_reserve"), mock.patch.object(m.subprocess, "run") as child:
                    m.run_loader(self.root, self.binary, self.config)
                child.assert_not_called()

    def test_writer_lock_matches_validator_pid_and_actual_file(self):
        self.start_capture(["validator", "plugin", "load", str(self.config)])
        path = self.capture_root / SESSION
        proc = self.root / "proc"
        proc.mkdir()
        lock = (path / "LIVE.lock").stat()
        identity = f"{os.major(lock.st_dev):02x}:{os.minor(lock.st_dev):02x}:{lock.st_ino}"
        (proc / "locks").write_text(f"1: FLOCK ADVISORY WRITE 123 {identity} 0 EOF\n")
        self.assertTrue(m.writer_present(path, 123, proc))
        self.assertFalse(m.writer_present(path, 456, proc))

    def test_journal_read_is_read_only_and_missing_schema_fails(self):
        self.start_capture(["validator", "plugin", "load", str(self.config)])
        path = self.capture_root / SESSION
        database = path / "events.sqlite"
        before = database.read_bytes()
        self.assertEqual(m.journal_status(path), (100, 40))
        self.assertEqual(database.read_bytes(), before)
        database.unlink()
        with self.assertRaises(sqlite3.OperationalError):
            m.journal_status(path)
        self.assertFalse(database.exists())


if __name__ == "__main__":
    unittest.main()
