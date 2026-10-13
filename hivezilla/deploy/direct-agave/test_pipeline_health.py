import contextlib
import fcntl
import io
import json
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import pipeline_health as health


class DurableHealthTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.session = self.root / "capture" / "00000000000000000000000000000001-1"
        self.session.mkdir(parents=True)
        self.output = self.root / "output" / self.session.name
        self.output.mkdir(parents=True)
        self.write(self.session / "SESSION.json", {
            "session_id": self.session.name,
            "config": {"max_journal_bytes": 1024**3, "reserve_disk_bytes": 0,
                       "node_identity": "test-identity", "genesis_hash": "test-genesis"}})
        self.write(self.session / "FAILURE.json", {})
        with contextlib.closing(sqlite3.connect(self.session / "events.sqlite")) as connection:
            connection.executescript("CREATE TABLE status(id,state,durable_sequence,error);"
                                     "INSERT INTO status VALUES(1,'CAPTURING',10,NULL);"
                                     "CREATE TABLE events(slot,kind);"
                                     "INSERT INTO events VALUES(100,'rooted');")
        self.lock = (self.session / "LIVE.lock").open("w+")
        self.addCleanup(self.lock.close)
        fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        self.write(self.output / "STATUS.json", {"state": "BUILDING", "last_compact_slot": 99})
        self.args = SimpleNamespace(capture_root=self.session.parent,
                                    output_root=self.output.parent,
                                    report=self.root / "health.json",
                                    shred_root=None, rpc="http://test.invalid")

    @staticmethod
    def write(path, value):
        path.write_text(json.dumps(value))

    def inspect(self):
        def rpc(_url, method, _params=None):
            return {"getHealth": "ok", "getSlot": 101,
                    "getIdentity": {"identity": "test-identity"},
                    "getGenesisHash": "test-genesis"}[method]
        with patch.object(health, "rpc", side_effect=rpc), \
                patch.object(health.shutil, "disk_usage", return_value=SimpleNamespace(free=1024**4)), \
                contextlib.redirect_stdout(io.StringIO()):
            result = health.report(self.args)
        return result, json.loads(self.args.report.read_text())

    def test_service_can_be_live_while_source_is_failed(self):
        self.write(self.session / "FAILURE.json", {"state": "FAILED", "error": "queue limit"})
        result, value = self.inspect()
        self.assertEqual(result, 1)
        self.assertTrue(value["capture"]["writer_live"])
        self.assertIn("queue limit", value["problems"])

    def test_unclean_writer_exit_is_failed(self):
        fcntl.flock(self.lock, fcntl.LOCK_UN)
        result, value = self.inspect()
        self.assertEqual(result, 1)
        self.assertIn("Capture writer stopped without a clean close", value["problems"])

    def test_stalled_durable_sequence_is_failed(self):
        self.write(self.args.report, {"capture": {"session_id": self.session.name,
                                                  "durable_sequence": 10},
                                     "last_durable_change_unix_ms": 1})
        result, value = self.inspect()
        self.assertEqual(result, 1)
        self.assertIn("No capture records became durable for more than 30 seconds", value["problems"])

    def test_healthy_capture_reports_root_to_compact_lag(self):
        result, value = self.inspect()
        self.assertEqual(result, 0)
        self.assertEqual(value["state"], "HEALTHY")
        self.assertEqual(value["compact_lag_slots"], 1)
        self.assertFalse(value["full_epoch"])

    def test_retirement_failure_is_visible_while_capture_continues(self):
        self.write(self.session / "RETIREMENT-FAILED.json", {"error": "compact evidence changed"})
        result, value = self.inspect()
        self.assertEqual(result, 1)
        self.assertEqual(value["capture"]["state"], "CAPTURING")
        self.assertIn("Source retirement stopped: compact evidence changed", value["problems"])

    def test_live_backlog_is_warning_but_source_failure_still_has_priority(self):
        with contextlib.closing(sqlite3.connect(self.session / "events.sqlite")) as connection:
            connection.execute("INSERT INTO events VALUES(500,'rooted')")
            connection.commit()
        result, value = self.inspect()
        self.assertEqual(result, 0)
        self.assertEqual(value["state"], "WARNING")
        self.assertEqual(value["compact_lag_slots"], 401)
        self.write(self.session / "FAILURE.json", {"state": "FAILED", "error": "queue limit"})
        result, value = self.inspect()
        self.assertEqual(result, 1)
        self.assertEqual(value["state"], "FAILED")
        self.assertIn("queue limit", value["problems"])


if __name__ == "__main__":
    unittest.main()
