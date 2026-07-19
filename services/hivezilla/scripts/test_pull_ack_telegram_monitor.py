#!/usr/bin/env python3

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("pull_ack_telegram_monitor.py")
SPEC = importlib.util.spec_from_file_location("pull_ack_telegram_monitor", SCRIPT)
assert SPEC and SPEC.loader
monitor = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = monitor
SPEC.loader.exec_module(monitor)


class PullAckTelegramMonitorTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        root = Path(self.directory.name)
        self.status = root / "pull-ack-status.json"
        self.state = root / "pull-ack-alert.json"
        self.token = root / "token"
        self.token.write_text("123456:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghi", encoding="ascii")
        self.config = monitor.Config(
            ack_status_file=self.status,
            state_file=self.state,
            token_file=self.token,
            chat_id="-100123456",
            message_thread_id=None,
            stale_after_secs=300,
            startup_grace_secs=300,
            interval_secs=30,
        )
        self.store = monitor.StateStore(self.state)
        self.sent = []

    def tearDown(self):
        self.directory.cleanup()

    def write_status(self, updated, sequence=7):
        self.status.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "through_sequence": sequence,
                    "updated_unix_secs": updated,
                }
            ),
            encoding="utf-8",
        )

    def sender(self, message):
        self.sent.append(message)
        return monitor.DELIVERED

    def run_check(self, now, elapsed):
        monitor.run_once(
            self.config,
            self.store,
            self.sender,
            now=now,
            startup_elapsed=elapsed,
        )

    def test_fresh_status_is_silent(self):
        self.write_status(990)
        self.run_check(1000, 301)
        self.assertEqual(self.sent, [])
        self.assertEqual(self.store.load(), "inactive")

    def test_stale_status_opens_once_and_recovers_once(self):
        self.write_status(600)
        self.run_check(1000, 299)
        self.assertEqual(self.sent, [])
        self.run_check(1000, 300)
        self.assertEqual(len(self.sent), 1)
        self.assertIn("6 minutes ago", self.sent[0])
        self.assertIn("every unconfirmed gRPC block", self.sent[0])
        self.run_check(1030, 330)
        self.assertEqual(len(self.sent), 1)

        self.write_status(1035, sequence=8)
        self.run_check(1040, 340)
        self.assertEqual(len(self.sent), 2)
        self.assertIn("RECOVERED", self.sent[1])
        self.assertIn("not indexing", self.sent[1])
        self.run_check(1070, 370)
        self.assertEqual(len(self.sent), 2)

    def test_missing_status_uses_clear_unknown_age(self):
        self.run_check(1000, 300)
        self.assertEqual(len(self.sent), 1)
        self.assertIn("Last confirmation: unavailable.", self.sent[0])

    def test_definite_rejection_retries_without_marking_active(self):
        calls = []

        def fails(message):
            calls.append(message)
            return monitor.REJECTED

        monitor.run_once(
            self.config,
            self.store,
            fails,
            now=1000,
            startup_elapsed=300,
        )
        self.assertEqual(len(calls), 1)
        self.assertEqual(self.store.load(), "inactive")
        self.run_check(1030, 330)
        self.assertEqual(len(self.sent), 1)

    def test_ambiguous_opening_is_not_retried(self):
        calls = []

        def ambiguous(message):
            calls.append(message)
            return monitor.AMBIGUOUS

        monitor.run_once(
            self.config,
            self.store,
            ambiguous,
            now=1000,
            startup_elapsed=300,
        )
        self.assertEqual(len(calls), 1)
        self.assertEqual(self.store.load(), "active")
        monitor.run_once(
            self.config,
            self.store,
            ambiguous,
            now=1030,
            startup_elapsed=330,
        )
        self.assertEqual(len(calls), 1)

    def test_ambiguous_recovery_is_not_retried(self):
        calls = []

        def ambiguous(message):
            calls.append(message)
            return monitor.AMBIGUOUS

        self.store.save("active", 900)
        self.write_status(995)
        monitor.run_once(
            self.config,
            self.store,
            ambiguous,
            now=1000,
            startup_elapsed=300,
        )
        self.assertEqual(len(calls), 1)
        self.assertEqual(self.store.load(), "inactive")
        monitor.run_once(
            self.config,
            self.store,
            ambiguous,
            now=1030,
            startup_elapsed=330,
        )
        self.assertEqual(len(calls), 1)

    def test_ambiguous_crash_phases_do_not_duplicate(self):
        self.store.save("opening", 1000)
        self.assertEqual(self.store.load(), "active")
        self.store.save("recovery", 1001)
        self.assertEqual(self.store.load(), "inactive")

    def test_symlink_status_is_not_followed(self):
        target = self.status.with_name("target")
        target.write_text("{}", encoding="utf-8")
        self.status.symlink_to(target)
        self.assertIsNone(monitor.read_ack_snapshot(self.status, 1000))


if __name__ == "__main__":
    unittest.main()
