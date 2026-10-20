from contextlib import redirect_stderr, redirect_stdout
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

import snapshot_budget
from snapshot_budget import (BOUNDED_RETENTION_ARGS, DEFAULT_NEXT_ARCHIVE_BYTES, FULL_FLAG, GIB,
                             INCREMENTAL_FLAG, SnapshotFile, bounded_snapshot_args,
                             inventory_snapshots, snapshot_plan)


# Checked with the actual bank-stream-v2-20261010/agave-validator on Foundation.
PINNED_HELP = """agave-validator 4.3.0 (src:167a39d2; feat:c9ad34d2, client:Agave)
        --maximum-full-snapshots-to-retain <NUMBER>
            [default: 2]
        --maximum-incremental-snapshots-to-retain <NUMBER>
            [default: 4]
"""


def archive(kind, slot, allocated_bytes=1, base_slot=None, hash_digit="1"):
    name = (f"snapshot-{slot}-{hash_digit * 32}.tar.zst" if kind == "full" else
            f"incremental-snapshot-{base_slot}-{slot}-{hash_digit * 32}.tar.zst")
    return SnapshotFile(name, kind, slot, base_slot, allocated_bytes,
                        allocated_bytes, 1, slot, slot)


class SnapshotBudgetTest(unittest.TestCase):
    def test_incident_preserves_recovery_pair_and_active_write(self):
        records = [
            archive("full", 456263292, 123807854592),
            archive("full", 456363384, 123800000000),
            archive("incremental", 456363184, 572633088, 456263292),
            archive("incremental", 456450272, 99977834496, 456363384),
            archive("incremental", 456454087, 106223312896, 456363384),
            archive("incremental", 456458691, 114085154816, 456363384),
            archive("incremental", 456462499, 121916055552, 456363384),
            SnapshotFile("tmp-snapshot-archive-456463503.tar.zst", "active_temporary",
                         None, None, 95000000000, 95000000000, 1, 99, 99),
        ]
        result = snapshot_plan(records, 59966951424)
        self.assertEqual(result["latest_full_slot"], 456363384)
        self.assertEqual(result["latest_matching_incremental_slot"], 456462499)
        self.assertEqual(len(result["retirement_candidates_require_approval"]), 5)
        self.assertEqual(len(result["retained"]), 3)
        self.assertEqual(result["candidate_release_allocated_bytes"], 444666789888)
        self.assertEqual(result["projected_free_after_candidate_retirement_bytes"], 504633741312)
        self.assertFalse(result["projected_recovery_minimum_met"])
        self.assertFalse(result["projected_recovery_with_next_archive_budget_met"])
        self.assertTrue(result["pressure_before_next_archive"])
        self.assertFalse(result["archive_contents_verified"])

    def test_orders_archives_by_slot_not_modification_time_or_name(self):
        records = [archive("full", 9, 8), archive("full", 10, 9),
                   archive("incremental", 12, 10, 10),
                   archive("incremental", 13, 11, 10)]
        first = snapshot_plan(records, 1000, 10, 20)
        second = snapshot_plan(list(reversed(records)), 1000, 10, 20)
        self.assertEqual(first, second)
        self.assertEqual(first["latest_matching_incremental_slot"], 13)
        self.assertEqual({record["slot"] for record in first["retained"]}, {10, 13})

    def test_later_incremental_with_wrong_base_does_not_replace_matching_one(self):
        records = [archive("full", 100), archive("incremental", 110, 1, 100),
                   archive("incremental", 200, 1, 90)]
        result = snapshot_plan(records, 1000, 10, 20)
        self.assertEqual(result["latest_matching_incremental_slot"], 110)
        self.assertEqual([row["slot"] for row in result["retirement_candidates_require_approval"]], [200])

    def test_no_full_archive_proposes_no_retirement(self):
        result = snapshot_plan([archive("incremental", 110, 10, 100)], 0)
        self.assertEqual(result["plan_state"], "BLOCKED_NO_COMPLETED_FULL")
        self.assertEqual(result["retirement_candidates_require_approval"], [])
        self.assertFalse(result["projected_recovery_minimum_met"])

    def test_latest_full_without_incremental_stays_retained(self):
        result = snapshot_plan([archive("full", 100), archive("full", 200),
                                archive("incremental", 150, 1, 100)], 1000, 10, 20)
        self.assertEqual([row["slot"] for row in result["retained"]], [200])
        self.assertIsNone(result["latest_matching_incremental_slot"])

    def test_empty_archive_cannot_replace_the_recovery_pair(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            (directory / archive("full", 100).name).write_bytes(b"full")
            (directory / archive("incremental", 110, base_slot=100).name).write_bytes(b"incremental")
            (directory / archive("full", 200).name).touch()
            result = snapshot_plan(inventory_snapshots(directory), 1000, 10, 20)
            self.assertEqual(result["latest_full_slot"], 100)
            self.assertEqual(result["latest_matching_incremental_slot"], 110)
            self.assertEqual(result["retirement_candidates_require_approval"], [])

    def test_equal_newest_identities_are_all_retained(self):
        records = [archive("full", 100), archive("full", 100, hash_digit="2"),
                   archive("incremental", 110, 1, 100),
                   archive("incremental", 110, 1, 100, hash_digit="2")]
        result = snapshot_plan(records, 1000, 10, 20)
        self.assertEqual(len(result["retained"]), 4)
        self.assertEqual(result["retirement_candidates_require_approval"], [])
        self.assertIn("Equal newest slots", result["warnings"][-1])

    def test_inventory_keeps_symlinks_directories_and_unknown_names(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            full = archive("full", 100).name
            (directory / full).write_bytes(b"full")
            (directory / archive("full", 90).name).symlink_to(full)
            (directory / archive("full", 80).name).mkdir()
            (directory / "tmp-snapshot-archive-120.tar.zst").write_bytes(b"writing")
            (directory / "snapshot-not-a-slot.tar.zst").write_bytes(b"unknown")
            (directory / f"incremental-snapshot-100-90-{'1' * 32}.tar.zst").write_bytes(b"invalid")
            result = snapshot_plan(inventory_snapshots(directory), 1000, 10, 20)
            self.assertEqual(result["retirement_candidates_require_approval"], [])
            self.assertEqual(len(result["retained"]), 6)
            self.assertEqual(sum(row["kind"] == "other" for row in result["retained"]), 4)

    def test_larger_write_budget_cannot_be_reduced_to_observed_floor(self):
        records = [archive("full", 100, 100)]
        result = snapshot_plan(records, 600, 400, 500, 200)
        self.assertEqual(result["next_archive_budget_bytes"], 200)
        self.assertEqual(result["recovery_with_next_archive_threshold_free_bytes"], 700)
        self.assertTrue(result["projected_recovery_minimum_met"])
        self.assertFalse(result["projected_recovery_with_next_archive_budget_met"])
        self.assertEqual(snapshot_plan(records, 600, 400, 500, 1)["next_archive_budget_bytes"], 100)

    def test_invalid_budgets_and_duplicate_paths_fail_closed(self):
        records = [archive("full", 100)]
        for free, reserve, recovery, budget in [(-1, 400, 500, None),
                                              (1000, 500, 400, None),
                                              (1000, 400, 500, -1),
                                              (1000, 400, 500, 0),
                                              (True, 400, 500, None)]:
            with self.subTest((free, reserve, recovery, budget)), self.assertRaises(ValueError):
                snapshot_plan(records, free, reserve, recovery, budget)
        with self.assertRaises(ValueError):
            snapshot_plan(records * 2, 1000)

    def test_cli_is_read_only_and_emits_recorded_sample_identity(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            for record in [archive("full", 90), archive("full", 100)]:
                (directory / record.name).write_bytes(b"full")
            before = [(path.name, path.read_bytes()) for path in directory.iterdir()]
            process = subprocess.run([sys.executable, str(Path(__file__).with_name("snapshot_budget.py")),
                                      "--snapshots", str(directory), "--free-bytes", str(500 * GIB)],
                                     capture_output=True, text=True, check=True)
            result = json.loads(process.stdout)
            self.assertTrue(result["read_only"])
            self.assertEqual(result["free_space_source"], "recorded_sample")
            self.assertEqual(len(result["retirement_candidates_require_approval"]), 1)
            self.assertEqual(before, [(path.name, path.read_bytes()) for path in directory.iterdir()])

    def test_startup_gate_requires_recovery_minimum_and_archive_floor(self):
        required = 500 * GIB + DEFAULT_NEXT_ARCHIVE_BYTES
        for free, expected in [(required - 1, False), (required, True)]:
            result = snapshot_plan([], free)
            self.assertEqual(result["startup_disk_gate_passed"], expected)
            self.assertEqual(result["next_archive_budget_bytes"], 128 * GIB)
            self.assertEqual(result["retirement_candidates_require_approval"], [])
            self.assertEqual(result["plan_state"], "BLOCKED_NO_COMPLETED_FULL")

    def test_larger_observed_archive_overrides_startup_floor(self):
        result = snapshot_plan([archive("full", 100, 250 * GIB)], 628 * GIB)
        self.assertEqual(result["next_archive_budget_bytes"], 250 * GIB)
        self.assertFalse(result["startup_disk_gate_passed"])

    def test_check_start_cli_uses_current_statvfs_and_returns_temporary_failure(self):
        with tempfile.TemporaryDirectory() as temporary:
            required = 628 * GIB
            for free, expected_code in [(required - 1, 75), (required, 0)]:
                output = io.StringIO()
                usage = type("Usage", (), {"f_bavail": free, "f_frsize": 1})()
                with mock.patch.object(snapshot_budget.os, "statvfs", return_value=usage), redirect_stdout(output):
                    result = snapshot_budget.main(["--snapshots", temporary, "--check-start"])
                self.assertEqual(result, expected_code)
                report = json.loads(output.getvalue())
                self.assertEqual(report["free_space_source"], "statvfs")
                self.assertEqual(report["startup_disk_gate_passed"], expected_code == 0)

    def test_startup_gate_rejects_recorded_space_and_missing_directory(self):
        with tempfile.TemporaryDirectory() as temporary:
            with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
                snapshot_budget.main(["--snapshots", temporary, "--check-start",
                                      "--free-bytes", str(999 * GIB)])
            self.assertEqual(error.exception.code, 2)
            output = io.StringIO()
            with redirect_stdout(output):
                result = snapshot_budget.main(["--snapshots", str(Path(temporary) / "absent"), "--check-start"])
            self.assertEqual(result, 75)
            self.assertFalse(json.loads(output.getvalue())["startup_disk_gate_passed"])

    def test_startup_gate_rejects_snapshot_directory_symlink(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "snapshots").mkdir()
            (root / "link").symlink_to(root / "snapshots")
            with redirect_stdout(io.StringIO()):
                result = snapshot_budget.main(["--snapshots", str(root / "link"), "--check-start"])
            self.assertEqual(result, 75)


class SnapshotArgsTest(unittest.TestCase):
    def test_preserves_binary_and_existing_options_and_is_idempotent(self):
        argv = ["/release/agave-validator", "--ledger", "/ledger", "--no-voting"]
        bounded = bounded_snapshot_args(argv, PINNED_HELP)
        self.assertEqual(bounded, argv + list(BOUNDED_RETENTION_ARGS))
        self.assertEqual(bounded_snapshot_args(bounded, PINNED_HELP), bounded)

    def test_replaces_existing_values_in_both_argument_forms(self):
        argv = ["validator", FULL_FLAG + "=2", INCREMENTAL_FLAG, "4", "--no-snapshot-fetch"]
        self.assertEqual(bounded_snapshot_args(argv, PINNED_HELP),
                         ["validator", "--no-snapshot-fetch"] + list(BOUNDED_RETENTION_ARGS))

    def test_unsupported_help_disabled_snapshots_and_bad_values_fail_closed(self):
        for argv, help_text in [(["validator"], PINNED_HELP.replace(FULL_FLAG, "--other")),
                                (["validator", "--no-snapshots"], PINNED_HELP),
                                (["validator", "--no-incremental-snapshots"], PINNED_HELP),
                                (["validator", FULL_FLAG], PINNED_HELP),
                                (["validator", FULL_FLAG, "--no-voting"], PINNED_HELP),
                                (["validator", INCREMENTAL_FLAG + "=0"], PINNED_HELP)]:
            with self.subTest(argv=argv), self.assertRaises(ValueError):
                bounded_snapshot_args(argv, help_text)


if __name__ == "__main__":
    unittest.main()
