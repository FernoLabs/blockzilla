"""Read-only archive telemetry checks using observed Foundation log shapes."""
from datetime import datetime, timezone
import hashlib
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import json

import archive_status

NOW = 1791865000000


class ArchiveStatusTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.job = self.root / archive_status.JOB
        self.calls = []
        self.write_json(self.job / "PLAN.json", {"epochs": [
            {"epoch": 1049, "bytes": 10**12}, {"epoch": 1050, "bytes": 10**12},
            {"epoch": 1051, "bytes": 12 * 10**11}, {"epoch": 1052, "bytes": 10**12}]})
        self.set_state("REGISTRIES")
        self.write_json(self.root / "reports/live-priority-20261012/STATUS.json",
                        {"state": "BACKGROUND_RUNNING", "updated_unix_ms": NOW - 1000})

    @staticmethod
    def write_json(path, value, at_ms=NOW):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value))
        os.utime(path, ns=(at_ms * 1000000, at_ms * 1000000))

    def set_state(self, state, start_ms=NOW - 100000, epoch=1051):
        self.write_json(self.job / "STATUS.json", {
            "state": state, "epoch": epoch, "updated_unix_ms": start_ms})

    @staticmethod
    def iso(at_ms):
        return datetime.fromtimestamp(at_ms / 1000, timezone.utc).isoformat().replace("+00:00", "Z")

    @staticmethod
    def rclone_time(at_ms):
        return datetime.fromtimestamp(at_ms / 1000, timezone.utc).strftime("%Y/%m/%d %H:%M:%S")

    def log(self, lines, state="REGISTRIES", at_ms=NOW - 3000):
        path = self.job / ("controller.log" if state == "UPLOADING" else "epoch-1051.log")
        path.write_text("\n".join(lines) + "\n")
        os.utime(path, ns=(at_ms * 1000000, at_ms * 1000000))
        return path

    def collect(self, active="active"):
        def services(unit):
            self.calls.append(unit)
            return active, ""
        return archive_status.collect(self.root, NOW, services)

    def history(self, epoch, stages, selected_bytes=100000000000):
        source = NOW // 1000 - (9000 if epoch == 1049 else 5000)
        names = ("source.json", "download.json", "REGISTRIES.json", "REGISTRY_INDEX.json", "COMPACTING.json")
        at = source
        for index, name in enumerate(names):
            self.write_json(self.job / f"epoch-{epoch}" / name, {}, at * 1000)
            at += stages[index]
        self.write_json(self.job / f"upload-state/epoch-{epoch}/complete.json", {
            "schema": "blockzilla-stored-compact-backup-v1", "epoch": epoch,
            "upload_profile": "query-with-signatures", "bytes": selected_bytes,
            "completed_unix_seconds": at, "remote_file_sizes_verified": True,
            "full_remote_readback": False, "canonical_publication": False})

    def registry_line(self, at_ms=NOW - 3000, percent="82.0", eta="3m 25s"):
        return (self.iso(at_ms) + " INFO blockzilla: [Split Compact Registry] "
                f"progress={percent}% ETA={eta} | blocks=354309 (378 blk/s) "
                "txs=434033026 (463266 tx/s) | slots=454032000-454386423 (354423) | elapsed=15m 36s")

    def test_observed_progress_shapes_have_exact_scopes_and_rates(self):
        cases = [
            ("DOWNLOADING", "[#d210fe 745GiB/746GiB(99%) CN:8 DL:368MiB ETA:1s]", 99, 1, "download", 368 * 1024**2),
            ("REGISTRIES", "\x1b[2m" + self.registry_line() + "\x1b[0m", 82, 205, "stage", None),
            ("COMPACTING", self.iso(NOW - 3000) + " INFO blockzilla: [Archive V2 Hot Write] "
             "progress=73.9% ETA=12m 3s | blocks=318994 (145 blk/s) txs=398144328 (179044 tx/s) "
             "| slots=453600000-453919091 (319091) | elapsed=37m 14s", 73.9, 723, "stage", None),
            ("UPLOADING", self.rclone_time(NOW - 3000) +
             " INFO  :   114.469 GiB / 118.556 GiB, 97%, 90.589 MiB/s, ETA 46s", 97, 46, "upload", 90.589 * 1024**2),
        ]
        for state, line, percent, eta, scope, speed in cases:
            with self.subTest(state=state):
                self.set_state(state)
                self.log([line], state)
                result = self.collect()
                self.assertEqual(result["progress_percent"], percent)
                self.assertEqual(result["eta_seconds"], eta)
                self.assertEqual(result["eta_scope"], scope)
                self.assertEqual(result["progress_updated_unix_ms"], NOW - 3000)
                if speed is None:
                    self.assertIsNone(result["bytes_per_second"])
                else:
                    self.assertAlmostEqual(result["bytes_per_second"], speed)
        self.assertEqual(set(self.calls), {archive_status.UNIT})

    def test_old_stage_lines_cannot_be_renewed_by_new_log_bytes(self):
        for state, line in [
            ("REGISTRIES", self.registry_line(NOW - 121000)),
            ("UPLOADING", self.rclone_time(NOW - 121000) + " INFO : 14 GiB / 100 GiB, 14%, 90.5 MiB/s, ETA 16m4s"),
        ]:
            with self.subTest(state=state):
                self.set_state(state, NOW - 300000)
                self.log([line, "A later non-progress line must not renew the estimate."], state, NOW)
                result = self.collect()
                self.assertIsNone(result["progress_percent"])
                self.assertIsNone(result["eta_seconds"])
                self.assertIsNone(result["progress_updated_unix_ms"])
        self.set_state("DOWNLOADING", NOW - 300000)
        self.log(["[#d210fe 100GiB/746GiB(13%) CN:8 DL:368MiB ETA:30m1s]"], "DOWNLOADING", NOW - 121000)
        self.assertIsNone(self.collect()["eta_seconds"])

    def test_previous_upload_and_other_stage_lines_do_not_leak(self):
        self.set_state("UPLOADING", NOW - 5000)
        self.log([self.rclone_time(NOW - 10000) + " INFO : 100 GiB / 100 GiB, 100%, 35.8 MiB/s, ETA 0s"], "UPLOADING", NOW)
        self.assertIsNone(self.collect()["progress_percent"])
        self.set_state("REGISTRIES")
        self.log(["[#d210fe 745GiB/746GiB(99%) CN:8 DL:368MiB ETA:1s]",
                  self.iso(NOW - 1000) + " INFO : [Archive V2 Hot Write] progress=99.9% ETA=2s | blocks=431328"])
        self.assertIsNone(self.collect()["progress_percent"])

    def test_final_upload_eta_dash_keeps_current_progress_without_old_eta(self):
        self.set_state("UPLOADING")
        self.log([self.rclone_time(NOW - 3000) +
                  " INFO : 117.171 GiB / 118.556 GiB, 99%, 91.935 MiB/s, ETA 15s",
                  self.rclone_time(NOW - 1000) +
                  " INFO : 118.578 GiB / 118.556 GiB, 100%, 35.891 MiB/s, ETA -"],
                 "UPLOADING", NOW)
        result = self.collect()
        self.assertEqual(result["state"], "UPLOADING")
        self.assertEqual(result["progress_percent"], 100)
        self.assertAlmostEqual(result["bytes_per_second"], 35.891 * 1024**2)
        self.assertIsNone(result["eta_seconds"])
        self.assertEqual(result["eta_scope"], "upload")
        self.assertEqual(result["progress_updated_unix_ms"], NOW - 1000)

    def test_final_upload_eta_dash_must_be_fresh_and_from_current_stage(self):
        for start, stamp in ((NOW - 300000, NOW - 121000), (NOW - 5000, NOW - 10000),
                             (NOW - 5000, NOW + 31000)):
            with self.subTest(stage_start=start, line_time=stamp):
                self.set_state("UPLOADING", start)
                self.log([self.rclone_time(stamp - 1000) +
                          " INFO : 117.171 GiB / 118.556 GiB, 99%, 91.935 MiB/s, ETA 15s",
                          self.rclone_time(stamp) +
                          " INFO : 118.578 GiB / 118.556 GiB, 100%, 35.891 MiB/s, ETA -"],
                         "UPLOADING", NOW)
                result = self.collect()
                for key in ("progress_percent", "bytes_per_second", "eta_seconds", "progress_updated_unix_ms"):
                    self.assertIsNone(result[key])

    def test_complete_transfer_and_zero_scan_eta_do_not_claim_stage_completion(self):
        self.set_state("DOWNLOADING")
        self.log(["[#d210fe 746GiB/746GiB(100%) CN:1 DL:368MiB ETA:0s]"], "DOWNLOADING")
        result = self.collect()
        self.assertEqual(result["state"], "DOWNLOADING")
        self.assertEqual(result["progress_percent"], 100)
        self.assertIsNone(result["eta_seconds"])
        self.assertIsNone(result["bytes_per_second"])
        self.set_state("REGISTRIES")
        self.log([self.registry_line(percent="99.9", eta="0s")])
        result = self.collect()
        self.assertEqual(result["state"], "REGISTRIES")
        self.assertIsNone(result["eta_seconds"])

    def test_actual_checksum_lines_cannot_renew_the_old_transfer_eta(self):
        self.set_state("DOWNLOADING")
        self.log(["[#d210fe 745GiB/746GiB(99%) CN:8 DL:368MiB ETA:1s]",
                  "[#d210fe 746GiB/746GiB(100%) CN:0] [Checksum:#d210fe 734GiB/746GiB(98%)]"],
                 "DOWNLOADING", NOW - 1000)
        result = self.collect()
        self.assertEqual(result["state"], "CHECKING_CAR_HASH")
        self.assertEqual(result["progress_percent"], 98)
        self.assertIsNone(result["eta_seconds"])
        self.assertIsNone(result["bytes_per_second"])
        self.assertEqual(result["progress_updated_unix_ms"], NOW - 1000)

    def test_paused_unknown_and_inactive_jobs_hide_running_eta(self):
        self.history(1049, [120, 25, 5, 280, 90])
        self.log([self.registry_line()])
        priority = self.root / "reports/live-priority-20261012/STATUS.json"
        for value in [{"state": "BACKGROUND_PAUSED", "updated_unix_ms": NOW},
                      {"state": "BACKGROUND_RUNNING", "updated_unix_ms": NOW - 121000}]:
            with self.subTest(priority=value):
                self.write_json(priority, value)
                result = self.collect()
                for key in ("eta_seconds", "epoch_eta_min_seconds", "queue_eta_max_seconds", "progress_percent"):
                    self.assertIsNone(result[key])
        self.write_json(priority, {"state": "BACKGROUND_RUNNING", "updated_unix_ms": NOW})
        for active in ("inactive", "failed", "unknown"):
            result = self.collect(active)
            self.assertEqual(result["state"], "STOPPED_REGISTRIES")
            self.assertIsNone(result["eta_seconds"])
            self.assertIsNone(result["epoch_eta_min_seconds"])
        self.set_state("COMPLETE")
        self.assertEqual(self.collect("inactive")["state"], "COMPLETE")

    def test_completed_receipts_and_history_produce_bounded_work_estimates(self):
        self.history(1049, [120, 25, 5, 280, 90], 116032745925)
        self.history(1050, [100, 20, 5, 300, 100], 127286509165)
        self.log([self.registry_line()])
        result = self.collect()
        self.assertEqual(result["completed_epochs"], [1049, 1050])
        self.assertEqual(result["upload_queue_epochs"], [1051, 1052])
        self.assertEqual(result["last_completed_epoch"], 1050)
        self.assertEqual(result["last_compact_bytes"], 127286509165)
        self.assertEqual((result["epoch_eta_min_seconds"], result["epoch_eta_max_seconds"]), (567, 860))
        self.assertEqual((result["queue_eta_min_seconds"], result["queue_eta_max_seconds"]), (988, 1520))
        # A fresh collection of an old stage cannot renew estimates once progress stops.
        self.log([self.registry_line(NOW - 121000)], at_ms=NOW)
        self.set_state("REGISTRIES", NOW - 300000)
        self.assertIsNone(self.collect()["queue_eta_min_seconds"])

    def test_invalid_receipt_identity_profile_and_numbers_are_rejected(self):
        receipt = self.job / "upload-state/epoch-1050/complete.json"
        good = {"schema": "blockzilla-stored-compact-backup-v1", "epoch": 1050,
                "upload_profile": "query-with-signatures", "bytes": 127286509165,
                "completed_unix_seconds": NOW // 1000 - 1000, "remote_file_sizes_verified": True}
        for changes in ({"schema": "partial-copy-v1"}, {"remote_file_sizes_verified": False},
                        {"remote_file_sizes_verified": 1}, {"epoch": 1051},
                        {"upload_profile": "all-files"}, {"bytes": True},
                        {"bytes": -1}, {"completed_unix_seconds": 1.5}):
            with self.subTest(changes=changes):
                self.write_json(receipt, {**good, **changes})
                with self.assertRaises(ValueError):
                    self.collect()

    def test_plan_and_files_are_bounded_and_symlinks_are_rejected(self):
        plan = self.job / "PLAN.json"
        for epochs in ([], [{"epoch": i, "bytes": 1} for i in range(33)],
                       [{"epoch": 1051, "bytes": 0}], [{"epoch": 1051, "bytes": True}],
                       [{"epoch": 1051, "bytes": 1}, {"epoch": 1051, "bytes": 2}]):
            with self.subTest(epochs=epochs):
                self.write_json(plan, {"epochs": epochs})
                with self.assertRaises(ValueError):
                    self.collect()
        plan.write_bytes(b"x" * (archive_status.LIMIT + 1))
        with self.assertRaises(ValueError):
            self.collect()
        target = self.root / "other.json"
        self.write_json(target, {"epochs": [{"epoch": 1051, "bytes": 1}]})
        plan.unlink()
        plan.symlink_to(target)
        with self.assertRaises(ValueError):
            self.collect()

    def test_missing_plan_and_unreadable_upload_log_leave_optional_eta_unavailable(self):
        self.set_state("UPLOADING")
        with patch.object(archive_status, "tail", side_effect=PermissionError("private file cannot be read")):
            result = self.collect()
        self.assertIsNone(result["eta_seconds"])
        self.assertEqual(result["state"], "UPLOADING")
        (self.job / "PLAN.json").unlink()
        self.calls.clear()
        self.assertIsNone(self.collect())
        self.assertEqual(self.calls, [])

    def test_collection_preserves_bytes_and_modification_times(self):
        self.history(1049, [120, 25, 5, 280, 90])
        self.history(1050, [100, 20, 5, 300, 100])
        self.log([self.registry_line()])
        def inventory():
            return {str(path.relative_to(self.root)): (hashlib.sha256(path.read_bytes()).hexdigest(),
                    path.stat().st_mtime_ns) for path in self.root.rglob("*") if path.is_file()}
        before = inventory()
        self.assertIsNotNone(self.collect()["epoch_eta_min_seconds"])
        self.assertEqual(inventory(), before)


if __name__ == "__main__":
    unittest.main()
