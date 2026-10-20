import contextlib
import importlib.util
import io
import json
from pathlib import Path
import stat
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock


SPEC = importlib.util.spec_from_file_location("capacity_guard_test", Path(__file__).with_name("capacity_guard.py"))
GUARD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GUARD)
SNAPSHOTS, HELPER_SHA = GUARD.load_snapshot_helper()
GIB = 1024**3
SOURCE = {
    "genesis_hash": "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d",
    "node_identity": "J7qpvCEWhdBgPdYRV3s3btWPrRVXa2qnAzgvxWTvtbrQ",
    "agave_revision": "87167f3906bdf5111e99ead64d8f4098b7874e33",
    "session_id": "000000000000000018df884b2474cf8e-20b258",
}


def disk(free_bytes):
    return SimpleNamespace(f_bavail=free_bytes // 4096, f_frsize=4096, f_blocks=7 * 1024**4 // 4096)


class CapacityGuardTest(unittest.TestCase):
    def fixture(self, directory):
        root = Path(directory).resolve()
        (root / "config").mkdir()
        (root / "config/native-active-source.json").write_text(json.dumps(SOURCE))
        (root / "snapshots").mkdir()
        (root / "snapshots" / ("snapshot-100-" + "A" * 32 + ".tar.zst")).write_bytes(b"preserve full")
        (root / "snapshots" / ("incremental-snapshot-100-200-" + "B" * 32 + ".tar.zst")).write_bytes(b"preserve incremental")
        (root / "snapshots/unknown.keep").write_bytes(b"preserve unknown")
        return root

    def collect(self, root, free=600 * GIB, **kwargs):
        with mock.patch.object(GUARD.os, "statvfs", return_value=disk(free)):
            return GUARD.collect(root, SNAPSHOTS, HELPER_SHA, GUARD.Forecast(), monotonic=0,
                                 now_ms=100000, **kwargs)

    def test_signed_snapshot_headroom_uses_existing_policy_and_preserves_inputs(self):
        with tempfile.TemporaryDirectory() as d:
            root = self.fixture(d)
            before = {str(p): p.read_bytes() for p in root.rglob("*") if p.is_file()}
            value = self.collect(root)
            self.assertEqual(value["schema"], "blockzilla-native-capacity-v1")
            self.assertEqual(value["state"], "RUNNING")
            self.assertEqual(value["capacity_state"], "WARNING")
            self.assertEqual(value["capture_reserve_bytes"], 400 * GIB)
            self.assertEqual(value["recovery_minimum_free_bytes"], 500 * GIB)
            self.assertEqual(value["next_archive_budget_bytes"], 128 * GIB)
            self.assertEqual(value["snapshot_headroom_bytes"], 72 * GIB)
            self.assertEqual(value["recovery_headroom_bytes"], -28 * GIB)
            self.assertFalse(value["snapshot_capture_reserve_at_risk"])
            self.assertTrue(value["snapshot_headroom_low"])
            self.assertFalse(value["forecast_ready"])
            self.assertFalse(value["forecast_is_guarantee"])
            self.assertEqual(value["service_operations"], [])
            self.assertEqual(value["deleted_files"], [])
            self.assertNotIn("snapshot-100-", json.dumps(value))
            self.assertEqual(before, {str(p): p.read_bytes() for p in root.rglob("*") if p.is_file()})

    def test_observed_large_archive_raises_budget_and_temporary_allocation_is_counted(self):
        with tempfile.TemporaryDirectory() as d:
            root = self.fixture(d)
            records = [
                SNAPSHOTS.SnapshotFile("full", "full", 100, None, 160 * GIB, 160 * GIB, 1, 1, 1),
                SNAPSHOTS.SnapshotFile("tmp", "active_temporary", None, None, 80 * GIB, 80 * GIB, 1, 2, 1),
            ]
            with mock.patch.object(SNAPSHOTS, "inventory_snapshots", return_value=records):
                value = self.collect(root, free=550 * GIB)
            self.assertEqual(value["next_archive_budget_bytes"], 160 * GIB)
            self.assertEqual(value["snapshot_archive_allocated_bytes"], 240 * GIB)
            self.assertEqual(value["active_temporary_archive_count"], 1)
            self.assertEqual(value["snapshot_headroom_bytes"], -10 * GIB)
            self.assertEqual(value["capacity_state"], "CRITICAL")

    def test_forecast_requires_three_samples_and_sixty_monotonic_seconds(self):
        forecast = GUARD.Forecast()
        self.assertFalse(forecast.observe("source", 0, 1000, 0, 100, 500)["forecast_ready"])
        self.assertFalse(forecast.observe("source", 30, 970, 0, 100, 500)["forecast_ready"])
        value = forecast.observe("source", 60, 940, 0, 100, 500)
        self.assertTrue(value["forecast_ready"])
        self.assertEqual(value["net_disk_growth_bytes_per_second"], 1)
        self.assertEqual(value["non_archive_growth_bytes_per_second"], 1)
        self.assertEqual(value["estimated_seconds_to_capture_reserve"], 500)
        self.assertEqual(value["estimated_seconds_to_snapshot_headroom_exhaustion"], 100)

    def test_snapshot_overlap_does_not_become_permanent_non_archive_growth(self):
        forecast = GUARD.Forecast()
        for now, free, archives in ((0, 1000, 100), (30, 900, 200), (60, 800, 300)):
            value = forecast.observe("source", now, free, archives, 50, 400)
        self.assertGreater(value["net_disk_growth_bytes_per_second"], 0)
        self.assertEqual(value["non_archive_growth_bytes_per_second"], 0)
        self.assertIsNone(value["estimated_seconds_to_snapshot_headroom_exhaustion"])
        # The current free-space budget still reports the actual occupied bytes.
        self.assertIsNotNone(value["estimated_seconds_to_capture_reserve"])

    def test_negative_growth_and_expired_or_changed_samples_have_no_false_eta(self):
        forecast = GUARD.Forecast()
        for now, free in ((0, 800), (30, 900), (60, 1000)):
            value = forecast.observe("source", now, free, 0, 50, 400)
        self.assertLess(value["net_disk_growth_bytes_per_second"], 0)
        self.assertIsNone(value["estimated_seconds_to_capture_reserve"])
        self.assertIsNone(value["estimated_seconds_to_snapshot_headroom_exhaustion"])
        self.assertFalse(forecast.observe("new source", 90, 100, 0, -10, 30)["forecast_ready"])
        self.assertFalse(forecast.observe("new source", 80, 100, 0, -10, 30)["forecast_ready"])
        self.assertFalse(forecast.observe("new source", 800, 100, 0, -10, 30)["forecast_ready"])
        self.assertEqual(len(forecast.samples), 1)

    def test_median_growth_ignores_one_intermediate_allocation_spike(self):
        forecast = GUARD.Forecast()
        for now, free in ((0, 1000), (30, 970), (60, 600), (90, 910), (120, 880)):
            value = forecast.observe("source", now, free, 0, 100, 500)
        self.assertEqual(value["net_disk_growth_bytes_per_second"], 1)
        self.assertEqual(value["non_archive_growth_bytes_per_second"], 1)

    def test_unavailable_or_changed_source_resets_forecast_and_does_not_reuse_success(self):
        with tempfile.TemporaryDirectory() as d:
            root = self.fixture(d)
            forecast = GUARD.Forecast()
            forecast.observe("old", 0, 1000, 0, 100, 500)
            (root / "config/native-active-source.json").write_text('{"private-value":"must-not-appear"}')
            value = GUARD.sample(root, SNAPSHOTS, HELPER_SHA, forecast, monotonic=60, now_ms=200000)
            self.assertEqual(value["state"], "FAILED")
            self.assertEqual(value["updated_unix_ms"], 200000)
            self.assertNotIn("free_disk_bytes", value)
            self.assertNotIn("must-not-appear", json.dumps(value))
            self.assertEqual(len(forecast.samples), 0)
            changed = dict(SOURCE, session_id="1" * 32 + "-7b")
            with mock.patch.object(GUARD, "read_source", side_effect=[SOURCE, changed]), \
                 mock.patch.object(GUARD.os, "statvfs", return_value=disk(600 * GIB)):
                value = GUARD.sample(root, SNAPSHOTS, HELPER_SHA, forecast, now_ms=300000)
            self.assertEqual(value["state"], "FAILED")
            self.assertEqual(len(forecast.samples), 0)

    def test_optional_runtime_health_is_independent_and_requires_matching_fresh_source(self):
        with tempfile.TemporaryDirectory() as d:
            root = self.fixture(d)
            report = root / "runtime.json"
            for healthy in (True, False):
                report.write_text(json.dumps(dict(source=SOURCE["session_id"], healthy=healthy, updated_unix_ms=99999)))
                value = self.collect(root, runtime_health_report=report)
                self.assertEqual(value["runtime_health"]["healthy"], healthy)
                self.assertEqual(value["state"], "RUNNING")
                self.assertEqual(value["snapshot_headroom_bytes"], 72 * GIB)
            for source, updated in (("wrong-source", 99999), (SOURCE["session_id"], 1)):
                report.write_text(json.dumps(dict(source=source, healthy=True, updated_unix_ms=updated)))
                value = self.collect(root, runtime_health_report=report)
                self.assertEqual(value["runtime_health"]["state"], "UNAVAILABLE")
                self.assertEqual(value["state"], "RUNNING")

    def test_source_symlink_and_different_snapshot_filesystem_fail_closed(self):
        with tempfile.TemporaryDirectory() as d:
            root = self.fixture(d)
            source = root / "config/native-active-source.json"
            other = source.with_name("other.json")
            source.rename(other); source.symlink_to(other)
            self.assertEqual(GUARD.sample(root, SNAPSHOTS, HELPER_SHA, GUARD.Forecast())["state"], "FAILED")
            source.unlink(); other.rename(source)
            original_stat = Path.stat
            def device(path, *args, **kwargs):
                if path == root: return SimpleNamespace(st_dev=1)
                if path == root / "snapshots": return SimpleNamespace(st_dev=2)
                return original_stat(path, *args, **kwargs)
            with mock.patch.object(Path, "stat", new=device):
                value = GUARD.sample(root, SNAPSHOTS, HELPER_SHA, GUARD.Forecast())
            self.assertEqual(value["state"], "FAILED")

    def test_once_writes_private_atomic_status_in_persistent_report_directory(self):
        with tempfile.TemporaryDirectory() as d:
            root = self.fixture(d)
            report = root / "reports/native-capacity"
            output = io.StringIO()
            previous_umask = GUARD.os.umask(0o077)
            self.addCleanup(GUARD.os.umask, previous_umask)
            with mock.patch.object(GUARD.os, "statvfs", return_value=disk(700 * GIB)), contextlib.redirect_stdout(output):
                result = GUARD.main(["--root", str(root), "--report", str(report), "--once"])
            value = json.loads(output.getvalue())
            self.assertEqual(result, 0)
            self.assertEqual(value["capacity_state"], "OK")
            self.assertEqual(json.loads((report / "STATUS.json").read_text()), value)
            self.assertEqual(stat.S_IMODE((report / "STATUS.json").stat().st_mode), 0o600)
            self.assertTrue(report.is_dir())
            self.assertEqual([p.name for p in report.iterdir()], ["STATUS.json"])

    def test_missing_helper_publishes_fixed_failure_and_enables_monitor_checks(self):
        with tempfile.TemporaryDirectory() as d:
            root = self.fixture(d)
            report = root / "reports/native-capacity"
            previous_umask = GUARD.os.umask(0o077)
            self.addCleanup(GUARD.os.umask, previous_umask)
            with mock.patch.object(GUARD, "load_snapshot_helper", side_effect=FileNotFoundError("private path")), \
                 contextlib.redirect_stdout(io.StringIO()):
                result = GUARD.main(["--root", str(root), "--report", str(report), "--once"])
            self.assertEqual(result, 1)
            self.assertTrue(report.is_dir())
            value = json.loads((report / "STATUS.json").read_text())
            self.assertEqual(value["schema"], GUARD.SCHEMA)
            self.assertEqual(value["state"], "FAILED")
            self.assertEqual(value["error"], "snapshot_helper_unavailable")
            self.assertNotIn("private path", json.dumps(value))


if __name__ == "__main__":
    unittest.main()
