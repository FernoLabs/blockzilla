"""Saved-run comparison checks. No reader or NAS process is started."""
import contextlib
import copy
import io
import json
from pathlib import Path
import tempfile
import unittest

import compare_archive_sample_runs as compare


class ComparisonTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.root = Path(self.folder.name)
        self.v2, self.v3, self.current = [self.root / name for name in ("v2", "v3", "current")]
        self.epochs = (0,)
        self.workloads = ("slot-hours", "usdc")
        self.scope = [(fmt, "local", 0, workload) for fmt in compare.FORMATS for workload in self.workloads]
        for root, formats in ((self.v2, ("compact-v2",)), (self.v3, ("indexer-v3",)), (self.current, compare.FORMATS)):
            root.mkdir()
            jobs = [dict(format=fmt, mode=mode, epoch=epoch, workload=workload)
                    for fmt, mode, epoch, workload in self.scope if fmt in formats]
            self.save(root / "plan.json", jobs)
            self.save(root / "status.json", dict(state="PASS", completed=len(jobs), total=len(jobs)))
            self.save(root / "run.json", dict(threads=12, wallet="fixed-wallet"))
            self.save(root / "host.json", dict(host="NAS", logical_cpus=12))
            self.save(root / "inventory.json", [dict(format=fmt, epoch=0, source="local", object="data", size_bytes=123, mtime_ns=1000) for fmt in formats])
            for job in jobs:
                identity = compare.key(job)
                result_path = compare.job_path(root, identity)
                attempt = result_path.parent / "attempt-001"
                attempt.mkdir(parents=True)
                result = dict(job, status="PASS", exit_code=0, blocks="2", transactions="3",
                              total_s="10", scan_s="9", wall_s="11", attempt=str(attempt))
                if job["workload"] == "slot-hours":
                    result.update(recorded_inner_instructions="5", buckets=[
                        dict(approximate_hour="0", start_slot="0", end_slot_exclusive="9000", blocks="1", transactions="1", recorded_inner_instructions="2"),
                        dict(approximate_hour="1", start_slot="9000", end_slot_exclusive="18000", blocks="1", transactions="2", recorded_inner_instructions="3")])
                else:
                    output = attempt / "output.bin"
                    output.write_bytes(b"1234")
                    result.update(output_path=str(output), output_schema="usdc/2", output_rows="1", output_bytes="4",
                                  output_complete="false", indeterminate_transactions="1", coverage_sha256="a" * 64)
                self.save(result_path, result)
        self.identity = ("compact-v2", "local", 0, "usdc")

    @staticmethod
    def save(path, data):
        path.write_text(json.dumps(data))

    def edit(self, root, identity=None, **fields):
        path = compare.job_path(root, identity or self.identity)
        result = json.loads(path.read_text())
        result.update(fields)
        self.save(path, result)
        return result

    def run_compare(self, **kwargs):
        return compare.compare_runs(self.v2, self.v3, self.current, self.epochs, self.workloads, **kwargs)

    def selected_row(self, report, identity=None):
        selected = identity or self.identity
        return next(row for row in report["rows"] if compare.key(row) == selected)

    def test_complete_equal_run_passes_with_equal_incomplete_source_coverage(self):
        report = self.run_compare()
        self.assertEqual(report["state"], "PASS")
        self.assertEqual(report["job_states"], {"PASS": 4})
        self.assertEqual(self.selected_row(report)["correctness"], "MATCH")
        self.assertIn("not content identity proof", report["source_identity_policy"])

    def test_unrelated_baseline_car_failure_does_not_reject_complete_v3_rows(self):
        self.save(self.v3 / "status.json", dict(state="FAIL", completed=49, total=88, current="car/local/epoch-100/slot-hours"))
        self.assertEqual(self.run_compare()["state"], "PASS")

    def test_missing_and_failed_current_rows_are_incomplete(self):
        path = compare.job_path(self.current, self.identity)
        original = path.read_text()
        path.unlink()
        self.assertEqual(self.selected_row(self.run_compare())["state"], "INCOMPLETE_CURRENT")
        path.write_text(original)
        self.edit(self.current, status="FAIL", exit_code=1)
        self.assertEqual(self.selected_row(self.run_compare())["state"], "INCOMPLETE_CURRENT")

    def test_missing_baseline_is_distinct_from_correctness_mismatch(self):
        compare.job_path(self.v2, self.identity).unlink()
        row = self.selected_row(self.run_compare())
        self.assertEqual(row["state"], "MISSING_BASELINE")
        self.assertEqual(row["correctness"], "UNVERIFIED")

    def test_same_length_output_corruption_is_detected(self):
        result = self.edit(self.current)
        Path(result["output_path"]).write_bytes(b"4321")
        row = self.selected_row(self.run_compare())
        self.assertEqual(row["state"], "CORRECTNESS_MISMATCH")
        self.assertIn("different output_bytes_content", row["issues"])

    def test_source_counts_and_each_bucket_are_checked(self):
        identity = ("compact-v2", "local", 0, "slot-hours")
        result = self.edit(self.current, identity)
        result["buckets"][0]["transactions"], result["buckets"][1]["transactions"] = "2", "1"
        self.edit(self.current, identity, buckets=result["buckets"])
        row = self.selected_row(self.run_compare(), identity)
        self.assertEqual(row["state"], "CORRECTNESS_MISMATCH")
        self.assertIn("different buckets", row["issues"])
        self.edit(self.current, identity, transactions="4")
        self.assertEqual(self.selected_row(self.run_compare(), identity)["state"], "INCOMPLETE_CURRENT")

    def test_schema_rows_and_coverage_mismatch_are_not_timing_flags(self):
        for field, value in (("output_schema", "usdc/1"), ("output_rows", "2"),
                             ("output_complete", "true"), ("coverage_sha256", "b" * 64)):
            with self.subTest(field=field):
                result = self.edit(self.current)
                old = result[field]
                self.edit(self.current, **{field: value})
                self.assertEqual(self.selected_row(self.run_compare())["state"], "CORRECTNESS_MISMATCH")
                self.edit(self.current, **{field: old})

    def test_unavailable_absolute_output_does_not_pass_on_reported_metrics(self):
        self.edit(self.current, output_path="/unavailable-nas-result/output.bin")
        row = self.selected_row(self.run_compare())
        self.assertEqual(row["state"], "INCOMPLETE_CURRENT")
        self.assertTrue(any("unavailable" in issue for issue in row["issues"]))

    def test_size_alone_missing_mtime_and_changed_input_are_incomparable(self):
        inventory = json.loads((self.current / "inventory.json").read_text())
        for changed in ({"mtime_ns": 1001}, {"size_bytes": 124}, {"object": "replacement"}):
            with self.subTest(changed=changed):
                modified = copy.deepcopy(inventory)
                modified[0].update(changed)
                self.save(self.current / "inventory.json", modified)
                self.assertEqual(self.selected_row(self.run_compare())["state"], "INCOMPARABLE")
        del inventory[0]["mtime_ns"]
        self.save(self.current / "inventory.json", inventory)
        self.assertEqual(self.selected_row(self.run_compare())["state"], "INCOMPARABLE")

    def test_changed_host_or_thread_count_is_incomparable(self):
        self.save(self.current / "run.json", dict(threads=6, wallet="fixed-wallet"))
        self.assertEqual(self.selected_row(self.run_compare())["state"], "INCOMPARABLE")
        self.save(self.current / "run.json", dict(threads=12, wallet="fixed-wallet"))
        self.save(self.current / "host.json", dict(host="different-NAS", logical_cpus=12))
        self.assertEqual(self.selected_row(self.run_compare())["state"], "INCOMPARABLE")

    def test_invalid_inventory_and_host_metadata_are_reported_without_crashing(self):
        self.save(self.current / "inventory.json", [None])
        self.save(self.current / "host.json", [])
        self.save(self.current / "run.json", 1)
        row = self.selected_row(self.run_compare())
        self.assertEqual(row["state"], "INCOMPARABLE")
        self.assertEqual(row["correctness"], "MATCH")

    def test_equal_output_with_changed_workload_source_totals_is_mismatch(self):
        self.edit(self.current, transactions="2")
        row = self.selected_row(self.run_compare())
        self.assertEqual(row["state"], "CORRECTNESS_MISMATCH")
        self.assertIn("different transactions", row["issues"])

    def test_threshold_is_configurable_and_not_a_correctness_failure(self):
        self.edit(self.current, total_s="11.1")
        row = self.selected_row(self.run_compare())
        self.assertEqual(row["state"], "PERFORMANCE_FLAG")
        self.assertEqual(row["correctness"], "MATCH")
        self.assertAlmostEqual(row["timing"]["total_s"]["ratio"], 1.11)
        self.assertEqual(self.run_compare(threshold=12)["state"], "PASS")
        self.edit(self.current, total_s="11")
        self.assertEqual(self.run_compare()["state"], "PASS")

    def test_invalid_or_missing_timing_never_passes(self):
        for value in ("nan", "inf", "-1", "0", None):
            with self.subTest(value=value):
                self.edit(self.current, total_s=value)
                self.assertEqual(self.selected_row(self.run_compare())["state"], "INCOMPARABLE")

    def test_empty_duplicate_and_partial_plan_never_pass(self):
        original = json.loads((self.current / "plan.json").read_text())
        for plan in ([], original[:-1], original + [original[0]]):
            with self.subTest(count=len(plan)):
                self.save(self.current / "plan.json", plan)
                report = self.run_compare()
                self.assertEqual(report["state"], "NEEDS_ATTENTION")
                self.assertTrue(report["run_errors"])
        self.assertEqual(compare.compare_runs(self.v2, self.v3, self.current, (), self.workloads)["state"], "NEEDS_ATTENTION")

    def test_running_state_and_wrong_completion_counters_never_pass(self):
        for status in (dict(state="RUNNING", completed=3, total=4), dict(state="PASS", completed=3, total=4)):
            self.save(self.current / "status.json", status)
            self.assertEqual(self.run_compare()["state"], "NEEDS_ATTENTION")

    def test_resource_metrics_are_sampled_and_missing_is_not_zero(self):
        result = self.edit(self.current)
        samples = [dict(time=10, cpu_seconds=1, process_read_bytes=100, rss_bytes=500),
                   dict(time=12, cpu_seconds=3, process_read_bytes=2000100, rss_bytes=400)]
        (Path(result["attempt"]) / "resources.jsonl").write_text("\n".join(json.dumps(row) for row in samples))
        row = self.selected_row(self.run_compare())
        self.assertEqual(row["current_resources"]["sampled_max_rss_bytes"], 500)
        self.assertEqual(row["current_resources"]["sampled_cpu_percent"], 100)
        self.assertEqual(row["current_resources"]["sampled_storage_read_mb_s"], 1)
        self.assertNotIn("sampled_cpu_percent", row["baseline_resources"])

    def test_cli_reports_and_refuses_to_overwrite_existing_evidence(self):
        output = self.root / "report"
        argv = ["--baseline-v2", str(self.v2), "--baseline-v3", str(self.v3), "--current", str(self.current),
                "--output-dir", str(output), "--epochs", "0", "--workloads", "slot-hours,usdc"]
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(compare.main(argv), 0)
        before = {path.name: path.read_bytes() for path in output.iterdir()}
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as stopped:
            compare.main(argv)
        self.assertEqual(stopped.exception.code, 2)
        self.assertEqual(before, {path.name: path.read_bytes() for path in output.iterdir()})
        self.assertEqual(set(before), {"comparison.json", "comparison.tsv", "README.md"})


if __name__ == "__main__":
    unittest.main()
