"""Runner checks only. These do not add tests to the beginner examples."""
import contextlib
import io
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import archive_sample_matrix as matrix


STUB = r'''#!/usr/bin/env python3
import os, pathlib, sys
args = dict(zip(sys.argv[1::2], sys.argv[2::2]))
name = pathlib.Path(sys.argv[0]).name
fmt = 'compact-v2' if 'compact-v2' in name else 'indexer-v3' if 'indexer-v3' in name else 'car'
epoch = args['--epoch']
count = name == 'read-car' or name.endswith('slot-hours')
base = f'format={fmt} epoch={epoch} setup_s=1 scan_s=2 total_s=3 scan_tps=10 total_tps=6.667 bound_source_size_bytes=100 setup_network_bytes=5 scan_network_bytes=20 total_network_bytes=25 scan_network_mb_s=0.00001 total_network_mb_s=0.000008'
base += ' source_read_bytes=20 scan_source_mb_s=0.00001' if fmt == 'car' else ' scan_logical_read_bytes=20 scan_logical_read_mb_s=0.00001'
base += ' requested_blocks=10 requested_transactions=20 decoded_blocks=2 decoded_transactions=4' if fmt == 'indexer-v3' and not count else ' blocks=10 transactions=20'
if count:
    print(base + ' recorded_inner_instructions=5')
    print('approximate_hour=0 start_slot=0 end_slot_exclusive=9000 blocks=10 transactions=20 recorded_inner_instructions=5')
else:
    output = pathlib.Path(args['--output'])
    with output.open('xb') as f: f.write(b'canonical')
    coverage = 'coverage_indeterminate_transactions=0' if fmt == 'car' else 'indeterminate_transactions=0'
    print(base + f' output_schema=example output_rows=1 output_bytes=9 output_complete=true {coverage} coverage_sha256=abc')
print('progress=scan blocks=10 transactions=20 tps=10 blocks_s=5 eta_s=0', file=sys.stderr)
'''


class RunnerTests(unittest.TestCase):
    def test_size_check_reports_all_mismatches(self):
        rows = [dict(format="indexer-v3", epoch=200, object="messages", source="local", size_bytes=10),
                dict(format="indexer-v3", epoch=200, object="messages", source="network", size_bytes=20),
                dict(format="indexer-v3", epoch=300, object="messages", source="local", size_bytes=30),
                dict(format="indexer-v3", epoch=300, object="messages", source="network", size_bytes=40)]
        self.assertEqual([r["local_size_bytes"] for r in matrix.size_mismatches(rows)], [10, 30])
    def test_preflight_uses_identified_head_requests(self):
        class Response:
            headers = {"Content-Length": "123", "ETag": "test-etag"}
            def __enter__(self): return self
            def __exit__(self, *_): pass
        def reply(request, timeout):
            self.assertEqual(request.method, "HEAD")
            self.assertEqual(request.get_header("User-agent"), "blockzilla-sample-preflight/1")
            return Response()
        args = SimpleNamespace(archive_root=None, mode="network", origin="https://example.invalid")
        with patch.object(matrix.urllib.request, "urlopen", side_effect=reply):
            rows = matrix.inventory(args)
        self.assertTrue(rows)
        self.assertTrue(all(row["size_bytes"] == 123 for row in rows))

    def test_failed_retry_keeps_output_and_uses_new_cache(self):
        with tempfile.TemporaryDirectory() as folder, contextlib.redirect_stdout(io.StringIO()):
            root = Path(folder)
            bins = root / "bin"
            bins.mkdir()
            job = dict(format="compact-v2", mode="network", epoch=0, workload="usdc", binary="read-compact-v2-usdc")
            reader = bins / job["binary"]
            reader.write_text(STUB + "\nraise SystemExit(7)\n")
            reader.chmod(0o755)
            args = SimpleNamespace(bin_dir=bins, results_root=root / "results", archive_root=None,
                                   origin=matrix.ORIGIN, threads=12, interval=1, wallet=matrix.WALLET)
            sizes = {("network", "compact-v2", 0): 100}
            failed = matrix.run_one(args, job, sizes, [])
            self.assertEqual(failed["status"], "FAIL")
            self.assertEqual(failed["exit_code"], 7)
            reader.write_text(STUB)
            passed = matrix.run_one(args, job, sizes, [])
            self.assertEqual(passed["status"], "PASS")
            self.assertNotEqual(failed["attempt"], passed["attempt"])
            for result in (failed, passed):
                attempt = Path(result["attempt"])
                self.assertTrue((attempt / "output.bin").exists())
                self.assertIn(str(attempt / "cache"), json.loads((attempt / "command.json").read_text()))

    def test_order_and_scope(self):
        jobs = matrix.plan(SimpleNamespace(mode="both", workloads=matrix.WORKLOADS))
        self.assertEqual(len(jobs), 264)
        self.assertEqual(len({job["binary"] for job in jobs}), 12)
        self.assertEqual([jobs[i]["format"] for i in (0, 88, 176)], list(matrix.FORMATS))
        self.assertEqual(jobs[43]["mode"], "local")
        self.assertEqual(jobs[44]["mode"], "network")

    def test_epoch_300_comparison_keeps_format_order(self):
        jobs = matrix.plan(SimpleNamespace(mode="local", workloads=("slot-hours",), epochs=[300]))
        self.assertEqual([(job["format"], job["epoch"]) for job in jobs],
                         [(fmt, 300) for fmt in matrix.FORMATS])

    def test_incomplete_summary_is_not_success(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "stdout.log"
            path.write_text("format=car epoch=0 blocks=10\n")
            with self.assertRaises((KeyError, TypeError, ValueError)):
                matrix.parse_result(dict(format="car", epoch=0, workload="slot-hours"), path)

    def test_same_length_output_difference(self):
        with tempfile.TemporaryDirectory() as folder:
            a, b = Path(folder) / "a", Path(folder) / "b"
            a.write_bytes(b"one")
            b.write_bytes(b"two")
            self.assertFalse(matrix.same_bytes(a, b))

    def test_parity_checks_each_bucket(self):
        job = dict(epoch=0, workload="slot-hours")
        baseline = dict(job, status="PASS", buckets=[dict(blocks="1"), dict(blocks="2")])
        changed = dict(baseline, buckets=[dict(blocks="2"), dict(blocks="1")])
        self.assertEqual(matrix.check_parity(job, changed, [baseline]), "MISMATCH")

    def test_full_stub_matrix_and_resume(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            bins, results = root / "bin", root / "results"
            bins.mkdir()
            objects = []
            for fmt in matrix.FORMATS:
                for epoch in matrix.EPOCHS:
                    for mode in ("local", "network"):
                        objects.append(dict(format=fmt, epoch=epoch, object="fixture", source=mode, size_bytes=100))
                for workload in matrix.WORKLOADS:
                    path = bins / matrix.binary(fmt, workload)
                    path.write_text(STUB)
                    path.chmod(0o755)
            command = ["runner", "--mode", "both", "--archive-root", str(root),
                       "--bin-dir", str(bins), "--results-root", str(results)]
            with patch.object(sys, "argv", command), patch.object(matrix, "inventory", return_value=objects), \
                    contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(matrix.main(), 0)
                self.assertEqual(matrix.main(), 0)
            self.assertEqual(len(list(results.glob("jobs/**/attempt-*"))), 264)
            self.assertEqual(json.loads((results / "status.json").read_text())["state"], "PASS")
            self.assertEqual(len((results / "summary.tsv").read_text().splitlines()), 265)
            # A changed binary must not re-use old PASS markers.
            path = bins / "read-car"
            path.write_text(STUB + "\n# different build\n")
            with patch.object(sys, "argv", command), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    matrix.main()


if __name__ == "__main__":
    unittest.main()
