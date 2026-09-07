"""Runner checks only. These do not add tests to the beginner examples."""
import contextlib
import io
import json
from pathlib import Path
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import archive_sample_matrix as matrix


STUB = r'''#!/usr/bin/env python3
import os, pathlib, sys
args = dict(zip(sys.argv[1::2], sys.argv[2::2]))
name = pathlib.Path(sys.argv[0]).name
fmt = 'compact-v2' if 'compact-v2' in name else 'indexer-v3' if 'archive-v3' in name else 'car'
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
    print(base + f' output_schema=example output_rows=1 output_bytes=9 output_complete=true {coverage} coverage_sha256=e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855')
print('progress=scan blocks=10 transactions=20 tps=10 blocks_s=5 eta_s=0', file=sys.stderr)
'''


class RunnerTests(unittest.TestCase):
    def test_local_car_inventory_selects_raw_then_streamed_zstd(self):
        with tempfile.TemporaryDirectory() as folder:
            archive = Path(folder)
            for epoch in (100, 300):
                root = archive / "car" / str(epoch)
                root.mkdir(parents=True)
                (root / "epoch-{}.car.zst".format(epoch)).write_bytes(b"compressed")
                (root / "epoch-{}-slot-ranges.raw".format(epoch)).write_bytes(b"index")
            (archive / "car/300/epoch-300.car").write_bytes(b"raw")
            args = SimpleNamespace(archive_root=archive, mode="local", formats=["car"], epochs=[100, 300])
            rows = matrix.inventory(args)
            self.assertEqual([r["object"] for r in rows], ["epoch-100.car.zst", "epoch-100-slot-ranges.raw",
                                                           "epoch-300.car", "epoch-300-slot-ranges.raw"])
            self.assertTrue(all(r["inode"] and r["resolved_path"] for r in rows))
            with self.assertRaisesRegex(ValueError, "missing raw or zstd CAR"):
                matrix.local_object_names(archive, "car", 200)

    def test_mixed_car_encodings_keep_index_and_same_object_size_checks(self):
        rows = [dict(format="car", epoch=100, object="epoch-100.car.zst", source="local", size_bytes=10),
                dict(format="car", epoch=100, object="epoch-100.car", source="network", size_bytes=100),
                dict(format="car", epoch=100, object="epoch-100-slot-ranges.raw", source="local", size_bytes=20),
                dict(format="car", epoch=100, object="epoch-100-slot-ranges.raw", source="network", size_bytes=20)]
        self.assertEqual(matrix.size_mismatches(rows), [])
        rows[-1]["size_bytes"] = 21
        self.assertEqual([r["object"] for r in matrix.size_mismatches(rows)], ["epoch-100-slot-ranges.raw"])
        rows[0].update(object="epoch-100.car")
        self.assertEqual(len(matrix.size_mismatches(rows)), 2)

    def test_network_car_preflight_keeps_raw_route_and_rejects_weak_identity(self):
        seen = []
        class Response:
            status = 200
            headers = {"Content-Length": "123", "ETag": '"identity"'}
            def __init__(self, url): self.url = url
            def geturl(self): return self.url
            def __enter__(self): return self
            def __exit__(self, *_): pass
        def reply(request, timeout):
            seen.append(request.full_url)
            return Response(request.full_url)
        args = SimpleNamespace(archive_root=None, mode="network", origin="https://example.invalid",
                               formats=["car"], epochs=[100])
        with patch.object(matrix.urllib.request, "urlopen", side_effect=reply):
            rows = matrix.inventory(args)
            self.assertEqual({r["object"] for r in rows}, {"epoch-100.car", "epoch-100-slot-ranges.raw"})
            self.assertTrue(all(not url.endswith(".zst") for url in seen))
            Response.headers = {"Content-Length": "123", "ETag": 'W/"identity"'}
            with self.assertRaisesRegex(ValueError, "strong ETag"):
                matrix.inventory(args)

    def test_results_cannot_be_inside_archive_or_binary_tree_even_through_symlink(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            archive, bins = root / "archive", root / "bin"
            archive.mkdir()
            bins.mkdir()
            alias = root / "alias"
            alias.symlink_to(archive, target_is_directory=True)
            for results in (archive, archive / "results", bins / "results", alias / "results"):
                command = ["runner", "--mode", "local", "--archive-root", str(archive),
                           "--bin-dir", str(bins), "--results-root", str(results)]
                with patch.object(sys, "argv", command), patch.object(matrix, "inventory") as inventory, \
                        contextlib.redirect_stderr(io.StringIO()):
                    with self.assertRaises(SystemExit):
                        matrix.main()
                    inventory.assert_not_called()
            self.assertFalse((archive / "results").exists())

    def test_resume_rejects_same_size_output_damage_before_reader_restart(self):
        with tempfile.TemporaryDirectory() as folder, contextlib.redirect_stdout(io.StringIO()):
            root = Path(folder)
            bins = root / "bin"
            bins.mkdir()
            job = dict(format="compact-v2", mode="network", epoch=0, workload="usdc", binary="read-compact-v2-usdc")
            reader = bins / job["binary"]
            reader.write_text(STUB)
            reader.chmod(0o755)
            args = SimpleNamespace(bin_dir=bins, results_root=root / "results", archive_root=None,
                                   origin=matrix.ORIGIN, threads=12, interval=1, wallet=matrix.WALLET)
            result = matrix.run_one(args, job, {("network", "compact-v2", 0): 100}, [])
            Path(result["output_path"]).write_bytes(b"corrupted")
            with self.assertRaisesRegex(ValueError, "missing or changed"):
                matrix.run_one(args, job, {("network", "compact-v2", 0): 100}, [])
            self.assertEqual(len(list((root / "results").glob("jobs/**/attempt-*"))), 1)

    def test_interrupt_joins_reader_and_preserves_partial_output(self):
        with tempfile.TemporaryDirectory() as folder, contextlib.redirect_stdout(io.StringIO()):
            root = Path(folder).resolve()
            bins = root / "bin"
            bins.mkdir()
            job = dict(format="compact-v2", mode="network", epoch=0, workload="usdc", binary="read-compact-v2-usdc")
            reader = bins / job["binary"]
            reader.write_text(STUB + "\nimport time\nwhile True: time.sleep(1)\n")
            reader.chmod(0o755)
            args = SimpleNamespace(bin_dir=bins, results_root=root / "results", archive_root=None,
                                   origin=matrix.ORIGIN, threads=12, interval=1, wallet=matrix.WALLET)
            processes = []
            real_popen = matrix.subprocess.Popen
            def start(*args, **kwargs):
                process = real_popen(*args, **kwargs)
                processes.append(process)
                return process
            def interrupt(pid):
                deadline = time.monotonic() + 5
                while not list((root / "results").glob("jobs/**/output.bin")) and time.monotonic() < deadline:
                    time.sleep(0.01)
                raise KeyboardInterrupt()
            with patch.object(matrix.subprocess, "Popen", side_effect=start), \
                    patch.object(matrix, "sample_process", side_effect=interrupt):
                with self.assertRaises(KeyboardInterrupt):
                    matrix.run_one(args, job, {("network", "compact-v2", 0): 100}, [])
            self.assertEqual(len(processes), 1)
            self.assertIsNotNone(processes[0].poll())
            result = json.loads(next((root / "results").glob("jobs/**/result.json")).read_text())
            self.assertEqual(result["status"], "FAIL")
            self.assertEqual((Path(result["attempt"]) / "output.bin").read_bytes(), b"canonical")

    def test_preflight_error_and_changed_source_have_failure_status(self):
        with tempfile.TemporaryDirectory() as folder, contextlib.redirect_stdout(io.StringIO()):
            root = Path(folder)
            bins = root / "bin"
            bins.mkdir()
            reader = bins / "read-car"
            reader.write_text(STUB)
            reader.chmod(0o755)
            command = ["runner", "--mode", "local", "--archive-root", str(root / "archive"),
                       "--bin-dir", str(bins), "--results-root", str(root / "results"),
                       "--formats", "car", "--epochs", "300", "--workloads", "slot-hours"]
            with patch.object(sys, "argv", command), patch.object(matrix, "inventory", side_effect=OSError("missing source")):
                with self.assertRaises(OSError):
                    matrix.main()
            self.assertEqual(json.loads((root / "results/status.json").read_text())["state"], "PREFLIGHT_FAILED")
            before = [dict(format="car", epoch=300, object="epoch-300.car", source="local", size_bytes=100)]
            after = [dict(before[0], size_bytes=101)]
            with patch.object(sys, "argv", command), patch.object(matrix, "inventory", side_effect=[before, after]):
                with self.assertRaisesRegex(ValueError, "changed during"):
                    matrix.main()
            self.assertEqual(json.loads((root / "results/status.json").read_text())["state"], "FAIL")
            self.assertEqual(json.loads((root / "results/inventory.json").read_text()), before)
            self.assertEqual(json.loads((root / "results/inventory-after.json").read_text()), after)

    def test_car_reader_must_bind_the_selected_preflight_size(self):
        with tempfile.TemporaryDirectory() as folder, contextlib.redirect_stdout(io.StringIO()):
            root = Path(folder)
            bins = root / "bin"
            bins.mkdir()
            reader = bins / "read-car"
            reader.write_text(STUB)
            reader.chmod(0o755)
            args = SimpleNamespace(bin_dir=bins, results_root=root / "results", archive_root=root / "archive",
                                   origin=matrix.ORIGIN, interval=1)
            job = dict(format="car", mode="local", epoch=300, workload="slot-hours", binary="read-car")
            result = matrix.run_one(args, job, {("local", "car", 300): 101}, [])
            self.assertEqual(result["status"], "FAIL")
            self.assertIn("different payload/index size", result["error"])
            args.car_sources = {("local", 300): "epoch-300.car.zst"}
            compressed = matrix.run_one(args, job, {("local", "car", 300): 100}, [])
            self.assertEqual(compressed["status"], "PASS")
            self.assertEqual(compressed["scan_source_bytes"], "20")
            self.assertEqual(compressed["source_encoding"], "zstd")
            self.assertIsNone(compressed["scan_local_read_bytes"])
            self.assertIsNone(compressed["scan_local_read_mb_s"])
            self.assertEqual(matrix.run_one(args, job, {("local", "car", 300): 100}, []), compressed)

    def test_size_check_reports_all_mismatches(self):
        rows = [dict(format="indexer-v3", epoch=200, object="messages", source="local", size_bytes=10),
                dict(format="indexer-v3", epoch=200, object="messages", source="network", size_bytes=20),
                dict(format="indexer-v3", epoch=300, object="messages", source="local", size_bytes=30),
                dict(format="indexer-v3", epoch=300, object="messages", source="network", size_bytes=40)]
        self.assertEqual([r["local_size_bytes"] for r in matrix.size_mismatches(rows)], [10, 30])
    def test_preflight_uses_identified_head_requests(self):
        class Response:
            headers = {"Content-Length": "123", "ETag": '"test-etag"'}
            status = 200
            def __init__(self, url): self.url = url
            def geturl(self): return self.url
            def __enter__(self): return self
            def __exit__(self, *_): pass
        def reply(request, timeout):
            self.assertEqual(request.method, "HEAD")
            self.assertEqual(request.get_header("User-agent"), "blockzilla-sample-preflight/1")
            return Response(request.full_url)
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

    def test_v2_only_plan_and_inventory(self):
        args = SimpleNamespace(mode="local", workloads=matrix.WORKLOADS, formats=["compact-v2"])
        jobs = matrix.plan(args)
        self.assertEqual(len(jobs), 44)
        self.assertEqual({j["format"] for j in jobs}, {"compact-v2"})
        with tempfile.TemporaryDirectory() as folder:
            args.archive_root = Path(folder)
            args.epochs = [0]
            root = args.archive_root / "compact-v2" / "0"
            root.mkdir(parents=True)
            for name in matrix.FILES["compact-v2"]:
                (root / name).touch()
            rows = matrix.inventory(args)
            self.assertEqual(len(rows), len(matrix.FILES["compact-v2"]))
            self.assertEqual({r["format"] for r in rows}, {"compact-v2"})

    def test_car_count_only_keeps_nine_examples_in_order(self):
        jobs = matrix.plan(SimpleNamespace(mode="local", workloads=matrix.WORKLOADS, epochs=[300], car_count_only=True))
        self.assertEqual(len(jobs), 9)
        self.assertEqual([j["format"] for j in jobs], ["compact-v2"] * 4 + ["indexer-v3"] * 4 + ["car"])
        self.assertEqual(jobs[-1]["workload"], "slot-hours")

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

    def test_car_count_filter_reports_only_scheduled_workloads(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            bins = root / "bin"
            bins.mkdir()
            reader = bins / "read-car"
            reader.write_text(STUB)
            reader.chmod(0o755)
            command = ["runner", "--mode", "local", "--archive-root", str(root / "archive"),
                       "--bin-dir", str(bins), "--results-root", str(root / "results"),
                       "--formats", "car", "--epochs", "300", "--car-count-only"]
            objects = [dict(format="car", epoch=300, object="fixture", source="local", size_bytes=100)]
            with patch.object(sys, "argv", command), patch.object(matrix, "inventory", return_value=objects), \
                    contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(matrix.main(), 0)
            status = json.loads((root / "results/status.json").read_text())
            self.assertEqual((status["state"], status["completed"], status["total"]), ("PASS", 1, 1))
            self.assertEqual(len((root / "results/parity.tsv").read_text().splitlines()), 2)
            command += ["--workloads", "usdc"]
            with patch.object(sys, "argv", command), patch.object(matrix, "inventory") as inventory, \
                    contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    matrix.main()
                inventory.assert_not_called()

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
            command = ["runner", "--mode", "both", "--archive-root", str(root / "archive"),
                       "--bin-dir", str(bins), "--results-root", str(results)]
            with patch.object(sys, "argv", command), patch.object(matrix, "inventory", return_value=objects), \
                    contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(matrix.main(), 0)
                self.assertEqual(matrix.main(), 0)
            self.assertEqual(len(list(results.glob("jobs/**/attempt-*"))), 264)
            self.assertEqual(json.loads((results / "status.json").read_text())["state"], "PASS")
            self.assertEqual(len((results / "summary.tsv").read_text().splitlines()), 265)
            # The reduced comparison must finish cleanly without requiring CAR dumps.
            reduced = root / "reduced"
            reduced_command = command[:]
            reduced_command[reduced_command.index(str(results))] = str(reduced)
            reduced_command += ["--epochs", "300", "--car-count-only"]
            with patch.object(sys, "argv", reduced_command), patch.object(matrix, "inventory", return_value=objects), \
                    contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(matrix.main(), 0)
            self.assertEqual(json.loads((reduced / "status.json").read_text())["completed"], 18)
            # A changed binary must not re-use old PASS markers.
            path = bins / "read-car"
            path.write_text(STUB + "\n# different build\n")
            with patch.object(sys, "argv", command), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    matrix.main()


if __name__ == "__main__":
    unittest.main()
