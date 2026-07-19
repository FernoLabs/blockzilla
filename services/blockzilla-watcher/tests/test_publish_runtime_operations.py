import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


SCRIPT = Path(__file__).parents[1] / "scripts" / "publish-runtime-operations.py"
SPEC = importlib.util.spec_from_file_location("runtime_operations_publisher", SCRIPT)
publisher = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(publisher)


def sample(pid, args, **overrides):
    values = {
        "pid": pid,
        "ppid": 1,
        "start_ticks": pid * 10,
        "start_unix_secs": 1_000,
        "name": Path(args[0]).name,
        "user": "operator",
        "args": tuple(args),
        "rchar": 0,
        "read_bytes": 0,
        "write_bytes": 0,
        "cpu_ticks": 10,
        "rss_bytes": 4096,
    }
    values.update(overrides)
    return publisher.ProcessSample(**values)


class RuntimeOperationsPublisherTests(unittest.TestCase):
    def test_checksum_job_uses_open_car_part_and_process_counters(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            part = root / "epoch-1001.car.part"
            part.write_bytes(b"\0" * 1_000)
            fd_root = root / "proc" / "43" / "fd"
            fd_root.mkdir(parents=True)
            (fd_root / "3").symlink_to(part)
            current = sample(
                43,
                ["sha256sum", "--check", "--strict", "-"],
                rchar=600,
                read_bytes=600,
            )
            previous = sample(
                43,
                current.args,
                rchar=200,
                read_bytes=200,
            )
            jobs = publisher.checksum_jobs(
                root / "proc", {43: current}, {43: previous}, 2, 2_000
            )
            self.assertEqual(len(jobs), 1)
            self.assertEqual(jobs[0]["kind"], "car_verify")
            self.assertEqual(jobs[0]["epoch"], 1001)
            self.assertEqual(jobs[0]["bytes_done"], 600)
            self.assertEqual(jobs[0]["progress_pct"], 60)
            self.assertEqual(jobs[0]["eta_secs"], 2)

    def test_raw_capture_status_contains_no_command_line_or_endpoint(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            output = root / "raw"
            output.mkdir()
            journal = output / "raw-blocks.jsonl"
            journal.write_text(
                json.dumps({
                    "schema_version": 1,
                    "frame_id": 9,
                    "slot": 433_719_121,
                    "epoch": 1003,
                }) + "\n",
                encoding="utf-8",
            )
            current = sample(
                44,
                [
                    "/bin/blockzilla-live-producer",
                    "record-grpc-raw",
                    "--endpoint",
                    "https://secret.example",
                    "--output-dir",
                    str(output),
                ],
                write_bytes=10 * publisher.MIB,
            )
            previous = sample(44, current.args, write_bytes=2 * publisher.MIB)
            status = publisher.build_status(
                root / "proc", {44: current}, {44: previous}, 2, 0, int(journal.stat().st_mtime)
            )
            capture = status["live_capture"]
            self.assertEqual(capture["state"], "capturing")
            self.assertEqual(capture["epoch"], 1003)
            self.assertEqual(capture["last_slot"], 433_719_121)
            self.assertEqual(capture["write_mib_per_sec"], 4)
            serialized = json.dumps(status)
            self.assertNotIn("secret.example", serialized)
            self.assertNotIn(str(output), serialized)

    def test_process_output_is_bounded_and_hides_private_commands(self):
        current = sample(
            45,
            ["sha256sum", "--check", "secret-path"],
            read_bytes=20 * publisher.MIB,
            cpu_ticks=30,
        )
        previous = sample(
            45,
            current.args,
            read_bytes=10 * publisher.MIB,
            cpu_ticks=10,
        )
        result = publisher.process_io_status(
            {45: current}, {45: previous}, 2, 0, 2_000
        )
        self.assertEqual(result["state"], "ready")
        self.assertEqual(result["processes"][0]["name"], "sha256sum")
        self.assertEqual(result["processes"][0]["read_mib_per_sec"], 5)
        self.assertNotIn("secret-path", json.dumps(result))


if __name__ == "__main__":
    unittest.main()
