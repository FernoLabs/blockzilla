import importlib.util
from pathlib import Path
import unittest


SCRIPT = Path(__file__).parents[1] / "scripts" / "publish-block-time-gap-backfill.py"
SPEC = importlib.util.spec_from_file_location("block_time_gap_backfill_publisher", SCRIPT)
publisher = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(publisher)


def private_status(**overrides):
    value = {
        "schema_version": 1,
        "state": "running",
        "started_unix_seconds": 1_000,
        "updated_unix_seconds": 1_100,
        "backfill": {
            "epochs_done": 265,
            "epochs_total": 526,
            "workers_configured": 2,
            "active_workers": 2,
            "overall_source_bytes_done": 2_500,
            "source_bytes_total": 10_000,
            "wall_throughput_bytes_per_second": 120.5,
            "eta_seconds": 63,
            "eta_reliable": True,
        },
        "current": {"epoch": 289, "progress_state": "running"},
        "resources": {"paused_seconds": 182},
        "last_error": None,
    }
    value.update(overrides)
    return value


class BlockTimeGapBackfillPublisherTests(unittest.TestCase):
    def test_publishes_stopped_state_and_bounded_worker_counts(self):
        public = publisher.public_status(private_status(state="stopped"))

        self.assertEqual(public["state"], "stopped")
        self.assertEqual(public["backfill"]["workers_configured"], 2)
        self.assertEqual(public["backfill"]["active_workers"], 2)

    def test_rejects_more_active_workers_than_configured(self):
        value = private_status()
        value["backfill"] = {**value["backfill"], "active_workers": 3}

        with self.assertRaisesRegex(ValueError, "active_workers exceeds workers_configured"):
            publisher.public_status(value)


if __name__ == "__main__":
    unittest.main()
