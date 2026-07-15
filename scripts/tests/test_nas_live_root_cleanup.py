from __future__ import annotations

import argparse
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


SCRIPT = Path(__file__).resolve().parents[1] / "nas-live-root-cleanup.py"
SPEC = importlib.util.spec_from_file_location("nas_live_root_cleanup", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
cleanup = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = cleanup
SPEC.loader.exec_module(cleanup)


def write_repair_marker(bundle: Path, source: Path) -> Path:
    bundle.mkdir()
    marker = bundle / cleanup.REPAIR_MARKER
    marker.write_text(
        json.dumps(
            {
                "version": 1,
                "state": "rpc_fallback_missing_poh_and_shredding",
                "epoch": 1000,
                "publication_ready": False,
                "block_sources": [
                    {
                        "original_capture_dir": str(source),
                        "selected_blocks": 7,
                    }
                ],
                "capture_and_completion_receipts": [],
                "rpc_only_slots": [
                    {
                        "slot": 1,
                        "source_path": "repair/rpc-get-block/epoch-1000/slot-1.json",
                    }
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    return marker


class LiveRootCleanupTests(unittest.TestCase):
    def test_name_allow_list_is_deliberately_narrow(self) -> None:
        self.assertEqual(
            cleanup.classify_non_production_name(".recovery-failed"),
            "failed_recovery",
        )
        self.assertEqual(
            cleanup.classify_non_production_name("codex-bench-runs-1k-20260710T192408Z"),
            "benchmark",
        )
        self.assertEqual(
            cleanup.classify_non_production_name("codex-samples"), "samples"
        )
        self.assertIsNone(
            cleanup.classify_non_production_name(
                "capture-20260712T180150Z-epoch1001-compact-v2-live"
            )
        )
        self.assertIsNone(
            cleanup.classify_non_production_name(
                "epoch-1001-capture-postupgrade-20260713T134948Z"
            )
        )

    def test_repair_manifest_protects_original_capture(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            live = Path(directory) / "live"
            live.mkdir()
            source = live / "codex-samples"
            source.mkdir()
            (source / "sample.bin").write_bytes(b"sample")
            marker_path = write_repair_marker(live / "epoch-1000-repair", source)

            markers = cleanup.discover_repair_markers(live)
            self.assertEqual(len(markers), 1)
            self.assertEqual(markers[0].path, marker_path.resolve())
            candidates = cleanup.inspect_candidates(
                live, [source.name], markers, max_entries=100
            )
            self.assertFalse(candidates[0].eligible)
            self.assertEqual(candidates[0].protected_by, (marker_path.resolve(),))

    def test_capture_shaped_debug_name_is_not_eligible(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            live = Path(directory) / "live"
            candidate = live / "codex-bench-unsafe"
            (candidate / "journal").mkdir(parents=True)
            (candidate / "journal" / "progress.json").write_text("{}\n")

            inspected = cleanup.inspect_candidates(
                live, [candidate.name], (), max_entries=100
            )[0]
            self.assertFalse(inspected.eligible)
            self.assertEqual(inspected.production_signals, ("journal/progress.json",))

    def test_apply_renames_to_archive_and_publishes_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            live = root / "live"
            archive = root / "quarantine"
            live.mkdir()
            archive.mkdir()
            candidate_path = live / "codex-samples"
            candidate_path.mkdir()
            (candidate_path / "sample.bin").write_bytes(b"retained bytes")
            candidates = cleanup.inspect_candidates(
                live, [candidate_path.name], (), max_entries=100
            )
            args = argparse.Namespace(
                mode="apply",
                run_id="test-run",
                min_age_seconds=0,
                max_candidate_entries=100,
            )
            receipt = cleanup.build_base_receipt(
                args=args,
                live_root=live,
                archive_root=archive,
                markers=(),
                candidates=candidates,
                inventory=cleanup.inventory_live_root(live, ()),
                started_at=cleanup.utc_now(),
            )
            proc_report = {
                "method": "test",
                "global_scope_complete": True,
                "matches": [],
            }
            with mock.patch.object(
                cleanup, "proc_quiescence_scan", return_value=proc_report
            ):
                receipt_path = cleanup.apply_cleanup(
                    args=args,
                    live_root=live,
                    archive_root=archive,
                    markers=(),
                    candidates=candidates,
                    base_receipt=receipt,
                )

            self.assertFalse(candidate_path.exists())
            moved = archive / "test-run" / "items" / "codex-samples" / "sample.bin"
            self.assertEqual(moved.read_bytes(), b"retained bytes")
            document = json.loads(receipt_path.read_text(encoding="utf-8"))
            self.assertEqual(document["state"], "applied")
            self.assertFalse(document["policy"]["deletes_files"])
            self.assertEqual(document["candidates"][0]["decision"], "quarantined")

    def test_duplicate_marker_key_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / cleanup.REPAIR_MARKER
            marker.write_text(
                '{"version":1,"version":1,"epoch":1000,'
                '"state":"repair","publication_ready":false,'
                '"block_sources":[]}\n',
                encoding="utf-8",
            )
            with self.assertRaisesRegex(cleanup.CleanupError, "duplicate JSON key"):
                cleanup.read_repair_marker(marker)

    def test_missing_repair_source_blocks_all_cleanup(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            missing = root / "live" / "missing-capture"
            marker = write_repair_marker(root / "repair-bundle", missing)
            with self.assertRaisesRegex(cleanup.CleanupError, "source is unavailable"):
                cleanup.read_repair_marker(marker)


if __name__ == "__main__":
    unittest.main()
