import contextlib
import io
import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch

import preserve_rpc_gap as backup


class RecoveryCoverageTests(unittest.TestCase):
    def test_pruned_rpc_error_has_no_retry_delay(self):
        response = io.BytesIO(json.dumps({"error": {"code": -32001, "message": "pruned"}}).encode())
        with patch.object(backup.urllib.request, "urlopen", return_value=response) as request, \
                patch.object(backup.time, "sleep") as sleep:
            with self.assertRaises(backup.RpcError) as error:
                backup.rpc("getBlock", [1, {}])
            self.assertEqual(error.exception.code, -32001)
            self.assertEqual(request.call_count, 1)
            sleep.assert_not_called()

    def run_backup(self, available, allow_pruned, resume_available=None, corrupt=False, remove=False):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for session, cursor in [("old", {"last_slot": 99, "last_hash": [0] * 32}),
                                    ("new", {"first_slot": 102})]:
                path = root / "compact/direct-agave-live" / session
                path.mkdir(parents=True)
                (path / "CURSOR.json").write_text(json.dumps(cursor))

            def rpc(method, params):
                if method == "getIdentity":
                    value = {"identity": "J7qpvCEWhdBgPdYRV3s3btWPrRVXa2qnAzgvxWTvtbrQ"}
                elif method == "getGenesisHash":
                    value = "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d"
                elif method == "getFirstAvailableBlock":
                    value = available
                elif method == "getBlocks":
                    value = list(range(params[0], params[1] + 1))
                elif method == "getBlock":
                    slot = params[0]
                    value = {"blockhash": f"hash-{slot}", "parentSlot": slot - 1,
                             "previousBlockhash": backup.base58([0] * 32) if slot == 100 else f"hash-{slot - 1}",
                             "transactions": [{"transaction": ["AA==", "base64"], "meta": {"err": None}}],
                             "rewards": []}
                else:
                    raise AssertionError(method)
                return json.dumps({"result": value}).encode(), value

            destination = root / "backup"
            arguments = ["backup", str(root), "old", "new", str(destination)]
            if allow_pruned:
                arguments.append("--allow-pruned-prefix")
            with patch.object(backup, "rpc", rpc), patch.object(backup.sys, "argv", arguments), \
                    patch.object(backup.shutil, "disk_usage", return_value=types.SimpleNamespace(free=1000 * 1024**3)), \
                    contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(backup.main(), 0)
                if resume_available is not None:
                    saved = destination / (str(available) + ".rpc.json.gz")
                    original = saved.read_bytes()
                    if corrupt:
                        saved.write_bytes(b"corrupt saved bytes")
                    if remove:
                        saved.unlink()
                    available = resume_available
                    arguments.append("--resume")
                    self.assertEqual(backup.main(), 1 if corrupt or remove else 0)
                    if corrupt:
                        self.assertEqual(saved.read_bytes(), b"corrupt saved bytes")
                    elif not remove:
                        self.assertEqual(saved.read_bytes(), original)
            return json.loads((destination / "RECEIPT.json").read_text())

    def test_complete_gap_has_both_chain_boundaries(self):
        receipt = self.run_backup(100, False)
        self.assertEqual(receipt["state"], "SAVED")
        self.assertTrue(receipt["chain_matches_boundaries"])
        self.assertEqual(receipt["blocks_saved"], 2)
        self.assertIsNone(receipt["unavailable_rpc_prefix"])
        self.assertFalse(receipt["compact_gap_rebuilt"])

    def test_pruned_prefix_is_explicit_and_never_claims_full_chain(self):
        receipt = self.run_backup(101, True)
        self.assertEqual(receipt["state"], "SAVED_PARTIAL")
        self.assertEqual(receipt["unavailable_rpc_prefix"], [100, 100])
        self.assertEqual(receipt["skipped_slots"], [])
        self.assertFalse(receipt["chain_matches_boundaries"])
        self.assertTrue(receipt["retained_chain_matches_following_block"])
        self.assertEqual(receipt["blocks_saved"], 1)
        self.assertFalse(receipt["native_capture_complete"])

    def test_resume_reads_saved_blocks_after_rpc_pruning(self):
        receipt = self.run_backup(100, False, resume_available=102)
        self.assertEqual(receipt["state"], "SAVED")
        self.assertEqual(receipt["produced_slots"], [100, 101])
        self.assertEqual(receipt["blocks_saved"], 2)

    def test_resume_never_overwrites_corrupt_saved_data(self):
        receipt = self.run_backup(100, False, resume_available=102, corrupt=True)
        self.assertEqual(receipt["state"], "INCOMPLETE")
        self.assertEqual(len(receipt["errors"]), 1)

    def test_resume_keeps_missing_pruned_blocks_in_coverage(self):
        receipt = self.run_backup(100, False, resume_available=102, remove=True)
        self.assertEqual(receipt["state"], "INCOMPLETE")
        self.assertEqual(receipt["produced_slots"], [100, 101])
        self.assertEqual(receipt["blocks_saved"], 1)


if __name__ == "__main__":
    unittest.main()
