import hashlib
import importlib.util
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("verify_epoch", Path(__file__).with_name("verify_epoch.py"))
verify = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verify)


class EpochVerificationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.output = self.root / "session"
        self.state = self.root / "results"
        self.state.mkdir()
        self.output.mkdir()
        self.database = sqlite3.connect(self.output / "committed.sqlite")
        self.database.execute("CREATE TABLE commits(slot INTEGER PRIMARY KEY,artifact TEXT)")
        self.artifacts = {}
        previous_slot, previous_hash = 453167998, [5] * 32
        for slot in (453167999, 453168000, 453168003, 453599999, 453600000):
            epoch = slot // 432000
            artifact = self.output / f"epoch-{epoch}" / f"{slot}-{slot}.compact"
            artifact.mkdir(parents=True)
            block = dict(block_id=0, slot=slot, bank_id=slot, blockhash=[slot % 256] * 32,
                         parent_slot=previous_slot, parent_blockhash=previous_hash,
                         transactions=0, signatures=0, entries=1)
            receipt = dict(schema="hivezilla-direct-agave-normalized-v1", epoch=epoch,
                           session_id="session", agave_revision="fixture", genesis_hash="fixture",
                           node_identity="fixture", source_profile="agave-bank-callbacks-v1",
                           start_slot=slot, end_slot_exclusive=slot + 1, finalized=True,
                           canonical=False, blocks=[block])
            self.write_receipt(artifact, receipt)
            (artifact / "blockhash_registry.bin").write_bytes(bytes(block["blockhash"]))
            (artifact / "prev_blockhash_tail.bin").write_bytes(bytes(previous_hash) + previous_slot.to_bytes(8, "little"))
            self.database.execute("INSERT INTO commits VALUES(?,?)", (slot, str(artifact)))
            self.artifacts[slot] = artifact
            previous_slot, previous_hash = slot, block["blockhash"]
        self.database.commit()

    def tearDown(self):
        self.database.close()
        self.temp.cleanup()

    def write_receipt(self, artifact, receipt):
        raw = json.dumps(receipt).encode()
        (artifact / "DIRECT-AGAVE-NORMALIZED.v1.json").write_bytes(raw)
        marker = dict(receipt, schema="hivezilla-direct-agave-compact-v1", state="COMPLETE",
                      publication_ready=False, produced_blocks=receipt["blocks"], blocks=len(receipt["blocks"]),
                      source_receipt_sha256=hashlib.sha256(raw).hexdigest(), transactions=0,
                      signatures=0, entries=len(receipt["blocks"]))
        (artifact / "DIRECT-AGAVE-COMPACT.v1.json").write_text(json.dumps(marker))

    def test_all_parent_links_and_both_epoch_boundaries(self):
        jobs, report = verify.inventory(self.output, 1049, self.state)
        self.assertEqual(len(jobs), 3)
        self.assertEqual(report["blocks"], 3)
        self.assertEqual(report["skipped_slots"], 431997)
        self.assertEqual(report["gap_ranges"], 2)
        self.assertEqual(len(report["boundaries"]), 2)
        self.assertFalse(report["independent_archive_coverage_confirmed"])

    def test_a_real_missing_block_is_not_counted_as_a_skipped_slot(self):
        self.database.execute("DELETE FROM commits WHERE slot=453168003")
        self.database.commit()
        with self.assertRaisesRegex(ValueError, "parent chain gap"):
            verify.inventory(self.output, 1049, self.state)
        self.assertFalse((self.state / "CONTINUITY.json").exists())

    def test_changed_registry_and_changed_receipt_stop_continuity(self):
        artifact = self.artifacts[453168003]
        (artifact / "blockhash_registry.bin").write_bytes(bytes(32))
        with self.assertRaisesRegex(ValueError, "block identity mismatch"):
            verify.inventory(self.output, 1049, self.state)
        (artifact / "blockhash_registry.bin").write_bytes(bytes([453168003 % 256] * 32))
        (artifact / "DIRECT-AGAVE-NORMALIZED.v1.json").write_text("{}")
        with self.assertRaises((ValueError, KeyError)):
            verify.inventory(self.output, 1049, self.state)

    def test_wrong_successor_is_not_full_epoch_continuity(self):
        artifact = self.artifacts[453600000]
        receipt = json.loads((artifact / "DIRECT-AGAVE-NORMALIZED.v1.json").read_text())
        receipt["blocks"][0]["parent_slot"] = 453599998
        self.write_receipt(artifact, receipt)
        with self.assertRaisesRegex(ValueError, "successor hash mismatch"):
            verify.inventory(self.output, 1049, self.state)

    def test_verification_rejects_mutation_after_inventory(self):
        jobs, _ = verify.inventory(self.output, 1049, self.state)
        (Path(jobs[0]["artifact"]) / "DIRECT-AGAVE-COMPACT.v1.json").write_text("{}")
        with self.assertRaisesRegex(ValueError, "pinned receipt changed"):
            verify.verify_chunk(jobs[0], Path("unused-verifier"), 1)


if __name__ == "__main__":
    unittest.main()
