import hashlib
import json
from pathlib import Path
import tempfile
import unittest

import compare_car_epoch as check


class ComparisonTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.export = self.root / "export"
        self.archive = self.root / "car"
        self.previous = self.root / "previous"
        self.source = self.root / "source"
        self.state = self.root / "state"
        for p in (self.export, self.archive, self.previous, self.source):
            p.mkdir()
        self.slots = [432000, 432002, 432003]
        self.hashes = [bytes([i]) * 32 for i in (1, 2, 3)]
        self.seed = bytes([9]) * 32
        sigs = [b"a" * 64, b"b" * 128, b"c" * 64]
        rows = []
        records = []
        first_sig = 0
        parent_slot, parent_hash = 431999, self.seed
        for i, slot in enumerate(self.slots):
            n = len(sigs[i]) // 64
            rows.append(check.INDEX_ROW.pack(i, slot, i, 1, 1, 1, i, first_sig, n))
            records.append(check.RECORD.pack(slot, parent_slot, self.hashes[i], parent_hash, 1, n, 2,
                                             hashlib.sha256(sigs[i]).digest()))
            first_sig += n
            parent_slot, parent_hash = slot, self.hashes[i]
        index = check.INDEX_HEADER.pack(b"BZV2HIX1", 1, 0, 3, 3, 1, 0) + b"".join(rows)
        (self.archive / "archive-v2-blocks.index").write_bytes(index)
        (self.archive / "archive-v2-blocks.zstd").write_bytes(b"xyz")
        (self.archive / "signatures.bin").write_bytes(b"".join(sigs))
        hashes = check.HASH_HEADER.pack(b"BZBHIX3!", 3, 48, 3) + b"".join(
            check.HASH_ROW.pack(slot, h, 0) for slot, h in zip(self.slots, self.hashes))
        (self.archive / "blockhash_index_v3.bin").write_bytes(hashes)
        (self.previous / "blockhash_index_v3.bin").write_bytes(
            check.HASH_HEADER.pack(b"BZBHIX3!", 3, 48, 1) + check.HASH_ROW.pack(431999, self.seed, 0))
        native = check.HEADER.pack(check.MAGIC, 1, 3) + b"".join(records)
        (self.export / "native-blocks.bin").write_bytes(native)
        (self.export / "EXPORT.json").write_text(json.dumps(dict(
            schema="hivezilla-native-car-export-v1", state="COMPLETE", epoch=1, blocks=3,
            transactions=3, signatures=4, source="native-source",
            inventory_sha256=hashlib.sha256(native).hexdigest(),
            poh_receipt={"state": "PASSED", "blocks_verified": 3,
                         "transactions_consumed": 3, "signatures_hashed": 4})))
        (self.archive / "blockzilla-car-import-receipt.json").write_text(json.dumps({"epoch": 1}))
        slots = "431999\n" + "\n".join(map(str, self.slots)) + "\n"
        (self.source / "1.slots.txt").write_text(slots)
        (self.source / "source.json").write_text(json.dumps(dict(
            epoch=1, num_slots=3, num_transactions=3, epoch_cid="source-cid",
            source_records={"1.slots.txt": dict(bytes=len(slots), sha256=hashlib.sha256(slots.encode()).hexdigest())})))

    def tearDown(self):
        self.temp.cleanup()

    def run_comparison(self):
        return check.compare(self.export, self.archive, self.previous, self.source, self.state)

    def test_complete_match_includes_skipped_slot_and_multisig(self):
        result = self.run_comparison()
        self.assertEqual(result["state"], "PASSED")
        self.assertEqual(result["blocks_checked"], 3)
        self.assertEqual(result["signature_bytes_checked"], 256)
        self.assertEqual(result["execution_metadata_comparison"], "not-run")

    def test_changed_signature_bytes_are_reported(self):
        p = self.archive / "signatures.bin"
        data = bytearray(p.read_bytes())
        data[64] ^= 1
        p.write_bytes(data)
        result = self.run_comparison()
        self.assertEqual(result["state"], "FAILED")
        self.assertEqual(result["mismatches"], [{"slot": 432002, "fields": ["signature_bytes_and_order"]}])

    def test_changed_blockhash_is_reported_at_block_and_parent(self):
        p = self.archive / "blockhash_index_v3.bin"
        data = bytearray(p.read_bytes())
        data[check.HASH_HEADER.size + check.HASH_ROW.size + 8] ^= 1
        p.write_bytes(data)
        result = self.run_comparison()
        self.assertEqual(result["mismatch_blocks"], 2)
        self.assertEqual(result["mismatches"][0]["fields"], ["blockhash"])
        self.assertEqual(result["mismatches"][1]["fields"], ["parent_blockhash"])

    def test_changed_native_inventory_is_rejected(self):
        p = self.export / "native-blocks.bin"
        data = bytearray(p.read_bytes())
        data[-1] ^= 1
        p.write_bytes(data)
        with self.assertRaisesRegex(ValueError, "native export digest mismatch"):
            self.run_comparison()

    def test_truncated_signature_file_is_rejected(self):
        (self.archive / "signatures.bin").write_bytes(b"a" * 64)
        with self.assertRaisesRegex(ValueError, "signature file length mismatch"):
            self.run_comparison()

    def test_noncontiguous_frame_offsets_are_rejected(self):
        p = self.archive / "archive-v2-blocks.index"
        data = bytearray(p.read_bytes())
        offset = check.INDEX_HEADER.size + check.INDEX_ROW.size + 12
        data[offset] = 7
        p.write_bytes(data)
        with self.assertRaisesRegex(ValueError, "noncontiguous index row"):
            self.run_comparison()

    def test_public_car_export_can_be_compared_on_native_machine(self):
        car_export = self.root / "public-car-export"
        result = check.export_car(self.archive, self.previous, self.source, car_export)
        self.assertFalse(result["includes_private_native_data"])
        compared = check.compare_inventories(self.export, car_export, self.state)
        self.assertEqual(compared["state"], "PASSED")
        self.assertEqual(compared["signatures"], 4)

    def test_reversed_comparison_detects_signature_change(self):
        (self.archive / "signatures.bin").write_bytes(b"a" * 256)
        car_export = self.root / "public-car-export"
        check.export_car(self.archive, self.previous, self.source, car_export)
        result = check.compare_inventories(self.export, car_export, self.state)
        self.assertEqual(result["state"], "FAILED")
        self.assertEqual(result["mismatch_blocks"], 2)

    def test_car_export_digest_is_checked(self):
        car_export = self.root / "public-car-export"
        check.export_car(self.archive, self.previous, self.source, car_export)
        p = car_export / "car-blocks.bin"
        data = bytearray(p.read_bytes())
        data[-1] ^= 1
        p.write_bytes(data)
        with self.assertRaisesRegex(ValueError, "inventory digest mismatch"):
            check.compare_inventories(self.export, car_export, self.state)


if __name__ == "__main__":
    unittest.main()
