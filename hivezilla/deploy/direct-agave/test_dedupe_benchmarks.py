import importlib.util
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("dedupe", Path(__file__).with_name("dedupe_benchmarks.py"))
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


class DedupeTests(unittest.TestCase):
    def test_exact_matches_share_storage_reports_and_different_files_survive(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d) / "bench"
            files = []
            for name, data in [("results-a", b"same"), ("results-b", b"same"), ("results-c", b"diff")]:
                folder = root / name
                folder.mkdir(parents=True)
                p = folder / "transactions.bin"
                p.write_bytes(data)
                (folder / "report.json").write_text("keep")
                files.append(p)
            with patch.object(m, "writer_open", return_value=False):
                result = m.run(root, Path(d) / "receipt", True, min_bytes=1)
            self.assertEqual(result["matching_groups"], 1)
            self.assertEqual(files[0].stat().st_ino, files[1].stat().st_ino)
            self.assertNotEqual(files[0].stat().st_ino, files[2].stat().st_ino)
            self.assertEqual(files[1].read_bytes(), b"same")
            self.assertEqual(files[2].read_bytes(), b"diff")
            self.assertFalse(files[0].stat().st_mode & 0o222)
            self.assertTrue(all(p.with_name("report.json").read_text() == "keep" for p in files))

    def test_changed_file_rejects_replacement(self):
        with tempfile.TemporaryDirectory() as d:
            a, b = Path(d) / "a", Path(d) / "b"
            a.write_bytes(b"same")
            b.write_bytes(b"same")
            ra, rb = m.hashed(a), m.hashed(b)
            b.write_bytes(b"evil")
            with self.assertRaisesRegex(RuntimeError, "file_changed"):
                m.link_identical(ra, rb)
            self.assertEqual(b.read_bytes(), b"evil")

    def test_different_digest_rejects_replacement(self):
        with tempfile.TemporaryDirectory() as d:
            a, b = Path(d) / "a", Path(d) / "b"
            a.write_bytes(b"same")
            b.write_bytes(b"diff")
            with self.assertRaisesRegex(RuntimeError, "digest_mismatch"):
                m.link_identical(m.hashed(a), m.hashed(b))
            self.assertEqual(b.read_bytes(), b"diff")

    def test_dry_run_and_symlink_leave_outputs_unchanged(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d) / "bench"
            for name in ["results-a", "results-b"]:
                (root / name).mkdir(parents=True)
                (root / name / "transactions.bin").write_bytes(b"same")
            (root / "results-link").symlink_to(root / "results-a", target_is_directory=True)
            a, b = [root / n / "transactions.bin" for n in ["results-a", "results-b"]]
            before = (a.stat().st_ino, b.stat().st_ino)
            result = m.run(root, Path(d) / "receipt", False, min_bytes=1)
            self.assertEqual(result["candidate_files"], 2)
            self.assertEqual(before, (a.stat().st_ino, b.stat().st_ino))

    def test_already_linked_is_idempotent(self):
        with tempfile.TemporaryDirectory() as d:
            a, b = Path(d) / "a", Path(d) / "b"
            a.write_bytes(b"same")
            os.link(a, b)
            self.assertEqual(m.link_identical(m.hashed(a), m.hashed(b)), 0)


if __name__ == "__main__":
    unittest.main()
