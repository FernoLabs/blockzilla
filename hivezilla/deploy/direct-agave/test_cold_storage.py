import importlib.util
import json
from pathlib import Path
import tempfile
import sqlite3
import threading
import unittest

spec = importlib.util.spec_from_file_location("cold", Path(__file__).with_name("cold_storage.py"))
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


class Remote:
    namespace = "namespace"
    fail = False
    def __init__(self): self.objects = {}
    def put_verified(self, key, path, sha):
        if self.fail: raise RuntimeError("readback_mismatch")
        assert m.digest(path) == sha
        self.objects[key] = path.read_bytes()
    def download(self, key, path, sha):
        path.write_bytes(self.objects[key])
        if m.digest(path) != sha: raise RuntimeError("readback_mismatch")


class ColdTests(unittest.TestCase):
    def test_async_verification_keeps_sqlite_commit_and_retirement_on_owner_thread(self):
        class HeldRemote(Remote):
            def __init__(self):
                super().__init__()
                self.started, self.release = threading.Event(), threading.Event()
            def put_verified(self, key, path, sha):
                self.started.set()
                if not self.release.wait(5):
                    raise RuntimeError('verification_not_released')
                super().put_verified(key, path, sha)
        with tempfile.TemporaryDirectory() as d:
            root = Path(d); source, record, data, evidence = self.fixture(root)
            state = root / 'cold-storage'; remote = HeldRemote()
            db = m.database(state, source, remote.namespace)
            transfers = m.Transfers(root, db, remote, state, True, 2)
            try:
                transfers.submit([record])
                self.assertTrue(remote.started.wait(2))
                self.assertEqual(transfers.collect(), [])
                self.assertEqual(db.execute('SELECT count(*) FROM archives').fetchone()[0], 0)
                self.assertTrue(data.exists() and evidence.exists())
                self.assertEqual(transfers.pending_banks(), {(10, 20)})
                remote.release.set()
                committed = []
                for _ in range(5):
                    committed = transfers.collect(block=True)
                    if committed: break
                self.assertEqual(committed[0]['banks'], 1)
                self.assertFalse(data.exists() or evidence.exists())
                self.assertEqual(db.execute('SELECT retired FROM archives').fetchone()[0], 1)
            finally:
                remote.release.set()
                transfers.close()
                db.close()

    def test_candidates_skip_archived_and_unrooted_banks(self):
        db = sqlite3.connect(':memory:')
        db.executescript("ATTACH DATABASE ':memory:' AS cold;"
            "CREATE TABLE cold.archives(slot INTEGER,bank INTEGER,PRIMARY KEY(slot,bank));"
            "CREATE TABLE archive_catalog(slot INTEGER,bank INTEGER,artifact TEXT,marker_sha TEXT,"
            "custody_path TEXT,custody_sha TEXT,retirement_enqueued INTEGER,root_sequence INTEGER);"
            "CREATE TABLE banks(slot INTEGER,bank INTEGER,state TEXT,dirty INTEGER);"
            "CREATE TABLE feed_events(slot INTEGER,bank INTEGER,sequence INTEGER);")
        for slot, state in [(1, 'rooted'), (2, 'rooted'), (3, 'processed'), (4, 'rooted')]:
            db.execute('INSERT INTO archive_catalog VALUES(?,?,?,?,?,?,?,?)',
                       (slot, slot, 'payload', 'sha', 'proof', 'proofsha', 1, 1_000_000 + slot))
            db.execute('INSERT INTO banks VALUES(?,?,?,0)', (slot, slot, state))
            db.execute('INSERT INTO feed_events VALUES(?,?,?)', (slot, slot, slot))
        db.execute('INSERT INTO cold.archives VALUES(1,1)')
        self.assertEqual([row[0] for row in m.candidates(db, 4)], [2])
        self.assertEqual([row[0] for row in m.candidates(db, 5)], [2, 4])
        self.assertEqual([row[0] for row in m.candidates(db, 5, scan_after=2)], [4])
        db.close()

    def fixture(self, root):
        source = dict(genesis_hash="g", node_identity="n", agave_revision="r", session_id="session")
        (root / "FEED.json").write_text(json.dumps(source))
        artifact, custody = root / "payload", root / "custody"
        artifact.mkdir(); custody.mkdir()
        data = artifact / "blocks.bin"
        data.write_bytes(b"compact execution")
        evidence = custody / "source-events.json.zst"
        evidence.write_bytes(b"native events")
        bank = {"slot": 10, "bank_id": 20}
        marker = dict(source, produced_blocks=[bank], files=[dict(path=data.name, bytes=data.stat().st_size, sha256=m.digest(data))])
        marker_path = artifact / "DIRECT-AGAVE-COMPACT.v1.json"
        marker_path.write_text(json.dumps(marker))
        proof = dict(state="COMPLETE", finalized=True, session_id="session", produced_blocks=[bank], through_sequence=500,
                     payload_marker_path=str(marker_path), payload_marker_sha256=m.digest(marker_path),
                     files=[dict(path=evidence.name, bytes=evidence.stat().st_size, sha256=m.digest(evidence))])
        proof_path = custody / "CUSTODY.json"
        proof_path.write_text(json.dumps(proof))
        row = (10, 20, str(artifact), m.digest(marker_path), str(proof_path), m.digest(proof_path), 100)
        return source, m.build_bank(root, row, source), data, evidence

    def test_remote_failure_keeps_every_file_and_has_no_retirement_commit(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d); source, record, data, evidence = self.fixture(root)
            state = root / "cold-storage"; remote = Remote(); remote.fail = True
            db = m.database(state, source, remote.namespace)
            with self.assertRaisesRegex(RuntimeError, "readback_mismatch"):
                m.archive_group(root, db, remote, [record], state, True)
            self.assertTrue(data.exists() and evidence.exists())
            self.assertEqual(db.execute("SELECT count(*) FROM archives").fetchone()[0], 0)
            db.close()

    def test_verified_retirement_and_restore_keep_exact_bytes_and_markers(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d); source, record, data, evidence = self.fixture(root)
            originals = {p: p.read_bytes() for p in [data, evidence]}
            state = root / "cold-storage"; remote = Remote(); db = m.database(state, source, remote.namespace)
            m.archive_group(root, db, remote, [record], state, True)
            self.assertFalse(data.exists() or evidence.exists())
            self.assertTrue((data.parent / "DIRECT-AGAVE-COMPACT.v1.json").exists())
            self.assertTrue((evidence.parent / "CUSTODY.json").exists())
            m.restore(root, evidence, state, remote)
            for path, value in originals.items(): self.assertEqual(path.read_bytes(), value)
            db.close()

    def test_remote_corruption_cannot_restore_a_deleted_file(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d); source, record, data, evidence = self.fixture(root)
            state = root / "cold-storage"; remote = Remote(); db = m.database(state, source, remote.namespace)
            m.archive_group(root, db, remote, [record], state, True)
            remote.objects[record["key"]] = b"corrupt"
            with self.assertRaisesRegex(RuntimeError, "readback_mismatch"):
                m.restore(root, data, state, remote)
            self.assertFalse(data.exists())
            db.close()

    def test_different_source_cannot_use_a_retirement_receipt(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d); source, record, data, _ = self.fixture(root)
            state = root / "cold-storage"; remote = Remote(); db = m.database(state, source, remote.namespace)
            m.archive_group(root, db, remote, [record], state, True)
            (root / "FEED.json").write_text(json.dumps(dict(source, session_id="another")))
            with self.assertRaisesRegex(RuntimeError, "restore_source_mismatch"):
                m.restore(root, data, state, remote)
            db.close()

    def test_archive_database_cannot_be_rebound(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d)
            m.database(p, {"session_id": "one"}, "destination").close()
            with self.assertRaisesRegex(RuntimeError, "source_or_destination_changed"):
                m.database(p, {"session_id": "two"}, "destination")

    def test_symlink_and_traversal_paths_reject(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            (root / "link").symlink_to("/tmp", target_is_directory=True)
            for p in [root / "../outside", root / "link/file"]:
                with self.assertRaises(RuntimeError): m.checked(root, p)


if __name__ == "__main__": unittest.main()
