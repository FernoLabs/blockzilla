import argparse
import hashlib
import json
from pathlib import Path
import sqlite3
import struct
import tarfile
import tempfile
import unittest
from unittest import mock

import archive_upload as archive
import native_backup as native


class UploadChecks(unittest.TestCase):
    def query_fixture(self, root):
        folder = root / "epoch-1"
        folder.mkdir()
        for name in archive.CORE | archive.QUERY_EXCLUDED_FILES:
            (folder / name).write_bytes(b"data")
        (folder / "signatures.bin").write_bytes(b"s" * 64)
        (folder / "archive-v2-blocks.index").write_bytes(
            struct.pack("<8sHHQQiI", b"BZV2HIX1", 1, 0, 1, 4, 1, 0)
            + struct.pack("<IQQIIIQQI", 0, 432000, 0, 4, 4, 1, 0, 0, 1))
        args = argparse.Namespace(root=str(root), destination="foundation:bucket/existing",
                                  rclone="rclone", config="config", bandwidth="1M",
                                  profile=archive.QUERY_SIGNATURES_PROFILE)
        return args, folder

    def test_query_reuses_uploaded_files_and_resumes_lost_manifest_reply(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            args, folder = self.query_fixture(root)
            state = root / "query-state"
            state.mkdir()
            original = {p.name: p.read_bytes() for p in folder.iterdir()}
            remote = dict(original)
            remote["BLOCKZILLA-BACKUP-MANIFEST.json"] = b'{"legacy":true}'
            lose_reply = [True]
            copied_payload_names = []
            published_manifests = []

            def transport(args, *command, capture=False):
                if command[0] == "copy":
                    names = Path(command[command.index("--files-from-raw") + 1]).read_text().splitlines()
                    self.assertIn("signatures.bin", names)
                    self.assertTrue(archive.QUERY_EXCLUDED_FILES.isdisjoint(names))
                    for name in names:
                        data = (folder / name).read_bytes()
                        if name in remote:
                            self.assertEqual(remote[name], data)
                        else:
                            remote[name] = data
                            copied_payload_names.append(name)
                elif command[0] == "lsjson":
                    return json.dumps([{"Name": name, "Size": len(data)}
                                       for name, data in remote.items()])
                elif command[0] == "copyto":
                    manifest = Path(command[1])
                    data = manifest.read_bytes()
                    if manifest.name in remote:
                        self.assertEqual(remote[manifest.name], data)
                    remote[manifest.name] = data
                    published_manifests.append(data)
                    if lose_reply[0]:
                        lose_reply[0] = False
                        raise RuntimeError("lost manifest reply")
                elif command[0] == "cat":
                    return remote[command[1].rsplit("/", 1)[1]].decode()
                else:
                    raise AssertionError(command)

            with mock.patch.object(archive, "rclone", transport):
                with self.assertRaisesRegex(RuntimeError, "lost manifest reply"):
                    archive.upload(args, state, 1)
                self.assertFalse((state / "epoch-1/complete.json").exists())
                archive.upload(args, state, 1)
            report = json.loads((state / "epoch-1/complete.json").read_text())
            self.assertEqual(report["upload_profile"], archive.QUERY_SIGNATURES_PROFILE)
            self.assertEqual(report["source_files"]["signatures.bin"][2], 64)
            self.assertEqual(report["bytes"], sum(len(data) for name, data in original.items()
                                                  if name not in archive.QUERY_EXCLUDED_FILES))
            self.assertEqual(set(report["previously_uploaded_extras_retained"]),
                             archive.QUERY_EXCLUDED_FILES | {"BLOCKZILLA-BACKUP-MANIFEST.json"})
            self.assertEqual(published_manifests[0], published_manifests[1])
            self.assertEqual(copied_payload_names, [])
            self.assertEqual({p.name: p.read_bytes() for p in folder.iterdir()}, original)
            for name, data in original.items():
                self.assertEqual(remote[name], data)

    def test_query_requires_signatures_and_rejects_wrong_remote_signature_length(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            args, folder = self.query_fixture(root)
            (folder / "signatures.bin").unlink()
            with self.assertRaisesRegex(RuntimeError, "Missing core compact files"):
                archive.inventory(folder, args.profile)
            (folder / "signatures.bin").write_bytes(b"s" * 64)
            state = root / "query-state"
            state.mkdir()

            def transport(args, *command, capture=False):
                if command[0] == "lsjson":
                    return json.dumps([{"Name": name, "Size": identity[2] - (name == "signatures.bin")}
                                       for name, identity in archive.inventory(folder, args.profile).items()])
                return None

            with mock.patch.object(archive, "rclone", transport):
                with self.assertRaisesRegex(RuntimeError, "Remote object inventory mismatch"):
                    archive.upload(args, state, 1)
            self.assertFalse((state / "epoch-1/complete.json").exists())

    def test_archive_scope_excludes_restart_data_and_rejects_raw_car(self):
        with tempfile.TemporaryDirectory() as temp:
            folder = Path(temp)
            for name in archive.CORE:
                (folder / name).write_bytes(b"data")
            (folder / "archive-v2-blocks.index").write_bytes(
                struct.pack("<8sHHQQiI", b"BZV2HIX1", 1, 0, 0, 4, 1, 0))
            partial = folder / "partial-loop-restart-20260622T011222Z"
            partial.mkdir()
            (partial / "epoch.car").write_bytes(b"old source")
            (folder / ".poh-signature-count-migration.lock.stale-20260816").touch()
            self.assertEqual(set(archive.inventory(folder)), archive.CORE)
            (folder / "epoch.car").write_bytes(b"raw source")
            with self.assertRaisesRegex(RuntimeError, "Not a Compact V2 archive file"):
                archive.inventory(folder)
            (folder / "epoch.car").unlink()
            (folder / "unknown-directory").mkdir()
            with self.assertRaisesRegex(RuntimeError, "Not a Compact V2 archive file"):
                archive.inventory(folder)
            (folder / "unknown-directory").rmdir()
            (partial / "epoch.car").unlink()
            partial.rmdir()
            partial.symlink_to(folder, target_is_directory=True)
            with self.assertRaisesRegex(RuntimeError, "Not a regular source file"):
                archive.inventory(folder)

    def test_archive_rejects_remote_size_mismatch(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            folder = root / "epoch-1"
            folder.mkdir()
            for name in archive.CORE:
                (folder / name).write_bytes(b"data")
            (folder / "archive-v2-blocks.index").write_bytes(
                struct.pack("<8sHHQQiI", b"BZV2HIX1", 1, 0, 0, 4, 1, 0))
            state = root / "state"
            state.mkdir()
            args = argparse.Namespace(root=str(root), destination="foundation:bucket/new",
                                      rclone="rclone", config="config", bandwidth="1M")

            def transport(args, *command, capture=False):
                if command[0] == "lsjson":
                    return json.dumps([{"Name": p.name, "Size": p.stat().st_size + 1}
                                       for p in folder.iterdir()])
                return None

            with mock.patch.object(archive, "rclone", transport):
                with self.assertRaisesRegex(RuntimeError, "Remote object inventory"):
                    archive.upload(args, state, 1)
            self.assertFalse((state / "epoch-1/complete.json").exists())

    def fixture(self, temp):
        root = Path(temp) / "native"
        folder = root / "session/epoch-1/432000-432000.compact"
        folder.mkdir(parents=True)
        body = b"compact-source-data" * 100
        (folder / "source-events.json.zst").write_bytes(body)
        (folder / "signatures.bin").write_bytes(b"")
        receipt = {"schema": "hivezilla-direct-agave-compact-v1",
                   "source_profile": "agave-bank-callbacks-v1", "canonical": False,
                   "publication_ready": False, "state": "COMPLETE", "finalized": True,
                   "session_id": "session", "start_slot": 432000, "end_slot_exclusive": 432001,
                   "blocks": 1, "produced_blocks": [{"slot": 432000}],
                   "files": [{"path": name, "bytes": (folder / name).stat().st_size,
                              "sha256": hashlib.sha256((folder / name).read_bytes()).hexdigest()}
                             for name in ["source-events.json.zst", "signatures.bin"]]}
        (folder / "DIRECT-AGAVE-COMPACT.v1.json").write_text(json.dumps(receipt))
        audit = sqlite3.connect(root / "session/committed.sqlite")
        audit.execute("CREATE TABLE commits(slot INTEGER PRIMARY KEY,artifact TEXT)")
        audit.execute("INSERT INTO commits VALUES(432000,?)", (str(folder),))
        audit.commit()
        audit.close()
        args = argparse.Namespace(root=str(root), state=str(Path(temp) / "state"),
                                  config="config", destination="foundation:bucket/native-new",
                                  rclone="rclone", bandwidth="1M", pack_bytes=256 * 1024**2)
        return args, folder, body

    def test_native_lost_reply_resumes_same_pack_and_keeps_source(self):
        with tempfile.TemporaryDirectory() as temp:
            args, folder, body = self.fixture(temp)
            remote = {}
            lose_reply = [True]

            def copy(args, source, destination):
                payload = Path(source).read_bytes()
                if destination in remote:
                    self.assertEqual(remote[destination], payload)
                remote[destination] = payload
                if destination.endswith(".tar") and lose_reply[0]:
                    lose_reply[0] = False
                    raise RuntimeError("lost upload reply")

            def run(args, *command, capture=False):
                if command[0] == "lsjson":
                    return json.dumps({"Size": len(remote[command[1]])})
                if command[0] == "cat":
                    return remote[command[1]].decode()
                raise AssertionError(command)

            disk = type("Usage", (), {"free": 1024**4})()
            with mock.patch.object(native, "copy", copy), mock.patch.object(native, "run", run), \
                    mock.patch.object(native.shutil, "disk_usage", return_value=disk):
                with self.assertRaisesRegex(RuntimeError, "lost upload reply"):
                    native.main(args)
                native.main(args)
            status = json.loads((Path(args.state) / "STATUS.json").read_text())
            self.assertEqual(status["state"], "COMPLETE")
            self.assertEqual(status["packs"], 1)
            self.assertEqual((folder / "source-events.json.zst").read_bytes(), body)
            import io
            payload = next(data for name, data in remote.items() if name.endswith(".tar"))
            with tarfile.open(fileobj=io.BytesIO(payload)) as tar:
                self.assertEqual(tar.extractfile("session/epoch-1/432000-432000.compact/source-events.json.zst").read(), body)
                self.assertEqual(tar.getmember("session/epoch-1/432000-432000.compact/signatures.bin").size, 0)
                self.assertFalse(json.load(tar.extractfile("PACK-MANIFEST.json"))["canonical"])

    def test_native_changed_source_cannot_be_packed(self):
        with tempfile.TemporaryDirectory() as temp:
            args, folder, body = self.fixture(temp)
            state = Path(args.state)
            state.mkdir()
            chunk = native.describe(Path(args.root).resolve(), (str(folder), "session", 432000, 432000, 1, None))
            (folder / "source-events.json.zst").write_bytes(b"changed")
            with mock.patch.object(native.shutil, "disk_usage", return_value=type("Usage", (), {"free": 1024**4})()):
                with self.assertRaisesRegex(RuntimeError, "source changed"):
                    native.build_pack(args, state, 1, [chunk])
            self.assertFalse((state / "spool/pack-00000001.json").exists())


if __name__ == "__main__":
    unittest.main()
