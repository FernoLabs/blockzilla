"""Back up a frozen set of committed native chunks as bounded tar packs.

This is a retained-data backup, not a canonical archive publisher or live feed.
Only original compact files selected by the worker's commit database are read.
"""
import argparse
import fcntl
import hashlib
import io
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import subprocess
import tarfile
import time

from archive_upload import save, stamp, sync_dir


def run(args, *command, capture=False):
    result = subprocess.run([args.rclone, "--config", args.config,
                             "--contimeout", "10s", "--timeout", "2m"] + list(map(str, command)),
                            text=True, check=True,
                            stdout=subprocess.PIPE if capture else None)
    return result.stdout if capture else None


def copy(args, source, destination):
    run(args, "copyto", source, destination, "--immutable", "--size-only",
        "--s3-disable-checksum", "--s3-chunk-size", "64M", "--s3-upload-concurrency", "2",
        "--transfers", "1", "--bwlimit", args.bandwidth, "--retries", "4",
        "--retries-sleep", "15s", "--stats", "30s", "--stats-one-line", "--log-level", "INFO")


def describe(root, row):
    artifact, session, first, last, blocks, _ = row
    folder = Path(artifact)
    relative = folder.resolve(strict=True).relative_to(root)
    if len(relative.parts) != 3 or relative.parts[0] != session or not folder.name.endswith(".compact"):
        raise RuntimeError("Commit points outside the selected native compact session")
    marker = folder / "DIRECT-AGAVE-COMPACT.v1.json"
    raw = marker.read_bytes()
    if len(raw) > 2 * 1024 * 1024:
        raise RuntimeError("Native compact marker is too large")
    receipt = json.loads(raw)
    if (receipt.get("schema") != "hivezilla-direct-agave-compact-v1"
            or receipt.get("source_profile") != "agave-bank-callbacks-v1"
            or receipt.get("canonical") is not False
            or receipt.get("publication_ready") is not False
            or receipt.get("state") != "COMPLETE"
            or receipt.get("finalized") is not True
            or receipt.get("session_id") != session):
        raise RuntimeError("Unsupported native compact source contract")
    slots = [block["slot"] for block in receipt["produced_blocks"]]
    if (receipt["blocks"] != blocks or len(slots) != blocks
            or min(slots) != first or max(slots) != last
            or receipt["start_slot"] != first or receipt["end_slot_exclusive"] != last + 1):
        raise RuntimeError("Native marker block range differs from durable commits")
    rows = receipt["files"]
    if not 1 <= len(rows) <= 64:
        raise RuntimeError("Invalid native file count")
    files = []
    seen = set()
    for item in rows:
        name = item["path"]
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", name) or name in seen:
            raise RuntimeError("Unsafe or duplicate native compact filename")
        seen.add(name)
        identity = stamp(folder / name)
        if identity[2] != item["bytes"]:
            raise RuntimeError("Native file length differs from its commit marker")
        files.append(dict(item, identity=identity))
    files.append({"path": marker.name, "bytes": len(raw),
                  "sha256": hashlib.sha256(raw).hexdigest(), "identity": stamp(marker)})
    return {"artifact": artifact, "relative": str(relative), "session": session,
            "first_slot": first, "last_slot": last, "blocks": blocks,
            "files": files, "canonical": False, "publication_ready": False}


def snapshot(args, state, db):
    if db.execute("SELECT value FROM settings WHERE key='snapshot_complete'").fetchone():
        return
    db.execute("DELETE FROM chunks")
    sessions = []
    for session in sorted(Path(args.root).iterdir()):
        audit = session / "committed.sqlite"
        if not session.is_dir() or not audit.is_file():
            continue
        source = sqlite3.connect(audit.as_uri() + "?mode=ro", uri=True, timeout=10)
        try:
            rows = source.execute("SELECT artifact,MIN(slot),MAX(slot),COUNT(*) FROM commits GROUP BY artifact")
            db.executemany("INSERT INTO chunks VALUES(?,?,?,?,?,NULL)",
                           ((artifact, session.name, first, last, count)
                            for artifact, first, last, count in rows))
            count, first, last, blocks = db.execute(
                "SELECT COUNT(*),MIN(first_slot),MAX(last_slot),SUM(blocks) FROM chunks WHERE session=?",
                (session.name,)).fetchone()
            sessions.append({"session": session.name, "chunks": count, "blocks": blocks,
                             "first_slot": first, "last_slot": last})
        finally:
            source.close()
    if not db.execute("SELECT COUNT(*) FROM chunks").fetchone()[0]:
        raise RuntimeError("No durably committed native chunks are available")
    db.execute("INSERT INTO settings VALUES('snapshot_complete','true')")
    db.commit()
    save(state / "SNAPSHOT.json", {"schema": "blockzilla-native-backup-snapshot-v1",
                                  "sessions": sessions, "source_root": args.root,
                                  "destination": args.destination, "canonical": False,
                                  "publication_ready": False, "local_source_deletion": False})


class HashingWriter:
    def __init__(self, output):
        self.output = output
        self.digest = hashlib.sha256()
        self.bytes = 0

    def write(self, data):
        self.digest.update(data)
        self.bytes += len(data)
        return self.output.write(data)


def build_pack(args, state, pack_id, chunks):
    spool = state / "spool"
    spool.mkdir(exist_ok=True)
    tar_path = spool / f"pack-{pack_id:08d}.tar"
    json_path = spool / f"pack-{pack_id:08d}.json"
    if tar_path.is_file() and json_path.is_file():
        report = json.loads(json_path.read_text())
        if report["bytes"] != tar_path.stat().st_size or report["chunks"] != chunks:
            raise RuntimeError("Retained backup pack binding changed")
        return tar_path, json_path, report
    required = sum(f["bytes"] for chunk in chunks for f in chunk["files"])
    if shutil.disk_usage(state).free < 512 * 1024**3 + required + 16 * 1024**2:
        raise RuntimeError("Native backup cannot preserve the free-disk reserve")
    manifest = {"schema": "blockzilla-native-tar-backup-v1", "pack": pack_id,
                "canonical": False, "publication_ready": False, "chunks": chunks,
                "source_hashes": "retained worker receipts; no new whole-source hash pass"}
    temporary = tar_path.with_suffix(".tar.tmp")
    with temporary.open("wb") as output:
        writer = HashingWriter(output)
        with tarfile.open(fileobj=writer, mode="w|", format=tarfile.USTAR_FORMAT) as tar:
            for chunk in chunks:
                for file in chunk["files"]:
                    source = Path(chunk["artifact"]) / file["path"]
                    if stamp(source) != file["identity"]:
                        raise RuntimeError("Native source changed before backup")
                    info = tarfile.TarInfo(chunk["relative"] + "/" + file["path"])
                    info.size = file["bytes"]
                    info.mode = 0o600
                    with source.open("rb") as data:
                        s = os.fstat(data.fileno())
                        if [s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns] != file["identity"]:
                            raise RuntimeError("Native source file identity changed at open")
                        tar.addfile(info, data)
                        s = os.fstat(data.fileno())
                        if [s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns] != file["identity"]:
                            raise RuntimeError("Native source changed during backup")
            payload = json.dumps(manifest, sort_keys=True).encode() + b"\n"
            info = tarfile.TarInfo("PACK-MANIFEST.json")
            info.size = len(payload)
            info.mode = 0o600
            tar.addfile(info, io.BytesIO(payload))
        output.flush()
        os.fsync(output.fileno())
    temporary.replace(tar_path)
    sync_dir(spool)
    report = dict(manifest, bytes=writer.bytes, sha256=writer.digest.hexdigest(),
                  destination=f"{args.destination}/{tar_path.name}", full_remote_readback=False)
    save(json_path, report)
    return tar_path, json_path, report


def main(args):
    root = Path(args.root).resolve(strict=True)
    args.root = str(root)
    state = Path(args.state).resolve()
    state.mkdir(parents=True, exist_ok=True)
    with (state / "LOCK").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        binding = {"root": args.root, "destination": args.destination, "pack_bytes": args.pack_bytes}
        intent = state / "INTENT.json"
        if intent.exists() and json.loads(intent.read_text()) != binding:
            raise RuntimeError("Native backup source or destination binding changed")
        if not intent.exists():
            save(intent, binding)
        db = sqlite3.connect(state / "backup.sqlite")
        db.executescript("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;"
                         "CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY,value TEXT);"
                         "CREATE TABLE IF NOT EXISTS chunks(artifact TEXT PRIMARY KEY,session TEXT,first_slot INTEGER,last_slot INTEGER,blocks INTEGER,pack INTEGER);"
                         "CREATE INDEX IF NOT EXISTS pending_chunks ON chunks(pack,last_slot DESC);"
                         "CREATE TABLE IF NOT EXISTS packs(id INTEGER PRIMARY KEY,state TEXT,description TEXT);")
        save(state / "STATUS.json", {"state": "SNAPSHOTTING", "updated_unix_seconds": int(time.time())})
        snapshot(args, state, db)
        while True:
            pending = db.execute("SELECT id,description FROM packs WHERE state!='COMPLETE' ORDER BY id LIMIT 1").fetchone()
            if pending:
                pack_id, description = pending
                chunks = json.loads(description)
            else:
                chunks = []
                total = 0
                for row in db.execute("SELECT * FROM chunks WHERE pack IS NULL ORDER BY last_slot DESC LIMIT 256"):
                    chunk = describe(root, row)
                    chunks.append(chunk)
                    total += sum(f["bytes"] for f in chunk["files"])
                    if total >= args.pack_bytes:
                        break
                if not chunks:
                    break
                pack_id = db.execute("SELECT COALESCE(MAX(id),0)+1 FROM packs").fetchone()[0]
                db.execute("INSERT INTO packs VALUES(?,'PENDING',?)", (pack_id, json.dumps(chunks)))
                db.executemany("UPDATE chunks SET pack=? WHERE artifact=?",
                               ((pack_id, chunk["artifact"]) for chunk in chunks))
                db.commit()
            save(state / "STATUS.json", {"state": "PACKING", "pack": pack_id,
                                         "chunks": len(chunks), "updated_unix_seconds": int(time.time())})
            tar_path, json_path, report = build_pack(args, state, pack_id, chunks)
            copy(args, tar_path, report["destination"])
            stat = json.loads(run(args, "lsjson", report["destination"], "--stat", capture=True))
            if stat["Size"] != report["bytes"]:
                raise RuntimeError("Remote backup pack length mismatch")
            copy(args, json_path, f"{args.destination}/{json_path.name}")
            if json.loads(run(args, "cat", f"{args.destination}/{json_path.name}", capture=True)) != report:
                raise RuntimeError("Remote native backup manifest mismatch")
            db.execute("UPDATE packs SET state='COMPLETE' WHERE id=?", (pack_id,))
            db.commit()
            save(state / "STATUS.json", {"state": "UPLOADING", "last_completed_pack": pack_id,
                                         "last_pack_bytes": report["bytes"], "last_pack_sha256": report["sha256"],
                                         "updated_unix_seconds": int(time.time())})
            tar_path.unlink()
            sync_dir(tar_path.parent)
        count = db.execute("SELECT COUNT(*) FROM chunks").fetchone()[0]
        packs = db.execute("SELECT COUNT(*) FROM packs WHERE state='COMPLETE'").fetchone()[0]
        save(state / "STATUS.json", {"state": "COMPLETE", "chunks": count, "packs": packs,
                                     "canonical": False, "publication_ready": False,
                                     "snapshot_backup_only": True, "local_source_deletion": False})


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    for name in ["root", "state", "config", "destination", "rclone"]:
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--bandwidth", default="32M")
    parser.add_argument("--pack-bytes", type=int, default=256 * 1024**2)
    arguments = parser.parse_args()
    if arguments.pack_bytes <= 0:
        parser.error("--pack-bytes must be positive")
    try:
        main(arguments)
    except Exception as error:
        save(Path(arguments.state) / "STATUS.json", {"state": "FAILED", "error": str(error)})
        raise
