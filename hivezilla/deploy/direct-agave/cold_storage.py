#!/usr/bin/env python3
"""Archive exact rooted compact files and custody to R2 before local retirement."""
import argparse
import fcntl
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import shlex
import shutil
import sqlite3
import stat
import tarfile
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

MAX_FILE = 256 * 1024**2
MAX_BUNDLE = 512 * 1024**2
SOURCE_KEYS = ("genesis_hash", "node_identity", "agave_revision", "session_id")


def digest(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while b := f.read(1024**2):
            h.update(b)
    return h.hexdigest()


def atomic(path, value):
    temp = path.with_name(path.name + ".new")
    with open(temp, "w") as f:
        json.dump(value, f)
        f.flush()
        os.fsync(f.fileno())
    temp.chmod(0o600)
    os.replace(temp, path)
    fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def checked(root, path):
    path = Path(path)
    rel = path.relative_to(root)
    if not rel.parts or any(p in ("..", ".") for p in rel.parts):
        raise RuntimeError("unsafe_archive_path")
    current = root
    for part in rel.parts:
        current = current / part
        if current.is_symlink():
            raise RuntimeError("symlink_archive_path")
    return path


def identity(path):
    s = path.lstat()
    if not stat.S_ISREG(s.st_mode) or s.st_size > MAX_FILE:
        raise RuntimeError("invalid_archive_file")
    return [s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns]


def read_json(path):
    if path.is_symlink() or path.stat().st_size > 1024**2:
        raise RuntimeError("metadata_too_large")
    return json.loads(path.read_text())


def replay_base(path, source, bank=False):
    if path.is_symlink() or path.stat().st_size > 64 * 1024**2:
        raise RuntimeError("invalid_replay_checkpoint")
    cp = json.loads(path.read_text())
    if bank and cp["source"] != source:
        raise RuntimeError("replay_checkpoint_source_mismatch")
    session, position, _ = cp["base"].split(":")
    if session != source["session_id"] or not position.startswith("p"):
        raise RuntimeError("replay_checkpoint_source_mismatch")
    return int(position[1:])


class R2:
    def __init__(self, credentials):
        import boto3
        from botocore.config import Config
        values = {}
        for line in credentials.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[7:]
            k, v = line.split("=", 1)
            parts = shlex.split(v)
            if len(parts) != 1 or k in values:
                raise RuntimeError("invalid_credentials_file")
            values[k] = parts[0]
        endpoint = values.get("R2_S3_ENDPOINT", values.get("R2_ENDPOINT", values.get("AWS_ENDPOINT_URL_S3")))
        self.bucket = values.get("R2_BUCKET", values.get("S3_BUCKET"))
        if not endpoint or not self.bucket or not endpoint.startswith("https://"):
            raise RuntimeError("missing_r2_destination")
        self.namespace = hashlib.sha256((endpoint + "\0" + self.bucket).encode()).hexdigest()
        self.client = boto3.client("s3", endpoint_url=endpoint,
            region_name=values.get("R2_S3_REGION", values.get("R2_REGION", "auto")),
            aws_access_key_id=values.get("R2_ACCESS_KEY_ID", values.get("AWS_ACCESS_KEY_ID")),
            aws_secret_access_key=values.get("R2_SECRET_ACCESS_KEY", values.get("AWS_SECRET_ACCESS_KEY")),
            config=Config(connect_timeout=10, read_timeout=60, retries={"max_attempts": 3},
                          request_checksum_calculation="when_required", response_checksum_validation="when_required"))

    def put_verified(self, key, path, sha):
        from botocore.exceptions import ClientError
        try:
            with open(path, "rb") as f:
                self.client.put_object(Bucket=self.bucket, Key=key, Body=f,
                    ContentLength=path.stat().st_size, IfNoneMatch="*", Metadata={"sha256": sha})
        except ClientError as e:
            if e.response.get("ResponseMetadata", {}).get("HTTPStatusCode") != 412:
                raise
        h, count = hashlib.sha256(), 0
        response = self.client.get_object(Bucket=self.bucket, Key=key)
        with response["Body"] as body:
            while b := body.read(1024**2):
                count += len(b)
                if count > MAX_BUNDLE:
                    raise RuntimeError("remote_bundle_too_large")
                h.update(b)
        if count != path.stat().st_size or h.hexdigest() != sha:
            raise RuntimeError("remote_bundle_readback_mismatch")

    def download(self, key, destination, sha):
        response = self.client.get_object(Bucket=self.bucket, Key=key)
        count = 0
        with response["Body"] as body, open(destination, "xb") as f:
            while b := body.read(1024**2):
                count += len(b)
                if count > MAX_BUNDLE:
                    raise RuntimeError("remote_bundle_too_large")
                f.write(b)
            f.flush()
            os.fsync(f.fileno())
        if digest(destination) != sha:
            raise RuntimeError("remote_bundle_readback_mismatch")


def database(state, source, namespace):
    state.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(state / "archive.sqlite", timeout=30)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=FULL")
    db.executescript("CREATE TABLE IF NOT EXISTS meta(id INTEGER PRIMARY KEY,value TEXT);"
                     "CREATE TABLE IF NOT EXISTS archives(slot INTEGER,bank INTEGER,record TEXT,retired INTEGER DEFAULT 0,PRIMARY KEY(slot,bank));")
    expected = json.dumps({"source": source, "namespace": namespace}, sort_keys=True)
    db.execute("INSERT OR IGNORE INTO meta VALUES(1,?)", (expected,))
    if db.execute("SELECT value FROM meta WHERE id=1").fetchone()[0] != expected:
        raise RuntimeError("archive_source_or_destination_changed")
    db.commit()
    return db


def candidates(feed, after, scan_after=0):
    # Match the durable cloud index in SQLite. Do not rescan every already
    # archived bank for each small bundle as retained history grows.
    return feed.execute("SELECT a.slot,a.bank,a.artifact,a.marker_sha,a.custody_path,a.custody_sha,"
        "(SELECT max(e.sequence) FROM feed_events e WHERE e.slot=a.slot AND e.bank=a.bank) "
        "FROM archive_catalog a JOIN banks b ON b.slot=a.slot AND b.bank=a.bank "
        "WHERE b.state='rooted' AND b.dirty=0 AND a.retirement_enqueued=1 AND a.slot>? "
        "AND (SELECT max(e.sequence) FROM feed_events e WHERE e.slot=a.slot AND e.bank=a.bank)<? "
        "AND NOT EXISTS(SELECT 1 FROM cold.archives c WHERE c.slot=a.slot AND c.bank=a.bank) "
        "ORDER BY a.slot LIMIT 128", (scan_after, after)).fetchall()


def build_bank(root, row, source):
    slot, bank, artifact, marker_sha, proof_path, proof_sha, _ = row
    artifact, proof_path = checked(root, artifact), checked(root, proof_path)
    marker_path = artifact / "DIRECT-AGAVE-COMPACT.v1.json"
    marker, proof = read_json(marker_path), read_json(proof_path)
    if digest(marker_path) != marker_sha or digest(proof_path) != proof_sha:
        raise RuntimeError("source_marker_changed")
    if any(marker.get(k) != source[k] for k in SOURCE_KEYS) or proof["session_id"] != source["session_id"]:
        raise RuntimeError("source_identity_mismatch")
    if proof["state"] != "COMPLETE" or not proof["finalized"] or proof["payload_marker_sha256"] != marker_sha:
        raise RuntimeError("custody_not_complete")
    if Path(proof["payload_marker_path"]) != marker_path:
        raise RuntimeError("custody_payload_path_mismatch")
    for b in [marker["produced_blocks"][0], proof["produced_blocks"][0]]:
        if b["slot"] != slot or b["bank_id"] != bank:
            raise RuntimeError("custody_bank_mismatch")
    files = []
    for base, header, name in [(artifact, marker, "DIRECT-AGAVE-COMPACT.v1.json"),
                               (proof_path.parent, proof, proof_path.name)]:
        entries = header["files"] + [{"path": name, "bytes": (base / name).stat().st_size,
                                      "sha256": digest(base / name), "keep": True}]
        for entry in entries:
            if len(PurePosixPath(entry["path"]).parts) != 1 or entry["path"] in (".", ".."):
                raise RuntimeError("unsafe_manifest_name")
            path = checked(root, base / entry["path"])
            before = identity(path)
            sha = digest(path)
            if before != identity(path) or before[2] != entry["bytes"] or sha != entry["sha256"]:
                raise RuntimeError("source_file_changed")
            files.append({"path": str(path.relative_to(root)), "bytes": before[2],
                          "sha256": sha, "stamp": before, "keep": entry.get("keep", False)})
    return {"slot": slot, "bank": bank, "artifact": str(artifact.relative_to(root)),
            "custody": str(proof_path.parent.relative_to(root)), "source": source,
            "through_sequence": proof["through_sequence"], "files": files}


def pack(root, records, path):
    manifest = json.dumps({"schema": "blockzilla-native-custody-bundle-v1", "banks": records}).encode()
    if sum(f["bytes"] for r in records for f in r["files"]) + len(manifest) + 1024**2 > MAX_BUNDLE:
        raise RuntimeError("bundle_too_large")
    with tarfile.open(path, "w") as tar:
        info = tarfile.TarInfo("BUNDLE.json")
        info.size = len(manifest)
        info.mode = 0o600
        tar.addfile(info, io.BytesIO(manifest))
        for r in records:
            for entry in r["files"]:
                p = checked(root, root / entry["path"])
                if identity(p) != entry["stamp"]:
                    raise RuntimeError("source_changed_before_bundle")
                info = tarfile.TarInfo(entry["path"])
                info.size, info.mode = entry["bytes"], 0o600
                with open(p, "rb") as f:
                    tar.addfile(info, f)
                if identity(p) != entry["stamp"]:
                    raise RuntimeError("source_changed_during_bundle")
    with open(path, "rb") as f:
        os.fsync(f.fileno())


def retire(root, db, record):
    # Preserve small native markers. The full bytes and their file hashes are in
    # the verified remote bundle. Restore can recreate exact original paths.
    note = {k: record[k] for k in ["source", "slot", "bank", "key", "bundle_sha256", "namespace", "files"]}
    note["schema"] = "blockzilla-native-cold-copy-v1"
    for folder in [record["artifact"], record["custody"]]:
        atomic(checked(root, root / folder) / "REMOTE-CUSTODY.json", note)
    removed = 0
    changed_directories = set()
    for entry in record["files"]:
        if entry["keep"]:
            continue
        path = checked(root, root / entry["path"])
        if not path.exists():
            continue
        if digest(path) != entry["sha256"]:
            raise RuntimeError("source_changed_before_retirement")
        # A newly restored copy has another inode. Leave it available to its reader.
        if identity(path) != entry["stamp"]:
            continue
        removed += path.stat().st_blocks * 512
        path.unlink()
        changed_directories.add(path.parent)
    # One directory sync makes all of this bank's unlinks durable. Commit the
    # retired flag only after both artifact/custody directories are synced.
    for parent in changed_directories:
        fd = os.open(parent, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    db.execute("UPDATE archives SET retired=1 WHERE slot=? AND bank=?", (record["slot"], record["bank"]))
    db.commit()
    return removed


def upload_bundle(root, remote, records, path):
    pack(root, records, path)
    sha = digest(path)
    key = "native-custody/processed-v1/" + records[0]["source"]["session_id"] + "/" + sha + ".tar"
    remote.put_verified(key, path, sha)
    return sha, key


def commit_bundle(root, db, remote, records, path, sha, key, retire_files):
    for record in records:
        record.update(key=key, bundle_sha256=sha, namespace=remote.namespace)
        db.execute("INSERT INTO archives(slot,bank,record) VALUES(?,?,?)",
                   (record["slot"], record["bank"], json.dumps(record)))
    db.commit()  # Recovery index commits strictly after full remote readback.
    freed = sum(retire(root, db, r) for r in records) if retire_files else 0
    path.unlink()
    return freed


def archive_group(root, db, remote, records, state, retire_files):
    path = state / "bundle.tar"
    sha, key = upload_bundle(root, remote, records, path)
    return commit_bundle(root, db, remote, records, path, sha, key, retire_files)


class Transfers:
    """Only network/packing runs in threads. SQLite and retirement have one owner.

    The client is created before threads start, per the Boto3 client guidance:
    https://docs.aws.amazon.com/boto3/latest/guide/clients.html
    """
    def __init__(self, root, db, remote, state, retire_files, limit):
        self.root, self.db, self.remote, self.state = root, db, remote, state
        self.retire_files, self.limit = retire_files, limit
        self.pool = ThreadPoolExecutor(max_workers=limit)
        self.pending = {}

    def pending_banks(self):
        return {(r['slot'], r['bank']) for data in self.pending.values() for r in data['records']}

    def submit(self, records):
        used = {data['number'] for data in self.pending.values()}
        number = next(n for n in range(self.limit) if n not in used)
        path = self.state / f'bundle-{number}.tar'
        future = self.pool.submit(upload_bundle, self.root, self.remote, records, path)
        self.pending[future] = dict(number=number, path=path, records=records, started=time.monotonic())

    def collect(self, block=False):
        if block and self.pending:
            wait(self.pending, timeout=1, return_when=FIRST_COMPLETED)
        committed = []
        for future in list(self.pending):
            if not future.done():
                continue
            data = self.pending[future]
            sha, key = future.result()  # A failed verification has no SQLite or deletion side effects.
            freed = commit_bundle(self.root, self.db, self.remote, data['records'], data['path'],
                                  sha, key, self.retire_files)
            committed.append(dict(banks=len(data['records']), freed=freed,
                                  seconds=time.monotonic() - data['started']))
            del self.pending[future]
        return committed

    def close(self):
        self.pool.shutdown(wait=True)


def restore(root, requested, state, remote):
    requested = checked(root, requested)
    parent = requested.parent
    note = read_json(parent / "REMOTE-CUSTODY.json")
    if note["schema"] != "blockzilla-native-cold-copy-v1" or note["namespace"] != remote.namespace:
        raise RuntimeError("invalid_cold_copy_source")
    header = read_json(root / "FEED.json")
    if any(note["source"].get(k) != header.get(k) for k in SOURCE_KEYS):
        raise RuntimeError("restore_source_mismatch")
    if not note["key"].startswith("native-custody/processed-v1/" + note["source"]["session_id"] + "/"):
        raise RuntimeError("invalid_cold_copy_key")
    if str(requested.relative_to(root)) not in {f["path"] for f in note["files"]}:
        raise RuntimeError("requested_path_not_archived")
    state.mkdir(parents=True, exist_ok=True)
    temporary = state / ("restore-" + str(os.getpid()) + ".tar")
    try:
        remote.download(note["key"], temporary, note["bundle_sha256"])
        with tarfile.open(temporary, "r") as tar:
            all_members = tar.getmembers()
            members = {member.name: member for member in all_members}
            if len(members) != len(all_members):
                raise RuntimeError("duplicate_archive_members")
            for entry in note["files"]:
                member = members.get(entry["path"])
                if member is None or not member.isfile() or member.size != entry["bytes"]:
                    raise RuntimeError("archive_member_mismatch")
                path = checked(root, root / entry["path"])
                if path.exists():
                    if digest(path) != entry["sha256"]:
                        raise RuntimeError("restore_conflicts_with_local_file")
                    continue
                stage = path.with_name(path.name + ".restore-" + str(os.getpid()))
                try:
                    with tar.extractfile(member) as source, open(stage, "xb") as f:
                        shutil.copyfileobj(source, f, 1024**2)
                        f.flush()
                        os.fsync(f.fileno())
                    if digest(stage) != entry["sha256"]:
                        raise RuntimeError("archive_member_digest_mismatch")
                    stage.chmod(0o600)
                    # Hard-link publication never overwrites another reader's copy.
                    try:
                        os.link(stage, path)
                    except FileExistsError:
                        if digest(path) != entry["sha256"]:
                            raise RuntimeError("restore_conflicts_with_local_file")
                    fd = os.open(path.parent, os.O_RDONLY)
                    try:
                        os.fsync(fd)
                    finally:
                        os.close(fd)
                finally:
                    stage.unlink(missing_ok=True)
    finally:
        temporary.unlink(missing_ok=True)


def archive(a):
    root = a.output.resolve(strict=True)
    header = read_json(root / "FEED.json")
    source = {k: header[k] for k in SOURCE_KEYS}
    bank_header = read_json(a.bank_output / "FEED.json")
    if any(bank_header[k] != source[k] for k in SOURCE_KEYS) or Path(bank_header["payload_root"]).resolve() != root:
        raise RuntimeError("bank_source_mismatch")
    remote = R2(a.credentials_file)
    state = root / "cold-storage"
    db = database(state, source, remote.namespace)
    lock = open(state / "LOCK", "a")
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    feed = sqlite3.connect("file:" + str(root / "feed.sqlite") + "?mode=ro", uri=True, timeout=30)
    feed.execute("ATTACH DATABASE ? AS cold", ("file:" + str(state / "archive.sqlite") + "?mode=ro",))
    banks = sqlite3.connect("file:" + str(a.bank_output / "feed.sqlite") + "?mode=ro", uri=True, timeout=30)
    capture = sqlite3.connect("file:" + str(a.capture / "events.sqlite") + "?mode=ro", uri=True, timeout=30)
    retired, freed, batches, scan_after = 0, 0, 0, 0
    transfers = Transfers(root, db, remote, state, a.retire, a.parallel_bundles)
    submitted_once = False
    batch_seconds, last_batch_banks = None, 0
    while True:
        for done in transfers.collect(block=bool(transfers.pending)):
            freed += done['freed']
            retired += done['banks']
            batches += 1
            batch_seconds, last_batch_banks = done['seconds'], done['banks']
        if a.retire:
            for raw, in db.execute("SELECT record FROM archives WHERE retired=0").fetchall():
                freed += retire(root, db, json.loads(raw))
        cp = read_json(a.publisher_state / "CHECKPOINT.json")
        if Path(cp["source"]).resolve() != root or cp["session"] != source["session_id"]:
            raise RuntimeError("frame_publisher_source_mismatch")
        session, position, _ = cp["cursor"].split(":")
        if session != source["session_id"] or not position.startswith("p"):
            raise RuntimeError("frame_publisher_cursor_mismatch")
        after = min(int(position[1:]), banks.execute("SELECT processed_sequence FROM feed_state WHERE id=1").fetchone()[0])
        after = min(after, replay_base(a.v1_spool / "CHECKPOINT.json", source))
        bank_base = replay_base(a.bank_origin_spool / "CHECKPOINT.json", source, bank=True)
        bounded_done = bool(a.max_batches and batches + len(transfers.pending) >= a.max_batches)
        can_submit = (len(transfers.pending) < a.parallel_bundles and not bounded_done
                      and (a.follow or not submitted_once))
        rows = candidates(feed, after, scan_after) if can_submit else []
        if can_submit and not rows:
            scan_after = 0
        records = []
        group_bytes = 0
        for row in rows:
            previous_scan = scan_after
            scan_after = row[0]
            if row[6] is None or row[6] >= after:
                continue
            if row[:2] in transfers.pending_banks():
                continue
            if db.execute("SELECT 1 FROM archives WHERE slot=? AND bank=?", row[:2]).fetchone():
                continue
            marker = Path(row[2]) / "DIRECT-AGAVE-COMPACT.v1.json"
            if time.time() - marker.stat().st_mtime < a.keep_seconds:
                continue
            last_bank_event = banks.execute("SELECT max(sequence) FROM feed_events WHERE artifact=?", (row[2],)).fetchone()[0]
            if last_bank_event is None or last_bank_event >= bank_base:
                continue
            record = build_bank(root, row, source)
            if capture.execute("SELECT 1 FROM events WHERE slot=? AND bank=? AND sequence<=? LIMIT 1",
                               (row[0], row[1], record["through_sequence"])).fetchone():
                continue  # Wait for native writer acknowledgement, never delete its proof first.
            record_bytes = sum(f["bytes"] for f in record["files"])
            if group_bytes + record_bytes + 1024**2 > MAX_BUNDLE:
                if not records:
                    raise RuntimeError("single_bank_exceeds_bundle_bound")
                scan_after = previous_scan
                break
            records.append(record)
            group_bytes += record_bytes
            if len(records) >= a.batch_banks:
                break
        if records:
            transfers.submit(records)
            submitted_once = True
        status = {"schema": "blockzilla-native-cold-storage-status-v1", "state": "RUNNING",
                  "archived_banks": db.execute("SELECT count(*) FROM archives").fetchone()[0],
                  "retired_banks": db.execute("SELECT count(*) FROM archives WHERE retired=1").fetchone()[0],
                  "freed_bytes_this_run": freed, "keep_seconds": a.keep_seconds,
                  "batch_bank_limit": a.batch_banks,
                  "parallel_bundle_limit": a.parallel_bundles, "pending_bundles": len(transfers.pending),
                  "last_batch_banks": last_batch_banks, "last_batch_seconds": batch_seconds,
                  "full_remote_readback_required": True, "updated_unix_ms": int(time.time() * 1000)}
        atomic(state / "STATUS.json", status)
        if not transfers.pending and ((a.max_batches and batches >= a.max_batches) or not a.follow):
            transfers.close()
            print(json.dumps(status))
            return
        if not records and not transfers.pending:
            time.sleep(15)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    modes = p.add_subparsers(dest="mode", required=True)
    put = modes.add_parser("archive")
    for name in ["output", "bank-output", "capture", "publisher-state", "credentials-file", "v1-spool", "bank-origin-spool"]:
        put.add_argument("--" + name, type=Path, required=True)
    put.add_argument("--keep-seconds", type=int, default=7200)
    put.add_argument("--retire", action="store_true")
    put.add_argument("--follow", action="store_true")
    put.add_argument("--max-batches", type=int)
    put.add_argument("--batch-banks", type=int, choices=range(1, 129), default=64)
    put.add_argument("--parallel-bundles", type=int, choices=[1, 2, 4], default=2)
    get = modes.add_parser("restore")
    get.add_argument("--output", type=Path, required=True)
    get.add_argument("--path", type=Path, required=True)
    get.add_argument("--credentials-file", type=Path,
                     default=Path(os.environ.get("HIVEZILLA_NATIVE_ARCHIVE_CREDS", "/missing-credentials")))
    a = p.parse_args()
    os.umask(0o077)
    if a.mode == "archive":
        if a.keep_seconds < 7200 or a.max_batches is not None and a.max_batches < 1:
            p.error("retain at least two hours; --max-batches must be positive")
        archive(a)
    else:
        root = a.output.resolve(strict=True)
        restore(root, a.path, root / "cold-storage", R2(a.credentials_file))
