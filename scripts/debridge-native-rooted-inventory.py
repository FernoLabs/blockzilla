#!/usr/bin/env python3
"""Freeze and verify native rooted bank metadata without reading compact payloads.

The live SQLite connection is read-only and is closed after a streamed inventory
copy. Marker and custody checks, parent walking and output writes use only the
new task directory. R2 reads are limited to HEAD and one root commit manifest.
"""
import argparse
import datetime
import hashlib
import hmac
import json
import os
from pathlib import Path
import resource
import shlex
import sqlite3
import struct
import sys
import time
import urllib.parse
import urllib.request

MAX_META = 65536
FIELDS = ("slot", "bank_id", "blockhash", "parent_slot", "parent_blockhash")
SOURCE_FIELDS = ("session_id", "genesis_hash", "node_identity", "agave_revision")


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def save(path, value):
    temporary = path.with_suffix(".new")
    temporary.write_bytes(json.dumps(value, indent=2, sort_keys=True).encode() + b"\n")
    temporary.replace(path)


def bounded(path):
    require(not path.is_symlink(), "marker_symlink")
    with path.open("rb") as file:
        raw = file.read(MAX_META + 1)
    require(len(raw) <= MAX_META, "marker_too_large")
    return raw, json.loads(raw)


def safe_path(root, relative):
    path = root / relative
    require(path.is_relative_to(root) and ".." not in path.relative_to(root).parts, "unsafe_source_path")
    return path


def read_credentials(path):
    values = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:]
        key, value = line.split("=", 1)
        fields = shlex.split(value)
        require(len(fields) == 1 and key not in values, "invalid_credentials_file")
        values[key] = fields[0]
    return values


def r2_read(credentials, key, limit=MAX_META):
    """Signed S3 whole-object GET; no credentials are written to output."""
    endpoint = credentials.get("R2_S3_ENDPOINT", credentials.get("R2_ENDPOINT", credentials.get("AWS_ENDPOINT_URL_S3")))
    bucket = credentials.get("R2_BUCKET", credentials.get("S3_BUCKET"))
    access = credentials.get("R2_ACCESS_KEY_ID", credentials.get("AWS_ACCESS_KEY_ID"))
    secret = credentials.get("R2_SECRET_ACCESS_KEY", credentials.get("AWS_SECRET_ACCESS_KEY"))
    region = credentials.get("R2_S3_REGION", credentials.get("R2_REGION", "auto"))
    require(all((endpoint, bucket, access, secret)), "missing_r2_configuration")
    parsed = urllib.parse.urlsplit(endpoint)
    require(parsed.scheme == "https" and parsed.hostname and not parsed.query, "invalid_r2_endpoint")
    path = parsed.path.rstrip("/") + "/" + urllib.parse.quote(bucket, safe="") + "/" + urllib.parse.quote(key, safe="/")
    now = datetime.datetime.now(datetime.timezone.utc)
    stamp, day = now.strftime("%Y%m%dT%H%M%SZ"), now.strftime("%Y%m%d")
    body_hash = digest(b"")
    headers = "host:" + parsed.netloc + "\nx-amz-content-sha256:" + body_hash + "\nx-amz-date:" + stamp + "\n"
    signed = "host;x-amz-content-sha256;x-amz-date"
    request_hash = digest(("GET\n" + path + "\n\n" + headers + "\n" + signed + "\n" + body_hash).encode())
    scope = day + "/" + region + "/s3/aws4_request"
    value = ("AWS4-HMAC-SHA256\n" + stamp + "\n" + scope + "\n" + request_hash).encode()
    signing = ("AWS4" + secret).encode()
    for component in (day, region, "s3", "aws4_request"):
        signing = hmac.new(signing, component.encode(), hashlib.sha256).digest()
    authorization = "AWS4-HMAC-SHA256 Credential=" + access + "/" + scope + ", SignedHeaders=" + signed + ", Signature=" + hmac.new(signing, value, hashlib.sha256).hexdigest()
    request = urllib.request.Request(urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, path, "", "")), headers={
        "Authorization": authorization, "x-amz-content-sha256": body_hash, "x-amz-date": stamp})
    with urllib.request.urlopen(request, timeout=30) as response:
        raw = response.read(limit + 1)
    require(len(raw) <= limit, "remote_object_too_large")
    return raw


def r2_get(credentials, key):
    raw = r2_read(credentials, key)
    return raw, json.loads(raw)


def identity(value):
    result = {key: value[key] for key in FIELDS}
    for key in ("slot", "bank_id", "parent_slot"):
        require(isinstance(result[key], int) and not isinstance(result[key], bool) and result[key] >= 0, "invalid_bank_coordinate")
    for key in ("blockhash", "parent_blockhash"):
        require(isinstance(result[key], list) and len(result[key]) == 32 and all(isinstance(x, int) and not isinstance(x, bool) and 0 <= x <= 255 for x in result[key]), "invalid_bank_hash")
    return result


def source_relative(root, value):
    return str(Path(value).relative_to(root))


def root_frame(marker, event):
    control = {key: marker[key] for key in ("schema", "state", "source_profile", "session_id", "genesis_hash", "node_identity", "agave_revision", "finalized", "canonical", "publication_ready")}
    control["schema"] = "hivezilla-processed-control-v1"
    bank = {key: event["bank"][key] for key in FIELDS}
    ordered_event = {key: event[key] for key in ("sequence", "source_sequence", "kind")}
    ordered_event.update(bank=bank, payload_sha256=event["payload_sha256"], reason=event["reason"])
    envelope = {"schema": "blockzilla-stream-envelope-v1", "profile": "native-processed-compact-v1",
                "evidence_included": False, "marker": dict(sorted(control.items())), "objects": [], "event": ordered_event}
    body = json.dumps(envelope, separators=(",", ":"), ensure_ascii=False).encode()
    return b"BZSTRM01" + struct.pack("<II", len(body), 0) + body


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--credentials-file", type=Path, required=True)
    parser.add_argument("--prefix", required=True)
    parser.add_argument("--epoch", type=int, default=1056)
    args = parser.parse_args()
    if sys.platform.startswith("linux"):
        resource.setrlimit(resource.RLIMIT_AS, (256 * 1024**2, 256 * 1024**2))
    os.umask(0o077)
    root = args.source.resolve(strict=True)
    out = args.out_dir
    out.mkdir(parents=True, exist_ok=False)
    start = time.time()
    certificate = {"schema": "debridge-native-rooted-inventory-v1", "state": "STARTED",
                   "canonical": False, "publication_ready": False, "payload_bytes_verified": False,
                   "cluster_consensus_independently_verified": False, "car_previous_epoch_boundary_compared": False,
                   "frame_map_complete": False, "epoch": args.epoch}
    save(out / "STATUS.json", certificate)
    local = None
    live = None
    try:
        feed_raw, feed = bounded(root / "FEED.json")
        (out / "FEED.json").write_bytes(feed_raw)
        require(feed["profile"] == "native-processed-compact-v1" and feed["canonical"] is False and feed["publication_ready"] is False, "wrong_source_profile")
        credentials = read_credentials(args.credentials_file)
        head_raw, head = r2_get(credentials, args.prefix + "/HEAD.json")
        (out / "CLOUD-HEAD.json").write_bytes(head_raw)
        require(head["session"] == feed["session_id"] and head["profile"] == feed["profile"], "cloud_source_mismatch")
        require(head["canonical"] is False and head["publication_ready"] is False, "cloud_flags_changed")
        cursor_parts = head["latest_cursor"].split(":")
        anchor_parts = head["first_available_after"].split(":")
        require(len(cursor_parts) == len(anchor_parts) == 3 and cursor_parts[0] == anchor_parts[0] == feed["session_id"] and cursor_parts[1].startswith("p") and anchor_parts[1].startswith("p") and anchor_parts[2] == "-", "invalid_cloud_cursor")
        cloud_cut = int(cursor_parts[1][1:])
        offset = int(anchor_parts[1][1:])
        require(cloud_cut - offset == head["sequence"], "publisher_sequence_mapping_not_exact")
        require(head["start_slot"] == offset + 1 and head["last_commit"] == args.prefix + "/commits/" + str(head["sequence"]).zfill(20) + ".json", "cloud_commit_mapping_conflict")
        floor = args.epoch * 432000
        live = sqlite3.connect("file:" + str(root / "feed.sqlite") + "?mode=ro", uri=True, timeout=5)
        live.execute("PRAGMA query_only=ON")
        live.execute("PRAGMA cache_size=-4096")
        live.execute("BEGIN")
        latest = live.execute("SELECT a.slot,a.bank,b.identity,r.sequence FROM archive_catalog a JOIN banks b ON b.slot=a.slot AND b.bank=a.bank JOIN feed_events r ON r.slot=a.slot AND r.bank=a.bank AND r.kind='root' WHERE r.sequence<=? ORDER BY a.slot DESC LIMIT 1", [cloud_cut]).fetchone()
        require(latest is not None and latest[0] >= floor, "no_cloud_published_root_in_requested_epoch")
        fixed_slot, fixed_bank, fixed_identity_raw, fixed_root_event = latest
        fixed_identity = identity(json.loads(fixed_identity_raw))
        require(fixed_slot < floor + 432000, "latest_root_outside_requested_epoch")
        previous = live.execute("SELECT max(a.slot) FROM archive_catalog a JOIN feed_events r ON r.slot=a.slot AND r.bank=a.bank AND r.kind='root' WHERE a.slot<? AND r.sequence<=?", [floor, cloud_cut]).fetchone()[0]
        require(previous is not None, "no_previous_epoch_rooted_boundary")
        local = sqlite3.connect(out / "inventory.sqlite")
        local.execute("PRAGMA journal_mode=DELETE")
        local.execute("PRAGMA cache_size=-4096")
        local.execute("PRAGMA mmap_size=0")
        local.execute("CREATE TABLE banks(slot INTEGER PRIMARY KEY,bank INTEGER,parent_slot INTEGER,blockhash BLOB,parent_hash BLOB,artifact TEXT,marker_sha TEXT,custody_path TEXT,custody_sha TEXT,root_source_seq INTEGER,root_event_seq INTEGER,block_event_seq INTEGER,root_event_json TEXT,root_event_sha TEXT,block_event_sha TEXT,selected INTEGER DEFAULT 0,metadata_verified INTEGER DEFAULT 0,transactions INTEGER,signatures INTEGER,root_frame_sha TEXT)")
        query = "SELECT a.slot,a.bank,a.artifact,a.marker_sha,a.root_sequence,a.custody_path,a.custody_sha,b.identity,b.state,b.invalid_sequence,b.marker_sha,r.sequence,r.event_json,p.sequence,p.event_json FROM archive_catalog a JOIN banks b ON b.slot=a.slot AND b.bank=a.bank JOIN feed_events r ON r.slot=a.slot AND r.bank=a.bank AND r.kind='root' JOIN feed_events p ON p.slot=a.slot AND p.bank=a.bank AND p.kind='block' WHERE (a.slot BETWEEN ? AND ? OR a.slot=?) AND r.sequence<=? ORDER BY a.slot"
        copied = 0
        for row in live.execute(query, [floor, fixed_slot, previous, cloud_cut]):
            slot, bank, artifact, marker_sha, root_source_seq, proof_path, proof_sha, raw_id, state, invalid, bank_marker, rseq, rjson, pseq, pjson = row
            bank_id = identity(json.loads(raw_id))
            revent, pevent = json.loads(rjson), json.loads(pjson)
            require(state == "rooted" and invalid == 0 and bank_marker == marker_sha, "rooted_bank_state_conflict")
            require(bank_id["slot"] == slot and bank_id["bank_id"] == bank, "catalog_identity_conflict")
            require(identity(revent["bank"]) == bank_id == identity(pevent["bank"]), "event_bank_identity_conflict")
            require(revent["kind"] == "root" and pevent["kind"] == "block" and revent["sequence"] == rseq and pevent["sequence"] == pseq and offset < pseq < rseq <= cloud_cut, "event_order_conflict")
            require(revent["source_sequence"] == root_source_seq and revent["payload_sha256"] == marker_sha == pevent["payload_sha256"], "root_payload_marker_conflict")
            local.execute("INSERT INTO banks(slot,bank,parent_slot,blockhash,parent_hash,artifact,marker_sha,custody_path,custody_sha,root_source_seq,root_event_seq,block_event_seq,root_event_json,root_event_sha,block_event_sha) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", [slot, bank, bank_id["parent_slot"], bytes(bank_id["blockhash"]), bytes(bank_id["parent_blockhash"]), source_relative(root, artifact), marker_sha, source_relative(root, proof_path), proof_sha, root_source_seq, rseq, pseq, rjson, digest(rjson.encode()), digest(pjson.encode())])
            copied += 1
            if copied % 5000 == 0:
                local.commit()
                print(json.dumps({"phase": "copy", "banks": copied}), flush=True)
        local.commit()
        live.rollback()
        live.close()
        live = None
        certificate.update(fixed_root=fixed_identity, fixed_root_event_sequence=fixed_root_event,
                           frozen_cloud_event_sequence=cloud_cut, publisher_sequence_offset=offset,
                           source={field: feed[field] for field in SOURCE_FIELDS}, source_root=str(root),
                           snapshot_banks=copied, snapshot_seconds=time.time() - start,
                           cloud_head_sha256=digest(head_raw), feed_sha256=digest(feed_raw), state="VERIFYING")
        save(out / "STATUS.json", certificate)
        current = fixed_slot
        selected = 0
        total_transactions = total_signatures = 0
        first_identity = None
        while current >= floor:
            row = local.execute("SELECT slot,bank,parent_slot,blockhash,parent_hash,artifact,marker_sha,custody_path,custody_sha,root_source_seq,root_event_seq,block_event_seq,root_event_json FROM banks WHERE slot=?", [current]).fetchone()
            if row is None:
                certificate["missing_parents"] = [current]
            require(row is not None, "missing_parent_slot:" + str(current))
            slot, bank, parent_slot, blockhash, parent_hash, artifact, marker_sha, proof_path, proof_sha, root_source_seq, rseq, pseq, rjson = row
            expected = {"slot": slot, "bank_id": bank, "blockhash": list(blockhash), "parent_slot": parent_slot, "parent_blockhash": list(parent_hash)}
            marker_path = safe_path(root, artifact) / "DIRECT-AGAVE-COMPACT.v1.json"
            raw_marker, marker = bounded(marker_path)
            raw_proof, proof = bounded(safe_path(root, proof_path))
            require(digest(raw_marker) == marker_sha and digest(raw_proof) == proof_sha, "marker_or_custody_digest_conflict:" + str(slot))
            require(all(marker[field] == feed[field] for field in SOURCE_FIELDS), "marker_source_conflict")
            require(marker["schema"] == "hivezilla-direct-agave-processed-compact-v1" and marker["state"] == "COMPLETE" and marker["finalized"] is False and marker["canonical"] is False and marker["publication_ready"] is False, "invalid_processed_marker")
            require(len(marker["produced_blocks"]) == 1 and identity(marker["produced_blocks"][0]) == expected, "marker_bank_conflict")
            require(proof["schema"] == "hivezilla-direct-agave-processed-custody-v1" and proof["state"] == "COMPLETE" and proof["finalized"] is True and proof["canonical"] is False and proof["session_id"] == feed["session_id"], "invalid_rooted_custody")
            require(proof["payload_marker_sha256"] == marker_sha and Path(proof["payload_marker_path"]) == marker_path and proof["through_sequence"] >= root_source_seq, "custody_payload_or_root_conflict")
            require(len(proof["produced_blocks"]) == 1 and identity(proof["produced_blocks"][0]) == expected, "custody_bank_conflict")
            require(proof["produced_blocks"] == marker["produced_blocks"], "custody_block_counts_conflict")
            frame_sha = digest(root_frame(marker, json.loads(rjson)))
            local.execute("UPDATE banks SET selected=1,metadata_verified=1,transactions=?,signatures=?,root_frame_sha=? WHERE slot=?", [marker["transactions"], marker["signatures"], frame_sha, slot])
            total_transactions += marker["transactions"]
            total_signatures += marker["signatures"]
            selected += 1
            require(parent_slot < slot, "parent_slot_not_decreasing")
            parent = local.execute("SELECT blockhash FROM banks WHERE slot=?", [parent_slot]).fetchone()
            if parent is None:
                certificate["missing_parents"] = [parent_slot]
            require(parent is not None and parent[0] == parent_hash, "missing_or_changed_parent:" + str(parent_slot))
            first_identity = expected
            current = parent_slot
            if selected % 5000 == 0:
                local.commit()
                certificate.update(verified_marker_pairs=selected, verification_next_slot=current)
                save(out / "STATUS.json", certificate)
                print(json.dumps({"phase": "verify", "banks": selected, "slot": slot}), flush=True)
        local.commit()
        extra = local.execute("SELECT count(*) FROM banks WHERE slot>=? AND selected=0", [floor]).fetchone()[0]
        require(extra == 0, "rooted_catalog_contains_banks_outside_selected_ancestry")
        root_manifest_key = args.prefix + "/commits/" + str(fixed_root_event - offset).zfill(20) + ".json"
        manifest_raw, manifest = r2_get(credentials, root_manifest_key)
        (out / "ROOT-COMMIT.json").write_bytes(manifest_raw)
        root_sha = local.execute("SELECT root_frame_sha FROM banks WHERE slot=?", [fixed_slot]).fetchone()[0]
        root_key = args.prefix + "/chunks/" + str(fixed_slot) + "-" + root_sha + ".frame"
        require(manifest["schema"] == "blockzilla-native-stream-commit-v1" and manifest["session"] == feed["session_id"] and manifest["profile"] == feed["profile"] and manifest["sequence"] == fixed_root_event - offset and manifest["canonical"] is False and manifest["publication_ready"] is False and all(manifest[field] == feed[field] for field in SOURCE_FIELDS[1:]), "root_commit_provenance_conflict")
        require(manifest["sha256"] == root_sha and manifest["object"] == root_key and manifest["event"]["kind"] == "root" and identity(manifest["event"]["bank"]) == fixed_identity, "root_frame_reconstruction_or_cloud_commit_conflict")
        requests_path = out / "frame-requests.jsonl"
        with requests_path.open("wb") as file:
            for slot, bank, marker_sha, pseq, rseq, root_sha in local.execute("SELECT slot,bank,marker_sha,block_event_seq,root_event_seq,root_frame_sha FROM banks WHERE selected=1 ORDER BY slot"):
                file.write(canonical({"slot": slot, "bank_id": bank, "payload_marker_sha256": marker_sha,
                                      "block_event_sequence": pseq, "block_commit_key": args.prefix + "/commits/" + str(pseq - offset).zfill(20) + ".json",
                                      "frame_key": None, "frame_sha256": None,
                                      "root_event_sequence": rseq, "root_frame_key": args.prefix + "/chunks/" + str(slot) + "-" + root_sha + ".frame"}) + b"\n")
        hasher = hashlib.sha256()
        with requests_path.open("rb") as file:
            while chunk := file.read(1024**2):
                hasher.update(chunk)
        certificate.update(state="PASSED", recorded_rooted_chain_complete=True, first_bank=first_identity,
                           previous_epoch_parent={"slot": current, "blockhash": first_identity["parent_blockhash"]},
                           selected_rooted_banks=selected, verified_marker_pairs=selected, transactions=total_transactions,
                           verification_next_slot=current,
                           signatures=total_signatures, slot_span=fixed_slot - floor + 1,
                           skipped_slots_in_recorded_chain=fixed_slot - floor + 1 - selected, missing_parents=[],
                           block_frame_keys_need_commit_resolution=True, frame_requests_file=requests_path.name,
                           frame_requests_sha256=hasher.hexdigest(), frame_requests_bytes=requests_path.stat().st_size,
                           root_frame_reconstruction_verified_against_cloud_commit=True,
                           limits=["Node-recorded rooted ancestry, not independent cluster consensus.", "Compact payload bytes were not read in this proof.", "CAR previous-epoch boundary comparison remains pending."])
        local.close()
        local = None
        hasher = hashlib.sha256()
        with (out / "inventory.sqlite").open("rb") as file:
            while chunk := file.read(1024**2):
                hasher.update(chunk)
        certificate["inventory_sha256"] = hasher.hexdigest()
    except Exception as error:
        certificate.update(state="FAILED", recorded_rooted_chain_complete=False, error=str(error), error_type=type(error).__name__)
        raise
    finally:
        if live is not None:
            live.close()
        if local is not None:
            local.close()
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        certificate.update(elapsed_seconds=time.time() - start, peak_rss_kib=rss // 1024 if sys.platform == "darwin" else rss)
        save(out / "STATUS.json", certificate)
        print(json.dumps(certificate), flush=True)


if __name__ == "__main__":
    main()
