#!/usr/bin/env python3
"""One-off candidate scan of a frozen native rooted inventory; default pilot: 100 banks."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import resource
import sqlite3
import struct
import subprocess
import sys
import time

spec = importlib.util.spec_from_file_location("native_inventory", Path(__file__).with_name("debridge-native-rooted-inventory.py"))
inventory = importlib.util.module_from_spec(spec)
spec.loader.exec_module(inventory)
require, digest, canonical, save = inventory.require, inventory.digest, inventory.canonical, inventory.save
STATE = "73dUbDAWUDZRfXM3DcuRgR4q74nr97MyuBt95738JnMz"
PROGRAMS = {"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA", "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"}
MAX_FRAME = 16 * 1024**2 + 65536 + 16
MAX_OUTPUT = 32 * 1024**2


def file_sha(path):
    value = hashlib.sha256()
    with path.open("rb") as file:
        while chunk := file.read(1024**2):
            value.update(chunk)
    return value.hexdigest()


def base58(data):
    alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    n = int.from_bytes(bytes(data), "big")
    result = ""
    while n:
        n, remainder = divmod(n, 58)
        result = alphabet[remainder] + result
    return "1" * (len(data) - len(bytes(data).lstrip(b"\0"))) + result


def byte_list(value):
    require(isinstance(value, list) and all(isinstance(v, int) and not isinstance(v, bool) and 0 <= v <= 255 for v in value), "invalid_instruction_bytes")
    return bytes(value)


def children(accounts, data):
    if not data or data[0] != 255:
        yield -1, accounts, data
        return
    require(len(data) > 1 and len(data) <= 65535 and len(accounts) <= 65535, "invalid_token_batch_geometry")
    position, account_position, batch_ix = 1, 0, 0
    while position < len(data):
        require(position + 2 <= len(data), "truncated_batch_header")
        count, size = data[position:position + 2]
        position += 2
        require(size > 0 and position + size <= len(data) and account_position + count <= len(accounts), "truncated_batch_child")
        child = data[position:position + size]
        require(child[0] != 255, "nested_token_batch")
        yield batch_ix, accounts[account_position:account_position + count], child
        position += size
        account_position += count
        batch_ix += 1


def extract(tx):
    require(tx["success"] is True, "selected_transaction_not_successful")
    keys = tx["accountKeys"]
    require(isinstance(keys, list) and len(keys) <= 256, "invalid_resolved_accounts")
    if STATE not in keys or not PROGRAMS.intersection(keys):
        return []
    message = tx["message"]
    require(isinstance(message, dict) and len(message) == 1, "invalid_message_variant")
    kind, body = next(iter(message.items()))
    require(kind in ("Legacy", "V0", "V1"), "unknown_message_variant")
    outer = body["instructions"]
    groups = tx["metadata"]["inner_instructions"]
    require(tx["innerCoverage"] == "recorded" and isinstance(groups, list), "selected_state_token_transaction_cpi_unavailable")
    found = []

    def visit(ix, outer_ix, inner_ix, data):
        index = ix["program_id_index"]
        require(isinstance(index, int) and not isinstance(index, bool) and 0 <= index < len(keys), "invalid_program_index")
        program = keys[index]
        if program not in PROGRAMS:
            return
        accounts = byte_list(ix["accounts"])
        require(all(v < len(keys) for v in accounts), "invalid_account_index")
        for batch_ix, child_accounts, child in children(accounts, data):
            if not child or child[0] not in (4, 13):
                continue
            tag = child[0]
            require(len(child) >= (9 if tag == 4 else 10), "truncated_approve_data")
            delegate_position, authority_position, needed = (1, 2, 3) if tag == 4 else (2, 3, 4)
            require(len(child_accounts) >= needed, "truncated_approve_accounts")
            if keys[child_accounts[delegate_position]] != STATE:
                continue
            found.append({"tx_index": tx["index"], "signature": tx["signature"], "signatures": tx["signatures"],
                          "outer_ix": outer_ix, "inner_ix": inner_ix, "batch_ix": batch_ix,
                          "token_account": keys[child_accounts[0]], "token_program": program, "delegate": STATE,
                          "authority": keys[child_accounts[authority_position]], "instruction_tag": tag,
                          "amount": str(int.from_bytes(child[1:9], "little")),
                          "mint": keys[child_accounts[1]] if tag == 13 else None,
                          "decimals": child[9] if tag == 13 else None,
                          "instruction_data_hex": child.hex(), "account_indices_hex": child_accounts.hex(),
                          "parent_instruction_data_hex": data.hex() if batch_ix >= 0 else None,
                          "parent_account_indices_hex": accounts.hex() if batch_ix >= 0 else None,
                          "stack_height": ix.get("stack_height"), "resolved_account_keys": keys,
                          "cpi_execution_independently_verified": inner_ix < 0,
                          "candidate_superset_requires_current_account_read": True})

    for outer_ix, ix in enumerate(outer):
        index = ix["program_id_index"]
        require(isinstance(index, int) and not isinstance(index, bool) and 0 <= index < len(keys), "invalid_outer_program_index")
        if keys[index] in PROGRAMS:
            require(isinstance(ix["data"], dict) and set(ix["data"]) == {"Raw"}, "token_instruction_raw_bytes_unavailable")
            visit(ix, outer_ix, -1, byte_list(ix["data"]["Raw"]))
    seen = set()
    for group in groups:
        index = group["index"]
        require(isinstance(index, int) and not isinstance(index, bool) and 0 <= index < len(outer) and index not in seen, "invalid_cpi_group")
        seen.add(index)
        for inner_ix, ix in enumerate(group["instructions"]):
            visit(ix, index, inner_ix, byte_list(ix["data"]))
    return found


def exact_read(file, size):
    parts = bytearray()
    while len(parts) < size:
        piece = file.read(size - len(parts))
        require(piece, "native_decoder_closed_or_truncated_output")
        parts.extend(piece)
    return bytes(parts)


def verify_inventory_chain(source, proof):
    previous_slot = proof["previous_epoch_parent"]["slot"]
    previous_hash = bytes(proof["previous_epoch_parent"]["blockhash"])
    count = 0
    last_identity = None
    for slot, bank, parent_slot, blockhash, parent_hash, rseq, pseq, verified in source.execute("SELECT slot,bank,parent_slot,blockhash,parent_hash,root_event_seq,block_event_seq,metadata_verified FROM banks WHERE selected=1 ORDER BY slot"):
        require(verified == 1 and parent_slot == previous_slot and parent_hash == previous_hash and pseq < rseq <= proof["frozen_cloud_event_sequence"], "frozen_inventory_parent_or_root_conflict")
        last_identity = {"slot": slot, "bank_id": bank, "parent_slot": parent_slot, "blockhash": list(blockhash), "parent_blockhash": list(parent_hash)}
        if count == 0:
            require(last_identity == proof["first_bank"], "frozen_inventory_first_bank_conflict")
        previous_slot, previous_hash = slot, blockhash
        count += 1
    require(count == proof["selected_rooted_banks"] and last_identity == proof["fixed_root"], "frozen_inventory_chain_count_or_last_bank_conflict")


def helper_limits():
    resource.setrlimit(resource.RLIMIT_AS, (512 * 1024**2, 512 * 1024**2))


def self_test():
    classic, token2022 = sorted(PROGRAMS)
    keys = ["authority", "account", classic, token2022, "mint", STATE, "application"]
    def approval(tag, amount):
        return [tag, *amount.to_bytes(8, "little"), *([6] if tag == 13 else [])]
    a = approval(4, 2**64 - 1) + [99]
    checked = approval(13, 0)
    tx = {"success": True, "index": 1, "signature": "signature", "signatures": ["signature"], "accountKeys": keys,
          "innerCoverage": "recorded", "message": {"V0": {"instructions": [{"program_id_index": 2, "accounts": [1,5,0], "data": {"Raw": a}}, {"program_id_index": 6, "accounts": [], "data": {"Raw": []}}]}},
          "metadata": {"inner_instructions": [{"index": 1, "instructions": [{"program_id_index": 3, "accounts": [1,4,5,0], "data": [255,4,len(checked),*checked], "stack_height": 2}]}]}}
    events = extract(tx)
    assert len(events) == 2 and events[0]["amount"] == str(2**64 - 1) and events[0]["instruction_data_hex"] == bytes(a).hex()
    assert events[1]["batch_ix"] == 0 and events[1]["mint"] == "mint" and events[1]["amount"] == "0" and events[1]["inner_ix"] == 0
    tx["metadata"]["inner_instructions"] = None
    try:
        extract(tx)
    except RuntimeError as error:
        assert str(error) == "selected_state_token_transaction_cpi_unavailable"
    else:
        raise AssertionError("missing CPI accepted")
    try:
        list(children(b"\x00", bytes([255,1,1,255])))
    except RuntimeError as error:
        assert str(error) == "nested_token_batch"
    else:
        raise AssertionError("nested Batch accepted")
    assert base58([0, 1]) == "12"
    print("PASS: exact u64, trailing bytes, resolved loaded keys, standalone outer Token, CPI Token2022 Batch, zero allowance, missing CPI and nested Batch rejection")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory-dir", type=Path)
    parser.add_argument("--out-dir", type=Path)
    parser.add_argument("--decoder", type=Path)
    parser.add_argument("--credentials-file", type=Path)
    parser.add_argument("--decoder-source-record", type=Path)
    parser.add_argument("--max-blocks", type=int)
    parser.add_argument("--allow-full-scan", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return
    require(all((args.inventory_dir, args.out_dir, args.decoder, args.credentials_file)), "missing_required_paths")
    require(args.max_blocks is None or args.max_blocks > 0, "invalid_max_blocks")
    limit = args.max_blocks if args.max_blocks is not None else None if args.allow_full_scan else 100
    if sys.platform.startswith("linux"):
        # The child inherits the hard ceiling. Keep its 512 MiB ceiling available
        # while the controller itself remains limited to 256 MiB.
        resource.setrlimit(resource.RLIMIT_AS, (256 * 1024**2, 512 * 1024**2))
    os.umask(0o077)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    start = time.time()
    status = {"schema": "debridge-native-approval-scan-v1", "state": "STARTED", "all_wallet_coverage_proven": False,
              "canonical": False, "publication_ready": False, "candidate_superset_requires_current_account_read": True}
    proof_raw, proof = inventory.bounded(args.inventory_dir / "STATUS.json")
    require(proof["state"] == "PASSED" and proof["recorded_rooted_chain_complete"] is True, "rooted_inventory_not_verified")
    head_raw, head = inventory.bounded(args.inventory_dir / "CLOUD-HEAD.json")
    require(digest(head_raw) == proof["cloud_head_sha256"], "frozen_head_changed")
    prefix = head["last_commit"].rsplit("/commits/", 1)[0]
    credentials = inventory.read_credentials(args.credentials_file)
    binding = {"proof_sha256": digest(proof_raw), "inventory_sha256": file_sha(args.inventory_dir / "inventory.sqlite"),
               "decoder_sha256": file_sha(args.decoder), "controller_sha256": file_sha(Path(__file__)),
               "inventory_helper_sha256": file_sha(Path(inventory.__file__)),
               "source": proof["source"], "fixed_root": proof["fixed_root"], "frozen_cloud_event_sequence": proof["frozen_cloud_event_sequence"]}
    if "inventory_sha256" in proof:
        require(binding["inventory_sha256"] == proof["inventory_sha256"], "frozen_inventory_digest_changed")
    if args.decoder_source_record:
        raw, record = inventory.bounded(args.decoder_source_record)
        binding.update(decoder_source_record_sha256=digest(raw), decoder_source_record=record)
    source = sqlite3.connect("file:" + str(args.inventory_dir / "inventory.sqlite") + "?mode=ro", uri=True)
    source.execute("PRAGMA query_only=ON")
    source.execute("PRAGMA cache_size=-4096")
    db = sqlite3.connect(args.out_dir / "approvals.sqlite")
    db.execute("PRAGMA cache_size=-4096")
    db.executescript("CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL); CREATE TABLE IF NOT EXISTS blocks(slot INTEGER PRIMARY KEY,bank INTEGER,commit_key TEXT,commit_sha TEXT,frame_key TEXT,frame_sha TEXT,payload_marker_sha TEXT,transactions INTEGER,selected_transactions INTEGER,approvals INTEGER); CREATE TABLE IF NOT EXISTS approvals(slot INTEGER,tx_index INTEGER,outer_ix INTEGER,inner_ix INTEGER,batch_ix INTEGER,token_account TEXT,token_program TEXT,amount TEXT,record_json TEXT,PRIMARY KEY(slot,tx_index,outer_ix,inner_ix,batch_ix)); CREATE TABLE IF NOT EXISTS candidates(token_account TEXT PRIMARY KEY);")
    encoded = canonical(binding).decode()
    old = db.execute("SELECT value FROM metadata WHERE key='source_binding'").fetchone()
    require(old is None or old[0] == encoded, "source_or_decoder_changed_on_resume")
    db.execute("INSERT OR IGNORE INTO metadata VALUES('source_binding',?)", [encoded])
    db.commit()
    helper = None

    def export(state, error=None):
        count, transactions, matched, events = db.execute("SELECT count(*),coalesce(sum(transactions),0),coalesce(sum(selected_transactions),0),coalesce(sum(approvals),0) FROM blocks").fetchone()
        temporary = args.out_dir / "candidates.new"
        with temporary.open("w") as file:
            for row in db.execute("SELECT token_account FROM candidates ORDER BY token_account"):
                file.write(row[0] + "\n")
        temporary.replace(args.out_dir / "candidates.txt")
        status.update(state=state, source_binding=binding, banks_processed=count, banks_expected=proof["selected_rooted_banks"],
                      complete_frozen_rooted_range=count == proof["selected_rooted_banks"], transactions=transactions,
                      selected_successful_state_transactions=matched, approval_events=events,
                      candidate_token_accounts=db.execute("SELECT count(*) FROM candidates").fetchone()[0],
                      downloaded_bytes=downloaded, elapsed_seconds=time.time() - start, error=error)
        save(args.out_dir / "STATUS.json", status)

    downloaded = processed = 0
    try:
        verify_inventory_chain(source, proof)
        helper = subprocess.Popen([str(args.decoder)], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=(args.out_dir / "decoder.stderr").open("ab"), preexec_fn=helper_limits if sys.platform.startswith("linux") else None)
        last = db.execute("SELECT coalesce(max(slot),-1) FROM blocks").fetchone()[0]
        rows = source.execute("SELECT slot,bank,parent_slot,blockhash,parent_hash,artifact,marker_sha,block_event_seq,block_event_sha,transactions FROM banks WHERE selected=1 AND metadata_verified=1 AND slot>? ORDER BY slot", [last])
        for slot, bank, parent_slot, blockhash, parent_hash, artifact, marker_sha, pseq, event_sha, transaction_count in rows:
            if limit is not None and processed >= limit:
                break
            commit_key = prefix + "/commits/" + str(pseq - proof["publisher_sequence_offset"]).zfill(20) + ".json"
            commit_raw, commit = inventory.r2_get(credentials, commit_key)
            downloaded += len(commit_raw)
            event = commit["event"]
            expected = {"slot": slot, "bank_id": bank, "parent_slot": parent_slot, "blockhash": list(blockhash), "parent_blockhash": list(parent_hash)}
            require(commit["schema"] == "blockzilla-native-stream-commit-v1" and commit["profile"] == "native-processed-compact-v1" and commit["session"] == proof["source"]["session_id"] and commit["canonical"] is False and commit["publication_ready"] is False, "commit_source_profile_conflict")
            require(all(commit[k] == proof["source"][k] for k in inventory.SOURCE_FIELDS[1:]), "commit_source_identity_conflict")
            require(commit["sequence"] == pseq - proof["publisher_sequence_offset"] and event["sequence"] == pseq and pseq <= proof["frozen_cloud_event_sequence"] and event["kind"] == "block" and inventory.identity(event["bank"]) == expected and event["payload_sha256"] == marker_sha and digest(canonical(event)) == event_sha, "commit_inventory_event_conflict")
            frame_key = prefix + "/chunks/" + str(slot) + "-" + commit["sha256"] + ".frame"
            require(commit["object"] == frame_key and isinstance(commit["bytes"], int) and 16 <= commit["bytes"] <= MAX_FRAME, "invalid_commit_frame_reference")
            wire = inventory.r2_read(credentials, frame_key, MAX_FRAME)
            downloaded += len(wire)
            require(len(wire) == commit["bytes"] and digest(wire) == commit["sha256"] and wire[:8] == b"BZSTRM01", "whole_frame_digest_conflict")
            size = struct.unpack("<I", wire[8:12])[0]
            require(0 < size <= 65536 and len(wire) >= 16 + size, "invalid_frame_metadata_length")
            envelope = json.loads(wire[16:16 + size])
            marker_raw, marker = inventory.bounded(inventory.safe_path(Path(proof["source_root"]), artifact) / "DIRECT-AGAVE-COMPACT.v1.json")
            require(digest(marker_raw) == marker_sha and envelope["marker"] == marker and envelope["event"] == event, "frame_inventory_marker_or_event_conflict")
            helper.stdin.write(struct.pack("<I", len(wire)))
            helper.stdin.write(wire)
            helper.stdin.flush()
            del wire
            length = struct.unpack("<I", exact_read(helper.stdout, 4))[0]
            require(0 < length <= MAX_OUTPUT, "native_decoder_output_limit")
            decoded = json.loads(exact_read(helper.stdout, length))
            require("error" not in decoded, "native_decode_failed:" + str(decoded.get("error")))
            require(decoded["profile"] == "native-processed-compact-v1" and decoded["session"] == proof["source"]["session_id"] and decoded["canonical"] is False and decoded["publicationReady"] is False and decoded["epoch"] == proof["epoch"] and len(decoded["blocks"]) == 1, "decoded_source_conflict")
            block = decoded["blocks"][0]
            require(block["slot"] == slot and block["bankId"] == bank and block["parentSlot"] == parent_slot and block["blockhash"] == base58(blockhash) and block["parentBlockhash"] == base58(parent_hash) and block["transactionCount"] == transaction_count and decoded["transactionCount"] == transaction_count and block["finality"] == "processed", "decoded_inventory_bank_conflict")
            events = []
            for tx in block["transactions"]:
                events.extend(extract(tx))
            with db:
                db.execute("INSERT INTO blocks VALUES(?,?,?,?,?,?,?,?,?,?)", [slot,bank,commit_key,digest(commit_raw),frame_key,commit["sha256"],marker_sha,transaction_count,len(block["transactions"]),len(events)])
                for record in events:
                    record.update(slot=slot,bank_id=bank,block_commit_key=commit_key,block_commit_sha256=digest(commit_raw),frame_key=frame_key,frame_sha256=commit["sha256"],payload_marker_sha256=marker_sha,block_event_sequence=pseq,source_identity=proof["source"])
                    db.execute("INSERT INTO approvals VALUES(?,?,?,?,?,?,?,?,?)", [slot,record["tx_index"],record["outer_ix"],record["inner_ix"],record["batch_ix"],record["token_account"],record["token_program"],record["amount"],canonical(record).decode()])
                    db.execute("INSERT OR IGNORE INTO candidates VALUES(?)", [record["token_account"]])
            processed += 1
            if processed % 10 == 0:
                export("RUNNING")
                print(json.dumps({"banks_this_run":processed,"slot":slot,"downloaded_bytes":downloaded}), flush=True)
        count = db.execute("SELECT count(*) FROM blocks").fetchone()[0]
        export("COMPLETE_FROZEN_RANGE" if count == proof["selected_rooted_banks"] else "PILOT_PARTIAL")
    except Exception as error:
        export("COVERAGE_UNKNOWN", str(error))
        raise
    finally:
        if helper is not None:
            helper.stdin.close()
            try:
                helper.wait(timeout=5)
            except subprocess.TimeoutExpired:
                helper.kill()
                helper.wait()
        source.close()
        db.close()
        print(json.dumps(status), flush=True)


if __name__ == "__main__":
    main()
