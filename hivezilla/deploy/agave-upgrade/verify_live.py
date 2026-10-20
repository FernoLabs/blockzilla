#!/usr/bin/env python3
"""Read one live Agave 4.3.2 sample. Write only the requested JSON report."""

import argparse
from contextlib import closing
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import selectors
import sqlite3
import subprocess
import time
import urllib.request


SOURCE_KEYS = ("genesis_hash", "node_identity", "agave_revision", "session_id")
SERVICES = {
    "validator": "hivezilla-validator.service",
    "worker": "hivezilla-direct-worker.service",
    "producer": "hivezilla-bank-v2-producer.service",
    "v2origin": "hivezilla-bank-v2-origin.service",
    "v1origin": "hivezilla-stream-origin.service",
    "relay": "hivezilla-stream-relay.service",
    "v2publisher": "hivezilla-bank-v2-r2-publisher.service",
    "livecold": "hivezilla-native-cold-storage-20261012-live.service",
    "rawrecorder": "hivezilla-shred-hook-recorder.service",
}
RPC_URL = "http://127.0.0.1:8899"
PRIVATE_URL = "http://127.0.0.1:8871"
PUBLIC_URL = "https://stream.blockzilla.dev/v1/blockhashes"
BASE58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


class CheckFailure(Exception):
    pass


def require(condition, code):
    if not condition:
        raise CheckFailure(code)


def integer(value):
    return type(value) is int and value >= 0


def read_json(path, limit=1024 * 1024):
    with path.open("rb") as stream:
        body = stream.read(limit + 1)
    require(len(body) <= limit, "json_file_too_large")
    value = json.loads(body)
    require(isinstance(value, dict), "json_object_required")
    return value


def source_tuple(value):
    return {key: value[key] for key in SOURCE_KEYS}


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise CheckFailure("http_redirect_rejected")


OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())


def http_bytes(url, *, headers=None, data=None, limit=1024 * 1024):
    request = urllib.request.Request(
        url, data=data, headers={"User-Agent": "blockzilla-upgrade-check/1", **(headers or {})})
    with OPENER.open(request, timeout=8) as response:
        require(response.status == 200, "http_status_not_200")
        body = response.read(limit + 1)
    require(len(body) <= limit, "http_body_too_large")
    return body


def rpc(method, params=None):
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method,
                       "params": params or []}).encode()
    reply = json.loads(http_bytes(RPC_URL, data=body,
                                 headers={"Content-Type": "application/json"}))
    require(isinstance(reply, dict) and "error" not in reply and "result" in reply,
            "rpc_" + method + "_failed")
    return reply["result"]


def base58(value):
    require(isinstance(value, list) and len(value) == 32
            and all(type(byte) is int and 0 <= byte <= 255 for byte in value),
            "invalid_bank_hash")
    raw = bytes(value)
    number, encoded = int.from_bytes(raw, "big"), ""
    while number:
        number, digit = divmod(number, 58)
        encoded = BASE58[digit] + encoded
    return "1" * (len(raw) - len(raw.lstrip(b"\0"))) + encoded


def option(argv, name):
    values = []
    for index, arg in enumerate(argv):
        if arg == name:
            require(index + 1 < len(argv), "missing_validator_option_value")
            values.append(argv[index + 1])
        elif arg.startswith(name + "="):
            values.append(arg.split("=", 1)[1])
    require(len(values) == 1, "missing_or_repeated_validator_option")
    return values[0]


def service_sample(details):
    failed = False
    for label, name in SERVICES.items():
        result = subprocess.run(
            ["systemctl", "show", name, "--property=MainPID,ActiveState,SubState"],
            check=True, text=True, capture_output=True, timeout=8)
        fields = dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)
        pid = int(fields.get("MainPID", "0"))
        item = {"unit": name, "pid": pid, "active_state": fields.get("ActiveState"),
                "sub_state": fields.get("SubState")}
        details[label] = item
        failed |= item["active_state"] != "active" or item["sub_state"] != "running" or pid <= 0
    require(not failed, "required_service_not_running")


def capture_sample(root, source, details):
    capture = root / "captures/direct-agave-live" / source["session_id"]
    meta = read_json(capture / "SESSION.json")
    actual = {"session_id": meta.get("session_id"), "agave_revision": meta.get("agave_revision"),
              **{key: meta.get("config", {}).get(key) for key in ("genesis_hash", "node_identity")}}
    require(actual == source, "capture_source_mismatch")
    require(meta.get("bank_observations_v2") is True, "capture_bank_v2_missing")
    details["closed"] = (capture / "CLOSED.json").exists()
    require(not details["closed"], "capture_closed")
    failure = read_json(capture / "FAILURE.json")
    details["failure_state"] = failure.get("state")
    require(failure.get("state") != "FAILED", "capture_failed")
    with closing(sqlite3.connect((capture / "events.sqlite").as_uri() + "?mode=ro",
                                uri=True, timeout=5)) as db:
        db.execute("PRAGMA query_only=ON")
        row = db.execute("SELECT state,durable_sequence,latest_rooted_slot FROM status WHERE id=1").fetchone()
    require(row is not None, "capture_status_missing")
    details.update(state=row[0], durable_sequence=row[1], latest_rooted_slot=row[2])
    require(row[0] == "CAPTURING" and integer(row[1]) and row[1] > 0
            and integer(row[2]), "capture_not_durable")
    with (capture / "LIVE.lock").open("rb") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            details["writer_lock_held"] = True
        else:
            fcntl.flock(lock, fcntl.LOCK_UN)
            details["writer_lock_held"] = False
    require(details["writer_lock_held"], "capture_writer_missing")


def processed_sample(root, source, native, details):
    output = root / "compact/direct-agave-processed-live" / source["session_id"]
    require(source_tuple(read_json(output / "FEED.json")) == source, "processed_source_mismatch")
    status = read_json(output / "STATUS.json")
    for key in ("state", "source_sequence", "event_sequence", "last_compact_slot", "updated_unix_ms"):
        details[key] = status.get(key)
    require(isinstance(status.get("state"), str) and status["state"].upper() != "FAILED",
            "processed_failed")
    require(integer(status.get("event_sequence")) and status["event_sequence"] > 0,
            "processed_events_missing")
    require(integer(status.get("last_compact_slot")), "processed_slot_missing")
    require(integer(native.get("latest_rooted_slot")), "native_root_missing")
    details["lag_slots"] = max(0, native["latest_rooted_slot"] - status["last_compact_slot"])
    require(details["lag_slots"] <= 32, "processed_lag_over_32_slots")


def private_sample(root, source, details):
    key = (root / "config/stream-20261004/client-api.key").read_text().strip()
    require(bool(key) and "\n" not in key and "\r" not in key, "invalid_api_key_file")
    headers = {"Authorization": "Bearer " + key}
    head = json.loads(http_bytes(PRIVATE_URL + "/v2/banks/head", headers=headers))
    require(isinstance(head, dict) and head.get("source") == source, "head_source_mismatch")
    details.update(status=head.get("status"), source_lag_ms=head.get("source_lag_ms"),
                   cursor=head.get("cursor"))
    require(head.get("status") == "live", "head_not_live")
    require(integer(head.get("source_lag_ms")) and head["source_lag_ms"] <= 10000,
            "head_lag_over_10000_ms")
    confirmed = head.get("latest_confirmed")
    require(isinstance(confirmed, dict), "confirmed_head_missing")
    bank, recent = confirmed.get("bank"), confirmed.get("recent_blockhash")
    require(isinstance(bank, dict) and isinstance(recent, dict), "confirmed_bank_missing")
    digest = confirmed.get("payload_frame_sha256")
    require(isinstance(digest, str) and re.fullmatch(r"[0-9a-f]{64}", digest), "invalid_frame_digest")
    frame = http_bytes(PRIVATE_URL + "/v2/banks/chunks/" + digest, headers=headers,
                       limit=128 * 1024 * 1024)
    actual_digest = hashlib.sha256(frame).hexdigest()
    details.update(payload_len=len(frame), payload_frame_sha256=actual_digest)
    require(frame.startswith(b"BZSTRM01") and actual_digest == digest, "frame_bytes_mismatch")
    slot, height = bank.get("slot"), confirmed.get("block_height")
    require(integer(slot) and integer(height), "confirmed_slot_or_height_invalid")
    blockhash, parent_hash = base58(bank.get("blockhash")), base58(bank.get("parent_blockhash"))
    require(recent.get("blockhash") == blockhash and recent.get("context_slot") == slot
            and integer(recent.get("last_valid_block_height"))
            and recent["last_valid_block_height"] >= height, "head_recent_blockhash_mismatch")
    block = rpc("getBlock", [slot, {"commitment": "confirmed", "transactionDetails": "none",
                                  "rewards": False, "maxSupportedTransactionVersion": 1}])
    require(isinstance(block, dict), "confirmed_rpc_block_missing")
    details.update(slot=slot, bank_id=bank.get("bank_id"), block_height=height,
                   blockhash=blockhash, parent_slot=bank.get("parent_slot"),
                   parent_blockhash=parent_hash,
                   last_valid_block_height=recent["last_valid_block_height"])
    require(block.get("blockhash") == blockhash and block.get("previousBlockhash") == parent_hash
            and block.get("parentSlot") == bank.get("parent_slot")
            and block.get("blockHeight") == height, "confirmed_rpc_bank_mismatch")
    details["rpc_bank_match"] = True


def publisher_sample(root, source, details):
    state = root / "reports/native-processed-r2-bank-v2" / source["session_id"]
    checkpoint, status = read_json(state / "CHECKPOINT.json"), read_json(state / "STATUS.json")
    cursor = checkpoint.get("cursor")
    require(checkpoint.get("session") == source["session_id"], "publisher_session_mismatch")
    require(checkpoint.get("source") == str(root / "compact/direct-agave-processed-live" / source["session_id"]),
            "publisher_source_path_mismatch")
    require(isinstance(cursor, str)
            and re.fullmatch(re.escape(source["session_id"]) + r":p[0-9]+:(?:[0-9a-f]{64}|-)", cursor),
            "publisher_cursor_source_mismatch")
    details.update(state=status.get("state"), cursor=cursor,
                   sequence=checkpoint.get("sequence"), blocks=checkpoint.get("blocks"),
                   bytes=checkpoint.get("bytes"), updated_unix_ms=status.get("updated_unix_ms"))
    require(isinstance(status.get("state"), str) and status["state"].upper() != "FAILED",
            "publisher_failed")
    embedded = status.get("checkpoint")
    require(isinstance(embedded, dict) and embedded.get("session") == source["session_id"]
            and isinstance(embedded.get("cursor"), str)
            and embedded["cursor"].startswith(source["session_id"] + ":p"),
            "publisher_status_source_mismatch")


def public_sample(details):
    """Read the unauthenticated stream, with a strict wall-clock bound."""
    started = time.monotonic()
    process = subprocess.Popen(
        ["curl", "--fail", "--silent", "--show-error", "--no-buffer", "--connect-timeout", "5",
         "--max-time", "20", "--proto", "=https", PUBLIC_URL],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    slots, pending, total = [], b"", 0
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ)
            while len(slots) < 5:
                remaining = 20 - (time.monotonic() - started)
                if remaining <= 0:
                    break
                if not selector.select(remaining):
                    break
                chunk = os.read(process.stdout.fileno(), 65536)
                if not chunk:
                    break
                total += len(chunk)
                require(total <= 1024 * 1024, "public_sse_too_large")
                pending += chunk.replace(b"\r\n", b"\n")
                while b"\n\n" in pending and len(slots) < 5:
                    record, pending = pending.split(b"\n\n", 1)
                    event, data = None, []
                    for line in record.decode("utf-8").splitlines():
                        if line.startswith("event:"):
                            event = line[6:].strip()
                        elif line.startswith("data:"):
                            data.append(line[5:].lstrip())
                    require(event != "gap", "public_sse_gap")
                    if event == "block":
                        value = json.loads("\n".join(data))
                        require(isinstance(value, dict) and integer(value.get("slot")),
                                "public_sse_invalid_block")
                        slots.append(value["slot"])
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=1)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=1)
        process.stdout.close()
        process.stderr.close()
        details.update(slots=slots, block_events=len(slots),
                       elapsed_seconds=round(time.monotonic() - started, 3))
    require(len(slots) >= 2 and max(slots) > slots[0], "public_sse_not_advancing")
    details["advancing"] = True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--source-file", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True, help="Output JSON file")
    args = parser.parse_args()
    root = args.root.resolve(strict=True)
    report = {"schema": "blockzilla-agave-upgrade-live-sample-v1", "state": "FAILED",
              "sampled_at_unix_ms": int(time.time() * 1000), "source": None, "checks": {}}

    def sample(name, callback):
        details = {}
        item = {"state": "FAILED", "details": details}
        report["checks"][name] = item
        try:
            callback(details)
            item["state"] = "PASSED"
        except CheckFailure as error:
            item["error"] = str(error)
        except Exception as error:
            # Never copy HTTP headers, credentials, argv, or file contents to a report.
            item["error"] = type(error).__name__
        return details

    def source_check(details):
        value = read_json(args.source_file)
        require(set(value) == set(SOURCE_KEYS) and all(isinstance(value[k], str) for k in SOURCE_KEYS),
                "invalid_source_file")
        require(re.fullmatch(r"[0-9a-f]{32}-[0-9a-f]{1,16}", value["session_id"])
                and re.fullmatch(r"[0-9a-f]{40}", value["agave_revision"]), "invalid_source_identity")
        report["source"] = value
        details["exact_source_loaded"] = True

    sample("source", source_check)
    services = sample("services", service_sample)
    source = report["source"]

    def validator_check(details):
        pid = services.get("validator", {}).get("pid", 0)
        require(pid > 0, "validator_pid_missing")
        argv = (Path("/proc") / str(pid) / "cmdline").read_bytes().decode().rstrip("\0").split("\0")
        details.update(pid=pid, no_voting=argv.count("--no-voting") == 1,
                       full_snapshots=option(argv, "--maximum-full-snapshots-to-retain"),
                       incremental_snapshots=option(argv, "--maximum-incremental-snapshots-to-retain"))
        require(details["no_voting"] and details["full_snapshots"] == "1"
                and details["incremental_snapshots"] == "1", "validator_configuration_mismatch")
        if source is not None:
            details["source_pid_match"] = int(source["session_id"].rsplit("-", 1)[1], 16) == pid
            require(details["source_pid_match"], "validator_source_pid_mismatch")

    sample("validator", validator_check)
    if source is not None:
        def rpc_check(details):
            version, health = rpc("getVersion"), rpc("getHealth")
            identity, genesis = rpc("getIdentity"), rpc("getGenesisHash")
            slot = rpc("getSlot", [{"commitment": "finalized"}])
            details.update(version=version.get("solana-core") if isinstance(version, dict) else None,
                           health=health, identity=identity.get("identity") if isinstance(identity, dict) else None,
                           genesis_hash=genesis, finalized_slot=slot)
            require(details["version"] == "4.3.2" and health == "ok", "rpc_version_or_health_mismatch")
            require(details["identity"] == source["node_identity"] and genesis == source["genesis_hash"],
                    "rpc_source_mismatch")
            require(integer(slot) and slot > 0, "rpc_finalized_slot_invalid")

        sample("rpc", rpc_check)
        native = sample("capture", lambda details: capture_sample(root, source, details))
        sample("processed", lambda details: processed_sample(root, source, native, details))
        sample("private_head_and_frame", lambda details: private_sample(root, source, details))
        sample("publisher", lambda details: publisher_sample(root, source, details))
    else:
        for name in ("rpc", "capture", "processed", "private_head_and_frame", "publisher"):
            report["checks"][name] = {"state": "FAILED", "error": "source_unavailable"}
    sample("public_sse", public_sample)
    report["pids"] = {name: item.get("pid") for name, item in services.items()}
    report["completed_at_unix_ms"] = int(time.time() * 1000)
    if all(item["state"] == "PASSED" for item in report["checks"].values()):
        report["state"] = "PASSED"
    args.report.parent.mkdir(parents=True, exist_ok=True)
    with args.report.open("w") as stream:
        json.dump(report, stream, separators=(",", ":"), sort_keys=True)
        stream.write("\n")
    args.report.chmod(0o600)
    print(json.dumps(report, separators=(",", ":"), sort_keys=True))
    return 0 if report["state"] == "PASSED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
