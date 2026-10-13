#!/usr/bin/env python3
"""Exercise the adapter against real local HTTP body cuts, without external data.

Run with --binary PATH [--output-dir NEW_DIR]. The binary must support
--reported-error-policy strict|body-transport. --check-fixture validates only
the fixture and fault server; it does not claim to test the adapter.

The checked-in CAR fixture has a zero-length header. We replace that header
with a CARv1 header rooted at its existing Block CID. Every node stays intact.
The generated slot index describes only this one-block test archive. It is
not a published epoch index or a full-epoch completeness claim.
"""

import argparse
import collections
import contextlib
import hashlib
import http.client
import http.server
import json
import os
from pathlib import Path
import re
import socket
import struct
import subprocess
import tempfile
import threading


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_FIXTURE = ROOT / "crates/old-faithful/of-car-reader/benches/fixtures/epoch-822-biggest.car"
FIXTURE_SHA256 = "7cabc9c60e146fe43b864ad860edfd29961abd70ad7cc359f767437ebea89ec7"
FIXTURE_BYTES = 4_654_582
EXPECTED_TRANSACTIONS = 2_969
BODY_ERROR = "Error reading until block: error decoding response body"
HEADER_ERROR = "Error reading header: error decoding response body"
BAD_HEADER_ERROR = "Error reading header: Header length too long"


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def uvarint(data, position):
    value = 0
    for i in range(10):
        require(position < len(data), "truncated varint")
        byte = data[position]
        position += 1
        require(i != 9 or byte <= 1, "varint overflow")
        value |= (byte & 127) << (7 * i)
        if byte < 128:
            return value, position
    raise AssertionError("varint overflow")


def encode_uvarint(value):
    result = bytearray()
    while value >= 128:
        result.append((value & 127) | 128)
        value >>= 7
    result.append(value)
    return bytes(result)


def cbor(data, position=0):
    """Decode the definite-length CBOR subset used by the hash-pinned fixture."""
    require(position < len(data), "truncated CBOR")
    first = data[position]
    position += 1
    major, extra = first >> 5, first & 31
    if major == 7 and extra in (20, 21, 22):
        return {20: False, 21: True, 22: None}[extra], position
    if extra < 24:
        size = extra
    else:
        require(extra in (24, 25, 26, 27), "unsupported CBOR length")
        width = 1 << (extra - 24)
        require(position + width <= len(data), "truncated CBOR length")
        size = int.from_bytes(data[position:position + width], "big")
        position += width
    if major in (0, 1):
        return (size if major == 0 else -1 - size), position
    if major in (2, 3):
        end = position + size
        require(end <= len(data), "truncated CBOR string")
        value = data[position:end]
        return (value if major == 2 else value.decode("utf-8")), end
    if major in (4, 5):
        values = []
        for _ in range(size * (2 if major == 5 else 1)):
            value, position = cbor(data, position)
            values.append(value)
        return (dict(zip(values[::2], values[1::2])) if major == 5 else values), position
    if major == 6:
        value, position = cbor(data, position)
        return ("tag", size, value), position
    raise AssertionError(f"unsupported CBOR major type {major}")


def prepare_fixture(path):
    original = path.read_bytes()
    require(len(original) == FIXTURE_BYTES, "fixture size changed")
    require(hashlib.sha256(original).hexdigest() == FIXTURE_SHA256, "fixture hash changed")
    header_size, position = uvarint(original, 0)
    require(header_size == 0, "expected the checked-in fixture's empty header")
    nodes_start = position
    kinds = collections.Counter()
    transactions = []
    entry_transactions = 0
    blocks = []
    first_payload = None
    while position < len(original):
        section_size, cid_start = uvarint(original, position)
        end = cid_start + section_size
        require(end <= len(original), "truncated CAR section")
        cid_version, payload_start = uvarint(original, cid_start)
        codec, payload_start = uvarint(original, payload_start)
        hash_kind, payload_start = uvarint(original, payload_start)
        digest_size, payload_start = uvarint(original, payload_start)
        require((cid_version, codec, hash_kind, digest_size) == (1, 113, 18, 32), "unexpected CID")
        payload_start += digest_size
        require(payload_start < end, "empty CAR node")
        payload = original[payload_start:end]
        require(hashlib.sha256(payload).digest() == original[payload_start - 32:payload_start], "node CID mismatch")
        node, consumed = cbor(payload)
        require(consumed == len(payload) and isinstance(node, list), "invalid CBOR node")
        kinds[node[0]] += 1
        if first_payload is None:
            first_payload = payload_start
        if node[0] == 0:
            transactions.append((node[3], node[4]))
        elif node[0] == 1:
            entry_transactions += len(node[3])
        elif node[0] == 2:
            blocks.append((node[1], original[cid_start:payload_start]))
            require(end == len(original), "fixture must end with its single Block node")
        position = end
    require(len(blocks) == 1, "fixture must have one block")
    slot, block_cid = blocks[0]
    require(slot // 432_000 == 822, "fixture epoch changed")
    require(len(transactions) == entry_transactions == EXPECTED_TRANSACTIONS, "fixture count differs from prior measurement")
    require(transactions == [(slot, i) for i in range(EXPECTED_TRANSACTIONS)], "transaction slot/index order mismatch")
    # {"roots": [tag(42, bytes(0 + BlockCID))], "version": 1}
    root = b"\0" + block_cid
    header = b"\xa2\x65roots\x81\xd8\x2a\x58" + bytes([len(root)]) + root + b"\x67version\x01"
    header, header_len = encode_uvarint(len(header)) + header, len(header)
    require(len(header) == header_len + 1, "unexpected CAR header width")
    car = header + original[nodes_start:]
    index = bytearray(432_000 * 12)
    struct.pack_into("<QI", index, (slot % 432_000) * 12, len(header), len(car) - len(header))
    plan = {"epoch": 822, "start_slot": slot, "end_slot_exclusive": slot + 1,
            "block_transaction_rows": [[slot, EXPECTED_TRANSACTIONS]]}
    metadata = {"original_fixture_sha256": FIXTURE_SHA256, "original_fixture_bytes": len(original),
                "served_car_sha256": hashlib.sha256(car).hexdigest(), "served_car_bytes": len(car),
                "header_bytes": len(header), "node_counts": dict(kinds), "plan": plan,
                "provenance": "Node bytes and CIDs are unchanged. A CARv1 header and a one-block slot index are generated for this local fault test."}
    return car, bytes(index), plan, metadata, first_payload - nodes_start + len(header)


class FaultServer(http.server.ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, car, index, header_bytes, cuts=0, corrupt_at=None,
                 header_cuts=0, corrupt_header=False):
        super().__init__(("127.0.0.1", 0), FaultHandler)
        self.car, self.index, self.header_bytes = car, index, header_bytes
        self.cuts_left, self.corrupt_at = cuts, corrupt_at
        self.header_cuts_left, self.corrupt_header = header_cuts, corrupt_header
        self.requests = []
        self.lock = threading.Lock()


class FaultHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        pass

    def do_HEAD(self):
        self.serve(head=True)

    def do_GET(self):
        self.serve(head=False)

    def serve(self, head):
        server = self.server
        if self.path == "/822/epoch-822.car":
            data, is_car = server.car, True
        elif self.path == "/822/epoch-822-slot-ranges.raw":
            data, is_car = server.index, False
        else:
            self.send_error(404)
            return
        range_header = self.headers.get("Range")
        start, end = 0, len(data) - 1
        if range_header:
            match = re.fullmatch(r"bytes=(\d+)-(\d*)", range_header)
            if not match:
                self.send_error(416)
                return
            start = int(match[1])
            end = min(int(match[2]) if match[2] else end, end)
            if start > end:
                self.send_error(416)
                return
        length = end - start + 1
        cut = False
        cut_stage = None
        with server.lock:
            # Do not cut the 1-byte size probe. The initial full CAR response
            # supplies the header; a sought response supplies the block nodes.
            header_request = not head and is_car and start == 0 and length > server.header_bytes
            block_request = not head and is_car and start == server.header_bytes and length > 65_536
            if header_request and server.header_cuts_left:
                server.header_cuts_left -= 1
                cut, cut_stage = True, "header"
            elif block_request and server.cuts_left:
                server.cuts_left -= 1
                cut, cut_stage = True, "block"
            corrupt = block_request and server.corrupt_at is not None
            corrupt_header = header_request and server.corrupt_header
            sent = 10 if cut_stage == "header" else min(65_536, length // 2) if cut else length
            server.requests.append({"method": self.command, "path": self.path, "start": start,
                                    "end_inclusive": end, "declared_bytes": length,
                                    "scheduled_body_bytes": 0 if head else sent, "body_cut": cut,
                                    "cut_stage": cut_stage, "corrupt": corrupt,
                                    "corrupt_header": corrupt_header})
        self.send_response(206 if range_header else 200)
        self.send_header("Content-Length", str(length))
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("ETag", '"local-fixture"')
        self.send_header("Connection", "close")
        if range_header:
            self.send_header("Content-Range", f"bytes {start}-{end}/{len(data)}")
        self.end_headers()
        self.close_connection = True
        if not head:
            body = data[start:start + sent]
            if corrupt_header:
                # Upstream does not parse the header CBOR here. An excessive
                # length varint (1025, above its 1024 limit) must be rejected.
                body = bytearray(body)
                body[:2] = b"\x81\x08"
            if corrupt:
                body = bytearray(body)
                # The node starts with a CBOR array followed by kind 0. Change
                # kind to 7, which is not a recognized Old Faithful node type.
                offset = server.corrupt_at - start
                require(body[offset] == 0x85 and body[offset + 1] == 0, "unexpected first transaction encoding")
                body[offset + 1] = 7
            try:
                self.wfile.write(body)
                self.wfile.flush()
                if cut:
                    self.connection.shutdown(socket.SHUT_WR)
            except (BrokenPipeError, ConnectionResetError):
                pass  # The adapter closes the initial header stream after seek.


@contextlib.contextmanager
def serve_fixture(car, index, metadata, cuts=0, corrupt_at=None,
                  header_cuts=0, corrupt_header=False):
    server = FaultServer(car, index, metadata["header_bytes"], cuts, corrupt_at,
                         header_cuts, corrupt_header)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server, f"http://127.0.0.1:{server.server_port}/"
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def check_fault_server(car, index, metadata):
    for stage, start, size in [("block", metadata["header_bytes"], 65_536), ("header", 0, 10)]:
        with serve_fixture(car, index, metadata, cuts=int(stage == "block"),
                           header_cuts=int(stage == "header")) as (server, _):
            connection = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=5)
            connection.request("GET", "/822/epoch-822.car", headers={"Range": f"bytes={start}-"})
            response = connection.getresponse()
            require(response.status == 206, "fault server did not return 206")
            try:
                response.read()
            except http.client.IncompleteRead as error:
                require(len(error.partial) == size, "fault server cut at wrong byte")
            else:
                raise AssertionError("fault server did not truncate the declared body")
            finally:
                connection.close()
            require(sum(row["body_cut"] for row in server.requests) == 1, "missing recorded cut")
            require(server.requests[0]["cut_stage"] == stage, "wrong recorded cut stage")
    with serve_fixture(car, index, metadata, corrupt_header=True) as (server, _):
        connection = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=5)
        connection.request("GET", "/822/epoch-822.car", headers={"Range": "bytes=0-"})
        body = connection.getresponse().read()
        connection.close()
        require(len(body) == len(car) and uvarint(body, 0)[0] == 1025,
                "bad header test must have a complete body with an invalid header length")


def run_case(binary, destination, name, policy, cuts, fixture, timeout, corrupt=False,
             header_cuts=0, corrupt_header=False):
    car, index, plan, metadata, first_payload = fixture
    output = destination / name
    output.mkdir()
    report_path, export_path = output / "report.json", output / "export.bin"
    plan_path = destination / "plan.json"
    with serve_fixture(car, index, metadata, cuts, first_payload if corrupt else None,
                       header_cuts, corrupt_header) as (server, base):
        command = [str(binary), "--epoch", "822", "--http-base", base, "--index-base", base,
                   "--output", str(report_path), "--plan", str(plan_path), "--export", str(export_path),
                   "--workers", "1", "--start-slot", str(plan["start_slot"]),
                   "--end-slot-exclusive", str(plan["end_slot_exclusive"]), "--mode", "decoded"]
        if policy is not None:
            command += ["--reported-error-policy", policy]
        environment = {key: value for key, value in os.environ.items()
                       if not key.startswith("JETSTREAMER_") and key.lower() not in
                       {"http_proxy", "https_proxy", "all_proxy", "no_proxy"}}
        environment["NO_PROXY"] = "127.0.0.1,localhost"
        with (output / "stdout.log").open("wb") as stdout, (output / "stderr.log").open("wb") as stderr:
            try:
                result = subprocess.run(command, env=environment, stdout=stdout, stderr=stderr,
                                        timeout=timeout, check=False)
            finally:
                (output / "http-requests.json").write_text(json.dumps(server.requests, indent=2) + "\n")
        actual_cuts = sum(row["body_cut"] for row in server.requests)
        require(actual_cuts == cuts + header_cuts,
                f"{name}: expected {cuts + header_cuts} HTTP cuts, observed {actual_cuts}")
        require(sum(row["cut_stage"] == "header" for row in server.requests) == header_cuts,
                f"{name}: header cut count differs")
        if corrupt_header:
            require(sum(row["corrupt_header"] for row in server.requests) == 1,
                    f"{name}: bad header was not sent exactly once")
        require(report_path.exists(), f"{name}: adapter did not write a report; see {output}")
        report = json.loads(report_path.read_text())
        require(report["reported_error_policy"] == (policy or "strict"), f"{name}: policy receipt mismatch")
        require(report["reported_body_retry_limit_per_worker_slot"] == 2, f"{name}: reported limit differs")
        require(report["reported_error_events_truncated"] is False, f"{name}: unexpected event truncation")
        events = report["reported_error_events"]
        require(len(events) == report["reported_body_retries"], f"{name}: retry event count differs")
        expected_errors = [HEADER_ERROR] * header_cuts + [BODY_ERROR] * cuts
        for attempt, event in enumerate(events, 1):
            require(event["worker"] == 0 and event["slot"] == plan["start_slot"] and
                    event["attempt"] == attempt and event["error_message"] == expected_errors[attempt - 1],
                    f"{name}: retry event identity differs")
            require(0 <= event["elapsed_seconds"] <= report["total_seconds"],
                    f"{name}: retry event is outside the measured interval")
        return result.returncode, report, export_path, actual_cuts


def check_valid(name, result, plan, expected_retries):
    code, report, path, cuts = result
    require(code == 0 and report["valid"] is True and report["errors"] == [], f"{name}: adapter failed")
    require(report["block_transaction_rows"] == plan["block_transaction_rows"], f"{name}: block rows differ")
    require(report["blocks"] == 1 and report["transactions"] == EXPECTED_TRANSACTIONS,
            f"{name}: completed counts differ")
    require(report["transaction_callbacks"] == EXPECTED_TRANSACTIONS, f"{name}: duplicate or missing callbacks")
    require(report["reported_body_retries"] == expected_retries, f"{name}: retry count differs")
    require(report["export_bytes"] == path.stat().st_size, f"{name}: export size receipt differs")
    workers = report["worker_counters"]
    require(len(workers) == 1 and workers[0]["blocks"] == 1 and
            workers[0]["transaction_callbacks"] == EXPECTED_TRANSACTIONS, f"{name}: worker totals differ")
    require(cuts == expected_retries, f"{name}: server cuts/retries differ")


def check_failed(name, result, retries, expected_error):
    code, report, _, _ = result
    require(code != 0 and report["valid"] is False and report["errors"], f"{name}: failure was accepted")
    require(report["reported_body_retries"] == retries, f"{name}: retry bound differs")
    require(report["export_bytes"] is None, f"{name}: invalid export was finalized")
    require(report["transaction_callbacks"] == 0 and report["block_transaction_rows"] == [],
            f"{name}: failed block emitted callbacks")
    if expected_error:
        require(expected_error in "\n".join(report["errors"]), f"{name}: did not report the intended fatal error")


def test_adapter(binary, destination, fixture, timeout):
    _, _, plan, metadata, _ = fixture
    (destination / "plan.json").write_text(json.dumps(plan, indent=2) + "\n")
    (destination / "fixture.json").write_text(json.dumps(metadata, indent=2) + "\n")
    cases = {}
    for name, policy, cuts, corrupt, header_cuts, corrupt_header, retries, fatal_error in [
        ("clean-default-strict", None, 0, False, 0, False, 0, None),
        ("one-cut-recovered", "body-transport", 1, False, 0, False, 1, None),
        ("two-cuts-recovered", "body-transport", 2, False, 0, False, 2, None),
        ("one-cut-strict", "strict", 1, False, 0, False, 0, BODY_ERROR),
        ("three-cuts-budget-exhausted", "body-transport", 3, False, 0, False, 2, BODY_ERROR),
        ("malformed-node-fatal", "body-transport", 0, True, 0, False, 0, "Error reading until block:"),
        ("one-header-cut-recovered", "body-transport", 0, False, 1, False, 1, None),
        ("two-header-cuts-recovered", "body-transport", 0, False, 2, False, 2, None),
        ("one-header-cut-strict", "strict", 0, False, 1, False, 0, HEADER_ERROR),
        ("three-header-cuts-budget-exhausted", "body-transport", 0, False, 3, False, 2, HEADER_ERROR),
        ("malformed-header-fatal", "body-transport", 0, False, 0, True, 0, BAD_HEADER_ERROR),
        ("header-and-block-cuts-recovered", "body-transport", 1, False, 1, False, 2, None),
        ("header-and-block-cuts-budget-exhausted", "body-transport", 1, False, 2, False, 2, BODY_ERROR),
    ]:
        case = run_case(binary, destination, name, policy, cuts, fixture, timeout,
                        corrupt, header_cuts, corrupt_header)
        cases[name] = case
        clean = cases["clean-default-strict"]
        if fatal_error is not None:
            check_failed(name, case, retries, fatal_error)
        else:
            check_valid(name, case, plan, retries)
            require(case[2].read_bytes() == clean[2].read_bytes(), f"{name}: exact export bytes differ")
            require(case[1]["worker_counters"] == clean[1]["worker_counters"], f"{name}: callback totals differ")
        print(f"passed {name}", flush=True)
    summary = {"status": "PASS", "fixture": metadata, "adapter": str(binary),
               "exact_output_sha256": hashlib.sha256(clean[2].read_bytes()).hexdigest(),
               "cases": {name: {"returncode": case[0], "valid": case[1]["valid"],
                                 "reported_body_retries": case[1]["reported_body_retries"],
                                 "injected_body_cuts": case[3]} for name, case in cases.items()}}
    (destination / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps({"status": "PASS", "cases": len(cases), "output_dir": str(destination)}, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--fixture", type=Path, default=DEFAULT_FIXTURE)
    parser.add_argument("--output-dir", type=Path, help="new directory for all test evidence")
    parser.add_argument("--timeout", type=float, default=60)
    parser.add_argument("--check-fixture", action="store_true")
    args = parser.parse_args()
    fixture = prepare_fixture(args.fixture)
    check_fault_server(fixture[0], fixture[1], fixture[3])
    if args.check_fixture:
        require(args.binary is None, "--check-fixture cannot be used with --binary")
        print(json.dumps({"status": "fixture-and-fault-server-PASS", "adapter_tested": False,
                          **fixture[3]}, indent=2))
        return
    require(args.binary is not None, "--binary is required unless --check-fixture is set")
    binary = args.binary.resolve(strict=True)
    if args.output_dir:
        destination = args.output_dir.resolve()
        destination.mkdir()
        test_adapter(binary, destination, fixture, args.timeout)
    else:
        with tempfile.TemporaryDirectory(prefix="jetstreamer-body-recovery-") as temporary:
            test_adapter(binary, Path(temporary), fixture, args.timeout)


if __name__ == "__main__":
    main()
