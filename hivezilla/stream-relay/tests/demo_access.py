#!/usr/bin/env python3
"""Bounded check of public metadata and URL-key access; never print the key."""
import argparse
import json
from pathlib import Path
import struct
import time
import urllib.error
import urllib.parse
import urllib.request


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--url", required=True)
    p.add_argument("--key-file", required=True, type=Path)
    p.add_argument("--report", required=True, type=Path)
    args = p.parse_args()
    key = args.key_file.read_text().strip()
    base = args.url.rstrip("/")
    tests = []
    report = {"url": base, "state": "RUNNING", "tests": tests}

    def request(path, query=None, headers=None):
        url = base + path
        if query:
            url += "?" + urllib.parse.urlencode(query)
        h = {"User-Agent": "blockzilla-rust-consumer/0.1"}
        h.update(headers or {})
        try:
            return urllib.request.urlopen(urllib.request.Request(url, headers=h), timeout=15)
        except urllib.error.HTTPError as e:
            return e

    def status(path, expected, query=None, headers=None):
        with request(path, query, headers) as r:
            assert r.status == expected, (path, r.status, expected)

    def sse(path, query, count, headers=None):
        rows = []
        with request(path, query, headers) as r:
            assert r.status == 200, (path, r.status)
            assert r.headers["Content-Type"].startswith("text/event-stream")
            event = {}
            for raw in r:
                line = raw.decode().rstrip("\r\n")
                assert key not in line, "Key leaked into event data"
                if not line:
                    if "data" in event and event.get("event") not in ("status", "gap"):
                        data = json.loads(event["data"])
                        if path == "/v1/events":
                            assert data["cursor"] == event["id"]
                        rows.append({"id": event["id"], "event": event["event"], "data": data})
                        if len(rows) == count:
                            return rows
                    elif event.get("event") == "gap":
                        raise AssertionError("Stream returned a gap")
                    event = {}
                elif ": " in line:
                    name, value = line.split(": ", 1)
                    event[name] = value
        raise AssertionError("Stream ended before the requested events")

    def base58_bytes(s):
        alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
        n = 0
        for c in s:
            n = n * 58 + alphabet.index(c)
        return b"\0" * (len(s) - len(s.lstrip("1"))) + n.to_bytes((n.bit_length() + 7) // 8, "big")

    try:
        with request("/v1/head", {"api_key": key}) as r:
            assert r.status == 200
            assert r.headers["Referrer-Policy"] == "no-referrer"
            assert r.headers["Cache-Control"] == "no-store"
            raw = r.read()
            assert key.encode() not in raw
            head = json.loads(raw)
            assert head["profile"] == "native-processed-compact-v1"
        tests.append("url_key_head_and_response_headers")
        status("/v1/head", 401)
        status("/v1/head", 401, {"api_key": "wrong"})
        status("/v1/head", 401, [("api_key", key), ("api_key", key)])
        status("/v1/head", 401, {"api_key": key}, {"Authorization": "Bearer wrong"})
        status("/v1/head", 200, headers={"Authorization": "Bearer " + key})
        status("/v1/events", 401)
        status("/v1/blocks", 401)
        status("/v1/head", 403, {"api_key": key}, {"Origin": "https://demo.example"})
        tests.append("private_auth_and_conflict_checks")
        with request("/demo") as r:
            assert r.status == 200
            assert "connect-src 'self'" in r.headers["Content-Security-Policy"]
            assert b'Complete SSE messages' in r.read()
        status("/demo.js", 200)
        status("/demo.css", 200)
        tests.append("public_demo_page_and_assets")
        after = head["latest_cursor"]
        rows = sse("/v1/blockhashes", {"after": after}, 8, {"Origin": "https://demo.example"})
        for prev, row in zip(rows, rows[1:]):
            prev_id, row_id = prev["id"].split(":"), row["id"].split(":")
            assert row_id[0] == prev_id[0]
            assert int(row_id[1].removeprefix("p")) == int(prev_id[1].removeprefix("p")) + 1
        assert any(row["event"] == "block" for row in rows)
        for row in rows:
            data = row["data"]
            expected = {"slot", "blockhash", "parent_blockhash"} if row["event"] == "block" else {"slot", "blockhash"}
            assert set(data) == expected, (row["event"], set(data))
            sha = row["id"].split(":")[2]
            path = "/v1/chunks/" + sha
            with request(path, {"api_key": key}) as r:
                assert r.status == 200
                wire = r.read()
            assert wire[:8] == b"BZSTRM01"
            meta_len, payload_len = struct.unpack("<II", wire[8:16])
            assert len(wire) == 16 + meta_len + payload_len
            event = json.loads(wire[16:16 + meta_len])["event"]
            assert int(row["id"].split(":")[1].removeprefix("p")) == event["sequence"]
            assert row["event"] == event["kind"]
            assert data["slot"] == event["bank"]["slot"]
            for name in expected - {"slot"}:
                assert base58_bytes(data[name]) == bytes(event["bank"][name])
        status(path, 401)
        tests.append("public_events_match_authenticated_binary_bank_identities")
        resumed = sse("/v1/blockhashes", None, 3, {"Last-Event-ID": rows[0]["id"]})
        assert resumed == rows[1:4]
        tests.append("public_exact_resume")
        notices = sse("/v1/events", {"api_key": key, "after": after}, 1)
        assert "api_key" not in notices[0]["data"]["url"]
        assert notices[0]["id"] == rows[0]["id"]
        tests.append("private_url_key_notices")
        status("/v1/blockhashes", 400, {"after": rows[0]["id"]}, {"Last-Event-ID": rows[1]["id"]})
        status("/v1/blockhashes", 410, {"after": "missing-session:p0:-"})
        tests.append("public_cursor_conflict_and_expiry")
        sizes = [{"event": row["event"], "json_bytes": len(json.dumps(row["data"], separators=(",", ":")).encode()),
                  "sse_bytes": len(("id: " + row["id"] + "\nevent: " + row["event"] + "\ndata: " + json.dumps(row["data"], separators=(",", ":")) + "\n\n").encode())}
                 for row in rows]
        report.update(state="PASSED", events=rows, message_sizes=sizes, checked_unix_ms=int(time.time() * 1000))
    except Exception as e:
        report.update(state="FAILED", error=str(e).replace(key, "[REDACTED]").replace(urllib.parse.quote_plus(key), "[REDACTED]"))
    args.report.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"state": report["state"], "tests": tests, "report": str(args.report)}))
    if report["state"] != "PASSED":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
