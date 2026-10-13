#!/usr/bin/env python3
"""Real origin/relay processes, mTLS, wire equality and durable restart."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import socket
import sqlite3
import ssl
import struct
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

FILES = ["DIRECT-AGAVE-NORMALIZED.v1.json", "archive-v2-blocks.index",
         "archive-v2-blocks.zstd", "archive-v2-meta.wincode", "available-poh.wincode",
         "blockhash_registry.bin", "registry.bin", "signatures.bin"]
TOKEN = "transport-test-" + "a" * 48


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait(fn, seconds=15):
    end = time.monotonic() + seconds
    last = None
    while time.monotonic() < end:
        try:
            value = fn()
            if value:
                return value
        except (OSError, ValueError, urllib.error.URLError) as e:
            last = e
        time.sleep(.1)
    raise AssertionError(f"timeout: {last}")


def req(url, context=None, token=TOKEN, headers=None):
    h = {"Authorization": "Bearer " + token}
    h.update(headers or {})
    return urllib.request.urlopen(urllib.request.Request(url, headers=h), timeout=5, context=context)


def head(base, context=None):
    with req(base + "/v1/head", context) as r:
        return json.load(r)


def exact(r, n):
    out = bytearray()
    while len(out) < n:
        b = r.read(n - len(out))
        assert b, "incomplete frame"
        out.extend(b)
    return bytes(out)


def frame(r):
    h = exact(r, 16)
    assert h[:8] == b"BZSTRM01"
    n, m = struct.unpack("<II", h[8:])
    assert 0 < n <= 65536 and m <= 16 * 1024 * 1024
    wire = h + exact(r, n + m)
    meta = json.loads(wire[16:16 + n])
    for f in meta["objects"]:
        b = wire[16 + n + f["offset"]:16 + n + f["offset"] + f["bytes"]]
        assert hashlib.sha256(b).hexdigest() == f["sha256"]
    return wire, meta


def certs(p, days=1):
    def run(*args):
        subprocess.run(["openssl", *args], cwd=p, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    run("req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes", "-days", str(days), "-keyout", "ca.key", "-out", "ca.crt", "-subj", "/CN=Hivezilla dedicated relay CA", "-addext", "basicConstraints=critical,CA:TRUE", "-addext", "keyUsage=critical,keyCertSign,cRLSign")
    for name, usage in [("origin", "serverAuth"), ("relay", "clientAuth")]:
        run("req", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes", "-keyout", name + ".key", "-out", name + ".csr", "-subj", "/CN=" + name)
        (p / (name + ".ext")).write_text("basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage=" + usage + "\nsubjectAltName=IP:127.0.0.1\n")
        run("x509", "-req", "-in", name + ".csr", "-CA", "ca.crt", "-CAkey", "ca.key", "-CAcreateserial", "-days", str(days), "-extfile", name + ".ext", "-out", name + ".crt")
    (p / "relay.pem").write_bytes((p / "relay.crt").read_bytes() + (p / "relay.key").read_bytes())
    for key in p.glob("*.key"):
        key.chmod(0o600)
    (p / "relay.pem").chmod(0o600)


def main():
    a = argparse.ArgumentParser()
    a.add_argument("--binary", required=True, type=Path)
    a.add_argument("--report", required=True, type=Path)
    args = a.parse_args()
    tests = []
    with tempfile.TemporaryDirectory(prefix="blockzilla-stream-test-") as temp:
        p = Path(temp)
        certs(p)
        (p / "api.key").write_text(TOKEN)
        source = p / "source"
        source.mkdir()
        db = sqlite3.connect(source / "committed.sqlite")
        db.execute("CREATE TABLE commits(slot INTEGER PRIMARY KEY,bank INTEGER,artifact TEXT)")

        def append(slot, parent):
            d = source / "epoch-0" / f"{slot}-{slot}.compact"
            d.mkdir(parents=True)
            files = []
            for name in FILES:
                b = b"compact-test-object\n"
                (d / name).write_bytes(b)
                files.append({"path": name, "bytes": len(b), "sha256": hashlib.sha256(b).hexdigest()})
            m = {"schema": "hivezilla-direct-agave-compact-v1", "state": "COMPLETE",
                 "source_profile": "agave-bank-callbacks-v1", "session_id": "session1",
                 "genesis_hash": "genesis", "node_identity": "operator", "agave_revision": "rev",
                 "epoch": 0, "start_slot": slot, "end_slot_exclusive": slot + 1,
                 "canonical": False, "publication_ready": False, "finalized": True,
                 "finality_basis": "operator-trusted-agave-rooted-bank-callback", "blocks": 1,
                 "produced_blocks": [{"slot": slot, "bank_id": slot, "block_id": 0, "parent_slot": parent,
                                      "blockhash": [1] * 32, "parent_blockhash": [1] * 32}], "files": files}
            (d / "DIRECT-AGAVE-COMPACT.v1.json").write_text(json.dumps(m))
            db.execute("INSERT INTO commits VALUES(?,?,?)", (slot, slot, str(d)))
            db.commit()
            tmp = source / "CURSOR.tmp"
            tmp.write_text(json.dumps({"session_id": "session1", "last_slot": slot, "updated_unix_ms": int(time.time() * 1000)}))
            tmp.replace(source / "CURSOR.json")

        append(100, 99)
        origin_port, relay_port = free_port(), free_port()
        origin = f"https://127.0.0.1:{origin_port}"
        relay = f"http://127.0.0.1:{relay_port}"
        ctx = ssl.create_default_context(cafile=p / "ca.crt")
        ctx.load_cert_chain(p / "relay.crt", p / "relay.key")
        anonymous = ssl.create_default_context(cafile=p / "ca.crt")
        origin_args = [str(args.binary), "origin", "--output", str(source), "--start-slot", "100",
                       "--bind", f"127.0.0.1:{origin_port}", "--token-file", str(p / "api.key"),
                       "--tls-cert", str(p / "origin.crt"), "--tls-key", str(p / "origin.key"), "--client-ca", str(p / "ca.crt")]
        relay_args = [str(args.binary), "relay", "--upstream", origin, "--upstream-token-file", str(p / "api.key"),
                      "--upstream-ca", str(p / "ca.crt"), "--upstream-identity", str(p / "relay.pem"),
                      "--spool", str(p / "spool"), "--bind", f"127.0.0.1:{relay_port}", "--token-file", str(p / "api.key"),
                      "--cors-origin", "http://127.0.0.1:8766"]
        procs = []
        log = open(p / "process.log", "wb")

        def launch(command):
            process = subprocess.Popen(command, stdout=log, stderr=log)
            procs.append(process)
            return process

        try:
            op = launch(origin_args)
            wait(lambda: head(origin, ctx).get("last_slot") == 100)
            try:
                req(origin + "/v1/head", anonymous)
                raise AssertionError("origin allowed anonymous TLS")
            except (ssl.SSLError, OSError, urllib.error.URLError):
                pass
            tests.append("mTLS rejects missing client certificate")
            rp = launch(relay_args)
            wait(lambda: head(relay).get("state") == "STARTING")
            for base, c in [(origin, ctx), (relay, None)]:
                try:
                    req(base + "/v1/head", c, token="wrong")
                    raise AssertionError("wrong key accepted")
                except urllib.error.HTTPError as e:
                    assert e.code == 401
            tests.append("origin and relay reject wrong API keys")
            base = head(relay)["latest_cursor"]
            append(101, 100)
            wait(lambda: head(relay).get("last_slot") == 101)
            assert head(relay)["upstream_http_version"] == "HTTP/2.0"
            tests.append("relay negotiates HTTP/2 over mTLS")
            with req(relay + "/v1/blocks?after=" + urllib.parse.quote(base)) as r:
                wire, meta = frame(r)
            sha = hashlib.sha256(wire).hexdigest()
            cursor = f"session1:101:{sha}"
            with req(relay + "/v1/events?after=" + urllib.parse.quote(base)) as r:
                lines = []
                while True:
                    line = r.readline().decode().strip()
                    if not line and lines:
                        break
                    lines.append(line)
            notice = json.loads(next(line[6:] for line in lines if line.startswith("data: ")))
            assert notice["cursor"] == cursor and notice["sha256"] == sha
            with req(relay + notice["url"]) as r:
                assert r.read() == wire
            tests.append("SSE notice, chunk fetch and binary stream are byte-identical")
            with req(relay + "/v1/head", headers={"Origin": "http://127.0.0.1:8766"}) as r:
                assert r.headers["Access-Control-Allow-Origin"] == "http://127.0.0.1:8766"
            try:
                req(relay + "/v1/head", headers={"Origin": "https://wrong.invalid"})
                raise AssertionError("wrong CORS origin accepted")
            except urllib.error.HTTPError as e:
                assert e.code == 403
            tests.append("CORS accepts only the configured origin")
            append(103, 101)
            wait(lambda: head(relay).get("last_slot") == 103)
            tests.append("skipped slots with matching parent chain are accepted")
            rp.kill()
            rp.wait(timeout=5)
            append(105, 103)
            rp = launch(relay_args)
            wait(lambda: head(relay).get("last_slot") == 105)
            with req(relay + notice["url"]) as r:
                assert r.read() == wire
            tests.append("forced relay restart resumes durable cursor and retains exact old bytes")
            bad = "session1:100:" + "0" * 64
            try:
                req(relay + "/v1/blocks?after=" + bad)
                raise AssertionError("expired cursor accepted")
            except urllib.error.HTTPError as e:
                assert e.code == 410 and json.load(e)["error"] == "replay_unavailable"
            tests.append("unavailable cursor returns 410 instead of latest")
            slow = socket.create_connection(("127.0.0.1", relay_port), timeout=5)
            slow.sendall(f"GET /v1/blocks?after={cursor} HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {TOKEN}\r\n\r\n".encode())
            append(106, 105)
            wait(lambda: head(relay).get("last_slot") == 106)
            slow.close()
            tests.append("unread client does not block relay intake")
            op.kill()
            op.wait(timeout=5)
            op = launch(origin_args)
            append(107, 106)
            wait(lambda: head(relay).get("last_slot") == 107, 25)
            tests.append("origin restart reproduces stable chunk identities and relay resumes")
            result = {"schema": "hivezilla-stream-transport-test-v1", "state": "PASS", "checks": tests,
                      "relay_head": head(relay), "fixture_only": True, "validator_changed": False}
            args.report.write_text(json.dumps(result, indent=2) + "\n")
            print(json.dumps({"state": "PASS", "checks": len(tests)}))
        except BaseException:
            log.flush()
            print((p / "process.log").read_text())
            raise
        finally:
            for process in procs:
                if process.poll() is None:
                    process.terminate()
            for process in procs:
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
            log.close()
            db.close()


if __name__ == "__main__":
    main()
