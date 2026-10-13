#!/usr/bin/env python3
"""Install only the new loopback stream services after transport tests pass."""
import hashlib
import json
from pathlib import Path
import secrets
import shutil
import ssl
import subprocess
import sys
import time

SOURCE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SOURCE / "tests"))
from transport import certs, head, wait

ROOT = Path("/home/ubuntu/live/hivezilla-validator-20260930")
CONFIG = ROOT / "config/stream-20261004"
RELEASE = ROOT / "releases/hivezilla-stream-v1-20261004"
REPORT = ROOT / "reports/hivezilla-stream-pilot-20261004.json"


def service_state():
    return subprocess.check_output(["systemctl", "show", "hivezilla-validator.service", "hivezilla-shred-hook-recorder.service",
                                    "hivezilla-direct-worker.service", "--property=Id,MainPID,NRestarts,ActiveState"], text=True)


def main():
    tests = json.loads((ROOT / "reports/hivezilla-stream-transport-tests-20261004.json").read_text())
    assert tests["state"] == "PASS"
    before = service_state()
    binary = SOURCE / "target/release/hivezilla-stream"
    sha = hashlib.sha256(binary.read_bytes()).hexdigest()
    RELEASE.mkdir(exist_ok=True)
    target = RELEASE / "hivezilla-stream"
    if target.exists():
        assert hashlib.sha256(target.read_bytes()).hexdigest() == sha, "immutable release already differs"
    else:
        shutil.copy2(binary, target)
        target.chmod(0o555)
    CONFIG.mkdir(mode=0o700, exist_ok=True)
    CONFIG.chmod(0o700)
    if not (CONFIG / "ca.crt").exists():
        certs(CONFIG, days=30)
        shutil.copyfile(CONFIG / "relay.pem", CONFIG / "relay-identity.pem")
        (CONFIG / "relay-identity.pem").chmod(0o600)
    for name in ["origin-api.key", "client-api.key"]:
        path = CONFIG / name
        if not path.exists():
            with path.open("x") as f:
                f.write(secrets.token_urlsafe(48) + "\n")
        path.chmod(0o600)
    spool = ROOT / "stream-relay-spool"
    spool.mkdir(mode=0o700, exist_ok=True)
    units = ["hivezilla-stream-origin.service", "hivezilla-stream-relay.service"]
    for name in units:
        subprocess.run(["sudo", "install", "-m", "644", str(SOURCE / "deploy" / name), "/etc/systemd/system/" + name], check=True)
    subprocess.run(["sudo", "systemd-analyze", "verify", *["/etc/systemd/system/" + n for n in units]], check=True)
    subprocess.run(["sudo", "systemctl", "daemon-reload"], check=True)
    subprocess.run(["sudo", "systemctl", "enable", "--now", units[0]], check=True)
    ctx = ssl.create_default_context(cafile=CONFIG / "ca.crt")
    ctx.load_cert_chain(CONFIG / "relay.crt", CONFIG / "relay.key")
    # transport.head uses a fixture token; production keys are never printed.
    from transport import req
    def origin_head():
        with req("https://127.0.0.1:8870/v1/head", ctx, token=(CONFIG / "origin-api.key").read_text().strip()) as r:
            return json.load(r)
    wait(lambda: origin_head().get("state") == "LIVE", 30)
    subprocess.run(["sudo", "systemctl", "enable", "--now", units[1]], check=True)
    def relay_head():
        with req("http://127.0.0.1:8871/v1/head", token=(CONFIG / "client-api.key").read_text().strip()) as r:
            return json.load(r)
    wait(lambda: relay_head().get("state") == "LIVE", 30)
    after = service_state()
    assert before == after, "native service state changed during pilot setup"
    report = {"schema": "hivezilla-stream-pilot-v1", "state": "RUNNING", "updated_unix_ms": int(time.time() * 1000),
              "binary": str(target), "binary_sha256": sha, "origin": "https://127.0.0.1:8870", "relay": "http://127.0.0.1:8871",
              "mTLS": True, "token_paths": [str(CONFIG / n) for n in ["origin-api.key", "client-api.key"]],
              "certificate_days": 30, "certificate_renewal_automated": False,
              "source_session": "000000000000000018daf342ba38d3d0-10e3f5", "source_session_switch_automated": False,
              "native_services_before": before, "native_services_after": after,
              "origin_head": origin_head(), "relay_head": relay_head(),
              "same_machine_pilot": True, "public_tunnel_changed": False, "second_machine_deployed": False}
    REPORT.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"state": "RUNNING", "mTLS": True, "upstream_http_version": report["relay_head"]["upstream_http_version"]}))


if __name__ == "__main__":
    main()
