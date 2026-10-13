#!/usr/bin/env python3
"""Install a tested demo relay with an immutable release and a unit backup."""
import argparse
import hashlib
import json
from pathlib import Path
import shlex
import shutil
import subprocess
import time
import urllib.request


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--root", required=True, type=Path)
    p.add_argument("--binary", required=True, type=Path)
    p.add_argument("--release-name", required=True)
    p.add_argument("--report-dir", required=True, type=Path)
    a = p.parse_args()
    assert a.release_name and all(c.isalnum() or c == "-" for c in a.release_name)
    service = "hivezilla-stream-relay.service"
    unit = Path("/etc/systemd/system") / service
    drop = unit.with_name(service + ".d") / "20-demo-access.conf"
    existing = drop.read_text() if drop.exists() else None
    overrides = subprocess.check_output(["systemctl", "show", service, "-p", "DropInPaths", "--value"], text=True).strip()
    assert overrides in ("", str(drop)), "Other overrides need explicit handling"
    original = unit.read_text()
    starts = [s.removeprefix("ExecStart=") for s in (existing or original).splitlines() if s.startswith("ExecStart=") and s.removeprefix("ExecStart=").strip()]
    assert len(starts) == 1
    command = shlex.split(starts[0])
    assert command[1] == "relay"
    release = a.root / "releases" / a.release_name
    assert not release.exists(), "Release already exists"
    a.report_dir.mkdir(parents=True, exist_ok=False)
    (a.report_dir / "relay-before.service").write_text(original)
    if existing is not None:
        (a.report_dir / "20-demo-access-before.conf").write_text(existing)
    release.mkdir()
    binary = release / "hivezilla-stream"
    shutil.copyfile(a.binary, binary)
    binary.chmod(0o555)
    release.chmod(0o555)
    command[0] = str(binary)
    for flag in ("--allow-query-api-key", "--public-blockhashes"):
        if flag not in command:
            command.append(flag)
    if "--public-max-requests" in command:
        assert command[command.index("--public-max-requests") + 1] == "8"
    else:
        command += ["--public-max-requests", "8"]
    override = "[Service]\nExecStart=\nExecStart=" + shlex.join(command) + "\n"
    staged = a.report_dir / "20-demo-access.conf"
    staged.write_text(override)
    key = (a.root / "config/stream-20261004/client-api.key").read_text().strip()

    def head():
        req = urllib.request.Request("http://127.0.0.1:8871/v1/head", headers={"Authorization": "Bearer " + key})
        with urllib.request.urlopen(req, timeout=2) as r:
            return json.load(r)

    before = head()
    validator_pid = subprocess.check_output(["systemctl", "show", "hivezilla-validator.service", "-p", "MainPID", "--value"], text=True).strip()
    report = {"state": "PREPARED", "release": str(release), "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
              "override": str(drop), "before_cursor": before["latest_cursor"], "validator_pid_before": validator_pid,
              "public_max_requests": 8, "allow_query_api_key": True, "public_blockhashes": True}

    def save():
        (a.report_dir / "DEPLOYMENT.json").write_text(json.dumps(report, indent=2) + "\n")

    save()
    installed = False
    try:
        subprocess.run(["sudo", "install", "-d", str(drop.parent)], check=True)
        subprocess.run(["sudo", "install", "-m", "644", str(staged), str(drop)], check=True)
        installed = True
        subprocess.run(["sudo", "systemctl", "daemon-reload"], check=True)
        subprocess.run(["sudo", "systemctl", "restart", service], check=True)
        for _ in range(100):
            try:
                current = head()
                if current["state"] == "LIVE" and current["latest_cursor"] != before["latest_cursor"]:
                    break
            except (OSError, ValueError):
                pass
            time.sleep(.2)
        else:
            raise RuntimeError("Relay did not advance after restart")
        req = urllib.request.Request("http://127.0.0.1:8871/v1/blockhashes")
        with urllib.request.urlopen(req, timeout=2) as r:
            assert r.status == 200
            assert r.readline().startswith(b": Blockzilla")
        pid = subprocess.check_output(["systemctl", "show", "hivezilla-validator.service", "-p", "MainPID", "--value"], text=True).strip()
        assert pid == validator_pid and pid != "0", "Validator process changed"
        report.update(state="COMPLETE", after_cursor=current["latest_cursor"], validator_pid_after=pid, completed_unix_ms=int(time.time() * 1000))
        save()
        print(json.dumps({"state": report["state"], "release": str(release), "validator_pid": pid}))
    except Exception as e:
        if installed:
            if existing is not None:
                subprocess.run(["sudo", "install", "-m", "644", str(a.report_dir / "20-demo-access-before.conf"), str(drop)], check=True)
            else:
                subprocess.run(["sudo", "rm", "--", str(drop)], check=True)
            subprocess.run(["sudo", "systemctl", "daemon-reload"], check=True)
            subprocess.run(["sudo", "systemctl", "restart", service], check=True)
        report.update(state="ROLLED_BACK" if installed else "FAILED", error=str(e).replace(key, "[REDACTED]"))
        save()
        raise SystemExit("Demo deployment failed; see the saved receipt")


if __name__ == "__main__":
    main()
