#!/usr/bin/env python3
"""Install the reviewed processed stream on Foundation. No validator restart."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import time


def command(*args):
    return subprocess.run(args, check=True, capture_output=True, text=True).stdout.strip()


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".new")
    temporary.write_text(value)
    temporary.chmod(0o600)
    temporary.replace(path)


def unit(executable, writable, *, memory="1G", cpu="100%", after="network.target", description):
    return f"""[Unit]
Description={description}
After={after}
StartLimitIntervalSec=300
StartLimitBurst=5

[Service]
User=ubuntu
Group=ubuntu
UMask=0077
ExecStart={executable}
Restart=on-failure
RestartSec=5
CPUQuota={cpu}
MemoryMax={memory}
TasksMax=256
LimitNOFILE=2048
Nice=5
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=read-only
ReadWritePaths={writable}
PrivateTmp=true
ProtectKernelTunables=true
ProtectKernelModules=true
ProtectControlGroups=true
CapabilityBoundingSet=
TimeoutStopSec=30
KillMode=control-group

[Install]
WantedBy=multi-user.target
"""


def deploy(root, package):
    release = root / "releases/processed-stream-v1-20261010"
    engine = root / "releases/processed-worker-v4-20261010"
    publisher_release = root / "releases/processed-publisher-v3-20261010"
    for path in [release / "hivezilla-stream", release / "blockzilla",
                 publisher_release / "hivezilla-stream",
                 engine / "hivezilla-direct-agave", engine / "libhivezilla_direct_agave.so",
                 package / "worker_launcher.py", package / "pipeline_health.py"]:
        if not path.is_file():
            raise RuntimeError(f"Missing release file: {path}")
    reserve = 400 * 1024**3
    if shutil.disk_usage(root).free < reserve + 16 * 1024**3:
        raise RuntimeError("Less than 16 GiB of capture space above the disk reserve")
    report = root / "reports/processed-stream-deploy-20261010"
    report.mkdir(mode=0o700, exist_ok=False)
    names = ["hivezilla-direct-worker", "hivezilla-direct-health", "hivezilla-stream-origin",
             "hivezilla-stream-relay", "hivezilla-processed-stream-publisher"]
    for name in names:
        result = subprocess.run(["systemctl", "cat", name + ".service"], capture_output=True, text=True)
        write(report / (name + ".previous-unit"), result.stdout)
    config_path = root / "config/direct-agave.json"
    config = json.loads(config_path.read_text())
    write(report / "previous-plugin-config.json", json.dumps(config, indent=2) + "\n")
    validator_pid = int(command("systemctl", "show", "hivezilla-validator", "-p", "MainPID", "--value"))
    capture_root = root / "captures/direct-agave-live"
    previous = {p.name for p in capture_root.iterdir() if p.is_dir()}
    output_root = root / "compact/direct-agave-processed-live"
    output_root.mkdir(mode=0o700, exist_ok=True)
    spool = root / "stream-relay-spool-processed-20261010"
    spool.mkdir(mode=0o700, exist_ok=True)
    publisher_state = root / "reports/native-processed-r2-live-20261010"
    publisher_state.mkdir(mode=0o700, exist_ok=True)
    worker = unit(
        f"/usr/bin/python3 {package}/worker_launcher.py --processed --capture-root {capture_root} "
        f"--output-root {output_root} --engine {engine}/hivezilla-direct-agave --blockzilla {release}/blockzilla",
        f"{capture_root} {output_root}", memory="16G", cpu="800%",
        after="hivezilla-validator.service", description="Hivezilla complete processed bank builder")

    def install(name, text):
        path = report / (name + ".service")
        write(path, text)
        command("sudo", "install", "-m", "644", str(path), f"/etc/systemd/system/{name}.service")

    command("sudo", "systemctl", "stop", "hivezilla-direct-worker.service")
    install("hivezilla-direct-worker", worker)
    config["libpath"] = str(engine / "libhivezilla_direct_agave.so")
    config["reserve_disk_bytes"] = reserve
    write(config_path, json.dumps(config, indent=2) + "\n")
    validator = root / "releases/agave-shred-hook-v4.3.0/agave-validator"
    command(str(validator), "--ledger", str(root / "ledger"), "plugin", "reload",
            "hivezilla-direct-agave-v1", str(config_path))
    deadline = time.monotonic() + 30
    session = None
    while time.monotonic() < deadline:
        sessions = [p for p in capture_root.iterdir() if p.is_dir() and p.name not in previous
                    and (p / "SESSION.json").is_file() and (p / "events.sqlite").is_file()]
        if len(sessions) == 1:
            session = sessions[0]
            break
        time.sleep(0.2)
    if session is None:
        raise RuntimeError("New native capture did not start")
    output = output_root / session.name
    output.mkdir(mode=0o700, exist_ok=True)
    command("sudo", "systemctl", "daemon-reload")
    command("sudo", "systemctl", "reset-failed", "hivezilla-direct-worker.service")
    command("sudo", "systemctl", "start", "hivezilla-direct-worker.service")
    write(report / "DEPLOYMENT.json", json.dumps({"state": "WAITING_FOR_FEED", "session": str(session),
          "output": str(output), "validator_pid": validator_pid, "reserve_disk_bytes": reserve}) + "\n")
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        status_path = output / "STATUS.json"
        if status_path.is_file():
            status = json.loads(status_path.read_text())
            if status["state"] == "FAILED":
                raise RuntimeError(f"Processed worker failed: {status}")
            if status.get("event_sequence", 0) >= 16:
                break
        time.sleep(0.5)
    else:
        raise RuntimeError("Processed feed did not become ready; public services remain unchanged")
    keys = root / "config/stream-20261004"
    stream = publisher_release / "hivezilla-stream"
    origin = unit(
        f"{stream} origin --processed --output {output} --tail-events 256 --bind 127.0.0.1:8870 "
        f"--token-file {keys}/origin-api.key --tls-cert {keys}/origin.crt --tls-key {keys}/origin.key "
        f"--client-ca {keys}/ca.crt --max-requests 4",
        str(output / "feed.sqlite-shm"), memory="1G", cpu="200%",
        after="network.target hivezilla-direct-worker.service", description="Hivezilla read-only processed compact origin")
    relay = unit(
        f"{stream} relay --upstream https://127.0.0.1:8870 --upstream-token-file {keys}/origin-api.key "
        f"--upstream-ca {keys}/ca.crt --upstream-identity {keys}/relay-identity.pem --spool {spool} "
        f"--bind 127.0.0.1:8871 --token-file {keys}/client-api.key --cors-origin http://127.0.0.1:8891",
        str(spool), memory="2G", cpu="200%", after="network.target hivezilla-stream-origin.service",
        description="Hivezilla processed binary HTTP and SSE relay")
    publisher = unit(
        f"{stream} publish --processed --output {output} --state {publisher_state} "
        f"--credentials-file {root}/config/foundation-r2/native-stream.env "
        "--prefix native-stream/processed/direct-agave-live-v1 --start-sequence 1 --upload-workers 64",
        f"{publisher_state} {output}/feed.sqlite-shm", memory="2G", cpu="400%",
        after="network-online.target hivezilla-direct-worker.service", description="Hivezilla processed stream publication to Foundation R2")
    health = unit(
        f"/usr/bin/python3 {package}/pipeline_health.py --capture-root {capture_root} --output-root {output_root} "
        f"--shred-root {root}/captures/shred-hook-direct-20261002 --report {root}/reports/direct-agave-health.json",
        str(root / "reports"), memory="128M", description="Check native capture and processed compact progress")
    health = health.replace("Restart=on-failure\nRestartSec=5\n", "Type=oneshot\n")
    health = health.replace("StartLimitIntervalSec=300\nStartLimitBurst=5", "StartLimitIntervalSec=0")
    for name, text in [("hivezilla-stream-origin", origin), ("hivezilla-stream-relay", relay),
                       ("hivezilla-processed-stream-publisher", publisher), ("hivezilla-direct-health", health)]:
        install(name, text)
    command("sudo", "systemctl", "stop", "hivezilla-stream-relay.service", "hivezilla-stream-origin.service")
    command("sudo", "systemctl", "daemon-reload")
    command("sudo", "systemctl", "start", "hivezilla-stream-origin.service")
    time.sleep(2)
    command("sudo", "systemctl", "start", "hivezilla-stream-relay.service", "hivezilla-processed-stream-publisher.service")
    command("sudo", "systemctl", "enable", "hivezilla-direct-worker.service", "hivezilla-stream-origin.service",
            "hivezilla-stream-relay.service", "hivezilla-processed-stream-publisher.service")
    current_pid = int(command("systemctl", "show", "hivezilla-validator", "-p", "MainPID", "--value"))
    if current_pid != validator_pid:
        raise RuntimeError("Validator PID changed during deployment")
    hashes = {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in [
        engine / "hivezilla-direct-agave", engine / "libhivezilla_direct_agave.so",
        stream, release / "blockzilla"]}
    receipt = {"state": "DEPLOYED_PENDING_PUBLIC_CHECK", "session": str(session), "output": str(output),
               "validator_pid": validator_pid, "reserve_disk_bytes": reserve, "binaries": hashes,
               "previous_capture_gap_preserved": True, "local_retention_after_r2": False,
               "updated_unix_ms": int(time.time() * 1000)}
    write(report / "DEPLOYMENT.json", json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--package", type=Path, default=Path(__file__).resolve().parent)
    args = parser.parse_args()
    deploy(args.root.resolve(), args.package.resolve())
