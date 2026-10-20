#!/usr/bin/env python3
"""Prepare or install storage guards without restarting the validator or feed."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import time


FILES = ("cold_storage.py", "recover_session.py", "snapshot_budget.py", "load_when_ready.py",
         "heartbeat.py", "archive_status.py", "live_priority.py", "archive_upload.py")
VALIDATOR = "hivezilla-validator"
HEARTBEAT = "hivezilla-foundation-heartbeat"
PRIORITY = "hivezilla-live-priority-20261012"
STORAGE = "hivezilla-native-cold-storage-20261012-live"
DROPIN = "z" * 64 + "-storage-patch.conf"
MINIMUM_STARTUP_BYTES = (500 + 128) * 1024**3


def run(*args):
    return subprocess.check_output(args, text=True).strip()


def regular_bytes(path, limit=1024**2):
    before = path.lstat()
    if not stat.S_ISREG(before.st_mode) or before.st_size > limit:
        raise RuntimeError("invalid_patch_file")
    with path.open("rb") as stream:
        body = stream.read(limit + 1)
    if len(body) > limit:
        raise RuntimeError("invalid_patch_file")
    after = path.lstat()
    if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
            after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns):
        raise RuntimeError("patch_file_changed_during_read")
    return body


def module(name, body, path):
    spec = importlib.util.spec_from_file_location(name, path)
    value = importlib.util.module_from_spec(spec)
    sys.modules[name] = value  # dataclass resolves its module through this table.
    exec(compile(body, str(path), "exec"), value.__dict__)
    return value


def option(args, name, recovery):
    if args.count(name) != 1:
        raise RuntimeError("missing_or_repeated_option")
    position = args.index(name)
    if position + 1 == len(args) or args[position + 1].startswith("--"):
        raise RuntimeError("missing_option_value")
    value = args[position + 1]
    recovery.replace_option(args, name, value)
    return value


def script_command(args, name, path, recovery):
    positions = [i for i, value in enumerate(args) if Path(value).name == name]
    if len(positions) != 1 or positions[0] == 0:
        raise RuntimeError("unexpected_script_command")
    result = args.copy()
    result[positions[0]] = str(path)
    recovery.systemd_command(result)
    return result


def hooks(text, directive, recovery):
    result = []
    for line in text.splitlines():
        if line.startswith(directive + "="):
            value = line[len(directive) + 1:]
            if not value:
                result = []
            else:
                result.append(recovery.exec_args("ExecStart=" + value))
    return result


def replace_hook(commands, name, replacement):
    positions = [i for i, args in enumerate(commands) if any(Path(a).name == name for a in args)]
    if len(positions) > 1:
        raise RuntimeError("repeated_validator_hook")
    result = [args.copy() for args in commands]
    if positions:
        result[positions[0]] = replacement
    else:
        result.append(replacement)
    return result


def preflight(root, release, recovery, snapshots, proc_root):
    names = list(dict.fromkeys([*recovery.NAMES, VALIDATOR, HEARTBEAT, PRIORITY]))
    units = {name: run("systemctl", "cat", name + ".service") for name in names}
    commands = {name: recovery.exec_args(text) for name, text in units.items()}
    targets = {name: DROPIN for name in (VALIDATOR, "hivezilla-direct-worker", HEARTBEAT, PRIORITY)}
    # The explicit source handoff must replace this same cold-storage drop-in.
    # A later override here would keep the service bound to its failed source.
    targets[STORAGE] = recovery.SOURCE_DROPIN
    for name, args in commands.items():
        recovery.systemd_command(args)
        for line in units[name].splitlines():
            if name in targets and line.startswith("# ") and ".service.d/" in line and line.endswith(".conf"):
                if Path(line[2:]).name != targets[name] and Path(line[2:]).name >= targets[name]:
                    raise RuntimeError("storage_dropin_does_not_sort_last")
    relay = commands["hivezilla-stream-relay"]
    source_path = Path(option(relay, "--banks-source-file", recovery))
    source = recovery.source_tuple(json.loads(regular_bytes(source_path)))
    if (not all(isinstance(value, str) for value in source.values())
            or not re.fullmatch(r"[0-9a-f]{32}-[0-9a-f]{1,16}", source["session_id"])):
        raise RuntimeError("invalid_current_source")
    # These helpers validate every option before any service or source change.
    for name, args, _ in recovery.replacement_commands(commands, root, source["session_id"], source_path):
        if args != commands[name]:
            raise RuntimeError("current_service_source_mismatch")
    worker = commands["hivezilla-direct-worker"]
    for flag in ("--engine", "--blockzilla", "--capture-root", "--output-root"):
        option(worker, flag, recovery)
    pid = run("systemctl", "show", VALIDATOR, "-p", "MainPID", "--value")
    if not pid.isdigit() or int(pid) <= 0:
        raise RuntimeError("validator_not_running")
    binary = Path(commands[VALIDATOR][0])
    if binary.resolve(strict=True) != (proc_root / pid / "exe").resolve(strict=True):
        raise RuntimeError("running_validator_binary_mismatch")
    help_text = run(str(binary), "--help")
    validator = snapshots.bounded_snapshot_args(commands[VALIDATOR], help_text)
    snapshot_path = Path(option(validator, "--snapshots", recovery))
    if snapshot_path != root / "snapshots":
        raise RuntimeError("unexpected_snapshot_directory")
    # The deployed host already loads the plugin after catch-up. Automatic
    # plugin startup plus a load hook would create an unsafe double load.
    if "--geyser-plugin-config" in validator or any(a.startswith("--geyser-plugin-config=") for a in validator):
        raise RuntimeError("automatic_plugin_start_requires_review")
    if "--geyser-plugin-always-enabled" not in validator:
        raise RuntimeError("missing_dynamic_plugin_support")
    gate = ["/usr/bin/python3", str(release / "snapshot_budget.py"), "--check-start",
            "--snapshots", str(snapshot_path)]
    loader = ["/usr/bin/python3", str(release / "load_when_ready.py"), "--root", str(root),
              "--binary", str(binary), "--config", str(root / "config/direct-agave.json")]
    before = replace_hook(hooks(units[VALIDATOR], "ExecStartPre", recovery), "snapshot_budget.py", gate)
    after = replace_hook(hooks(units[VALIDATOR], "ExecStartPost", recovery), "load_when_ready.py", loader)
    extra = "ExecStartPre=\n" + "".join("ExecStartPre=" + recovery.systemd_command(a) + "\n" for a in before)
    extra += "ExecStartPost=\n" + "".join("ExecStartPost=" + recovery.systemd_command(a) + "\n" for a in after)
    extra += "ReadWritePaths=" + recovery.systemd_command([str(root / "reports")]) + "\n"
    overrides = {
        VALIDATOR: recovery.override_text(validator, extra=extra),
        "hivezilla-direct-worker": "[Service]\nEnvironment=" + recovery.systemd_command([
            "HIVEZILLA_NATIVE_ARCHIVE_HELPER=" + str(release / "cold_storage.py")]) + "\n",
    }
    for name, filename in ((STORAGE, "cold_storage.py"), (HEARTBEAT, "heartbeat.py"), (PRIORITY, "live_priority.py")):
        changed = script_command(commands[name], filename, release / filename, recovery)
        overrides[name] = recovery.override_text(changed)
    return units, commands, overrides, targets, gate, pid, source


def prepare_release(release, bodies, hashes):
    manifest = {"schema": "blockzilla-storage-patch-release-v1", "files": hashes}
    if release.exists():
        if release.is_symlink() or json.loads(regular_bytes(release / "MANIFEST.json")) != manifest:
            raise RuntimeError("existing_release_mismatch")
        for name, sha in hashes.items():
            path = release / name
            if hashlib.sha256(regular_bytes(path)).hexdigest() != sha or path.stat().st_mode & 0o222:
                raise RuntimeError("existing_release_mismatch")
        if release.stat().st_mode & 0o222:
            raise RuntimeError("existing_release_is_writable")
        return
    release.parent.mkdir(parents=True, exist_ok=True)
    release.mkdir(mode=0o700)
    for name, body in bodies.items():
        with (release / name).open("xb") as stream:
            stream.write(body); stream.flush(); os.fsync(stream.fileno())
        (release / name).chmod(0o500)
    with (release / "MANIFEST.json").open("x") as stream:
        json.dump(manifest, stream, sort_keys=True)
        stream.flush(); os.fsync(stream.fileno())
    (release / "MANIFEST.json").chmod(0o400)
    release.chmod(0o500)


def startup_budget(gate):
    try:
        output = run(*gate)
    except subprocess.CalledProcessError as error:
        if error.returncode != 75:
            raise
        output = error.output
    value = json.loads(output)
    threshold = value.get("recovery_with_next_archive_threshold_free_bytes", MINIMUM_STARTUP_BYTES)
    free = value.get("current_free_bytes", 0)
    valid = type(threshold) is int and threshold >= MINIMUM_STARTUP_BYTES and type(free) is int
    passed = valid and value.get("startup_disk_gate_passed") is True and free >= threshold
    return value, passed


def install_patch(source_dir, root, *, prepare_only=False, systemd_root=Path("/etc/systemd/system"),
                  proc_root=Path("/proc")):
    root, source_dir = root.resolve(strict=True), source_dir.resolve(strict=True)
    bodies = {name: regular_bytes(source_dir / name) for name in FILES}
    for name, body in bodies.items():
        compile(body, str(source_dir / name), "exec")
    hashes = {name: hashlib.sha256(body).hexdigest() for name, body in bodies.items()}
    content_hash = hashlib.sha256(json.dumps(hashes, sort_keys=True).encode()).hexdigest()
    release = root / "releases" / ("storage-recovery-20261013-" + content_hash[:16])
    report = root / "reports" / release.name
    recovery = module("storage_patch_recovery_" + content_hash, bodies["recover_session.py"], source_dir / "recover_session.py")
    snapshots = module("storage_patch_snapshots_" + content_hash, bodies["snapshot_budget.py"], source_dir / "snapshot_budget.py")
    units, commands, overrides, targets, gate, pid, source = preflight(root, release, recovery, snapshots, proc_root)
    prepare_release(release, bodies, hashes)
    report.mkdir(parents=True, exist_ok=True, mode=0o700)
    for name, text in units.items():
        recovery.write(report / (name + ".previous-unit"), text + "\n")
    budget, passed = startup_budget(gate)
    record = {"schema": "blockzilla-storage-patch-install-v1", "state": "PREPARED",
              "release": str(release), "report": str(report), "content_sha256": content_hash,
              "artifact_sha256": hashes, "source": source, "validator_pid": int(pid),
              "startup_budget": budget, "startup_disk_gate_passed": passed,
              "cold_storage_helper": str(release / "cold_storage.py"),
              "recovery_helper": str(release / "recover_session.py"),
              "snapshot_budget_helper": str(release / "snapshot_budget.py"),
              "native_loader": str(release / "load_when_ready.py"),
              "dropin_paths": {name: str(systemd_root / (name + ".service.d") / targets[name]) for name in overrides},
              "overrides": overrides, "restarted_units": [], "source_handoff_required": True,
              "credentials_read_or_copied": False, "updated_unix_ms": int(time.time() * 1000)}
    recovery.json_write(report / "INSTALLATION.json", record)
    if prepare_only:
        return record
    if not passed:
        raise RuntimeError("insufficient_current_startup_budget")
    # Recheck host identity and all current unit definitions at the apply boundary.
    if run("systemctl", "show", VALIDATOR, "-p", "MainPID", "--value") != pid:
        raise RuntimeError("validator_pid_changed_before_install")
    for name, old in units.items():
        if run("systemctl", "cat", name + ".service") != old:
            raise RuntimeError("unit_changed_after_preflight")
    recovery.run = run
    for name, text in overrides.items():
        target = systemd_root / (name + ".service.d") / targets[name]
        recovery.install(report, target, text)
    run("sudo", "systemctl", "daemon-reload")
    run("sudo", "systemctl", "restart", HEARTBEAT + ".service", PRIORITY + ".service")
    if run("systemctl", "show", VALIDATOR, "-p", "MainPID", "--value") != pid:
        raise RuntimeError("validator_pid_changed_during_install")
    record.update(state="INSTALLED_PENDING_SOURCE_HANDOFF", validator_pid_unchanged=True,
                  restarted_units=[HEARTBEAT, PRIORITY], updated_unix_ms=int(time.time() * 1000))
    recovery.json_write(report / "INSTALLATION.json", record)
    return record


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dir", type=Path, required=True)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--prepare-only", action="store_true")
    args = parser.parse_args()
    os.umask(0o077)
    print(json.dumps(install_patch(args.source_dir, args.root, prepare_only=args.prepare_only)))
