#!/usr/bin/env python3
"""Stage tested Agave 4.3.2 artifacts and prepare or apply service configuration.

Prepare (default): --base BUILD --root LIVE --report NEW_REPORT
Apply saved plan:  --base BUILD --root LIVE --report SAME_REPORT --apply

No operation here stops, starts, restarts, loads, or unloads a service or plugin.
The configured host and plugin take effect only at a separate validator restart.
The new worker and block builder require a separate, explicit source handoff.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import stat
import subprocess
import time


VERSION = "4.3.2"
UPSTREAM_REVISION = "a203300045b405f86c9c84308b2eae76af69ac3b"
ARTIFACTS = ("agave-validator", "libhivezilla_direct_agave.so",
             "hivezilla-direct-agave", "blockzilla")
VALIDATOR = "hivezilla-validator.service"
RECORDER = "hivezilla-shred-hook-recorder.service"
DROPIN = "z" * 128 + "-agave-4.3.2.conf"
FULL_FLAG = "--maximum-full-snapshots-to-retain"
INCREMENTAL_FLAG = "--maximum-incremental-snapshots-to-retain"
SYSTEMD_ROOT = Path("/etc/systemd/system")


def run(*args):
    return subprocess.check_output(args, text=True, stderr=subprocess.STDOUT).strip()


def sha256(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def regular(path):
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode):
        raise RuntimeError("expected_regular_file: " + str(path))
    return info


def read_bytes(path):
    regular(path)
    return path.read_bytes()


def write(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".new")
    with temporary.open("wb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    temporary.chmod(0o600)
    temporary.replace(path)


def json_bytes(value):
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()


def save_record(report, record):
    record["updated_unix_ms"] = int(time.time() * 1000)
    write(report / "INSTALLATION.json", json_bytes(record))


def command(args):
    # systemd does not use shell quoting. Refuse directive/specifier expansion.
    if not args or not args[0].startswith("/") or any(
            not isinstance(arg, str) or any(c in arg for c in "\n\r\0%$")
            for arg in args):
        raise RuntimeError("unsafe_systemd_command")
    return " ".join(json.dumps(arg, ensure_ascii=False) for arg in args)


def effective_commands(unit_text):
    """Read effective Service commands, including resets and continuations."""
    directives = {name: [] for name in ("ExecStart", "ExecStartPre", "ExecStartPost")}
    service, continued = False, ""
    for raw in unit_text.splitlines():
        line = raw.strip()
        if line.startswith(("#", ";")):
            continue
        line = continued + line
        if len(line) - len(line.rstrip("\\")) & 1:
            continued = line[:-1] + " "
            continue
        continued = ""
        if line.startswith("["):
            service = line == "[Service]"
            continue
        if not service or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key, value = key.strip(), value.strip()
        if key not in directives:
            continue
        if value:
            args = shlex.split(value)
            command(args)
            directives[key].append(args)
        else:
            directives[key] = []
    if continued or len(directives["ExecStart"]) != 1:
        raise RuntimeError("ambiguous_service_command")
    return directives


def option(args, flag):
    positions = [i for i, arg in enumerate(args)
                 if arg == flag or arg.startswith(flag + "=")]
    if len(positions) != 1:
        raise RuntimeError("missing_or_repeated_option: " + flag)
    index = positions[0]
    if args[index] == flag:
        if index + 1 == len(args) or args[index + 1].startswith("--"):
            raise RuntimeError("missing_option_value: " + flag)
        return index + 1, args[index + 1], False
    return index, args[index].split("=", 1)[1], True


def replace_option(args, flag, old, new):
    index, value, equal = option(args, flag)
    if value != str(old):
        raise RuntimeError("unexpected_option_value: " + flag)
    result = args.copy()
    result[index] = flag + "=" + str(new) if equal else str(new)
    return result


def bounded_validator(args, binary, help_text):
    if args.count("--no-voting") != 1:
        raise RuntimeError("validator_must_remain_non_voting")
    if any(arg.split("=", 1)[0] == "--geyser-plugin-config" for arg in args):
        raise RuntimeError("automatic_plugin_load_requires_separate_review")
    if args.count("--geyser-plugin-always-enabled") != 1:
        raise RuntimeError("missing_dynamic_plugin_support")
    if "--no-snapshots" in args or "--no-incremental-snapshots" in args:
        raise RuntimeError("snapshot_generation_must_remain_enabled")
    result = args.copy()
    result[0] = str(binary)
    for flag in ("--no-voting", "--geyser-plugin-always-enabled"):
        if not re.search(r"(?m)^\s*" + re.escape(flag) + r"(?:\s|$)", help_text):
            raise RuntimeError("new_host_missing_required_flag: " + flag)
    # Keep all existing argument positions and values except explicit retention.
    for flag in (FULL_FLAG, INCREMENTAL_FLAG):
        if not re.search(r"(?m)^\s*" + re.escape(flag) + r"\s+<NUMBER>\s*$", help_text):
            raise RuntimeError("new_host_missing_retention_flag: " + flag)
        _, previous, _ = option(result, flag)
        if not previous.isdigit() or int(previous) < 1:
            raise RuntimeError("invalid_snapshot_retention")
        result = replace_option(result, flag, previous, "1")
    command(result)
    return result


def check_dropin_order(unit, text):
    for line in text.splitlines():
        if line.startswith("# ") and unit + ".d/" in line and line.endswith(".conf"):
            if Path(line[2:]).name >= DROPIN:
                raise RuntimeError("upgrade_dropin_does_not_sort_last: " + unit)


def release_inputs(base):
    build_body, source_body = read_bytes(base / "BUILD.json"), read_bytes(base / "SOURCE.json")
    build, source = json.loads(build_body), json.loads(source_body)
    revision = build.get("agave_revision", "")
    if (build.get("state") != "PASSED" or not re.fullmatch(r"[0-9a-f]{40}", revision)
            or source.get("agave_revision") != revision
            or source.get("upstream_tag") != "v" + VERSION
            or source.get("upstream_revision") != UPSTREAM_REVISION):
        raise RuntimeError("passed_build_and_official_source_receipts_required")
    if run("git", "-C", str(base / "agave"), "rev-parse", "HEAD") != revision:
        raise RuntimeError("build_source_revision_changed")
    if run("git", "-C", str(base / "agave"), "status", "--porcelain"):
        raise RuntimeError("build_source_has_uncommitted_changes")
    if run("git", "-C", str(base / "agave"), "rev-parse", "refs/tags/v4.3.2^{}") != UPSTREAM_REVISION:
        raise RuntimeError("official_tag_revision_mismatch")
    artifacts = {}
    for name in ARTIFACTS:
        value = build.get("artifacts", {}).get(name, {})
        digest = value.get("sha256", "")
        path = Path(value.get("path", ""))
        if not path.is_absolute() or not path.is_relative_to(base) or not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise RuntimeError("invalid_artifact_receipt: " + name)
        regular(path)
        if path.resolve(strict=True) != path or sha256(path) != digest:
            raise RuntimeError("artifact_hash_or_path_mismatch: " + name)
        artifacts[name] = {"path": str(path), "sha256": digest}
    return build, source, {"BUILD.json": build_body, "SOURCE.json": source_body}, artifacts


def stage_release(root, revision, receipts, artifacts):
    release = root / "releases" / ("agave-4.3.2-20261016-" + revision[:16])
    hashes = {name: value["sha256"] for name, value in artifacts.items()}
    hashes.update({name: hashlib.sha256(body).hexdigest() for name, body in receipts.items()})
    manifest = {"schema": "blockzilla-agave-upgrade-release-v1", "agave_revision": revision,
                "upstream_revision": UPSTREAM_REVISION, "upstream_tag": "v" + VERSION,
                "files": hashes}
    if release.exists():
        if release.is_symlink() or json.loads(read_bytes(release / "MANIFEST.json")) != manifest:
            raise RuntimeError("existing_release_manifest_mismatch")
        verify_release(release, manifest)
        return release, manifest
    release.parent.mkdir(parents=True, exist_ok=True)
    pending = release.with_name(release.name + ".staging-" + str(os.getpid()))
    pending.mkdir(mode=0o700)
    try:
        for name, item in artifacts.items():
            shutil.copyfile(item["path"], pending / name)
        for name, body in receipts.items():
            write(pending / name, body)
        for name, expected in hashes.items():
            if sha256(pending / name) != expected:
                raise RuntimeError("staged_artifact_hash_mismatch: " + name)
            with (pending / name).open("rb") as stream:
                os.fsync(stream.fileno())
            (pending / name).chmod(0o500 if name in ARTIFACTS else 0o400)
        write(pending / "MANIFEST.json", json_bytes(manifest))
        (pending / "MANIFEST.json").chmod(0o400)
        pending.chmod(0o500)
        pending.rename(release)
    except Exception:
        if pending.exists():
            pending.chmod(0o700)
            shutil.rmtree(pending)
        raise
    return release, manifest


def verify_release(release, manifest):
    if release.is_symlink() or release.stat().st_mode & 0o222:
        raise RuntimeError("release_is_writable_or_symlink")
    if json.loads(read_bytes(release / "MANIFEST.json")) != manifest:
        raise RuntimeError("release_manifest_changed")
    regular(release / "MANIFEST.json")
    if (release / "MANIFEST.json").stat().st_mode & 0o222:
        raise RuntimeError("release_manifest_is_writable")
    for name, expected in manifest["files"].items():
        path = release / name
        if regular(path).st_mode & 0o222 or sha256(path) != expected:
            raise RuntimeError("immutable_release_changed: " + name)


def target_state(path):
    if not path.exists() and not path.is_symlink():
        return {"existed": False}
    info = regular(path)
    return {"existed": True, "sha256": sha256(path), "mode": stat.S_IMODE(info.st_mode),
            "uid": info.st_uid, "gid": info.st_gid}


def gate_command(commands, snapshot_directory, root):
    gates = [args for args in commands["ExecStartPre"]
             if any(Path(arg).name == "snapshot_budget.py" for arg in args)]
    if len(gates) != 1:
        raise RuntimeError("one_existing_snapshot_startup_gate_required")
    gate = gates[0]
    if (len(gate) < 3 or gate[0] != "/usr/bin/python3"
            or Path(gate[1]).name != "snapshot_budget.py"
            or not Path(gate[1]).is_relative_to(root / "releases")
            or gate.count("--check-start") != 1
            or option(gate, "--snapshots")[1] != str(snapshot_directory)):
        raise RuntimeError("unexpected_snapshot_startup_gate")
    regular(Path(gate[1]))
    return gate


def startup_budget(gate):
    try:
        output = run(*gate)
    except subprocess.CalledProcessError as error:
        if error.returncode != 75:
            raise
        output = error.output
    budget = json.loads(output)
    threshold = budget.get("recovery_with_next_archive_threshold_free_bytes")
    free = budget.get("current_free_bytes")
    passed = (budget.get("startup_disk_gate_passed") is True and type(threshold) is int
              and threshold >= 500 * 1024**3 and type(free) is int and free >= threshold)
    return budget, passed


def prepare(base, root, report):
    if report.exists():
        raise RuntimeError("new_report_directory_required; use --apply for a saved plan")
    build, source, receipts, artifacts = release_inputs(base)
    release, manifest = stage_release(root, build["agave_revision"], receipts, artifacts)
    binary = release / "agave-validator"
    version = run(str(binary), "--version")
    if not re.search(r"\b4\.3\.2\b", version):
        raise RuntimeError("new_validator_version_mismatch")
    help_text = run(str(binary), "--help")
    units = {name: run("sudo", "systemctl", "cat", name) for name in (VALIDATOR, RECORDER)}
    commands = {name: effective_commands(text) for name, text in units.items()}
    for name, text in units.items():
        check_dropin_order(name, text)
    old_validator = commands[VALIDATOR]["ExecStart"][0]
    validator = bounded_validator(old_validator, binary, help_text)
    snapshots = Path(option(validator, "--snapshots")[1])
    if snapshots != root / "snapshots":
        raise RuntimeError("unexpected_snapshot_directory")
    gate = gate_command(commands[VALIDATOR], snapshots, root)
    config_path = root / "config/direct-agave.json"
    config_body = read_bytes(config_path)
    config = json.loads(config_body)
    if not isinstance(config, dict) or not isinstance(config.get("libpath"), str):
        raise RuntimeError("invalid_existing_plugin_configuration")
    old_libpath = config["libpath"]
    config["libpath"] = str(release / "libhivezilla_direct_agave.so")
    after = commands[VALIDATOR]["ExecStartPost"]
    loaders = [i for i, args in enumerate(after)
               if any(Path(arg).name == "load_when_ready.py" for arg in args)]
    if len(loaders) != 1:
        raise RuntimeError("one_existing_native_plugin_loader_required")
    after = [args.copy() for args in after]
    loader = after[loaders[0]]
    if option(loader, "--config")[1] != str(config_path) or option(loader, "--root")[1] != str(root):
        raise RuntimeError("unexpected_native_plugin_loader_configuration")
    after[loaders[0]] = replace_option(loader, "--binary", old_validator[0], binary)
    old_recorder = commands[RECORDER]["ExecStart"][0]
    old_expected = Path(option(old_recorder, "--expected-executable")[1])
    if (not old_expected.is_absolute() or old_expected.name != "agave-validator"
            or not old_expected.is_relative_to(root / "releases")
            or not regular(old_expected).st_mode & 0o111
            or not old_expected.resolve(strict=True).is_relative_to(root / "releases")):
        raise RuntimeError("unexpected_recorder_executable_identity")
    recorder = replace_option(old_recorder, "--expected-executable", old_expected, binary)
    # Changing only this option preserves all recorder byte and reserve limits.
    proposed_validator = ("[Service]\nExecStart=\nExecStart=" + command(validator)
                          + "\nExecStartPost=\n" + "".join(
                              "ExecStartPost=" + command(args) + "\n" for args in after))
    proposed_recorder = "[Service]\nExecStart=\nExecStart=" + command(recorder) + "\n"
    proposed = {
        config_path: json_bytes(config),
        SYSTEMD_ROOT / (VALIDATOR + ".d") / DROPIN: proposed_validator.encode(),
        SYSTEMD_ROOT / (RECORDER + ".d") / DROPIN: proposed_recorder.encode(),
    }
    budget, passed = startup_budget(gate)
    report.mkdir(mode=0o700, parents=True)
    for name, text in units.items():
        write(report / (name + ".previous-unit"), (text + "\n").encode())
    write(report / "previous-plugin-config.json", config_body)
    write(report / "effective-commands.json", json_bytes(commands))
    changes = []
    for target, body in proposed.items():
        before = target_state(target)
        relative = str(target).lstrip("/")
        new = report / "new" / relative
        write(new, body)
        if before["existed"]:
            write(report / "saved" / relative, read_bytes(target))
        changes.append({"path": str(target), "proposed": str(new), "before": before,
                        "after_sha256": hashlib.sha256(body).hexdigest(),
                        "mode": before["mode"] if target == config_path else 0o644,
                        "uid": before["uid"] if target == config_path else 0,
                        "gid": before["gid"] if target == config_path else 0})
    record = {
        "schema": "blockzilla-agave-upgrade-install-v1", "state": "PREPARED",
        "base": str(base), "root": str(root), "report": str(report), "release": str(release),
        "agave_revision": build["agave_revision"], "upstream_source": source,
        "release_manifest": manifest, "artifacts": manifest["files"],
        "validator_binary": str(binary), "validator_version": version,
        "worker_binary": str(release / "hivezilla-direct-agave"),
        "worker_sha256": artifacts["hivezilla-direct-agave"]["sha256"],
        "blockzilla_binary": str(release / "blockzilla"),
        "blockzilla_sha256": artifacts["blockzilla"]["sha256"],
        "plugin_config": str(config_path), "old_plugin_libpath": old_libpath,
        "old_validator_binary": old_validator[0], "changes": changes,
        "old_recorder_expected_executable": str(old_expected),
        "old_recorder_validator_path_mismatch": str(old_expected) != old_validator[0],
        "unit_sha256": {name: hashlib.sha256(body.encode()).hexdigest() for name, body in units.items()},
        "effective_commands": commands, "proposed_validator_args": validator,
        "proposed_recorder_args": recorder, "proposed_loader_args": after[loaders[0]],
        "snapshot_gate": gate, "snapshot_gate_sha256": sha256(Path(gate[1])),
        "startup_budget": budget, "startup_disk_gate_passed": passed,
        "validator_pid": run("systemctl", "show", VALIDATOR, "-p", "MainPID", "--value"),
        "restarted_units": [], "plugin_operations": [], "source_handoff_required": True,
    }
    save_record(report, record)
    return record


def apply(base, root, report, record):
    if (record.get("schema") != "blockzilla-agave-upgrade-install-v1"
            or record.get("state") != "PREPARED" or record.get("base") != str(base)
            or record.get("root") != str(root) or record.get("report") != str(report)):
        raise RuntimeError("matching_prepared_upgrade_receipt_required")
    verify_release(Path(record["release"]), record["release_manifest"])
    expected_paths = {str(root / "config/direct-agave.json"), *(
        str(SYSTEMD_ROOT / (name + ".d") / DROPIN) for name in (VALIDATOR, RECORDER))}
    if {change["path"] for change in record["changes"]} != expected_paths or len(record["changes"]) != 3:
        raise RuntimeError("unexpected_upgrade_target_paths")
    for change in record["changes"]:
        proposed = Path(change["proposed"])
        if (proposed != report / "new" / change["path"].lstrip("/")
                or hashlib.sha256(read_bytes(proposed)).hexdigest() != change["after_sha256"]):
            raise RuntimeError("proposed_configuration_changed")
        if target_state(Path(change["path"])) != change["before"]:
            raise RuntimeError("configuration_changed_after_prepare: " + change["path"])
    for name, expected in record["unit_sha256"].items():
        actual = run("sudo", "systemctl", "cat", name)
        if hashlib.sha256(actual.encode()).hexdigest() != expected:
            raise RuntimeError("effective_unit_changed_after_prepare: " + name)
        check_dropin_order(name, actual)
    gate = record["snapshot_gate"]
    if sha256(Path(gate[1])) != record["snapshot_gate_sha256"]:
        raise RuntimeError("snapshot_startup_gate_changed_after_prepare")
    budget, passed = startup_budget(gate)
    record.update(startup_budget=budget, startup_disk_gate_passed=passed)
    save_record(report, record)
    if not passed:
        raise RuntimeError("current_snapshot_startup_disk_gate_failed")
    pid = run("systemctl", "show", VALIDATOR, "-p", "MainPID", "--value")
    if pid != record["validator_pid"]:
        raise RuntimeError("validator_pid_changed_after_prepare")
    # Repeat each content check at the write boundary. This does not restart a service.
    for change in record["changes"]:
        target = Path(change["path"])
        if target_state(target) != change["before"]:
            raise RuntimeError("configuration_changed_at_apply: " + str(target))
        if not target.parent.exists():
            run("sudo", "install", "-d", "-m", "755", str(target.parent))
        run("sudo", "install", "-m", format(change["mode"], "o"),
            "-o", str(change["uid"]), "-g", str(change["gid"]), change["proposed"], str(target))
        if sha256(target) != change["after_sha256"]:
            raise RuntimeError("installed_configuration_hash_mismatch: " + str(target))
    run("sudo", "systemctl", "daemon-reload")
    after_pid = run("systemctl", "show", VALIDATOR, "-p", "MainPID", "--value")
    record.update(state="CONFIGURED", validator_pid_unchanged=after_pid == pid,
                  configured_unix_ms=int(time.time() * 1000))
    save_record(report, record)
    if after_pid != pid:
        raise RuntimeError("validator_pid_changed_during_configuration")
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--apply", action="store_true", help="apply a saved or new prepared plan; never restart")
    args = parser.parse_args()
    os.umask(0o077)
    base, root = args.base.resolve(strict=True), args.root.resolve(strict=True)
    report = args.report.resolve()
    if args.apply and (report / "INSTALLATION.json").exists():
        record = json.loads(read_bytes(report / "INSTALLATION.json"))
    else:
        record = prepare(base, root, report)
    if args.apply:
        record = apply(base, root, report, record)
    print(json.dumps(record))


if __name__ == "__main__":
    main()
