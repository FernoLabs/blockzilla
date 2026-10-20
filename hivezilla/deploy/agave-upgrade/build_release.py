#!/usr/bin/env python3
"""Build and test an isolated patched Agave host and its native plugin pair."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--jobs", type=int, default=8)
    args = parser.parse_args()
    base = args.base.resolve(strict=True)
    host, provider = base / "agave", base / "provider"
    revision = subprocess.check_output(
        ["git", "-C", str(host), "rev-parse", "HEAD"], text=True
    ).strip()
    env = os.environ.copy()
    env.update(
        PATH="/home/ubuntu/.cargo/bin:" + env.get("PATH", "/usr/bin:/bin"),
        HIVEZILLA_AGAVE_REVISION=revision,
        CARGO_BUILD_JOBS=str(args.jobs),
        RUSTFLAGS="-C target-cpu=x86-64 -C target-feature=+aes,+sse2",
        BLOCKZILLA_BUILDER=str(base / "blockzilla"),
        BLOCKZILLA_TEST_BINARY=str(base / "blockzilla"),
    )
    receipt = {
        "state": "RUNNING", "agave_revision": revision,
        "started_unix_ms": int(time.time() * 1000), "commands": [],
    }

    def save():
        tmp = base / "BUILD.json.new"
        tmp.write_text(json.dumps(receipt, indent=2) + "\n")
        tmp.replace(base / "BUILD.json")

    def cargo(label, cwd, target, *argv, toolchain="1.97.1"):
        env["CARGO_TARGET_DIR"] = str(base / target)
        command = ["/home/ubuntu/.cargo/bin/cargo", "+" + toolchain, *argv]
        entry = {"label": label, "cwd": str(cwd), "argv": command,
                 "started_unix_ms": int(time.time() * 1000)}
        receipt["commands"].append(entry)
        save()
        print(label, flush=True)
        with (base / (label + ".log")).open("w") as log:
            result = subprocess.run(command, cwd=cwd, env=env, stdout=log,
                                    stderr=subprocess.STDOUT)
        entry.update(returncode=result.returncode, completed_unix_ms=int(time.time() * 1000))
        save()
        if result.returncode:
            raise RuntimeError(f"{label} failed; see {base / (label + '.log')}")

    patch = []
    for name, directory in [
        ("agave-geyser-plugin-interface", "geyser-plugin-interface"),
        ("solana-transaction-status", "transaction-status"),
        ("solana-transaction-status-client-types", "transaction-status-client-types"),
    ]:
        patch += ["--config", f'patch."https://github.com/anza-xyz/agave".{name}.path="{host / directory}"']
    native = provider / "hivezilla/direct-agave"
    manifest = native / "Cargo.toml"
    lines = manifest.read_text().splitlines()
    for name, directory in [("agave-geyser-plugin-interface", "geyser-plugin-interface"),
                            ("solana-transaction-status", "transaction-status")]:
        indexes = [i for i, line in enumerate(lines) if line.startswith(name + " = ")]
        if len(indexes) != 1:
            raise RuntimeError(f"expected one native dependency: {name}")
        lines[indexes[0]] = (f'{name} = {{ path = "{host / directory}", '
                             'features = ["agave-unstable-api"], optional = true }')
    manifest.write_text("\n".join(lines) + "\n")
    receipt["native_manifest_sha256"] = hashlib.sha256(manifest.read_bytes()).hexdigest()
    common = ["--features", "bank-v2", *patch]
    try:
        cargo("builder-tests", provider, "target-builder", "test", "--offline", "--locked",
              "-p", "blockzilla", "--bin", "blockzilla", "direct_agave", toolchain="1.98.0")
        cargo("builder-build", provider, "target-builder", "build", "--offline", "--locked",
              "--release", "-p", "blockzilla", "--bin", "blockzilla", toolchain="1.98.0")
        shutil.copy2(base / "target-builder/release/blockzilla", base / "blockzilla")
        cargo("shared-tests", provider / "crates/blockzilla-bank-stream-v2", "target-native",
              "test", "--offline")
        # Resolve the new patched path packages once, then freeze that lock file.
        cargo("native-tests", native, "target-native", "test", "--ignore-rust-version", *common)
        cargo("native-builder-tests", native, "target-native", "test", "--offline", "--locked",
              "--ignore-rust-version", "--lib", *common, "--", "--ignored")
        cargo("native-build", native, "target-native", "build", "--offline", "--locked",
              "--ignore-rust-version", "--release", *common)
        canary = base / "native-load-canary"
        if canary.exists():
            canary.rename(base / ("native-load-canary-previous-" + str(time.time_ns())))
        canary.mkdir()
        config = {
            "bank_v2": True,
            "libpath": str(base / "target-native/release/libhivezilla_direct_agave.so"),
            "output_root": str(canary / "capture"),
            "genesis_hash": "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d",
            "node_identity": "J7qpvCEWhdBgPdYRV3s3btWPrRVXa2qnAzgvxWTvtbrQ",
            "queue_events": 1024, "queue_bytes": 8 * 1024**2,
            "max_event_bytes": 1024**2, "max_journal_bytes": 64 * 1024**2,
            "reserve_disk_bytes": 1024**2, "commit_interval_ms": 2,
        }
        (canary / "config.json").write_text(json.dumps(config, indent=2) + "\n")
        env.update(HIVEZILLA_NATIVE_TEST_CONFIG=str(canary / "config.json"),
                   HIVEZILLA_NATIVE_TEST_RECEIPT=str(canary / "NATIVE-LOAD.json"))
        cargo("native-host-load", host, "target-host", "test", "--release", "-p",
              "solana-geyser-plugin-manager", "--features", "agave-unstable-api", "--lib",
              "native_bank_v2_actual_plugin_load", "--", "--ignored", "--nocapture")
        load = json.loads((canary / "NATIVE-LOAD.json").read_text())
        if load["state"] != "PASSED" or load["slot"] != 17 or load["last_valid_block_height"] != 151:
            raise RuntimeError("actual native host tuple test failed")
        captures = list((canary / "capture").glob("*/SESSION.json"))
        if len(captures) != 1:
            raise RuntimeError("canary must contain exactly one native session")
        session = json.loads(captures[0].read_text())
        if session["agave_revision"] != revision:
            raise RuntimeError("canary plugin revision mismatch")
        db = sqlite3.connect(captures[0].parent / "events.sqlite")
        kinds = dict(db.execute("SELECT kind,count(*) FROM events GROUP BY kind"))
        db.close()
        if kinds.get("bank_state_v2") != 2:
            raise RuntimeError(f"canary observations missing: {kinds}")
        receipt["native_canary"] = {"load": load, "events": kinds, "source_revision": revision}
        cargo("host-tuple-tests", host, "target-host", "test", "--offline", "--release", "-p",
              "solana-rpc", "--features", "agave-unstable-api", "--lib",
              "test_hivezilla_native_bank_tuple_v2")
        cargo("host-build", host, "target-host", "build", "--offline", "--release",
              "--bin", "agave-validator")
        if subprocess.check_output(["git", "-C", str(host), "status", "--porcelain"], text=True).strip():
            raise RuntimeError("host source changed after its revision was pinned")
        source_manifest = {
            str(path.relative_to(provider)): hashlib.file_digest(path.open("rb"), "sha256").hexdigest()
            for path in sorted(provider.rglob("*")) if path.is_file() and not path.is_symlink()
        }
        manifest_path = base / "PROVIDER-SOURCE.json"
        manifest_path.write_text(json.dumps(source_manifest, sort_keys=True, indent=2) + "\n")
        receipt["provider_manifest_sha256"] = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
        artifacts = {
            "agave-validator": base / "target-host/release/agave-validator",
            "hivezilla-direct-agave": base / "target-native/release/hivezilla-direct-agave",
            "libhivezilla_direct_agave.so": base / "target-native/release/libhivezilla_direct_agave.so",
            "blockzilla": base / "blockzilla",
        }
        receipt["artifacts"] = {name: {"path": str(path), "sha256": hashlib.file_digest(
            path.open("rb"), "sha256").hexdigest()} for name, path in artifacts.items()}
        receipt["validator_version"] = subprocess.check_output(
            [str(artifacts["agave-validator"]), "--version"], text=True).strip()
        receipt.update(state="PASSED", completed_unix_ms=int(time.time() * 1000))
        save()
    except Exception as error:
        receipt.update(state="FAILED", error=str(error), completed_unix_ms=int(time.time() * 1000))
        save()
        raise


if __name__ == "__main__":
    main()
