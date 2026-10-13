"""Copy retained CAR-derived compact archives to a private R2 namespace."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import re
import struct
import subprocess
import time


CORE = {
    "registry.bin", "registry_counts.bin", "registry.mphf",
    "blockhash_registry.bin", "archive-v2-meta.wincode",
    "archive-v2-blocks.index", "archive-v2-blocks.zstd", "poh.wincode",
    "signatures.bin", "shredding.wincode", "vote_hash_registry.bin",
}

COMPACT_FILES = CORE | {
    "archive-v2-get-block.index", "archive-v2-block-access.wincode",
    "archive-v2-block-access.index", "archive-v2-pre-to-post.candidate.v1.json",
    "blockhash_index_v3.bin", "block-time-gaps.bin", "prev_blockhash_tail.bin",
    "skipped_slots.bin", "registry-first-seen.manifest", "registry-hot-seed.bin",
    ".hivezilla-pipeline-owned.v1.json", "blockzilla-compaction-receipt.tsv",
    "blockzilla-car-import-receipt.json", ".complete-hot-v2-no-access-delete-car",
}

ALL_FILES_PROFILE = "all-compact-files"
QUERY_SIGNATURES_PROFILE = "query-with-signatures"
QUERY_EXCLUDED_FILES = {
    "poh.wincode", "shredding.wincode", "archive-v2-block-access.wincode",
    "archive-v2-block-access.index", "archive-v2-get-block.index",
}


def profile_files(profile):
    if profile == ALL_FILES_PROFILE:
        return set(), "BLOCKZILLA-BACKUP-MANIFEST.json"
    if profile == QUERY_SIGNATURES_PROFILE:
        return QUERY_EXCLUDED_FILES, "BLOCKZILLA-QUERY-WITH-SIGNATURES-MANIFEST.json"
    raise RuntimeError(f"Unsupported upload profile: {profile}")


def sync_dir(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def save(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w") as output:
        json.dump(value, output, indent=2)
        output.write("\n")
        output.flush()
        os.fsync(output.fileno())
    temporary.replace(path)
    sync_dir(path.parent)


def stamp(path):
    s = path.stat(follow_symlinks=False)
    if path.is_symlink() or not path.is_file():
        raise RuntimeError(f"Not a regular source file: {path}")
    return [s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns]


def inventory(folder, profile=ALL_FILES_PROFILE):
    excluded, _ = profile_files(profile)
    files = {}
    for path in folder.iterdir():
        if path.is_symlink():
            raise RuntimeError(f"Not a regular source file: {path}")
        if (path.is_dir() and re.fullmatch(
                r"partial-loop-restart-[0-9]{8}T[0-9]{6}Z", path.name)):
            continue
        if path.name.endswith(".lock"):
            continue
        if re.fullmatch(r"\.poh-signature-count-migration\.lock\.stale-[0-9]{8}", path.name):
            continue
        if not re.fullmatch(r"[A-Za-z0-9._-]+", path.name):
            raise RuntimeError(f"Unsupported archive filename: {path}")
        if path.name not in COMPACT_FILES:
            raise RuntimeError(f"Not a Compact V2 archive file: {path}")
        identity = stamp(path)
        if path.name not in excluded:
            files[path.name] = identity
    if not (CORE - excluded).issubset(files):
        raise RuntimeError(f"Missing core compact files: {folder}")
    with (folder / "archive-v2-blocks.index").open("rb") as index:
        magic, version, reserved, count, blob_bytes, level, flags = struct.unpack(
            "<8sHHQQiI", index.read(36)
        )
    if (magic, version, reserved, flags) != (b"BZV2HIX1", 1, 0, 0):
        raise RuntimeError(f"Unsupported compact index: {folder}")
    if count > 432000 or files["archive-v2-blocks.index"][2] != 36 + count * 52:
        raise RuntimeError(f"Invalid index length: {folder}")
    if files["archive-v2-blocks.zstd"][2] != blob_bytes:
        raise RuntimeError(f"Compact blob length differs from index: {folder}")
    return dict(sorted(files.items()))


def rclone(args, *command, capture=False):
    base = [args.rclone, "--config", args.config, "--contimeout", "10s", "--timeout", "2m"]
    result = subprocess.run(base + list(map(str, command)), text=True,
                            stdout=subprocess.PIPE if capture else None, check=True)
    return result.stdout if capture else None


def upload(args, state, epoch):
    profile = getattr(args, "profile", ALL_FILES_PROFILE)
    excluded, manifest_name = profile_files(profile)
    folder = Path(args.root) / f"epoch-{epoch}"
    before = inventory(folder, profile)
    target = f"{args.destination}/epoch-{epoch}"
    epoch_state = state / f"epoch-{epoch}"
    epoch_state.mkdir(exist_ok=True)
    binding = {"source": str(folder), "destination": target, "files": before}
    if profile != ALL_FILES_PROFILE:
        binding["upload_profile"] = profile
    intent = epoch_state / "intent.json"
    if intent.exists():
        if json.loads(intent.read_text()) != binding:
            raise RuntimeError(f"Source or destination changed for epoch {epoch}")
    else:
        save(intent, binding)
    complete = epoch_state / "complete.json"
    if complete.exists():
        return
    file_list = epoch_state / "files.txt"
    file_list.write_text("\n".join(before) + "\n")
    save(state / "STATUS.json", {"state": "UPLOADING", "epoch": epoch,
                                 "destination": target, "upload_profile": profile})
    rclone(args, "copy", folder, target, "--files-from-raw", file_list,
           "--immutable", "--size-only", "--transfers", getattr(args, "transfers", 2), "--checkers", "2",
           "--s3-disable-checksum", "--s3-chunk-size", getattr(args, "chunk_size", "32M"),
           "--s3-upload-concurrency", getattr(args, "upload_concurrency", 8),
           "--bwlimit", args.bandwidth, "--retries", "4", "--retries-sleep", "15s",
           "--stats", "30s", "--stats-one-line", "--log-level", "INFO")
    if inventory(folder, profile) != before:
        raise RuntimeError(f"Archive changed during upload: epoch {epoch}")
    remote = json.loads(rclone(args, "lsjson", target, "--files-only", "--max-depth", "1", capture=True))
    actual = {row["Name"]: row["Size"] for row in remote}
    expected = {name: identity[2] for name, identity in before.items()}
    actual.pop(manifest_name, None)
    retained_extras = {}
    if profile == QUERY_SIGNATURES_PROFILE:
        for name in excluded | {"BLOCKZILLA-BACKUP-MANIFEST.json"}:
            if name in actual:
                retained_extras[name] = actual.pop(name)
    if actual != expected:
        raise RuntimeError(f"Remote object inventory mismatch: epoch {epoch}")
    report = {"schema": "blockzilla-stored-compact-backup-v1", "epoch": epoch,
              "source_profile": "retained-car-derived-compact-directory",
              "source": str(folder), "destination": target,
              "canonical_publication": False, "archive_coverage_verified_by_backup": False,
              "remote_file_sizes_verified": True, "full_remote_readback": False,
              "source_files": before, "bytes": sum(expected.values()),
              "completed_unix_seconds": int(time.time())}
    if profile != ALL_FILES_PROFILE:
        report.update(upload_profile=profile, excluded_files=sorted(excluded),
                      previously_uploaded_extras_retained=retained_extras)
    manifest = epoch_state / manifest_name
    if manifest.exists():
        saved = json.loads(manifest.read_text())
        if (saved["source_files"] != before or saved["destination"] != target
                or saved.get("upload_profile", ALL_FILES_PROFILE) != profile):
            raise RuntimeError("Backup manifest binding changed")
        report = saved
    else:
        save(manifest, report)
    rclone(args, "copyto", manifest, target + "/" + manifest.name, "--immutable", "--size-only")
    if json.loads(rclone(args, "cat", target + "/" + manifest.name, capture=True)) != report:
        raise RuntimeError("Remote manifest differs from local manifest")
    save(complete, report)
    print(json.dumps({"state": "COMPLETE", "epoch": epoch, "bytes": report["bytes"]}), flush=True)


def main(args):
    profile = getattr(args, "profile", ALL_FILES_PROFILE)
    profile_files(profile)
    state = Path(args.state).resolve()
    state.mkdir(parents=True, exist_ok=True)
    with (state / "LOCK").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        epochs = sorted({int(folder.name[6:]) for folder in Path(args.root).glob("epoch-*")
                         if folder.is_dir() and folder.name[6:].isdigit()}, reverse=True)
        if args.epochs != "all":
            epochs = sorted({int(n) for n in args.epochs.split(",")}, reverse=True)
        if args.future_epoch in epochs:
            epochs.remove(args.future_epoch)
        plan_path = state / "PLAN.json"
        if plan_path.exists() and args.epochs == "all":
            epochs = json.loads(plan_path.read_text())["epochs"]
        plan = {"source": args.root, "destination": args.destination, "epochs": epochs,
                "future_epoch": args.future_epoch, "local_source_deletion": False}
        if profile != ALL_FILES_PROFILE:
            plan["upload_profile"] = profile
        if plan_path.exists() and json.loads(plan_path.read_text()) != plan:
            raise RuntimeError("Existing upload plan differs from this invocation")
        if not plan_path.exists():
            save(plan_path, plan)
        future_done = args.future_epoch is None
        for epoch in epochs:
            future = Path(args.root) / f"epoch-{args.future_epoch}"
            if not future_done and (future / "blockzilla-car-import-receipt.json").is_file():
                upload(args, state, args.future_epoch)
                future_done = True
            upload(args, state, epoch)
        while not future_done:
            producer_status = Path(args.producer_state) / "status.json"
            if producer_status.is_file() and json.loads(producer_status.read_text()).get("state") == "FAIL":
                raise RuntimeError("Latest CAR producer failed; retained archive uploads are complete")
            if (future / "blockzilla-car-import-receipt.json").is_file():
                upload(args, state, args.future_epoch)
                future_done = True
            else:
                save(state / "STATUS.json", {"state": "WAITING_COMPACTION", "epoch": args.future_epoch})
                time.sleep(30)
        save(state / "STATUS.json", {"state": "COMPLETE", "epochs": epochs,
                                     "future_epoch": args.future_epoch, "upload_profile": profile})


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    for name in ["root", "state", "config", "destination"]:
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--rclone", default="/usr/bin/rclone")
    parser.add_argument("--epochs", default="all")
    parser.add_argument("--profile", choices=[ALL_FILES_PROFILE, QUERY_SIGNATURES_PROFILE],
                        default=ALL_FILES_PROFILE)
    parser.add_argument("--future-epoch", type=int)
    parser.add_argument("--producer-state")
    parser.add_argument("--bandwidth", default="400M")
    parser.add_argument("--transfers", type=int, default=2)
    parser.add_argument("--upload-concurrency", type=int, default=8)
    parser.add_argument("--chunk-size", default="32M")
    arguments = parser.parse_args()
    if arguments.transfers <= 0 or arguments.upload_concurrency <= 0:
        parser.error("Transfer and multipart concurrency must be positive")
    try:
        main(arguments)
    except Exception as error:
        save(Path(arguments.state) / "STATUS.json", {"state": "FAILED", "error": str(error)})
        raise
