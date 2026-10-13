#!/usr/bin/env python3
"""Share sealed, byte-identical benchmark outputs. Keep all paths and reports."""
import argparse
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import stat
import time
import uuid

NAMES = {"transactions.bin", "transactions.bin.spool"}


def save(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + ".new")
    with open(temp, "w") as f:
        json.dump(data, f, indent=2)
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(temp, path)
    sync_dir(path.parent)


def sync_dir(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def stamp(s):
    return s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns


def hashed(path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode):
            raise RuntimeError("not_regular_file")
        digest = hashlib.sha256()
        while block := os.read(fd, 8 * 1024**2):
            digest.update(block)
        after = os.fstat(fd)
        if stamp(before) != stamp(after) or stamp(after) != stamp(path.lstat()):
            raise RuntimeError("file_changed_while_hashing")
        return {"path": str(path), "stamp": list(stamp(after)),
                "sha256": digest.hexdigest(), "allocated_bytes": after.st_blocks * 512}
    finally:
        os.close(fd)


def writer_open(records):
    """Reject a candidate if an accessible process has its inode open for writing."""
    inodes = {(r["stamp"][0], r["stamp"][1]) for r in records}
    proc = Path("/proc")
    if not proc.is_dir():
        raise RuntimeError("writer_check_requires_linux")
    for process in proc.iterdir():
        if not process.name.isdigit():
            continue
        try:
            for fd in (process / "fd").iterdir():
                try:
                    s = fd.stat()
                    if (s.st_dev, s.st_ino) not in inodes:
                        continue
                    info = (process / "fdinfo" / fd.name).read_text()
                    flags = next(int(line.split()[1], 8) for line in info.splitlines()
                                 if line.startswith("flags:"))
                    if flags & os.O_ACCMODE != os.O_RDONLY:
                        return True
                except (OSError, StopIteration, ValueError):
                    continue
        except OSError:
            continue
    return False


def link_identical(canonical, duplicate):
    if canonical["sha256"] != duplicate["sha256"]:
        raise RuntimeError("digest_mismatch")
    a, b = Path(canonical["path"]), Path(duplicate["path"])
    sa, sb = a.lstat(), b.lstat()
    if not stat.S_ISREG(sa.st_mode) or not stat.S_ISREG(sb.st_mode):
        raise RuntimeError("not_regular_file")
    if (sa.st_dev, sa.st_ino) == (sb.st_dev, sb.st_ino):
        return 0
    if list(stamp(sa)) != canonical["stamp"] or list(stamp(sb)) != duplicate["stamp"]:
        raise RuntimeError("file_changed_before_link")
    if sa.st_dev != sb.st_dev or sa.st_size != sb.st_size:
        raise RuntimeError("filesystem_or_size_mismatch")
    if sa.st_uid != os.getuid() or sb.st_uid != os.getuid():
        raise RuntimeError("file_not_owned")
    # These old outputs are sealed. Shared copies must not be opened for writes.
    os.chmod(a, stat.S_IMODE(sa.st_mode) & ~0o222)
    canonical["stamp"] = list(stamp(a.lstat()))
    temp = b.with_name(".benchmark-dedupe-" + uuid.uuid4().hex)
    try:
        os.link(a, temp, follow_symlinks=False)
        if (temp.stat().st_dev, temp.stat().st_ino) != (sa.st_dev, sa.st_ino):
            raise RuntimeError("linked_file_changed")
        # Creating a link changes canonical ctime. Check content identity fields.
        current = a.lstat()
        if (current.st_dev, current.st_ino, current.st_size, current.st_mtime_ns) != stamp(sa)[:4]:
            raise RuntimeError("canonical_changed_before_replace")
        if list(stamp(b.lstat())) != duplicate["stamp"]:
            raise RuntimeError("duplicate_changed_before_replace")
        os.replace(temp, b)
        sync_dir(b.parent)
        canonical["stamp"] = list(stamp(a.lstat()))
        return sb.st_blocks * 512 if sb.st_nlink == 1 else 0
    finally:
        if temp.exists():
            temp.unlink()


def run(root, report, apply=False, min_bytes=1024**3, workers=4):
    root = root.resolve(strict=True)
    sizes = {}
    for directory, folders, files in os.walk(root, followlinks=False):
        folders[:] = [d for d in folders if not (Path(directory) / d).is_symlink()]
        for name in files:
            path = Path(directory) / name
            if name not in NAMES or not path.relative_to(root).parts[0].startswith("results-"):
                continue
            s = path.lstat()
            if stat.S_ISREG(s.st_mode) and s.st_size >= min_bytes:
                sizes.setdefault((s.st_dev, s.st_size), []).append(path)
    candidates = [p for group in sizes.values() if len(group) > 1 for p in group]
    state = {"schema": "blockzilla-benchmark-dedupe-v1", "state": "HASHING",
             "root": str(root), "apply": apply, "candidate_files": len(candidates),
             "hashed_files": 0, "linked_files": 0, "estimated_freed_bytes": 0,
             "all_paths_retained": True, "reports_retained": True, "records": []}
    save(report / "STATUS.json", state)
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(hashed, p) for p in candidates]
        for future in concurrent.futures.as_completed(futures):
            state["records"].append(future.result())
            state["hashed_files"] += 1
            state["updated_unix_ms"] = int(time.time() * 1000)
            save(report / "STATUS.json", state)
    groups = {}
    for record in state["records"]:
        groups.setdefault((record["stamp"][0], record["stamp"][2], record["sha256"]), []).append(record)
    groups = [sorted(g, key=lambda r: r["path"]) for g in groups.values() if len(g) > 1]
    state["matching_groups"] = len(groups)
    if apply:
        if writer_open(state["records"]):
            raise RuntimeError("benchmark_output_in_use")
        state["state"] = "LINKING"
        save(report / "STATUS.json", state)
        for group in groups:
            for duplicate in group[1:]:
                freed = link_identical(group[0], duplicate)
                if freed:
                    state["linked_files"] += 1
                    state["estimated_freed_bytes"] += freed
                save(report / "STATUS.json", state)
    state["state"] = "DONE" if apply else "PLAN"
    state["updated_unix_ms"] = int(time.time() * 1000)
    save(report / "RECEIPT.json", state)
    save(report / "STATUS.json", state)
    return {k: v for k, v in state.items() if k != "records"}


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--root", type=Path, required=True)
    p.add_argument("--report", type=Path, required=True)
    p.add_argument("--apply", action="store_true")
    p.add_argument("--workers", type=int, choices=range(1, 9), default=4)
    p.add_argument("--min-gib", type=int, default=1)
    a = p.parse_args()
    if a.min_gib < 1:
        p.error("--min-gib must be positive")
    print(json.dumps(run(a.root, a.report, a.apply, min_bytes=a.min_gib * 1024**3, workers=a.workers)))
