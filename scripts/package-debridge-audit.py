#!/usr/bin/env python3
"""Copy the audit's local Rust dependencies into a private build bundle."""
import hashlib
import json
from pathlib import Path
import re
import shutil
import tarfile
import tempfile
import tomllib


root = Path(__file__).resolve().parents[1]
tool = root / "indexer/blockzilla-delegation-audit"
packages = set()


def visit(directory):
    directory = directory.resolve()
    if directory in packages:
        return
    directory.relative_to(root)
    packages.add(directory)
    config = tomllib.loads((directory / "Cargo.toml").read_text())

    def dependencies(table):
        for kind in ("dependencies", "dev-dependencies", "build-dependencies"):
            for dependency in table.get(kind, {}).values():
                if isinstance(dependency, dict) and "path" in dependency:
                    visit(directory / dependency["path"])
        for target in table.get("target", {}).values():
            dependencies(target)

    dependencies(config)


visit(tool)
stage = Path(tempfile.mkdtemp(prefix="debridge-build-source-", dir="/private/tmp"))
for package in sorted(packages):
    destination = stage / package.relative_to(root)
    shutil.copytree(package, destination, ignore=shutil.ignore_patterns(
        "target", ".git", "node_modules", "evidence", "__pycache__", "*.sqlite", "*.sqlite-wal", "*.sqlite-shm"
    ))

members = [str(package.relative_to(root)) for package in sorted(packages) if package != tool]
workspace = (root / "Cargo.toml").read_text()
workspace = re.sub(r"members\s*=\s*\[.*?\n\]", "members = " + json.dumps(members, indent=2), workspace, count=1, flags=re.S)
workspace = re.sub(r"exclude\s*=\s*\[[^\]]*\]", "exclude = [\"indexer/blockzilla-delegation-audit\"]", workspace, count=1)
(stage / "Cargo.toml").write_text(workspace)
shutil.copyfile(root / "Cargo.lock", stage / "Cargo.lock")
archive = stage.with_suffix(".tar.gz")
with tarfile.open(archive, "w:gz") as output:
    for path in sorted(stage.iterdir()):
        output.add(path, arcname=path.name)
print(json.dumps({"source": str(stage), "bundle": str(archive), "bytes": archive.stat().st_size,
                  "sha256": hashlib.sha256(archive.read_bytes()).hexdigest(), "packages": members}))
