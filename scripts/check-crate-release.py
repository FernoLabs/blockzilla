#!/usr/bin/env python3
"""Check the CLI's complete registry dependency set. Never uploads packages."""

import argparse
import json
from pathlib import Path
import shlex
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]


def release_packages(metadata, load_metadata=None):
    packages = {package["name"]: package for package in metadata["packages"]}
    ordered = []
    visiting = set()
    visited = set()

    def local_dependency(dependency):
        name = dependency["name"]
        if name in packages:
            return
        manifest = Path(dependency["path"]) / "Cargo.toml"
        if load_metadata is None:
            raise ValueError(f"Local dependency {name} is outside the inspected workspace: {manifest}")
        # --no-deps omits path dependencies that have their own [workspace].
        # Inspect that workspace separately without resolving registry crates.
        for package in load_metadata(manifest)["packages"]:
            previous = packages.get(package["name"])
            if previous is not None and previous["manifest_path"] != package["manifest_path"]:
                raise ValueError(f"Multiple local packages named {package['name']}")
            packages[package["name"]] = package
        if name not in packages:
            raise ValueError(f"Local dependency {name} is absent from {manifest}")

    def visit(name):
        if name in visited:
            return
        if name in visiting:
            raise ValueError(f"Local dependency cycle at {name}")
        visiting.add(name)
        package = packages[name]
        # Cargo requires registry versions for optional dependencies too.
        for dependency in package["dependencies"]:
            if dependency.get("path") and dependency.get("kind") != "dev":
                local_dependency(dependency)
                visit(dependency["name"])
        visiting.remove(name)
        visited.add(name)
        ordered.append(package)

    visit("blockzilla")
    return ordered


def cargo_metadata(manifest=None, offline=False):
    command = ["cargo", "metadata", "--no-deps", "--format-version", "1"]
    if manifest is not None:
        command.extend(["--manifest-path", str(manifest)])
    if offline:
        command.append("--offline")
    return json.loads(subprocess.check_output(command, cwd=ROOT))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package", action="store_true", help="Create .crate files without compiling them")
    parser.add_argument("--dry-run", action="store_true", help="Run Cargo publish checks and compile the packages without uploading")
    parser.add_argument("--offline", action="store_true", help="Use only the local registry cache")
    parser.add_argument("--allow-dirty", action="store_true", help="Include current uncommitted source in the local check")
    parser.add_argument("--print-publish-command", action="store_true", help="Print the complete release command; do not run it")
    args = parser.parse_args()
    if args.package and args.dry_run:
        parser.error("Select --package or --dry-run")
    try:
        packages = release_packages(
            cargo_metadata(offline=args.offline),
            lambda manifest: cargo_metadata(manifest, args.offline),
        )
    except (ValueError, OSError, subprocess.CalledProcessError) as error:
        print(f"Cannot inspect the release dependency set: {error}", file=sys.stderr)
        return 1
    issues = []
    for package in packages:
        registries = package["publish"]
        if registries is not None and "crates-io" not in registries:
            issues.append(f"{package['name']}: crates.io publication is disabled")
        if not package["description"]:
            issues.append(f"{package['name']}: package description is missing")
        for dependency in package["dependencies"]:
            if dependency.get("path") and dependency["req"] == "*":
                issues.append(f"{package['name']}: {dependency['name']} has no registry version")
        print(f"{package['name']} {package['version']}")
    if issues:
        print("\n".join(issues), file=sys.stderr)
        return 1
    print(f"Checked {len(packages)} release packages. No upload was made.", flush=True)
    if args.print_publish_command:
        command = ["cargo", "publish"]
        for package in packages:
            command.extend(["-p", package["name"]])
        print(shlex.join(command))
    if args.package or args.dry_run:
        command = ["cargo", "package", "--no-verify"] if args.package else ["cargo", "publish", "--dry-run"]
        if args.offline:
            command.append("--offline")
        if args.allow_dirty:
            command.append("--allow-dirty")
        for package in packages:
            command.extend(["-p", package["name"]])
        return subprocess.call(command, cwd=ROOT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
