#!/usr/bin/env python3
"""Compare Cargo metadata across a layout change with explicit name changes."""
import argparse
import collections
import json
from pathlib import Path


def targets(metadata, package_renames=None, removed=(), target_renames=None, relocations=()):
    package_renames = package_renames or {}
    target_renames = target_renames or {}
    relocation_map = {(r['from_package'], r['target']): r['to_package'] for r in relocations}
    members = set(metadata['workspace_members'])
    result = collections.Counter()
    for package in metadata['packages']:
        if package['id'] not in members or package['name'] in removed:
            continue
        name = package_renames.get(package['name'], package['name'])
        for target in package['targets']:
            target_name = target['name']
            for old, new in package_renames.items():
                if target_name == old:
                    target_name = new
                elif target_name == old.replace('-', '_'):
                    target_name = new.replace('-', '_')
            target_name = target_renames.get(target_name, target_name)
            owner = relocation_map.get((name, target_name), name)
            result[(owner, target_name, tuple(sorted(target['kind'])),
                    tuple(sorted(target.get('required-features', []))))] += 1
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('before', type=Path)
    parser.add_argument('after', type=Path)
    parser.add_argument('--layout', type=Path, required=True)
    args = parser.parse_args()
    layout = json.loads(args.layout.read_text())
    before = json.loads(args.before.read_text())
    after = json.loads(args.after.read_text())
    members_before = set(before['workspace_members'])
    baseline_names = {p['name'] for p in before['packages'] if p['id'] in members_before}
    mapped_names = [entry['package'] for entry in layout['packages']]
    if len(mapped_names) != len(set(mapped_names)) or set(mapped_names) != baseline_names:
        raise SystemExit('Layout must list every baseline workspace package exactly once.')
    expected = targets(before, layout.get('package_renames', {}),
                       layout.get('removed_packages', []),
                       layout.get('target_renames', {}), layout.get('target_relocations', []))
    actual = targets(after)
    for title, differences in [('Missing targets', expected - actual),
                               ('Unexpected targets', actual - expected)]:
        if differences:
            print(title + ':')
            for item, count in sorted(differences.items()):
                print(f'  {count} x {item}')
    if expected != actual:
        raise SystemExit(1)
    members = set(after['workspace_members'])
    paths = {package['name']: Path(package['manifest_path']).parent
             for package in after['packages'] if package['id'] in members}
    root = Path(after['workspace_root'])
    for entry in layout['packages']:
        if entry['package'] in layout.get('removed_packages', []):
            continue
        name = layout.get('package_renames', {}).get(entry['package'], entry['package'])
        expected_path = root / entry['destination']
        if paths.get(name) != expected_path:
            raise SystemExit(f'Wrong destination for {name}: {paths.get(name)}; expected {expected_path}')
    binaries = sum(count for key, count in actual.items() if 'bin' in key[2])
    print(f'Verified {len(paths)} packages and {sum(actual.values())} targets, including {binaries} binaries.')


if __name__ == '__main__':
    main()
