#!/usr/bin/env python3
"""Verify that package merges cannot hide lost binaries or feature changes."""
import importlib.util
from pathlib import Path
import unittest


SPEC = importlib.util.spec_from_file_location(
    'workspace_targets', Path(__file__).with_name('check-workspace-targets.py'))
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def package(name, targets):
    return {'id': name, 'name': name, 'targets': targets}


def target(name, kind='bin', features=()):
    return {'name': name, 'kind': [kind], 'required-features': list(features)}


def metadata(*packages):
    return {'workspace_members': [p['id'] for p in packages], 'packages': packages}


class MergeTargetsTest(unittest.TestCase):
    def setUp(self):
        self.before = metadata(
            package('old-index', [target('old_index', 'lib'), target('old-index'), target('audit')]),
            package('user-index', [target('user_index', 'lib')]))
        self.after = metadata(package('user-index', [
            target('user_index', 'lib'), target('user-index', features=['cli']),
            target('audit', features=['tools'])]))
        self.rules = {
            'package_renames': {'old-index': 'user-index'},
            'removed_targets': [{'package': 'old-index', 'target': 'old_index', 'kind': ['lib']}],
            'feature_changes': [
                {'package': 'user-index', 'target': 'user-index', 'from': [], 'to': ['cli']},
                {'package': 'user-index', 'target': 'audit', 'from': [], 'to': ['tools']}],
        }

    def test_merge_retains_each_binary(self):
        self.assertEqual(MODULE.targets(self.before, **self.rules), MODULE.targets(self.after))

    def test_missing_binary_is_detected(self):
        self.after['packages'][0]['targets'].pop()
        missing = MODULE.targets(self.before, **self.rules) - MODULE.targets(self.after)
        self.assertEqual(list(missing), [('user-index', 'audit', ('bin',), ('tools',))])

    def test_only_declared_library_is_removed(self):
        self.before['packages'][0]['targets'].append(target('old_index'))
        self.assertNotEqual(MODULE.targets(self.before, **self.rules), MODULE.targets(self.after))

    def test_unexpected_feature_change_is_detected(self):
        self.after['packages'][0]['targets'][-1]['required-features'] = ['cli']
        self.assertNotEqual(MODULE.targets(self.before, **self.rules), MODULE.targets(self.after))

    def test_baseline_feature_drift_is_rejected(self):
        self.before['packages'][0]['targets'][-1]['required-features'] = ['extra']
        with self.assertRaisesRegex(ValueError, 'Unexpected baseline features'):
            MODULE.targets(self.before, **self.rules)

    def test_duplicate_library_requires_explicit_removal(self):
        self.rules['removed_targets'] = []
        extra = MODULE.targets(self.before, **self.rules) - MODULE.targets(self.after)
        self.assertEqual(list(extra), [('user-index', 'user_index', ('lib',), ())])


if __name__ == '__main__':
    unittest.main()
