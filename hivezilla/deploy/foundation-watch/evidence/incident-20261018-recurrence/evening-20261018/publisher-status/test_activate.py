import copy
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location('publisher_activate', Path(__file__).with_name('activate.py'))
m = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(m)


class ActivationTests(unittest.TestCase):
    def test_binary_replacement_preserves_every_argument_and_other_directive(self):
        body = b'[Service]\nExecStart=\nExecStart="/old/bin" "publish" --output "/a b" --processed\nReadWritePaths=/a\n'
        old = ['/old/bin', 'publish', '--output', '/a b', '--processed']
        proposed = m.replacement(body, old, '/new/bin')
        self.assertEqual(proposed, body.replace(b'"/old/bin"', b'/new/bin'))
        self.assertEqual(m.command(proposed), ['/new/bin', *old[1:]])
        with self.assertRaisesRegex(RuntimeError, 'expanded_execstart'):
            m.command(b'ExecStart=/old/bin $ARGS\n')
        self.assertEqual(m.command(b'ExecStart=/ignored\nExecStart=\nExecStart=/effective x\n'), ['/effective', 'x'])

    def cloud_fixture(self):
        source = {'session_id': 'session-1', 'genesis_hash': 'genesis', 'node_identity': 'node', 'agave_revision': 'revision'}
        cp = {'prefix': 'native-stream/processed/test/session-1', 'sequence': 100, 'start_slot': 1}
        cursor = 'session-1:p110:' + 'a' * 64
        head = {'schema': 'blockzilla-native-stream-catalog-v1', 'profile': m.PROFILE, 'session': 'session-1',
                'start_slot': 1, 'sequence': 110, 'latest_cursor': cursor, 'canonical': False,
                'publication_ready': False, 'last_commit': cp['prefix'] + '/commits/00000000000000000110.json'}
        commit = {'schema': 'blockzilla-native-stream-commit-v1', 'profile': m.PROFILE, 'session': 'session-1',
                  'sequence': 110, 'cursor': cursor, 'genesis_hash': 'genesis', 'node_identity': 'node',
                  'agave_revision': 'revision', 'canonical': False, 'publication_ready': False,
                  'object': cp['prefix'] + '/chunks/123-' + 'b' * 64 + '.frame', 'sha256': 'b' * 64, 'bytes': 1024}
        return head, commit, cp, source

    def test_cloud_head_and_commit_bind_exact_source_sequence_and_namespace_path(self):
        head, commit, cp, source = self.cloud_fixture()
        self.assertEqual(m.validate_cloud(head, commit, cp, source), head['last_commit'])
        for target, field, value in [('head', 'sequence', 99), ('head', 'last_commit', 'foreign/commit'),
                                     ('commit', 'agave_revision', 'other'), ('commit', 'cursor', 'wrong'),
                                     ('commit', 'object', 'foreign/chunk'), ('commit', 'bytes', 0)]:
            h, c = copy.deepcopy(head), copy.deepcopy(commit)
            (h if target == 'head' else c)[field] = value
            with self.subTest(field=field), self.assertRaises(RuntimeError):
                m.validate_cloud(h, c, cp, source)

    def test_cloud_child_timeout_kills_and_reaps_process_group(self):
        with tempfile.TemporaryDirectory() as d:
            pidfile = Path(d) / 'pid'
            code = 'import os,time;open(' + repr(str(pidfile)) + ',"w").write(str(os.getpid()));time.sleep(10)'
            started = time.monotonic()
            with self.assertRaisesRegex(RuntimeError, 'publisher_cloud_deadline'):
                m.proof_process({}, timeout=.3, executable=[sys.executable, '-c', code])
            self.assertLess(time.monotonic() - started, 3)
            pid = int(pidfile.read_text())
            with self.assertRaises(ProcessLookupError):
                os.kill(pid, 0)
            with self.assertRaises(ChildProcessError):
                os.waitpid(pid, os.WNOHANG)

    def test_cloud_output_and_exception_text_are_bounded(self):
        with self.assertRaisesRegex(RuntimeError, 'publisher_cloud_child_failed'):
            m.proof_process({}, timeout=3, executable=[sys.executable, '-c', 'import os;os.write(1,b"x"*100000)'])
        p = subprocess.run([sys.executable, str(Path(m.__file__)), '_cloud'], input=b'{}',
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=3)
        self.assertEqual(p.returncode, 1)
        self.assertEqual(json.loads(p.stdout), {'state': 'FAILED', 'error_type': 'KeyError'})
        self.assertEqual(p.stderr, b'')

    def test_build_requires_frozen_full_provenance_and_passed_regressions(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d).resolve()
            release = root / 'releases' / 'publisher-test'
            release.mkdir(parents=True)
            names = ['hivezilla-stream', 'SOURCE.json', 'BUILD-PROVENANCE.json', 'PATCH-MANIFEST.json',
                     'publisher-status.patch', 'GRAPH.json', 'SOURCE-BEFORE.json']
            entries = {}
            for name in names:
                body = json.dumps({'fixture': name}).encode() if name.endswith('.json') else ('fixture ' + name).encode()
                (release / name).write_bytes(body)
                (release / name).chmod(0o444)
                entries[name] = {'sha256': m.sha(body), 'bytes': len(body)}
            manifest = {'schema': 'blockzilla-publisher-status-release-v1', 'files': entries}
            (release / 'MANIFEST.json').write_text(json.dumps(manifest))
            (release / 'MANIFEST.json').chmod(0o444)
            value = {'schema': 'blockzilla-publisher-status-build-v1', 'state': 'READY_NOT_ACTIVE',
                     'tests_passed': True, 'tests_count': 44, 'tests_failed': 0,
                     'required_regression_tests': m.REQUIRED_TESTS,
                     'binary': str(release / 'hivezilla-stream'), 'binary_sha256': entries['hivezilla-stream']['sha256'],
                     'release': str(release), 'release_manifest_sha256': m.digest(release / 'MANIFEST.json'),
                     'source_manifest_sha256': entries['SOURCE.json']['sha256'],
                     'patch_sha256': entries['publisher-status.patch']['sha256'],
                     'donor_manifest_sha256': entries['SOURCE-BEFORE.json']['sha256']}
            build = root / 'BUILD.json'
            build.write_text(json.dumps(value))
            release.chmod(0o555)
            try:
                with patch.multiple(m, ROOT=root, PATCH_SHA=entries['publisher-status.patch']['sha256'],
                                    PATCH_MANIFEST_SHA=entries['PATCH-MANIFEST.json']['sha256'],
                                    DONOR_SHA=entries['SOURCE-BEFORE.json']['sha256']), \
                     patch.object(m, 'validate_graph'):
                    self.assertEqual(m.release_check(build, m.digest(build)), value)
                    with self.assertRaisesRegex(RuntimeError, 'build_hash_changed'):
                        m.release_check(build, '0' * 64)
                    for key, bad in [('tests_count', 43), ('tests_failed', 1), ('tests_passed', False),
                                     ('required_regression_tests', [])]:
                        changed = copy.deepcopy(value)
                        changed[key] = bad
                        build.write_text(json.dumps(changed))
                        with self.subTest(field=key), self.assertRaises(RuntimeError):
                            m.release_check(build, m.digest(build))
                    build.write_text(json.dumps(value))
                    (release / 'publisher-status.patch').chmod(0o644)
                    (release / 'publisher-status.patch').write_text('unreviewed bytes')
                    with self.assertRaisesRegex(RuntimeError, 'release_file_changed'):
                        m.release_check(build, m.digest(build))
            finally:
                release.chmod(0o755)

    def test_source_graph_rejects_unrelated_chain_or_dependency_changes(self):
        paths = [m.PUBLISHER_PATH, 'hivezilla/stream-relay/src/chain.rs']
        paths += ['hivezilla/stream-relay/src/fixture' + str(i) + '.rs' for i in range(27)]
        paths += ['Cargo.toml', 'hivezilla/stream-relay/Cargo.toml', 'hivezilla/stream-relay/Cargo.lock',
                  'hivezilla/object-store/Cargo.toml', 'crates/blockzilla-bank-stream-v2/Cargo.toml']
        old = {p: 'a' * 64 for p in paths}
        old.update({'evidence/' + str(i): 'b' * 64 for i in range(929)})
        old[m.PUBLISHER_PATH] = m.SOURCE_CHANGE['before_sha256']
        new = {**old, m.PUBLISHER_PATH: m.SOURCE_CHANGE['after_sha256']}
        source = {'schema': 'blockzilla-publisher-status-source-v1', 'files': new, 'file_count': 963,
                  'donor_manifest_sha256': m.DONOR_SHA, 'patch_sha256': m.PATCH_SHA,
                  'patch_manifest_sha256': m.PATCH_MANIFEST_SHA, 'changed_files': {m.PUBLISHER_PATH: m.SOURCE_CHANGE}}
        before = {'files': old}
        graph = {'file_count': 34, 'changed_files': [m.PUBLISHER_PATH],
                 'files': {p: {'before_sha256': old[p], 'after_sha256': new[p]} for p in paths}}
        m.validate_graph(source, before, graph)
        for kind in ('chain', 'graph', 'scope', 'patch'):
            s, g = copy.deepcopy(source), copy.deepcopy(graph)
            if kind == 'chain':
                s['files']['hivezilla/stream-relay/src/chain.rs'] = 'c' * 64
            elif kind == 'graph':
                g['files']['Cargo.toml']['after_sha256'] = 'c' * 64
            elif kind == 'scope':
                s['changed_files']['hivezilla/stream-relay/src/chain.rs'] = m.SOURCE_CHANGE
            else:
                s['patch_sha256'] = 'd' * 64
            with self.subTest(kind=kind), self.assertRaises(RuntimeError):
                m.validate_graph(s, before, g)

    def fixture(self, directory):
        report = Path(directory)
        body = b'[Service]\nExecStart=\nExecStart=/old/bin publish --state /state --credentials-file /credentials\nReadWritePaths=/state\n'
        full = b'# /old/unit\n' + body
        old = m.command(body)
        new = ['/new/bin', *old[1:]]
        proposed = m.replacement(body, old, new[0])
        source = {'session_id': 'session-1'}
        cp = {'schema': 'cp', 'source': '/source', 'namespace': 'n', 'prefix': 'p', 'session': 'session-1',
              'publisher_id': 'publisher', 'start_slot': 1, 'sequence': 100, 'blocks': 50, 'bytes': 123, 'cursor': 'cursor'}
        build = {'binary': new[0], 'binary_sha256': 'newhash', 'release': '/new'}
        before = {'state': 'PREPARED', 'build': '/build', 'build_sha256': 'hash', 'source': source,
                  'old_override_sha256': m.sha(body), 'old_unit_sha256': m.sha(full),
                  'proposed_override_sha256': m.sha(proposed), 'old_command': old, 'new_command': new,
                  'checkpoint_identity': cp}
        for name, value in [('publisher-before-override.conf', body), ('publisher-before-unit.txt', full),
                            ('publisher-proposed-override.conf', proposed), ('BEFORE.json', before)]:
            m.save(report / name, value, create=True)
        return report, body, full, source, cp, build

    def run_apply(self, directory, fail_install=False, changed_authority=False):
        report, body, full, source, cp, build = self.fixture(directory)
        args = type('Args', (), {'build': Path('/build'), 'build_sha256': 'hash'})()
        proof = {'pid': 12345, 'checkpoint': m.safe_checkpoint(cp), 'cloud': {'state': 'VERIFIED'}}
        authority = [source, source, RuntimeError('source_pin_changed')] if changed_authority else None
        with patch.multiple(m, REPORT=report, OLD_OVERRIDE_SHA=m.sha(body), OLD_UNIT_SHA=m.sha(full)), \
             patch.object(m, 'release_check', return_value=build), \
             patch.object(m, 'authority', side_effect=authority, return_value=source), \
             patch.object(m, 'unit_text', return_value=full), \
             patch.object(m, 'checked_process', return_value=m.OLD_PID), \
             patch.object(m, 'checkpoint', return_value=cp), \
             patch.object(m, 'proof_process', return_value={'state': 'VERIFIED'}), \
             patch.object(m, 'fresh_flow', return_value=proof), \
             patch.object(m, 'run', side_effect=lambda *a, **kw: body if a[:2] == ('sudo', 'cat') else b''), \
             patch.object(m, 'install', side_effect=[RuntimeError('first_install_partial'), None] if fail_install else None) as install:
            # Mock the loaded command only after preflight exact old unit checks.
            updated = full.replace(body, m.replacement(body, m.command(body), '/new/bin'))
            calls = [full] * 20 if fail_install else [full] * 3 + [updated] * 10
            with patch.object(m, 'unit_text', side_effect=calls):
                if fail_install:
                    with self.assertRaisesRegex(RuntimeError, 'publisher_activation_failed'):
                        m.apply(args)
                else:
                    m.apply(args)
            return json.loads((report / 'INSTALLATION.json').read_bytes()), install.call_count

    def test_partial_first_install_failure_is_rolled_back(self):
        with tempfile.TemporaryDirectory() as d:
            result, calls = self.run_apply(d, fail_install=True)
            self.assertEqual(calls, 2)
            self.assertEqual(result['state'], 'ROLLED_BACK')
            self.assertEqual(result['rollback_pid'], 12345)

    def test_source_change_blocks_configuration_restore(self):
        with tempfile.TemporaryDirectory() as d:
            result, calls = self.run_apply(d, fail_install=True, changed_authority=True)
            self.assertEqual(calls, 1)
            self.assertEqual(result['state'], 'ROLLBACK_FAILED')

    def test_apply_changes_only_publisher_and_saves_counter_proof(self):
        with tempfile.TemporaryDirectory() as d:
            result, calls = self.run_apply(d)
            self.assertEqual(calls, 1)
            self.assertEqual(result['state'], 'ACTIVE')
            self.assertEqual(result['before_checkpoint']['sequence'], 100)
            self.assertEqual(result['protected_pids'], m.PIDS)
            self.assertFalse(result['validator_restarted'])

    def test_old_process_progress_and_changed_new_pid_cannot_satisfy_flow(self):
        with tempfile.TemporaryDirectory() as d:
            _, _, _, source, cp, _ = self.fixture(d)
        first = {**cp, 'sequence': 101}
        second = {**cp, 'sequence': 102}
        fake = type('Verifier', (), {'CheckFailure': type('CheckFailure', (Exception,), {})})()
        for pids, checkpoints in [([123, 123], [first, first]), ([123, 456], [first, second])]:
            with self.subTest(pids=pids), patch.object(m, 'verifier', return_value=fake), \
                 patch.object(m, 'authority'), patch.object(m, 'checked_process', side_effect=pids), \
                 patch.object(m, 'checkpoint', side_effect=checkpoints), \
                 patch.object(m.time, 'monotonic', side_effect=[0, .5, 2]), patch.object(m.time, 'sleep'), \
                 patch.object(m, 'proof_process') as remote:
                with self.assertRaisesRegex(RuntimeError, 'publisher_flow_deadline'):
                    m.fresh_flow(['/new'], 'hash', cp, 100, 1, source, 1)
                remote.assert_not_called()

    def test_flow_proves_advance_after_the_verified_new_process_floor(self):
        with tempfile.TemporaryDirectory() as d:
            _, _, _, source, cp, _ = self.fixture(d)
        first = {**cp, 'sequence': 101}
        later = {**cp, 'sequence': 102}
        fake = type('Verifier', (), {'CheckFailure': type('CheckFailure', (Exception,), {}),
                                    'publisher_sample': lambda *a: None})()
        proof = {'state': 'VERIFIED', 'namespace': cp['namespace'], 'session': source['session_id'],
                 'sequence': 102, 'cursor': cp['cursor'], 'source_verified': True, 'full_gets': 2}
        status = {'state': 'RUNNING', 'updated_unix_ms': 5000, 'checkpoint': later}
        args = ['/new', '--state', '/state', '--credentials-file', '/credentials']
        with patch.object(m, 'verifier', return_value=fake), patch.object(m, 'authority'), \
             patch.object(m, 'checked_process', return_value=123), \
             patch.object(m, 'checkpoint', side_effect=[first, later, later]), \
             patch.object(m, 'obj', return_value=status), patch.object(m, 'proof_process', return_value=proof), \
             patch.object(m, 'now', return_value=5000), patch.object(m.time, 'sleep'):
            result = m.fresh_flow(args, 'hash', cp, 100, 4990, source, time.monotonic() + .5)
        self.assertEqual(result['post_restart_baseline']['sequence'], 101)
        self.assertEqual(result['checkpoint']['sequence'], 102)
        self.assertEqual(result['pid'], 123)


if __name__ == '__main__':
    unittest.main()
