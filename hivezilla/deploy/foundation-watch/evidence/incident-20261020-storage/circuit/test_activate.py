import copy
import hashlib
import importlib.util
import json
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location('storage_circuit_activate', Path(__file__).with_name('activate.py'))
m = importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(m)


def registry_fixture():
    source = {'session_id': 'session-1', 'genesis_hash': 'genesis', 'node_identity': 'node', 'agave_revision': 'revision'}
    rows = []
    for unit in [m.LIVE, *m.FINITE]:
        helper = str(m.OLD_RELEASE / 'cold_storage.py') if unit == m.LIVE else str(m.ROOT / 'releases/finite/cold_storage.py')
        args = ['/usr/bin/python3', helper, 'archive', '--output', '/source', '--keep-seconds', '7200']
        row = dict(unit=unit, source=source, output='/source', max_archived_banks=m.FINITE.get(unit),
                   namespace='a' * 64, helper=helper,
                   helper_sha256=m.OLD_COLD_SHA if unit == m.LIVE else m.FINITE_SHA,
                   expected_command=args, expected_command_sha256=m.command_sha(args),
                   allow_restart=True, max_retries=3, retry_delay_seconds=60)
        rows.append(row)
    return (json.dumps(dict(schema='blockzilla-storage-jobs-registry-v1', root=str(m.ROOT), jobs=rows),
                       indent=3, sort_keys=False) + '\n').encode()


class ActivationTests(unittest.TestCase):
    def test_registry_changes_only_four_live_fields_and_keeps_finite_bytes(self):
        raw = registry_fixture()
        old, new, proposed = m.transition(raw, str(m.ROOT / 'releases/new/cold_storage.py'), 'b' * 64)
        a, b = m.row_bytes(raw), m.row_bytes(proposed)
        self.assertEqual({k: a[k] for k in m.FINITE}, {k: b[k] for k in m.FINITE})
        self.assertEqual([r for r in old['jobs'] if r['unit'] != m.LIVE], [r for r in new['jobs'] if r['unit'] != m.LIVE])
        before = next(r for r in old['jobs'] if r['unit'] == m.LIVE)
        after = next(r for r in new['jobs'] if r['unit'] == m.LIVE)
        self.assertEqual({k for k in before if before[k] != after[k]},
                         {'helper', 'helper_sha256', 'expected_command', 'expected_command_sha256'})
        self.assertEqual(before['expected_command'][2:], after['expected_command'][2:])
        self.assertEqual(m.strict_json(proposed), new)

    def test_dropin_keeps_all_other_bytes_and_rejects_repeated_helper(self):
        args = ['/usr/bin/python3', str(m.OLD_RELEASE / 'cold_storage.py'), 'archive', '--output', '/source']
        body = ('[Service]\nExecStart=\nExecStart=' + ' '.join(json.dumps(a) for a in args)
                + '\nReadWritePaths=/source\n').encode()
        target = str(m.ROOT / 'releases/new/cold_storage.py')
        changed = m.replace_helper(body, args, target)
        self.assertEqual(changed, body.replace(json.dumps(args[1]).encode(), json.dumps(target).encode()))
        self.assertEqual(m.command(changed), [args[0], target, *args[2:]])
        with self.assertRaisesRegex(RuntimeError, 'helper_token_not_unique'):
            m.replace_helper(body + ('# ' + json.dumps(args[1]) + '\n').encode(), args, target)

    def ledger_fixture(self, directory, state=None, marker=True):
        output = Path(directory).resolve(); cold = output / 'cold-storage'; cold.mkdir()
        row = dict(output=str(output), source={'session_id': 'session'}, namespace='n', max_archived_banks=None)
        helper = output / 'cold_storage.py'
        actual = Path(__file__).parents[4] / 'direct-agave/cold_storage.py'
        build = Path(__file__).with_name('BUILD.json')
        if build.is_file():
            value = json.loads(build.read_bytes())
            candidate = Path(value['release']) / 'cold_storage.py'
            if candidate.is_file():
                self.assertEqual(m.digest(candidate), value['cold_sha256'])
                actual = candidate
        helper.write_bytes(actual.read_bytes())
        row.update(helper=str(helper), helper_sha256=m.digest(helper))
        binding = {k: row[k] for k in ('source', 'namespace', 'max_archived_banks')}
        db = sqlite3.connect(cold / 'archive.sqlite')
        db.execute('CREATE TABLE meta(id INTEGER PRIMARY KEY, value TEXT)')
        if state is not None:
            value = dict(schema=m.LEDGER_SCHEMA, binding=binding, state=state, burst_retries_used=0,
                         outage_since_unix_ms=None, next_attempt_unix_ms=None, reservation=None, last_error=None,
                         last_failure_unix_ms=None, healthy_since_unix_ms=None, healthy_start_archived_banks=None)
            if state != 'NORMAL':
                value['last_error'] = dict(error_type='BundleTransferTimeout', http_status=None, network_error_type=None)
            if state in ('BACKOFF', 'OPEN', 'RECOVERING'):
                value.update(burst_retries_used=3 if state == 'OPEN' else 1, outage_since_unix_ms=100,
                             last_failure_unix_ms=200, next_attempt_unix_ms=1000000)
            if state == 'RECOVERING':
                value.update(healthy_since_unix_ms=300, healthy_start_archived_banks=100)
            db.execute('INSERT INTO meta VALUES(3,?)', (json.dumps(value),))
        if marker:
            db.execute('INSERT INTO meta VALUES(4,?)', (json.dumps(dict(schema=m.LEDGER_MARKER_SCHEMA, binding=binding)),))
        db.commit(); db.close()
        return row, cold / 'archive.sqlite'

    def test_rollback_ledger_reads_never_change_or_rearm_state(self):
        for state in ('NORMAL', 'BACKOFF', 'OPEN', 'RECOVERING', 'BLOCKED'):
            with self.subTest(state=state), tempfile.TemporaryDirectory() as directory:
                row, path = self.ledger_fixture(directory, state)
                before = path.read_bytes()
                with patch.object(m, 'LEDGER_MARKER_SCHEMA', 'blockzilla-native-cold-recovery-initialized-v1'):
                    if state != 'NORMAL':
                        with self.assertRaisesRegex(RuntimeError, 'recovery_budget_blocks_old_helper'):
                            m.rollback_allowed(row, 100)
                    else:
                        self.assertEqual(m.rollback_allowed(row, 100)['state'], state)
                self.assertEqual(path.read_bytes(), before)

    def test_missing_or_malformed_ledger_never_allows_old_helper(self):
        for kind in ('only3', 'only4', 'json', 'binding', 'state'):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory:
                row, path = self.ledger_fixture(directory, None if kind == 'only4' else 'NORMAL', marker=kind != 'only3')
                db = sqlite3.connect(path)
                if kind == 'json':
                    db.execute('UPDATE meta SET value=? WHERE id=3', ('{invalid',))
                elif kind in ('binding', 'state'):
                    value = json.loads(db.execute('SELECT value FROM meta WHERE id=3').fetchone()[0])
                    if kind == 'binding': value['binding']['namespace'] = 'foreign'
                    else: value['state'] = 'UNKNOWN'
                    db.execute('UPDATE meta SET value=? WHERE id=3', (json.dumps(value),))
                db.commit(); db.close()
                with self.assertRaises((RuntimeError, ValueError)):
                    m.rollback_allowed(row, 100)

    def test_both_absent_allows_only_no_permanent_new_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            row, path = self.ledger_fixture(directory, marker=False)
            with patch.object(m, 'LEDGER_MARKER_SCHEMA', 'blockzilla-native-cold-recovery-initialized-v1'):
                self.assertEqual(m.rollback_allowed(row, 100)['state'], 'NOT_INITIALIZED')
                failure = path.parent / 'LAST-FAILURE.json'
                failure.write_text(json.dumps(dict(updated_unix_ms=200, state='FAILED', source=row['source'],
                                                  error_type='ClientError', http_status=403)))
                with self.assertRaisesRegex(RuntimeError, 'permanent_failure'):
                    m.rollback_allowed(row, 100)
                failure.write_text(json.dumps(dict(updated_unix_ms=200, state='FAILED', source=row['source'],
                                                  error_type='BundleTransferTimeout')))
                self.assertEqual(m.rollback_allowed(row, 100)['state'], 'NOT_INITIALIZED')

    def test_normal_durable_state_does_not_hide_permanent_or_uncertain_fresh_failure(self):
        cases = [('RuntimeError', None, None, True), ('ClientError', 403, None, True),
                 ('RecoveryBlocked', None, None, True), ('BundleTransferTimeout', None, None, False),
                 ('ResponseStreamingError', None, None, True)]
        for kind, status, network, persisted in cases:
            with self.subTest(kind=kind, persisted=persisted), tempfile.TemporaryDirectory() as directory:
                row, path = self.ledger_fixture(directory, 'NORMAL')
                before = path.read_bytes()
                failure = dict(updated_unix_ms=200, source=row['source'], state='FAILED', error_type=kind,
                               http_status=status, network_error_type=network, recovery_state_persisted=persisted)
                (path.parent / 'LAST-FAILURE.json').write_text(json.dumps(failure))
                with self.assertRaises(RuntimeError):
                    m.rollback_allowed(row, 100)
                self.assertEqual(path.read_bytes(), before)

    def progress_fixture(self):
        row = json.loads(registry_fixture())['jobs'][0]
        plan = {'source': row['source'], 'guardian_command': ['python', '-B', 'guardian']}
        live = {'MainPID': '123', 'InvocationID': 'generation1'}
        guardian = {'MainPID': '234', 'InvocationID': 'guardian1'}
        baseline = dict(archived_banks=101, retired_banks=101, max_archive_rowid=101,
                        last_archive_progress_unix_ms=900, metadata={'source': row['source'], 'namespace': row['namespace']})
        return row, plan, live, guardian, baseline

    def test_old_progress_before_new_pid_floor_does_not_count(self):
        row, plan, live, guardian, baseline = self.progress_fixture()
        status = dict(state='RUNNING', updated_unix_ms=1000)
        jobs = dict(state='HEALTHY', issues=[], registry_sha256=m.sha(b'registry'), active_source=plan['source'], updated_unix_ms=1000)
        with patch.object(m, 'archive', return_value=baseline), patch.object(m, 'authority'), \
             patch.object(m, 'check_process', side_effect=[live, guardian, live, guardian]), patch.object(m, 'obj', side_effect=[status, jobs]), \
             patch.object(m, 'now', return_value=1000), patch.object(m.time, 'monotonic', side_effect=[0, .5, 2]), \
             patch.object(m.time, 'sleep'), self.assertRaisesRegex(RuntimeError, 'new_process_progress_deadline'):
            m.progress(plan, row, live, guardian, b'registry', 990, seconds=1)

    def test_new_same_pid_progress_and_guardian_hash_are_required(self):
        row, plan, live, guardian, baseline = self.progress_fixture()
        after = {**baseline, 'archived_banks': 102, 'retired_banks': 102, 'max_archive_rowid': 102,
                 'last_archive_progress_unix_ms': 1001}
        status = dict(state='RUNNING', updated_unix_ms=1000)
        jobs = dict(state='HEALTHY', issues=[], registry_sha256=m.sha(b'registry'), active_source=plan['source'], updated_unix_ms=1000)
        with patch.object(m, 'archive', side_effect=[baseline, after]), patch.object(m, 'authority'), \
             patch.object(m, 'check_process', side_effect=[live, guardian, live, guardian]), patch.object(m, 'obj', side_effect=[status, jobs]), \
             patch.object(m, 'now', return_value=1000):
            result = m.progress(plan, row, live, guardian, b'registry', 990, seconds=1)
        self.assertEqual(result['post_restart_baseline']['archived_banks'], 101)
        self.assertEqual(result['post_progress']['archived_banks'], 102)

    def test_changed_live_invocation_cannot_reuse_prior_progress(self):
        row, plan, live, guardian, baseline = self.progress_fixture()
        with patch.object(m, 'archive', return_value=baseline), patch.object(m, 'authority'), \
             patch.object(m, 'check_process', return_value={**live, 'InvocationID': 'replacement'}), \
             self.assertRaisesRegex(RuntimeError, 'live_generation_changed'):
            m.progress(plan, row, live, guardian, b'registry', 990, seconds=1)

    def apply_fixture(self, directory):
        report = Path(directory).resolve()
        oldraw = registry_fixture(); registry_sha = m.sha(oldraw)
        release = str(m.ROOT / 'releases/new')
        old, new, newraw = m.transition(oldraw, release + '/cold_storage.py', 'b' * 64)
        oldrow, newrow = old['jobs'][0], new['jobs'][0]
        guardian = ['/usr/bin/python3', '-B', str(m.OLD_RELEASE / 'storage_jobs.py')]
        body = ('[Service]\nExecStart=\nExecStart=' + ' '.join(json.dumps(a) for a in oldrow['expected_command'])
                + '\nReadWritePaths=/source\n').encode()
        after = m.replace_helper(body, oldrow['expected_command'], newrow['helper'])
        full = b'# source-unit\n' + body
        files = {'registry-before.json': oldraw, 'registry-after.json': newraw, 'live-before.conf': body,
                 'live-after.conf': after, 'live-before.unit': full, 'live-after.unit': full.replace(body, after),
                 'guardian-before.unit': ('ExecStart=' + ' '.join(json.dumps(a) for a in guardian) + '\n').encode()}
        for name, value in files.items(): m.save(report / name, value, create=True)
        live = {'MainPID': '100', 'InvocationID': 'a' * 32}
        guard = {'MainPID': '200', 'InvocationID': 'b' * 32}
        plan = dict(state='PREPARED', build='/build', build_sha256='c' * 64, manifest_sha256='d' * 64,
                    release=release, cold_sha256='b' * 64, registry_before_sha256=registry_sha,
                    registry_after_sha256=m.sha(newraw), source=oldrow['source'], protected_pids=m.PIDS,
                    old_live_row=oldrow, new_live_row=newrow, old_live_command=oldrow['expected_command'],
                    live_command=newrow['expected_command'], guardian_command=guardian,
                    old_live_state=live, old_guardian_state=guard, files={n: m.sha(v) for n, v in files.items()})
        m.save(report / 'PLAN.json', plan, create=True)
        args = type('Options', (), dict(build=Path('/build'), build_sha256='c' * 64, manifest_sha256='d' * 64,
                                       plan_sha256=m.digest(report / 'PLAN.json')))()
        return report, registry_sha, plan, args

    def test_apply_orders_supervisor_and_live_stops_before_owned_config_change(self):
        with tempfile.TemporaryDirectory() as directory:
            report, registry_sha, plan, args = self.apply_fixture(directory)
            live = {'MainPID': '300', 'InvocationID': 'c' * 32}
            guardian = {'MainPID': '400', 'InvocationID': 'd' * 32}
            events = []
            with patch.multiple(m, REPORT=report, OLD_REGISTRY_SHA=registry_sha), \
                 patch.object(m, 'release_check', return_value={'release': plan['release'], 'cold_sha256': plan['cold_sha256']}), \
                 patch.object(m, 'authority', return_value=plan['source']), patch.object(m, 'finite_check'), \
                 patch.object(m, 'unchanged_units'), patch.object(m, 'check_process',
                     side_effect=lambda unit, *a: plan['old_live_state'] if unit == m.LIVE else plan['old_guardian_state']), \
                 patch.object(m, 'show', return_value={'MainPID': '0'}), \
                 patch.object(m, 'archive', return_value={'max_archive_rowid': 100}), \
                 patch.object(m, 'run', side_effect=lambda *a, **kw: events.append(('run', a)) or b''), \
                 patch.object(m, 'install_unit', side_effect=lambda p: events.append(('install', p.name))), \
                 patch.object(m, 'registry_install', side_effect=lambda p: events.append(('registry', m.sha(p)))), \
                 patch.object(m, 'wait_running', side_effect=[live, guardian]), \
                 patch.object(m, 'progress', return_value={'post_restart_baseline': {'max_archive_rowid': 101},
                                                         'post_progress': {'max_archive_rowid': 102}}), \
                 patch.object(m, 'argv', side_effect=lambda pid: plan['live_command'] if pid == '300' else plan['guardian_command']):
                m.apply(args)
            self.assertEqual(events[:2], [('run', ('sudo', 'systemctl', 'stop', m.GUARDIAN)),
                                         ('run', ('sudo', 'systemctl', 'stop', m.LIVE))])
            self.assertEqual(events[2][0], 'install'); self.assertEqual(events[3][0], 'registry')
            starts = [e[1][-1] for e in events if e[0] == 'run' and e[1][2] == 'start']
            self.assertEqual(starts, [m.LIVE, m.GUARDIAN])
            value = m.obj(report / 'INSTALLATION.json')
            self.assertEqual(value['state'], 'ACTIVE_PENDING_CLOUD_PROOF')
            self.assertTrue(value['recovery_ledger_preserved'])
            self.assertEqual(value['protected_pids'], m.PIDS)
            self.assertFalse(value['validator_restarted'])

    def test_partial_first_stop_failure_still_invokes_rollback(self):
        with tempfile.TemporaryDirectory() as directory:
            report, registry_sha, plan, args = self.apply_fixture(directory)
            def rollback(*a):
                value = {'state': 'PREVIOUS_HELPER_RESTORED'}
                m.save(report / 'ROLLBACK.json', value, create=True)
                return value
            with patch.multiple(m, REPORT=report, OLD_REGISTRY_SHA=registry_sha), \
                 patch.object(m, 'release_check', return_value={'release': plan['release'], 'cold_sha256': plan['cold_sha256']}), \
                 patch.object(m, 'authority', return_value=plan['source']), patch.object(m, 'finite_check'), \
                 patch.object(m, 'unchanged_units'), patch.object(m, 'check_process',
                     side_effect=lambda unit, *a: plan['old_live_state'] if unit == m.LIVE else plan['old_guardian_state']), \
                 patch.object(m, 'run', side_effect=RuntimeError('stop_partially_failed')), \
                 patch.object(m, 'rollback', side_effect=rollback) as restore, patch.object(m, 'install_unit') as install:
                with self.assertRaisesRegex(RuntimeError, 'storage_circuit_activation_failed'):
                    m.apply(args)
            restore.assert_called_once(); install.assert_not_called()
            self.assertEqual(m.obj(report / 'INSTALLATION.json')['rollback_state'], 'PREVIOUS_HELPER_RESTORED')

    def test_wrong_prepare_hash_blocks_before_any_service_call(self):
        with tempfile.TemporaryDirectory() as directory:
            report, registry_sha, plan, args = self.apply_fixture(directory)
            args.plan_sha256 = '0' * 64
            with patch.multiple(m, REPORT=report, OLD_REGISTRY_SHA=registry_sha), \
                 patch.object(m, 'release_check', return_value={'release': plan['release']}), \
                 patch.object(m, 'run') as run:
                with self.assertRaisesRegex(RuntimeError, 'prepared_plan_hash_changed'):
                    m.apply(args)
            run.assert_not_called()

    def test_permanent_circuit_blocks_old_restart_and_keeps_database_and_config(self):
        with tempfile.TemporaryDirectory() as directory:
            report, registry_sha, plan, args = self.apply_fixture(directory)
            ledger_dir = report / 'source'; ledger_dir.mkdir()
            row, database = self.ledger_fixture(ledger_dir, 'BLOCKED')
            plan['new_live_row'] = {**plan['new_live_row'], **row}
            registry = report / 'current-registry.json'
            registry.write_bytes((report / 'registry-after.json').read_bytes())
            before_db, before_registry = database.read_bytes(), registry.read_bytes()
            body = (report / 'live-after.conf').read_bytes()
            def text(unit):
                return (report / ('guardian-before.unit' if unit == m.GUARDIAN else 'live-after.unit')).read_bytes()
            events = []
            with patch.multiple(m, REPORT=report, REGISTRY=registry), patch.object(m, 'authority'), \
                 patch.object(m, 'unit_text', side_effect=text), patch.object(m, 'show', return_value={'MainPID': '0'}), \
                 patch.object(m, 'run', side_effect=lambda *a, **kw: body if a[:2] == ('sudo', 'cat') else events.append(a) or b''), \
                 patch.object(m, 'install_unit') as install, patch.object(m, 'registry_install') as write_registry, \
                 patch.object(m, 'wait_running') as start:
                value = m.rollback(plan, {'started_unix_ms': 100})
            self.assertEqual(value['state'], 'BLOCKED_ROLLBACK')
            self.assertEqual(value['reason_code'], 'recovery_budget_blocks_old_helper')
            self.assertFalse(value['old_helper_started'])
            install.assert_not_called(); write_registry.assert_not_called(); start.assert_not_called()
            self.assertFalse(any('start' in event for event in events))
            self.assertEqual(database.read_bytes(), before_db)
            self.assertEqual(registry.read_bytes(), before_registry)

    def test_progress_rechecks_generation_after_database_read(self):
        row, plan, live, guardian, baseline = self.progress_fixture()
        after = {**baseline, 'archived_banks': 102, 'retired_banks': 102, 'max_archive_rowid': 102,
                 'last_archive_progress_unix_ms': 1001}
        status = dict(state='RUNNING', updated_unix_ms=1000)
        jobs = dict(state='HEALTHY', issues=[], registry_sha256=m.sha(b'registry'), active_source=plan['source'], updated_unix_ms=1000)
        with patch.object(m, 'archive', side_effect=[baseline, after]), patch.object(m, 'authority'), \
             patch.object(m, 'check_process', side_effect=[live, guardian, {**live, 'InvocationID': 'replacement'}, guardian]), \
             patch.object(m, 'obj', side_effect=[status, jobs]), patch.object(m, 'now', return_value=1000), \
             self.assertRaisesRegex(RuntimeError, 'runtime_generation_changed_after_read'):
            m.progress(plan, row, live, guardian, b'registry', 990, seconds=1)


if __name__ == '__main__':
    unittest.main()
