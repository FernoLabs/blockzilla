import copy
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

SPEC = importlib.util.spec_from_file_location('circuit_postverify', Path(__file__).with_name('verify.py'))
m = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(m)
PRODUCTION_ROOT, PRODUCTION_OUTPUT = m.ROOT, m.OUTPUT


class Tests(unittest.TestCase):
    def options(self):
        return SimpleNamespace(installation_sha256='1' * 64, build_sha256='2' * 64,
            manifest_sha256='3' * 64, cold_sha256=m.COLD_SHA, live_pid=123, guardian_pid=456,
            build=Path('/frozen/BUILD.json'))

    def registry(self):
        helper = str(m.OLD_RELEASE / 'cold_storage.py')
        args = ['/usr/bin/python3', helper, 'archive', '--output', str(m.OUTPUT), '--follow', '--retire']
        live = dict(unit=m.LIVE, source=m.SOURCE, namespace=m.NAMESPACE, output=str(m.OUTPUT),
                    max_archived_banks=None, helper=helper, helper_sha256=m.OLD_COLD_SHA,
                    expected_command=args, expected_command_sha256=m.command_sha(args),
                    allow_restart=True, max_retries=3, retry_delay_seconds=60)
        return dict(schema='blockzilla-storage-jobs-registry-v1', root=str(m.ROOT),
                    jobs=[live, *[dict(unit=unit, max_archived_banks=cap, unchanged='old') for unit, cap in m.CAPS.items()]])

    def fixture(self, folder):
        with patch.multiple(m, ROOT=PRODUCTION_ROOT, OUTPUT=PRODUCTION_OUTPUT):
            common = m.support_module()
        root = Path(folder).resolve()
        self.enterContext(patch.multiple(m, ROOT=root, OUTPUT=root / 'output'))
        common.ROOT, common.OUTPUT = root, m.OUTPUT
        state = m.OUTPUT / 'cold-storage'
        state.mkdir(parents=True)
        db = sqlite3.connect(state / 'archive.sqlite')
        self.addCleanup(db.close)
        db.executescript('CREATE TABLE archives(slot INTEGER,bank INTEGER,record TEXT,retired INTEGER,PRIMARY KEY(slot,bank));'
                         'CREATE INDEX archives_retired ON archives(retired); CREATE TABLE meta(id INTEGER PRIMARY KEY,value TEXT);')
        db.execute('INSERT INTO meta VALUES(1,?)', (json.dumps(dict(source=m.SOURCE, namespace=m.NAMESPACE)),))
        db.execute('INSERT INTO meta VALUES(2,?)', ('400',))
        for bank, digest in enumerate(('a', 'a', 'b', 'c', 'c'), 1):
            digest *= 64
            record = dict(slot=100 + bank, bank=bank, source=m.SOURCE, namespace=m.NAMESPACE,
                key='native-custody/processed-v1/' + m.SOURCE['session_id'] + '/' + digest + '.tar',
                bundle_sha256=digest, artifact='artifact-' + str(bank), custody='custody-' + str(bank), files=[])
            db.execute('INSERT INTO archives VALUES(?,?,?,1)', (record['slot'], bank, json.dumps(record)))
            note = {key: record[key] for key in ('source', 'slot', 'bank', 'key', 'bundle_sha256', 'namespace', 'files')}
            note['schema'] = 'blockzilla-native-cold-copy-v1'
            for name in (record['artifact'], record['custody']):
                path = m.OUTPUT / name
                path.mkdir()
                (path / 'REMOTE-CUSTODY.json').write_text(json.dumps(note))
        db.commit()
        baseline = dict(archived_banks=2, retired_banks=2, max_archive_rowid=2, last_archive_progress_unix_ms=100)
        baseline['metadata'] = dict(source=m.SOURCE, namespace=m.NAMESPACE)
        context = dict(before=dict(live_before=baseline),
                       install=dict(completed_unix_ms=350, post_restart_baseline={**baseline, 'last_archive_progress_unix_ms': 200}))
        return common, db, context

    def test_external_frozen_arguments_are_required_before_any_read(self):
        args = self.options()
        m.trusted_arguments(args)
        for field, bad in [('installation_sha256', None), ('build_sha256', 'wrong'),
                           ('manifest_sha256', ''), ('cold_sha256', {}), ('live_pid', True), ('guardian_pid', 0)]:
            changed = copy.deepcopy(args)
            setattr(changed, field, bad)
            with self.subTest(field=field), patch.object(m, 'read') as read:
                with self.assertRaises(m.ProofError): m.prove(changed)
                read.assert_not_called()

    def test_missing_receipt_hash_never_becomes_unbound_authority(self):
        with patch.object(m, 'read') as read:
            for value in (None, '', {}, 'not-a-hash'):
                with self.assertRaises(m.ProofError): m.bound_document(Path('/not-read'), value)
            read.assert_not_called()

    def test_registry_changes_only_four_live_fields_and_preserves_finite_rows(self):
        old = self.registry()
        frozen = copy.deepcopy(old)
        new, prior, live = m.registry_transition(old, m.ROOT / 'releases/new-circuit', 'a' * 64)
        self.assertEqual(old, frozen)
        self.assertEqual(new['jobs'][1:], old['jobs'][1:])
        self.assertEqual({key for key in live if live[key] != prior[key]},
                         {'helper', 'helper_sha256', 'expected_command', 'expected_command_sha256'})
        self.assertEqual(live['expected_command'][2:], prior['expected_command'][2:])
        raw = json.dumps(old, indent=2).encode()
        transformed = m.registry_bytes_transition(raw, prior, live, new)
        self.assertEqual(json.loads(transformed), new)
        self.assertEqual(transformed, raw.replace(json.dumps(prior['helper']).encode(), json.dumps(live['helper']).encode())
            .replace(json.dumps(prior['helper_sha256']).encode(), json.dumps(live['helper_sha256']).encode())
            .replace(json.dumps(prior['expected_command_sha256']).encode(), json.dumps(live['expected_command_sha256']).encode()))
        for target, field, value in [(0, 'source', {}), (0, 'helper_sha256', 'b' * 64),
                                     (1, 'max_archived_banks', 999999)]:
            changed = copy.deepcopy(old)
            changed['jobs'][target][field] = value
            with self.assertRaises(m.ProofError): m.registry_transition(changed, m.ROOT / 'releases/new', 'a' * 64)

    def test_selects_complete_group_only_after_new_process_floor_and_active_receipt(self):
        with tempfile.TemporaryDirectory() as folder:
            common, db, context = self.fixture(folder)
            counts, selected = m.live_selection(context, common)
            self.assertEqual([row[0] for row in selected], [5, 4])
            self.assertEqual(counts, m.recheck_selection(selected, common))
            context['install']['post_restart_baseline']['max_archive_rowid'] = 5
            with self.assertRaises(m.ProofError): m.live_selection(context, common)

    def test_durable_progress_must_be_later_than_activation_completion(self):
        with tempfile.TemporaryDirectory() as folder:
            common, db, context = self.fixture(folder)
            context['install']['completed_unix_ms'] = 400
            with self.assertRaisesRegex(m.ProofError, 'new_live_durable_progress_missing'):
                m.live_selection(context, common)

    def test_key_scan_metrics_and_fixed_deadline_error_have_no_weaker_fallback(self):
        with tempfile.TemporaryDirectory() as folder:
            common, db, context = self.fixture(folder)
            scans = []
            _, selected = m.live_selection(context, common, scans)
            self.assertEqual(len(scans), 1)
            self.assertEqual((scans[0]['archive_rows_at_scan'], scans[0]['returned_rows'], scans[0]['selected_rows']), (5, 2, 2))
            self.assertEqual((scans[0]['deadline_seconds'], scans[0]['output_row_limit']), (15, 129))
            self.assertGreaterEqual(scans[0]['elapsed_seconds'], 0)
            query = common.query
            def fail(db, sql, args=(), seconds=15):
                if 'json_valid(record)' in sql: raise sqlite3.OperationalError('DO_NOT_PRINT')
                return query(db, sql, args, seconds)
            with patch.object(common, 'query', side_effect=fail), self.assertRaisesRegex(m.ProofError, '^bundle_index_scan_deadline_or_invalid$'):
                m.recheck_selection(selected, common)

    def test_successful_cloud_proof_cannot_hide_changed_activation_authority(self):
        args = self.options(); args.validator_file = Path(__file__).resolve().with_name('cloud-validator.py')
        context = {key: b'frozen' for key in ('pin', 'registry_raw', 'install_raw', 'before_raw', 'plan_raw', 'build_raw', 'manifest_raw')}
        context.update(anchors={'old': b'old'}, live_service=dict(MainPID='123', InvocationID='a' * 32),
                       guardian_service=dict(MainPID='456', InvocationID='b' * 32))
        final = copy.deepcopy(context); final['registry_raw'] = b'changed'
        counts = dict(archived_banks=5, retired_banks=5)
        common = SimpleNamespace(bounded_cloud=Mock(return_value={'state': 'PASSED'}))
        with patch.object(m, 'support_module', return_value=common), \
             patch.object(m, 'snapshot_context', side_effect=[context, final]), \
             patch.object(m, 'live_selection', return_value=(counts, [])), \
             patch.object(m, 'admit_cloud'), patch.object(m, 'recheck_selection', return_value=counts):
            with self.assertRaisesRegex(m.ProofError, 'authority_changed_during_proof'): m.prove(args)
        common.bounded_cloud.assert_called_once()

    def test_off_group_key_match_invalid_json_and_oversized_rows_fail_closed(self):
        for change in ('off_group', 'invalid_json', 'oversized', 'blob'):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as folder:
                common, db, context = self.fixture(folder)
                _, selected = m.live_selection(context, common)
                original = json.loads(db.execute('SELECT record FROM archives WHERE bank=1').fetchone()[0])
                original['key'], original['bundle_sha256'] = selected[0][1]['key'], selected[0][1]['bundle_sha256']
                value = json.dumps(original)
                if change == 'invalid_json': value = '{broken'
                elif change == 'oversized': value = json.dumps(dict(original, padding='x' * 65536))
                elif change == 'blob': value = b'not-text'
                db.execute('UPDATE archives SET record=? WHERE bank=1', (value,))
                db.commit()
                with self.assertRaises(m.ProofError): m.recheck_selection(selected, common)

    def test_partial_retirement_and_changed_custody_receipt_reject(self):
        with tempfile.TemporaryDirectory() as folder:
            common, db, context = self.fixture(folder)
            db.execute('UPDATE archives SET retired=0 WHERE bank=5'); db.commit()
            _, selected = m.live_selection(context, common)
            self.assertEqual([row[0] for row in selected], [3])
            (m.OUTPUT / selected[0][1]['custody'] / 'REMOTE-CUSTODY.json').write_text('{}')
            with self.assertRaises(m.ProofError): m.recheck_selection(selected, common)

    def test_cloud_admission_preserves_reserve_and_bounds_temporary_input(self):
        with patch.object(m.shutil, 'disk_usage', side_effect=[SimpleNamespace(free=m.RESERVE + m.MARGIN + m.MAX_INPUT),
                                                            SimpleNamespace(free=m.MARGIN + m.MAX_INPUT)]):
            m.admit_cloud()
        with patch.object(m.shutil, 'disk_usage', return_value=SimpleNamespace(free=m.RESERVE + m.MARGIN + m.MAX_INPUT - 1)):
            with self.assertRaises(m.ProofError): m.admit_cloud()

    def test_recovery_ledger_binding_and_ready_state_are_required(self):
        with tempfile.TemporaryDirectory() as folder:
            common, db, _ = self.fixture(folder)
            binding = dict(source=m.SOURCE, namespace=m.NAMESPACE, max_archived_banks=None)
            marker = dict(schema='blockzilla-native-cold-recovery-initialized-v1', binding=binding)
            ledger = dict(schema='blockzilla-native-cold-recovery-v1', binding=binding, state='NORMAL',
                          reservation=None, burst_retries_used=0, outage_since_unix_ms=None,
                          next_attempt_unix_ms=None, last_error=None, last_failure_unix_ms=None,
                          healthy_since_unix_ms=None, healthy_start_archived_banks=None)
            build_path = Path(__file__).resolve().parent.parent / 'BUILD.json'
            helper = None
            if build_path.is_file():
                release_helper = Path(json.loads(build_path.read_bytes())['release']) / 'cold_storage.py'
                if release_helper.is_file(): helper = release_helper
            if helper is None:
                deploy = next((path for path in Path(__file__).resolve().parents if path.name == 'deploy'), None)
                self.assertIsNotNone(deploy, 'No staged release or local deploy directory was found')
                helper = deploy / 'direct-agave/cold_storage.py'
            helper_sha = m.sha(helper.read_bytes())
            self.assertEqual(helper_sha, m.COLD_SHA)
            db.executemany('INSERT INTO meta VALUES(?,?)', [(3, json.dumps(ledger)), (4, json.dumps(marker))]); db.commit()
            self.assertEqual(m.recovery_state(common, helper, helper_sha)['state'], 'NORMAL')
            for change in ('BLOCKED', 'OPEN', 'BACKOFF', 'cap', 'normal_stale_error', 'recovering_zero_burst', 'missing_anchor'):
                changed = copy.deepcopy(ledger)
                if change == 'cap': changed['binding']['max_archived_banks'] = 785546
                elif change == 'normal_stale_error': changed['last_error'] = dict(error_type='ReadTimeoutError', http_status=None, network_error_type=None)
                elif change == 'recovering_zero_burst': changed['state'] = 'RECOVERING'
                elif change == 'missing_anchor': db.execute('DELETE FROM meta WHERE id=4')
                else: changed['state'] = change
                db.execute('UPDATE meta SET value=? WHERE id=3', (json.dumps(changed),)); db.commit()
                with self.assertRaises(m.ProofError): m.recovery_state(common, helper, helper_sha)

    def test_bounded_cloud_child_kills_and_reaps_on_total_deadline_or_output_limit(self):
        for mode in ('deadline', 'output'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as folder:
                common, db, context = self.fixture(folder)
                _, selected = m.live_selection(context, common)
                original, children = subprocess.Popen, []
                script = ('import time\nwhile True:\n print(" ",flush=True);time.sleep(.01)' if mode == 'deadline'
                          else 'import os,time;os.write(1,b"x"*70000);time.sleep(60)')
                def launch(command, **kwargs):
                    value = original([sys.executable, '-c', script], **kwargs)
                    children.append(value)
                    return value
                clock = SimpleNamespace(monotonic=lambda: time.monotonic() * 1000)
                started = time.monotonic()
                with patch.object(common.subprocess, 'Popen', side_effect=launch), patch.object(common, 'time', clock):
                    with self.assertRaises(common.ProofError): common.bounded_cloud(Path('/unused'), selected)
                self.assertLess(time.monotonic() - started, 3)
                self.assertEqual(len(children), 1)
                self.assertIsNotNone(children[0].returncode)
                self.assertTrue(children[0].stdout.closed)
                with self.assertRaises(ProcessLookupError): os.kill(children[0].pid, 0)
                with self.assertRaises(ChildProcessError): os.waitpid(children[0].pid, os.WNOHANG)

    def test_pinned_full_tar_rejects_member_object_missing_extra_and_receipt_corruption(self):
        common = m.support_module()
        for change in ('success', 'member', 'object_hash', 'extra', 'missing', 'receipt'):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as folder:
                root = Path(folder).resolve()
                common.ROOT, common.OUTPUT = root, root / 'output'
                validator = common.validator_module(Path(__file__).resolve().with_name('cloud-validator.py'))
                artifact = 'artifacts/' + 'long-pax-path-' * 10 + '/bank-1'
                custody = 'custody/bank-1'
                bodies = {artifact + '/payload.bin': b'execution bytes' * 100,
                          custody + '/evidence.bin': b'original native custody bytes'}
                files = [dict(path=name, bytes=len(body), sha256=hashlib.sha256(body).hexdigest(),
                              keep=False, stamp=[1, 2, len(body), 3, 4]) for name, body in bodies.items()]
                bank = dict(slot=100, bank=1, artifact=artifact, custody=custody, source=m.SOURCE,
                            through_sequence=500, files=files)
                manifest = json.dumps(dict(schema='blockzilla-native-custody-bundle-v1', banks=[bank])).encode()
                output = io.BytesIO()
                with tarfile.open(fileobj=output, mode='w', format=tarfile.PAX_FORMAT) as tar:
                    info = tarfile.TarInfo('BUNDLE.json'); info.size = len(manifest)
                    tar.addfile(info, io.BytesIO(manifest))
                    for name, body in bodies.items():
                        if change == 'missing' and name.endswith('/evidence.bin'): continue
                        info = tarfile.TarInfo(name); info.size = len(body)
                        tar.addfile(info, io.BytesIO(body))
                    if change == 'extra':
                        info = tarfile.TarInfo('unexpected.bin'); info.size = 1
                        tar.addfile(info, io.BytesIO(b'x'))
                raw = output.getvalue(); digest = hashlib.sha256(raw).hexdigest()
                indexed = dict(bank, namespace=m.NAMESPACE, bundle_sha256=digest,
                               key='native-custody/processed-v1/' + m.SOURCE['session_id'] + '/' + digest + '.tar')
                note = {key: indexed[key] for key in ('source', 'slot', 'bank', 'key', 'bundle_sha256', 'namespace', 'files')}
                note['schema'] = 'blockzilla-native-cold-copy-v1'
                for name in (artifact, custody):
                    destination = common.OUTPUT / name; destination.mkdir(parents=True)
                    (destination / 'REMOTE-CUSTODY.json').write_text(json.dumps(note))
                if change == 'member':
                    start = raw.index(b'execution bytes'); raw = raw[:start] + b'X' + raw[start + 1:]
                elif change == 'object_hash': raw = raw[:-1] + b'X'
                elif change == 'receipt': (common.OUTPUT / custody / 'REMOTE-CUSTODY.json').write_text('{}')
                stream = validator.HashedStream(io.BytesIO(raw), time.monotonic() + 3)
                def verify():
                    details = validator.validate_bundle(stream, [(99, indexed)])
                    m.require(stream.count == len(raw) == details['expected_object_bytes'], 'cloud_size_mismatch')
                    m.require(stream.hasher.hexdigest() == digest, 'cloud_digest_mismatch')
                    return details
                if change == 'success':
                    details = verify()
                    self.assertEqual((details['bundle_banks'], details['member_files'], details['local_receipts_checked']), (1, 2, 2))
                else:
                    with self.assertRaises((m.ProofError, validator.ProofError)): verify()


if __name__ == '__main__':
    unittest.main()
