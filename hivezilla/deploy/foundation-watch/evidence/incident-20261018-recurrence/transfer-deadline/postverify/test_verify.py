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
import types
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('postverify', Path(__file__).with_name('verify.py'))
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


class Tests(unittest.TestCase):
    def fixture(self, folder):
        root = Path(folder).resolve()
        m.ROOT = root
        m.OUTPUT = root / 'output'
        state = m.OUTPUT / 'cold-storage'
        state.mkdir(parents=True)
        db = sqlite3.connect(state / 'archive.sqlite')
        db.executescript('CREATE TABLE archives(slot INTEGER,bank INTEGER,record TEXT,retired INTEGER,PRIMARY KEY(slot,bank));'
                         'CREATE INDEX archives_retired ON archives(retired); CREATE TABLE meta(id INTEGER PRIMARY KEY,value TEXT);')
        db.execute('INSERT INTO meta VALUES(1,?)', (json.dumps(dict(source=m.SOURCE, namespace=m.NAMESPACE)),))
        db.execute('INSERT INTO meta VALUES(2,?)', ('200',))
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
        context = dict(before=dict(live_before=dict(archived_banks=2, retired_banks=2,
                            max_archive_rowid=2, last_archive_progress_unix_ms=100)),
                       install=dict(completed_unix_ms=150),
                       prior=dict(old_final_status=dict(storage={m.LIVE: dict(archived_banks=2, retired_banks=2)})))
        return db, context

    def test_selects_complete_new_bundle_and_rechecks_full_exact_group_and_notes(self):
        with tempfile.TemporaryDirectory() as folder:
            db, context = self.fixture(folder)
            counts, selected = m.live_snapshot(context)
            self.assertEqual([item[0] for item in selected], [5, 4])
            self.assertEqual(m.recheck_selection(selected), counts)
            db.close()

    def test_partial_latest_bundle_is_skipped_for_complete_new_preceding_bundle(self):
        with tempfile.TemporaryDirectory() as folder:
            db, context = self.fixture(folder)
            db.execute('UPDATE archives SET retired=0 WHERE bank=5')
            db.commit()
            _, selected = m.live_snapshot(context)
            self.assertEqual([item[0] for item in selected], [3])
            m.recheck_selection(selected)
            db.close()

    def test_no_new_rowid_wrong_namespace_and_wrong_source_fail(self):
        for change in ('old_rowids', 'namespace', 'source'):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as folder:
                db, context = self.fixture(folder)
                if change == 'old_rowids':
                    context['before']['live_before']['max_archive_rowid'] = 5
                elif change == 'namespace':
                    db.execute('UPDATE meta SET value=? WHERE id=1', (json.dumps(dict(source=m.SOURCE, namespace='wrong')),))
                else:
                    raw = json.loads(db.execute('SELECT record FROM archives WHERE bank=5').fetchone()[0])
                    raw['source'] = {}
                    db.execute('UPDATE archives SET record=? WHERE bank=5', (json.dumps(raw),))
                db.commit()
                with self.assertRaises(m.ProofError):
                    m.live_snapshot(context)
                db.close()

    def test_key_scan_rejects_off_group_match_changed_exact_record_and_malformed_json(self):
        for change in ('off_group', 'exact_record', 'malformed'):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as folder:
                db, context = self.fixture(folder)
                _, selected = m.live_snapshot(context)
                bank = 4 if change == 'exact_record' else 1
                raw = json.loads(db.execute('SELECT record FROM archives WHERE bank=?', (bank,)).fetchone()[0])
                if change == 'off_group':
                    raw['key'] = selected[0][1]['key']
                    raw['bundle_sha256'] = selected[0][1]['bundle_sha256']
                else:
                    raw['unexpected'] = True
                db.execute('UPDATE archives SET record=? WHERE bank=?', ('not-json' if change == 'malformed' else json.dumps(raw), bank))
                db.commit()
                with self.assertRaises((m.ProofError, sqlite3.Error)):
                    m.recheck_selection(selected)
                db.close()

    def test_changed_receipt_fails_after_readback(self):
        with tempfile.TemporaryDirectory() as folder:
            db, context = self.fixture(folder)
            _, selected = m.live_snapshot(context)
            (m.OUTPUT / selected[0][1]['custody'] / 'REMOTE-CUSTODY.json').write_text('{}')
            with self.assertRaises(m.ProofError):
                m.recheck_selection(selected)
            db.close()

    def test_unfrozen_completion_anchors_block_before_host_reads(self):
        with patch.object(m, 'ANCHORS', {'incomplete': (Path('/not-read'), None)}), \
                patch.object(m, 'object_file') as reader:
            with self.assertRaisesRegex(m.ProofError, 'canonical_completion_anchors_not_frozen'):
                m.anchored()
            reader.assert_not_called()

    def test_cloud_child_total_deadline_and_output_limit_kill_and_reap(self):
        for change in ('deadline', 'output'):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as folder:
                db, context = self.fixture(folder)
                _, selected = m.live_snapshot(context)
                started = time.monotonic()
                original = subprocess.Popen
                processes = []
                script = ('import time\nwhile True:\n print(" ",flush=True);time.sleep(.01)' if change == 'deadline'
                          else 'import os,time;os.write(1,b"x"*70000);time.sleep(60)')
                def launch(command, **kwargs):
                    process = original([sys.executable, '-c', script], **kwargs)
                    processes.append(process)
                    return process
                clock = types.SimpleNamespace(monotonic=lambda: time.monotonic() * 1000)
                with patch.object(m.subprocess, 'Popen', side_effect=launch), patch.object(m, 'time', clock):
                    with self.assertRaises(m.ProofError):
                        m.bounded_cloud(Path('/not-used'), selected)
                self.assertLess(time.monotonic() - started, 3)
                self.assertEqual(len(processes), 1)
                self.assertIsNotNone(processes[0].returncode)
                self.assertTrue(processes[0].stdout.closed)
                with self.assertRaises(ProcessLookupError):
                    os.kill(processes[0].pid, 0)
                with self.assertRaises(ChildProcessError):
                    os.waitpid(processes[0].pid, os.WNOHANG)
                db.close()

    def test_pinned_full_tar_validation_rejects_corruption_extra_missing_and_receipt_mismatch(self):
        for change in ('success', 'member', 'object_hash', 'extra', 'missing', 'receipt'):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as folder:
                root = Path(folder).resolve()
                m.ROOT, m.OUTPUT = root, root / 'output'
                module = m.validator_module(Path(__file__).resolve().with_name('cloud-validator.py'))
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
                    info = tarfile.TarInfo('BUNDLE.json')
                    info.size, info.mode = len(manifest), 0o600
                    tar.addfile(info, io.BytesIO(manifest))
                    for name, body in bodies.items():
                        if change == 'missing' and name.endswith('/evidence.bin'):
                            continue
                        info = tarfile.TarInfo(name)
                        info.size, info.mode = len(body), 0o600
                        tar.addfile(info, io.BytesIO(body))
                    if change == 'extra':
                        info = tarfile.TarInfo('unexpected.bin')
                        info.size, info.mode = 1, 0o600
                        tar.addfile(info, io.BytesIO(b'x'))
                raw = output.getvalue()
                digest = hashlib.sha256(raw).hexdigest()
                indexed = dict(bank, namespace=m.NAMESPACE, bundle_sha256=digest,
                               key='native-custody/processed-v1/' + m.SOURCE['session_id'] + '/' + digest + '.tar')
                note = {key: indexed[key] for key in ('source', 'slot', 'bank', 'key', 'bundle_sha256', 'namespace', 'files')}
                note['schema'] = 'blockzilla-native-cold-copy-v1'
                for name in (artifact, custody):
                    destination = m.OUTPUT / name
                    destination.mkdir(parents=True)
                    (destination / 'REMOTE-CUSTODY.json').write_text(json.dumps(note))
                if change == 'member':
                    start = raw.index(b'execution bytes')
                    raw = raw[:start] + b'X' + raw[start + 1:]
                elif change == 'object_hash':
                    raw = raw[:-1] + b'X'
                elif change == 'receipt':
                    (m.OUTPUT / custody / 'REMOTE-CUSTODY.json').write_text('{}')
                stream = module.HashedStream(io.BytesIO(raw), time.monotonic() + 3)
                def verify_tar():
                    details = module.validate_bundle(stream, [(99, indexed)])
                    m.require(stream.count == len(raw) == details['expected_object_bytes'], 'cloud_object_size_mismatch')
                    m.require(stream.hasher.hexdigest() == digest, 'cloud_object_digest_mismatch')
                    return details
                if change == 'success':
                    details = verify_tar()
                    self.assertEqual((details['bundle_banks'], details['member_files'], details['local_receipts_checked']), (1, 2, 2))
                    self.assertEqual(stream.count, len(raw))
                else:
                    with self.assertRaises((m.ProofError, module.ProofError)):
                        verify_tar()


if __name__ == '__main__':
    unittest.main()
