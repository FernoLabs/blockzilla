import importlib.util
import json
from pathlib import Path
import shlex
import shutil
import subprocess
import tempfile
import unittest
from unittest import mock


spec = importlib.util.spec_from_file_location("storage_installer", Path(__file__).with_name("install_storage_patch.py"))
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)
HELP = """agave-validator 4.3.0
    --maximum-full-snapshots-to-retain <NUMBER>
    --maximum-incremental-snapshots-to-retain <NUMBER>
"""
SESSION = "000000000000000018ddd9a6bb6fc50e-282459"


class Fixture:
    def __init__(self, directory):
        directory = directory.resolve()
        self.root, self.source, self.proc, self.etc = [directory / name for name in ('live', 'source', 'proc', 'etc')]
        self.root.mkdir(); self.source.mkdir(); (self.proc / '2579205').mkdir(parents=True)
        for folder in ('config', 'snapshots', 'releases', 'reports'):
            (self.root / folder).mkdir()
        deploy = Path(__file__).parent.parent
        sources = {
            'cold_storage.py': deploy / 'direct-agave/cold_storage.py',
            'recover_session.py': deploy / 'bank-stream-v2/recover_session.py',
            'snapshot_budget.py': deploy / 'direct-agave/snapshot_budget.py',
            'load_when_ready.py': deploy / 'bank-stream-v2/load_when_ready.py',
            'heartbeat.py': deploy / 'foundation-watch/heartbeat.py',
            'archive_status.py': deploy / 'foundation-watch/archive_status.py',
            'live_priority.py': deploy / 'foundation-upload/live_priority.py',
            'archive_upload.py': deploy / 'foundation-upload/archive_upload.py',
        }
        for name, path in sources.items(): shutil.copyfile(path, self.source / name)
        (self.source / 'credentials.env').write_text('PRIVATE_TEST_CREDENTIAL')
        self.binary = self.root / 'releases/current/agave-validator'
        self.binary.parent.mkdir(); self.binary.write_text('exact tested host'); self.binary.chmod(0o500)
        (self.proc / '2579205/exe').symlink_to(self.binary)
        self.pin = self.root / 'config/expected-source.json'
        self.pin.write_text(json.dumps(dict(genesis_hash='genesis', node_identity='identity',
            agave_revision='revision', session_id=SESSION)))
        r = self.root
        processed = str(r / 'compact/direct-agave-processed-live' / SESSION)
        banks = str(r / 'compact/bank-v2-live' / SESSION)
        capture = str(r / 'captures/direct-agave-live' / SESSION)
        origin_spool = str(r / 'bank-v2-origin-spool' / SESSION)
        v1_spool = str(r / 'stream-relay-spool-bank-v2' / SESSION)
        v2_spool = str(r / 'bank-v2-relay-spool' / SESSION)
        publisher_state = str(r / 'reports/native-processed-r2-bank-v2' / SESSION)
        self.commands = {
            'hivezilla-direct-worker': ['/usr/bin/python3', '/old/worker_launcher.py', '--processed',
                '--capture-root', str(r / 'captures/direct-agave-live'),
                '--output-root', str(r / 'compact/direct-agave-processed-live'),
                '--engine', '/old/native-engine', '--blockzilla', '/old/blockzilla-builder'],
            'hivezilla-bank-v2-producer': ['/old/native-engine', 'build-confirmed', processed, banks, '--follow'],
            'hivezilla-bank-v2-origin': ['/old/stream', 'bank-origin', '--output', banks, '--spool', origin_spool],
            'hivezilla-stream-origin': ['/old/stream', 'origin', '--output', processed],
            'hivezilla-stream-relay': ['/old/stream', 'relay', '--spool', v1_spool,
                '--banks-spool', v2_spool, '--banks-source-file', str(self.pin)],
            'hivezilla-bank-v2-r2-publisher': ['/old/stream', 'publish', '--output', processed,
                '--state', publisher_state, '--credentials-file', '/private/credentials.env'],
            m.STORAGE: ['/usr/bin/python3', '/old/cold_storage.py', 'archive', '--output', processed,
                '--bank-output', banks, '--capture', capture, '--publisher-state', publisher_state,
                '--credentials-file', '/private/credentials.env', '--v1-spool', v1_spool,
                '--bank-origin-spool', origin_spool, '--keep-seconds', '7200', '--retire', '--follow'],
            m.HEARTBEAT: ['/usr/bin/python3', '-B', '/old/heartbeat.py', '--report', str(r / 'reports/watch/STATUS.json')],
            m.PRIORITY: ['/usr/bin/python3', '-B', '/old/live_priority.py', '--root', str(r), '--report', str(r / 'reports/priority')],
            m.VALIDATOR: [str(self.binary), '--identity', '/private/identity.json', '--ledger', str(r / 'ledger'),
                '--accounts', str(r / 'accounts'), '--snapshots', str(r / 'snapshots'), '--no-voting',
                '--geyser-plugin-always-enabled', '--maximum-full-snapshots-to-retain', '2',
                '--maximum-incremental-snapshots-to-retain=4'],
        }
        self.units = {name: '[Service]\nEnvironmentFile=/private/environment.env\nExecStart=' + shlex.join(args) + '\n'
                      for name, args in self.commands.items()}
        self.units[m.VALIDATOR] += ('ExecStartPre=/usr/bin/test -d ' + str(r / 'snapshots') + '\n'
            'ExecStartPost=/usr/bin/python3 /old/load_when_ready.py --root ' + str(r) + ' --binary ' +
            str(self.binary) + ' --config ' + str(r / 'config/direct-agave.json') + '\n')
        self.calls, self.supported, self.budget_ok = [], True, True

    def run(self, *args):
        self.calls.append(args)
        if args[:2] == ('systemctl', 'cat'):
            return self.units[args[2].removesuffix('.service')]
        if args[:2] == ('systemctl', 'show'):
            return '2579205'
        if args == (str(self.binary), '--help'):
            return HELP if self.supported else HELP.replace('--maximum-incremental-snapshots-to-retain', '--unsupported')
        if '--check-start' in args:
            budget = json.dumps({'startup_disk_gate_passed': self.budget_ok,
                'current_free_bytes': 1000 * 1024**3 if self.budget_ok else 10 * 1024**3,
                'recovery_with_next_archive_threshold_free_bytes': m.MINIMUM_STARTUP_BYTES})
            if not self.budget_ok: raise subprocess.CalledProcessError(75, args, output=budget)
            return budget
        if args[:5] == ('sudo', 'install', '-d', '-m', '755'):
            Path(args[5]).mkdir(parents=True, exist_ok=True); return ''
        if args[:4] == ('sudo', 'install', '-m', '644'):
            shutil.copyfile(args[4], args[5]); return ''
        if args[:3] in (('sudo', 'systemctl', 'daemon-reload'), ('sudo', 'systemctl', 'restart')):
            return ''
        raise AssertionError(args)

    def install(self, prepare_only=False):
        def protected_read(args, text):
            if args[:3] != ['sudo', 'cat', '--'] or len(args) != 4 or text is not True:
                raise AssertionError(args)
            return Path(args[3]).read_text()
        with mock.patch.object(m, 'run', side_effect=self.run), \
             mock.patch.object(subprocess, 'check_output', side_effect=protected_read):
            return m.install_patch(self.source, self.root, prepare_only=prepare_only,
                                   proc_root=self.proc, systemd_root=self.etc)

    def mutations(self): return [args for args in self.calls if args[0] == 'sudo']


class StorageInstallerTests(unittest.TestCase):
    def test_prepare_at_low_space_creates_pinned_review_without_service_changes(self):
        with tempfile.TemporaryDirectory() as d:
            f = Fixture(Path(d)); f.budget_ok = False
            result = f.install(prepare_only=True)
            self.assertEqual(result['state'], 'PREPARED')
            self.assertFalse(result['startup_disk_gate_passed'])
            self.assertEqual(f.mutations(), [])
            release = Path(result['release'])
            self.assertEqual({p.name for p in release.iterdir()}, set(m.FILES) | {'MANIFEST.json'})
            self.assertFalse(release.stat().st_mode & 0o222)
            self.assertTrue((release / 'cold_storage.py').stat().st_mode & 0o100)
            for name in m.FILES:
                self.assertEqual((release / name).read_bytes(), (f.source / name).read_bytes())
                self.assertFalse((release / name).stat().st_mode & 0o222)

    def test_apply_reuses_reviewed_release_and_restarts_only_readers(self):
        with tempfile.TemporaryDirectory() as d:
            f = Fixture(Path(d)); prepared = f.install(prepare_only=True)
            f.calls.clear(); applied = f.install()
            self.assertEqual(applied['release'], prepared['release'])
            self.assertEqual(applied['state'], 'INSTALLED_PENDING_SOURCE_HANDOFF')
            self.assertTrue(applied['validator_pid_unchanged'])
            restarts = [args for args in f.calls if args[:3] == ('sudo', 'systemctl', 'restart')]
            self.assertEqual(restarts, [('sudo', 'systemctl', 'restart', m.HEARTBEAT + '.service', m.PRIORITY + '.service')])
            self.assertFalse(any('stop' in args or 'plugin' in args or 'load' in args for args in f.calls))

    def test_install_preserves_programs_source_paths_and_environment(self):
        with tempfile.TemporaryDirectory() as d:
            f = Fixture(Path(d)); result = f.install()
            release = Path(result['release'])
            for name, script in ((m.STORAGE, 'cold_storage.py'), (m.HEARTBEAT, 'heartbeat.py'), (m.PRIORITY, 'live_priority.py')):
                lines = result['overrides'][name].splitlines()
                new = shlex.split(next(line[10:] for line in lines if line.startswith('ExecStart=') and line != 'ExecStart='))
                old = f.commands[name].copy()
                index = next(i for i, value in enumerate(old) if Path(value).name == script)
                old[index] = str(release / script)
                self.assertEqual(new, old)
                self.assertNotIn('EnvironmentFile=', result['overrides'][name])
            worker = result['overrides']['hivezilla-direct-worker']
            self.assertNotIn('ExecStart=', worker)
            self.assertIn('HIVEZILLA_NATIVE_ARCHIVE_HELPER=' + str(release / 'cold_storage.py'), worker)
            validator = result['overrides'][m.VALIDATOR]
            args = shlex.split(next(line[10:] for line in validator.splitlines() if line.startswith('ExecStart=') and line != 'ExecStart='))
            self.assertEqual(args[0], str(f.binary))
            self.assertIn('/private/identity.json', args)
            self.assertIn('--geyser-plugin-always-enabled', args)
            self.assertIn('--no-voting', args)
            for flag in ('--maximum-full-snapshots-to-retain', '--maximum-incremental-snapshots-to-retain'):
                self.assertEqual(args.count(flag), 1); self.assertEqual(args[args.index(flag) + 1], '1')
            self.assertIn('ExecStartPre="/usr/bin/test" "-d"', validator)
            self.assertIn(str(release / 'snapshot_budget.py'), validator)
            self.assertIn(str(release / 'load_when_ready.py'), validator)
            self.assertFalse(result['credentials_read_or_copied'])

    def test_unsupported_exact_validator_help_fails_before_service_mutations(self):
        with tempfile.TemporaryDirectory() as d:
            f = Fixture(Path(d)); f.supported = False
            with self.assertRaisesRegex(ValueError, 'pinned binary help'):
                f.install()
            self.assertEqual(f.mutations(), [])
            self.assertEqual(list((f.root / 'releases').glob('storage-recovery-*')), [])

    def test_bad_current_storage_source_fails_before_service_mutations(self):
        with tempfile.TemporaryDirectory() as d:
            f = Fixture(Path(d))
            f.units[m.STORAGE] = f.units[m.STORAGE].replace('/captures/direct-agave-live/' + SESSION, '/captures/wrong/' + SESSION)
            with self.assertRaisesRegex(RuntimeError, 'current_service_source_mismatch'):
                f.install()
            self.assertEqual(f.mutations(), [])

    def test_low_budget_blocks_apply_without_service_mutations(self):
        with tempfile.TemporaryDirectory() as d:
            f = Fixture(Path(d)); f.budget_ok = False
            with self.assertRaisesRegex(RuntimeError, 'insufficient_current_startup_budget'):
                f.install()
            self.assertEqual(f.mutations(), [])

    def test_symlink_source_is_rejected_before_any_service_query(self):
        with tempfile.TemporaryDirectory() as d:
            f = Fixture(Path(d)); path = f.source / 'cold_storage.py'
            path.unlink(); path.symlink_to(Path(__file__).parent.parent / 'direct-agave/cold_storage.py')
            with self.assertRaisesRegex(RuntimeError, 'invalid_patch_file'):
                f.install()
            self.assertEqual(f.calls, [])

    def test_changed_dropin_is_saved_before_install(self):
        with tempfile.TemporaryDirectory() as d:
            f = Fixture(Path(d))
            target = f.etc / (m.PRIORITY + '.service.d') / m.DROPIN
            target.parent.mkdir(parents=True); target.write_text('[Service]\n# original override\n')
            f.units[m.PRIORITY] += '# ' + str(target) + '\n'
            result = f.install()
            saved = Path(result['report']) / 'saved' / str(target).lstrip('/')
            self.assertEqual(saved.read_text(), '[Service]\n# original override\n')
            self.assertEqual(target.read_text(), result['overrides'][m.PRIORITY])

    def test_cold_installer_and_source_handoff_use_the_same_dropin(self):
        with tempfile.TemporaryDirectory() as d:
            f = Fixture(Path(d)); result = f.install()
            recovery = m.module('storage_test_recovery', (f.source / 'recover_session.py').read_bytes(),
                                f.source / 'recover_session.py')
            path = Path(result['dropin_paths'][m.STORAGE])
            self.assertEqual(path.name, recovery.SOURCE_DROPIN)
            self.assertEqual(path.read_text(), result['overrides'][m.STORAGE])
            self.assertNotEqual(path.name, m.DROPIN)

    def test_unknown_higher_cold_override_fails_before_service_mutation(self):
        with tempfile.TemporaryDirectory() as d:
            f = Fixture(Path(d))
            f.units[m.STORAGE] += '# /etc/systemd/system/' + m.STORAGE + '.service.d/' + 'z' * 97 + '-unknown.conf\n'
            with self.assertRaisesRegex(RuntimeError, 'storage_dropin_does_not_sort_last'):
                f.install()
            self.assertEqual(f.mutations(), [])

    def test_read_only_source_unit_order_does_not_block_installer(self):
        with tempfile.TemporaryDirectory() as d:
            f = Fixture(Path(d))
            f.units['hivezilla-stream-origin'] += '# /etc/systemd/system/hivezilla-stream-origin.service.d/' + 'z' * 70 + '.conf\n'
            self.assertEqual(f.install()['state'], 'INSTALLED_PENDING_SOURCE_HANDOFF')


if __name__ == '__main__': unittest.main()
