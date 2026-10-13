#!/usr/bin/env python3
"""Small control/receipt tests; no sudo, CAR scans, perf or network calls."""
import copy
from contextlib import ExitStack
import json
import os
from pathlib import Path
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import run_resident_profile as runner


def fixture():
    case = runner.selected_cases([{'name': 'baseline', 'parsers': 2, 'workers': 4}])[0]
    dataset = {'epoch': 1000, 'transactions': 547899266, 'blocks': 2}
    rows = [[432000000, 1], [432431999, 547899265]]
    files = {name: {'stat': {'st_dev': 1, 'st_ino': inode, 'st_size': size, 'st_mtime_ns': 2, 'st_ctime_ns': 3}}
             for name, inode, size in [('epoch-1000.car', 10, 16385), ('epoch-1000-slot-ranges.raw', 11, 8192)]}
    source = {'files': files}
    resident = {phase: {} for phase in ('before_scan', 'after_scan')}
    identity = {'transport': 'mlocked-local-mmap'}
    for label, name in [('car', 'epoch-1000.car'), ('index', 'epoch-1000-slot-ranges.raw')]:
        stat = files[name]['stat']
        identity[label] = {'stat': stat, 'mlock_succeeded': True}
        for phase in resident:
            resident[phase][label] = {'all_pages_resident': True, 'mlock_succeeded': True,
                'total_pages': (stat['st_size'] + 4095) // 4096, 'resident_pages': (stat['st_size'] + 4095) // 4096,
                'mapped_bytes': stat['st_size'], 'page_bytes': 4096}
    resident['source'] = {'http_requests': 0}
    receipt = {'valid': True, 'block_transaction_rows': rows, 'transactions': dataset['transactions'],
        'blocks': 2, 'physical_blocks': 2, 'epoch': 1000, 'votes': 302828458, 'failed': 66856874,
        'metadata_mode': 'Owned', 'signature_verification': False, 'scheduling': 'dynamic', 'workers': 4,
        'parser_workers': 2, 'http_clients': 1, 'export_schema': None, 'export_bytes': None,
        'export_storage': None, 'http': None, 'effective_http_workers': 0, 'http_body_window_bytes': 0,
        'http_transport_mode': 'resident-range', 'http_chunk_bytes': 128 << 20, 'job_target_bytes': 256 << 20,
        'scan_seconds': 1.5, 'scan_resources': {'major_faults': 0, 'cpu_seconds': 4., 'user_cpu_seconds': 3.,
            'system_cpu_seconds': 1., 'mean_cpu_cores': 4 / 1.5}, 'resident_input': resident, 'identity': identity,
        'job_count': 2, 'pipeline': {'job_count': 2, 'jobs_claimed': 2, 'jobs_completed': 2,
            'blocks_parsed': 2, 'blocks_decoded': 2, 'parser_workers_finished': 2, 'decode_workers_finished': 4,
            'raw_block_pool_size': 8, 'work_queue_capacity': 8}, 'decode_stages': None}
    return case, dataset, rows, source, receipt


class SettingsTests(unittest.TestCase):
    def test_bounds_and_command_keep_full_owned_no_export_work(self):
        case, *_ = fixture()
        for key, value in [('name', '../escape'), ('parsers', 0), ('parsers', 129), ('workers', 257),
                           ('workers', True), ('profile', 'all'), ('stage_sample_every', -1),
                           ('stage_sample_every', True), ('extra', 1)]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                runner.selected_cases([dict(case, **{key: value})])
        with self.assertRaises(ValueError):
            runner.selected_cases([case, case])
        args = SimpleNamespace(bin=Path('/bin/reader'), car=Path('/cache/epoch-1000.car'),
            index=Path('/cache/epoch-1000-slot-ranges.raw'), manifest=Path('/cache/manifest.json'), plan=Path('/full.json'))
        command = runner.reader_command(args, case, Path('/results/baseline'))
        self.assertIn('--memlock=unlimited:unlimited', command)
        self.assertIn('--resident-cache-manifest', command)
        self.assertNotIn('--export', command)
        self.assertNotIn('--url', command)
        self.assertNotIn('--stage-sample-every', command)
        command = runner.reader_command(args, dict(case, stage_sample_every=1024), Path('/results/stage'))
        self.assertEqual(command[-2:], ['--stage-sample-every', '1024'])

    def test_perf_attaches_disabled_with_explicit_ack_and_frame_pointers(self):
        for mode in ('record', 'stat'):
            command = runner.perf_command(mode, 123, Path('/results/case'), Path('/usr/bin/perf'))
            self.assertEqual(command[:4], ['/usr/bin/perf', mode, '--pid', '123'])
            self.assertIn('--delay=-1', command)
            self.assertIn('--control=fifo:/results/case/perf-control.fifo,/results/case/perf-ack.fifo', command)
            self.assertNotIn('-a', command)
            if mode == 'record':
                self.assertIn('fp', command)
                self.assertEqual(command[command.index('-e') + 1], 'cpu-clock')
                self.assertEqual(command[command.index('-F') + 1], '99')
            else: self.assertIn('{cycles,instructions}', command[command.index('-e') + 1])

    def test_owned_process_rejects_pid_reuse_and_non_descendants(self):
        owned = runner.OwnedProcess.__new__(runner.OwnedProcess)
        owned.identity = {'pid': 123, 'starttime_ticks': 42}
        with patch.object(runner, 'process_snapshot', return_value={'pid': 123, 'starttime_ticks': 43}):
            with self.assertRaisesRegex(ValueError, 'reused'): owned.snapshot()
        with patch.object(runner, 'process_snapshot', side_effect=lambda pid: {'parent_pid': {3: 2, 2: 1}[pid]}):
            self.assertTrue(runner.is_descendant(3, 2))
            self.assertFalse(runner.is_descendant(3, 5))
        self.assertIn('pidfd_open', runner.PIDFD_SIGNAL)
        self.assertIn('pidfd_send_signal', runner.PIDFD_SIGNAL)

    def test_memory_preflight_includes_source_buffers_and_spare(self):
        case, _, _, source, _ = fixture()
        with patch.object(Path, 'read_text', return_value='MemAvailable: 1 kB\n'):
            value = runner.memory_preflight(case, source)
        self.assertFalse(value['sufficient'])
        self.assertEqual(value['spare_bytes'], 32 << 30)
        self.assertEqual(value['active_buffer_allowance_bytes'], (2 * 128 + 8 * 16) << 20)
        with patch.object(Path, 'read_text', return_value='MemAvailable: 9999999999 kB\n'):
            self.assertTrue(runner.memory_preflight(case, source)['sufficient'])


class ReceiptTests(unittest.TestCase):
    def test_exact_decode_residency_and_count_contract(self):
        case, dataset, rows, source, value = fixture()
        runner.validate_receipt(value, case, rows, dataset, source)
        for key, bad in [('valid', False), ('blocks', 1), ('transactions', 1), ('votes', 1), ('failed', 1),
                         ('metadata_mode', 'Visitor'), ('signature_verification', True), ('http', {}),
                         ('export_bytes', 1), ('export_schema', 'car-common-export-v1'),
                         ('effective_http_workers', 1), ('scan_seconds', float('nan'))]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                runner.validate_receipt(dict(value, **{key: bad}), case, rows, dataset, source)
        for phase in ('before_scan', 'after_scan'):
            for label in ('car', 'index'):
                for key, bad in [('all_pages_resident', False), ('mlock_succeeded', False), ('resident_pages', 0),
                                 ('total_pages', 1), ('mapped_bytes', 1), ('page_bytes', 0)]:
                    broken = copy.deepcopy(value)
                    broken['resident_input'][phase][label][key] = bad
                    with self.subTest(phase=phase, label=label, key=key), self.assertRaises(ValueError):
                        runner.validate_receipt(broken, case, rows, dataset, source)

    def test_faults_incomplete_pipeline_identity_and_stage_samples_fail(self):
        case, dataset, rows, source, value = fixture()
        for where, key, bad in [('scan_resources', 'major_faults', 1), ('scan_resources', 'cpu_seconds', float('inf')),
                                ('pipeline', 'jobs_completed', 1), ('pipeline', 'blocks_decoded', 1),
                                ('pipeline', 'decode_workers_finished', 3)]:
            broken = copy.deepcopy(value); broken[where][key] = bad
            with self.subTest(key=key), self.assertRaises(ValueError):
                runner.validate_receipt(broken, case, rows, dataset, source)
        broken = copy.deepcopy(value); broken['identity']['car']['stat']['st_ino'] = 99
        with self.assertRaises(ValueError): runner.validate_receipt(broken, case, rows, dataset, source)
        staged = dict(case, stage_sample_every=1024)
        with self.assertRaises(ValueError): runner.validate_receipt(value, staged, rows, dataset, source)
        value['decode_stages'] = {'sample_every': 1024, 'transactions_seen': dataset['transactions']}
        runner.validate_receipt(value, staged, rows, dataset, source)
        with self.assertRaises(ValueError): runner.validate_receipt(value, case, rows, dataset, source)


class FifoTests(unittest.TestCase):
    @unittest.skipUnless(hasattr(os, 'mkfifo'), 'requires local Unix FIFOs')
    def test_real_fifo_ack_and_timeout(self):
        with tempfile.TemporaryDirectory() as folder:
            control = runner.PerfControl(Path(folder))
            fake = SimpleNamespace(process=SimpleNamespace(poll=lambda: None))
            def respond():
                fd = os.open(control.control, os.O_RDONLY)
                try:
                    self.assertEqual(os.read(fd, 64), b'enable\n')
                    ack = os.open(control.ack, os.O_WRONLY)
                    try: os.write(ack, b'ack\n\0')
                    finally: os.close(ack)
                finally: os.close(fd)
            thread = threading.Thread(target=respond); thread.start()
            try:
                control.command('enable', fake, 1)
                thread.join(1)
                self.assertFalse(thread.is_alive())
                self.assertIn('ack_unix_seconds', control.events[0])
                self.assertEqual(control.events[0]['received_hex'], '61636b0a00')
                with self.assertRaises(TimeoutError): control.command('disable', fake, .02)
            finally: control.close()

    @unittest.skipUnless(hasattr(os, 'mkfifo'), 'requires local Unix FIFOs')
    def test_fragmented_null_terminated_ack_is_fully_consumed_before_disable(self):
        with tempfile.TemporaryDirectory() as folder:
            control = runner.PerfControl(Path(folder))
            fake = SimpleNamespace(process=SimpleNamespace(poll=lambda: None))
            observed = []
            def respond():
                fd = os.open(control.control, os.O_RDONLY)
                ack = os.open(control.ack, os.O_WRONLY)
                try:
                    for command in (b'enable\n', b'disable\n'):
                        observed.append(os.read(fd, 64))
                        for fragment in (b'a', b'ck\n', b'\0'):
                            os.write(ack, fragment)
                            time.sleep(.005)
                finally:
                    os.close(ack); os.close(fd)
            thread = threading.Thread(target=respond); thread.start()
            try:
                control.command('enable', fake, 1)
                control.command('disable', fake, 1)
                thread.join(1)
                self.assertFalse(thread.is_alive())
                self.assertEqual(observed, [b'enable\n', b'disable\n'])
                self.assertEqual([event['received_hex'] for event in control.events], ['61636b0a00'] * 2)
            finally: control.close()

    @unittest.skipUnless(hasattr(os, 'mkfifo'), 'requires local Unix FIFOs')
    def test_missing_or_invalid_terminator_fails_with_received_bytes(self):
        for reply in (b'ack\n', b'ack\nx', b'ack\n\0extra'):
            with self.subTest(reply=reply), tempfile.TemporaryDirectory() as folder:
                control = runner.PerfControl(Path(folder))
                fake = SimpleNamespace(process=SimpleNamespace(poll=lambda: None))
                def respond():
                    fd = os.open(control.control, os.O_RDONLY)
                    try:
                        os.read(fd, 64)
                        ack = os.open(control.ack, os.O_WRONLY)
                        try: os.write(ack, reply)
                        finally: os.close(ack)
                    finally: os.close(fd)
                thread = threading.Thread(target=respond); thread.start()
                try:
                    with self.assertRaises((TimeoutError, ValueError)) as error:
                        control.command('enable', fake, .05)
                    self.assertIn(repr(reply), str(error.exception))
                    self.assertEqual(control.events[0]['received_hex'], reply.hex())
                    thread.join(1)
                    self.assertFalse(thread.is_alive())
                finally: control.close()


class ControllerTests(unittest.TestCase):
    def test_scan_gate_order_and_failure_cleanup_without_export(self):
        for mode, failure in [('none', None), ('record', None), ('stat', None), ('record', 'disable'), ('none', 'ready'), ('record', 'interrupt')]:
            with self.subTest(mode=mode, failure=failure), tempfile.TemporaryDirectory() as folder:
                case, dataset, rows, source, receipt = fixture()
                case['profile'] = mode
                directory = Path(folder) / 'case'
                args = SimpleNamespace(bin=Path('/bin/reader'), car=Path('/cache/epoch-1000.car'),
                    index=Path('/cache/epoch-1000-slot-ranges.raw'), manifest=Path('/cache/manifest.json'),
                    plan=Path('/full.json'), perf=Path('/usr/bin/perf'), timeout_seconds=10, setup_timeout_seconds=5)
                order, saved = [], []
                class Child:
                    def __init__(self, command, directory, name):
                        self.name, self.command = name, command
                        self.identity = {'pid': 123 if name == 'reader' else 124, 'starttime_ticks': 1}
                        self.process = SimpleNamespace(poll=lambda: None, wait=lambda timeout: 0)
                        if name == 'perf': (directory / ('perf.data' if mode == 'record' else 'perf-stat.csv')).write_bytes(b'profile')
                    def capture(self): return self.identity
                    def snapshot(self): return dict(self.identity, threads=4, rss_bytes=100)
                    def validate_executable(self, expected=None): return str(expected or '/usr/bin/perf')
                    def stop(self, *_args): order.append(self.name + '-stop'); return 0
                    def close(self): pass
                class Control:
                    def __init__(self, directory): self.events = []
                    def command(self, command, child):
                        order.append(command)
                        if command == failure: raise TimeoutError('injected acknowledgement failure')
                        self.events.append({'command': command, 'ack_unix_seconds': len(self.events) + 1})
                    def close(self): order.append('control-close')
                def gate(path, child, deadline, sample):
                    if path.name == 'ready.json':
                        if failure == 'ready': raise TimeoutError('injected preload timeout')
                        order.append('ready'); return {'pid': 123}
                    self.assertTrue((directory / 'gate/start').exists())
                    if mode != 'none': self.assertIn('enable', order)
                    order.append('scan-done')
                    if failure == 'interrupt': raise InterruptedError('injected controller stop')
                    (directory / 'report.json').write_text(json.dumps(receipt))
                    return {'pid': 123, 'scan_seconds': 1.5, 'scan_returned_ok': True}
                with patch.object(runner, 'OwnedProcess', Child), patch.object(runner, 'PerfControl', Control), \
                     patch.object(runner, 'wait_json', side_effect=gate), \
                     patch.object(runner, 'memory_preflight', return_value={'sufficient': True}), \
                     patch.object(runner.dynamic, 'host_counters', return_value={'network': {}, 'disks': {}}):
                    if failure == 'interrupt':
                        with self.assertRaises(InterruptedError):
                            runner.run_case(args, case, directory, source, dataset, rows, lambda row: saved.append(copy.deepcopy(row)))
                        row = saved[-1]
                    else:
                        row = runner.run_case(args, case, directory, source, dataset, rows, lambda row: saved.append(copy.deepcopy(row)))
                self.assertEqual(row['state'], 'FAILED' if failure else 'MEASURED')
                self.assertIn('reader-stop', order)
                self.assertNotIn('--export', row['reader_command'])
                self.assertNotIn('output_sha256', row)
                self.assertNotIn('byte_parity', row)
                if mode != 'none' and failure != 'ready':
                    self.assertIn('perf-stop', order)
                    self.assertLess(order.index('enable'), order.index('scan-done'))
                    if failure != 'interrupt': self.assertLess(order.index('scan-done'), order.index('disable'))
                if not failure:
                    self.assertTrue((directory / 'gate/release').exists())

    def test_series_state_keeps_failed_attempt_and_no_acceptance_claim(self):
        for failure in (False, True):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as folder:
                root = Path(folder)
                case, dataset, rows, source, _ = fixture()
                cases = [case, dict(case, name='repeat')]
                paths = {name: root / name for name in ('bin', 'manifest', 'plan', 'car', 'index', 'cases')}
                for name, path in paths.items(): path.write_text(json.dumps(cases if name == 'cases' else {}))
                result = root / 'results'
                argv = ['runner', '--results', str(result)]
                for name, path in paths.items(): argv += ['--' + name, str(path)]
                def run(args, case, directory, source, dataset, rows, save):
                    value = {'name': case['name'], 'state': 'FAILED' if failure and case['name'] == 'baseline' else 'MEASURED'}
                    save(value); return value
                with ExitStack() as stack:
                    stack.enter_context(patch('sys.argv', argv))
                    stack.enter_context(patch.object(runner.platform, 'system', return_value='Linux'))
                    stack.enter_context(patch.object(runner.signal, 'signal'))
                    stack.enter_context(patch.object(runner.full, 'host_snapshot', return_value={}))
                    stack.enter_context(patch.object(runner, 'source_check', return_value=(source, dataset, rows)))
                    stack.enter_context(patch.object(runner, 'run_case', side_effect=run))
                    if failure:
                        with self.assertRaises(ValueError): runner.main()
                    else: runner.main()
                status = json.loads((result / 'status.json').read_text())
                self.assertEqual(status['state'], 'FAILED' if failure else 'MEASURED_UNVERIFIED')
                self.assertEqual(len(status['cases']), 2)
                self.assertEqual(status['acceptance'], 'FAILED' if failure else 'UNVERIFIED')


if __name__ == '__main__':
    unittest.main()
