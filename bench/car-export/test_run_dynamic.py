#!/usr/bin/env python3
"""Control-path checks for selected scheduling experiments; no network access."""
from contextlib import ExitStack, redirect_stderr, redirect_stdout
import copy
import io
import http.client
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from urllib.error import HTTPError
from unittest.mock import patch

import run_dynamic as runner


def cases():
    return runner.selected_cases([
        {'name': 'static-c1', 'reader': 'car', 'workers': 4, 'parser_workers': 2},
        {'name': 'dynamic-c2', 'reader': 'car', 'workers': 4, 'parser_workers': 2,
         'scheduling': 'dynamic', 'http_clients': 2},
        {'name': 'jetstreamer', 'reader': 'jetstreamer', 'workers': 4}])


def dataset_and_plan():
    dataset = runner.full.default_dataset()
    dataset.update(epoch=1000, start_slot=432000000, end_slot_exclusive=432432000,
                   blocks=3, transactions=6, canonical_index_sha256='a' * 64)
    plan = {'epoch': 1000, 'start_slot': dataset['start_slot'], 'end_slot_exclusive': dataset['end_slot_exclusive'],
            'blocks': 3, 'transactions': 6,
            'block_transaction_rows': [[432000000, 2], [432000001, 1], [432431999, 3]],
            'source_index': {'index_sha256': 'a' * 64, 'accepted_index_sha256': 'a' * 64,
                             'index_block_count': 3, 'index_transactions': 6}}
    return dataset, plan


def receipt(case, dataset, prefix):
    rows = prefix['block_transaction_rows']
    common = {'valid': True, 'epoch': dataset['epoch'], 'blocks': len(rows),
              'transactions': sum(row[1] for row in rows), 'block_transaction_rows': rows,
              'export_schema': 'car-common-export-v1', 'export_bytes': 20,
              'allocator': 'mimalloc', 'signature_verification': False,
              'total_seconds': 2.0, 'scan_seconds': 1.0}
    if case.get('export_mode') == 'memory':
        common['export_storage'] = {'mode': 'memory', 'memory_limit_bytes': case['export_memory_mib'] << 20,
                                    'peak_memory_bytes': 20, 'spool_bytes': 0}
        common['export_finish_seconds'] = 0.5
    if case['reader'] == 'jetstreamer':
        common.update(processing_workers=case['workers'], errors=[], upstream_version='0.7.0',
                      upstream_commit=runner.full.JETSTREAMER_COMMIT, reported_error_policy='body-transport',
                      reported_body_retries=0, reported_body_retry_limit_per_worker_slot=2,
                      reported_error_events=[], reported_error_events_truncated=False,
                      transaction_callbacks=common['transactions'],
                      worker_counters=[{'simple_vote_callbacks': 1, 'failed_status_callbacks': 1}],
                      http_base='https://files.old-faithful.net/', index_base='https://files.old-faithful.net/',
                      start_slot=prefix['start_slot'], end_slot_exclusive=prefix['end_slot_exclusive'])
        return common
    common.update({key: case[key] for key in ['workers', 'parser_workers', 'scheduling', 'http_clients']})
    common.update(metadata_mode='Owned', reuse_http_buffers=False, votes=1, failed=1,
                  http_workers=16, http_window_chunks=16, http_chunk_bytes=case['http_chunk_mib'] << 20,
                  http_transport_mode='dynamic-range' if case['scheduling'] == 'dynamic' else 'sharded-range',
                  http_body_window_bytes=case['parser_workers'] * (case['http_chunk_mib'] << 20),
                  effective_http_workers=case['parser_workers'], job_target_bytes=case['job_mib'] << 20,
                  job_count=2, physical_blocks=2, http={'scan_completed': True, 'workers_finished': 2},
                  identity={'url': 'https://files.old-faithful.net/1000/epoch-1000.car',
                            'bytes': dataset['car_bytes'], 'operator_trusted': True,
                            'slot_index_url': 'https://files.old-faithful.net/1000/epoch-1000-slot-ranges.raw',
                            'slot_index_bytes': dataset['index_bytes']}, pipeline=None)
    if case['scheduling'] == 'dynamic':
        common['pipeline'] = {'raw_block_pool_size': case['workers'] + 2 * case['parser_workers'],
                              'work_queue_capacity': case['workers'] + 2 * case['parser_workers'],
                              'job_count': 2, 'jobs_claimed': 2, 'jobs_completed': 2,
                              'blocks_parsed': 2, 'blocks_decoded': 2,
                              'parser_workers_finished': case['parser_workers'],
                              'decode_workers_finished': case['workers'], 'max_ready_queue_depth': 2}
    return common


class SettingsTests(unittest.TestCase):
    def test_loopback_base_needs_flag_and_exact_literal_host(self):
        self.assertFalse(runner.validate_base('https://files.old-faithful.net/'))
        for base in ('http://127.0.0.1:8123/', 'http://[::1]:8123/', 'http://127.0.0.1/'):
            self.assertTrue(runner.validate_base(base, True))
            with self.assertRaises(ValueError):
                runner.validate_base(base)
        for base in ('http://localhost:8123/', 'http://127.1:8123/', 'http://2130706433/',
                     'http://127.0.0.2/', 'http://[::ffff:127.0.0.1]/', 'http://192.0.2.1/',
                     'http://user@127.0.0.1/', 'http://127.0.0.1@remote.test/',
                     'http://127.0.0.1.evil.test/', 'http://127.0.0.1:0/', 'http://127.0.0.1:65536/',
                     'http://127.0.0.1/?q=/', 'http://127.0.0.1/#fragment/',
                     'http://127.0.0.1\\remote.test/', 'http://127.0.0.1:\n8123/'):
            with self.subTest(base=base), self.assertRaises(ValueError):
                runner.validate_base(base, True)

    def test_selected_cases_reject_unknown_unsafe_or_unbounded_settings(self):
        self.assertEqual(cases()[0]['job_mib'], 256)
        self.assertEqual(cases()[0]['http_chunk_mib'], 32)
        self.assertEqual(runner.selected_cases([dict(cases()[1], workers=1)])[0]['parser_workers'], 2)
        for field, value in [('name', '../escape'), ('workers', True), ('workers', 257),
                             ('parser_workers', 5), ('http_clients', 3), ('job_mib', 4097),
                             ('http_chunk_mib', 0), ('http_chunk_mib', 129), ('http_chunk_mib', True),
                             ('scheduling', 'automatic'), ('unknown', 1)]:
            with self.subTest(field=field), self.assertRaises(ValueError):
                runner.selected_cases([dict(cases()[0], **{field: value})])
        with self.assertRaises(ValueError):
            runner.selected_cases([cases()[0], cases()[0]])
        with self.assertRaises(ValueError):
            runner.selected_cases([dict(cases()[2], parser_workers=2)])

    def test_order_is_explicit_balanced_or_seeded(self):
        pair = cases()[:2]
        self.assertEqual(runner.case_schedule(pair, 3), [(1, 0), (1, 1), (2, 1), (2, 0), (3, 0), (3, 1)])
        self.assertEqual(runner.case_schedule(pair, 2, 'round-robin'), [(1, 0), (1, 1), (2, 0), (2, 1)])
        self.assertEqual(runner.case_schedule(cases(), 3, 'random', 42), runner.case_schedule(cases(), 3, 'random', 42))
        for count in (0, 9):
            with self.assertRaises(ValueError):
                runner.case_schedule(pair, count)

    def test_export_mode_defaults_and_bounds_apply_to_both_readers(self):
        for original in cases():
            self.assertEqual(original['export_mode'], 'disk')
            self.assertNotIn('export_memory_mib', original)
            memory = runner.selected_cases([dict(original, export_mode='memory')])[0]
            self.assertEqual(memory['export_memory_mib'], 196608)
            for limit in (1, 196608, 262144):
                chosen = runner.selected_cases([dict(memory, export_memory_mib=limit)])[0]
                self.assertEqual(chosen['export_memory_mib'], limit)
            for limit in (0, -1, 262145, True, 1.0, '196608', None):
                with self.subTest(reader=original['reader'], limit=limit), self.assertRaises(ValueError):
                    runner.selected_cases([dict(memory, export_memory_mib=limit)])
            for mode in ('automatic', '', None, True):
                with self.subTest(reader=original['reader'], mode=mode), self.assertRaises(ValueError):
                    runner.selected_cases([dict(original, export_mode=mode)])
            with self.assertRaises(ValueError):
                runner.selected_cases([dict(original, export_memory_mib=196608)])

    def test_prefix_is_selected_from_the_verified_full_plan(self):
        dataset, plan = dataset_and_plan()
        prefix = runner.prefix_plan(plan, dataset, 2)
        self.assertEqual(prefix['block_transaction_rows'], plan['block_transaction_rows'][:2])
        self.assertEqual(prefix['end_slot_exclusive'], 432000002)
        self.assertEqual(prefix['selection']['transactions'], 3)
        self.assertEqual((prefix['blocks'], prefix['transactions']), (2, 3))
        self.assertEqual((plan['blocks'], plan['transactions']), (3, 6))
        self.assertEqual(len(plan['block_transaction_rows']), 3)
        self.assertEqual(runner.prefix_plan(plan, dataset, 3)['end_slot_exclusive'], dataset['end_slot_exclusive'])
        for count in (0, 4):
            with self.assertRaises(ValueError):
                runner.prefix_plan(plan, dataset, count)
        plan['source_index']['index_sha256'] = 'b' * 64
        with self.assertRaises(ValueError):
            runner.prefix_plan(plan, dataset, 2)


class ReceiptTests(unittest.TestCase):
    def setUp(self):
        self.dataset, plan = dataset_and_plan()
        self.prefix = runner.prefix_plan(plan, self.dataset, 2)
        self.base = 'https://files.old-faithful.net/'

    def check(self, value, case):
        return runner.validate_receipt(value, case, self.prefix['block_transaction_rows'], self.dataset, self.prefix, self.base)

    def test_static_dynamic_and_latest_jet_have_equal_counter_contracts(self):
        for case in cases():
            value = receipt(case, self.dataset, self.prefix)
            self.assertEqual(self.check(value, case), {'votes': 1, 'failed': 1})
            command = runner.reader_command(case, Path('/bin/reader'), Path('/data/plan'), Path('/data/case'),
                                            self.dataset, self.prefix, self.base)
            self.assertNotIn('--export-mode', command)
            self.assertNotIn('--export-memory-mib', command)
            if case['reader'] == 'car':
                for flag, expected in [('--scheduling', case['scheduling']), ('--http-clients', case['http_clients']),
                                       ('--job-mib', case['job_mib']), ('--parser-workers', case['parser_workers'])]:
                    self.assertEqual(command[command.index(flag) + 1], str(expected))
            else:
                self.assertEqual(command[command.index('--reported-error-policy') + 1], 'body-transport')

    def test_memory_export_command_and_receipt_match_the_selected_bound(self):
        for original in cases():
            for limit in (1, 196608, 262144):
                case = runner.selected_cases([dict(original, export_mode='memory', export_memory_mib=limit)])[0]
                value = receipt(case, self.dataset, self.prefix)
                self.assertEqual(self.check(value, case), {'votes': 1, 'failed': 1})
                command = runner.reader_command(case, Path('/bin/reader'), Path('/data/plan'), Path('/data/case'),
                                                self.dataset, self.prefix, self.base)
                self.assertEqual(command[command.index('--export-mode') + 1], 'memory')
                self.assertEqual(command[command.index('--export-memory-mib') + 1], str(limit))
                self.assertIn('--export', command)
                value['export_storage']['peak_memory_bytes'] = limit << 20
                self.assertEqual(self.check(value, case), {'votes': 1, 'failed': 1})

    def test_memory_export_requires_complete_valid_storage_receipts(self):
        for original in cases():
            case = runner.selected_cases([dict(original, export_mode='memory')])[0]
            good = receipt(case, self.dataset, self.prefix)
            limit = case['export_memory_mib'] << 20
            for field, value in [('mode', 'disk'), ('memory_limit_bytes', limit - 1),
                                 ('memory_limit_bytes', float(limit)), ('memory_limit_bytes', True),
                                 ('peak_memory_bytes', limit + 1), ('peak_memory_bytes', -1),
                                 ('peak_memory_bytes', True), ('peak_memory_bytes', float('nan')),
                                 ('spool_bytes', 1), ('spool_bytes', False), ('spool_bytes', 0.0)]:
                bad = copy.deepcopy(good)
                bad['export_storage'][field] = value
                with self.subTest(reader=case['reader'], field=field, value=value), self.assertRaises(ValueError):
                    self.check(bad, case)
            for field in good['export_storage']:
                bad = copy.deepcopy(good)
                del bad['export_storage'][field]
                with self.subTest(reader=case['reader'], missing=field), self.assertRaises(ValueError):
                    self.check(bad, case)
            for storage in (None, {}, [], 'memory'):
                with self.subTest(reader=case['reader'], storage=storage), self.assertRaises(ValueError):
                    self.check(dict(good, export_storage=storage), case)
            del good['export_storage']
            with self.assertRaises(ValueError):
                self.check(good, case)

    def test_disk_receipts_support_frozen_binaries_but_reject_memory_mode(self):
        for case in cases():
            good = receipt(case, self.dataset, self.prefix)
            self.assertNotIn('export_storage', good)
            self.assertEqual(self.check(good, case), {'votes': 1, 'failed': 1})
            good['export_storage'] = {'mode': 'disk', 'memory_limit_bytes': 0,
                                      'peak_memory_bytes': 0, 'spool_bytes': 20}
            self.assertEqual(self.check(good, case), {'votes': 1, 'failed': 1})
            good['export_storage']['mode'] = 'memory'
            with self.assertRaises(ValueError):
                self.check(good, case)

    def test_export_finish_duration_must_be_finite_and_within_total(self):
        for case in cases():
            good = receipt(case, self.dataset, self.prefix)
            for finish in (0, 0.5, good['total_seconds']):
                self.assertEqual(self.check(dict(good, export_finish_seconds=finish), case), {'votes': 1, 'failed': 1})
            for finish in (-1, good['total_seconds'] + 1, float('inf'), float('nan'), True, '0.5'):
                with self.subTest(finish=finish), self.assertRaises(ValueError):
                    self.check(dict(good, export_finish_seconds=finish), case)

    def test_loopback_command_and_receipt_keep_the_actual_source(self):
        base = 'http://127.0.0.1:8123/'
        for case in cases():
            value = receipt(case, self.dataset, self.prefix)
            command = runner.reader_command(case, Path('/bin/reader'), Path('/data/plan'), Path('/data/case'),
                                            self.dataset, self.prefix, base, True)
            if case['reader'] == 'car':
                self.assertIn('--allow-loopback-http', command)
                value['allow_loopback_http'] = True
                value['identity']['loopback_http'] = True
                value['identity']['url'] = base + '1000/epoch-1000.car'
                value['identity']['slot_index_url'] = base + '1000/epoch-1000-slot-ranges.raw'
            else:
                self.assertNotIn('--allow-loopback-http', command)
                self.assertEqual(command[command.index('--http-base') + 1], base)
                value['http_base'] = value['index_base'] = base
            self.assertEqual(runner.validate_receipt(value, case, self.prefix['block_transaction_rows'],
                             self.dataset, self.prefix, base, True), {'votes': 1, 'failed': 1})
            with self.assertRaises(ValueError):
                runner.validate_receipt(value, case, self.prefix['block_transaction_rows'],
                                        self.dataset, self.prefix, base)
            if case['reader'] == 'car':
                value['identity']['loopback_http'] = False
                with self.assertRaises(ValueError):
                    runner.validate_receipt(value, case, self.prefix['block_transaction_rows'],
                                            self.dataset, self.prefix, base, True)


    def test_indexed_chunk_bounds_reach_command_and_exact_receipt_checks(self):
        for original in cases()[:2]:
            for chunk_mib in (1, 32, 128):
                with self.subTest(scheduling=original['scheduling'], chunk_mib=chunk_mib):
                    case = runner.selected_cases([dict(original, http_chunk_mib=chunk_mib)])[0]
                    value = receipt(case, self.dataset, self.prefix)
                    self.assertEqual(self.check(value, case), {'votes': 1, 'failed': 1})
                    command = runner.reader_command(case, Path('/bin/reader'), Path('/data/plan'),
                                                    Path('/data/case'), self.dataset, self.prefix, self.base)
                    self.assertEqual(command[command.index('--http-chunk-mib') + 1], str(chunk_mib))
                    self.assertEqual(value['http_body_window_bytes'], case['parser_workers'] * chunk_mib * (1 << 20))
                    for field in ('http_chunk_bytes', 'http_body_window_bytes'):
                        with self.assertRaises(ValueError):
                            self.check(dict(value, **{field: value[field] + 1}), case)

    def test_dynamic_cannot_pass_with_wrong_settings_or_incomplete_pipeline(self):
        case = cases()[1]
        good = receipt(case, self.dataset, self.prefix)
        for field, value in [('valid', False), ('block_transaction_rows', []), ('http_clients', 1),
                             ('scheduling', 'static'), ('http_transport_mode', 'sharded-range'),
                             ('job_target_bytes', 1), ('http_body_window_bytes', 1),
                             ('physical_blocks', 1), ('scan_seconds', float('nan')), ('scan_seconds', 3.0)]:
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.check(dict(good, **{field: value}), case)
        for field, value in [('raw_block_pool_size', 7), ('work_queue_capacity', 7), ('job_count', 1),
                             ('jobs_completed', 1), ('blocks_decoded', 1), ('parser_workers_finished', 1),
                             ('decode_workers_finished', 3), ('max_ready_queue_depth', 9)]:
            bad = copy.deepcopy(good)
            bad['pipeline'][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.check(bad, case)
        for field, value in [('slot_index_url', 'https://wrong/index'), ('slot_index_bytes', 1)]:
            bad = copy.deepcopy(good)
            bad['identity'][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.check(bad, case)
        for field, value in [('scan_completed', False), ('workers_finished', 1)]:
            bad = copy.deepcopy(good)
            bad['http'][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.check(bad, case)
        static = cases()[0]
        bad = receipt(static, self.dataset, self.prefix)
        bad['job_count'] = 1
        with self.assertRaises(ValueError):
            self.check(bad, static)

    def test_current_jet_revision_policy_and_counts_stay_mandatory(self):
        case = cases()[2]
        good = receipt(case, self.dataset, self.prefix)
        for field, value in [('reported_error_policy', 'strict'), ('upstream_commit', 'older'),
                             ('transaction_callbacks', 2), ('errors', ['error']), ('scan_seconds', -1)]:
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.check(dict(good, **{field: value}), case)


class ExportMemoryPreflightTests(unittest.TestCase):
    def test_available_memory_includes_a_separate_overhead_allowance(self):
        case = runner.selected_cases([dict(cases()[1], export_mode='memory')])[0]
        required = (case['export_memory_mib'] << 20) + (16 << 30)
        for available in (required - 1024, required, required + 1024):
            with patch.object(Path, 'read_text', return_value=f'MemTotal: 999999999 kB\nMemAvailable: {available // 1024} kB\n'):
                result = runner.export_memory_preflight(case)
            self.assertEqual(result['available_bytes'], available)
            self.assertEqual(result['payload_limit_bytes'], case['export_memory_mib'] << 20)
            self.assertEqual(result['overhead_allowance_bytes'], 16 << 30)
            self.assertEqual(result['required_bytes'], required)
            self.assertEqual(result['sufficient'], available >= required)
            self.assertEqual(result['source'], '/proc/meminfo MemAvailable')

    def test_missing_malformed_or_duplicate_memory_samples_fail(self):
        case = runner.selected_cases([dict(cases()[1], export_mode='memory')])[0]
        for sample in ('', 'MemFree: 999999999 kB', 'MemAvailable: -1 kB', 'MemAvailable: 1 MB',
                       'MemAvailable: 1', 'MemAvailable: 1.5 kB', 'MemAvailable: 1 kB\nMemAvailable: 2 kB'):
            with self.subTest(sample=sample), patch.object(Path, 'read_text', return_value=sample), self.assertRaises(ValueError):
                runner.export_memory_preflight(case)


class LocalSourceTests(unittest.TestCase):
    def test_server_identity_binds_verified_manifest_endpoint_and_warm_file(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            files = {}
            for name in ('epoch-1000.car', 'epoch-1000-slot-ranges.raw'):
                path = root / name
                path.write_bytes(('complete fixture ' + name).encode())
                path.chmod(0o444)
                files[name] = {'bytes': path.stat().st_size, 'sha256': runner.digest(path),
                               'stat': runner.file_identity(path.stat())}
            manifest_path = root / 'manifest.json'
            manifest_path.write_text(json.dumps({'schema': 'zilla-local-car-cache-v1', 'state': 'COMPLETE',
                                                 'epoch': 1000, 'files': files}))
            status_path = root / 'status.json'
            base = 'http://127.0.0.1:8123/'
            status = {'schema': 'zilla-local-car-server-status-v1', 'state': 'READY', 'pid': 123,
                      'bind_host': '127.0.0.1', 'base_url': base, 'epoch': 1000, 'cache_dir': str(root),
                      'manifest_path': str(manifest_path), 'manifest_sha256': runner.digest(manifest_path),
                      'files': files}
            status_path.write_text(json.dumps(status))
            dataset = {'epoch': 1000, 'car_bytes': files['epoch-1000.car']['bytes'],
                       'index_bytes': files['epoch-1000-slot-ranges.raw']['bytes'],
                       'index_sha256': files['epoch-1000-slot-ranges.raw']['sha256']}
            script = Path(runner.__file__).with_name('serve_cache.py').resolve()
            command = b'python3\0' + os.fsencode(script) + b'\0--status\0' + os.fsencode(status_path) + b'\0'
            original_read = Path.read_bytes
            original_resolve = Path.resolve
            def read(path):
                return command if str(path) == '/proc/123/cmdline' else original_read(path)
            def resolve(path, *args, **kwargs):
                return root if str(path) == '/proc/123/cwd' else original_resolve(path, *args, **kwargs)
            with patch.object(runner, 'source_server_snapshot', return_value={'starttime_ticks': 42}), \
                    patch.object(Path, 'read_bytes', read), patch.object(Path, 'resolve', resolve):
                actual = runner.source_server_identity(123, base, dataset, root / 'epoch-1000.car')
                self.assertEqual(actual['files'], files)
                self.assertEqual(actual['starttime_ticks'], 42)
                for changed in (dict(status, pid=124), dict(status, base_url='http://127.0.0.1:8124/'),
                                dict(status, state='FAILED'), dict(status, manifest_sha256='0' * 64)):
                    status_path.write_text(json.dumps(changed))
                    with self.assertRaises(ValueError):
                        runner.source_server_identity(123, base, dataset, root / 'epoch-1000.car')
                status_path.write_text(json.dumps(status))
                with self.assertRaises(ValueError):
                    runner.source_server_identity(123, base, dataset, root / 'epoch-1000-slot-ranges.raw')
                path = root / 'epoch-1000.car'
                path.chmod(0o644)
                with self.assertRaises(ValueError):
                    runner.source_server_identity(123, base, dataset, path)

    def test_server_cpu_is_separate_and_pid_reuse_is_rejected(self):
        before = {'pid': 123, 'starttime_ticks': 42, 'cpu_seconds': 2, 'rss_mib': 3}
        after = dict(before, cpu_seconds=5, rss_mib=4, peak_rss_mib_lifetime=8)
        reader = {'cpu_seconds': 9, 'process_seconds': 2}
        result = runner.source_server_delta(before, after, reader)
        self.assertEqual(result['cpu_seconds'], 3)
        self.assertEqual(result['mean_cpu_cores'], 1.5)
        self.assertEqual(result['reader_and_server_mean_cpu_cores'], 6)
        self.assertEqual(reader['cpu_seconds'], 9)
        for changed in (dict(after, pid=124), dict(after, starttime_ticks=43), dict(after, cpu_seconds=1)):
            with self.assertRaises(ValueError):
                runner.source_server_delta(before, changed, reader)

    def test_warmup_reads_exact_file_and_rejects_identity_changes(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'car'
            data = b'whole CAR data' * 1000
            path.write_bytes(data)
            identity = runner.file_identity(path.stat())
            warmed = runner.warm_source_file(path, len(data), identity)
            self.assertEqual(warmed['bytes'], len(data))
            self.assertEqual(warmed['file_identity'], identity)
            self.assertEqual(warmed['buffer_bytes'], 32 << 20)
            with self.assertRaises(ValueError):
                runner.warm_source_file(path, len(data) + 1, identity)
            path.write_bytes(b'different data')
            with self.assertRaises(ValueError):
                runner.warm_source_file(path, len(data), identity)



class SummaryTests(unittest.TestCase):
    def test_unverified_summary_needs_every_measurement_and_cannot_claim_parity(self):
        selected = cases()[:1]
        rows = [{'state': 'MEASURED', 'case_name': selected[0]['name'], 'repetition': number,
                 'total_seconds': seconds, 'transactions': 100, 'directory': f'run{number}',
                 'output_verification': 'SKIPPED'} for number, seconds in ((1, 2), (2, 4))]
        result = runner.result_summary(rows, selected, 2, True)
        self.assertEqual(result[selected[0]['name']]['mean_total_seconds'], 3)
        self.assertEqual(result[selected[0]['name']]['output_verification'], 'SKIPPED')
        with self.assertRaises(ValueError):
            runner.result_summary(rows, selected, 2)
        for changed in (rows[:1], [rows[0], dict(rows[1], state='FAILED')],
                        [rows[0], dict(rows[1], byte_parity='PASS')],
                        [rows[0], dict(rows[1], output_sha256='a' * 64)]):
            with self.assertRaises(ValueError):
                runner.result_summary(changed, selected, 2, True)

    def test_no_failed_or_missing_repetition_is_removed_from_the_mean(self):
        measurements = []
        for case in cases():
            for repeat, seconds in [(1, 1), (2, 2), (3, 3)]:
                measurements.append({'case_name': case['name'], 'repetition': repeat, 'state': 'PASS',
                                     'output_sha256': 'same', 'output_bytes': 20, 'byte_parity': 'PASS',
                                     'total_seconds': seconds, 'transactions': 100, 'directory': f'{case["name"]}-{repeat}'})
        self.assertEqual(runner.result_summary(measurements, cases(), 3)['static-c1']['mean_total_seconds'], 2)
        for changed in [measurements[:-1], [dict(row, state='FAILED') if i == 1 else row for i, row in enumerate(measurements)],
                        [dict(row, output_sha256='different') if i == 1 else row for i, row in enumerate(measurements)]]:
            with self.assertRaises(ValueError):
                runner.result_summary(changed, cases(), 3)


class SourceCheckTests(unittest.TestCase):
    def test_transport_retry_repeats_the_complete_check_and_returns_only_complete_proof(self):
        state, saved, error_log = {}, [], io.StringIO()
        complete = {'all_samples': 'checked', 'index': 'checked'}
        failures = [ConnectionResetError(104, 'Connection reset by peer'), http.client.IncompleteRead(b'partial', 100)]
        with patch.object(runner.full, 'source_checks', side_effect=failures + [complete]) as source, \
                patch.object(runner.time, 'sleep') as sleep, redirect_stderr(error_log):
            result = runner.checked_source('base', {'dataset': 1}, state, 'BEFORE',
                                           lambda: saved.append(copy.deepcopy(state)))
        self.assertIs(result, complete)
        self.assertEqual(source.call_count, 3)
        self.assertEqual([call.args for call in source.call_args_list], [('base', {'dataset': 1})] * 3)
        self.assertEqual([call.args for call in sleep.call_args_list], [(0.5,), (1.0,)])
        events = state['source_check_failed_attempts']
        self.assertEqual([event['error_type'] for event in events], ['ConnectionResetError', 'IncompleteRead'])
        self.assertEqual([event['attempt'] for event in events], [1, 2])
        self.assertTrue(all(event['phase'] == 'BEFORE' for event in events))
        self.assertEqual(len(saved), 2)
        self.assertIn('BEFORE: source check attempt 2/3 failed: IncompleteRead', error_log.getvalue())
        self.assertNotIn('source_before', state)

    def test_third_transport_failure_is_fatal_and_retained(self):
        state = {}
        with patch.object(runner.full, 'source_checks', side_effect=http.client.IncompleteRead(b'x', 5)) as source, \
                patch.object(runner.time, 'sleep') as sleep, redirect_stderr(io.StringIO()):
            with self.assertRaises(http.client.IncompleteRead):
                runner.checked_source('base', {}, state, 'AFTER', lambda: None)
        self.assertEqual(source.call_count, 3)
        self.assertEqual(sleep.call_count, 2)
        self.assertEqual(len(state['source_check_failed_attempts']), 3)
        self.assertIsNone(state['source_check_failed_attempts'][-1]['retry_delay_seconds'])
        self.assertNotIn('source_after', state)

    def test_mismatch_http_status_and_other_errors_are_never_retried(self):
        errors = [ValueError('CAR sample hash differs'), HTTPError('https://source', 503, 'unavailable', {}, None),
                  TimeoutError('timeout'), OSError('other transport error')]
        for error in errors:
            with self.subTest(error=type(error).__name__), \
                    patch.object(runner.full, 'source_checks', side_effect=error) as source, \
                    patch.object(runner.time, 'sleep') as sleep:
                state = {}
                with self.assertRaises(type(error)):
                    runner.checked_source('base', {}, state, 'BEFORE', lambda: None)
                self.assertEqual(source.call_count, 1)
                sleep.assert_not_called()
                self.assertEqual(state, {})
                if isinstance(error, HTTPError):
                    error.close()


class ControllerTests(unittest.TestCase):
    def test_all_cases_complete_but_any_failed_case_prevents_an_accepted_summary(self):
        scenarios = [(failed, loopback, skip, False) for failed, loopback, skip in
                     ((False, False, False), (True, False, False), (False, True, False),
                      (False, False, True), (True, False, True), (False, True, True), (True, True, True))]
        scenarios += [(False, False, True, True), (False, True, True, True), (False, False, True, 'low')]
        for fail_middle, loopback, skip, memory in scenarios:
            with self.subTest(fail_middle=fail_middle, loopback=loopback, skip=skip, memory=memory), tempfile.TemporaryDirectory() as folder:
                root = Path(folder)
                dataset, full_plan = dataset_and_plan()
                selected = cases()
                if memory:
                    selected = runner.selected_cases([dict(case, export_mode='memory') for case in selected])
                series_failed = fail_middle or memory == 'low'
                data = b'checked output bytes'
                paths = {name: root / name for name in ['car-bin', 'jetstreamer-bin', 'verifier-bin', 'plan',
                                                       'dataset-manifest', 'cases']}
                for name, path in paths.items():
                    path.write_text(json.dumps({'plan': full_plan, 'dataset-manifest': dataset,
                                                'cases': selected}.get(name, 'binary fixture')))
                result = root / 'results'
                argv = ['run_dynamic.py', '--results', str(result), '--blocks', '2', '--repetitions', '1']
                if skip:
                    argv.append('--skip-output-verification')
                for name, path in paths.items():
                    argv.extend(['--' + name, str(path)])
                base = 'http://127.0.0.1:8123/' if loopback else 'https://files.old-faithful.net/'
                if loopback:
                    argv.extend(['--base', base, '--allow-loopback-http', '--source-server-pid', '123',
                                 '--warm-source-file', '/cache/epoch-1000.car'])
                prefix = runner.prefix_plan(full_plan, dataset, 2)
                executed = []
                order = []

                def process(command, directory, prefix_name, _timeout, _progress):
                    stats = {'exit_code': 0, 'process_seconds': 2.2, 'cpu_seconds': 4,
                             'user_cpu_seconds': 3, 'system_cpu_seconds': 1, 'peak_rss_mib': 100}
                    if prefix_name == 'verify':
                        self.assertFalse(skip, 'unverified mode invoked the native output verifier')
                        (directory / 'verify.stdout.log').write_text(json.dumps(
                            {'output_bytes': len(data), 'blocks': 2, 'transactions': 3, 'votes': 1, 'failed': 1}))
                        return stats
                    case = next(case for case in selected if directory.name.endswith(case['name']))
                    executed.append(case['name'])
                    order.append('reader')
                    value = receipt(case, dataset, prefix)
                    if loopback and case['reader'] == 'car':
                        self.assertIn('--allow-loopback-http', command)
                        value['allow_loopback_http'] = True
                        value['identity'].update(loopback_http=True, url=base + '1000/epoch-1000.car',
                                                 slot_index_url=base + '1000/epoch-1000-slot-ranges.raw')
                    elif loopback:
                        self.assertNotIn('--allow-loopback-http', command)
                        value['http_base'] = value['index_base'] = base
                    value['export_bytes'] = len(data)
                    if fail_middle and case['name'] == 'dynamic-c2':
                        value['pipeline']['jobs_completed'] = 1
                    (directory / 'report.json').write_text(json.dumps(value))
                    (directory / 'transactions.bin').write_bytes(data)
                    return stats

                with ExitStack() as stack:
                    stack.enter_context(patch('sys.argv', argv))
                    stack.enter_context(patch.object(runner.platform, 'system', return_value='Linux'))
                    stack.enter_context(patch.object(runner.signal, 'signal'))
                    stack.enter_context(patch.object(runner.full, 'host_snapshot', return_value={}))
                    samples = iter(range(20))
                    server = stack.enter_context(patch.object(runner, 'source_server_snapshot', side_effect=lambda _pid:
                        {'pid': 123, 'starttime_ticks': 42, 'cpu_seconds': next(samples), 'rss_mib': 3,
                         'peak_rss_mib_lifetime': 4}))
                    stack.enter_context(patch.object(runner, 'source_server_identity',
                        return_value={'files': {'epoch-1000.car': {'stat': {'fixed': True}}}}))
                    def warmed(*_args):
                        order.append('warmup')
                        return {'bytes': dataset['car_bytes'], 'seconds': 10}
                    warmup = stack.enter_context(patch.object(runner, 'warm_source_file', side_effect=warmed))
                    def memory_check(case):
                        order.append('memory-check')
                        return {'source': '/proc/meminfo MemAvailable', 'sampled_at': 1,
                                'available_bytes': 0 if memory == 'low' else 1 << 50,
                                'payload_limit_bytes': case['export_memory_mib'] << 20,
                                'overhead_allowance_bytes': 16 << 30,
                                'required_bytes': (case['export_memory_mib'] << 20) + (16 << 30),
                                'sufficient': memory != 'low'}
                    preflight = stack.enter_context(patch.object(runner, 'export_memory_preflight', side_effect=memory_check))
                    source = stack.enter_context(patch.object(runner.full, 'source_checks', return_value={'unchanged': True}))
                    stack.enter_context(patch.object(runner, 'host_counters', return_value={'network': {}, 'disks': {}}))
                    stack.enter_context(patch.object(runner.full, 'run_process', side_effect=process))
                    if skip:
                        hashed = stack.enter_context(patch.object(runner.full, 'hash_output',
                                                     side_effect=AssertionError('full output hash must be skipped')))
                        compared = stack.enter_context(patch.object(runner.full, 'compare_outputs',
                                                       side_effect=AssertionError('full byte comparison must be skipped')))
                    stack.enter_context(patch.object(runner.shutil, 'disk_usage', return_value=SimpleNamespace(free=1 << 50)))
                    stack.enter_context(redirect_stdout(io.StringIO()))
                    if series_failed:
                        with self.assertRaisesRegex(ValueError, 'every selected repetition'):
                            runner.main()
                    else:
                        runner.main()
                saved = json.loads((result / 'status.json').read_text())
                self.assertEqual(executed, [] if memory == 'low' else [case['name'] for case in selected])
                self.assertEqual(source.call_count, 2)
                self.assertEqual(saved['state'], 'FAILED' if series_failed else 'MEASURED_UNVERIFIED' if skip else 'PASS')
                self.assertEqual(saved['acceptance']['state'], 'FAILED' if series_failed else 'UNVERIFIED' if skip else 'PASS')
                self.assertEqual(len(saved['cases']), 3)
                self.assertEqual(saved['cases'][-1]['state'], 'FAILED' if memory == 'low' else 'MEASURED' if skip else 'PASS')
                self.assertEqual('summary' in saved, not series_failed)
                self.assertEqual(saved['output_verification'], 'SKIPPED' if skip else 'FULL')
                self.assertEqual(preflight.call_count, 3 if memory else 0)
                if memory:
                    for row in saved['cases']:
                        self.assertEqual(row['settings']['export_mode'], 'memory')
                        self.assertEqual(row['settings']['export_memory_mib'], 196608)
                        self.assertEqual(row['export_memory_preflight']['sufficient'], memory != 'low')
                        if memory != 'low':
                            self.assertEqual(row['export_finish_seconds'], 0.5)
                            self.assertEqual(row['export_storage']['mode'], 'memory')
                        else:
                            self.assertIn('insufficient available RAM', row['error'])
                if skip:
                    hashed.assert_not_called()
                    compared.assert_not_called()
                    self.assertNotIn('reference_export', saved)
                    for row in saved['cases']:
                        for key in ('output_sha256', 'byte_parity', 'verification_process', 'verification_command'):
                            self.assertNotIn(key, row)
                        if row['state'] == 'MEASURED':
                            self.assertEqual(row['output_verification'], 'SKIPPED')
                            self.assertEqual(row['output_bytes'], len(data))
                self.assertEqual(warmup.call_count, 3 if loopback else 0)
                if loopback:
                    self.assertEqual(order, (['warmup', 'memory-check', 'reader'] if memory else ['warmup', 'reader']) * 3)
                    self.assertEqual(saved['source_kind'], 'loopback-cache-http')
                    for row in saved['cases']:
                        self.assertEqual(row['cpu_seconds'], 4)
                        self.assertEqual(row['source_server']['cpu_seconds'], 1)
                        self.assertEqual(row['source_server']['reader_and_server_cpu_seconds'], 5)
                else:
                    server.assert_not_called()


if __name__ == '__main__':
    unittest.main()
