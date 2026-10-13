#!/usr/bin/env python3
"""Full-epoch control checks without network access or archive data."""
import copy
from contextlib import ExitStack, redirect_stdout
import hashlib
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import run_full_epoch as runner


def jet_policy_fields(body=False, events=None):
    events = [] if events is None else events
    return {'upstream_commit': runner.JETSTREAMER_COMMIT,
            'reported_error_policy': 'body-transport' if body else 'strict',
            'reported_body_retries': len(events), 'reported_body_retry_limit_per_worker_slot': 2,
            'reported_error_events': events, 'reported_error_events_truncated': False}


def small_dataset():
    dataset = runner.default_dataset()
    dataset.update(epoch=1000, start_slot=432000000, end_slot_exclusive=432432000,
                   blocks=3, transactions=5, canonical_index_sha256='a' * 64,
                   index_sha256='b' * 64, car_bytes=2 << 20,
                   car_samples=[{'start': i * 65536, 'sha256': f'{i:064x}'} for i in range(17)],
                   provenance={'canonical_index': 'independent current index fixture'})
    return dataset


class DatasetTests(unittest.TestCase):
    def test_default_900_and_custom_full_epoch_validation(self):
        original = runner.validate_dataset(runner.default_dataset())
        self.assertEqual((original['epoch'], original['blocks'], original['transactions']),
                         (900, 431858, 476026811))
        dataset = runner.validate_dataset(small_dataset())
        rows = [[dataset['start_slot'], 2], [dataset['start_slot'] + 1, 0],
                [dataset['end_slot_exclusive'] - 1, 3]]
        plan = {'epoch': dataset['epoch'], 'start_slot': dataset['start_slot'],
                'end_slot_exclusive': dataset['end_slot_exclusive'], 'block_transaction_rows': rows,
                'source_index': {'index_sha256': dataset['canonical_index_sha256'],
                                 'accepted_index_sha256': dataset['canonical_index_sha256'],
                                 'index_block_count': 3, 'index_transactions': 5}}
        self.assertEqual(runner.validate_full_plan(plan, dataset), rows)
        plan['source_index']['index_sha256'] = dataset['index_sha256']
        with self.assertRaisesRegex(ValueError, 'canonical index hash'):
            runner.validate_full_plan(plan, dataset)

    def test_reject_incomplete_epoch_bad_hash_or_unbounded_sample(self):
        mutations = [('end_slot_exclusive', 432431999), ('start_slot', 432000001),
                     ('canonical_index_sha256', 'bad'), ('index_bytes', 1), ('blocks', 432001),
                     ('car_samples', small_dataset()['car_samples'][:2]),
                     ('car_samples', [{'start': 0, 'sha256': 'a' * 64}] * 3),
                     ('car_samples', [{'start': i * (2 << 20), 'sha256': 'a' * 64} for i in range(3)])]
        for field, value in mutations:
            with self.subTest(field=field), self.assertRaises(ValueError):
                runner.validate_dataset(dict(small_dataset(), **{field: value}))

    def test_source_checks_use_custom_urls_lengths_and_every_sample(self):
        dataset = runner.validate_dataset(small_dataset())
        base = 'https://example.test/archive/'
        car_url = base + '1000/epoch-1000.car'
        head = io.BytesIO()
        head.status, head.url = 200, car_url
        head.headers = {'Content-Length': str(dataset['car_bytes'])}
        expected = {sample['start']: sample['sha256'] for sample in dataset['car_samples']}

        def fetch(url, **kwargs):
            if url.endswith('.raw'):
                self.assertEqual(url, base + '1000/epoch-1000-slot-ranges.raw')
                self.assertEqual(kwargs, {'size': dataset['index_bytes']})
                return dataset['index_sha256']
            self.assertEqual(url, car_url)
            self.assertEqual(kwargs['total'], dataset['car_bytes'])
            self.assertEqual(kwargs['size'], 65536)
            return expected[kwargs['start']]

        with patch.object(runner.urllib.request, 'urlopen', return_value=head), \
                patch.object(runner, 'fetch_exact', side_effect=fetch) as fetch_mock:
            checked = runner.source_checks(base, dataset)
        self.assertEqual(fetch_mock.call_count, 18)
        self.assertEqual(len(checked['car_samples']), 17)
        self.assertEqual(checked['index_sha256'], dataset['index_sha256'])


class FullPlanTests(unittest.TestCase):
    def plan(self):
        return {'epoch': runner.EPOCH, 'start_slot': 100, 'end_slot_exclusive': 104,
                'block_transaction_rows': [[100, 2], [101, 0], [103, 3]],
                'source_index': {'index_sha256': runner.CANONICAL_INDEX_SHA256,
                                 'accepted_index_sha256': runner.CANONICAL_INDEX_SHA256,
                                 'index_block_count': 3, 'index_transactions': 5}}

    def test_full_plan_requires_all_rows_and_independent_source(self):
        with patch.multiple(runner, START=100, END=104, BLOCKS=3, TRANSACTIONS=5):
            plan = self.plan()
            self.assertEqual(runner.validate_full_plan(plan), plan['block_transaction_rows'])
            for field, wrong in [('index_block_count', 4), ('index_transactions', 6),
                                 ('accepted_index_sha256', '0' * 64)]:
                bad = copy.deepcopy(plan)
                bad['source_index'][field] = wrong
                with self.subTest(field=field), self.assertRaises(ValueError):
                    runner.validate_full_plan(bad)
            bad = copy.deepcopy(plan)
            bad['block_transaction_rows'][1][0] = 100
            with self.assertRaisesRegex(ValueError, 'slot order'):
                runner.validate_full_plan(bad)
            bad = copy.deepcopy(plan)
            bad['block_transaction_rows'].pop()
            with self.assertRaisesRegex(ValueError, 'block count'):
                runner.validate_full_plan(bad)


class ReceiptTests(unittest.TestCase):
    def setUp(self):
        self.rows = [[100, 2], [101, 0], [103, 3]]
        self.receipt = {'valid': True, 'blocks': 3, 'transactions': 5,
                        'block_transaction_rows': self.rows, 'allocator': 'mimalloc',
                        'export_schema': 'car-common-export-v1', 'export_bytes': 1000,
                        'signature_verification': False, 'total_seconds': 10.0,
                        'workers': 32, 'parser_workers': 8, 'http_transport_mode': 'sharded-range',
                        'effective_http_workers': 8, 'metadata_mode': 'Owned', 'reuse_http_buffers': False,
                        'votes': 3, 'failed': 1, 'http': {'incomplete_body_retries': 1}}
        self.checked = {'output_bytes': 1000, 'blocks': 3, 'transactions': 5, 'votes': 3, 'failed': 1}

    def test_accept_validated_transport_retry_and_full_native_check(self):
        counters = runner.validate_full_receipt(self.receipt, self.rows, 'car', 32, 8)
        runner.validate_verified_export(self.checked, self.receipt, counters, self.rows)

    def test_reject_partial_plan_wrong_parser_or_failed_verifier_totals(self):
        for field, wrong in [('valid', False), ('block_transaction_rows', self.rows[:-1]),
                             ('parser_workers', 1), ('allocator', 'system')]:
            with self.subTest(field=field), self.assertRaises(ValueError):
                runner.validate_full_receipt(dict(self.receipt, **{field: wrong}), self.rows, 'car', 32, 8)
        for field, wrong in [('output_bytes', 999), ('transactions', 4), ('votes', 2), ('failed', 0)]:
            with self.subTest(field=field), self.assertRaises(ValueError):
                runner.validate_verified_export(dict(self.checked, **{field: wrong}), self.receipt,
                                                {'votes': 3, 'failed': 1}, self.rows)

    def test_jetstreamer_rejects_reported_errors_and_wrong_callback_count(self):
        receipt = dict(self.receipt, processing_workers=64, errors=[], upstream_version='0.7.0',
                       transaction_callbacks=5, start_slot=100, end_slot_exclusive=104,
                       worker_counters=[{'simple_vote_callbacks': 3, 'failed_status_callbacks': 1}])
        receipt.update(jet_policy_fields())
        self.assertEqual(runner.validate_full_receipt(receipt, self.rows, 'jetstreamer', 64, 8),
                         {'votes': 3, 'failed': 1})
        for field, wrong in [('errors', ['reported retry']), ('transaction_callbacks', 4)]:
            with self.subTest(field=field), self.assertRaises(ValueError):
                runner.validate_full_receipt(dict(receipt, **{field: wrong}), self.rows, 'jetstreamer', 64, 8)

    def test_body_retry_policy_is_explicit_bounded_and_does_not_skip_count_checks(self):
        event = {'worker': 0, 'slot': 101, 'attempt': 1, 'elapsed_seconds': 1.0,
                 'error_message': runner.JETSTREAMER_BODY_ERROR}
        receipt = dict(self.receipt, processing_workers=64, errors=[], upstream_version='0.7.0',
                       transaction_callbacks=5, start_slot=100, end_slot_exclusive=104,
                       worker_counters=[{'simple_vote_callbacks': 3, 'failed_status_callbacks': 1}])
        receipt.update(jet_policy_fields(True, [event]))
        with self.assertRaisesRegex(ValueError, 'policy differs'):
            runner.validate_full_receipt(receipt, self.rows, 'jetstreamer', 64, 8)
        self.assertEqual(runner.validate_full_receipt(receipt, self.rows, 'jetstreamer', 64, 8, True),
                         {'votes': 3, 'failed': 1})
        for field, value in [('attempt', 3), ('worker', 64), ('slot', 104),
                             ('error_message', 'Unknown CID version'),
                             ('error_message', 'Error reading header: Header size too large'),
                             ('error_message', runner.JETSTREAMER_HEADER_BODY_ERROR + ': changed'),
                             ('elapsed_seconds', float('nan'))]:
            bad = copy.deepcopy(receipt)
            bad['reported_error_events'][0][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                runner.validate_full_receipt(bad, self.rows, 'jetstreamer', 64, 8, True)
        for field, value in [('reported_body_retries', 2), ('reported_error_events_truncated', True),
                             ('reported_body_retry_limit_per_worker_slot', 3),
                             ('upstream_commit', 'cffaf3d891b3cbe45a46dd963d6d3571b2aa1a24'),
                             ('transaction_callbacks', 4), ('block_transaction_rows', self.rows[:-1]),
                             ('errors', ['fatal error'])]:
            with self.subTest(field=field), self.assertRaises(ValueError):
                runner.validate_full_receipt(dict(receipt, **{field: value}), self.rows, 'jetstreamer', 64, 8, True)

    def test_body_retry_sample_order_and_limit(self):
        events = [{'worker': 0, 'slot': 101, 'attempt': attempt, 'elapsed_seconds': attempt,
                   'error_message': runner.JETSTREAMER_BODY_ERROR} for attempt in (1, 2)]
        receipt = dict(jet_policy_fields(True, events), errors=[], upstream_version='0.7.0',
                       start_slot=100, end_slot_exclusive=104, total_seconds=10)
        runner.validate_jetstreamer_policy(receipt, 1, True)
        receipt['reported_error_events'] = events[::-1]
        with self.assertRaisesRegex(ValueError, 'out of order'):
            runner.validate_jetstreamer_policy(receipt, 1, True)

    def test_header_and_block_body_retries_share_the_same_worker_slot_budget(self):
        events = [{'worker': 0, 'slot': 101, 'attempt': attempt, 'elapsed_seconds': attempt,
                   'error_message': message} for attempt, message in
                  [(1, runner.JETSTREAMER_HEADER_BODY_ERROR), (2, runner.JETSTREAMER_BODY_ERROR)]]
        receipt = dict(jet_policy_fields(True, events), errors=[], upstream_version='0.7.0',
                       start_slot=100, end_slot_exclusive=104, total_seconds=10)
        runner.validate_jetstreamer_policy(receipt, 1, True)
        events.append(dict(events[0], attempt=3, elapsed_seconds=3))
        receipt.update(jet_policy_fields(True, events))
        with self.assertRaisesRegex(ValueError, 'exceeds'):
            runner.validate_jetstreamer_policy(receipt, 1, True)


class ByteChecksTests(unittest.TestCase):
    def test_hash_and_exact_comparison_report_progress_and_reject_changes(self):
        updates = []
        with tempfile.TemporaryDirectory() as directory:
            a, b = Path(directory) / 'a', Path(directory) / 'b'
            data = b'full-epoch-fixture' * 100
            a.write_bytes(data)
            b.write_bytes(data)
            sha, size = runner.hash_output(a, updates.append, 60)
            self.assertEqual((sha, size), (hashlib.sha256(data).hexdigest(), len(data)))
            self.assertEqual(runner.compare_outputs(a, b, updates.append, 60), len(data))
            self.assertTrue(updates)
            b.write_bytes(data[:-1] + b'x')
            with self.assertRaisesRegex(ValueError, 'export bytes differ'):
                runner.compare_outputs(a, b, updates.append, 60)
            b.write_bytes(data + b'x')
            with self.assertRaisesRegex(ValueError, 'export bytes differ'):
                runner.compare_outputs(a, b, updates.append, 60)


class IndependentReaderTests(unittest.TestCase):
    def test_first_reader_failure_does_not_stop_opt_in_second_reader(self):
        started = []
        cases = []

        def execute(reader):
            started.append(reader)
            if reader == 'car':
                raise ValueError('CAR response body failed')
            cases.append({'reader': reader, 'state': 'PASS'})

        def failed(reader, error):
            cases.append({'reader': reader, 'state': 'FAILED', 'error': str(error)})

        failures = runner.run_reader_series(['car', 'jetstreamer'], execute, failed, True)
        self.assertEqual(started, ['car', 'jetstreamer'])
        self.assertEqual(failures, ['car'])
        self.assertEqual([case['state'] for case in cases], ['FAILED', 'PASS'])
        with self.assertRaisesRegex(ValueError, 'both complete readers must pass'):
            runner.validate_comparison_candidates(cases)

    def test_default_stops_after_failure_and_interrupt_always_stops(self):
        for continued, error in [(False, ValueError('failed')), (True, InterruptedError('stop'))]:
            started, failures = [], []

            def execute(reader):
                started.append(reader)
                raise error

            with self.subTest(continued=continued, error=error), self.assertRaises(type(error)):
                runner.run_reader_series(['car', 'jetstreamer'], execute,
                                         lambda reader, error: failures.append(reader), continued)
            self.assertEqual(started, ['car'])
            self.assertEqual(failures, [] if isinstance(error, InterruptedError) else ['car'])

    def test_both_successful_readers_require_matching_hashes_and_counts(self):
        cases = []

        def execute(reader):
            cases.append({'reader': reader, 'state': 'PASS', 'output_bytes': 1000,
                          'output_sha256': 'same', 'votes': 3, 'failed': 1, 'blocks': 3, 'transactions': 5})

        failures = runner.run_reader_series(['jetstreamer', 'car'], execute,
                                           lambda *_: self.fail('unexpected reader error'), True)
        self.assertEqual(failures, [])
        self.assertEqual(set(runner.validate_comparison_candidates(cases)), {'car', 'jetstreamer'})
        cases[1]['output_sha256'] = 'different'
        with self.assertRaisesRegex(ValueError, 'output_sha256 differs'):
            runner.validate_comparison_candidates(cases)


class ControllerTests(unittest.TestCase):
    def test_saved_result_needs_two_verified_readers_and_complete_byte_parity(self):
        for fail_first, custom in [(True, False), (False, False), (False, True)]:
            with self.subTest(fail_first=fail_first, custom=custom), tempfile.TemporaryDirectory() as folder:
                root = Path(folder)
                binaries = {name: root / name for name in ['car-bin', 'jetstreamer-bin', 'verifier-bin']}
                for path in binaries.values():
                    path.write_bytes(b'test executable')
                plan = root / 'plan.json'
                plan.write_text(json.dumps({'source_index': {'test': True}}))
                results = root / 'results'
                dataset = small_dataset() if custom else runner.default_dataset()
                epoch, start, end = dataset['epoch'], dataset['start_slot'], dataset['end_slot_exclusive']
                rows = [[start, 1]]
                executed = []

                def run_process(command, directory, prefix, _timeout, _progress):
                    reader = directory.name
                    usage = {'exit_code': 0, 'process_seconds': 1, 'cpu_seconds': 1,
                             'user_cpu_seconds': 1, 'system_cpu_seconds': 0, 'peak_rss_mib': 1}
                    if prefix == 'verify':
                        checked = {'blocks': 1, 'transactions': 1, 'votes': 1, 'failed': 0,
                                   'output_bytes': len(b'checked export')}
                        (directory / 'verify.stdout.log').write_text(json.dumps(checked))
                        return usage
                    executed.append(reader)
                    if reader == 'car':
                        self.assertEqual(command[command.index('--url') + 1],
                                         f'https://files.old-faithful.net/{epoch}/epoch-{epoch}.car')
                    else:
                        for flag, expected in [('--epoch', epoch), ('--start-slot', start),
                                               ('--end-slot-exclusive', end)]:
                            self.assertEqual(command[command.index(flag) + 1], str(expected))
                        self.assertEqual(command[command.index('--reported-error-policy') + 1],
                                         'body-transport' if custom else 'strict')
                    if reader == 'car' and fail_first:
                        return dict(usage, exit_code=1)
                    receipt = {'valid': True, 'epoch': epoch, 'blocks': 1, 'transactions': 1,
                               'block_transaction_rows': rows, 'allocator': 'mimalloc',
                               'export_schema': 'car-common-export-v1', 'export_bytes': len(b'checked export'),
                               'signature_verification': False, 'total_seconds': 1, 'scan_seconds': .8,
                               'workers': 8, 'processing_workers': 8, 'metadata_mode': 'Owned',
                               'reuse_http_buffers': False, 'votes': 1, 'failed': 0,
                               'http_workers': 16, 'http_window_chunks': 16, 'http_chunk_bytes': 32 << 20,
                               'identity': {'url': f'https://files.old-faithful.net/{epoch}/epoch-{epoch}.car',
                                            'bytes': dataset['car_bytes']},
                               'errors': [], 'upstream_version': '0.7.0',
                               'transaction_callbacks': 1,
                               'worker_counters': [{'simple_vote_callbacks': 1, 'failed_status_callbacks': 0}],
                               'http_base': 'https://files.old-faithful.net/',
                               'index_base': 'https://files.old-faithful.net/',
                               'start_slot': start, 'end_slot_exclusive': end}
                    receipt.update(jet_policy_fields(custom))
                    (directory / 'report.json').write_text(json.dumps(receipt))
                    (directory / 'transactions.bin').write_bytes(b'checked export')
                    return usage

                argv = ['run_full_epoch.py', '--plan', str(plan), '--results', str(results),
                        '--car-workers', '8', '--jetstreamer-workers', '8', '--continue-after-reader-failure']
                for name, path in binaries.items():
                    argv.extend(['--' + name, str(path)])
                if custom:
                    manifest = root / 'dataset.json'
                    manifest.write_text(json.dumps(dataset))
                    argv.extend(['--dataset-manifest', str(manifest)])
                    argv.append('--jetstreamer-body-retries')
                with ExitStack() as stack:
                    stack.enter_context(patch('sys.argv', argv))
                    stack.enter_context(patch.object(runner.platform, 'system', return_value='Linux'))
                    stack.enter_context(patch.object(runner.signal, 'signal'))
                    stack.enter_context(patch.object(runner, 'host_snapshot', return_value={}))
                    stack.enter_context(patch.object(runner, 'source_checks', return_value={'same': True}))
                    stack.enter_context(patch.object(runner, 'validate_full_plan', return_value=rows))
                    stack.enter_context(patch.object(runner, 'run_process', side_effect=run_process))
                    stack.enter_context(patch.object(runner.shutil, 'disk_usage', return_value=SimpleNamespace(free=1 << 50)))
                    stack.enter_context(redirect_stdout(io.StringIO()))
                    if fail_first:
                        with self.assertRaisesRegex(RuntimeError, 'reader failures: car'):
                            runner.main()
                    else:
                        runner.main()
                saved = json.loads((results / 'status.json').read_text())
                self.assertEqual(executed, ['car', 'jetstreamer'])
                self.assertEqual(saved['state'], 'FAILED' if fail_first else 'PASS')
                self.assertEqual(saved['comparison']['state'], 'NOT_RUN' if fail_first else 'PASS')
                self.assertEqual(saved['dataset_manifest'], dataset)
                self.assertEqual(saved['dataset_manifest_sha256'], runner.digest(results / 'dataset-manifest.json'))
                self.assertEqual(saved['plan']['epoch'], epoch)
                jet_case = next(row for row in saved['cases'] if row['reader'] == 'jetstreamer')
                self.assertEqual(jet_case['reported_error_policy'], 'body-transport' if custom else 'strict')
                if fail_first:
                    self.assertNotIn('summary', saved)
                    self.assertEqual([row['state'] for row in saved['cases']], ['FAILED', 'PASS'])
                    self.assertEqual(saved['cases'][1]['byte_parity'], 'NOT_COMPARED')
                else:
                    self.assertEqual(set(saved['summary']), {'car', 'jetstreamer'})
                    self.assertTrue(all(row['byte_parity'] == 'PASS' for row in saved['cases']))


if __name__ == '__main__':
    unittest.main()
