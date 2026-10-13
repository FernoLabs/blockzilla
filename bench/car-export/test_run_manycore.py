#!/usr/bin/env python3
"""Check source admission and output rejection paths without network access."""
import copy
import io
import unittest
from unittest.mock import patch

import run_manycore as runner


class Response(io.BytesIO):
    def __init__(self, body, *, status=200, headers=None, url='https://example.test/object'):
        super().__init__(body)
        self.status = status
        self.headers = headers or {}
        self.url = url


class SourceTests(unittest.TestCase):
    def fetch(self, response, **kwargs):
        with patch.object(runner.urllib.request, 'urlopen', return_value=response):
            return runner.fetch_exact('https://example.test/object', size=3, **kwargs)

    def test_accept_full_index_and_exact_range(self):
        full = self.fetch(Response(b'abc'))
        partial = self.fetch(Response(b'abc', status=206, headers={'Content-Range': 'bytes 7-9/20'}),
                             start=7, total=20)
        self.assertEqual(full, partial)

    def test_reject_ignored_wrong_or_encoded_range(self):
        responses = [Response(b'abc'),
                     Response(b'abc', status=206, headers={'Content-Range': 'bytes 0-2/20'}),
                     Response(b'abc', status=206, headers={'Content-Range': 'bytes 7-9/20',
                                                          'Content-Encoding': 'gzip'}),
                     Response(b'abc', status=206, headers={'Content-Range': 'bytes 7-9/20'},
                              url='https://other.test/object')]
        for response in responses:
            with self.subTest(headers=response.headers, url=response.url), self.assertRaises(ValueError):
                self.fetch(response, start=7, total=20)

    def test_reject_short_and_excess_body(self):
        for body in [b'ab', b'abcd']:
            with self.subTest(body=body), self.assertRaises(ValueError):
                self.fetch(Response(body))


class OutputTests(unittest.TestCase):
    def setUp(self):
        self.rows = [[runner.START, runner.TRANSACTIONS]]
        self.receipt = {'valid': True, 'block_transaction_rows': self.rows,
                        'transactions': runner.TRANSACTIONS, 'export_schema': 'car-common-export-v1',
                        'allocator': 'mimalloc', 'export_bytes': runner.OUTPUT_BYTES,
                        'votes': 6533434, 'failed': 341094, 'total_seconds': 10.0,
                        'signature_verification': False}
        self.checked = {'output_bytes': runner.OUTPUT_BYTES, 'output_sha256': runner.OUTPUT_SHA256,
                        'votes': 6533434, 'failed': 341094}

    def validate(self, receipt=None, checked=None, reader='car'):
        runner.validate_report(receipt or self.receipt, checked or self.checked, self.rows, reader, False)

    def test_accept_reference_export(self):
        self.validate()

    def test_reject_equal_counts_with_different_bytes(self):
        checked = dict(self.checked, output_sha256='0' * 64)
        with self.assertRaisesRegex(ValueError, 'previously accepted'):
            self.validate(checked=checked)

    def test_reject_missing_rows_bad_allocator_and_invalid_time(self):
        for field, value in [('block_transaction_rows', []), ('allocator', 'system'),
                             ('valid', False), ('total_seconds', float('inf'))]:
            receipt = dict(self.receipt, **{field: value})
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.validate(receipt=receipt)

    def test_reject_jetstreamer_recovered_error(self):
        receipt = dict(self.receipt, errors=['transport retry'], upstream_version='0.7.0',
                       upstream_commit=runner.JETSTREAMER_COMMIT, reported_error_policy='strict',
                       reported_body_retries=0, reported_body_retry_limit_per_worker_slot=2,
                       reported_error_events=[], reported_error_events_truncated=False,
                       transaction_callbacks=runner.TRANSACTIONS,
                       worker_counters=[{'simple_vote_callbacks': 6533434, 'failed_status_callbacks': 341094}])
        with self.assertRaisesRegex(ValueError, 'reported an error'):
            self.validate(receipt=receipt, reader='jetstreamer')
        receipt['errors'] = []
        self.validate(receipt=receipt, reader='jetstreamer')
        for field, value in [('upstream_commit', 'cffaf3d891b3cbe45a46dd963d6d3571b2aa1a24'),
                             ('upstream_commit', 'unreviewed-revision'),
                             ('reported_error_policy', 'body-transport'), ('reported_body_retries', 1),
                             ('reported_body_retry_limit_per_worker_slot', 3),
                             ('reported_error_events', [{'slot': runner.START}]),
                             ('reported_error_events_truncated', True),
                             ('transaction_callbacks', runner.TRANSACTIONS - 1)]:
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                self.validate(receipt=dict(receipt, **{field: value}), reader='jetstreamer')


class PlanTests(unittest.TestCase):
    def test_reject_duplicate_or_incomplete_canonical_plan(self):
        rows = [[runner.START + index, 0] for index in range(8192)]
        rows[0][1] = runner.TRANSACTIONS
        rows[-1][0] = runner.END - 1
        plan = {'epoch': runner.EPOCH, 'start_slot': runner.START, 'end_slot_exclusive': runner.END,
                'block_transaction_rows': rows}
        self.assertEqual(runner.validate_plan(plan), rows)
        duplicate = copy.deepcopy(plan)
        duplicate['block_transaction_rows'][3][0] = duplicate['block_transaction_rows'][2][0]
        with self.assertRaisesRegex(ValueError, 'slot order'):
            runner.validate_plan(duplicate)
        plan['block_transaction_rows'].pop()
        with self.assertRaisesRegex(ValueError, '8192 canonical'):
            runner.validate_plan(plan)


class ParserTests(unittest.TestCase):
    def test_baseline_and_smoke_preserve_the_original_parser(self):
        self.assertEqual(runner.parser_configuration(16, 12, baseline=True), (1, []))
        self.assertEqual(runner.parser_configuration(16, 12, smoke=True), (1, []))
        self.assertEqual(runner.parser_configuration(1, 12), (1, []))
        self.assertEqual(runner.parser_configuration(8, 12), (8, ['--parser-workers', '8']))
        with self.assertRaisesRegex(ValueError, 'must not exceed'):
            runner.parser_configuration(16, 12)

    def test_parallel_receipt_must_confirm_parser_and_transport(self):
        runner.validate_parser_receipt({}, 1)  # The original probe has no parser field.
        valid = {'parser_workers': 8, 'http_transport_mode': 'sharded-range', 'effective_http_workers': 8}
        runner.validate_parser_receipt(valid, 8)
        runner.validate_parser_receipt(dict(valid, http={'server_error_retries': 1}), 8)
        for receipt in [{}, dict(valid, parser_workers=1), dict(valid, http_transport_mode='ordered-prefetch'),
                        dict(valid, effective_http_workers=16)]:
            with self.subTest(receipt=receipt), self.assertRaises(ValueError):
                runner.validate_parser_receipt(receipt, 8)


class SummaryTests(unittest.TestCase):
    def test_summary_excludes_fast_tuning_result(self):
        cases = []
        for reader in ['car', 'jetstreamer']:
            for label, seconds in [('sweep-128', 1), ('confirm-1', 10), ('confirm-2', 14)]:
                cases.append({'label': label + '-' + reader, 'reader': reader,
                              'total_seconds': seconds, 'state': 'PASS'})
        selected = {reader: {'workers': 128} for reader in ['car', 'jetstreamer']}
        summary = runner.confirmation_summary(cases, selected)
        for result in summary.values():
            self.assertEqual(result['mean_total_seconds'], 12)
            self.assertEqual(len(result['cases']), 2)


if __name__ == '__main__':
    unittest.main()
