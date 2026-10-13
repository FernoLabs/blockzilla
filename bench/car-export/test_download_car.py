#!/usr/bin/env python3
"""Real loopback HTTP and durable-resume tests; no external source access."""
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from email.utils import formatdate
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
import os
from pathlib import Path
import tempfile
import threading
import time
import unittest
from urllib.error import HTTPError

import download_car as download


CAR = bytes(range(256)) * 768
INDEX = (b'index-bytes-' * 432000)[:5184000]
CAR_NAME = 'epoch-1000.car'
INDEX_NAME = 'epoch-1000-slot-ranges.raw'


@contextmanager
def source(mode='normal'):
    records, lock = [], threading.Lock()
    counts = {'car_get': 0, 'car_head': 0}

    class Handler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'

        def log_message(self, *_args):
            pass

        def do_HEAD(self):
            self.respond(True)

        def do_GET(self):
            self.respond(False)

        def respond(self, head):
            name = self.path.rsplit('/', 1)[-1]
            body = {CAR_NAME: CAR, INDEX_NAME: INDEX}.get(name)
            if body is None:
                self.send_error(404)
                return
            with lock:
                if name == CAR_NAME:
                    key = 'car_head' if head else 'car_get'
                    counts[key] += 1
                    ordinal = counts[key]
                else:
                    ordinal = 1
                requested = self.headers.get('Range')
                records.append((self.command, name, requested, time.monotonic()))
            if not head and name == CAR_NAME and mode == '429_once' and ordinal == 1:
                self.send_response(429)
                self.send_header('Content-Length', '0')
                self.send_header('Retry-After', '1')
                self.send_header('Connection', 'close')
                self.end_headers()
                return
            if not head and name == CAR_NAME and mode == 'redirect':
                self.send_response(302)
                self.send_header('Location', f'/{1000}/{INDEX_NAME}')
                self.send_header('Content-Length', '0')
                self.send_header('Connection', 'close')
                self.end_headers()
                return
            start, end = (0, len(body))
            if requested:
                lo, hi = requested.removeprefix('bytes=').split('-')
                start, end = int(lo), int(hi) + 1
            wrong = name == CAR_NAME and not head
            code = 200 if (head or (wrong and mode == 'status200')
                           or (mode == 'whole_range200' and start == 0 and end == len(body))) else 206
            self.send_response(code)
            self.send_header('Content-Length', str(end - start + (1 if wrong and mode == 'bad_length' else 0)))
            if not head:
                range_start = start + (1 if wrong and mode == 'bad_range' else 0)
                self.send_header('Content-Range', f'bytes {range_start}-{end - 1}/{len(body)}')
            if wrong and mode == 'encoded':
                self.send_header('Content-Encoding', 'gzip')
            etag = '"other"' if mode == 'changed_etag' and head and name == CAR_NAME and ordinal > 1 else '"fixture"'
            self.send_header('ETag', etag)
            self.send_header('Connection', 'close')
            self.end_headers()
            if not head:
                result = body[start:end]
                if wrong and mode == 'truncate_once' and ordinal == 1:
                    result = result[:12345]
                try:
                    self.wfile.write(result)
                    self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError):
                    pass
            self.close_connection = True

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f'http://127.0.0.1:{server.server_port}/', records
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def dataset(path, bad_index=False, bad_sample=False):
    value = {'epoch': 1000, 'start_slot': 432000000, 'end_slot_exclusive': 432432000,
             'blocks': 3, 'transactions': 3, 'canonical_index_sha256': 'a' * 64,
             'car_bytes': len(CAR), 'index_bytes': len(INDEX),
             'index_sha256': 'b' * 64 if bad_index else hashlib.sha256(INDEX).hexdigest(),
             'source_sample_bytes': 65536,
             'car_samples': [{'start': start, 'sha256': hashlib.sha256(CAR[start:start + 65536]).hexdigest()}
                             for start in (0, 65536, 131072)]}
    if bad_sample:
        value['car_samples'][1]['sha256'] = 'b' * 64
    path.write_text(json.dumps(value))
    return path


def worker(root, base, attempts=3, threads=2, **kwargs):
    return download.Downloader(root / 'dataset.json', root / 'cache', base=base, threads=threads,
                               job_bytes=65536, checkpoint_bytes=8192, timeout=3, max_attempts=attempts,
                               max_seconds=60, operator_trusted=True, allow_loopback=True, **kwargs)


class DownloadTests(unittest.TestCase):
    def quiet(self):
        return redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO())

    def test_full_cache_has_exact_files_hashes_read_only_identity_and_no_network_on_reopen(self):
        with tempfile.TemporaryDirectory() as folder, source() as (base, records):
            root = Path(folder)
            dataset(root / 'dataset.json')
            stdout, stderr = self.quiet()
            with stdout, stderr:
                manifest = worker(root, base).run()
                count = len(records)
                self.assertEqual(worker(root, base).run(), manifest)
                self.assertEqual(len(records), count)
            self.assertEqual(manifest['state'], 'COMPLETE')
            self.assertEqual(manifest['schema'], 'zilla-local-car-cache-v1')
            for name, expected in [(CAR_NAME, CAR), (INDEX_NAME, INDEX)]:
                path = root / 'cache' / name
                self.assertEqual(path.read_bytes(), expected)
                entry = manifest['files'][name]
                self.assertEqual(entry['sha256'], hashlib.sha256(expected).hexdigest())
                self.assertEqual(entry['bytes'], len(expected))
                self.assertEqual(entry['stat'], download.file_stat(path))
                self.assertEqual(path.stat().st_mode & 0o222, 0)
            self.assertEqual(len(manifest['validation']['car_samples']), 3)
            state = json.loads((root / 'cache/download-state.json').read_text())
            jobs = state['objects'][CAR_NAME]['jobs']
            self.assertEqual([(j['start'], j['end']) for j in jobs], [(0, 65536), (65536, 131072), (131072, 196608)])
            self.assertTrue(all(j['completed_bytes'] == 65536 for j in jobs))

    def test_real_truncated_body_retries_only_the_unread_span(self):
        with tempfile.TemporaryDirectory() as folder, source('truncate_once') as (base, records):
            root = Path(folder)
            dataset(root / 'dataset.json')
            stdout, stderr = self.quiet()
            with stdout, stderr:
                worker(root, base, threads=1).run()
            ranges = [row[2] for row in records if row[0] == 'GET' and row[1] == CAR_NAME]
            self.assertEqual(ranges[:2], ['bytes=0-65535', 'bytes=12345-65535'])
            self.assertEqual((root / 'cache' / CAR_NAME).read_bytes(), CAR)

    def test_failed_process_retains_partial_sha_and_next_process_resumes_without_overwrite(self):
        with tempfile.TemporaryDirectory() as folder, source('truncate_once') as (base, records):
            root = Path(folder)
            dataset(root / 'dataset.json')
            stdout, stderr = self.quiet()
            with stdout, stderr:
                with self.assertRaises(download.DownloadStopped):
                    worker(root, base, attempts=1, threads=1).run()
                state = json.loads((root / 'cache/download-state.json').read_text())
                first = state['objects'][CAR_NAME]['jobs'][0]
                self.assertEqual(first['completed_bytes'], 12345)
                self.assertEqual(first['sha256'], hashlib.sha256(CAR[:12345]).hexdigest())
                self.assertEqual(first['attempts'], 1)
                self.assertFalse((root / 'cache/manifest.json').exists())
                before = len([r for r in records if r[0] == 'GET' and r[1] == CAR_NAME])
                with self.assertRaisesRegex(download.InvalidSource, 'exhausted saved attempts'):
                    worker(root, base, attempts=1, threads=1).run()
                self.assertEqual(len([r for r in records if r[0] == 'GET' and r[1] == CAR_NAME]), before)
                manifest = worker(root, base, attempts=2, threads=1).run()
            ranges = [row[2] for row in records if row[0] == 'GET' and row[1] == CAR_NAME]
            self.assertEqual(ranges[:2], ['bytes=0-65535', 'bytes=12345-65535'])
            self.assertEqual(manifest['files'][CAR_NAME]['sha256'], hashlib.sha256(CAR).hexdigest())

    def test_corrupt_saved_prefix_is_rejected_before_it_can_be_trusted(self):
        with tempfile.TemporaryDirectory() as folder, source('truncate_once') as (base, _records):
            root = Path(folder)
            dataset(root / 'dataset.json')
            stdout, stderr = self.quiet()
            with stdout, stderr:
                with self.assertRaises(download.DownloadStopped):
                    worker(root, base, attempts=1, threads=1).run()
                with (root / 'cache' / CAR_NAME).open('r+b') as stream:
                    stream.write(b'corrupted')
                with self.assertRaisesRegex(download.InvalidSource, 'partial-job SHA-256 differs'):
                    worker(root, base, attempts=2, threads=1).run()
            self.assertFalse((root / 'cache/manifest.json').exists())

    def test_protocol_errors_and_redirects_never_become_complete_or_retryable(self):
        for mode in ('status200', 'bad_range', 'bad_length', 'encoded', 'redirect'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as folder, source(mode) as (base, records):
                root = Path(folder)
                dataset(root / 'dataset.json')
                stdout, stderr = self.quiet()
                with stdout, stderr, self.assertRaises((download.InvalidSource, HTTPError)):
                    worker(root, base, threads=1).run()
                self.assertFalse((root / 'cache/manifest.json').exists())
                requested = [r[2] for r in records if r[0] == 'GET' and r[1] == CAR_NAME]
                self.assertEqual(requested.count('bytes=0-65535'), 1)
                self.assertEqual(sum(r[0] == 'GET' and r[1] == INDEX_NAME for r in records), 2)

    def test_complete_index_is_fetched_as_two_strict_partial_ranges(self):
        with tempfile.TemporaryDirectory() as folder, source('whole_range200') as (base, records):
            root = Path(folder)
            dataset(root / 'dataset.json')
            stdout, stderr = self.quiet()
            with stdout, stderr:
                manifest = worker(root, base).run()
            ranges = [row[2] for row in records if row[0] == 'GET' and row[1] == INDEX_NAME]
            self.assertEqual(ranges, ['bytes=0-2591999', 'bytes=2592000-5183999'])
            self.assertEqual(manifest['files'][INDEX_NAME]['sha256'], hashlib.sha256(INDEX).hexdigest())

    def test_http429_honors_retry_after_before_next_range_attempt(self):
        with tempfile.TemporaryDirectory() as folder, source('429_once') as (base, records):
            root = Path(folder)
            dataset(root / 'dataset.json')
            stdout, stderr = self.quiet()
            with stdout, stderr:
                worker(root, base, threads=1).run()
            car_requests = [r for r in records if r[0] == 'GET' and r[1] == CAR_NAME]
            self.assertEqual(car_requests[0][2], car_requests[1][2])
            self.assertGreaterEqual(car_requests[1][3] - car_requests[0][3], 0.95)
            events = [json.loads(line) for line in (root / 'cache/events.jsonl').read_text().splitlines()]
            self.assertEqual(events[0]['kind'], 'global-http-429-cooldown')
            self.assertEqual(events[0]['cooldown_seconds'], 1)

    def test_index_sample_and_final_source_identity_mismatches_prevent_publication(self):
        for mode, bad_index, bad_sample in [('normal', True, False), ('normal', False, True), ('changed_etag', False, False)]:
            with self.subTest(mode=mode, bad_index=bad_index, bad_sample=bad_sample), \
                    tempfile.TemporaryDirectory() as folder, source(mode) as (base, records):
                root = Path(folder)
                dataset(root / 'dataset.json', bad_index, bad_sample)
                stdout, stderr = self.quiet()
                with stdout, stderr, self.assertRaises(download.InvalidSource):
                    worker(root, base).run()
                self.assertFalse((root / 'cache/manifest.json').exists())
                if bad_index:
                    self.assertFalse(any(r[0] == 'GET' and r[1] == CAR_NAME for r in records))

    def test_completed_cache_stat_change_is_rejected_without_network(self):
        with tempfile.TemporaryDirectory() as folder, source() as (base, records):
            root = Path(folder)
            dataset(root / 'dataset.json')
            stdout, stderr = self.quiet()
            with stdout, stderr:
                worker(root, base).run()
                count = len(records)
                os.chmod(root / 'cache' / CAR_NAME, 0o600)
                with self.assertRaises(download.InvalidSource):
                    worker(root, base).run()
            self.assertEqual(len(records), count)


class CooldownTests(unittest.TestCase):
    def test_retry_after_accepts_seconds_dates_and_has_respectful_fallback(self):
        self.assertEqual(download.retry_after('7', now=100), 7)
        self.assertEqual(download.retry_after(formatdate(130, usegmt=True), now=100), 30)
        self.assertEqual(download.retry_after(formatdate(90, usegmt=True), now=100), 0)
        for invalid in (None, '', '-1', 'junk', '0.5'):
            self.assertEqual(download.retry_after(invalid, now=100), 60)

    def test_one_cooldown_blocks_all_workers_and_stop_interrupts_it(self):
        stop = threading.Event()
        gate = download.Cooldown(stop, time.monotonic() + 10)
        gate.defer(0.1)
        completed = []
        started = time.monotonic()
        threads = [threading.Thread(target=lambda: (gate.wait(), completed.append(time.monotonic()))) for _ in range(3)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(len(completed), 3)
        self.assertGreaterEqual(min(completed) - started, 0.08)
        gate.defer(60)
        stop.set()
        with self.assertRaises(download.DownloadStopped):
            gate.wait()


if __name__ == '__main__':
    unittest.main()
