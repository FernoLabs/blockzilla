#!/usr/bin/env python3
"""Exercise the verified local archive server through actual loopback HTTP."""
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
import errno
import hashlib
import http.client
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

import serve_cache as server


CAR = bytes(range(256)) * 9000
INDEX = b'raw-index-fixture\x00\xff' * 37


def make_cache(path):
    document = {'schema': server.MANIFEST_SCHEMA, 'state': 'COMPLETE', 'epoch': 1000, 'files': {}}
    for name, body in zip(server.NAMES, (CAR, INDEX)):
        target = path / name
        target.write_bytes(body)
        target.chmod(0o444)
        document['files'][name] = {'bytes': len(body), 'sha256': hashlib.sha256(body).hexdigest(),
                                   'stat': server.file_identity(target.stat())}
    (path / 'manifest.json').write_text(json.dumps(document))
    return document


@contextmanager
def running(path):
    cache = server.VerifiedCache(path)
    try:
        instance = server.ArchiveServer(('127.0.0.1', 0), cache)
    except BaseException:
        cache.close()
        raise
    thread = threading.Thread(target=instance.serve_forever, kwargs={'poll_interval': 0.01})
    thread.start()
    try:
        yield instance
    finally:
        instance.shutdown()
        thread.join(timeout=5)
        instance.server_close()
        cache.close()


def request(instance, path='/1000/epoch-1000.car', method='GET', headers=None):
    connection = http.client.HTTPConnection('127.0.0.1', instance.server_port, timeout=5)
    try:
        connection.request(method, path, headers=headers or {})
        response = connection.getresponse()
        return response.status, dict(response.getheaders()), response.read()
    finally:
        connection.close()


class ServerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name)
        self.manifest = make_cache(self.path)

    def tearDown(self):
        self.temporary.cleanup()

    def test_head_full_and_partial_responses_have_stable_strong_identity(self):
        with running(self.path) as instance:
            status, head, body = request(instance, method='HEAD')
            self.assertEqual((status, body), (200, b''))
            self.assertEqual(int(head['Content-Length']), len(CAR))
            expected_etag = '"sha256-' + hashlib.sha256(CAR).hexdigest() + '"'
            self.assertEqual(head['ETag'], expected_etag)
            for headers, expected_status, expected_body, content_range in [
                    ({}, 200, CAR, None),
                    ({'Range': 'bytes=7-1030'}, 206, CAR[7:1031], f'bytes 7-1030/{len(CAR)}'),
                    ({'Range': 'bytes=2303990-'}, 206, CAR[2303990:], f'bytes 2303990-{len(CAR)-1}/{len(CAR)}'),
                    ({'Range': 'bytes=-11'}, 206, CAR[-11:], f'bytes {len(CAR)-11}-{len(CAR)-1}/{len(CAR)}'),
                    ({'Range': f'bytes={len(CAR)-3}-{len(CAR)+10}'}, 206, CAR[-3:],
                     f'bytes {len(CAR)-3}-{len(CAR)-1}/{len(CAR)}'),
                    ({'Range': f'bytes=0-{len(CAR)-1}'}, 206, CAR, f'bytes 0-{len(CAR)-1}/{len(CAR)}')]:
                with self.subTest(headers=headers):
                    status, response, body = request(instance, headers=dict(headers, **{'Accept-Encoding': 'gzip'}))
                    self.assertEqual((status, body), (expected_status, expected_body))
                    self.assertEqual(int(response['Content-Length']), len(expected_body))
                    self.assertEqual(response['ETag'], expected_etag)
                    self.assertEqual(response.get('Content-Range'), content_range)
                    self.assertEqual(response['Accept-Ranges'], 'bytes')
                    self.assertNotIn('Content-Encoding', response)
                    self.assertNotIn('Location', response)
            self.assertEqual(request(instance, '/1000/epoch-1000-slot-ranges.raw')[2], INDEX)

    def test_if_match_guards_every_response_and_if_range_cannot_use_wrong_identity(self):
        with running(self.path) as instance:
            etag = request(instance, method='HEAD')[1]['ETag']
            for method in ('HEAD', 'GET'):
                for value in ('"wrong"', 'W/' + etag):
                    status, headers, body = request(instance, method=method,
                                                    headers={'If-Match': value, 'Range': 'bytes=0-2'})
                    self.assertEqual((status, body), (412, b''))
                    self.assertEqual(headers['ETag'], etag)
            for value in (etag, '*', '"other", ' + etag):
                self.assertEqual(request(instance, headers={'If-Match': value, 'Range': 'bytes=0-2'})[2], CAR[:3])
            self.assertEqual(request(instance, headers={'If-Range': '"wrong"', 'Range': 'bytes=0-2'})[0:3:2], (200, CAR))

    def test_invalid_ranges_paths_and_methods_do_not_expose_files(self):
        (self.path / 'secret.txt').write_text('not served')
        with running(self.path) as instance:
            for value in ('bytes=0-1,4-5', 'bytes=9-3', f'bytes={len(CAR)}-', 'bytes=-0',
                          'items=0-5', 'bytes=-', '', 'bytes=1-' + '9' * 5000):
                with self.subTest(range=value[:40]):
                    status, headers, body = request(instance, headers={'Range': value})
                    self.assertEqual((status, body), (416, b''))
                    self.assertEqual(headers['Content-Range'], f'bytes */{len(CAR)}')
            for path in ('/', '/1000/', '/secret.txt', '/1000/../secret.txt', '/1000/%2e%2e/secret.txt',
                         '/1000/epoch-1000.car?x=1', '/1000/epoch-1000.car/extra', '/1000/epoch-1000.car%00'):
                self.assertEqual(request(instance, path)[0], 404, path)
            self.assertEqual(request(instance, method='POST')[0], 405)
            self.assertEqual(request(instance, headers={'Content-Length': '1'})[0], 400)

    def test_keepalive_and_range_head_have_no_unexpected_body(self):
        with running(self.path) as instance:
            connection = http.client.HTTPConnection('127.0.0.1', instance.server_port, timeout=5)
            try:
                connection.request('HEAD', '/1000/epoch-1000.car', headers={'Range': 'bytes=5-9'})
                response = connection.getresponse()
                self.assertEqual((response.status, response.getheader('Content-Length'), response.read()), (206, '5', b''))
                connection.request('GET', '/1000/epoch-1000.car', headers={'Range': 'bytes=5-9'})
                response = connection.getresponse()
                self.assertEqual((response.status, response.read()), (206, CAR[5:10]))
            finally:
                connection.close()
            snapshot = instance.snapshot()
            self.assertEqual(snapshot['counters']['requests'], 2)
            self.assertEqual(snapshot['counters']['bytes_sent'], 5)
            self.assertEqual(snapshot['files'][server.NAMES[0]]['sha256'], hashlib.sha256(CAR).hexdigest())

    def test_fallback_reads_are_bounded_and_bytes_are_exact(self):
        calls = []
        original_pread = os.pread
        def read(fd, length, offset):
            calls.append((length, offset))
            return original_pread(fd, length, offset)
        with running(self.path) as instance, patch.object(server.os, 'sendfile', side_effect=OSError(errno.ENOSYS, 'test fallback')), \
                patch.object(server.os, 'pread', side_effect=read):
            self.assertEqual(request(instance)[2], CAR)
            self.assertGreater(len(calls), 1)
            self.assertTrue(all(length <= server.FALLBACK_CHUNK for length, _ in calls))
            self.assertEqual(sum(length for length, _ in calls), len(CAR))

    def test_concurrent_ranges_use_independent_file_offsets(self):
        with running(self.path) as instance:
            def read(index):
                start = index * 12347
                end = start + 65535
                result = request(instance, headers={'Range': f'bytes={start}-{end}'})
                self.assertEqual((result[0], result[2]), (206, CAR[start:end + 1]))
            with ThreadPoolExecutor(max_workers=16) as pool:
                list(pool.map(read, range(48)))
            deadline = time.monotonic() + 2
            while instance.snapshot()['counters']['active_requests'] and time.monotonic() < deadline:
                time.sleep(0.001)
            counters = instance.snapshot()['counters']
            self.assertEqual(counters['requests'], 48)
            self.assertEqual(counters['bytes_sent'], 48 * 65536)
            self.assertEqual((counters['active_requests'], counters['errors']), (0, 0))

    def test_file_change_during_transfer_aborts_instead_of_finishing_changed_body(self):
        original_sendfile = os.sendfile
        changed = False
        def send(out_fd, in_fd, offset, count):
            nonlocal changed
            sent = original_sendfile(out_fd, in_fd, offset, min(count, 65536))
            if sent and not changed:
                changed = True
                (self.path / server.NAMES[0]).chmod(0o644)
            return sent
        with running(self.path) as instance, patch.object(server.os, 'sendfile', side_effect=send):
            with self.assertRaises(http.client.IncompleteRead):
                request(instance)
            self.assertTrue(changed)
            self.assertEqual(instance.snapshot()['state'], 'FAILED')
            self.assertEqual(request(instance, method='HEAD')[0], 503)

    def test_mutation_or_replacement_poison_cache_and_stop_further_delivery(self):
        for replace in (False, True):
            with self.subTest(replace=replace):
                # Restore a separate verified cache for each mutation type.
                with tempfile.TemporaryDirectory() as directory:
                    path = Path(directory)
                    make_cache(path)
                    with running(path) as instance:
                        self.assertEqual(request(instance, headers={'Range': 'bytes=0-2'})[0], 206)
                        target = path / server.NAMES[0]
                        if replace:
                            replacement = path / 'replacement'
                            replacement.write_bytes(CAR)
                            replacement.chmod(0o444)
                            replacement.replace(target)
                        else:
                            target.chmod(0o644)
                            with target.open('r+b') as output:
                                output.write(b'changed')
                            target.chmod(0o444)
                        self.assertEqual(request(instance)[0], 503)
                        self.assertEqual(request(instance, '/1000/epoch-1000-slot-ranges.raw')[0], 503)
                        self.assertEqual(instance.snapshot()['state'], 'FAILED')

    def test_incomplete_bad_or_stale_manifest_and_writable_files_are_rejected_before_bind(self):
        for field, value in [('state', 'DOWNLOADING'), ('schema', 'other'), ('epoch', 900)]:
            bad = dict(self.manifest, **{field: value})
            (self.path / 'manifest.json').write_text(json.dumps(bad))
            with self.assertRaises(ValueError):
                server.VerifiedCache(self.path)
        (self.path / 'manifest.json').write_text(json.dumps(self.manifest))
        target = self.path / server.NAMES[0]
        target.chmod(0o644)
        with self.assertRaises(server.CacheChanged):
            server.VerifiedCache(self.path)
        target.chmod(0o444)
        with self.assertRaises(server.CacheChanged):
            server.VerifiedCache(self.path)  # chmod changed ctime even after reverting mode.
        with self.assertRaises(ValueError):
            server.ArchiveServer(('0.0.0.0', 0), None)

    def test_symlink_cannot_replace_a_whitelisted_file(self):
        target = self.path / server.NAMES[0]
        saved = self.path / 'saved-car'
        target.rename(saved)
        target.symlink_to(saved)
        with self.assertRaises(OSError):
            server.VerifiedCache(self.path)

    def test_cli_publishes_resource_status_and_stops_cleanly(self):
        status_path = self.path / 'server-status.json'
        process = subprocess.Popen([sys.executable, str(Path(server.__file__).resolve()), '--cache-dir', str(self.path),
                                    '--status', str(status_path), '--status-interval', '0.1'],
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            deadline = time.monotonic() + 5
            while not status_path.exists() and process.poll() is None and time.monotonic() < deadline:
                time.sleep(0.02)
            self.assertTrue(status_path.exists())
            status = json.loads(status_path.read_text())
            self.assertEqual((status['pid'], status['bind_host'], status['state']), (process.pid, '127.0.0.1', 'READY'))
            self.assertEqual(status['manifest_sha256'], hashlib.sha256((self.path / 'manifest.json').read_bytes()).hexdigest())
            self.assertIn('total_cpu_seconds', status['resource_usage'])
            connection = http.client.HTTPConnection('127.0.0.1', status['port'], timeout=5)
            connection.request('GET', '/1000/epoch-1000-slot-ranges.raw')
            response = connection.getresponse()
            self.assertEqual((response.status, response.read()), (200, INDEX))
            connection.close()
            process.terminate()
            stdout, stderr = process.communicate(timeout=5)
            self.assertEqual(process.returncode, 0, stderr)
            status = json.loads(status_path.read_text())
            self.assertEqual(status['state'], 'STOPPED')
            self.assertEqual(status['counters']['bytes_sent'], len(INDEX))
        finally:
            if process.poll() is None:
                process.kill()
            process.communicate(timeout=5)


if __name__ == '__main__':
    unittest.main()
