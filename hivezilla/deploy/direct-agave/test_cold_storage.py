import importlib.util
import io
import json
from pathlib import Path
import tempfile
import sqlite3
import subprocess
import sys
import tarfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

spec = importlib.util.spec_from_file_location("cold", Path(__file__).with_name("cold_storage.py"))
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


class Remote:
    namespace = "namespace"
    fail = False
    def __init__(self): self.objects = {}; self.downloads = 0
    def put_verified(self, key, path, sha):
        if self.fail: raise RuntimeError("readback_mismatch")
        assert m.digest(path) == sha
        self.objects[key] = path.read_bytes()
    def download(self, key, path, sha, before_write):
        self.downloads += 1
        before_write(len(self.objects[key]))
        path.write_bytes(self.objects[key])
        if m.digest(path) != sha: raise RuntimeError("readback_mismatch")


class ColdTests(unittest.TestCase):
    def r2_errors(self):
        # The archive tests do not need the S3 SDK or any network access.
        boto = SimpleNamespace(**{name: type(name, (Exception,), {}) for name in (
            'ClientError', 'ConnectionClosedError', 'ReadTimeoutError', 'NoCredentialsError',
            'ResponseStreamingError', 'IncompleteReadError')})
        class ResponseStreamingError(Exception):
            def __init__(self, *args, **kwargs):
                super().__init__(*args)
                self.kwargs = kwargs
        boto.ResponseStreamingError = ResponseStreamingError
        urllib = SimpleNamespace(**{name: type(name, (Exception,), {})
                                    for name in ('ReadTimeoutError', 'ProtocolError')})
        self.enterContext(patch.dict(sys.modules, {
            'botocore': SimpleNamespace(exceptions=boto), 'botocore.exceptions': boto,
            'urllib3': SimpleNamespace(exceptions=urllib), 'urllib3.exceptions': urllib}))
        return boto, urllib

    @staticmethod
    def readback_body(value, error=None):
        class Body(io.BytesIO):
            calls = 0
            def read(self, size):
                self.calls += 1
                if error is not None and self.calls == 2:
                    raise error
                return super().read(min(size, 3))
        return Body(value)

    @staticmethod
    def r2_remote(bodies):
        remote = m.R2.__new__(m.R2)
        remote.bucket, remote.namespace = 'bucket', 'namespace'
        remote.client = SimpleNamespace(put_object=Mock(),
            get_object=Mock(side_effect=[{'Body': body} for body in bodies]))
        # These fake SDK tests exercise the exact implementation used inside
        # the isolated child. Process-boundary tests below cover the parent.
        remote.put_verified = remote._put_verified_in_process
        return remote

    @staticmethod
    def isolated_remote(root):
        remote = m.R2.__new__(m.R2)
        remote.namespace = 'b' * 64
        remote.helper = Path(m.__file__).resolve()
        remote.helper_sha256 = m.digest(remote.helper)
        remote.credentials_file = root / 'credentials.env'
        return remote

    def test_transfer_process_total_deadline_kills_and_reaps_slow_active_child(self):
        with tempfile.TemporaryDirectory() as d:
            pidfile = Path(d) / 'pid'
            script = ('import os,pathlib,signal,time; pathlib.Path(' + repr(str(pidfile)) + ').write_text(str(os.getpid())); '
                      'signal.signal(signal.SIGTERM,signal.SIG_IGN); '
                      '\nwhile True:\n print(" ",flush=True); time.sleep(.01)')
            processes = []
            original = m.subprocess.Popen
            def launch(*args, **kwargs):
                process = original(*args, **kwargs); processes.append(process); return process
            started = time.monotonic()
            with patch.object(m.subprocess, 'Popen', side_effect=launch):
                with self.assertRaises(m.BundleTransferTimeout):
                    m.transfer_process([sys.executable, '-c', script], {}, timeout=.3)
            self.assertLess(time.monotonic() - started, 3)
            self.assertTrue(pidfile.exists())
            self.assertEqual(len(processes), 1)
            self.assertIsNotNone(processes[0].returncode)
            self.assertTrue(processes[0].stdout.closed)
            with self.assertRaises(ProcessLookupError): m.os.kill(int(pidfile.read_text()), 0)
            with self.assertRaises(ChildProcessError): m.os.waitpid(processes[0].pid, m.os.WNOHANG)

    def test_transfer_process_bounds_output_and_discards_secret_stderr(self):
        script = 'import os,time; os.write(2,b"DO_NOT_PRINT"*100000); os.write(1,b"DO_NOT_PRINT"*100000); time.sleep(60)'
        with self.assertRaises(m.BundleTransferError) as error, patch('builtins.print') as output:
            m.transfer_process([sys.executable, '-c', script], {}, timeout=2)
        self.assertNotIn('DO_NOT_PRINT', str(error.exception))
        self.assertNotIn('DO_NOT_PRINT', str(vars(error.exception)))
        output.assert_not_called()

    def test_transfer_process_strict_json_rejects_duplicate_and_trailing_proofs(self):
        for raw in ('{"state":"VERIFIED","state":"FAILED"}', '{}\n{}', 'DO_NOT_PRINT'):
            with self.subTest(raw=raw), self.assertRaises(m.BundleTransferError):
                m.transfer_process([sys.executable, '-c', 'print(' + repr(raw) + ')'], {}, timeout=2)

    def test_parent_requires_exact_proof_and_unchanged_bundle_and_helper(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d).resolve(); path = root / 'bundle.tar'; path.write_bytes(b'verified bytes')
            remote = self.isolated_remote(root); sha = m.digest(path); key = 'native-custody/processed-v1/session/' + sha + '.tar'
            def result(command, request):
                self.assertEqual(command, [sys.executable, str(remote.helper), '_transfer', '--credentials-file', str(remote.credentials_file)])
                self.assertEqual(request['path'], str(path))
                self.assertEqual(request['namespace'], remote.namespace)
                self.assertEqual(request['sha256'], sha)
                return 0, {k: v for k, v in request.items() if k != 'path'}
            with patch.object(m, 'transfer_process', side_effect=result): remote.put_verified(key, path, sha)
            for field in ('namespace', 'key', 'sha256', 'bytes', 'nonce', 'helper_sha256', 'state', 'extra', 'missing', 'bool', 'nonzero', 'changed'):
                with self.subTest(field=field):
                    def bad(command, request):
                        code, value = result(command, request)
                        if field == 'extra': value['unexpected'] = True
                        elif field == 'missing': value.pop('namespace')
                        elif field == 'bool': value['bytes'] = True
                        elif field == 'nonzero': code = 1
                        elif field == 'changed': path.write_bytes(b'changed same size')
                        else: value[field] = 'wrong'
                        return code, value
                    with patch.object(m, 'transfer_process', side_effect=bad), self.assertRaises(m.BundleTransferError):
                        remote.put_verified(key, path, sha)
                    path.write_bytes(b'verified bytes')
            remote.helper_sha256 = '0' * 64
            with patch.object(m, 'transfer_process') as invoke, self.assertRaises(m.BundleTransferError): remote.put_verified(key, path, sha)
            invoke.assert_not_called()

    def test_timeout_keeps_index_and_custody_then_a_proven_retry_commits_once(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d).resolve(); source, record, data, evidence = self.fixture(root)
            remote = self.isolated_remote(root); state = root / 'cold-storage'; db = m.database(state, source, remote.namespace)
            run = m.transfer_process
            def timeout(command, request):
                return run([sys.executable, '-c', 'import time; time.sleep(60)'], request, timeout=.1)
            with patch.object(m, 'transfer_process', side_effect=timeout), self.assertRaises(m.BundleTransferTimeout):
                m.archive_group(root, db, remote, [record], state, True)
            self.assertEqual(db.execute('SELECT count(*) FROM archives').fetchone()[0], 0)
            self.assertIsNone(db.execute('SELECT value FROM meta WHERE id=2').fetchone())
            self.assertTrue(data.exists() and evidence.exists() and (state / 'bundle.tar').exists())
            self.assertFalse((data.parent / 'REMOTE-CUSTODY.json').exists())
            def success(command, request):
                script = 'import json,sys; r=json.load(sys.stdin); r.pop("path"); print(json.dumps(r))'
                return run([sys.executable, '-c', script], request, timeout=2)
            with patch.object(m, 'transfer_process', side_effect=success):
                m.archive_group(root, db, remote, [record], state, True)
            self.assertEqual(db.execute('SELECT count(*),sum(retired) FROM archives').fetchone(), (1, 1))
            self.assertFalse(data.exists() or evidence.exists()); db.close()

    def test_failed_or_mismatched_child_proofs_never_commit_or_retire(self):
        for failure in ('namespace', 'sha256', 'bytes', 'nonzero', 'credentials', 'hash', 'incomplete'):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as d:
                root = Path(d).resolve(); source, record, data, evidence = self.fixture(root)
                remote = self.isolated_remote(root); state = root / 'cold-storage'; db = m.database(state, source, remote.namespace)
                def result(command, request):
                    value = {k: v for k, v in request.items() if k != 'path'}
                    if failure in ('credentials', 'hash', 'incomplete'):
                        kind = {'credentials': 'NoCredentialsError', 'hash': 'RuntimeError', 'incomplete': 'IncompleteReadError'}[failure]
                        return 1, dict(schema=m.TRANSFER_SCHEMA, state='FAILED', error_type=kind,
                                       http_status=None, network_error_type=None)
                    if failure == 'nonzero': return 1, value
                    value[failure] = 'wrong'
                    return 0, value
                with patch.object(m, 'transfer_process', side_effect=result), self.assertRaises(m.BundleTransferError):
                    m.archive_group(root, db, remote, [record], state, True)
                self.assertEqual(db.execute('SELECT count(*) FROM archives').fetchone()[0], 0)
                self.assertIsNone(db.execute('SELECT value FROM meta WHERE id=2').fetchone())
                self.assertTrue(data.exists() and evidence.exists() and (state / 'bundle.tar').exists())
                self.assertFalse((data.parent / 'REMOTE-CUSTODY.json').exists()); db.close()

    def test_parallel_transfer_failure_keeps_only_proven_success_and_pending_cap_reservation(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d).resolve(); source, items, options = self.archive_fixture(root, count=2)
            remote = self.isolated_remote(root); state = root / 'cold-storage'; db = m.database(state, source, remote.namespace)
            transfers = m.Transfers(root, db, remote, state, True, 2)
            def result(command, request):
                if request['path'].endswith('bundle-1.tar'):
                    raise m.BundleTransferTimeout('bundle_transfer_deadline')
                return 0, {k: v for k, v in request.items() if k != 'path'}
            try:
                with patch.object(m, 'transfer_process', side_effect=result):
                    transfers.submit([items[0][0]]); transfers.submit([items[1][0]])
                    for future in transfers.pending:
                        try: future.result(timeout=3)
                        except m.BundleTransferTimeout: pass
                    with self.assertRaises(m.BundleTransferTimeout): transfers.collect()
                self.assertEqual(db.execute('SELECT slot,retired FROM archives').fetchall(), [(10, 1)])
                self.assertFalse(items[0][1].exists())
                self.assertTrue(items[1][1].exists() and items[1][2].exists())
                self.assertEqual(transfers.pending_banks(), {(11, 21)})
                self.assertEqual(m.archive_allowance(1, transfers.pending_banks(), 2), 0)
            finally: transfers.close(); db.close()

    def test_child_checks_namespace_before_network_and_emits_only_safe_failure_fields(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d).resolve(); path = root / 'bundle.tar'; path.write_bytes(b'verified bytes'); sha = m.digest(path)
            request = dict(schema=m.TRANSFER_SCHEMA, state='VERIFIED', namespace='b' * 64,
                key='native-custody/processed-v1/session/' + sha + '.tar', sha256=sha, bytes=path.stat().st_size,
                nonce='c' * 32, helper_sha256=m.digest(Path(m.__file__).resolve()), path=str(path))
            remote = SimpleNamespace(namespace='wrong', _put_verified_in_process=Mock())
            reader, writer = m.os.pipe()
            try:
                for namespace in ('wrong', 'b' * 64):
                    remote.namespace = namespace
                    with patch.object(m.threading, 'Thread'), patch.object(m, 'R2', return_value=remote), \
                         patch.object(m.sys, 'stdin', SimpleNamespace(buffer=io.BytesIO(json.dumps(request).encode()))), \
                         patch.object(m.sys, 'stdout', io.StringIO()) as output:
                        code = m.transfer_child(root / 'DO_NOT_PRINT.env', reader)
                        result = json.loads(output.getvalue())
                    if namespace == 'wrong':
                        self.assertEqual(code, 1); remote._put_verified_in_process.assert_not_called()
                        self.assertEqual(set(result), {'schema', 'state', 'error_type', 'http_status', 'network_error_type'})
                        self.assertNotIn('DO_NOT_PRINT', json.dumps(result))
                    else:
                        self.assertEqual(code, 0)
                        self.assertEqual(result, {k: v for k, v in request.items() if k != 'path'})
                        remote._put_verified_in_process.assert_called_once_with(request['key'], path, sha)
            finally: m.os.close(reader); m.os.close(writer)

    def test_child_exits_when_parent_liveness_pipe_closes(self):
        script = ('import importlib.util,sys,time,types; from pathlib import Path; '
                  's=importlib.util.spec_from_file_location("cold",sys.argv[1]); m=importlib.util.module_from_spec(s); s.loader.exec_module(m); '
                  'm.sys.stdin=types.SimpleNamespace(buffer=types.SimpleNamespace(read=lambda n:time.sleep(60))); '
                  'm.transfer_child(Path("/unused"),int(sys.argv[2]))')
        reader, writer = m.os.pipe()
        process = subprocess.Popen([sys.executable, '-c', script, m.__file__, str(reader)], pass_fds=(reader,),
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        m.os.close(reader)
        try:
            time.sleep(.15); self.assertIsNone(process.poll()); m.os.close(writer); writer = None
            self.assertEqual(process.wait(timeout=3), 70)
        finally:
            if process.poll() is None: process.kill()
            process.wait()
            if writer is not None: m.os.close(writer)

    def test_child_failure_classification_is_redacted_and_preserved_in_durable_status(self):
        cases = [('ReadTimeoutError', None, None), ('ResponseStreamingError', None, 'ProtocolError'),
                 ('ClientError', 503, None), ('ClientError', 403, None), ('NoCredentialsError', None, None),
                 ('RuntimeError', None, None), ('IncompleteReadError', None, None)]
        with tempfile.TemporaryDirectory() as d:
            state = Path(d)
            for kind, http, network in cases:
                failure = dict(schema=m.TRANSFER_SCHEMA, state='FAILED', error_type=kind,
                               http_status=http, network_error_type=network)
                with self.assertRaises(m.BundleTransferError) as error: m.validate_transfer_result(1, failure, {})
                with patch.object(m.sys, 'stderr', io.StringIO()): m.record_archive_failure(dict(state=state), error.exception)
                saved = json.loads((state / 'LAST-FAILURE.json').read_text())
                self.assertEqual((saved['error_type'], saved['http_status'], saved['network_error_type']), (kind, http, network))
            with patch.object(m.sys, 'stderr', io.StringIO()):
                m.record_archive_failure(dict(state=state), m.BundleTransferTimeout('bundle_transfer_deadline'))
            saved = json.loads((state / 'LAST-FAILURE.json').read_text())
            self.assertEqual((saved['error_type'], saved['http_status'], saved['network_error_type']), ('BundleTransferTimeout', None, None))
            for failure in (dict(schema=m.TRANSFER_SCHEMA, state='FAILED', error_type='DO_NOT_PRINT private URL', http_status=None, network_error_type=None),
                            dict(schema=m.TRANSFER_SCHEMA, state='FAILED', error_type='RuntimeError', http_status=503, network_error_type='ProtocolError')):
                with self.assertRaises(m.BundleTransferError) as error: m.validate_transfer_result(1, failure, {})
                self.assertIsNone(error.exception.evidence)

    def test_r2_readback_retries_partial_network_fault_from_byte_zero_with_fresh_hash_and_count(self):
        boto, urllib = self.r2_errors()
        wrapped = lambda value: boto.ResponseStreamingError(error=urllib.ProtocolError(value))
        for fault in (boto.ReadTimeoutError, boto.ConnectionClosedError, urllib.ReadTimeoutError, wrapped):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as d:
                path = Path(d) / 'bundle.tar'; path.write_bytes(b'complete verified bytes')
                partial = self.readback_body(b'bad prefix', fault('DO_NOT_PRINT private URL'))
                complete = self.readback_body(path.read_bytes())
                remote = self.r2_remote([partial, complete])
                def get_object(**kwargs):
                    if remote.client.get_object.call_count == 1:
                        return {'Body': partial}
                    self.assertTrue(partial.closed)
                    return {'Body': complete}
                remote.client.get_object.side_effect = get_object
                with patch.object(m.time, 'sleep') as sleep, patch('builtins.print') as output:
                    remote.put_verified('key', path, m.digest(path))
                self.assertEqual(remote.client.put_object.call_count, 1)
                self.assertEqual(remote.client.get_object.call_args_list,
                    [unittest.mock.call(Bucket='bucket', Key='key')] * 2)
                sleep.assert_called_once_with(1)
                self.assertEqual(partial.calls, 2)
                self.assertTrue(partial.closed and complete.closed)
                output.assert_not_called()

    def test_r2_readback_network_retries_stop_after_three_attempts_without_commit_or_retirement(self):
        boto, urllib = self.r2_errors()
        wrapped = lambda value: boto.ResponseStreamingError(error=urllib.ProtocolError(value))
        for fault in (boto.ReadTimeoutError, wrapped):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as d:
                root = Path(d); source, record, data, evidence = self.fixture(root)
                bodies = [self.readback_body(b'partial', fault('DO_NOT_PRINT')) for _ in range(3)]
                remote = self.r2_remote(bodies)
                state = root / 'cold-storage'; db = m.database(state, source, remote.namespace)
                with patch.object(m.time, 'sleep') as sleep, patch('builtins.print') as output:
                    with self.assertRaises(type(fault('DO_NOT_PRINT'))):
                        m.archive_group(root, db, remote, [record], state, True)
                self.assertEqual(remote.client.get_object.call_count, 3)
                self.assertEqual(remote.client.put_object.call_count, 1)
                self.assertEqual(sleep.call_args_list, [unittest.mock.call(1), unittest.mock.call(2)])
                self.assertTrue(all(body.closed for body in bodies))
                self.assertEqual(db.execute('SELECT count(*) FROM archives').fetchone()[0], 0)
                self.assertIsNone(db.execute('SELECT value FROM meta WHERE id=2').fetchone())
                self.assertTrue(data.exists() and evidence.exists())
                output.assert_not_called(); db.close()

    def test_r2_readback_size_digest_and_permanent_faults_do_not_retry(self):
        boto, _ = self.r2_errors()
        for failure in ('digest', 'size', 'too_large', 'credentials', 'http', 'stream', 'stream_os',
                        'length', 'source', 'os'):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as d:
                path = Path(d) / 'bundle.tar'; path.write_bytes(b'expected')
                value, error = b'expected', None
                if failure == 'digest': value = b'changed!'
                elif failure == 'size': value = b'short'
                elif failure == 'too_large': value = b'excessive'
                elif failure == 'credentials': error = boto.NoCredentialsError('DO_NOT_PRINT')
                elif failure == 'http': error = boto.ClientError('DO_NOT_PRINT')
                elif failure == 'stream': error = boto.ResponseStreamingError('DO_NOT_PRINT')
                elif failure == 'stream_os': error = boto.ResponseStreamingError(error=OSError('DO_NOT_PRINT'))
                elif failure == 'length': error = boto.IncompleteReadError('DO_NOT_PRINT')
                elif failure == 'source': error = RuntimeError('source_identity_mismatch')
                else: error = OSError('DO_NOT_PRINT')
                body = self.readback_body(value, error)
                remote = self.r2_remote([body])
                if failure in ('credentials', 'http'):
                    remote.client.get_object.side_effect = error
                with patch.object(m.time, 'sleep') as sleep, patch.object(m, 'MAX_BUNDLE', 8):
                    with self.assertRaises(type(error) if error is not None else RuntimeError):
                        remote.put_verified('key', path, m.digest(path))
                self.assertEqual(remote.client.get_object.call_count, 1)
                if failure not in ('credentials', 'http'):
                    self.assertTrue(body.closed)
                body.close()
                sleep.assert_not_called()

    def test_r2_readback_retry_does_not_retry_the_upload(self):
        boto, _ = self.r2_errors()
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / 'bundle.tar'; path.write_bytes(b'expected')
            remote = self.r2_remote([])
            remote.client.put_object.side_effect = boto.ReadTimeoutError('DO_NOT_PRINT')
            with patch.object(m.time, 'sleep') as sleep:
                with self.assertRaises(boto.ReadTimeoutError):
                    remote.put_verified('key', path, m.digest(path))
            self.assertEqual(remote.client.put_object.call_count, 1)
            remote.client.get_object.assert_not_called()
            sleep.assert_not_called()

    def test_r2_readback_keeps_stream_wrapper_length_validation(self):
        boto, _ = self.r2_errors()
        class WrappedBody:
            # StreamingBody.__enter__ returns the underlying stream. Reading
            # that stream would bypass the wrapper's content-length check.
            def __init__(self, value): self.raw, self.calls = io.BytesIO(value), 0
            def __enter__(self): return self.raw
            def __exit__(self, *_): self.close()
            def close(self): self.raw.close()
            def read(self, size):
                self.calls += 1
                value = self.raw.read(size)
                if not value: raise boto.IncompleteReadError('DO_NOT_PRINT invalid content length')
                return value
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / 'bundle.tar'; path.write_bytes(b'expected')
            body = WrappedBody(path.read_bytes())
            remote = self.r2_remote([body])
            with patch.object(m.time, 'sleep') as sleep:
                with self.assertRaises(boto.IncompleteReadError):
                    remote.put_verified('key', path, m.digest(path))
            self.assertEqual(body.calls, 2)
            self.assertTrue(body.raw.closed)
            self.assertEqual(remote.client.get_object.call_count, 1)
            sleep.assert_not_called()

    def archive_fixture(self, root, count=5):
        items = []
        for slot in range(10, 10 + count):
            source, record, data, evidence = self.fixture(root, slot=slot, bank=slot + 10, suffix=f'-{slot}')
            m.os.utime(data.parent / 'DIRECT-AGAVE-COMPACT.v1.json', (1, 1))
            for entry in record['files']:
                entry['stamp'] = m.identity(root / entry['path'])
            items.append((record, data, evidence))
        bank_output, capture = root / 'v2', root / 'capture'
        bank_output.mkdir(); capture.mkdir()
        (bank_output / 'FEED.json').write_text(json.dumps(dict(source, payload_root=str(root))))
        feed = sqlite3.connect(root / 'feed.sqlite')
        feed.executescript('CREATE TABLE archive_catalog(slot INTEGER,bank INTEGER,artifact TEXT,'
            'marker_sha TEXT,custody_path TEXT,custody_sha TEXT,retirement_enqueued INTEGER);'
            'CREATE TABLE banks(slot INTEGER,bank INTEGER,state TEXT,dirty INTEGER);'
            'CREATE TABLE feed_events(slot INTEGER,bank INTEGER,sequence INTEGER);'
            'CREATE INDEX feed_bank_events ON feed_events(slot,bank,sequence);')
        banks = sqlite3.connect(bank_output / 'feed.sqlite')
        banks.executescript('CREATE TABLE feed_state(id INTEGER,processed_sequence INTEGER);'
            'INSERT INTO feed_state VALUES(1,1000000);'
            'CREATE TABLE feed_events(artifact TEXT,sequence INTEGER);')
        for record, data, evidence in items:
            marker, proof = data.parent / 'DIRECT-AGAVE-COMPACT.v1.json', evidence.parent / 'CUSTODY.json'
            slot, bank = record['slot'], record['bank']
            feed.execute('INSERT INTO archive_catalog VALUES(?,?,?,?,?,?,1)',
                         (slot, bank, str(data.parent), m.digest(marker), str(proof), m.digest(proof)))
            feed.execute("INSERT INTO banks VALUES(?,?,'rooted',0)", (slot, bank))
            feed.execute('INSERT INTO feed_events VALUES(?,?,?)', (slot, bank, slot))
            banks.execute('INSERT INTO feed_events VALUES(?,?)', (str(data.parent), slot))
        feed.commit(); feed.close(); banks.commit(); banks.close()
        native = sqlite3.connect(capture / 'events.sqlite')
        native.execute('CREATE TABLE events(slot INTEGER,bank INTEGER,sequence INTEGER)'); native.close()
        publisher, spool, bank_spool = [root / p for p in ('publisher', 'v1-spool', 'v2-spool')]
        for path in (publisher, spool, bank_spool): path.mkdir()
        (publisher / 'CHECKPOINT.json').write_text(json.dumps(
            dict(source=str(root), session='session', cursor='session:p1000000:x')))
        (spool / 'CHECKPOINT.json').write_text(json.dumps(dict(base='session:p1000000:x')))
        (bank_spool / 'CHECKPOINT.json').write_text(json.dumps(dict(source=source, base='session:p1000000:x')))
        options = SimpleNamespace(output=root, bank_output=bank_output, capture=capture,
            publisher_state=publisher, v1_spool=spool, bank_origin_spool=bank_spool,
            credentials_file=root / 'unused', retire=True, keep_seconds=7200,
            parallel_bundles=4, max_batches=460, max_archived_banks=None, batch_banks=64, follow=True)
        return source, items, options

    def test_absolute_archive_cap_clamps_a_partial_group_and_survives_restart(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d).resolve(); source, items, options = self.archive_fixture(root)
            options.max_archived_banks = 2
            remote = Remote()
            with patch.object(m, 'R2', return_value=remote), patch('builtins.print'):
                m.archive(options)
                m.archive(options)  # The durable total, not this run's counter, is the cap.
            db = m.database(root / 'cold-storage', source, remote.namespace)
            self.assertEqual(db.execute('SELECT slot,retired FROM archives ORDER BY slot').fetchall(), [(10, 1), (11, 1)])
            db.close()
            self.assertEqual(len(remote.objects), 1)
            self.assertFalse(any(path.exists() for _, *paths in items[:2] for path in paths))
            self.assertTrue(all(path.exists() for _, *paths in items[2:] for path in paths))
            status = json.loads((root / 'cold-storage/STATUS.json').read_text())
            self.assertEqual((status['state'], status['stop_reason'], status['max_archived_banks']),
                             ('COMPLETE', 'max_archived_banks', 2))

    def test_absolute_archive_cap_reserves_banks_in_parallel_pending_groups(self):
        class DeferredTransfers:
            largest_pending = 0
            def __init__(self, root, db, remote, state, retire_files, limit, recovery=None):
                self.root, self.db, self.remote, self.state = root, db, remote, state
                self.retire_files, self.pending, self.limit = retire_files, {}, limit
            def can_submit(self): return len(self.pending) < self.limit
            def pending_banks(self):
                return {(r['slot'], r['bank']) for data in self.pending.values() for r in data['records']}
            def submit(self, records):
                self.pending[len(self.pending)] = dict(records=records, submitted_unix_ms=1)
                self.largest_pending = max(self.largest_pending, len(self.pending_banks()))
                DeferredTransfers.largest_pending = self.largest_pending
                if len(self.pending_banks()) > 2:
                    raise AssertionError('pending groups exceeded absolute allowance')
            def collect(self, block=False):
                if len(self.pending) < 2: return []
                done = []
                for data in self.pending.values():
                    freed = m.archive_group(self.root, self.db, self.remote, data['records'], self.state, self.retire_files)
                    done.append(dict(banks=len(data['records']), freed=freed, seconds=0))
                self.pending.clear()
                return done
            def close(self): pass
        with tempfile.TemporaryDirectory() as d:
            root = Path(d).resolve(); source, items, options = self.archive_fixture(root)
            remote = Remote(); state = root / 'cold-storage'; db = m.database(state, source, remote.namespace)
            m.archive_group(root, db, remote, [items[0][0]], state, True); db.close()
            options.max_archived_banks, options.batch_banks = 3, 1
            with patch.object(m, 'R2', return_value=remote), patch.object(m, 'Transfers', DeferredTransfers), \
                 patch('builtins.print'):
                m.archive(options)
            db = m.database(state, source, remote.namespace)
            self.assertEqual(db.execute('SELECT count(*) FROM archives').fetchone()[0], 3); db.close()
            self.assertEqual(DeferredTransfers.largest_pending, 2)
            self.assertTrue(all(path.exists() for _, *paths in items[3:] for path in paths))

    def test_archive_failure_is_durable_and_does_not_expose_exception_secrets(self):
        class FailingRemote(Remote):
            def put_verified(self, key, path, sha):
                raise OSError('private-access-key=DO_NOT_PRINT https://signed.invalid/?secret=DO_NOT_PRINT')
        with tempfile.TemporaryDirectory() as d:
            root = Path(d).resolve(); source, items, options = self.archive_fixture(root)
            remote = Remote(); state = root / 'cold-storage'; db = m.database(state, source, remote.namespace)
            with patch.object(m.time, 'time', return_value=123):
                m.archive_group(root, db, remote, [items[0][0]], state, True)
            db.close()
            stderr = io.StringIO()
            with patch.object(m, 'R2', return_value=FailingRemote()), patch.object(m.sys, 'stderr', stderr):
                with self.assertRaisesRegex(RuntimeError, '^cold_storage_archive_failed;') as error:
                    m.archive(options)
            status = json.loads((state / 'STATUS.json').read_text())
            self.assertEqual((status['state'], status['storage_progress'], status['error'], status['error_type']),
                             ('FAILED', 'FAILED', 'archive_operation_failed', 'OSError'))
            self.assertEqual((status['archived_banks'], status['retired_banks']), (1, 1))
            self.assertEqual(status['last_archive_progress_unix_ms'], 123000)
            self.assertEqual(json.loads((state / 'LAST-FAILURE.json').read_text()), status)
            self.assertIn('put_verified', status['failure_traceback'])
            self.assertLessEqual(len(status['failure_traceback']), 8192)
            self.assertNotIn('DO_NOT_PRINT', json.dumps(status) + stderr.getvalue() + str(error.exception))
            self.assertTrue(all(path.exists() for _, *paths in items[1:] for path in paths))

    def test_failure_without_archive_lock_does_not_replace_owner_status(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d).resolve(); _, _, options = self.archive_fixture(root)
            state = root / 'cold-storage'; state.mkdir()
            original = json.dumps(dict(state='RUNNING', last_archive_progress_unix_ms=123))
            (state / 'STATUS.json').write_text(original)
            with (state / 'LOCK').open('w') as lock:
                m.fcntl.flock(lock, m.fcntl.LOCK_EX | m.fcntl.LOCK_NB)
                with patch.object(m.sys, 'stderr', io.StringIO()):
                    with self.assertRaisesRegex(RuntimeError, '^cold_storage_archive_failed;'):
                        m.archive(options)
            self.assertEqual((state / 'STATUS.json').read_text(), original)
            self.assertFalse((state / 'LAST-FAILURE.json').exists())

    def test_failure_report_replaces_invalid_status_structure_with_safe_metadata(self):
        with tempfile.TemporaryDirectory() as d:
            state = Path(d)
            (state / 'STATUS.json').write_text('[]')
            with patch.object(m.sys, 'stderr', io.StringIO()):
                m.record_archive_failure(dict(state=state), OSError('private credential'))
            status = json.loads((state / 'STATUS.json').read_text())
            self.assertEqual(status['state'], 'FAILED')
            self.assertNotIn('private credential', json.dumps(status))

    def test_failure_evidence_binds_source_invocation_and_safe_http_status_only(self):
        class ClientError(Exception):
            response = {'ResponseMetadata': {'HTTPStatusCode': 503}, 'private-body': 'DO_NOT_PRINT'}
        with tempfile.TemporaryDirectory() as d:
            state = Path(d); source = dict(session_id='source', agave_revision='revision',
                                          genesis_hash='genesis', node_identity='node')
            with patch.dict(m.os.environ, {'INVOCATION_ID': 'a' * 32}), patch.object(m.sys, 'stderr', io.StringIO()):
                m.record_archive_failure(dict(state=state, source=source), ClientError('DO_NOT_PRINT'))
            saved = json.loads((state / 'LAST-FAILURE.json').read_text())
            self.assertEqual((saved['source'], saved['invocation_id'], saved['http_status']),
                             (source, 'a' * 32, 503))
            self.assertNotIn('DO_NOT_PRINT', json.dumps(saved))

    def test_failure_evidence_records_only_verified_protocol_wrapper_subtype(self):
        boto, urllib = self.r2_errors()
        errors = [(boto.ResponseStreamingError(error=urllib.ProtocolError('DO_NOT_PRINT private URL')),
                   'ProtocolError'),
                  (boto.ResponseStreamingError(error=OSError('DO_NOT_PRINT private URL')), None),
                  (boto.ResponseStreamingError('DO_NOT_PRINT private URL'), None),
                  (OSError('DO_NOT_PRINT private URL'), None)]
        with tempfile.TemporaryDirectory() as d:
            state = Path(d)
            for error, expected in errors:
                with self.subTest(expected=expected), patch.object(m.sys, 'stderr', io.StringIO()) as stderr:
                    m.record_archive_failure(dict(state=state), error)
                    saved = json.loads((state / 'LAST-FAILURE.json').read_text())
                    self.assertEqual(saved['network_error_type'], expected)
                    self.assertNotIn('DO_NOT_PRINT', json.dumps(saved) + stderr.getvalue())

    def restore(self, root, requested, state, remote, **kw):
        options = dict(reserve_bytes=0, margin_bytes=0,
                       disk_usage=lambda _: SimpleNamespace(free=4 * m.MAX_BUNDLE))
        options.update(kw)
        return m.restore(root, requested, state, remote, **options)

    def change_remote_bundle(self, remote, record, folders, change):
        rebuilt = io.BytesIO()
        with tarfile.open(fileobj=io.BytesIO(remote.objects[record['key']]), mode='r:') as old:
            with tarfile.open(fileobj=rebuilt, mode='w') as new:
                for member in old:
                    value = change(member.name, old.extractfile(member).read())
                    member.size = len(value)
                    new.addfile(member, io.BytesIO(value))
        remote.objects[record['key']] = rebuilt.getvalue()
        for folder in folders:
            note_path = folder / 'REMOTE-CUSTODY.json'; note = json.loads(note_path.read_text())
            note['bundle_sha256'] = m.hashlib.sha256(rebuilt.getvalue()).hexdigest()
            note_path.write_text(json.dumps(note))

    def test_async_verification_keeps_sqlite_commit_and_retirement_on_owner_thread(self):
        class HeldRemote(Remote):
            def __init__(self):
                super().__init__()
                self.started, self.release = threading.Event(), threading.Event()
            def put_verified(self, key, path, sha):
                self.started.set()
                if not self.release.wait(5):
                    raise RuntimeError('verification_not_released')
                super().put_verified(key, path, sha)
        with tempfile.TemporaryDirectory() as d:
            root = Path(d); source, record, data, evidence = self.fixture(root)
            state = root / 'cold-storage'; remote = HeldRemote()
            db = m.database(state, source, remote.namespace)
            transfers = m.Transfers(root, db, remote, state, True, 2)
            try:
                transfers.submit([record])
                self.assertTrue(remote.started.wait(2))
                self.assertEqual(transfers.collect(), [])
                self.assertEqual(db.execute('SELECT count(*) FROM archives').fetchone()[0], 0)
                self.assertIsNone(db.execute('SELECT value FROM meta WHERE id=2').fetchone())
                self.assertTrue(data.exists() and evidence.exists())
                self.assertEqual(transfers.pending_banks(), {(10, 20)})
                remote.release.set()
                committed = []
                with patch.object(m.time, 'time', return_value=123456):
                    for _ in range(5):
                        committed = transfers.collect(block=True)
                        if committed: break
                self.assertEqual(committed[0]['banks'], 1)
                self.assertFalse(data.exists() or evidence.exists())
                self.assertEqual(db.execute('SELECT retired FROM archives').fetchone()[0], 1)
                self.assertEqual(db.execute('SELECT value FROM meta WHERE id=2').fetchone()[0], '123456000')
            finally:
                remote.release.set()
                transfers.close()
                db.close()
            reopened = m.database(state, source, remote.namespace)
            self.assertEqual(reopened.execute('SELECT value FROM meta WHERE id=2').fetchone()[0], '123456000')
            reopened.close()

    def test_progress_requires_a_pending_eligible_bank_before_stall(self):
        now = 10_000_000
        transfers = SimpleNamespace(pending={})
        self.assertEqual(m.progress_status(transfers, None, now)['storage_progress'], 'WAITING')
        self.assertEqual(m.progress_status(transfers, now - 1000, now)['storage_progress'], 'ADVANCING')
        self.assertEqual(m.progress_status(transfers, 1, now)['storage_progress'], 'WAITING')
        transfers.pending[0] = dict(submitted_unix_ms=now - 1000, records=[dict(slot=10)])
        active = m.progress_status(transfers, None, now)
        self.assertEqual(active['storage_progress'], 'TRANSFERRING')
        self.assertEqual(active['eligible_wait_since_unix_ms'], now - 1000)
        self.assertEqual(active['oldest_eligible_pending_slot'], 10)
        wait = m.STORAGE_STALL_SECONDS * 1000
        self.assertEqual(m.progress_status(transfers, None, now + wait - 1001)['storage_progress'], 'TRANSFERRING')
        self.assertEqual(m.progress_status(transfers, None, now + wait - 1000)['storage_progress'], 'STALLED')
        # Another bundle's durable commit is evidence that storage still moves.
        self.assertEqual(m.progress_status(transfers, now + wait - 2000, now + wait)['storage_progress'], 'TRANSFERRING')

    def test_candidates_skip_archived_and_unrooted_banks(self):
        db = sqlite3.connect(':memory:')
        db.executescript("ATTACH DATABASE ':memory:' AS cold;"
            "CREATE TABLE cold.archives(slot INTEGER,bank INTEGER,PRIMARY KEY(slot,bank));"
            "CREATE TABLE archive_catalog(slot INTEGER,bank INTEGER,artifact TEXT,marker_sha TEXT,"
            "custody_path TEXT,custody_sha TEXT,retirement_enqueued INTEGER,root_sequence INTEGER);"
            "CREATE TABLE banks(slot INTEGER,bank INTEGER,state TEXT,dirty INTEGER);"
            "CREATE TABLE feed_events(slot INTEGER,bank INTEGER,sequence INTEGER);")
        for slot, state in [(1, 'rooted'), (2, 'rooted'), (3, 'processed'), (4, 'rooted')]:
            db.execute('INSERT INTO archive_catalog VALUES(?,?,?,?,?,?,?,?)',
                       (slot, slot, 'payload', 'sha', 'proof', 'proofsha', 1, 1_000_000 + slot))
            db.execute('INSERT INTO banks VALUES(?,?,?,0)', (slot, slot, state))
            db.execute('INSERT INTO feed_events VALUES(?,?,?)', (slot, slot, slot))
        db.execute('INSERT INTO cold.archives VALUES(1,1)')
        self.assertEqual([row[0] for row in m.candidates(db, 4)], [2])
        self.assertEqual([row[0] for row in m.candidates(db, 5)], [2, 4])
        self.assertEqual([row[0] for row in m.candidates(db, 5, scan_after=2)], [4])
        self.assertEqual([row[0] for row in m.candidates(db, 5, scan_through=2)], [2])
        # A bundle can end between two bank identities at the same slot.
        db.execute('INSERT INTO archive_catalog VALUES(2,20,?,?,?,?,1,1)', ('payload', 'sha', 'proof', 'proofsha'))
        db.execute("INSERT INTO banks VALUES(2,20,'rooted',0)")
        db.execute('INSERT INTO feed_events VALUES(2,20,2)')
        self.assertEqual([row[:2] for row in m.candidates(db, 5, scan_after=2, scan_bank=2)], [(2, 20), (4, 4)])
        for slot in range(5, 200):
            db.execute('INSERT INTO archive_catalog VALUES(?,?,?,?,?,?,1,1)', (slot, slot, 'p', 's', 'c', 'h'))
            db.execute("INSERT INTO banks VALUES(?,?,'rooted',0)", (slot, slot))
            db.execute('INSERT INTO feed_events VALUES(?,?,?)', (slot, slot, slot))
        rows = m.candidates(db, 1000, scan_through=150)
        self.assertEqual(len(rows), 128)
        following = m.candidates(db, 1000, scan_after=rows[-1][0], scan_bank=rows[-1][1], scan_through=150)
        self.assertEqual((rows + following)[-1][:2], (150, 150))
        self.assertEqual(len({row[:2] for row in rows + following}), 149)
        db.close()

    def test_live_input_revisits_age_replay_and_writer_ack_skips(self):
        # Exercise the real archive loop, SQL gates, verified bundle commit and
        # retirement. Only network and thread scheduling are synchronous here.
        class ImmediateTransfers:
            def __init__(self, root, db, remote, state, retire_files, limit, recovery=None):
                self.root, self.db, self.remote, self.state = root, db, remote, state
                self.retire_files = retire_files
                self.pending = {}
            def can_submit(self): return not self.pending
            def pending_banks(self):
                return {(r['slot'], r['bank']) for d in self.pending.values() for r in d['records']}
            def submit(self, records):
                self.pending[0] = dict(records=records, started=m.time.monotonic(),
                                       submitted_unix_ms=int(m.time.time() * 1000))
            def collect(self, block=False):
                if not self.pending: return []
                data = self.pending.pop(0)
                freed = m.archive_group(self.root, self.db, self.remote, data['records'],
                                        self.state, self.retire_files)
                return [dict(banks=len(data['records']), freed=freed, seconds=0)]
            def close(self): pass

        with tempfile.TemporaryDirectory() as d:
            root = Path(d).resolve(); now = [100_000.0]; remote = Remote()
            source, _, _, _ = self.fixture(root, slot=10, bank=20, suffix='-10')
            bank_output, capture = root / 'v2', root / 'capture'
            bank_output.mkdir(); capture.mkdir()
            (bank_output / 'FEED.json').write_text(json.dumps(dict(source, payload_root=str(root))))
            feed = sqlite3.connect(root / 'feed.sqlite')
            feed.executescript('CREATE TABLE archive_catalog(slot INTEGER,bank INTEGER,artifact TEXT,'
                'marker_sha TEXT,custody_path TEXT,custody_sha TEXT,retirement_enqueued INTEGER);'
                'CREATE INDEX archive_pending_retirement ON archive_catalog(retirement_enqueued,slot);'
                'CREATE TABLE banks(slot INTEGER,bank INTEGER,state TEXT,dirty INTEGER);'
                'CREATE TABLE feed_events(slot INTEGER,bank INTEGER,sequence INTEGER);'
                'CREATE INDEX feed_bank_events ON feed_events(slot,bank,sequence);')
            banks = sqlite3.connect(bank_output / 'feed.sqlite')
            banks.executescript('CREATE TABLE feed_state(id INTEGER,processed_sequence INTEGER);'
                'INSERT INTO feed_state VALUES(1,1000000);'
                'CREATE TABLE feed_events(artifact TEXT,sequence INTEGER);'
                'CREATE INDEX event_artifact ON feed_events(artifact,sequence);')
            native = sqlite3.connect(capture / 'events.sqlite')
            native.executescript('CREATE TABLE events(slot INTEGER,bank INTEGER,sequence INTEGER);'
                                'CREATE INDEX bank_events ON events(slot,bank,sequence);')
            publisher, spool, bank_spool = [root / p for p in ('publisher', 'v1-spool', 'v2-spool')]
            for path in (publisher, spool, bank_spool): path.mkdir()
            (publisher / 'CHECKPOINT.json').write_text(json.dumps(
                dict(source=str(root), session='session', cursor='session:p1000000:x')))
            (spool / 'CHECKPOINT.json').write_text(json.dumps(dict(base='session:p1000000:x')))
            def bank_base(sequence):
                (bank_spool / 'CHECKPOINT.json').write_text(json.dumps(
                    dict(source=source, base=f'session:p{sequence}:x')))
            bank_base(100)
            originals, future = [], []
            def add(slot, *, age_seconds=7201, replay_sequence=20):
                _, record, data, evidence = self.fixture(root, slot=slot, bank=slot + 10, suffix=f'-{slot}')
                artifact, proof = data.parent, evidence.parent / 'CUSTODY.json'
                marker = artifact / 'DIRECT-AGAVE-COMPACT.v1.json'
                m.os.utime(marker, (now[0] - age_seconds,) * 2)
                feed.execute('INSERT INTO archive_catalog VALUES(?,?,?,?,?,?,1)',
                             (slot, slot + 10, str(artifact), m.digest(marker), str(proof), m.digest(proof)))
                feed.execute("INSERT INTO banks VALUES(?,?,'rooted',0)", (slot, slot + 10))
                feed.execute('INSERT INTO feed_events VALUES(?,?,?)', (slot, slot + 10, slot))
                banks.execute('INSERT INTO feed_events VALUES(?,?)', (str(artifact), replay_sequence))
                feed.commit(); banks.commit()
                return data, evidence
            # Remove the initial fixture's files before the same fixture is added.
            for name in ('payload-10', 'custody-10'): m.shutil.rmtree(root / name)
            originals += add(10, age_seconds=7199)
            originals += add(11, replay_sequence=200)
            originals += add(12)
            native.execute('INSERT INTO events VALUES(12,22,1)'); native.commit()
            sleeps = []
            def tick(seconds):
                sleeps.append(seconds)
                self.assertLess(len(sleeps), 12, 'old banks starved while new live rows arrived')
                # Every wait has new input, so the old unbounded scan never
                # reaches an empty page. Gates open without changing source data.
                self.assertTrue(all(p.exists() for p in originals))
                now[0] += seconds
                bank_base(1000)
                native.execute('DELETE FROM events'); native.commit()
                future.extend(add(100 + len(sleeps), age_seconds=0))
            options = SimpleNamespace(output=root, bank_output=bank_output, capture=capture,
                publisher_state=publisher, v1_spool=spool, bank_origin_spool=bank_spool,
                credentials_file=root / 'unused', retire=True, keep_seconds=7200,
                parallel_bundles=1, max_batches=1, batch_banks=64, follow=True)
            try:
                with patch.object(m, 'R2', return_value=remote), patch.object(m, 'Transfers', ImmediateTransfers), \
                     patch('builtins.print'), \
                     patch.object(m.time, 'time', side_effect=lambda: now[0]), \
                     patch.object(m.time, 'monotonic', side_effect=lambda: now[0]), \
                     patch.object(m.time, 'sleep', side_effect=tick):
                    m.archive(options)
                archive = sqlite3.connect(root / 'cold-storage/archive.sqlite')
                self.assertEqual(archive.execute('SELECT slot,retired FROM archives ORDER BY slot').fetchall(),
                                 [(10, 1), (11, 1), (12, 1)])
                archive.close()
                self.assertFalse(any(p.exists() for p in originals))
                self.assertTrue(all(p.exists() for p in future))
                self.assertEqual(len(remote.objects), 1)
                status = json.loads((root / 'cold-storage/STATUS.json').read_text())
                self.assertGreaterEqual(status['scan_sweeps_completed'], 1)
                self.assertEqual(status['last_archive_progress_unix_ms'], int(now[0] * 1000))
                self.assertEqual(status['storage_progress'], 'ADVANCING')
            finally:
                feed.close(); banks.close(); native.close()

    def fixture(self, root, *, slot=10, bank=20, suffix=''):
        source = dict(genesis_hash="g", node_identity="n", agave_revision="r", session_id="session")
        (root / "FEED.json").write_text(json.dumps(source))
        artifact, custody = root / ("payload" + suffix), root / ("custody" + suffix)
        artifact.mkdir(); custody.mkdir()
        data = artifact / "blocks.bin"
        data.write_bytes(b"compact execution")
        evidence = custody / "source-events.json.zst"
        evidence.write_bytes(b"native events")
        bank_id = {"slot": slot, "bank_id": bank}
        marker = dict(source, produced_blocks=[bank_id], files=[dict(path=data.name, bytes=data.stat().st_size, sha256=m.digest(data))])
        marker_path = artifact / "DIRECT-AGAVE-COMPACT.v1.json"
        marker_path.write_text(json.dumps(marker))
        proof = dict(state="COMPLETE", finalized=True, session_id="session", produced_blocks=[bank_id], through_sequence=500,
                     payload_marker_path=str(marker_path), payload_marker_sha256=m.digest(marker_path),
                     files=[dict(path=evidence.name, bytes=evidence.stat().st_size, sha256=m.digest(evidence))])
        proof_path = custody / "CUSTODY.json"
        proof_path.write_text(json.dumps(proof))
        row = (slot, bank, str(artifact), m.digest(marker_path), str(proof_path), m.digest(proof_path), 100)
        return source, m.build_bank(root, row, source), data, evidence

    def test_remote_failure_keeps_every_file_and_has_no_retirement_commit(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d); source, record, data, evidence = self.fixture(root)
            state = root / "cold-storage"; remote = Remote(); remote.fail = True
            db = m.database(state, source, remote.namespace)
            with self.assertRaisesRegex(RuntimeError, "readback_mismatch"):
                m.archive_group(root, db, remote, [record], state, True)
            self.assertTrue(data.exists() and evidence.exists())
            self.assertEqual(db.execute("SELECT count(*) FROM archives").fetchone()[0], 0)
            db.close()

    def test_verified_retirement_and_restore_keep_exact_bytes_and_markers(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d); source, record, data, evidence = self.fixture(root)
            originals = {p: p.read_bytes() for p in [data, evidence]}
            state = root / "cold-storage"; remote = Remote(); db = m.database(state, source, remote.namespace)
            m.archive_group(root, db, remote, [record], state, True)
            self.assertFalse(data.exists() or evidence.exists())
            self.assertTrue((data.parent / "DIRECT-AGAVE-COMPACT.v1.json").exists())
            self.assertTrue((evidence.parent / "CUSTODY.json").exists())
            self.restore(root, evidence, state, remote)
            self.assertFalse(data.exists())
            self.assertEqual(evidence.read_bytes(), originals[evidence])
            self.restore(root, data, state, remote)
            for path, value in originals.items(): self.assertEqual(path.read_bytes(), value)
            self.assertEqual(remote.downloads, 2)
            self.restore(root, data, state, remote)
            self.assertEqual(remote.downloads, 2)
            db.close()

    def test_remote_corruption_cannot_restore_a_deleted_file(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d); source, record, data, evidence = self.fixture(root)
            state = root / "cold-storage"; remote = Remote(); db = m.database(state, source, remote.namespace)
            m.archive_group(root, db, remote, [record], state, True)
            remote.objects[record["key"]] = b"corrupt"
            with self.assertRaisesRegex(RuntimeError, "readback_mismatch"):
                self.restore(root, data, state, remote)
            self.assertFalse(data.exists())
            self.assertEqual(list(state.glob('restore-*')), [])
            db.close()

    def test_different_source_cannot_use_a_retirement_receipt(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d); source, record, data, _ = self.fixture(root)
            state = root / "cold-storage"; remote = Remote(); db = m.database(state, source, remote.namespace)
            m.archive_group(root, db, remote, [record], state, True)
            (root / "FEED.json").write_text(json.dumps(dict(source, session_id="another")))
            with self.assertRaisesRegex(RuntimeError, "restore_source_mismatch"):
                self.restore(root, data, state, remote)
            db.close()

    def test_restore_reserve_rejects_before_download_or_temporary_files(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d); source, record, data, evidence = self.fixture(root)
            state = root / 'cold-storage'; remote = Remote()
            db = m.database(state, source, remote.namespace)
            m.archive_group(root, db, remote, [record], state, True)
            reserve, margin = 1000, 100
            need = reserve + margin + m.MAX_BUNDLE + len(b'native events')
            with self.assertRaisesRegex(RuntimeError, 'restore_disk_reserve_reached'):
                self.restore(root, evidence, state, remote, reserve_bytes=reserve,
                             margin_bytes=margin, disk_usage=lambda _: SimpleNamespace(free=need - 1))
            self.assertEqual(remote.downloads, 0)
            self.assertFalse(data.exists() or evidence.exists())
            self.assertEqual(list(state.glob('restore-*')), [])
            db.close()

    def test_restore_space_loss_before_extraction_keeps_markers_and_cleans_download(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d); source, record, data, evidence = self.fixture(root)
            state = root / 'cold-storage'; remote = Remote()
            db = m.database(state, source, remote.namespace)
            m.archive_group(root, db, remote, [record], state, True)
            marker = evidence.parent / 'CUSTODY.json'; original = marker.read_bytes()
            free = iter([4 * m.MAX_BUNDLE, 4 * m.MAX_BUNDLE, 1100])
            with self.assertRaisesRegex(RuntimeError, 'restore_disk_reserve_reached'):
                self.restore(root, evidence, state, remote, reserve_bytes=1000, margin_bytes=100,
                             disk_usage=lambda _: SimpleNamespace(free=next(free)))
            self.assertEqual(remote.downloads, 1)
            self.assertFalse(data.exists() or evidence.exists())
            self.assertEqual(marker.read_bytes(), original)
            self.assertEqual(list(state.glob('restore-*')), [])
            db.close()

    def test_failed_download_removes_only_this_calls_partial_files(self):
        class FailedDownload(Remote):
            def download(self, key, path, sha, before_write):
                before_write(3)
                path.write_bytes(b'bad')
                raise OSError('connection_lost')
        with tempfile.TemporaryDirectory() as d:
            root = Path(d); source, record, data, evidence = self.fixture(root)
            state = root / 'cold-storage'; remote = FailedDownload()
            db = m.database(state, source, remote.namespace)
            m.archive_group(root, db, remote, [record], state, True)
            stale = state / 'restore-123.tar'; stale.write_bytes(b'prior interrupted evidence')
            with self.assertRaisesRegex(OSError, 'connection_lost'):
                self.restore(root, evidence, state, remote)
            self.assertEqual(stale.read_bytes(), b'prior interrupted evidence')
            self.assertEqual(list(state.glob('restore-*')), [stale])
            self.assertFalse(data.exists() or evidence.exists())
            db.close()

    def test_r2_download_reserve_check_stops_a_partial_transfer_before_next_write(self):
        class SmallReads(io.BytesIO):
            def read(self, size): return super().read(min(size, 32))
        with tempfile.TemporaryDirectory() as d:
            root = Path(d); source, record, data, evidence = self.fixture(root)
            state = root / 'cold-storage'; archived = Remote()
            db = m.database(state, source, archived.namespace)
            m.archive_group(root, db, archived, [record], state, True)
            remote = m.R2.__new__(m.R2)
            remote.namespace, remote.bucket = archived.namespace, 'bucket'
            remote.client = SimpleNamespace(get_object=lambda **_: {
                'Body': SmallReads(archived.objects[record['key']])})
            # Admission and the first write pass; the next write must stop.
            free = iter([4 * m.MAX_BUNDLE, 4 * m.MAX_BUNDLE, 1113])
            with self.assertRaisesRegex(RuntimeError, 'restore_disk_reserve_reached'):
                self.restore(root, evidence, state, remote, reserve_bytes=1000, margin_bytes=100,
                             disk_usage=lambda _: SimpleNamespace(free=next(free)))
            self.assertFalse(data.exists() or evidence.exists())
            self.assertEqual(list(state.glob('restore-*')), [])
            db.close()

    def test_concurrent_sessions_share_admission_and_recheck_free_space(self):
        started, release, second_started = threading.Event(), threading.Event(), threading.Event()
        free = [4 * m.MAX_BUNDLE]
        class HeldDownload(Remote):
            def download(self, key, path, sha, before_write):
                started.set()
                if not release.wait(5):
                    raise RuntimeError('download_not_released')
                super().download(key, path, sha, before_write)
                free[0] = 4096  # First restore can finish; a second bundle cannot be admitted.
        with tempfile.TemporaryDirectory() as d:
            parent = Path(d); roots = [parent / 'first', parent / 'second']
            remotes = [HeldDownload(), Remote()]; items = []; databases = []
            for root, remote in zip(roots, remotes):
                root.mkdir(); source, record, data, evidence = self.fixture(root)
                state = root / 'cold-storage'; db = m.database(state, source, remote.namespace)
                m.archive_group(root, db, remote, [record], state, True)
                items.append((root, evidence, state, remote)); databases.append(db)
            def call(index):
                if index: second_started.set()
                self.restore(*items[index], disk_usage=lambda _: SimpleNamespace(free=free[0]))
            try:
                with ThreadPoolExecutor(max_workers=2) as pool:
                    first = pool.submit(call, 0)
                    self.assertTrue(started.wait(2))
                    second = pool.submit(call, 1)
                    self.assertTrue(second_started.wait(2))
                    self.assertFalse(second.done())
                    release.set()
                    first.result(timeout=5)
                    with self.assertRaisesRegex(RuntimeError, 'restore_disk_reserve_reached'):
                        second.result(timeout=5)
                self.assertTrue(items[0][1].exists())
                self.assertFalse(items[1][1].exists())
                self.assertEqual([r.downloads for r in remotes], [1, 0])
            finally:
                release.set()
                for db in databases: db.close()

    def test_bundle_manifest_source_must_match_saved_receipt_before_publication(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d); source, record, data, evidence = self.fixture(root)
            state = root / 'cold-storage'; remote = Remote()
            db = m.database(state, source, remote.namespace)
            m.archive_group(root, db, remote, [record], state, True)
            def change(name, value):
                if name == 'BUNDLE.json':
                    bundle = json.loads(value)
                    bundle['banks'][0]['source']['session_id'] = 'another'
                    return json.dumps(bundle).encode()
                return value
            self.change_remote_bundle(remote, record, [data.parent, evidence.parent], change)
            with self.assertRaisesRegex(RuntimeError, 'bundle_receipt_mismatch'):
                self.restore(root, evidence, state, remote)
            self.assertFalse(data.exists() or evidence.exists())
            self.assertEqual(list(state.glob('restore-*')), [])
            db.close()

    def test_requested_member_hash_must_match_even_when_bundle_hash_matches(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d); source, record, data, evidence = self.fixture(root)
            state = root / 'cold-storage'; remote = Remote()
            db = m.database(state, source, remote.namespace)
            m.archive_group(root, db, remote, [record], state, True)
            def change(name, value):
                return b'wrong! events' if name == 'custody/source-events.json.zst' else value
            self.change_remote_bundle(remote, record, [data.parent, evidence.parent], change)
            with self.assertRaisesRegex(RuntimeError, 'archive_member_digest_mismatch'):
                self.restore(root, evidence, state, remote)
            self.assertFalse(data.exists() or evidence.exists())
            self.assertEqual(list(state.glob('restore-*')), [])
            db.close()

    def test_archive_database_cannot_be_rebound(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d)
            m.database(p, {"session_id": "one"}, "destination").close()
            with self.assertRaisesRegex(RuntimeError, "source_or_destination_changed"):
                m.database(p, {"session_id": "two"}, "destination")

    def test_archive_counters_use_compact_retired_covering_index(self):
        with tempfile.TemporaryDirectory() as d:
            state = Path(d)
            # Upgrade a prior database without changing its rows or records.
            with closing(sqlite3.connect(state / 'archive.sqlite')) as db, db:
                db.execute('CREATE TABLE archives(slot INTEGER,bank INTEGER,record TEXT,retired INTEGER DEFAULT 0,PRIMARY KEY(slot,bank))')
                db.execute('INSERT INTO archives VALUES(1,2,?,1)', ('large original record',))
            db = m.database(state, dict(session_id='session'), 'namespace')
            for query in ['SELECT count(*) FROM archives',
                          'SELECT count(*) FROM archives WHERE retired=1',
                          'SELECT count(*),coalesce(sum(retired),0) FROM archives']:
                plan = ' '.join(row[-1] for row in db.execute('EXPLAIN QUERY PLAN ' + query))
                self.assertIn('COVERING INDEX archives_retired', plan)
            self.assertEqual(db.execute('SELECT * FROM archives').fetchall(), [(1, 2, 'large original record', 1)])
            db.close()

    def test_recovery_outage_reservations_and_probe_cadence_survive_restarts(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d); db = m.database(root, {'session_id': 'session'}, 'namespace')
            circuit = m.RecoveryCircuit(db, {'session_id': 'session'}, 'namespace', 9, now_ms=1000)
            circuit.failed(m.BundleTransferTimeout(), 0, None, 1000)
            deadline = 31000
            for used, next_delay in [(1, 60), (2, 120), (3, 900)]:
                self.assertFalse(circuit.due(deadline - 1))
                self.assertEqual(circuit.reserve(deadline), 'retry')
                # A new connection sees the spent reservation before any child exists.
                with closing(sqlite3.connect(root / 'archive.sqlite')) as reader:
                    saved = json.loads(reader.execute('SELECT value FROM meta WHERE id=3').fetchone()[0])
                self.assertEqual(saved['burst_retries_used'], used)
                self.assertEqual(saved['reservation']['kind'], 'retry')
                expected = deadline + next_delay * 1000
                db.close(); db = m.database(root, {'session_id': 'session'}, 'namespace')
                circuit = m.RecoveryCircuit(db, {'session_id': 'session'}, 'namespace', 9, now_ms=deadline + 1)
                self.assertEqual(circuit.value['burst_retries_used'], used)
                self.assertEqual(circuit.value['next_attempt_unix_ms'], expected)
                self.assertEqual(circuit.value['outage_since_unix_ms'], 0)
                deadline = expected
            self.assertEqual(circuit.value['state'], 'OPEN')
            self.assertFalse(circuit.due(deadline - 1))
            self.assertEqual(circuit.reserve(deadline), 'probe')
            db.close(); db = m.database(root, {'session_id': 'session'}, 'namespace')
            circuit = m.RecoveryCircuit(db, {'session_id': 'session'}, 'namespace', 9, now_ms=deadline + 1)
            self.assertFalse(circuit.due(deadline + 899999))
            self.assertEqual(circuit.reserve(deadline + 900000), 'probe')
            circuit.failed(m.BundleTransferTimeout(), deadline, 'probe', deadline + 900001)
            self.assertGreaterEqual(circuit.value['next_attempt_unix_ms'], deadline + 1800001)
            empty = SimpleNamespace(pending={}, recovery=circuit)
            status = m.progress_status(empty, None, deadline + 900001)
            self.assertEqual(status['storage_progress'], 'STALLED')
            self.assertEqual(status['eligible_wait_since_unix_ms'], 0)
            db.close()

    def test_recovery_reset_needs_sixty_fresh_seconds_and_later_durable_progress(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d); source = {'session_id': 'session'}; db = m.database(root, source, 'namespace')
            circuit = m.RecoveryCircuit(db, source, 'namespace', None, now_ms=1000)
            circuit.failed(m.BundleTransferTimeout(), 0, None, 1000)
            circuit.reserve(31000); circuit.succeeded('retry', 32000)
            circuit.tick(92000, False)
            self.assertEqual(circuit.value['burst_retries_used'], 1)
            db.execute("INSERT INTO archives(slot,bank,record,retired) VALUES(1,2,'{}',1)"); db.commit()
            circuit.tick(91999, False)
            self.assertEqual(circuit.value['state'], 'RECOVERING')
            circuit.tick(92000, True)
            self.assertEqual(circuit.value['state'], 'RECOVERING')
            # Restart cannot count stopped time as observed healthy time.
            circuit = m.RecoveryCircuit(db, source, 'namespace', None, now_ms=92000)
            circuit.tick(152000, False)
            self.assertEqual(circuit.value['burst_retries_used'], 1)
            db.execute("INSERT INTO archives(slot,bank,record,retired) VALUES(3,4,'{}',1)"); db.commit()
            circuit.tick(152000, False)
            self.assertEqual(circuit.value['state'], 'NORMAL')
            self.assertIsNone(circuit.value['outage_since_unix_ms'])
            self.assertEqual(circuit.value['burst_retries_used'], 0)
            db.close()

    def test_recovery_rejects_missing_corrupt_foreign_and_permanent_retry_state(self):
        mutations = [
            lambda db, v: db.execute('DELETE FROM meta WHERE id=3'),
            lambda db, v: db.execute('DELETE FROM meta WHERE id=4'),
            lambda db, v: db.execute("UPDATE meta SET value='broken' WHERE id=3"),
            lambda db, v: v.update(last_error=dict(error_type='ClientError', http_status=403, network_error_type=None)),
            lambda db, v: v.update(last_error=dict(error_type='NoCredentialsError', http_status=None, network_error_type=None)),
            lambda db, v: v.update(last_error=dict(error_type='ReadTimeoutError', http_status=403, network_error_type=None)),
            lambda db, v: v.update(next_attempt_unix_ms=1001),
            lambda db, v: v.update(state='OPEN'),
            lambda db, v: v.update(reservation=dict(kind='probe', reserved_unix_ms=1000)),
            lambda db, v: v['binding'].update(max_archived_banks=10),
            lambda db, v: db.execute("UPDATE meta SET value=replace(value,'\"schema\":','\"schema\":\"bad\",\"schema\":') WHERE id=4"),
        ]
        for mutate in mutations:
            with self.subTest(mutation=mutations.index(mutate)), tempfile.TemporaryDirectory() as d:
                db = m.database(Path(d), {'session_id': 'session'}, 'namespace')
                circuit = m.RecoveryCircuit(db, {'session_id': 'session'}, 'namespace', 9, now_ms=1000)
                circuit.failed(m.BundleTransferTimeout(), 0, None, 1000)
                value = json.loads(db.execute('SELECT value FROM meta WHERE id=3').fetchone()[0])
                before = json.dumps(value, sort_keys=True)
                mutate(db, value)
                if json.dumps(value, sort_keys=True) != before:
                    db.execute('UPDATE meta SET value=? WHERE id=3', (json.dumps(value),))
                db.commit()
                with self.assertRaises(m.RecoveryStateError):
                    m.RecoveryCircuit(db, {'session_id': 'session'}, 'namespace', 9, now_ms=2000)
                db.close()

    def test_recovery_permanent_fault_is_durably_blocked_across_restart(self):
        for kind, http in [('ClientError', 403), ('NoCredentialsError', None), ('RuntimeError', None),
                           ('IncompleteReadError', None), ('ResponseStreamingError', None)]:
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as d:
                root = Path(d); db = m.database(root, {'session_id': 'session'}, 'namespace')
                circuit = m.RecoveryCircuit(db, {'session_id': 'session'}, 'namespace', None, now_ms=1000)
                circuit.failed(m.BundleTransferTimeout(), 0, None, 1000)
                error = m.BundleTransferError(dict(error_type=kind, http_status=http, network_error_type=None))
                with self.assertRaises(m.BundleTransferError): circuit.failed(error, 0, None, 2000)
                self.assertEqual(circuit.value['state'], 'BLOCKED')
                db.close(); db = m.database(root, {'session_id': 'session'}, 'namespace')
                with self.assertRaises(m.RecoveryBlocked):
                    m.RecoveryCircuit(db, {'session_id': 'session'}, 'namespace', None, now_ms=3000)
                db.close()

    def test_recovery_parallel_failures_share_budget_keep_caps_and_commit_only_proofs(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d).resolve(); source, items, _ = self.archive_fixture(root, count=2)
            remote = self.isolated_remote(root); state = root / 'cold-storage'; db = m.database(state, source, remote.namespace)
            now = [1000]; circuit = m.RecoveryCircuit(db, source, remote.namespace, 2, now_ms=now[0])
            transfers = m.Transfers(root, db, remote, state, True, 2, circuit, lambda: now[0])
            calls = {}; lock = threading.Lock()
            def result(command, request):
                with lock:
                    calls[request['path']] = calls.get(request['path'], 0) + 1
                    attempt = calls[request['path']]
                if attempt == 1: raise m.BundleTransferTimeout()
                with closing(sqlite3.connect(state / 'archive.sqlite')) as reader:
                    ledger = json.loads(reader.execute('SELECT value FROM meta WHERE id=3').fetchone()[0])
                self.assertIsNotNone(ledger['reservation'])
                return 0, {k: v for k, v in request.items() if k != 'path'}
            def finished():
                for future, data in list(transfers.pending.items()):
                    if not data['retry_wait']:
                        try: future.result(timeout=3)
                        except m.BundleTransferTimeout: pass
            try:
                with patch.object(m, 'transfer_process', side_effect=result):
                    transfers.submit([items[0][0]]); transfers.submit([items[1][0]])
                    finished(); self.assertEqual(transfers.collect(), [])
                    self.assertEqual(len(transfers.pending), 2)
                    self.assertFalse(transfers.can_submit())
                    self.assertEqual(m.archive_allowance(0, transfers.pending_banks(), 2), 0)
                    self.assertTrue(all(data['submitted_unix_ms'] == 1000 for data in transfers.pending.values()))
                    self.assertTrue(all(path.exists() for _, *paths in items for path in paths))
                    now[0] = 31000; transfers.collect(); finished(); transfers.collect()
                    self.assertEqual(circuit.value['burst_retries_used'], 1)
                    self.assertEqual(circuit.value['state'], 'RECOVERING')
                    self.assertEqual(m.archive_allowance(1, transfers.pending_banks(), 2), 0)
                    now[0] = 91000; transfers.collect(); finished(); transfers.collect()
                self.assertEqual(circuit.value['burst_retries_used'], 2)
                self.assertEqual(db.execute('SELECT count(*),sum(retired) FROM archives').fetchone(), (2, 2))
                self.assertEqual(transfers.pending_banks(), set())
            finally: transfers.close(); db.close()

    def test_recovery_successful_old_sibling_does_not_reset_outage(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d).resolve(); source, items, _ = self.archive_fixture(root, count=2)
            remote = self.isolated_remote(root); state = root / 'cold-storage'; db = m.database(state, source, remote.namespace)
            circuit = m.RecoveryCircuit(db, source, remote.namespace, 2, now_ms=1000)
            transfers = m.Transfers(root, db, remote, state, True, 2, circuit, lambda: 1000)
            def result(command, request):
                if request['path'].endswith('bundle-0.tar'): raise m.BundleTransferTimeout()
                return 0, {k: v for k, v in request.items() if k != 'path'}
            try:
                with patch.object(m, 'transfer_process', side_effect=result):
                    transfers.submit([items[0][0]]); transfers.submit([items[1][0]])
                    for future in transfers.pending:
                        try: future.result(timeout=3)
                        except m.BundleTransferTimeout: pass
                    self.assertEqual(len(transfers.collect()), 1)
                circuit.tick(100000, True)
                self.assertEqual(circuit.value['state'], 'BACKOFF')
                self.assertIsNone(circuit.value['healthy_since_unix_ms'])
                self.assertEqual(circuit.value['outage_since_unix_ms'], 1000)
                self.assertEqual(db.execute('SELECT count(*),sum(retired) FROM archives').fetchone(), (1, 1))
            finally: transfers.close(); db.close()

    def test_recovery_restart_allows_only_one_due_probe_group_then_real_progress(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d).resolve(); source, items, _ = self.archive_fixture(root, count=2)
            remote = self.isolated_remote(root); state = root / 'cold-storage'; db = m.database(state, source, remote.namespace)
            now = [1000]; circuit = m.RecoveryCircuit(db, source, remote.namespace, 2, now_ms=now[0])
            circuit.failed(m.BundleTransferTimeout(), 0, None, now[0])
            for _ in range(3):
                now[0] = circuit.value['next_attempt_unix_ms']; kind = circuit.reserve(now[0])
                circuit.failed(m.BundleTransferTimeout(), 0, kind, now[0])
            circuit = m.RecoveryCircuit(db, source, remote.namespace, 2, now_ms=now[0])
            transfers = m.Transfers(root, db, remote, state, True, 2, circuit, lambda: now[0])
            def result(command, request): return 0, {k: v for k, v in request.items() if k != 'path'}
            try:
                self.assertFalse(transfers.can_submit())
                now[0] = circuit.value['next_attempt_unix_ms']
                with patch.object(m, 'transfer_process', side_effect=result):
                    self.assertTrue(transfers.submit([items[0][0]]))
                    self.assertFalse(transfers.submit([items[1][0]]))
                    self.assertEqual(circuit.value['reservation']['kind'], 'probe')
                    next(iter(transfers.pending)).result(timeout=3); transfers.collect()
                self.assertEqual(circuit.value['state'], 'RECOVERING')
                self.assertEqual(circuit.value['burst_retries_used'], 3)
                self.assertTrue(transfers.can_submit())
                self.assertEqual(db.execute('SELECT count(*),sum(retired) FROM archives').fetchone(), (1, 1))
                self.assertEqual(m.archive_allowance(1, transfers.pending_banks(), 2), 1)
            finally: transfers.close(); db.close()

    def test_recovery_lost_put_reply_then_412_requires_full_readback_before_retirement(self):
        boto, _ = self.r2_errors()
        with tempfile.TemporaryDirectory() as d:
            root = Path(d).resolve(); source, record, data, evidence = self.fixture(root)
            remote = self.isolated_remote(root); remote.bucket = 'bucket'; state = root / 'cold-storage'
            db = m.database(state, source, remote.namespace); now = [1000]
            circuit = m.RecoveryCircuit(db, source, remote.namespace, 1, now_ms=now[0])
            transfers = m.Transfers(root, db, remote, state, True, 2, circuit, lambda: now[0])
            stored, bodies = [], []
            def put(**kwargs):
                if not stored:
                    stored.append(kwargs['Body'].read()); raise boto.ReadTimeoutError('DO_NOT_PRINT')
                error = boto.ClientError('DO_NOT_PRINT'); error.response = {'ResponseMetadata': {'HTTPStatusCode': 412}}
                raise error
            def get(**kwargs):
                body = io.BytesIO(stored[0]); bodies.append(body); return {'Body': body}
            remote.client = SimpleNamespace(put_object=Mock(side_effect=put), get_object=Mock(side_effect=get))
            def child(command, request):
                try: remote._put_verified_in_process(request['key'], Path(request['path']), request['sha256'])
                except Exception as error: return 1, dict(schema=m.TRANSFER_SCHEMA, state='FAILED', **m.failure_fields(error))
                return 0, {k: v for k, v in request.items() if k != 'path'}
            try:
                with patch.object(m, 'transfer_process', side_effect=child):
                    transfers.submit([record])
                    for future in transfers.pending:
                        with self.assertRaises(m.BundleTransferError): future.result(timeout=3)
                    transfers.collect()
                    self.assertTrue(data.exists() and evidence.exists())
                    self.assertEqual(db.execute('SELECT count(*) FROM archives').fetchone()[0], 0)
                    self.assertEqual(remote.client.get_object.call_count, 0)
                    now[0] = 31000; transfers.collect()
                    for future, entry in transfers.pending.items():
                        if not entry['retry_wait']: future.result(timeout=3)
                    transfers.collect()
                self.assertEqual(remote.client.put_object.call_count, 2)
                self.assertEqual(remote.client.get_object.call_count, 1)
                self.assertTrue(bodies[0].closed)
                self.assertEqual(db.execute('SELECT count(*),sum(retired) FROM archives').fetchone(), (1, 1))
                self.assertFalse(data.exists() or evidence.exists())
            finally: transfers.close(); db.close()

    def test_recovery_crash_after_verified_index_resumes_partial_retirement_without_upload(self):
        class Crash(BaseException): pass
        with tempfile.TemporaryDirectory() as d:
            root = Path(d).resolve(); source, items, options = self.archive_fixture(root, count=1)
            record, data, evidence = items[0]; remote = Remote(); state = root / 'cold-storage'
            remote.put_verified = Mock(wraps=remote.put_verified)
            db = m.database(state, source, remote.namespace)
            circuit = m.RecoveryCircuit(db, source, remote.namespace, 1, now_ms=1000)
            transfers = m.Transfers(root, db, remote, state, True, 2, circuit, lambda: 1000)
            unlink = Path.unlink
            def crash(path, *args, **kwargs):
                result = unlink(path, *args, **kwargs)
                if path == data: raise Crash()
                return result
            try:
                transfers.submit([record]); next(iter(transfers.pending)).result(timeout=3)
                with patch.object(Path, 'unlink', crash), self.assertRaises(Crash): transfers.collect()
                self.assertEqual(db.execute('SELECT count(*),sum(retired) FROM archives').fetchone(), (1, 0))
                self.assertFalse(data.exists()); self.assertTrue(evidence.exists())
                self.assertEqual(circuit.value['state'], 'NORMAL')
            finally: transfers.close(); db.close()
            options.max_archived_banks = 1
            with patch.object(m, 'R2', return_value=remote), patch('builtins.print'): m.archive(options)
            with closing(m.database(state, source, remote.namespace)) as db:
                self.assertEqual(db.execute('SELECT count(*),sum(retired) FROM archives').fetchone(), (1, 1))
            self.assertEqual(remote.put_verified.call_count, 1)
            self.assertFalse(data.exists() or evidence.exists())

    def test_recovery_nonfollow_restart_keeps_outage_and_waits_for_due_probe(self):
        class StopAfterWait(BaseException): pass
        with tempfile.TemporaryDirectory() as d:
            root = Path(d).resolve(); source, items, options = self.archive_fixture(root, count=1)
            state = root / 'cold-storage'; remote = Remote(); remote.put_verified = Mock(wraps=remote.put_verified)
            db = m.database(state, source, remote.namespace)
            circuit = m.RecoveryCircuit(db, source, remote.namespace, None, now_ms=100000)
            circuit.failed(m.BundleTransferTimeout(), 0, None, 100000); db.close()
            options.follow = False
            with patch.object(m, 'R2', return_value=remote), patch.object(m.time, 'time', return_value=100), \
                 patch.object(m.time, 'sleep', side_effect=StopAfterWait) as sleep, \
                 self.assertRaises(StopAfterWait): m.archive(options)
            sleep.assert_called_once_with(1)
            status = json.loads((state / 'STATUS.json').read_text())
            self.assertEqual(status['state'], 'RUNNING')
            self.assertEqual(status['recovery']['state'], 'BACKOFF')
            self.assertEqual(status['eligible_wait_since_unix_ms'], 0)
            self.assertEqual(status['recovery']['burst_retries_used'], 0)
            remote.put_verified.assert_not_called()
            self.assertTrue(all(path.exists() for _, *paths in items for path in paths))

    def test_recovery_source_gate_fault_outside_transfer_durably_blocks(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d).resolve(); source, _, options = self.archive_fixture(root, count=1)
            path = options.publisher_state / 'CHECKPOINT.json'
            value = json.loads(path.read_text()); value['session'] = 'wrong'; path.write_text(json.dumps(value))
            with patch.object(m, 'R2', return_value=Remote()), patch('builtins.print'), self.assertRaises(RuntimeError):
                m.archive(options)
            with closing(sqlite3.connect(root / 'cold-storage/archive.sqlite')) as db:
                value = json.loads(db.execute('SELECT value FROM meta WHERE id=3').fetchone()[0])
            self.assertEqual(value['state'], 'BLOCKED')
            with patch.object(m, 'R2', return_value=Remote()), patch('builtins.print'), self.assertRaises(RuntimeError):
                m.archive(options)
            failure = json.loads((root / 'cold-storage/LAST-FAILURE.json').read_text())
            self.assertEqual(failure['error_type'], 'RecoveryBlocked')

    def test_recovery_latch_write_fault_still_reports_safe_failed_evidence(self):
        with tempfile.TemporaryDirectory() as d:
            state = Path(d)
            recovery = SimpleNamespace(block=Mock(side_effect=sqlite3.OperationalError('DO_NOT_PRINT')),
                                       status=lambda: {'state': 'BLOCKED'})
            def fail(options, resources, context):
                context.update(state=state, source={'session_id': 'session'}, recovery=recovery)
                raise RuntimeError('DO_NOT_PRINT signed URL')
            with patch.object(m, 'archive_open', side_effect=fail), patch('builtins.print') as output, self.assertRaises(RuntimeError):
                m.archive(None)
            saved = json.loads((state / 'LAST-FAILURE.json').read_text())
            self.assertEqual(saved['state'], 'FAILED')
            self.assertFalse(saved['recovery_state_persisted'])
            self.assertNotIn('DO_NOT_PRINT', json.dumps(saved))
            self.assertNotIn('DO_NOT_PRINT', str(output.call_args_list))

    def test_recovery_failed_latch_write_is_not_claimed_durable_and_can_be_retried(self):
        with tempfile.TemporaryDirectory() as d:
            db = m.database(Path(d), {'session_id': 'session'}, 'namespace')
            circuit = m.RecoveryCircuit(db, {'session_id': 'session'}, 'namespace', None, now_ms=1000)
            class OnceFailingDB:
                failed = False
                def execute(self, sql, *args):
                    if sql.startswith('INSERT OR REPLACE INTO meta') and not self.failed:
                        self.failed = True; raise sqlite3.OperationalError('DO_NOT_PRINT')
                    return db.execute(sql, *args)
                def commit(self): return db.commit()
                def rollback(self): return db.rollback()
            circuit.db = OnceFailingDB()
            with self.assertRaises(sqlite3.OperationalError): circuit.block(RuntimeError('DO_NOT_PRINT'))
            self.assertFalse(circuit.persisted)
            self.assertEqual(json.loads(db.execute('SELECT value FROM meta WHERE id=3').fetchone()[0])['state'], 'NORMAL')
            circuit.block(OSError('secondary error'))
            self.assertTrue(circuit.persisted)
            value = json.loads(db.execute('SELECT value FROM meta WHERE id=3').fetchone()[0])
            self.assertEqual(value['state'], 'BLOCKED')
            self.assertEqual(value['last_error']['error_type'], 'RuntimeError')
            self.assertNotIn('DO_NOT_PRINT', json.dumps(value))
            db.close()

    def test_symlink_and_traversal_paths_reject(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            (root / "link").symlink_to("/tmp", target_is_directory=True)
            for p in [root / "../outside", root / "link/file"]:
                with self.assertRaises(RuntimeError): m.checked(root, p)


if __name__ == "__main__": unittest.main()
