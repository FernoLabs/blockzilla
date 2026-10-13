#!/usr/bin/env python3
"""Download a verified, resumable local CAR cache. No benchmark runs are started.

Example: download_car.py --dataset-manifest dataset-1000.json --operator-trusted
  --directory /home/ubuntu/benchmarks/zilla-triton-20260930/cache-epoch1000

Eight workers stream disjoint 1 GiB ranges into sparse files. Durable SHA-256
checkpoints preserve completed and partial jobs. A final full read checks every
job hash and computes each object hash before publishing manifest.json. The
operator-trusted option accepts the lack of a strong remote object identity;
local hashes cannot prove that an unversioned remote CAR stayed immutable.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timezone
from email.utils import parsedate_to_datetime
import fcntl
import hashlib
import http.client
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import stat
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

from run_full_epoch import validate_dataset


EMPTY_SHA = hashlib.sha256(b'').hexdigest()
STAT_FIELDS = ('st_dev', 'st_ino', 'st_size', 'st_mtime_ns', 'st_ctime_ns')


class InvalidSource(ValueError):
    """A protocol, identity, or content error that must not be retried."""


class DownloadStopped(RuntimeError):
    pass


def require(condition, message):
    if not condition:
        raise InvalidSource(message)


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, _request, _file, _code, _message, _headers, _url):
        return None


def atomic_json(path, value):
    temporary = path.with_name(path.name + '.tmp')
    with temporary.open('w') as stream:
        json.dump(value, stream, indent=2)
        stream.write('\n')
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def file_stat(path):
    value = path.stat(follow_symlinks=False)
    require(stat.S_ISREG(value.st_mode), f'cache object is not a regular file: {path.name}')
    return {key: getattr(value, key) for key in STAT_FIELDS}


def retry_after(value, now=None):
    """Respect a nonnegative delay or HTTP date; otherwise wait 60 seconds."""
    if value is not None:
        value = value.strip()
        if value.isascii() and value.isdigit():
            return int(value)
        try:
            parsed = parsedate_to_datetime(value)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return max(0.0, parsed.timestamp() - (time.time() if now is None else now))
        except (ValueError, TypeError, OverflowError):
            pass
    return 60


class Cooldown:
    def __init__(self, stop, deadline):
        self.stop, self.deadline = stop, deadline
        self.lock = threading.Lock()
        self.until = 0.0

    def defer(self, seconds):
        with self.lock:
            self.until = max(self.until, time.monotonic() + seconds)

    def check(self):
        if self.stop.is_set():
            raise DownloadStopped('download stopped; durable checkpoints are retained')
        if time.monotonic() >= self.deadline:
            raise DownloadStopped('download exceeded its configured time limit')

    def wait(self):
        while True:
            self.check()
            with self.lock:
                remaining = self.until - time.monotonic()
            if remaining <= 0:
                return
            self.stop.wait(min(remaining, 0.25))


def header(response, name):
    values = response.headers.get_all(name, [])
    require(len(values) <= 1, f'duplicate {name} response header')
    return values[0].strip() if values else None


def response_identity(response, url):
    require(response.geturl() == url, 'source redirect is forbidden')
    require((header(response, 'Content-Encoding') or 'identity').lower() == 'identity',
            'source returned encoded bytes')
    require(header(response, 'Transfer-Encoding') is None, 'source must send an exact Content-Length')


def strong_etag(value):
    return value if value and value.startswith('"') and value.endswith('"') else None


def put_bytes(fd, data, position):
    view = memoryview(data)
    while view:
        count = os.pwrite(fd, view, position)
        require(count > 0, 'pwrite did not make progress')
        view, position = view[count:], position + count


def hash_span(fd, start, length, whole=None, check=lambda: None):
    digest = hashlib.sha256()
    position = start
    while position < start + length:
        check()
        data = os.pread(fd, min(8 << 20, start + length - position), position)
        require(data, f'local file ends before byte {start + length}')
        digest.update(data)
        if whole is not None:
            whole.update(data)
        position += len(data)
    return digest


class Downloader:
    def __init__(self, dataset_path, directory, *, base='https://files.old-faithful.net/',
                 threads=8, job_bytes=1 << 30, checkpoint_bytes=64 << 20, max_attempts=6,
                 timeout=120, max_seconds=86400, operator_trusted=False, allow_loopback=False):
        require(operator_trusted, 'explicit --operator-trusted is required for this unversioned source')
        parsed = urllib.parse.urlsplit(base)
        require(parsed.scheme == 'https' or (allow_loopback and parsed.scheme == 'http'
                and parsed.hostname == '127.0.0.1'), 'base must use HTTPS; test HTTP requires literal 127.0.0.1')
        require(parsed.hostname and not parsed.username and not parsed.password and not parsed.query
                and not parsed.fragment and base.endswith('/'), 'invalid source base URL')
        require(1 <= threads <= 32 and 1 <= max_attempts <= 64 and min(job_bytes, checkpoint_bytes, timeout, max_seconds) > 0,
                'invalid worker, job, checkpoint, retry, or timeout setting')
        self.dataset_bytes = Path(dataset_path).read_bytes()
        self.dataset = validate_dataset(json.loads(self.dataset_bytes))
        self.dataset_sha = hashlib.sha256(self.dataset_bytes).hexdigest()
        self.directory, self.base = Path(directory), base
        self.threads, self.job_bytes, self.checkpoint_bytes = threads, job_bytes, checkpoint_bytes
        self.max_attempts, self.timeout = max_attempts, timeout
        self.stop = threading.Event()
        self.gate = Cooldown(self.stop, time.monotonic() + max_seconds)
        self.lock = threading.RLock()
        self.state, self.fds = {}, {}
        self.last_progress = 0.0
        epoch = self.dataset['epoch']
        self.car = f'epoch-{epoch}.car'
        self.index = f'epoch-{epoch}-slot-ranges.raw'
        self.lengths = {self.car: self.dataset['car_bytes'], self.index: self.dataset['index_bytes']}
        self.state_path = self.directory / 'download-state.json'
        self.manifest_path = self.directory / 'manifest.json'

    def save(self):
        with self.lock:
            self.state['updated_at'] = time.time()
            atomic_json(self.state_path, self.state)

    def event(self, kind, **details):
        with self.lock:
            event = {'time': time.time(), 'kind': kind, **details}
            self.state['event_count'] = self.state.get('event_count', 0) + 1
            self.state.setdefault('recent_events', []).append(event)
            self.state['recent_events'] = self.state['recent_events'][-128:]
            with (self.directory / 'events.jsonl').open('a') as stream:
                stream.write(json.dumps(event) + '\n')
                stream.flush()
                os.fsync(stream.fileno())
            print(json.dumps(event), file=sys.stderr, flush=True)
            self.save()

    def request(self, name, method='GET', start=None, end=None):
        self.gate.wait()
        url = f"{self.base}{self.dataset['epoch']}/{name}"
        headers = {'Accept-Encoding': 'identity', 'User-Agent': 'blockzilla-local-car-cache/1'}
        if start is not None:
            headers['Range'] = f'bytes={start}-{end - 1}'
            etag = strong_etag(self.state['objects'][name].get('source_etag'))
            if etag:
                headers['If-Match'] = etag
        request = urllib.request.Request(url, method=method, headers=headers)
        response = urllib.request.build_opener(NoRedirect()).open(request, timeout=self.timeout)
        try:
            response_identity(response, url)
            if start is None:
                require(response.status == 200, f'{name}: HEAD must return HTTP 200')
                require(header(response, 'Content-Length') == str(self.lengths[name]), f'{name}: source length changed')
            else:
                require(response.status == 206, f'{name}: range must return HTTP 206, got {response.status}')
                require(header(response, 'Content-Length') == str(end - start), f'{name}: range Content-Length differs')
                require(header(response, 'Content-Range') == f'bytes {start}-{end - 1}/{self.lengths[name]}',
                        f'{name}: Content-Range differs from the exact request')
                etag = strong_etag(self.state['objects'][name].get('source_etag'))
                require(etag is None or header(response, 'ETag') == etag, f'{name}: range ETag changed')
            return response
        except BaseException:
            response.close()
            raise

    def network_retry(self, error, name, attempt, start=None):
        delay = min(2 ** (attempt - 1), 8)
        if isinstance(error, urllib.error.HTTPError):
            code = error.code
            after = error.headers.get('Retry-After')
            error.close()
            if code == 429:
                delay = retry_after(after)
                self.gate.defer(delay)
                self.event('global-http-429-cooldown', object=name, attempt=attempt, start=start,
                           retry_after=after, cooldown_seconds=delay)
            elif code in (500, 502, 503, 504):
                self.event('http-retry', object=name, attempt=attempt, start=start, status=code)
            else:
                raise error
        elif isinstance(error, (urllib.error.URLError, ConnectionError, TimeoutError,
                                http.client.IncompleteRead, http.client.RemoteDisconnected, socket.timeout)):
            self.event('body-or-connection-retry', object=name, attempt=attempt, start=start,
                       error_type=type(error).__name__, reason=str(error))
        else:
            raise error
        if attempt >= self.max_attempts:
            raise DownloadStopped(f'{name}: exhausted {self.max_attempts} attempts; resume data retained') from error
        if isinstance(error, urllib.error.HTTPError) and error.code == 429:
            self.gate.wait()
        elif self.stop.wait(delay):
            self.gate.check()

    def observe_source(self, name):
        for attempt in range(1, self.max_attempts + 1):
            try:
                with self.request(name, method='HEAD') as response:
                    etag = header(response, 'ETag')
                with self.lock:
                    obj = self.state['objects'][name]
                    if obj.get('source_observed'):
                        require(obj.get('source_etag') == etag, f'{name}: source ETag changed across observations')
                    obj.update(source_etag=etag, source_observed=True)
                    self.save()
                return
            except Exception as error:
                self.network_retry(error, name, attempt)

    def checkpoint(self, name, job, position, digest):
        os.fsync(self.fds[name])
        with self.lock:
            job.update(completed_bytes=position - job['start'], sha256=digest.hexdigest())
            self.save()
            now = time.monotonic()
            if now - self.last_progress >= 5:
                done = sum(j['completed_bytes'] for obj in self.state['objects'].values() for j in obj['jobs'])
                print(f'durable download bytes: {done}/{sum(self.lengths.values())}', flush=True)
                self.last_progress = now

    def download_job(self, name, job):
        self.gate.check()
        fd = self.fds[name]
        digest = hash_span(fd, job['start'], job['completed_bytes'], check=self.gate.check)
        require(digest.hexdigest() == job['sha256'], f'{name}: saved partial-job SHA-256 differs at {job["start"]}')
        position = job['start'] + job['completed_bytes']
        if position == job['end']:
            return
        last_checkpoint = position
        while position < job['end']:
            self.gate.check()
            require(job['attempts'] < self.max_attempts,
                    f'{name}: job {job["start"]} exhausted saved attempts; raise --max-attempts explicitly to resume')
            with self.lock:
                job['attempts'] += 1
                attempt = job['attempts']
                self.save()
            try:
                with self.request(name, start=position, end=job['end']) as response:
                    while position < job['end']:
                        self.gate.check()
                        try:
                            data = response.read(min(1 << 20, job['end'] - position))
                        except http.client.IncompleteRead as error:
                            data = error.partial
                            require(len(data) <= job['end'] - position, 'incomplete response exceeds its requested range')
                            if data:
                                put_bytes(fd, data, position)
                                digest.update(data)
                                position += len(data)
                            raise
                        if not data:
                            raise http.client.IncompleteRead(b'', job['end'] - position)
                        require(len(data) <= job['end'] - position, 'response body exceeds its requested range')
                        put_bytes(fd, data, position)
                        digest.update(data)
                        position += len(data)
                        if position - last_checkpoint >= self.checkpoint_bytes:
                            self.checkpoint(name, job, position, digest)
                            last_checkpoint = position
                    require(response.read(1) == b'', 'response contains bytes beyond its declared range')
                self.checkpoint(name, job, position, digest)
                return
            except BaseException as error:
                self.checkpoint(name, job, position, digest)
                last_checkpoint = position
                if isinstance(error, Exception):
                    self.network_retry(error, name, attempt, position)
                else:
                    raise

    def prepare(self):
        if self.state_path.exists():
            self.state = json.loads(self.state_path.read_text())
            require(self.state.get('schema') == 'zilla-car-download-state-v1'
                    and self.state.get('dataset_manifest_sha256') == self.dataset_sha
                    and self.state.get('base') == self.base and self.state.get('job_bytes') == self.job_bytes,
                    'resume identity or job size differs from the saved download')
            require(set(self.state.get('objects', {})) == set(self.lengths), 'resume object list differs')
        else:
            self.state = {'schema': 'zilla-car-download-state-v1', 'state': 'PREPARING',
                          'epoch': self.dataset['epoch'], 'dataset_manifest_sha256': self.dataset_sha,
                          'base': self.base, 'job_bytes': self.job_bytes, 'objects': {}}
            for name, length in self.lengths.items():
                # Triton can answer200 when Range spans the complete index.
                # Keep both index requests partial and retain strict206 checks.
                step = self.job_bytes if name == self.car else length // 2
                self.state['objects'][name] = {'bytes': length, 'jobs': [
                    {'start': start, 'end': min(start + step, length), 'completed_bytes': 0,
                     'sha256': EMPTY_SHA, 'attempts': 0} for start in range(0, length, step)]}
            require(not any((self.directory / name).exists() for name in self.lengths),
                    'cache objects exist without a download state; refusing to replace them')
            self.save()
        saved_dataset = self.directory / 'dataset-manifest.json'
        if saved_dataset.exists():
            require(hashlib.sha256(saved_dataset.read_bytes()).hexdigest() == self.dataset_sha,
                    'saved dataset manifest differs from the accepted input')
        else:
            temporary = saved_dataset.with_name(saved_dataset.name + '.tmp')
            with temporary.open('wb') as output:
                output.write(self.dataset_bytes)
                output.flush()
                os.fsync(output.fileno())
            temporary.replace(saved_dataset)
        for name, length in self.lengths.items():
            obj = self.state['objects'][name]
            require(obj.get('bytes') == length, 'saved object length differs')
            cursor = 0
            for job in obj['jobs']:
                require(type(job.get('start')) is int and type(job.get('end')) is int
                        and job['start'] == cursor < job['end'] <= length, 'saved job coverage has a gap or overlap')
                require(type(job.get('completed_bytes')) is int and 0 <= job['completed_bytes'] <= job['end'] - cursor
                        and type(job.get('attempts')) is int and 0 <= job['attempts'] <= 64
                        and isinstance(job.get('sha256'), str) and len(job['sha256']) == 64,
                        'invalid saved job progress')
                cursor = job['end']
            require(cursor == length, 'saved jobs do not cover the complete object')
            path = self.directory / name
            flags = os.O_RDWR | os.O_NOFOLLOW
            if path.exists():
                require(file_stat(path)['st_size'] == length, 'existing cache object has an incorrect length')
                os.chmod(path, 0o600, follow_symlinks=False)
            else:
                require(not any(job['completed_bytes'] for job in obj['jobs']), 'saved bytes are missing from disk')
                flags |= os.O_CREAT | os.O_EXCL
            fd = os.open(path, flags, 0o600)
            self.fds[name] = fd
            if os.fstat(fd).st_size == 0:
                os.ftruncate(fd, length)
                os.fsync(fd)
        remaining = sum(obj['bytes'] - sum(job['completed_bytes'] for job in obj['jobs'])
                        for obj in self.state['objects'].values())
        require(shutil.disk_usage(self.directory).free >= remaining, 'insufficient free disk space for the remaining input')
        self.state.update(state='DOWNLOADING', threads=self.threads, max_attempts=self.max_attempts,
                          checkpoint_bytes=self.checkpoint_bytes)
        self.save()

    def verify_and_publish(self):
        self.state['state'] = 'VERIFYING'
        self.save()
        files = {}
        for name, length in self.lengths.items():
            path = self.directory / name
            os.chmod(path, 0o444, follow_symlinks=False)
            before = file_stat(path)
            require(before['st_size'] == length, 'local file length differs')
            whole = hashlib.sha256()
            with path.open('rb') as stream:
                for job in self.state['objects'][name]['jobs']:
                    require(job['completed_bytes'] == job['end'] - job['start'], 'cannot publish an incomplete job')
                    actual = hash_span(stream.fileno(), job['start'], job['completed_bytes'], whole, self.gate.check)
                    require(actual.hexdigest() == job['sha256'], f'{name}: final job SHA-256 mismatch at {job["start"]}')
                    print(f'verified local bytes: {name} {job["end"]}/{length}', flush=True)
            require(file_stat(path) == before, 'cache file changed during complete verification')
            files[name] = {'bytes': length, 'sha256': whole.hexdigest(), 'stat': before}
        require(files[self.index]['sha256'] == self.dataset['index_sha256'], 'local raw slot-index hash differs')
        samples = []
        with (self.directory / self.car).open('rb') as stream:
            for sample in self.dataset['car_samples']:
                actual = hash_span(stream.fileno(), sample['start'], 65536, check=self.gate.check).hexdigest()
                require(actual == sample['sha256'], f'local CAR sample hash differs at {sample["start"]}')
                samples.append({'start': sample['start'], 'bytes': 65536, 'sha256': actual})
        for name in self.lengths:
            self.observe_source(name)
            require(file_stat(self.directory / name) == files[name]['stat'], 'cache file changed before publication')
        manifest = {'schema': 'zilla-local-car-cache-v1', 'state': 'COMPLETE', 'epoch': self.dataset['epoch'],
                    'dataset_manifest_sha256': self.dataset_sha, 'completed_at': time.time(), 'files': files,
                    'source': {'base': self.base, 'operator_trusted': True,
                               'identity_limit': 'Local full hashes, exact range coverage, all manifest CAR samples and the full '
                                                 'raw-index hash are verified. An unversioned remote CAR can change between '
                                                 'requests; this does not establish an immutable original full-CAR hash.',
                               'objects': {name: {'bytes': length, 'etag': self.state['objects'][name].get('source_etag')}
                                           for name, length in self.lengths.items()}},
                    'validation': {'job_coverage': 'PASS', 'all_job_sha256': 'PASS', 'car_samples': samples,
                                   'raw_index_sha256': files[self.index]['sha256']},
                    'jobs': {name: [{'start': job['start'], 'end': job['end'], 'sha256': job['sha256']}
                                    for job in obj['jobs']] for name, obj in self.state['objects'].items()}}
        atomic_json(self.manifest_path, manifest)
        self.state.update(state='COMPLETE', manifest_sha256=hashlib.sha256(self.manifest_path.read_bytes()).hexdigest())
        self.save()
        print(f'COMPLETE: {self.manifest_path}', flush=True)
        return manifest

    def existing_complete(self):
        manifest = json.loads(self.manifest_path.read_text())
        require(manifest.get('schema') == 'zilla-local-car-cache-v1' and manifest.get('state') == 'COMPLETE'
                and manifest.get('dataset_manifest_sha256') == self.dataset_sha
                and manifest.get('source', {}).get('base') == self.base
                and set(manifest.get('files', {})) == set(self.lengths), 'completed cache identity differs')
        for name, length in self.lengths.items():
            path, recorded = self.directory / name, manifest['files'][name]
            require(recorded.get('bytes') == length and file_stat(path) == recorded.get('stat')
                    and not (path.stat().st_mode & 0o222), 'completed cache file identity or read-only mode changed')
        print(f'Already complete: {self.manifest_path}', flush=True)
        return manifest

    def run(self):
        self.directory.mkdir(parents=True, exist_ok=True)
        with (self.directory / '.download.lock').open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            if self.manifest_path.exists():
                return self.existing_complete()
            try:
                self.prepare()
                for name in self.lengths:
                    self.observe_source(name)
                for job in self.state['objects'][self.index]['jobs']:
                    self.download_job(self.index, job)
                index_hash = hash_span(self.fds[self.index], 0, self.lengths[self.index], check=self.gate.check).hexdigest()
                require(index_hash == self.dataset['index_sha256'], 'downloaded raw slot-index hash differs')
                with ThreadPoolExecutor(max_workers=self.threads) as pool:
                    futures = [pool.submit(self.download_job, self.car, job)
                               for job in self.state['objects'][self.car]['jobs']]
                    try:
                        for future in as_completed(futures):
                            future.result()
                    except BaseException:
                        self.stop.set()
                        for future in futures:
                            future.cancel()
                        raise
                for fd in self.fds.values():
                    os.fsync(fd)
                    os.close(fd)
                self.fds.clear()
                return self.verify_and_publish()
            except BaseException as error:
                self.stop.set()
                if self.state:
                    self.state.update(state='FAILED', error=repr(error))
                    self.save()
                raise
            finally:
                for fd in self.fds.values():
                    os.close(fd)
                self.fds.clear()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--dataset-manifest', required=True, type=Path)
    parser.add_argument('--directory', required=True, type=Path)
    parser.add_argument('--base', default='https://files.old-faithful.net/')
    parser.add_argument('--threads', type=int, default=8)
    parser.add_argument('--job-mib', type=int, default=1024)
    parser.add_argument('--checkpoint-mib', type=int, default=64)
    parser.add_argument('--max-attempts', type=int, default=6,
                        help='cumulative range attempts per saved job; increase explicitly after exhaustion')
    parser.add_argument('--timeout-seconds', type=int, default=120)
    parser.add_argument('--max-seconds', type=int, default=86400)
    parser.add_argument('--operator-trusted', action='store_true', required=True)
    parser.add_argument('--allow-loopback-http', action='store_true', help='test only: permit literal127.0.0.1 HTTP')
    args = parser.parse_args()
    os.umask(0o077)
    downloader = Downloader(args.dataset_manifest, args.directory, base=args.base, threads=args.threads,
                            job_bytes=args.job_mib << 20, checkpoint_bytes=args.checkpoint_mib << 20,
                            max_attempts=args.max_attempts, timeout=args.timeout_seconds,
                            max_seconds=args.max_seconds, operator_trusted=args.operator_trusted,
                            allow_loopback=args.allow_loopback_http)
    signal.signal(signal.SIGTERM, lambda _signum, _frame: downloader.stop.set())
    try:
        downloader.run()
    except KeyboardInterrupt:
        downloader.stop.set()
        raise SystemExit(130)


if __name__ == '__main__':
    main()
