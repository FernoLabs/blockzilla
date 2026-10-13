#!/usr/bin/env python3
"""Serve a completed, verified CAR cache on literal IPv4 loopback only.

The downloader computes the hashes. This server pins its completed manifest and
read-only file identities; it does not repeat a full CAR hash at each start.
"""
import argparse
import errno
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import re
import resource
import select
import signal
import socket
import stat
import sys
import threading
import time


MANIFEST_SCHEMA = 'zilla-local-car-cache-v1'
STAT_FIELDS = ('st_dev', 'st_ino', 'st_size', 'st_mtime_ns', 'st_ctime_ns')
SEND_CHUNK = 8 << 20
FALLBACK_CHUNK = 1 << 20
NAMES = ('epoch-1000.car', 'epoch-1000-slot-ranges.raw')


def require(condition, message):
    if not condition:
        raise ValueError(message)


def file_identity(value):
    return {key: getattr(value, key) for key in STAT_FIELDS}


class CacheChanged(RuntimeError):
    pass


class VerifiedCache:
    def __init__(self, directory, manifest=None):
        self.directory = Path(directory).resolve(strict=True)
        require(self.directory.is_dir(), 'cache path must be a directory')
        self.manifest_path = Path(manifest).absolute() if manifest else self.directory / 'manifest.json'
        self.files = {}
        self.lock = threading.Lock()
        self.fatal_error = None
        descriptor = os.open(self.manifest_path, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            info = os.fstat(descriptor)
            require(stat.S_ISREG(info.st_mode) and 0 < info.st_size <= 2 << 20,
                    'cache manifest must be a regular file of at most 2 MiB')
            with os.fdopen(descriptor, 'rb', closefd=False) as source:
                payload = source.read((2 << 20) + 1)
            require(file_identity(os.fstat(descriptor)) == file_identity(info), 'manifest changed while read')
        finally:
            os.close(descriptor)
        document = json.loads(payload)
        require(document.get('schema') == MANIFEST_SCHEMA and document.get('state') == 'COMPLETE',
                'only a COMPLETE verified cache manifest can be served')
        require(type(document.get('epoch')) is int and document['epoch'] == 1000, 'cache epoch must be 1000')
        require(isinstance(document.get('files'), dict) and set(document['files']) == set(NAMES),
                'manifest must name exactly the two epoch-1000 files')
        self.manifest_sha256 = hashlib.sha256(payload).hexdigest()
        try:
            for name in NAMES:
                record = document['files'][name]
                require(isinstance(record, dict) and type(record.get('bytes')) is int and record['bytes'] > 0,
                        f'invalid file length: {name}')
                require(isinstance(record.get('sha256'), str)
                        and re.fullmatch('[0-9a-f]{64}', record['sha256']), f'invalid SHA-256: {name}')
                identity = record.get('stat')
                require(isinstance(identity, dict) and set(identity) == set(STAT_FIELDS)
                        and all(type(identity[key]) is int and identity[key] >= 0 for key in STAT_FIELDS)
                        and identity['st_size'] == record['bytes'], f'invalid file identity: {name}')
                path = self.directory / name
                descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
                self.files['/1000/' + name] = {'fd': descriptor, 'path': path, 'bytes': record['bytes'],
                                              'sha256': record['sha256'], 'stat': dict(identity),
                                              'etag': '"sha256-' + record['sha256'] + '"'}
                self.check(self.files['/1000/' + name])
        except BaseException:
            self.close()
            raise

    def check(self, record):
        with self.lock:
            if self.fatal_error:
                raise CacheChanged(self.fatal_error)
        try:
            for info in (os.fstat(record['fd']), record['path'].stat(follow_symlinks=False)):
                if (not stat.S_ISREG(info.st_mode) or info.st_mode & 0o222
                        or file_identity(info) != record['stat']):
                    raise CacheChanged('verified cache file identity or read-only mode changed')
        except (OSError, CacheChanged) as error:
            with self.lock:
                self.fatal_error = self.fatal_error or str(error)
            raise CacheChanged(self.fatal_error) from error

    def close(self):
        for record in self.files.values():
            if record['fd'] is not None:
                os.close(record['fd'])
                record['fd'] = None


def byte_range(value, size):
    """Return an end-exclusive single range, including open/suffix forms."""
    match = re.fullmatch(r'bytes=(\d*)-(\d*)', value.strip())
    require(match is not None and any(match.groups()), 'invalid single byte range')
    first, last = match.groups()
    if not first:
        suffix = int(last)
        require(suffix > 0, 'zero suffix is unsatisfiable')
        return max(0, size - suffix), size
    start = int(first)
    end = min(size, int(last) + 1) if last else size
    require(start < size and end > start, 'range is outside the file')
    return start, end


class ArchiveServer(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 256
    allow_reuse_address = True

    def __init__(self, address, cache):
        require(address[0] == '127.0.0.1', 'server must bind literal 127.0.0.1')
        self.cache = cache
        self.started = time.monotonic()
        self.counter_lock = threading.Lock()
        self.counters = {'connections': 0, 'requests': 0, 'active_requests': 0,
                         'bytes_sent': 0, 'errors': 0, 'transport_errors': 0, 'status_counts': {}}
        self.state = 'READY'
        super().__init__(address, ArchiveHandler)

    def get_request(self):
        connection, address = super().get_request()
        connection.settimeout(60)
        connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        with self.counter_lock:
            self.counters['connections'] += 1
        return connection, address

    def verify_request(self, request, client_address):
        return client_address[0] == '127.0.0.1'

    def update(self, **increments):
        with self.counter_lock:
            for name, value in increments.items():
                self.counters[name] += value

    def response(self, status):
        with self.counter_lock:
            statuses = self.counters['status_counts']
            statuses[str(status)] = statuses.get(str(status), 0) + 1
            self.counters['errors'] += int(status >= 400)

    def snapshot(self):
        usage = resource.getrusage(resource.RUSAGE_SELF)
        peak_rss = usage.ru_maxrss if sys.platform == 'darwin' else usage.ru_maxrss * 1024
        rss = None
        try:
            rss = int(Path('/proc/self/statm').read_text().split()[1]) * os.sysconf('SC_PAGE_SIZE')
        except (OSError, ValueError, IndexError):
            pass
        with self.counter_lock:
            counts = dict(self.counters, status_counts=dict(self.counters['status_counts']))
        return {'schema': 'zilla-local-car-server-status-v1', 'state': 'FAILED' if self.cache.fatal_error else self.state,
                'pid': os.getpid(), 'bind_host': self.server_address[0], 'port': self.server_address[1],
                'base_url': f'http://127.0.0.1:{self.server_address[1]}/', 'epoch': 1000,
                'cache_dir': str(self.cache.directory), 'manifest_path': str(self.cache.manifest_path),
                'manifest_sha256': self.cache.manifest_sha256, 'updated_unix_seconds': time.time(),
                'files': {record['path'].name: {key: record[key] for key in ('bytes', 'sha256', 'etag', 'stat')}
                          for record in self.cache.files.values()},
                'elapsed_seconds': time.monotonic() - self.started, 'counters': counts,
                'resource_usage': {'user_cpu_seconds': usage.ru_utime, 'system_cpu_seconds': usage.ru_stime,
                                   'total_cpu_seconds': usage.ru_utime + usage.ru_stime,
                                   'rss_bytes': rss, 'peak_rss_bytes': peak_rss},
                'error': self.cache.fatal_error}


class ArchiveHandler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    server_version = 'VerifiedCARCache/1'
    sys_version = ''

    def log_message(self, *_):
        pass

    def send_error(self, code, message=None, explain=None):
        # Do not echo user paths or produce HTML/directory pages.
        self.server.update(requests=1)
        self.empty(code, close=True)

    def empty(self, code, record=None, close=False, unsatisfied=False):
        self.send_response(code)
        self.send_header('Content-Length', '0')
        self.send_header('Cache-Control', 'no-transform')
        if record:
            self.send_header('ETag', record['etag'])
            self.send_header('Accept-Ranges', 'bytes')
            if unsatisfied:
                self.send_header('Content-Range', f"bytes */{record['bytes']}")
        if close:
            self.send_header('Connection', 'close')
            self.close_connection = True
        self.end_headers()
        self.server.response(code)

    def do_HEAD(self):
        self.serve(head=True)

    def do_GET(self):
        self.serve(head=False)

    def do_POST(self):
        self.server.update(requests=1)
        self.empty(405, close=True)

    do_PUT = do_POST
    do_DELETE = do_POST
    do_PATCH = do_POST

    def serve(self, head):
        self.server.update(requests=1, active_requests=1)
        try:
            if (self.headers.get_all('Transfer-Encoding') or len(self.headers.get_all('Content-Length', [])) > 1
                    or self.headers.get('Content-Length', '0') != '0'
                    or len(self.headers.get_all('Range', [])) > 1
                    or len(self.headers.get_all('If-Match', [])) > 1):
                self.empty(400, close=True)
                return
            record = self.server.cache.files.get(self.path)
            if record is None:
                self.empty(404, close=True)
                return
            try:
                self.server.cache.check(record)
            except CacheChanged:
                self.empty(503, close=True)
                return
            if_match = self.headers.get('If-Match')
            if if_match is not None and if_match != '*' and record['etag'] not in [part.strip() for part in if_match.split(',')]:
                self.empty(412, record, close=True)
                return
            start, end, status = 0, record['bytes'], 200
            requested_range = self.headers.get('Range')
            if_range = self.headers.get('If-Range')
            if requested_range is not None and (if_range is None or if_range == record['etag']):
                try:
                    start, end = byte_range(requested_range, record['bytes'])
                except ValueError:
                    self.empty(416, record, unsatisfied=True)
                    return
                status = 206
            self.send_response(status)
            self.send_header('Content-Type', 'application/octet-stream')
            self.send_header('Content-Length', str(end - start))
            self.send_header('Accept-Ranges', 'bytes')
            self.send_header('ETag', record['etag'])
            self.send_header('Cache-Control', 'no-transform')
            if status == 206:
                self.send_header('Content-Range', f"bytes {start}-{end - 1}/{record['bytes']}")
            self.end_headers()
            self.server.response(status)
            if not head:
                self.send_file(record, start, end)
        except CacheChanged:
            self.close_connection = True
            self.server.update(errors=1)
        except (BrokenPipeError, ConnectionResetError, TimeoutError, OSError):
            self.close_connection = True
            self.server.update(errors=1, transport_errors=1)
        finally:
            self.server.update(active_requests=-1)

    def send_file(self, record, start, end):
        position = start
        sendfile = hasattr(os, 'sendfile')
        self.wfile.flush()
        while position < end:
            self.server.cache.check(record)
            if sendfile:
                try:
                    count = os.sendfile(self.connection.fileno(), record['fd'], position,
                                        min(SEND_CHUNK, end - position))
                except InterruptedError:
                    continue
                except BlockingIOError:
                    # A socket with a timeout uses a nonblocking file descriptor.
                    # os.sendfile does not perform socket.sendall's readiness wait.
                    if not select.select([], [self.connection], [], self.connection.gettimeout())[1]:
                        raise TimeoutError('local archive client did not accept data')
                    continue
                except OSError as error:
                    if error.errno not in (errno.ENOSYS, errno.EINVAL, errno.ENOTSUP, errno.EOPNOTSUPP):
                        raise
                    sendfile = False
                    continue
            else:
                body = os.pread(record['fd'], min(FALLBACK_CHUNK, end - position), position)
                count = len(body)
                self.wfile.write(body)
            if not count:
                with self.server.cache.lock:
                    self.server.cache.fatal_error = 'verified cache file became shorter during transfer'
                raise CacheChanged(self.server.cache.fatal_error)
            position += count
            self.server.update(bytes_sent=count)
        self.server.cache.check(record)


def save_status(path, value):
    temporary = path.with_name(path.name + f'.tmp-{os.getpid()}')
    with temporary.open('w') as destination:
        json.dump(value, destination, sort_keys=True, indent=2)
        destination.write('\n')
    os.replace(temporary, path)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cache-dir', type=Path, required=True)
    parser.add_argument('--manifest', type=Path)
    parser.add_argument('--status', type=Path, required=True)
    parser.add_argument('--port', type=int, default=0)
    parser.add_argument('--status-interval', type=float, default=1.0)
    args = parser.parse_args(argv)
    require(0 <= args.port <= 65535, 'port must be in 0..65535')
    require(0.1 <= args.status_interval <= 60, 'status interval must be in 0.1..60 seconds')
    cache = VerifiedCache(args.cache_dir, args.manifest)
    server = None
    try:
        status_path = args.status.resolve()
        require(status_path not in {cache.manifest_path.resolve(), *(value['path'].resolve() for value in cache.files.values())},
                'status path must be separate from cache objects and manifest')
        status_path.parent.mkdir(parents=True, exist_ok=True)
        server = ArchiveServer(('127.0.0.1', args.port), cache)
        stopped = threading.Event()
        for name in (signal.SIGINT, signal.SIGTERM):
            signal.signal(name, lambda *_: stopped.set())
        thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': 0.1}, daemon=True)
        thread.start()
        save_status(status_path, server.snapshot())
        print(json.dumps(server.snapshot()), flush=True)
        while not stopped.wait(args.status_interval):
            save_status(status_path, server.snapshot())
            if cache.fatal_error:
                break
        server.shutdown()
        thread.join()
        server.state = 'STOPPED'
        save_status(status_path, server.snapshot())
        return 1 if cache.fatal_error else 0
    finally:
        if server:
            server.server_close()
        cache.close()


if __name__ == '__main__':
    raise SystemExit(main())
