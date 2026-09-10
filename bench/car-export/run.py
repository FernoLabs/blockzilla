#!/usr/bin/env python3
"""Finite Linux comparison: same CAR source, plan, allocator, and binary export.

Run from a new control directory containing bin/{car,jetstreamer}, plan.json,
and manifest.json. No archive payload is downloaded to a local CAR file.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import struct
import subprocess
import time
import urllib.request

MAGIC = b'CAR-TX-EXPORT-01'


def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(4 << 20), b''):
            h.update(chunk)
    return h.hexdigest()


def verify_export(path, rows):
    """Check every record's slot, index, flags, lengths and exact coverage."""
    votes = failed = 0
    with path.open('rb') as stream:
        assert stream.read(16) == MAGIC, 'export magic'
        for slot, count in rows:
            header = stream.read(24)
            assert len(header) == 24, 'missing block header'
            actual_slot, actual_count, size = struct.unpack('<QQQ', header)
            assert (actual_slot, actual_count) == (slot, count), 'block coverage'
            assert size <= 64 << 20, 'oversized block'
            block = stream.read(size)
            assert len(block) == size, 'truncated block'
            offset = 0
            for index in range(count):
                assert offset + 122 <= size, 'truncated transaction'
                assert struct.unpack_from('<QQ', block, offset) == (slot, index), 'transaction order'
                vote, error = block[offset + 112:offset + 114]
                assert vote in (0, 1) and error in (0, 1), 'invalid flags'
                votes += vote
                failed += error
                offset += 122
                for _ in range(2):
                    assert offset + 8 <= size, 'missing balance count'
                    length, = struct.unpack_from('<Q', block, offset)
                    offset += 8 + length * 8
                    assert offset <= size, 'truncated balances'
            assert offset == size, 'extra block bytes'
        assert stream.read(1) == b'', 'extra blocks'
    return {'output_sha256': digest(path), 'output_bytes': path.stat().st_size,
            'votes': votes, 'failed': failed}


def identical(left, right):
    with left.open('rb') as a, right.open('rb') as b:
        offset = 0
        while True:
            x, y = a.read(4 << 20), b.read(4 << 20)
            assert x == y, f'output byte mismatch in chunk starting at {offset}'
            if not x:
                return
            offset += len(x)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--control', type=Path, required=True)
    parser.add_argument('--results', type=Path, required=True)
    parser.add_argument('--archive', type=Path, required=True)
    parser.add_argument('--wait-for', type=Path)
    parser.add_argument('--base', required=True)
    args = parser.parse_args()
    os.umask(0o077)
    control, results = args.control.resolve(), args.results.resolve()
    assert args.base.endswith('/') and args.base.startswith('https://')
    results.mkdir(parents=True, exist_ok=False)
    state = {'state': 'PREPARING', 'pid': os.getpid(), 'cases': [], 'started_at': time.time()}

    def save():
        temp = results / 'status.tmp'
        temp.write_text(json.dumps(state, indent=2) + '\n')
        temp.replace(results / 'status.json')

    def inventory():
        return {str(p): [p.stat().st_size, p.stat().st_mtime_ns, p.stat().st_ino]
                for p in sorted(args.archive.iterdir()) if p.is_file()}

    def source():
        result = {}
        for name in ['epoch-900.car', 'epoch-900-slot-ranges.raw']:
            url = args.base + '900/' + name
            request = urllib.request.Request(
                url,
                method='HEAD',
                headers={'Accept-Encoding': 'identity', 'User-Agent': 'blockzilla-reader-benchmark/1'},
            )
            with urllib.request.urlopen(request, timeout=45) as response:
                etag = response.headers.get('ETag')
                assert response.status == 200 and response.url == url
                assert etag and not etag.startswith('W/'), 'strong source ETag required'
                result[name] = {'url': url, 'bytes': int(response.headers['Content-Length']), 'etag': etag}
        assert result['epoch-900.car']['bytes'] == 527045598158
        assert result['epoch-900-slot-ranges.raw']['bytes'] == 5184000
        return result

    def no_competing_reader():
        for proc in Path('/proc').iterdir():
            if not proc.name.isdigit() or int(proc.name) == os.getpid():
                continue
            try:
                exe = (proc / 'cmdline').read_bytes().split(b'\0')[0].decode()
            except (OSError, UnicodeError):
                continue
            name = Path(exe).name
            assert not (name.startswith(('read-car-', 'read-compact-v2-', 'read-archive-v3-', 'jetstreamer-'))
                        or name in ['car-decode-reference', 'blockzilla-reader-profile']
                        or '/blockzilla-bench/control/' in exe and '/bin/' in exe), f'competing reader: {exe}'

    try:
        save()
        if args.wait_for:
            state['state'] = 'WAITING_FOR_EXISTING_TESTS'
            save()
            deadline = time.monotonic() + 6 * 3600
            while True:
                previous = json.loads(args.wait_for.read_text())
                if previous['state'] not in ['RUNNING', 'PREPARING']:
                    assert previous['state'] == 'PASS', 'existing tests failed; comparison not started'
                    break
                assert time.monotonic() < deadline, 'existing-test wait deadline'
                time.sleep(30)
        no_competing_reader()
        manifest = json.loads((control / 'manifest.json').read_text())
        for name, expected_hash in manifest['files'].items():
            assert digest(control / name) == expected_hash, name
        plan = json.loads((control / 'plan.json').read_text())
        assert plan['epoch'] == 900 and plan['start_slot'] == 388800000
        rows = plan['block_transaction_rows']
        assert len(rows) == 8192 and sum(r[1] for r in rows) == 8925832
        small = dict(plan, block_transaction_rows=rows[:64], end_slot_exclusive=rows[63][0] + 1)
        (results / 'smoke-plan.json').write_text(json.dumps(small) + '\n')
        initial = inventory()
        assert initial, 'archive scan is empty'
        state['archive_before'] = initial
        state['source_before'] = source()
        state['build_manifest_sha256'] = digest(control / 'manifest.json')
        state['state'] = 'RUNNING'
        references = {}
        for label, reader, smoke in [('smoke-car', 'car', True), ('smoke-jetstreamer', 'jetstreamer', True),
                                      ('a1-car', 'car', False), ('b1-jetstreamer', 'jetstreamer', False),
                                      ('b2-jetstreamer', 'jetstreamer', False), ('a2-car', 'car', False)]:
            no_competing_reader()
            assert shutil.disk_usage(results).free > 32 << 30, 'need 32 GiB free for exports'
            assert inventory() == initial, 'archive changed'
            assert source() == state['source_before'], 'HTTP object changed'
            case = results / label
            case.mkdir()
            current_plan = small if smoke else plan
            plan_path = results / 'smoke-plan.json' if smoke else control / 'plan.json'
            cmd = [str(control / 'bin' / reader), '--workers', '12', '--plan', str(plan_path),
                   '--output', str(case / 'report.json'), '--export', str(case / 'transactions.bin')]
            if reader == 'car':
                cmd += ['--url', args.base + '900/epoch-900.car', '--legacy-http-buffers']
            else:
                cmd += ['--epoch', '900', '--http-base', args.base, '--index-base', args.base,
                        '--start-slot', str(current_plan['start_slot']),
                        '--end-slot-exclusive', str(current_plan['end_slot_exclusive'])]
            row = {'label': label, 'reader': reader, 'smoke': smoke, 'command': cmd,
                   'binary_sha256': digest(control / 'bin' / reader),
                   'load_before': Path('/proc/loadavg').read_text().strip()}
            state['cases'].append(row)
            state['current'] = label
            save()
            started = time.monotonic()
            with (case / 'stdout.log').open('xb') as out, (case / 'stderr.log').open('xb') as err:
                child = subprocess.Popen(cmd, stdout=out, stderr=err, stdin=subprocess.DEVNULL, start_new_session=True)
                try:
                    while True:
                        pid, status, usage = os.wait4(child.pid, os.WNOHANG)
                        if pid:
                            child.returncode = os.waitstatus_to_exitcode(status)
                            break
                        assert time.monotonic() - started < 1800, 'case exceeded 30 minutes'
                        time.sleep(.1)
                finally:
                    if child.returncode is None:
                        os.killpg(child.pid, signal.SIGKILL)
                        _, status, usage = os.wait4(child.pid, 0)
                        child.returncode = os.waitstatus_to_exitcode(status)
            row.update(process_seconds=time.monotonic() - started, exit_code=child.returncode,
                       cpu_seconds=usage.ru_utime + usage.ru_stime, peak_rss_mib=usage.ru_maxrss / 1024,
                       load_after=Path('/proc/loadavg').read_text().strip())
            assert child.returncode == 0, 'reader process failed'
            receipt = json.loads((case / 'report.json').read_text())
            expected = current_plan['block_transaction_rows']
            assert receipt['valid'] and receipt['block_transaction_rows'] == expected
            assert receipt['transactions'] == sum(r[1] for r in expected)
            assert receipt['export_schema'] == 'car-common-export-v1' and receipt['allocator'] == 'mimalloc'
            checked = verify_export(case / 'transactions.bin', expected)
            assert checked['output_bytes'] == receipt['export_bytes']
            if reader == 'car':
                assert checked['votes'] == receipt['votes'] and checked['failed'] == receipt['failed']
            else:
                assert checked['votes'] == sum(w['simple_vote_callbacks'] for w in receipt['worker_counters'])
                assert checked['failed'] == sum(w['failed_status_callbacks'] for w in receipt['worker_counters'])
            if not smoke:
                assert checked['votes'] == 6533434 and checked['failed'] == 341094
            if smoke in references:
                identical(references[smoke], case / 'transactions.bin')
                row['byte_parity'] = 'PASS'
            else:
                references[smoke] = case / 'transactions.bin'
                row['byte_parity'] = 'REFERENCE'
            assert source() == state['source_before'], 'HTTP object changed'
            assert inventory() == initial, 'archive changed'
            row.update(checked, state='PASS', total_seconds=receipt['total_seconds'],
                       total_tps=receipt['transactions'] / receipt['total_seconds'],
                       report_sha256=digest(case / 'report.json'))
            save()
        state.update(state='PASS', current=None, archive_after=inventory(), source_after=source())
    except BaseException as error:
        state.update(state='FAILED', error=repr(error))
        raise
    finally:
        state['updated_at'] = time.time()
        save()


if __name__ == '__main__':
    main()
