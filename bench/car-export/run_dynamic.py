#!/usr/bin/env python3
"""Run selected CAR scheduling and Jetstreamer cases on one exact epoch prefix."""
import argparse
import copy
import http.client
import json
import math
import os
from pathlib import Path
import platform
import random
import re
import shutil
import signal
import stat
import statistics
import sys
import time
from urllib.parse import urlsplit

import run_full_epoch as full
from run_manycore import digest, require


def validate_base(base, allow_loopback_http=False):
    require(isinstance(base, str) and base.endswith('/')
            and not any(ord(char) <= 32 or ord(char) == 127 or char == '\\' for char in base),
            'base must be an unambiguous URL with a final slash')
    parsed = urlsplit(base)
    require(parsed.hostname and parsed.username is None and parsed.password is None
            and not parsed.query and not parsed.fragment, 'base must have no credentials, query, or fragment')
    if base.startswith('https://'):
        require(parsed.scheme == 'https', 'invalid HTTPS base')
        return False
    require(allow_loopback_http and base.startswith('http://') and parsed.scheme == 'http',
            'HTTP base requires --allow-loopback-http')
    require(re.fullmatch(r'(127\.0\.0\.1|\[::1\])(?::[0-9]+)?', parsed.netloc) is not None,
            'HTTP base must use literal 127.0.0.1 or ::1')
    require(parsed.port is None or 1 <= parsed.port <= 65535, 'invalid loopback port')
    return True


def checked_source(base, dataset, state, phase, save):
    """Repeat complete source proof only for two observed body transport errors."""
    for attempt in range(1, 4):
        try:
            return full.source_checks(base, dataset)
        except (ConnectionResetError, http.client.IncompleteRead) as error:
            event = {'phase': phase, 'attempt': attempt, 'error_type': type(error).__name__,
                     'reason': str(error), 'retry_delay_seconds': (0.5, 1.0)[attempt - 1] if attempt < 3 else None}
            state.setdefault('source_check_failed_attempts', []).append(event)
            print(f"{phase}: source check attempt {attempt}/3 failed: {type(error).__name__}: {error}",
                  file=sys.stderr, flush=True)
            save()
            if attempt == 3:
                raise
            time.sleep(event['retry_delay_seconds'])


def selected_cases(value):
    require(isinstance(value, list) and 1 <= len(value) <= 16, 'cases must be a list of 1..16 selected cases')
    result, names = [], set()
    common = {'name', 'reader', 'workers', 'export_mode', 'export_memory_mib'}
    car_fields = {'parser_workers', 'scheduling', 'http_clients', 'job_mib', 'http_chunk_mib'}
    for case in value:
        require(isinstance(case, dict) and case.get('reader') in ('car', 'jetstreamer'), 'invalid case reader')
        require(set(case) <= common | (car_fields if case['reader'] == 'car' else set()), 'unknown case setting')
        name = case.get('name')
        require(isinstance(name, str) and re.fullmatch('[a-z0-9][a-z0-9-]{0,63}', name)
                and name not in names, 'case names must be unique safe directory names')
        require(type(case.get('workers')) is int and 1 <= case['workers'] <= 256, 'workers must be in 1..256')
        item = dict(case)
        item.setdefault('export_mode', 'disk')
        require(item['export_mode'] in ('disk', 'memory'), 'export mode must be disk or memory')
        if item['export_mode'] == 'memory':
            item.setdefault('export_memory_mib', 196608)
            require(type(item['export_memory_mib']) is int and 1 <= item['export_memory_mib'] <= 262144,
                    'export memory MiB must be in 1..262144')
        else:
            require('export_memory_mib' not in item, 'export memory MiB requires memory export mode')
        if item['reader'] == 'car':
            for key, default in [('parser_workers', 64), ('scheduling', 'static'), ('http_clients', 1),
                                 ('job_mib', 256), ('http_chunk_mib', 32)]:
                item.setdefault(key, default)
            require(item['scheduling'] in ('static', 'dynamic'), 'scheduling must be static or dynamic')
            require(type(item['parser_workers']) is int and 1 <= item['parser_workers'] <= 128,
                    'CAR parser workers must be in 1..128')
            require(item['scheduling'] == 'dynamic' or item['parser_workers'] <= item['workers'],
                    'static CAR parser workers must not exceed decode workers')
            require(item['scheduling'] == 'dynamic' or item['parser_workers'] > 1,
                    'this comparison requires indexed CAR input; static mode needs more than one parser')
            require(type(item['http_clients']) is int and 1 <= item['http_clients'] <= item['parser_workers'],
                    'HTTP clients must be positive and not exceed parser workers')
            require(type(item['job_mib']) is int and 1 <= item['job_mib'] <= 4096, 'job MiB must be in 1..4096')
            require(type(item['http_chunk_mib']) is int and 1 <= item['http_chunk_mib'] <= 128,
                    'HTTP chunk MiB must be in 1..128')
        names.add(name)
        result.append(item)
    return result


def case_schedule(cases, repetitions, order='abba', seed=1):
    require(type(repetitions) is int and 1 <= repetitions <= 8, 'repetitions must be in 1..8')
    require(order in ('abba', 'round-robin', 'random'), 'invalid case order')
    generator = random.Random(seed)
    schedule = []
    for repetition in range(1, repetitions + 1):
        indices = list(range(len(cases)))
        if order == 'abba' and repetition % 2 == 0:
            indices.reverse()
        elif order == 'random':
            generator.shuffle(indices)
        schedule.extend((repetition, index) for index in indices)
    return schedule


def prefix_plan(plan, dataset, blocks):
    rows = full.validate_full_plan(plan, dataset)
    require(type(blocks) is int and 1 <= blocks <= len(rows), 'prefix block count is outside the full plan')
    prefix = copy.deepcopy({key: value for key, value in plan.items() if key != 'block_transaction_rows'})
    prefix['block_transaction_rows'] = rows[:blocks]
    prefix['end_slot_exclusive'] = rows[blocks - 1][0] + 1
    prefix['blocks'] = blocks
    prefix['transactions'] = sum(row[1] for row in rows[:blocks])
    prefix['selection'] = {'kind': 'canonical-prefix', 'blocks': blocks,
                           'transactions': prefix['transactions']}
    return prefix


def reader_command(case, binary, plan_path, directory, dataset, prefix, base, allow_loopback_http=False):
    loopback = validate_base(base, allow_loopback_http)
    command = [str(binary), '--workers', str(case['workers']), '--plan', str(plan_path),
               '--output', str(directory / 'report.json'), '--export', str(directory / 'transactions.bin')]
    if case.get('export_mode', 'disk') == 'memory':
        command += ['--export-mode', 'memory', '--export-memory-mib', str(case['export_memory_mib'])]
    if case['reader'] == 'car':
        command += ['--url', f"{base}{dataset['epoch']}/epoch-{dataset['epoch']}.car", '--operator-trusted',
                    '--legacy-http-buffers', '--http-workers', '16', '--http-window-chunks', '16',
                    '--http-chunk-mib', str(case['http_chunk_mib']), '--parser-workers', str(case['parser_workers']),
                    '--scheduling', case['scheduling'], '--http-clients', str(case['http_clients']),
                    '--job-mib', str(case['job_mib'])]
        if loopback:
            command.append('--allow-loopback-http')
    else:
        command += ['--epoch', str(dataset['epoch']), '--http-base', base, '--index-base', base,
                    '--start-slot', str(prefix['start_slot']), '--end-slot-exclusive', str(prefix['end_slot_exclusive']),
                    '--reported-error-policy', 'body-transport']
    return command


def validate_export_storage(receipt, case):
    mode = case.get('export_mode', 'disk')
    # Frozen disk-mode readers predate this receipt field and the new flags.
    if mode == 'disk' and 'export_storage' not in receipt:
        return
    storage = receipt.get('export_storage')
    require(isinstance(storage, dict) and storage.get('mode') == mode, 'reader export storage mode differs')
    if mode == 'memory':
        limit = case['export_memory_mib'] * (1 << 20)
        require(type(storage.get('memory_limit_bytes')) is int and storage['memory_limit_bytes'] == limit,
                'reader export memory limit differs')
        require(type(storage.get('peak_memory_bytes')) is int and 0 <= storage['peak_memory_bytes'] <= limit,
                'reader export memory peak exceeds its bound or is invalid')
        require(type(storage.get('spool_bytes')) is int and storage['spool_bytes'] == 0,
                'memory export wrote a disk spool or reported an invalid spool count')


def validate_receipt(receipt, case, rows, dataset, prefix, base, allow_loopback_http=False):
    loopback = validate_base(base, allow_loopback_http)
    total, scan = receipt.get('total_seconds'), receipt.get('scan_seconds')
    require(type(total) in (int, float) and type(scan) in (int, float)
            and math.isfinite(total) and math.isfinite(scan) and total > 0 and 0 <= scan <= total,
            'invalid reader total or scan duration')
    validate_export_storage(receipt, case)
    if 'export_finish_seconds' in receipt:
        finish = receipt['export_finish_seconds']
        require(type(finish) in (int, float) and math.isfinite(finish) and 0 <= finish <= total,
                'invalid reader export finish duration')
    if case['reader'] == 'jetstreamer':
        counters = full.validate_full_receipt(receipt, rows, 'jetstreamer', case['workers'], 1, True)
        require(receipt.get('epoch') == dataset['epoch'] and receipt.get('http_base') == base
                and receipt.get('index_base') == base and receipt.get('start_slot') == prefix['start_slot']
                and receipt.get('end_slot_exclusive') == prefix['end_slot_exclusive'], 'Jetstreamer source/range differs')
        return counters
    require(receipt.get('valid') is True and receipt.get('block_transaction_rows') == rows,
            'CAR receipt differs from the complete canonical prefix')
    require(receipt.get('blocks') == len(rows) and receipt.get('transactions') == sum(row[1] for row in rows),
            'CAR block or transaction count differs')
    require(receipt.get('epoch') == dataset['epoch'] and receipt.get('export_schema') == 'car-common-export-v1'
            and receipt.get('allocator') == 'mimalloc' and receipt.get('metadata_mode') == 'Owned'
            and receipt.get('reuse_http_buffers') is False and receipt.get('signature_verification') is False,
            'CAR output or decode settings differ')
    for key in ['workers', 'parser_workers', 'scheduling', 'http_clients']:
        require(receipt.get(key) == case[key], f'CAR {key} differs')
    require(receipt.get('job_target_bytes') == case['job_mib'] * (1 << 20), 'CAR job size differs')
    require(receipt.get('http_chunk_bytes') == case['http_chunk_mib'] * (1 << 20)
            and receipt.get('http_workers') == 16 and receipt.get('http_window_chunks') == 16,
            'CAR HTTP settings differ')
    expected_transport = 'dynamic-range' if case['scheduling'] == 'dynamic' else 'sharded-range'
    require(receipt.get('http_transport_mode') == expected_transport
            and receipt.get('effective_http_workers') == case['parser_workers'], 'CAR transport differs')
    require(receipt.get('http_body_window_bytes') == case['parser_workers'] * case['http_chunk_mib'] * (1 << 20),
            'CAR body window differs')
    jobs, physical = receipt.get('job_count'), receipt.get('physical_blocks')
    require(type(jobs) is int and type(physical) is int and 1 <= jobs <= physical <= len(rows),
            'invalid CAR job or physical block count')
    pipeline = receipt.get('pipeline')
    if case['scheduling'] == 'static':
        require(pipeline is None and jobs == case['parser_workers'],
                'static CAR job plan differs from the parser count')
    else:
        pool = case['workers'] + 2 * case['parser_workers']
        require(isinstance(pipeline, dict)
                and pipeline.get('raw_block_pool_size') == pipeline.get('work_queue_capacity') == pool
                and pipeline.get('job_count') == pipeline.get('jobs_claimed') == pipeline.get('jobs_completed') == jobs
                and pipeline.get('blocks_parsed') == pipeline.get('blocks_decoded') == physical
                and pipeline.get('parser_workers_finished') == case['parser_workers']
                and pipeline.get('decode_workers_finished') == case['workers']
                and type(pipeline.get('max_ready_queue_depth')) is int
                and 0 <= pipeline['max_ready_queue_depth'] <= pool,
                'dynamic CAR pipeline did not finish its complete job/block plan')
    identity = receipt.get('identity', {})
    require(receipt.get('allow_loopback_http', False) is loopback
            and identity.get('loopback_http', False) is loopback, 'CAR loopback source setting differs')
    require(identity.get('url') == f"{base}{dataset['epoch']}/epoch-{dataset['epoch']}.car"
            and identity.get('bytes') == dataset['car_bytes'] and identity.get('operator_trusted') is True,
            'CAR source identity differs')
    require(identity.get('slot_index_url') == f"{base}{dataset['epoch']}/epoch-{dataset['epoch']}-slot-ranges.raw"
            and identity.get('slot_index_bytes') == dataset['index_bytes'], 'CAR slot-index identity differs')
    require(receipt.get('http', {}).get('scan_completed') is True
            and receipt['http'].get('workers_finished') == case['parser_workers'],
            'CAR transport scan did not complete with every parser worker')
    duration = receipt.get('total_seconds')
    require(type(duration) in (int, float) and math.isfinite(duration) and duration > 0, 'invalid CAR duration')
    counters = {key: receipt.get(key) for key in ('votes', 'failed')}
    require(all(type(value) is int and 0 <= value <= receipt['transactions'] for value in counters.values()),
            'invalid CAR vote/failure counts')
    return counters


def export_memory_preflight(case):
    fields = [line.split() for line in Path('/proc/meminfo').read_text().splitlines()
              if line.startswith('MemAvailable:')]
    require(len(fields) == 1 and len(fields[0]) == 3 and fields[0][2] == 'kB'
            and fields[0][1].isdigit(), 'missing or invalid Linux MemAvailable')
    available = int(fields[0][1]) * 1024
    limit = case['export_memory_mib'] * (1 << 20)
    overhead = 16 << 30
    return {'source': '/proc/meminfo MemAvailable', 'sampled_at': time.time(),
            'available_bytes': available, 'payload_limit_bytes': limit,
            'overhead_allowance_bytes': overhead, 'required_bytes': limit + overhead,
            'sufficient': available >= limit + overhead}


def host_counters():
    counters = {'network': {}, 'disks': {}}
    for line in Path('/proc/net/dev').read_text().splitlines()[2:]:
        interface, fields = line.split(':', 1)
        fields = fields.split()
        counters['network'][interface.strip()] = {'rx_bytes': int(fields[0]), 'tx_bytes': int(fields[8])}
    for line in Path('/proc/diskstats').read_text().splitlines():
        fields = line.split()
        if len(fields) >= 14 and not fields[2].startswith(('loop', 'ram')):
            counters['disks'][fields[2]] = {'read_bytes': int(fields[5]) * 512,
                                          'write_bytes': int(fields[9]) * 512, 'busy_ms': int(fields[12])}
    counters['loadavg'] = Path('/proc/loadavg').read_text().strip()
    return counters


def counter_deltas(before, after):
    return {kind: {name: {field: values[field] - before[kind][name][field] for field in values}
                   for name, values in after[kind].items() if name in before[kind]}
            for kind in ('network', 'disks')}


def process_counters(pid):
    try:
        status = Path(f'/proc/{pid}/status').read_text()
        threads = next(int(line.split(':', 1)[1]) for line in status.splitlines() if line.startswith('Threads:'))
        io = {key: int(value) for key, value in (line.split(':', 1) for line in Path(f'/proc/{pid}/io').read_text().splitlines())}
        return {'threads': threads, 'io': io}
    except (OSError, ValueError, StopIteration):
        return {}


def source_server_snapshot(pid):
    require(type(pid) is int and pid > 1, 'invalid source server PID')
    proc = Path(f'/proc/{pid}')
    fields = (proc / 'stat').read_text().rsplit(')', 1)[1].split()
    require(len(fields) >= 22 and fields[0] not in ('Z', 'X', 'x'), 'source server is not running')
    status = (proc / 'status').read_text()
    peak_kib = next(int(line.split()[1]) for line in status.splitlines() if line.startswith('VmHWM:'))
    ticks = os.sysconf('SC_CLK_TCK')
    return {'pid': pid, 'starttime_ticks': int(fields[19]),
            'cpu_seconds': (int(fields[11]) + int(fields[12])) / ticks,
            'rss_mib': int(fields[21]) * os.sysconf('SC_PAGE_SIZE') / (1 << 20),
            'peak_rss_mib_lifetime': peak_kib / 1024,
            'sampled_at': time.time()}


def file_identity(value):
    return {key: getattr(value, key) for key in ('st_dev', 'st_ino', 'st_size', 'st_mtime_ns', 'st_ctime_ns')}


def source_server_identity(pid, base, dataset, warm_file):
    process = source_server_snapshot(pid)
    proc = Path(f'/proc/{pid}')
    command = [value.decode() for value in (proc / 'cmdline').read_bytes().split(b'\0') if value]
    scripts = [Path(value) for value in command if Path(value).name == 'serve_cache.py']
    require(len(scripts) == 1 and '--status' in command, 'PID is not the controlled local cache server')
    cwd = (proc / 'cwd').resolve(strict=True)
    script = scripts[0] if scripts[0].is_absolute() else cwd / scripts[0]
    require(digest(script) == digest(Path(__file__).with_name('serve_cache.py')), 'local server source differs')
    position = command.index('--status') + 1
    require(position < len(command), 'local server status argument is missing')
    status_path = Path(command[position])
    if not status_path.is_absolute():
        status_path = cwd / status_path
    status = json.loads(status_path.read_text())
    require(status.get('schema') == 'zilla-local-car-server-status-v1' and status.get('state') == 'READY'
            and status.get('pid') == pid and status.get('bind_host') == '127.0.0.1'
            and status.get('base_url') == base and status.get('epoch') == dataset['epoch'],
            'local server status or source endpoint differs')
    manifest_path = Path(status['manifest_path'])
    require(digest(manifest_path) == status.get('manifest_sha256'), 'cache manifest changed')
    manifest = json.loads(manifest_path.read_text())
    names = {f"epoch-{dataset['epoch']}.car", f"epoch-{dataset['epoch']}-slot-ranges.raw"}
    files = manifest.get('files', {})
    require(manifest.get('schema') == 'zilla-local-car-cache-v1' and manifest.get('state') == 'COMPLETE'
            and manifest.get('epoch') == dataset['epoch'] and set(files) == names,
            'cache manifest is not a complete two-object cache')
    car_name = f"epoch-{dataset['epoch']}.car"
    index_name = f"epoch-{dataset['epoch']}-slot-ranges.raw"
    require(files[car_name]['bytes'] == dataset['car_bytes']
            and files[index_name]['bytes'] == dataset['index_bytes']
            and files[index_name]['sha256'] == dataset['index_sha256'], 'cache objects differ from dataset')
    cache_dir = Path(status['cache_dir']).resolve(strict=True)
    require(warm_file.resolve(strict=True) == cache_dir / car_name, 'warm source is not the served CAR file')
    for name, record in files.items():
        path = cache_dir / name
        value = path.stat()
        require(stat.S_ISREG(value.st_mode) and value.st_mode & 0o222 == 0
                and file_identity(value) == record['stat'], f'cache file changed: {name}')
        require(re.fullmatch('[0-9a-f]{64}', record['sha256']) is not None, 'invalid cache file SHA-256')
        live = status.get('files', {}).get(name, {})
        require(all(live.get(key) == record[key] for key in ('bytes', 'sha256', 'stat')),
                f'server file identity differs: {name}')
    return {'pid': pid, 'starttime_ticks': process['starttime_ticks'], 'base_url': base,
            'manifest_path': str(manifest_path), 'manifest_sha256': status['manifest_sha256'],
            'server_source_sha256': digest(script), 'cache_dir': str(cache_dir), 'files': files}


def warm_source_file(path, expected_bytes, expected_identity):
    started = time.monotonic()
    with path.open('rb', buffering=0) as source:
        before = os.fstat(source.fileno())
        require(stat.S_ISREG(before.st_mode) and file_identity(before) == expected_identity
                and before.st_size == expected_bytes, 'warm source file identity differs')
        buffer = bytearray(32 << 20)
        total = 0
        while count := source.readinto(buffer):
            total += count
        require(total == expected_bytes and file_identity(os.fstat(source.fileno())) == expected_identity,
                'warm source changed or had an unexpected length')
    require(file_identity(path.stat()) == expected_identity, 'warm source path changed during the read')
    return {'path': str(path.resolve()), 'bytes': total, 'seconds': time.monotonic() - started,
            'buffer_bytes': len(buffer), 'file_identity': expected_identity,
            'scope': 'Complete sequential CAR read before this reader, outside reader timing. '
                     'Warm local-cache comparison; this does not guarantee cache residency.'}


def source_server_delta(before, after, reader):
    require(before['pid'] == after['pid'] and before['starttime_ticks'] == after['starttime_ticks'],
            'source server PID changed or was reused during the reader')
    cpu = after['cpu_seconds'] - before['cpu_seconds']
    require(cpu >= 0, 'source server CPU counter moved backwards')
    seconds = reader['process_seconds']
    require(seconds > 0, 'reader process duration must be positive')
    return {'cpu_seconds': cpu, 'mean_cpu_cores': cpu / seconds,
            'rss_mib_before': before['rss_mib'], 'rss_mib_after': after['rss_mib'],
            'peak_rss_mib_lifetime': after['peak_rss_mib_lifetime'],
            'reader_and_server_cpu_seconds': reader['cpu_seconds'] + cpu,
            'reader_and_server_mean_cpu_cores': (reader['cpu_seconds'] + cpu) / seconds,
            'scope': 'Separate server process CPU sampled around the reader process; includes all server threads. '
                     'RSS before/after is sampled; peak RSS covers the server lifetime. '
                     'This does not change reader-only CPU fields.'}


def result_summary(measurements, cases, repetitions, skip_output_verification=False):
    expected_state = 'MEASURED' if skip_output_verification else 'PASS'
    require(len(measurements) == len(cases) * repetitions
            and all(row.get('state') == expected_state for row in measurements),
            'every selected repetition must complete with the requested checks')
    if skip_output_verification:
        require(all(row.get('output_verification') == 'SKIPPED' and 'output_sha256' not in row
                    and 'byte_parity' not in row for row in measurements), 'unverified samples must not claim output parity')
    else:
        hashes = {(row['output_sha256'], row['output_bytes']) for row in measurements}
        require(len(hashes) == 1 and all(row['byte_parity'] in ('REFERENCE', 'PASS') for row in measurements),
                'all complete exports must match before summary')
    summary = {}
    for case in cases:
        rows = [row for row in measurements if row['case_name'] == case['name']]
        require(len(rows) == repetitions and {row['repetition'] for row in rows} == set(range(1, repetitions + 1)),
                'case is missing a requested repetition')
        times = [row['total_seconds'] for row in rows]
        mean = statistics.mean(times)
        summary[case['name']] = {'settings': case, 'repetitions': repetitions, 'total_seconds': times,
                               'output_verification': 'SKIPPED' if skip_output_verification else 'FULL',
                               'mean_total_seconds': mean, 'median_total_seconds': statistics.median(times),
                               'min_total_seconds': min(times), 'max_total_seconds': max(times),
                               'total_tps_from_mean': rows[0]['transactions'] / mean,
                               'samples': [row['directory'] for row in rows]}
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ['car-bin', 'jetstreamer-bin', 'verifier-bin', 'plan', 'dataset-manifest', 'cases', 'results']:
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--blocks', type=int, default=32768)
    parser.add_argument('--repetitions', type=int, default=3)
    parser.add_argument('--order', choices=['abba', 'round-robin', 'random'], default='abba')
    parser.add_argument('--seed', type=int, default=1)
    parser.add_argument('--base', default='https://files.old-faithful.net/')
    parser.add_argument('--allow-loopback-http', action='store_true',
                        help='permit only literal 127.0.0.1 or ::1 HTTP input from the verified local cache server')
    parser.add_argument('--source-server-pid', type=int,
                        help='local cache server PID; required for loopback measurements')
    parser.add_argument('--warm-source-file', type=Path,
                        help='served CAR file to read fully before each local measurement; required for loopback')
    parser.add_argument('--timeout-seconds', type=int, default=1800)
    parser.add_argument('--verification-timeout-seconds', type=int, default=1800)
    parser.add_argument('--skip-output-verification', action='store_true',
                        help='measure performance without native full-export verification, output SHA-256, or byte comparison')
    parser.add_argument('--minimum-free-gib', type=int, default=64)
    args = parser.parse_args()
    require(platform.system() == 'Linux', 'this runner requires Linux resource accounting')
    loopback = validate_base(args.base, args.allow_loopback_http)
    require((args.source_server_pid is not None) == loopback,
            'source-server-pid is required only for loopback HTTP measurements')
    require((args.warm_source_file is not None) == loopback,
            'warm-source-file is required only for loopback HTTP measurements')
    require(min(args.timeout_seconds, args.verification_timeout_seconds, args.minimum_free_gib) > 0,
            'timeouts and disk threshold must be positive')
    cases = selected_cases(json.loads(args.cases.read_text()))
    schedule = case_schedule(cases, args.repetitions, args.order, args.seed)
    dataset = full.validate_dataset(json.loads(args.dataset_manifest.read_text()))
    prefix = prefix_plan(json.loads(args.plan.read_text()), dataset, args.blocks)
    rows = prefix['block_transaction_rows']
    os.umask(0o077)
    def interrupted(signum, _frame):
        raise InterruptedError(f'runner received signal {signum}')
    signal.signal(signal.SIGTERM, interrupted)
    result = args.results.resolve()
    result.mkdir(parents=True, exist_ok=False)
    state = {'schema': 'triton-selected-cases-experiment-v1', 'state': 'PREPARING', 'pid': os.getpid(),
             'started_at': time.time(), 'cases': [], 'selected_cases': cases,
             'settings': {key: str(value) if isinstance(value, Path) else value for key, value in vars(args).items()},
             'schedule': [{'repetition': repetition, 'case': cases[index]['name']} for repetition, index in schedule],
             'host_before': full.host_snapshot(), 'dataset_manifest': dataset,
             'source_kind': 'loopback-cache-http' if loopback else 'remote-https',
             'cache_state': 'full sequential warmup before each reader; residency not guaranteed' if loopback else None,
             'output_verification': 'SKIPPED' if args.skip_output_verification else 'FULL',
             'jetstreamer_environment': {key: value for key, value in os.environ.items() if key in (
                 'JETSTREAMER_SPAWN_PENDING', 'JETSTREAMER_SPAWN_GRACE_SECS', 'JETSTREAMER_RECYCLE_PCT',
                 'JETSTREAMER_FORCE_LEGACY_INDEX')},
             'timing_scope': 'Fresh reader process per sample. Receipt total_seconds includes source setup, decoding, '
                             'disk spooling or RAM storage, ordered export and sync. All verification is outside reader time.',
             'counter_limits': 'Network and disk deltas are host counters, not process attribution. Disk devices and '
                               'partitions overlap; do not sum them. Thread and process-I/O counters are sampled '
                               'every five seconds; they can miss peaks or the final interval.',
             'source_check_limit': (
                 'Local server pins the complete cache manifest, whole-file SHA-256 identities and read-only file stats; '
                 'it checks those stats during transfers and sends strong ETags. The controller repeats the full raw-index '
                 'hash, CAR length and all manifest samples before/after the series. Readers do not hash the full source again.'
                 if loopback else
                 'Complete raw-index hash, CAR length and every manifest sample before/after series; '
                 'no immutable full-CAR identity or per-response version pin.'),
             'acceptance': {'state': 'NOT_RUN'}}

    def save():
        state['updated_at'] = time.time()
        temporary = result / 'status.tmp'
        temporary.write_text(json.dumps(state, indent=2) + '\n')
        temporary.replace(result / 'status.json')

    def phase(row, value):
        row['phase'] = value
        row.pop('progress', None)
        print(f"{row.get('case_name', 'series')}: {value}", flush=True)
        save()

    def progress(row, sample_process=False):
        def update(sample):
            if sample_process and 'pid' in sample:
                sampled = process_counters(sample['pid'])
                if sampled:
                    row['peak_threads_sampled'] = max(row.get('peak_threads_sampled', 0), sampled['threads'])
                    row['last_process_io_sample'] = sampled['io']
                    sample.update(sampled)
            row['progress'] = sample
            save()
        return update

    reference = None
    measured_export_bytes = None
    try:
        save()
        for name, source in [('dataset-manifest.json', args.dataset_manifest), ('selected-cases.json', args.cases)]:
            shutil.copyfile(source, result / name)
        plan_path, tsv = result / 'prefix-plan.json', result / 'prefix-plan.tsv'
        plan_path.write_text(json.dumps(prefix, separators=(',', ':')) + '\n')
        with tsv.open('x') as output:
            for slot, count in rows:
                output.write(f'{slot}\t{count}\n')
        state['plan'] = {key: value for key, value in prefix.items() if key != 'block_transaction_rows'}
        state['plan'].update(full_plan_sha256=digest(args.plan), prefix_plan_sha256=digest(plan_path),
                             tsv_sha256=digest(tsv), dataset_manifest_sha256=digest(result / 'dataset-manifest.json'),
                             selected_cases_sha256=digest(result / 'selected-cases.json'))
        binaries = {'car': args.car_bin.resolve(), 'jetstreamer': args.jetstreamer_bin.resolve(),
                    'verifier': args.verifier_bin.resolve()}
        state['binary_sha256'] = {name: digest(path) for name, path in binaries.items()}
        state['control_sha256'] = {name: digest(Path(__file__).with_name(name)) for name in
                                   ['run_dynamic.py', 'run_full_epoch.py', 'run_manycore.py', 'run.py']}
        if loopback:
            state['source_server_before_series'] = source_server_snapshot(args.source_server_pid)
            state['source_server_identity'] = source_server_identity(args.source_server_pid, args.base, dataset,
                                                                     args.warm_source_file)
        phase(state, 'CHECKING_SOURCE_BEFORE_SERIES')
        state['source_before'] = checked_source(args.base, dataset, state, state['phase'], save)
        state['state'] = 'RUNNING'
        for repetition, index in schedule:
            case = cases[index]
            directory = result / f"r{repetition:02d}-{case['name']}"
            row = {'case_name': case['name'], 'reader': case['reader'], 'repetition': repetition,
                   'settings': case, 'directory': directory.name, 'state': 'RUNNING'}
            state['cases'].append(row)
            try:
                required_space = args.minimum_free_gib * (1 << 30)
                if args.blocks == dataset['blocks']:
                    required_space = max(required_space, 512 * (1 << 30))
                if reference is not None:
                    required_space = max(required_space, 2 * reference.stat().st_size + (4 << 30))
                elif measured_export_bytes is not None:
                    required_space = max(required_space, 2 * measured_export_bytes + (4 << 30))
                require(shutil.disk_usage(result).free >= required_space, 'insufficient export/spool space')
                for name in (case['reader'], 'verifier'):
                    require(digest(binaries[name]) == state['binary_sha256'][name], f'{name} executable changed')
                directory.mkdir()
                export = directory / 'transactions.bin'
                command = reader_command(case, binaries[case['reader']], plan_path, directory, dataset, prefix,
                                         args.base, args.allow_loopback_http)
                row['command'] = command
                if loopback:
                    require(source_server_identity(args.source_server_pid, args.base, dataset, args.warm_source_file)
                            == state['source_server_identity'], 'local source changed before the reader')
                    phase(row, 'WARMING_LOCAL_CAR')
                    identity = state['source_server_identity']['files'][f"epoch-{dataset['epoch']}.car"]['stat']
                    row['source_warmup'] = warm_source_file(args.warm_source_file, dataset['car_bytes'], identity)
                if case['export_mode'] == 'memory':
                    row['export_memory_preflight'] = export_memory_preflight(case)
                    require(row['export_memory_preflight']['sufficient'],
                            'insufficient available RAM for export payload cap plus 16 GiB overhead')
                row['host_before_reader'] = host_counters()
                phase(row, 'READING_AND_EXPORTING')
                if loopback:
                    row['source_server_before_reader'] = source_server_snapshot(args.source_server_pid)
                row.update(full.run_process(command, directory, 'reader', args.timeout_seconds, progress(row, True)))
                if loopback:
                    row['source_server_after_reader'] = source_server_snapshot(args.source_server_pid)
                    row['source_server'] = source_server_delta(row['source_server_before_reader'],
                                                              row['source_server_after_reader'], row)
                    require(source_server_identity(args.source_server_pid, args.base, dataset, args.warm_source_file)
                            == state['source_server_identity'], 'local source changed during the reader')
                row['host_after_reader'] = host_counters()
                row['host_counter_deltas'] = counter_deltas(row['host_before_reader'], row['host_after_reader'])
                require(row['exit_code'] == 0, 'reader failed; inspect receipt and reader.stderr.log')
                receipt = json.loads((directory / 'report.json').read_text())
                counters = validate_receipt(receipt, case, rows, dataset, prefix, args.base, args.allow_loopback_http)
                row.update(total_seconds=receipt['total_seconds'], scan_seconds=receipt.get('scan_seconds'),
                           transactions=receipt['transactions'], blocks=receipt['blocks'],
                           total_tps=receipt['transactions'] / receipt['total_seconds'],
                           mean_cpu_cores=row['cpu_seconds'] / row['process_seconds'],
                           report_sha256=digest(directory / 'report.json'))
                for key in ('export_storage', 'export_finish_seconds'):
                    if key in receipt:
                        row[key] = receipt[key]
                row['receipt_metrics'] = {key: value for key, value in receipt.items() if key not in
                                          ('block_transaction_rows', 'block_decode_digests', 'worker_counters', 'shards')}
                if args.skip_output_verification:
                    size = export.stat().st_size
                    require(type(receipt.get('export_bytes')) is int and size > 0
                            and size == receipt['export_bytes'], 'export file size differs from the receipt')
                    measured_export_bytes = max(measured_export_bytes or 0, size)
                    row.update(state='MEASURED', phase='COMPLETE', output_verification='SKIPPED',
                               output_bytes=size, **counters)
                    row.pop('progress', None)
                    save()
                    continue
                phase(row, 'VERIFYING_EVERY_TRANSACTION')
                verification = [str(binaries['verifier']), '--plan-tsv', str(tsv), '--input', str(export)]
                row['verification_command'] = verification
                row['verification_process'] = full.run_process(verification, directory, 'verify',
                                                               args.verification_timeout_seconds, progress(row))
                require(row['verification_process']['exit_code'] == 0, 'native export verification failed')
                checked = json.loads((directory / 'verify.stdout.log').read_text())
                full.validate_verified_export(checked, receipt, counters, rows)
                phase(row, 'HASHING_EXPORT')
                sha, size = full.hash_output(export, progress(row), args.verification_timeout_seconds)
                require(size == checked['output_bytes'], 'export changed after structural verification')
                row.update(checked, output_sha256=sha)
                if reference is None:
                    reference = export
                    state['reference_export'] = str(reference.relative_to(result))
                    row['byte_parity'] = 'REFERENCE'
                else:
                    phase(row, 'COMPARING_EXACT_EXPORT_BYTES')
                    compared = full.compare_outputs(reference, export, progress(row), args.verification_timeout_seconds)
                    require(compared == size, 'export comparison length differs')
                    row['byte_parity'] = 'PASS'
                row.update(state='PASS', phase='COMPLETE', output_verification='FULL')
                row.pop('progress', None)
            except InterruptedError:
                raise
            except Exception as error:
                row.update(state='FAILED', phase='FAILED', error=repr(error))
                print(f"FAILED {case['name']}: {error}", flush=True)
            save()
        phase(state, 'CHECKING_SOURCE_AFTER_SERIES')
        state['source_after'] = checked_source(args.base, dataset, state, state['phase'], save)
        if loopback:
            state['source_server_after_series'] = source_server_snapshot(args.source_server_pid)
            require(source_server_identity(args.source_server_pid, args.base, dataset, args.warm_source_file)
                    == state['source_server_identity'], 'local source changed during the series')
            require(state['source_server_before_series']['starttime_ticks']
                    == state['source_server_after_series']['starttime_ticks'], 'source server changed during the series')
        require(state['source_after'] == state['source_before'], 'source changed during the series')
        require(all(digest(path) == state['binary_sha256'][name] for name, path in binaries.items()),
                'an executable changed during the series')
        state['summary'] = result_summary(state['cases'], cases, args.repetitions, args.skip_output_verification)
        state.update(state='MEASURED_UNVERIFIED' if args.skip_output_verification else 'PASS',
                     phase='COMPLETE', host_after=full.host_snapshot())
        state['acceptance'] = {
            'state': 'UNVERIFIED' if args.skip_output_verification else 'PASS', 'samples': len(state['cases']),
            'note': ('Performance measurements only. Receipt/count/plan and source checks completed; '
                     'native full-export verification, full output SHA-256, and byte comparison were skipped.'
                     if args.skip_output_verification else
                     'Every selected repetition passed; all complete output bytes match.')}
        state.pop('progress', None)
    except BaseException as error:
        state.update(state='FAILED', error=repr(error), acceptance={'state': 'FAILED', 'reason': repr(error)})
        state.pop('summary', None)
        for row in state['cases']:
            if row['state'] == 'RUNNING':
                row.update(state='FAILED', error=repr(error))
        raise
    finally:
        save()


if __name__ == '__main__':
    main()
