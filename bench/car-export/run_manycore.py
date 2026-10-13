#!/usr/bin/env python3
"""Run a finite, equal-output CAR/Jetstreamer worker sweep on one Linux host."""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import shutil
import signal
import subprocess
import time
import urllib.request

from run import digest, identical, verify_export

EPOCH = 900
START = 388800000
END = 388808198
TRANSACTIONS = 8925832
OUTPUT_BYTES = 2300383008
OUTPUT_SHA256 = 'bf9b07fbda244bb1e5c5d07059db8489133e544d1ade482808936eceb01fd035'
INDEX_SHA256 = 'b9551b93bd720c221c7d1a605452bd9a676e326344ef56371a03ad44e32c1056'
CAR_BYTES = 527045598158
INDEX_BYTES = 5184000
JETSTREAMER_COMMIT = '048d2cd25af949679c56347440b7cf5a9a900063'
CAR_SAMPLES = [
    (0, '5141dbea7fe2724e52ff1b08991150275b86e5c34b21714931e569f1fa0f1bae'),
    (263522799079, 'df4f3e42414a9e272e864f545bad77b95b4afcf5b8b325ee141fc06904f777a4'),
    (527045532622, '3ad15a721ca63e7fec39fe4fef0e70091b5bbe639c5118f88fda169d5642f35c'),
]


def require(condition, message):
    if not condition:
        raise ValueError(message)


def worker_list(value):
    try:
        workers = [int(part) for part in value.split(',')]
    except ValueError as error:
        raise argparse.ArgumentTypeError('workers must be comma-separated integers') from error
    if not workers or len(workers) != len(set(workers)) or any(w < 1 or w > 256 for w in workers):
        raise argparse.ArgumentTypeError('workers must be distinct integers from 1 to 256')
    return workers


def validate_plan(plan):
    require(plan.get('epoch') == EPOCH and plan.get('start_slot') == START
            and plan.get('end_slot_exclusive') == END, 'expected the epoch-900 8192-block prefix')
    rows = plan.get('block_transaction_rows')
    require(isinstance(rows, list) and len(rows) == 8192, 'expected 8192 canonical rows')
    require(all(isinstance(row, list) and len(row) == 2
                and all(type(v) is int for v in row) and row[1] >= 0 for row in rows),
            'invalid slot/count row')
    require(all(START <= row[0] < END for row in rows)
            and all(a[0] < b[0] for a, b in zip(rows, rows[1:])), 'invalid slot order or range')
    require(rows[-1][0] + 1 == END and sum(row[1] for row in rows) == TRANSACTIONS,
            'canonical prefix count differs from the accepted reference')
    return rows


def parser_configuration(parser_workers, workers, *, smoke=False, baseline=False):
    effective = 1 if smoke or baseline else parser_workers
    require(1 <= effective <= workers, 'parser workers must be positive and must not exceed decode workers')
    return effective, ['--parser-workers', str(effective)] if effective > 1 else []


def validate_parser_receipt(receipt, expected):
    if expected > 1 or 'parser_workers' in receipt:
        require(receipt.get('parser_workers') == expected, 'receipt parser worker count differs')
    if expected > 1:
        require(receipt.get('http_transport_mode') == 'sharded-range'
                and receipt.get('effective_http_workers') == expected,
                'receipt does not confirm the requested parser range transport')


def fetch_exact(url, *, size, start=None, total=None):
    headers = {'Accept-Encoding': 'identity', 'User-Agent': 'blockzilla-reader-benchmark/1'}
    if start is not None:
        headers['Range'] = f'bytes={start}-{start + size - 1}'
    request = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(request, timeout=60) as response:
        require(response.url == url, f'unexpected source redirect: {response.url}')
        require(response.status == (206 if start is not None else 200), 'unexpected source HTTP status')
        require(response.headers.get('Content-Encoding', 'identity') == 'identity', 'encoded source body')
        if start is not None:
            require(response.headers.get('Content-Range') == f'bytes {start}-{start + size - 1}/{total}',
                    'incorrect source Content-Range')
        body = response.read(size + 1)
        require(len(body) == size, 'source body length differs')
        return hashlib.sha256(body).hexdigest()


def source_checks(base):
    car_url = base + '900/epoch-900.car'
    request = urllib.request.Request(car_url, method='HEAD', headers={
        'Accept-Encoding': 'identity', 'User-Agent': 'blockzilla-reader-benchmark/1'})
    with urllib.request.urlopen(request, timeout=60) as response:
        require(response.status == 200 and response.url == car_url, 'CAR HEAD status or redirect')
        require(int(response.headers['Content-Length']) == CAR_BYTES, 'CAR object length changed')
        etag = response.headers.get('ETag')
    index_sha = fetch_exact(base + '900/epoch-900-slot-ranges.raw', size=INDEX_BYTES)
    require(index_sha == INDEX_SHA256, 'slot index differs from the accepted Triton source')
    samples = []
    for start, expected in CAR_SAMPLES:
        actual = fetch_exact(car_url, size=65536, start=start, total=CAR_BYTES)
        require(actual == expected, f'CAR source sample changed at {start}')
        samples.append({'start': start, 'bytes': 65536, 'sha256': actual})
    return {'car_bytes': CAR_BYTES, 'car_etag': etag, 'car_samples': samples,
            'index_bytes': INDEX_BYTES, 'index_sha256': index_sha}


def validate_report(receipt, checked, rows, reader, smoke):
    require(receipt.get('valid') is True and receipt.get('block_transaction_rows') == rows,
            'receipt does not match the exact canonical plan')
    require(receipt.get('transactions') == sum(row[1] for row in rows), 'transaction count differs')
    require(receipt.get('export_schema') == 'car-common-export-v1'
            and receipt.get('allocator') == 'mimalloc', 'export schema or allocator differs')
    require(receipt.get('signature_verification') is False, 'signature verification setting differs')
    require(checked['output_bytes'] == receipt.get('export_bytes'), 'export byte count differs')
    if reader == 'car':
        votes, failed = receipt.get('votes'), receipt.get('failed')
    else:
        require(receipt.get('errors') == [], 'Jetstreamer reported an error')
        require(receipt.get('upstream_version') == '0.7.0', 'Jetstreamer version differs')
        require(receipt.get('upstream_commit') == JETSTREAMER_COMMIT,
                'Jetstreamer revision differs')
        require(receipt.get('reported_error_policy') == 'strict'
                and type(receipt.get('reported_body_retries')) is int
                and receipt['reported_body_retries'] == 0
                and receipt.get('reported_body_retry_limit_per_worker_slot') == 2
                and receipt.get('reported_error_events') == []
                and receipt.get('reported_error_events_truncated') is False,
                'prefix sweep requires the strict Jetstreamer reported-error policy with no body retries')
        require(receipt.get('transaction_callbacks') == sum(row[1] for row in rows),
                'Jetstreamer transaction callback count differs')
        counters = receipt['worker_counters']
        votes = sum(worker['simple_vote_callbacks'] for worker in counters)
        failed = sum(worker['failed_status_callbacks'] for worker in counters)
    require((votes, failed) == (checked['votes'], checked['failed']), 'vote/failure counters differ')
    require(isinstance(receipt.get('total_seconds'), (int, float))
            and math.isfinite(receipt['total_seconds']) and receipt['total_seconds'] > 0,
            'invalid total duration')
    if not smoke:
        require((votes, failed) == (6533434, 341094), 'accepted vote/failure totals differ')
        require(checked['output_bytes'] == OUTPUT_BYTES and checked['output_sha256'] == OUTPUT_SHA256,
                'export differs from the previously accepted epoch-900 output')


def execute(cmd, directory, timeout):
    started = time.monotonic()
    child = None
    with (directory / 'stdout.log').open('xb') as stdout, (directory / 'stderr.log').open('xb') as stderr:
        child = subprocess.Popen(cmd, stdout=stdout, stderr=stderr, stdin=subprocess.DEVNULL,
                                 start_new_session=True)
        try:
            while True:
                pid, status, usage = os.wait4(child.pid, os.WNOHANG)
                if pid:
                    child.returncode = os.waitstatus_to_exitcode(status)
                    return {'exit_code': child.returncode, 'process_seconds': time.monotonic() - started,
                            'user_cpu_seconds': usage.ru_utime, 'system_cpu_seconds': usage.ru_stime,
                            'cpu_seconds': usage.ru_utime + usage.ru_stime,
                            'peak_rss_mib': usage.ru_maxrss / 1024}
                require(time.monotonic() - started < timeout, 'case exceeded its time limit')
                time.sleep(.1)
        finally:
            if child.returncode is None:
                os.killpg(child.pid, signal.SIGKILL)
                _, status, _ = os.wait4(child.pid, 0)
                child.returncode = os.waitstatus_to_exitcode(status)


def host_snapshot():
    def read(path):
        try:
            return Path(path).read_text()
        except OSError:
            return None
    return {'platform': platform.platform(), 'logical_cpus': os.cpu_count(),
            'cpuinfo': read('/proc/cpuinfo'), 'meminfo': read('/proc/meminfo'),
            'loadavg': read('/proc/loadavg'), 'network': read('/proc/net/dev')}


def confirmation_summary(cases, selected_settings):
    summary = {}
    for reader in ['car', 'jetstreamer']:
        confirmed = [row for row in cases if row['reader'] == reader
                     and row['label'].startswith('confirm-') and row['state'] == 'PASS']
        require(confirmed, f'missing confirmation results for {reader}')
        mean = sum(row['total_seconds'] for row in confirmed) / len(confirmed)
        summary[reader] = {'workers': selected_settings[reader]['workers'],
                           'cases': [row['label'] for row in confirmed],
                           'mean_total_seconds': mean, 'total_tps': TRANSACTIONS / mean,
                           'min_total_seconds': min(row['total_seconds'] for row in confirmed),
                           'max_total_seconds': max(row['total_seconds'] for row in confirmed)}
        if reader == 'car':
            summary[reader]['parser_workers'] = selected_settings[reader].get('parser_workers', 1)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--car-bin', type=Path, required=True)
    parser.add_argument('--jetstreamer-bin', type=Path, required=True)
    parser.add_argument('--plan', type=Path, required=True)
    parser.add_argument('--results', type=Path, required=True)
    parser.add_argument('--base', default='https://files.old-faithful.net/')
    parser.add_argument('--workers', type=worker_list, default=worker_list('12,32,64,128'))
    parser.add_argument('--parser-workers', type=int, default=1,
                        help='CAR parser ranges for sweep and confirmation; smoke and baseline use one parser')
    parser.add_argument('--http-workers', type=int, default=16)
    parser.add_argument('--http-window-chunks', type=int, default=16)
    parser.add_argument('--http-chunk-mib', type=int, default=32)
    parser.add_argument('--baseline', action='store_true', help='also run the old 12-worker, 4-HTTP-worker settings')
    parser.add_argument('--best-repeats', type=int, default=2, help='confirmation runs of each reader at its fastest sweep setting')
    parser.add_argument('--timeout-seconds', type=int, default=1800)
    args = parser.parse_args()
    require(__debug__, 'do not run with python -O; the shared output verifier uses assertions')
    require(platform.system() == 'Linux', 'this runner requires Linux resource accounting')
    require(args.base.startswith('https://') and args.base.endswith('/'), 'base must be HTTPS with a final slash')
    require(1 <= args.parser_workers <= min(args.workers),
            'parser workers must be positive and must not exceed any sweep decode worker count')
    require(1 <= args.http_workers <= args.http_window_chunks <= 16, 'invalid HTTP worker/window bounds')
    require(1 <= args.http_chunk_mib <= 32, 'invalid HTTP chunk size')
    require(1 <= args.best_repeats <= 8 and args.timeout_seconds > 0, 'invalid repeat count or timeout')
    os.umask(0o077)
    def interrupted(signum, _frame):
        raise InterruptedError(f'runner received signal {signum}')
    signal.signal(signal.SIGTERM, interrupted)
    results = args.results.resolve()
    results.mkdir(parents=True, exist_ok=False)
    state = {'state': 'PREPARING', 'pid': os.getpid(), 'started_at': time.time(), 'cases': [],
             'settings': {key: str(value) if isinstance(value, Path) else value for key, value in vars(args).items()},
             'host_before': host_snapshot(),
             'source_check_limit': 'Full slot-index hash, object lengths, and three 64-KiB CAR samples. '
                                   'These checks do not prove full-CAR identity or pin every HTTP response.',
             'timing_scope': 'total_seconds includes source setup, decode, shared output spool, ordered copy, and file sync. '
                             'CAR parallel parser mode also fetches its raw slot index inside timing. '
                             'scan_seconds excludes final output copy. Output verification is outside timing.',
             'parser_semantics': 'CAR smoke and baseline use one parser. Sweep and confirmation use parser_workers. '
                                 'With more than one parser, CAR uses one blocking range downloader per parser; '
                                 'the HTTP prefetch worker and window settings are not active. Jetstreamer is unchanged.',
             'selection_rule': 'Select the fastest passing sweep setting per reader by total_seconds, then confirm it. '
                               'Only confirmation runs enter the final summary.'}

    def save():
        state['updated_at'] = time.time()
        temporary = results / 'status.tmp'
        temporary.write_text(json.dumps(state, indent=2) + '\n')
        temporary.replace(results / 'status.json')

    references = {}
    try:
        save()
        plan = json.loads(args.plan.read_text())
        rows = validate_plan(plan)
        plan_path = results / 'plan.json'
        plan_path.write_text(json.dumps(plan) + '\n')
        small = dict(plan, block_transaction_rows=rows[:64], end_slot_exclusive=rows[63][0] + 1)
        small_path = results / 'smoke-plan.json'
        small_path.write_text(json.dumps(small) + '\n')
        binaries = {'car': args.car_bin.resolve(), 'jetstreamer': args.jetstreamer_bin.resolve()}
        state['binary_sha256'] = {reader: digest(path) for reader, path in binaries.items()}
        state['plan_sha256'] = digest(args.plan)
        state['runner_sha256'] = digest(Path(__file__).resolve())
        state['verifier_sha256'] = digest(Path(__file__).with_name('run.py').resolve())
        state['source_before'] = source_checks(args.base)
        state['state'] = 'RUNNING'
        save()

        def run_case(label, reader, workers, *, smoke=False, baseline=False):
            require(digest(binaries[reader]) == state['binary_sha256'][reader], 'executable changed')
            require(shutil.disk_usage(results).free > 2 * OUTPUT_BYTES + (5 << 30), 'insufficient export space')
            case = results / label
            case.mkdir()
            current = small if smoke else plan
            cmd = [str(binaries[reader]), '--workers', str(workers),
                   '--plan', str(small_path if smoke else plan_path),
                   '--output', str(case / 'report.json'), '--export', str(case / 'transactions.bin')]
            if reader == 'car':
                parser_workers, parser_args = parser_configuration(args.parser_workers, workers,
                                                                    smoke=smoke, baseline=baseline)
                cmd += ['--url', args.base + '900/epoch-900.car', '--operator-trusted',
                        '--legacy-http-buffers', '--http-workers', str(4 if baseline else args.http_workers),
                        '--http-window-chunks', str(8 if baseline else args.http_window_chunks),
                        '--http-chunk-mib', str(32 if baseline else args.http_chunk_mib)]
                cmd += parser_args
            else:
                cmd += ['--epoch', str(EPOCH), '--http-base', args.base, '--index-base', args.base,
                        '--start-slot', str(START), '--end-slot-exclusive', str(current['end_slot_exclusive']),
                        '--reported-error-policy', 'strict']
            row = {'label': label, 'reader': reader, 'workers': workers, 'smoke': smoke,
                   'baseline': baseline, 'command': cmd, 'state': 'RUNNING',
                   'host_before': host_snapshot()}
            if reader == 'car':
                row['parser_workers'] = parser_workers
            state['cases'].append(row)
            state['current'] = label
            save()
            print(f'RUN {label}', flush=True)
            row.update(execute(cmd, case, args.timeout_seconds))
            row['host_after'] = host_snapshot()
            require(row['exit_code'] == 0, f'{label}: reader failed; see retained stderr.log')
            receipt = json.loads((case / 'report.json').read_text())
            expected = current['block_transaction_rows']
            checked = verify_export(case / 'transactions.bin', expected)
            validate_report(receipt, checked, expected, reader, smoke)
            require(receipt.get('workers' if reader == 'car' else 'processing_workers') == workers,
                    'receipt worker count differs from the requested setting')
            if reader == 'car':
                validate_parser_receipt(receipt, parser_workers)
                require(receipt.get('http_workers') == (4 if baseline else args.http_workers)
                        and receipt.get('http_window_chunks') == (8 if baseline else args.http_window_chunks)
                        and receipt.get('http_chunk_bytes') == (32 if baseline else args.http_chunk_mib) * (1 << 20)
                        and receipt.get('reuse_http_buffers') is False
                        and receipt.get('metadata_mode') == 'Owned', 'CAR settings differ from the requested case')
            if smoke in references:
                identical(references[smoke], case / 'transactions.bin')
                row['byte_parity'] = 'PASS'
            else:
                references[smoke] = case / 'transactions.bin'
                row['byte_parity'] = 'REFERENCE'
            row.update(checked, state='PASS', total_seconds=receipt['total_seconds'],
                       scan_seconds=receipt.get('scan_seconds'),
                       total_tps=receipt['transactions'] / receipt['total_seconds'],
                       mean_cpu_cores=row['cpu_seconds'] / row['process_seconds'],
                       report_sha256=digest(case / 'report.json'), http=receipt.get('http'),
                       http_transport_mode=receipt.get('http_transport_mode'),
                       effective_http_workers=receipt.get('effective_http_workers'),
                       http_body_window_bytes=receipt.get('http_body_window_bytes'))
            if reader == 'jetstreamer':
                for key in ['upstream_commit', 'upstream_version', 'reported_error_policy',
                            'reported_body_retries', 'reported_body_retry_limit_per_worker_slot',
                            'reported_error_events', 'reported_error_events_truncated',
                            'upstream_connection_recycles', 'upstream_operation_timeouts', 'upstream_work_steals']:
                    row[key] = receipt.get(key)
            if receipt.get('scan_seconds', 0) > 0:
                row['scan_tps'] = receipt['transactions'] / receipt['scan_seconds']
            save()
            print(f"PASS {label}: {row['total_seconds']:.3f}s, {row['total_tps']:.0f} tx/s", flush=True)

        for reader in ['car', 'jetstreamer']:
            run_case('smoke-' + reader, reader, 12, smoke=True, baseline=True)
        if args.baseline:
            for reader in ['car', 'jetstreamer']:
                run_case('baseline-' + reader, reader, 12, baseline=True)
        for index, workers in enumerate(args.workers):
            order = ['car', 'jetstreamer'] if index % 2 == 0 else ['jetstreamer', 'car']
            for reader in order:
                run_case(f'sweep-{workers}-{reader}', reader, workers)
        best = {reader: min((row for row in state['cases'] if row['reader'] == reader
                             and row['label'].startswith('sweep-')), key=lambda row: row['total_seconds'])
                for reader in ['car', 'jetstreamer']}
        state['selected_settings'] = {reader: {'workers': row['workers'], 'selection_case': row['label']}
                                      for reader, row in best.items()}
        state['selected_settings']['car']['parser_workers'] = best['car']['parser_workers']
        for repeat in range(args.best_repeats):
            order = ['jetstreamer', 'car'] if repeat % 2 == 0 else ['car', 'jetstreamer']
            for reader in order:
                run_case(f'confirm-{repeat + 1}-{reader}', reader, best[reader]['workers'])
        state['summary'] = confirmation_summary(state['cases'], state['selected_settings'])
        state['source_after'] = source_checks(args.base)
        require(state['source_after'] == state['source_before'], 'source identity changed during sweep')
        state.update(state='PASS', current=None, host_after=host_snapshot())
    except BaseException as error:
        for row in state['cases']:
            if row['state'] == 'RUNNING':
                row.update(state='FAILED', error=repr(error))
        state.update(state='FAILED', error=repr(error), host_after=host_snapshot())
        raise
    finally:
        save()


if __name__ == '__main__':
    main()
