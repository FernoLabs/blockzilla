#!/usr/bin/env python3
"""Run and verify one full Triton epoch with CAR and Jetstreamer on Linux."""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import shutil
import signal
import subprocess
import time
import urllib.request

from run_manycore import (CAR_BYTES, CAR_SAMPLES, EPOCH, INDEX_BYTES, INDEX_SHA256, JETSTREAMER_COMMIT,
                          START, digest, fetch_exact, host_snapshot,
                          parser_configuration, require,
                          validate_parser_receipt)

END = 389232000
BLOCKS = 431858
TRANSACTIONS = 476026811
CANONICAL_INDEX_SHA256 = '4488c7cf2750c0a7fe11ae798b9d960ed72a0a9e904aa336a30c35198697d55d'
JETSTREAMER_BODY_ERROR = 'Error reading until block: error decoding response body'
JETSTREAMER_HEADER_BODY_ERROR = 'Error reading header: error decoding response body'


def default_dataset():
    return {'epoch': EPOCH, 'start_slot': START, 'end_slot_exclusive': END,
            'blocks': BLOCKS, 'transactions': TRANSACTIONS,
            'canonical_index_sha256': CANONICAL_INDEX_SHA256,
            'car_bytes': CAR_BYTES, 'index_bytes': INDEX_BYTES, 'index_sha256': INDEX_SHA256,
            'source_sample_bytes': 65536,
            'car_samples': [{'start': start, 'sha256': sha} for start, sha in CAR_SAMPLES]}


def validate_dataset(dataset):
    require(isinstance(dataset, dict), 'dataset manifest must be an object')
    for key in ['epoch', 'start_slot', 'end_slot_exclusive', 'blocks', 'transactions',
                'car_bytes', 'index_bytes']:
        require(type(dataset.get(key)) is int and 0 <= dataset[key] < 1 << 64,
                f'invalid dataset integer: {key}')
    require(dataset['start_slot'] == dataset['epoch'] * 432000
            and dataset['end_slot_exclusive'] == dataset['start_slot'] + 432000,
            'dataset must cover one complete 432000-slot epoch')
    require(0 < dataset['blocks'] <= 432000 and dataset['transactions'] > 0
            and dataset['car_bytes'] > 0 and dataset['index_bytes'] == 432000 * 12,
            'invalid dataset block, transaction, CAR, or raw slot-index size')
    require(dataset.get('source_sample_bytes', 65536) == 65536,
            'dataset source samples must contain 65536 bytes')
    for key in ['canonical_index_sha256', 'index_sha256']:
        require(isinstance(dataset.get(key), str) and re.fullmatch('[0-9a-f]{64}', dataset[key]),
                f'invalid dataset SHA-256: {key}')
    samples = dataset.get('car_samples')
    require(isinstance(samples, list) and len(samples) >= 3, 'dataset requires at least three CAR samples')
    previous = -1
    for sample in samples:
        require(isinstance(sample, dict) and type(sample.get('start')) is int
                and previous < sample['start'] <= dataset['car_bytes'] - 65536,
                'CAR sample starts must be distinct, sorted, and within the object')
        require(isinstance(sample.get('sha256'), str) and re.fullmatch('[0-9a-f]{64}', sample['sha256']),
                'invalid CAR sample SHA-256')
        previous = sample['start']
    return dataset


def source_checks(base, dataset=None):
    dataset = default_dataset() if dataset is None else dataset
    epoch = dataset['epoch']
    car_url = f'{base}{epoch}/epoch-{epoch}.car'
    request = urllib.request.Request(car_url, method='HEAD', headers={
        'Accept-Encoding': 'identity', 'User-Agent': 'blockzilla-reader-benchmark/1'})
    with urllib.request.urlopen(request, timeout=60) as response:
        require(response.status == 200 and response.url == car_url, 'CAR HEAD status or redirect')
        require(int(response.headers['Content-Length']) == dataset['car_bytes'], 'CAR object length changed')
        etag = response.headers.get('ETag')
    index_sha = fetch_exact(f'{base}{epoch}/epoch-{epoch}-slot-ranges.raw', size=dataset['index_bytes'])
    require(index_sha == dataset['index_sha256'], 'raw slot index differs from the dataset manifest')
    samples = []
    for sample in dataset['car_samples']:
        actual = fetch_exact(car_url, size=65536, start=sample['start'], total=dataset['car_bytes'])
        require(actual == sample['sha256'], f"CAR source sample changed at {sample['start']}")
        samples.append({'start': sample['start'], 'bytes': 65536, 'sha256': actual})
    return {'car_bytes': dataset['car_bytes'], 'car_etag': etag, 'car_samples': samples,
            'index_bytes': dataset['index_bytes'], 'index_sha256': index_sha}


def validate_full_plan(plan, dataset=None):
    dataset = default_dataset() if dataset is None else dataset
    start, end = dataset['start_slot'], dataset['end_slot_exclusive']
    require(plan.get('epoch') == dataset['epoch'] and plan.get('start_slot') == start
            and plan.get('end_slot_exclusive') == end, 'expected the complete dataset epoch range')
    rows = plan.get('block_transaction_rows')
    require(isinstance(rows, list) and len(rows) == dataset['blocks'], 'full canonical block count differs')
    require(all(isinstance(row, list) and len(row) == 2 and all(type(v) is int for v in row)
                and start <= row[0] < end and row[1] >= 0 for row in rows), 'invalid canonical row')
    require(rows[0][0] == start and rows[-1][0] + 1 == end
            and all(a[0] < b[0] for a, b in zip(rows, rows[1:])), 'invalid canonical slot order')
    require(sum(row[1] for row in rows) == dataset['transactions'], 'full canonical transaction count differs')
    source = plan.get('source_index', {})
    require(source.get('index_sha256') == dataset['canonical_index_sha256']
            and source.get('accepted_index_sha256') == dataset['canonical_index_sha256'],
            'full plan lacks the independently verified canonical index hash')
    require(source.get('index_block_count') == len(rows)
            and source.get('index_transactions') == sum(row[1] for row in rows),
            'plan does not include every row and transaction of its canonical index')
    return rows


def validate_jetstreamer_policy(receipt, workers, body_retries=False):
    expected = 'body-transport' if body_retries else 'strict'
    require(receipt.get('reported_error_policy') == expected, 'Jetstreamer reported-error policy differs')
    require(receipt.get('errors') == [], 'Jetstreamer reported a fatal error')
    require(receipt.get('upstream_version') == '0.7.0'
            and receipt.get('upstream_commit') == JETSTREAMER_COMMIT,
            'Jetstreamer version or revision differs')
    count, events = receipt.get('reported_body_retries'), receipt.get('reported_error_events')
    require(type(count) is int and count >= 0 and (body_retries or count == 0),
            'invalid Jetstreamer body retry count')
    require(receipt.get('reported_body_retry_limit_per_worker_slot') == 2,
            'Jetstreamer body retry limit differs')
    require(isinstance(events, list) and len(events) == min(count, 64)
            and receipt.get('reported_error_events_truncated') is (count > 64),
            'Jetstreamer reported error samples differ from the retry count')
    attempts = {}
    for event in events:
        require(isinstance(event, dict) and event.get('error_message') in
                (JETSTREAMER_BODY_ERROR, JETSTREAMER_HEADER_BODY_ERROR),
                'Jetstreamer retry sample has an unapproved error')
        worker, slot, attempt = event.get('worker'), event.get('slot'), event.get('attempt')
        require(type(worker) is int and 0 <= worker < workers and type(slot) is int
                and receipt['start_slot'] <= slot < receipt['end_slot_exclusive'],
                'Jetstreamer retry sample has an invalid worker or slot')
        require(type(attempt) is int and 1 <= attempt <= 2
                and attempt == attempts.get((worker, slot), 0) + 1,
                'Jetstreamer retry sample exceeds its worker/slot limit or is out of order')
        elapsed = event.get('elapsed_seconds')
        require(type(elapsed) in (int, float) and math.isfinite(elapsed)
                and 0 <= elapsed <= receipt['total_seconds'], 'invalid Jetstreamer retry event time')
        attempts[worker, slot] = attempt


def validate_full_receipt(receipt, rows, reader, workers, parser_workers, jetstreamer_body_retries=False):
    if reader == 'jetstreamer':
        validate_jetstreamer_policy(receipt, workers, jetstreamer_body_retries)
    require(receipt.get('valid') is True and receipt.get('block_transaction_rows') == rows,
            'receipt differs from the exact full canonical plan')
    require(receipt.get('blocks') == len(rows) and receipt.get('transactions') == sum(row[1] for row in rows),
            'receipt block or transaction count differs')
    require(receipt.get('export_schema') == 'car-common-export-v1'
            and receipt.get('allocator') == 'mimalloc', 'export schema or allocator differs')
    require(receipt.get('signature_verification') is False, 'signature verification setting differs')
    require(isinstance(receipt.get('total_seconds'), (int, float))
            and math.isfinite(receipt['total_seconds']) and receipt['total_seconds'] > 0,
            'invalid total duration')
    require(receipt.get('workers' if reader == 'car' else 'processing_workers') == workers,
            'receipt worker count differs')
    if reader == 'car':
        validate_parser_receipt(receipt, parser_workers)
        require(receipt.get('metadata_mode') == 'Owned' and receipt.get('reuse_http_buffers') is False,
                'CAR metadata or allocation mode differs')
        counters = {'votes': receipt.get('votes'), 'failed': receipt.get('failed')}
    else:
        require(receipt.get('transaction_callbacks') == sum(row[1] for row in rows),
                'Jetstreamer callback count differs')
        counters = {'votes': sum(row['simple_vote_callbacks'] for row in receipt['worker_counters']),
                    'failed': sum(row['failed_status_callbacks'] for row in receipt['worker_counters'])}
    require(all(type(value) is int and 0 <= value <= receipt['transactions'] for value in counters.values()),
            'invalid vote or failure counters')
    return counters


def validate_verified_export(checked, receipt, counters, rows):
    require(checked.get('blocks') == len(rows)
            and checked.get('transactions') == sum(row[1] for row in rows), 'verifier coverage differs')
    require(checked.get('output_bytes') == receipt.get('export_bytes'), 'verifier byte count differs')
    require(checked.get('votes') == counters['votes'] and checked.get('failed') == counters['failed'],
            'export vote or failure totals differ from the reader receipt')


def process_sample(pid):
    try:
        stat = Path(f'/proc/{pid}/stat').read_text()
        fields = stat[stat.rfind(')') + 2:].split()
        return {'cpu_seconds': (int(fields[11]) + int(fields[12])) / os.sysconf('SC_CLK_TCK'),
                'rss_mib': int(fields[21]) * os.sysconf('SC_PAGE_SIZE') / (1 << 20)}
    except (OSError, ValueError, IndexError):
        return {}


def run_process(command, directory, prefix, timeout, progress):
    started = last_update = time.monotonic()
    with (directory / f'{prefix}.stdout.log').open('xb') as stdout, \
            (directory / f'{prefix}.stderr.log').open('xb') as stderr:
        child = subprocess.Popen(command, stdout=stdout, stderr=stderr, stdin=subprocess.DEVNULL,
                                 start_new_session=True)
        try:
            progress({'pid': child.pid, 'elapsed_seconds': 0})
            while True:
                pid, status, usage = os.wait4(child.pid, os.WNOHANG)
                now = time.monotonic()
                if pid:
                    child.returncode = os.waitstatus_to_exitcode(status)
                    return {'exit_code': child.returncode, 'process_seconds': now - started,
                            'cpu_seconds': usage.ru_utime + usage.ru_stime,
                            'user_cpu_seconds': usage.ru_utime, 'system_cpu_seconds': usage.ru_stime,
                            'peak_rss_mib': usage.ru_maxrss / 1024}
                require(now - started < timeout, f'{prefix} process exceeded its time limit')
                if now - last_update >= 5:
                    progress(dict(process_sample(child.pid), pid=child.pid, elapsed_seconds=now - started))
                    last_update = now
                time.sleep(.1)
        finally:
            if child.returncode is None:
                os.killpg(child.pid, signal.SIGKILL)
                _, status, _ = os.wait4(child.pid, 0)
                child.returncode = os.waitstatus_to_exitcode(status)


def hash_output(path, progress, timeout):
    started = last_update = time.monotonic()
    hasher = hashlib.sha256()
    consumed = 0
    with path.open('rb') as stream:
        while chunk := stream.read(8 << 20):
            hasher.update(chunk)
            consumed += len(chunk)
            now = time.monotonic()
            require(now - started < timeout, 'output hashing exceeded its time limit')
            if now - last_update >= 5:
                progress({'bytes': consumed, 'elapsed_seconds': now - started})
                last_update = now
    progress({'bytes': consumed, 'elapsed_seconds': time.monotonic() - started})
    return hasher.hexdigest(), consumed


def compare_outputs(left, right, progress, timeout):
    started = last_update = time.monotonic()
    compared = 0
    with left.open('rb') as a, right.open('rb') as b:
        while True:
            x, y = a.read(8 << 20), b.read(8 << 20)
            require(x == y, f'export bytes differ in the chunk at offset {compared}')
            if not x:
                break
            compared += len(x)
            now = time.monotonic()
            require(now - started < timeout, 'byte comparison exceeded its time limit')
            if now - last_update >= 5:
                progress({'bytes': compared, 'elapsed_seconds': now - started})
                last_update = now
    progress({'bytes': compared, 'elapsed_seconds': time.monotonic() - started})
    return compared


def run_reader_series(order, run_reader, record_failure, continue_after_failure=False):
    failures = []
    for reader in order:
        try:
            run_reader(reader)
        except InterruptedError:
            raise
        except Exception as error:
            record_failure(reader, error)
            failures.append(reader)
            if not continue_after_failure:
                raise
    return failures


def validate_comparison_candidates(cases):
    by_reader = {row['reader']: row for row in cases}
    require(len(cases) == 2 and set(by_reader) == {'car', 'jetstreamer'}
            and all(row.get('state') == 'PASS' for row in cases),
            'both complete readers must pass before comparison')
    car, jetstreamer = by_reader['car'], by_reader['jetstreamer']
    for key in ['output_bytes', 'output_sha256', 'votes', 'failed', 'blocks', 'transactions']:
        require(car[key] == jetstreamer[key], f'cross-reader {key} differs')
    return by_reader


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--car-bin', type=Path, required=True)
    parser.add_argument('--jetstreamer-bin', type=Path, required=True)
    parser.add_argument('--verifier-bin', type=Path, required=True)
    parser.add_argument('--plan', type=Path, required=True)
    parser.add_argument('--dataset-manifest', type=Path,
                        help='independently prepared full-epoch dataset; defaults to the accepted epoch-900 specification')
    parser.add_argument('--results', type=Path, required=True)
    parser.add_argument('--car-workers', type=int, required=True)
    parser.add_argument('--car-parser-workers', type=int, default=1)
    parser.add_argument('--jetstreamer-workers', type=int, required=True)
    parser.add_argument('--jetstreamer-body-retries', action='store_true',
                        help='permit only bounded response-body retries between complete Jetstreamer blocks')
    parser.add_argument('--car-http-workers', type=int, default=16)
    parser.add_argument('--car-http-window-chunks', type=int, default=16)
    parser.add_argument('--car-http-chunk-mib', type=int, default=32)
    parser.add_argument('--base', default='https://files.old-faithful.net/')
    parser.add_argument('--order', choices=['car-first', 'jetstreamer-first'], default='car-first')
    parser.add_argument('--timeout-seconds', type=int, default=7200)
    parser.add_argument('--verification-timeout-seconds', type=int, default=3600)
    parser.add_argument('--minimum-free-gib', type=int, default=512)
    parser.add_argument('--continue-after-reader-failure', action='store_true',
                        help='try both readers independently; any failed reader still fails the overall result')
    args = parser.parse_args()
    require(platform.system() == 'Linux', 'this runner requires Linux resource accounting')
    require(1 <= args.car_workers <= 256 and 1 <= args.jetstreamer_workers <= 256,
            'decode workers must be in 1..256')
    parser_workers, parser_flags = parser_configuration(args.car_parser_workers, args.car_workers)
    require(1 <= args.car_http_workers <= args.car_http_window_chunks <= 16
            and 1 <= args.car_http_chunk_mib <= 32, 'invalid CAR HTTP settings')
    require(args.timeout_seconds > 0 and args.verification_timeout_seconds > 0
            and args.minimum_free_gib > 0, 'timeouts and disk threshold must be positive')
    require(args.base.startswith('https://') and args.base.endswith('/'), 'base must be HTTPS with a final slash')
    dataset = validate_dataset(json.loads(args.dataset_manifest.read_text())
                               if args.dataset_manifest else default_dataset())
    epoch, start, end = dataset['epoch'], dataset['start_slot'], dataset['end_slot_exclusive']
    car_url = f'{args.base}{epoch}/epoch-{epoch}.car'
    os.umask(0o077)
    def interrupted(signum, _frame):
        raise InterruptedError(f'runner received signal {signum}')
    signal.signal(signal.SIGTERM, interrupted)
    results = args.results.resolve()
    results.mkdir(parents=True, exist_ok=False)
    state = {'schema': 'triton-full-epoch-common-export-v1', 'state': 'PREPARING',
             'pid': os.getpid(), 'started_at': time.time(), 'cases': [], 'host_before': host_snapshot(),
             'comparison': {'state': 'NOT_RUN', 'reason': 'Both independent readers must pass first.'},
             'settings': {key: str(value) if isinstance(value, Path) else value for key, value in vars(args).items()},
             'dataset_manifest': dataset,
             'jetstreamer_environment': {name: os.environ.get(name) for name in [
                 'JETSTREAMER_SPAWN_PENDING', 'JETSTREAMER_SPAWN_GRACE_SECS',
                 'JETSTREAMER_RECYCLE_PCT', 'JETSTREAMER_FORCE_LEGACY_INDEX']},
             'timing_scope': 'Reader total_seconds includes source setup, decode, spool writes, ordered output copy, '
                             'and final file sync. Verification, SHA-256, and byte comparison occur after timing.',
             'source_check_limit': f"Full slot-index hash, CAR length, and {len(dataset['car_samples'])} 64-KiB CAR samples. "
                                   'Full CAR identity and every response version are not pinned.',
             'retry_policy': 'CAR successful transport retries remain included in time and counters. '
                             'Returned transport or decode errors fail. Jetstreamer uses ' +
                             ('body-transport: at most two exact response-body errors per worker/slot, '
                              'only with empty callback/export state; all other reported errors fail.'
                              if args.jetstreamer_body_retries else 'strict: all reported errors fail.') +
                             ' All callback, plan, structure, hash, and byte-parity checks remain mandatory.',
             'measurement_limit': 'One full-epoch process per reader. This result has no repeated full-epoch sample.'}

    def save():
        state['updated_at'] = time.time()
        temporary = results / 'status.tmp'
        temporary.write_text(json.dumps(state, indent=2) + '\n')
        temporary.replace(results / 'status.json')

    def phase(row, name):
        row['phase'] = name
        row.pop('progress', None)
        print(f"{row['reader']}: {name}", flush=True)
        save()

    def progress(row):
        def update(sample):
            row['progress'] = sample
            save()
        return update

    try:
        save()
        dataset_path = results / 'dataset-manifest.json'
        if args.dataset_manifest:
            shutil.copyfile(args.dataset_manifest, dataset_path)
        else:
            dataset_path.write_text(json.dumps(dataset, indent=2) + '\n')
        state['dataset_manifest_sha256'] = digest(dataset_path)
        state['dataset_manifest_source'] = str(args.dataset_manifest.resolve()) if args.dataset_manifest else 'built-in epoch-900 default'
        plan = json.loads(args.plan.read_text())
        rows = validate_full_plan(plan, dataset)
        plan_path = results / 'plan.json'
        plan_path.write_text(json.dumps(plan, separators=(',', ':')) + '\n')
        plan_tsv = results / 'plan.tsv'
        with plan_tsv.open('x') as output:
            for slot, count in rows:
                output.write(f'{slot}\t{count}\n')
        state['plan'] = {'epoch': epoch, 'start_slot': start, 'end_slot_exclusive': end,
                         'blocks': len(rows), 'transactions': sum(row[1] for row in rows),
                         'source_index': plan['source_index'], 'source_plan_sha256': digest(args.plan),
                         'saved_plan_sha256': digest(plan_path), 'tsv_sha256': digest(plan_tsv)}
        binaries = {'car': args.car_bin.resolve(), 'jetstreamer': args.jetstreamer_bin.resolve(),
                    'verifier': args.verifier_bin.resolve()}
        state['binary_sha256'] = {name: digest(path) for name, path in binaries.items()}
        state['control_sha256'] = {name: digest(Path(__file__).with_name(name))
                                   for name in ['run_full_epoch.py', 'run_manycore.py', 'run.py']}
        print('Check Triton source before the full epoch.', flush=True)
        state['source_before'] = source_checks(args.base, dataset)
        state['state'] = 'RUNNING'
        save()
        order = ['car', 'jetstreamer'] if args.order == 'car-first' else ['jetstreamer', 'car']

        def run_reader(reader):
            require(shutil.disk_usage(results).free >= args.minimum_free_gib * (1 << 30),
                    'insufficient free disk space for the full export and spool')
            for name in [reader, 'verifier']:
                require(digest(binaries[name]) == state['binary_sha256'][name], f'{name} executable changed')
            workers = args.car_workers if reader == 'car' else args.jetstreamer_workers
            case = results / reader
            case.mkdir()
            export_path = case / 'transactions.bin'
            command = [str(binaries[reader]), '--workers', str(workers), '--plan', str(plan_path),
                       '--output', str(case / 'report.json'), '--export', str(export_path)]
            if reader == 'car':
                command += ['--url', car_url, '--operator-trusted', '--legacy-http-buffers',
                            '--http-workers', str(args.car_http_workers),
                            '--http-window-chunks', str(args.car_http_window_chunks),
                            '--http-chunk-mib', str(args.car_http_chunk_mib)] + parser_flags
            else:
                command += ['--epoch', str(epoch), '--http-base', args.base, '--index-base', args.base,
                            '--start-slot', str(start), '--end-slot-exclusive', str(end),
                            '--reported-error-policy', 'body-transport' if args.jetstreamer_body_retries else 'strict']
            row = {'reader': reader, 'workers': workers, 'command': command, 'state': 'RUNNING',
                   'host_before': host_snapshot()}
            if reader == 'car':
                row['parser_workers'] = parser_workers
            state['cases'].append(row)
            state['current'] = reader
            phase(row, 'READING_AND_EXPORTING')
            row.update(run_process(command, case, 'reader', args.timeout_seconds, progress(row)))
            row['host_after_reader'] = host_snapshot()
            require(row['exit_code'] == 0, f'{reader} failed; see reader.stderr.log')
            receipt = json.loads((case / 'report.json').read_text())
            counters = validate_full_receipt(receipt, rows, reader, workers, parser_workers,
                                             args.jetstreamer_body_retries)
            require(receipt.get('epoch') == epoch, 'reader receipt epoch differs')
            if reader == 'car':
                require(receipt.get('http_workers') == args.car_http_workers
                        and receipt.get('http_window_chunks') == args.car_http_window_chunks
                        and receipt.get('http_chunk_bytes') == args.car_http_chunk_mib * (1 << 20),
                        'CAR requested HTTP settings differ')
                require(receipt.get('identity', {}).get('url') == car_url
                        and receipt.get('identity', {}).get('bytes') == dataset['car_bytes'], 'CAR source identity differs')
            else:
                require(receipt.get('http_base') == args.base and receipt.get('index_base') == args.base
                        and receipt.get('start_slot') == start and receipt.get('end_slot_exclusive') == end,
                        'Jetstreamer source or slot range differs')
            row.update(total_seconds=receipt['total_seconds'], scan_seconds=receipt.get('scan_seconds'),
                       total_tps=receipt['transactions'] / receipt['total_seconds'],
                       mean_cpu_cores=row['cpu_seconds'] / row['process_seconds'],
                       transactions=receipt['transactions'], blocks=receipt['blocks'],
                       http=receipt.get('http'), http_transport_mode=receipt.get('http_transport_mode'),
                       effective_http_workers=receipt.get('effective_http_workers'),
                       http_body_window_bytes=receipt.get('http_body_window_bytes'),
                       source_identity=receipt.get('identity'), report_sha256=digest(case / 'report.json'))
            if reader == 'jetstreamer':
                for key in ['upstream_commit', 'upstream_version', 'reported_error_policy',
                            'reported_body_retries', 'reported_body_retry_limit_per_worker_slot',
                            'reported_error_events', 'reported_error_events_truncated',
                            'upstream_connection_recycles', 'upstream_operation_timeouts', 'upstream_work_steals']:
                    row[key] = receipt.get(key)
            if receipt.get('scan_seconds', 0) > 0:
                row['scan_tps'] = receipt['transactions'] / receipt['scan_seconds']
            phase(row, 'VERIFYING_EVERY_TRANSACTION')
            verification = [str(binaries['verifier']), '--plan-tsv', str(plan_tsv), '--input', str(export_path)]
            row['verification_command'] = verification
            row['verification_process'] = run_process(verification, case, 'verify',
                                                      args.verification_timeout_seconds, progress(row))
            require(row['verification_process']['exit_code'] == 0, 'native export verification failed')
            checked = json.loads((case / 'verify.stdout.log').read_text())
            validate_verified_export(checked, receipt, counters, rows)
            phase(row, 'HASHING_EXPORT')
            sha256, output_bytes = hash_output(export_path, progress(row), args.verification_timeout_seconds)
            require(output_bytes == checked['output_bytes'], 'export changed after structural verification')
            row.update(checked, output_sha256=sha256, byte_parity='NOT_COMPARED')
            phase(row, 'CHECKING_SOURCE_AFTER_READER')
            row['source_after'] = source_checks(args.base, dataset)
            require(row['source_after'] == state['source_before'], 'source changed during the full epoch run')
            row.update(state='PASS', phase='COMPLETE', host_after=host_snapshot())
            row.pop('progress', None)
            save()
            print(f"PASS {reader}: {row['total_seconds']:.3f}s, {row['total_tps']:.0f} tx/s", flush=True)

        def record_reader_failure(reader, error):
            row = next((row for row in state['cases'] if row['reader'] == reader), None)
            if row is None:
                row = {'reader': reader}
                state['cases'].append(row)
            row.update(state='FAILED', phase='FAILED', error=repr(error),
                       byte_parity='NOT_COMPARED', host_after=host_snapshot())
            print(f'FAILED {reader}: {error}', flush=True)
            save()
            try:
                row['source_after_failure'] = source_checks(args.base, dataset)
                require(row['source_after_failure'] == state['source_before'],
                        'source changed during failed reader attempt')
            except InterruptedError:
                raise
            except Exception as source_error:
                row['source_check_error'] = repr(source_error)
            save()

        failures = run_reader_series(order, run_reader, record_reader_failure,
                                     args.continue_after_reader_failure)
        if failures:
            state['failed_readers'] = failures
            state['comparison'] = {'state': 'NOT_RUN', 'reason': 'One or more independent readers failed.'}
            raise RuntimeError('full comparison failed; reader failures: ' + ', '.join(failures))
        state['current'] = 'comparison'
        state['comparison'] = {'state': 'RUNNING', 'phase': 'CHECKING_COUNTERS_AND_HASHES'}
        save()
        by_reader = validate_comparison_candidates(state['cases'])
        state['comparison']['phase'] = 'COMPARING_EXACT_EXPORT_BYTES'
        print('Compare both complete exports byte for byte.', flush=True)
        save()
        compared = compare_outputs(results / 'car' / 'transactions.bin',
                                   results / 'jetstreamer' / 'transactions.bin',
                                   progress(state['comparison']), args.verification_timeout_seconds)
        require(compared == by_reader['car']['output_bytes'], 'byte comparison length differs')
        for row in state['cases']:
            row['byte_parity'] = 'PASS'
        state['comparison'].update(state='PASS', phase='COMPLETE', compared_bytes=compared)
        state['comparison'].pop('progress', None)
        state['summary'] = {row['reader']: {key: row[key] for key in
            ['workers', 'total_seconds', 'total_tps', 'cpu_seconds', 'mean_cpu_cores', 'peak_rss_mib',
             'transactions', 'blocks', 'output_bytes', 'output_sha256', 'votes', 'failed', 'byte_parity']}
                            for row in state['cases']}
        state['summary']['car']['parser_workers'] = parser_workers
        state.update(state='PASS', current=None, host_after=host_snapshot())
    except BaseException as error:
        for row in state['cases']:
            if row['state'] == 'RUNNING':
                row.update(state='FAILED', error=repr(error))
        if state['comparison']['state'] == 'RUNNING':
            state['comparison'].update(state='FAILED', error=repr(error))
        state.update(state='FAILED', current=None, error=repr(error), host_after=host_snapshot())
        raise
    finally:
        save()


if __name__ == '__main__':
    main()
