#!/usr/bin/env python3
"""Historical one-off control: compare current Jetstreamer with verified CAR output.

Run from /home/ubuntu/benchmarks/zilla-triton-20260930 on the test host.
The prior series remains FAILED; this control creates a separate comparison.
"""
import argparse
import copy
import json
import os
from pathlib import Path
import shutil
import signal
import sys
import time

ROOT = Path.cwd()
sys.path.insert(0, str(ROOT / 'source/bench/car-export'))
import run_full_epoch as control


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--result-name', default='results-latest-jetstreamer-full')
    args = parser.parse_args()
    if not args.result_name.startswith('results-') or Path(args.result_name).name != args.result_name:
        parser.error('result-name must be a single directory name that starts with results-')
    os.umask(0o077)
    result = ROOT / args.result_name
    result.mkdir(exist_ok=False)
    old = ROOT / 'results-full-epoch-1000-64'
    original = json.loads((old / 'status.json').read_text())
    state = {'schema': 'triton-cross-run-full-epoch-comparison-v1', 'state': 'PREPARING',
             'started_at': time.time(), 'pid': os.getpid(), 'cases': [],
             'measurement_limit': 'One retained, individually verified CAR run and one new Jetstreamer run; '
                                  'no repeated-run mean or randomized-order claim.',
             'source_check_limit': 'Complete raw index, CAR length, and 17 samples; not every CAR response is pinned.',
             'timing_scope': 'Receipt total time includes setup, decode, export ordering and final sync. '
                             'All independent verification and comparison are outside reader timing.',
             'prior_controller_sha256': control.digest(old / 'status.json'),
             'controller_sha256': control.digest(Path(__file__)),
             'prior_controller_state': original['state'],
             'prior_controller': str(old / 'status.json'),
             'jetstreamer_environment': {k: os.environ.get(k) for k in [
                 'JETSTREAMER_SPAWN_PENDING', 'JETSTREAMER_SPAWN_GRACE_SECS',
                 'JETSTREAMER_RECYCLE_PCT', 'JETSTREAMER_FORCE_LEGACY_INDEX']},
             'comparison': {'state': 'NOT_RUN'}}

    def save():
        state['updated_at'] = time.time()
        tmp = result / 'status.tmp'
        tmp.write_text(json.dumps(state, indent=2) + '\n')
        tmp.replace(result / 'status.json')

    def phase(name):
        state['phase'] = name
        state.pop('progress', None)
        print(name, flush=True)
        save()

    def progress(sample):
        state['progress'] = sample
        save()

    def stop(number, _frame):
        raise InterruptedError(f'controller received signal {number}')

    signal.signal(signal.SIGTERM, stop)
    try:
        phase('VALIDATING_RETAINED_CAR_RESULT')
        dataset = control.validate_dataset(json.loads((ROOT / 'control/dataset-1000.json').read_text()))
        plan_path, tsv = old / 'plan.json', old / 'plan.tsv'
        rows = control.validate_full_plan(json.loads(plan_path.read_text()), dataset)
        control.require(control.digest(plan_path) == original['plan']['saved_plan_sha256']
                        and control.digest(tsv) == original['plan']['tsv_sha256'], 'retained plan changed')
        control.require(dataset == original['dataset_manifest'], 'retained dataset differs')
        car = copy.deepcopy(next(row for row in original['cases'] if row['reader'] == 'car'))
        control.require(car['state'] == 'PASS' and car['workers'] == car['parser_workers'] == 64,
                        'retained CAR case must already pass at the selected settings')
        car_report_path = old / 'car/report.json'
        control.require(control.digest(car_report_path) == car['report_sha256'], 'retained CAR receipt changed')
        car_report = json.loads(car_report_path.read_text())
        car_counters = control.validate_full_receipt(car_report, rows, 'car', 64, 64)
        control.validate_verified_export(json.loads((old / 'car/verify.stdout.log').read_text()),
                                         car_report, car_counters, rows)
        car['retained_from'] = str(old / 'car')
        car['byte_parity'] = 'NOT_COMPARED'
        state['cases'].append(car)
        state['dataset_manifest'] = dataset
        state['plan'] = original['plan']
        jet_bin = ROOT / 'source/bench/jetstreamer-reference/target/release/jetstreamer-common-export-reference'
        verifier = ROOT / 'control/verify-export'
        control.require(control.digest(verifier) == original['binary_sha256']['verifier'], 'verifier changed')
        control.require(control.digest(ROOT / 'source/target/release/car-decode-reference')
                        == original['binary_sha256']['car'], 'retained CAR executable changed')
        state['binary_sha256'] = {'car': original['binary_sha256']['car'],
                                  'jetstreamer': control.digest(jet_bin), 'verifier': control.digest(verifier)}
        state['helper_sha256'] = {name: control.digest(Path(control.__file__).with_name(name))
                                  for name in ['run_full_epoch.py', 'run_manycore.py', 'run.py']}
        phase('CHECKING_SOURCE_BEFORE_JETSTREAMER')
        before = control.source_checks('https://files.old-faithful.net/', dataset)
        control.require(before == original['source_before'] == car['source_after'],
                        'source differs from the retained CAR run')
        state['source_before'] = before
        control.require(shutil.disk_usage(result).free >= 512 * (1 << 30),
                        'insufficient free space for the full export and spool')
        case = result / 'jetstreamer'
        case.mkdir()
        export_path = case / 'transactions.bin'
        command = [str(jet_bin), '--epoch', str(dataset['epoch']),
                   '--http-base', 'https://files.old-faithful.net/',
                   '--index-base', 'https://files.old-faithful.net/', '--workers', '64',
                   '--start-slot', str(dataset['start_slot']),
                   '--end-slot-exclusive', str(dataset['end_slot_exclusive']),
                   '--plan', str(plan_path), '--output', str(case / 'report.json'),
                   '--export', str(export_path), '--reported-error-policy', 'body-transport']
        jet = {'reader': 'jetstreamer', 'workers': 64, 'state': 'RUNNING', 'command': command,
               'host_before': control.host_snapshot()}
        state['cases'].append(jet)
        state['state'] = 'RUNNING'
        phase('JETSTREAMER_READING_AND_EXPORTING')
        jet.update(control.run_process(command, case, 'reader', 7200, progress))
        jet['host_after_reader'] = control.host_snapshot()
        control.require(jet['exit_code'] == 0, 'Jetstreamer failed; inspect its receipt and stderr')
        report = json.loads((case / 'report.json').read_text())
        counters = control.validate_full_receipt(report, rows, 'jetstreamer', 64, 64, True)
        control.require(report['epoch'] == dataset['epoch']
                        and report['start_slot'] == dataset['start_slot']
                        and report['end_slot_exclusive'] == dataset['end_slot_exclusive']
                        and report['http_base'] == report['index_base'] == 'https://files.old-faithful.net/',
                        'Jetstreamer source or slot range differs')
        jet.update(total_seconds=report['total_seconds'], scan_seconds=report['scan_seconds'],
                   total_tps=report['transactions'] / report['total_seconds'],
                   scan_tps=report['transactions'] / report['scan_seconds'],
                   mean_cpu_cores=jet['cpu_seconds'] / jet['process_seconds'],
                   report_sha256=control.digest(case / 'report.json'),
                   reported_error_policy=report['reported_error_policy'],
                   reported_body_retries=report['reported_body_retries'])
        for key in ['upstream_commit', 'upstream_version', 'reported_body_retry_limit_per_worker_slot',
                    'reported_error_events', 'reported_error_events_truncated',
                    'upstream_connection_recycles', 'upstream_operation_timeouts', 'upstream_work_steals']:
            jet[key] = report[key]
        phase('VERIFYING_EVERY_JETSTREAMER_TRANSACTION')
        jet['verification_process'] = control.run_process(
            [str(verifier), '--plan-tsv', str(tsv), '--input', str(export_path)],
            case, 'verify', 3600, progress)
        control.require(jet['verification_process']['exit_code'] == 0, 'Jetstreamer export verification failed')
        checked = json.loads((case / 'verify.stdout.log').read_text())
        control.validate_verified_export(checked, report, counters, rows)
        phase('HASHING_JETSTREAMER_EXPORT')
        sha, size = control.hash_output(export_path, progress, 3600)
        control.require(size == checked['output_bytes'], 'Jetstreamer export length changed')
        jet.update(checked, output_sha256=sha, byte_parity='NOT_COMPARED')
        phase('CHECKING_SOURCE_AFTER_JETSTREAMER')
        jet['source_after'] = control.source_checks('https://files.old-faithful.net/', dataset)
        control.require(jet['source_after'] == before, 'source changed during Jetstreamer')
        control.require(control.digest(jet_bin) == state['binary_sha256']['jetstreamer'], 'Jetstreamer binary changed')
        phase('RECHECKING_RETAINED_CAR_EXPORT_HASH')
        car_export = old / 'car/transactions.bin'
        car_sha, car_size = control.hash_output(car_export, progress, 3600)
        control.require((car_sha, car_size) == (car['output_sha256'], car['output_bytes']),
                        'retained CAR export changed')
        jet['state'] = 'PASS'
        control.validate_comparison_candidates(state['cases'])
        phase('COMPARING_EVERY_EXPORT_BYTE')
        compared = control.compare_outputs(car_export, export_path, progress, 3600)
        control.require(compared == car['output_bytes'], 'comparison length differs')
        car['byte_parity'] = jet['byte_parity'] = 'PASS'
        state['comparison'] = {'state': 'PASS', 'compared_bytes': compared}
        state['summary'] = {row['reader']: {key: row[key] for key in [
            'workers', 'total_seconds', 'total_tps', 'scan_seconds', 'scan_tps', 'cpu_seconds',
            'mean_cpu_cores', 'peak_rss_mib', 'blocks', 'transactions', 'output_bytes',
            'output_sha256', 'votes', 'failed', 'byte_parity']} for row in state['cases']}
        state.update(state='PASS', phase='COMPLETE')
        state.pop('progress', None)
    except BaseException as error:
        state.update(state='FAILED', error=repr(error))
        for row in state['cases']:
            if row['state'] == 'RUNNING':
                row.update(state='FAILED', error=repr(error))
        raise
    finally:
        save()


if __name__ == '__main__':
    main()
