#!/usr/bin/env python3
"""Linux-only, scan-gated diagnostics for the full CAR locked in RAM.

No HTTP, export, full-CAR rehash, or full output verification is performed.
Perf control protocol: https://man7.org/linux/man-pages/man1/perf-record.1.html
and https://man7.org/linux/man-pages/man1/perf-stat.1.html (--control, --delay).
"""
import argparse
import json
import math
import os
from pathlib import Path
import platform
import re
import select
import shutil
import signal
import stat
import subprocess
import sys
import time

import run_dynamic as dynamic
import run_full_epoch as full
from run_manycore import digest, require


# The earlier independently verified full epoch-1000 common export established
# these counters. They are receipt checks here, not output-content verification.
REFERENCE_COUNTERS = {1000: {'votes': 302828458, 'failed': 66856874}}
STAT_FIELDS = ('st_dev', 'st_ino', 'st_size', 'st_mtime_ns', 'st_ctime_ns')
WRAPPER = 'set -eu; umask 022; printf "%s\\n" "$$" > "$1"; shift; exec "$@"'
PIDFD_SIGNAL = '''import os,signal,sys
pid,start,sig=map(int,sys.argv[1:])
try:
 fd=os.pidfd_open(pid)
except ProcessLookupError:
 sys.exit(0)
try:
 with open('/proc/%d/stat'%pid) as f: fields=f.read().rsplit(')',1)[1].split()
 if int(fields[19])!=start: raise RuntimeError('PID start time changed; refusing signal')
 signal.pidfd_send_signal(fd,sig)
except (FileNotFoundError,ProcessLookupError):
 pass
finally:
 os.close(fd)
'''
# Linux v7.0 tools/perf/util/evlist.c writes sizeof(EVLIST_CTL_CMD_ACK_TAG),
# including the C string terminator: three letters, newline and NUL (five bytes).
# https://raw.githubusercontent.com/torvalds/linux/v7.0/tools/perf/util/evlist.c
PERF_ACK = b'ack\n\0'


def selected_cases(value):
    require(isinstance(value, list) and 1 <= len(value) <= 16, 'cases must contain 1..16 diagnostic attempts')
    cases, names = [], set()
    for raw in value:
        require(isinstance(raw, dict) and set(raw) <= {'name', 'parsers', 'workers', 'stage_sample_every', 'profile'},
                'unknown diagnostic case field')
        case = dict(raw)
        name = case.get('name')
        require(isinstance(name, str) and re.fullmatch('[a-z0-9][a-z0-9-]{0,63}', name) and name not in names,
                'case names must be distinct safe directory names')
        for key, limit in [('parsers', 128), ('workers', 256)]:
            require(type(case.get(key)) is int and 1 <= case[key] <= limit, f'{key} outside 1..{limit}')
        case.setdefault('stage_sample_every', 0)
        case.setdefault('profile', 'none')
        require(type(case['stage_sample_every']) is int and 0 <= case['stage_sample_every'] < 1 << 64,
                'invalid stage sampling interval')
        require(case['profile'] in ('none', 'stat', 'record'), 'profile must be none, stat or record')
        names.add(name)
        cases.append(case)
    return cases


def file_stat(path):
    value = path.lstat()
    require(stat.S_ISREG(value.st_mode) and not value.st_mode & 0o222, f'{path} must be a readonly regular file')
    return {key: getattr(value, key) for key in STAT_FIELDS}


def source_check(manifest_path, car, index, plan_path):
    manifest = json.loads(manifest_path.read_text())
    require(manifest.get('schema') == 'zilla-local-car-cache-v1' and manifest.get('state') == 'COMPLETE',
            'cache manifest is not complete')
    epoch = manifest.get('epoch')
    require(type(epoch) is int and epoch in REFERENCE_COUNTERS, 'no verified vote/failure reference for this epoch')
    dataset_path = manifest_path.with_name('dataset-manifest.json')
    require(digest(dataset_path) == manifest.get('dataset_manifest_sha256'), 'dataset manifest hash differs')
    dataset = full.validate_dataset(json.loads(dataset_path.read_text()))
    require(dataset['epoch'] == epoch, 'dataset epoch differs')
    plan = json.loads(plan_path.read_text())
    rows = full.validate_full_plan(plan, dataset)
    checked = {}
    for path, name, size in [(car, f'epoch-{epoch}.car', dataset['car_bytes']),
                             (index, f'epoch-{epoch}-slot-ranges.raw', dataset['index_bytes'])]:
        require(path.name == name, 'unexpected cache filename')
        record = manifest.get('files', {}).get(name, {})
        identity = file_stat(path)
        require(identity == record.get('stat') and record.get('bytes') == size == identity['st_size'],
                f'{name} stat identity differs from the completed manifest')
        require(isinstance(record.get('sha256'), str) and re.fullmatch('[0-9a-f]{64}', record['sha256']),
                'missing cache file content identity')
        checked[name] = {'stat': identity, 'sha256_from_verified_manifest': record['sha256']}
    index_before = file_stat(index)
    index_hash = digest(index)
    require(file_stat(index) == index_before and index_hash == dataset['index_sha256']
            == manifest['files'][index.name]['sha256'], 'raw index hash or identity differs')
    return {'manifest_sha256': digest(manifest_path), 'dataset_manifest_sha256': digest(dataset_path),
            'plan_sha256': digest(plan_path), 'files': checked, 'raw_index_sha256': index_hash}, dataset, rows


def reader_command(args, case, directory):
    command = [str(args.bin), '--file', str(args.car), '--resident-cache-manifest', str(args.manifest),
               '--raw-index', str(args.index), '--plan', str(args.plan), '--scheduling', 'dynamic',
               '--parser-workers', str(case['parsers']), '--workers', str(case['workers']),
               '--http-clients', '1', '--http-chunk-mib', '128', '--job-mib', '256',
               '--output', str(directory / 'report.json'), '--profile-control', str(directory / 'gate')]
    if case['stage_sample_every']:
        command += ['--stage-sample-every', str(case['stage_sample_every'])]
    return [shutil.which('prlimit') or '/usr/bin/prlimit', '--memlock=unlimited:unlimited', '--'] + command


def perf_command(kind, pid, directory, perf_path):
    common = [str(perf_path), kind, '--pid', str(pid), '--delay=-1',
              f'--control=fifo:{directory / "perf-control.fifo"},{directory / "perf-ack.fifo"}']
    if kind == 'record':
        return common + ['-e', 'cpu-clock', '-F', '99', '--call-graph', 'fp', '-o', str(directory / 'perf.data')]
    require(kind == 'stat', 'invalid profiler command')
    return common + ['-x', ';', '-e', 'task-clock,context-switches,cpu-migrations,page-faults,major-faults,{cycles,instructions}',
                     '-o', str(directory / 'perf-stat.csv')]


def process_snapshot(pid):
    fields = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
    require(len(fields) >= 22, 'truncated process stat')
    values = {}
    for line in Path(f'/proc/{pid}/status').read_text().splitlines():
        key, _, value = line.partition(':')
        if key in ('VmRSS', 'VmHWM', 'VmLck', 'Threads'):
            values[key] = int(value.split()[0]) * (1024 if key.startswith('Vm') else 1)
    ticks = os.sysconf('SC_CLK_TCK')
    return {'pid': pid, 'state': fields[0], 'parent_pid': int(fields[1]), 'starttime_ticks': int(fields[19]),
            'user_cpu_seconds': int(fields[11]) / ticks, 'system_cpu_seconds': int(fields[12]) / ticks,
            'minor_faults': int(fields[7]), 'major_faults': int(fields[9]),
            'rss_bytes': values.get('VmRSS', 0), 'peak_rss_bytes': values.get('VmHWM', 0),
            'locked_bytes': values.get('VmLck', 0), 'threads': values.get('Threads', 0), 'unix_seconds': time.time()}


def is_descendant(pid, ancestor):
    seen = set()
    while pid > 1 and pid not in seen:
        if pid == ancestor:
            return True
        seen.add(pid)
        try:
            pid = process_snapshot(pid)['parent_pid']
        except (OSError, ValueError):
            return False
    return False


def privileged_run(command, **kwargs):
    return subprocess.run(['sudo', '-n', '--'] + command, check=True, timeout=15, **kwargs)


def memory_preflight(case, source):
    lines = [line.split() for line in Path('/proc/meminfo').read_text().splitlines()
             if line.startswith('MemAvailable:')]
    require(len(lines) == 1 and len(lines[0]) == 3 and lines[0][2] == 'kB'
            and lines[0][1].isdigit(), 'missing or invalid MemAvailable')
    available = int(lines[0][1]) * 1024
    locked = sum(record['stat']['st_size'] for record in source['files'].values())
    buffers = (case['parsers'] * (128 << 20)
               + (case['workers'] + 2 * case['parsers']) * (16 << 20))
    spare = 32 << 30
    return {'available_bytes': available, 'locked_file_bytes': locked, 'active_buffer_allowance_bytes': buffers,
            'spare_bytes': spare, 'required_bytes': locked + buffers + spare,
            'sufficient': available >= locked + buffers + spare,
            'limit': 'conservative preflight estimate, not a memory reservation or total RSS bound'}


class OwnedProcess:
    """A fixed exec wrapper records the exact child PID before preload begins."""
    def __init__(self, command, directory, name):
        self.pid_file = directory / (name + '.pid')
        self.identity = None
        self.command = ['sudo', '-n', '--', '/bin/sh', '-c', WRAPPER, 'resident-profile-child',
                        str(self.pid_file)] + command
        self.stdout = (directory / (name + '.stdout.log')).open('xb')
        self.stderr = (directory / (name + '.stderr.log')).open('xb')
        self.process = subprocess.Popen(self.command, stdin=subprocess.DEVNULL, stdout=self.stdout,
                                        stderr=self.stderr, start_new_session=True)

    def capture(self):
        if self.identity is None and self.pid_file.exists():
            value = self.pid_file.read_text().strip()
            if value:
                require(value.isdecimal() and int(value) > 1, 'invalid owned child PID')
                pid = int(value)
                require(is_descendant(pid, self.process.pid), 'recorded PID is not our launched child')
                self.identity = process_snapshot(pid)
        return self.identity

    def snapshot(self):
        identity = self.capture()
        require(identity is not None, 'child PID not yet recorded')
        current = process_snapshot(identity['pid'])
        require(current['starttime_ticks'] == identity['starttime_ticks'], 'owned child PID was reused')
        return current

    def validate_executable(self, expected=None):
        current = self.snapshot()
        result = privileged_run(['/usr/bin/readlink', '--', f'/proc/{current["pid"]}/exe'],
                                capture_output=True, text=True)
        executable = result.stdout.strip()
        require(executable == str(expected) if expected is not None else Path(executable).name == 'perf',
                'owned process executable differs')
        return executable

    def signal_child(self, number):
        identity = self.capture()
        require(identity is not None, 'cannot signal a process without its recorded identity')
        privileged_run([sys.executable, '-c', PIDFD_SIGNAL, str(identity['pid']),
                        str(identity['starttime_ticks']), str(number)], capture_output=True, text=True)

    def stop(self, first_signal=signal.SIGTERM):
        if self.process.poll() is not None:
            return self.process.returncode
        deadline = time.monotonic() + 5
        while self.capture() is None and self.process.poll() is None and time.monotonic() < deadline:
            time.sleep(0.02)
        if self.process.poll() is not None:
            return self.process.returncode
        self.signal_child(first_signal)
        try:
            return self.process.wait(timeout=20)
        except subprocess.TimeoutExpired:
            self.signal_child(signal.SIGKILL)
            return self.process.wait(timeout=10)

    def close(self):
        self.stdout.close()
        self.stderr.close()


class PerfControl:
    def __init__(self, directory):
        self.control = directory / 'perf-control.fifo'
        self.ack = directory / 'perf-ack.fifo'
        os.mkfifo(self.control, 0o600)
        os.mkfifo(self.ack, 0o600)
        # RDWR avoids FIFO-open deadlocks while perf opens the opposite ends.
        self.write_fd = os.open(self.control, os.O_RDWR | os.O_NONBLOCK)
        self.read_fd = os.open(self.ack, os.O_RDWR | os.O_NONBLOCK)
        self.events = []

    def command(self, command, process, timeout=30):
        require(command in ('enable', 'disable'), 'invalid perf control command')
        require(not select.select([self.read_fd], [], [], 0)[0], 'unexpected stale perf acknowledgement')
        event = {'command': command, 'sent_unix_seconds': time.time()}
        self.events.append(event)
        os.write(self.write_fd, (command + '\n').encode())
        deadline, data = time.monotonic() + timeout, b''
        while time.monotonic() < deadline:
            require(process.process.poll() is None, 'perf exited before control acknowledgement')
            if select.select([self.read_fd], [], [], min(0.1, max(0, deadline - time.monotonic())))[0]:
                data += os.read(self.read_fd, 64)
                event['received_hex'] = data.hex()
                require(PERF_ACK.startswith(data), f'unexpected perf acknowledgement: {data!r}')
                if data == PERF_ACK:
                    event['ack_unix_seconds'] = time.time()
                    return
        raise TimeoutError(f'perf did not acknowledge {command}; received {data!r}')

    def close(self):
        os.close(self.write_fd)
        os.close(self.read_fd)


def wait_json(path, child, deadline, sample=None):
    next_sample = 0
    while time.monotonic() < deadline:
        child.capture()
        if path.exists():
            return json.loads(path.read_text())
        require(child.process.poll() is None, f'reader exited before {path.name}')
        if sample is not None and child.identity is not None and time.monotonic() >= next_sample:
            sample(child.snapshot())
            next_sample = time.monotonic() + 1
        time.sleep(0.02)
    raise TimeoutError(f'timed out waiting for {path.name}')


def validate_receipt(receipt, case, rows, dataset, source):
    require(receipt.get('valid') is True and receipt.get('block_transaction_rows') == rows,
            'resident reader does not match the complete canonical plan')
    require(receipt.get('transactions') == dataset['transactions'] and receipt.get('blocks') == len(rows)
            and receipt.get('epoch') == dataset['epoch'], 'resident reader counts differ')
    for key, expected in REFERENCE_COUNTERS[dataset['epoch']].items():
        require(receipt.get(key) == expected, f'{key} differs from the verified reference')
    require(receipt.get('metadata_mode') == 'Owned' and receipt.get('signature_verification') is False
            and receipt.get('scheduling') == 'dynamic' and receipt.get('workers') == case['workers']
            and receipt.get('parser_workers') == case['parsers'] and receipt.get('http_clients') == 1,
            'resident decode settings differ')
    require(receipt.get('export_schema') is None and receipt.get('export_bytes') is None
            and receipt.get('export_storage') is None and receipt.get('http') is None
            and receipt.get('effective_http_workers') == receipt.get('http_body_window_bytes') == 0
            and receipt.get('http_transport_mode') == 'resident-range', 'resident run used HTTP or export')
    require(receipt.get('http_chunk_bytes') == 128 << 20 and receipt.get('job_target_bytes') == 256 << 20,
            'resident input chunk/job settings differ')
    seconds = receipt.get('scan_seconds')
    require(type(seconds) in (float, int) and math.isfinite(seconds) and seconds > 0, 'invalid scan duration')
    usage = receipt.get('scan_resources', {})
    require(type(usage.get('major_faults')) is int and usage['major_faults'] == 0,
            'resident scan had a major page fault')
    for key in ('cpu_seconds', 'user_cpu_seconds', 'system_cpu_seconds', 'mean_cpu_cores'):
        require(type(usage.get(key)) in (float, int) and math.isfinite(usage[key]) and usage[key] >= 0,
                'invalid scan CPU accounting')
    resident = receipt.get('resident_input', {})
    identity = receipt.get('identity', {})
    require(identity.get('transport') == 'mlocked-local-mmap' and resident.get('source', {}).get('http_requests') == 0,
            'resident source transport differs')
    for label, name in [('car', f'epoch-{dataset["epoch"]}.car'), ('index', f'epoch-{dataset["epoch"]}-slot-ranges.raw')]:
        expected = source['files'][name]['stat']
        actual = identity.get(label, {})
        require(actual.get('stat') == expected and actual.get('mlock_succeeded') is True,
                'resident mapped identity or lock differs')
        for phase in ('before_scan', 'after_scan'):
            r = resident.get(phase, {}).get(label, {})
            require(r.get('all_pages_resident') is True and r.get('mlock_succeeded') is True
                    and type(r.get('total_pages')) is int and r['total_pages'] > 0
                    and r.get('resident_pages') == r['total_pages'] and r.get('mapped_bytes') == expected['st_size']
                    and type(r.get('page_bytes')) is int and r['page_bytes'] > 0
                    and r['total_pages'] == (expected['st_size'] + r['page_bytes'] - 1) // r['page_bytes'],
                    'resident source is not fully resident before and after scan')
    p = receipt.get('pipeline', {})
    jobs, blocks = receipt.get('job_count'), receipt.get('physical_blocks')
    require(type(jobs) is int and type(blocks) is int and 1 <= jobs <= blocks <= len(rows)
            and p.get('job_count') == p.get('jobs_claimed') == p.get('jobs_completed') == jobs
            and p.get('blocks_parsed') == p.get('blocks_decoded') == blocks
            and p.get('parser_workers_finished') == case['parsers']
            and p.get('decode_workers_finished') == case['workers']
            and p.get('raw_block_pool_size') == p.get('work_queue_capacity') == case['workers'] + 2 * case['parsers'],
            'resident pipeline did not finish its job/block plan')
    stages = receipt.get('decode_stages')
    if case['stage_sample_every']:
        require(isinstance(stages, dict) and stages.get('sample_every') == case['stage_sample_every']
                and stages.get('transactions_seen') == dataset['transactions'], 'stage metrics differ or are incomplete')
    else:
        require(stages is None, 'unexpected stage instrumentation')


def run_case(args, case, directory, source, dataset, rows, save):
    directory.mkdir()
    (directory / 'gate').mkdir(mode=0o755)
    row = {'name': case['name'], 'settings': case, 'state': 'RUNNING', 'directory': directory.name,
           'profiled': case['profile'] != 'none', 'stage_instrumented': bool(case['stage_sample_every']),
           'output_verification': 'NOT_APPLICABLE_NO_EXPORT', 'samples': [], 'cleanup_errors': []}
    reader = profiler = control = None
    began = time.monotonic()
    def sample(value):
        row['samples'].append(value)
        row['peak_threads_sampled'] = max(row.get('peak_threads_sampled', 0), value['threads'])
        row['peak_rss_bytes_sampled'] = max(row.get('peak_rss_bytes_sampled', 0), value['rss_bytes'])
        save(row)
    try:
        row['memory_preflight'] = memory_preflight(case, source)
        require(row['memory_preflight']['sufficient'], 'insufficient available RAM for locked source, active buffers and32GiB spare')
        row['host_before_setup'] = dynamic.host_counters()
        reader = OwnedProcess(reader_command(args, case, directory), directory, 'reader')
        row['reader_command'] = reader.command
        save(row)
        ready = wait_json(directory / 'gate/ready.json', reader,
                          min(began + args.timeout_seconds, began + args.setup_timeout_seconds), sample)
        require(reader.identity is not None and ready.get('pid') == reader.identity['pid'], 'reader gate PID differs')
        row['reader_executable'] = reader.validate_executable(args.bin)
        row['reader_ready'] = reader.snapshot()
        if case['profile'] != 'none':
            control = PerfControl(directory)
            profiler = OwnedProcess(perf_command(case['profile'], ready['pid'], directory, args.perf), directory, 'perf')
            row['perf_command'] = profiler.command
            deadline = min(time.monotonic() + 30, began + args.timeout_seconds)
            while profiler.capture() is None:
                require(profiler.process.poll() is None and time.monotonic() < deadline, 'perf failed to start')
                time.sleep(0.02)
            control.command('enable', profiler)
            row['perf_executable'] = profiler.validate_executable()
            row['perf_identity'] = profiler.identity
            row['perf_control'] = control.events
        row['host_before_scan'] = dynamic.host_counters()
        row['scan_start_unix_seconds'] = time.time()
        (directory / 'gate/start').touch(exist_ok=False)
        save(row)
        done = wait_json(directory / 'gate/scan-done.json', reader, began + args.timeout_seconds, sample)
        row['scan_done_unix_seconds'] = time.time()
        row['gate_done'] = done
        row['reader_after_scan'] = reader.snapshot()
        row['host_after_scan'] = dynamic.host_counters()
        row['scan_host_counter_deltas'] = dynamic.counter_deltas(row['host_before_scan'], row['host_after_scan'])
        if profiler is not None:
            control.command('disable', profiler)
            row['perf_control'] = control.events
            row['perf_control_window_seconds'] = control.events[-1]['ack_unix_seconds'] - control.events[0]['ack_unix_seconds']
            row['perf_exit_code'] = profiler.stop(signal.SIGINT)
            require(row['perf_exit_code'] in (0, -signal.SIGINT, 128 + signal.SIGINT), 'perf failed; inspect its logs')
            profile_path = directory / ('perf.data' if case['profile'] == 'record' else 'perf-stat.csv')
            require(profile_path.is_file() and profile_path.stat().st_size > 0, 'perf produced no profile artifact')
        (directory / 'gate/release').touch(exist_ok=False)
        row['reader_exit_code'] = reader.process.wait(timeout=max(1, began + args.timeout_seconds - time.monotonic()))
        row['process_seconds'] = time.monotonic() - began
        require(row['reader_exit_code'] == 0 and done.get('scan_returned_ok') is True
                and done.get('pid') == ready['pid'], 'reader scan or process failed')
        receipt = json.loads((directory / 'report.json').read_text())
        validate_receipt(receipt, case, rows, dataset, source)
        require(receipt['scan_seconds'] == done.get('scan_seconds'), 'gate duration differs from receipt')
        row['report_sha256'] = digest(directory / 'report.json')
        row['receipt'] = {k: v for k, v in receipt.items() if k not in ('block_transaction_rows', 'block_decode_digests')}
        row['scan_seconds'] = receipt['scan_seconds']
        row['scan_tps'] = dataset['transactions'] / receipt['scan_seconds']
        row['state'] = 'MEASURED'
    except (InterruptedError, KeyboardInterrupt):
        row.update(state='FAILED', error='diagnostic interrupted')
        raise
    except Exception as error:
        row.update(state='FAILED', error=repr(error))
    finally:
        for child, name in [(profiler, 'perf'), (reader, 'reader')]:
            if child is not None:
                try:
                    row[name + '_final_exit_code'] = child.stop(signal.SIGINT if name == 'perf' else signal.SIGTERM)
                except Exception as error:
                    row['cleanup_errors'].append({'process': name, 'error': repr(error)})
                    row['state'] = 'FAILED'
                child.close()
        if control is not None:
            row['perf_control'] = control.events
            control.close()
        row['process_seconds'] = time.monotonic() - began
        row['host_after_process'] = dynamic.host_counters()
        for name in ('perf.data', 'perf-stat.csv', 'perf.stdout.log', 'perf.stderr.log', 'reader.stdout.log', 'reader.stderr.log'):
            path = directory / name
            if path.exists():
                row.setdefault('artifacts', {})[name] = {'bytes': path.stat().st_size}
        save(row)
    return row


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ('bin', 'manifest', 'plan', 'car', 'index', 'results', 'cases'):
        p.add_argument('--' + name, type=Path, required=True)
    p.add_argument('--perf', type=Path, default=Path(shutil.which('perf') or '/usr/bin/perf'))
    p.add_argument('--setup-timeout-seconds', type=float, default=600)
    p.add_argument('--timeout-seconds', type=float, default=1800)
    args = p.parse_args()
    require(platform.system() == 'Linux', 'resident profiling runner requires Linux')
    require(0 < args.setup_timeout_seconds <= args.timeout_seconds <= 86400, 'invalid setup/full timeout')
    for name in ('bin', 'manifest', 'plan', 'car', 'index', 'cases', 'perf'):
        setattr(args, name, getattr(args, name).absolute())
    args.bin = args.bin.resolve(strict=True)
    args.results = args.results.absolute()
    args.results.mkdir()
    def interrupted(_number, _frame):
        raise InterruptedError('resident diagnostic interrupted')
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    state = {'schema': 'zilla-resident-car-diagnostic-v1', 'state': 'RUNNING', 'started_at': time.time(),
             'cases': [], 'acceptance': 'UNVERIFIED', 'workload': 'full-owned-decode-hashes-no-export',
             'measurement_scope': 'scan timer excludes preload/mlock and control gates; includes memory copies, CAR parsing, full decode, hashes and worker startup/join',
             'profiling_limit': 'perf and stage instrumentation change the workload cost; do not rank profiled samples against baseline timings',
             'verification_limit': 'No export, output SHA, full output verifier or byte comparison; exact plan/count/source and residency checks remain mandatory'}
    def save(row=None):
        if row is not None:
            current = next((i for i, old in enumerate(state['cases']) if old['name'] == row['name']), None)
            if current is None: state['cases'].append(row)
            else: state['cases'][current] = row
        state['updated_at'] = time.time()
        temporary = args.results / 'status.tmp'
        temporary.write_text(json.dumps(state, indent=2) + '\n')
        temporary.replace(args.results / 'status.json')
    try:
        save()
        cases = selected_cases(json.loads(args.cases.read_text()))
        state['selected_cases'] = cases
        state['source_before'], dataset, rows = source_check(args.manifest, args.car, args.index, args.plan)
        state['binary_sha256'] = digest(args.bin)
        state['control_sha256'] = {name: digest(Path(__file__).with_name(name)) for name in
                                   ('run_resident_profile.py', 'run_dynamic.py', 'run_full_epoch.py', 'run_manycore.py')}
        source_root = Path(__file__).resolve().parents[2]
        state['source_sha256'] = {str(path.relative_to(source_root)): digest(path) for path in
            [source_root / 'bench/reader-profile/src/bin/car-decode-reference.rs',
             *sorted((source_root / 'bench/reader-profile/src/bin/car-decode-reference').glob('*.rs'))] if path.is_file()}
        state['host_before'] = full.host_snapshot()
        state['versions'] = {'python': sys.version, 'kernel': platform.release()}
        if any(c['profile'] != 'none' for c in cases):
            state['versions']['perf'] = subprocess.run([str(args.perf), '--version'], check=True,
                capture_output=True, text=True, timeout=15).stdout.strip()
        shutil.copyfile(args.cases, args.results / 'selected-cases.json')
        for case in cases:
            require(digest(args.bin) == state['binary_sha256'], 'reader executable changed')
            checked, _, _ = source_check(args.manifest, args.car, args.index, args.plan)
            require(checked == state['source_before'], 'source or plan changed before case')
            run_case(args, case, args.results / case['name'], state['source_before'], dataset, rows, save)
            checked, _, _ = source_check(args.manifest, args.car, args.index, args.plan)
            require(checked == state['source_before'], 'source or plan changed after case')
        state['source_after'], _, _ = source_check(args.manifest, args.car, args.index, args.plan)
        require(state['source_after'] == state['source_before'] and digest(args.bin) == state['binary_sha256'],
                'source or executable changed during diagnostic')
        require(all(row['state'] == 'MEASURED' for row in state['cases']) and len(state['cases']) == len(cases),
                'every diagnostic attempt must complete; failed attempts remain recorded')
        state['state'] = 'MEASURED_UNVERIFIED'
    except BaseException as error:
        state.update(state='FAILED', error=repr(error), acceptance='FAILED')
        raise
    finally:
        state['finished_at'] = time.time()
        save()


if __name__ == '__main__':
    main()
