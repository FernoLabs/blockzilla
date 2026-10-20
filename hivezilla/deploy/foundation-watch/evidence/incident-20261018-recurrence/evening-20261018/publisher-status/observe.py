#!/usr/bin/env python3
"""Read publisher status for 120 seconds. Create only a new FLOW-120S.json."""
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import time

sys.dont_write_bytecode = True
ROOT = Path('/home/ubuntu/live/hivezilla-validator-20260930')
REPORT = ROOT / 'reports/native-recovery-20261018/recurrence/evening-20261018/publisher-status'
UNIT = 'hivezilla-bank-v2-r2-publisher.service'
PIN_SHA = 'ddfe097a2915a946c0917adcc135a2c7d997b1517d578044bfea80aadd7ea315'
REGISTRY_SHA = 'a52115479c26ccff6367b8439cd58dbb2354bb7b61d25691e797a96611655f6f'
PATCH_SHA = '92f2376d361a62b4e34f4671e93c1b5f70bcf1b1f95c9c8b653a3c70c94c708e'
PIDS = {'hivezilla-validator': 2142808, 'hivezilla-direct-worker': 916425,
        'hivezilla-bank-v2-producer': 917148, 'hivezilla-shred-hook-recorder': 2142799,
        'hivezilla-bank-v2-origin': 2134416, 'hivezilla-stream-origin': 2135639,
        'hivezilla-stream-relay': 2136117}
IDENTITY_KEYS = ('schema', 'source', 'namespace', 'prefix', 'session', 'publisher_id', 'start_slot')
SOURCE_KEYS = ('genesis_hash', 'node_identity', 'agave_revision', 'session_id')
DURATION = 120
MAX_STATUS_AGE_MS = 15_000


class CheckFailure(Exception):
    pass


def require(ok, code):
    if not ok:
        raise CheckFailure(code)


def integer(value):
    return type(value) is int and 0 <= value <= 2 ** 64 - 1


def now():
    return time.time_ns() // 1_000_000


def sha(body):
    return hashlib.sha256(body).hexdigest()


def read(path, limit=2 ** 20):
    require(stat.S_ISREG(path.lstat().st_mode), 'file_not_regular')
    with path.open('rb') as stream:
        body = stream.read(limit + 1)
    require(len(body) <= limit, 'file_too_large')
    return body


def document(path):
    body = read(path)
    value = json.loads(body)
    require(isinstance(value, dict), 'json_object_required')
    return body, value


def digest(path, executed=False):
    if not executed:
        require(stat.S_ISREG(path.lstat().st_mode), 'binary_not_regular')
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def show(unit):
    process = subprocess.run(['systemctl', 'show', unit, '-p', 'MainPID,ActiveState,SubState'],
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=8, check=False)
    require(process.returncode == 0 and len(process.stdout) <= 4096, 'service_read_failed')
    fields = dict(line.split('=', 1) for line in process.stdout.decode().splitlines() if '=' in line)
    pid = fields.get('MainPID', '')
    require(re.fullmatch('[1-9][0-9]{0,9}', pid) is not None
            and fields.get('ActiveState') == 'active' and fields.get('SubState') == 'running',
            'required_process_not_running')
    return int(pid)


def option(args, name):
    require(isinstance(args, list) and all(isinstance(arg, str) for arg in args)
            and args.count(name) == 1 and args.index(name) + 1 < len(args), 'publisher_command_invalid')
    return args[args.index(name) + 1]


def identity(cp, expected):
    require(isinstance(cp, dict) and all(cp.get(key) == expected[key] for key in IDENTITY_KEYS),
            'publisher_identity_changed')
    require(all(integer(cp.get(key)) for key in ('sequence', 'blocks', 'bytes')),
            'publisher_counters_invalid')
    require(cp['sequence'] > 0 and isinstance(cp.get('cursor'), str)
            and len(cp['cursor']) <= 256
            and re.fullmatch(re.escape(expected['session']) + r':p[0-9]{1,20}:(?:[0-9a-f]{64}|-)', cp['cursor']) is not None,
            'publisher_cursor_invalid')


def counters(cp):
    return {key: cp[key] for key in ('sequence', 'blocks', 'bytes', 'cursor')}


def sample(state_path, expected, previous=None):
    # STATUS first: its checkpoint can precede a subsequent atomic CHECKPOINT read.
    _, status = document(state_path / 'STATUS.json')
    _, cp = document(state_path / 'CHECKPOINT.json')
    identity(cp, expected)
    old = status.get('checkpoint')
    identity(old, expected)
    stamp = status.get('updated_unix_ms')
    sampled = now()
    require(status.get('state') == 'RUNNING' and integer(stamp)
            and 0 <= sampled - stamp <= MAX_STATUS_AGE_MS, 'publisher_status_not_fresh')
    phase, active = status.get('phase'), status.get('in_flight')
    require(type(active) is bool and ((phase == 'upload_commit' and active)
            or (phase == 'waiting' and not active)), 'publisher_phase_invalid')
    started = status.get('batch_started_unix_ms')
    require((active and integer(started) and 0 < started <= stamp)
            or (not active and started is None), 'publisher_batch_start_invalid')
    progress = status.get('last_durable_progress_unix_ms')
    require(integer(progress) and 0 < progress <= stamp, 'publisher_progress_time_invalid')
    require(all(old[key] <= cp[key] for key in ('sequence', 'blocks', 'bytes')),
            'publisher_status_checkpoint_ahead')
    if old['sequence'] == cp['sequence']:
        require(counters(old) == counters(cp), 'publisher_status_checkpoint_conflict')
    if previous is not None:
        require(all(cp[key] >= previous[key] for key in ('sequence', 'blocks', 'bytes')),
                'publisher_counter_regressed')
        if cp['sequence'] == previous['sequence']:
            require(counters(cp) == counters(previous), 'publisher_checkpoint_changed_without_progress')
    return {'sampled_unix_ms': sampled, 'status_age_ms': sampled - stamp,
        'state': status['state'], 'phase': phase, 'in_flight': active,
        'batch_started_unix_ms': started, 'last_durable_progress_unix_ms': progress,
        'status_checkpoint': counters(old), 'durable_checkpoint': counters(cp)}, cp


def load_binding():
    installation_bytes, installed = document(REPORT / 'INSTALLATION.json')
    build_bytes, build = document(REPORT / 'BUILD.json')
    before_bytes, before = document(REPORT / 'BEFORE.json')
    require(installed.get('schema') == 'blockzilla-publisher-status-installation-v1'
            and installed.get('state') == 'ACTIVE' and installed.get('protected_pids') == PIDS
            and installed.get('protected_pids_unchanged') is True
            and installed.get('publisher_id_unchanged') is True, 'installation_not_active')
    require(sha(build_bytes) == installed.get('build_sha256')
            and sha(before_bytes) == installed.get('before_sha256'), 'activation_receipt_changed')
    require(build.get('schema') == 'blockzilla-publisher-status-build-v1'
            and build.get('state') == 'READY_NOT_ACTIVE' and build.get('tests_passed') is True
            and integer(build.get('tests_count')) and build['tests_count'] >= 44
            and build.get('tests_failed') == 0 and build.get('patch_sha256') == PATCH_SHA,
            'build_not_verified')
    require(installed.get('registry_sha256') == REGISTRY_SHA
            and installed.get('binary_sha256') == build.get('binary_sha256')
            and installed.get('release') == build.get('release'), 'installation_build_mismatch')
    source = installed.get('source')
    require(isinstance(source, dict) and set(source) == set(SOURCE_KEYS)
            and all(isinstance(source[key], str) for key in SOURCE_KEYS), 'source_invalid')
    args = before.get('new_command')
    binary = Path(build['binary'])
    require(binary == Path(build['release']) / 'hivezilla-stream'
            and binary.parent.parent == ROOT / 'releases' and binary.resolve(strict=True) == binary
            and isinstance(args, list) and args[0] == str(binary)
            and before.get('build_sha256') == sha(build_bytes)
            and before.get('source') == source, 'publisher_release_binding_invalid')
    require(not binary.stat().st_mode & 0o222 and digest(binary) == build['binary_sha256'],
            'publisher_release_binary_changed')
    require(digest(binary.parent / 'MANIFEST.json') == build.get('release_manifest_sha256')
            and digest(binary.parent / 'SOURCE.json') == build.get('source_manifest_sha256'),
            'publisher_release_provenance_changed')
    pid = installed.get('new_pid')
    require(type(pid) is int and pid > 0 and pid != installed.get('old_pid')
            and installed.get('proof', {}).get('pid') == pid, 'publisher_pid_binding_invalid')
    expected = before.get('checkpoint_identity')
    require(isinstance(expected, dict) and set(expected) == set(IDENTITY_KEYS)
            and expected['source'] == str(ROOT / 'compact/direct-agave-processed-live' / source['session_id'])
            and expected['session'] == source['session_id']
            and expected['prefix'] == option(args, '--prefix') + '/' + source['session_id']
            and '--processed' in args, 'checkpoint_binding_invalid')
    for checkpoint in (installed.get('before_checkpoint'), installed.get('proof', {}).get('checkpoint'),
                       installed.get('proof', {}).get('post_restart_baseline')):
        require(isinstance(checkpoint, dict) and all(checkpoint.get(key) == expected[key]
                for key in ('schema', 'namespace', 'session', 'publisher_id', 'start_slot')),
                'installation_publisher_identity_changed')
    state_path = Path(option(args, '--state'))
    require(state_path == ROOT / 'reports/native-processed-r2-bank-v2' / source['session_id']
            and option(args, '--output') == expected['source'], 'publisher_source_path_changed')
    return {'installation_bytes': installation_bytes, 'build_bytes': build_bytes, 'before_bytes': before_bytes,
        'source': source, 'args': args, 'binary': binary, 'binary_sha256': build['binary_sha256'],
        'pid': pid, 'identity': expected, 'state_path': state_path,
        'installation_sha256': sha(installation_bytes), 'build_sha256': sha(build_bytes)}


def authority(binding, check_binary):
    require(read(REPORT / 'INSTALLATION.json') == binding['installation_bytes']
            and read(REPORT / 'BUILD.json') == binding['build_bytes']
            and read(REPORT / 'BEFORE.json') == binding['before_bytes'], 'activation_receipt_changed')
    pin_bytes, source = document(ROOT / 'config/native-active-source.json')
    require(sha(pin_bytes) == PIN_SHA and source == binding['source'], 'source_pin_changed')
    require(sha(read(ROOT / 'config/storage-jobs.json')) == REGISTRY_SHA, 'registry_changed')
    actual = {unit: show(unit) for unit in PIDS}
    require(actual == PIDS, 'protected_pid_changed')
    require(show(UNIT) == binding['pid'], 'publisher_pid_changed')
    args = (Path('/proc') / str(binding['pid']) / 'cmdline').read_bytes().decode().rstrip('\0').split('\0')
    require(args == binding['args'], 'publisher_argv_changed')
    if check_binary:
        require(digest(binding['binary']) == binding['binary_sha256']
                and digest(Path('/proc') / str(binding['pid']) / 'exe', executed=True) == binding['binary_sha256'],
                'publisher_executed_binary_changed')
    return {'protected_pids': actual, 'publisher_pid': binding['pid']}


def write_report(record):
    path = REPORT / 'FLOW-120S.json'
    body = (json.dumps(record, indent=2, sort_keys=True) + '\n').encode()
    # Do not overwrite any earlier proof. This is the script's only file write.
    with path.open('xb') as stream:
        os.fchmod(stream.fileno(), 0o600)
        stream.write(body)
        stream.flush()
        os.fsync(stream.fileno())
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return sha(body)


def main():
    require(not (REPORT / 'FLOW-120S.json').exists(), 'flow_report_already_exists')
    record = {'schema': 'blockzilla-publisher-status-flow-v1', 'state': 'FAILED',
        'scope': 'local_process_status_and_durable_checkpoint_progress_only',
        'cloud_verification': False, 'duration_target_seconds': DURATION,
        'sample_interval_seconds': 1, 'maximum_status_age_ms': MAX_STATUS_AGE_MS,
        'started_unix_ms': now(), 'samples': [], 'errors': []}
    clock = time.monotonic()
    try:
        binding = load_binding()
        record.update(source=binding['source'], installation_sha256=binding['installation_sha256'],
            build_sha256=binding['build_sha256'], binary_sha256=binding['binary_sha256'],
            registry_sha256=REGISTRY_SHA, publisher_identity=binding['identity'])
        record['before_processes'] = authority(binding, True)
        started = time.monotonic()
        next_sample, next_authority = started, started + 10
        previous, start_sequence = None, None
        while True:
            if time.monotonic() >= next_authority:
                authority(binding, False)
                next_authority = time.monotonic() + 10
            item, previous = sample(binding['state_path'], binding['identity'], previous)
            elapsed = time.monotonic() - started
            item['elapsed_seconds'] = round(elapsed, 3)
            record['samples'].append(item)
            if start_sequence is None:
                start_sequence = previous['sequence']
            if elapsed >= DURATION:
                break
            next_sample += 1
            time.sleep(max(0, min(next_sample, started + DURATION) - time.monotonic()))
        record['after_processes'] = authority(binding, True)
        record.update(start_sequence=start_sequence, end_sequence=previous['sequence'],
            sequence_advance=previous['sequence'] - start_sequence,
            max_status_age_ms=max(item['status_age_ms'] for item in record['samples']),
            in_flight_samples=sum(item['in_flight'] for item in record['samples']))
        require(record['sequence_advance'] > 0, 'publisher_checkpoint_not_advanced')
        record['state'] = 'PASSED'
    except Exception as error:
        record['errors'] = [str(error) if isinstance(error, CheckFailure) else 'observer_read_failed']
    if record['samples']:
        first, last = record['samples'][0], record['samples'][-1]
        record.update(start_sequence=first['durable_checkpoint']['sequence'],
            end_sequence=last['durable_checkpoint']['sequence'],
            sequence_advance=last['durable_checkpoint']['sequence'] - first['durable_checkpoint']['sequence'],
            max_status_age_ms=max(item['status_age_ms'] for item in record['samples']),
            in_flight_samples=sum(item['in_flight'] for item in record['samples']),
            observation_seconds=last['elapsed_seconds'])
    record.update(completed_unix_ms=now(), elapsed_seconds=round(time.monotonic() - clock, 3),
        sample_count=len(record['samples']))
    checksum = write_report(record)
    print(json.dumps({key: record.get(key) for key in ('state', 'sample_count', 'elapsed_seconds',
        'start_sequence', 'end_sequence', 'sequence_advance', 'max_status_age_ms', 'in_flight_samples', 'errors')} |
        {'report': str(REPORT / 'FLOW-120S.json'), 'sha256': checksum}))
    raise SystemExit(0 if record['state'] == 'PASSED' else 1)


if __name__ == '__main__':
    main()
