#!/usr/bin/env python3
"""Install only the approved 18ddd9a6 storage allowance. No output rollback."""
import argparse
import base64
import copy
import fcntl
import hashlib
import importlib.util
import json
import os
import re
import shlex
import shutil
import sqlite3
import stat
import subprocess
import time
from contextlib import ExitStack, closing
from pathlib import Path

ROOT = Path('/home/ubuntu/live/hivezilla-validator-20260930')
UNIT = 'hivezilla-native-cold-storage-20261016-source-18ddd9a6.service'
LIVE = 'hivezilla-native-cold-storage-20261012-live.service'
OLD = 'hivezilla-native-cold-storage-20261016-source-18de229d.service'
GUARDIAN = 'hivezilla-native-storage-jobs.service'
BUNDLE_SHA256 = '6b375520aa0bda4f830f5690eb04541f7406b468914758f9a59198ac6914ef7b'
PLAN_SHA256 = 'de5dde9c63f48ef17bcfedc8301764614608d7a6881d9c22a5934ec3f067ee3b'
APPROVAL = 'Approve this bounded cleanup'
REPORT = ROOT / 'reports/native-recovery-20261018/recurrence/approved-extra-18ddd9a6'
REGISTRY = ROOT / 'config/storage-jobs.json'
PIN = ROOT / 'config/native-active-source.json'
PROTECTED = [
    'hivezilla-validator.service', 'hivezilla-direct-worker.service',
    'hivezilla-bank-v2-producer.service', 'hivezilla-bank-v2-origin.service',
    'hivezilla-stream-origin.service', 'hivezilla-stream-relay.service',
    'hivezilla-bank-v2-r2-publisher.service', 'hivezilla-shred-hook-recorder.service',
    LIVE, 'hivezilla-native-cold-storage-20261012-old.service',
]


def require(condition, code):
    if not condition:
        raise RuntimeError(code)


def sha(body):
    return hashlib.sha256(body).hexdigest()


def read(path, maximum=64 * 1024 * 1024):
    path = Path(path)
    require(stat.S_ISREG(path.lstat().st_mode), 'non_regular_input')
    with path.open('rb') as stream:
        body = stream.read(maximum + 1)
    require(len(body) <= maximum, 'input_too_large')
    return body


def atomic(path, body):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + '.new')
    with temp.open('wb') as stream:
        stream.write(body)
        stream.flush()
        os.fsync(stream.fileno())
    temp.chmod(0o600)
    os.replace(temp, path)
    fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def save(path, value):
    atomic(path, (json.dumps(value, indent=2) + '\n').encode())


def run(*args, timeout=30):
    return subprocess.check_output(args, text=True, timeout=timeout).strip()


def show(unit):
    value = run('systemctl', 'show', unit, '-p',
                'MainPID,ActiveState,SubState,Restart,InvocationID,FreezerState,Result')
    return dict(line.split('=', 1) for line in value.splitlines() if '=' in line)


def command(definition):
    starts = [line[10:] for line in definition.splitlines()
              if line.startswith('ExecStart=') and line != 'ExecStart=']
    require(bool(starts), 'missing_exec_start')
    return shlex.split(starts[-1])


def proc_command(pid):
    return (Path('/proc') / str(pid) / 'cmdline').read_bytes().decode().rstrip('\0').split('\0')


def protected_pids():
    result = {}
    for unit in PROTECTED:
        status = show(unit)
        require(status['ActiveState'] == 'active' and int(status['MainPID']) > 0,
                'protected_unit_not_active')
        result[unit] = int(status['MainPID'])
    require(result['hivezilla-validator.service'] == 2142808, 'validator_pid_changed')
    return result


def inspect_archive(output, expected_source, namespace, require_initial=False):
    path = output / 'cold-storage/archive.sqlite'
    deadline = time.monotonic() + 15
    with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=1)) as db:
        db.execute('PRAGMA query_only=ON')
        db.set_progress_handler(lambda: time.monotonic() > deadline, 10000)
        meta = json.loads(db.execute('SELECT value FROM meta WHERE id=1').fetchone()[0])
        require(meta == {'source': expected_source, 'namespace': namespace}, 'archive_binding_changed')
        count, retired = db.execute('SELECT count(*),coalesce(sum(retired),0) FROM archives').fetchone()
        indexed = 'archives_retired' in {row[1] for row in db.execute('PRAGMA index_list(archives)')}
    if require_initial:
        require(count == retired == 257162, 'initial_archive_count_changed')
    else:
        require(257162 <= retired <= count <= 308768, 'approved_archive_cap_or_counter_invalid')
    return dict(archived_banks=count, retired_banks=retired, count_index_present=indexed)


def install(path, body, name):
    require(not path.exists() and not path.is_symlink(), 'new_dropin_already_exists')
    stage = REPORT / 'new' / name
    atomic(stage, body)
    run('sudo', 'install', '-d', '-m', '755', str(path.parent))
    run('sudo', 'install', '-m', '644', str(stage), str(path))
    require(subprocess.check_output(['sudo', 'cat', '--', str(path)]) == body, 'installed_dropin_mismatch')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--expected-registry-sha256', required=True)
    parser.add_argument('--expected-live-helper-sha256', required=True)
    parser.add_argument('--expected-guardian-sha256', required=True)
    parser.add_argument('--apply', action='store_true', help='Apply the already approved exact plan.')
    args = parser.parse_args()
    for digest in (args.expected_registry_sha256, args.expected_live_helper_sha256,
                   args.expected_guardian_sha256):
        require(re.fullmatch('[0-9a-f]{64}', digest), 'invalid_expected_digest')
    directory = Path(__file__).resolve().parent
    bundle_bytes = read(directory / 'BUNDLE.json', 256 * 1024)
    require(sha(bundle_bytes) == BUNDLE_SHA256, 'bundle_changed')
    bundle = json.loads(bundle_bytes)
    require(bundle['approval_text'] == APPROVAL, 'approval_scope_changed')
    files = {name: base64.b64decode(body, validate=True) for name, body in bundle['files'].items()}
    require(all(sha(body) == bundle['sha256'][name] for name, body in files.items()), 'prepared_file_changed')
    require(sha(files['STORAGE-PLAN.json']) == PLAN_SHA256, 'approved_plan_changed')
    plan = json.loads(files['STORAGE-PLAN.json'])
    row = json.loads(files['PROPOSED-18DDD9A6-REGISTRY-ROW.json'])
    output = Path(row['output'])
    require(plan['unit'] == row['unit'] == UNIT and row['max_archived_banks'] == 308768,
            'approved_unit_or_cap_changed')
    require(plan['source'] == row['source'] and plan['proposed']['maximum_additional_banks'] == 51606,
            'approved_source_or_allowance_changed')
    require(row['expected_command'] == plan['proposed']['command'], 'approved_command_changed')
    require(command(files['PROPOSED-18DDD9A6-OVERRIDE.conf'].decode()) == row['expected_command'],
            'override_command_changed')
    require(sha(files['PROPOSED-18DDD9A6-OVERRIDE.conf']) == plan['proposed']['override_sha256']
            and sha(files['PROPOSED-SUPERVISOR-MOUNT.conf']) == plan['proposed']['supervisor_mount_sha256'],
            'approved_dropin_changed')
    for name in ('helper',):
        helper = Path(row[name])
        require(helper.resolve(strict=True) == helper and not helper.is_symlink()
                and sha(read(helper)) == row['helper_sha256'], 'approved_helper_changed')
    require(sha(read(Path(plan['original']['command'][1]))) == plan['original']['helper_sha256'],
            'original_helper_changed')
    definition = subprocess.check_output(['sudo', 'systemctl', 'cat', UNIT], text=True)
    original_unit = subprocess.check_output(['sudo', 'cat', '--', plan['original']['unit_path']])
    require(sha(original_unit) == plan['original']['unit_sha256']
            and original_unit == files['ORIGINAL-18DDD9A6.service'], 'original_unit_changed')
    require(command(definition) == plan['original']['command'], 'effective_original_command_changed')
    require([line for line in definition.splitlines() if line.startswith('# /')] ==
            ['# ' + plan['original']['unit_path']], 'unexpected_old_unit_dropin')
    initial_unit = show(UNIT)
    require(initial_unit['ActiveState'] == 'inactive' and int(initial_unit['MainPID']) == 0
            and initial_unit['Result'] == 'success', 'old_storage_not_inactive_success')
    enabled = subprocess.run(['systemctl', 'is-enabled', UNIT], text=True, capture_output=True, timeout=15)
    require(enabled.stdout.strip() == 'disabled', 'old_unit_enablement_changed')
    guardian_definition = subprocess.check_output(['sudo', 'systemctl', 'cat', GUARDIAN], text=True)
    guardian_command = command(guardian_definition)
    require(guardian_command[:2] == ['/usr/bin/python3', '-B'] and len(guardian_command) > 3,
            'unexpected_guardian_launcher')
    guardian_helper = Path(guardian_command[2])
    require(guardian_helper.is_relative_to(ROOT / 'releases') and guardian_helper.name == 'storage_jobs.py'
            and guardian_helper.resolve(strict=True) == guardian_helper
            and sha(read(guardian_helper)) == args.expected_guardian_sha256, 'guardian_helper_changed')
    spec = importlib.util.spec_from_file_location('approved_storage_jobs', guardian_helper)
    guardian = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(guardian)
    registry_bytes = read(REGISTRY, 256 * 1024)
    require(sha(registry_bytes) == args.expected_registry_sha256, 'current_registry_changed')
    root, previous_jobs, _ = guardian.load_registry(REGISTRY)
    require(root == ROOT and len(previous_jobs) == 2, 'unexpected_current_registry_scope')
    baseline = {job['unit']: job for job in bundle['baseline_registry']['jobs']}
    previous = {job['unit']: job for job in previous_jobs}
    require(set(previous) == {LIVE, OLD} and previous[OLD] == baseline[OLD]
            and previous[OLD]['max_archived_banks'] == 785546, 'existing_old_cap_or_row_changed')
    live = previous[LIVE]
    require(live['helper_sha256'] == args.expected_live_helper_sha256
            and sha(read(Path(live['helper']))) == args.expected_live_helper_sha256, 'current_live_helper_changed')
    comparable = copy.deepcopy(live)
    for name in ('helper', 'helper_sha256', 'expected_command', 'expected_command_sha256'):
        comparable[name] = baseline[LIVE][name]
    require(comparable == baseline[LIVE], 'current_live_scope_changed')
    baseline_live_command = list(baseline[LIVE]['expected_command'])
    baseline_live_command[1] = live['helper']
    require(live['expected_command'] == baseline_live_command, 'current_live_options_changed')
    guardian.validate_job(ROOT, row)
    initial_health = guardian.collect(ROOT, previous_jobs, registry_sha256=sha(registry_bytes))
    require(initial_health['state'] == 'HEALTHY' and not initial_health['issues'], 'current_storage_not_healthy')
    require(next(job for job in initial_health['jobs'] if job['unit'] == LIVE)['state'] == 'RUNNING',
            'current_live_storage_not_running')
    require(next(job for job in initial_health['jobs'] if job['unit'] == OLD)['state'] == 'COMPLETE',
            'previous_capped_job_not_complete')
    pids = protected_pids()
    source_bytes = read(PIN, 65536)
    require(sha(source_bytes) == plan['active_source_pin_sha256']
            and json.loads(source_bytes) == plan['active_source'], 'active_source_changed')

    def source_checks():
        require(read(PIN, 65536) == source_bytes, 'active_source_changed')
        for name, digest in plan['read_only_measurement']['source_header_and_checkpoint_sha256'].items():
            require(sha(read(ROOT / name)) == digest, 'old_source_header_or_checkpoint_changed')
        require(protected_pids() == pids, 'protected_pid_changed')

    source_checks()
    proposed_registry = json.loads(registry_bytes)
    proposed_registry['jobs'].append(row)
    proposed_registry_bytes = (json.dumps(proposed_registry, indent=2) + '\n').encode()
    proposed_registry_sha = sha(proposed_registry_bytes)
    override_path = Path('/etc/systemd/system') / (UNIT + '.d') / plan['proposed']['override_filename']
    mount_path = Path('/etc/systemd/system') / (GUARDIAN + '.d') / '97-approved-source-18ddd9a6.conf'
    require(not override_path.exists() and not mount_path.exists(), 'prepared_dropin_already_exists')
    before = dict(source=plan['source'], active_source=plan['active_source'],
                  protected_pids=pids, current_registry_sha256=sha(registry_bytes),
                  current_jobs=previous_jobs, guardian_command=guardian_command,
                  guardian_sha256=args.expected_guardian_sha256, guardian_unit_sha256=sha(guardian_definition.encode()),
                  old_unit=initial_unit, free_disk_bytes=shutil.disk_usage(ROOT).free)
    mutated = False
    record = None
    with ExitStack() as resources:
        lock_path = output / 'cold-storage/LOCK'
        lock = resources.enter_context(lock_path.open('rb'))
        require(stat.S_ISREG(os.fstat(lock.fileno()).st_mode) and not lock_path.is_symlink(), 'invalid_archive_lock')
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        before['archive'] = inspect_archive(output, row['source'], row['namespace'], require_initial=True)
        cold_status = json.loads(read(output / 'cold-storage/STATUS.json', 1024 * 1024))
        require(cold_status.get('pending_bundles') == 0, 'pending_bundles_recorded')
        require(not list((output / 'cold-storage').glob('bundle*.tar'))
                and not [p for p in (output / 'cold-storage').iterdir() if 'pending' in p.name.lower()],
                'pending_transfer_files_present')
        if not args.apply:
            print(json.dumps(dict(state='PREFLIGHT_PASSED_NO_SERVICE_ACTIONS', **before)))
            return
        require(not (REPORT / 'INSTALLATION.json').exists(), 'installation_record_already_exists')
        REPORT.mkdir(mode=0o700, parents=True, exist_ok=True)
        installer_lock = resources.enter_context((REPORT / 'INSTALL.lock').open('a'))
        fcntl.flock(installer_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        require(not (REPORT / 'INSTALLATION.json').exists(), 'installation_record_already_exists')
        for name, body in files.items():
            atomic(REPORT / 'prepared' / name, body)
        atomic(REPORT / 'previous-registry.json', registry_bytes)
        atomic(REPORT / 'previous-guardian-unit.conf', guardian_definition.encode())
        save(REPORT / 'APPROVAL.json', dict(approval_text=APPROVAL, authority='Direct human approval in the current chat.',
             plan_sha256=PLAN_SHA256, source=row['source'], absolute_bank_limit=308768,
             maximum_additional_banks=51606, recorded_unix_ms=int(time.time() * 1000)))
        save(REPORT / 'BEFORE.json', before)
        record = dict(schema='blockzilla-approved-extra-storage-install-v1', state='PREPARED',
                      started_unix_ms=int(time.time() * 1000), plan_sha256=PLAN_SHA256,
                      installer_sha256=sha(read(Path(__file__))), source=row['source'], active_source=plan['active_source'],
                      absolute_bank_limit=308768, maximum_additional_banks=51606,
                      previous_cap_785546_changed=False, protected_pids=pids,
                      registry_sha256=proposed_registry_sha, command_sha256=row['expected_command_sha256'])
        save(REPORT / 'INSTALLATION.json', record)
        try:
            mutated = True
            run('sudo', 'systemctl', 'stop', GUARDIAN, timeout=90)
            require(show(GUARDIAN)['ActiveState'] == 'inactive', 'guardian_did_not_stop')
            source_checks()
            require(read(REGISTRY, 256 * 1024) == registry_bytes, 'registry_changed_before_install')
            require(command(run('sudo', 'systemctl', 'cat', UNIT)) == plan['original']['command'],
                    'old_command_changed_before_install')
            require(show(UNIT)['ActiveState'] == 'inactive', 'old_job_started_before_install')
            inspect_archive(output, row['source'], row['namespace'], require_initial=True)
            install(override_path, files['PROPOSED-18DDD9A6-OVERRIDE.conf'], 'old-source-override.conf')
            install(mount_path, files['PROPOSED-SUPERVISOR-MOUNT.conf'], 'guardian-mount.conf')
            atomic(REGISTRY, proposed_registry_bytes)
            run('sudo', 'systemctl', 'daemon-reload')
            require(command(run('sudo', 'systemctl', 'cat', UNIT)) == row['expected_command']
                    and show(UNIT)['Restart'] == 'no', 'installed_old_command_or_restart_mismatch')
            require(command(run('sudo', 'systemctl', 'cat', GUARDIAN)) == guardian_command,
                    'guardian_command_changed')
            require(read(REGISTRY) == proposed_registry_bytes, 'installed_registry_changed')
            source_checks()
            fcntl.flock(lock, fcntl.LOCK_UN)
            lock.close()
            job_started = int(time.time() * 1000)
            run('sudo', 'systemctl', 'start', UNIT, timeout=90)
            deadline = time.monotonic() + 240
            last = None
            while time.monotonic() < deadline:
                source_checks()
                status = show(UNIT)
                if status['ActiveState'] == 'failed':
                    raise RuntimeError('approved_job_failed_during_start')
                if status['ActiveState'] == 'active' and int(status['MainPID']) > 0:
                    try:
                        if proc_command(status['MainPID']) == row['expected_command']:
                            last = guardian.inspect_job(ROOT, row)
                            cold_status = json.loads(read(output / 'cold-storage/STATUS.json', 1024 * 1024))
                            if (last['state'] in ('RUNNING', 'PAUSED', 'COMPLETE') and not last['issues']
                                    and cold_status.get('updated_unix_ms', 0) >= job_started):
                                break
                    except (FileNotFoundError, json.JSONDecodeError, sqlite3.Error):
                        pass
                time.sleep(2)
            else:
                raise RuntimeError('approved_job_or_archive_index_not_ready')
            record.update(state='JOB_READY', job_pid=int(status['MainPID']), initial_job_status=last)
            save(REPORT / 'INSTALLATION.json', record)
            run('sudo', 'systemctl', 'start', GUARDIAN, timeout=90)
            guardian_started = int(time.time() * 1000)
            deadline = time.monotonic() + 150
            while time.monotonic() < deadline:
                source_checks()
                require(read(REGISTRY, 256 * 1024) == proposed_registry_bytes, 'registry_changed_during_readiness')
                status = json.loads(read(ROOT / 'reports/storage-jobs/STATUS.json', 1024 * 1024))
                expected = {job['unit']: (job['source'], job['max_archived_banks']) for job in proposed_registry['jobs']}
                actual = {job['unit']: (job['source'], job['max_archived_banks']) for job in status.get('jobs', [])}
                if (status.get('state') == 'HEALTHY' and status.get('issues') == []
                        and status.get('registry_sha256') == proposed_registry_sha
                        and status.get('active_source') == plan['active_source']
                        and guardian_started <= status.get('updated_unix_ms', 0) <= int(time.time() * 1000) + 5000
                        and actual == expected and len(status['jobs']) == 3):
                    break
                time.sleep(2)
            else:
                raise RuntimeError('registered_guardian_not_healthy')
            source_checks()
            after = inspect_archive(output, row['source'], row['namespace'])
            require(after['count_index_present'], 'archive_count_index_missing')
            after.update(updated_unix_ms=int(time.time() * 1000), free_disk_bytes=shutil.disk_usage(ROOT).free,
                         storage_jobs=status, protected_pids=protected_pids())
            save(REPORT / 'AFTER.json', after)
            record.update(state='ACTIVE', completed_unix_ms=int(time.time() * 1000), after=after,
                          protected_pids_unchanged=True, data_rollback_performed=False,
                          old_job_enablement_preserved='disabled')
            save(REPORT / 'INSTALLATION.json', record)
            print(json.dumps(dict(state='ACTIVE', source=row['source'], absolute_bank_limit=308768,
                                  maximum_additional_banks=51606, archived_banks=after['archived_banks'],
                                  retired_banks=after['retired_banks'], protected_pids_unchanged=True)))
        except Exception as error:
            failure = dict(state='FAILED_SAFE_STOP', error_type=type(error).__name__,
                           absolute_cap_override_retained=override_path.exists(), data_rollback_performed=False)
            try:
                # Never restore the old per-process command: keep the absolute cap.
                run('sudo', 'systemctl', 'stop', GUARDIAN, UNIT, timeout=120)
                require(show(UNIT)['ActiveState'] in ('inactive', 'failed'), 'failed_job_did_not_stop')
                require(read(PIN, 65536) == source_bytes, 'source_changed_during_failure')
                current = read(REGISTRY, 256 * 1024)
                require(current in (registry_bytes, proposed_registry_bytes), 'concurrent_registry_change')
                if current == proposed_registry_bytes:
                    atomic(REGISTRY, registry_bytes)
                run('sudo', 'systemctl', 'start', GUARDIAN, timeout=90)
                failure['previous_two_row_registry_restored'] = True
            except Exception as stop_error:
                failure.update(state='FAILED_STOP_OR_GUARDIAN_RESTORE_UNVERIFIED', stop_error_type=type(stop_error).__name__)
            record.update(failure, failed_unix_ms=int(time.time() * 1000))
            save(REPORT / 'INSTALLATION.json', record)
            raise RuntimeError('approved_storage_install_failed; inspect INSTALLATION.json') from None


if __name__ == '__main__':
    main()
