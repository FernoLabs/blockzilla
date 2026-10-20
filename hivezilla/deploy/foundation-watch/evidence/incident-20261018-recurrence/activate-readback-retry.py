import hashlib
import importlib.util
import json
import subprocess
import time
from pathlib import Path

root = Path('/home/ubuntu/live/hivezilla-validator-20260930')
release = root / 'releases/readback-retry-20261018-e5db5eeaf39a4f88'
report = root / 'reports/native-recovery-20261018/recurrence/readback-retry'
stage = json.loads((report / 'STAGE.json').read_text())
assert stage['state'] == 'STAGED_TESTED_NOT_ACTIVE' and stage['release'] == str(release)
assert hashlib.sha256((release / 'MANIFEST.json').read_bytes()).hexdigest() == stage['manifest_sha256']
for name, digest in stage['manifest'].items():
    assert not (release / name).is_symlink()
    assert hashlib.sha256((release / name).read_bytes()).hexdigest() == digest

def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value

recovery = module('recovery', root / 'releases/incident-resume-20261018-d25366e623ecead4/recover_session.py')
jobs = module('jobs', root / 'releases/storage-protection-20261018-94c2aaca04758c49/storage_jobs.py')
live = 'hivezilla-native-cold-storage-20261012-live.service'
supervisor = 'hivezilla-native-storage-jobs.service'
registry_path = root / 'config/storage-jobs.json'
registry_bytes = registry_path.read_bytes()
registry = json.loads(registry_bytes)
assert len(registry['jobs']) == 2
old_job = next(job for job in registry['jobs'] if job['max_archived_banks'] == 785546)
source = json.loads((root / 'config/native-active-source.json').read_text())
job = next(job for job in registry['jobs'] if job['unit'] == live)
assert job['source'] == source and job['max_archived_banks'] is None
assert job['helper_sha256'] == '7f0d6719792abf9b65d954e824fe59a8fa3126210136ebc6c0a8386f26a57b71'
assert hashlib.sha256(Path(job['helper']).read_bytes()).hexdigest() == job['helper_sha256']
old_command = list(job['expected_command'])
assert old_command[1] == job['helper']
definition = recovery.run('sudo', 'systemctl', 'cat', live)
assert recovery.exec_args(definition) == old_command
override = Path('/etc/systemd/system') / (live + '.d') / recovery.SOURCE_DROPIN
old_override = recovery.run('sudo', 'cat', str(override))
assert recovery.exec_args(old_override) == old_command
protected = ['hivezilla-validator', 'hivezilla-direct-worker', 'hivezilla-bank-v2-producer',
             'hivezilla-bank-v2-origin', 'hivezilla-stream-origin', 'hivezilla-stream-relay',
             'hivezilla-bank-v2-r2-publisher', 'hivezilla-shred-hook-recorder']
pids = {unit: recovery.run('systemctl', 'show', unit, '-p', 'MainPID', '--value') for unit in protected}
assert pids['hivezilla-validator'] == '2142808' and all(int(pid) > 0 for pid in pids.values())
installation = report / 'INSTALLATION.json'
assert not installation.exists()
recovery.write(report / 'previous-registry.json', registry_bytes.decode())
recovery.write(report / 'previous-live-override.conf', old_override + '\n')
new_command = list(old_command)
new_command[1] = str(release / 'cold_storage.py')
job.update(helper=new_command[1], helper_sha256=stage['manifest']['cold_storage.py'],
           expected_command=new_command, expected_command_sha256=jobs.command_sha256(new_command))
jobs.validate_job(root, job)
assert old_job['max_archived_banks'] == 785546
lines = old_override.splitlines()
matches = [n for n, line in enumerate(lines) if line.startswith('ExecStart=') and line != 'ExecStart=']
assert len(matches) == 1
lines[matches[0]] = 'ExecStart=' + recovery.systemd_command(new_command)
new_override = '\n'.join(lines) + '\n'
record = dict(state='PREPARED', release=str(release), started_unix_ms=int(time.time()*1000),
              source=source, protected_pids=pids, old_command=old_command, command=new_command,
              helper_sha256=job['helper_sha256'], tests_on_host=29, cap_changes=[],
              old_source_extra_retirement_started=False)
recovery.json_write(installation, record)
try:
    recovery.run('sudo', 'systemctl', 'stop', supervisor)
    recovery.run('sudo', 'systemctl', 'stop', live)
    assert json.loads((root / 'config/native-active-source.json').read_text()) == source
    recovery.install(report, override, new_override)
    recovery.json_write(registry_path, registry)
    recovery.run('sudo', 'systemctl', 'daemon-reload')
    recovery.run('sudo', 'systemctl', 'start', live)
    for _ in range(100):
        pid = recovery.run('systemctl', 'show', live, '-p', 'MainPID', '--value')
        if int(pid) > 0:
            try:
                args = (Path('/proc') / pid / 'cmdline').read_bytes().decode().rstrip('\0').split('\0')
                if args == new_command:
                    break
            except FileNotFoundError:
                pass
        time.sleep(.1)
    else:
        raise RuntimeError('new_live_storage_command_unavailable')
    recovery.run('sudo', 'systemctl', 'start', supervisor)
    for _ in range(350):
        status = json.loads((root / 'reports/storage-jobs/STATUS.json').read_text())
        if (status['state'] == 'HEALTHY' and status['registry_sha256'] == hashlib.sha256(registry_path.read_bytes()).hexdigest()
                and status['updated_unix_ms'] >= record['started_unix_ms']):
            break
        time.sleep(.2)
    else:
        raise RuntimeError('storage_supervisor_not_healthy')
    assert json.loads((root / 'config/native-active-source.json').read_text()) == source
    assert {unit: recovery.run('systemctl','show',unit,'-p','MainPID','--value') for unit in protected} == pids
    record.update(state='ACTIVE', completed_unix_ms=int(time.time()*1000),
                  live_pid=pid, protected_pids_unchanged=True, storage_jobs=status)
    recovery.json_write(installation, record)
    print(json.dumps(dict(state=record['state'], release=str(release), live_pid=pid,
                         protected_pids_unchanged=True, cap_changes=[], tests_on_host=29)))
except Exception as error:
    record.update(state='ACTIVATION_FAILED', error_type=type(error).__name__)
    recovery.json_write(installation, record)
    try:
        recovery.run('sudo', 'systemctl', 'stop', supervisor, live)
        assert json.loads((root / 'config/native-active-source.json').read_text()) == source
        recovery.install(report, override, old_override + '\n')
        recovery.write(registry_path, registry_bytes.decode())
        recovery.run('sudo', 'systemctl', 'daemon-reload')
        rollback_started = int(time.time()*1000)
        recovery.run('sudo', 'systemctl', 'start', live)
        for _ in range(100):
            rollback_pid = recovery.run('systemctl', 'show', live, '-p', 'MainPID', '--value')
            if int(rollback_pid) > 0:
                try:
                    args = (Path('/proc') / rollback_pid / 'cmdline').read_bytes().decode().rstrip('\0').split('\0')
                    if args == old_command:
                        break
                except FileNotFoundError:
                    pass
            time.sleep(.1)
        else:
            raise RuntimeError('previous_live_storage_command_unavailable')
        recovery.run('sudo', 'systemctl', 'start', supervisor)
        for _ in range(350):
            restored = json.loads((root / 'reports/storage-jobs/STATUS.json').read_text())
            if (restored['state'] == 'HEALTHY' and restored['registry_sha256'] == hashlib.sha256(registry_bytes).hexdigest()
                    and restored['updated_unix_ms'] >= rollback_started):
                break
            time.sleep(.2)
        else:
            raise RuntimeError('restored_supervisor_not_healthy')
        assert json.loads((root / 'config/native-active-source.json').read_text()) == source
        assert {unit: recovery.run('systemctl','show',unit,'-p','MainPID','--value') for unit in protected} == pids
        record.update(rollback='PREVIOUS_HELPER_VERIFIED', rollback_pid=rollback_pid,
                      rollback_registry_sha256=restored['registry_sha256'])
        recovery.json_write(installation, record)
    except Exception as rollback_error:
        record.update(state='ROLLBACK_FAILED', rollback_error_type=type(rollback_error).__name__)
        recovery.json_write(installation, record)
        raise RuntimeError('activation_and_rollback_verification_failed; inspect INSTALLATION.json') from None
    raise RuntimeError('activation_failed_previous_helper_restored') from None
