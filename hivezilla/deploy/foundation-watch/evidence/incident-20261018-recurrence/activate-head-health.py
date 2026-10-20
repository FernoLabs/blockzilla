import hashlib
import importlib.util
import json
import subprocess
import time
from pathlib import Path

root = Path('/home/ubuntu/live/hivezilla-validator-20260930')
report = root / 'reports/native-recovery-20261018/recurrence/head-health'
build = json.loads((report / 'BUILD.json').read_text())
assert build['state'] == 'READY_NOT_ACTIVE'
assert build['tests_passed'] is True and build['tests_count'] >= 33
assert build['patch_sha256'] == 'c319ba5d6243b0526527371a1bdfcc54ed14bc477378fd7d5b2d94c72c5a6539'
release = Path(build['release'])
assert release.parent == root / 'releases' and release.resolve(strict=True) == release
binary = release / 'hivezilla-stream'
assert not binary.is_symlink() and hashlib.sha256(binary.read_bytes()).hexdigest() == build['binary_sha256']

def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value

m = module('recovery', root / 'releases/incident-resume-20261018-d25366e623ecead4/recover_session.py')
v = module('verify', root / 'releases/incident-recovery-20261018-a6b89ac28c538eaa/verify_live.py')
unit = 'hivezilla-bank-v2-origin.service'
source_path = root / 'config/native-active-source.json'
source_bytes = source_path.read_bytes()
source = json.loads(source_bytes)
assert source['session_id'] == '000000000000000018df884b2474cf8e-20b258'
definition = m.run('sudo', 'systemctl', 'cat', unit)
old_command = m.exec_args(definition)
assert old_command[1] == 'bank-origin'
assert Path(old_command[0]) == root / 'releases/bank-stream-v2-transport-r10-20261010/hivezilla-stream'
assert hashlib.sha256(Path(old_command[0]).read_bytes()).hexdigest() == '1bfc4577e5090d2a073a7114a0dd086f6397f4205e0909fcd20cc701d79846ae'
assert old_command[old_command.index('--output') + 1] == str(root / 'compact/bank-v2-live' / source['session_id'])
spool = Path(old_command[old_command.index('--spool') + 1])
assert spool == root / 'bank-v2-origin-spool' / source['session_id']
checkpoint = json.loads((spool / 'CHECKPOINT.json').read_text())
assert checkpoint['source'] == source
before_confirmed_sequence = checkpoint['chain']['latest']['event_sequence']
override = Path('/etc/systemd/system') / (unit + '.d') / m.SOURCE_DROPIN
old_override = m.run('sudo', 'cat', str(override))
assert m.exec_args(old_override) == old_command
protected = ['hivezilla-validator', 'hivezilla-direct-worker', 'hivezilla-bank-v2-producer',
             'hivezilla-stream-origin', 'hivezilla-stream-relay', 'hivezilla-bank-v2-r2-publisher',
             'hivezilla-shred-hook-recorder']
pids = {name: m.run('systemctl', 'show', name, '-p', 'MainPID', '--value') for name in protected}
assert pids['hivezilla-validator'] == '2142808' and all(int(pid) > 0 for pid in pids.values())
old_pid = m.run('systemctl', 'show', unit, '-p', 'MainPID', '--value')
assert int(old_pid) > 0
assert (Path('/proc') / old_pid / 'cmdline').read_bytes().decode().rstrip('\0').split('\0') == old_command
assert v.rpc('getHealth') == 'ok'
new_command = [str(binary), *old_command[1:]]
lines = old_override.splitlines()
starts = [i for i, line in enumerate(lines) if line.startswith('ExecStart=') and line != 'ExecStart=']
assert len(starts) == 1
lines[starts[0]] = 'ExecStart=' + m.systemd_command(new_command)
assert not (report / 'INSTALLATION.json').exists()
m.write(report / 'previous-origin-override.conf', old_override + '\n')
record = dict(state='PREPARED', started_unix_ms=int(time.time()*1000), source=source,
    source_pin_sha256=hashlib.sha256(source_bytes).hexdigest(), release=str(release),
    binary_sha256=build['binary_sha256'], old_origin_pid=old_pid, protected_pids=pids,
    old_command=old_command, command=new_command, before_confirmed_sequence=before_confirmed_sequence,
    status_interval_ms=1000, head_health_expiry_ms=5000, retirement_cap_changes=[])
m.json_write(report / 'INSTALLATION.json', record)

def verify_running(command, not_before, timeout=180):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        pid = m.run('systemctl', 'show', unit, '-p', 'MainPID', '--value')
        try:
            actual = (Path('/proc') / pid / 'cmdline').read_bytes().decode().rstrip('\0').split('\0')
            if actual == command and time.monotonic() >= not_before:
                assert hashlib.sha256((Path('/proc') / pid / 'exe').read_bytes()).hexdigest() == hashlib.sha256(Path(command[0]).read_bytes()).hexdigest()
                assert source_path.read_bytes() == source_bytes
                assert {name: m.run('systemctl','show',name,'-p','MainPID','--value') for name in protected} == pids
                cp = json.loads((spool / 'CHECKPOINT.json').read_text())
                assert cp['source'] == source
                if cp['chain']['latest']['event_sequence'] <= before_confirmed_sequence:
                    time.sleep(1)
                    continue
                details = {}
                v.private_sample(root, source, details)
                assert cp['chain']['latest']['event_sequence'] > before_confirmed_sequence
                assert source_path.read_bytes() == source_bytes
                assert {name: m.run('systemctl','show',name,'-p','MainPID','--value') for name in protected} == pids
                return pid, details, cp['chain']['latest']['event_sequence']
        except (FileNotFoundError, v.CheckFailure, OSError):
            pass
        time.sleep(1)
    raise RuntimeError('origin_or_private_feed_did_not_recover')

try:
    assert source_path.read_bytes() == source_bytes
    m.install(report, override, '\n'.join(lines) + '\n')
    m.run('sudo', 'systemctl', 'daemon-reload')
    m.run('sudo', 'systemctl', 'restart', unit)
    pid, details, sequence = verify_running(new_command, time.monotonic() + 6)
    record.update(state='ACTIVE', completed_unix_ms=int(time.time()*1000), origin_pid=pid,
        protected_pids_unchanged=True, private_head_and_frame=details, confirmed_sequence=sequence,
        validator_restarted=False, source_changed=False)
    m.json_write(report / 'INSTALLATION.json', record)
    print(json.dumps({k:record[k] for k in ('state','origin_pid','old_origin_pid','protected_pids_unchanged',
                                         'status_interval_ms','head_health_expiry_ms','source_changed')}))
except Exception as error:
    record.update(state='ACTIVATION_FAILED', error_type=type(error).__name__)
    m.json_write(report / 'INSTALLATION.json', record)
    try:
        m.run('sudo', 'systemctl', 'stop', unit)
        assert source_path.read_bytes() == source_bytes
        m.install(report, override, old_override + '\n')
        m.run('sudo', 'systemctl', 'daemon-reload')
        m.run('sudo', 'systemctl', 'start', unit)
        pid, details, sequence = verify_running(old_command, time.monotonic() + 6)
        record.update(rollback='PREVIOUS_BINARY_VERIFIED', rollback_origin_pid=pid,
                      rollback_private_head_and_frame=details, rollback_confirmed_sequence=sequence)
        m.json_write(report / 'INSTALLATION.json', record)
    except Exception as rollback_error:
        record.update(state='ROLLBACK_FAILED', rollback_error_type=type(rollback_error).__name__)
        m.json_write(report / 'INSTALLATION.json', record)
        raise RuntimeError('origin_activation_and_rollback_failed; inspect INSTALLATION.json') from None
    raise RuntimeError('origin_activation_failed_previous_binary_restored') from None
