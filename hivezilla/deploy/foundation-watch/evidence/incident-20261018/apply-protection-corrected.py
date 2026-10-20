import hashlib
import importlib.util
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import time


root = Path('/home/ubuntu/live/hivezilla-validator-20260930')
release = Path(sys.argv[1])
assert release.parent == root / 'releases' and release.resolve(strict=True) == release
manifest = json.loads((release / 'MANIFEST.json').read_text())
for name, digest in manifest.items():
    assert Path(name).name == name
    path = release / name
    assert not path.is_symlink() and hashlib.sha256(path.read_bytes()).hexdigest() == digest

spec = importlib.util.spec_from_file_location('recovery', root / 'releases/incident-resume-20261018-d25366e623ecead4/recover_session.py')
recovery = importlib.util.module_from_spec(spec)
spec.loader.exec_module(recovery)
spec = importlib.util.spec_from_file_location('jobs', release / 'storage_jobs.py')
jobs = importlib.util.module_from_spec(spec)
spec.loader.exec_module(jobs)

report = root / 'reports/native-recovery-20261018/long-term-protection'
report.mkdir(mode=0o700, exist_ok=False)
live = 'hivezilla-native-cold-storage-20261012-live.service'
old = 'hivezilla-native-cold-storage-20261016-source-18de229d.service'
priority = 'hivezilla-live-priority-20261012.service'
heartbeat = 'hivezilla-foundation-heartbeat.service'
capacity = 'hivezilla-native-capacity.service'
supervisor = 'hivezilla-native-storage-jobs.service'
units = [live, old, priority, heartbeat, capacity]
source = json.loads((root / 'config/native-active-source.json').read_text())
assert source['session_id'] == '000000000000000018df884b2474cf8e-20b258'
assert recovery.run('systemctl', 'show', 'hivezilla-validator', '-p', 'MainPID', '--value') == '2142808'
protected = ['hivezilla-validator', 'hivezilla-direct-worker', 'hivezilla-bank-v2-producer',
             'hivezilla-bank-v2-origin', 'hivezilla-stream-origin', 'hivezilla-stream-relay',
             'hivezilla-bank-v2-r2-publisher', 'hivezilla-shred-hook-recorder']
pids = {unit: recovery.run('systemctl', 'show', unit, '-p', 'MainPID', '--value') for unit in protected}
assert all(int(pid) > 0 for pid in pids.values())
definitions = {unit: recovery.run('sudo', 'systemctl', 'cat', unit) for unit in units}
commands = {unit: recovery.exec_args(text) for unit, text in definitions.items()}
previous_commands = {unit: list(args) for unit, args in commands.items()}
for unit, text in definitions.items():
    recovery.write(report / (unit + '.previous-unit'), text + '\n')

for unit in (live, old):
    commands[unit][1] = str(release / 'cold_storage.py')
assert commands[old][-2:] == ['--max-archived-banks', '785546']
assert '--max-archived-banks' not in commands[live]
assert commands[priority][:2] == ['/usr/bin/python3', '-u']
commands[priority][2] = str(release / 'live_priority.py')
assert commands[heartbeat][:2] == ['/usr/bin/python3', '-B']
commands[heartbeat][2] = str(release / 'heartbeat.py')
commands[capacity][1] = str(release / 'capacity_guard.py')

entries = []
for unit in (live, old):
    args = commands[unit]
    output = Path(args[args.index('--output') + 1])
    feed = json.loads((output / 'FEED.json').read_text())
    identity = {key: feed[key] for key in jobs.SOURCE_KEYS}
    maximum = None if unit == live else 785546
    if maximum is None:
        assert identity == source
    else:
        assert identity['session_id'] == '000000000000000018de229d3fc8a9d9-275b05'
    with sqlite3.connect((output / 'cold-storage/archive.sqlite').as_uri() + '?mode=ro', uri=True) as db:
        meta = json.loads(db.execute('SELECT value FROM meta WHERE id=1').fetchone()[0])
        count = db.execute('SELECT count(*) FROM archives').fetchone()[0]
    assert meta['source'] == identity and (maximum is None or count <= maximum)
    entry = dict(unit=unit, source=identity, output=str(output), max_archived_banks=maximum,
                 namespace=meta['namespace'], helper=str(release / 'cold_storage.py'),
                 helper_sha256=manifest['cold_storage.py'], expected_command=args,
                 expected_command_sha256=jobs.command_sha256(args), allow_restart=True,
                 max_retries=3, retry_delay_seconds=60)
    jobs.validate_job(root, entry)
    entries.append(entry)
registry = dict(schema=jobs.SCHEMA, root=str(root), jobs=entries)
registry_path = root / 'config/storage-jobs.json'
assert not registry_path.exists()
job_report = root / 'reports/storage-jobs'
job_report.mkdir(mode=0o700, exist_ok=False)
record = dict(state='PREPARED', release=str(release), source=source, protected_pids=pids,
              started_unix_ms=int(time.time() * 1000), registry=registry,
              notes=['No validator or feed restart.', 'Existing source, age, replay and full readback gates remain.'])
recovery.json_write(report / 'INSTALLATION.json', record)

override_paths = {
    live: Path('/etc/systemd/system') / (live + '.d') / recovery.SOURCE_DROPIN,
    old: Path('/etc/systemd/system') / (old + '.d') / 'zz-incident-cap-20261018.conf',
    priority: Path('/etc/systemd/system') / (priority + '.d') / ('z' * 64 + '-storage-patch.conf'),
    heartbeat: Path('/etc/systemd/system') / (heartbeat + '.d') / ('z' * 128 + '-agave-4.3.2.conf'),
    capacity: Path('/etc/systemd/system') / capacity,
}
for unit, path in override_paths.items():
    assert recovery.exec_args(recovery.run('sudo', 'cat', str(path))) == previous_commands[unit]


def replace_start(path, args, suffix=''):
    text = recovery.run('sudo', 'cat', str(path))
    lines = text.splitlines()
    found = [i for i, line in enumerate(lines) if line.startswith('ExecStart=') and line != 'ExecStart=']
    assert len(found) == 1
    lines[found[0]] = 'ExecStart=' + recovery.systemd_command(args)
    recovery.install(report, path, '\n'.join(lines) + '\n' + suffix)


# Stop only storage jobs while their implementation and retry owner change.
# Interrupted immutable uploads remain uncommitted; retirement resumes from its durable index.
recovery.run('sudo', 'systemctl', 'stop', live, old)
retry_owner = '[Unit]\nStartLimitIntervalSec=0\n[Service]\nRestart=no\n'
replace_start(override_paths[live], commands[live], retry_owner)
replace_start(override_paths[old], commands[old], retry_owner)
replace_start(override_paths[priority], commands[priority])
replace_start(override_paths[heartbeat], commands[heartbeat])
replace_start(override_paths[capacity], commands[capacity])
recovery.json_write(registry_path, registry)
policy = root / 'config/raw-capture-monitor.json'
if not policy.exists():
    recovery.json_write(policy, dict(schema='blockzilla-raw-capture-monitor-policy-v1', required=False))

control = ['/usr/bin/python3', '-B', str(release / 'storage_jobs.py'), '--registry', str(registry_path),
           '--report-dir', str(job_report), '--supervise', '--follow', '--interval-seconds', '30']
# Directory mounts follow SQLite sidecar replacement across a job restart.
# The supervisor opens each database mode=ro with query_only enabled.
cold_directories = [str(Path(entry['output']) / 'cold-storage') for entry in entries]
text = ('[Unit]\nDescription=Verify approved native storage jobs and retry safe network faults\n'
        'After=network-online.target\nStartLimitIntervalSec=0\n[Service]\nUser=ubuntu\nGroup=ubuntu\n'
        'UMask=0077\nExecStart=' + recovery.systemd_command(control) + '\nRestart=on-failure\nRestartSec=30\n'
        'MemoryMax=512M\nCPUQuota=20%\nNice=10\nIOWeight=10\nProtectSystem=strict\nProtectHome=read-only\n'
        'PrivateTmp=true\nReadWritePaths=' + recovery.systemd_command([str(job_report), *cold_directories]) +
        '\n[Install]\nWantedBy=multi-user.target\n')
destination = Path('/etc/systemd/system') / supervisor
assert not destination.exists()
recovery.install(report, destination, text)
record.update(state='CONFIGURED', commands=commands, supervisor_command=control)
recovery.json_write(report / 'INSTALLATION.json', record)
recovery.run('sudo', 'systemctl', 'daemon-reload')
recovery.run('sudo', 'systemctl', 'restart', capacity)
for unit in (live, old):
    if recovery.run('systemctl', 'show', unit, '-p', 'ActiveState', '--value') == 'failed':
        recovery.run('sudo', 'systemctl', 'reset-failed', unit)
recovery.run('sudo', 'systemctl', 'start', live, old)


def running(unit, args):
    pid = recovery.run('systemctl', 'show', unit, '-p', 'MainPID', '--value')
    try:
        return int(pid) > 0 and (Path('/proc') / pid / 'cmdline').read_bytes().decode().rstrip('\0').split('\0') == args
    except (FileNotFoundError, ProcessLookupError):
        return False


for unit in (live, old, capacity):
    recovery.wait(lambda unit=unit: running(unit, commands[unit]), 20, 'new_service_command_not_running')
recovery.wait(lambda: all((Path(path) / 'archive.sqlite').is_file() for path in cold_directories),
              30, 'storage_databases_not_ready')
recovery.run('sudo', 'systemctl', 'restart', priority)
recovery.wait(lambda: running(priority, commands[priority]), 20, 'priority_command_not_running')
recovery.run('sudo', 'systemctl', 'enable', '--now', supervisor)


def registry_healthy():
    path = job_report / 'STATUS.json'
    if not path.exists():
        return None
    value = json.loads(path.read_text())
    return value if value.get('state') == 'HEALTHY' else None


status = recovery.wait(registry_healthy, 120, 'storage_supervisor_not_healthy')
recovery.run('sudo', 'systemctl', 'start', heartbeat)
assert {unit: recovery.run('systemctl', 'show', unit, '-p', 'MainPID', '--value') for unit in protected} == pids
record.update(state='ACTIVE', supervisor_status=status, protected_pids_unchanged=True,
              updated_unix_ms=int(time.time() * 1000))
recovery.json_write(report / 'INSTALLATION.json', record)
print(json.dumps(dict(state='ACTIVE', protected_pids_unchanged=True, jobs=status['jobs'])))
