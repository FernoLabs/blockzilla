#!/usr/bin/env python3
"""Prepare by default. --apply changes only live storage and restarts its supervisor."""
import argparse
import copy
import fcntl
import hashlib
import importlib.util
import json
import os
import re
import shlex
import sqlite3
import stat
import subprocess
import sys
import tempfile
import time
from contextlib import closing
from pathlib import Path

sys.dont_write_bytecode = True
ROOT = Path('/home/ubuntu/live/hivezilla-validator-20260930')
REPORT = ROOT / 'reports/native-recovery-20261018/recurrence/incident-20261020-storage/circuit'
REGISTRY = ROOT / 'config/storage-jobs.json'
PIN = ROOT / 'config/native-active-source.json'
LIVE = 'hivezilla-native-cold-storage-20261012-live.service'
GUARDIAN = 'hivezilla-native-storage-jobs.service'
LIVE_OVERRIDE = Path('/etc/systemd/system') / (LIVE + '.d') / ('z' * 96 + '-native-source.conf')
OLD_REGISTRY_SHA = 'a52115479c26ccff6367b8439cd58dbb2354bb7b61d25691e797a96611655f6f'
PIN_SHA = 'ddfe097a2915a946c0917adcc135a2c7d997b1517d578044bfea80aadd7ea315'
OLD_COLD_SHA = '8a95785f2df4e2f53b414322f0fc42a8d2edd93a02f04cfca6358bb312c19800'
COLD_SHA = 'f757f2be29f01378448cbe5bb5b529db2625e6654c065f0f17ebd8ba3cbb6d41'
TEST_COLD_SHA = '3a5fe9b96e7b3503c4d8c2d1a4efb8009f79889f4d92dbaa07420bc215e41cee'
GUARDIAN_SHA = '567c15e41b3f21665a765fbfaa5b2c70d9a565d1280463da959d946472a3c87c'
FINITE_SHA = '7f0d6719792abf9b65d954e824fe59a8fa3126210136ebc6c0a8386f26a57b71'
TEST_GUARDIAN_SHA = '6eef654a016d1f18be588f28bafd287d69b8aa95d8e9d02695df7cab858d7827'
OLD_RELEASE = ROOT / 'releases/transfer-deadline-20261018-498831996a2dc34c'
FINITE = {'hivezilla-native-cold-storage-20261016-source-18de229d.service': 785546,
          'hivezilla-native-cold-storage-20261016-source-18ddd9a6.service': 308768}
PIDS = {'hivezilla-validator': 2142808, 'hivezilla-direct-worker': 916425,
        'hivezilla-bank-v2-producer': 917148, 'hivezilla-bank-v2-r2-publisher': 4167494,
        'hivezilla-shred-hook-recorder': 2142799, 'hivezilla-bank-v2-origin': 2134416,
        'hivezilla-stream-origin': 2135639, 'hivezilla-stream-relay': 2136117}
LEDGER_SCHEMA = 'blockzilla-native-cold-recovery-v1'
LEDGER_MARKER_SCHEMA = 'blockzilla-native-cold-recovery-initialized-v1'
REQUIRED_TESTS = [
    'test_recovery_crash_after_verified_index_resumes_partial_retirement_without_upload',
    'test_recovery_failed_latch_write_is_not_claimed_durable_and_can_be_retried',
    'test_recovery_latch_write_fault_still_reports_safe_failed_evidence',
    'test_recovery_lost_put_reply_then_412_requires_full_readback_before_retirement',
    'test_recovery_nonfollow_restart_keeps_outage_and_waits_for_due_probe',
    'test_recovery_outage_reservations_and_probe_cadence_survive_restarts',
    'test_recovery_parallel_failures_share_budget_keep_caps_and_commit_only_proofs',
    'test_recovery_permanent_fault_is_durably_blocked_across_restart',
    'test_recovery_rejects_missing_corrupt_foreign_and_permanent_retry_state',
    'test_recovery_reset_needs_sixty_fresh_seconds_and_later_durable_progress',
    'test_recovery_restart_allows_only_one_due_probe_group_then_real_progress',
    'test_recovery_source_gate_fault_outside_transfer_durably_blocks',
    'test_recovery_successful_old_sibling_does_not_reset_outage']


def require(ok, code):
    if not ok:
        raise RuntimeError(code)


def sha(body):
    return hashlib.sha256(body).hexdigest()


def read(path, maximum=2 ** 20):
    require(stat.S_ISREG(path.lstat().st_mode), 'metadata_not_regular')
    with path.open('rb') as f:
        body = f.read(maximum + 1)
    require(len(body) <= maximum, 'metadata_too_large')
    return body


def digest(path):
    require(stat.S_ISREG(path.lstat().st_mode), 'file_not_regular')
    with path.open('rb') as f:
        return hashlib.file_digest(f, 'sha256').hexdigest()


def strict_json(body):
    def pairs(items):
        value = {}
        for key, val in items:
            require(key not in value, 'duplicate_json_key')
            value[key] = val
        return value
    return json.loads(body, object_pairs_hook=pairs,
                      parse_constant=lambda _: (_ for _ in ()).throw(RuntimeError('nonfinite_json')))


def obj(path):
    return strict_json(read(path))


def now():
    return time.time_ns() // 1000000


def dump(value):
    return (json.dumps(value, indent=2, sort_keys=True) + '\n').encode()


def save(path, value, create=False):
    body = value if isinstance(value, bytes) else dump(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    if create:
        with path.open('xb') as f:
            os.chmod(path, 0o600)
            f.write(body); f.flush(); os.fsync(f.fileno())
    else:
        with tempfile.NamedTemporaryFile(dir=path.parent, prefix=path.name + '.', delete=False) as f:
            temporary = Path(f.name)
            f.write(body); f.flush(); os.fsync(f.fileno())
        os.replace(temporary, path)
    fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def run(*args, timeout=75):
    value = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=timeout)
    require(value.returncode == 0, 'command_failed')
    return value.stdout


def unit_text(unit):
    return run('sudo', 'systemctl', 'cat', unit, timeout=15)


def command(body):
    result = []
    for line in body.decode().splitlines():
        if line == 'ExecStart=':
            result = []
        elif line.startswith('ExecStart='):
            require(not line.endswith('\\'), 'continued_command_unsupported')
            result.append(shlex.split(line[10:]))
    require(len(result) == 1 and all(not any(c in arg for c in '\0\r\n%$') for arg in result[0]),
            'effective_command_invalid')
    return result[0]


def command_sha(args):
    return sha(json.dumps(args, separators=(',', ':'), ensure_ascii=False).encode())


def replace_helper(body, old, new):
    require(command(body) == old and old[:3] == ['/usr/bin/python3', str(OLD_RELEASE / 'cold_storage.py'), 'archive'],
            'old_live_command_invalid')
    oldtoken, newtoken = json.dumps(old[1]).encode(), json.dumps(new).encode()
    # The source drop-in has a quoted helper path. Keep all other bytes and arguments.
    require(body.count(oldtoken) == 1 and re.fullmatch(r'/[A-Za-z0-9_./-]+', new), 'helper_token_not_unique')
    proposed = body.replace(oldtoken, newtoken)
    require(command(proposed) == [old[0], new, *old[2:]], 'live_arguments_changed')
    return proposed


def show(unit):
    body = run('systemctl', 'show', unit, '-p', 'MainPID,ActiveState,SubState,Result,Restart,InvocationID').decode()
    return dict(line.split('=', 1) for line in body.splitlines() if '=' in line)


def argv(pid):
    return (Path('/proc') / str(pid) / 'cmdline').read_bytes().decode().rstrip('\0').split('\0')


def authority(expected_registry=None):
    require(sha(read(PIN)) == PIN_SHA, 'source_pin_changed')
    if expected_registry is not None:
        require(read(REGISTRY) == expected_registry, 'registry_changed')
    for unit, pid in PIDS.items():
        require(int(show(unit)['MainPID']) == pid, 'protected_pid_changed')
    return obj(PIN)


def module(path):
    body = read(path); require(sha(body) == GUARDIAN_SHA, 'guardian_source_changed')
    spec = importlib.util.spec_from_loader('circuit_unchanged_guardian', loader=None)
    result = importlib.util.module_from_spec(spec); result.__file__ = str(path)
    exec(compile(body, str(path), 'exec'), result.__dict__)
    return result


def check_process(unit, expected, helper_sha, expected_pid=None):
    value = show(unit); pid = int(value['MainPID'])
    require(value['ActiveState'] == 'active' and value['SubState'] == 'running' and pid > 0,
            'runtime_not_running')
    require(value['Restart'] == ('no' if unit == LIVE else 'on-failure')
            and re.fullmatch('[0-9a-f]{32}', value['InvocationID']), 'runtime_policy_changed')
    require(expected_pid is None or pid == expected_pid, 'runtime_pid_changed')
    require(argv(pid) == expected and digest(Path(expected[1 if unit == LIVE else 2])) == helper_sha,
            'runtime_command_or_helper_changed')
    require(command(unit_text(unit)) == expected, 'runtime_effective_command_changed')
    return value


def row_bytes(raw):
    require(raw.isascii(), 'registry_encoding_unsupported')
    text = raw.decode(); found = list(re.finditer(r'"jobs"\s*:\s*\[', text))
    require(len(found) == 1, 'registry_jobs_not_unique')
    at = found[0].end(); decoder = json.JSONDecoder(); result = {}
    while True:
        while text[at].isspace() or text[at] == ',':
            at += 1
        if text[at] == ']':
            return result
        start = at; row, at = decoder.raw_decode(text, at)
        require(row['unit'] not in result, 'registry_duplicate_unit')
        result[row['unit']] = text[start:at].encode()


def transition(raw, helper, helper_sha):
    old = strict_json(raw); new = copy.deepcopy(old)
    require(set(old) == {'schema', 'root', 'jobs'} and old['root'] == str(ROOT)
            and old['schema'] == 'blockzilla-storage-jobs-registry-v1' and len(old['jobs']) == 3,
            'registry_scope_changed')
    row = next(j for j in new['jobs'] if j['unit'] == LIVE)
    before = next(j for j in old['jobs'] if j['unit'] == LIVE)
    require(row['helper'] == str(OLD_RELEASE / 'cold_storage.py') and row['helper_sha256'] == OLD_COLD_SHA
            and row['max_archived_banks'] is None, 'old_live_row_changed')
    args = row['expected_command'].copy()
    require(args[:3] == ['/usr/bin/python3', row['helper'], 'archive']
            and command_sha(args) == row['expected_command_sha256'], 'old_registry_command_invalid')
    args[1] = helper
    row.update(helper=helper, helper_sha256=helper_sha, expected_command=args,
               expected_command_sha256=command_sha(args))
    proposed = raw
    for oldvalue, newvalue, count in ((before['helper'], helper, 2),
            (before['helper_sha256'], helper_sha, 1),
            (before['expected_command_sha256'], row['expected_command_sha256'], 1)):
        oldtoken, newtoken = json.dumps(oldvalue).encode(), json.dumps(newvalue).encode()
        require(proposed.count(oldtoken) == count, 'registry_token_count_changed')
        proposed = proposed.replace(oldtoken, newtoken)
    require(strict_json(proposed) == new, 'registry_transform_mismatch')
    oldparts, newparts = row_bytes(raw), row_bytes(proposed)
    require(set(oldparts) == set(newparts) == set(FINITE) | {LIVE}
            and all(oldparts[u] == newparts[u] for u in FINITE), 'finite_row_bytes_changed')
    return old, new, proposed


def archive(row):
    path = Path(row['output']) / 'cold-storage/archive.sqlite'
    require(path.resolve(strict=True) == path and stat.S_ISREG(path.lstat().st_mode), 'archive_path_changed')
    with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=1)) as db:
        db.execute('PRAGMA query_only=ON'); db.execute('BEGIN')
        deadline = time.monotonic() + 20
        db.set_progress_handler(lambda: time.monotonic() > deadline, 10000)
        metadata = strict_json(db.execute('SELECT value FROM meta WHERE id=1').fetchone()[0])
        require(metadata == {'source': row['source'], 'namespace': row['namespace']}, 'archive_binding_changed')
        require('archives_retired' in {r[1] for r in db.execute('PRAGMA index_list(archives)')}, 'archive_count_index_missing')
        count = db.execute('SELECT count(*) FROM archives INDEXED BY archives_retired').fetchone()[0]
        retired = db.execute('SELECT count(*) FROM archives INDEXED BY archives_retired WHERE retired=1').fetchone()[0]
        maximum = db.execute('SELECT coalesce(max(rowid),0) FROM archives').fetchone()[0]
        progress = db.execute('SELECT value FROM meta WHERE id=2').fetchone()
    return dict(archived_banks=count, retired_banks=retired, max_archive_rowid=maximum,
                last_archive_progress_unix_ms=int(progress[0]) if progress else None, metadata=metadata)


def finite_check(registry):
    rows = {j['unit']: j for j in registry['jobs'] if j['unit'] != LIVE}
    require(set(rows) == set(FINITE), 'finite_scope_changed')
    for unit, cap in FINITE.items():
        row = rows[unit]; value = show(unit)
        require(row['max_archived_banks'] == cap and row['helper_sha256'] == FINITE_SHA
                and digest(Path(row['helper'])) == FINITE_SHA and command(unit_text(unit)) == row['expected_command'],
                'finite_command_or_cap_changed')
        require(value['MainPID'] == '0' and value['ActiveState'] == 'inactive' and value['Result'] == 'success',
                'finite_job_not_complete')
        observed = archive(row); status = obj(Path(row['output']) / 'cold-storage/STATUS.json')
        require(observed['archived_banks'] == observed['retired_banks'] == cap
                and status['state'] == 'COMPLETE' and status['archived_banks'] == status['retired_banks'] == cap
                and status['pending_bundles'] == 0 and status['max_archived_banks'] == cap,
                'finite_completion_changed')


def release_check(args):
    require(re.fullmatch('[0-9a-f]{64}', args.build_sha256)
            and re.fullmatch('[0-9a-f]{64}', args.manifest_sha256), 'release_hash_argument_invalid')
    body = read(args.build); require(sha(body) == args.build_sha256, 'build_hash_changed')
    build = strict_json(body); release = Path(build['release'])
    require(build['schema'] == 'blockzilla-storage-recovery-build-v1' and build['state'] == 'READY_NOT_ACTIVE'
            and build['tests_passed'] is True and type(build['tests_count']) is int and build['tests_count'] >= 64
            and type(build['tests_failed']) is int and build['tests_failed'] == 0
            and build['required_tests'] == REQUIRED_TESTS, 'build_not_tested')
    require(release.parent == ROOT / 'releases' and release.resolve(strict=True) == release
            and not release.stat().st_mode & 0o222, 'release_path_or_mode_invalid')
    require(digest(release / 'MANIFEST.json') == args.manifest_sha256, 'manifest_hash_changed')
    manifest = obj(release / 'MANIFEST.json')
    require(manifest['schema'] == 'blockzilla-storage-recovery-release-v1', 'manifest_schema_invalid')
    require({'cold_storage.py', 'test_cold_storage.py', 'storage_jobs.py', 'test_storage_jobs.py', 'BUILD.json'}
            <= set(manifest['files']), 'release_files_missing')
    for name, entry in manifest['files'].items():
        require(Path(name).name == name and name not in ('.', '..'), 'manifest_path_invalid')
        path = release / name
        require(not path.stat().st_mode & 0o222 and path.stat().st_size == entry['bytes']
                and digest(path) == entry['sha256'], 'release_file_changed')
    require(read(release / 'BUILD.json') == body and digest(release / 'cold_storage.py') == build['cold_sha256'] == COLD_SHA
            and digest(release / 'test_cold_storage.py') == build['test_cold_sha256'] == TEST_COLD_SHA
            and digest(release / 'storage_jobs.py') == build['guardian_sha256'] == GUARDIAN_SHA
            and digest(release / 'test_storage_jobs.py') == build['test_guardian_sha256'] == TEST_GUARDIAN_SHA
            and build['cold_sha256'] != OLD_COLD_SHA, 'release_source_binding_invalid')
    return build


def prepare(args):
    require(not (REPORT / 'PLAN.json').exists() and not (REPORT / 'INSTALLATION.json').exists(), 'already_prepared')
    build = release_check(args); raw = read(REGISTRY); require(sha(raw) == OLD_REGISTRY_SHA, 'old_registry_changed')
    source = authority(raw); old, new, newraw = transition(raw, str(Path(build['release']) / 'cold_storage.py'), build['cold_sha256'])
    finite_check(old)
    live = next(j for j in old['jobs'] if j['unit'] == LIVE); newrow = next(j for j in new['jobs'] if j['unit'] == LIVE)
    require(live['source'] == source, 'live_source_changed')
    guardian_text = unit_text(GUARDIAN); guardian = command(guardian_text)
    require(guardian[:3] == ['/usr/bin/python3', '-B', str(OLD_RELEASE / 'storage_jobs.py')], 'guardian_command_changed')
    validator = module(Path(guardian[2])); validator.validate_job(ROOT, newrow)
    oldstate = check_process(LIVE, live['expected_command'], OLD_COLD_SHA)
    guardianstate = check_process(GUARDIAN, guardian, GUARDIAN_SHA)
    full = unit_text(LIVE); override = run('sudo', 'cat', '--', str(LIVE_OVERRIDE), timeout=15)
    require(command(full) == command(override) == live['expected_command'] and full.count(override) == 1,
            'live_override_binding_changed')
    require(run('sudo', 'stat', '-c', '%a', str(LIVE_OVERRIDE)).strip() == b'644', 'live_override_mode_changed')
    proposed = replace_helper(override, live['expected_command'], newrow['helper'])
    files = {'registry-before.json': raw, 'registry-after.json': newraw, 'live-before.conf': override,
             'live-after.conf': proposed, 'live-before.unit': full,
             'live-after.unit': full.replace(override, proposed), 'guardian-before.unit': guardian_text}
    for name, body in files.items():
        save(REPORT / name, body, create=True)
    plan = dict(schema='blockzilla-storage-recovery-plan-v1', state='PREPARED', prepared_unix_ms=now(),
        build=str(args.build), build_sha256=args.build_sha256, manifest_sha256=args.manifest_sha256,
        release=build['release'], cold_sha256=build['cold_sha256'], source=source, source_pin_sha256=PIN_SHA,
        protected_pids=PIDS, registry_before_sha256=OLD_REGISTRY_SHA, registry_after_sha256=sha(newraw),
        old_live_row=live, new_live_row=newrow, old_live_command=live['expected_command'], live_command=newrow['expected_command'],
        guardian_command=guardian, guardian_sha256=GUARDIAN_SHA, old_live_state=oldstate, old_guardian_state=guardianstate,
        prepare_observed_live=archive(live), files={n: sha(b) for n, b in files.items()},
        live_override=str(LIVE_OVERRIDE), finite_rows_byte_identical=True, source_changed=False, bank_caps_changed=False)
    save(REPORT / 'PLAN.json', plan, create=True)
    print(json.dumps({'state': 'PREPARED', 'plan_sha256': digest(REPORT / 'PLAN.json'), 'report': str(REPORT)}))


def install_unit(path):
    run('sudo', 'install', '-m', '0644', str(path), str(LIVE_OVERRIDE))


def registry_install(body):
    info = REGISTRY.lstat()
    require(stat.S_ISREG(info.st_mode) and info.st_uid == os.geteuid() and not info.st_mode & 0o022,
            'registry_owner_or_mode_changed')
    mode = info.st_mode & 0o777
    save(REGISTRY, body); REGISTRY.chmod(mode)


def wait_running(unit, args, helper_sha, seconds=45):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            return check_process(unit, args, helper_sha)
        except (RuntimeError, FileNotFoundError, ProcessLookupError):
            time.sleep(.25)
    raise RuntimeError('runtime_start_deadline')


def unchanged_units(plan, new):
    require(unit_text(GUARDIAN) == read(REPORT / 'guardian-before.unit'), 'guardian_unit_changed')
    expected = read(REPORT / ('live-after.unit' if new else 'live-before.unit'))
    require(unit_text(LIVE) == expected, 'live_full_unit_changed')


def progress(plan, row, live, guardian, registry_bytes, started, seconds=600):
    # Floor follows verification of the new process, so old in-flight work cannot count.
    baseline = archive(row); sampled = now(); deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        authority(registry_bytes)
        current = check_process(LIVE, row['expected_command'], row['helper_sha256'], int(live['MainPID']))
        require(current['InvocationID'] == live['InvocationID'], 'live_generation_changed')
        guard = check_process(GUARDIAN, plan['guardian_command'], GUARDIAN_SHA, int(guardian['MainPID']))
        require(guard['InvocationID'] == guardian['InvocationID'], 'guardian_generation_changed')
        value = obj(Path(row['output']) / 'cold-storage/STATUS.json'); jobs = obj(ROOT / 'reports/storage-jobs/STATUS.json')
        if (value.get('state') == 'RUNNING' and type(value.get('updated_unix_ms')) is int
                and started <= value['updated_unix_ms'] <= now() and now() - value['updated_unix_ms'] <= 90000
                and jobs.get('state') == 'HEALTHY' and not jobs.get('issues')
                and jobs.get('registry_sha256') == sha(registry_bytes) and jobs.get('active_source') == plan['source']
                and type(jobs.get('updated_unix_ms')) is int and started <= jobs['updated_unix_ms'] <= now()
                and now() - jobs['updated_unix_ms'] <= 90000):
            after = archive(row)
            require(after['archived_banks'] >= baseline['archived_banks']
                    and after['retired_banks'] >= baseline['retired_banks'], 'archive_counter_regressed')
            if (after['archived_banks'] > baseline['archived_banks']
                    and after['retired_banks'] > baseline['retired_banks']
                    and after['max_archive_rowid'] > baseline['max_archive_rowid']
                    and after['last_archive_progress_unix_ms'] > sampled):
                authority(registry_bytes)
                final_live = check_process(LIVE, row['expected_command'], row['helper_sha256'], int(live['MainPID']))
                final_guardian = check_process(GUARDIAN, plan['guardian_command'], GUARDIAN_SHA, int(guardian['MainPID']))
                require(final_live['InvocationID'] == live['InvocationID']
                        and final_guardian['InvocationID'] == guardian['InvocationID'], 'runtime_generation_changed_after_read')
                return dict(post_restart_baseline=baseline, post_restart_baseline_unix_ms=sampled,
                            post_progress=after, storage_jobs=jobs, live_status=value)
        time.sleep(1)
    raise RuntimeError('new_process_progress_deadline')


def rollback_failure_guard(row, started):
    path = Path(row['output']) / 'cold-storage/LAST-FAILURE.json'
    if not path.exists():
        return
    failure = obj(path)
    require(isinstance(failure, dict) and failure.get('source') == row['source']
            and type(failure.get('updated_unix_ms')) is int and 0 <= failure['updated_unix_ms'] <= now() + 5000,
            'rollback_failure_evidence_invalid')
    require(failure.get('recovery_state_persisted') is not False, 'recovery_latch_persistence_uncertain')
    if failure['updated_unix_ms'] < started:
        return
    kind, status, network = failure.get('error_type'), failure.get('http_status'), failure.get('network_error_type')
    require(failure.get('state') == 'FAILED' and isinstance(kind, str)
            and re.fullmatch('[A-Za-z_][A-Za-z0-9_]{0,63}', kind)
            and (status is None or type(status) is int and 100 <= status <= 599)
            and network in (None, 'ProtocolError') and (network is None or kind == 'ResponseStreamingError'),
            'rollback_failure_evidence_invalid')
    transient = (kind in {'BundleTransferTimeout', 'ReadTimeoutError', 'ConnectTimeoutError',
                         'ConnectionClosedError', 'EndpointConnectionError'}
                 or kind == 'ResponseStreamingError' and network == 'ProtocolError'
                 or kind == 'ClientError' and status in {408, 429, 500, 502, 503, 504})
    require(transient, 'fresh_permanent_failure_blocks_old_helper')


def rollback_allowed(row, started):
    # Never initialize, mutate or rearm meta id3/id4.
    require(LEDGER_MARKER_SCHEMA is not None, 'ledger_contract_not_frozen')
    path = Path(row['output']) / 'cold-storage/archive.sqlite'
    require(path.resolve(strict=True) == path and stat.S_ISREG(path.lstat().st_mode), 'recovery_database_path_changed')
    rollback_failure_guard(row, started)
    with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=1)) as db:
        db.execute('PRAGMA query_only=ON'); db.execute('BEGIN')
        found = list(db.execute('SELECT id,CASE WHEN length(CAST(value AS BLOB))<=16384 THEN value ELSE NULL END '
                                'FROM meta WHERE id IN (3,4)'))
        rows = dict(found)
        require(len(rows) == len(found), 'recovery_ledger_duplicate_rows')
    if not rows:
        return {'state': 'NOT_INITIALIZED', 'rows_preserved': True}
    require(set(rows) == {3, 4} and all(isinstance(v, str) and len(v.encode()) <= 16384 for v in rows.values()),
            'recovery_ledger_incomplete')
    ledger, marker = strict_json(rows[3]), strict_json(rows[4])
    binding = {'source': row['source'], 'namespace': row['namespace'], 'max_archived_banks': row['max_archived_banks']}
    require(isinstance(ledger, dict) and marker == {'schema': LEDGER_MARKER_SCHEMA, 'binding': binding},
            'recovery_ledger_invalid')
    body = read(Path(row['helper'])); require(sha(body) == row['helper_sha256'], 'recovery_validator_changed')
    spec = importlib.util.spec_from_loader('readonly_recovery_validator', loader=None)
    cold = importlib.util.module_from_spec(spec); cold.__file__ = row['helper']
    exec(compile(body, row['helper'], 'exec'), cold.__dict__)
    require(cold.RECOVERY_SCHEMA == LEDGER_SCHEMA and cold.RECOVERY_MARKER_SCHEMA == LEDGER_MARKER_SCHEMA,
            'recovery_validator_contract_changed')
    # This reviewed method is pure: no constructor, db, writes or budget reset.
    checker = cold.RecoveryCircuit.__new__(cold.RecoveryCircuit)
    checker.value, checker.binding = ledger, binding
    checker.validate()
    require(ledger['state'] == 'NORMAL', 'recovery_budget_blocks_old_helper')
    return {'state': ledger['state'], 'ledger_sha256': sha(rows[3].encode()), 'marker_sha256': sha(rows[4].encode()), 'rows_preserved': True}


def rollback(plan, record):
    receipt = dict(schema='blockzilla-storage-recovery-rollback-v1', state='STARTED', started_unix_ms=now(),
                   source=plan['source'], protected_pids=PIDS, recovery_ledger_preserved=True)
    try:
        authority()
        oldraw, newraw = read(REPORT / 'registry-before.json'), read(REPORT / 'registry-after.json')
        def owned_configuration():
            require(read(REGISTRY) in (oldraw, newraw), 'rollback_registry_changed_externally')
            require(unit_text(GUARDIAN) == read(REPORT / 'guardian-before.unit')
                    and unit_text(LIVE) in (read(REPORT / 'live-before.unit'), read(REPORT / 'live-after.unit'))
                    and run('sudo', 'cat', '--', str(LIVE_OVERRIDE)) in
                    (read(REPORT / 'live-before.conf'), read(REPORT / 'live-after.conf')), 'rollback_unit_changed_externally')
        owned_configuration()
        run('sudo', 'systemctl', 'stop', GUARDIAN, LIVE, timeout=105)
        require(show(GUARDIAN)['MainPID'] == show(LIVE)['MainPID'] == '0', 'rollback_units_not_stopped')
        authority()
        owned_configuration()
        try:
            receipt['recovery_ledger'] = rollback_allowed(plan['new_live_row'], record['started_unix_ms'])
        except (RuntimeError, ValueError, KeyError, TypeError, sqlite3.Error, OSError) as blocked:
            receipt.update(state='BLOCKED_ROLLBACK', error_type=type(blocked).__name__, old_helper_started=False,
                           services_left_stopped=True, completed_unix_ms=now())
            if isinstance(blocked, RuntimeError) and re.fullmatch('[a-z0-9_]+', str(blocked)):
                receipt['reason_code'] = str(blocked)
            save(REPORT / 'ROLLBACK.json', receipt, create=True)
            return receipt
        authority()
        owned_configuration()
        install_unit(REPORT / 'live-before.conf'); registry_install(oldraw)
        run('sudo', 'systemctl', 'daemon-reload'); authority(oldraw); unchanged_units(plan, False)
        started = now(); run('sudo', 'systemctl', 'start', LIVE)
        live = wait_running(LIVE, plan['old_live_command'], OLD_COLD_SHA)
        run('sudo', 'systemctl', 'start', GUARDIAN)
        guardian = wait_running(GUARDIAN, plan['guardian_command'], GUARDIAN_SHA)
        proof = progress(plan, plan['old_live_row'], live, guardian, oldraw, started)
        authority(oldraw); unchanged_units(plan, False); finite_check(strict_json(oldraw))
        receipt.update(state='PREVIOUS_HELPER_RESTORED', completed_unix_ms=now(), live_pid=int(live['MainPID']),
                       guardian_pid=int(guardian['MainPID']), registry_sha256=sha(oldraw), proof=proof)
    except BaseException as error:
        receipt.update(state='ROLLBACK_FAILED', error_type=type(error).__name__, completed_unix_ms=now())
    save(REPORT / 'ROLLBACK.json', receipt, create=True)
    return receipt


def apply(args):
    require(LEDGER_MARKER_SCHEMA is not None, 'ledger_contract_not_frozen')
    build = release_check(args); plan = obj(REPORT / 'PLAN.json')
    require(re.fullmatch('[0-9a-f]{64}', args.plan_sha256 or '')
            and digest(REPORT / 'PLAN.json') == args.plan_sha256, 'prepared_plan_hash_changed')
    require(plan['state'] == 'PREPARED' and not (REPORT / 'INSTALLATION.json').exists()
            and plan['build'] == str(args.build) and plan['build_sha256'] == args.build_sha256
            and plan['manifest_sha256'] == args.manifest_sha256 and plan['release'] == build['release'], 'prepared_plan_binding_changed')
    for name, value in plan['files'].items():
        require(Path(name).name == name and digest(REPORT / name) == value, 'prepared_file_changed')
    oldraw, newraw = read(REPORT / 'registry-before.json'), read(REPORT / 'registry-after.json')
    old, new, derived = transition(oldraw, str(Path(build['release']) / 'cold_storage.py'), build['cold_sha256'])
    oldrow = next(j for j in old['jobs'] if j['unit'] == LIVE)
    newrow = next(j for j in new['jobs'] if j['unit'] == LIVE)
    require(sha(oldraw) == OLD_REGISTRY_SHA == plan['registry_before_sha256'] and derived == newraw
            and sha(newraw) == plan['registry_after_sha256'], 'prepared_registry_transition_changed')
    require(plan['old_live_row'] == oldrow and plan['new_live_row'] == newrow
            and plan['old_live_command'] == oldrow['expected_command']
            and plan['live_command'] == newrow['expected_command'] and plan['cold_sha256'] == build['cold_sha256'],
            'prepared_row_or_command_changed')
    old_override, new_override = read(REPORT / 'live-before.conf'), read(REPORT / 'live-after.conf')
    old_unit = read(REPORT / 'live-before.unit')
    require(old_unit.count(old_override) == 1 and command(old_unit) == oldrow['expected_command']
            and new_override == replace_helper(old_override, oldrow['expected_command'], newrow['helper'])
            and read(REPORT / 'live-after.unit') == old_unit.replace(old_override, new_override),
            'prepared_unit_transform_changed')
    source = authority(oldraw); require(source == plan['source'] and plan['protected_pids'] == PIDS, 'prepared_authority_changed')
    finite_check(old); unchanged_units(plan, False)
    for unit, args_before, helper_sha, state in [(LIVE, plan['old_live_command'], OLD_COLD_SHA, plan['old_live_state']),
            (GUARDIAN, plan['guardian_command'], GUARDIAN_SHA, plan['old_guardian_state'])]:
        observed = check_process(unit, args_before, helper_sha, int(state['MainPID']))
        require(observed['InvocationID'] == state['InvocationID'], 'prepared_service_generation_changed')
    record = dict(schema='blockzilla-storage-recovery-install-v1', state='ACTIVATING', started_unix_ms=now(),
        plan_sha256=digest(REPORT / 'PLAN.json'), build_sha256=args.build_sha256, manifest_sha256=args.manifest_sha256,
        source=source, protected_pids=PIDS, release=build['release'], cold_sha256=build['cold_sha256'],
        registry_before_sha256=OLD_REGISTRY_SHA, registry_after_sha256=sha(newraw), old_live_row=plan['old_live_row'],
        new_live_row=plan['new_live_row'], live_command=plan['live_command'], guardian_command=plan['guardian_command'],
        guardian_sha256=GUARDIAN_SHA, finite_rows_byte_identical=True, bank_caps_changed=False,
        source_changed=False, validator_restarted=False, recovery_ledger_preserved=True, service_actions=[])
    save(REPORT / 'INSTALLATION.json', record, create=True)
    touched = False
    try:
        authority(oldraw); unchanged_units(plan, False)
        touched = True
        record['phase'] = 'STOPPING_GUARDIAN'; save(REPORT / 'INSTALLATION.json', record)
        run('sudo', 'systemctl', 'stop', GUARDIAN, timeout=105); record['service_actions'].append(['stop', GUARDIAN])
        record['phase'] = 'STOPPING_LIVE'; save(REPORT / 'INSTALLATION.json', record)
        run('sudo', 'systemctl', 'stop', LIVE, timeout=105); record['service_actions'].append(['stop', LIVE])
        save(REPORT / 'INSTALLATION.json', record)
        require(show(GUARDIAN)['MainPID'] == show(LIVE)['MainPID'] == '0', 'services_not_stopped')
        authority(oldraw); unchanged_units(plan, False)
        before = dict(schema='blockzilla-storage-recovery-before-v1', sampled_unix_ms=now(), source=source,
            source_pin_sha256=PIN_SHA, protected_pids=PIDS, registry_before_sha256=OLD_REGISTRY_SHA,
            live_before=archive(plan['old_live_row']), old_live_row=plan['old_live_row'],
            old_live_pid=int(plan['old_live_state']['MainPID']), old_live_invocation=plan['old_live_state']['InvocationID'])
        save(REPORT / 'BEFORE.json', before, create=True)
        record.update(before_sha256=digest(REPORT / 'BEFORE.json'), live_before=before['live_before'])
        authority(oldraw); install_unit(REPORT / 'live-after.conf'); registry_install(newraw)
        run('sudo', 'systemctl', 'daemon-reload'); authority(newraw); unchanged_units(plan, True)
        record.update(state='CONFIGURED', configured_unix_ms=now()); save(REPORT / 'INSTALLATION.json', record)
        started = now(); run('sudo', 'systemctl', 'start', LIVE); record['service_actions'].append(['start', LIVE])
        record['phase'] = 'STARTING_LIVE'; save(REPORT / 'INSTALLATION.json', record)
        live = wait_running(LIVE, plan['live_command'], build['cold_sha256'])
        run('sudo', 'systemctl', 'start', GUARDIAN); record['service_actions'].append(['start', GUARDIAN])
        record['phase'] = 'VERIFYING_PROGRESS'; save(REPORT / 'INSTALLATION.json', record)
        guardian = wait_running(GUARDIAN, plan['guardian_command'], GUARDIAN_SHA)
        require(live['MainPID'] != plan['old_live_state']['MainPID']
                and guardian['MainPID'] != plan['old_guardian_state']['MainPID'], 'service_not_restarted')
        proof = progress(plan, plan['new_live_row'], live, guardian, newraw, started)
        authority(newraw); unchanged_units(plan, True); finite_check(new)
        record.update(state='ACTIVE_PENDING_CLOUD_PROOF', activation_started_unix_ms=started,
            phase='PENDING_INDEPENDENT_PROOF',
            completed_unix_ms=now(), live_pid=int(live['MainPID']), guardian_pid=int(guardian['MainPID']),
            live_invocation_id=live['InvocationID'], guardian_invocation_id=guardian['InvocationID'],
            observed_live_command=argv(live['MainPID']), observed_guardian_command=argv(guardian['MainPID']),
            protected_pids_unchanged=True, post_transition_cloud_proof='PENDING_INDEPENDENT_CHECK', **proof)
        save(REPORT / 'INSTALLATION.json', record)
        print(json.dumps({'state': record['state'], 'installation_sha256': digest(REPORT / 'INSTALLATION.json')}))
    except BaseException as error:
        record.update(state='ACTIVATION_FAILED', error_type=type(error).__name__, failed_unix_ms=now())
        save(REPORT / 'INSTALLATION.json', record)
        if touched:
            result = rollback(plan, record)
            record.update(rollback_state=result['state'], rollback_sha256=digest(REPORT / 'ROLLBACK.json'))
            save(REPORT / 'INSTALLATION.json', record)
        raise RuntimeError('storage_circuit_activation_failed_inspect_receipts') from None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build', type=Path, required=True)
    parser.add_argument('--build-sha256', required=True)
    parser.add_argument('--manifest-sha256', required=True)
    parser.add_argument('--plan-sha256', help='Exact prepare output; required by --apply.')
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    require(LEDGER_MARKER_SCHEMA is not None, 'ledger_contract_not_frozen')
    REPORT.mkdir(parents=True, exist_ok=True)
    with (REPORT / 'INSTALL.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        (apply if args.apply else prepare)(args)


if __name__ == '__main__':
    main()
