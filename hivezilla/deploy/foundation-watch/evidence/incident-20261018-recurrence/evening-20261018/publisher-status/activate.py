#!/usr/bin/env python3
"""Prepare by default. --apply replaces only the current v2 publisher executable."""
import argparse
import contextlib
import hashlib
import importlib.util
import json
import os
import re
import shlex
import signal
import stat
import subprocess
import sys
import tempfile
import time
from pathlib import Path

sys.dont_write_bytecode = True
ROOT = Path('/home/ubuntu/live/hivezilla-validator-20260930')
REPORT = ROOT / 'reports/native-recovery-20261018/recurrence/evening-20261018/publisher-status'
UNIT = 'hivezilla-bank-v2-r2-publisher.service'
OVERRIDE = Path('/etc/systemd/system') / (UNIT + '.d') / ('z' * 96 + '-native-source.conf')
OLD_SHA = '02a93aa83202d688bc42fdb9581b135a65f1e1903d179915f0b850638a7ddc48'
OLD_OVERRIDE_SHA = 'c99d9c87b87d8b880f8cb64d0f4a5c6c86263715996a38d5d65d4f436e857fc4'
OLD_UNIT_SHA = '79ba4e5b760a24834ac530b6c4575552a51ba4be8f333a58f99e5c4726eef21c'
PIN_SHA = 'ddfe097a2915a946c0917adcc135a2c7d997b1517d578044bfea80aadd7ea315'
REGISTRY_SHA = 'a52115479c26ccff6367b8439cd58dbb2354bb7b61d25691e797a96611655f6f'
VERIFY = ROOT / 'releases/incident-recovery-20261018-a6b89ac28c538eaa/verify_live.py'
VERIFY_SHA = '8d12ef74ca8047c58c8774625337995693806a17388f4fb561424a2c03b612d4'
PIDS = {'hivezilla-validator': 2142808, 'hivezilla-direct-worker': 916425,
        'hivezilla-bank-v2-producer': 917148, 'hivezilla-shred-hook-recorder': 2142799,
        'hivezilla-bank-v2-origin': 2134416, 'hivezilla-stream-origin': 2135639,
        'hivezilla-stream-relay': 2136117}
OLD_PID = 917318
PROFILE = 'native-processed-compact-v1'
PATCH_SHA = '92f2376d361a62b4e34f4671e93c1b5f70bcf1b1f95c9c8b653a3c70c94c708e'
PATCH_MANIFEST_SHA = 'e8dfe7b56cbfe332380217100dab0fa4fae9ce4e2c1ab5ae2e3d7099a4e23c5b'
DONOR_SHA = '58ca93f632098973f8353632e87415a2dc9e01a98bedf786c453ec3178fae41b'
PUBLISHER_PATH = 'hivezilla/stream-relay/src/publisher.rs'
SOURCE_CHANGE = {'before_sha256': '4b2a92f90cfe282614cea8a599e6212e55da353cd2fd2484e67f5941fed25ce6',
                 'after_sha256': 'b31fba2acf86a93dd1506cfdd0d6d0a1cea174d91fba461dc7689561943f75dd'}
REQUIRED_TESTS = ['publisher::tests::' + name for name in (
    'cancelled_object_upload_joins_without_starting_manifest_or_head',
    'final_status_write_failure_after_remote_head_does_not_commit',
    'initial_status_write_failure_starts_no_remote_work',
    'remote_failure_joins_before_failed_status_and_keeps_intent',
    'slow_batch_log_is_bounded_and_only_reports_completed_slow_batches',
    'status_write_failure_during_head_readback_does_not_commit',
    'upload_status_is_live_during_blocked_objects_without_advancing_checkpoint',
    'upload_status_is_live_during_head_readback_without_advancing_checkpoint')]


def require(ok, code):
    if not ok:
        raise RuntimeError(code)


def sha(body):
    return hashlib.sha256(body).hexdigest()


def read(path, limit=2 ** 20):
    require(stat.S_ISREG(path.lstat().st_mode), 'file_not_regular')
    with path.open('rb') as f:
        body = f.read(limit + 1)
    require(len(body) <= limit, 'file_too_large')
    return body


def digest(path):
    require(stat.S_ISREG(path.lstat().st_mode), 'file_not_regular')
    with path.open('rb') as f:
        return hashlib.file_digest(f, 'sha256').hexdigest()


def obj(path):
    return json.loads(read(path))


def now():
    return time.time_ns() // 1000000


def save(path, value, create=False):
    data = value if isinstance(value, bytes) else (json.dumps(value, indent=2, sort_keys=True) + '\n').encode()
    path.parent.mkdir(parents=True, exist_ok=True)
    if create:
        with path.open('xb') as f:
            os.chmod(path, 0o600)
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
    else:
        with tempfile.NamedTemporaryFile(dir=path.parent, prefix=path.name + '.', delete=False) as f:
            temp = Path(f.name)
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp, path)
    fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def run(*args, timeout=75):
    # Never echo a command failure: unit text can contain local configuration.
    p = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=timeout)
    require(p.returncode == 0, 'command_failed')
    return p.stdout


def unit_text():
    return run('sudo', 'systemctl', 'cat', UNIT, timeout=15)


def command(body):
    values = []
    for line in body.decode().splitlines():
        if line == 'ExecStart=':
            values = []
        elif line.startswith('ExecStart='):
            require(not line.endswith('\\'), 'continued_execstart_unsupported')
            values.append(shlex.split(line[len('ExecStart='):]))
    require(len(values) == 1, 'effective_execstart_not_unique')
    args = values[0]
    require(all(not any(x in a for x in ('$', '%', '\n', '\r', '\0')) for a in args),
            'expanded_execstart_unsupported')
    return args


def replacement(body, old, new):
    require(command(body) == old, 'override_command_mismatch')
    require(re.fullmatch(r'/[A-Za-z0-9_./-]+', new), 'new_binary_path_invalid')
    lines = body.splitlines(keepends=True)
    indexes = [i for i, line in enumerate(lines) if line.startswith(b'ExecStart=') and line.strip() != b'ExecStart=']
    require(len(indexes) == 1, 'override_execstart_not_unique')
    at = indexes[0]
    # Retain every byte after argv[0], including all original quoting and flags.
    match = re.match(rb'(ExecStart=\s*)("[^"\n]+"|\'[^\'\n]+\'|[^\s]+)(.*)', lines[at], re.DOTALL)
    require(match is not None and shlex.split(match[2].decode()) == old[:1], 'old_binary_token_invalid')
    lines[at] = match[1] + new.encode() + match[3]
    proposed = b''.join(lines)
    require(command(proposed) == [new, *old[1:]], 'proposed_command_changed_arguments')
    return proposed


def show(unit):
    raw = run('systemctl', 'show', unit, '-p', 'MainPID,ActiveState,SubState,Result,Restart,InvocationID').decode()
    return dict(line.split('=', 1) for line in raw.splitlines() if '=' in line)


def argv(pid):
    return (Path('/proc') / str(pid) / 'cmdline').read_bytes().decode().rstrip('\0').split('\0')


def authority():
    require(sha(read(ROOT / 'config/native-active-source.json')) == PIN_SHA, 'source_pin_changed')
    require(sha(read(ROOT / 'config/storage-jobs.json')) == REGISTRY_SHA, 'registry_changed')
    for unit, expected in PIDS.items():
        require(int(show(unit)['MainPID']) == expected, 'protected_pid_changed')
    return obj(ROOT / 'config/native-active-source.json')


def checked_process(args, expected_sha, expected_pid=None):
    value = show(UNIT)
    pid = int(value['MainPID'])
    require(value['ActiveState'] == 'active' and value['SubState'] == 'running' and pid > 0,
            'publisher_process_unavailable')
    require(expected_pid is None or pid == expected_pid, 'publisher_pid_changed')
    require(argv(pid) == args, 'publisher_argv_changed')
    # /proc/PID/exe is intentionally followed to check the executed bytes.
    with (Path('/proc') / str(pid) / 'exe').open('rb') as f:
        actual = hashlib.file_digest(f, 'sha256').hexdigest()
    require(actual == expected_sha, 'publisher_executed_binary_mismatch')
    return pid


def option(args, flag):
    require(args.count(flag) == 1 and args.index(flag) + 1 < len(args), 'publisher_option_invalid')
    return args[args.index(flag) + 1]


def checkpoint(args, source):
    state = Path(option(args, '--state'))
    require(state == ROOT / 'reports/native-processed-r2-bank-v2' / source['session_id'], 'publisher_state_path_changed')
    cp = obj(state / 'CHECKPOINT.json')
    require(cp['schema'] == 'blockzilla-native-publisher-checkpoint-v1'
            and cp['source'] == str(ROOT / 'compact/direct-agave-processed-live' / source['session_id'])
            and cp['session'] == source['session_id']
            and cp['prefix'] == option(args, '--prefix') + '/' + source['session_id']
            and '--processed' in args and cp['sequence'] > 0
            and re.fullmatch(re.escape(source['session_id']) + r':p[0-9]+:(?:[0-9a-f]{64}|-)', cp['cursor'])
            and re.fullmatch('[0-9a-f]{64}', cp['publisher_id']), 'publisher_checkpoint_invalid')
    return cp


def same_identity(before, after):
    return all(before[k] == after[k] for k in ('schema', 'source', 'namespace', 'prefix', 'session', 'publisher_id', 'start_slot'))


def safe_checkpoint(cp):
    return {k: cp[k] for k in ('schema', 'session', 'publisher_id', 'namespace', 'start_slot', 'sequence', 'blocks', 'bytes', 'cursor')}


def release_check(path, expected):
    body = read(path)
    require(re.fullmatch('[0-9a-f]{64}', expected or '') and sha(body) == expected, 'build_hash_changed')
    value = json.loads(body)
    names = value.get('required_regression_tests')
    require(names == REQUIRED_TESTS, 'required_regressions_missing')
    require(value['schema'] == 'blockzilla-publisher-status-build-v1'
            and value['state'] == 'READY_NOT_ACTIVE' and value['tests_passed'] is True
            and type(value['tests_count']) is int and value['tests_count'] >= 44
            and value['tests_failed'] == 0 and value['patch_sha256'] == PATCH_SHA
            and value['donor_manifest_sha256'] == DONOR_SHA, 'build_tests_or_authority_invalid')
    release, binary = Path(value['release']), Path(value['binary'])
    require(release.parent == ROOT / 'releases' and release.resolve(strict=True) == release
            and binary == release / 'hivezilla-stream', 'release_path_invalid')
    require(not release.stat().st_mode & 0o222 and not binary.stat().st_mode & 0o222,
            'release_writable')
    require(digest(binary) == value['binary_sha256'] and value['binary_sha256'] != OLD_SHA, 'new_binary_hash_changed')
    manifest = obj(release / 'MANIFEST.json')
    require(digest(release / 'MANIFEST.json') == value['release_manifest_sha256']
            and manifest['schema'] == 'blockzilla-publisher-status-release-v1', 'release_manifest_changed')
    required = {'hivezilla-stream', 'SOURCE.json', 'BUILD-PROVENANCE.json', 'PATCH-MANIFEST.json',
                'publisher-status.patch', 'GRAPH.json', 'SOURCE-BEFORE.json'}
    require(required <= set(manifest['files']), 'release_provenance_missing')
    for name, entry in manifest['files'].items():
        require(Path(name).name == name and name not in ('.', '..'), 'manifest_path_invalid')
        p = release / name
        require(not p.stat().st_mode & 0o222 and p.stat().st_size == entry['bytes']
                and digest(p) == entry['sha256'], 'release_file_changed')
    require(digest(release / 'SOURCE.json') == value['source_manifest_sha256']
            and digest(release / 'publisher-status.patch') == PATCH_SHA
            and digest(release / 'PATCH-MANIFEST.json') == PATCH_MANIFEST_SHA
            and digest(release / 'SOURCE-BEFORE.json') == DONOR_SHA, 'source_or_patch_changed')
    validate_graph(obj(release / 'SOURCE.json'), obj(release / 'SOURCE-BEFORE.json'), obj(release / 'GRAPH.json'))
    return value


def validate_graph(source, before, graph):
    old, new = before['files'], source['files']
    changes = {PUBLISHER_PATH: SOURCE_CHANGE}
    require(source['schema'] == 'blockzilla-publisher-status-source-v1'
            and source['donor_manifest_sha256'] == DONOR_SHA and source['patch_sha256'] == PATCH_SHA
            and source['patch_manifest_sha256'] == PATCH_MANIFEST_SHA and source['changed_files'] == changes
            and source['file_count'] == len(new) == len(old) == 963 and set(old) == set(new), 'source_graph_authority_invalid')
    require(old[PUBLISHER_PATH] == SOURCE_CHANGE['before_sha256']
            and new[PUBLISHER_PATH] == SOURCE_CHANGE['after_sha256']
            and all(new[p] == h for p, h in old.items() if p != PUBLISHER_PATH), 'unrelated_source_change')
    graph_paths = {p for p in old if any(p.startswith(d + '/src/') and p.endswith('.rs') for d in
                   ('hivezilla/stream-relay', 'hivezilla/object-store', 'crates/blockzilla-bank-stream-v2'))}
    graph_paths.update(('Cargo.toml', 'hivezilla/stream-relay/Cargo.toml', 'hivezilla/stream-relay/Cargo.lock',
                        'hivezilla/object-store/Cargo.toml', 'crates/blockzilla-bank-stream-v2/Cargo.toml'))
    require(len(graph_paths) == graph['file_count'] == 34 and set(graph['files']) == graph_paths
            and graph['changed_files'] == [PUBLISHER_PATH]
            and graph['files'] == {p: {'before_sha256': old[p], 'after_sha256': new[p]} for p in graph_paths},
            'dependency_graph_changed')


def proof_process(request, timeout=45, executable=None):
    command_args = executable or [sys.executable, str(Path(__file__).resolve()), '_cloud']
    def output_limit():
        import resource
        resource.setrlimit(resource.RLIMIT_FSIZE, (65536, 65536))
    with tempfile.TemporaryFile() as output:
        p = subprocess.Popen(command_args, stdin=subprocess.PIPE, stdout=output,
                             stderr=subprocess.DEVNULL, start_new_session=True, preexec_fn=output_limit)
        try:
            p.communicate(json.dumps(request).encode(), timeout=timeout)
        except subprocess.TimeoutExpired:
            os.killpg(p.pid, signal.SIGKILL)
            p.wait(timeout=5)
            raise RuntimeError('publisher_cloud_deadline') from None
        output.seek(0)
        data = output.read(4097)
        require(0 < len(data) <= 4096, 'publisher_cloud_child_failed')
        result = json.loads(data)
        if p.returncode != 0:
            code = result.get('error_type') if isinstance(result, dict) else None
            require(isinstance(code, str) and re.fullmatch('[A-Za-z_][A-Za-z0-9_]{0,63}', code),
                    'publisher_cloud_child_failed')
            http = result.get('http_status')
            suffix = '_' + str(http) if type(http) is int and 100 <= http <= 599 else ''
            raise RuntimeError('publisher_cloud_' + code.lower() + suffix)
        require(result.get('state') == 'VERIFIED' and result.get('schema') == 'blockzilla-publisher-cloud-proof-v1',
                'publisher_cloud_proof_failed')
        return result


def validate_cloud(head, commit, cp, source):
    sequence = head.get('sequence')
    require(type(sequence) is int and sequence >= cp['sequence'], 'remote_head_behind_before_checkpoint')
    require(head.get('schema') == 'blockzilla-native-stream-catalog-v1'
            and head.get('profile') == PROFILE and head.get('session') == source['session_id']
            and head.get('start_slot') == cp['start_slot']
            and head.get('canonical') is False and head.get('publication_ready') is False,
            'remote_head_identity_invalid')
    key = cp['prefix'] + f'/commits/{sequence:020}.json'
    require(head.get('last_commit') == key, 'remote_commit_path_invalid')
    require(commit.get('schema') == 'blockzilla-native-stream-commit-v1'
            and commit.get('profile') == PROFILE and commit.get('session') == source['session_id']
            and commit.get('sequence') == sequence and commit.get('cursor') == head.get('latest_cursor')
            and re.fullmatch(re.escape(source['session_id']) + r':p[0-9]+:[0-9a-f]{64}', commit.get('cursor', ''))
            and all(commit.get(k) == source[k] for k in ('genesis_hash', 'node_identity', 'agave_revision'))
            and commit.get('canonical') is False and commit.get('publication_ready') is False,
            'remote_commit_identity_invalid')
    require(commit.get('object', '').startswith(cp['prefix'] + '/chunks/')
            and re.fullmatch('[0-9a-f]{64}', commit.get('sha256', ''))
            and type(commit.get('bytes')) is int and 0 < commit['bytes'] <= 16 + 65536 + 16 * 2 ** 20,
            'remote_commit_object_invalid')
    return key


def cloud_child():
    # The child has no write operations and emits no provider values or exception text.
    import resource
    resource.setrlimit(resource.RLIMIT_FSIZE, (65536, 65536))
    request = json.loads(sys.stdin.buffer.read(262145))
    cp, source = request['checkpoint'], request['source']
    values = {}
    for line in Path(request['credentials']).read_text().splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        line = line.removeprefix('export ')
        key, val = line.split('=', 1)
        parts = shlex.split(val)
        require(len(parts) == 1 and key not in values, 'credentials_invalid')
        values[key] = parts[0]
    endpoint = values.get('R2_S3_ENDPOINT', values.get('R2_ENDPOINT', values.get('AWS_ENDPOINT_URL_S3')))
    bucket = values.get('R2_BUCKET')
    access = values.get('R2_ACCESS_KEY_ID', values.get('AWS_ACCESS_KEY_ID'))
    secret = values.get('R2_SECRET_ACCESS_KEY', values.get('AWS_SECRET_ACCESS_KEY'))
    prefix = cp['prefix'].removesuffix('/' + source['session_id'])
    require(isinstance(endpoint, str) and endpoint.startswith('https://') and bucket and access and secret
            and sha((endpoint + '\0' + bucket + '\0' + prefix).encode()) == cp['namespace'], 'remote_namespace_invalid')
    with open(os.devnull, 'w') as null, contextlib.redirect_stdout(null), contextlib.redirect_stderr(null):
        import boto3
        from botocore.config import Config
        client = boto3.client('s3', endpoint_url=endpoint,
            region_name=values.get('R2_S3_REGION', values.get('R2_REGION', 'auto')),
            aws_access_key_id=access, aws_secret_access_key=secret,
            config=Config(connect_timeout=5, read_timeout=10, retries={'total_max_attempts': 2},
                request_checksum_calculation='when_required', response_checksum_validation='when_required'))
        def get(key):
            response = client.get_object(Bucket=bucket, Key=key)
            with response['Body'] as body:
                data = body.read(65537)
            require(response['ResponseMetadata']['HTTPStatusCode'] == 200
                    and len(data) <= 65536 and len(data) == response['ContentLength'], 'remote_object_size_invalid')
            return data, json.loads(data)
        hb, head = get(cp['prefix'] + '/HEAD.json')
        seq = head.get('sequence')
        require(type(seq) is int and seq >= cp['sequence'], 'remote_sequence_invalid')
        key = cp['prefix'] + f'/commits/{seq:020}.json'
        require(head.get('last_commit') == key, 'remote_commit_path_invalid')
        cb, commit = get(key)
        validate_cloud(head, commit, cp, source)
    print(json.dumps({'schema': 'blockzilla-publisher-cloud-proof-v1', 'state': 'VERIFIED',
        'sampled_unix_ms': now(), 'namespace': cp['namespace'], 'session': source['session_id'],
        'sequence': seq, 'cursor': head['latest_cursor'], 'head_sha256': sha(hb),
        'commit_sha256': sha(cb), 'head_bytes': len(hb), 'commit_bytes': len(cb),
        'head_key_sha256': sha((cp['prefix'] + '/HEAD.json').encode()),
        'commit_key_sha256': sha(key.encode()), 'source_verified': True, 'full_gets': 2}))


def verifier():
    require(digest(VERIFY) == VERIFY_SHA, 'verifier_hash_changed')
    spec = importlib.util.spec_from_file_location('publisher_local_verifier', VERIFY)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def fresh_flow(args, binary_sha, identity, since, started, source, deadline):
    v = verifier()
    last_code = 'publisher_not_ready'
    floor, floor_pid, floor_time = None, None, None
    while time.monotonic() < deadline:
        authority()
        try:
            pid = checked_process(args, binary_sha)
            cp = checkpoint(args, source)
            require(same_identity(identity, cp), 'publisher_checkpoint_identity_changed')
            if floor is None:
                # Old publication can advance during install. Credit only work after
                # the new executed bytes and PID have been verified.
                floor, floor_pid, floor_time = cp, pid, now()
            require(pid == floor_pid, 'publisher_changed_during_verification')
            require(cp['sequence'] > max(since, floor['sequence']), 'publisher_checkpoint_not_advanced')
            state = Path(option(args, '--state'))
            status = obj(state / 'STATUS.json')
            require(status['state'] == 'RUNNING' and started <= status['updated_unix_ms'] <= now()
                    and now() - status['updated_unix_ms'] <= 15000, 'publisher_status_not_fresh')
            require(same_identity(identity, status['checkpoint']), 'publisher_status_identity_changed')
            local = {}
            v.publisher_sample(ROOT, source, local)
            proof = proof_process({'credentials': option(args, '--credentials-file'), 'checkpoint': cp, 'source': source})
            require(proof['namespace'] == cp['namespace'] and proof['session'] == source['session_id']
                    and proof['sequence'] >= cp['sequence'] and proof['source_verified'] is True
                    and proof['full_gets'] == 2, 'cloud_result_binding_invalid')
            # HEAD can be written just before the durable checkpoint. Wait for that exact sampled head.
            catchup = min(deadline, time.monotonic() + 20)
            while time.monotonic() < catchup:
                authority()
                after = checkpoint(args, source)
                require(same_identity(identity, after), 'publisher_identity_changed')
                if after['sequence'] >= proof['sequence']:
                    if after['sequence'] == proof['sequence']:
                        require(after['cursor'] == proof['cursor'], 'remote_cursor_mismatch')
                    checked_process(args, binary_sha, pid)
                    return {'pid': pid, 'checkpoint': safe_checkpoint(after), 'local_sample': local,
                            'post_restart_baseline': safe_checkpoint(floor),
                            'post_restart_baseline_unix_ms': floor_time,
                            'cloud': proof, 'phase': status.get('phase'),
                            'batch_started_unix_ms': status.get('batch_started_unix_ms'),
                            'in_flight': status.get('in_flight'),
                            'last_durable_progress_unix_ms': status.get('last_durable_progress_unix_ms')}
                time.sleep(.5)
            raise RuntimeError('local_checkpoint_behind_remote_proof')
        except (RuntimeError, v.CheckFailure, OSError, ValueError, KeyError) as error:
            # Only fixed local error codes survive. Provider exception text never leaves the child.
            last_code = str(error) if isinstance(error, RuntimeError) and re.fullmatch('[a-z0-9_]+', str(error)) else type(error).__name__
        time.sleep(1)
    raise RuntimeError('publisher_flow_deadline_' + last_code)


def install(body):
    temp = REPORT / 'install-override.conf'
    save(temp, body)
    run('sudo', 'install', '-m', '0644', str(temp), str(OVERRIDE))


def prepare(args):
    require(not (REPORT / 'BEFORE.json').exists() and not (REPORT / 'INSTALLATION.json').exists(), 'already_prepared')
    build = release_check(args.build, args.build_sha256)
    source = authority()
    full = unit_text()
    body = run('sudo', 'cat', str(OVERRIDE))
    require(sha(full) == OLD_UNIT_SHA and sha(body) == OLD_OVERRIDE_SHA, 'old_unit_changed')
    require(run('sudo', 'stat', '-c', '%a', str(OVERRIDE)).strip() == b'644', 'old_override_mode_changed')
    old = command(full)
    require(command(body) == old and digest(Path(old[0])) == OLD_SHA, 'old_publisher_binary_changed')
    checked_process(old, OLD_SHA, OLD_PID)
    cp = checkpoint(old, source)
    cloud_before = proof_process({'credentials': option(old, '--credentials-file'), 'checkpoint': cp, 'source': source})
    proposed = replacement(body, old, build['binary'])
    save(REPORT / 'publisher-before-override.conf', body, create=True)
    save(REPORT / 'publisher-before-unit.txt', full, create=True)
    save(REPORT / 'publisher-proposed-override.conf', proposed, create=True)
    # These local commands contain paths only, and keep all existing arguments.
    before = {'schema': 'blockzilla-publisher-status-before-v1', 'state': 'PREPARED',
        'prepared_unix_ms': now(), 'build': str(args.build), 'build_sha256': args.build_sha256,
        'source': source, 'source_pin_sha256': PIN_SHA, 'registry_sha256': REGISTRY_SHA,
        'protected_pids': PIDS, 'old_pid': OLD_PID, 'old_binary_sha256': OLD_SHA,
        'old_command': old, 'new_command': [build['binary'], *old[1:]],
        'override': str(OVERRIDE), 'old_override_sha256': sha(body),
        'old_unit_sha256': sha(full), 'proposed_override_sha256': sha(proposed),
        'checkpoint': safe_checkpoint(cp), 'cloud_before': cloud_before, 'checkpoint_identity': {k: cp[k] for k in
            ('schema', 'source', 'namespace', 'prefix', 'session', 'publisher_id', 'start_slot')}}
    save(REPORT / 'BEFORE.json', before, create=True)
    print(json.dumps({'state': 'PREPARED', 'before_sha256': digest(REPORT / 'BEFORE.json'), 'report': str(REPORT)}))


def apply(args):
    before = obj(REPORT / 'BEFORE.json')
    require(before['state'] == 'PREPARED' and not (REPORT / 'INSTALLATION.json').exists(), 'not_prepared_or_already_applied')
    require(before['build'] == str(args.build) and before['build_sha256'] == args.build_sha256, 'prepared_build_changed')
    build = release_check(args.build, args.build_sha256)
    source = authority()
    require(source == before['source'], 'prepared_source_changed')
    body = read(REPORT / 'publisher-before-override.conf')
    proposed = read(REPORT / 'publisher-proposed-override.conf')
    full_before = read(REPORT / 'publisher-before-unit.txt')
    require(sha(body) == before['old_override_sha256'] == OLD_OVERRIDE_SHA
            and sha(proposed) == before['proposed_override_sha256']
            and proposed == replacement(body, before['old_command'], build['binary']), 'prepared_override_changed')
    require(full_before.count(body) == 1, 'full_unit_override_binding_invalid')
    full_after = full_before.replace(body, proposed)
    require(unit_text() == full_before
            and sha(unit_text()) == OLD_UNIT_SHA and run('sudo', 'cat', str(OVERRIDE)) == body, 'unit_changed_after_prepare')
    checked_process(before['old_command'], OLD_SHA, OLD_PID)
    cp = checkpoint(before['old_command'], source)
    require(same_identity(before['checkpoint_identity'], cp), 'prepared_publisher_identity_changed')
    cloud_before = proof_process({'credentials': option(before['old_command'], '--credentials-file'),
                                  'checkpoint': cp, 'source': source})
    record = {'schema': 'blockzilla-publisher-status-installation-v1', 'state': 'APPLYING',
        'started_unix_ms': now(), 'before_sha256': digest(REPORT / 'BEFORE.json'),
        'source': source, 'protected_pids': PIDS, 'registry_sha256': REGISTRY_SHA,
        'build_sha256': args.build_sha256, 'old_pid': OLD_PID,
        'old_binary_sha256': OLD_SHA, 'binary_sha256': build['binary_sha256'],
        'release': build['release'], 'before_checkpoint': safe_checkpoint(cp), 'cloud_before': cloud_before,
        'source_changed': False, 'bank_limits_changed': False, 'validator_restarted': False}
    save(REPORT / 'INSTALLATION.json', record, create=True)
    touched = False
    try:
        authority()
        require(unit_text() == full_before and run('sudo', 'cat', str(OVERRIDE)) == body,
                'unit_changed_during_cloud_preflight')
        checked_process(before['old_command'], OLD_SHA, OLD_PID)
        touched = True
        install(proposed)
        run('sudo', 'systemctl', 'daemon-reload')
        loaded = unit_text()
        require(loaded == full_after and command(loaded) == before['new_command'], 'loaded_command_mismatch')
        authority()
        require(unit_text() == full_after, 'unit_changed_during_verification')
        restarted = now()
        run('sudo', 'systemctl', 'restart', UNIT)
        proof = fresh_flow(before['new_command'], build['binary_sha256'], cp, cp['sequence'],
                           restarted, source, time.monotonic() + 240)
        require(proof['pid'] != OLD_PID, 'publisher_not_restarted')
        authority()
        require(unit_text() == full_after, 'unit_changed_during_verification')
        record.update(state='ACTIVE', completed_unix_ms=now(), new_pid=proof['pid'],
                      restart_requested_unix_ms=restarted, proof=proof,
                      protected_pids_unchanged=True, publisher_id_unchanged=True)
        save(REPORT / 'INSTALLATION.json', record)
    except Exception as error:
        record.update(state='ACTIVATION_FAILED', error_type=type(error).__name__)
        if isinstance(error, RuntimeError) and re.fullmatch('[a-z0-9_]+', str(error)):
            record['error_code'] = str(error)
        save(REPORT / 'INSTALLATION.json', record)
        if touched:
            try:
                # Check scope before every restoration; do not overwrite a concurrent handoff.
                authority()
                current = run('sudo', 'cat', str(OVERRIDE))
                require(current in (body, proposed) and unit_text() in (full_before, full_after),
                        'rollback_override_changed')
                rollback_cp = checkpoint(before['old_command'], source)
                require(same_identity(cp, rollback_cp), 'rollback_publisher_identity_changed')
                authority()
                install(body)
                run('sudo', 'systemctl', 'daemon-reload')
                restored_unit = unit_text()
                require(restored_unit == full_before and command(restored_unit) == before['old_command'],
                        'rollback_command_mismatch')
                authority()
                restarted = now()
                run('sudo', 'systemctl', 'restart', UNIT)
                restored = fresh_flow(before['old_command'], OLD_SHA, cp, rollback_cp['sequence'],
                                      restarted, source, time.monotonic() + 240)
                authority()
                record.update(state='ROLLED_BACK', rollback_completed_unix_ms=now(),
                              rollback_pid=restored['pid'], rollback_proof=restored)
                save(REPORT / 'INSTALLATION.json', record)
            except Exception as rollback_error:
                record.update(state='ROLLBACK_FAILED', rollback_error_type=type(rollback_error).__name__)
                save(REPORT / 'INSTALLATION.json', record)
        raise RuntimeError('publisher_activation_failed_see_installation') from None
    print(json.dumps({'state': record['state'], 'new_pid': record['new_pid'],
                      'installation_sha256': digest(REPORT / 'INSTALLATION.json')}))


def main():
    if sys.argv[1:2] == ['_cloud']:
        try:
            cloud_child()
        except Exception as error:
            # Exception text can contain provider URLs. Keep only the Python type.
            value = {'state': 'FAILED', 'error_type': type(error).__name__}
            response = getattr(error, 'response', None)
            if isinstance(response, dict):
                code = response.get('ResponseMetadata', {}).get('HTTPStatusCode')
                if type(code) is int and 100 <= code <= 599:
                    value['http_status'] = code
            print(json.dumps(value))
            raise SystemExit(1)
        return
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build', type=Path, required=True)
    parser.add_argument('--build-sha256', required=True)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    (apply if args.apply else prepare)(args)


if __name__ == '__main__':
    main()
