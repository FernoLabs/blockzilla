#!/usr/bin/env python3
"""Read-only post-change check and independent live last-bundle cloud proof."""
import argparse
from contextlib import closing
import copy
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import selectors
import shlex
import sqlite3
import stat
import subprocess
import sys
import threading
import time
sys.dont_write_bytecode = True

ROOT = Path('/home/ubuntu/live/hivezilla-validator-20260930')
REPORTS = ROOT / 'reports/native-recovery-20261018/recurrence'
TRANSITION = REPORTS / 'transfer-deadline'
EXTRA = REPORTS / 'approved-extra-18ddd9a6'
RELEASE = ROOT / 'releases/transfer-deadline-20261018-498831996a2dc34c'
OLD_REGISTRY_SHA = 'c813c9e2accfb857eba1bf331f60af616ddfc8084b7aa2bce276ebf1fdb0c4c3'
PIN_SHA = 'ddfe097a2915a946c0917adcc135a2c7d997b1517d578044bfea80aadd7ea315'
COLD_SHA = '8a95785f2df4e2f53b414322f0fc42a8d2edd93a02f04cfca6358bb312c19800'
GUARDIAN_SHA = '567c15e41b3f21665a765fbfaa5b2c70d9a565d1280463da959d946472a3c87c'
PATCH_SHA = '498831996a2dc34c3eccc73df94b901219d81e3707038790b77c3531ae24731e'
VALIDATOR_SHA = 'd4fcf1d98da782e9b019c723c620e0be8ffd7e8a2026958cf72311abd96d069b'
SOURCE = dict(genesis_hash='5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d',
              node_identity='J7qpvCEWhdBgPdYRV3s3btWPrRVXa2qnAzgvxWTvtbrQ',
              agave_revision='87167f3906bdf5111e99ead64d8f4098b7874e33',
              session_id='000000000000000018df884b2474cf8e-20b258')
LIVE = 'hivezilla-native-cold-storage-20261012-live.service'
GUARDIAN = 'hivezilla-native-storage-jobs.service'
FINITE_CAPS = {'hivezilla-native-cold-storage-20261016-source-18de229d.service': 785546,
               'hivezilla-native-cold-storage-20261016-source-18ddd9a6.service': 308768}
NAMESPACE = '1a93d01e2c293e1f61c246747319a92fea69ab27060f050b986304c4c4715bcf'
OUTPUT = ROOT / 'compact/direct-agave-processed-live' / SOURCE['session_id']
ANCHORS = {
    'protocol': (REPORTS / 'protocol-retry/INSTALLATION.json', 'eec692609be680d0f58090383dedd7897dd5e2e35eb01a015953baf13132b475'),
    'transport': (REPORTS / 'connection-idle/INSTALLATION.json', 'a6c047dbc3e64eae965abf2215ae738e5099160502a18754c2a25257edf490f8'),
    'pid_binding': (EXTRA / 'TRANSPORT-PIDS.json', '0bc47d250141d0bdfbc6c3e521727f7f37b318191c77275a6c1ec2a1d6169f29'),
    'transport_proof': (REPORTS / 'connection-idle/SAME-CONNECTION-630S.json', '6d443c4c7edba0c6df77c005b9b74175de6fd857e045c07a29f55d16c5df816f'),
    'extra_installation': (EXTRA / 'INSTALLATION.json', 'af2b662cb508ad7d4364f4eb446c60a74619824f2a1a627afc74918bf1298306'),
    'stage': (TRANSITION / 'STAGE.json', '5007e83d3e646bc9b1591f57d0d4e6476ec98f7bf9d508e526dd3ea1ac773058'),
    # These two literals are set only after the completed c813 proofs exist.
    'old_final_status': (EXTRA / 'FINAL-STATUS.json', '0b6ce276039c9b7c623c07f0e6c8b56ef3a472560f27aa125ad5366ed2536ba9'),
    'old_cloud_proof': (EXTRA / 'CLOUD-PROOF-FINAL.json', '3568ffe1ae71ea9228f82e80d5c6b66c5c74616c9a5f23698a33e948c7aaf8e5'),
}
MAX_INPUT = 8 * 1024 * 1024
MAX_OUTPUT = 64 * 1024


class ProofError(Exception):
    pass


def require(value, code):
    if not value:
        raise ProofError(code)


def sha(value):
    return hashlib.sha256(value).hexdigest()


def strict_json(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, 'duplicate_json_field')
            result[key] = value
        return result
    return json.loads(raw, object_pairs_hook=pairs)


def read(path, limit=1024 * 1024):
    require(path.is_absolute() and path.resolve(strict=True) == path
            and stat.S_ISREG(path.lstat().st_mode), 'file_not_regular_or_path_changed')
    with path.open('rb') as stream:
        raw = stream.read(limit + 1)
    require(len(raw) <= limit, 'file_too_large')
    return raw


def object_file(path, limit=1024 * 1024):
    raw = read(path, limit)
    value = strict_json(raw)
    require(isinstance(value, dict), 'json_object_required')
    return value, raw


def anchored():
    values, raw = {}, {}
    require(all(isinstance(expected, str) and re.fullmatch('[0-9a-f]{64}', expected)
                for _, expected in ANCHORS.values()), 'canonical_completion_anchors_not_frozen')
    for name, (path, expected) in ANCHORS.items():
        require(isinstance(expected, str) and re.fullmatch('[0-9a-f]{64}', expected),
                'canonical_completion_anchors_not_frozen')
        values[name], raw[name] = object_file(path, 4 * 1024 * 1024)
        require(sha(raw[name]) == expected, 'frozen_anchor_changed')
    return values, raw


def command_sha(command):
    return sha(json.dumps(command, separators=(',', ':'), ensure_ascii=False).encode())


def show(unit):
    raw = subprocess.check_output(['systemctl', 'show', unit, '-p',
        'MainPID,ActiveState,SubState,Result,Restart,InvocationID'], text=True, timeout=10)
    return dict(line.split('=', 1) for line in raw.splitlines() if '=' in line)


def effective_command(unit):
    raw = subprocess.check_output(['sudo', 'systemctl', 'cat', unit], text=True, timeout=10)
    starts = []
    for line in raw.splitlines():
        if line == 'ExecStart=':
            starts = []
        elif line.startswith('ExecStart='):
            starts.append(shlex.split(line[10:]))
    require(len(starts) == 1, 'unit_command_not_unique')
    return starts[0]


def process(unit, command):
    service = show(unit)
    restart = {LIVE: 'no', GUARDIAN: 'on-failure'}[unit]
    require(service['ActiveState'] == 'active' and service['SubState'] == 'running'
            and int(service['MainPID']) > 0 and service['Restart'] == restart, 'service_not_running_or_policy_changed')
    args = (Path('/proc') / service['MainPID'] / 'cmdline').read_bytes().decode().rstrip('\0').split('\0')
    require(args == command == effective_command(unit), 'service_command_changed')
    require(re.fullmatch('[0-9a-f]{32}', service.get('InvocationID', '')), 'service_invocation_missing')
    return service


def query(db, sql, args=(), seconds=15):
    deadline = time.monotonic() + seconds
    db.set_progress_handler(lambda: time.monotonic() > deadline, 10000)
    return db.execute(sql, args).fetchall()


def database(output):
    path = output / 'cold-storage/archive.sqlite'
    require(path.resolve(strict=True) == path and stat.S_ISREG(path.lstat().st_mode), 'archive_database_not_regular')
    db = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=1)
    db.execute('PRAGMA query_only=ON')
    db.execute('BEGIN')
    return db


def archive_binding(db, source, namespace):
    require('archives_retired' in {row[1] for row in query(db, 'PRAGMA index_list(archives)')}, 'count_index_missing')
    require(strict_json(query(db, 'SELECT value FROM meta WHERE id=1')[0][0])
            == dict(source=source, namespace=namespace), 'archive_source_or_namespace_changed')
    count = query(db, 'SELECT count(*) FROM archives INDEXED BY archives_retired')[0][0]
    retired = query(db, 'SELECT count(*) FROM archives INDEXED BY archives_retired WHERE retired=1')[0][0]
    progress = int(query(db, 'SELECT value FROM meta WHERE id=2')[0][0])
    rowid = query(db, 'SELECT coalesce(max(rowid),0) FROM archives')[0][0]
    return dict(archived_banks=count, retired_banks=retired, max_archive_rowid=rowid,
                last_archive_progress_unix_ms=progress)


def prior_completion(prior, old_registry, transition):
    status, cloud = prior['old_final_status'], prior['old_cloud_proof']
    require(status.get('state') == 'PASSED' and status.get('source') == SOURCE
            and status.get('registry_sha256') == OLD_REGISTRY_SHA and status.get('registry') == old_registry,
            'prior_final_status_not_canonical_complete')
    require(cloud.get('state') == 'PASSED' and cloud.get('registry_sha256') == OLD_REGISTRY_SHA
            and cloud.get('archived_banks') == cloud.get('retired_banks') == 308768
            and cloud.get('additional_banks') == 51606 and cloud.get('pending_bundles') == 0
            and cloud.get('full_wrapped_get') is True and cloud.get('proof_scope') == 'independent_final_new_bundle'
            and cloud.get('index_reopened_read_only') is True, 'prior_cloud_proof_incomplete')
    old_source = next(row['source'] for row in old_registry['jobs'] if row['unit'] in FINITE_CAPS and row['max_archived_banks'] == 308768)
    require(cloud.get('source') == old_source and cloud.get('namespace') == NAMESPACE, 'prior_cloud_proof_source_changed')
    extra = prior['extra_installation']
    require(extra.get('state') == 'ACTIVE' and extra.get('source') == old_source
            and extra.get('active_source') == SOURCE and extra.get('absolute_bank_limit') == 308768
            and extra.get('maximum_additional_banks') == 51606
            and extra.get('previous_cap_785546_changed') is False
            and extra.get('plan_sha256') == 'de5dde9c63f48ef17bcfedc8301764614608d7a6881d9c22a5934ec3f067ee3b',
            'approved_extra_scope_changed')
    require(status['sampled_unix_ms'] <= transition['started_unix_ms']
            and cloud['completed_unix_ms'] <= transition['started_unix_ms'], 'completion_proof_after_transition')
    for unit, cap in FINITE_CAPS.items():
        item = status['storage'][unit]
        row = next(row for row in old_registry['jobs'] if row['unit'] == unit)
        require(item['archived_banks'] == item['retired_banks'] == item['absolute_cap'] == cap
                and item['source'] == row['source'] and item['namespace'] == row['namespace']
                and item['verified_helper_sha256'] == row['helper_sha256']
                and item['status']['state'] == 'COMPLETE' and item['status']['pending_bundles'] == 0
                and item['service']['ActiveState'] == 'inactive' and item['service']['Result'] == 'success'
                and item['service']['Restart'] == 'no', 'prior_finite_job_incomplete')


def snapshot_context():
    prior, anchor_raw = anchored()
    pin = read(ROOT / 'config/native-active-source.json', 65536)
    require(sha(pin) == PIN_SHA and strict_json(pin) == SOURCE, 'active_source_changed')
    old_registry, old_raw = object_file(TRANSITION / 'registry-before.json', 256 * 1024)
    require(sha(old_raw) == OLD_REGISTRY_SHA and old_registry.get('root') == str(ROOT)
            and old_registry.get('schema') == 'blockzilla-storage-jobs-registry-v1', 'prior_registry_changed')
    require(len(old_registry['jobs']) == 3 and {row['unit'] for row in old_registry['jobs']} == {*FINITE_CAPS, LIVE}, 'registry_job_set_changed')
    install, install_raw = object_file(TRANSITION / 'INSTALLATION.json')
    before, before_raw = object_file(TRANSITION / 'BEFORE.json')
    require(install.get('state') == 'ACTIVE' and install.get('source') == SOURCE, 'transition_not_active')
    require(install.get('release') == str(RELEASE) and install.get('finite_rows_unchanged') is True
            and install.get('bank_caps_changed') is False and install.get('protected_pids_unchanged') is True,
            'transition_scope_flags_invalid')
    require(install.get('before_sha256') == sha(before_raw)
            and install.get('registry_before_sha256') == OLD_REGISTRY_SHA, 'transition_baseline_unbound')
    prior_completion(prior, old_registry, install)
    transport, binding, protocol = prior['transport'], prior['pid_binding'], prior['protocol']
    native = {unit: int(pid) for unit, pid in transport['protected_native_pids'].items()}
    expected_pids = {**native, **{unit.removesuffix('.service'): int(pid) for unit, pid in transport['transport_pids'].items()}}
    require(transport.get('state') == 'ACTIVE' and transport.get('source') == SOURCE
            and transport.get('protected_native_pids_unchanged') is True
            and transport.get('source_changed') is False and transport.get('validator_restarted') is False
            and transport.get('bank_limits_changed') is False, 'frozen_transport_invalid')
    require(binding.get('state') == 'VERIFIED' and binding.get('source') == SOURCE
            and binding.get('installation_sha256') == ANCHORS['transport'][1]
            and binding.get('protected_pids') == expected_pids
            and len(native) == 5 and len(expected_pids) == 8, 'frozen_pid_binding_invalid')
    require(all(native[unit] == int(protocol['protected_pids'][unit]) for unit in native)
            and install.get('protected_pids') == expected_pids, 'native_or_transport_pid_authority_changed')
    require(protocol.get('state') == 'ACTIVE' and protocol.get('source') == SOURCE
            and protocol.get('bank_caps_changed') is False
            and prior['old_final_status']['pids'] == expected_pids,
            'prior_protocol_or_final_pid_binding_changed')
    old_transport_proof = prior['transport_proof']
    require(old_transport_proof.get('state') == 'PASSED' and old_transport_proof.get('source') == SOURCE
            and old_transport_proof.get('registry_sha256') == OLD_REGISTRY_SHA,
            'frozen_transport_connection_proof_invalid')
    for unit, expected in expected_pids.items():
        require(int(show(unit)['MainPID']) == expected, 'protected_pid_changed')
    for unit, pid in transport['transport_pids'].items():
        actual = (Path('/proc') / pid / 'cmdline').read_bytes().decode().rstrip('\0').split('\0')
        require(actual == transport['commands'][unit], 'protected_transport_command_changed')
    require(RELEASE.resolve(strict=True) == RELEASE, 'release_path_changed')
    require(sha(read(RELEASE / 'MANIFEST.json')) == '90661d5e0d5c02f465056a8109faf8975f856d28b84a46c764232232c10aa03a',
            'release_manifest_changed')
    stage, _ = object_file(TRANSITION / 'STAGE.json')
    require(stage.get('state') == 'STAGED_TESTED_NOT_ACTIVE' and stage.get('release') == str(RELEASE)
            and stage.get('tests_passed') is True and stage.get('tests_count') == 51
            and stage.get('patch_sha256') == PATCH_SHA
            and stage.get('manifest_sha256') == '90661d5e0d5c02f465056a8109faf8975f856d28b84a46c764232232c10aa03a',
            'stage_test_receipt_invalid')
    for name, expected in (('cold_storage.py', COLD_SHA), ('storage_jobs.py', GUARDIAN_SHA)):
        require(sha(read(RELEASE / name)) == expected, 'new_helper_hash_changed')
    expected_registry = copy.deepcopy(old_registry)
    old_live = next(row for row in old_registry['jobs'] if row['unit'] == LIVE)
    live = next(row for row in expected_registry['jobs'] if row['unit'] == LIVE)
    require(old_live['source'] == SOURCE and old_live['namespace'] == NAMESPACE
            and old_live['output'] == str(OUTPUT) and old_live['max_archived_banks'] is None
            and old_live['helper_sha256'] == '837b89f1c2dcfac6fb34415f783d88274ae0fd8d9200c53d5a70333cfbc57b63', 'prior_live_contract_changed')
    live['helper'] = str(RELEASE / 'cold_storage.py')
    live['helper_sha256'] = COLD_SHA
    require(live['expected_command'].count(old_live['helper']) == 1, 'prior_live_helper_argument_invalid')
    live['expected_command'][live['expected_command'].index(old_live['helper'])] = live['helper']
    live['expected_command_sha256'] = command_sha(live['expected_command'])
    registry, registry_raw = object_file(ROOT / 'config/storage-jobs.json', 256 * 1024)
    require(registry == expected_registry and install.get('registry_after_sha256') == sha(registry_raw), 'registry_transition_scope_changed')
    require(install.get('live_command') == live['expected_command'], 'installed_live_command_unbound')
    require(before.get('source') == SOURCE and before.get('source_pin_sha256') == PIN_SHA
            and before.get('registry_before_sha256') == OLD_REGISTRY_SHA
            and before.get('protected_pids') == expected_pids and before.get('old_live_row') == old_live
            and before.get('live_before') == install.get('live_before')
            and before['live_before'].get('metadata') == dict(source=SOURCE, namespace=NAMESPACE),
            'stopped_live_baseline_invalid')
    require(install.get('final_status_sha256') == ANCHORS['old_final_status'][1]
            and install.get('cloud_proof_sha256') == ANCHORS['old_cloud_proof'][1]
            and before.get('final_status_sha256') == ANCHORS['old_final_status'][1]
            and before.get('cloud_proof_sha256') == ANCHORS['old_cloud_proof'][1],
            'completion_anchor_not_bound_in_transition')
    guardian_command = protocol['guardian_command'].copy()
    old_guardian = next(value for value in guardian_command if value.endswith('/storage_jobs.py'))
    guardian_command[guardian_command.index(old_guardian)] = str(RELEASE / 'storage_jobs.py')
    require(install.get('guardian_command') == guardian_command, 'installed_guardian_command_unbound')
    live_service = process(LIVE, live['expected_command'])
    guardian_service = process(GUARDIAN, guardian_command)
    require(int(live_service['MainPID']) == int(install['live_pid'])
            and int(guardian_service['MainPID']) == int(install['guardian_pid'])
            and live_service['InvocationID'] == install['live_invocation_id']
            and guardian_service['InvocationID'] == install['guardian_invocation_id'], 'new_service_pid_or_invocation_changed')
    now = int(time.time() * 1000)
    health, _ = object_file(ROOT / 'reports/storage-jobs/STATUS.json')
    require(health.get('state') == 'HEALTHY' and health.get('issues') == []
            and health.get('active_source') == SOURCE and health.get('registry_sha256') == sha(registry_raw)
            and type(health.get('updated_unix_ms')) is int and -5000 <= now - health['updated_unix_ms'] <= 90000,
            'guardian_not_fresh_healthy')
    require(len(health['jobs']) == 3 and {item['unit'] for item in health['jobs']} == set(expected_registry['jobs'][i]['unit'] for i in range(3)), 'guardian_job_set_changed')
    for row in registry['jobs']:
        item = next(item for item in health['jobs'] if item['unit'] == row['unit'])
        require(item.get('source') == row['source'] and item.get('max_archived_banks') == row['max_archived_banks']
                and item.get('issues') == [] and item.get('state') == ('RUNNING' if row['unit'] == LIVE else 'COMPLETE'),
                'guardian_job_identity_or_state_changed')
    live_status, _ = object_file(OUTPUT / 'cold-storage/STATUS.json')
    require(live_status.get('state') == 'RUNNING' and live_status.get('full_remote_readback_required') is True
            and type(live_status.get('updated_unix_ms')) is int
            and -5000 <= now - live_status['updated_unix_ms'] <= 90000,
            'live_storage_status_not_fresh_running')
    finite = {}
    for row in registry['jobs']:
        if row['unit'] == LIVE:
            continue
        require(row['max_archived_banks'] == FINITE_CAPS[row['unit']]
                and sha(read(Path(row['helper']))) == row['helper_sha256']
                and effective_command(row['unit']) == row['expected_command']
                and command_sha(row['expected_command']) == row['expected_command_sha256'], 'finite_job_contract_changed')
        service = show(row['unit'])
        require(service['ActiveState'] == 'inactive' and service['SubState'] == 'dead'
                and service['MainPID'] == '0' and service['Result'] == 'success'
                and service['Restart'] == 'no', 'finite_job_not_complete_inactive')
        with closing(database(Path(row['output']))) as db:
            counts = archive_binding(db, row['source'], row['namespace'])
        status, _ = object_file(Path(row['output']) / 'cold-storage/STATUS.json')
        cap = FINITE_CAPS[row['unit']]
        require(counts['archived_banks'] == counts['retired_banks'] == cap
                and status.get('state') == 'COMPLETE' and status.get('stop_reason') == 'max_archived_banks'
                and status.get('archived_banks') == status.get('retired_banks') == status.get('max_archived_banks') == cap
                and status.get('pending_bundles') == 0, 'finite_archive_or_status_changed')
        require(not list((Path(row['output']) / 'cold-storage').glob('bundle*.tar')), 'finite_pending_bundle_present')
        if cap == 308768:
            require(counts['max_archive_rowid'] == prior['old_cloud_proof']['final_archive_rowid']
                    and counts['last_archive_progress_unix_ms'] == prior['old_cloud_proof']['last_archive_progress_unix_ms'],
                    'completed_finite_index_changed_after_proof')
        finite[row['unit']] = dict(**counts, absolute_cap=cap, helper_sha256=row['helper_sha256'])
    return dict(prior=prior, anchors=anchor_raw, pin=pin, registry_raw=registry_raw,
                install=install, install_raw=install_raw, before=before, before_raw=before_raw,
                live=live, live_service=live_service, guardian_service=guardian_service,
                protected_pids=expected_pids, finite=finite)


def live_snapshot(context):
    baseline = context['before']['live_before']
    require(type(baseline['max_archive_rowid']) is int and baseline['max_archive_rowid'] >= 0,
            'invalid_before_archive_rowid')
    old_live = context['prior']['old_final_status']['storage'][LIVE]
    require(type(baseline['archived_banks']) is int and type(baseline['retired_banks']) is int
            and baseline['archived_banks'] >= old_live['archived_banks']
            and baseline['retired_banks'] >= old_live['retired_banks']
            and baseline['max_archive_rowid'] >= baseline['archived_banks']
            and type(baseline['last_archive_progress_unix_ms']) is int,
            'stopped_live_baseline_regressed')
    with closing(database(OUTPUT)) as db:
        counts = archive_binding(db, SOURCE, NAMESPACE)
        require(counts['archived_banks'] > baseline['archived_banks']
                and counts['retired_banks'] > baseline['retired_banks']
                and counts['last_archive_progress_unix_ms'] > max(
                    baseline['last_archive_progress_unix_ms'], context['install']['completed_unix_ms']),
                'new_live_durable_progress_missing')
        rows = query(db, 'SELECT rowid,slot,bank,record,retired FROM archives ORDER BY rowid DESC LIMIT 257')
    require(rows, 'live_archive_empty')
    groups = []
    for rowid, slot, bank, raw, retired in rows:
        require(isinstance(raw, str) and len(raw.encode()) <= 65536, 'archive_record_too_large')
        record = strict_json(raw)
        require(record.get('source') == SOURCE and record.get('namespace') == NAMESPACE
                and type(record.get('slot')) is int and record['slot'] == slot
                and type(record.get('bank')) is int and record['bank'] == bank,
                'archive_record_identity_changed')
        digest = record.get('bundle_sha256')
        require(isinstance(digest, str) and re.fullmatch('[0-9a-f]{64}', digest), 'archive_bundle_hash_invalid')
        require(record.get('key') == 'native-custody/processed-v1/' + SOURCE['session_id'] + '/' + digest + '.tar', 'archive_key_invalid')
        if not groups or groups[-1][0][1]['key'] != record['key']:
            groups.append([])
        groups[-1].append((rowid, record, raw, retired))
    selected = None
    for group in groups[:-1]:
        if (1 <= len(group) <= 128 and all(item[3] == 1 for item in group)
                and min(item[0] for item in group) > baseline['max_archive_rowid']):
            selected = group
            break
    require(selected is not None, 'fully_retired_new_live_bundle_not_ready')
    require([item[0] for item in selected] == list(range(selected[0][0], selected[-1][0] - 1, -1)), 'bundle_group_not_contiguous')
    return counts, selected


def validator_module(path):
    require(sha(read(path, 1024 * 1024)) == VALIDATOR_SHA, 'pinned_cloud_validator_changed')
    spec = importlib.util.spec_from_file_location('pinned_read_only_cloud_validator', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    # Only client(), HashedStream(), and validate_bundle() are used. The old
    # completion and archive selection routines retain their old finite scope.
    module.ROOT, module.SOURCE, module.OUTPUT, module.NAMESPACE = ROOT, SOURCE, OUTPUT, NAMESPACE
    return module


def cloud_child(options):
    require(stat.S_ISFIFO(os.fstat(options.parent_watch_fd).st_mode), 'invalid_parent_watch_pipe')
    def watch():
        try:
            os.read(options.parent_watch_fd, 1)
        finally:
            os._exit(70)
    threading.Thread(target=watch, daemon=True).start()
    try:
        raw = sys.stdin.buffer.read(MAX_INPUT + 1)
        require(len(raw) <= MAX_INPUT, 'cloud_request_too_large')
        request = strict_json(raw)
        require(set(request) == {'source', 'namespace', 'key', 'sha256', 'indexed'}
                and request['source'] == SOURCE and request['namespace'] == NAMESPACE, 'cloud_request_identity_changed')
        indexed = request['indexed']
        require(isinstance(indexed, list) and 1 <= len(indexed) <= 128, 'cloud_index_count_invalid')
        module = validator_module(options.validator_file)
        s3, bucket = module.client()
        deadline = time.monotonic() + 180
        with closing(s3):
            response = s3.get_object(Bucket=bucket, Key=request['key'])
            with closing(response['Body']) as body:
                length = response.get('ContentLength')
                require(response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 200
                        and type(length) is int and 0 < length <= module.MAX_BUNDLE
                        and response.get('Metadata', {}).get('sha256') == request['sha256'], 'cloud_response_metadata_invalid')
                stream = module.HashedStream(body, deadline)
                details = module.validate_bundle(stream, indexed)
                require(stream.count == length == details['expected_object_bytes'], 'cloud_object_size_mismatch')
                require(stream.hasher.hexdigest() == request['sha256'], 'cloud_object_digest_mismatch')
        result = dict(state='PASSED', source=SOURCE, namespace=NAMESPACE, key=request['key'],
                      bundle_sha256=request['sha256'], object_bytes=stream.count,
                      full_wrapped_get=True, **details)
        code = 0
    except Exception as error:
        code = 1
        result = dict(state='FAILED', error='cloud_bundle_verification_failed', error_type=type(error).__name__[:64])
    print(json.dumps(result, separators=(',', ':')), flush=True)
    return code


def bounded_cloud(validator, selected):
    request = dict(source=SOURCE, namespace=NAMESPACE, key=selected[0][1]['key'],
                   sha256=selected[0][1]['bundle_sha256'], indexed=[[item[0], item[1]] for item in selected])
    raw = json.dumps(request, separators=(',', ':')).encode()
    require(len(raw) <= MAX_INPUT, 'cloud_request_too_large')
    reader, writer = os.pipe()
    child = None
    deadline = time.monotonic() + 200
    try:
        import tempfile
        with tempfile.TemporaryFile() as input_file:
            input_file.write(raw)
            input_file.seek(0)
            child = subprocess.Popen([sys.executable, '-B', str(Path(__file__).resolve()),
                '--cloud-child', '--parent-watch-fd', str(reader), '--validator-file', str(validator)],
                stdin=input_file, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, pass_fds=(reader,))
        os.close(reader)
        reader = None
        output = bytearray()
        with selectors.DefaultSelector() as poll:
            poll.register(child.stdout, selectors.EVENT_READ)
            while poll.get_map():
                remaining = deadline - time.monotonic()
                require(remaining > 0, 'cloud_proof_total_deadline')
                for key, _ in poll.select(remaining):
                    body = os.read(key.fd, MAX_OUTPUT + 1 - len(output))
                    if not body:
                        poll.unregister(key.fileobj)
                    else:
                        output.extend(body)
                        require(len(output) <= MAX_OUTPUT, 'cloud_proof_output_limit')
        remaining = deadline - time.monotonic()
        require(remaining > 0, 'cloud_proof_total_deadline')
        code = child.wait(timeout=remaining)
        result = strict_json(output)
        require(code == 0 and isinstance(result, dict) and result.get('state') == 'PASSED'
                and result.get('source') == SOURCE and result.get('namespace') == NAMESPACE
                and result.get('key') == request['key'] and result.get('bundle_sha256') == request['sha256']
                and result.get('bundle_banks') == len(selected) and result.get('local_receipts_checked') == 2 * len(selected)
                and result.get('full_wrapped_get') is True, 'cloud_child_proof_failed')
        return result
    finally:
        if child is not None:
            if child.poll() is None:
                child.kill()
            child.wait()
            child.stdout.close()
        if reader is not None:
            os.close(reader)
        os.close(writer)


def recheck_selection(selected):
    digest, key = selected[0][1]['bundle_sha256'], selected[0][1]['key']
    with closing(database(OUTPUT)) as db:
        counts = archive_binding(db, SOURCE, NAMESPACE)
        # This is the only JSON-record scan. The SQLite progress deadline stops
        # it on a slow/large index; no write or index migration is permitted.
        rows = query(db, 'SELECT rowid,slot,bank,record,retired FROM archives WHERE json_extract(record,\'$.key\')=? LIMIT 129', (key,))
        expected = sorted((item[0], item[1]['slot'], item[1]['bank'], item[2], 1) for item in selected)
        require(sorted(rows) == expected, 'selected_bundle_index_rows_changed_or_incomplete')
    for _, record, _, _ in selected:
        require(record['bundle_sha256'] == digest, 'selected_bundle_hash_changed')
        note = {name: record[name] for name in ('source', 'slot', 'bank', 'key', 'bundle_sha256', 'namespace', 'files')}
        note['schema'] = 'blockzilla-native-cold-copy-v1'
        for folder in (record['artifact'], record['custody']):
            relative = Path(folder)
            require(not relative.is_absolute() and '..' not in relative.parts, 'custody_path_invalid')
            actual, _ = object_file(OUTPUT / relative / 'REMOTE-CUSTODY.json')
            require(actual == note, 'selected_custody_receipt_changed')
    return counts


def prove(options):
    started = int(time.time() * 1000)
    context = snapshot_context()
    initial_counts, selected = live_snapshot(context)
    cloud = bounded_cloud(options.validator_file, selected)
    final_counts = recheck_selection(selected)
    final_context = snapshot_context()
    for field in ('anchors', 'pin', 'registry_raw', 'install_raw', 'before_raw'):
        require(context[field] == final_context[field], 'authority_changed_during_cloud_proof')
    for field in ('MainPID', 'InvocationID'):
        require(context['live_service'][field] == final_context['live_service'][field]
                and context['guardian_service'][field] == final_context['guardian_service'][field], 'service_restarted_during_proof')
    require(final_counts['archived_banks'] >= initial_counts['archived_banks']
            and final_counts['retired_banks'] >= initial_counts['retired_banks'], 'live_archive_regressed')
    return dict(schema='blockzilla-transfer-deadline-postverify-v1', state='PASSED',
                proof_scope='independent_one_new_live_fully_retired_bundle', source=SOURCE,
                namespace=NAMESPACE, old_registry_sha256=OLD_REGISTRY_SHA,
                registry_sha256=sha(context['registry_raw']), transition_sha256=sha(context['install_raw']),
                before_sha256=sha(context['before_raw']), prior_anchors={name: sha(raw) for name, raw in context['anchors'].items()},
                protected_pids=context['protected_pids'], finite_jobs=context['finite'],
                live_before=context['before']['live_before'], live_snapshot=initial_counts, live_after=final_counts,
                selected_rowids=[item[0] for item in selected], cloud=cloud,
                live_pid=int(context['live_service']['MainPID']), guardian_pid=int(context['guardian_service']['MainPID']),
                started_unix_ms=started, completed_unix_ms=int(time.time() * 1000), service_actions=[], data_mutations=[])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--validator-file', type=Path, default=Path(__file__).resolve().with_name('cloud-validator.py'))
    parser.add_argument('--cloud-child', action='store_true')
    parser.add_argument('--parent-watch-fd', type=int)
    options = parser.parse_args()
    if options.cloud_child:
        return cloud_child(options)
    try:
        result = prove(options)
    except Exception as error:
        code = error.args[0] if isinstance(error, ProofError) else 'postverify_operation_failed'
        result = dict(schema='blockzilla-transfer-deadline-postverify-v1', state='FAILED',
                      error=code, error_type=re.sub('[^A-Za-z0-9_]', '', type(error).__name__)[:64],
                      service_actions=[], data_mutations=[])
    print(json.dumps(result, separators=(',', ':')), flush=True)
    return 0 if result['state'] == 'PASSED' else 1


if __name__ == '__main__':
    raise SystemExit(main())
