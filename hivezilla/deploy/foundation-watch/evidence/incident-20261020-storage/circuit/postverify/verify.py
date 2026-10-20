#!/usr/bin/env python3
"""Read-only circuit activation proof and full new-bundle cloud readback.

Uses pinned prior proof routines only for read-only database and cloud work.
It never calls their historical completion, authority or main functions.
"""
import argparse
from contextlib import closing
import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import stat
import sys
import time

sys.dont_write_bytecode = True
ROOT = Path('/home/ubuntu/live/hivezilla-validator-20260930')
REPORTS = ROOT / 'reports/native-recovery-20261018/recurrence'
REPORT = REPORTS / 'incident-20261020-storage/circuit'
OLD_RELEASE = ROOT / 'releases/transfer-deadline-20261018-498831996a2dc34c'
OLD_REGISTRY_SHA = 'a52115479c26ccff6367b8439cd58dbb2354bb7b61d25691e797a96611655f6f'
PIN_SHA = 'ddfe097a2915a946c0917adcc135a2c7d997b1517d578044bfea80aadd7ea315'
OLD_COLD_SHA = '8a95785f2df4e2f53b414322f0fc42a8d2edd93a02f04cfca6358bb312c19800'
COLD_SHA = 'f757f2be29f01378448cbe5bb5b529db2625e6654c065f0f17ebd8ba3cbb6d41'
COLD_TEST_SHA = '3a5fe9b96e7b3503c4d8c2d1a4efb8009f79889f4d92dbaa07420bc215e41cee'
GUARDIAN_SHA = '567c15e41b3f21665a765fbfaa5b2c70d9a565d1280463da959d946472a3c87c'
GUARDIAN_TEST_SHA = '6eef654a016d1f18be588f28bafd287d69b8aa95d8e9d02695df7cab858d7827'
SUPPORT_SHA = '30851c6644304d86daebc050da0ab4e63b30f37e8de698b7d5ea25cb68ef3f35'
VALIDATOR_SHA = 'd4fcf1d98da782e9b019c723c620e0be8ffd7e8a2026958cf72311abd96d069b'
SOURCE = dict(genesis_hash='5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d',
              node_identity='J7qpvCEWhdBgPdYRV3s3btWPrRVXa2qnAzgvxWTvtbrQ',
              agave_revision='87167f3906bdf5111e99ead64d8f4098b7874e33',
              session_id='000000000000000018df884b2474cf8e-20b258')
PIDS = {'hivezilla-validator': 2142808, 'hivezilla-direct-worker': 916425,
        'hivezilla-bank-v2-producer': 917148, 'hivezilla-bank-v2-r2-publisher': 4167494,
        'hivezilla-shred-hook-recorder': 2142799, 'hivezilla-bank-v2-origin': 2134416,
        'hivezilla-stream-origin': 2135639, 'hivezilla-stream-relay': 2136117}
LIVE = 'hivezilla-native-cold-storage-20261012-live.service'
GUARDIAN = 'hivezilla-native-storage-jobs.service'
CAPS = {'hivezilla-native-cold-storage-20261016-source-18de229d.service': 785546,
        'hivezilla-native-cold-storage-20261016-source-18ddd9a6.service': 308768}
NAMESPACE = '1a93d01e2c293e1f61c246747319a92fea69ab27060f050b986304c4c4715bcf'
OUTPUT = ROOT / 'compact/direct-agave-processed-live' / SOURCE['session_id']
ANCHORS = {
    'old_install': (REPORTS / 'transfer-deadline/INSTALLATION.json', 'ab318ad9164cfc5fd7d0ad1580294d776a3f5049fb0fab01edff3cf709d72ab3'),
    'old_cloud': (REPORTS / 'transfer-deadline/POSTVERIFY.json', '486a393c1c913231651040a3e5e7834c1d2d73f9a284bbc0b94fcfce609f29be'),
    'publisher': (REPORTS / 'evening-20261018/publisher-status/INSTALLATION.json', '004c3b7d94f0840c93b7054c01c8d4ee602615474bc0b0f491cdad96801740fb'),
    'finite_status': (REPORTS / 'approved-extra-18ddd9a6/FINAL-STATUS.json', '0b6ce276039c9b7c623c07f0e6c8b56ef3a472560f27aa125ad5366ed2536ba9'),
    'finite_cloud': (REPORTS / 'approved-extra-18ddd9a6/CLOUD-PROOF-FINAL.json', '3568ffe1ae71ea9228f82e80d5c6b66c5c74616c9a5f23698a33e948c7aaf8e5'),
}
MAX_INPUT = 8 * 1024**2
RESERVE = 400 * 1024**3
MARGIN = 1024**3


class ProofError(Exception):
    pass


def require(ok, code):
    if not ok:
        raise ProofError(code)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def strict_json(raw):
    def pairs(items):
        value = {}
        for key, item in items:
            require(key not in value, 'duplicate_json_field')
            value[key] = item
        return value
    return json.loads(raw, object_pairs_hook=pairs)


def read(path, maximum=1024**2):
    require(path.is_absolute() and path.resolve(strict=True) == path
            and stat.S_ISREG(path.lstat().st_mode), 'file_path_changed')
    with path.open('rb') as stream:
        body = stream.read(maximum + 1)
    require(len(body) <= maximum, 'metadata_too_large')
    return body


def document(path, expected=None, maximum=1024**2):
    body = read(path, maximum)
    require(expected is None or sha(body) == expected, 'authority_hash_changed')
    value = strict_json(body)
    require(isinstance(value, dict), 'metadata_not_object')
    return value, body


def bound_document(path, expected, maximum=1024**2):
    require(isinstance(expected, str) and re.fullmatch('[0-9a-f]{64}', expected), 'missing_receipt_binding')
    return document(path, expected, maximum)


def support_module():
    path = Path(__file__).resolve().with_name('proof-support.py')
    require(sha(read(path)) == SUPPORT_SHA, 'proof_support_changed')
    spec = importlib.util.spec_from_file_location('pinned_circuit_proof_support', path)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    require(value.ROOT == ROOT and value.OUTPUT == OUTPUT and value.SOURCE == SOURCE
            and value.NAMESPACE == NAMESPACE and value.VALIDATOR_SHA == VALIDATOR_SHA,
            'proof_support_identity_changed')
    return value


def command_sha(args):
    return sha(json.dumps(args, separators=(',', ':'), ensure_ascii=False).encode())


def registry_transition(old, release, cold_sha):
    require(old.get('schema') == 'blockzilla-storage-jobs-registry-v1'
            and old.get('root') == str(ROOT) and isinstance(old.get('jobs'), list)
            and len(old['jobs']) == 3 and {row['unit'] for row in old['jobs']} == {*CAPS, LIVE},
            'old_registry_scope_invalid')
    new = copy.deepcopy(old)
    prior = next(row for row in old['jobs'] if row['unit'] == LIVE)
    live = next(row for row in new['jobs'] if row['unit'] == LIVE)
    require(prior['source'] == SOURCE and prior['namespace'] == NAMESPACE
            and prior['output'] == str(OUTPUT) and prior['max_archived_banks'] is None
            and prior['helper'] == str(OLD_RELEASE / 'cold_storage.py')
            and prior['helper_sha256'] == OLD_COLD_SHA
            and prior['expected_command_sha256'] == command_sha(prior['expected_command'])
            and prior['expected_command'][:3] == ['/usr/bin/python3', prior['helper'], 'archive']
            and prior['expected_command'].count(prior['helper']) == 1, 'old_live_scope_invalid')
    live['helper'] = str(release / 'cold_storage.py')
    live['helper_sha256'] = cold_sha
    live['expected_command'][1] = live['helper']
    live['expected_command_sha256'] = command_sha(live['expected_command'])
    for row in new['jobs']:
        if row['unit'] in CAPS:
            require(row['max_archived_banks'] == CAPS[row['unit']], 'finite_cap_changed')
    return new, prior, live


def registry_bytes_transition(raw, prior, live, expected):
    proposed = raw
    for old, new, count in ((prior['helper'], live['helper'], 2),
            (prior['helper_sha256'], live['helper_sha256'], 1),
            (prior['expected_command_sha256'], live['expected_command_sha256'], 1)):
        before, after = json.dumps(old).encode(), json.dumps(new).encode()
        require(proposed.count(before) == count, 'registry_replacement_scope_invalid')
        proposed = proposed.replace(before, after)
    require(strict_json(proposed) == expected, 'registry_byte_transform_invalid')
    return proposed


def trusted_arguments(options):
    for name in ('installation_sha256', 'build_sha256', 'manifest_sha256', 'cold_sha256'):
        require(isinstance(getattr(options, name, None), str)
                and re.fullmatch('[0-9a-f]{64}', getattr(options, name)), 'missing_frozen_authority')
    require(type(options.live_pid) is int and options.live_pid > 0
            and type(options.guardian_pid) is int and options.guardian_pid > 0,
            'missing_expected_service_pids')
    require(options.build.is_absolute(), 'build_path_not_absolute')
    require(options.cold_sha256 == COLD_SHA, 'unreviewed_recovery_helper')


def recovery_state(common, helper, expected_sha):
    binding = dict(source=SOURCE, namespace=NAMESPACE, max_archived_banks=None)
    with closing(common.database(OUTPUT)) as db:
        rows = dict(common.query(db, 'SELECT id,CASE WHEN length(CAST(value AS BLOB))<=16384 '
                                'THEN value ELSE NULL END FROM meta WHERE id IN (3,4)'))
    require(set(rows) == {3, 4} and all(isinstance(raw, str) for raw in rows.values()), 'recovery_ledger_missing')
    marker, ledger = strict_json(rows[4]), strict_json(rows[3])
    require(marker == dict(schema='blockzilla-native-cold-recovery-initialized-v1', binding=binding),
            'recovery_marker_changed')
    body = read(helper)
    require(sha(body) == expected_sha, 'recovery_validator_helper_changed')
    # Execute exact approved source bytes, with no bytecode-cache fallback.
    # Only the pure validator is called; __init__/save/tick are never called.
    spec = importlib.util.spec_from_loader('read_only_circuit_contract', loader=None)
    module = importlib.util.module_from_spec(spec)
    exec(compile(body, str(helper), 'exec'), module.__dict__)
    candidate = module.RecoveryCircuit.__new__(module.RecoveryCircuit)
    candidate.value, candidate.binding = ledger, binding
    try:
        candidate.validate()
    except (ValueError, TypeError, KeyError):
        raise ProofError('recovery_ledger_invalid') from None
    require(ledger['state'] in ('NORMAL', 'RECOVERING') and ledger['reservation'] is None,
            'recovery_circuit_not_ready')
    return {key: ledger.get(key) for key in ('schema', 'state', 'burst_retries_used', 'outage_since_unix_ms',
            'next_attempt_unix_ms', 'healthy_since_unix_ms', 'healthy_start_archived_banks')}


def snapshot_context(options, common):
    trusted_arguments(options)
    anchors, raw = {}, {}
    for name, (path, expected) in ANCHORS.items():
        anchors[name], raw[name] = document(path, expected, 4 * 1024**2)
    old = anchors['old_install']
    require(old['state'] == 'ACTIVE' and old['source'] == SOURCE
            and old['registry_after_sha256'] == OLD_REGISTRY_SHA
            and old['finite_rows_unchanged'] is True and old['bank_caps_changed'] is False,
            'prior_installation_scope_invalid')
    cloud = anchors['old_cloud']
    require(cloud['state'] == 'PASSED' and cloud['source'] == SOURCE
            and cloud['registry_sha256'] == OLD_REGISTRY_SHA
            and cloud['transition_sha256'] == ANCHORS['old_install'][1]
            and cloud['cloud']['full_wrapped_get'] is True, 'prior_cloud_proof_invalid')
    pub = anchors['publisher']
    require(pub['state'] == 'ACTIVE' and pub['source'] == SOURCE
            and pub['registry_sha256'] == OLD_REGISTRY_SHA and pub['new_pid'] == PIDS['hivezilla-bank-v2-r2-publisher']
            and {**pub['protected_pids'], 'hivezilla-bank-v2-r2-publisher': pub['new_pid']} == PIDS
            and pub['protected_pids_unchanged'] is True and pub['source_changed'] is False,
            'publisher_pid_authority_invalid')
    pin = read(ROOT / 'config/native-active-source.json', 65536)
    require(sha(pin) == PIN_SHA and strict_json(pin) == SOURCE, 'active_source_changed')
    registry_before, registry_before_raw = document(REPORT / 'registry-before.json', OLD_REGISTRY_SHA, 256 * 1024)
    install, install_raw = document(REPORT / 'INSTALLATION.json', options.installation_sha256)
    before, before_raw = bound_document(REPORT / 'BEFORE.json', install.get('before_sha256'))
    plan, plan_raw = bound_document(REPORT / 'PLAN.json', install.get('plan_sha256'))
    build, build_raw = document(options.build, options.build_sha256)
    release = Path(build['release'])
    require(release.parent == ROOT / 'releases' and release.resolve(strict=True) == release
            and not release.stat().st_mode & 0o222, 'release_path_or_mode_changed')
    manifest, manifest_raw = document(release / 'MANIFEST.json', options.manifest_sha256)
    require(build.get('schema') == 'blockzilla-storage-recovery-build-v1'
            and build['state'] == 'READY_NOT_ACTIVE' and build['tests_passed'] is True
            and type(build['tests_count']) is int and build['tests_count'] == 64 and build['tests_failed'] == 0
            and build['cold_sha256'] == options.cold_sha256 and build['guardian_sha256'] == GUARDIAN_SHA
            and build['test_cold_sha256'] == COLD_TEST_SHA and build['test_guardian_sha256'] == GUARDIAN_TEST_SHA
            and manifest.get('schema') == 'blockzilla-storage-recovery-release-v1', 'build_or_manifest_invalid')
    for name, entry in manifest['files'].items():
        require(isinstance(name, str) and Path(name).name == name and name not in ('.', '..'), 'manifest_path_invalid')
        path = release / name
        body = read(path, 2 * 1024**2)
        require(type(entry['bytes']) is int and len(body) == entry['bytes']
                and sha(body) == entry['sha256'] and not path.stat().st_mode & 0o222, 'release_file_changed')
    require(manifest['files']['cold_storage.py']['sha256'] == options.cold_sha256,
            'release_cold_helper_unbound')
    require(read(release / 'BUILD.json') == build_raw
            and manifest['files']['BUILD.json']['sha256'] == options.build_sha256,
            'release_build_unbound')
    expected, prior_live, live = registry_transition(registry_before, release, options.cold_sha256)
    registry, registry_raw = document(ROOT / 'config/storage-jobs.json', maximum=256 * 1024)
    require(registry == expected and registry_raw == registry_bytes_transition(registry_before_raw, prior_live, live, expected),
            'registry_transition_scope_changed')
    require(install.get('schema') == 'blockzilla-storage-recovery-install-v1'
            and install.get('state') in ('ACTIVE', 'ACTIVE_PENDING_CLOUD_PROOF')
            and install['source'] == SOURCE and install['protected_pids'] == PIDS
            and install['registry_before_sha256'] == OLD_REGISTRY_SHA
            and install['registry_after_sha256'] == sha(registry_raw)
            and install['build_sha256'] == options.build_sha256
            and install['manifest_sha256'] == options.manifest_sha256
            and install['release'] == str(release) and install['cold_sha256'] == options.cold_sha256
            and install['guardian_sha256'] == GUARDIAN_SHA
            and install['finite_rows_byte_identical'] is True and install['bank_caps_changed'] is False
            and install['source_changed'] is False and install['validator_restarted'] is False
            and install['recovery_ledger_preserved'] is True
            and install['old_live_row'] == prior_live and install['new_live_row'] == live
            and install['live_command'] == live['expected_command']
            and install['guardian_command'] == old['guardian_command'], 'new_installation_unbound')
    require(plan.get('state') == 'PREPARED' and plan['source'] == SOURCE and plan['protected_pids'] == PIDS
            and plan['build_sha256'] == options.build_sha256 and plan['manifest_sha256'] == options.manifest_sha256
            and plan['release'] == str(release) and plan['cold_sha256'] == options.cold_sha256
            and plan['source_pin_sha256'] == PIN_SHA and plan['registry_before_sha256'] == OLD_REGISTRY_SHA
            and plan['registry_after_sha256'] == sha(registry_raw)
            and plan['old_live_row'] == prior_live and plan['new_live_row'] == live
            and plan['old_live_command'] == prior_live['expected_command']
            and plan['live_command'] == live['expected_command']
            and plan['guardian_command'] == old['guardian_command'], 'prepared_plan_scope_invalid')
    require(before.get('schema') == 'blockzilla-storage-recovery-before-v1'
            and before['source'] == SOURCE and before['protected_pids'] == PIDS
            and before['source_pin_sha256'] == PIN_SHA
            and before['registry_before_sha256'] == OLD_REGISTRY_SHA
            and before['live_before'] == install['live_before']
            and before['live_before']['metadata'] == dict(source=SOURCE, namespace=NAMESPACE),
            'stopped_baseline_invalid')
    require(anchors['finite_status']['state'] == 'PASSED'
            and anchors['finite_cloud']['state'] == 'PASSED'
            and anchors['finite_cloud']['archived_banks'] == anchors['finite_cloud']['retired_banks'] == 308768
            and anchors['finite_cloud']['full_wrapped_get'] is True, 'finite_completion_proof_invalid')
    for unit, expected_pid in PIDS.items():
        require(int(common.show(unit)['MainPID']) == expected_pid, 'protected_pid_changed')
    require(sha(read(OLD_RELEASE / 'storage_jobs.py')) == GUARDIAN_SHA, 'guardian_helper_changed')
    live_service = common.process(LIVE, live['expected_command'])
    guardian_service = common.process(GUARDIAN, old['guardian_command'])
    require(int(live_service['MainPID']) == options.live_pid == install['live_pid']
            and int(guardian_service['MainPID']) == options.guardian_pid == install['guardian_pid']
            and live_service['InvocationID'] == install['live_invocation_id']
            and guardian_service['InvocationID'] == install['guardian_invocation_id'],
            'new_service_pid_changed')
    # Hash both Python script inputs as well as the exact effective argv.
    require(sha(read(Path(live['helper']))) == options.cold_sha256
            and sha(read(Path(old['guardian_command'][2]))) == GUARDIAN_SHA, 'running_helper_changed')
    now_ms = int(time.time() * 1000)
    health, _ = document(ROOT / 'reports/storage-jobs/STATUS.json')
    require(health.get('state') == 'HEALTHY' and health.get('issues') == []
            and health.get('active_source') == SOURCE and health.get('registry_sha256') == sha(registry_raw)
            and type(health.get('updated_unix_ms')) is int
            and -5000 <= now_ms - health['updated_unix_ms'] <= 90000
            and len(health['jobs']) == 3 and {row['unit'] for row in health['jobs']} == {*CAPS, LIVE},
            'guardian_not_fresh_healthy')
    for row in registry['jobs']:
        status = next(item for item in health['jobs'] if item['unit'] == row['unit'])
        require(status['source'] == row['source'] and status.get('max_archived_banks') == row['max_archived_banks']
                and status['issues'] == [] and status['state'] == ('RUNNING' if row['unit'] == LIVE else 'COMPLETE'),
                'guardian_job_binding_changed')
    status, _ = document(OUTPUT / 'cold-storage/STATUS.json')
    require(status.get('state') == 'RUNNING' and status.get('full_remote_readback_required') is True
            and type(status.get('updated_unix_ms')) is int and -5000 <= now_ms - status['updated_unix_ms'] <= 90000,
            'live_storage_not_fresh_running')
    feed, _ = document(OUTPUT / 'FEED.json')
    require({key: feed[key] for key in SOURCE} == SOURCE, 'live_feed_source_changed')
    finite = {}
    for row in registry['jobs']:
        if row['unit'] == LIVE:
            continue
        unit, cap = row['unit'], CAPS[row['unit']]
        frozen = cloud['finite_jobs'][unit]
        require(row['max_archived_banks'] == cap and row['helper_sha256'] == frozen['helper_sha256']
                and sha(read(Path(row['helper']))) == row['helper_sha256']
                and common.effective_command(unit) == row['expected_command']
                and command_sha(row['expected_command']) == row['expected_command_sha256'], 'finite_contract_changed')
        service = common.show(unit)
        require(service['ActiveState'] == 'inactive' and service['SubState'] == 'dead' and service['MainPID'] == '0'
                and service['Result'] == 'success' and service['Restart'] == 'no', 'finite_service_changed')
        with closing(common.database(Path(row['output']))) as db:
            counts = common.archive_binding(db, row['source'], row['namespace'])
        require(counts == {key: frozen[key] for key in counts}
                and counts['archived_banks'] == counts['retired_banks'] == cap, 'finite_index_changed')
        receipt, _ = document(Path(row['output']) / 'cold-storage/STATUS.json')
        require(receipt['state'] == 'COMPLETE' and receipt['stop_reason'] == 'max_archived_banks'
                and receipt['archived_banks'] == receipt['retired_banks'] == receipt['max_archived_banks'] == cap
                and receipt['pending_bundles'] == 0
                and not list((Path(row['output']) / 'cold-storage').glob('bundle*.tar')), 'finite_completion_changed')
        finite[unit] = counts
    recovery = recovery_state(common, Path(live['helper']), options.cold_sha256)
    return dict(anchors=raw, pin=pin, registry_raw=registry_raw, install_raw=install_raw,
                before_raw=before_raw, plan_raw=plan_raw, build_raw=build_raw, manifest_raw=manifest_raw,
                install=install, before=before, live_service=live_service, guardian_service=guardian_service,
                finite=finite, recovery=recovery)


def live_selection(context, common, scans=None):
    baseline = context['before']['live_before']
    floor = context['install']['post_restart_baseline']
    require(baseline.get('metadata') == floor.get('metadata') == dict(source=SOURCE, namespace=NAMESPACE),
            'post_restart_floor_binding_changed')
    for key in ('archived_banks', 'retired_banks', 'max_archive_rowid', 'last_archive_progress_unix_ms'):
        require(type(baseline.get(key)) is int and type(floor.get(key)) is int
                and 0 <= baseline[key] <= floor[key], 'post_restart_floor_invalid')
    with closing(common.database(OUTPUT)) as db:
        counts = common.archive_binding(db, SOURCE, NAMESPACE)
        require(counts['archived_banks'] > floor['archived_banks']
                and counts['retired_banks'] > floor['retired_banks']
                and counts['last_archive_progress_unix_ms'] > max(floor['last_archive_progress_unix_ms'],
                    context['install']['completed_unix_ms']), 'new_live_durable_progress_missing')
        rows = common.query(db, "SELECT rowid,slot,bank,CASE WHEN typeof(record)='text' "
            "AND length(CAST(record AS BLOB))<=65536 THEN record ELSE NULL END,retired "
            "FROM archives ORDER BY rowid DESC LIMIT 257")
    require(rows, 'live_archive_empty')
    groups = []
    for rowid, slot, bank, raw, retired in rows:
        require(isinstance(raw, str), 'archive_record_invalid_or_too_large')
        record = strict_json(raw)
        require(isinstance(record, dict) and record.get('source') == SOURCE and record.get('namespace') == NAMESPACE
                and type(record.get('slot')) is int and record['slot'] == slot
                and type(record.get('bank')) is int and record['bank'] == bank, 'archive_record_identity_changed')
        digest = record.get('bundle_sha256')
        require(isinstance(digest, str) and re.fullmatch('[0-9a-f]{64}', digest)
                and record.get('key') == 'native-custody/processed-v1/' + SOURCE['session_id'] + '/' + digest + '.tar',
                'archive_key_or_digest_invalid')
        if not groups or groups[-1][0][1]['key'] != record['key']:
            groups.append([])
        groups[-1].append((rowid, record, raw, retired))
    selected = next((group for group in groups[:-1] if 1 <= len(group) <= 128
                     and all(row[3] == 1 for row in group)
                     and min(row[0] for row in group) > floor['max_archive_rowid']), None)
    require(selected is not None, 'fully_retired_new_bundle_not_ready')
    require([row[0] for row in selected] == list(range(selected[0][0], selected[-1][0] - 1, -1)),
            'bundle_group_not_contiguous')
    # Prove global completeness before cloud work and again after its full GET.
    recheck_selection(selected, common, scans)
    return counts, selected


def recheck_selection(selected, common, scans=None):
    key, digest = selected[0][1]['key'], selected[0][1]['bundle_sha256']
    with closing(common.database(OUTPUT)) as db:
        counts = common.archive_binding(db, SOURCE, NAMESPACE)
        # Invalid and oversized records also match, with bounded Python output.
        # A malformed historical record fails the proof rather than weakening it.
        scan_started = time.monotonic()
        try:
            rows = common.query(db, "SELECT rowid,slot,bank,CASE WHEN typeof(record)='text' "
                "AND length(CAST(record AS BLOB))<=65536 THEN record ELSE NULL END,retired FROM archives "
                "WHERE CASE WHEN typeof(record)='text' AND length(CAST(record AS BLOB))<=65536 "
                "AND json_valid(record) THEN json_extract(record,'$.key')=? ELSE 1 END LIMIT 129", (key,))
        except sqlite3.Error:
            raise ProofError('bundle_index_scan_deadline_or_invalid') from None
        if scans is not None:
            scans.append(dict(elapsed_seconds=round(time.monotonic() - scan_started, 6),
                archive_rows_at_scan=counts['archived_banks'], returned_rows=len(rows),
                selected_rows=len(selected), deadline_seconds=15, output_row_limit=129))
        require(sorted(rows) == sorted((row[0], row[1]['slot'], row[1]['bank'], row[2], 1) for row in selected),
                'bundle_group_incomplete_or_changed')
    for _, record, _, _ in selected:
        require(record['bundle_sha256'] == digest, 'bundle_digest_changed')
        note = {name: record[name] for name in ('source', 'slot', 'bank', 'key', 'bundle_sha256', 'namespace', 'files')}
        note['schema'] = 'blockzilla-native-cold-copy-v1'
        for folder in (record['artifact'], record['custody']):
            relative = Path(folder)
            require(not relative.is_absolute() and '..' not in relative.parts, 'custody_path_invalid')
            actual, _ = document(OUTPUT / relative / 'REMOTE-CUSTODY.json')
            require(actual == note, 'custody_receipt_changed')
    return counts


def admit_cloud():
    # Streaming tar validation does not download/extract a payload archive.
    # The sole temporary input is<=8MiB; keep the existing400GiB+1GiB reserve.
    require(shutil.disk_usage(ROOT).free >= RESERVE + MARGIN + MAX_INPUT, 'cloud_proof_disk_reserve')
    require(shutil.disk_usage(Path(os.environ.get('TMPDIR', '/tmp'))).free >= MARGIN + MAX_INPUT,
            'cloud_proof_temp_space')


def prove(options):
    trusted_arguments(options)
    common = support_module()
    require(sha(read(options.validator_file)) == VALIDATOR_SHA, 'cloud_validator_changed')
    started = int(time.time() * 1000)
    initial = snapshot_context(options, common)
    scans = []
    counts, selected = live_selection(initial, common, scans)
    admit_cloud()
    cloud = common.bounded_cloud(options.validator_file, selected)
    after_counts = recheck_selection(selected, common, scans)
    final = snapshot_context(options, common)
    for field in ('anchors', 'pin', 'registry_raw', 'install_raw', 'before_raw', 'plan_raw', 'build_raw', 'manifest_raw'):
        require(initial[field] == final[field], 'authority_changed_during_proof')
    for field in ('MainPID', 'InvocationID'):
        require(initial['live_service'][field] == final['live_service'][field]
                and initial['guardian_service'][field] == final['guardian_service'][field], 'service_changed_during_proof')
    require(after_counts['archived_banks'] >= counts['archived_banks']
            and after_counts['retired_banks'] >= counts['retired_banks'], 'archive_count_regressed')
    return dict(schema='blockzilla-storage-recovery-postverify-v1', state='PASSED',
        proof_scope='independent_complete_new_live_custody_bundle', source=SOURCE, namespace=NAMESPACE,
        installation_sha256=options.installation_sha256, build_sha256=options.build_sha256,
        manifest_sha256=options.manifest_sha256, helper_sha256=options.cold_sha256,
        registry_before_sha256=OLD_REGISTRY_SHA, registry_after_sha256=sha(initial['registry_raw']),
        protected_pids=PIDS, live_pid=options.live_pid, guardian_pid=options.guardian_pid,
        before=initial['before']['live_before'], live_snapshot=counts, live_after=after_counts,
        selected_rowids=[row[0] for row in selected], finite_jobs=initial['finite'], cloud=cloud,
        recovery_before=initial['recovery'], recovery_after=final['recovery'],
        anchors={key: sha(raw) for key, raw in initial['anchors'].items()},
        cloud_total_deadline_seconds=200, child_stdout_max_bytes=65536,
        sqlite_key_scan_deadline_seconds_each=15, sqlite_key_scans=scans,
        disk_reserve_bytes=RESERVE, disk_margin_bytes=MARGIN, payload_extraction=False,
        started_unix_ms=started, completed_unix_ms=int(time.time() * 1000), service_actions=[], data_mutations=[])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build', type=Path, required=True)
    for name in ('installation', 'build', 'manifest', 'cold'):
        parser.add_argument('--' + name + '-sha256', required=True)
    parser.add_argument('--live-pid', type=int, required=True)
    parser.add_argument('--guardian-pid', type=int, required=True)
    parser.add_argument('--validator-file', type=Path, default=Path(__file__).resolve().with_name('cloud-validator.py'))
    options = parser.parse_args()
    try:
        result = prove(options)
    except Exception as error:
        code = error.args[0] if isinstance(error, ProofError) else 'postverify_operation_failed'
        result = dict(schema='blockzilla-storage-recovery-postverify-v1', state='FAILED', error=code,
                      error_type=re.sub('[^A-Za-z0-9_]', '', type(error).__name__)[:64], service_actions=[], data_mutations=[])
    print(json.dumps(result, separators=(',', ':')), flush=True)
    return 0 if result['state'] == 'PASSED' else 1


if __name__ == '__main__':
    raise SystemExit(main())
