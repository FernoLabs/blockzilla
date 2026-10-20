#!/usr/bin/env python3
"""Read-only completion and independent full cloud proof for the approved job."""
import fcntl
import hashlib
import json
import re
import shlex
import sqlite3
import stat
import subprocess
import sys
import tarfile
import time
from contextlib import closing
from pathlib import Path, PurePosixPath

ROOT = Path('/home/ubuntu/live/hivezilla-validator-20260930')
SOURCE = dict(agave_revision='d688dd0e3a576b048a848e5faf2e8d277f8feb6e',
              genesis_hash='5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d',
              node_identity='J7qpvCEWhdBgPdYRV3s3btWPrRVXa2qnAzgvxWTvtbrQ',
              session_id='000000000000000018ddd9a6bb6fc50e-282459')
UNIT = 'hivezilla-native-cold-storage-20261016-source-18ddd9a6.service'
NAMESPACE = '1a93d01e2c293e1f61c246747319a92fea69ab27060f050b986304c4c4715bcf'
REGISTRY_SHA = 'c813c9e2accfb857eba1bf331f60af616ddfc8084b7aa2bce276ebf1fdb0c4c3'
PIN_SHA = 'ddfe097a2915a946c0917adcc135a2c7d997b1517d578044bfea80aadd7ea315'
ROW_SHA = '4b00f3f794159f735d724f5ff9c59d84aabf8242549fcd8ea99ed8effbd58aad'
INITIAL, FINAL, ADDITIONAL = 257162, 308768, 51606
MAX_BUNDLE, MAX_FILE = 512 * 1024**2, 256 * 1024**2
OUTPUT = ROOT / 'compact/direct-agave-processed-live' / SOURCE['session_id']
STATE = OUTPUT / 'cold-storage'
REPORT = ROOT / 'reports/native-recovery-20261018/recurrence/approved-extra-18ddd9a6'


class ProofError(Exception):
    pass


def require(condition, code):
    if not condition:
        raise ProofError(code)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def read(path, limit=1024**2):
    require(stat.S_ISREG(path.lstat().st_mode), 'metadata_not_regular')
    with path.open('rb') as stream:
        data = stream.read(limit + 1)
    require(len(data) <= limit, 'metadata_too_large')
    return data


def object_json(path, limit=1024**2):
    value = json.loads(read(path, limit))
    require(isinstance(value, dict), 'metadata_not_object')
    return value


def relative(value):
    require(isinstance(value, str) and value and '\x00' not in value and '\\' not in value,
            'invalid_bundle_path')
    path = PurePosixPath(value)
    require(not path.is_absolute() and str(path) == value
            and all(part not in ('.', '..') for part in path.parts), 'invalid_bundle_path')
    return path


def local_path(value):
    path = OUTPUT
    for part in relative(value).parts:
        path /= part
        require(not path.is_symlink(), 'local_receipt_symlink')
    return path


def unit_state():
    raw = subprocess.check_output(['systemctl', 'show', UNIT, '-p',
        'MainPID,ActiveState,SubState,Result,Restart'], text=True, timeout=15)
    value = dict(line.split('=', 1) for line in raw.splitlines() if '=' in line)
    require(value.get('ActiveState') == 'inactive' and value.get('SubState') == 'dead'
            and value.get('MainPID') == '0' and value.get('Result') == 'success'
            and value.get('Restart') == 'no', 'job_not_complete_inactive_success')
    return value


def completion():
    registry_raw = read(ROOT / 'config/storage-jobs.json', 256 * 1024)
    require(sha(registry_raw) == REGISTRY_SHA, 'registry_changed')
    registry = json.loads(registry_raw)
    require(registry.get('schema') == 'blockzilla-storage-jobs-registry-v1'
            and registry.get('root') == str(ROOT) and len(registry.get('jobs', [])) == 3,
            'registry_scope_changed')
    prepared = read(REPORT / 'prepared/PROPOSED-18DDD9A6-REGISTRY-ROW.json')
    require(sha(prepared) == ROW_SHA, 'approved_row_changed')
    row = json.loads(prepared)
    require([job for job in registry['jobs'] if job.get('unit') == UNIT] == [row]
            and row['source'] == SOURCE and row['namespace'] == NAMESPACE
            and row['max_archived_banks'] == FINAL and row['output'] == str(OUTPUT),
            'approved_registry_binding_changed')
    require(sha(read(Path(row['helper']))) == row['helper_sha256'], 'approved_helper_changed')
    pin = read(ROOT / 'config/native-active-source.json', 65536)
    require(sha(pin) == PIN_SHA, 'active_source_changed')
    active = json.loads(pin)
    install = object_json(REPORT / 'INSTALLATION.json')
    before = object_json(REPORT / 'BEFORE.json')
    require(install.get('schema') == 'blockzilla-approved-extra-storage-install-v1'
            and install.get('state') == 'ACTIVE' and install.get('source') == SOURCE
            and install.get('active_source') == active and install.get('absolute_bank_limit') == FINAL
            and install.get('maximum_additional_banks') == ADDITIONAL
            and before['archive']['archived_banks'] == before['archive']['retired_banks'] == INITIAL,
            'installation_binding_changed')
    require({key: object_json(OUTPUT / 'FEED.json')[key] for key in SOURCE} == SOURCE,
            'processed_source_changed')
    status = object_json(STATE / 'STATUS.json')
    require(status.get('schema') == 'blockzilla-native-cold-storage-status-v1'
            and status.get('state') == 'COMPLETE' and status.get('stop_reason') == 'max_archived_banks'
            and status.get('max_archived_banks') == FINAL and status.get('archived_banks') == FINAL
            and status.get('retired_banks') == FINAL and status.get('pending_bundles') == 0
            and status.get('full_remote_readback_required') is True
            and status.get('updated_unix_ms', 0) >= install['started_unix_ms'],
            'job_completion_status_invalid')
    require(not list(STATE.glob('bundle*.tar')), 'pending_bundle_files_present')
    unit = unit_state()
    definition = subprocess.check_output(['systemctl', 'cat', UNIT], text=True, timeout=15)
    commands = [line[10:] for line in definition.splitlines()
                if line.startswith('ExecStart=') and line != 'ExecStart=']
    require(commands and shlex.split(commands[-1]) == row['expected_command']
            and sha(json.dumps(row['expected_command'], separators=(',', ':'), ensure_ascii=False).encode())
            == row['expected_command_sha256'], 'completed_unit_command_changed')
    guardian = object_json(ROOT / 'reports/storage-jobs/STATUS.json')
    now = int(time.time() * 1000)
    stamp = guardian.get('updated_unix_ms')
    require(type(stamp) is int and -5000 <= now - stamp <= 90000
            and guardian.get('state') == 'HEALTHY' and guardian.get('issues') == []
            and guardian.get('active_source') == active and guardian.get('registry_sha256') == REGISTRY_SHA
            and len(guardian.get('jobs', [])) == 3, 'guardian_not_current_healthy')
    jobs = [job for job in guardian['jobs'] if job.get('unit') == UNIT]
    require(len(jobs) == 1 and jobs[0].get('state') == 'COMPLETE' and jobs[0].get('issues') == []
            and jobs[0].get('source') == SOURCE and jobs[0].get('max_archived_banks') == FINAL
            and jobs[0].get('archived_banks') == jobs[0].get('retired_banks') == FINAL,
            'guardian_job_completion_mismatch')
    return row, status, unit, install


def query(db, sql, args=()):
    deadline = time.monotonic() + 15
    db.set_progress_handler(lambda: time.monotonic() > deadline, 10000)
    return db.execute(sql, args).fetchall()


def archive_snapshot(db):
    db.execute('PRAGMA query_only=ON')
    db.execute('BEGIN')
    columns = {row[1]: row[5] for row in query(db, 'PRAGMA table_info(archives)')}
    require(columns == dict(slot=1, bank=2, record=0, retired=0), 'archive_schema_changed')
    require('archives_retired' in {row[1] for row in query(db, 'PRAGMA index_list(archives)')},
            'archive_count_index_missing')
    require(json.loads(query(db, 'SELECT value FROM meta WHERE id=1')[0][0])
            == dict(source=SOURCE, namespace=NAMESPACE), 'archive_binding_changed')
    total = query(db, 'SELECT count(*) FROM archives INDEXED BY archives_retired')[0][0]
    retired = query(db, 'SELECT count(*) FROM archives INDEXED BY archives_retired WHERE retired=1')[0][0]
    require(total == retired == FINAL, 'archive_final_count_mismatch')
    rows = query(db, 'SELECT rowid,slot,bank,record,retired FROM archives ORDER BY rowid DESC LIMIT 128')
    require(rows and rows[0][0] > INITIAL, 'no_new_final_archive_row')
    records = []
    for rowid, slot, bank, raw, retired in rows:
        require(isinstance(raw, str) and len(raw) <= 1024**2, 'archive_record_too_large')
        record = json.loads(raw)
        require(isinstance(record, dict) and type(record.get('slot')) is int and record['slot'] == slot
                and type(record.get('bank')) is int and record['bank'] == bank
                and record.get('source') == SOURCE and record.get('namespace') == NAMESPACE
                and retired == 1, 'archive_record_identity_mismatch')
        records.append((rowid, record))
    final_rowid, selected = records[0]
    digest = selected.get('bundle_sha256')
    require(isinstance(digest, str) and re.fullmatch('[0-9a-f]{64}', digest), 'invalid_indexed_digest')
    key = 'native-custody/processed-v1/' + SOURCE['session_id'] + '/' + digest + '.tar'
    require(selected.get('key') == key, 'indexed_key_digest_mismatch')
    last = []
    for rowid, record in records:
        if record.get('key') != key:
            break
        require(record.get('bundle_sha256') == digest, 'bundle_index_digest_mismatch')
        last.append((rowid, record))
    require(1 <= len(last) <= 128 and [rowid for rowid, _ in last]
            == list(range(final_rowid, final_rowid - len(last), -1)), 'bundle_index_group_invalid')
    return final_rowid, digest, key, last


def client():
    import boto3
    from botocore.config import Config
    values = {}
    for line in read(ROOT / 'config/native-cold-storage.env').decode().splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        if line.startswith('export '):
            line = line[7:]
        name, value = line.split('=', 1)
        parts = shlex.split(value)
        require(len(parts) == 1 and name not in values, 'invalid_credentials_file')
        values[name] = parts[0]
    endpoint = values.get('R2_S3_ENDPOINT', values.get('R2_ENDPOINT', values.get('AWS_ENDPOINT_URL_S3')))
    bucket = values.get('R2_BUCKET', values.get('S3_BUCKET'))
    access = values.get('R2_ACCESS_KEY_ID', values.get('AWS_ACCESS_KEY_ID'))
    secret = values.get('R2_SECRET_ACCESS_KEY', values.get('AWS_SECRET_ACCESS_KEY'))
    require(endpoint and endpoint.startswith('https://') and bucket and access and secret,
            'missing_credentials_or_destination')
    require(sha((endpoint + '\0' + bucket).encode()) == NAMESPACE, 'cloud_namespace_changed')
    return boto3.client('s3', endpoint_url=endpoint,
        region_name=values.get('R2_S3_REGION', values.get('R2_REGION', 'auto')),
        aws_access_key_id=access, aws_secret_access_key=secret,
        config=Config(connect_timeout=10, read_timeout=30, retries={'max_attempts': 2},
                      request_checksum_calculation='when_required', response_checksum_validation='when_required')), bucket


class HashedStream:
    def __init__(self, body, deadline):
        self.body, self.deadline, self.count = body, deadline, 0
        self.hasher = hashlib.sha256()

    def read(self, size=-1):
        require(time.monotonic() <= self.deadline, 'cloud_read_deadline')
        size = min(size, 1024**2) if size >= 0 else 1024**2
        data = self.body.read(size)
        self.count += len(data)
        require(self.count <= MAX_BUNDLE, 'cloud_bundle_too_large')
        self.hasher.update(data)
        return data


def validate_bundle(stream, indexed):
    by_bank = {(record['slot'], record['bank']): record for _, record in indexed}
    require(len(by_bank) == len(indexed), 'duplicate_indexed_bank')
    with tarfile.open(fileobj=stream, mode='r|') as tar:
        member = tar.next()
        require(member and member.name == 'BUNDLE.json' and member.isfile()
                and 0 < member.size <= 1024**2, 'bundle_manifest_missing_or_invalid')
        with tar.extractfile(member) as raw:
            manifest_raw = raw.read(1024**2 + 1)
        manifest = json.loads(manifest_raw)
        banks = manifest.get('banks')
        require(manifest.get('schema') == 'blockzilla-native-custody-bundle-v1'
                and isinstance(banks, list) and 1 <= len(banks) <= 128
                and len(banks) == len(indexed), 'bundle_manifest_identity_invalid')
        expected, seen = {}, set()
        for bank in banks:
            require(isinstance(bank, dict) and bank.get('source') == SOURCE, 'bundle_source_mismatch')
            identity = (bank.get('slot'), bank.get('bank'))
            require(identity in by_bank and identity not in seen, 'bundle_bank_index_mismatch')
            seen.add(identity)
            record = by_bank[identity]
            require(bank == {key: value for key, value in record.items()
                             if key not in ('key', 'bundle_sha256', 'namespace')}, 'bundle_record_changed')
            note = {key: record[key] for key in ('source', 'slot', 'bank', 'key', 'bundle_sha256', 'namespace', 'files')}
            note['schema'] = 'blockzilla-native-cold-copy-v1'
            for folder in (record['artifact'], record['custody']):
                require(object_json(local_path(folder) / 'REMOTE-CUSTODY.json') == note,
                        'local_retirement_receipt_mismatch')
            require(isinstance(record.get('files'), list), 'bundle_file_list_invalid')
            for entry in record['files']:
                name = str(relative(entry['path']))
                require(name not in expected and name != 'BUNDLE.json'
                        and type(entry.get('bytes')) is int and 0 <= entry['bytes'] <= MAX_FILE
                        and isinstance(entry.get('sha256'), str)
                        and re.fullmatch('[0-9a-f]{64}', entry['sha256']), 'bundle_file_identity_invalid')
                expected[name] = entry
        require(len(expected) <= 8192, 'bundle_member_count_limit')
        # The original packer writes these exact headers and 10-KiB record padding.
        info = tarfile.TarInfo('BUNDLE.json'); info.size, info.mode = len(manifest_raw), 0o600
        archive_bytes = len(info.tobuf(format=tarfile.PAX_FORMAT)) + ((info.size + 511) // 512) * 512
        for name, entry in expected.items():
            info = tarfile.TarInfo(name); info.size, info.mode = entry['bytes'], 0o600
            archive_bytes += len(info.tobuf(format=tarfile.PAX_FORMAT)) + ((info.size + 511) // 512) * 512
        expected_bytes = ((archive_bytes + 1024 + tarfile.RECORDSIZE - 1) // tarfile.RECORDSIZE) * tarfile.RECORDSIZE
        checked, payload_bytes = set(), 0
        while (member := tar.next()) is not None:
            require(member.isfile() and member.name in expected and member.name not in checked,
                    'unexpected_cloud_member')
            entry = expected[member.name]
            require(member.size == entry['bytes'], 'cloud_member_size_mismatch')
            digest, count = hashlib.sha256(), 0
            with tar.extractfile(member) as raw:
                while data := raw.read(1024**2):
                    digest.update(data); count += len(data)
            require(count == entry['bytes'] and digest.hexdigest() == entry['sha256'],
                    'cloud_member_digest_mismatch')
            checked.add(member.name); payload_bytes += count
        require(checked == set(expected), 'cloud_members_missing')
    while stream.read(1024**2):
        pass  # Include all unread trailing bytes in the independent object hash.
    return dict(bundle_banks=len(banks), member_files=len(checked), payload_bytes=payload_bytes,
                local_receipts_checked=2 * len(banks), expected_object_bytes=expected_bytes)


def prove():
    started = int(time.time() * 1000)
    row, status, unit, install = completion()
    lock_path = STATE / 'LOCK'
    require(not lock_path.is_symlink(), 'archive_lock_symlink')
    with lock_path.open('rb') as lock:
        require(stat.S_ISREG(Path(lock_path).lstat().st_mode), 'archive_lock_not_regular')
        fcntl.flock(lock, fcntl.LOCK_SH | fcntl.LOCK_NB)
        unit_state()
        database = STATE / 'archive.sqlite'
        require(stat.S_ISREG(database.lstat().st_mode), 'archive_database_not_regular')
        with closing(sqlite3.connect(database.as_uri() + '?mode=ro', uri=True, timeout=1)) as db:
            final_rowid, digest, key, indexed = archive_snapshot(db)
            progress = int(query(db, 'SELECT value FROM meta WHERE id=2')[0][0])
            require(progress >= install['started_unix_ms'], 'no_new_durable_archive_progress')
            s3, bucket = client()
            with closing(s3):
                deadline = time.monotonic() + 180
                response = s3.get_object(Bucket=bucket, Key=key)
                with closing(response['Body']) as body:
                    length = response.get('ContentLength')
                    require(response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 200
                            and type(length) is int and 0 < length <= MAX_BUNDLE
                            and response.get('Metadata', {}).get('sha256') == digest, 'cloud_response_metadata_invalid')
                    stream = HashedStream(body, deadline)
                    details = validate_bundle(stream, indexed)
                    require(stream.count == length == details['expected_object_bytes'], 'cloud_object_size_mismatch')
                    require(stream.hasher.hexdigest() == digest, 'cloud_object_digest_mismatch')
            completion()
            return dict(schema='blockzilla-approved-extra-cloudproof-v1', state='PASSED', source=SOURCE,
                        namespace=NAMESPACE, registry_sha256=REGISTRY_SHA,
                        archived_banks=FINAL, retired_banks=FINAL, additional_banks=ADDITIONAL,
                        pending_bundles=0, unit_state=unit['ActiveState'],
                        proof_scope='independent_final_new_bundle',
                        selection='maximum_durable_archive_rowid', final_archive_rowid=final_rowid,
                        bundle_sha256=digest, object_bytes=stream.count, full_wrapped_get=True,
                        index_reopened_read_only=True,
                        last_archive_progress_unix_ms=progress, started_unix_ms=started,
                        completed_unix_ms=int(time.time() * 1000), **details)


def main():
    try:
        result = prove()
    except Exception as error:
        code = error.args[0] if isinstance(error, ProofError) else 'cloudproof_operation_failed'
        result = dict(schema='blockzilla-approved-extra-cloudproof-v1', state='FAILED', error=code,
                      error_type=re.sub('[^A-Za-z0-9_]', '', type(error).__name__)[:64])
    print(json.dumps(result, separators=(',', ':')))
    return 0 if result['state'] == 'PASSED' else 1


if __name__ == '__main__':
    sys.exit(main())
