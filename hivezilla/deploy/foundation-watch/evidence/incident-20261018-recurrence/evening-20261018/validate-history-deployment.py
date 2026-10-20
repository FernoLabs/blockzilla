#!/usr/bin/env python3
"""Use authenticated GET only. Save safe, separate deployment evidence."""
import hashlib
import json
from pathlib import Path
import re
import time
import tomllib
import urllib.error
import urllib.request

BASE = Path('hivezilla/deploy/foundation-watch/evidence/incident-20261018-recurrence/evening-20261018')
CURRENT = 'c19ed9e6-86df-4c98-a15d-2936e5daf6db'
PREVIOUS = '670ba00d-a5c3-4608-a553-c305656f143f'
BUNDLE_SHA = 'a376cfd61cfd66c1d0d80d34d69f90d04b80b8f7f1beb01dec02a38523109fc9'
REQUIRED = {'hivezilla-validator.service', 'hivezilla-direct-worker.service',
    'hivezilla-bank-v2-r2-publisher.service', 'hivezilla-native-cold-storage-20261012-live.service',
    'hivezilla-live-priority-20261012.service', 'hivezilla-bank-v2-producer.service'}


class CheckFailure(Exception):
    pass


def require(ok, code):
    if not ok:
        raise CheckFailure(code)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args):
        return None


OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())


def get(url, headers=None, limit=1024 * 1024):
    request = urllib.request.Request(url, method='GET', headers={
        'User-Agent': 'Blockzilla-Operations/1.0', **(headers or {})})
    try:
        with OPENER.open(request, timeout=20) as response:
            raw = response.read(limit + 1)
            result = response.status, {key.lower(): value for key, value in response.headers.items()}, raw
    except urllib.error.HTTPError as error:
        with error:
            raw = error.read(limit + 1)
            result = error.code, {key.lower(): value for key, value in error.headers.items()}, raw
    require(len(raw) <= limit, 'http_body_limit')
    return result


def save(name, raw):
    path = BASE / name
    require(not path.exists(), 'evidence_file_already_exists')
    path.write_bytes(raw)
    path.chmod(0o400)
    return dict(file=name, bytes=len(raw), sha256=sha(raw))


def json_bytes(value):
    return (json.dumps(value, indent=2) + '\n').encode()


def history_shape(value):
    require(isinstance(value, dict) and set(value) == {'schema', 'limit', 'truncated', 'alerts'}
            and value['schema'] == 'blockzilla-foundation-alert-history-v1'
            and type(value['limit']) is int and value['limit'] == 100
            and type(value['truncated']) is bool and isinstance(value['alerts'], list)
            and len(value['alerts']) <= 100, 'invalid_history_envelope')
    ids = set()
    for row in value['alerts']:
        require(isinstance(row, dict) and set(row) == {'id', 'kind', 'issues', 'createdMs', 'status', 'attempts'}, 'invalid_history_dto')
        require(isinstance(row['id'], str) and re.fullmatch('[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}', row['id'])
                and row['id'] not in ids and row['kind'] in ('failure', 'recovery', 'test')
                and row['status'] in ('pending', 'sending', 'delivered')
                and type(row['createdMs']) is int and 0 <= row['createdMs'] <= 2**53 - 1
                and type(row['attempts']) is int and 0 <= row['attempts'] <= 2**53 - 1,
                'invalid_history_row')
        require(isinstance(row['issues'], list) and len(row['issues']) <= 80, 'invalid_history_issues')
        for code in row['issues']:
            require(isinstance(code, str) and len(code) <= 120
                    and (re.fullmatch('[a-z0-9_]{1,80}', code)
                         or re.fullmatch('telemetry:[a-z0-9_]{1,80}', code)
                         or code in {'service:' + name for name in REQUIRED}), 'invalid_history_issue')
        ids.add(row['id'])


def main():
    started = time.time_ns() // 1000000
    records = {}
    preserved = {name: sha((BASE / name).read_bytes()) for name in (
        'ALERT-HISTORY.json', 'MONITOR-DEPLOY-BEFORE.json', 'MONITOR-DEPLOYMENT-BEFORE.json', 'worker-before/index.js')}
    before = json.loads((BASE / 'MONITOR-DEPLOY-BEFORE.json').read_bytes())
    require(before['startedMs'] == 1791846078202, 'wrong_saved_monitor_baseline')
    metadata_before = json.loads((BASE / 'MONITOR-DEPLOYMENT-BEFORE.json').read_bytes())
    require(metadata_before['deployment']['versions'] == [{'version_id': PREVIOUS, 'percentage': 100}], 'wrong_saved_deployment_baseline')
    bundle = (BASE / 'worker-after/index.js').read_bytes()
    require(len(bundle) == 44949 and sha(bundle) == BUNDLE_SHA, 'deployed_bundle_mismatch')
    cfg = json.loads((Path.home() / '.config/blockzilla/foundation-watch-20261013.json').read_text())
    url = cfg['url'].rstrip('/')
    require(url == 'https://blockzilla-foundation-watch.cheron-augustin.workers.dev', 'operator_url_changed')
    header = {'Authorization': 'Bearer ' + cfg['ADMIN_TOKEN']}
    oauth = tomllib.loads((Path.home() / 'Library/Preferences/.wrangler/config/default.toml').read_text())
    code, _, raw = get('https://api.cloudflare.com/client/v4/accounts/8df0426c80fcd1a04ed439f441db9ff8/workers/scripts/blockzilla-foundation-watch/deployments',
                       {'Authorization': 'Bearer ' + oauth['oauth_token']})
    require(code == 200, 'deployment_metadata_http_failed')
    api = json.loads(raw)
    require(api.get('success') is True, 'deployment_metadata_api_failed')
    result = api['result']
    deployments = result if isinstance(result, list) else result.get('deployments')
    require(isinstance(deployments, list) and 1 <= len(deployments) <= 100, 'deployment_metadata_shape_invalid')
    current = max(deployments, key=lambda item: item['created_on'])
    require(current['versions'] == [{'version_id': CURRENT, 'percentage': 100}], 'active_version_changed')
    deployment = {name: current[name] for name in ('id', 'created_on', 'source', 'strategy', 'versions')}
    records['deployment'] = save('MONITOR-DEPLOYMENT-AFTER.json', json_bytes(dict(
        schema='blockzilla-monitor-deployment-after-v1', deployment=deployment,
        previous_version=PREVIOUS, deployment_count_returned=len(deployments),
        authorization='existing Wrangler OAuth; no credentials or recipient data saved')))
    status, response_headers, status_raw = get(url + '/status', header, 262144)
    require(status == 200 and response_headers.get('cache-control', '').lower() == 'no-store', 'private_status_http_invalid')
    after = json.loads(status_raw)
    sample, prior = after['sample'], before['sample']
    require(after['startedMs'] == before['startedMs'] == 1791846078202 and after['armed'] is True,
            'durable_monitor_state_reset')
    require(sample['machine_id'] == prior['machine_id'] == 'foundation'
            and sample['boot_id'] == prior['boot_id']
            and sample['capture']['session'] == prior['capture']['session']
            and after['counters']['session'] == before['counters']['session'] == sample['capture']['session'],
            'machine_boot_or_source_changed')
    require(sample['sampled_unix_ms'] >= prior['sampled_unix_ms']
            and sample['monotonic_ms'] >= prior['monotonic_ms']
            and sample['capture']['durable_sequence'] >= prior['capture']['durable_sequence']
            and sample['compact']['event_sequence'] >= prior['compact']['event_sequence']
            and sample['publisher']['sequence'] >= prior['publisher']['sequence'], 'saved_progress_regressed')
    records['status'] = save('MONITOR-DEPLOY-AFTER.json', status_raw)
    unauthorized, _, unauthorized_raw = get(url + '/history', limit=1024)
    require(unauthorized == 401, 'history_missing_auth_not_rejected')
    authorized, history_headers, history_raw = get(url + '/history', header)
    require(authorized == 200 and history_headers.get('cache-control', '').lower() == 'no-store', 'history_authorized_http_invalid')
    history = json.loads(history_raw)
    history_shape(history)
    records['history'] = save('HISTORY-DEPLOY-AFTER.json', history_raw)
    require(all(sha((BASE / name).read_bytes()) == expected for name, expected in preserved.items()), 'prior_evidence_changed')
    clear = after.get('currentIssues') == [] and after['incident'].get('active') == [] and sample.get('errors') == []
    proof = dict(schema='blockzilla-history-deployment-readonly-proof-v1',
                 state='PASSED' if clear else 'MONITOR_FAULTS_PRESENT', started_unix_ms=started,
                 completed_unix_ms=time.time_ns() // 1000000, previous_version=PREVIOUS,
                 current_version=CURRENT, deployed_bundle_sha256=BUNDLE_SHA, deployed_bundle_bytes=len(bundle),
                 durable_started_ms=after['startedMs'], started_ms_preserved=True,
                 boot_id=sample['boot_id'], source_session=sample['capture']['session'], boot_and_source_preserved=True,
                 monitor_clear=clear, current_issues=after.get('currentIssues'), active_incident=after['incident'].get('active'),
                 sample_errors=sample.get('errors'), pending_alerts=after.get('pending_alerts'),
                 history_unauthorized_http_status=unauthorized, history_authorized_http_status=authorized,
                 history_count=len(history['alerts']), history_truncated=history['truncated'], strict_history_dto=True,
                 unauthorized_body_sha256=sha(unauthorized_raw), records=records,
                 preserved_evidence_sha256=preserved, methods_used=['GET'],
                 deployments=[], service_actions=[], manual_messages=[], configuration_changes=[])
    records['proof'] = save('HISTORY-DEPLOY-VALIDATION.json', json_bytes(proof))
    print(json.dumps({name: proof[name] for name in (
        'state', 'current_version', 'deployed_bundle_sha256', 'started_ms_preserved', 'boot_and_source_preserved',
        'monitor_clear', 'current_issues', 'active_incident', 'sample_errors', 'pending_alerts',
        'history_unauthorized_http_status', 'history_authorized_http_status', 'history_count', 'history_truncated')}), flush=True)
    return 0 if clear else 2


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except Exception as error:
        print(json.dumps(dict(state='FAILED', error_type=type(error).__name__,
            error=error.args[0] if isinstance(error, CheckFailure) else 'readonly_verification_failed')), flush=True)
        raise SystemExit(1)
