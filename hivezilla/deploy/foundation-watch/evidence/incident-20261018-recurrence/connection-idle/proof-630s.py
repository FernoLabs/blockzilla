#!/usr/bin/env python3
"""Read-only host proof. One HTTP/1.1 SSE request per endpoint, no reconnect."""

import hashlib
import importlib.util
import json
import os
from pathlib import Path
import ssl
import subprocess
import threading
import time
import urllib.parse
import urllib.request


ROOT = Path('/home/ubuntu/live/hivezilla-validator-20260930')
REPORT = ROOT / 'reports/native-recovery-20261018/recurrence/connection-idle/SAME-CONNECTION-630S.json'
DURATION = 630
PIN = ROOT / 'config/native-active-source.json'
REGISTRY = ROOT / 'config/storage-jobs.json'
INSTALLATION = REPORT.parent / 'INSTALLATION.json'
PID_BINDING = ROOT / 'reports/native-recovery-20261018/recurrence/approved-extra-18ddd9a6/TRANSPORT-PIDS.json'
MAX_BYTES = 256 * 1024 * 1024
MAX_LINE = 128 * 1024
assert not REPORT.exists(), 'proof_report_already_exists'
assert REPORT.parent.is_dir(), 'activation_report_directory_missing'
pin_bytes = PIN.read_bytes()
registry_bytes = REGISTRY.read_bytes()
installation_bytes = INSTALLATION.read_bytes()
source = json.loads(pin_bytes)
installed = json.loads(installation_bytes)
assert installed['state'] == 'ACTIVE' and installed['source'] == source
assert installed['connection_idle_seconds'] == 300
assert installed['total_connection_lifetime_seconds'] is None
binary = Path(installed['release']) / 'hivezilla-stream'
assert hashlib.sha256(binary.read_bytes()).hexdigest() == installed['binary_sha256']

UNITS = ('hivezilla-validator', 'hivezilla-direct-worker', 'hivezilla-bank-v2-producer',
         'hivezilla-bank-v2-r2-publisher', 'hivezilla-shred-hook-recorder',
         'hivezilla-bank-v2-origin', 'hivezilla-stream-origin', 'hivezilla-stream-relay')


def pids():
    return {unit: subprocess.check_output(
        ['systemctl', 'show', unit, '-p', 'MainPID', '--value'],
        text=True, timeout=5).strip() for unit in UNITS}


before_pids = pids()
assert before_pids['hivezilla-validator'] == '2142808'
assert all(int(pid) > 0 for pid in before_pids.values())
assert PID_BINDING.is_file() and not PID_BINDING.is_symlink()
binding_bytes = PID_BINDING.read_bytes()
assert len(binding_bytes) <= 65536
binding = json.loads(binding_bytes)
assert binding['state'] == 'VERIFIED' and binding['source'] == source
assert binding['installation_sha256'] == hashlib.sha256(installation_bytes).hexdigest()
approved_pids = {**{unit: int(pid) for unit, pid in installed['protected_native_pids'].items()},
                 **{unit.removesuffix('.service'): int(pid)
                    for unit, pid in installed['transport_pids'].items()}}
assert set(approved_pids) == set(UNITS)
assert binding['protected_pids'] == approved_pids
assert all(type(pid) is int and pid > 0 for pid in binding['protected_pids'].values())
assert {unit: int(pid) for unit, pid in before_pids.items()} == approved_pids
for unit in UNITS[-3:]:
    args = (Path('/proc') / before_pids[unit] / 'cmdline').read_bytes().decode().rstrip('\0').split('\0')
    assert args == installed['commands'][unit + '.service']
    assert args[0] == str(binary)

relay_args = (Path('/proc') / before_pids['hivezilla-stream-relay'] / 'cmdline').read_bytes().decode().rstrip('\0').split('\0')


def option(name):
    assert relay_args.count(name) == 1
    return relay_args[relay_args.index(name) + 1]


def loopback_origin(url):
    parsed = urllib.parse.urlsplit(url)
    assert parsed.scheme == 'https' and parsed.hostname == '127.0.0.1'
    assert parsed.path in ('', '/') and not parsed.query and not parsed.fragment
    assert parsed.username is None and parsed.password is None
    return url.rstrip('/')


v2_origin = loopback_origin(option('--banks-upstream'))
v1_origin = loopback_origin(option('--upstream'))
assert v2_origin == 'https://127.0.0.1:8880'
ca, identity = option('--upstream-ca'), option('--upstream-identity')
origin_key = Path(option('--upstream-token-file')).read_text().strip()
private_key = (ROOT / 'config/stream-20261004/client-api.key').read_text().strip()
assert origin_key and private_key
assert all('\n' not in key and '\r' not in key for key in (origin_key, private_key))

spec = importlib.util.spec_from_file_location(
    'verify', ROOT / 'releases/incident-recovery-20261018-a6b89ac28c538eaa/verify_live.py')
verify = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verify)


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise RuntimeError('redirect_rejected')


def opener(mtls=False):
    handlers = [urllib.request.ProxyHandler({}), NoRedirect()]
    if mtls:
        context = ssl.create_default_context(cafile=ca)
        context.load_cert_chain(identity)
        handlers.append(urllib.request.HTTPSHandler(context=context))
    return urllib.request.build_opener(*handlers)


lock = threading.RLock()
stop = threading.Event()
record = dict(schema='blockzilla-connection-idle-proof-v1', state='RUNNING',
              started_unix_ms=int(time.time() * 1000), duration_target_seconds=DURATION,
              source=source, installation_sha256=hashlib.sha256(installation_bytes).hexdigest(),
              binary_sha256=installed['binary_sha256'], protected_pids=before_pids,
              source_pin_sha256=hashlib.sha256(pin_bytes).hexdigest(),
              registry_sha256=hashlib.sha256(registry_bytes).hexdigest(),
              transport_pid_binding_sha256=hashlib.sha256(binding_bytes).hexdigest(),
              http_client_protocol='HTTP/1.1', reconnect_allowed=False,
              scope='three_independent_sse_connections_and_existing_relay_head_health',
              limitations=['Does not identify the existing relay HTTP/2 TCP connection.'],
              streams={}, private_head_samples=[], faults=[], service_actions=[], secret_output=False)


def fault(code, **detail):
    with lock:
        record['fault_count'] = record.get('fault_count', 0) + 1
        if len(record['faults']) < 16:
            record['faults'].append(dict(code=code, unix_ms=int(time.time() * 1000), **detail))
    print(json.dumps(dict(event='PROOF_FAULT', code=code, **detail)), flush=True)


def save():
    with lock:
        body = json.dumps(record, separators=(',', ':')).encode()
    assert len(body) < 4 * 1024 * 1024
    temp = REPORT.with_name(REPORT.name + '.new')
    with temp.open('wb') as output:
        output.write(body)
        output.flush()
        os.fsync(output.fileno())
    temp.chmod(0o600)
    os.replace(temp, REPORT)


def parse_event(label, item, event, data, now, opened):
    if event == 'gap':
        raise RuntimeError('sse_gap')
    value = json.loads('\n'.join(data)) if data else None
    item['events'] += 1
    item['event_counts'][event] = item['event_counts'].get(event, 0) + 1
    assert len(item['event_counts']) <= 16
    elapsed = now - opened
    item['last_event_elapsed_seconds'] = round(elapsed, 3)
    if item['last_event_unix_ms'] is not None:
        gap_ms = int(time.time() * 1000) - item['last_event_unix_ms']
        item['max_event_interval_ms'] = max(item['max_event_interval_ms'], gap_ms)
    item['last_event_unix_ms'] = int(time.time() * 1000)
    for boundary in (300, 600):
        if elapsed >= boundary and str(boundary) not in item['boundary_events']:
            item['boundary_events'][str(boundary)] = dict(
                elapsed_seconds=round(elapsed, 3), unix_ms=item['last_event_unix_ms'], event=event)
    if label == 'v2_origin' and event == 'status':
        assert isinstance(value, dict) and value.get('source') == source
        sampled = value.get('sampled_at_unix_ms')
        assert type(sampled) is int
        summary = dict(status=value.get('status'), source_lag_ms=value.get('source_lag_ms'),
                       sampled_at_unix_ms=sampled, delivery_age_ms=item['last_event_unix_ms'] - sampled)
        item['last_status'] = summary
        if summary['status'] != 'live' or not 0 <= summary['delivery_age_ms'] <= 5000:
            fault('origin_status_not_live_or_fresh', stream=label, status=summary)
    if label == 'public_relay' and event == 'block':
        assert isinstance(value, dict) and type(value.get('slot')) is int
        if item.get('first_slot') is None:
            item['first_slot'] = value['slot']
        item['max_slot'] = max(item.get('max_slot', 0), value['slot'])
    if label == 'v1_origin' and event == 'chunk':
        assert isinstance(value, dict) and isinstance(value.get('cursor'), str)
        assert value['cursor'].startswith(source['session_id'] + ':')
        if item.get('first_cursor') is None:
            item['first_cursor'] = value['cursor']
        item['last_cursor'] = value['cursor']


def stream_reader(label, url, mtls=False, key=None):
    item = dict(open_calls=0, events=0, bytes=0, event_counts={}, boundary_events={},
                state='OPENING', last_event_unix_ms=None, max_event_interval_ms=0)
    with lock:
        record['streams'][label] = item
    try:
        # A single opener.open call. Redirects, proxy use, and reconnects are disabled.
        request = urllib.request.Request(url, headers={'User-Agent': 'blockzilla-idle-proof/1',
            **({'Authorization': 'Bearer ' + key} if key else {})})
        with lock:
            item['open_calls'] += 1
        with opener(mtls).open(request, timeout=15) as response:
            assert response.status == 200
            assert response.headers.get('Content-Type', '').startswith('text/event-stream')
            opened = time.monotonic()
            with lock:
                item.update(state='CONNECTED', opened_unix_ms=int(time.time() * 1000))
            event, data, event_bytes = '', [], 0
            while not stop.is_set():
                line = response.readline(MAX_LINE + 1)
                assert 0 < len(line) <= MAX_LINE, 'sse_closed_or_line_too_large'
                with lock:
                    item['bytes'] += len(line)
                    assert item['bytes'] <= MAX_BYTES, 'sse_byte_limit'
                text = line.decode().rstrip('\r\n')
                if text.startswith('event:'):
                    event = text[6:].strip()
                elif text.startswith('data:'):
                    data.append(text[5:].lstrip())
                    event_bytes += len(line)
                    assert event_bytes <= 512 * 1024, 'sse_event_too_large'
                elif not text:
                    now = time.monotonic()
                    if event:
                        with lock:
                            parse_event(label, item, event, data, now, opened)
                    event, data, event_bytes = '', [], 0
                    if now - opened >= DURATION:
                        with lock:
                            item.update(state='COMPLETED', elapsed_seconds=round(now - opened, 3),
                                        completed_unix_ms=int(time.time() * 1000))
                        break
            assert item['state'] == 'COMPLETED', 'proof_stopped_before_duration'
            assert item['open_calls'] == 1 and set(item['boundary_events']) == {'300', '600'}
            if label == 'v2_origin':
                assert item['event_counts'].get('status', 0) >= 100
            elif label == 'public_relay':
                assert item.get('max_slot', 0) > item.get('first_slot', 0)
            else:
                assert item.get('last_cursor') != item.get('first_cursor')
    except Exception as error:
        with lock:
            item.update(state='FAILED', error_type=type(error).__name__)
        # Do not expose exception messages, request objects, certificates, or tokens.
        fault('same_connection_stream_failed', stream=label, error_type=type(error).__name__)


def unchanged():
    assert PIN.read_bytes() == pin_bytes and REGISTRY.read_bytes() == registry_bytes
    assert INSTALLATION.read_bytes() == installation_bytes
    assert PID_BINDING.read_bytes() == binding_bytes
    assert pids() == before_pids


initial = {}
verify.private_sample(ROOT, source, initial)
assert verify.rpc('getHealth') == 'ok'
record['initial_private_frame_and_rpc'] = initial
save()
threads = [threading.Thread(target=stream_reader, args=args, daemon=True) for args in (
    ('v2_origin', v2_origin + '/v2/banks/events', True, origin_key),
    ('v1_origin', v1_origin + '/v1/events', True, origin_key),
    ('public_relay', 'https://stream.blockzilla.dev/v1/blockhashes', False, None))]
started = time.monotonic()
for thread in threads:
    thread.start()
next_sample = next_save = next_guard = started
try:
    while any(thread.is_alive() for thread in threads):
        now = time.monotonic()
        assert now - started <= DURATION + 40, 'proof_wall_clock_limit'
        if now >= next_sample:
            sampled_ms = int(time.time() * 1000)
            try:
                head = json.loads(verify.http_bytes(verify.PRIVATE_URL + '/v2/banks/head',
                    headers={'Authorization': 'Bearer ' + private_key}, limit=256 * 1024))
                sample = dict(unix_ms=sampled_ms, status=head.get('status'),
                              source_lag_ms=head.get('source_lag_ms'), cursor=head.get('cursor'))
                with lock:
                    record['private_head_samples'].append(sample)
                if head.get('source') != source or head.get('status') != 'live':
                    fault('private_head_not_live', head=head)
            except Exception as error:
                fault('private_head_read_failed', error_type=type(error).__name__)
            next_sample = now + 1
        if now >= next_guard:
            unchanged()
            next_guard = now + 30
        if now >= next_save:
            save()
            next_save = now + 10
        time.sleep(.1)
    for thread in threads:
        thread.join(timeout=1)
    unchanged()
    final = {}
    verify.private_sample(ROOT, source, final)
    assert verify.rpc('getHealth') == 'ok'
    assert final['slot'] > initial['slot']
    record['final_private_frame_and_rpc'] = final
except Exception as error:
    fault('proof_guard_or_final_check_failed', error_type=type(error).__name__)
finally:
    stop.set()
    for thread in threads:
        thread.join(timeout=16)
    record.update(completed_unix_ms=int(time.time() * 1000),
                  elapsed_seconds=round(time.monotonic() - started, 3))
    record['state'] = 'PASSED' if not record.get('fault_count', 0) and all(
        item['state'] == 'COMPLETED' for item in record['streams'].values()) and len(record['streams']) == 3 else 'FAILED'
    save()
print(json.dumps({key: value for key, value in record.items()
                  if key not in ('private_head_samples', 'faults', 'source')}), flush=True)
raise SystemExit(0 if record['state'] == 'PASSED' else 1)
