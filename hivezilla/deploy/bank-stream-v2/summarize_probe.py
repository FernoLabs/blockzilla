#!/usr/bin/env python3
"""Export operator counts only. Keep payloads and raw probe files on Foundation."""
import argparse
import json
import pathlib
import sqlite3

p = argparse.ArgumentParser()
p.add_argument('--root', type=pathlib.Path, required=True)
a = p.parse_args()
report = a.root / 'reports/bank-stream-v2-20261010'
probe = report / 'live-probe'
summary = json.loads((probe / 'SUMMARY.json').read_text())
assert summary['state'] == 'PASSED'
samples = json.loads((probe / 'samples.json').read_text())
duration = samples[-1]['elapsed_s'] - samples[0]['elapsed_s']

def percentile(values, fraction):
    return sorted(values)[int((len(values) - 1) * fraction)] if values else None

resources = {}
for unit in ['hivezilla-validator', 'hivezilla-direct-worker',
             'hivezilla-bank-v2-producer', 'hivezilla-bank-v2-origin',
             'hivezilla-stream-relay']:
    cpu_ns = 0
    for left, right in zip(samples, samples[1:]):
        if (left[unit]['MainPID'] == right[unit]['MainPID']
                and left[unit]['CPUUsageNSec'].isdigit()
                and right[unit]['CPUUsageNSec'].isdigit()):
            cpu_ns += max(0, int(right[unit]['CPUUsageNSec']) - int(left[unit]['CPUUsageNSec']))
    resources[unit] = {
        'mean_cpu_cores': round(cpu_ns / (duration * 1e9), 3),
        'peak_cgroup_memory_bytes_including_cache': max(
            (int(s[unit]['MemoryCurrent']) for s in samples if s[unit]['MemoryCurrent'].isdigit()), default=None),
        'unavailable_memory_samples': sum(not s[unit]['MemoryCurrent'].isdigit() for s in samples),
        'process_changed': len({s[unit]['MainPID'] for s in samples}) > 1,
    }
cpu_ticks = sum(max(0, right.get('consumer_cpu_ticks', 0) - left.get('consumer_cpu_ticks', 0))
                for left, right in zip(samples, samples[1:])
                if left.get('consumer_pid') == right.get('consumer_pid'))
import os
resources['rust_consumer'] = {
    'mean_cpu_cores': round(cpu_ticks / os.sysconf('SC_CLK_TCK') / duration, 3),
    'peak_rss_bytes': max(s.get('consumer_rss_bytes') or 0 for s in samples),
    'intentional_restart_count': 1,
}
db = sqlite3.connect('file:' + str(probe / 'consumer.sqlite') + '?mode=ro', uri=True)
last_received = db.execute('SELECT max(received_at_unix_ms) FROM events').fetchone()[0]
recent = [received - observed for observed, received in db.execute(
    "SELECT observed_at_unix_ms,received_at_unix_ms FROM events WHERE kind='confirmed' AND received_at_unix_ms>=?",
    [last_received - 120000])]
metrics = {
    'state': 'PASSED',
    'sampled_collection_seconds': round(duration, 3),
    'post_collection_verification_included_in_summary_seconds': True,
    'provider_events_per_second': round((samples[-1]['event_sequence'] - samples[0]['event_sequence']) / duration, 3),
    'consumer_events_per_second': round(summary['events'] / duration, 3),
    'decoded_transactions_per_second': round(summary['decoded_transactions'] / duration, 3),
    'last_120_seconds_callback_to_consumer_ms': {
        'confirmed_observations': len(recent), 'p50': percentile(recent, .5), 'p95': percentile(recent, .95)},
    'head_sample_states': {state: sum(s['head_status'] == state for s in samples)
                          for state in ['live', 'stale', 'starting', 'failed', 'unavailable']},
    'public_head_request_unavailable_samples': sum(s['head_request_unavailable'] for s in samples),
    'minimum_disk_free_bytes': min(s['disk_free_bytes'] for s in samples),
    'peak_checkpoint_bytes': max(s['checkpoint_bytes'] for s in samples),
    'resources': resources,
    'private_data_kept_on_server': True,
}
out = report / 'export-summary'
out.mkdir(exist_ok=True)
(out / 'SOAK-SUMMARY.json').write_text(json.dumps(summary, indent=2) + '\n')
(out / 'RESOURCES.json').write_text(json.dumps(metrics, indent=2) + '\n')
(out / 'HTTP-CHECKS.json').write_text((probe / 'http-checks.json').read_text())
print(json.dumps(metrics))
