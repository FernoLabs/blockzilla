#!/usr/bin/env python3
"""Render the verified September 9 prefix results for the full reader report.

Requires matplotlib. Run from any directory. No benchmark or network access.
"""
import hashlib
import json
import os
from pathlib import Path

os.environ.setdefault('MPLCONFIGDIR', '/tmp/blockzilla-reader-report-matplotlib')
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

ROOT = Path(__file__).resolve().parents[1]
ART = ROOT / 'docs/benchmarks/artifacts'
OUT = ART / 'reader-network-update-20260909'
OUT.mkdir(exist_ok=True)
COLORS = {'v2': '#265eaa', 'v3': '#177765', 'car': '#b65312'}
plt.rcParams.update({'font.family': 'DejaVu Sans', 'font.size': 11,
                     'text.color': '#1c2735', 'axes.labelcolor': '#344054',
                     'xtick.color': '#526170', 'ytick.color': '#1c2735',
                     'svg.fonttype': 'none'})
data = {'source_sha256': {}, 'groups': []}

def read(name):
    p = ART / name
    raw = p.read_bytes()
    data['source_sha256'][name] = hashlib.sha256(raw).hexdigest()
    a = json.loads(raw)
    assert a['status']['state'] == 'COMPLETE'
    assert all(r['valid'] and r['exit_code'] == 0 for r in a['status']['cases'])
    return a

for fmt in ['v2', 'v3']:
    a = read(f'{fmt}-reader-window-20260909.json')
    for workload in ['count', 'transactions', 'usdc']:
        for legacy in [True, False]:
            rows = [r for r in a['status']['cases'] if r['mode'] == 'network'
                    and r['workload'] == workload and r['legacy'] == legacy]
            assert len(rows) == (1 if workload == 'usdc' else 2)
            seconds = sum(r['scan']['seconds'] for r in rows)
            data['groups'].append({
                'format': fmt, 'workload': workload, 'legacy': legacy,
                'blocks_per_run': rows[0]['blocks'], 'runs': len(rows),
                'case_names': [r['name'] for r in rows],
                'tps': sum(r['scan']['transactions'] for r in rows) / seconds,
                'mbs': sum(r['scan']['source_bytes'] for r in rows) / seconds / 1e6,
            })
car = read('car-reader-window-20260909.json')
data['car'] = []
for workers, reuse in [(4, False), (4, True), (8, True)]:
    rows = [r for r in car['status']['cases'] if r['http_workers'] == workers
            and r['reuse'] == reuse]
    assert len(rows) == 2
    seconds = sum(r['receipt']['scan_seconds'] for r in rows)
    data['car'].append({
        'label': f'{workers} workers / ' + ('reuse' if reuse else 'current'),
        'reuse': reuse, 'workers': workers, 'runs': len(rows),
        'case_names': [r['name'] for r in rows],
        'tps': sum(r['receipt']['transactions'] for r in rows) / seconds,
        'mbs': sum(r['receipt']['http']['body_bytes'] for r in rows) / seconds / 1e6,
        'mean_scan_seconds': seconds / len(rows),
    })
data['definitions'] = {
    'tps': 'Total transactions / combined scan seconds; setup excluded.',
    'v2_v3_mbs': 'Source bytes / combined scan seconds / 1e6; can include registry reads.',
    'car_mbs': 'Received HTTP body bytes / combined scan seconds / 1e6; includes read-ahead.',
    'scope': 'Epoch 900 prefixes. CAR fully decodes transactions and metadata. V2/V3 identities do not.',
    'usdc': 'V2 indexed and V3 standard are distinct workloads; one run per setting.',
}

def bars(ax, rows, metric, title, car_mode=False):
    labels = [r['label'] if car_mode else r['format'].upper() + (' previous' if r['legacy'] else ' new') for r in rows]
    values = [r[metric] for r in rows]
    colors = [COLORS['car' if car_mode else r['format']] for r in rows]
    patches = ax.barh(range(len(rows)), values, height=.56, color=colors)
    for p, r in zip(patches, rows):
        p.set_alpha(.48 if (not r['reuse'] if car_mode else r['legacy']) else 1)
    ax.set_yticks(range(len(rows)), labels)
    ax.invert_yaxis()
    ax.set_xlim(0, max(values) * 1.34)
    ax.set_title(title, loc='left', fontsize=13, fontweight='bold', pad=15)
    ax.set_xlabel('Scan TPS' if metric == 'tps' else ('Received MB/s' if car_mode else 'Source MB/s'))
    ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f'{v / 1e6:g}M' if metric == 'tps' and max(values) >= 1e6 else (f'{v / 1e3:g}k' if metric == 'tps' else f'{v:g}')))
    ax.grid(axis='x', color='#e5e9ee', linewidth=.7)
    ax.set_axisbelow(True)
    ax.tick_params(axis='both', length=0)
    for spine in ax.spines.values(): spine.set_visible(False)
    for y, value in enumerate(values):
        ax.text(value + max(values) * .025, y, f'{value:,.0f}' if metric == 'tps' else f'{value:.2f}', va='center', fontsize=11)

def save(fig, name):
    for ext in ['png', 'svg']:
        path = OUT / f'{name}.{ext}'
        fig.savefig(path, dpi=150, facecolor='white', metadata={'Creator': 'Blockzilla reader benchmark'})
        if ext == 'svg':
            path.write_text('\n'.join(line.rstrip() for line in path.read_text().splitlines()) + '\n')
    plt.close(fig)

for metric, name, title in [('tps', 'network-prefix-tps', 'Network transaction rate — latest measured changes'),
                            ('mbs', 'network-prefix-mbs', 'Network source read rate — latest measured changes')]:
    fig, axes = plt.subplots(2, 2, figsize=(14, 9))
    fig.subplots_adjust(left=.12, right=.97, top=.82, bottom=.13, wspace=.44, hspace=.65)
    fig.text(.04, .95, title, fontsize=21, fontweight='bold')
    fig.text(.04, .905, '9 September 2026 · epoch 900 prefixes · 12 decode workers · setup excluded', fontsize=11)
    panels = [('count', None, 'Count · 32,768 blocks · 2 runs per setting'),
              ('transactions', None, 'Transaction identities · 8,192 blocks · 2 runs'),
              ('usdc', 'v2', 'V2 indexed USDC · 8,192 blocks · 1 run'),
              ('usdc', 'v3', 'V3 standard USDC · 8,192 blocks · 1 run')]
    for ax, (workload, fmt, panel_title) in zip(axes.flat, panels):
        rows = [r for r in data['groups'] if r['workload'] == workload and (fmt is None or r['format'] == fmt)]
        bars(ax, rows, metric, panel_title)
    fig.text(.04, .055, 'Rates use combined work / combined scan time. USDC paths differ; do not rank them as equivalent work.', fontsize=10)
    fig.text(.04, .028, 'Network conditions were not controlled. Short tests do not establish full-epoch rates. Source: verified result artifacts.', fontsize=10, color='#526170')
    save(fig, name)

fig, axes = plt.subplots(1, 2, figsize=(14, 5))
fig.subplots_adjust(left=.14, right=.97, top=.68, bottom=.25, wspace=.48)
fig.text(.04, .93, 'CAR network buffer reuse — no confirmed speed gain', fontsize=21, fontweight='bold')
fig.text(.04, .86, 'Epoch 900 · 8,192 blocks · full transaction and metadata decode · 2 runs per setting', fontsize=11)
for ax, metric, title in zip(axes, ['tps', 'mbs'], ['Transaction rate', 'HTTP body rate']):
    bars(ax, data['car'], metric, title, car_mode=True)
fig.text(.04, .105, 'Four-worker reuse: 18.3% lower TPS. Eight-worker scans varied from 35 to 56 seconds. Reuse remains disabled.', fontsize=10)
fig.text(.04, .055, 'Setup excluded; received bytes include read-ahead. CAR does different work from the V2/V3 identity scans.', fontsize=10, color='#526170')
save(fig, 'car-buffer-comparison')
(OUT / 'chart-data.json').write_text(json.dumps(data, indent=2) + '\n')
print(f'Wrote three figures (PNG + SVG) and source-bound data to {OUT}')
