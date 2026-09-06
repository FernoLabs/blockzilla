"""Run the real CAR count example once per SSD representation, in sequence."""
import datetime
import json
import os
from pathlib import Path
import resource
import subprocess
import sys
import time


def fields(line):
    return dict(item.split('=', 1) for item in line.split() if '=' in item)


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')


def main():
    run = Path(sys.argv[1]).resolve(strict=True)
    ssd = Path('/volume2/blockzilla-bench').resolve(strict=True)
    binary = run / 'read-car'
    baseline = json.loads((run / 'baseline.json').read_text())
    inputs = [
        ('raw', ssd / 'archive', 'epoch-300.car', 508337873180),
        ('zstd-3', ssd / 'archive-zstd-trial', 'epoch-300.car.zst', 206321123867),
    ]
    save(run / 'status.json', {'state': 'PREFLIGHT'})
    indexes = []
    for name, root, filename, expected_size in inputs:
        epoch = root / 'car/300'
        payload = (epoch / filename).resolve(strict=True)
        index = (epoch / 'epoch-300-slot-ranges.raw').resolve(strict=True)
        assert payload.is_relative_to(ssd) and index.is_relative_to(ssd)
        assert payload.stat().st_dev == ssd.stat().st_dev
        assert index.stat().st_dev == ssd.stat().st_dev
        assert payload.stat().st_size == expected_size
        assert name == 'raw' or not (epoch / 'epoch-300.car').exists(), 'raw shadows zstd'
        indexes.append(index.read_bytes())
    assert indexes[0] == indexes[1] and len(indexes[0]) == 5184000
    results = []
    for name, root, filename, size in inputs:
        command = [str(binary), '--epoch', '300', '--archive-root', str(root)]
        save(run / 'status.json', {'state': 'RUNNING', 'current': name, 'completed': len(results)})
        started = time.monotonic()
        before = resource.getrusage(resource.RUSAGE_CHILDREN)
        with (run / (name + '.stdout.log')).open('x') as out, (run / (name + '.progress.log')).open('x') as err:
            process = subprocess.Popen(command, stdout=out, stderr=err)
            with (run / (name + '.resources.jsonl')).open('x') as metrics:
                while process.poll() is None:
                    sample = {'elapsed_s': time.monotonic() - started, 'pid': process.pid}
                    try:
                        sample['proc_io'] = {
                            k: int(v) for k, v in (
                                line.split(':', 1) for line in Path(f'/proc/{process.pid}/io').read_text().splitlines()
                            )
                        }
                    except (OSError, ValueError):
                        pass
                    metrics.write(json.dumps(sample) + '\n')
                    metrics.flush()
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        pass
        elapsed = time.monotonic() - started
        after = resource.getrusage(resource.RUSAGE_CHILDREN)
        assert process.returncode == 0, f'{name} reader failed: {process.returncode}'
        lines = (run / (name + '.stdout.log')).read_text().splitlines()
        totals = fields(next(line for line in lines if line.startswith('format=car ')))
        buckets = [fields(line) for line in lines if line.startswith('approximate_hour=')]
        for key in ('blocks', 'transactions', 'instructions', 'recorded_inner_instructions',
                    'transactions_with_incomplete_instructions', 'transactions_with_incomplete_cpi'):
            assert totals[key] == baseline[key], (name, key, totals[key], baseline[key])
        assert buckets == baseline['buckets'], f'{name}: historical slot buckets differ'
        assert int(totals['source_read_bytes']) == inputs[0][3], 'decoded stream length differs'
        assert int(totals['bound_source_size_bytes']) == size + len(indexes[0])
        assert int(totals['total_network_bytes']) == 0
        seconds = float(totals['total_s'])
        row = {'representation': name, 'command': command, 'stored_car_bytes': size,
               'total_s': seconds, 'wall_s': elapsed, 'tps': float(totals['total_tps']),
               'stored_file_MB_per_total_s': size / seconds / 1e6,
               'decoded_car_MB_per_total_s': inputs[0][3] / seconds / 1e6,
               'cpu_s': after.ru_utime + after.ru_stime - before.ru_utime - before.ru_stime,
               'totals': totals, 'buckets': buckets, 'parity': 'MATCH'}
        save(run / (name + '.result.json'), row)
        results.append(row)
        print(name, 'PASS', seconds, 'seconds', flush=True)
    summary = {'state': 'PASS', 'epoch': 300, 'workload': 'real count example',
               'inputs_and_outputs': 'SSD RAID0 Btrfs', 'compression': 'zstd -3 -T12; existing copy',
               'completed_at': datetime.datetime.now().astimezone().isoformat(),
               'size_reduction_percent': 100 * (1 - inputs[1][3] / inputs[0][3]),
               'compressed_speedup': results[0]['total_s'] / results[1]['total_s'],
               'parity': 'MATCH: raw, zstd, historical totals and all 48 slot buckets',
               'limits': 'One sequential run per representation; OS cache not cleared. '
                         'File bytes/time is not physical disk throughput. proc_io counters are sampled.',
               'runs': results}
    save(run / 'comparison.json', summary)
    save(run / 'status.json', {k: v for k, v in summary.items() if k != 'runs'})
    print('COMPARISON_PASS', summary['compressed_speedup'], flush=True)


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        save(Path(sys.argv[1]) / 'status.json', {'state': 'FAIL', 'error': str(error)})
        raise
