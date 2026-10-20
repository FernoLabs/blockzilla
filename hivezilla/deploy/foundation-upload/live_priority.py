"""Pause only background work when native conversion or cloud publication is behind."""
import argparse
from contextlib import closing
import json
from pathlib import Path
import re
import sqlite3
import stat
import subprocess
import time

import archive_upload as archive

CAR_UNIT = 'hivezilla-foundation-car-backfill-20261012.service'
OLD_STORAGE_UNIT = 'hivezilla-native-cold-storage-20261012-old.service'
UNITS = [CAR_UNIT, OLD_STORAGE_UNIT]
# RPC gap preservation has a moving retention deadline. Keep its separate low
# resource limits; freezing it can let the node prune the remaining blocks.
CLOUD_PAUSE_EVENTS = 8192
CLOUD_RESUME_EVENTS = 4096
CAPTURE_PROGRESS_TIMEOUT = 60
SOURCE_KEYS = {'genesis_hash', 'node_identity', 'agave_revision', 'session_id'}
MAX_SOURCE_BYTES = 256 * 1024
MAX_CAPACITY_BYTES = 64 * 1024
CAPACITY_STALE_MS = 90000
CAPACITY_RESUME_BYTES = 32 * 1024**3
# Native conversion has the tight slot bound. Cloud commits are asynchronous
# and can retain a bounded queue while the two-hour local copy stays protected.
# A 256-event resume limit unnecessarily blocked archive CPU work during drain.


def should_pause(was_paused, slot_lag, event_lag, healthy):
    if not healthy:
        return True
    if was_paused:
        return slot_lag > 8 or event_lag > CLOUD_RESUME_EVENTS
    return slot_lag > 32 or event_lag > CLOUD_PAUSE_EVENTS


def capacity_sample(root, now_ms=None):
    """An installed collector fails closed for disk consumers, not reclaimers."""
    directory = root / 'reports/native-capacity'
    try:
        mode = directory.lstat().st_mode
    except FileNotFoundError:
        return dict(installed=False, valid=False)
    except OSError:
        return dict(installed=True, valid=False, error='invalid_or_unavailable_capacity_sample')
    try:
        if not stat.S_ISDIR(mode):
            raise ValueError('invalid_capacity_directory')
        path = directory / 'STATUS.json'
        if not stat.S_ISREG(path.lstat().st_mode):
            raise ValueError('invalid_capacity_file')
        with path.open('rb') as stream:
            body = stream.read(MAX_CAPACITY_BYTES + 1)
        if len(body) > MAX_CAPACITY_BYTES:
            raise ValueError('invalid_capacity_size')
        value = json.loads(body)
        now_ms = int(time.time() * 1000) if now_ms is None else now_ms
        updated = value['updated_unix_ms']
        if (value['schema'] != 'blockzilla-native-capacity-v1' or value['state'] != 'RUNNING'
                or type(updated) is not int or not 0 <= now_ms - updated <= CAPACITY_STALE_MS
                or value['source'] != select_source(root)):
            raise ValueError('invalid_capacity_identity_or_age')
        names = ('free_disk_bytes', 'capture_reserve_bytes', 'recovery_minimum_free_bytes',
                 'next_archive_budget_bytes', 'snapshot_headroom_bytes', 'recovery_headroom_bytes')
        if any(type(value[name]) is not int for name in names):
            raise ValueError('invalid_capacity_number')
        free, reserve, recovery, budget, snapshot_headroom, recovery_headroom = (value[name] for name in names)
        if (free < 0 or reserve != 400 * 1024**3 or recovery != 500 * 1024**3 or budget < 128 * 1024**3
                or snapshot_headroom != free - reserve - budget or recovery_headroom != free - recovery - budget
                or type(value['snapshot_capture_reserve_at_risk']) is not bool
                or value['snapshot_capture_reserve_at_risk'] != (snapshot_headroom < 0)
                or type(value['snapshot_headroom_low']) is not bool
                or value['snapshot_headroom_low'] != (recovery_headroom < 0)):
            raise ValueError('invalid_capacity_equations')
        return dict(installed=True, valid=True, recovery_headroom_bytes=recovery_headroom)
    except (OSError, ValueError, KeyError, TypeError, RuntimeError, OverflowError):
        return dict(installed=True, valid=False, error='invalid_or_unavailable_capacity_sample')


def should_pause_capacity(was_paused, current):
    if not current['installed']:
        return False
    if not current['valid']:
        return True
    threshold = CAPACITY_RESUME_BYTES if was_paused else 0
    return current['recovery_headroom_bytes'] < threshold


def background_role(unit):
    if unit == CAR_UNIT:
        return 'disk_consumer'
    if unit == OLD_STORAGE_UNIT or background_unit(unit) == unit:
        return 'native_reclaimer'
    raise RuntimeError('invalid_background_role')


class CaptureProgress:
    """A worker heartbeat does not prove that the capture source advances."""

    def __init__(self):
        self.source = None
        self.positions = None
        self.changed_at = None
        self.observed = None

    def check(self, current, now=None):
        now = time.monotonic() if now is None else now
        positions = (current['capture_sequence'], current['capture_rooted_slot'])
        # After a source change or counter regression, require new progress.
        # Restarting this guard therefore cannot make a stalled source healthy.
        reset = (self.source != current['source'] or self.positions is None
                 or any(value < previous for value, previous in zip(positions, self.positions)))
        if reset:
            self.source = current['source']
            self.positions = positions
            self.changed_at = [now, now]
            self.observed = [False, False]
        else:
            for i, (value, previous) in enumerate(zip(positions, self.positions)):
                if value > previous:
                    self.changed_at[i] = now
                    self.observed[i] = True
            self.positions = positions
        ages = [now - changed for changed in self.changed_at]
        seen = all(self.observed)
        return dict(capture_progress_observed=seen,
                    capture_progress_healthy=seen and max(ages) <= CAPTURE_PROGRESS_TIMEOUT,
                    capture_sequence_age_seconds=round(ages[0], 3),
                    capture_root_age_seconds=round(ages[1], 3),
                    capture_progress_timeout_seconds=CAPTURE_PROGRESS_TIMEOUT)


def select_source(root):
    """Read the explicit current pin; only an absent pin permits legacy input."""
    pin = root / 'config/native-active-source.json'
    try:
        mode = pin.lstat().st_mode
    except FileNotFoundError:
        record = json.loads((root / 'reports/bank-v2-recovery-20261012/RECOVERY.json').read_text())
        source = record['source']
    else:
        if not stat.S_ISREG(mode):
            raise RuntimeError('invalid_live_source')
        with pin.open('rb') as stream:
            body = stream.read(MAX_SOURCE_BYTES + 1)
        if len(body) > MAX_SOURCE_BYTES:
            raise RuntimeError('invalid_live_source')
        source = json.loads(body)
        if not isinstance(source, dict) or set(source) != SOURCE_KEYS:
            raise RuntimeError('invalid_live_source')
        if any(not isinstance(source[key], str) or not re.fullmatch(r'[A-Za-z0-9]{1,128}', source[key])
               for key in SOURCE_KEYS - {'session_id'}):
            raise RuntimeError('invalid_live_source')
    session = source['session_id']
    if not isinstance(session, str) or not re.fullmatch(r'[0-9a-f]{32}-[0-9a-f]{1,16}', session):
        raise RuntimeError('invalid_live_source')
    return source


def sample(root):
    session = select_source(root)['session_id']
    worker = json.loads((root / 'compact/direct-agave-processed-live' / session / 'STATUS.json').read_text())
    checkpoint = json.loads((root / 'reports/native-processed-r2-bank-v2' / session / 'CHECKPOINT.json').read_text())
    path = root / 'captures/direct-agave-live' / session / 'events.sqlite'
    with closing(sqlite3.connect('file:' + str(path) + '?mode=ro', uri=True, timeout=1)) as db:
        state, durable, rooted = db.execute(
            'SELECT state,durable_sequence,latest_rooted_slot FROM status WHERE id=1').fetchone()
    return dict(source=session, slot_lag=max(0, rooted - worker['last_compact_slot']),
                event_lag=max(0, worker['event_sequence'] - checkpoint['sequence']),
                capture_sequence=durable, capture_rooted_slot=rooted,
                healthy=state == 'CAPTURING' and worker['state'] == 'RUNNING'
                and int(time.time() * 1000) - worker['updated_unix_ms'] < 15000)


def checked_sample(root, progress):
    try:
        current = sample(root)
        current.update(progress.check(current))
        current['healthy'] = current['healthy'] and current['capture_progress_healthy']
        return current
    except (OSError, ValueError, KeyError, sqlite3.Error, RuntimeError, TypeError, OverflowError):
        return dict(slot_lag=0, event_lag=0, healthy=False, error='invalid_or_unavailable_live_sample')


def background_unit(value):
    if not re.fullmatch(r'hivezilla-native-cold-storage-[0-9]{8}-source-[0-9a-f]{8}\.service', value):
        raise argparse.ArgumentTypeError('expected a dated native source storage unit')
    return value


def main(root, report, extra_units=()):
    units = list(dict.fromkeys([*UNITS, *(background_unit(unit) for unit in extra_units)]))
    roles = {unit: background_role(unit) for unit in units}
    report.mkdir(parents=True, exist_ok=True)
    paused = True
    capacity_paused = True
    progress = CaptureProgress()
    while True:
        current = checked_sample(root, progress)
        paused = should_pause(paused, current['slot_lag'], current['event_lag'], current['healthy'])
        capacity = capacity_sample(root)
        capacity_paused = should_pause_capacity(capacity_paused, capacity)
        disk_consumers_paused = paused or capacity_paused
        for unit in units:
            unit_paused = disk_consumers_paused if roles[unit] == 'disk_consumer' else paused
            active = subprocess.check_output(['systemctl', 'show', unit, '-p', 'ActiveState', '--value'], text=True).strip()
            if active != 'active':
                continue
            frozen = subprocess.check_output(['systemctl', 'show', unit, '-p', 'FreezerState', '--value'], text=True).strip()
            if (unit_paused and frozen != 'frozen') or (not unit_paused and frozen != 'running'):
                subprocess.run(['sudo', '-n', 'systemctl', 'freeze' if unit_paused else 'thaw', unit], check=True)
        archive.save(report / 'STATUS.json', dict(state='BACKGROUND_PAUSED' if disk_consumers_paused else 'BACKGROUND_RUNNING',
            updated_unix_ms=int(time.time() * 1000), capacity=capacity, capacity_paused=capacity_paused,
            disk_consumers_paused=disk_consumers_paused, native_reclaimers_paused=paused,
            background_roles=roles, **current))
        time.sleep(3)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--report', type=Path, required=True)
    parser.add_argument('--background-unit', action='append', type=background_unit, default=[])
    args = parser.parse_args()
    main(args.root, args.report, args.background_unit)
