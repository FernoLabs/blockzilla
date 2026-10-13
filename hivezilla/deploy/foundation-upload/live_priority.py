"""Pause only background work when native conversion or cloud publication is behind."""
import argparse
from contextlib import closing
import json
from pathlib import Path
import re
import sqlite3
import subprocess
import time

import archive_upload as archive

UNITS = ['hivezilla-foundation-car-backfill-20261012.service',
         'hivezilla-native-cold-storage-20261012-old.service']
# RPC gap preservation has a moving retention deadline. Keep its separate low
# resource limits; freezing it can let the node prune the remaining blocks.
CLOUD_PAUSE_EVENTS = 8192
CLOUD_RESUME_EVENTS = 4096
CAPTURE_PROGRESS_TIMEOUT = 60
# Native conversion has the tight slot bound. Cloud commits are asynchronous
# and can retain a bounded queue while the two-hour local copy stays protected.
# A 256-event resume limit unnecessarily blocked archive CPU work during drain.


def should_pause(was_paused, slot_lag, event_lag, healthy):
    if not healthy:
        return True
    if was_paused:
        return slot_lag > 8 or event_lag > CLOUD_RESUME_EVENTS
    return slot_lag > 32 or event_lag > CLOUD_PAUSE_EVENTS


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


def sample(root):
    record = json.loads((root / 'reports/bank-v2-recovery-20261012/RECOVERY.json').read_text())
    session = record['source']['session_id']
    if not re.fullmatch(r'[0-9a-f]{32}-[0-9a-f]+', session):
        raise RuntimeError('invalid_live_source')
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


def main(root, report):
    report.mkdir(parents=True, exist_ok=True)
    paused = True
    progress = CaptureProgress()
    while True:
        current = checked_sample(root, progress)
        paused = should_pause(paused, current['slot_lag'], current['event_lag'], current['healthy'])
        for unit in UNITS:
            active = subprocess.check_output(['systemctl', 'show', unit, '-p', 'ActiveState', '--value'], text=True).strip()
            if active != 'active':
                continue
            frozen = subprocess.check_output(['systemctl', 'show', unit, '-p', 'FreezerState', '--value'], text=True).strip()
            if (paused and frozen != 'frozen') or (not paused and frozen != 'running'):
                subprocess.run(['sudo', '-n', 'systemctl', 'freeze' if paused else 'thaw', unit], check=True)
        archive.save(report / 'STATUS.json', dict(state='BACKGROUND_PAUSED' if paused else 'BACKGROUND_RUNNING',
            updated_unix_ms=int(time.time() * 1000), **current))
        time.sleep(3)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--report', type=Path, required=True)
    args = parser.parse_args()
    main(args.root, args.report)
