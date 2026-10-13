import json
from contextlib import closing
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest import mock

from live_priority import CaptureProgress, checked_sample, sample, should_pause


class PriorityTests(unittest.TestCase):
    def test_invalid_samples_pause_without_stopping_the_guard(self):
        for error in [RuntimeError('invalid_live_source'), TypeError('null compact slot')]:
            with self.subTest(error=type(error).__name__), mock.patch('live_priority.sample', side_effect=error):
                current = checked_sample(Path('/unused'), CaptureProgress())
                self.assertFalse(current['healthy'])
                self.assertTrue(should_pause(False, current['slot_lag'], current['event_lag'], current['healthy']))

    def test_capture_failure_always_pauses_history(self):
        self.assertTrue(should_pause(False, 0, 0, False))

    def test_hysteresis_prevents_repeated_freeze_and_thaw(self):
        self.assertTrue(should_pause(False, 33, 0, True))
        self.assertTrue(should_pause(True, 9, 0, True))
        self.assertFalse(should_pause(True, 8, 4096, True))
        self.assertFalse(should_pause(False, 20, 800, True))

    def test_cloud_backlog_protects_publication_priority(self):
        self.assertTrue(should_pause(False, 0, 8193, True))
        self.assertTrue(should_pause(True, 0, 4097, True))
        self.assertFalse(should_pause(False, 0, 5000, True))

    def test_capture_must_advance_both_counters_before_resume(self):
        progress = CaptureProgress()
        current = dict(source='one', capture_sequence=100, capture_rooted_slot=20)
        self.assertFalse(progress.check(current, now=0)['capture_progress_healthy'])
        current['capture_sequence'] += 1
        self.assertFalse(progress.check(current, now=1)['capture_progress_healthy'])
        current['capture_rooted_slot'] += 1
        self.assertTrue(progress.check(current, now=2)['capture_progress_healthy'])
        self.assertTrue(progress.check(current, now=61)['capture_progress_healthy'])
        self.assertFalse(progress.check(current, now=61.001)['capture_progress_healthy'])

    def test_callbacks_alone_do_not_hide_a_stalled_root(self):
        progress = CaptureProgress()
        current = dict(source='one', capture_sequence=100, capture_rooted_slot=20)
        progress.check(current, now=0)
        current.update(capture_sequence=101, capture_rooted_slot=21)
        self.assertTrue(progress.check(current, now=1)['capture_progress_healthy'])
        current['capture_sequence'] = 1000
        status = progress.check(current, now=62)
        self.assertFalse(status['capture_progress_healthy'])
        self.assertEqual(status['capture_sequence_age_seconds'], 0)
        self.assertEqual(status['capture_root_age_seconds'], 61)

    def test_source_change_and_counter_regression_need_new_progress(self):
        progress = CaptureProgress()
        current = dict(source='one', capture_sequence=100, capture_rooted_slot=20)
        progress.check(current, now=0)
        current.update(capture_sequence=101, capture_rooted_slot=21)
        self.assertTrue(progress.check(current, now=1)['capture_progress_healthy'])
        current['source'] = 'two'
        self.assertFalse(progress.check(current, now=2)['capture_progress_healthy'])
        current.update(capture_sequence=102, capture_rooted_slot=22)
        self.assertTrue(progress.check(current, now=3)['capture_progress_healthy'])
        current['capture_sequence'] = 50
        self.assertFalse(progress.check(current, now=4)['capture_progress_healthy'])
        current.update(capture_sequence=51, capture_rooted_slot=23)
        self.assertTrue(progress.check(current, now=5)['capture_progress_healthy'])

    def test_fresh_worker_status_cannot_hide_static_capture(self):
        session = '0' * 32 + '-1'
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            recovery = root / 'reports/bank-v2-recovery-20261012'
            worker = root / 'compact/direct-agave-processed-live' / session
            publisher = root / 'reports/native-processed-r2-bank-v2' / session
            capture = root / 'captures/direct-agave-live' / session
            for folder in [recovery, worker, publisher, capture]:
                folder.mkdir(parents=True)
            (recovery / 'RECOVERY.json').write_text(json.dumps(dict(source=dict(session_id=session))))
            (publisher / 'CHECKPOINT.json').write_text(json.dumps(dict(sequence=10)))
            with closing(sqlite3.connect(capture / 'events.sqlite')) as db:
                db.execute('CREATE TABLE status(id INTEGER,state TEXT,durable_sequence INTEGER,latest_rooted_slot INTEGER)')
                db.execute("INSERT INTO status VALUES(1,'CAPTURING',100,20)")
                db.commit()
            progress = CaptureProgress()
            for now, durable, slot in [(0, 100, 20), (1, 101, 21), (62, 101, 21)]:
                with closing(sqlite3.connect(capture / 'events.sqlite')) as db:
                    db.execute('UPDATE status SET durable_sequence=?,latest_rooted_slot=?', (durable, slot))
                    db.commit()
                (worker / 'STATUS.json').write_text(json.dumps(dict(
                    state='RUNNING', last_compact_slot=slot, event_sequence=10,
                    updated_unix_ms=1000 * (1000 + now))))
                with mock.patch('live_priority.time.time', return_value=1000 + now):
                    current = sample(root)
                self.assertTrue(current['healthy'])
                current.update(progress.check(current, now=now))
                healthy = current['healthy'] and current['capture_progress_healthy']
                self.assertEqual(should_pause(False, current['slot_lag'], current['event_lag'], healthy), now != 1)

