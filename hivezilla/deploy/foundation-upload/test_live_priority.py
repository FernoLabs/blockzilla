import argparse
import json
from contextlib import closing
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest import mock

import live_priority
from live_priority import (CaptureProgress, background_role, background_unit, capacity_sample, checked_sample,
                           sample, select_source, should_pause, should_pause_capacity)


class PriorityTests(unittest.TestCase):
    def capacity_fixture(self, root, headroom=-1):
        source = dict(genesis_hash='genesis', node_identity='identity', agave_revision='revision',
                      session_id='0' * 32 + '-1')
        pin = root / 'config/native-active-source.json'
        pin.parent.mkdir(parents=True); pin.write_text(json.dumps(source))
        path = root / 'reports/native-capacity/STATUS.json'
        path.parent.mkdir(parents=True)
        reserve, recovery, budget = 400 * 1024**3, 500 * 1024**3, 128 * 1024**3
        free = recovery + budget + headroom
        value = dict(schema='blockzilla-native-capacity-v1', state='RUNNING', updated_unix_ms=100000,
                     source=source, free_disk_bytes=free, capture_reserve_bytes=reserve,
                     recovery_minimum_free_bytes=recovery, next_archive_budget_bytes=budget,
                     snapshot_headroom_bytes=free-reserve-budget, recovery_headroom_bytes=headroom,
                     snapshot_capture_reserve_at_risk=free-reserve-budget < 0, snapshot_headroom_low=headroom < 0)
        path.write_text(json.dumps(value))
        return path, value

    def test_capacity_is_optional_until_installed_then_missing_failed_or_stale_is_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            current = capacity_sample(root, now_ms=100000)
            self.assertFalse(current['installed'])
            self.assertFalse(should_pause_capacity(True, current))
            path, value = self.capacity_fixture(root)
            path.unlink()
            self.assertTrue(should_pause_capacity(False, capacity_sample(root, now_ms=100000)))
            for changed in (dict(value, state='FAILED'), dict(value, updated_unix_ms=9999),
                            dict(value, updated_unix_ms=100001)):
                path.write_text(json.dumps(changed))
                result = capacity_sample(root, now_ms=100000)
                self.assertTrue(result['installed'])
                self.assertFalse(result['valid'])
                self.assertTrue(should_pause_capacity(False, result))

    def test_capacity_requires_exact_source_signed_equations_and_boolean_types(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); path, value = self.capacity_fixture(root)
            self.assertEqual(capacity_sample(root, now_ms=100000)['recovery_headroom_bytes'], -1)
            for changed in (dict(value, source=dict(value['source'], node_identity='other')),
                            dict(value, recovery_headroom_bytes=0), dict(value, snapshot_headroom_low=False),
                            dict(value, snapshot_capture_reserve_at_risk=0), dict(value, free_disk_bytes=True),
                            dict(value, next_archive_budget_bytes=1)):
                path.write_text(json.dumps(changed))
                self.assertFalse(capacity_sample(root, now_ms=100000)['valid'])

    def test_capacity_resume_needs_thirty_two_gibibytes_of_hysteresis(self):
        def current(headroom):
            return dict(installed=True, valid=True, recovery_headroom_bytes=headroom)
        self.assertTrue(should_pause_capacity(False, current(-1)))
        self.assertFalse(should_pause_capacity(False, current(0)))
        self.assertTrue(should_pause_capacity(True, current(32 * 1024**3 - 1)))
        self.assertFalse(should_pause_capacity(True, current(32 * 1024**3)))

    def test_capacity_rejects_symlink_and_oversized_report_without_changing_it(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); path, _ = self.capacity_fixture(root)
            target = path.with_name('retained.json')
            path.rename(target); path.symlink_to(target)
            before = target.read_bytes()
            self.assertFalse(capacity_sample(root, now_ms=100000)['valid'])
            self.assertEqual(target.read_bytes(), before)
            path.unlink(); path.write_bytes(b' ' * (live_priority.MAX_CAPACITY_BYTES + 1))
            self.assertFalse(capacity_sample(root, now_ms=100000)['valid'])

    def test_background_roles_cannot_select_live_validator_or_feed_units(self):
        self.assertEqual(background_role(live_priority.CAR_UNIT), 'disk_consumer')
        self.assertEqual(background_role(live_priority.OLD_STORAGE_UNIT), 'native_reclaimer')
        self.assertEqual(background_role('hivezilla-native-cold-storage-20261016-source-18de229d.service'),
                         'native_reclaimer')
        for unit in ('hivezilla-validator.service', 'hivezilla-direct-worker.service',
                     'hivezilla-bank-v2-r2-publisher.service', 'hivezilla-bank-v2-producer.service',
                     'hivezilla-native-cold-storage-20261012-live.service'):
            with self.subTest(unit=unit), self.assertRaises(argparse.ArgumentTypeError):
                background_role(unit)

    def test_capacity_pauses_only_car_and_leaves_healthy_native_reclaimers_running(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); self.capacity_fixture(root)
            extra = 'hivezilla-native-cold-storage-20261016-source-18de229d.service'
            def systemd(args, text):
                return 'active' if 'ActiveState' in args else 'running'
            with mock.patch('live_priority.checked_sample', return_value=dict(
                     healthy=True, slot_lag=0, event_lag=0, source='0' * 32 + '-1')), \
                 mock.patch('live_priority.time.time', return_value=100), \
                 mock.patch('live_priority.subprocess.check_output', side_effect=systemd), \
                 mock.patch('live_priority.subprocess.run') as control, \
                 mock.patch('live_priority.archive.save') as save, \
                 mock.patch('live_priority.time.sleep', side_effect=RuntimeError('one iteration')):
                with self.assertRaisesRegex(RuntimeError, 'one iteration'):
                    live_priority.main(root, root / 'priority', [extra])
            control.assert_called_once_with(['sudo', '-n', 'systemctl', 'freeze', live_priority.CAR_UNIT], check=True)
            status = save.call_args.args[1]
            self.assertEqual(status['state'], 'BACKGROUND_PAUSED')
            self.assertTrue(status['capacity_paused'])
            self.assertTrue(status['disk_consumers_paused'])
            self.assertFalse(status['native_reclaimers_paused'])
            self.assertEqual(status['background_roles'][extra], 'native_reclaimer')

    def test_live_lag_still_pauses_consumers_and_reclaimers_together(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            def systemd(args, text):
                return 'active' if 'ActiveState' in args else 'running'
            with mock.patch('live_priority.checked_sample', return_value=dict(healthy=True, slot_lag=33, event_lag=0)), \
                 mock.patch('live_priority.subprocess.check_output', side_effect=systemd), \
                 mock.patch('live_priority.subprocess.run') as control, \
                 mock.patch('live_priority.archive.save') as save, \
                 mock.patch('live_priority.time.sleep', side_effect=RuntimeError('one iteration')):
                with self.assertRaisesRegex(RuntimeError, 'one iteration'):
                    live_priority.main(root, root / 'priority')
            self.assertEqual({call.args[0][-1] for call in control.call_args_list}, set(live_priority.UNITS))
            self.assertTrue(save.call_args.args[1]['native_reclaimers_paused'])

    def test_extra_background_units_cannot_target_live_services_or_options(self):
        unit = 'hivezilla-native-cold-storage-20261016-source-18de229d.service'
        self.assertEqual(background_unit(unit), unit)
        for invalid in ('hivezilla-validator.service', 'hivezilla-direct-worker.service',
                        'hivezilla-native-cold-storage-20261012-live.service', '--all',
                        unit + '\n', unit + ' --no-block', '../' + unit):
            with self.subTest(unit=invalid), self.assertRaises(argparse.ArgumentTypeError):
                background_unit(invalid)

    def test_source_pin_overrides_legacy_and_changes_only_by_explicit_write(self):
        first = '0' * 32 + '-1'
        second = '0' * 32 + '-2'
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            legacy = root / 'reports/bank-v2-recovery-20261012/RECOVERY.json'
            legacy.parent.mkdir(parents=True)
            legacy.write_text(json.dumps(dict(source=dict(session_id=first))))
            before = legacy.read_bytes()
            self.assertEqual(select_source(root)['session_id'], first)
            pin = root / 'config/native-active-source.json'
            pin.parent.mkdir()
            source = dict(genesis_hash='genesis', node_identity='identity', agave_revision='revision', session_id=second)
            pin.write_text(json.dumps(source))
            self.assertEqual(select_source(root)['session_id'], second)
            source['session_id'] = first
            pin.write_text(json.dumps(source))
            self.assertEqual(select_source(root)['session_id'], first)
            self.assertEqual(legacy.read_bytes(), before)

    def test_existing_invalid_pin_fails_closed_without_legacy_fallback(self):
        session = '0' * 32 + '-1'
        source = dict(genesis_hash='genesis', node_identity='identity', agave_revision='revision', session_id=session)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            legacy = root / 'reports/bank-v2-recovery-20261012/RECOVERY.json'
            legacy.parent.mkdir(parents=True)
            legacy.write_text(json.dumps(dict(source=dict(session_id=session))))
            pin = root / 'config/native-active-source.json'
            pin.parent.mkdir()
            for invalid in ({}, {'source': source}, {**source, 'session_id': '../../outside'},
                            {**source, 'node_identity': None}, {**source, 'extra': 'ignored'}):
                with self.subTest(value=invalid):
                    pin.write_text(json.dumps(invalid))
                    with self.assertRaises((RuntimeError, KeyError, TypeError)):
                        select_source(root)
                    current = checked_sample(root, CaptureProgress())
                    self.assertFalse(current['healthy'])
                    self.assertTrue(should_pause(False, current['slot_lag'], current['event_lag'], current['healthy']))
            pin.write_text('invalid JSON')
            with self.assertRaises(ValueError):
                select_source(root)
            pin.unlink()
            pin.symlink_to(pin.with_name('missing.json'))
            with self.assertRaises(RuntimeError):
                select_source(root)

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

