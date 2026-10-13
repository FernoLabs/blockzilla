import concurrent.futures
import importlib.util
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('scan_controller', Path(__file__).with_name('debridge-two-ended-scan.py'))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class ClaimTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.claims = module.Claims(Path(self.temp.name) / 'claims.sqlite')
        self.claims.initialize(893, 900, [])

    def tearDown(self):
        self.temp.cleanup()

    def complete(self, owner, epoch):
        self.claims.complete(owner, epoch, {'state': 'complete', 'epoch': {'epoch': epoch, 'state': 'complete', 'next_row': 10, 'total_rows': 10}})

    def test_opposite_order_and_restart_keeps_active_claim(self):
        self.assertEqual(self.claims.claim('megaloadon'), 893)
        self.assertEqual(self.claims.claim('nas'), 900)
        reopened = module.Claims(self.claims.path)
        self.assertEqual(reopened.claim('megaloadon'), 893)
        self.assertEqual(reopened.claim('nas'), 900)

    def test_faster_worker_can_take_more_and_meeting_does_not_overlap(self):
        nas = self.claims.claim('nas')
        foundation = []
        while (epoch := self.claims.claim('megaloadon')) is not None:
            foundation.append(epoch)
            self.complete('megaloadon', epoch)
        self.assertEqual(foundation, list(range(893, 900)))
        self.assertNotIn(nas, foundation)
        self.complete('nas', nas)
        self.assertIsNone(self.claims.claim('nas'))
        self.assertTrue(all(r['state'] == 'complete' for r in self.claims.rows()))

    def test_simultaneous_claim_is_atomic(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            epochs = list(pool.map(self.claims.claim, ['megaloadon', 'nas']))
        self.assertEqual(set(epochs), {893, 900})
        self.assertEqual(sum(r['state'] == 'claimed' for r in self.claims.rows()), 2)

    def test_multiple_forward_workers_keep_separate_epochs(self):
        owners = ['megaloadon', 'megaloadon2', 'megaloadon3', 'nas']
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            epochs = list(pool.map(self.claims.claim, owners))
        self.assertEqual(set(epochs), {893, 894, 895, 900})
        self.assertEqual(epochs[-1], 900)
        for owner, epoch in zip(owners, epochs):
            self.assertEqual(self.claims.claim(owner), epoch)

    def test_wrong_owner_or_partial_receipt_cannot_release_claim(self):
        epoch = self.claims.claim('nas')
        with self.assertRaises(RuntimeError):
            self.complete('megaloadon', epoch)
        with self.assertRaises(RuntimeError):
            self.claims.complete('nas', epoch, {'state': 'complete', 'epoch': {'epoch': epoch, 'state': 'complete', 'next_row': 9, 'total_rows': 10}})
        self.assertEqual(self.claims.claim('nas'), epoch)

    def test_old_seed_does_not_reopen_completed_epoch(self):
        seed = [{'epoch': 900, 'state': 'scanning', 'next_row': 5, 'total_rows': 10}]
        self.claims.initialize(893, 900, seed)
        self.complete('nas', 900)
        self.claims.initialize(893, 900, seed)
        self.assertEqual(self.claims.claim('nas'), 899)

    def test_changed_range_fails(self):
        with self.assertRaises(RuntimeError):
            self.claims.initialize(893, 901, [])


if __name__ == '__main__':
    unittest.main()
