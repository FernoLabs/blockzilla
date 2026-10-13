import argparse
import hashlib
import json
from pathlib import Path
import struct
import tempfile
import unittest
from unittest import mock

import archive_upload as archive
import backfill as m


class BackfillChecks(unittest.TestCase):
    def test_interrupted_registry_write_rebuilds_before_stage_commit(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cache = root / 'cache'
            cache.mkdir()
            job = m.Backfill.__new__(m.Backfill)
            job.job = root / 'job'
            job.job.mkdir()
            job.out = job.job / 'output'
            job.out.mkdir()
            job.state = job.job / 'upload-state'
            job.state.mkdir()
            job.binary = 'pinned-blockzilla'
            job.status = mock.Mock()
            job.reserve = mock.Mock()
            job.seed = mock.Mock()
            control = job.job / 'epoch-1'
            control.mkdir()
            car = cache / 'epoch-1.car'
            car.write_bytes(b'official checksummed CAR input')
            input_identity = archive.stamp(car)
            input_sha = hashlib.sha256(car.read_bytes()).hexdigest()
            download = dict(identity=input_identity, published_sha256=input_sha)
            archive.save(control / 'download.json', download)
            folder = job.out / 'epoch-1'
            folder.mkdir()
            counts = folder / 'registry_counts.bin'
            calls = []
            interrupted = [True]

            def command(epoch, args):
                calls.append(args)
                if args[1] == 'build-archive-v2-registries':
                    if interrupted[0]:
                        interrupted[0] = False
                        # The native builder writes the count file directly.
                        # A stopped write can leave a nonempty incomplete file.
                        counts.write_bytes(b'partial')
                        raise RuntimeError('registry writer interrupted')
                    # Model the actual CLI reuse rule: nonempty existing files
                    # are reused unless --force is present.
                    if '--force' in args or not counts.exists():
                        counts.write_bytes(b'complete counts')
                elif args[1] == 'build-archive-v2-registry-index':
                    raise RuntimeError('test stops after registry commit')
                else:
                    self.fail(f'unexpected command: {args}')

            with mock.patch.object(m, 'CACHE', cache), \
                    mock.patch.object(m, 'prepare_source', return_value={}), \
                    mock.patch.object(job, 'run', side_effect=command), \
                    mock.patch.object(m, 'head') as head:
                with self.assertRaisesRegex(RuntimeError, 'writer interrupted'):
                    job.epoch(1, car.stat().st_size)
                self.assertEqual(counts.read_bytes(), b'partial')
                self.assertFalse((control / 'REGISTRIES.json').exists())
                with self.assertRaisesRegex(RuntimeError, 'test stops'):
                    job.epoch(1, car.stat().st_size)
                self.assertEqual(counts.read_bytes(), b'complete counts')
                self.assertEqual(json.loads((control / 'REGISTRIES.json').read_text()),
                                 dict(state='COMPLETE', input_sha256=input_sha))
                registry_calls = sum(args[1] == 'build-archive-v2-registries' for args in calls)
                with self.assertRaisesRegex(RuntimeError, 'test stops'):
                    job.epoch(1, car.stat().st_size)
                self.assertEqual(sum(args[1] == 'build-archive-v2-registries' for args in calls), registry_calls)
                head.assert_not_called()
            self.assertEqual(archive.stamp(car), input_identity)
            self.assertEqual(json.loads((control / 'download.json').read_text()), download)

    def test_memory_recovery_releases_only_this_jobs_public_cache(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for name in ['epoch-1.car', 'epoch-1.car.aria2', 'epoch-2.car']:
                (root / name).write_bytes(b'temporary')
            job = m.Backfill.__new__(m.Backfill)
            job.plan = dict(epochs=[dict(epoch=1)])
            job.status = mock.Mock()
            with mock.patch.object(m, 'CACHE', root), mock.patch.object(Path, 'is_mount', return_value=True):
                with self.assertRaisesRegex(RuntimeError, 'cache_owner'):
                    job.release_memory_cache(2)
                job.release_memory_cache(1)
            self.assertFalse((root / 'epoch-1.car').exists())
            self.assertTrue((root / 'epoch-2.car').exists())

    def fixture(self, root):
        folder, control = root / 'output', root / 'control'
        folder.mkdir()
        control.mkdir()
        for name in archive.CORE:
            (folder / name).write_bytes(b'data')
        (folder / 'signatures.bin').write_bytes(b's' * 256)
        (folder / 'archive-v2-blocks.zstd').write_bytes(b'blobblob')
        (folder / 'archive-v2-blocks.index').write_bytes(
            struct.pack('<8sHHQQiI', b'BZV2HIX1', 1, 0, 2, 8, 1, 0) +
            struct.pack('<IQQIIIQQI', 0, 432000, 0, 4, 8, 2, 0, 0, 3) +
            struct.pack('<IQQIIIQQI', 1, 432002, 4, 4, 8, 1, 2, 3, 1))
        (control / '1.slots.txt').write_text('431999\n432000\n432002\n')
        return folder, control, dict(num_slots=2, num_transactions=3)

    def test_exact_slot_transaction_and_signature_coverage(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder, control, source = self.fixture(Path(tmp))
            result = m.coverage(folder, control, source, 1)
            self.assertEqual((result['blocks'], result['transactions'], result['signatures']), (2, 3, 4))
            (folder / 'signatures.bin').write_bytes(b's' * 192)
            with self.assertRaisesRegex(RuntimeError, 'signature_coverage'):
                m.coverage(folder, control, source, 1)

    def test_same_size_wrong_slot_or_ordinal_rejects(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder, control, source = self.fixture(Path(tmp))
            index = folder / 'archive-v2-blocks.index'
            original = index.read_bytes()
            for offset, value in [(36 + 4, 432001), (36 + 32, 1)]:
                data = bytearray(original)
                struct.pack_into('<Q', data, offset, value)
                index.write_bytes(data)
                with self.assertRaisesRegex(RuntimeError, 'ordinal_mismatch'):
                    m.coverage(folder, control, source, 1)

    def test_cleanup_requires_same_remote_commit_and_keeps_seed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            folder, control, source = self.fixture(root)
            job = m.Backfill.__new__(m.Backfill)
            job.job = root
            job.state = root / 'state'
            (job.state / 'epoch-1').mkdir(parents=True)
            job.args = argparse.Namespace()
            archive.save(folder / '.hivezilla-pipeline-owned.v1.json',
                         dict(job=str(root), epoch=1, temporary_staging=True))
            archive.save(control / 'coverage.json', m.coverage(folder, control, source, 1))
            complete = dict(destination='foundation:bucket/prefix/epoch-1', epoch=1)
            archive.save(job.state / 'epoch-1/complete.json', complete)
            car = root / 'absent-temporary.car'
            before = {p.name for p in folder.iterdir()}
            with mock.patch.object(archive, 'rclone', return_value='{}'):
                with self.assertRaisesRegex(RuntimeError, 'remote_commit_changed'):
                    job.cleanup(1, folder, car, control)
            self.assertEqual({p.name for p in folder.iterdir()}, before)
            with mock.patch.object(archive, 'rclone', return_value=json.dumps(complete)):
                job.cleanup(1, folder, car, control)
                job.cleanup(1, folder, car, control)
            self.assertEqual({p.name for p in folder.iterdir()}, m.SEED_FILES)
            self.assertTrue(json.loads((control / 'cleanup.json').read_text())['remote_manifest_verified'])


if __name__ == '__main__':
    unittest.main()
