"""Sequential CAR import into the Foundation query namespace, with bounded staging."""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import struct
import subprocess
import time
import urllib.request

import archive_upload as archive
from prepare_car_source import prepare_source

GIB = 1024**3
SEED_FILES = {"blockhash_registry.bin", "archive-v2-blocks.index"}
ROOT = Path('/home/ubuntu/live/hivezilla-validator-20260930')
CACHE = Path('/mnt/blockzilla_car_backfill')
DESTINATION = 'foundation:solana-archive-data/compact-v2/blockzilla-20261004'


def memory_available():
    return int(re.search(r'^MemAvailable:\s+(\d+) kB$',
                         Path('/proc/meminfo').read_text(), re.M)[1]) * 1024


def head(epoch, expected):
    url = f'https://files.old-faithful.net/{epoch}/epoch-{epoch}.car'
    req = urllib.request.Request(url, method='HEAD', headers={'User-Agent': 'aria2/1.36.0'})
    with urllib.request.urlopen(req, timeout=30) as response:
        if response.status != 200 or response.geturl() != url:
            raise RuntimeError('unexpected_CAR_source_response')
        if int(response.headers['Content-Length']) != expected:
            raise RuntimeError('CAR_source_size_changed')
    return url


def coverage(folder, control, source, epoch):
    index = folder / 'archive-v2-blocks.index'
    with index.open('rb') as f:
        magic, version, reserved, blocks, blob_bytes, level, flags = struct.unpack('<8sHHQQiI', f.read(36))
        if (magic, version, reserved, flags) != (b'BZV2HIX1', 1, 0, 0):
            raise RuntimeError('unsupported_compact_index')
        slots = [int(x) for x in (control / f'{epoch}.slots.txt').read_text().splitlines()]
        slots = [x for x in slots if epoch * 432000 <= x < (epoch + 1) * 432000]
        if blocks != source['num_slots'] or len(slots) != blocks or index.stat().st_size != 36 + 52 * blocks:
            raise RuntimeError('compact_block_count_mismatch')
        offset = txs = sigs = 0
        for i, slot in enumerate(slots):
            row = struct.unpack('<IQQIIIQQI', f.read(52))
            block, actual_slot, position, compressed, raw, transactions, tx_ordinal, sig_ordinal, signatures = row
            if (block, actual_slot, position, tx_ordinal, sig_ordinal) != (i, slot, offset, txs, sigs):
                raise RuntimeError('compact_coverage_or_ordinal_mismatch')
            if compressed == 0 or raw == 0 or signatures < transactions:
                raise RuntimeError('invalid_compact_block_or_signature_count')
            offset += compressed
            txs += transactions
            sigs += signatures
        if f.read(1) or offset != blob_bytes or (folder / 'archive-v2-blocks.zstd').stat().st_size != offset:
            raise RuntimeError('compact_blob_coverage_mismatch')
        if txs != source['num_transactions'] or (folder / 'signatures.bin').stat().st_size != sigs * 64:
            raise RuntimeError('transaction_or_signature_coverage_mismatch')
    archive.inventory(folder, archive.QUERY_SIGNATURES_PROFILE)
    return dict(blocks=blocks, transactions=txs, signatures=sigs, blob_bytes=blob_bytes,
                source_slots_match=True, complete_blob_coverage=True, signature_bytes_match=True,
                full_signature_cryptographic_check=False, full_remote_readback=False)


class Backfill:
    def __init__(self, job):
        self.job = job.resolve(strict=True)
        if self.job != ROOT / 'jobs/car-backfill-20261012' or not CACHE.is_mount():
            raise RuntimeError('unexpected_backfill_staging_location')
        self.out = self.job / 'output'
        self.state = self.job / 'upload-state'
        self.out.mkdir(exist_ok=True)
        self.state.mkdir(exist_ok=True)
        self.plan = json.loads((self.job / 'PLAN.json').read_text())
        for key, sha in self.plan['tools'].items():
            path = self.job / 'tools' / key
            if hashlib.sha256(path.read_bytes()).hexdigest() != sha:
                raise RuntimeError('pinned_tool_changed')
        self.args = argparse.Namespace(root=str(self.out), destination=DESTINATION,
            config=str(ROOT / 'config/foundation-r2/rclone.conf'), rclone=str(self.job / 'tools/rclone'),
            profile=archive.QUERY_SIGNATURES_PROFILE, bandwidth='400M', transfers=2,
            upload_concurrency=8, chunk_size='32M')
        self.binary = str(self.job / 'tools/blockzilla')

    def status(self, state, epoch, **extra):
        data = dict(state=state, epoch=epoch, updated_unix_ms=int(time.time() * 1000), **extra)
        archive.save(self.job / 'STATUS.json', data)
        print(json.dumps(data), flush=True)

    def release_memory_cache(self, epoch):
        if epoch not in {row['epoch'] for row in self.plan['epochs']} or not CACHE.is_mount():
            raise RuntimeError('invalid_temporary_cache_owner')
        for name in [f'epoch-{epoch}.car', f'epoch-{epoch}.car.aria2']:
            path = CACHE / name
            if path.exists():
                archive.stamp(path)  # Reject links and nonregular paths.
                path.unlink()
        archive.sync_dir(CACHE)
        self.status('CACHE_RELEASED_FOR_LIVE_MEMORY', epoch, public_CAR_can_be_downloaded_again=True)

    def reserve(self, epoch, raw_bytes=0, disk_gib=500):
        while True:
            free = shutil.disk_usage(self.job).free
            memory = memory_available()
            held = sum(p.stat().st_blocks * 512 for p in CACHE.glob('epoch-*.car'))
            if held and memory < 96 * GIB:
                self.release_memory_cache(epoch)
                raise RuntimeError('temporary_CAR_released_to_preserve_live_memory')
            if free >= disk_gib * GIB and memory + held >= raw_bytes + 128 * GIB:
                return
            self.status('WAITING_RESERVE', epoch, free_disk_bytes=free,
                        minimum_free_disk_bytes=disk_gib * GIB, available_memory_bytes=memory,
                        required_memory_bytes=raw_bytes + 128 * GIB)
            time.sleep(30)

    def run(self, epoch, command):
        with (self.job / f'epoch-{epoch}.log').open('a') as log:
            proc = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
            while proc.poll() is None:
                # Stop this lower-priority job before it consumes the native reserve.
                low_memory = memory_available() < 64 * GIB
                if shutil.disk_usage(self.job).free < 475 * GIB or low_memory:
                    proc.terminate()
                    try:
                        proc.wait(timeout=30)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait()
                    if low_memory:
                        self.release_memory_cache(epoch)
                    raise RuntimeError('backfill_stopped_to_preserve_live_reserve')
                time.sleep(5)
            if proc.returncode:
                raise RuntimeError(f'backfill_command_failed_exit_{proc.returncode}')

    def seed(self, epoch):
        folder = self.out / f'epoch-{epoch}'
        if all((folder / name).is_file() for name in SEED_FILES):
            return
        folder.mkdir(exist_ok=True)
        target = f'{DESTINATION}/epoch-{epoch}'
        manifest = json.loads(archive.rclone(self.args, 'cat', target +
            '/BLOCKZILLA-QUERY-WITH-SIGNATURES-MANIFEST.json', capture=True))
        if manifest['epoch'] != epoch or manifest['upload_profile'] != archive.QUERY_SIGNATURES_PROFILE:
            raise RuntimeError('previous_epoch_seed_manifest_mismatch')
        for name in sorted(SEED_FILES):
            if not (folder / name).exists():
                archive.rclone(self.args, 'copyto', target + '/' + name, folder / name,
                               '--immutable', '--size-only')
            if (folder / name).stat().st_size != manifest['source_files'][name][2]:
                raise RuntimeError('previous_epoch_seed_size_mismatch')

    def cleanup(self, epoch, folder, car, control):
        complete = json.loads((self.state / f'epoch-{epoch}/complete.json').read_text())
        remote = json.loads(archive.rclone(self.args, 'cat', complete['destination'] +
            '/BLOCKZILLA-QUERY-WITH-SIGNATURES-MANIFEST.json', capture=True))
        if remote != complete:
            raise RuntimeError('remote_commit_changed_before_staging_release')
        receipt = control / 'cleanup.json'
        if receipt.exists() and json.loads(receipt.read_text())['state'] == 'COMPLETE':
            return
        intent = control / 'cleanup-intent.json'
        if intent.exists():
            files = json.loads(intent.read_text())['files']
        else:
            owned = json.loads((folder / '.hivezilla-pipeline-owned.v1.json').read_text())
            if owned != dict(job=str(self.job), epoch=epoch, temporary_staging=True):
                raise RuntimeError('staging_ownership_mismatch')
            if not (control / 'coverage.json').is_file():
                raise RuntimeError('coverage_receipt_missing')
            files = {p.name: archive.stamp(p) for p in folder.iterdir()
                     if p.name in archive.COMPACT_FILES and p.name not in SEED_FILES}
            archive.save(intent, dict(files=files, verified_remote_manifest=remote))
        removed = 0
        for name, identity in files.items():
            if name not in archive.COMPACT_FILES or name in SEED_FILES:
                raise RuntimeError('unexpected_staging_file')
            path = folder / name
            if path.exists():
                if archive.stamp(path) != identity:
                    raise RuntimeError('staging_file_changed_before_release')
                removed += path.stat().st_blocks * 512
                path.unlink()
        archive.sync_dir(folder)
        if car.exists():
            marker = json.loads((control / 'download.json').read_text())
            if archive.stamp(car) != marker['identity']:
                raise RuntimeError('temporary_CAR_changed_before_release')
            car.unlink()
            archive.sync_dir(CACHE)
        archive.save(receipt, dict(state='COMPLETE', job_staging_only=True,
            compact_disk_bytes_released=removed, predecessor_seed_retained=sorted(SEED_FILES),
            remote_manifest_verified=True, original_NAS_and_native_files_preserved=True))

    def epoch(self, epoch, size):
        control = self.job / f'epoch-{epoch}'
        control.mkdir(exist_ok=True)
        folder = self.out / f'epoch-{epoch}'
        car = CACHE / f'epoch-{epoch}.car'
        if (self.state / f'epoch-{epoch}/complete.json').is_file():
            self.cleanup(epoch, folder, car, control)
            return
        self.status('SOURCE_METADATA', epoch)
        source = prepare_source(control, epoch, size)
        self.reserve(epoch, size)
        self.seed(epoch - 1)
        if not (control / 'download.json').is_file() or not car.exists():
            url = head(epoch, size)
            recap = (control / f'{epoch}.recap.yaml').read_text()
            sha = re.search(r'^car_file_sha256:\s*([a-f0-9]{64})$', recap, re.M)[1]
            self.status('DOWNLOADING', epoch, car_bytes=size, temporary_cache='private_tmpfs')
            self.run(epoch, ['/usr/bin/aria2c', '--continue=true', '--file-allocation=none',
                '--auto-file-renaming=false', '--allow-overwrite=false', '--check-integrity=true',
                '--checksum=sha-256=' + sha, '--dir=' + str(CACHE), '--out=' + car.name,
                '--split=8', '--max-connection-per-server=8', '--min-split-size=64M',
                '--max-download-limit=400M', '--user-agent=aria2/1.36.0', '--max-tries=4',
                '--retry-wait=300', '--auto-save-interval=30', '--summary-interval=30', url])
            if car.stat().st_size != size or car.with_name(car.name + '.aria2').exists():
                raise RuntimeError('incomplete_CAR_download')
            head(epoch, size)
            archive.save(control / 'download.json', dict(identity=archive.stamp(car),
                bytes=size, published_sha256=sha, aria2_full_input_checksum_passed=True,
                temporary_cache=True))
        original = json.loads((control / 'download.json').read_text())['identity']
        if archive.stamp(car) != original:
            raise RuntimeError('downloaded_CAR_changed')
        folder.mkdir(exist_ok=True)
        owned = folder / '.hivezilla-pipeline-owned.v1.json'
        ownership = dict(job=str(self.job), epoch=epoch, temporary_staging=True)
        if owned.exists() and json.loads(owned.read_text()) != ownership:
            raise RuntimeError('output_owned_by_other_job')
        if not owned.exists():
            archive.save(owned, ownership)
        for stage, command in [
            # Registry files can be nonempty after an interrupted direct write.
            # Only our durable stage receipt permits reuse; this command runs
            # only when that receipt is absent and the output belongs to this job.
            ('REGISTRIES', [self.binary, 'build-archive-v2-registries', '--force',
                '--workers', '8', str(car), str(folder)]),
            ('REGISTRY_INDEX', [self.binary, 'build-archive-v2-registry-index', str(folder / 'registry.bin')]),
            ('COMPACTING', [self.binary, 'build-archive-v2-hot-blocks', '--previous-car',
                str(CACHE / f'epoch-{epoch-1}.car'), '--registry-dir', str(folder), '--level', '1',
                '--decode-workers', '8', '--no-access', '--resume', str(car), str(folder)])]:
            done = control / (stage + '.json')
            if not done.exists():
                self.reserve(epoch, size, disk_gib=800)
                self.status(stage, epoch)
                self.run(epoch, command)
                if archive.stamp(car) != original:
                    raise RuntimeError('CAR_changed_during_build')
                archive.save(done, dict(state='COMPLETE', input_sha256=
                    json.loads((control / 'download.json').read_text())['published_sha256']))
        self.status('VALIDATING', epoch)
        checked = coverage(folder, control, source, epoch)
        if not (control / 'coverage.json').exists():
            archive.save(control / 'coverage.json', checked)
        elif json.loads((control / 'coverage.json').read_text()) != checked:
            raise RuntimeError('verified_coverage_changed')
        import_receipt = folder / 'blockzilla-car-import-receipt.json'
        receipt = dict(
            schema='blockzilla-car-import-v1', epoch=epoch, source=source, coverage=checked,
            input=original, converter_sha256=self.plan['tools']['blockzilla'],
            input_sha256=json.loads((control / 'download.json').read_text())['published_sha256'],
            canonical_publication=False, temporary_staging=True)
        if not import_receipt.exists():
            archive.save(import_receipt, receipt)
        else:
            previous = json.loads(import_receipt.read_text())
            if any(previous[k] != receipt[k] for k in ['epoch', 'coverage', 'input_sha256', 'converter_sha256']):
                raise RuntimeError('immutable_import_receipt_changed')
        self.status('UPLOADING', epoch)
        archive.upload(self.args, self.state, epoch)
        self.cleanup(epoch, folder, car, control)
        self.status('EPOCH_COMPLETE', epoch, **checked)

    def follow(self):
        with (self.job / 'LOCK').open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            for row in self.plan['epochs']:
                self.epoch(row['epoch'], row['bytes'])
            self.status('COMPLETE', self.plan['epochs'][-1]['epoch'])


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--job', type=Path, required=True)
    args = p.parse_args()
    Backfill(args.job).follow()
