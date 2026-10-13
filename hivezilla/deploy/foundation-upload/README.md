# Foundation data backups

Destination bucket: `solana-archive-data`.

Current upload scope, confirmed by the owner on 2026-10-05: upload only
CAR-derived Compact V2 archives. The native snapshot upload was stopped and
disabled. Its partial remote backup and all local data remain in place. Do not
start that backup service again without a new instruction from the owner.

Later on 2026-10-05 the owner requested publication of new live compact blocks
from the Foundation machine. A separate Rust stream publisher now saves data
from the start of epoch 1050 under `native-stream/direct-agave-live-v1/SESSION`.
The earlier finite native snapshot stays disabled. The CAR query copy retains
its selected scope and separate upload job. See the
[stream publication record](../../stream-relay/README.md#foundation-r2-publication-and-stream-repair-5-october-2026)
and the [native deployment record](../direct-agave/README.md).

The owner selected **query data with signatures** on 2026-10-05. The archive
upload resumed with profile `query-with-signatures` and is enabled for reboot.
Its selected source set is 92,898,238,532,602 bytes (92.90 TB), 12.60 TB smaller
than all stored files. This selection excludes PoH and shred evidence and the
three optional serving files from future uploads. It retains all signature
bytes and the query files. Local files and previously uploaded cloud extras
remain in place. Live compaction was not changed.

The alternatives from the size review were:

| Cloud copy | Source bytes | Approximate TB | Omitted files |
| --- | ---: | ---: | --- |
| Query data with signatures (**selected**) | 92,898,238,532,602 | 92.9 | PoH, shredding evidence, and the three optional serving files |
| Query data without signatures | 54,862,781,783,738 | 54.9 | The same files plus `signatures.bin` |
| All stored compact files | 105,496,762,499,833 | 105.5 | None |

The reader's four mandatory objects total 54,509,908,394,736 bytes (54.51 TB).
The query profiles also keep the small hash and key lookup files and supporting
records. Reader admission permits optional signatures, PoH, and shred records.
Omitting signatures removes signature reads from that cloud copy; it does not
make signature-dependent applications work with fewer bytes. Omitting PoH and
shred records removes their evidence from the cloud copy. The optional serving
blob is rebuildable and repeats signatures and resolved keys; it is not a small
index. The existing source measurements are in `scope-20261005.json`, and exact
proposed file exclusions are in `file-selection-proposal-20261005.json`.

The existing source credential file is
`/Users/augustin/Developement/ferno/ferno-btc/secrets/fondation-s3.txt`.
Its endpoint contains the Foundation R2 account and bucket. The upload jobs use
separate private Rclone configurations. They do not use the archive machine's
older personal R2 account. No secret values are in this repository.

## Stored compact archives

`blockzilla-foundation-archive-upload.service` is a user service on
`ach@192.168.1.46`, SSH port 22. It is enabled for reboot and starts from its
durable plan after failure. It copies the stored epoch directories in descending
epoch order. The initial plan has 1,047 epochs, 0 through 1046, with about
105.34 TB of files. Epoch 1047 is added after its separate CAR producer publishes
the checked import receipt. The job retains every local archive and CAR.

On 2026-10-05 the checked inventory contained 1,048 epochs, 0 through 1047,
totaling 105,496,762,499,833 bytes (105.50 TB). Compressed block frames account
for 53.76 TB, signatures 38.04 TB, PoH data 7.51 TB, and optional compact access
data 4.44 TB. The remaining data supplies compact dictionaries, indexes, and
supporting records. Raw CAR files are outside this upload source.

The active controller is `archive_upload-query-signatures-fast-20261005.py`. It accepts only
known Compact V2 filenames and their supporting records. It excludes locks and
the five known `partial-loop-restart-TIMESTAMP` directories. It rejects unknown
files, unknown directories, and symbolic links. All 1,048 query inventories
passed the compact checks. The selected file identities matched all eight old
upload intents. The source files and old upload state were not rewritten.

The remote prefix is `compact-v2/blockzilla-20261004/epoch-N`. The job preserves
the bucket's existing `compact-v2/0` prefix.

The job directory is
`/volume1/blockzilla/jobs/foundation-archive-upload-20261004`. The active
`state-query-signatures/PLAN.json` binds the source, frozen epoch list, destination,
and query profile. The original `state/` remains historical. Each epoch has a durable
file inventory and completion receipt. The copy checks the compact index header,
index length, blob length, stable source file identities, remote object names and
sizes, and an exact read-back of the small final query manifest. The archive
versions and existing source receipts remain in the copied data. A backup
receipt does not certify canonical publication or full archive coverage.

The query job reuses existing remote objects. Its completion receipt accounts
only for the selected query files. It allows retained PoH/shred/serving objects
and the old backup manifest in an already populated epoch prefix, and records
their names and sizes as extras. Unknown remote objects or wrong selected-file
sizes still stop the copy. It does not delete those extras or replace the
original backup receipt. `BLOCKZILLA-QUERY-WITH-SIGNATURES-MANIFEST.json` is
published last, read back exactly, and reused after a lost reply. The old all-file
backup uses the separate `BLOCKZILLA-BACKUP-MANIFEST.json` name. The selected
92.90 TB size is not a claim about total bucket usage; previously uploaded extras
and the stopped native backup still occupy space.

The initial service used one file transfer, two 64 MiB multipart buffers, an
80 MiB/s bandwidth ceiling, one CPU core at most, and a 512 MiB memory limit.
On 2026-10-05 these conservative throughput settings were reviewed after the
owner questioned the month-long upload estimate. The port negotiated 10 Gbit/s,
CPU quota was not throttling during the sample, and disks had spare capacity.
The memory limit caused cache reclaim, with no OOM event. Two Cloudflare upload
connections were strongly limited by receiver window availability. This is a
sample, not proof of the internet link's maximum throughput.

A bounded test uploaded the next approved 1,224,689,216-byte `registry.bin`
object from epoch 1038, with eight 32 MiB multipart parts and no bandwidth cap.
The existing main job continued during the test. Transfer samples reached
111–114 MiB/s; overall elapsed time, including startup and commit, was 13.51
seconds, or 86.44 MiB/s. The remote length matched the source. That object is
part of the approved query set and will be reused by the bulk job. No arbitrary
test object or extra copy was added. This short test does not prove sustained
whole-archive speed.

The active service now has two file transfers, eight 32 MiB upload parts per
file, a 400 MiB/s bandwidth ceiling, at most four CPU cores, and a 2 GiB memory
limit. Its multipart buffer budget is normally 512 MiB; other process and cache
memory is additional. These settings follow the upload concurrency and memory
model in [Rclone's multipart documentation](https://rclone.org/s3/#multipart-uploads).
The query source selection, destination, durable intents, checks, and existing
receipts are unchanged. Transfer retries remain bounded, with three failure
starts per hour. The older controller remains available for recovery.

After the main job restarted, its payload samples advanced from 153.88 to
181.75 and 186.46 MiB/s. Over 30 seconds the interface transmitted 197.66 MiB/s
and received only 0.19 MiB/s, confirming that the bulk traffic was upload.
No CPU throttling, memory-limit event, or OOM event occurred in that sample;
current charged memory was about 345 MB. This is about six times the earlier
30 MiB/s payload rate. At 180 MiB/s the remaining selected archive would take
about six days. This replaces the month-long estimate only if the higher speed
continues; it is not a completion guarantee. See `evidence/throughput-20261005.json`.

## Native compact snapshot

`hivezilla-foundation-native-backup.service` is a separate system service on
`ubuntu@213.239.141.61`. It was enabled for reboot on 2026-10-04, then stopped
and disabled on 2026-10-05 at the owner's scope correction. Its release is
`ROOT/releases/foundation-upload-20261004-r2`, where
`ROOT=/home/ubuntu/live/hivezilla-validator-20260930`.

The job freezes a list from the read-only native commit databases. The first
snapshot contains 363,496 committed chunks and 518,636 blocks from three source
sessions, ending at slot 453348420. These are stored blocks with known gaps;
they do not constitute a full continuous epoch. Later native commits are outside
this finite snapshot backup.

The remote prefix is `native-backup/direct-agave-20261004`. The job groups compact
chunks into standard tar files, normally about 256 MiB each. It retains every
original compact marker, source evidence file, and normalized receipt. Each pack
also has `PACK-MANIFEST.json`. The external pack manifest records the tar SHA-256
computed during pack creation. It preserves `canonical=false` and
`publication_ready=false`.

The job checks each selected chunk against its durable commit range, native
marker contract, listed file lengths, and source file identities. It checks the
remote tar length and reads back the small pack manifest. It does not do a full
remote tar read-back or a new whole-source hash pass. The source hashes remain
those in the worker's existing receipts. A later restore can verify the pack
SHA-256 and original file hashes.

The job state is `ROOT/reports/foundation-native-backup-20261004`. SQLite commits
the snapshot, pending pack, and uploaded pack state. After a verified size and
manifest commit, only the job's temporary tar file is removed. The original
compact output remains. The job preserves a 512 GiB free-disk reserve, uses one
CPU core at most and a 1 GiB memory limit, and has a 32 MiB/s bandwidth ceiling.
It does not restart the validator, worker, recorder, origin, relay, or consumer.

The first backup reader used the marker's numeric `blocks` count as a list.
Release r2 reads `produced_blocks` and checks the numeric count and slot bounds.
The original snapshot and failed release remain available. Only the separate
backup service was stopped and started for that repair.

This snapshot backup does not configure a public native archive API, live R2
publication, retention of new native output, or source deletion.

## Validation

Run the focused recovery checks with:

```sh
python3 -m unittest discover -s hivezilla/deploy/foundation-upload -p 'test_uploads.py' -v
```

Three checks passed: a remote size mismatch cannot complete an archive copy,
a changed native source cannot be packed, and a lost upload reply resumes the
same tar bytes without removing the source. The native check also restores the
tar payload and preserves zero-length signature files.

Four checks passed after the upload scope correction. The added check rejects
raw CAR files and unexpected directories, excludes old restart data, and rejects
a symbolic link that has the name of a restart directory.

Six checks passed after the query selection. The two added checks establish
that signature files are required, a wrong remote signature length prevents
completion, common uploaded payloads are reused, old extra objects are retained,
and a lost query manifest reply resumes with the identical manifest bytes.

At first live verification, the uploaded epoch-1046 index was 22,438,504 bytes.
Its header and first row matched the source. Full epoch and native snapshot
completion must be read from their durable receipts, not inferred from running
services.

A later check found seven native packs complete, totaling 1,909,862,400 bytes.
A 1,901-byte member from the first remote tar matched its retained source and
original receipt SHA-256. No backup OOM or OOM-kill event was present. The native
worker was one slot behind and the validator PID remained 1106933. The archive
transfer had advanced to 8.794 GiB, and the epoch-1047 CAR download was at
72 percent. These are progress samples, not final completion receipts. See
`evidence/startup-20261004.json`.

At the 2026-10-05 check, epochs 1041 through 1047 had completed upload receipts,
totaling 1,007,131,291,113 bytes. Epoch 1047 finished compaction at 00:49:05
Europe/Paris; its verified uploaded archive is 153,003,758,757 bytes. Epoch 1040
was uploading. The native job stopped after 1,931 complete packs with one pending
pack; its `STOPPED-BY-OWNER-20261005.json` records the decision. The validator and
live compact worker were not restarted. See `evidence/scope-20261005.json`.

After the query job resumed, epochs 1044 through 1047 had separate query
completion receipts totaling 496,152,879,885 bytes at the first sample. The
epoch-1047 query set is 131,563,659,105 bytes. One 64-byte signature read from R2
matched the retained local source. The job was active without restarts and was
checking epoch 1043. Full query upload completion is still pending. See
`evidence/query-signatures-20261005.json` and the live query state.

## Cloud completion checked on 12 October 2026

The finite query archive upload is complete. All 1,048 query manifests for
epochs 0 through 1047 are present in Foundation R2. Their selected remote files
total 92,898,238,532,602 bytes, exactly the frozen source total. The final epoch-0
manifest was stored at 06:40:26 Europe/Paris. Three final manifests were read and
matched their selected remote file sizes. No full payload read-back was done.

Epochs 1048 through 1054 have no objects in this completed batch's prefix.
Epoch 1054's public CAR is available. Adding later archives needs a new checked
source plan; the completed finite plan does not follow new epochs.

Foundation's separate processed publisher has stored compact frames through
part of epoch 1055. Its current source catalog last advanced at 16:28:33
Europe/Paris, with 311,864 blocks and 151,819,048,051 bytes. At the later 18:42
checks, v1 and v2 public heads reported failure and neither the source nor cloud
catalog advanced. The last native confirmation was at 12:47:57. This is not a
complete epoch query archive. At that check, the failure cause was not verified
because direct machine access failed. See `evidence/status-20261012.json`.

## Foundation flow recovery, 12 October 2026

After the owner loaded the SSH identity, direct access worked. The machine had
less than its 400 GiB capture reserve. Closed benchmark payloads were repacked
with lossless zstd. Each recovered file's SHA-256 and length were checked before
the uncompressed names were retired. Hardlinks, source data and reports were
preserved. Unused compiler dependency caches were also removed; live binaries,
source, fixtures and debug executables remain.

The failed capture was replaced with explicit source
`000000000000000018ddd9a6bb6fc50e-282459`. Validator PID 2630745 did not change.
Capture, compaction, the public v1/v2 streams and processed R2 publication
advance. The public payload and cloud receipts are in `evidence`.

The owner then approved storage of native validation evidence and release of
verified local copies. The new `native-custody/processed-v1/SESSION` prefix
preserves exact native compact files, custody and their markers. The helper
reads back the full uploaded bundle and checks its hash before any local space
release. One cloud restore test recovered all 16 original files in a bank.
The services keep at least two hours and protect replay, cloud publication and
writer acknowledgement boundaries. Bundles are at most 512 MiB and 64 banks.
Two transfers can run at once; SQLite commits and local retirement stay on one
owner thread. Pending banks cannot enter a second transfer. Eight storage
tests include a paused remote verification that leaves all local files and
SQLite records unchanged until the verified result returns.
The worker can restore exact native evidence before a later callback audit.
This new storage process is separate from the disabled finite snapshot backup.

`hivezilla-foundation-car-backfill-20261012.service` is enabled on Foundation.
Its frozen plan is CAR epochs 1048–1054, in ascending order. Each epoch uses:

1. Published recap, CID and slot records, exact source length, and an official
   SHA-256 checked by the resumable downloader.
2. A private tmpfs input cache for one CAR. The job pauses for disk/memory
   reserves and can discard only its refetchable memory cache under pressure.
3. Eight decoder workers, normal registry construction, previous epoch hash
   sidecars, and hot Compact V2 conversion without access blobs.
4. Exact source slot, transaction, signature length and blob/index coverage.
5. Immutable query/signature upload to `compact-v2/blockzilla-20261004/epoch-N`.
   Remote file sizes and the final query manifest are checked. No full remote
   CAR-query payload readback is added.
6. Release of this job's temporary output and CAR after the remote commit check.
   Previous epoch hash sidecars remain for the next conversion. Existing NAS
   archives, source CARs and native data outside this job remain intact.

The job root is `ROOT/jobs/car-backfill-20261012`. Read `STATUS.json`,
`controller.log`, `epoch-N.log`, the coverage receipts and `upload-state`.
`backfill.py` and `test_backfill.py` are the checked controller and tests.
Source retry, restart and receipt rules preserve completed uploads. The input
cache is temporary; after a host restart, its official CAR can be downloaded
again. CAR 1048 completed its official SHA-256 download check and started
compaction. It was paused at about 4.2% while live conversion caught up, then
resumed after the worker repair. Import/upload completion remains pending.
The finite plan does not follow epochs after 1054. Public CARs 1055 and 1056
returned 404 at the 21:46 UTC check on 12 October.

The live priority guard freezes this job and the old native cold-storage drain
if native capture or conversion is unhealthy, compact slot lag exceeds 32, or
the cloud queue exceeds 8,192 events. It resumes with slot lag at most eight
and queue at most 4,096 events. The separate live cold-storage process keeps
running. The RPC gap backup uses its own low resource limits and is excluded
from freezing because old RPC blocks can be pruned while it waits.
Read `ROOT/reports/live-priority-20261012/STATUS.json` and
`evidence/background-load-20261012.json` for the bounded load check. Finite
samples do not establish sustained capacity or automatic source changes.

At 00:17 Europe/Paris on 13 October, native slot lag was zero, the cloud queue
was about 150 events and CAR 1048 was about 73% compacted. Live cold storage
had retired 33,216 verified banks, releasing 126.47 GB in that run. Both the
live and old native storage services were active. The resumable RPC backup
retained 41,405 blocks; 18,940 originally listed produced blocks and the RPC
following-boundary check are still missing after pruning. Its `INCOMPLETE`
receipt preserves this limit and all saved data. The earlier missing prefix
is separate. CAR upload completion for 1048–1054 remains pending.

Current live storage units are `hivezilla-native-cold-storage-20261012-live`
and `hivezilla-native-cold-storage-20261012-old`. Read each session's
`compact/direct-agave-processed-live/SESSION/cold-storage/STATUS.json`.
Server install/restore reports are under `ROOT/reports/bank-v2-recovery-20261012-tools`.
This deployment has finite flow and restore checks. Full native gap recovery,
invalid fork retention, automatic source changes, sustained capacity and public
archive query integration remain open. The separate RPC gap backup preserves
available blocks and explicitly records a pruned prefix.

Evidence: `evidence/recovery-flow-20261012.json`,
`evidence/native-cloud-restore-20261012.json`,
`evidence/native-retention-install-20261012.json`, and
`evidence/car-backfill-plan-20261012.json`.
