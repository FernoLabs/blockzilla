# Archive V2 LegacyPre-to-Post handoff — 2026-08-24

> Point-in-time operational handoff. The status below was updated after the
> final two recovery conversions on 2026-08-24 at approximately 20:05 CEST.
> Recheck live state before any later operation.

## Current result

- All 211 LegacyPre epochs now have a Post candidate and an old-pair backup.
- The clean start-700 batch completed 52 of 52 selected epochs.
- Epoch 103 completed in 18 seconds at 185.14 MiB/s.
- Epoch 104 completed in 19 seconds.
- The final metadata-only check found 211 of 211 candidate descriptors and
  211 of 211 backup directories.
- No converter or conversion lock remains.
- The archive scheduler remains stopped.
- The output is still noncanonical. Canonical audit, manifest work, and final
  publication remain deferred.

Final recovery checks found zero converter processes, no root conversion lock,
and the scheduler inactive.

## Paths and admitted tools

Archive root:

```text
/volume1/blockzilla/archive
```

Control state:

```text
/volume1/blockzilla/archive-v2-pre-to-post-state/canonical-post-v1-20260824-cf2319c3-r2
```

Exact LegacyPre cohort:

```text
.../fast-in-place-candidate/all-legacy-pre-epochs.txt
.../fast-in-place-candidate/all-legacy-pre-reports.tsv
```

Current converter:

```text
/volume1/blockzilla/bin/archive-v2-pre-to-post-1b92f790bdb3c231d9862b2ccebfd5d6dfd492b55438e63b60e524add6c43393
SHA-256 1b92f790bdb3c231d9862b2ccebfd5d6dfd492b55438e63b60e524add6c43393
```

One-epoch runner:

```text
/volume1/blockzilla/bin/run-archive-v2-pre-to-post-fast-canary-1f80e96a68ed792b97b06bce0ee9e78016b2e56e4feaec92533cdf3deecb175a.sh
SHA-256 1f80e96a68ed792b97b06bce0ee9e78016b2e56e4feaec92533cdf3deecb175a
```

Batch supervisor:

```text
/volume1/blockzilla/bin/run-archive-v2-pre-to-post-fast-canary-batch-061f19e7f53b19c243068f016a1fad624a05015e20b936e6a0b3df7ee5b6722f.sh
SHA-256 061f19e7f53b19c243068f016a1fad624a05015e20b936e6a0b3df7ee5b6722f
```

Current batch state:

```text
.../fast-canary-batch-start-700/config.json
.../fast-canary-batch-start-700/legacy-pre-epochs.snapshot
.../fast-canary-batch-start-700/complete.json
```

`complete.json` must not exist until all 52 cohort rows at or above epoch 700
have passed the one-epoch runner. It is a non-canonical candidate completion
record.

## What the fast conversion does

The message transcoder reads the exact historical Pre grammar and changes only
the compact instruction-data enum tags:

```text
0 -> 0
1 -> 3
2 -> 4
3 -> 5
4 -> 6
5 -> 7
6 -> 8
```

Message lengths stay unchanged. The converter:

- reads compressed blocks in increasing file order;
- decodes, validates, transcodes, and compresses blocks in parallel;
- writes blocks and index rows in original row order with one writer;
- keeps the old blocks/index pair in a per-epoch backup;
- publishes a non-canonical candidate descriptor;
- does not create the final canonical manifest or final content hashes.

Canonical hashing and final publication were deliberately deferred until the
new-format indexer has processed the candidates.

## Failures and completed repairs

### Compression output-buffer defect

The first multicore binary could fail when a reused output vector had some
capacity but less than the zstd bound. The reserve calculation used the wrong
amount. The current binary fixes this and passed the converter and reader test
suites. Do not use the older `62059a...` binary.

### Epoch 500 derived-index links

Epoch 500 contained dangling links for derived access/get-block artifacts.
The block audit had succeeded because it reads the core archive files and does
not enumerate optional derived entries. The converter enumerates every direct
entry and rejected the links before it changed blocks.

The same three links existed in epochs 500, 600, 700, and 800. All 12 links
were moved, without following their targets, to these evidence directories:

```text
/volume1/blockzilla/archive/.epoch-500.pre-to-post.edge-symlink-evidence-20260824T1128Z
/volume1/blockzilla/archive/.epoch-600.pre-to-post.edge-symlink-evidence-20260824T1128Z
/volume1/blockzilla/archive/.epoch-700.pre-to-post.edge-symlink-evidence-20260824T1128Z
/volume1/blockzilla/archive/.epoch-800.pre-to-post.edge-symlink-evidence-20260824T1128Z
```

Each record says `canonical:false` and
`edge_rebuild_or_review_required:true`. Rebuild or review these derived indexes
after the new-format indexer is ready.

### Direct work directories in source epochs

Epoch 700 contained two old benchmark directories. Forty-seven later cohort
epochs contained 49 old partial-work directories. The converter rejected these
real directories because direct non-file entries are not part of its admitted
archive inventory.

All 51 directory entries were moved by same-device rename into:

```text
/volume1/blockzilla/archive/.archive-v2-workdir-quarantine-evidence-20260824T1210Z
```

No file content was read or copied. The operation preserved directory inodes.
The completion record binds:

```text
entry_count: 51
plan_sha256: 6c565e241d5c1518fc7a12683312dd83b3a4a59dd4e043708449a3dd159cca76
stat_tree_before/after: a239a0fed09c820924acf0c5e3e4a849ab1a8fe7c879432f8b2817b22cefea56
regular_before/after: 57ee0d0a4ce2e23d763473d2b7a485340e1ccf8c0eee8c6b530789a994f22698
```

The exact 211-epoch cohort had zero direct non-regular entries after this
operation. The record says `canonical:false`, `content_hashed:false`, and
`later_review_required:true`.

Reviewed local sources:

- `scripts/quarantine-archive-v2-cohort-workdirs.sh`
- `scripts/test-quarantine-archive-v2-cohort-workdirs.sh`
- `scripts/archive-v2-pre-to-post-workdir-quarantine-20260824.tsv`

### Monitoring false positive

One monitoring command placed the archive path in the NAS process command
line. The one-epoch runner correctly treated it as a possible reader and
stopped after publishing a valid epoch result. This did not damage an epoch.

While a converter is active, do not place the archive or handoff path in a
remote command argument. For state inspection, start the remote shell as:

```sh
ssh blockzilla.local sh -s
```

Then send paths in standard input, use shell `cd`, and give child commands only
relative paths. Commands that use only a systemd unit name, `ps -C`, `free`,
`vmstat`, or `/sys/block/md1` are also safe.

## Check the detached job after return

First, use checks that do not name the archive path:

```sh
ssh blockzilla.local \
  'systemctl --user is-active blockzilla-pre-post-fast-canary-batch-700-t10-r2.service'

ssh blockzilla.local \
  'ps -C archive-v2-pre-to-post -o pid=,etime=,pcpu=,rss=,args='

ssh blockzilla.local \
  'free -m; vmstat 1 2; cat /sys/block/md1/md/array_state; cat /proc/mdstat; for f in /sys/block/md1/md/dev-*/state; do printf "%s " "$f"; cat "$f"; done'
```

Interpret the result:

- `active`, one converter, and `md1` `clean`: leave the job running.
- inactive with exit status zero and a valid batch `complete.json`: the clean
  suffix is complete.
- inactive with non-zero exit status and no batch completion: inspect the
  failed epoch. Do not start the archive scheduler.
- root lock without a live converter/supervisor: treat it as a stopped recovery
  case. Do not remove the lock without reviewing staging, backup, and result
  state.

Detailed unit status:

```sh
ssh blockzilla.local \
  'systemctl --user show blockzilla-pre-post-fast-canary-batch-700-t10-r2.service -p ActiveState -p SubState -p ExecMainStatus'

ssh blockzilla.local \
  'journalctl --user -u blockzilla-pre-post-fast-canary-batch-700-t10-r2.service -n 100 --no-pager'
```

Normal health limits:

- exactly one converter;
- archive scheduler inactive;
- `md1` clean with all eight members;
- available memory above 1 GiB; normal observed value is about 5 GiB;
- no sustained swap input/output;
- converter process and root lock either both present or both absent.

Do not increase beyond 10 decode workers. A measured 20-worker run improved
throughput by only 0.11% and doubled worker count. Storage is the limit.

The short `vmstat` command is a snapshot. If it reports swap input or output,
sample for at least 30 seconds before deciding that pressure is sustained.
The RAID check must show exactly eight `dev-*` member-state rows and every row
must say `in_sync`.

## If the clean-suffix service failed

Do not remove staging, backup, candidate descriptors, result files, or locks as
a first response.

1. Keep `blockzilla-archive.service` stopped.
2. Record the failed epoch and the unit exit status.
3. Inspect the one-epoch state at:
   `.../fast-canary-epoch-N/`.
4. Check whether the source has a candidate descriptor and whether the backup
   has switch intent and completion records.
5. If the one-epoch result is complete and valid, restart the same batch state
   with a new transient unit name. The runner revalidates completed epochs and
   does not convert them again.
6. If the converter stopped during a switch, use its journal recovery path.
   Do not make manual pair renames.

Use standard input for launch so the initial reader guard cannot see the
archive path in an SSH command argument:

```sh
ssh blockzilla.local sh -s <<'REMOTE'
systemd-run --user --no-block \
  --unit=blockzilla-pre-post-fast-canary-batch-700-t10-resume-1 \
  --collect --property=KillMode=mixed --property=TimeoutStopSec=900 \
  /volume1/blockzilla/bin/run-archive-v2-pre-to-post-fast-canary-batch-061f19e7f53b19c243068f016a1fad624a05015e20b936e6a0b3df7ee5b6722f.sh \
  /volume1/blockzilla/archive-v2-pre-to-post-state/canonical-post-v1-20260824-cf2319c3-r2 \
  700 \
  /volume1/blockzilla/bin/run-archive-v2-pre-to-post-fast-canary-1f80e96a68ed792b97b06bce0ee9e78016b2e56e4feaec92533cdf3deecb175a.sh \
  /volume1/blockzilla/bin/archive-v2-pre-to-post-1b92f790bdb3c231d9862b2ccebfd5d6dfd492b55438e63b60e524add6c43393 \
  10
REMOTE
```

Change the transient unit suffix if that unit name already exists. Keep the
same batch start epoch, converter, runner, and thread count.

## Epochs 103 and 104 — reprocess complete

The old interrupted staging trees were not resumed. They were moved by
same-device rename into this evidence directory:

```text
/volume1/blockzilla/archive/.archive-v2-pre-to-post-interrupted-evidence-103-104-redo-103-104-20260824-v1
```

The admission record binds the old claims, flags, logs, empty reports, audit
reports, staging directory identities, and partial-file identities. Both old
partial files remain in the evidence directory.

Fresh conversion results are in the permanent shadow handoff:

```text
.../fast-in-place-reprocess-103-104/shadow-handoff/fast-canary-epoch-103/result.json
.../fast-in-place-reprocess-103-104/shadow-handoff/fast-canary-epoch-104/result.json
```

For both epochs, the final check confirmed:

- a valid noncanonical canary result;
- a live candidate descriptor;
- an old-pair backup;
- switch intent and completion records;
- no live staging directory;
- exact message-delta proof recorded by the converter.

The old immutable claims, flags, logs, and interrupted staging evidence must
remain. Do not delete the backups or claim canonical publication.

## Clean-suffix completion gate — passed

Before treating the clean suffix as complete, require all of these:

1. `fast-canary-batch-start-700/complete.json` is valid and reports 52 epochs,
   start epoch 700, 10 threads, and `canonical:false`.
2. The transient service exited with status zero.
3. No converter, supervisor, staging directory, or conversion lock remains for
   the clean suffix.
4. Every exact cohort epoch other than 103 and 104 has a valid candidate
   descriptor and validated one-epoch result.
5. The clean suffix completed 209 of 211 epochs, after which epochs 103 and 104
   were freshly reprocessed.
6. All 211 LegacyPre epochs now have a candidate descriptor and backup.
7. `md1` remains clean and the archive scheduler remains stopped.

## Work after all 211 conversions

The fast output is a non-canonical candidate. Do not delete old block pairs or
source generations yet.

Remaining publication work:

1. Process every candidate with the new-format indexer.
2. Rebuild or review the derived access/get-block data recorded in the four
   symlink evidence directories.
3. Review the 51 moved benchmark/partial-work directories. They are not archive
   inputs and do not need to return to the epoch directories.
4. Run the full Post semantic audit over the final candidate set.
5. Compute final content hashes and canonical manifests.
6. Bind each canonical result to its old-pair backup and conversion record.
7. Apply provider-enforced immutability or object lock.
8. Delete old backups only after indexer parity, canonical audit, hashes,
   manifests, and retention approval all pass.
9. Restart `blockzilla-archive.service` only after the selected publication
   boundary is safe for readers.

## Local implementation references

- `blockzilla/src/bin/archive_v2_pre_to_post.rs`
- `crates/blockzilla-format/src/v2/wire_rewrite.rs`
- `crates/blockzilla-read-sdk/src/reader.rs`
- `scripts/run-archive-v2-pre-to-post-fast-canary.sh`
- `scripts/run-archive-v2-pre-to-post-fast-canary-batch.sh`
- `scripts/quarantine-archive-v2-cohort-workdirs.sh`
- `scripts/test-quarantine-archive-v2-cohort-workdirs.sh`
- `scripts/archive-v2-pre-to-post-workdir-quarantine-20260824.tsv`
