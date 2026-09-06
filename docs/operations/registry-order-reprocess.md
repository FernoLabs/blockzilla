# Registry-order reprocessing runbook

This runbook converts a completed Compact V2 epoch from the experimental
`first_seen_v1` registry order to the historical direct-CAR `usage_sorted`
policy. It is a generation migration, not a repair: the source remains
readable and must stay immutable throughout the operation.

## Scope and invariants

The [2026-08-04 completion audit](archive-completion-audit-2026-08-04.md)
found 23 completed first-seen epochs:

```text
277–281, 301–305, 401–405, 501–505, 864, 997, 1000
```

Do not reprocess the 971 epochs already classified `usage_sorted`. Of those,
969 were built by the direct-CAR historical path whose count domain this
migration reproduces. Epochs 998 and 999 were built by the live path and use a
different count domain that includes block-level rewards; they are already
usage-sorted and are outside this migration. The scheduler accepts only epochs
classified both `Complete` and `FirstSeen`.

The safety rules are:

1. Never modify or replace `ARCHIVE_ROOT/epoch-N`.
2. Write `TARGET_ROOT/epoch-N` as a separate immutable generation. The default
   target root is `ARCHIVE_ROOT/.usage-sorted-generations`.
3. Publish through same-parent staging and an atomic no-clobber rename. An
   existing target is either independently validated and accepted or blocks
   the job; it is never overwritten.
4. Treat the scheduler's terminal `complete` marker as the release gate. A
   child exit, a directory, or a receipt by itself is not proof of success.
5. Do not make this target canonical until an archive catalog/current-pointer
   protocol exists. Consumers must open the exact target path explicitly.

Epoch 0 is outside this workflow. The experimental first-seen builder does not
support its genesis record, and the reprocessor must reject epoch 0 until
genesis handling has a dedicated parity fixture.

## What the conversion guarantees

This is a strict two-pass rewrite, not a sort of `registry_counts.bin`:

- Pass 1 decodes the source blocks, resolves old IDs through the source
  registry, and counts only references eligible under the historical
  direct-CAR usage-sorted policy. Canonical keys are ordered by count
  descending, then by their 32-byte public key ascending as the deterministic
  tie-breaker.
- Pass 2 rewrites every compact-public-key location using the old-ID to new-ID
  map and rebuilds variable-width block/meta/index data. A key that appeared
  only in a first-seen-only location is preserved there as `Raw(pubkey)`; it is
  not retained as an artificial zero-usage registry member.

Historical direct-CAR eligible locations include message account keys and
address-table keys, loaded addresses, token-balance mint/owner/program fields,
transaction metadata rewards, and return-data program IDs. Block-level rewards
and keys found only in structured logs do not contribute to canonical ordering,
though their original public keys remain semantically present through raw
encoding. This exclusion is the explicit migration contract for the 23
first-seen epochs; it must not be generalized to the live-built 998/999 count
domain.

The target gets a rebuilt registry, counts, MPHF, hot-block indexes, and, when
the source has one, a rebuilt registry-dependent block-access attachment.
Registry-independent sidecars may be reflinked or copied byte-for-byte.
Publication writes
`archive-v2-registry-reprocess.receipt.json` last and removes the source-only
first-seen manifest from the target. Its algorithm identity is
`compact_v2_first_seen_v1_to_usage_sorted_historical_car_v1`. Validation binds
the receipt to the exact source and target, checks the canonical registry
structure/order, and verifies decoded source/target semantic parity.

## Scheduler rollout

The lane is deliberately disabled by default:

| Flag | Environment variable | Default | Meaning |
|---|---|---:|---|
| `--registry-reprocess-concurrency` | `BLOCKZILLA_REGISTRY_REPROCESS_CONCURRENCY` | `0` | Maximum new jobs (`0..=64`); zero prevents new rewrites and manual-target audits, while a durable audit continuation from an already-ended owned worker may drain |
| `--registry-reprocess-threads` | `BLOCKZILLA_REGISTRY_REPROCESS_THREADS` | `4` | Worker threads per child (`1..=256`) |
| `--registry-reprocess-memory-mib` | `BLOCKZILLA_REGISTRY_REPROCESS_MEMORY_MIB` | `2048` | Admission reservation per child, not an RSS limit |
| `--registry-reprocess-sort-memory-mib` | `BLOCKZILLA_REGISTRY_REPROCESS_SORT_MEMORY_MIB` | `256` | Sort-memory budget per child (`16..=65536`) |
| `--registry-reprocess-target-root` | `BLOCKZILLA_REGISTRY_REPROCESS_TARGET_ROOT` | `ARCHIVE_ROOT/.usage-sorted-generations` | Separate generation root |

The child also receives the scheduler's `--level` / `BLOCKZILLA_LEVEL`
(default `1`). Global `--memory-reserve-mib` and `--disk-reserve-gib` remain in
force. Registry worker threads also share the scheduler's
`--compact-cpu-budget-cores` ceiling; the total for managed jobs and any
adopted survivor must fit before another child is admitted. Concurrency
multiplies memory and sustained I/O, so start with one job. Disk admission
includes the target generation, a private source-registry snapshot, and the
fixed-width external-sort runs:

```text
BLOCKZILLA_REGISTRY_REPROCESS_CONCURRENCY=1
BLOCKZILLA_REGISTRY_REPROCESS_THREADS=4
BLOCKZILLA_REGISTRY_REPROCESS_MEMORY_MIB=2048
BLOCKZILLA_REGISTRY_REPROCESS_SORT_MEMORY_MIB=256
BLOCKZILLA_REGISTRY_REPROCESS_TARGET_ROOT=/volume1/blockzilla/archive/.usage-sorted-generations
```

Keep `--start-epoch` and `--end-epoch` on one pilot epoch for the first run,
then expand the range deliberately. Epoch 1000 is a useful pilot because the
audit found that it is the only backlog epoch whose source CAR is still
retained, although this Compact-V2-to-Compact-V2 path does not require the
CAR.

The lane waits for acquisition, scan, compact, finalizer, and PoH-migration
work to drain, then applies aggregate CPU, per-child memory, disk, reserve,
inventory-completeness, global-pause, and shared-pressure gates. Rewrite-child
admission reserves the full target and sort-scratch disk projection. A deep
audit is read-only apart from its bounded terminal marker, so it reserves the
configured disk floor plus marker headroom rather than double-counting the
already-published generation. Audit CPU admission comes from the immutable
receipt's clamped validation thread count, not from the controller's current
rewrite-thread setting. It requires an observable Linux process table so
restart ownership can be proven. Inspect these status fields when admission
does not occur:

```text
summary.registry_reprocess_capacity_configured
summary.registry_reprocess_running
summary.registry_reprocess_epochs_total
summary.registry_reprocess_epochs_done
summary.registry_reprocess_admission_blocked_reason
lanes[].kind == "archive_v2_registry_reprocess"
```

Per-epoch operational files are:

```text
STATE_ROOT/registry_reprocess/epoch-N.json
STATE_ROOT/registry_reprocess_locks/epoch-N.lock
STATE_ROOT/progress/epoch-N-registry-reprocess.json
STATE_ROOT/logs/epoch-N-registry-reprocess.log
```

On restart, the scheduler trusts a live-child adoption only when the durable
marker, PID start time, command arguments, source, target, and thread count
agree. A live but incompletely proven process is retained conservatively while
new admission is blocked; wholly unobservable ownership also blocks rather
than risking a duplicate writer. An exact scheduler-managed child that exits
successfully receives the bounded publication-receipt probe; the worker
fully validates the source, computes semantic parity while rewriting blocks,
hashes the exact ordered bytes it writes, binds and syncs every artifact, and
only then publishes that receipt. A trusted durable `complete` marker and an
exactly adopted scheduler child use the same bounded probe on restart. The
probe recomputes both receipt generation digests, checks the exact required
artifact sets and sizes, and binds the receipt's source and target to the
configured epoch paths. Untrusted or uncertain exits and manually discovered
targets first publish a durable `auditing` marker, then enter one owned, serial
background deep-audit queue. A controller restart reconstructs that required
audit from the marker; it cannot fall back to the bounded trusted-completion
probe. The full scan starts only after the scheduler's maintenance-lane and
resource gates admit it and never holds the controller or status mutex. A
pending audit reserves its epoch, so it cannot admit a duplicate worker; an
active audit reserves registry capacity and its resource footprint, and only
the normal reconciliation poll may commit the result. If a worker publishes
and syncs its target but then exits non-zero, a deep-valid target is
authoritative and is marked complete. Before any retry, an existing target is
queued for deep validation: success completes the job without a respawn, while
failure is a durable immutable-target manual incident. Only an absent target
may be retried. Retry starts a new audit-generation identity and does not
promise phase resume: matching staging is safely restarted from scratch.
Per-job pause/resume is not supported for registry reprocessing; use the
scheduler-wide pause before admission. Cancel and explicit failed-job retry
remain the operational controls.
Staging/checkpoint details are internal and must not be manipulated by hand.
The bounded trusted-restart path assumes immutable-generation continuity and
does not rehash payload bytes. Detect same-length post-publication mutation or
bit rot by rerunning `reprocess-archive-v2-registry` with the same source,
target, epoch, and resource flags as an explicit offline audit. Under its OS
lock, an existing target is deep-validated and returned unchanged; it is never
overwritten. Do not put that full scan on the controller's restart mutex.

Do not delete a running marker or lock to force progress. Resolve the process
identity first. An invalid existing target or ambiguous marker is a manual
incident, not permission to overwrite it.

For a one-off diagnostic run, the scheduler invokes the equivalent command:

```sh
blockzilla reprocess-archive-v2-registry \
  "$SOURCE_EPOCH_DIR" "$TARGET_EPOCH_DIR" \
  --epoch "$EPOCH_NUMBER" \
  --threads 4 \
  --sort-memory-mib 256 \
  --level 1
```

Prefer the scheduler for production because it owns admission, locking,
restart adoption, progress-file configuration, and terminal validation.

## Validation gate

Before starting Firewatch, require all of the following:

1. `STATE_ROOT/registry_reprocess/epoch-N.json` is `state: "complete"`, has no
   PID/start-time claim, and names the expected immutable source and target.
2. The scheduler status reports the epoch in the completed total and no active
   registry-reprocess lane for it.
3. `TARGET_ROOT/epoch-N/archive-v2-registry-reprocess.receipt.json` exists.
4. The source is still classified first-seen and unchanged; the target is
   classified usage-sorted and has no `registry-first-seen.manifest`.

For an exact scheduler-managed success, a trusted durable completion on
restart, or the end of an exactly adopted scheduler child, the scheduler writes
or accepts `complete` only after the bounded
`probe_published_reprocess(target, epoch)` succeeds and the receipt source is
the configured `ARCHIVE_ROOT/epoch-N`. This is safe because the worker fully
validates the source, computes rewrite-time semantic parity, hashes the exact
ordered target bytes, binds and syncs all artifacts, and only then makes its
publication-last receipt visible. Uncertain or untrusted exits and manually
discovered targets must pass the deep
`validate_published_reprocess(source, target, epoch)` path. A bare
receipt-existence check is never sufficient. Preserve the child log and
receipt with benchmark results.

## Build and query the Firewatch index

Use the exact target directory validated above. `build-dense` publishes its
own output with no-clobber semantics, so the index output directory must not
already exist.

If the reprocessed target does not yet contain a published
`archive-v2-generation.json`, explicitly use trusted-local mode:

```sh
REGISTRY_REPROCESS_RECEIPT="$TARGET_EPOCH_DIR/archive-v2-registry-reprocess.receipt.json"
TARGET_GENERATION_SHA256="$(
  jq -er '.target_generation_sha256 | strings | select(test("^[0-9a-f]{64}$"))' \
    "$REGISTRY_REPROCESS_RECEIPT"
)"

blockzilla-user-program-index build-dense \
  --epoch "$EPOCH_NUMBER" \
  --archive "$TARGET_EPOCH_DIR" \
  --out "$FIREWATCH_INDEX_DIR" \
  --trust-local \
  --cluster-id mainnet-beta \
  --generation-id "$TARGET_GENERATION_SHA256" \
  --threads 4
```

Before accepting the new index, compare its wallet/program relation set with
the index built from the first-seen source. This parity command resolves IDs
through each archive's own registry and externally sorts raw key pairs, so it
is the semantic gate across different registry numberings:

```sh
index-parity \
  --left-registry "$SOURCE_EPOCH_DIR/registry.bin" \
  --right-registry "$TARGET_EPOCH_DIR/registry.bin" \
  --sort-memory-mib 256 \
  --temp-dir "$SCRATCH_DIR" \
  "$OLD_FIREWATCH_INDEX_DIR" "$FIREWATCH_INDEX_DIR"
```

Then query wallet-to-program relations against that same path:

```sh
blockzilla-user-program-index query \
  --wallet "$WALLET_PUBKEY" \
  --index "$FIREWATCH_INDEX_DIR" \
  --archive "$TARGET_EPOCH_DIR" \
  --trust-local \
  --json
```

Trusted-local binding records the original path and file identities. Its
generation ID above is the receipt's exact target-generation digest, rather
than a reusable epoch label, so the Firewatch index identity is tied to this
specific rewrite. Keep the target immutable and do not rename it between build
and query. Once a valid generation manifest is published, omit `--trust-local`,
`--cluster-id`, and `--generation-id`; queries against an index built in
published mode must also omit `--trust-local`.

## Rollback and cutover

There is no canonical cutover in this release. Rollback is therefore
non-destructive:

1. Set registry-reprocess concurrency back to `0` to stop new admissions.
2. Stop directing Firewatch or other readers at the candidate target/index.
3. Continue serving the untouched `ARCHIVE_ROOT/epoch-N` source.
4. Retain the candidate, receipt, marker, and logs for diagnosis; remove them
   only through a later explicit garbage-collection procedure after proving no
   reader has the generation open.

A future catalog/current-pointer implementation must atomically select a
validated generation and retain the previous pointer for reversal. Until that
exists, never replace the canonical epoch directory, symlink it to the
candidate, or infer canonicality merely from the target's presence.
