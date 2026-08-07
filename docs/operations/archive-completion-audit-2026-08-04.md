# Archive completion audit — 2026-08-04

> Point-in-time, read-only audit of the NAS scheduler and archive tree. Do not
> treat the counts below as live status after this date.

## Executive result

- The scheduler reported 994 complete epochs out of 1011.
- Every epoch classified complete is readable as Compact V2 by the production
  scheduler. The current repository also declares Compact V2 payload version
  `2`; there is no mandatory whole-chain schema-version rewrite today.
- The archive population is not layout-policy uniform: 971 completed epochs
  use the canonical `usage_sorted` registry and 23 use the experimental
  `first_seen` layout plus block access. The 23 must be converted to
  `usage_sorted` generation-safely; they are readable, so this is migration
  debt rather than corruption.
- Five epochs were red: 761, 1005, 1006, 1008, and 1009. Epoch 761 has a real
  parser failure. The other four lost their worker during a scheduler restart
  and are in automatic-retry state.
- Epochs 1001–1004 contain completed scan output but are blocked by a marker
  compatibility bug in the running scheduler.
- Compact V2 retains enough data for PoH recomputation. The local
  `verify-archive-v2-poh` command now streams the archive and verifies all PoH
  entries and final blockhashes; cross-epoch continuity and signed-message
  verification remain separate follow-up checks.

## Compact layout inventory

Current Compact V2 constants in `crates/blockzilla-format/src/v2/mod.rs`:

- archive payload version: `2`;
- hot block version: `2`;
- block-access version: `2`;
- log archive version: `2`;
- integer encoding flag: LEB128.

Completed layout-policy counts:

| Layout | Epochs | Meaning |
|---|---:|---|
| `usage_sorted` | 971 | Canonical Compact V2 registry layout, generally without block-access attachment |
| `first_seen` | 23 | Readable experimental layout requiring migration to `usage_sorted` |

The 23 completed first-seen epochs were:

```text
277–281, 301–305, 401–405, 501–505, 864, 997, 1000
```

### Upgrade decision

Do not rewrite the 971 canonical `usage_sorted` epochs. Migrate only the 23
`first_seen` generations. Of those, only epoch 1000 still has its source CAR,
so the existing CAR compactor can rebuild it. The other 22 require a true
Compact-V2-to-Compact-V2 registry remapper; the current repack commands do not
perform that conversion.

The `archive.upgrade` implementation must:

1. read one immutable committed generation;
2. write a separate staging generation;
3. verify source/target semantic parity and all target structure;
4. atomically publish the new generation;
5. retain the old generation until an explicit later garbage-collection step.

For `first_seen` to `usage_sorted`, count ordering is descending usage count
with the public key as deterministic tie-breaker. Build an old-ID to new-ID
map and rewrite every compact pubkey reference in blocks, metadata, rewards,
loaded addresses, return data, logs, and any retained block-access attachment.
PoH, signatures, shredding, and blockhash sidecars are registry-independent
and should be byte-reused. Require semantic parity before publication.

For component-only changes, such as a log codec revision, decode and re-encode
only the affected component and byte-reuse the rest. Never modify a committed
generation in place.

## Incomplete and failed work

### Epoch 761 — real compactor failure

The old NAS compactor stopped at slot `329125060`, 86.334% through the hot
write:

```text
parse vote instruction data
canonical vote instruction decode failed: variant=short len=1 prefix=00
io error: unexpected end of file
```

The current repository implementation already degrades undecodable or
historical vote instruction data to byte-exact `UnknownVote` instead of
failing the archive. Required work:

- build and deploy the current compactor deliberately;
- add/retain a regression fixture for the one-byte `00` vote instruction;
- retry epoch 761 using the canonical CAR and existing registry/predecessor
  sidecars;
- require final filesystem validation before changing it to complete.

The input CAR, registry, registry counts/index, blockhash registry, and previous
blockhash tail are present. No new download is required.

### Epochs 794–799 — runnable priority tail

- 796–799 were actively compacting during the audit.
- 794–795 were queued.
- All six had their CAR, registry sidecars, and previous-blockhash tail.

No prerequisite repair was visible for this range.

### Epochs 1001–1004 — completed scans hidden by marker incompatibility

All four directories contain the hot blocks/index, block access/index, PoH,
shredding, signatures, registries, blockhash index, predecessor tail, and scan
completion marker. Their metadata and first-seen manifests remain under
`.prehot.tmp`, so they still need normal MPHF/finalizer publication.

The marker contains:

```text
timestamp_artifacts=1
```

The running/current scheduler parser rejects unknown marker fields, causing
the misleading message “output exists without a complete reader core or
scan-ready marker.” Fix the marker schema parser to accept the declared
optional field (and test forward-compatible parsing), then run the normal
finalizer. Do not rebuild these multi-hour scans.

### Epochs 1005–1010

- 1005, 1006, 1008, and 1009 were red because their pipeline-owned workers
  disappeared when the scheduler was restarted; they were marked for automatic
  retry rather than reporting a deterministic decode failure.
- 1007 and 1010 were queued.
- Canonical CAR paths existed for all six.
- Epoch 1006 was still actively downloading and had
  `epoch-1006.car.aria2`. It is not compaction-ready yet.
- `epoch-1008.1.car.aria2` is an incomplete duplicate name, not the canonical
  `epoch-1008.car`; audit and remove it only after proving no downloader owns
  it and the canonical CAR is complete.

The scheduler's admission gate must require both a stable finalized CAR and no
matching active `.aria2` state. File existence/non-zero length alone is unsafe.
Record expected source length (and preferably immutable source identity) in an
acquisition receipt, then bind the compaction task to that receipt.

## Archive integrity verifier gap

### What exists today

- Scheduler completion checks validate many structural properties: expected
  files, supported headers/versions, index shapes and offsets, row counts,
  selected artifact relationships, and final metadata.
- Compaction records ordered transactions and signatures, per-entry PoH
  `num_hashes`/hash/transaction count, per-block blockhashes, and predecessor
  blockhash tails.
- `find-poh-gaps` locates CAR blocks without PoH entry references.
- `verify-archive-v2-poh` streams completed Compact V2 directly: it mmaps the
  block/signature files, reuses decode and Merkle buffers, checks entry
  transaction/signature accounting, recomputes entries in parallel, and
  compares the final result with `blockhash_registry.bin`.
- Repair publication performs useful local cross-checks, but it is not a
  whole-epoch or cross-epoch continuity proof.

### Required `archive.verify` checks

Implement these as independent, streamable checks under the common task
protocol described in `scheduler-control-protocol.md`:

1. `structure`
   - Decode every frame with bounded allocation.
   - Validate headers, versions, flags, monotonically ordered slots, indexes,
     offsets, lengths, counts, registry references, and clean EOF.
   - Hash every immutable input artifact into the verification receipt.
2. `poh`
   - Start each block from its resolved previous blockhash.
   - Recompute every entry with Solana-compatible PoH semantics, including the
     ordered transaction mixin for entries containing transactions.
   - Compare every computed entry hash, not only the final blockhash.
   - Ensure entry transaction counts consume exactly the block's ordered
     transactions.
   - Mark external/gap overrides explicitly; never silently label an
     externally supplied hash as recomputed.
3. `blockhash`
   - Require each normal blockhash to equal its final recomputed PoH entry.
   - Validate every block's previous-blockhash reference against the preceding
     produced block (accounting for empty slots).
   - Validate the first produced block against the predecessor tail.
   - Validate the boundary from epoch N's final produced block to epoch N+1's
     first produced block.
4. `signatures` (separate but needed for a full integrity receipt)
   - Rebuild the exact signed message bytes from the compact transaction.
   - Verify every retained Ed25519 signature against its signer key.
   - Check signature counts and ordering against both the block index and
     signature sidecar.

The verifier must stream blocks and sidecars, report raw monotonic counters,
and produce a receipt bound to:

- epoch and slot range;
- verifier build/protocol version;
- archive generation and per-artifact hashes;
- selected checks;
- counts of verified, reconstructed, externally sourced, skipped, and failed
  records;
- predecessor/successor boundary hashes;
- terminal success or the first exact failure location.

## Ordered backlog

1. Finish active 794–799 work without changing its inputs.
2. Make scan-marker parsing compatible with `timestamp_artifacts=1`; finalize
   1001–1004 without rescanning.
3. Prevent compaction admission while a canonical CAR has active `.aria2`
   state; finish epoch 1006 acquisition first.
4. Deploy the current vote fallback with a one-byte regression test and retry
   epoch 761.
5. Reconcile/retry process-loss epochs 1005, 1008, and 1009; then compact 1006,
   1007, and 1010 when their predecessor/input gates are satisfied.
6. Finish streaming `archive.verify structure,blockhash`, including
   cross-epoch boundary receipts.
7. Run the implemented full PoH recomputation against representative real
   Compact V2 epochs and retain performance/parity receipts.
8. Add transaction signature verification and issue immutable verification
   receipts.
9. Implement and verify Compact-V2-to-Compact-V2 remapping for the 22
   `first_seen` epochs whose CAR has already been removed; rebuild epoch 1000
   from CAR, then retire all 23 old generations only after parity passes.
