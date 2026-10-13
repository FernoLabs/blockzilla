# Hivezilla Geyser plugin V1

## Goal

Build a Hivezilla-owned Agave Geyser plugin that emits a durable local stream
from the self-operated, non-voting validator. The first release must run beside
the pinned Yellowstone gRPC plugin. It must not replace Yellowstone until the
two outputs cover the same finalized banks and Hivezilla can recover the same
records after a process restart.

The plugin must not send data to R2 from an Agave callback. A separate uploader
can publish sealed local segments after the local stream has durable headers,
records, and a manifest. The stream must keep enough source evidence to
reconcile forks and incomplete banks before it can create a compact archive.

## Current baseline

The canary runs Agave `v4.3.0` from source revision
`825efd18292aff6ffcf9daa0f7612f21b3531a72`. Its Geyser interface crate is
`agave-geyser-plugin-interface = 4.3.0`. A plugin library must use the same
interface revision and export Agave's `_create_plugin` entry point.

Yellowstone `v16.0.0+solana.4.3.0` now runs on the host with the
[reduced full-block profile](../../hivezilla/deploy/yellowstone-blocks-only/README.md).
Its gRPC and metrics listeners bind to loopback. The profile rejects account
predicates and standalone account and transaction subscriptions. It copies
only the four system accounts required for bank sealing. Transaction, entry,
bank status, metadata, and footer callbacks remain enabled. The local service
is private to the validator host.

The first live test captured 32 finalized blocks in 3.81 seconds. It sealed
33,809 events in a 21,902,281-byte WAL: 31,693 transactions, 33,480 nested
block entries, 33,480 standalone entries, and 297 slot events. It reported no
footer or parent-update events. It kept the validator identity bound before
and after capture. This was a stream capture only. It did not run archive
verification, canonical coverage checks, or publication.

## Callback-to-stream contract

The plugin should subscribe only to the data required for live block
construction. It should disable account snapshot notifications and contact
info notifications. It should preserve these callback families:

- transaction updates, including transaction bytes, status metadata, index,
  slot, and bank ID;
- entry updates, including entry bytes, index, slot, and bank ID;
- block metadata, including parent slot and hash, block hash, executed
  transaction count, entry count, rewards, and bank ID;
- bank and slot status updates, including dead, completed, processed, confirmed,
  and rooted states; and
- block footer and entry-parent updates when the runtime sends them.

The record schema must preserve the concrete bank ID. It must not key records
by slot alone or discard dead banks. Fork choice belongs in a later
reconciliation step. An event sequence records the plugin's accepted write
order; block indexes and bank IDs record source relationships.

Raw shreds need a separate validator hook or TVU capture. The pinned Geyser
interface does not expose raw shred bytes. Deshred transaction updates are
decoded pre-execution transactions and must not be labeled raw shreds.

The [raw shred hook prototype](../../hivezilla/deploy/agave-shred-hook/README.md)
now provides the receive adapter and a Hivezilla-owned C ABI producer. Its
Agave build and real-packet fixture passed. The hook is active after one
validator restart on 2026-10-01. A 30-second live sample saved 144,820 packet
observations while finalized slots advanced by 112. A separate gRPC capture
sealed 32 finalized blocks with 32,844 transactions while raw capture remained
active. RPC health returned to `ok`, with the same identity and genesis hash.
The [activation report](../../hivezilla/deploy/agave-shred-hook/evidence/shred-hook-live-activation-20261001.json)
contains the measurements and deployment state. A future Geyser library can
export the same C factory, but this
prototype does not implement the Geyser execution callbacks. Its local capture
format is explicitly separate from registered HiveSync payload formats.

Define a new, versioned payload format for Geyser callback records. Do not
reuse the gRPC protobuf payload format for a different byte layout. Keep the
event envelope small and bounded. Store callback-specific fields as immutable
source bytes wherever the interface exposes them. Record interface versions
explicitly so an Agave upgrade cannot silently change how Hivezilla interprets
an event.

## Data and durability path

1. Validate a private plugin config on load. Bind the source identity, genesis
   hash, Agave interface version, payload format, output root, segment size,
   and queue limits into a producer-config digest.
2. Assign each callback a bounded serialized record with a monotonic sequence.
   Use a bounded queue. Never drop an event when the queue is full. Apply
   back-pressure or fail the plugin with a clear error.
3. Have one writer own segment order and rotation. Write the stream header,
   framed records, and sealed-segment footer through the existing Hivezilla
   protocol primitives. Keep raw callback data until a later compactor has
   committed a complete output.
4. Keep R2 I/O outside validator callbacks. A separate uploader reads sealed
   segments, verifies its local read, writes immutable objects, and publishes
   the manifest last. Keep upload state separate from callback state.
5. Reconcile records by `(slot, bank_id)`. Require block metadata, the expected
   transaction and entry indexes, and an accepted rooted/finalized status
   before a block can enter the compact path. Keep competing or dead banks in
   raw evidence.

The callback path must not use an unbounded channel. The initial prototype
should measure queue wait, append time, bytes per event, and validator replay
rate. It should not change validator CPU affinity or thread count until those
measurements identify a real bottleneck.

## Build and rollout steps

1. Add a `cdylib` crate and pin `agave-geyser-plugin-interface` to the exact
   Agave 4.3.0 source revision. Export `_create_plugin`; validate private config
   and lifecycle behavior; do not enable transaction callbacks yet.
2. Add the versioned Hivezilla Geyser event envelope and tests for every
   callback version exposed by Agave 4.3.0. Keep unknown interface variants as
   explicit errors.
3. Write events to a local bounded segment writer. Test restart recovery,
   queue saturation, disk-full behavior, segment rotation, and same-slot
   competing banks with synthetic callbacks.
4. Enable the plugin beside Yellowstone on the canary host. Compare event
   counts and `(slot, bank_id, index)` coverage over a bounded finalized
   capture. Keep Yellowstone as the operational stream during this phase.
5. Add local normalization and compaction as a separate consumer. Add R2
   publication only after segment custody and restart recovery work. Keep the
   R2 bucket private and use an execution-canary prefix until source coverage
   and archive policy are complete.
6. Consider removing Yellowstone only after the Hivezilla stream survives a
   validator restart and an uploader restart without a gap, duplicate
   acceptance, or data loss.

## Known limits

The running canary is non-voting and reports its own finalized slot. This is
not independent consensus proof. The first capture had no footer or parent
events, and it did not verify full-epoch coverage. The
[continuous service](hivezilla-validator-tunnel.md) has no maximum run time.
The reduced Yellowstone profile passed 168 tests and captured another 32
finalized blocks without a validator restart. That test measured avoided
account copying, not a CPU throughput gain. Plan any full-epoch capture around
the epoch boundary and start the capture before it; do not infer a complete
epoch from a successful short stream.
