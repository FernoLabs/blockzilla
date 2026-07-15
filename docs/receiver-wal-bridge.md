# Blockzilla receiver WAL bridge

The receiver WAL is the canonical Blockzilla copy of the exact compressed Yellowstone stream.
`bridge-receiver-grpc-raw` reads its locally fsynced prefix and copies those exact compressed bytes
into a separate standard `record-grpc-raw` generation. The existing PoH verifier and epoch
materializer can then consume the derived generation without stopping or modifying the receiver.

This deliberately keeps four different watermarks:

1. **Receiver durable progress**: exact compressed bytes and receiver progress are fsynced. This is
   the only source boundary the bridge reads.
2. **Signed ACK delivered to Hetzner**: Blockzilla signed the cumulative raw-byte receipt and
   Hetzner verified+fsynced it. Only this can authorize Hetzner retention cleanup.
3. **Bridge progress**: the exact receiver record also exists in the separate standard raw-gRPC
   generation. This never authorizes receiver or Hetzner cleanup.
4. **Indexed/compacted progress**: the derived epoch capture and Archive V2 outputs were validated
   and published. This is reported separately from raw-byte durability.

An R2 object, ping, slot number, storage watermark, materialization receipt, or successful
compaction is never accepted as a signed ACK or deletion authority.

## Run one bounded pass

The immutable stream identity is visible in the receiver spool path:

```text
<receiver-root>/<cluster-id>/<origin-node-id>/<source-id>/<32-hex-journal-id>/
```

Run the bridge with a target outside the receiver root:

```bash
cargo run --release -p blockzilla-live-producer -- bridge-receiver-grpc-raw \
  --receiver-spool-root /volume1/blockzilla/receiver-spool \
  --output-dir /volume1/blockzilla/receiver-derived/raw-current \
  --cluster-id solana-mainnet \
  --origin-node-id HETZNER_ORIGIN_ID \
  --source-id grpc-raw-hetzner-backup \
  --journal-id 32_HEX_CHARACTERS \
  --max-records 4096
```

The receiver may stay live. Each invocation opens the source files read-only, snapshots the
locally durable receiver-progress boundary, and copies at most `--max-records`. Re-run until the
JSON report says `reached_receiver_durable_tail: true`. A supervisor can keep doing bounded passes
without holding receiver resources indefinitely.

The target uses its own physical WAL identity but preserves the receiver stream as its logical
replication identity. `RECEIVER-BRIDGE-CURSOR.v1.json` binds the last source location and digest to
the target tail. Its order is:

1. target WAL append + fsync;
2. target handoff row + fsync;
3. bridge cursor atomic replace + fsync.

After a crash, the standard target WAL reconciles a one-record WAL/handoff gap. A missing or stale
bridge cursor is rebuilt by scanning the receiver read-only and comparing the exact compressed
source and target tail. No duplicate target frame is appended.

## Verify and materialize

Pause only the bridge process (the receiver continues), then verify its derived target:

```bash
cargo run --release -p blockzilla-live-producer -- verify-grpc-raw-poh \
  --output-dir /volume1/blockzilla/receiver-derived/raw-current
```

For a completed epoch, run the existing atomic materializer against that stopped bridge target or
a filesystem snapshot of it:

```bash
cargo run --release -p blockzilla-live-producer -- materialize-grpc-raw \
  --input-dir /volume1/blockzilla/receiver-derived/raw-current \
  --archive-dir /volume1/blockzilla/live/epoch-1002 \
  --epoch 1002
```

Then run the existing capture inspection, repair/coverage checks, registry build, and Archive V2
hot-block finalizer. Publish `READY-TO-PACKAGE` only after the epoch coverage plan has no unresolved
slot gaps and the materialized artifacts pass their normal validation. Receiver ACK progress must
remain a separate metric from this index/compaction progress.

## Cutover requirements

Do not stop the old direct Triton capture or claim Blockzilla indexing is caught up until all of
the following are true:

- the mTLS Hetzner → Blockzilla path has a proven signed cumulative ACK;
- a bridge pass reaches the same locally durable receiver sequence;
- `verify-grpc-raw-poh` succeeds on the derived target;
- a completed epoch materializes and its receipt/artifacts validate;
- the known epoch capture and raw-safety-WAL slot gaps are repaired or explicitly quarantined;
- the current finalizer/compacter successfully consumes the materialized capture;
- restart, receiver-active read, bridge crash-window recovery, and power-loss tests pass on the
  production filesystem;
- alerts expose receiver byte lag, signed-ACK lag, bridge lag, index lag, and compaction lag as
  distinct, human-readable values.

The bridge never removes source segments and contains no receiver cleanup path.
