# Finite finalized execution canary

This path builds a small Compact V2 archive from a sealed current-schema capture. It uses the `validator-finalized-execution-v1` source profile. The operator trusts the finalized subscription and RPC of the same local validator. This is not independent consensus proof or a canonical epoch archive.

The current raw envelopes remain the authoritative execution data. The Compact V2 hot data is a projection of known fields. It supports transaction V1 configuration. Fields that the hot format does not contain, such as reward `commission_bps`, remain in the raw evidence. The tool does not round basis points into a legacy commission percentage.

## Input evidence

The prepared Yellowstone config keeps gRPC and metrics on loopback and omits
filter limits. This allows the current capture's block-entry request without
changing the event request. Do not bind the canary gRPC listener to a public
interface.

Capture with the current format-3 command and a literal loopback gRPC endpoint. Configure the same local validator's loopback RPC endpoint. The capture must be sealed, with the following files:

- `events.wal`
- `CURRENT-GRPC-SEALED.v1.json`
- `CURRENT-GRPC-INVENTORY.v1.json`
- `validator-rpc-before.json`
- `validator-rpc-after.json`

The sealed reader checks file hashes, frame hashes, sequence, current protobuf schema, counts, and the RPC identity before and after capture. An unbound capture cannot enter this trusted profile.

The current writer groups disk synchronization. It syncs at each block, at
1 MiB of pending frame bytes, or when an append observes 100 ms since the last
sync. The seal performs a full file sync before checking and publishing the
receipt. A crash can lose an unsealed tail; that attempt is not counted or
resumed. The frame format and required event subscription are unchanged.
The reader starts with 32 MiB HTTP/2 connection and stream windows and enables
adaptive flow control so replay bursts can drain without one disk barrier per
entry.

After capture, request the exact finite range with `getBlocks` at finalized commitment from that same validator. Save the unmodified JSON reply as `rpc-get-blocks.json`. Confirm that the end of the range is finalized. Save this coverage object beside the reply:

```json
{
  "schema": "hivezilla-finalized-rpc-coverage-v1",
  "genesis_hash": "<same validator genesis hash>",
  "node_identity": "<same validator node identity>",
  "commitment": "finalized",
  "start_slot": 432000001,
  "end_slot_exclusive": 432000004,
  "finalized_slot": 432000003,
  "produced_slots": [432000001, 432000003],
  "rpc_response_sha256": "<SHA-256 of rpc-get-blocks.json>",
  "source_receipt_sha256": "<SHA-256 of CURRENT-GRPC-SEALED.v1.json>"
}
```

The range uses an exclusive end. It must be inside one 432,000-slot epoch. Produced slots must be sorted and unique. Under this source authority, the slots absent from the finalized reply are skipped. The saved reply and the declared list must match exactly. This receipt is operator evidence; it is not a cryptographic RPC signature.

## Run one finite local test

On a prepared Ubuntu host, run the native bounded command. It waits for the
specified validator to report healthy status, the expected identity and
genesis, and a finalized slot that advances. It then captures, checks,
normalizes, and compacts 32 blocks. It writes progress and a final result under
the prepared root's `reports` directory. It does not start or restart the
validator.

```sh
hivezilla validator-canary execute \
  --root /home/ubuntu/live/hivezilla-validator-20260930 \
  --hivezilla /home/ubuntu/live/hivezilla-validator-20260930/bin/hivezilla \
  --blockzilla /home/ubuntu/live/hivezilla-validator-20260930/bin/blockzilla \
  --validator-pid 313135 \
  --expected-node-identity J7qpvCEWhdBgPdYRV3s3btWPrRVXa2qnAzgvxWTvtbrQ \
  --expected-genesis-hash 5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d
```

The node identity and PID above belong to the current test. Read them again
with `hivezilla validator-canary status` before use. Do not use this example
for another validator. The result is a noncanonical execution test archive.

## Normalize and build

Use fresh output directories:

```sh
hivezilla normalize-current-execution \
  --input /data/capture \
  --coverage /data/coverage/coverage.json \
  --output /data/normalized \
  --max-blocks 128

blockzilla build-live-execution-canary \
  /data/normalized /data/execution-canary --level 1
```

The normalizer requires the exact set of produced blocks, complete transaction and entry counts, dense indices, entry bank identity, signature shape, and the parent slot/hash chain. It retains available PoH entries and checks their transaction coverage and final block hash. It rejects a captured dead or cleared bank when that bank is selected. It does not recompute PoH hashes or verify transaction signatures cryptographically.

The normalizer copies all raw capture evidence and checks the copied capture again. It writes `FINALIZED-EXECUTION-NORMALIZED.v1.json` only after the checks pass. The files are made read-only before the completed directory is made visible.

The compact builder checks all input file hashes and available PoH counts. It uses the existing Compact V2 writer. It then decodes every hot block, message, and metadata record and checks the index, counts, signatures, offsets, and file boundaries. It retains the full raw capture and coverage evidence beside the compact data. This check does not claim full semantic or cryptographic parity between every raw field and the hot projection.

The first implementation accepts at most 4,096 blocks. A normalized block frame must be no larger than 256 MiB. Each normalized input file is limited to 1 GiB, with an 8 GiB total input limit. Each output file, including the retained raw WAL, must fit the publisher's 512 MiB file limit. Use a small capture for the first canary. A failure keeps a private staging directory for inspection and does not create a complete canary marker.

## Publication scope

`LIVE-EXECUTION-CANARY.json` is the final local completion marker. It states:

- `source_profile: validator-finalized-execution-v1`
- `state: COMPLETE`, `finalized: true`, `archive_verified: true`
- `canonical: false`, `publication_ready: false`
- `shred_evidence: absent`
- `poh_hashes_recomputed: false`
- `signatures_cryptographically_verified: false`
- `footer_completeness_verified: false`

No canonical PoH or shred sidecar is fabricated. Available PoH entries use the separate `available-poh.wincode` evidence file. Current standalone footer and parent events remain in `events.wal`; their presence is not a claim of complete footer proof.

Use the [compact archive publisher](../object-store/COMPACT-ARCHIVE-PUBLISHER.md) only with an `execution-canary/` object prefix. The publisher checks file hashes, writes immutable payloads, checks object collisions, and writes the remote manifest last. This canary marker does not enable the disabled legacy canonical live finalizer or production promotion.
