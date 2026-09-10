# CAR and Jetstreamer: the same transaction export

This test measures the time to produce the same application output. Both paths
fully decode transactions and metadata, then use `shared.rs` to export the same
fields. Every transaction is included, including votes and failed transactions.
Signature verification is disabled in both readers.

The output contains the slot, transaction index, first signature, Solana message
hash, simple-vote flag, failure flag, fee, and pre/post lamport balance arrays.
Records follow canonical slot and transaction order. These fields are checked
byte for byte, not only by transaction count. This proves parity of this export;
it does not prove equality of every decoded metadata field. Logs, token balances,
inner instructions, error details, and rewards are decoded but are not exported.

Both readers use the same output implementation. Each worker holds at most one
64 MiB encoded block. Completed blocks go into a shared spool file. Finalization
validates block coverage, copies blocks into canonical order, flushes and syncs
the output file. It removes the spool after success. This bounds memory when
Jetstreamer workers reach later slots before the first worker finishes. It also
adds a disk write and read to both paths. All export work and final file sync are
inside `total_seconds`. External SHA-256 and byte comparison are outside timing.
The shared spool lock is acquired once per block, not per transaction.

## Reproduce

Build our probe from the repository root:

```sh
cargo build --release -p blockzilla-reader-profile \
  --features reference-mimalloc --bin car-decode-reference
cargo build --release --locked --manifest-path bench/jetstreamer-reference/Cargo.toml
```

The Jetstreamer adapter has its own workspace and lockfile. It pins upstream
`jetstreamer-firehose` 0.7.0 and mimalloc 0.1.52. No upstream decoder or transport
patch is applied. Its source is based on the earlier count adapter; the changes
add the common export and direct canonical-plan validation. Its dependencies do
not enter the production reader workspace.

Use the same canonical epoch-prefix `plan.json` for both commands. The plan
contains `epoch`, `start_slot`, `end_slot_exclusive`, and ordered
`block_transaction_rows` pairs of slot and transaction count. Use a new output
path for every run. Neither adapter overwrites an earlier output.

```sh
target/release/car-decode-reference \
  --url https://blockzilla-archive-samples-v1.cheron-augustin.workers.dev/car/900/epoch-900.car \
  --workers 12 --legacy-http-buffers --plan /data/plan.json \
  --export /data/car.bin --output /data/car.json

bench/jetstreamer-reference/target/release/jetstreamer-common-export-reference \
  --epoch 900 \
  --http-base https://blockzilla-archive-samples-v1.cheron-augustin.workers.dev/car/ \
  --index-base https://blockzilla-archive-samples-v1.cheron-augustin.workers.dev/car/ \
  --workers 12 --start-slot 388800000 --end-slot-exclusive 388808198 \
  --plan /data/plan.json --export /data/jetstreamer.bin --output /data/jetstreamer.json

cmp /data/car.bin /data/jetstreamer.bin
```

These commands cover the first 8,192 canonical blocks of epoch 900, containing
8,925,832 transactions. Set the end slot to match the plan when using a smaller
sample. The normal CAR buffer allocation mode is explicit in this comparison;
the experimental reusable HTTP-body mode remains disabled.

`run.py` is a finite Linux runner. It checks a 64-block sample first, then runs
CAR / Jetstreamer / Jetstreamer / CAR, one fresh process at a time. It requires
matching binary hashes, source ETags, exact block counts, output structure, byte
parity, and vote/failure totals. It records total time, TPS, output size, CPU time,
and kernel peak RSS. It stops on failure and retains the evidence. The optional
`--wait-for` controller path prevents overlap with the current NAS test series.
The runner reads raw CAR through HTTP. It never writes or decompresses a CAR file.

Jetstreamer also creates native Solana metadata objects and asynchronous
callbacks. Our reader uses its own metadata representation. Those internal costs
remain part of this application comparison. Equal output does not make it an
isolated parser microbenchmark. Do not reuse the earlier 3.10× ratio as the result
of this new test. Compare the new receipts only after output parity passes.

## Binary layout

All integers are unsigned 64-bit little-endian unless specified otherwise.
The file starts with the 16 ASCII bytes `CAR-TX-EXPORT-01`.
Each block starts with slot, transaction count, and payload byte length.
Each transaction contains, in order:

1. Slot and zero-based transaction index.
2. First signature (64 raw bytes) and message hash (32 raw bytes).
3. Vote and failure flags (one byte each, 0 or 1).
4. Fee in lamports.
5. Pre-balance count followed by that many lamport balances.
6. Post-balance count followed by that many lamport balances.

Zero-transaction blocks have an empty payload. The plan admits a missing physical
zero-transaction block; a missing nonempty block fails. There are no timestamps or
reader-specific fields in the exported file. Timing receipts are separate JSON.
