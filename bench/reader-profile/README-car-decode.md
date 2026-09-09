# CAR decode comparison probe

This optional benchmark calls `of-car-reader` directly. It reads raw CAR,
loads full transactions and status metadata, decodes rewards, computes the
Solana message hash, and counts simple votes and failed transactions. It does
not verify signatures. Empty metadata maps to a default status, as in the
Jetstreamer reference. No transaction filter is applied.

It uses the SDK's ordered HTTP range stream and a bounded queue of reusable
blocks feeding up to 12 decode workers. It reads while decoding. HTTP uses the
normal four-worker, eight-window, 32 MiB range defaults. There are `workers + 2`
raw block buffers, an 8 MiB input buffer, a 16 MiB payload limit per block, and
at most 16 MiB of retained dataframe buffers per recycled block. Oversized
blocks or unsupported transaction continuations fail the probe. These are
benchmark bounds, not new restrictions on the public SDK. They are not an RSS
limit: container allocations, decoder workspaces, HTTP and TLS also use memory.

The ordered CAR path requires canonical physical order. It does not resolve
arbitrary transaction CID references. It checks transaction slot/index and
entry counts, and verifies every block against a trusted external plan. A
missing zero-transaction block is admitted only when the plan specifies zero;
a missing nonempty block fails the run.

## Build and run

```sh
cargo build --release -p blockzilla-reader-profile \
  --features car-reference --bin car-decode-reference

target/release/car-decode-reference \
  --url https://files.old-faithful.net/900/epoch-900.car \
  --operator-trusted --workers 12 \
  --plan /absolute/path/prefix-plan.json \
  --output /absolute/path/new-car-report.json
```

Use the existing `extract_index_plan.py` output for the same first 8,192
canonical blocks of epoch 900 used by the Jetstreamer reference. The plan is
loaded before the timer. This probe supports an epoch prefix, not an arbitrary
seek range. The normal public archive SDK handles admission and sidecar loading;
this probe isolates transport and decode work.

For our gateway, use the explicit CAR URL and omit `--operator-trusted` to
require a strong ETag on HEAD and every range response. For a raw local CAR,
replace `--url ... --operator-trusted` with `--file /absolute/path/epoch-900.car`.
Do not decompress an archive for this test.

A second build with `--features reference-mimalloc` changes only this probe's
allocator. Keep both binaries and their hashes. The normal V2/V3 profile binary
and the public readers keep their existing allocators.

## Acceptance and comparison limits

Accept performance only if the receipt is valid and all expected rows match.
Compare vote and failed-status totals with the sum of the Jetstreamer worker
counters. Compare per-block decode digests across our one-worker, twelve-worker,
local, network and allocator cases. The old Jetstreamer reference did not record
these digests, so a matching transaction count alone does not prove byte parity
with Jetstreamer.

Report setup, scan and total time, exact HTTP body bytes including retries,
retry count, CPU and peak RSS. The scan timer includes reader teardown and any
outstanding HTTP reads. Use fresh processes and the same machine, source,
range and allocator when comparing readers. Run old/new in alternating order.
Keep profiled runs separate from ordinary timing runs.

This is a full decode comparison, but the output representations differ:
our decoder retains protobuf status metadata and borrowed transaction fields;
Jetstreamer also converts status metadata and rewards into Solana native types
and dispatches asynchronous callbacks. Our probe hashes encoded message bytes
directly, while Jetstreamer serializes its decoded message before hashing it.
These are design costs, not proof of a faster low-level decoder. Isolate those
stages if they explain a difference. Do not compare this full decode TPS with
the V2/V3 transaction-identity-only workload.
