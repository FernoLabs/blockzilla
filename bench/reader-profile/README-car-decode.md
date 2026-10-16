# CAR decode comparison probe

This optional benchmark calls `of-car-reader` directly. It reads raw CAR,
loads full transactions and status metadata, decodes rewards, computes the
Solana message hash, and counts simple votes and failed transactions. It does
not verify signatures. Empty metadata maps to a default status, as in the
Jetstreamer reference. No transaction filter is applied.

It uses the SDK's ordered HTTP range stream and a bounded queue of reusable
blocks feeding up to 256 decode workers (12 by default). It reads while decoding.
HTTP uses the normal four-worker, eight-window, 32 MiB range defaults. There are `workers + 2`
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

Use `--metadata-mode visitor` to decode protobuf transaction metadata through
the SDK's borrowed callbacks. Every known field is requested, including nested
instructions, token balances, and transaction rewards. Strings and byte fields
borrow the decompression buffer. The probe consumes them before reusing that
buffer. Legacy metadata and block rewards still use the owned decoder. The
default `--metadata-mode owned` preserves the previous output representation.
The visitor mode measures a streaming consumer; it does not produce a retained
metadata object for the application.

This probe recycles range bodies within the configured buffer window.
The public SDK leaves this experimental mode disabled by default because the
repeated CAR tests did not show a speed gain. The probe enables it explicitly
so both modes can be compared in one binary. Use
`--legacy-http-buffers` to allocate a fresh body for each range in the same
executable. Receipts report `reuse_http_buffers`, `body_buffer_allocations`,
and `body_buffer_allocated_bytes`. The last two values cover body-vector
capacity growth only, not all HTTP or decoder allocations. Successful reads
overwrite all bytes before they reach the parser; incomplete retries preserve
capacity without publishing partial data.

Use `--http-workers 8` to test more concurrent requests with the default
eight-chunk, 256 MiB HTTP body window. For more requests, set both
`--http-workers 16 --http-window-chunks 16`. Use `--http-chunk-mib` to set each
range size from 1 to 256 MiB. The window must contain at least as many chunks as
there are HTTP workers. Both counts are limited to 16. The receipt records the
selected counts, range size, and total body window. These flags change the
probe only; the SDK default remains four workers.

For a machine with many cores, compare `--workers 12`, `--workers 32`,
`--workers 64`, and `--workers 128` on the same input. A 64-worker example is:

```sh
target/release/car-decode-reference \
  --url https://files.old-faithful.net/900/epoch-900.car \
  --operator-trusted --workers 64 \
  --http-workers 16 --http-window-chunks 16 --http-chunk-mib 32 \
  --plan /absolute/path/prefix-plan.json \
  --export /absolute/path/new-car-export.bin \
  --output /absolute/path/new-car-report.json
```

More decode workers do not guarantee higher speed. By default, one thread
parses the CAR frames and supplies the decode workers. The network and shared
export file can also limit speed. Each decode worker has its own decoder and
export buffer, and the raw-block queue holds `workers + 2` reusable buffers.
Thus memory use can increase with the worker count. With one parser, HTTP body
storage stays bounded by the separate chunk window, which is at most 512 MiB.

## Independent parser streams

Use `--parser-workers` to read disjoint CAR byte ranges in parallel. This
benchmark option accepts 1 to 128 parsers. The default is 1 and retains the
existing pipeline. For HTTP input, multiple parsers require `--operator-trusted`
and an HTTPS URL ending in `/<epoch>/epoch-<epoch>.car`, without a query or
fragment. Explicit loopback HTTP and resident local input are described below.
The matching `epoch-<epoch>-slot-ranges.raw` file must be at the HTTP source.

```sh
target/release/car-decode-reference \
  --url https://files.old-faithful.net/900/epoch-900.car \
  --operator-trusted --parser-workers 32 --workers 32 \
  --http-chunk-mib 32 --legacy-http-buffers \
  --plan /absolute/path/prefix-or-full-epoch-plan.json \
  --export /absolute/path/new-car-export.bin \
  --output /absolute/path/new-car-report.json
```

With the default `--scheduling static`, the total decode budget stays at
`--workers`. For example, 16 parsers with
64 decode workers use four decode workers per parser. The parser count cannot
exceed the decode count or the number of physical blocks in the plan. Each
parser has one blocking HTTP request at a time and one range-body buffer.
Thus 32 parsers can issue at most 32 concurrent requests. `--http-workers` and
`--http-window-chunks` apply only to the default single-parser transport.
`--http-chunk-mib` sets the body size in both modes. Indexed input accepts 1 to
128 MiB; the default remains 32 MiB. The static single-parser SDK transport
accepts 1 to 256 MiB. The buffer reuse flag is honored in both modes.

The raw index and CAR header are fetched inside timed setup. Every index row
is checked for bounds and overlap. The selected epoch prefix must form one
continuous byte range from the CAR header. It is split only at indexed block
boundaries. Each parser checks the exact byte span, slot, and transaction count
of every physical block against the index and canonical plan. A missing
physical block is allowed only for a planned zero-transaction block. An invalid
range, unexpected block, or unsupported continuation fails the run. The same
checks apply to a full-epoch plan. No CAR is saved locally.

The export implementation is shared with the sequential probe and Jetstreamer.
Use a small output-parity test before a timed comparison. Sharded mode uses
the existing HTTP range source, which checks exact response geometry and keeps
available object validators. Without a strong ETag, it still depends on the
operator's immutable-source assumption. The receipt includes the raw index's
BLAKE3 hash; this is not a CAR content hash.

Receipts identify `http_transport_mode`, `parser_workers`, and
`effective_http_workers`. The HTTP worker/window flags are recorded as requested;
the effective count is the parser count in sharded mode. `http_body_window_bytes`
is then `parser_workers * http_chunk_bytes`. The `shards` array records exact
slot and byte bounds and the decode budget for each parser. HTTP counters include
timed setup, body retries, and server-error retries. A successful checked retry
remains part of elapsed time. Report retries with the result; Jetstreamer's
lower transport layers do not expose equivalent complete retry counters.

## Dynamic jobs and independent HTTP clients

Use `--scheduling dynamic` to assign block-boundary jobs to fixed parser and
decoder pools. Parsers take the next available job. All parsers feed one
bounded decode queue. A decoder stays alive for the scan and can consume blocks
from any parser. This avoids a fixed decode queue for each static shard.

```sh
target/release/car-decode-reference \
  --url https://files.old-faithful.net/1000/epoch-1000.car \
  --operator-trusted --scheduling dynamic \
  --parser-workers 64 --workers 64 --http-clients 8 --job-mib 256 \
  --http-chunk-mib 32 --legacy-http-buffers \
  --plan /absolute/path/prefix-or-full-epoch-plan.json \
  --export /absolute/path/new-car-export.bin \
  --output /absolute/path/new-car-report.json
```

`--job-mib` sets a target from 1 to 4,096 MiB; the default is 256 MiB.
A job ends at the first complete block that reaches the target, or at the end
of the plan. A block is never split across jobs. Parser and decoder counts are
independent in dynamic mode: 1 to 128 parsers and 1 to 256 decoders. These counts
are fixed limits, not automatic CPU tuning. Dynamic HTTP mode needs the trusted
HTTP source and raw slot index, including with one parser. The separate resident
diagnostic mode uses a verified local CAR and raw index instead.

`--http-clients` creates 1 to `parser-workers` independent HTTP clients once
during timed setup. The default is 1. The option works with both static shards
and dynamic jobs. Each client must read the same full index, CAR header sample,
and object lengths. Available strong ETags must match across clients and remain
unchanged at the end. Without a strong ETag, the immutable-source assumption is
still required. Setup reads and all client HTTP counters are included in the
receipt. Use static scheduling with 1 and 8 clients to measure the client-pool
change separately from dynamic job assignment.

Dynamic mode owns exactly `workers + 2 * parser_workers` reusable raw-block
containers across the free pool, parsers, decode queue, and decoders. Both
channels have this capacity. This bounds queued block storage independently of
epoch length; it is not a hard RSS bound. Each parser also has an 8 MiB input
buffer and one HTTP body buffer. Decoder metadata, export buffers, container
capacity, TLS, and allocator storage use additional memory.

Larger indexed HTTP bodies can reduce the number of GET requests. For example,
`--http-chunk-mib 128` sets a body window of 4 GiB with 32 parsers, or 8 GiB
with 64 parsers. Jobs still end at complete block boundaries, so their last
request can be shorter. The source, exact range, body length, and retry checks
are unchanged. Each retry can transfer more bytes, and a larger body can take
longer to complete. The source's rate-limit policy is unknown; a larger body
does not establish that HTTP 429 errors will stop.

Receipts add `scheduling`, `http_clients`, `job_target_bytes`, and `job_count`.
Dynamic mode uses `http_transport_mode=dynamic-range`; its `pipeline` records
job and block counts, pool capacity, observed queue depth, and worker wait and
work times. These times are sums of wall time, not CPU time. Parser work includes
HTTP reads; decoder work includes export writes. `jobs_completed` means parsed
and queued. A successful scan also checks all decoded rows and all completed
jobs. The `shards` field is null in dynamic mode.

Source, parser, decoder, and export errors cancel the dynamic queues and stop
new jobs. Worker panics also cancel queue waits. An HTTP request already in
progress can finish its existing timeout and bounded retry sequence before the
worker joins. Cancellation does not interrupt that request. Exact geometry,
source identity, canonical row, decode, export, and retry checks remain in use.

For an immutable local CAR cache, indexed input also accepts
`--allow-loopback-http --url http://127.0.0.1:8123/1000/epoch-1000.car`.
The flag permits only literal `127.0.0.1` or `::1`; DNS names, other addresses,
credentials, queries, and fragments are rejected. HTTPS remains the default.
The flag cannot enable HTTP for the static single-parser SDK transport.
The receipt records the actual URL and `identity.loopback_http`.
Use the same local range server and source checks for both readers; see
[the local-cache procedure](../car-export/README-dynamic.md).

## Resident decode profiling

Use [run_resident_profile.py](../car-export/run_resident_profile.py) on Linux to
find decode costs with the complete local CAR locked in RAM. It uses a read-only
`mmap`, requires `mlock` to succeed, and checks every page with `mincore` before
and after the scan. There is no HTTP. The existing bounded range copies and CAR
parser still run. The workload keeps full transaction decoding, owned metadata
and rewards decoding, message hashes, and receipt digests. It omits export. This is
a decode diagnostic, not an equal-output comparison with Jetstreamer.

Build a separate diagnostic binary with native code, symbols, and frame pointers:

```sh
RUSTFLAGS='-C target-cpu=native -C force-frame-pointers=yes -C force-unwind-tables=yes' \
CFLAGS='-fno-omit-frame-pointer -g' CXXFLAGS='-fno-omit-frame-pointer -g' \
cargo build --locked --profile release-debug -p blockzilla-reader-profile \
  --features reference-mimalloc --bin car-decode-reference --bin perf-flamegraph
```

Use separate cases for baseline timing, stage timers, and each perf method:

```json
[
  {"name":"baseline-p64-w64","parsers":64,"workers":64},
  {"name":"stages-p64-w64","parsers":64,"workers":64,"stage_sample_every":1024},
  {"name":"stat-p64-w64","parsers":64,"workers":64,"profile":"stat"},
  {"name":"record-p64-w64","parsers":64,"workers":64,"profile":"record"}
]
```

The current runner requires the full epoch-1000 plan and completed cache manifest.
It uses `sudo -n` for the reader's process-specific memlock limit and for perf.
It checks available RAM before each case. It does not change host perf settings.

```sh
python3 bench/car-export/run_resident_profile.py \
  --bin /data/bin/car-decode-reference-profile \
  --manifest /data/cache-epoch1000/manifest.json \
  --car /data/cache-epoch1000/epoch-1000.car \
  --index /data/cache-epoch1000/epoch-1000-slot-ranges.raw \
  --plan /data/epoch-1000-full-plan.json \
  --cases /data/resident-cases.json --results /data/new-resident-results
```

The runner supplies `--file`, `--resident-cache-manifest`, `--raw-index`,
`--scheduling dynamic`, and `--profile-control`. Resident mode requires
`--http-clients 1`; this means one shared mapping, not an HTTP client. It uses
128 MiB range copies and 256 MiB jobs. `--stage-sample-every 1024` selects about
one transaction in 1,024 by a deterministic slot/index hash. The default of zero
disables stage timers. Transaction stage times are sampled worker wall times,
not CPU time; block-stage times cover every block. Do not add these raw totals
without accounting for their different sampling scopes. Worker CPU counters and
`scan_resources` record CPU time separately.

Use `scan_seconds` for timing. It includes range copies, parsing, full decoding,
hashes, and worker startup and join. It excludes preload, memory locking,
residency checks, and profiler handshakes. `total_seconds` includes those phases.
Perf runs separately from stage sampling and records the controlled scan window.
Both methods add overhead; use the baseline case for speed measurements. The
runner retains exact plan/count, source identity, and residency checks, but does
not perform a full CAR rehash or output verification.

Convert saved perf stacks with the included Inferno-based converter:

```sh
sudo -n perf script -i /data/new-resident-results/record-p64-w64/perf.data \
  > /data/new-resident-results/record-p64-w64/perf.script
target/release-debug/perf-flamegraph \
  /data/new-resident-results/record-p64-w64/perf.script \
  /data/new-resident-results/record-p64-w64/stacks.folded \
  /data/new-resident-results/record-p64-w64/flamegraph.svg
```

The converter requires new folded-stack and SVG paths and does not sample a
process. See [the resident profiling report](../../docs/benchmarks/triton-resident-decode-profile-20260930.md)
for the measured settings, evidence, and interpretation limits.

## Transport diagnostics and memory

If a shard fails, an error is written to stderr immediately. It includes the
shard's slot and byte spans, the failed request's byte range when available,
and elapsed scan time. The failed-scan receipt includes source identity and
observed HTTP bytes and retry counters after all parser workers have joined.
It omits sharded `bytes_delivered`, because planned byte spans do not prove
delivery after a failure. These diagnostics do not add retries or change the
acceptance checks.

Static and dynamic indexed range reads have a separate retry rule for the exact sanitized
error `HTTP request failed (error sending request)`. They retry the same byte
range at most twice, after delays of 100 and 300 milliseconds. Each new attempt
uses all existing response geometry and source identity checks. No input offset
advances until a complete range passes. The default static single-parser path is unchanged.

The shared source has removed the typed cause from this error message, so the
allowed error is not known to be transient. A persistent send failure still
fails after three attempts. This wrapper does not retry malformed ranges,
changed ETags, missing objects, decode errors, or body failures that exhausted
the source's existing body-retry budget. Retries are logged immediately and are
included in elapsed time and HTTP request counters. Success and failure
receipts also report `http.request_send_retries` and
`http.request_send_retry_limit` (two extra attempts per range).
The source's body and server-status retry limits still apply inside each
attempt. Thus three wrapper attempts are not a global limit of three GETs.

Sharded mode has `workers + 2 * parser_workers` reusable raw-block buffers,
plus one 8 MiB parser buffer per parser. With 128 parsers and 32 MiB ranges,
the HTTP body window is 4 GiB; at the indexed 128 MiB maximum it is 16 GiB.
These are separate from metadata,
export, allocator, and TLS storage. The single-parser memory defaults remain
unchanged.

Use `--allocations` with the System build for a separate allocation diagnostic.
It counts Rust allocation and reallocation requests during the scan, including
HTTP activity, with counts by requested size. It does not count C zstd
allocations, and requested bytes are not retained memory. Shared counters add
overhead, so do not use these runs as speed measurements. This option is
rejected by the mimalloc build.

## Acceptance and comparison limits

For an equal-output application test, use `--export NEW_BIN` with the default
owned metadata mode and the independent Jetstreamer export adapter. Both paths
write the same ordered transaction fields with the same output implementation.
See [the common export contract](../car-export/README.md). Its `total_seconds`
includes export finalization and file sync; `scan_seconds` does not include the
final ordered copy. Old count-only receipts do not establish this new parity.

Accept performance only if the receipt is valid and all expected rows match.
Compare vote and failed-status totals with the sum of the Jetstreamer worker
counters. Compare per-block decode digests across our one-worker, many-worker,
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
