# Network input changes — 8 September 2026

> **9 September completed results:** all 16 runs passed. V3 improved; the V2 candidate was rejected and removed. See the [full results and comparison limits](network-reader-comparison-20260909.md).

**9 September update:** NAS access is restored. The transport-only diagnostic
completed all three 600 MiB transfers in about 20 minutes. Their hashes match;
the earlier HTTP body close did not recur. This does not identify its cause.
[Transport results](artifacts/car-transport-isolation-20260909.json).

The old/new V2/V3 and CAR/Jetstreamer benchmarks are complete. The result link
above supersedes the implementation plan below. The V2 experiment described
below was measured, rejected, and removed; V3 and CAR retry changes remain.

## Failure evidence

The [earlier diagnostic](network-reader-diagnosis-20260908.md) captured an
incomplete HTTP response body. It did not identify a CAR transaction decode
error. The matching gateway event was canceled, with no Worker exception.
The component that closed the response remains unknown.

Jetstreamer 0.7.0's parallel mode opens long HTTP/1.1 ranges from each worker's
current offset to the end of the CAR, with an 8 MiB reader buffer. Its normal
recovery can reconnect. Our strict benchmark stops on a reported firehose error
to prevent an incomplete run from becoming a speed result. Our CAR HTTP reader
already uses concurrent, closed ranges and preserves their delivery order.
It does not need to copy Jetstreamer's open-ended range policy.

The next transport-only control is prepared. It reads the same 600 MiB from
offset 8,870,428,740 at 1.5 MiB/s, first with a long mirror response, then a long
Triton response, then bounded 32 MiB mirror responses. It records exact body
lengths, request IDs and hashes. This removes transaction decoding and allocator
pressure from the reproduction. These rate-limited tests are fault diagnostics,
not speed benchmarks. All three passed on the restored NAS route.

## Implemented changes

- **CAR:** retry an incomplete body at most twice. Each attempt restarts the
  same closed range and repeats all identity, length and range checks. Partial
  bytes are never delivered to the CAR decoder. Read into the final buffer,
  reuse it on retry, count retry traffic, and retain the nested transport error.
  Status, range and identity failures are not retried by this new path.
- **V2:** read dense signature windows on a separate input worker while block
  reads and decode work proceed. Keep two 16 MiB normal signature windows in
  place of one 32 MiB window. Recycle their vectors. Large valid single-block
  windows run alone. Selective signature queries retain demand reads.
- **V3:** share a signature window across adjacent four-block decode jobs,
  alongside their shared semantic planes. Read signatures and semantic planes
  concurrently. Count both in the existing 16 MiB group target and 64 MiB shared
  input budget. Sparse gaps and oversized jobs retain their prior per-job path.

The memory limits above cover specific input allocations. They do not cover
all process memory. V2's added overlap can change the time buffers coexist;
V3's larger shared signature windows can also change peak RSS. Measure both.

## Validation completed

The affected reader suites passed: 234 V2 tests, 142 V3 tests and 159 CAR tests,
535 in total. The HTTP tests used local fixture servers. The new checks cover
short-body recovery, retry exhaustion, changed identity on retry, empty blocks,
multiple signature windows, oversized single-block windows, read failure and
cancellation. Existing V3 sparse/dense parity checks now include signatures.

A separate [CAR full-decode probe](../../bench/reader-profile/README-car-decode.md)
passes output parity with one and twelve workers on both checked-in real CAR
fixtures. It includes transaction and status-metadata decode, rewards, message
hashing, vote classification and failed-status counts. Its output types differ
from Jetstreamer's native callback types; the probe documents that limit.

[Build hashes and validation receipt](artifacts/network-reader-input-update-20260908.json).

## Next performance checks

After NAS access returns, scan the archive directory and check competing jobs.
Run one substantial reader at a time. Preserve the original reference binaries
and use new result directories.

1. Run the transport-only reproduction described above.
2. Compare ordinary CAR decode runs on Triton and our gateway for the same
   8,192-block plan. Include system and mimalloc builds and one versus twelve
   decode workers. Check every block, total votes and failed transactions.
3. Run old/new V2 and V3 transaction-identity scans in alternating order, with
   12 workers and a fresh sidecar cache for each case. Require the accepted
   identity hash `54531e48c946cb66c0c148d7339fdbb11511cffaaec9bc9c5a33f650a10d3685`.
4. Compare request count, input bytes, setup/scan/total time, CPU and RSS. Use
   separate profiling runs to explain remaining stalls. Do not expand to a
   full epoch before this range passes.

The Linux executables are built for the NAS. An ordinary Jetstreamer build
with mimalloc is also ready. Its existing dependency versions and reader code
match the original reference; only the allocator and adapter package name
change. It has no HTTP trace patch and has not been timed yet.

The [accepted baseline](network-reference-baseline-20260908.md) remains unchanged.
The short allocator experiment showed a 2.83× Jetstreamer gain, but it was an
instrumented 512-block test. The old 18,951 TPS reference must not be presented
as Jetstreamer's best possible performance.
