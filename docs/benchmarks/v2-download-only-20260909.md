# V2 download-only results — 9 September 2026

**Concurrent HTTP/1.1 downloads reached 547–558 MB/s in two 4 GiB runs.**
This is about 4.4 Gbit/s of response-body data. HTTP/2 did not establish a better
result: one longer adaptive-window run passed at 302 MB/s, while two other
longer HTTP/2 runs ended with incomplete TLS bodies. These failures remain in
the evidence and have no accepted throughput.

The user stopped the broader epoch300/900 suite to test download speed first.
All download processes have stopped. The full suite is deferred; it will not
restart automatically. No archive was rewritten or decompressed to disk.

## Longer runs

Every timing case requests the same 4 GiB interval starting at byte8,589,934,592
in epoch900 `archive-v2-blocks.zstd`, through our existing Workers gateway.
Bodies are discarded; there is no decompression, transaction processing,
ordered output, or disk payload write. Rates are decimal MB/s over scan time.
Setup is separate. HTTP/1.1 uses one shared client pool with multiple concurrent
connections. Pool count is not a measured physical-connection count.

| Case, in run order | Workers | Pools | Range (MiB) | Scan (s) | MB/s | Process CPU (s) | Kernel peak RSS (MiB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| h1-8-a | 8 | 1 | 32 | 14.217 | 302.1 | 8.410 | 20.3 |
| h2-adaptive-8-a | 8 | 1 | 32 | 14.236 | 301.7 | 6.937 | 20.3 |
| h2-adaptive-8-b | 8 | 1 | 32 | 0.393 | FAILED | 0.138 | 20.3 |
| h1-8-b | 8 | 1 | 32 | 13.583 | 316.2 | 8.414 | 20.3 |
| h1-16-64-a | 16 | 1 | 64 | 7.698 | 557.9 | 6.903 | 32.9 |
| h1-8-c | 8 | 1 | 32 | 12.261 | 350.3 | 8.104 | 20.3 |
| h1-16-64-b | 16 | 1 | 64 | 7.855 | 546.8 | 7.017 | 31.4 |
| h2-adaptive-8-pools4 | 8 | 4 | 32 | 0.763 | FAILED | 0.719 | 20.3 |

The first four long tests use an HTTP1/HTTP2/HTTP2/HTTP1 order. HTTP1 then tests
16 workers with64MiB ranges, returns to the8-worker/32MiB baseline, and repeats
16/64. Thus the16/64 improvement is a combined settings change, not an isolated
measurement of worker count or request size. Three HTTP1 eight-worker passes
measured302–350MB/s; both sixteen-worker passes measured547–558MB/s.

The failed adaptive HTTP2 shared-pool run received57,895,616 bytes before
failure. The four-pool HTTP2 run received231,635,392 bytes. Both reported a peer
closing TLS without close_notify while reading response bodies. The root cause
is not identified. No retry policy was used; the probe intentionally preserves
these failures. This does not prove that HTTP2 is generally faulty or that
reqwest, Cloudflare, R2, or the NAS caused the closes.

## Initial 512 MiB checks

| Protocol | Workers | Range (MiB) | Scan MB/s |
| --- | ---: | ---: | ---: |
| Http1 | 1 | 32 | 29.3 |
| Http1 | 4 | 32 | 166.9 |
| Http1 | 8 | 32 | 308.4 |
| Http2 | 1 | 32 | 35.3 |
| Http2 | 4 | 32 | 38.7 |
| Http2 | 8 | 32 | 108.4 |
| Http2Adaptive | 8 | 32 | 145.4 |

These short results are controls, not stable throughput guarantees. The higher
HTTP2 result in the longer pass illustrates how duration and network variation
can change the result. Ranges overlap between runs and remote cache state is
uncontrolled. No response supplied CF-Cache-Status, which does not establish
that every layer was uncached. Raw receipts retain CF-Ray where present.

## What this establishes

The current reqwest blocking stack can transfer this V2 object much faster
than the full query pipeline's60.6MB/s. This points to download scheduling as a
useful next target. It is not a measured9× SDK gain: the query reads a full epoch,
decodes blocks, enforces ordering, and has smaller shared input buffers.

V2 already has slot-to-compressed-offset/length rows. The query currently ties
HTTP batches to64-block/65,536-transaction/32MiB decoded limits, and admits only
three reusable compressed buffers. The transport probe instead lets workers
finish independently, each reusing a1MiB scratch buffer. Its low RSS cannot be
promised for an ordered reader that retains whole out-of-order64MiB responses.
Do not implement sixteen whole64MiB buffers without an explicit memory budget.

Source inspection also confirms real blocking-client overhead: reqwest0.13.4
Response::read adapts body chunks to AsyncRead and copies them into the caller
buffer. Its blocking wait creates an Arc-backed waker on each call. A separate
current-thread Tokio runtime handles requests for each independent blocking
Client; clones share it. Async reqwest Response::chunk returns frame Bytes
directly, avoiding this particular adapter copy. This does not remove TLS,
kernel buffering, or copies needed when a decoder requires contiguous input.
Low total process CPU alone cannot exclude a single busy thread.

The next useful comparison is the same transfer through async reqwest chunks,
with per-thread CPU and allocation measurements in a separate diagnostic pass.
Keep explicit connection reuse and bounded retained bytes. Direct Hyper should
be tested only as a further controlled variant, not assumed faster because it
has fewer API layers. No async or direct-Hyper speed result exists in this run.

## Compression

The measured V2 block object is already Zstandard-compressed. Additional HTTP
compression is unlikely to reduce it materially; this is an expectation, not a
recompression measurement. The range/index/ETag contract requires exact stored
bytes, so these tests use Accept-Encoding:identity. Cloudflare documents that
its successful-response compression applies to200 responses, not206 byte-range
responses. [Cloudflare content compression](https://developers.cloudflare.com/speed/optimization/content/compression/).

Uncompressed sidecars fetched whole are a separate compression experiment, with
validation against their decoded bytes. In the full V2 count, setup took13.88s
versus989.11s of scanning, so eliminating all setup would save only about1.4%
of that total. Sidecar compression does not address the main scan limit.

## Validation and reproduction

19 cases completed:17 passed,2 failed. Four8MiB hash controls passed, including
identical ordered1MiB BLAKE3 pieces between HTTP1 and HTTP2 with different range
sizes. Timed runs have hashing disabled. Accepted runs require exact206 ranges,
Content-Length, stable strong ETag and matching HEAD identity before/after.
Every saved receipt hash and successful byte coverage was checked locally.
This establishes the transport contract; only the small hash controls establish
cross-protocol content-hash parity. No automatic retries or redirects occurred.

CPU and RSS are process resource-usage values collected after exit. Kernel peak
RSS can include launcher memory inherited before exec; repeated20MiB readings
are not a precise measure of buffer residency. Application scratch buffers are
1MiB per worker, with HTTP/TLS memory additional. The two16-worker runs report
31.4–32.9MiB kernel peak RSS.

The new benchmark is [documented here](../../bench/download-only/README.md).
The tested executable hash is
`2bbd0dd2bc7b8acecaa4d79301c9e3373d6ae3e1118f929d49f24348bb1e9d3f`.
A later packaging-only change requires the explicit `transport` Cargo feature
to keep HTTP2 out of default production workspace builds. The benchmark Rust
logic is unchanged. Two local HTTP fixture tests cover request-boundary hash
parity and rejection of short bodies, changed ETags and wrong ranges. Checks
and Clippy pass. No production reader change or Cloudflare configuration change
was made, and nothing was committed or pushed.

[Raw results and controller measurements](artifacts/v2-download-only-20260909.json).
