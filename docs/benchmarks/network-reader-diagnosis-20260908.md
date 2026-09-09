# Network read failure and allocator diagnosis — 8 September 2026

> **9 September completed results:** all 16 runs passed. V3 improved; the V2 candidate was rejected and removed. See the [full results and comparison limits](network-reader-comparison-20260909.md).

**The mirror failure is a truncated HTTP response. A separate allocator test
also identified substantial overhead in our Jetstreamer benchmark build.**
The exact component that closed the failed stream remains unknown. No server
or SDK production fix is claimed by this investigation.

Implementation follow-up: [bounded retries and V2/V3 input changes](network-reader-input-update-20260908.md). The new NAS timing runs are pending.

## HTTP failure reproduced

The 8,192-block mirror diagnostic stopped after 319.816 seconds. It completed
5,524 blocks and 6,008,087 transactions. Every completed row matched the expected
plan, but the complete run failed. No TPS is accepted for this partial result.

The detailed error was:

```text
slot=388807994 Error reading until block:
reqwest::Error { kind: Decode, source: hyper::Error(Body,
Custom { kind: UnexpectedEof, error: IncompleteBody }) }
```

Here, `Decode` refers to the HTTP response body. It does not establish a CAR,
Solana transaction or metadata decode fault. The response ended before its
advertised content length. The earlier mirror attempt had only the shorter
message, so this run identifies a failure type without proving that every
previous timeout or body error had the same cause.

| Failed request | Recorded value |
| --- | --- |
| Origin | Our sample Worker and R2 bucket |
| CAR start offset | 8,870,428,740 |
| HTTP response | 206, HTTP/1.1 |
| Advertised body bytes | 518,175,169,418 |
| Bytes delivered by HTTP stream before error | 538,714,044 |
| Time from response headers to error | 318.612 s |
| Content encoding | None |
| Cloudflare request ID | `a37ef7ad091a57ed-CDG` |
| Matching gateway event | `canceled`, no exception or application log |

The full Content-Range was
`bytes 8870428740-527045598157/527045598158`.
Jetstreamer's seekable reader requests from each offset to the end of the CAR;
it usually closes these streams after its assigned blocks. The failed request
ended early while its worker still needed data. It triggered the strict adapter
stop; other stream cancellations after that stop are not additional failures.

The matching gateway record reports 262 ms of Worker wall time, while the
client consumed the response for 318.6 seconds. This record is not a measurement
of the complete body transfer. Its canceled outcome does not identify which
side initiated the close. No malformed range, changed source identity or Worker
exception was observed. The R2, CDN, network and client transport paths remain
possible locations of the initiating fault.

The elapsed time alone does not establish a five-minute Worker limit.
Cloudflare documents no hard duration limit for an HTTP-triggered Worker while
the client remains connected. See [Worker limits](https://developers.cloudflare.com/workers/platform/limits/).

## CPU cost isolated with an allocator comparison

We ran A/B/B/A with 12 workers, the same mirror and first 512 canonical blocks
of epoch 900. Each run decoded exactly 545,576 transactions. All four runs
passed exact ordered block counts, callback totals and source checks. Both
builds had the same HTTP diagnostics and dependency versions. The B build adds
`mimalloc 0.1.52` / `libmimalloc-sys 0.1.49` as the global allocator; the A build
uses the default allocator in our Linux musl executable. No reader or callback
logic was changed.

| Case, in run order | Elapsed (s) | Diagnostic TPS | User + system CPU (s) | System CPU (s) | Peak RSS (MiB) |
| --- | ---: | ---: | ---: | ---: | ---: |
| a1-musl | 27.040 | 20,177 | 252.9 | 153.1 | 106.2 |
| b1-mimalloc | 9.722 | 56,119 | 35.2 | 22.8 | 193.0 |
| b2-mimalloc | 9.576 | 56,975 | 35.6 | 23.3 | 210.0 |
| a2-musl | 27.505 | 19,836 | 260.3 | 160.3 | 106.2 |

Mean elapsed time changed from **27.272 s to 9.649 s**,
a **2.83×** throughput increase in this short test.
Mean CPU time changed from **256.6 s to 35.4 s**;
mean system CPU time changed from **156.7 s to 23.0 s**.
Memory use increased materially. It must remain part of the tradeoff.

This identifies allocator choice as a major cause of CPU cost in our reference
build. It does not prove the exact internal lock or allocation site, or explain
the initiating HTTP close. One-second system-call samples showed futex waits,
but are not syscall counts or a CPU flame graph. System CPU remains a large
share with mimalloc, despite the much lower absolute CPU time.

The same body-reading path delivered approximately 602.3 MB in each short run;
all 23 logged stream responses per case were HTTP 206. Small byte differences
come from stream buffering beyond the consumed block boundary. These trace
bytes exclude the separate index and size probes. They are not an end-to-end
HTTP traffic counter.

The original [18,951 TPS Triton baseline](network-reference-baseline-20260908.md)
is retained as the measured reference for its exact build. It is not a claim
about Jetstreamer's best possible performance. These instrumented 512-block
results do not replace the normal 8,192-block baseline. A future replacement
needs an ordinary build and the same accepted range, source and checks.

## Effect on our V2/V3 network reader

The shared HTTP source already reads bounded ranges, retries an incomplete body
at the original offset, and checks the source identity and response range again.
Only a complete successful read can enter the cache. Malformed ranges and
changed ETags fail rather than being accepted through a retry. The existing
20 HTTP-source tests passed, including truncated bodies, retry limits and ETag
changes during retry.

This is the correct existing protection for the failure type observed here.
There is no evidence from this run of an output-corruption bug in V2/V3, and no
SDK recovery rule was relaxed. Their separate performance issue remains input
waiting and many small signature requests, as shown in the
[V2/V3 profile](epoch900-network-profile-20260908.md).

The transport investigation remains open at the initiating-close level. The
saved request ID and byte position support a server-side trace or a controlled
comparison of long streams with bounded range reads. Changing the allocator
alone is not a verified repair for the broken stream.

## Evidence and verification

The diagnostic changed only local copies of the reference adapter and HTTP
helper. It preserves the nested error text and logs response range, HTTP version,
request ID and delivered body bytes. The original executable and accepted
artifacts are unchanged. All dependency versions were checked; only the local
rseek patch and, in the allocator variant, the two named allocator packages differ.

[Diagnostic patch](artifacts/jetstreamer-http-diagnostics-20260908.patch).
[Full machine-readable results, build identities and receipt hashes](artifacts/jetstreamer-http-diagnosis-20260908.json).
Raw data and build logs are retained in
`target/nas-validation/jetstreamer-http-diagnostic-20260908T152057Z` and
`target/nas-validation/jetstreamer-http-diagnostic-20260908T152057Z-allocator-abba`.

Exact output checks were repeated locally after receipt transfer. HTTP ETags,
local archive metadata and the canonical index hash were stable. All readers,
supervisors and the gateway log tail have stopped. There was no archive write,
CAR decompression or gateway deployment. These report and diagnostic changes
are local; they have not been committed or pushed.
