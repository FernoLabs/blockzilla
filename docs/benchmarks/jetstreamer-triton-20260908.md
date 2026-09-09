# Jetstreamer from Triton — 8 September 2026

> **Follow-up diagnosis:** a detailed mirror run reproduced an incomplete HTTP body.
> A separate allocator test found substantial overhead in our Jetstreamer build.
> See the [diagnosis and limits](network-reader-diagnosis-20260908.md); original baseline measurements remain unchanged.

**The 8,192-block Triton test passed. The response-body error did not recur.**
Jetstreamer decoded all 8,925,832 transactions in 471.002 seconds, or
**18,951 TPS**. Every ordered block and transaction count matched the canonical
plan. Slot 388805945, where the previous mirror attempt reported its error,
also passed with 1,223 transactions.

Both the CAR file and slot index came directly from
`https://files.old-faithful.net/900/`. This is Jetstreamer's public default
archive source. See the [upstream source configuration](https://github.com/anza-xyz/jetstreamer#alternate-archive-mirrors).
Our Cloudflare Worker and bucket were not in the timed read path.

## Same test, different source

| Measure | Our mirror, previous attempt | Triton, new attempt |
| --- | ---: | ---: |
| Epoch / requested blocks | 900 / 8,192 | 900 / 8,192 |
| Processing workers | 12 | 12 |
| Completed blocks | 6,020 | 8,192 |
| Decoded transaction callbacks | 6,546,976 | 8,925,832 |
| Firehose call time | 346.388 s, stopped on error | 471.002 s, complete |
| Complete ordered count check | FAIL | PASS |
| Accepted TPS for this prefix | — | 18,951 |
| Reported firehose errors | 1 | 0 |
| Mean CPU cores used | 10.38 | 10.36 |
| System share of CPU time | 62.5% | 60.9% |
| Sampled peak RSS | 52.1 MiB | 53.1 MiB |
| Kernel lifetime peak RSS | 109.5 MiB | 106.3 MiB |

The executable, decoded callback mode, 12-worker setting, slot range and strict
error policy were unchanged. The range was `[388800000, 388808198)`.
The previous incomplete run cannot be used as an accepted speed comparison.
This test covers one prefix, not the full epoch. No full epoch was started.

The measured time is the complete firehose call, including its internal source
and index setup. The adapter does not report separate HTTP setup time. It
counts all transactions, including votes and failed transactions, and decodes
transaction metadata. It does not verify signatures. Its work differs from the
V2/V3 identity-only scans, so these rates do not establish a format speed ratio.

## What this tells us

The error did not reproduce with Triton in this attempt. This supports further
checks of our delivery path, but it does not prove that our gateway caused the
previous failure. A transient network or client failure remains possible.

An open [Jetstreamer issue, #69](https://github.com/anza-xyz/jetstreamer/issues/69),
reports the same response-body error from `files.old-faithful.net` in epoch 975.
It was checked on 8 September 2026. That is a separate report with no confirmed
shared cause. Neither this passing test nor that issue proves that Triton or
Jetstreamer is free of bugs, or identifies the cause of our earlier error.

The high CPU cost persists with Triton: 1,908.8 user CPU seconds and 2,969.1
system CPU seconds. Our gateway alone therefore does not explain this cost.
The client, adapter, runtime and NAS remain common to both tests. Allocation,
thread contention and system calls are candidates for a separate profile;
none is established as the cause by this run.

There were no reported firehose errors, and the error policy was not relaxed.
Normal Jetstreamer can retry after errors. Lower-level retries and planned
connection changes are not counted by the adapter, so this is not a claim of
zero transport retries.

## Checks and retained evidence

The complete 5,184,000-byte slot index had the same SHA-256 on both origins:
`b9551b93bd720c221c7d1a605452bd9a676e326344ef56371a03ad44e32c1056`.
The raw CAR length was 527,045,598,158 bytes. Three 64-KiB CAR samples, at the
start, middle and end, also matched. Triton's checks were unchanged after the
run. Triton did not provide an ETag in the inspected responses. These checks
do not prove whole-CAR byte identity or pin each request to one file version.

The local archive metadata and canonical index hash were unchanged. Exact
ordered output checks passed on the NAS and again after receipt transfer.
All 12 workers processed blocks. The supervisor and reader have stopped.

NAS eth0 received 9.85 GB during the timed process. This includes other host
traffic, so it is not an exact Jetstreamer HTTP byte count. The adapter does not
provide that counter; no accepted MB/s is reported. Process storage reads were
zero. One-second RSS samples can miss short peaks; the kernel lifetime maximum
also includes startup and final report work.

A first preflight check stopped before Jetstreamer started: Triton returned a
valid HTTP 200 for a request covering the entire index, but the check expected
206. The check was changed to use a normal full GET for the small index. CAR
range checks still require 206 and exact Content-Range and body length. This
was a benchmark control correction, not a Triton or Jetstreamer bug.

Jetstreamer firehose: 0.7.0, revision
`cffaf3d891b3cbe45a46dd963d6d3571b2aa1a24`.
Executable SHA-256:
`5a0a17b53e85a2de48caae864d69a0d1baa295e7ba25d892dd4c822d8c527a87`.
Control package SHA-256:
`6a375b758cb9b2547cb9a6607ef70ac7b3168197c3e682caabbbb1b4fd3fbeba`.

[Machine-readable result and receipt hashes](artifacts/jetstreamer-triton-20260908.json).
[Previous mirror failure](jetstreamer-epoch900-retry-20260908.md).
Full logs, source checks, exact rows and resource samples are retained in
`target/nas-validation/jetstreamer-triton-20260908T150549Z`.
