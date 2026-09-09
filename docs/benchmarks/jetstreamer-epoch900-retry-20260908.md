# Jetstreamer epoch 900 retry — 8 September 2026

> **Follow-up:** the same prefix passed from Triton. See the
> [source comparison](jetstreamer-triton-20260908.md). The failed mirror result below is unchanged.

**The retry failed its 8,192-block check. The full epoch was not started.**
The same Jetstreamer 0.7.0 executable and 12 processing workers were used.
The source was raw CAR over the existing HTTPS sample gateway.

| Check | Result |
| --- | --- |
| Requested blocks / transactions | 8,192 / 8,925,832 |
| Completed blocks / transaction callbacks | 6,020 / 6,546,976 |
| Elapsed firehose call before failure | 346.388 s (5 min 46 s) |
| Process exit code | 1 |
| Complete exact block-plan check | FAIL |
| Completed rows match the expected rows | Yes, but 2,172 blocks are missing |
| Full epoch retry | Not started because the prefix failed |

The reported error was:

```text
upstream retry invalidates sample: slot=388805945 Error reading until block: error decoding response body
```

This is an HTTP response-body read error, not evidence of a transaction-count
mismatch. The unchanged benchmark adapter deliberately stops when Jetstreamer
reports an upstream error. Jetstreamer can normally retry a stream. This result
therefore describes the strict adapter run; it does not prove that Jetstreamer
cannot recover. No retry or acceptance rule was relaxed to obtain a pass.

The earlier full-epoch attempt stopped on read timeouts after 1,549.322 seconds.
This new attempt has a different reported error. It does not establish that the
same cause produced both failures. The adapter reports no nested transport
cause, so a server reset, network interruption or other body-stream problem
cannot be distinguished from this record alone.

## Resource observations

The process used 1,350.5 user CPU seconds and 2,246.7 system CPU seconds over
346.4 seconds elapsed: **10.38 CPU cores on average**, with 62.5% of CPU time
in the operating system. This differs from the V2/V3 network identity scans,
which used less than one core on average. High CPU use alone does not prove
useful decoding throughput. The cause of the high system CPU cost is unresolved.

RSS samples taken once per second peaked at 52.1 MiB. The kernel lifetime peak
was 109.5 MiB; it includes process startup and final report work, and can capture
peaks between samples. The process recorded zero storage-read blocks. NAS eth0
received 7.19 GB during the attempt; this includes other host traffic and is not
an exact Jetstreamer HTTP-body counter. Jetstreamer's body-byte total remains
unavailable. No accepted TPS or MB/s is published for this partial run.

The gateway log tail recorded no slow or failed R2 operation during its
observation window. Its timer ends when R2 returns the stream handle. It cannot
rule out a later failure while the response body is sent.

## Source and output checks

The raw CAR length (527,045,598,158 bytes), slot-range index length (5,184,000
bytes) and strong HTTP ETags were unchanged before and after the attempt. Local
archive metadata and the canonical V2 index hash were also unchanged. Every
completed block row and its transaction count match the expected plan, but the
complete requested plan does not match because the run stopped early.

The corrected reference executable has SHA-256
`5a0a17b53e85a2de48caae864d69a0d1baa295e7ba25d892dd4c822d8c527a87`.
Upstream revision: `cffaf3d891b3cbe45a46dd963d6d3571b2aa1a24`.
The independent verifier's two test methods passed before the retry, including
missing/duplicate/count/source/error mutation checks. No adapter, upstream,
dependency or reader code changed in this retry.

[Machine-readable failure and resource receipt](artifacts/jetstreamer-retry-20260908.json).
Full logs, partial output rows, expected prefix rows, process samples, executable
hashes and control metadata are retained under
`target/nas-validation/jetstreamer-retry-20260908T125915Z`.
The runtime control archive hash is
`3cbdc715a21ea8304ab7271fbd92ea829234456c74e82394b54799db5cc70865`.
The original failed run remains unchanged. The supervisor, reader and gateway
log tail have stopped. No local CAR decompression or archive writes occurred.

Before another long run, inspect the nested HTTP error and the high system CPU
cost in a separate diagnostic. If a future test permits recovered errors, record
them explicitly and still require complete exact ordered block-plan equality.
The adapter policy must be identified with that new result. Do not compare this
failed partial run with the V2/V3 identity scan as a speed ranking.
