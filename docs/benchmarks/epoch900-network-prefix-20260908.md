# Epoch 900: short network reader test

8 September 2026. **PASS:** V2 and the standalone V3 prototype produced the
same ordered transaction records from local files and from the public gateway.
The test selected the first 8,192 blocks, used 12 processing workers, and ran
one reader at a time on the NAS. Each output contains **8,925,832 transactions**.

This test exports transaction coordinates and primary signatures. It does not
count instructions or CPI. Its TPS must not be compared directly with the
full-epoch count results. This is a measurement of the current candidate, not
a before/after comparison with an old reader.

## Network timing and transaction rate

| Reader | Setup seconds | Scan seconds | Total seconds | Scan TPS | Total TPS |
| --- | ---: | ---: | ---: | ---: | ---: |
| V2 | 15.47 | 20.59 | 36.13 | 433,468 | 247,020 |
| V3 prototype | 62.20 | 28.71 | 91.03 | 310,940 | 98,050 |

Total time includes setup and durable output completion. Both readers reported
12 effective workers and a maximum of 12 active processing workers. That does
not mean all workers were busy for the complete run.

## Network transfer rate

Decimal GB and MB/s measure HTTP response-body bytes consumed by the reader.

| Reader | Setup GB | Scan GB | Scan MB/s | Total MB/s |
| --- | ---: | ---: | ---: | ---: |
| V2 | 0.912 | 1.714 | 83.22 | 72.66 |
| V3 prototype | 1.449 | 0.832 | 28.98 | 25.05 |

The local references took 1.71 seconds for V2 and 1.47 seconds for V3 overall.
Their scan times were 1.62 and 1.26 seconds. OS caches were uncontrolled; these
small local measurements are not a claim about cold SSD throughput.

## What this shows

V3 read fewer payload bytes, but its network scan was slower. Its setup took
46.73 seconds longer than V2, which accounts for about 85% of the total-time
gap in this short test. The ordered V3 cache policy downloads the complete
transaction directory (1.342 GB) and block index (0.107 GB) before scanning,
even for this small prefix. V2 downloads its registry and block index at setup.
These are existing cache policies, not new changes made for this test.

A useful next change is to test bounded transaction-directory reads for short
V3 scans, while retaining whole-file caching for full scans when it helps.
V3's input producer also still reads selected planes in sequence. These are
candidate improvements; this test does not establish their expected speed gain.

The live gateway log observation returned no slow/error R2 operation records.
That is not proof that no request failed or stalled. The operation timer ends
when R2 returns metadata or a stream handle; it does not time body transfer.
Both readers reported zero incomplete-body retries. These exporters do not
report GET counts or server-error retry counts, so those values remain unknown.

## Checks and preserved attempts

All four output files were independently hashed after timed reads stopped.
Each is 714,066,608 bytes and has SHA-256:

```text
54531e48c946cb66c0c148d7339fdbb11511cffaaec9bc9c5a33f650a10d3685
```

Exact block/transaction counts, first/last output slots, schema length, binary
hashes, local file identities, public lengths and strong ETags passed checks.
The first output slot is 388800002; the last is 388808197. All reader processes
and the live log observation have ended. No archive files were changed.

The runtime build used the same 639 recorded source files as the previous
tested candidate. The two exporter checks passed, and the Linux release build
passed. This run made no SDK or Worker source change.

Three separate control directories retain the complete history:

- `network-prefix-20260908T094910Z`: preflight failed before scans because the
  gateway rejected Python's default User-Agent with HTTP 403. An explicit
  benchmark User-Agent passed the same metadata request.
- `network-prefix-20260908T095408Z`: both local cases passed. Network startup
  failed before transfer because inherited NAS cache permissions were too broad.
- `network-prefix-20260908T095706Z`: explicit private folder permissions fixed
  startup. Both network cases passed against the retained local references.

The two network cases used new application caches. Remote caches, OS caches
and run order were not controlled. Each source/format was measured once.
The prior 132 local and nine network baseline results remain unchanged.

[Machine-readable results](artifacts/network-prefix-20260908.json) include the
metrics, binary hashes and receipt hashes. Full receipts and small logs are
retained locally under `target/nas-validation/network-prefix-20260908T095706Z/`.
