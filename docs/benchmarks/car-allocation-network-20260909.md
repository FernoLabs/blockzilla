# CAR allocations and network capacity — 9 September 2026

Borrowed transaction metadata removed **95.8% of Rust allocation requests** in
this CAR scan. With the musl System allocator, mean process CPU time fell from
263.9 to 21.2 CPU-seconds. The network remains a separate limit. More HTTP
workers did not improve the complete reader in this round.

The preceding V3 signature coalescing and CAR HTTP retry work is in commit
`6efc29f`. This report covers the next allocation pass. V2/V3 scan code and
their default settings are unchanged.

## Allocation changes

The CAR probe now has `--metadata-mode visitor`. It uses the existing SDK
callback API and requests all known transaction metadata fields. Strings,
bytes, balances, instructions, and token balances are consumed without building
metadata collections. Transaction rewards are also decoded; their raw callback
alone would not be sufficient. Block rewards and legacy metadata retain the
owned decoder. The default mode remains `owned` for comparison.

The SDK's older metadata converter now moves balance vectors, logs, instruction
payloads, token strings, return data, and reward keys into the result. It no
longer copies those buffers before dropping the originals. This benefits
legacy metadata; it does not account for the epoch 900 results below.

The generated borrowed protobuf view still allocates repeated-field vectors.
The visitor avoids those collections. Transaction instruction payloads still
use reusable owned buffers, and decompression still writes an output buffer.
This is not a claim that the complete reader performs zero allocations.

## Same-prefix comparison

The NAS read the first **2,048 canonical blocks of epoch 900**, containing
**2,208,720 transactions**, from our HTTPS mirror. Each case used 12 decode
workers and a fresh process. Only one benchmark reader ran at a time. For each
allocator, the order was owned/visitor/visitor/owned. No CAR was decompressed
to a local file.

These ordinary timing runs have allocation counters disabled. TPS is the
transaction count divided by mean elapsed time. CPU time is for the complete
process. MB/s uses exact received HTTP body bytes, including read-ahead, divided
by combined scan time; it is not useful decoded-payload throughput.

| Allocator / metadata | HTTP workers | Setup (s) | Scan (s) | Scan TPS | Total TPS | CPU-seconds |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| System / owned | 4 | 0.127 | 24.705 | 89,403 | 88,948 | 263.9 |
| System / visitor | 4 | 0.131 | 14.564 | 151,652 | 150,297 | 21.2 |
| mimalloc / owned | 4 | 0.130 | 19.350 | 114,144 | 113,382 | 19.1 |
| mimalloc / visitor | 4 | 0.118 | 10.627 | 207,832 | 205,556 | 15.8 |
| mimalloc / visitor | 8 | 0.122 | 16.478 | 134,038 | 133,049 | 15.7 |

| Allocator / metadata | HTTP workers | Scan body MB/s | Peak RSS range (MiB) |
| --- | ---: | ---: | ---: |
| System / owned | 4 | 107.3 | 339.9–341.9 |
| System / visitor | 4 | 178.5 | 273.9–338.1 |
| mimalloc / owned | 4 | 137.0 | 367.6–370.2 |
| mimalloc / visitor | 4 | 241.5 | 378.3–395.4 |
| mimalloc / visitor | 8 | 160.9 | 456.8–526.7 |

The eight-worker cases ran after the four-worker comparisons, twice. Both used
the same eight-chunk, 256 MiB HTTP body window. They were slower and used more
RSS in this round. Their placement and network variation prevent a clean causal
claim about concurrency. These results do not justify changing the four-worker
default. The HTTP body window is not a limit on process RSS or allocator caches.

The visitor improved measured TPS by 69.6% with System and 82.1% with mimalloc.
These are end-to-end results, not isolated parser improvements. Network variation
is substantial: the mimalloc owned results here are slower than the earlier
8,192-block test. Keep comparisons within this repeated, identical prefix.
The much larger CPU reduction with System is stronger evidence of allocation
cost than wall time alone. It does not identify a particular allocator lock.

## Separate allocation counts

One additional run per metadata mode enabled shared Rust allocation counters.
These counts include allocation and reallocation requests during the scan,
including HTTP work. They exclude setup and C zstd allocations. Instrumented
times are excluded from the tables above.

| Metadata mode | Allocation requests | Requests per transaction | Requested bytes (GB) | Requests of at most 64 bytes |
| --- | ---: | ---: | ---: | ---: |
| Owned | 66,401,028 | 30.06 | 12.49 | 60,206,631 |
| Visitor | 2,780,532 | 1.26 | 11.02 | 2,277,683 |

Total requests fell **95.8%** and requests of at most 64 bytes fell **96.2%**.
Cumulative requested bytes fell only **11.8%**. Large input and transport buffers
still dominate requested volume. Requested bytes are not live memory. The
mimalloc visitor cases did not show a peak-RSS reduction, despite fewer small
allocation requests.

## Why disk can be faster than this network reader

An 8 Gbps line is about **1,000 MB/s before overhead**. The NAS routes both our
mirror and Triton through `eth0`, whose negotiated link is 10 Gbps. Its separate
2.5 Gbps port is not on these routes. The sustainable WAN rate was not measured
independently.

A separate download-only test used persistent HTTP/1.1 connections, 16 MiB
ranges, and a reusable 1 MiB buffer per connection. Each case read 256 MiB, with
the order 1/4/8/8/4/1 connections per source. It performed no CAR decode or hash.
Every response passed status, range, length, and available ETag checks; source
HEAD values were stable. Triton supplied no ETag, so its check was size-only.

| Source | 1 connection (MB/s) | 4 connections (MB/s) | 8 connections (MB/s) |
| --- | ---: | ---: | ---: |
| Our mirror | 55.2 | 92.6 | 146.2 |
| Triton | 59.2 | 210.8 | 268.6 |

These are short Python/OpenSSL transport tests, not the Rust SDK or a maximum
line-capacity test. They include connection startup and use a different request
size. Do not compare their rates directly with the reader table. They show that
one stream uses only a small part of the stated line rate and that source and
concurrency affect the result. Median request-to-header time was approximately
113–151 ms on our mirror and 70–89 ms on Triton, depending on concurrency.

The code review explains part of the V3 gap too. V3 loads one input group at a
time. Its signature read overlaps with semantic-plane input, but the selected
semantic files are read in sequence. It therefore pays repeated HTTP request
delay even when decode workers are available. A fast line cannot remove those
waits. Disk has much lower request delay; local runs can also benefit from file
cache. The cache state was not reset for these comparisons.

The earlier V3 change already reduced scan requests from 2,176 to 192 with the
same input bytes. Further network work should test overlap between bounded
groups and measure request delay. Simply increasing worker count did not help
the CAR test here. This pass does not establish a WAN, gateway, or origin
bandwidth ceiling.

## Checks and records

All 12 comparison/diagnostic cases passed. Counts and per-block
signature/message-hash/status digests matched each other and the corresponding
prefix of the earlier committed CAR reference. Sources stayed stable and all
cases reported zero incomplete-body retries. These digests do not prove parity
of every decoded metadata field, and this workload differs from V2/V3 identity
scans and Jetstreamer's retained native metadata output.

There were **133 passing CAR SDK tests and 5 passing probe tests**, including
real CAR fixtures, buffer ownership, and malformed nested metadata. The final
build adds only a malformed-input test and warning suppression relative to the
timed candidate. Both final allocator builds passed a further NAS prefix check
against the earlier digests. Their validation timings are kept separate from
the repeated comparison. The V2/V3 executable is byte-identical to the preceding
package. Formatting and diff checks passed.

See the [CAR probe instructions](../../bench/reader-profile/README-car-decode.md),
[comparison records and binary hashes](artifacts/car-allocation-network-20260909.json),
[transport request records](artifacts/network-capacity-20260909.json), and
[preceding network results](network-reader-comparison-20260909.md).
