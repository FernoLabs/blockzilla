# From CAR to V3: read less data, then keep the decoder supplied

8 September 2026. Our accepted benchmark covers 132 local tests across 11
epochs, plus nine network tests at epoch 900. V3 won 31 of the 44 local test
comparisons. V2 won 13. The V3 results below are for our frozen standalone
prototype, not the canonical Archive V3 format intended to replace V2.

CAR is the starting point. It preserves content-addressed source nodes, with
a slot index to locate data. This makes it a useful source archive, but broad
transaction queries still traverse and decode those records.

Outer zstd makes CAR smaller. Our reader decompresses it as it reads, with
no full-file restoration step. Ten local samples use this representation;
epoch 300 uses raw CAR. We did not test raw and zstd CAR for the same epoch
in this cohort, so these results do not isolate a zstd speed boost. At epoch
900, local zstd CAR and its index occupy 226.07 GB. The raw HTTP count consumed
527.05 GB, including its index. Storage savings and processing speed are
separate measurements.

Compact V2 changes the data layout. It stores compressed block frames,
replaces repeated public keys with compact IDs and keeps shared registries
and selected data in sidecars. Borrowed views and reusable buffers reduce
allocation during decoding. At epoch 900, V2 occupies 104.05 GB and completes
the local transaction/CPI count in 80.92 seconds, compared with 813.90 seconds
for CAR: a measured **10.06× speed increase** for that query and cohort.

The V3 prototype lets a query read only the fields it needs. Messages,
outcomes, balances and inner instructions are stored separately. Reverse
indexes also find candidate blocks for selected program and signer queries.
The reader then checks exact matches. At epoch 900, V3 occupies 106.32 GB and
completes count in 65.77 seconds: **12.38× faster than CAR and 1.23× faster
than V2**. V3 is slightly larger than V2 in this sample; less query I/O does
not necessarily mean a smaller complete archive.

The benefit depends on the query. V3 finishes epoch-900 USDC in 76.26 seconds,
versus 89.66 for V2. Pump.fun is almost equal: 281.17 seconds for V3 and 278.81
for V2. The V2 lead is only 0.85%, which needs repeated trials before we can
call it stable. The wallet query selects just ten V3 blocks and completes in
0.442 seconds. That is an index-selection benefit, not a claim that the
reader decodes the complete epoch at that speed.

![Measured local winners](artifacts/all-samples-reader-20260908/winner-matrix.png)

The network test shows the next problem. CAR count reached 206.49 MB/s of
HTTP response bodies. V2 count reached 24.17 MB/s; V3 count reached 6.75 MB/s.
V3 transferred less data but took longer. V2 has one download producer. V3
makes many small range requests tied to decode jobs. The decoders need a
better supply of input.

Jetstreamer provides a useful design example: long HTTP streams, or a
separate download buffer filled while the current buffer is processed. Our
CAR reader already has bounded concurrent range downloads. The proposed
V2/V3 change will combine adjacent selected ranges and download ahead of
the decode workers, while preserving index filtering, shared buffers,
ordered output and a fixed memory limit. **This change is not implemented;
no network speed increase has been measured yet.**

The Jetstreamer reference passed both small correctness checks, but its
full-epoch attempt failed with block-read timeouts. It has no accepted
full-epoch speed result. Its full transaction decoding also differs from
our SDK count projection.

These are single-pass results with uncontrolled OS/CDN caches. The NAS also
ran a CPU compaction job; its effect is unknown. Epoch-0 USDC and Pump.fun
outputs agree across readers but contain incomplete historical data. We
retain those limits and every failed attempt instead of correcting times
or removing difficult cases.

See the [complete report](all-samples-reader-comparison-2026-09.md) for every
test, separate TPS and read-rate plots, stored sizes, exact acceptance rules
and machine-readable evidence. This article replaces neither the fixed early
snapshot nor the separate historical pipeline measurements.
