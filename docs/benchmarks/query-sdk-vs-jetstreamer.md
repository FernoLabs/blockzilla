# Query SDK and Jetstreamer comparison

Status: benchmark contract and one Jetstreamer CAR reference result,
2026-08-28. This document does not contain a measured Blockzilla Mac result.

This comparison has two goals:

1. Prove that CAR, Compact V2, and Indexer V3 produce the same application
   records.
2. Compare source bytes, request count, time, and application code size.

Do not rank formats until the same query, block range, network origin, cache
state, and output work are used.

## Ease-of-use rule

Each Blockzilla demonstration must contain:

- one source-open call;
- one scan request;
- one callback or sink;
- one scan receipt.

The query logic must not contain format-specific decoding. CAR, Compact V2,
and Indexer V3 must use the same query function.

Jetstreamer stays an independent CAR reference. It must not become a
dependency of the Blockzilla query SDK.

## Required benchmark outputs

Each run must report:

- source format and immutable identity;
- source verification level;
- exact block and transaction universe;
- semantic output count and digest;
- incomplete instruction, CPI, and execution coverage;
- open time, scan time, selection time, and output time;
- source requests and returned bytes when exact counters exist;
- decoded bytes;
- persistent-cache reads;
- cold or warm cache state;
- worker count and peak memory.

Server logs can supply network-byte counts when a reader does not expose them.
The report must state this difference.

## Implemented Blockzilla network command

The bounded
[`archive-token-events`](../../examples/archive-token-events/README.md)
command reads the three archive formats from one public HTTPS Worker origin.
It does not use a local HTTP server.

```bash
cargo run --locked -p blockzilla-archive-token-events -- \
  network \
  --origin https://blockzilla-network-format-benchmark-v1.cheron-augustin.workers.dev \
  --epoch 0 \
  --max-blocks 1024 \
  --output-root /private/tmp/blockzilla-token-events-e0
```

The output root must be an absolute private path. The command accepts exactly
these sample epochs: `0`, `100`, `200`, `300`, `400`, `500`, `600`, `700`,
`800`, `900`, and `1000`. One run can read at most 1,024 canonical block rows.
This is a demo limit. It is not an SDK or archive-format limit.

The command keeps each archive in a separate folder:

```text
<output-root>/
  archive-cache/
    origin-.../
      compact-v2/
      indexer-v3/
  car/epoch-N/
    token-events.sqlite
    report.json
  compact-v2/epoch-N/
    token-events.sqlite
    report.json
  indexer-v3/epoch-N/
    token-events.sqlite
    report.json
  comparison/epoch-N/
    comparison.json
```

The V3 reader caches the bounded block index and the required registry. It
uses pinned, uncached range reads for the large transaction directory,
optional signatures, and semantic planes. A benchmark must call
`ArchiveSource::finish_io` before it records final transport totals.

The three sources keep these exact SDK trust levels:

| Format | Trust level |
|---|---|
| CAR | `operator-trusted` |
| Compact V2 | `published-manifest` |
| Indexer V3 | `internal-binding-only` |

The V3 path explicitly accepts the weaker source. Equal output does not make
V3 publication-verified.

All three paths use the same source-neutral token scan driver and SQLite sink.
The result contains classic Token instruction events, account-lifetime
evidence, instruction-derived delta legs, and explicit coverage gaps. It does
not use pre-token or post-token balance observations. It is not an observed
balance ledger.

The comparison first audits each database in read-only mode. It then
merge-compares full token-event, coverage, tracker, and ledger-control rows. It
uses raw 32-byte public keys instead of database-local key IDs. It also checks
the stored SHA-256 digest of each complete canonical `BlockView`. The database
does not keep a second full source projection. Thus, full-row
source-projection parity stays `not-proved-full-row`.

Epoch 0 is only a structural network example. The current epoch-0 Compact V2
and Indexer V3 samples have limited metadata, and USDC is absent from this
range. Do not use an empty epoch-0 result as a throughput result or a
semantic-completeness result.

## Current Jetstreamer CAR reference

The retained corrected r2 evidence used the Cloudflare CAR source and the
exact epoch-0 slot range 0 through 1023. It did not use a local HTTP server.

| Item | Result |
|---|---:|
| Possible slots | 1,024 |
| Real blocks | 1,023 |
| Inferred skipped genesis slot | 1 |
| Transactions | 4,091 |
| Total wall time | 1.776 s |
| Worker CAR requests | 8 |
| Worker returned bytes | 234,881,025 |
| CAR semantic parity | Passed |

Jetstreamer did not request the separate slot index for this epoch-boundary
sequential run.

This is a valid Jetstreamer CAR reference result. It is not a three-format
performance result. A measured Blockzilla Mac run and an equal-output check
are still required before a speed comparison is valid.

The current Jetstreamer reference tool does not run the new instruction-event
ledger. Its USDC projection counts pre-token and post-token balance records.
Thus, its 1.776 s wall time is a CAR transport and transaction-universe
reference only. Do not compare this time with the Blockzilla instruction-event
times.

## Blockzilla advantages to prove

The final demonstrations must prove these properties in code and reports:

- one query function for all three archive formats;
- explicit source identity and verification;
- bounded historical ranges;
- exact outer and CPI coordinates;
- explicit missing-data coverage;
- deterministic empty-block checkpoints;
- optional exact instruction bytes by selected program;
- exact network and cache receipts where the source exposes them;
- restart-safe database output.

These properties must stay in the SDK. An example must not implement them
again.

## Next measurement

First, update the Jetstreamer example to use the same instruction-event rules
and output work. It must not use pre-token or post-token balance observations
for this comparison. Then run the same 1,024-block USDC instruction-event
query for:

1. Jetstreamer over the Cloudflare CAR object;
2. the Blockzilla CAR adapter over the same object;
3. the Compact V2 adapter over its Cloudflare folder;
4. the Indexer V3 adapter over its Cloudflare folder.

The implemented command creates a separate output folder for each format. Use
a fresh output root for a cold run. Use the same output root only for a named
resume or warm-cache run. Compare canonical event rows before the speed result
is accepted.
