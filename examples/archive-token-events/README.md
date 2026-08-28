# Archive token-event example

This example reads the same ordered classic SPL Token instruction stream from
three archive formats:

- Old Faithful CAR;
- Blockzilla Compact V2;
- Blockzilla Indexer V3.

It uses one source-neutral tracker and one SQLite sink. It does not use pre- or
post-token balances. It records instruction events, token-account lifetime
evidence, transfer legs, mint and burn deltas, and explicit coverage gaps.

## Network demo

The network command gives one Worker origin and one cache root to
`blockzilla-archive-sdk`. The facade owns the Worker file names, HTTP setup,
cache layout, object pinning, source bindings, range validation, and the full
CAR canonical plan. The example owns only token processing, SQLite output,
reports, and parity checks.

The archive part of the application flow is small:

```rust
use std::{num::NonZeroU32, path::Path};

use blockzilla_archive_sdk::{
    ArchiveFormat, ArchiveInstructionSource, ArchiveInstructionSourceExt,
    NetworkEpoch, ScanRequest, WORKER_FORMATS,
};

fn inspect_archives(
    origin: &str,
    epoch_number: u64,
    cache_root: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut epoch = NetworkEpoch::open(origin, epoch_number, cache_root)?;
    let range = epoch.bounded_range(0, NonZeroU32::new(1_024).unwrap())?;

    for format in WORKER_FORMATS {
        let mut source = epoch.open_source_for(format, range)?;
        let open = source.open_receipt().clone();
        let before_scan = source.io_snapshot();
        let request = match format {
            ArchiveFormat::IndexerV3 => {
                ScanRequest::bounded(range).allow_unverified_source()
            }
            _ => ScanRequest::bounded(range),
        };

        let receipt = source.for_each_block(&request, |block| {
            println!("slot={} transactions={}", block.header.slot, block.transactions.len());
            Ok(())
        })?;
        let after_scan = source.io_snapshot();
        let scan_io = after_scan.saturating_sub(before_scan);
        let final_io = source.finish_io();
        println!("{format}: {open:?} {receipt:?} {scan_io:?} {final_io:?}");
    }

    Ok(())
}
```

The token-event command uses the same setup and replaces the small callback
with `blockzilla_dump::scan_remaining_token_events` and its restart-safe SQLite
database.

The example does not import a CAR reader, an HTTP range source, a manifest
reader, or an Indexer V3 reader.

```bash
cargo run --locked -p blockzilla-archive-token-events -- \
  network \
  --origin https://blockzilla-network-format-benchmark-v1.cheron-augustin.workers.dev \
  --epoch 0 \
  --max-blocks 1024 \
  --output-root /private/tmp/blockzilla-token-events-e0
```

The output root must be an absolute path in a private folder. The command
accepts one supported sample epoch per run: `0`, `100`, `200`, `300`, `400`,
`500`, `600`, `700`, `800`, `900`, or `1000`. The demo has a hard limit of
1,024 block rows per run.

Indexer V3 remains `unverified-nonpublishable`. Its SDK identity is
`internal-binding-only`, and the request above explicitly accepts that weaker
source. Output parity does not make it publication-verified. Selecting CAR is
also an explicit operator-trust decision; its identity stays
`operator-trusted`.

The default mint is the Solana USDC mint. Use `--mint` for another classic SPL
Token mint. The default history mode is `sparse`. The alternative
`--history-start trusted-complete-empty` is an operator assertion that no
target token account existed before the selected range.

The command writes one isolated folder for each format:

```text
<output-root>/
  archive-cache/
    origin-.../
      compact-v2/...
      indexer-v3/...
  car/epoch-0/
    token-events.sqlite
    report.json
  compact-v2/epoch-0/
    token-events.sqlite
    report.json
  indexer-v3/epoch-0/
    token-events.sqlite
    report.json
  comparison/epoch-0/
    comparison.json
```

The facade gets the full canonical slot plan from Compact V2 and binds that
full plan to the CAR identity. A smaller requested range does not change the
CAR source identity. Empty and omitted slots stay identical at the SDK
boundary. The facade also requires the V3 rows to be a dense prefix of this
plan and requires the requested range to fit in that prefix.

Run the read-only comparison again without a network scan:

```bash
cargo run --locked -p blockzilla-archive-token-events -- \
  compare \
  --epoch 0 \
  --output-root /private/tmp/blockzilla-token-events-e0
```

## Result meaning

The database is an instruction-event ledger. It is not an observed token
balance ledger. A transfer row gives exact instruction evidence and ordered
debit and credit legs. It does not prove the absolute account balance unless
the run starts from complete trusted history.

Sparse mode keeps uncertainty explicit until exact instructions prove an
account lifetime.

Epoch 0 is a structural network example. The current Compact V2 and Indexer V3
epoch-0 samples have limited metadata, and USDC is absent from that historical
range. Do not use an empty event result as a throughput or semantic-completeness
claim.

## Performance scope

The example favors a small common API and exact output. The adapters are
sequential reference scanners. CAR still fetches closed HTTP ranges
concurrently, and V3 batches adjacent plane ranges. Reports use one normalized
I/O snapshot for all formats. Final totals come from `ArchiveSource::finish_io`,
after background transport work stops. V3 caches its bounded block index and
required registry; its large transaction directory, optional signatures, and
semantic planes remain pinned, uncached range reads. Reports keep SDK open
work, setup, scan, final audit, total wall time, request, byte, cache, and
coverage values separate. Do not compare one total time without these scopes.

The comparison merge-checks full token-event, coverage, tracker, and ledger
rows. It resolves database-local public-key IDs to raw 32-byte addresses. It
also compares each stored per-block SHA-256 digest of the full canonical
`BlockView`. The database does not retain a second complete source projection,
so the report marks full-row source-projection parity as `not-proved-full-row`.
