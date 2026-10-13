# Rust callbacks

Read blocks and transactions in your own Rust code. The reader downloads what it
needs from the archive and calls your function once per block, in order.

## Run the example

From the root of your clone (see [Quick start](quick-start.md#1-install-the-cli)):

```bash
cargo run --locked -p blockzilla-compact-v2-reader --features http \
  --example read_epoch -- 200 2
```

The arguments are the epoch and the number of blocks. The example prints one
line per transaction, then a summary:

```text
slot=86400001 tx=0 ok instructions=1 66xSXUEjaV5boDSico94k4h118TTBk1uhH9XhYnc81fnRbHb5fstTxMXnBZtarndEgsCDfnkY4cH19aSxQ4RbJtd
slot=86400001 tx=1 ok instructions=1 2HMHWqE7kZTBz1LUVgV6r69TmvfWwiaSSRKJ4L3TU1y9ZQEGaqZXaKfhVu6T7M8hKAQDYWrsFi9JJstdCF8487oZ
slot=86400001 tx=2 failed instructions=1 4S4V8HtwcxeQFvfBwrkedRroxRcqqxiNjzcV8zs8Mv8DfsQCyRB5Xqt3L99hWqGGVWpJP3wUgySCwt8mmfNgTgBm
…
Read 2 blocks and 608 transactions
```

The first build of the example compiles the reader. It took about two minutes
on an Apple M1.

## The code

This is the core of [read_epoch.rs](../../crates/compact-v2/blockzilla-compact-v2-reader/examples/read_epoch.rs):

```rust
use std::num::NonZeroU32;

use blockzilla_compact_v2_reader::archive::{
    ArchiveInstructionSourceExt, CompactV2Archive, ExecutionStatus, ScanRequest,
};
use solana_signature::Signature;

const ARCHIVE: &str = "https://blockzilla-archive-samples-v1.cheron-augustin.workers.dev";

let cache = std::env::current_dir()?.join(".blockzilla-cache");
let mut archive = CompactV2Archive::open(ARCHIVE, 200, cache)?;

let range = archive.bounded_range(0, NonZeroU32::new(2).unwrap())?;
let request = ScanRequest::bounded(range);

let receipt = archive.for_each_block(&request, |block| {
    for tx in block.transaction_views() {
        let status = match tx.header.status {
            ExecutionStatus::Succeeded => "ok",
            ExecutionStatus::Failed => "failed",
            ExecutionStatus::Unknown(_) => "unknown",
        };
        let signature = tx
            .primary_signature
            .map(|bytes| Signature::from(*bytes).to_string())
            .unwrap_or_default();
        println!(
            "slot={} tx={} {status} instructions={} {signature}",
            block.header.slot,
            tx.header.tx_index,
            tx.instructions.len(),
        );
    }
    Ok(())
})?;

eprintln!("Read {} blocks and {} transactions", receipt.blocks, receipt.transactions);
```

- The cache path must be absolute.
- Every block reaches the callback, also a block with no transactions.
- Return an error from the callback to stop the scan.

## Use the reader in your project

The reader is not yet on crates.io. Until it is, use a path dependency to your
clone:

```toml
[dependencies]
blockzilla-compact-v2-reader = { path = "../blockzilla/crates/compact-v2/blockzilla-compact-v2-reader", features = ["http"] }
solana-signature = "3"
```

After the release, this will be `cargo add blockzilla-compact-v2-reader --features http`.

## Select blocks

| Request | Blocks |
| --- | --- |
| `ScanRequest::all()` | The whole epoch |
| `ScanRequest::bounded(archive.bounded_range(first, count)?)` | `count` blocks, starting at block number `first` |

Block numbers count the blocks in the archive: 0, 1, 2, and so on. They are not
slot numbers. Skipped slots have no block, so they do not use a number.

## Request only the data you need

`ScanRequest::all()` and `ScanRequest::bounded()` request everything:
instructions, instruction data, inner calls (CPI), execution status, signers,
and signatures. Remove what you do not need. The scan is then faster:

```rust
let request = ScanRequest::all()
    .without_instructions()
    .without_primary_signatures()
    .without_required_signers()
    .without_execution_status();
```

To add token balances or to select programs, see [Local filters](filters.md).

If the archive cannot supply data that your request requires, the scan stops
with an error. It does not give you incomplete data without telling you. If
you remove a field from the request, an empty value in your callback does not
mean the transaction had none.

**Epoch 0.** The first Solana blocks have no recorded inner calls (CPI) and no
execution results. A full request on epoch 0 stops with
`incomplete CPI coverage`. Accept the gap explicitly:

```rust
let request = ScanRequest::all()
    .allow_incomplete_cpi()
    .allow_unknown_execution();
```

Each transaction then has the status `Unknown`, not `Succeeded`. Other options
of this kind: `allow_incomplete_instructions()`,
`allow_incomplete_instruction_data()`, and `allow_incomplete_token_balances()`.
The scan receipt counts the affected transactions.

## Transaction callbacks

If you need only transactions:

```rust
let receipt = archive.for_each_transaction(&request, |tx| {
    println!("slot={} tx={}", tx.block.slot, tx.header.tx_index);
    Ok(())
})?;
```

Use block callbacks when you save a resume position, because empty blocks do
not reach a transaction callback.

## Use more CPU cores

For large scans, decode with several workers. Blocks still reach your sink in
order:

```rust
use blockzilla_compact_v2_reader::archive::{BlockView, CompactV2ParallelScanConfig, FnBlockSink};

let request = request.without_instruction_data();
let mut sink = FnBlockSink::new(|block: BlockView<'_>| {
    println!("slot={}", block.header.slot);
    Ok(())
});
let receipt = archive.scan_ordered_parallel(
    &request,
    &mut sink,
    CompactV2ParallelScanConfig::new(4),
)?;
```

The parallel scan does not return instruction data bytes. Use the sequential
`for_each_block` when you parse instruction data. A slow sink slows the scan,
because the reader keeps only a few decoded blocks in memory.

## Save your position

The reader does not save a resume position for you.

- Blocks and transactions borrow memory from the reader. Copy what you keep.
- Save your output and the next block number in one database transaction.
- To resume, start a bounded request at the saved block number.
- Save the archive identity with your position, and check it on restart.
  `archive.identity()` (trait `ArchiveInstructionSource`) returns it, and it
  serializes with serde.

The receipt counts the blocks and transactions that the scan read. A complete
scan reads every block in the archive file. It does not prove that the archive
has every block that Solana produced. See [Data and coverage](../advanced/coverage.md).

Next: [Local filters](filters.md).
