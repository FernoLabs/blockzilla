# Read the standalone Indexer V3 prototype

The `read-archive-v3*` programs below read the frozen flat
`archive-v2-standalone-*` layout. They cannot open canonical Archive V3 converter
output. Use `ia-read` in this package for the new `catalog/` and `ledger/` layout;
see the [converter guide](../../crates/archive-v3/blockzilla-archive-v3-convert/README.md).

These four small programs show one job each. They use the Archive V3
SDK directly and do not select an archive format at run time.

- [`read-archive-v3-slot-hours`](src/bin/read-archive-v3-slot-hours.rs) reads
  every block and transaction and counts recorded inner instructions in
  slot-derived, approximate one-hour windows.
- [`read-archive-v3-usdc`](src/bin/read-archive-v3-usdc.rs) writes recorded
  USDC pre- and post-token balances.
- [`read-archive-v3-pumpfun`](src/bin/read-archive-v3-pumpfun.rs) writes
  transactions that call the Pump.fun program directly or through recorded
  CPI.
- [`read-archive-v3-firewatch`](src/bin/read-archive-v3-firewatch.rs) writes
  the distinct programs reached by successful transactions from one signer
  wallet.

Each program reads the complete selected epoch. There is no block-limit flag
in these examples.

USDC and Pump.fun exclude known failed transactions and report their count.
Unknown execution status remains a coverage warning. The count example still
includes all transactions. See the [shared workload rules](../workloads/README.md).

## Start with local archive files

Build the examples:

```console
cargo build --release --locked -p blockzilla-read-archive-v3 \
  --bin read-archive-v3-slot-hours \
  --bin read-archive-v3-usdc \
  --bin read-archive-v3-pumpfun \
  --bin read-archive-v3-firewatch
```

The clean public sample bucket is still being staged. To run the standard
reader now, put the epoch files in `archive/indexer-v3/900` and select the
local archive root:

```console
cargo run --release --locked -p blockzilla-read-archive-v3 \
  --bin read-archive-v3-slot-hours -- \
  --archive-root archive
```

It reads every block and transaction in local epoch 900. It prints block,
transaction, and recorded inner-instruction counts. It does not create an
output file.

The program groups counts into fixed 9,000-slot windows. The zero-based
`approximate_hour=0` window starts at the epoch start slot. At an assumed 400
ms for one slot, each window is about one hour. The hour labels are
approximate; the program does not use block time. It also rejects duplicate or
out-of-order block slots.

Run one real-world workload from the same local archive:

```console
cargo run --release --locked -p blockzilla-read-archive-v3 \
  --bin read-archive-v3-usdc -- \
  --archive-root archive
```

The source-code default remains the future public origin:

```text
https://blockzilla-archive-samples-v1.cheron-augustin.workers.dev
```

After the clean bucket is active, each object will use this direct path:

```text
https://blockzilla-archive-samples-v1.cheron-augustin.workers.dev/indexer-v3/900/<object>
```

It writes `indexer-v3-usdc.bin` in the current directory. The output must not
exist before the run.

The other two real-world examples are:

```console
cargo run --release --locked -p blockzilla-read-archive-v3 \
  --bin read-archive-v3-pumpfun -- \
  --archive-root archive

cargo run --release --locked -p blockzilla-read-archive-v3 \
  --bin read-archive-v3-firewatch -- \
  --archive-root archive
```

FireWatch uses the sample wallet
`5LikTUsx695BHRipWoRrn6YmTQEcPrvbR8YaHxdSRQo8` by default.

Use these simple options when needed:

```text
--epoch N          0, 100, 200, ..., or 1000; default 900
--archive-root DIR local archive tree; do not use with network options
--origin URL       another compatible origin; the default bucket is staged
--cache-root DIR   persistent network cache; default archive-cache/indexer-v3
--threads N        worker count; default is chosen by the SDK
--output FILE      USDC, Pump.fun, and FireWatch only
--wallet KEY       FireWatch only
```

## Use the same layout on disk

Use the planned public object paths without changing their names. A local
epoch has this layout:

```text
archive/
  indexer-v3/
    900/
      <all V3 ledger, reverse-index, and retained objects>
```

All V3 objects are flat in the epoch directory. Then add one option:

```console
cargo run --release --locked -p blockzilla-read-archive-v3 \
  --bin read-archive-v3-slot-hours -- \
  --archive-root archive
```

Change `--epoch` to select another local sample. Do not combine
`--archive-root` with `--origin` or `--cache-root`.

`IndexerV3Archive::open_local("archive", 900)` derives the full directory. It
does not need a candidate ID or separate ledger and sidecar roots. The V3 file
header selects its encoded message and metadata schema. The command does not
need a schema option. The same interface reads each sample epoch.

## What the examples show

USDC uses an ordered full scan because a public-key posting does not prove a
recorded token-balance mint match.

Pump.fun and FireWatch use the V3 reverse index to find sound candidate
blocks. Coverage fallback blocks stay in that set. The workload sink then
checks each decoded transaction before it writes an exact result. Thus, the
reverse index can skip unrelated blocks without changing the application
rule.

The SDK keeps output in ledger order when it uses multiple workers. The sinks
write the same deterministic fixed-record formats that the other archive
examples use.

## Advanced tools

The package also contains tools for measurements and special jobs:

- [`read-archive-v3`](src/main.rs) is the ordered reader benchmark.
- [`read-archive-v3-transactions`](src/bin/read-archive-v3-transactions.rs)
  writes the exact transaction identity stream for parity tests. Its epoch 900
  output is 38,082,144,928 bytes. It keeps the advanced benchmark controls and
  report fields.
- [`read-archive-v3-usdc-instructions`](src/bin/read-archive-v3-usdc-instructions.rs)
  is a bounded SQLite correctness runner.
- [`ia-read`](src/bin/ia-read.rs) reads one block from a converted candidate.
  Add `--full` to decode its runtime effects.

These tools have more controls and more report fields. They are not the first
examples to copy into an application. The advanced two-root
`IndexerV3Archive::open_local_split` API remains available for operator
storage layouts.

For cross-format output rules, see the
[`blockzilla-example-workloads` guide](../workloads/README.md).
For the complete SDK contract, see the
[Archive V3 SDK README](../../crates/archive-v3/blockzilla-archive-v3-reader/README.md).
