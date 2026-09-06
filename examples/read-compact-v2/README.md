# Read Compact V2

These four small programs show how to use the Compact V2 SDK:

- [Count the epoch in approximate slot-hours](src/bin/read-compact-v2-slot-hours.rs)
- [USDC recorded balances](src/bin/read-compact-v2-usdc.rs)
- [Pump.fun transactions](src/bin/read-compact-v2-pumpfun.rs)
- [FireWatch wallet-to-program index](src/bin/read-compact-v2-firewatch.rs)

Each program opens one Compact V2 archive, builds one query, and streams the
result to one sink. The format-specific code stays in the program. Shared
workload code makes the V2 and V3 workload output identical.

USDC and Pump.fun exclude known failed transactions and report their count.
Unknown execution status remains a coverage warning. The count example still
includes all transactions. See the [shared workload rules](../../crates/blockzilla-example-workloads/README.md).

## Start with local archive files

The clean public sample bucket is still being staged. To run the example now,
put the epoch files in `archive/compact-v2/900` and select the local archive
root. This command reads every block, transaction, and recorded inner
instruction in local epoch 900:

```console
cargo run --release --locked -p blockzilla-read-compact-v2 \
  --bin read-compact-v2-slot-hours -- \
  --archive-root archive
```

The program prints one row for each 9,000-slot window. It anchors the first
window at the deterministic epoch start slot. At the assumed 400 ms slot time,
each window is about one hour. The windows are approximate; they are not UTC
clock hours. Each row has block, transaction, and recorded inner-instruction
counts. A recorded inner instruction is a CPI instruction that is present in
the archive metadata; the program does not infer missing CPI history. The
program does not write an output file.

This command reads all USDC recorded balances in epoch 900 and writes
`read-compact-v2-usdc-epoch-900.bin`:

```console
cargo run --release --locked -p blockzilla-read-compact-v2 \
  --bin read-compact-v2-usdc -- \
  --archive-root archive
```

Run the other two workload jobs:

```console
cargo run --release --locked -p blockzilla-read-compact-v2 \
  --bin read-compact-v2-pumpfun -- \
  --archive-root archive

cargo run --release --locked -p blockzilla-read-compact-v2 \
  --bin read-compact-v2-firewatch -- \
  --archive-root archive \
  --wallet 5LikTUsx695BHRipWoRrn6YmTQEcPrvbR8YaHxdSRQo8
```

The programs scan the complete selected epoch. They do not have a hidden block
limit. A full epoch can take time. The three workload jobs can write large
files. Their output file must not exist before the run.

The planned public samples are epochs `0`, `100`, `200`, `300`, `400`, `500`,
`600`, `700`, `800`, `900`, and `1000`. After the bucket is active, select one
and an output name with simple flags:

```console
cargo run --release --locked -p blockzilla-read-compact-v2 \
  --bin read-compact-v2-usdc -- \
  --epoch 100 --output usdc-epoch-100.bin --threads 12
```

The source-code default for the future public origin is:

```text
https://blockzilla-archive-samples-v1.cheron-augustin.workers.dev
```

After publication, the epoch 900 registry will be at:

```text
https://blockzilla-archive-samples-v1.cheron-augustin.workers.dev/compact-v2/900/registry.bin
```

Use `--origin URL` to use another compatible gateway. Network reads use the
private `.blockzilla-cache` folder in the current directory. Use
`--cache-root DIR` to select another cache folder.

The clean bucket is planned to contain 11 normalized Compact V2 samples that
use the same current message grammar and the same typed transaction-error
metadata grammar. The example has no old/new metadata table and no
epoch-specific schema switch. A sample that does not pass the current SDK
reader is repaired or rebuilt before publication; the reader does not select a
compatibility profile for that sample.

## Use the same layout on disk

Put local files under the same keys as the planned public source:

```text
archive/
└── compact-v2/
    └── 900/
        ├── archive-v2-blocks.zstd
        ├── archive-v2-blocks.index
        ├── archive-v2-meta.wincode
        └── registry.bin
```

The epoch folder can also contain the other fixed Compact V2 files. The SDK
admits the required files and the supported optional files.

Use `--archive-root` to select the local mirror explicitly:

```console
cargo run --release --locked -p blockzilla-read-compact-v2 \
  --bin read-compact-v2-usdc -- \
  --archive-root archive --epoch 900 --output usdc-local-900.bin
```

The program resolves this to `archive/compact-v2/900`. No manifest, hash file,
candidate name, or metadata flag is required.

## What is shared

The three sinks and their record formats are in
[`blockzilla-example-workloads`](../../crates/blockzilla-example-workloads).
They contain the application rules only. The Compact V2 SDK owns object
discovery, range reads, cache binding, decoding, block order, and parallel
projection.

For the deterministic transaction exporter, instruction-ledger runner,
benchmark fields, and memory limits, see
[advanced Compact V2 examples](ADVANCED.md).
The
[`blockzilla-example-workloads` guide](../../crates/blockzilla-example-workloads/README.md)
defines the common output records and cross-format parity checks.
