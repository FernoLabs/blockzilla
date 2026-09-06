# Read CAR

The primary programs are small CAR examples:

- [`read-car`](src/main.rs) counts blocks, transactions, and recorded inner
  instructions in fixed 9,000-slot windows.
- [`read-car-usdc`](src/bin/read-car-usdc.rs) writes recorded USDC pre- and
  post-token balances.
- [`read-car-pumpfun`](src/bin/read-car-pumpfun.rs) writes transactions with a
  direct or recorded CPI call to the Pump.fun program.
- [`read-car-firewatch`](src/bin/read-car-firewatch.rs) writes the distinct
  programs reached by successful transactions from one signer wallet.

Each program opens one CAR archive through `blockzilla-car-read-sdk` and builds
one query. The three application jobs stream to shared workload sinks. The
ordered reader keeps about 48 slot-window counters in memory. No program
selects an archive format at run time.

USDC and Pump.fun exclude known failed transactions and report their count.
Unknown execution status remains a coverage warning. The count example still
includes all transactions. See the [shared workload rules](../../crates/blockzilla-example-workloads/README.md).

## Start with a local archive

The clean public CAR objects are still being staged. The currently runnable
path is a local mirror with the clean sample layout:

```text
archive/
  car/
    900/
      epoch-900.car
      epoch-900-slot-ranges.raw
```

Run the three complete epoch-900 jobs:

```console
cargo run --release --locked -p blockzilla-read-car \
  --bin read-car-usdc -- --archive-root archive

cargo run --release --locked -p blockzilla-read-car \
  --bin read-car-pumpfun -- --archive-root archive

cargo run --release --locked -p blockzilla-read-car \
  --bin read-car-firewatch -- --archive-root archive
```

The default output files are `car-usdc.bin`, `car-pumpfun.bin`, and
`car-firewatch.bin`. An output file must not exist before a run. FireWatch uses
`5LikTUsx695BHRipWoRrn6YmTQEcPrvbR8YaHxdSRQo8` by default.

The normal interface has only these options:

```text
--epoch N          0, 100, 200, ..., or 1000; default 900
--origin URL       another compatible clean Worker origin
--archive-root DIR local root that contains car/<epoch>/
--output FILE      output file; it must not exist
--wallet KEY       FireWatch only
```

The programs always scan the complete selected epoch. They contain the trusted
canonical block counts for all 11 sample epochs. There is no block-limit flag
in the normal interface.

## Public source after publication

After the staged CAR objects are published, no arguments will read public
epoch 900 from:

```text
https://blockzilla-archive-samples-v1.cheron-augustin.workers.dev/car/900/
```

For example:

```console
cargo run --release --locked -p blockzilla-read-car --bin read-car-usdc
```

The Worker route requires exact lengths and strong ETags. Use `--origin URL`
for another Worker with the same `/car/<epoch>/...` object layout.

The three application programs report setup, scan, and total time. They also
report transaction rate, source bytes delivered to the decoder, HTTP body
bytes, MB/s, output rows, output bytes, coverage status, and the coverage
SHA-256. The ordered count program reports the same time and read-rate units,
plus its block, transaction, and recorded inner-instruction totals.
Before a speed comparison, require the output row count, output byte count,
complete or incomplete state, indeterminate transaction count, and coverage
SHA-256 to match. Compare the output files byte-for-byte outside the timed run.
Use the same epoch, block universe, and target.
The
[`blockzilla-example-workloads` guide](../../crates/blockzilla-example-workloads/README.md)
defines this gate and the canonical records.

CAR scans all requested blocks for each application job. It does not use the
V3 reverse index. Thus, compare its full-scan work with the V3 requested
universe, not only with the V3 decoded candidate count.

## Advanced transaction export

`read-car-transactions` writes one common identity record for every
transaction. It is an advanced full-universe comparison tool and keeps its
explicit transport controls. A controlled loopback mirror can use cleartext
only with the strict route and `--allow-insecure-http`; the mirror must provide
strong ETags and HTTP range responses.

```console
cargo run --release --locked -p blockzilla-read-car --bin read-car-transactions -- \
  old-faithful http://127.0.0.1:8080 900 431858 epoch-900-transactions.bin \
  --http-workers 4 --http-window-chunks 8 --http-chunk-bytes 33554432 \
  --allow-insecure-http
```

The
[`blockzilla-example-workloads` guide](../../crates/blockzilla-example-workloads/README.md)
defines its common record and acceptance gate.

## Ordered read baseline

`read-car` is the smallest public CAR example. The SDK owns the URLs, complete
raw slot-index check, HTTP admission, range reads, CAR decoding, and canonical
block order. The example contains the trusted block counts for the 11 public
sample epochs.

With no arguments, it scans all of public epoch 900:

```console
cargo run --release --locked -p blockzilla-read-car --bin read-car
```

Use the same simple archive tree for a local scan:

```console
cargo run --release --locked -p blockzilla-read-car --bin read-car -- \
  --archive-root archive \
  --epoch 900
```

The complete interface is:

```text
--epoch N          0, 100, 200, ..., or 1000; default 900
--origin URL       another compatible clean Worker origin
--archive-root DIR local root that contains car/<epoch>/
```

There is no block-limit option. The program always scans the complete selected
epoch. It stops if the SDK does not deliver the trusted number of blocks or if
slots are not in strict increasing order.

The publication gate rejects a CAR when its slot index does not cover the
expected block count. Repair the index before publication. This rule prevents
a quiet block-ordinal shift in cross-format comparisons.

The program requests instruction coordinates but not instruction payloads,
account lists, signatures, signer keys, or execution status. It counts recorded
inner instructions from those coordinates. It also aggregates block,
transaction, and inner-instruction counts into 9,000-slot windows. At an
assumed 400 ms per slot, each window is approximately one hour. These are slot
windows, not UTC clock hours.

The first output line reports setup, scan, and total time, transaction rate,
source bytes, network MB/s, and incomplete instruction or CPI coverage. If
instruction coverage is incomplete, the recorded inner-instruction count is a
lower bound. One bucket-basis line follows it. About 48 bucket lines contain
the approximate-hour counts. The final line contains the full-epoch totals.
