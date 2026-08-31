# Read CAR

The three primary programs are small CAR examples:

- [`read-car-usdc`](src/bin/read-car-usdc.rs) writes recorded USDC pre- and
  post-token balances.
- [`read-car-pumpfun`](src/bin/read-car-pumpfun.rs) writes transactions with a
  direct or recorded CPI call to the Pump.fun program.
- [`read-car-firewatch`](src/bin/read-car-firewatch.rs) writes the distinct
  programs reached by successful transactions from one signer wallet.

Each program opens one CAR archive through `blockzilla-car-read-sdk`, builds
one query, and streams the result to one shared workload sink. It does not
select an archive format at run time.

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

Each program reports setup, scan, and total time. It also reports transaction
rate, source bytes delivered to the decoder, HTTP body bytes, MB/s, output
rows, output bytes, output SHA-256, coverage status, and the coverage SHA-256.
For a network source, it also reports the HTTP admission and the worker,
window, chunk, and body-window values. These programs use the SDK defaults:
four workers, eight chunks, and 32 MiB chunks, for a 256 MiB body window. The
HTTP fields are `not-applicable` or zero for a local source.
Before a speed comparison, require the output row count, output byte count,
output SHA-256, complete or incomplete state, indeterminate transaction count,
and coverage SHA-256 to match. Use the same epoch, block universe, and target.
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

This binary is the smallest public CAR example. It uses only
`blockzilla-car-read-sdk`. The SDK owns the URLs, the complete raw slot-index
check, HTTP admission, ranges, CAR decoding, and canonical block order.
The caller gives a trusted canonical block count. The SDK stops if it differs
from the nonempty range count.

```console
cargo run --release --locked -p blockzilla-read-car -- \
  worker \
  https://blockzilla-network-format-benchmark-v1.cheron-augustin.workers.dev \
  0 \
  431548 \
  1024
```

The first argument selects the explicit URL layout and HTTP admission. For the
public Old Faithful service, use:

```console
cargo run --release --locked -p blockzilla-read-car -- \
  old-faithful-operator-trusted \
  https://files.old-faithful.net \
  800 \
  430282 \
  1024
```

The plain `old-faithful` route stays strict and requires strong ETags. The
public service does not send them. The explicit operator-trusted route requires
HTTPS and reports `http_verification=operator-trusted`. Its source descriptor
digest covers only the accepted URLs, observed lengths, epoch, and canonical
slot plan. It is not an ETag, archive content hash, manifest, seal, or proof of
stable remote object identity. The result makes this limit explicit with
`http_object_binding=none` and `http_content_hash=none`.

The block count is not a speed-test tuning value. It must come from a trusted
canonical inventory. The last argument is optional. It is the maximum number
of canonical block rows, and its default is 1,024.

The normal command uses the SDK defaults: four HTTP workers, an eight-chunk
window, and 32 MiB chunks. This gives a 256 MiB maximum range-body window. A
benchmark can set all three controls after `max-blocks`:

```console
cargo run --release --locked -p blockzilla-read-car -- \
  worker \
  https://blockzilla-network-format-benchmark-v1.cheron-augustin.workers.dev \
  0 \
  431548 \
  1024 \
  8 \
  8 \
  33554432
```

The order is `http-workers http-window-chunks http-chunk-bytes`. The command
rejects a partial profile. The SDK also rejects zero values, more than 16
workers or window chunks, more than 33,554,432 chunk bytes, and a worker count
that is larger than the window-chunk count.

Some CAR epochs contain canonical blocks with no reconstructed CAR byte range.
For these epochs, the raw index has fewer nonempty rows than the canonical
inventory. This binary stops on that mismatch. Applications must then use the
SDK's exact-plan constructor. This rule prevents a quiet block-ordinal shift in
cross-format comparisons.

The callback fingerprints the ordered block universe and counts transactions.
It does not request instruction bytes. It accepts explicit historical
instruction, CPI, and execution coverage gaps.
This rule is the same in the Compact V2 and Indexer V3 examples.

The output separates setup, scan, and total time. `block_universe_sha256` hashes
one 16-byte `(block_ordinal, slot, transaction_count)` record for each callback.
`block_universe_records` gives the number of records. Matching values prove
that format comparisons used the same ordered ordinal, slot, and transaction-
count rows. They do not prove equal block content or equal application output.

`bound_source_size_bytes` includes the CAR object and its 5,184,000-byte raw
slot index. Network and cache bytes are also separate.
`http_verification`, `http_object_binding`, `http_content_hash`, `http_workers`,
`http_window_chunks`, `http_chunk_bytes`, and `http_body_window_bytes` record
the effective transport profile and its identity limits.
`scan_aggregate_io_mb_s` and `total_aggregate_io_mb_s` add network and cache
bytes and use decimal MB. The sum can count a downloaded cache object twice:
once as network input and once as a cache read. CAR does not use the persistent
cache, so its cache values are zero. The report matrix uses the end-to-end total
values because CAR can start prefetch work during setup.
