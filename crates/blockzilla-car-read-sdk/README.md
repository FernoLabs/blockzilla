# Blockzilla CAR read SDK

This crate is the dedicated SDK for one Blockzilla CAR epoch. It uses the
native zstd backend because normal Old Faithful transaction metadata uses zstd
frames. It:

- derives the Blockzilla Worker or Old Faithful CAR and
  `epoch-N-slot-ranges.raw` URLs;
- keeps the Worker route strict: exact length and one strong ETag for both
  objects and every range response;
- provides a separate operator-trusted admission for the public Old Faithful
  HTTPS service, which does not send ETags;
- validates all 5,184,000 bytes of the raw slot index;
- fails unless the nonempty rows match a trusted canonical block count, or the
  caller supplies the exact canonical slot plan;
- opens the same `car/<epoch>/...` object layout below a local archive root;
- gives the application the common ordered query interface.

The application does not use HTTP range code or a raw CAR parser.

```rust,no_run
use std::num::NonZeroU32;
use blockzilla_car_read_sdk::{
    ArchiveInstructionSourceExt, CarArchive, ScanRequest,
};

# fn main() -> Result<(), Box<dyn std::error::Error>> {
let mut archive = CarArchive::open(
    "https://archive.example",
    900,
    NonZeroU32::new(431_858).unwrap(),
)?;
let range = archive.bounded_range(0, NonZeroU32::new(1_024).unwrap())?;
let request = ScanRequest::bounded(range)
    .allow_incomplete_instructions()
    .allow_incomplete_cpi()
    .allow_unknown_execution()
    .without_instruction_data();
let receipt = archive.for_each_block(&request, |_block| Ok(()))?;
println!("transactions={}", receipt.transactions);
# Ok(())
# }
```

`CarArchive::open` keeps the transport setup simple. It uses four concurrent
HTTP workers, an eight-chunk window, and 32 MiB chunks. The maximum retained
range-body window is 256 MiB.

Benchmark programs can set all three range controls through
`CarArchiveOptions`:

```rust,no_run
# use std::num::NonZeroU32;
# use blockzilla_car_read_sdk::{CarArchive, CarArchiveOptions};
# fn main() -> Result<(), Box<dyn std::error::Error>> {
let options = CarArchiveOptions {
    http_workers: 8,
    http_window_chunks: 8,
    http_chunk_bytes: 32 * 1024 * 1024,
    ..CarArchiveOptions::default()
};
println!("body_window_bytes={}", options.http_body_window_bytes()?);
let archive = CarArchive::open_with_options(
    "https://archive.example",
    900,
    NonZeroU32::new(431_858).unwrap(),
    options,
)?;
# drop(archive);
# Ok(())
# }
```

The SDK uses the CAR transport limits. Workers and window chunks must be in
`1..=16`, workers cannot exceed window chunks, and chunk bytes must be in
`1..=33,554,432`. `http_body_window_bytes` validates the profile before a
network request. TLS, HTTP, channel, caller, and decoder buffers are outside
this body-window value.

`OperatorTrusted` is the source verification level because the canonical block
plan comes from the operator. On the strict Worker path, the binding covers the
strong-ETag CAR object, raw slot index, and accepted canonical plan.

The raw index is a byte-range index. A zero-length row does not prove that the
ledger has no canonical block at that slot. Thus, the SDK does not silently use
all nonempty rows as the canonical block universe. The count-based constructors
are valid only when a trusted canonical count equals the nonempty-row count.
They fail on a mismatch. Use `open_with_canonical_slots` or
`open_old_faithful_with_canonical_slots` when the plan includes zero-range
blocks.

## Local sample layout

`CarArchive::open_local` derives two fixed paths from an archive root:

```text
archive/car/900/epoch-900.car
archive/car/900/epoch-900-slot-ranges.raw
```

It opens the CAR file before it builds the reader and reads and validates the
complete slot index. The caller still supplies a trusted canonical block
count. The local path uses operator trust and a path-and-length descriptor; it
does not claim a content hash.

```rust,no_run
# use std::num::NonZeroU32;
# use blockzilla_car_read_sdk::CarArchive;
# fn main() -> Result<(), Box<dyn std::error::Error>> {
let archive = CarArchive::open_local(
    "archive",
    900,
    NonZeroU32::new(431_858).unwrap(),
)?;
# drop(archive);
# Ok(())
# }
```

Use `open_local_with_canonical_slots` when the raw index omits a canonical
block with no reconstructed byte range.

Use the public Old Faithful layout as follows:

```rust,no_run
# use std::num::NonZeroU32;
# use blockzilla_car_read_sdk::CarArchive;
# fn main() -> Result<(), Box<dyn std::error::Error>> {
let archive = CarArchive::open_old_faithful_operator_trusted(
    "https://files.old-faithful.net",
    800,
    NonZeroU32::new(430_282).unwrap(),
)?;
# drop(archive);
# Ok(())
# }
```

This constructor derives `/{epoch}/epoch-{epoch}.car` and
`/{epoch}/epoch-{epoch}-slot-ranges.raw`. It requires HTTPS. It accepts the
absence of an ETag, but it requires one exact HEAD `Content-Length`. A partial
GET must return `206 Partial Content` with exact `Content-Range`,
`Content-Length`, total object length, and response-body length. If one
scheduled range covers the complete object, this explicit route also accepts
`200 OK` with no `Content-Range` only when its declared and returned lengths
match the admitted HEAD length exactly. The strong-ETag routes do not use this
exception.

The resulting HTTP verification is `operator-trusted`. The effective source
binding covers only the accepted URLs, observed lengths, epoch, and canonical
slot plan. It is not an ETag, archive content hash, manifest, seal, or proof of
stable remote object identity. `bound_source_size_bytes` is the sum of the two
observed object lengths.

The older `open_old_faithful` constructor stays strict and still requires
strong ETags. Use
`open_old_faithful_operator_trusted_with_canonical_slots` when the public raw
index omits a canonical block with no reconstructed byte range.
