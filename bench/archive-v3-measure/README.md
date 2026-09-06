# Blockzilla Index Archive measurement tool

This crate contains experimental encoders. It does not define the published
archive wire format.

The first probe measures the two directions of the message-account relation:

```text
transaction -> ordered account IDs
account ID -> transaction postings and role bits
```

It keeps the pubkey dictionary and the pubkey lookup in separate byte totals.
The reverse posting rows are rebuilt from the normalized transaction rows and
are checked with a round trip before the tool writes a report.

Run the bundled largest-block fixture:

```sh
cargo run --locked -p blockzilla-archive-v3-measure -- \
  crates/old-faithful/of-car-reader/benches/fixtures/epoch-822-biggest.car \
  target/index-archive-measurement.json
```

The candidate keeps the largest block in one block-aligned canonical page and
uses 64 KiB key-aligned index pages. It uses zstd level 3, a 64-byte common file
header, 32-byte key-page directory entries, and 16 bytes of page framing and
checksum data. A page remains raw when zstd increases its size. These values
make the accounting reproducible. They are not final format values. The
3,000-block fixture and the epoch 900 and 920 runs must select the production
codecs and page sizes.
