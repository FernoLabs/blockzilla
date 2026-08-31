# Archive sample layout and format design

Status: public sample contract, 2026-08-31.

This guide defines one folder layout for the public sample source and for a
local mirror. It also explains why CAR, Compact V2, and Indexer V3 have
different read costs.

The sample epochs are:

```text
0, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000
```

There are three stored formats. Jetstreamer is a CAR reader. It reads the CAR
objects and does not need a fourth stored copy.

## One path for HTTPS and local files

The configured public origin is:

```text
https://blockzilla-archive-samples-v1.cheron-augustin.workers.dev
```

Every object route has this form:

```text
/<format>/<epoch>/<file-name>
```

A local `archive/` folder uses the same path below its root:

```text
archive/
├── car/900/
│   ├── epoch-900.car
│   └── epoch-900-slot-ranges.raw
├── compact-v2/900/
│   ├── archive-v2-blocks.index
│   ├── archive-v2-blocks.zstd
│   ├── archive-v2-meta.wincode
│   ├── registry.bin
│   └── ...
└── indexer-v3/900/
    ├── archive-v2-standalone-blocks.index
    ├── archive-v2-standalone-transaction-directory.wincode
    └── ...
```

For example, these two paths select the same Compact V2 object:

```text
https://blockzilla-archive-samples-v1.cheron-augustin.workers.dev/compact-v2/900/registry.bin
archive/compact-v2/900/registry.bin
```

The example programs use the public origin when `--archive-root` is absent.
Use `--archive-root archive` while the public sample is being staged, or when
you want a local read. The application query does not change.

## Stored objects

The sample source contains data objects only. Converter reports, candidate
records, build evidence, and schema marker files are not reader objects.

### CAR

Each epoch has two objects:

```text
epoch-<epoch>.car
epoch-<epoch>-slot-ranges.raw
```

The slot-to-offset index is exactly 5,184,000 bytes. It has one 12-byte row for
each slot in the 432,000-slot archive window. Each row contains an 8-byte
little-endian CAR offset and a 4-byte little-endian byte length. A zero length
means that the row has no CAR range.

The index is part of the CAR folder. A CAR sample is not complete without it.

### Compact V2

Each epoch has these ten objects:

```text
archive-v2-blocks.index
archive-v2-blocks.zstd
archive-v2-meta.wincode
blockhash_registry.bin
poh.wincode
registry.bin
registry.mphf
shredding.wincode
signatures.bin
vote_hash_registry.bin
```

Epochs after epoch 0 also have:

```text
prev_blockhash_tail.bin
```

All public Compact V2 samples use the current normalized message and
transaction-error grammar. The example has no epoch table and no schema flag.

### Indexer V3

Each epoch has these twelve ledger objects:

```text
archive-v2-standalone-blocks.index
archive-v2-standalone-transaction-directory.wincode
archive-v2-standalone-messages.wincode
archive-v2-standalone-loaded-addresses.wincode
archive-v2-standalone-inner-instructions.wincode
archive-v2-standalone-logs.wincode
archive-v2-standalone-token-balances.wincode
archive-v2-standalone-balances.wincode
archive-v2-standalone-outcomes.wincode
archive-v2-standalone-transaction-rewards.wincode
archive-v2-standalone-raw-metadata-fallbacks.wincode
archive-v2-standalone-block-rewards.wincode
```

It has these three reverse-lookup objects:

```text
archive-v2-standalone-account-postings-adaptive-v3.pages
archive-v2-standalone-account-postings-adaptive-v3.control
archive-v2-standalone-account-postings-adaptive-v3.coverage
```

It also has these eight retained objects:

```text
archive-v2-meta.wincode
blockhash_registry.bin
poh.wincode
registry.bin
registry.mphf
shredding.wincode
signatures.bin
vote_hash_registry.bin
```

Epochs after epoch 0 also have `prev_blockhash_tail.bin`.

## Why the formats differ

### CAR keeps the source graph

CAR is the independent Old Faithful representation. It keeps content-addressed
nodes and is useful as a reference source. The slot-to-offset index gives the
reader a direct byte range for one slot.

This format is portable and independent. It also repeats more structure and
is the largest of the three stored forms.

### Compact V2 stores the ordered read shape

Compact V2 stores compressed block rows, a direct block index, compact public
key IDs, and separate sidecars. One shared registry replaces repeated 32-byte
public keys.

This format is small and works well for ordered scans. A target query must
still read each selected block because the format has no general reverse
index for application targets.

### Indexer V3 stores the query shape

Indexer V3 separates messages, outcomes, token balances, logs, and other
semantic data into planes. Its transaction directory tells the reader where
each transaction is in those planes.

The adaptive reverse objects map a public key to sound candidate blocks. A
Pump.fun or FireWatch query can reject most blocks before it reads transaction
payloads. The application still checks each candidate for an exact match.
Coverage rows keep a block when the reverse index cannot prove that it is safe
to skip.

V3 can finish sooner while it reports a lower MB/s value. It wins when it
reads fewer bytes and decodes less data. A high MB/s value alone does not mean
that a job finishes sooner.

## Read and trust model

The readers do not download or require an archive publication manifest. They
do not hash a complete epoch or a part of an epoch. They do not require an
epoch seal.

For a public source, the SDK fixes the object names, reads the exact length and
strong ETag of each object, and checks the same identity on later range
responses. For a local source, the SDK opens only fixed object names and pins
the opened files for the scan.

The readers still validate internal headers, indexes, offsets, counts, and
epoch geometry. Output SHA-256 values in workload tools check application
result parity. They are not archive publication hashes.

## Dedicated SDKs and examples

Each example binary has one format and one job. There is no run-time format
switch in these beginner examples.

| Format | SDK | Starter guide |
|---|---|---|
| CAR | [`blockzilla-car-read-sdk`](../../crates/blockzilla-car-read-sdk/README.md) | [`read-car`](../../examples/read-car/README.md) |
| Compact V2 | [`blockzilla-compact-v2-read-sdk`](../../crates/blockzilla-compact-v2-read-sdk/README.md) | [`read-compact-v2`](../../examples/read-compact-v2/README.md) |
| Indexer V3 | [`blockzilla-indexer-v3-read-sdk`](../../crates/blockzilla-indexer-v3-read-sdk/README.md) | [`read-indexer-v3`](../../examples/read-indexer-v3/README.md) |

The three application jobs are separate source files for each format:

| Format | USDC | Pump.fun | FireWatch |
|---|---|---|---|
| CAR | [`read-car-usdc`](../../examples/read-car/src/bin/read-car-usdc.rs) | [`read-car-pumpfun`](../../examples/read-car/src/bin/read-car-pumpfun.rs) | [`read-car-firewatch`](../../examples/read-car/src/bin/read-car-firewatch.rs) |
| Compact V2 | [`read-compact-v2-usdc`](../../examples/read-compact-v2/src/bin/read-compact-v2-usdc.rs) | [`read-compact-v2-pumpfun`](../../examples/read-compact-v2/src/bin/read-compact-v2-pumpfun.rs) | [`read-compact-v2-firewatch`](../../examples/read-compact-v2/src/bin/read-compact-v2-firewatch.rs) |
| Indexer V3 | [`read-indexer-v3-usdc`](../../examples/read-indexer-v3/src/bin/read-indexer-v3-usdc.rs) | [`read-indexer-v3-pumpfun`](../../examples/read-indexer-v3/src/bin/read-indexer-v3-pumpfun.rs) | [`read-indexer-v3-firewatch`](../../examples/read-indexer-v3/src/bin/read-indexer-v3-firewatch.rs) |

The Compact V2 and Indexer V3 packages also have a small ordered slot-hour
reader. It scans the complete epoch and prints one count row for each fixed
9,000-slot window:

- [`read-compact-v2-slot-hours`](../../examples/read-compact-v2/src/bin/read-compact-v2-slot-hours.rs)
- [`read-indexer-v3-slot-hours`](../../examples/read-indexer-v3/src/bin/read-indexer-v3-slot-hours.rs)

Read the starter guide for the exact build and run commands. The default is
epoch 900 and the complete epoch. Use `--archive-root archive` for the folder
layout in this document.

## Publication checks

Publish one epoch and format only after its fixed object set passes these
checks:

- every required object name is present;
- no private build object is in the public set;
- each object has its expected size;
- the dedicated SDK can open the set; and
- a small ordered read succeeds.

These checks do not create or require a payload hash file, a partial hash, a
publication manifest, or an epoch seal.
