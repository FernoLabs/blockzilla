# Archive reader formats

Status: implemented reader reference, 2026-08-28.

The source-neutral query contract is in
[`blockzilla-query-sdk`](../../crates/blockzilla-query-sdk/README.md). CAR,
Compact V2, and Indexer V3 each have a source-neutral reference adapter. The
adapters preserve the different trust levels of the source formats.

This document does not make all formats equal. Each format has a different
read path and a different source-verification level.

## Format summary

| Format | Main read model | Remote read model | `NetworkEpoch` trust level | Common adapter |
|---|---|---|---|---|
| CAR | Sequential block stream | Concurrent ordered HTTPS ranges over one CAR object | `operator-trusted` | Implemented reference adapter |
| Compact V2 | Compressed block rows plus control files | Verified control files plus bounded block and signature range reads | `published-manifest` | Implemented reference adapter |
| Indexer V3 | Separate transaction, effect, and index planes | Cached bounded index and registry; uncached bounded reads for large sidecars and semantic planes | `internal-binding-only` | Implemented reference adapter |

These values are part of `SourceIdentity`. They do not change because two
formats produce the same application rows.

The table describes the high-level network facade. A direct Compact V2 adapter
over an explicit unpublished fixture can instead be `operator-trusted`.

All three adapters must produce the same
[Archive Instruction Stream V1](archive-instruction-stream-v1.md). An
application must not contain three copies of its token or program logic.

## Physical layout at the reader boundary

CAR normally uses one `epoch-N.car` object. Jetstreamer can also use the
separate `epoch-N-slot-ranges.raw` range index. The Blockzilla CAR adapter uses
an explicit canonical slot plan from a trusted index or manifest. It
reconstructs messages and metadata from nodes in the CAR stream.

The Compact V2 generation manifest binds these required objects:

- `archive-v2-blocks.zstd`;
- `archive-v2-blocks.index`;
- `archive-v2-meta.wincode`;
- `registry.bin`.

A generation can also include `signatures.bin`, `genesis.bin`, schema markers,
and other bound sidecars. The current common Compact V2 adapter publishes the
primary signature when the sidecar is present. It reads one bounded signature
window for each nonempty block in that case.

The Indexer V3 candidate has a block index and separate semantic objects for:

- the transaction directory;
- messages;
- loaded addresses;
- inner instructions;
- logs;
- token balances;
- lamport balances;
- outcomes;
- transaction rewards;
- raw metadata fallbacks;
- block rewards.

Its optional account posting index has separate control, coverage, and page
objects. The retained-sidecar candidate record describes the source sidecars
that stay with the result. The PoH V3 directory is a separate storage
experiment. It is not part of the instruction-stream reader contract.

## CAR

CAR is the Old Faithful source format. The current
[`of-car-reader`](../../crates/old-faithful/car-reader/Readme.md) can stream a
plain CAR file or a Zstandard-compressed CAR file. It reconstructs blocks and
their transaction metadata from CAR nodes.

CAR is a good reference source because it keeps the source data and has an
independent decoder. It is also useful for a sequential network comparison.

The common CAR adapter is one-pass. Selecting CAR is the application's explicit
operator-trust decision. Its SDK trust level is `operator-trusted`. It binds the
result to the HTTP object URL, exact length,
strong ETag, and a digest of the canonical slot plan. It limits CAR entry
sizes, block payload, reconstructed transaction and metadata sizes, resolved
accounts, and instruction counts before it publishes a common record. The
legacy CBOR, protobuf, wincode, and optional Zstandard decoders still use an
operator-trusted input policy. This adapter does not convert an ETag into a
CAR content-hash or root-CID proof.

The HTTPS stream disables redirects and proxy discovery. It uses `If-Match`
and exact closed ranges. The default has four workers, eight in-flight 32 MiB
ranges, and ordered delivery to the CAR decoder. The range-body window is
256 MiB. TLS, HTTP, caller, and CAR decoder buffers are outside this number.

CAR network access is primarily sequential. It is simple for a full scan, but
it does not give the small point reads that Indexer V3 gives.

## Compact V2

Compact V2 is the implemented Archive V2 generation read by
[`blockzilla-read-sdk`](../../crates/blockzilla-read-sdk/README.md). It stores
compact messages and metadata in compressed block frames. Small control files
describe the block rows, schemas, registry, and generation.

The local default verifies all declared files. The remote path can cache and
verify the control files, then read bounded ranges from the blocks and
signature files. Sequential scans coalesce adjacent compressed frames into
bounded reads. The common reference adapter is sequential and reads one
bounded signature window for each nonempty block when `signatures.bin` is
present.

A published Compact V2 generation has the SDK trust level
`published-manifest`. It has the strongest current publication contract of the
three formats:

- the manifest must state that the generation is complete;
- the generation digest must be valid;
- the manifest binds the message and metadata schemas;
- the index, registry, metadata footer, sizes, and epoch shape must agree;
- local full verification can check all declared file hashes.

An explicit unpublished Compact V2 fixture can use `operator-trusted`. This
fixture status does not change the published network-generation status.

The HTTP mode can use control-file verification without downloading all block
and signature bytes first. This mode depends on an immutable generation URL
and authenticated TLS.

Compact V2 remains row-oriented. A reader can skip signatures and can perform
bounded block reads, but a query that needs instruction data must decode the
applicable transaction rows and metadata.

The common reference adapter also resolves V0 loaded addresses, preserves
required signer order, outer and CPI order, CPI stack height, and the failed
outer-instruction index. It distinguishes absent metadata, raw metadata,
decoded `inner_instructions = None`, and a recorded empty CPI list. It is a
correctness reference. It does not make a throughput claim.

## Indexer V3

Indexer V3 is the current indexer-first candidate format. It separates
transaction and effect data into semantic planes. It can
also contain account posting indexes. This layout supports bounded transaction
reads and selective account queries without reading unrelated effect planes.

The current local and HTTPS V3 readers validate headers, object lengths,
locators, codecs, and internal cross-file bindings. The HTTPS path uses HEAD
requests and closed range GET requests. It rejects redirects, weak cache
identities, and invalid ranges.

The persistent cache stores the bounded block index and required
`registry.bin`. The large transaction directory, optional signatures, and
message and effect payload planes stay as pinned range reads. A warm read still
checks the exact strong ETag and object length. See
the [Indexer V3 section of the product guide](archive-formats-and-read-sdk.md#indexer-first-standalone-v3).

Indexer V3 is still an `unverified-nonpublishable` candidate. It has no
published manifest, source-generation digest, or registry digest. Strong
ETags and internal bindings detect changes and structural errors, but they do
not supply publication authority. The common adapter therefore uses the exact
SDK trust level `internal-binding-only`. The caller must also give a stable
candidate binding. The network example derives this binding from the base
URL, epoch, and the exact strong ETag and length of every required and present
optional object. The example must explicitly accept this weaker source. This
choice does not increase the trust level.

The reference adapter requires the production `VarintDelta` transaction
directory codec. It rejects the measurement-only fixed-width codec. Practical
independent limits apply to retained index rows, blockhash and vote sidecars,
Zstandard windows, and all allocation-driving lengths.

The high-level facade also checks every V3 block-row slot against the same
ordinal in the Compact V2 canonical plan. V3 must be a dense prefix of that
plan, and the requested range must fit in the prefix.

## Verification is part of the result

An adapter must publish its source identity before it publishes
transactions. The identity includes:

- format;
- cluster identity when the source binds it;
- epoch;
- exact first slot;
- slots per epoch;
- a user-visible label;
- verification strength;
- a stable binding when one is available.

The first slot is explicit because warm-up epoch schedules are not a simple
`epoch * slots_per_epoch` calculation. The `NetworkEpoch` facade supports
`mainnet-beta` and derives the first slot from Solana's warm-up-aware schedule.
`NetworkEpochOptions` changes only the local-fixture transport policy. A custom
cluster must use direct adapters with an explicit first slot.

The default scan request accepts a published source or an explicitly
operator-trusted input. The input can be local or remote. It rejects an
internal-only or unverified source. A
caller must make an explicit choice to use a weaker source. The API must not
silently change this policy because the source is local.

Semantic parity and source verification are different checks. The current
three-format comparison can prove that bounded outputs agree. That result does
not make the V3 candidate publication-verified.

## Bounded network demonstration

The implemented
[`archive-token-events`](../../examples/archive-token-events/README.md)
command reads all three formats from one public HTTPS Worker origin. It does
not use a local HTTP server. The command accepts exactly these sample epochs:
`0`, `100`, `200`, `300`, `400`, `500`, `600`, `700`, `800`, `900`, and
`1000`.

One run can read at most 1,024 canonical block rows. This is a hard demo limit,
not a full-epoch limit in the SDK. The command writes each archive result to a
separate folder:

```text
<output-root>/archive-cache/origin-.../compact-v2/
<output-root>/archive-cache/origin-.../indexer-v3/
<output-root>/car/epoch-N/
<output-root>/compact-v2/epoch-N/
<output-root>/indexer-v3/epoch-N/
<output-root>/comparison/epoch-N/
```

Each format result folder contains its own SQLite event database and JSON
report. Compact V2 and Indexer V3 have separate trees under `archive-cache`.
The comparison folder contains the cross-format report.

The application result is an instruction-event ledger. It does not use pre-
or post-token balance observations. It stores target classic SPL Token events,
account-lifetime evidence, instruction-derived delta legs, and explicit
coverage gaps.

Epoch 0 is only a structural network example. The current epoch-0 Compact V2
and Indexer V3 samples have limited metadata, and USDC is absent from this
range. An empty result from epoch 0 is not a throughput result and is not a
semantic-completeness result.

## Remote-read differences

CAR reads one sequential stream. This gives a small setup cost for a full
epoch scan. A random query can require a separate range index or a scan from an
earlier position.

Compact V2 reads small control files first. It then reads and decompresses
selected block frames. Its remote generation manifest can provide publication
identity.

Indexer V3 caches its bounded block index and required registry. It range-reads
the large transaction directory, optional signatures, and required semantic
planes without copying them into the persistent cache. Repeated and selective
reads can still use fewer payload bytes because they read only applicable
ranges and planes.

Network benchmarks must report these setup and payload bytes separately. A
warm control-file or bounded-index cache is not equal to a cold scan.

## Format choice

Use CAR when you need the independent source representation or a sequential
reference scan.

Use Compact V2 when you need the current published-generation verification
contract and the implemented local or gateway reader.

Use Indexer V3 for bounded point reads, account postings, and selective plane
reads during candidate evaluation. Do not use it as publication authority
yet. A caller must explicitly accept its weaker verification state.

Application code should use the common stream and a sink. It should select a
format only during source setup. See the
[Blockzilla query SDK guide](../guides/blockzilla-query-sdk.md).
