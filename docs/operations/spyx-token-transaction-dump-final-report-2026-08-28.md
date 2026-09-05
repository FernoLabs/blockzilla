# SPYX transaction dump: final report

Date: 2026-08-28

## Result

The schema-3 raw SPYX extraction is complete and verified.

| Item | Result |
| --- | ---: |
| Compact V2 epochs | 801–1018, continuous (218 epochs) |
| Extraction mode | Single read per retained block batch |
| Workers | 12 |
| Source blocks scanned | 93,982,801 |
| Source transactions scanned | 123,831,042,775 |
| Selected SPYX transactions | 7,311,137 |
| Discovered SPYX token accounts | 134,942 |
| Account-creation records | 152,609 |
| Owned block fallbacks | 0 |
| Bytes covered by the final independent hash scan | 14,158,430,473 |

The fixed mint identity is
`XsoCS1TfEyfFhfvj8EtZ528L3CaKBDBRqRapnBbDF2W`. The required mint transaction
was found at slot `346066298`, source block `34188`, transaction index `1509`.

The selected transactions are a safe superset. An account-list match is enough,
and a selected transaction can have failed on chain.

## Final controls

- Output:
  `/volume1/blockzilla/token-transaction-dumps/spyx-mainnet-e801-e1018-single-read-20260827T201409`
- Root manifest SHA-256:
  `841a8511cf1ad80060641bf0b81fa7feafe35fa71bc619312e39d71cd1d36783`
- Resume checkpoint SHA-256:
  `3b520de5e5df86d2e9ff1fcac65a98389e43dd8313c4280f90585637e7b0ab9c`
- Authenticated checkpoint payload digest:
  `235707838ca21648b7996059790c48403c64712838a796b950886ba494a67bf9`
- Frozen account file SHA-256:
  `e77aa22f96283ede6b8732bd48fdc34567582d09c10a65f024c70cda45f07060`

The final read-only audit hashed every discovery log, transaction stream,
account-ID log, shard manifest, root control, frozen-account file, and preserved
quarantine file. All 218 shard bindings and all cumulative counters matched.
There are no live partial or pending outputs. The extractor process and its tmux
session have ended.

The hashed-byte count includes preserved quarantine files. It is not the exact
size of the canonical raw data. Trusted-local source admission binds source
names, sizes, and wire profiles; it does not authenticate all source bytes.

## Consolidation decision

The raw dump is source-epoch bound, and its records can be in worker-completion
order. A final portable dump must build:

1. one deterministic, one-based global public-key registry, sorted by the raw
   32-byte key and containing every Compact pubkey referenced by selected records;
2. transaction records sorted by
   `(epoch, slot, source_block_id, tx_index)`;
3. records with only their Compact pubkey references rewritten to the global
   registry; and
4. one global `signatures.bin` stream, with each record holding its zero-based
   first-signature ordinal and signature count.

Public keys must be unique. Signatures should not be deduplicated by value. They
must be copied once per selected transaction in canonical order so that each
transaction keeps one contiguous signature range. The signature file is an
ordered occurrence stream, not a unique-signature registry. Its minimum size is
467,912,768 bytes because every selected transaction has at least one 64-byte
signature; multi-signature transactions make it larger.

Sorting does not require a second 14 GB record copy. The implementation builds
and sorts small per-epoch locator records. It then reads nearby payload ranges
into one reused bounded arena. It sorts only an index of physical offsets; it
always decodes and writes records in canonical locator order. Blockhash IDs,
vote-hash IDs, and all non-pubkey metadata bytes stay unchanged. Input is hashed
during the index scan, and output is hashed during its only write.

The raw epoch shards remain immutable input evidence. Consolidation writes a new
output directory and publishes its manifest last.

## Consolidation implementation status

The schema-3 consolidator is now implemented. Its steady-state hot path uses:

- borrowed Wincode transaction records over a reused read arena;
- reused message, metadata, comparison, frame, locator, range, and signature
  buffers;
- one resident dump registry and allocation-free prefix-bounded lookup;
- a dense per-epoch source-ID table that clears only touched entries;
- row-aligned, positioned source-ID map reads into one reused buffer;
- physically coalesced transaction reads with bounded gap and memory budgets;
- exact-adjacency signature range merging and bounded parallel signature reads;
- output hashing during the only output write; and
- the public read SDK for source admission, pinned range sources, message
  projection, metadata wire profiles, and generation identity.

The direct Archive V2 ordered-block SDK pipeline is not used for Pass 2 because
the input is a sparse locator stream, not a sequence of archive blocks. The
consolidator uses the same borrowed-buffer pattern with a local positioned-read
arena. Public-key rewriting is still serial inside each canonical batch. This
keeps order and failure handling simple until a benchmark shows that rewriting,
not storage reads, is the next limit.

The optimized static NAS binary is:

```text
SHA-256: bd98a5c752b9e4781f161dec554af5a8a0ac8e5c3c924a1e6b08873aa0b82749
Size: 3,341,248 bytes
Path: /volume1/blockzilla/bin/blockzilla-token-transaction-dump-bd98a5c752b9e4781f161dec554af5a8a0ac8e5c3c924a1e6b08873aa0b82749
Format: x86-64 static PIE, stripped
```

This binary passed 60 library tests, 10 command tests, formatting, and strict
Clippy checks. Its NAS SHA-256 matches the local build, and its `--help` startup
check passed.

The full consolidation started before these last optimizations. It used the
earlier immutable producer binary:

```text
SHA-256: 4f8647b3e4789a93bfda933326c244d889d6eae68ecbd916860dc56d5b097ec3
Path: /volume1/blockzilla/bin/blockzilla-token-transaction-dump-4f8647b3e4789a93bfda933326c244d889d6eae68ecbd916860dc56d5b097ec3
```

It completed without a restart:

| Item | Result |
| --- | ---: |
| Output | `/volume1/blockzilla/token-transaction-dumps/spyx-mainnet-e801-e1018-consolidated-final-20260828T224409` |
| Manifest SHA-256 | `ba894ea5848c4616b87230ff711c9307e70587dc51b3816ac14f1c68e1f616b8` |
| Public keys | 1,621,463 |
| Signature occurrences | 7,546,434 |
| Transactions | 7,311,137 |
| Preflight | 19.9 seconds |
| Pass 1 | 885.4 seconds |
| Registry merge | 0.7 seconds |
| Pass 2 | 3,859.1 seconds |
| Total | 4,765.3 seconds |

The completed directory contains exactly five regular files:

| File | Bytes | SHA-256 |
| --- | ---: | --- |
| `manifest.json` | 1,202 | `ba894ea5848c4616b87230ff711c9307e70587dc51b3816ac14f1c68e1f616b8` |
| `accounts.wincode` | 6,045,862 | `e77aa22f96283ede6b8732bd48fdc34567582d09c10a65f024c70cda45f07060` |
| `registry.bin` | 51,886,816 | `eb74ca724b2d8f7bb8effe47048d295083292df33b160d872840d988a72438e7` |
| `signatures.bin` | 482,971,776 | `4c636a7d1b343063b41bfe73279d23e56d8265fb4a130b06a5baca794799cb3c` |
| `transactions.wincode` | 14,613,517,576 | `2849a8e8fbe7d8dbb553022355cfd33d0e50971166242534a398334e79d977de` |

The manifest binds the transaction, signature, registry, and account hashes.
The separate full validator ran with the optimized binary in tmux session
`spyx-validate-final-20260829T000442`. Its log is beside the dump at:

```text
/volume1/blockzilla/token-transaction-dumps/spyx-mainnet-e801-e1018-consolidated-final-20260828T224409.validation-20260829T000442.log
```

The validator read and hashed all five final files and checked the complete
transaction stream. It exited with no error output. The command reports only
failures, so the empty log is its successful result. The checks include exact
file bindings, artifact hashes and sizes, public-key IDs, canonical transaction
order, signature ordinals and bytes, cumulative counts, and the fixed mint
anchor.

## Program inventory

The `program-inventory` command completed a full inventory of the program IDs in
all top-level and inner instructions. It read the final transaction stream once.
The hot path used borrowed decoding and reused buffers. The report is outside
the exact five-file dump, so the completed dump did not change.

The static NAS binary was:

```text
SHA-256: 8a8cad8b414322b755fa860f98cf068ba3c28cee4e05bd25603795435f110947
Size: 3,427,264 bytes
Path: /volume1/blockzilla/bin/blockzilla-token-transaction-dump-8a8cad8b414322b755fa860f98cf068ba3c28cee4e05bd25603795435f110947
Format: x86-64 static PIE
```

The report was:

```text
SHA-256: 92577147ce5ae07b17c64d68ac044dd97e41b6f8efca0d4867ae431e26508ddb
Size: 609,384 bytes
Path: /volume1/blockzilla/token-transaction-dumps/spyx-mainnet-e801-e1018-consolidated-final-20260828T224409.program-inventory-v1.json
```

| Item | Result |
| --- | ---: |
| Transactions | 7,311,137 |
| Programs | 1,070 |
| Top-level instructions | 34,049,289 |
| Inner instructions | 50,014,552 |
| Inner programs resolved from static accounts | 12,297,616 |
| Inner programs resolved from loaded writable accounts | 1,116,182 |
| Inner programs resolved from loaded read-only accounts | 36,600,754 |
| Metadata with current error schema only | 2,387,520 |
| Metadata with legacy error schema only | 621 |
| Metadata without an error | 4,922,996 |
| Metadata accepted by both schemas with the same result | 0 |
| Metadata accepted by both schemas with different results | 0 |
| Post messages | 7,311,137 |
| Unresolved program IDs | 0 |
| Inline raw program IDs | 0 |
| Divergent program resolutions | 0 |
| Elapsed time | 61.3 seconds |
| Transaction-stream speed | 227.4 MiB/s |

The report orders programs by total occurrence count from high to low. Raw
public-key bytes break ties. This gives the decoder work list and keeps the
result deterministic.

| Programs implemented in report order | Share of all instruction occurrences |
| ---: | ---: |
| 10 | 88.3272% |
| 50 | 97.1881% |
| 100 | 98.9322% |
| 250 | 99.8640% |
| 500 | 99.9903% |
| 1,070 | 100.0000% |

These percentages are instruction-occurrence coverage, not transaction
coverage. The complete parser target remains all 1,070 program IDs. The
implementation passed 65 library tests, 11 command tests, and strict Clippy
checks.

## Program identification and decoder sources

The identification pass checked current on-chain IDLs and `security.txt` data
first. It then used exact program-ID matches from public registries, explorer
data, verified source builds, Solscan, and protocol source repositories.
Generic class labels and prefix-only matches were excluded.

| Program set | Program coverage | Instruction-occurrence coverage | Fully covered transactions | Touched transactions |
| --- | ---: | ---: | ---: | ---: |
| Usable on-chain IDL candidates | 181 / 1,070 (16.92%) | 75.26% | 144,196 / 7,311,137 (1.97%) | 7,244,898 / 7,311,137 (99.0940%) |
| IDL or other decoder source | 193 / 1,070 (18.04%) | 92.03% | 2,189,575 / 7,311,137 (29.95%) | 7,310,058 / 7,311,137 (99.9852%) |
| Exact identity or source attribution | 280 / 1,070 (26.17%) | 95.51% | 4,016,487 / 7,311,137 (54.94%) | 7,310,060 / 7,311,137 (99.9853%) |

“Fully covered” means that every top-level and inner instruction program in the
transaction is in the set. “Touched” means that at least one instruction program
is in the set.

The detailed report, evidence, unknown-program list, IDL files, source snapshots,
and exact coverage reports are in
[`benchmark-results/spyx-program-identification-v1/README.md`](../../benchmark-results/spyx-program-identification-v1/README.md).
