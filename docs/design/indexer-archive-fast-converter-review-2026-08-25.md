# Indexer archive fast-converter review

Date: 2026-08-25

Updated: 2026-08-26

Status: design correction and measured implementation plan. The final Index
Archive schema is not frozen. The source-split canary records the first effect
split experiment. The lean block-chunk canary records the preferred measured
layout and its full epoch-900 raw/zstd results.

## Result

The current fast account-projection converter is not a complete Index Archive
converter. It writes only:

- `archive-v2-resolved-accounts.pages`;
- `archive-v2-resolved-accounts.index`;
- a non-publishable benchmark report.

This is part of the Phase 0 bridge described in
`blockzilla-index-archive.md`. It does not write the ledger, runtime effects,
reverse account postings, or a complete generation.

The existing full converter does write the 19-object nonzero-epoch layout, but
it is not a suitable base for the fast migration. Its normalized target schema
requires work that the migration contract now keeps out of conversion:

- it changes recent-blockhash ownership and reference namespaces;
- it assigns target IDs to inline pubkeys;
- it reconstructs typed source instructions as raw signed bytes;
- it reads and verifies one Ed25519 signature per transaction to select an
  ambiguous instruction encoding;
- it renders compact typed logs into text and tokenizes the text again;
- it pairs token balances and remaps their pubkeys;
- it validates, hashes, and rereads the completed target.

The converter must be a mechanical, source-preserving split. Verification,
file hashing, and sealing are separate later jobs.

## Measured size correction

The 10,000-block epoch-900 full-converter benchmark reported these logical
plane bytes:

| Object | Bytes |
| --- | ---: |
| `catalog/blocks.wincode` | 1,440,064 |
| `ledger/transactions.wincode` | 306,937,454 |
| `runtime/inner_instructions.wincode` | 308,219,803 |
| `runtime/outcomes.wincode` | 29,879,249 |
| `runtime/balances.wincode` | 214,642,492 |
| `runtime/token_balances.wincode` | 105,415,143 |
| `runtime/logs.wincode` | 510,682,469 |
| `runtime/rewards.wincode` | 64 |
| `runtime/block_rewards.wincode` | 12,829,348 |
| retained PoH | 219,066,041 |
| retained shredding | 17,921,694 |
| **Total** | **1,727,033,821** |

The same source prefix contains 1,361,734,890 compressed block bytes. Logs,
inner instructions, and token balances alone are 924,317,415 bytes, or 53.5%
of the reported split output. All runtime effects are 1,181,668,568 bytes, or
68.4%.

The account-projection fleet estimate of approximately 6.1 TB therefore
describes only the forward account bridge. It is not an estimate for the final
plane-split archive. A modern full-epoch split canary is required before a
fleet storage estimate is valid.

## Source-preserving target direction

The revised target must keep these source identities until a separate verifier
proves and seals a normalized representation:

- exact `CompactPubkey::{Id, Raw}` values;
- the existing pubkey registry ID order and lookup auxiliaries;
- existing blockhash registry IDs, the previous-blockhash tail, and nonce
  values;
- an explicitly selected Compact V2 message profile;
- typed System, Vote, and Compute Budget instruction forms;
- the exact compact log stream rather than rendered text;
- exact pre and post token-balance lists;
- explicit raw message, raw metadata, and missing-coverage states.

The final manifest can bind immutable source payload objects directly. On the
same filesystem, a complete candidate can use hard links or copy-on-write
clones. The converter must not read signature, PoH, or shredding payloads to
create those bindings.

## Next measured converter slice

Extend the existing borrowed, ordered account-projection pass. Decode each
source block once and produce:

1. the existing forward resolved-account page;
2. one explicit missing/decoded/raw metadata-state lane;
3. source-profile inner-instruction chunks;
4. exact compact-log chunks;
5. exact pre/post token-balance chunks;
6. exact pre/post lamport-balance chunks;
7. outcome, fee, return-data, compute-unit, and cost-unit chunks;
8. transaction-reward chunks;
9. block-reward pages;
10. one opaque exact-byte lane for raw metadata fallbacks.

These effect files and their common index are measurement containers. They do
not freeze or replace the current target runtime codecs. The account page is
also a deduplicated derived projection. It is not the future canonical ordered
message and loaded-account lane.

The slice has these rules:

- 12 bounded workers over borrowed block/message/metadata data;
- one source decompression per block;
- worker-local buffers and compression contexts are reused;
- effect chunks cover at most 256 transactions;
- zstd level 1 is used only when it makes a chunk smaller;
- output order follows the source block index;
- pubkey and hash IDs are not assigned or changed;
- no signature verification;
- no PoH recomputation;
- no content hashing or sealing;
- no saved-output reread in the conversion path;
- output status stays `unverified-nonpublishable`.

### Frozen canary container

The canary keeps the two existing resolved-account files byte-identical and
adds nine measurement data files: metadata states, inner instructions, logs,
token balances, balances, outcomes, transaction rewards, block rewards, and
opaque raw-metadata fallbacks. One common source-split index locates all nine
files. Every data file and the common index start with a 64-byte header that
binds the epoch, source profiles, scope, plane, and selected block count.

The common index has one 160-byte row per source block. A row contains
`block_id:u32`, `slot:u64`, `tx_count:u32`, followed by nine absolute
`{offset:u64, len:u64}` spans. The first offset in each data file is 64. Spans
are gapless in block order, including zero-length spans, and the final end is
the exact file length.

Transaction planes use frames of 1 to 256 source transactions. The exact
32-byte frame header is:

```text
plane:u16, version:u16, flags:u32, block_id:u32, first_tx:u32,
tx_count:u32, dense_count:u32, decoded_len:u32, stored_len:u32
```

Only the known zstd flag can be set. Zstd level 1 is used only when the stored
payload is strictly smaller than the decoded payload. Lengths exclude the
32-byte frame header; index spans include it. Empty dense effect frames are
omitted. A present block-reward record uses `first_tx=0`, `tx_count=0`, and
`dense_count=1`; an absent record has a zero-length span.

The metadata visitor records these exact, self-delimiting source ranges and
requires the final offset to equal the metadata length:

```text
err, fee, pre balances, post balances, inner instructions, logs,
pre token balances, post token balances, transaction rewards,
loaded writable, loaded readonly, return data, compute units, cost units
```

Balances and token balances use
`first_len:u32 | second_len:u32 | exact first range | exact second range`.
Outcomes use
`head_len:u32 | tail_len:u32 | head(err+fee) | tail(return/CU/cost)`. A raw
fallback record uses `len:u32 | exact source bytes`. Loaded-address ranges are
not copied into these effect files; the existing account page remains a
derived account projection. The canary must not claim that it can reconstruct
the full original metadata record.

The state lane uses two bytes per source transaction. Byte 0 records
missing/decoded/raw metadata and the source raw-transaction flag; all other
bits are zero. Byte 1 records the measured effect-state bits. Decoded metadata
always has outcome and balance records, and independently records CPI, token,
log, and reward presence. Missing or raw metadata has unavailable CPI and no
normal effect records; raw metadata has exactly one opaque fallback record.

For a raw-transaction row with decoded metadata, the source message shape is
not available. The visitor checks the complete selected metadata grammar,
exact end of input, and collection sizes with the 256-account protocol cap.
It does not compare inner-group, CPI-program, or CPI-account indexes with an
invented message-account or top-level-instruction count. The report labels
this path as structural-only validation.

Worker scratch has an aggregate 512 MiB cap for raw-plane and compression
capacities. The retained raw-plus-compression capacity is trimmed to 128 MiB,
and retained chunk descriptors have a separate 8 MiB cap. The packed stored
block result is owned by the bounded reader task after projection; the report
shows its maximum separately from worker scratch.

Block rewards require a current-schema borrowed view that validates the full
`Option<ArchiveV2HotRewards>` wire value and returns its exact source byte
slice. An owned decode followed by re-encoding is forbidden because it can
change a non-canonical source integer spelling. Historical outer-schema
fallback is rejected by this canary.

Tests can decode and compare output. The production conversion path cannot pay
that cost. A separate verifier performs structure, semantic, continuity, and
optional cryptographic checks after conversion.

## Lean block-chunk result

The source-split canary above proved the borrowed metadata traversal, but its
256-transaction frames add a page layer that the final reader does not need.
The lean canary keeps one independently decodable chunk per source block and
per effect object. It keeps the resolved-account bridge unchanged.

The lean canary writes one common index and these nine chunk objects:

1. transaction directory;
2. inner instructions;
3. logs;
4. token balances;
5. lamport balances;
6. outcomes;
7. transaction rewards;
8. raw metadata fallbacks;
9. block rewards.

Each object starts with one 64-byte file header. There is no page or frame
header inside an object. The common index starts with one 64-byte header and
has one 160-byte row per source block. A row contains the 16-byte block
identity and nine 16-byte locators. A locator contains an absolute offset, a
stored length with one zstd flag bit, and a decoded length. The locators are
gapless in block order.

The decoded transaction-directory chunk is exactly 24 bytes per source
transaction:

```text
source_flags:u16, effect_state:u8, reserved:u8,
inner_end:u32, logs_end:u32, token_end:u32,
balances_end:u32, outcome_end:u32
```

The five end offsets are relative to the start of their decoded block chunk.
A repeated end offset means that the transaction has no record in that
object. This lets a reader select one transaction and only the effect objects
that it needs.

Frequent effect records are exact self-delimiting source Wincode fields placed
back-to-back. They have no added per-record length or offset. Transaction
rewards use a sparse ordered stream of `tx_index:u32` plus the exact reward
vector. Raw metadata uses `tx_index:u32`, `raw_len:u32`, and exact source bytes.
Block rewards store one exact source `Option` value per block.

Each nonempty block chunk can be raw or one zstd level-1 frame. The locator bit
is authoritative. Blocks can therefore decode in parallel while all objects
keep the same index geometry. Conversion does not hash, reopen, read, or
validate written output.

### Full epoch-900 measurement

The full run covered 431,858 blocks and 476,026,811 transactions. Both raw and
zstd conversions completed in about 3.5 minutes. The raw lean objects used
141,372,985,857 bytes. The zstd lean objects used 49,154,731,708 bytes, a
65.2305% file-size reduction. The unchanged resolved-account bridge used
6,683,526,139 bytes.

| Object | Raw file bytes | Zstd file bytes | Saving |
| --- | ---: | ---: | ---: |
| Transaction directory | 11,424,643,528 | 5,947,972,795 | 47.937% |
| Inner instructions | 36,108,329,645 | 12,608,858,531 | 65.080% |
| Logs | 41,047,172,395 | 14,261,848,893 | 65.255% |
| Token balances | 17,755,570,460 | 4,414,460,391 | 75.138% |
| Balances | 29,462,990,702 | 10,633,424,246 | 63.909% |
| Outcomes | 5,473,327,929 | 1,184,606,918 | 78.357% |
| Transaction rewards | 64 | 64 | empty |
| Raw metadata fallbacks | 64 | 64 | empty |
| Block rewards | 31,853,726 | 34,462,462 | -8.190% |

A separate read-only scan measured all nine objects. With 12 workers, raw
needed 259.931 seconds and delivered 518.437 MiB/s. Zstd needed 96.657 seconds
and delivered 1,394.187 MiB/s of decoded data. Raw gained only 1.093% from one
to 12 workers because storage was the limit. Zstd was 2.69 times faster than
raw at 12 workers because it read 92.2 GB less data and decoded in parallel.

The measured v1 hybrid policy is therefore:

- zstd level 1 for the transaction directory, inner instructions, logs, token
  balances, balances, and outcomes;
- raw for block rewards, without a compression attempt;
- adaptive per-block storage for transaction rewards and raw metadata when
  these sparse objects contain records.

The full epoch-900 hybrid run completed in 206.902 seconds. It wrote
49,152,122,972 bytes of lean output and 55,835,649,111 bytes with the
resolved-account bridge. It produced 2,591,028 zstd chunks and 431,858 raw
block-reward chunks. Block rewards made zero compression attempts. Live
process monitoring measured approximately 446 MiB peak resident memory.

These files are still a measurement candidate, not a complete Index Archive.
The message ledger, retained-source manifest, reverse indexes, and final
catalog remain deferred.

### Zstd-level measurement

The same 10,000-block epoch-900 prefix was converted with zstd levels 1, 3,
5, and 9. All runs used the Hybrid object policy, 12 workers, and the same
source profiles. The table reports the complete lean files, summed worker
compression CPU, and converter wall time.

| Level | Lean file bytes | Saving vs L1 | Compression CPU | Wall |
| --- | ---: | ---: | ---: | ---: |
| 1 | 1,146,914,664 | - | 9.951 s | 4.608 s |
| 3 | 1,123,037,729 | 23,876,935 (2.082%) | 15.636 s | 5.241 s |
| 5 | 1,114,279,189 | 32,635,475 (2.846%) | 41.038 s | 7.384 s |
| 9 | 1,094,410,562 | 52,504,102 (4.578%) | 69.253 s | 10.118 s |

Level 9 used approximately 431 MiB peak resident memory in live process
samples. Memory pressure stayed at zero and there was no sustained swap.

The reader scanned each candidate in reverse level order for three iterations.
Mean wall time per iteration was:

| Level | 1 worker | 4 workers | 12 workers |
| --- | ---: | ---: | ---: |
| 1 | 3,129.7 ms | 854.3 ms | 457.0 ms |
| 3 | 2,593.7 ms | 831.7 ms | 457.0 ms |
| 5 | 2,494.3 ms | 833.0 ms | 455.7 ms |
| 9 | 2,347.3 ms | 797.7 ms | 437.7 ms |

Higher compression levels did not cause a read regression. With 12 workers,
levels 1, 3, and 5 were effectively equal. Level 9 was 4.2% faster than level
1 because it read less compressed data and used 8.1% less summed decode-worker
time.

If one level must apply to all zstd objects, level 3 is the balanced default.
It saves 2.08% more than level 1 for 13.74% more converter wall time. Level 5
is not an efficient intermediate point. Level 9 is valid for a storage-first
archive, but it uses 6.96 times the compression CPU of level 1.

The per-object data supports a better fixed, versioned policy:

- transaction directory: level 1;
- inner instructions, logs, token balances, and lamport balances: level 9;
- outcomes: level 5;
- block rewards: raw;
- sparse transaction rewards and raw fallbacks: adaptive level 1 until a
  nonempty source sample is measured.

Applying each 10,000-block object ratio to its exact full-epoch level-1 object
size estimates 46,741,016,417 bytes of lean output and 53,424,542,556 bytes
with accounts. This is 2,411,106,555 bytes (4.318%) smaller than the measured
level-1 candidate. It also beats uniform level 9 by an estimated 116,428,052
bytes per epoch while using 12.9% less compression CPU on the prefix. This is
an estimate, not a produced full-epoch mixed-level candidate.

## Work that stays deferred

- Do not build `indexes/selectors` until selectors over typed source
  instructions have a frozen definition.
- Do not add account or program external-sort runs to this first effect-split
  canary. Measure the split path without a second performance variable.
- Do not render logs during conversion.
- Do not pair token balances during conversion.
- Do not normalize source blockhash references during conversion.
- Do not publish or delete Archive V2 from a converter exit code.
- Do not run the all-epoch conversion until a modern full-epoch split canary
  reports exact bytes, throughput, memory, and coverage.
