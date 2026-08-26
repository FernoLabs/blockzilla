# Instruction data: where the bytes are, and what compresses them

Measured on the `epoch-822-biggest.car` block (2,969 transactions, 9,033
top-level instructions) converted to Index Archive planes. Reproduce with
`blockzilla-index-archive-convert` and the analysis in this document's
"How to reproduce" section.

Status: **findings, not decisions.** Nothing here has been implemented. The
dedup work is deferred because it needs a pass over every payload in the
archive, which is expensive to commit to before the format is finished.

---

## 1. Rank planes by compressed bytes, not by ratio

Ratio is a trap. The two highest ratios in the archive are its two smallest
planes:

| plane | zstd-3 | raw | ratio | share of archive |
|---|---:|---:|---:|---:|
| `runtime/logs` | 53,774 | 233,420 | 4.3× | 21.0% |
| `runtime/inner_instruction_data` | 48,979 | 99,691 | **2.0×** | 19.1% |
| `runtime/balances` | 42,253 | 187,285 | 4.4× | 16.5% |
| `ledger/instruction_data` | 40,939 | 267,055 | 6.5× | 16.0% |
| `runtime/token_balances` | 23,431 | 285,869 | 12.2× | 9.1% |
| `ledger/accounts` | 19,000 | 66,679 | 3.5× | 7.4% |
| `ledger/core` | 1,485 | 35,628 | **24.0×** | 0.6% |
| `runtime/rewards` | 23 | 5,938 | **258×** | 0.0% |

`core` at 24× holds 2,969 rows with **1 distinct blockhash ref, 5 distinct flag
values, and 25 distinct header tuples** — genuinely redundant, and entirely
captured by zstd already. Optimising it perfectly would save 0.6% of the
archive. `rewards` at 258× is 23 bytes.

**A high ratio means zstd already found it.** Look for large compressed
absolute size instead.

> Caveat: this fixture's blockhash registry has one entry (a single-block build
> with no `--previous-car`), so `core` looks more compressible here than at
> scale. In the full epoch-822 run it was 11.8× and 9% of the total.

---

## 2. Why `instruction_data` compresses 6.5× and `inner_instruction_data` 2.0×

They share nearly all their code. The difference is entirely in the data.

| | `instruction_data` | `inner_instruction_data` |
|---|---:|---:|
| payloads | 9,033 | 1,504 |
| distinct | **2,231 (24.7%)** | **955 (63.5%)** |
| bytes kept if deduped | **24.3%** | **87.3%** |
| mean length | 28.4 B | 65.1 B |

Top-level instruction data is **75.7% literal duplication**. Inner instruction
data is only 12.7% — it is real program call parameters with varying amounts,
which is why it sits near the incompressible floor.

Four byte strings cover 55% of top-level instructions:

| bytes | count | decoded |
|---|---:|---|
| `0000` | 1,365 | tag 0 `Raw`, zero length |
| `0303c63e` | 1,303 | tag 3 `ComputeBudget`, inner 3 `SetComputeUnitPrice` |
| `0018f8c69e91…` (26 B) | 1,303 | tag 0 `Raw`, 24-byte payload |
| `0701a3bdb5a9…` (142 B) | 977 | tag 7 `VoteTowerSync` |

---

## 3. Per-program distribution

Instruction *count* and instruction *bytes* rank completely differently.

| program | ix | % ix | bytes | % bytes | distinct payloads | dedup keeps |
|---|---:|---:|---:|---:|---:|---:|
| **Vote** | 1,023 | 11.3% | 145,219 | **56.5%** | **25** | **2.4%** |
| `dbcij3LWUpp…` | 1,303 | 14.4% | 33,878 | 13.2% | **1** | **0.1%** |
| `RouterBmuRBk…` | 54 | 0.6% | 21,526 | 8.4% | 54 | 100% |
| ComputeBudget | 3,685 | **40.8%** | 16,870 | 6.6% | 1,379 | 40.6% |
| System | 497 | 5.5% | 5,553 | 2.2% | 257 | 81.5% |
| Memo | 83 | 0.9% | 3,563 | 1.4% | 78 | 98.6% |
| AssocTokenAcct | 1,483 | 16.4% | 3,084 | 1.2% | **3** | **0.3%** |
| SPL Token | 268 | 3.0% | 853 | 0.3% | — | — |

84 distinct programs in the block.

**Vote is 56.5% of instruction bytes and has 25 distinct payloads.** Validators
voting on the same fork emit identical TowerSync structures; the per-validator
variance lives in `signatures.bin`, not here.

`dbcij3LWUpp…` is **1,303 instructions with exactly one distinct payload** — an
oracle or crank. AssocTokenAcct: 1,483 instructions, 3 distinct.

Meanwhile the programs whose payloads are genuinely unique — Router (54/54),
Memo (98.6%), Phoenix (98.2%) — are small and near-incompressible.

### Consequence: a per-program codec is the wrong investment

The bytes are concentrated in programs that are ~100% duplicate, and Archive V2
**already types Vote** as `VoteTowerSync` — yet the typed payload is still 142
bytes and 97.6% duplicate.

**Typing captures structure. Dedup captures repetition. This is a repetition
problem.** A Phoenix or Router codec would be effort spent on 8.4% of bytes that
do not compress anyway.

---

## 4. What the existing log codec teaches

`program_logs/` is a 19-program typed log codec, enabled by default, with a
283-entry `LogEvent` vocabulary, a string table, and a `ProgramLog::Unknown`
fallback that keeps it lossless. It is exactly the shape a per-program
instruction codec would take, already built and round-trip tested.

It makes the same mistake this document is about:

```rust
pub fn push(&mut self, s: &str) -> StrId {
    self.lengths.push(len);
    self.bytes.extend_from_slice(s.as_bytes());   // no dedup
```

`StringTable::push` never deduplicates, so a log line emitted 1,000 times in a
block is stored 1,000 times. `runtime/logs` is the largest plane in the archive
at 21% — after all that typing work.

Separately, `StringTable::resolve` recomputes a string's offset by summing every
preceding length on each call, so rendering a log stream is **O(n²)** in string
count. `render_logs` calls it per event, which puts it on the get-block serving
path. The layout cannot change — `StringTable` is serialized into all 1,011
generations — but the index can be rebuilt at decode time.

---

## 5. Does manual dedup beat zstd?

**Only above a page-count threshold — and then it also beats a zstd trained
dictionary.** The same payload mix, split into independently compressed pages:

| pages | plain zstd | zstd trained dict (16 KiB) | manual dedup + shared table |
|---:|---:|---:|---:|
| 1 | **40,912** | — | 44,318 (+8.3%) |
| 8 | 46,399 | 50,964 | **44,668** |
| 32 | 52,983 | 53,706 | **46,236** |
| 128 | 65,662 | 55,839 | **48,962** |

At one page zstd wins: its window covers the whole block, so it already finds
every repeat, and an explicit dictionary only adds framing.

As pages shrink the curves diverge. Plain zstd grows **+60%** (40.9K → 65.7K)
because every page re-establishes its context from scratch. Manual dedup grows
**+10%** because the table is paid once and pages hold only references.

This is structural, not a compression-quality argument: **zstd cannot see across
frame boundaries, and a shared table is the boundary-crossing mechanism.**

The trained dictionary underperforms manual dedup at every page count, and loses
to plain zstd at 8 and 32 pages. A dictionary gives zstd sample material to match
against, but each match still costs entropy-coded bytes; an explicit reference
costs 1–2 bytes for a 142-byte vote payload, flat. For data that is 75% exact
duplicates, references beat matching.

### Why this is not implemented yet

Two things are unmeasured, and both need a full-epoch pass:

1. **Epoch-wide distinct payload count.** The hot set looks like ~30 payloads,
   but "looks like" is not a number, and it decides whether the table is
   kilobytes or gigabytes.
2. **Cross-block recurrence.** The table above splits *one* block's payloads
   synthetically. Real recurrence across 430,954 blocks is probably stronger,
   but that is an expectation, not a measurement.

Building the table requires a pass over every payload in the archive, which is
why this is deferred until the format is finished.

There is also a read-speed argument independent of size: with an explicit table,
reading one instruction's payload is a table lookup rather than decompressing
its whole page.

---

## How to reproduce

```bash
blockzilla build-archive-v2-hot-blocks <fixture.car> /tmp/v2hot
blockzilla-index-archive-convert /tmp/v2hot /tmp/planes
```

Plane sizes and ratios come from zstd-3 over each `.pages` file. The payload,
per-program, and page-split analyses parse the plane encodings directly:
`ledger/accounts` is `(static, loaded_writable, loaded_readonly)` counts then
ULEB128 ids; `ledger/instructions` is a per-transaction count then
`(program_position, account_count, positions…, data_len)`; `ledger/instruction_data`
is ULEB128 `len + 1` then payload bytes, with `0` meaning absent. Program ids
resolve through `registry.bin` at `(id - 1) * 32`.
