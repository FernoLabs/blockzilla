# All-token event dump and balance replay

Date: 2026-08-28

## Objective

Dump every classic Token and Token-2022 instruction from the canonical archive.
Then replay the public raw `u64` token amount for every token-account lifecycle.
Compare the replay state with every available transaction token-balance row.

The result can claim **100% match** only when all comparison exclusions and all
unknown instruction counts are zero. UI token amounts are not part of this
claim. Interest and scaled-UI extensions can change UI values without a change
to the public raw amount.

## Immediate prerequisite

Finish the schema-3 SPYX consolidation before this audit starts. That smaller
completed dump is the canary for one sorted, one-based global public-key
registry and one canonical signature-occurrence stream. It also proves the
exact message and metadata public-key rewrite path on real historical epochs.

The SPYX consolidator must resolve only keys referenced by its 7,311,137
selected transactions. It must not merge all 8.19 billion rows from the 218
source epoch registries. It keeps blockhash references source-bound and copies
signatures without value deduplication.

## Source status

| Item | Result |
| --- | ---: |
| Canonical archive | `/volume1/blockzilla/archive` |
| Epochs | 0–1018, continuous |
| Epoch directories | 1,019 |
| Blocks | 418,258,201 |
| Transactions | 565,020,467,324 |
| Compressed block bytes | 51,010,468,782,501 (46.394 TiB) |
| Genesis archive | `/volume1/blockzilla/old-faithful/genesis.tar.bz2` |
| First epoch with the classic Token program in the registry | 72 |

All canonical epoch directories have non-empty block, index, metadata,
public-key registry, MPHF, and signature files. No raw transaction, metadata,
or reward fallback was found in the inventory. The historical epochs do not
have the current generation manifest and metadata marker. They must use the
trusted-local historical admission profile.

Epoch 72 is the first replay canary. It has 55,678,159 transactions and 6,596
classic Token instructions. All are top-level. There are no Token-2022
instructions. The old prototype reports 62 instruction tags that it does not
know. The new lossless decoder must classify or retain each one before replay.

Epoch 100 is the second classic-Token stress canary. It has 85,985,993
transactions, 37,509,438 classic Token instructions, and 37,427,444 recorded
inner instructions. Epoch 900 is the historical metadata-format canary.

## Strict comparison contract

The state key is `(token account public key, lifecycle generation)`. Replay
uses canonical `(epoch, slot, source block ID, transaction index)` order.

For each comparable transaction:

1. Compare replay state with each pre-token-balance row.
2. Classify each token invocation as committed, rolled back, or unknown.
3. Apply only committed effects from a successful transaction.
4. Compare replay state with the union of post-token-balance rows and modeled
   accounts touched by the transaction.
5. Compare row presence, token program, mint, public raw amount, decimals, and
   owner when the source supplies the owner.

A failed transaction changes no replay state. A successful transaction can
catch a failed CPI. A successful child CPI can also be rolled back when its
parent fails. Therefore the inner-instruction list alone is not proof that a
state change committed. The audit must combine instruction stack height with
ordered `Invoke`, `Success`, and `Failure` log boundaries. Missing or malformed
evidence makes the affected transaction non-comparable.

The strict model must never seed or repair its state from token-balance
metadata. A separate transaction-local diagnostic mode can start from pre rows,
but its results cannot prove a genesis-to-tip replay.

After the first continuous mismatch, mark that account lifecycle as tainted.
Report its later differences separately. This prevents one error from producing
a false cascade of independent mismatches.

## Required coverage gates

The final report must show these counters for each epoch and for the full run:

- source blocks and transactions;
- successful, failed, and status-unknown transactions;
- transactions with metadata, logs, inner instructions, and token-balance data;
- pre and post token-balance rows;
- classic Token and Token-2022 outer and inner instructions;
- every raw top-level tag and extension subtype;
- known, malformed, unknown, committed, rolled-back, and commit-unknown calls;
- comparable and excluded transactions, with one count for each exclusion
  reason;
- account lifecycles created, closed, reopened, clean, and tainted;
- pre mismatches, post mismatches, missing rows, unexpected rows, and field
  mismatches.

Compact historical metadata can encode an absent token-balance vector as the
same empty vector used for a true empty result. Before a full claim, the audit
must either read a source that preserves this presence bit or prove that token
activity starts after token-balance recording became complete. Epoch 72 is the
first test of this boundary.

Native wrapped SOL also needs pre/post lamport balances, account length, and the
historical rent reserve. Token-2022 transfer fees need mint fee state. Other
extensions need explicit public-amount rules. If required input is absent, the
transaction is excluded and the reason is counted. It must not be guessed.

## Data layout

Phase A writes one immutable source-bound shard per epoch. It reads each block
once and uses borrowed decoding and reused buffers. The shard contains:

- `transactions.bin`;
- `token-instructions.bin`;
- `instruction-accounts.bin`;
- `instruction-data.bin`;
- `inline-pubkeys.bin`;
- `token-balance-oracle.bin`;
- `coverage.bin`; and
- `manifest.json`, published last.

Each instruction record keeps the complete raw data, exact ordered account
list, outer or inner coordinate, stack height, transaction status, commit
classification, and source signature range. Unknown and malformed instructions
remain in the dump.

Phase B builds one sorted, one-based global public-key registry and a global
occurrence-ordered signature stream. Signatures are not deduplicated. It writes
transactions and events in canonical order.

Phase C uses dense memory-mapped state indexed by global public-key ID. It
writes mismatch rows and counters, not a large in-memory map or a full copy of
all account states.

## Run gates

Do not start the 46.394 TiB scan until all of these gates pass:

1. The phase-A wire-format tests pass.
2. The instruction classifier keeps unknown and malformed raw data.
3. The committed-CPI classifier passes caught-failure and failed-ancestor tests.
4. Epoch 72 proves the metadata-presence boundary and produces a valid shard.
5. Epoch 100 passes the high-CPI canary.
6. Epoch 900 passes the legacy metadata canary.
7. A small replay canary has zero unexplained coverage loss.
8. Resume, manifest-last publication, and file-hash checks pass.

The full run must use a named `tmux` session. It must write a checkpoint after
each completed epoch. The first estimate is 46 to 67 hours if the archive read
rate matches the earlier token and SPYX scans. At a sustained 60 MB/s, the read
alone takes about 9.8 days.

## Current decision

The archive is suitable for a full attempt, but the old token scanner is not
suitable for a 100% claim. It drops unknown instructions, does not classify
committed CPI subtrees, has incomplete Token-2022 support, and does not prove
historical token-balance presence. Use it only for tag census data.

The new audit code must first pass epoch 72. After that canary, continue with
epochs 100 and 900. Start the resumable all-epoch `tmux` run only after these
three results have no unexplained gap.

## SPYx canary implementation status

The consolidated SPYx dump now has a direct, fail-closed instruction replay
command:

```text
blockzilla-token-transaction-dump spyx-replay DUMP REPORT [--max-transactions N]
```

The command reads the canonical transaction stream once. It reuses decode
buffers, resolves compact keys against the consolidated registry, classifies
committed and rolled-back token calls, applies public raw balance effects, and
compares each pre-state and post-state with token-balance metadata. It stops
the proof at the first unexplained instruction or mismatch, but it continues
the full instruction census. A limited run is only a canary and cannot set the
complete-match flag.

The strict NAS replay is complete. It used binary SHA-256
`d0c64f069a73b62c9cc15648b996cb624c620cdb23d2ee6afecf12d3648a53d0`
and finished 7,311,137 transactions in 68.07 seconds.

| Final SPYx replay gate | Result |
| --- | ---: |
| Status | `complete_match` |
| Complete bounded scan | true |
| Transactions applied | 7,311,137 |
| Clean replay prefix | 7,311,137 |
| Pre rows compared | 21,545,596 |
| Post rows compared | 21,602,469 |
| Replay errors | 0 |
| Pre mismatches | 0 |
| Post mismatches | 0 |
| Missing metadata | 0 |
| Unknown commit status | 0 |
| Final positive-balance accounts | 29,064 |
| Final public raw balance | 9,523,486,565,248 |

The final reducer state SHA-256 is
`3570f9fb1ebe7e18fbda9d20c80fc16b80edbc0bdae3579347dd89419fb1bfe6`.
The local final report is
`benchmark-results/spyx-token-report-v1/spyx-instruction-replay-final.json`,
with SHA-256
`218fe5ffd9ebf6edba8fcaed151adc8c362f505471c15434af475fcf9d54ec89`.

The final oracle contract permits missing rows only for transaction-final
unchanged accounts. A changed account that was open before the transaction
must have a matching pre row. A changed account that is open after the
transaction must have a matching post row. A modeled close requires its pre
row and requires the post row to be absent.
