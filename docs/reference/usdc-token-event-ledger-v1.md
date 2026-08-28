# USDC token event ledger V1

Status: implemented contract, 2026-08-28. The source-neutral classic Token
processor, the restart-safe SQLite store, and the bounded three-format network
command are implemented. Run-specific network results are not part of this
contract.

This ledger reads instructions. It does not read pre-token or post-token
balance observations.

The result is an instruction-derived event and delta log. It is not an
observed balance ledger.

## Reference target

The reference run has this target:

- mint: `EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v`;
- program: classic SPL Token, `TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA`;
- unit: raw `u64` base units;
- decimals: 6.

The network command uses this mint by default. Its `--mint` option can select
another classic SPL Token mint. Such a run is not a USDC run. The report and
database bind the selected mint. The processor records decimal values from
the applicable instructions. It does not read a mint account or token-balance
metadata to infer them.

Do not accept the classic Token program and Token-2022 for the same mint in one
run.

## Input requirement

The processor consumes the source-neutral
[Archive Instruction Stream V1](archive-instruction-stream-v1.md). It needs:

- canonical slot and transaction order;
- resolved public keys for all instruction accounts;
- raw instruction data;
- outer and inner instruction coordinates;
- transaction execution status;
- complete CPI coverage, or an explicit coverage issue.

The token rules must not depend on CAR, Compact V2, or Indexer V3 types.

The scan request requires exact instruction data for the classic SPL Token
program. It can accept unavailable data for unrelated programs. Missing or
ambiguous data for a matching Token instruction is a coverage issue, not a
no-match result.

The decoder recognizes classic Token tags `0` through `24`, `38`, `45`, and
`255`. The processor records each recognized instruction that exact evidence
shows is related to the target mint or one of its tracked account lifetimes.
It does not copy unrelated Token events into the target ledger. An unknown tag
that can touch the target creates a coverage issue. The processor does not
guess an event or a delta.

## Output records

The core processor must produce source-neutral records. SQLite is one sink for
these records. It is not the core reader.

The logical records are:

- one account key record for each 32-byte public key;
- one account-lifetime record for each initialization-to-close interval;
- one token event for each target-related supported instruction;
- zero, one, or two exact token-delta legs for an event;
- one coverage issue when an exact decision is not possible;
- one source and checkpoint record for restart safety.

An application sink receives complete event batches. It can write SQLite,
Parquet, a service database, or an in-memory index without a new archive
adapter.

The USDC processor consumes `BlockView` records through the common
`BlockSink` boundary. Its event-output sink is a separate application
boundary. This separation keeps archive decoding, token semantics, and
database storage independent.

## Event order

Use this order:

1. slot;
2. canonical transaction index;
3. flattened canonical instruction order;
4. outer instruction index;
5. inner instruction index in its outer group;
6. batch sub-index, if a token Batch contains more than one child.

Keep each coordinate field. Do not store only one flattened number.

An outer instruction has no inner index. An inner instruction keeps the stack
height when the source records it. A missing stack height stays missing.

## Commit and invocation evidence

A successful transaction applies all of its token changes. A failed
transaction applies none of them.

Each event has a commit state:

- `committed`;
- `rolled-back`;
- `not-committed`;
- `unknown`.

It also has separate invocation evidence:

- `invoked`;
- `not-invoked`;
- `unknown`.

Only a committed event can have a ledger effect or change account-lifetime
state. Temporary discovery during a failed or unknown transaction does not
change durable account state.

An inner instruction in metadata is an observed invocation. An outer message
instruction after the failing outer instruction can be present in the message
without having run. The common stream publishes the exact failed outer index
when the source records an `InstructionError`. The processor uses that
boundary to distinguish invoked, not-invoked, and unknown outer instructions.

## Exact classic USDC deltas

These core SPL Token instructions have exact public USDC effects:

| Tags | Event | Applied delta |
|---|---|---|
| 1, 16, 18 | Initialize account | Start a lifetime with token amount zero |
| 3 | Transfer | Source `-amount`, destination `+amount` |
| 7 | MintTo | Destination `+amount` |
| 8 | Burn | Source `-amount` |
| 9 | Close account | End a lifetime; an applied USDC close has amount zero |
| 12 | TransferChecked | Source `-amount`, destination `+amount` |
| 14 | MintToChecked | Destination `+amount` |
| 15 | BurnChecked | Source `-amount` |

A normal applied transfer has two delta legs. Both legs belong to one atomic
event. A reader must apply the two legs together.

A self-transfer also keeps two legs for audit work. Both legs use the same
account and amount. Their grouped net change is zero. A reader must not show
the debit leg as a temporary account balance.

A grouped per-event and per-account view can sum the legs before it presents a
balance change. This view produces one zero change for a self-transfer.

The amount must stay a full `u64`. SQLite signed integers cannot store all
possible instruction amounts. A SQLite sink must use a canonical unsigned
encoding, such as an eight-byte little-endian value and canonical decimal
text.

## Other target-related events

Record supported target-related core instructions even when they have no USDC
amount change. This gives an audit trail for account and authority state.

This group includes:

- mint initialization;
- approve and approve-checked;
- revoke;
- set-authority;
- freeze and thaw;
- immutable-owner initialization;
- withdraw-excess-lamports and unwrap-lamports, with no USDC token delta;
- supported amount conversion and account-size queries.

`SyncNative` cannot apply to a USDC account. A successful target assignment to
this instruction is an error.

Mint and multisignature configuration are not token-account balance changes.
They must not create a token delta.

Tag 255 is the classic Token `Batch` instruction. Expand its children once and
keep the child index. A nested Batch is invalid. An empty Batch, an empty child
payload, and an account overrun are invalid. Unused parent accounts after the
last child are valid. There is no protocol child-count limit other than the
encoded transaction geometry and the processor resource limits.

A Batch runs its children in order. If a child or terminal Batch geometry
fails, the transaction cannot commit a prefix. When the source says that such
a Batch completed, the processor reports a hard source conflict before it
creates effects. When the exact failure boundary is the Batch itself, child
invocation remains unknown and no effect commits.

An unknown or new instruction that can touch the target must create an
indeterminate coverage record. It must not create a guessed delta.

## Account discovery

Do not use one frozen global set of token-account public keys. A token account
can close, and its address can later have a new lifetime or a different mint.

Use two identities:

- `account_id` identifies one 32-byte address;
- `lifetime_id` identifies one active token-account lifetime.

All applied delta rows refer to a lifetime.

Successful instructions can discover a USDC account through:

- `InitializeAccount`, `InitializeAccount2`, or `InitializeAccount3` with the
  USDC mint;
- a checked transfer or checked approval with the USDC mint;
- mint, burn, freeze, or thaw instructions that contain the USDC mint;
- an unchecked transfer when one endpoint is already an active USDC account.

An unchecked `Transfer` does not contain a mint. If neither endpoint is known,
that instruction alone cannot prove that it is a USDC transfer.

The implemented tracker uses ordered forward evidence. It does not rewrite an
earlier unchecked transfer after a later instruction identifies one endpoint.
Such an earlier transfer stays unresolved and makes coverage partial.

A later offline resolver can propagate proven identity backward and forward
through unchecked transfers in the same lifetime. It must not cross a close,
a reinitialization event, or a history gap. If no instruction in the connected
lifetime proves the mint, the resolver must keep the transfer unresolved.

A successful close ends the current lifetime. A later successful
initialization creates a new lifetime. Discovery must not cross this boundary.
After a history gap, new exact mint evidence starts a new generation, even if
the last observed generation used the same mint. A close and address reuse can
have occurred inside the gap.

Do not use one frozen epoch-wide public-key set. That set can assign an earlier
or later lifetime to the wrong mint after an address is reused.

## Sparse-history limit

Epochs `0, 100, 200, ... 1000` are separate history samples. They are not one
continuous ledger.

For each sparse segment, use one of these inputs:

- a trusted opening set of active USDC account lifetimes; or
- continuous instruction history from USDC creation to the segment start.

Without one of these inputs:

- explicitly identified instruction events and their deltas remain exact;
- an unchecked transfer between two unknown accounts can be missed;
- the start of a discovered lifetime can be unknown;
- absolute opening and resulting balances are unknown;
- the segment must have `partial` coverage.

The ledger does not use token-balance observations to fill this gap. It must
report the gap.

## Token-2022 limit

The first exact implementation is for classic USDC only.

Token-2022 can apply fees, withheld amounts, confidential changes, and other
extension effects. Some effects depend on account or mint state that is not in
the core transfer instruction. Do not emit a symmetric two-leg delta for such
an instruction unless its complete effect has an implemented and tested rule.

A later Token-2022 processor can use the same event records. It must mark each
unsupported target effect as indeterminate.

## SQLite store

The SQLite store keeps:

- immutable dump and source identity;
- exact epoch-generation bindings;
- block-row checkpoints;
- one public-key table with integer IDs;
- account lifetimes;
- minimal transaction identity;
- ordered token events and their participant roles;
- delta legs;
- coverage issues.

It should not store:

- full messages;
- full metadata;
- complete transaction account lists;
- Base58 copies of every public key;
- pre-token or post-token balances.

The store binds one database to the full source identity, selected scan range,
target mint, Token program, and opening tracker state. It commits block facts,
account IDs, lifetimes, events, delta legs, coverage issues, tracker updates,
and the next block checkpoint in one SQLite transaction. It validates this
identity and the saved tracker state before resume. A restart does not make
duplicate or partial events.

It uses one public-key dictionary and SQLite `STRICT` tables. It stores every
`u64` as an eight-byte little-endian value plus canonical decimal text. It
stores the primary signature when the source supplies it. It stores no full
message, full metadata, or pre/post token balance.

The store has a read-only audit operation. The audit validates the bound run
specification, checkpoint, digest chain, tracker state, account lifetimes, and
public-key references before the comparison reads semantic rows.

The processor has public per-transaction limits for token input bytes,
expanded Token leaves, coverage issues, and account updates. The SQLite store
uses the same limits before it starts a block transaction.

## Three-format network demonstration

The implemented
[`archive-token-events`](../../examples/archive-token-events/README.md)
command reads CAR, Compact V2, and Indexer V3 from one public HTTPS Worker
origin. It does not use a local HTTP server. It uses these exact source trust
levels:

| Format | Exact SDK trust level |
|---|---|
| CAR | `operator-trusted` |
| Compact V2 | `published-manifest` |
| Indexer V3 | `internal-binding-only` |

The V3 scan explicitly accepts the weaker `internal-binding-only` source. The
database and report keep this trust level. Cross-format parity does not change
it.

The command accepts exactly these sample epochs: `0`, `100`, `200`, `300`,
`400`, `500`, `600`, `700`, `800`, `900`, and `1000`. One run can read at most
1,024 canonical block rows. This is a demo limit, not a ledger-format limit.

The output has one result folder for each archive and one shared archive-cache
root:

```text
<output-root>/archive-cache/origin-.../compact-v2/
<output-root>/archive-cache/origin-.../indexer-v3/
<output-root>/car/epoch-N/
<output-root>/compact-v2/epoch-N/
<output-root>/indexer-v3/epoch-N/
<output-root>/comparison/epoch-N/
```

Each archive folder contains its own SQLite database and JSON report. Compact
V2 and Indexer V3 use separate cache trees under `archive-cache`.

Epoch 0 is only a structural network example. The current epoch-0 Compact V2
and Indexer V3 samples have limited metadata, and USDC is absent from this
range. An empty epoch-0 event ledger is not a throughput result and is not a
semantic-completeness result.

## Validation

For a completed run, the comparison audits all three databases in read-only
mode. It merge-compares full token-event, coverage, tracker, and ledger-control
rows. It resolves database-local key IDs to raw 32-byte public keys before the
comparison. The database also keeps one SHA-256 digest for each complete
canonical `BlockView`. The comparison checks these digests, but it does not
retain a second full source projection. Full-row source-projection parity is
therefore `not-proved-full-row`.

The validation set must prove:

- CAR, Compact V2, and Indexer V3 make the same canonical event records;
- one worker and many workers make the same records;
- a resumed run and a one-shot run make the same records;
- each applied transfer has exactly two matching legs;
- each applied mint or burn has exactly one matching leg;
- failed and configuration events have no delta;
- account lifetimes do not overlap;
- close and address reuse do not leak target identity;
- outer, CPI, stack-height, and batch order stays exact;
- unsupported target effects make coverage incomplete.
