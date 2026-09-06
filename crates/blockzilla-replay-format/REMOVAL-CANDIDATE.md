# Removal candidate: does Archive V3 already cover this?

This crate is extracted rather than deleted, but it should not be treated as
settled. It overlaps substantially with the Archive V3 ledger plane, and its
own payload format was never enabled.

## What it is

Replay Projection V1: a zero-copy codec for one produced slot's replay payload.

```
ReplaySlotEventV1 = EntryBatch { entry_count }
                  | Entry { num_hashes, signature_mixin, transaction_count }
                  | Transaction(ReplayTransactionV1)
                  | BlockMarker(bytes)
```

with `ReplayMessageV1` (Legacy and V0), `ReplayInstructionV1`,
`ReplayAddressTableLookupV1`, `ReplayAddressRefV1`, `RecentBlockhashRefV1`,
`StatusKeyClassRefV1`, 22 bound constants, and a single-pass validating
decoder that refuses trailing bytes.

It carries the transactions and entries to be executed. It does **not** carry
execution output: there are no account deltas, lamport or balance types. Its
design record states that Replay V1 never carries multiple Bank states.

## Why it is a removal candidate

**1. Archive V3 models the same ground, and more.**

| | this crate (2,592 lines) | V3 ledger + poh (1,243 lines) |
| --- | --- | --- |
| transactions | `ReplayTransactionV1` | `TransactionBlock`, `TransactionBlockHeader` |
| messages | `ReplayMessageV1` Legacy/V0 | `MessageHeader`, `HashOwner` |
| instructions | `ReplayInstructionV1`, ATLs | `MAX_INSTRUCTIONS_PER_TRANSACTION`, `MAX_ACCOUNTS_PER_INSTRUCTION`, `MAX_INSTRUCTION_DATA_LEN` |
| PoH entries | `Entry { num_hashes, signature_mixin }` | `CurrentPohEntry`, `LegacyPohEntry`, `PohWireProfile` |
| execution effects | none | `EffectKind`, `CpiState`, `EffectState`, `EffectFileIndex` |

V3 covers the replay input and adds the effects plane this crate does not have.

**2. Its format slot was never turned on.** The module has always said:

> Payload format 8 remains reserved and must not be enabled merely because
> this codec exists.

So this is a complete design that Archive V3 appears to have overtaken before
it shipped, not code that decayed.

**3. Almost none of it is used.** Of 52 public symbols, four are referenced
outside the crate:

- `MAX_REPLAY_NUM_HASHES_PER_ENTRY`
- `ReplaySignatureMixinBuilder`
- `derive_replay_entry_hash`
- `push_signature`

All four are the PoH entry-hash slice, consumed by
`blockzilla-read-sdk/src/archive_integrity.rs` and one test in
`blockzilla-dump`. Nothing uses the slot, transaction, message or instruction
types the other 48 symbols define.

## Why it was extracted anyway

It has zero workspace dependencies, so it isolates cleanly and costs nothing
to keep as its own crate. It also has a written design record
(`docs/design/blockzilla-replay-projection-v1.md`) covering a real trust
boundary: what removing outer-transaction signatures does to consensus account
state and to Bank and account hashes. Deleting the code without first deciding
whether V3 answers that question would discard the reasoning with it.

## To settle this

1. Determine whether the V3 ledger plane can carry everything Replay
   Projection V1 was reserved for, including the signature-stripping trust
   boundary in its design record.
2. If it can, move the PoH entry-hash slice to where its only real consumer
   lives — the V2 reader's integrity check — or into a small PoH crate shared
   with the V3 `sidecars/poh` reader, and delete the rest.
3. If it cannot, record what V3 is missing, and either extend V3 or state
   plainly why format 8 stays reserved.

Do not silently keep both. Two formats describing the same slot, one of them
never enabled, is the condition this note exists to prevent.
