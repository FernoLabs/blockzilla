# Transaction v1 (SIMD-0385) in the archive

Date: 2026-08-18. Implemented, with one verification still open.

[SIMD-0385][simd] was accepted and merged on 2025-12-17, alongside
[SIMD-0296][simd296] which raises the transaction size limit. v1 transactions
are identified by a leading version byte of **129** (`0x81`), cap at **4096
bytes**, carry **no address lookup tables**, and move the compute budget out of
the instruction list and into the message header.

`solana-message 4.2.0` — already in this tree — implements v1 as
`VersionedMessage::V1(v1::Message)`. It is the specification of record for
everything below.

[simd]: https://github.com/solana-foundation/solana-improvement-documents/pull/385
[simd296]: https://github.com/solana-foundation/solana-improvement-documents/blob/main/proposals/0296-larger-transactions.md

---

## 1. The wire layout

After the `0x81` prefix:

```text
LegacyHeader          3 × u8
TransactionConfigMask u32, little-endian
LifetimeSpecifier     [u8; 32]  (blockhash)
NumInstructions       u8, max 64
NumAddresses          u8, max 64
                      ── the fixed 41-byte prefix ends here
Addresses             [u8; 32] × NumAddresses
ConfigValues          [u8; 4] × popcount(mask)
InstructionHeaders    (u8 program_id_index, u8 num_accounts, u16 data_len) × NumInstructions
InstructionPayloads   per instruction: account indices, then data
```

Three differences from v0 matter for decoding, and all three make v1 *easier*:

- the counts are plain `u8`, not ShortU16;
- the addresses are one contiguous run, so they borrow without a per-element
  length prefix;
- the instruction headers are separated from their payloads, which is closer to
  this format's own columnar layout than v0 ever was.

### The config mask

| bits | field | width |
|---|---|---:|
| 0–1 | `priority_fee` | 8 B (`u64`) |
| 2 | `compute_unit_limit` | 4 B |
| 3 | `loaded_accounts_data_size_limit` | 4 B |
| 4 | `heap_size` | 4 B |

The value array is counted in **four-byte slots**, which is why the priority fee
takes two bits: the set-bit count is exactly the number of slots that follow.
Two shapes are invalid and must be rejected — any bit above 4, and one priority
fee bit without the other.

**Presence is load-bearing.** Which fields are set *is* the mask, so `Some(0)`
and `None` are different messages that hash differently. Nothing in the pipeline
may normalise an absent field to zero.

---

## 2. What was built

### Decode: `crates/old-faithful/car-reader/src/versioned_transaction.rs`

`V1Message<'a>` and `V1TransactionConfig`, wired as the `1 =>` arm of the
existing zero-copy `SchemaRead`. It borrows the account keys and blockhash
exactly as v0 does.

Validation is fail-closed: ≤64 instructions, ≤64 addresses, unknown mask bits
rejected, partial priority-fee bits rejected.

Worth recording: **v1 CARs already failed loudly before this work**, on
`_ => Err(invalid_tag_encoding(1))`. No CAR was ever silently mis-parsed. That
was the first thing worth ruling out and it held.

### Compact V2: four type families

| family | variant |
|---|---|
| `CompactMessage<'a>` | `V1(CompactV1Message<'a>)` |
| `OwnedCompactMessage` | `V1(OwnedCompactV1Message)` |
| `WincodeArchiveV2NoRegistryMessage` | `V1(WincodeArchiveV2NoRegistryV1Message)` |
| `ArchiveV2HotMessagePayload` | `V1(ArchiveV2HotV1Message)` |

Plus `CompactMessageVersion::V1`. Each carries `CompactTransactionConfig`; none
carries `address_table_lookups`.

**Every variant is appended**, so `Legacy` and `V0` keep wincode tags 0 and 1 and
existing generations decode unchanged. This is the constraint to preserve if
further versions arrive: **append, never reorder.** The golden tests in
`blockzilla-format` are what catch a violation.

### Signature verification

`SignedMessageVersion::V1 { config }` and `serialize_signed_v1_message` in
`crates/blockzilla-index-archive-convert/src/source_v2.rs`. v1 is not
legacy-with-extras, so it gets its own serialization pass rather than being
interleaved into the existing one.

The mask is derived from which config fields are `Some`, which is what keeps the
original header bytes reconstructible — and therefore keeps §7.6's
signature-as-byte-oracle argument working for v1.

### Protobuf

Matched to [solana-rpc/superbank#75][sb] verbatim, verified against the raw diff
rather than a summary, because field numbers are permanent once data is written:

```proto
optional TransactionConfig config = 7;

message TransactionConfig {
  optional uint64 priority_fee = 1;
  optional uint32 compute_unit_limit = 2;
  optional uint32 loaded_accounts_data_size_limit = 3;
  optional uint32 heap_size = 4;
}
```

`bool versioned = 5` is unchanged. The ecosystem's answer to "a bool cannot
distinguish v0 from v1" is that it does not need to: **the presence of `config`
is the v1 signal.** Our `confirmed_block.proto` was field-for-field identical to
their pre-PR state with field 7 free, so this keeps us wire-compatible with
old-faithful consumers.

That PR also adds `DeactivatedStake = 5` to `RewardType`, which we have **not**
adopted. If Agave 4.2 emits that reward kind, we would mis-decode it. Worth
closing separately.

[sb]: https://github.com/solana-rpc/superbank/pull/75

---

## 3. Three traps, recorded so they are not re-discovered

**The SDK's serde path is not the wire format.** `solana-message`'s
`impl Serialize for VersionedMessage` carries its own comment on the V1 arm:
*"Note that this format does not match the wire format per SIMD-0385."* Reaching
for bincode as the oracle — the pattern that worked for the vote decoder — would
have produced wrong bytes for every v1 signature, and tests built against the
same wrong serializer would have passed while production rejected real blocks.

**The SDK's canonical serializer is unreachable from this workspace.** The only
correct v1 serialization is behind `#[cfg(feature = "wincode")]`, and
`solana-message` pins `wincode ^0.5.0` while this workspace is on 0.6.1.
Attempting it as a *dev-dependency* fails too: the feature pulls
`solana-short-vec/wincode`, cargo unifies short-vec to the workspace's 3.3.0
(0.6-based), and the traits mismatch —
`ShortU16: SeqLen<__WincodeConfig> is not satisfied`. There is no dependency tier
at which the SDK oracle works here.

**quick-protobuf has no proto3 presence tracking.** It flattens `optional`
scalars to plain values, so on that renderer an unset config field is
indistinguishable from zero. The prost path and the archive both keep the
`Option`, which is where presence matters, but the fast get-block renderer
cannot round-trip the distinction.

---

## 4. The wincode consolidation that this depends on

Separate finding from the same pass, recorded because it **contradicts §7.7 of
the design of record**, which still says otherwise.

§7.7 freezes Compact V2 at exact wincode `0.5.5` on the assumption that 0.6
would move the bytes. It does not, for the shapes Compact V2 uses. The whole
workspace now runs a single `=0.6.1`, and
`legacy_payload_has_exact_golden_bytes_and_hash` — which asserts exact Compact V2
bytes *and* their hash — passes unchanged.

What unblocked it was `solana-short-vec` **3.2.1 → 3.3.0**, which moves from
`wincode ^0.5.0` to `^0.6.0`. The earlier belief that Compact V2's schemas were
incompatible with 0.6 was wrong: the compile error was version *skew* across a
crate boundary (`StoredTransactionError` is defined in car-reader), not an API
incompatibility.

The supporting evidence is that `blockzilla-format` uses no `BitVec`, and
`BitVec` canonical encoding becoming opt-in was 0.6.0's one wire-affecting
change.

### Confirmed on real fixtures, 2026-08-18

The golden vector only proved the shapes inside it, so the same converter was
built at both wincode versions and run over the retained CAR fixtures. Nothing
else was toggled but `wincode` and the `solana-short-vec` pin it forces.

| | |
|---|---:|
| fixtures | `epoch-157-biggest.car`, `epoch-822-biggest.car` |
| transactions | 4,208 + 2,969 = **7,177** |
| output files compared | **32** |
| files differing | **0** |

Byte-identical across every output object — `archive-v2-blocks.zstd`,
`registry.bin`, `registry_counts.bin`, `registry.mphf`, `poh.wincode`,
`shredding.wincode`, `signatures.bin`, `vote_hash_registry.bin`, the block and
access indexes, and the meta. The two fixtures are 665 epochs apart, so they
span very different transaction mixes (epoch 157 predates CPI and vote
compaction; epoch 822 is 34.5% vote instructions by count).

**What this still does not prove:** two blocks are not 1013 epochs. The
strongest remaining gap is any shape absent from both fixtures. It is now a
narrow gap rather than an open question.

---

## 5. Open, in the order I would close them

1. **Verify the v1 canonical bytes against a real transaction.** The golden
   vector proves our reading of SIMD-0385, not agreement with Agave. One real v1
   transaction — mainnet or a test validator — whose stored signature verifies
   against these bytes settles it. There is no urgency while mainnet is still
   legacy/v0, and no cheaper substitute given §3.

2. ~~Verify the wincode 0.6.1 claim on real fixtures.~~ **Done 2026-08-18** —
   7,177 transactions, 32 output files, zero differences. See §4. What remains
   of it is narrow: car-reader's `decode_bincode_2` path for epoch-156-era
   metadata is not exercised by either fixture.

3. **Decide the RPC shape for v1's config.** The JSON renderer emits v1's
   message fields but not its header config, because SIMD-0385's RPC
   representation is not settled. Marked in-code.

4. **`DeactivatedStake = 5`** on `RewardType`, per §2.

---

## 6. Consequences for the rest of the format

**§6.5's lookup-table rebuilder does not apply to v1 at all.** v1 has no address
lookup tables, so the three rules that make a naive rebuild wrong — same-slot
extends, deactivation cooldown, `(address, incarnation)` keying — are simply not
reachable for v1 transactions.

**The instruction-data dedup arithmetic shifts as v1 adoption grows.**
`measurements/instruction-data-compression.md` measured ComputeBudget at
**40.8% of instruction count** (6.6% of bytes). In v1 those cease to exist as
instructions, so both the count distribution and the per-program table in §3 of
that document describe a mix that will drift. The byte-dominant finding — Vote at
56.5% of instruction bytes with 25 distinct payloads — is unaffected.
