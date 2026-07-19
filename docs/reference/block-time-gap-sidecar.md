# Block-time gap sidecar

`block-time-gaps.bin` is a sparse, versioned record of slot and whole-second
time discontinuities observed in one local archive source. It is designed for
Blockzilla monitoring and historical analysis without an RPC or `getBlock`
dependency.

A row means that two consecutive archived blocks have either one or more slot
numbers between them, an available time delta above the header threshold, or a
decreasing stored time. A slot gap does **not** by itself prove a Solana outage:
the source archive could also be incomplete. Consumers should describe rows as
archive or block-production observations until another signal confirms the
cause.

## Version 1

All integers are little-endian. The file is one 160-byte header followed by
`gap_count` fixed-width 40-byte rows. Extra, truncated, non-canonical, or
reserved data is invalid.

### Header

| Offset | Type | Field |
| ---: | --- | --- |
| 0 | `[u8; 8]` | Magic `BZBTGAP1` |
| 8 | `u16` | Version, currently `1` |
| 10 | `u16` | Header length, `160` |
| 12 | `u16` | Row length, `40` |
| 14 | `u16` | Flags, currently `0` |
| 16 | `u64` | Epoch |
| 24 | `u64` | Slots per epoch |
| 32 | `u32` | Source kind: `1` blockhash index V3, `2` CAR or CAR.ZST |
| 36 | `u32` | Positive time-gap threshold in seconds |
| 40 | `u64` | Exact source byte length |
| 48 | `[u8; 32]` | SHA-256 of the exact source bytes |
| 80 | `u64` | Archived block count |
| 88 | `u64` | Slot/time gap row count |
| 96 | `u64` | Total absent slots between archived blocks |
| 104 | `u64` | First archived slot |
| 112 | `i64` | First archived block time |
| 120 | `u64` | Last archived slot |
| 128 | `i64` | Last archived block time |
| 136 | `u64` | Gaps with usable, non-decreasing times at both endpoints |
| 144 | `u64` | Gaps with at least one missing endpoint time |
| 152 | `u64` | Gaps whose available endpoint time decreased |

Block times are Unix seconds. `i64::MIN` is the sidecar's missing-time sentinel;
the blockhash index V3 source value `0` is converted to that sentinel.
For source kinds `1` and `2`, `source_bytes` and `source_sha256` cover the exact
bytes of that single local file, including compression when the CAR is a ZSTD
file. Other source-kind values are invalid in version 1.

### Gap row

| Offset | Type | Field |
| ---: | --- | --- |
| 0 | `u64` | Previous archived slot |
| 8 | `u64` | Next archived slot |
| 16 | `i64` | Previous block time |
| 24 | `i64` | Next block time |
| 32 | `u32` | Row flags |
| 36 | `u32` | Reserved, `0` |

The next slot must be greater than the previous slot. The number of absent slots
is `next_slot - previous_slot - 1`; it is zero for a time-only row. A
consecutive-slot row is canonical only when both times exist and their positive
delta exceeds the header threshold, or when the next time decreases.

Row flag bits are:

- `1 << 0`: previous time is missing
- `1 << 1`: next time is missing
- `1 << 2`: both times exist, but the next time decreased

The flags are derived from the stored times and must match them exactly. Equal
whole-second times are valid and have an elapsed value of zero.

## Coverage and epoch boundaries

The header preserves the first and last archived block even when neither is
part of a gap row. This lets a consumer compare adjacent epoch sidecars without
inventing an epoch-start or epoch-end block. A single sidecar only claims
coverage between its own first and last observed blocks.

The Blockzilla builder records the source fingerprint, publishes through a
temporary file, syncs it, then atomically renames it. Readers validate counts,
ordering, flags, and exact file length before using the data; deployments can
compare the recorded fingerprint with the source when revalidation is required.

Complete normal, first-seen, and PreHot Archive V2 compactions emit the V3
timestamp index and this sidecar during the compactor's existing source pass.
The explicit `build-blockhash-registry` command does the same. Partial
`--max-blocks` runs do not publish either artifact because they do not represent
a complete epoch.

New deferred first-seen scan markers and manifests record that both timestamp
artifacts are required before metadata finalization. Pre-upgrade deferred scans,
whose marker and manifest omit that contract, remain finalizable without a
second full CAR scan; this compatibility applies only to those already-written
legacy candidates.
