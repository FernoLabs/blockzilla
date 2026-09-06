# Replay Projection codec retirement

Status: implementation removed on 2026-09-06. Payload format **8 remains
reserved and unsupported**.

The removed `blockzilla-replay-format` package contained the proposed Replay
Projection V1 slot, transaction, message, and instruction codec. It had no
production encoder or decoder caller. It was not a registered Hivezilla input
format. Removing this unused implementation does not establish that Archive
V3 has the same semantics or resolves the same replay trust questions.

## Evidence and retained code

- [`hivezilla/protocol/src/record.rs`](../../hivezilla/protocol/src/record.rs)
  registers payload formats 1 through 7. `StreamHeaderV1::new` rejects any
  larger identifier. That contract is unchanged. Identifier 8 must not be
  reassigned or activated as part of this cleanup.
- The only external callers used PoH helpers: the Compact V2 integrity
  verifier and the dump verifier's fixture builder. No caller used the
  Replay slot codec or message types.
- The exact signature Merkle builder, entry-hash function, hash-count guard,
  and their two tests now live in
  [`blockzilla-compact-v2-reader::poh`](../../crates/compact-v2/blockzilla-compact-v2-reader/src/poh.rs).
  Hashing, odd-node duplication, limits, and diagnostic field labels are
  unchanged. Names no longer describe these helpers as a replay payload.
- Both callers use that module. Their manifests no longer depend on the
  removed package.

The removed implementation remains available in Git history before this
cleanup. The prior removal-candidate note has been replaced by this decision.

## Design questions remain open

The [Replay Projection V1 design](blockzilla-replay-projection-v1.md) is
retained. It defines requirements for signature removal, status evidence,
finality, checkpoints, runtime attachment, and publication. The related
[compaction job design](blockzilla-compaction-job-v1.md) is also retained.

Archive V3's ledger and effects planes overlap with parts of that proposal,
but overlap is not proof of equivalent data or trust contracts. Any future
replay implementation must resolve these requirements explicitly, select its
format contract, and obtain a separate protocol registration. This cleanup
changes neither the runtime experiment nor Archive V3's wire format.
