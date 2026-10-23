# Alpenglow consensus facts

Date: 23 October 2026. Profile: `native-consensus-fact-v1`.

The first release adds an offline writer, a strict Rust reader, and a shared
Rust/Wasm reader for JavaScript. It stores retained native footer evidence in
separate immutable files. It does not change a published execution payload or
any existing archive. It is not a production deployment or capture recovery.

## Protocol basis

Votor is active on mainnet. The migration genesis block is slot 459652999;
Alpenglow blocks start at 459653000. Rotor has a separate later rollout. Use
`getAgGenesisCert` and source evidence to identify the migration. A date or an
Agave version alone does not prove that a captured block used Alpenglow. See
the [official migration guide](https://solana.com/upgrades/alpenglow).

The installed plugin already records exact native `VersionedBlockFooter`
Wincode bytes. The schema in this release admits the deployed Agave source
`87167f3906bdf5111e99ead64d8f4098b7874e33` and upstream Agave 4.3.2 commit
`a203300045b405f86c9c84308b2eae76af69ac3b`. Their footer types match.
See the pinned [footer types](https://github.com/anza-xyz/agave/blob/a203300045b405f86c9c84308b2eae76af69ac3b/entry/src/block_component.rs#L238-L361)
and [reward certificate types](https://github.com/anza-xyz/agave/blob/a203300045b405f86c9c84308b2eae76af69ac3b/votor-messages/src/reward_certificate.rs#L23-L88).

## Separate identities and evidence

| Value | Meaning |
| --- | --- |
| Source four-tuple and `(slot, bank_id)` | One node-local bank in one capture session |
| Execution blockhash and parent blockhash | Last-entry hashes used by the existing execution payload and expiry logic |
| Footer bank hash | Producer's expected execution state hash |
| Consensus `BlockId` | A distinct consensus identity; a certificate carries the identity of its own target |
| Execution payload SHA256 | Digest of the exact execution frame bytes that this fact supplements |
| Source event sequence and SHA256 | Position and digest of the exact captured JSON event, including its callback envelope |

A footer does not contain its carrier's consensus `BlockId` or parent
`BlockId`. Readers report `carrier_consensus_block_id: null`. They must not
substitute the execution blockhash, bank hash, or a certificate target.
The certificate target can be older than its carrying bank. The native
[block ID rule](https://github.com/anza-xyz/agave/blob/a203300045b405f86c9c84308b2eae76af69ac3b/ledger/src/blockstore.rs#L3653-L3689)
also differs between Tower and Alpenglow.

The old captured `ClearBank.parent_blockhash` field contains the consensus
parent `BlockId` from `UpdateParent`. A later event version must give it an
accurate type and name. Do not reinterpret or rewrite historical event bytes.

Each fact retains the raw native footer once. Readers derive these fields:

- Bank hash, exact producer Unix nanoseconds, and raw user-agent bytes.
- Optional block finalization certificate with its own slot and consensus
  `BlockId`, final aggregate, and optional notarization aggregate.
- Optional skip reward and notarization reward aggregates as separate types.

All signatures and bitmaps remain exact captured bytes. The verification
state is always `captured_not_verified`. A reward aggregate does not establish
finality or a skipped slot. This release does not verify BLS signatures, stake
thresholds, epoch ranks, bitmap meaning, or producer time bounds. JavaScript
receives nanoseconds and certificate target slots as decimal strings. It can
use `BigInt` for exact arithmetic.

## Wire contract

The portable header is 20 bytes, in this order:

| Field | Encoding |
| --- | --- |
| Magic | Eight bytes: `BZCNF001` |
| Portable version | Little-endian u16, value 1 |
| Native grammar code | Little-endian u16, value 1 |
| Binding JSON byte length | Little-endian u32, 1 to 4096 |
| Native footer byte length | Little-endian u32, 1 to 65535 |

The binding JSON and exact native footer follow the header. The binding has
`source`, `bank`, `execution_payload_sha256`, `source_event_sequence`, and
`source_event_sha256`. Source and bank use the existing strict native bank v2
identities. The JSON accepts no unknown fields. The footer contains only native
`VersionedBlockFooter::V1`, without the outer block marker or length prefix.

The reader rejects an unsupported profile version, schema, source revision,
invalid option tag, truncated value, noncanonical reward bitmap length,
oversized field, or trailing bytes. Finality bitmaps have a fixed u16 length;
reward bitmaps use ShortU16. These encodings are not interchangeable.
The footer limit follows the native marker's enclosing u16 byte length.

`decode_bound` checks the full binding supplied by the caller. A consumer must
also check the complete fact digest from a trusted export receipt. A digest
proves byte identity; it does not independently prove the source's claims.

## Writer and reader

The direct Agave CLI provides four commands:

```sh
hivezilla-direct-agave export-consensus binding.json footer-event.json execution-frame.bin export-dir
hivezilla-direct-agave export-consensus-source-events execution-binding.json source-events.json.zst execution-frame.bin export-dir
hivezilla-direct-agave inspect-consensus-export export-dir
hivezilla-direct-agave inspect-consensus-fact fact.bin binding.json
```

The single-event command accepts an exact `Event::Footer` JSON value or one
`RawEvidence` record. Its binding includes the exact event sequence and hash.
The source-events command accepts the retained per-bank JSON array or its
Zstandard form. Its execution binding contains only `source`, `bank`, and
`execution_payload_sha256`; each footer record supplies its own event binding.
Export accepts native bank v2 block frames. Other execution profiles fail
admission. Source-events input is bounded to 256 MiB, both stored and decoded.
Each export has at most 1024 facts and 1 MiB of raw footer bytes in total.
The writer checks input bounds and exact hashes before it writes a fact.
It rejects a source, bank, payload, or event mismatch. It creates immutable
fact files and an export receipt. An equal retry can reuse equal bytes; it
cannot replace conflicting output.

The JavaScript entry point is `@blockzilla/stream-transport/consensus`:

```js
import { loadConsensusDecoder, decodeConsensusFact } from './consensus.mjs';
const decoder = await loadConsensusDecoder();
const result = await decodeConsensusFact(factBytes, receiptDigest, expectedBinding, decoder);
const timeNanos = BigInt(result.decoded.footer.block_producer_time_nanos);
```

The SDK uses the same Rust parser as the native tools. It does not implement a
second native schema in JavaScript or change the bank client's root state.

An export receipt grants **no source retirement authority**. Existing compact
markers, custody records, storage ledgers, caps, and archives remain separate.
The exporter can read retained evidence; it cannot claim that previously
retired events are available or that an entire epoch has footer coverage.

## Legacy field policy

For new Alpenglow consensus records, omit Tower lockout depths, the old
64-tick timing model, and a separate confirmed-to-finalized delay. Do not copy
execution transactions or old PoH timing fields into each consensus fact.
The new profile contains only the source binding and exact footer evidence.

Keep transaction entries, counts, hashes, source gaps, execution parent links,
block height, expiry, rewards, and fork invalidation. Agave 4.3.2 still requires
a terminal empty Alpentick with `num_hashes == 1`. Footer arrival precedes that
entry callback. It does not prove block completeness. The current execution
format's `available-poh.wincode` name stays unchanged to preserve its published
contract; its retained entry records are not an independent PoH timing proof.
See the [terminal entry rule](https://github.com/anza-xyz/agave/blob/a203300045b405f86c9c84308b2eae76af69ac3b/runtime/src/block_component_processor.rs#L364-L390)
and [callback ordering regression](https://github.com/anza-xyz/agave/blob/a203300045b405f86c9c84308b2eae76af69ac3b/ledger/src/blockstore_processor.rs#L5814-L5893).

Historical Tower readers and existing immutable archives remain supported.
Do not discard a real transaction because its program or `is_vote` flag looks
legacy. Do not infer Alpenglow from a missing footer.

## Delivery and production plan

1. **This release:** strict footer fact codec, offline immutable export and
   inspection, shared Rust/Wasm JavaScript reader, tests and retained native
   fixture checks. Existing execution v1/v2 byte contracts stay separate.
2. **Continuous evidence release:** give footer export its own durable source
   cursor, conflict detection, inventory, cloud upload/readback proof, and
   retention acknowledgement. Test a footer that arrives after the first rooted
   custody receipt and a crash at each commit boundary. The current first
   `CUSTODY.json` is immutable and does not cover later callbacks. A journal-only
   cursor must not skip retired prefixes or claim full historical coverage.
3. **Live format release:** negotiate a new profile with the Rust relay, Wasm
   reader, JavaScript SDK, publisher, and archive catalog. Reuse execution
   payloads and send small fact references. Give entry evidence an accurate
   name in that new contract. Keep explicit unknown/unsupported states. Add
   consensus parent/header and carrier `BlockId` only from a proven native
   source; the current footer callback cannot supply them.
   Full Votor vote/skip-certificate capture needs another native source.
   Independent certificate verification also needs exact epoch ranks, stakes,
   registered BLS keys and proof-of-possession provenance, bitmap rules and
   signature domains. Keep it distinct from a node's confirmation callback.
4. **Controlled deployment:** repair the source/registry/writable-path handoff,
   validate free capacity over time, preserve the failed source and its gap,
   and start a new capture session. Then prove actual capture, compaction,
   publication, client decode, cloud readback, restart, and retirement progress.
   Only enable new source retirement after all required fact custody is proved.

The failed 22 October capture cannot be made complete by a worker restart.
Format tests do not recover that capture or establish current live block flow.
Production cutover needs the coordinated storage handoff described in the
[deployment record](../../hivezilla/deploy/direct-agave/README.md).

## Acceptance

The local targeted suites passed 104 tests: 21 portable codec tests, 41 native
library tests, 13 execution decoder tests and 29 JavaScript tests. Four explicit
helpers remain ignored in the normal runs: two require the real compact writer,
one exports decoder fixtures, and one is invoked by the relative-path test in
a subprocess. The WebAssembly release build passed. A real retained 556-byte
footer and an independent Wincode fixture test the native grammar. These tests
do not establish full historical coverage or production progress.

The CI job is configured to build the consensus WebAssembly reader from source
and test it with the v1/v2/consensus JavaScript suites. Read the
[acceptance receipt](../operations/evidence/alpenglow-format-20261023.json)
for artifact hashes, commands and limits.
