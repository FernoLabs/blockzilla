# Raw shred capture: Hetzner to NAS

Status: **current operational path; not the proposed HiveSync V1 protocol**.
It intentionally documents the implemented stop-and-wait NAS flow. The target
live-first, permanent-raw-dataset contract is defined in the
[Hivezilla record and sync specification](../design/hivezilla-record-and-sync-protocol.md).

This rollout records and durably replicates shreds only. It does not reconstruct blocks, compact
records, or publish archives. After the NAS has fsynced and signed a cumulative ACK, Hetzner may
retire older sealed spool segments while retaining the exact ACK segment as a restart anchor.

```text
Solana gossip / TVU
        |
        v
Hetzner shred reader -> Hivezilla `record-shred-udp` -> /data/shred-ingest
                                                        |
                                  mTLS pull, opened by NAS |
                                                        v
                                              NAS durable receiver spool
```

The Hetzner recorder writes one independently zstd-compressed Solana shred per spool record. It
drains UDP into bounded groups of at most 512 records over at most 5 ms, then crosses one
`sync_data` boundary for the group. `recorder.json` advances only after that group is durable. This
keeps crash recovery and NAS replay exact without paying one disk flush per UDP packet. The NAS
cannot request an arbitrary cursor. The source selects its oldest unacknowledged durable records,
and repeats the same batch until the NAS has fsynced its copy and returned a signed cumulative ACK.

An ACK first advances the Hetzner replay cursor stored in its fsynced ACK WAL. Only after that
durability boundary may one older sealed source segment be retired. The active segment, the
segment containing the ACK, and every unacknowledged record remain on Hetzner.

## Deployment inputs

Prepare the two examples as private regular files, never as symlinks:

- Hetzner: [raw-shred-pull-source.example.json](../../services/hivezilla/config/raw-shred-pull-source.example.json)
- NAS client: [raw-shred-pull-client.example.json](../../services/hivezilla/config/raw-shred-pull-client.example.json)

The Hetzner Dokploy Compose file is
[services/hivezilla/docker-compose.hivezilla-shred.dokploy.yml](../../services/hivezilla/docker-compose.hivezilla-shred.dokploy.yml).
It now runs the pull listener instead of the old local-only `bridge-shred-spool` copy. Before
deploying it, provision its private `deploy/hivezilla-shred/{tls,replication,receipts,secrets}`
mounts and set `SHRED_JOURNAL_ID` to the active journal ID. The small container entrypoint renders
that non-secret ID into a private temporary runtime config; keys remain mounted files.

The TLS layout needs two independent mTLS relationships:

1. NAS client certificate accepted by Hetzner's pull listener.
2. NAS pull client certificate accepted by the local NAS durable receiver.

The receiver must permit the exact stream `(solana-mainnet, hivezilla-shred-01,
shred-reader-loopback, SHRED_JOURNAL_ID)` and use a `4 KiB` record limit. The same NAS receipt
public key is placed on Hetzner so it can authenticate the ACK before recording it.

## Startup order

1. Start `record-shred-udp` on Hetzner and wait for `/status/recorder.json` to report a non-null
   `durable_through_sequence`.
2. Start `serve-shred-spool-pull-source --config /etc/hivezilla/raw-shred-pull.json` on Hetzner.
   It listens on `18443` in the example. Allow this port only from the NAS egress IP.
3. Start the NAS durable receiver first, then run
   `pull-grpc-raw --config /etc/hivezilla/raw-shred-pull-client.json --protocol v2` on the NAS.
4. Confirm the Hetzner ACK status advances and the NAS spool sequence matches it.

Deploy the prefix-marker-aware recorder and pull source as one compatibility unit. Once a durable
ACK covers a later segment, startup maintenance drains all older sealed segments before serving;
caught-up sessions continue bounded maintenance. Never roll the recorder back to a binary that
requires segment zero after the first prefix marker is published.

## Failure behavior

- Hetzner disconnect: the NAS reconnects and the source replays the unacknowledged exact batch.
- NAS disk / receiver failure: no valid ACK is returned, so Hetzner retains the source spool.
- Hetzner restart: the local ACK WAL finds the next source sequence; replay remains at-least-once.
- Gossip gaps: this system cannot synthesize missing shreds. The recorder's accepted count and
  freshness remain separate health signals.

### Missing-shred repair boundary

The Dokploy reader enables bounded live repair by default. It runs behind an isolated supervisor,
uses authenticated gossip repair peers, correlates nonces and retry deadlines, and accepts only
data shreds whose request identity, shred version, scheduled-leader signature, Merkle identity, and
recorded chained-root trust path all validate. Transient failures restart only the repair worker;
raw TVU receive and forwarding remain live and define `/readyz`.

Accepted repair shreds are fsynced by a dedicated blocking writer into the separate segmented
provenance WAL rooted at `REPAIR_WAL_PATH`. They are never appended to the replicated raw source
WAL, never advance the NAS acknowledgement cursor, and never change raw retention decisions. The
repair WAL has retained-byte and filesystem-reserve admission limits; reaching either stops repair
without consuming space reserved for raw capture.

The release auditor can opt in to this evidence with both `--repair-wal` and an independently
frozen inclusive `--repair-durable-through-sequence`. It revalidates every accepted record and
reports raw-only and raw-plus-repair reconstruction separately. That diagnostic merge is not a
promotion into the archive, and no repair segment may be deleted until a durable consumer ACK and
retirement protocol exists.

Treat the shred-reader's `/readyz` as ready only when it has both recent gossip peers and a valid,
matching-version shred within 60 seconds, at least one forwarding target, a recent successful
forward, and no forwarding error in the last 15 seconds. The reader's bounded forwarding queue is
independent from its observation-only deduplication: every valid Turbine copy is eligible for
forwarding because UDP send success is not a durable recorder acknowledgement. To check the
handoff itself, compare
`forwarded_datagrams_total` with recorder `accepted_total` across recorder-status timestamps; UDP
send success alone is not a durable receipt.

## Verified reconstruction diagnostic

Block reconstruction is an offline diagnostic feature, not part of the small live ingest binary.
Build it explicitly with `--features shred-reconstruction --bin shred-reconstruct-trial`.

On 2026-07-22, the final ordered-component reader scanned a 500,000-record NAS sample from journal
`d574db1d3e3faab86f06f08a2ce33cfd`. From 256 candidate slots it reconstructed 187 exact,
parity-checked component streams. The first success was slot `434481177`: data shreds `0..767`,
1,189 entries, and 1,131 transactions. This proved the captured stream can yield complete blocks
while retaining any block markers in their original component order.

A fixed read-only pass reached that journal's then-durable tail at sequence `13,777,721` after
scanning 13,777,722 records. Before recovery, its newest 256-slot window contained 108 slots with
a known completion index and enough local FEC, 50 completed slots with at least one FEC set below
threshold, and 98 slots whose completion shred was not directly observed. No fork-identity,
adjacent-chain, geometry, or data-after-completion conflict was detected.

After local FEC recovery and Agave-compatible component decoding, the same frozen window produced
173 provisional blocks (67.6%). Those were exactly the 173 slots whose observed FEC sets met the
Reed-Solomon threshold. The other 83 slots (32.4%) contained 118 under-threshold FEC sets; their
combined minimum threshold deficit was 318 shreds. Of the 98 completion-unknown slots, 65 became
complete through local FEC and 33 also had a threshold deficit. The 83 primary post-recovery
failures were 78 missing-data ranges, three missing index zero, and two missing completion shreds.
Component decode, chained-root conflict, conflicting-duplicate, data-after-completion, and FEC
parity failures were all zero. Slot `434524390` reconstructed from data indices `0..895` into 1,394
entries and 1,352 transactions.

An earlier pass reported 126 reconstructed slots and 22 component decode failures from a different
candidate window. It used exact wincode consumption, while Agave Blockstore intentionally accepts
valid component padding. The fixed-cursor rerun above uses Agave-compatible decoding and reduces
component failures to zero; do not use the older 126/256 result as the current baseline.

The earlier zero-block result had two decoder defects:

- Solana Merkle proof entries occupy 20 wire bytes, not 32; the wrong size corrupted recovered FEC
  data.
- A slot contains independently serialized `BlockComponent` ranges ending at each
  `DATA_COMPLETE_SHRED`; concatenating a whole slot and permissively decoding once produced false
  partial results.

The trial now normalizes repair responses, verifies WAL metadata against each decoded shred,
rejects conflicting logical duplicates, checks coherent FEC identity, adjacent chained roots, and
parity, decodes every completed component using Agave's production-compatible wincode behavior,
and preserves the exact order of entry batches and block header/footer/update-parent markers. It
reports bounded failure samples by cause.
Its bounded candidate window follows the newest observed slots instead of permanently retaining
the first slots in a long scan. `fec_threshold_satisfied_slots` describes only observed FEC sets;
it must not generally be interpreted as a complete-slot count, even though it matched the complete
block count in this fixed window.
Its output remains provisional until leader-signature, fork, PoH, and rooted-slot validation are
part of the Blockzilla-scheduled Hivezilla compaction task.

### Exact epoch audit

`shred-epoch-audit` performs two read-only passes over one fixed durable journal prefix. The first
pass records every target slot's first/last observation and estimates the worst active
reconstruction buffer. The second retains a slot until its exact final observed record, applies
local FEC recovery, reconstructs every completed component, and then frees that slot. Both passes
must produce the same prefix fingerprint.

Always provide independently established `--coverage-start-slot` and `--coverage-end-slot` values.
With a complete finalized `getBlocks` manifest, the report keeps four facts separate:

- reconstructed candidate for a finalized produced slot;
- produced slot missed inside the asserted capture window;
- produced slot outside the asserted capture window and therefore never recorded;
- observed shred candidate for a slot absent from the finalized `getBlocks` set.

The equality `reconstructed + missed_capture + not_recorded = finalized produced slots` is enforced.
`getBlocks` membership proves slot coverage only: the rebuilt candidate is not proven canonical
until its final PoH hash is compared with the canonical blockhash. The caller must explicitly
assert that a plain manifest covers the whole declared range with finalized commitment.

An external `--after-*` anchor is allowed only with
`--assert-anchor-precedes-all-coverage-records`, after a separate boundary scan proved that no
target-range record exists before the anchor. Without an anchor, sequence zero is required so a
retired prefix cannot silently become missing input. `--max-resident-bytes` protects active shred
and reconstruction buffers; the persistent slot index, outcomes, canonical set, and streamed JSON
report need additional memory.

Repair merging is deliberately opt-in. The default remains raw Turbine plus local FEC recovery.
To include accepted repair shreds, freeze the receiver's inclusive durable repair sequence and add
both of these arguments:

```text
--repair-wal /path/to/accepted.repair.wal \
--repair-durable-through-sequence <inclusive-global-sequence>
```

`--repair-wal` accepts the legacy single file and automatically discovers its contiguous rolled
`*.segment-<20-digit-id>.repair.wal` siblings; a directory is also accepted when it contains
exactly one unsegmented base. Both audit passes verify the same bounded repair prefix: v2/v3
headers, per-frame CRC32, global sequence continuity, the v3 SHA-256 predecessor chain, and the
required `${base}.v3-seal` plus `${base}.v3-head` exact terminal checkpoint, followed by canonical
shred/request identity, leader signature, Merkle provenance, and the complete successor-anchor path.
Copy both v3 sidecars with every numbered segment; a file glob that omits them is not a complete v3
generation. Exact raw or repair repeats are counted and omitted. Any version, leader, duplicate,
FEC, fork, or anchor ambiguity becomes a named merge-conflict failure; the audit never chooses a
fork.
Use `--max-repair-records`, `--max-repair-payload-bytes`, and `--max-repair-segments` to lower or
raise the read-only resource bounds.

The epoch 1005/1006 boundary was independently inspected on the NAS:

- epoch 1005 ends at slot `434591999`, journal sequence `103843745`;
- that frame is at segment `632`, offset `38356447`, length `854`;
- epoch 1006 begins immediately at slot `434592000`, sequence `103843746`;
- no epoch-1006 record precedes that boundary and no epoch-1005 record follows it;
- slot `434592000` retained 475 shreds and every slot through `434592020` was observed.

Epoch 1005 has only three retained partial capture windows, so it cannot be described as a full
recorded epoch. The conservative complete-window assertions exclude each stopped journal's partial
edge slots: `434315062..434325837` (`f1b8c9ea…`), `434333188..434340458` (`69fb0784…`), and
`434480908..434591999` (`d574db1d…`). Epoch 1006 is the first journal positioned for an exact
slot-zero-to-epoch-end audit.

The finalized 2026-07-23 audit produced the following exact epoch-1005 baseline:

| Retained window | Finalized produced slots | Reconstructed | Missed inside window | No shreds observed |
| --- | ---: | ---: | ---: | ---: |
| `f1b8c9ea…` | 10,771 | 4,505 (41.83%) | 6,266 | 4,040 |
| `69fb0784…` | 7,269 | 5,059 (69.60%) | 2,210 | 0 |
| `d574db1d…` | 110,896 | 52,237 (47.10%) | 58,659 | 28,722 |
| Disjoint retained-window union | 128,936 | 61,801 (47.93%) | 67,135 | 32,762 |

Epoch 1005 contains 431,403 finalized produced slots. The retained windows cover 128,936 of them;
302,467 produced slots are outside all retained coverage. Consequently the retained shreds can
reconstruct 61,801 blocks, or 14.33% of the whole epoch. Inside retained coverage, 34,373 produced
slots had shreds but remained incomplete and 32,762 had no shred at all. The canonical missed-slot
causes are 30,097 `missing_data_shreds`, 1,216 `missing_index_zero`, 3,060
`missing_slot_completion`, and 32,762 `no_shreds_observed`.

Every journal's two passes reached its frozen durable tail and had an identical prefix SHA-256.
Thirty-one reconstructed blocks sampled across the three epoch-1005 windows and the early
epoch-1006 window had a final PoH hash exactly matching the finalized RPC blockhash; there were no
sample mismatches. This sampled identity check does not replace an exhaustive blockhash comparison.
