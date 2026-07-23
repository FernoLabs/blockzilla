# Fresh raw-shred reconstruction audit

This runbook measures one new 10,000-slot capture window after a shred-reader rollout. It freezes
two independently durable NAS cursors, fetches a finalized `getBlocks` membership manifest, and
runs `shred-epoch-audit` twice over the same immutable WAL prefix.

The procedure is read-only with respect to the live reader, Hetzner recorder, replication
services, NAS progress WAL, and raw-shred spool. It creates files only below a new audit-results
directory.

> [!IMPORTANT]
> The current `shred-epoch-audit` reads the replicated raw Turbine journal and performs local FEC
> recovery. It does **not** merge the separate repair-provenance WAL. This run therefore measures
> raw reception plus local Reed-Solomon recovery, not the final effectiveness of live repair.

## Safety model

The audit has four boundaries:

1. `A` is a checksum-verified, committed receiver-progress frame and its exact spool location.
2. After reading `A`, the operator reads a newer shred-reader status snapshot. The reader's
   `latest_slot` is a process-lifetime monotonic maximum (`AtomicU64::fetch_max`). The test begins
   128 slots beyond the greatest reader, recorder, and `A` slot. This establishes that no target
   record exists at or before `A` and justifies
   `--assert-anchor-precedes-all-coverage-records`.
3. `B` is frozen only after the reader, recorder, and NAS are at least 512 slots beyond the target
   end. This gives late Turbine copies time to enter the durable prefix.
4. `getBlocks` is fetched with finalized commitment after the finalized head passes the target
   end.

The audit then reads only `(A, B]`. Both passes must begin at `A + 1`, finish at `B`, and produce
the same prefix SHA-256.

Do not continue if the reader restarts, the journal ID changes, status becomes stale, replication
stops advancing, or the anchor segment disappears between `A` and the audit.

## Current NAS layout

These values were verified read-only on 2026-07-23. Recheck them before every run.

```text
spool root     /volume1/@home/ach/blockzilla-ingest-shreds/replication-receiver
cluster        solana-mainnet
origin         hivezilla-shred-01
source         shred-reader-loopback
journal        d574db1d3e3faab86f06f08a2ce33cfd
progress WAL   <journal>/receiver-progress.wal
audit binary   /volume1/@home/ach/dev/hivezilla-shred-pull/target/release/shred-epoch-audit
```

At inspection time the receiver and pull client had run continuously since July 21 and the NAS
had about 10 TB free. The audit binary SHA-256 was
`fd15704b01bea374a2e1b63008d0e48a2136f7b40d2be2117d1c951e7bfa27ba`; the run records and later
rechecks the actual hash instead of trusting this historical value.

## 1. Open an operator shell and set constants

Run the following in one Bash session on the NAS after the new Hetzner deployment is healthy:

```bash
set -euo pipefail
umask 077

ROOT=/volume1/@home/ach/blockzilla-ingest-shreds/replication-receiver
CLUSTER=solana-mainnet
ORIGIN=hivezilla-shred-01
SOURCE=shred-reader-loopback
JID=d574db1d3e3faab86f06f08a2ce33cfd
JDIR="$ROOT/$CLUSTER/$ORIGIN/$SOURCE/$JID"
PROGRESS="$JDIR/receiver-progress.wal"
AUDIT_BIN=/volume1/@home/ach/dev/hivezilla-shred-pull/target/release/shred-epoch-audit
STATUS_URL=https://hivezilla-shred-status-8dafe7-188-245-147-127.sslip.io/api/v1/sidecars/shred-ingest/status.json
RPC_URL=https://api.mainnet-beta.solana.com

# Set this to the immutable Dokploy deployment/revision identifier being measured.
ROLLOUT_ID=${ROLLOUT_ID:?export ROLLOUT_ID before starting the audit}
case "$ROLLOUT_ID" in
  (*[!A-Za-z0-9._-]*|'') printf 'invalid ROLLOUT_ID\n' >&2; exit 2 ;;
esac
RUN="$(date -u +%Y%m%dT%H%M%SZ)-$ROLLOUT_ID"
AUDIT_DIR="/volume1/@home/ach/dev/hivezilla-shred-pull/audits/fresh-raw-$RUN"
mkdir -m 700 "$AUDIT_DIR"

test -r "$PROGRESS"
test -x "$AUDIT_BIN"
test "$(findmnt -n -o TARGET --target "$JDIR")" = /volume1
df -h "$JDIR" "$AUDIT_DIR"
```

Creating `AUDIT_DIR` is the only write performed before the capture window. Never place it under
the spool root.

## 2. Define the read-only progress snapshot helper

This helper opens one progress-WAL descriptor, fixes its initial length, validates every complete
frame within that length with CRC32C and commit markers, checks stream identity and sequence
continuity, and returns the last committed cursor. It never opens the WAL for writing.

```bash
snapshot_progress() {
  python3 - "$PROGRESS" "$CLUSTER" "$ORIGIN" "$SOURCE" "$JID" <<'PY'
import json
import os
import struct
import sys
import time

path, cluster, origin, source, journal_hex = sys.argv[1:]
expected = (cluster, origin, source, list(bytes.fromhex(journal_hex)))

def crc32c(data):
    state = 0xffffffff
    for byte in data:
        state ^= byte
        for _ in range(8):
            state = (state >> 1) ^ (0x82f63b78 if state & 1 else 0)
    return (~state) & 0xffffffff

with open(path, "rb") as handle:
    observed = os.fstat(handle.fileno()).st_size
    if handle.read(8) != b"BZRPRG01":
        raise SystemExit("invalid receiver progress WAL magic")

    valid = 8
    latest = None
    previous = None
    frames = 0

    while valid + 14 <= observed:
        header = handle.read(14)
        if len(header) != 14:
            break

        magic, version, length, header_crc = struct.unpack("<4sHII", header)
        if magic != b"BZRP" or version != 1 or length > 1024 * 1024:
            raise SystemExit(f"invalid progress frame at {valid}")
        if valid + 22 + length > observed:
            break
        if crc32c(header[:10]) != header_crc:
            raise SystemExit(f"progress header CRC mismatch at {valid}")

        payload = handle.read(length)
        payload_crc, commit = struct.unpack("<I4s", handle.read(8))
        if crc32c(payload) != payload_crc or commit != b"CMIT":
            raise SystemExit(f"invalid progress frame trailer at {valid}")

        frame = json.loads(payload)
        stream = frame["stream"]
        actual = (
            stream["cluster_id"],
            stream["origin_node_id"],
            stream["source_id"],
            stream["journal_id"],
        )
        if actual != expected:
            raise SystemExit("progress stream identity mismatch")

        sequence = frame["offer"]["record"]["sequence"]
        if frame["durable_lsn"] != sequence + 1:
            raise SystemExit("sequence/durable-LSN mismatch")
        if previous is not None and sequence != previous + 1:
            raise SystemExit("non-contiguous progress sequence")

        previous = sequence
        latest = frame
        frames += 1
        valid += 22 + length

if latest is None:
    raise SystemExit("no committed progress frame")

key = latest["offer"]["logical_key"].get("Shred")
if key is None:
    raise SystemExit("latest durable record is not a shred")

print(json.dumps({
    "captured_unix_secs": int(time.time()),
    "through_sequence": latest["offer"]["record"]["sequence"],
    "durable_lsn": latest["durable_lsn"],
    "spool_location": latest["spool_location"],
    "logical_slot": key["slot"],
    "logical_kind": key["kind"],
    "logical_shred_index": key["shred_index"],
    "progress_frames_validated": frames,
    "progress_wal_observed_bytes": observed,
    "progress_wal_valid_bytes": valid,
    "progress_wal_unobserved_tail_bytes": observed - valid,
}, sort_keys=True))
PY
}
```

## 3. Freeze anchor A and choose the future window

`A` is read first. The loop then waits for a public status sample whose reader timestamp is newer
than `A` and whose source durable sequence covers `A`.

```bash
P="$(snapshot_progress)"

while :; do
  S="$(curl -fsS --max-time 10 "$STATUS_URL")"
  if jq -en --argjson p "$P" --argjson s "$S" '
    $s.tvu.state == "receiving"
    and $s.forwarding.state == "sending"
    and $s.hivezilla.state == "receiving"
    and $s.tvu.seconds_since_last_packet <= 5
    and $s.gossip.receiver_uptime_secs >= 60
    and $s.tvu.updated_unix_secs >= $p.captured_unix_secs
    and $s.hivezilla.durable_through_sequence >= $p.through_sequence
  ' >/dev/null; then
    break
  fi
  sleep 2
done

START="$(jq -nr --argjson p "$P" --argjson s "$S" '
  ([$p.logical_slot, $s.tvu.latest_slot, $s.hivezilla.latest_slot] | max) + 128
')"
END=$((START + 9999))
AUDIT_SHA="$(sha256sum "$AUDIT_BIN" | awk '{print $1}')"

jq -n \
  --arg rollout_id "$ROLLOUT_ID" \
  --arg audit_binary "$AUDIT_BIN" \
  --arg audit_sha "$AUDIT_SHA" \
  --argjson progress "$P" \
  --argjson status "$S" \
  --argjson start "$START" \
  --argjson end "$END" \
  '{
    schema_version: 1,
    rollout_id: $rollout_id,
    source_scope: "raw_turbine_plus_local_fec_no_repair_wal",
    audit_binary: $audit_binary,
    audit_binary_sha256: $audit_sha,
    progress: $progress,
    reader_status: $status,
    boundary: {
      start_slot: $start,
      end_slot: $end,
      slot_count: 10000,
      safety_slots_after_reader_max: 128,
      assertion_no_target_record_at_or_before_anchor: true
    }
  }' > "$AUDIT_DIR/baseline.json.tmp"

mv "$AUDIT_DIR/baseline.json.tmp" "$AUDIT_DIR/baseline.json"
chmod 0444 "$AUDIT_DIR/baseline.json"
printf 'capture range: %s..%s\n' "$START" "$END"
```

Do not recalculate `START` after this point. If the reader restarts or the journal changes, abandon
this results directory and begin a new run with a new `A`.

## 4. Wait through the late-arrival guard and freeze B

The loop is observational. It does not alter either service or cursor.

```bash
GUARD=$((END + 512))

while :; do
  P_END="$(snapshot_progress)"
  S_END="$(curl -fsS --max-time 10 "$STATUS_URL")"

  if jq -en \
    --argjson p "$P_END" \
    --argjson s "$S_END" \
    --argjson guard "$GUARD" '
      $s.tvu.state == "receiving"
      and $s.forwarding.state == "sending"
      and $s.hivezilla.state == "receiving"
      and $s.tvu.seconds_since_last_packet <= 5
      and $s.tvu.updated_unix_secs >= $p.captured_unix_secs
      and $s.hivezilla.durable_through_sequence >= $p.through_sequence
      and $s.tvu.latest_slot >= $guard
      and $s.hivezilla.latest_slot >= $guard
      and $p.logical_slot >= $guard
    ' >/dev/null; then
    break
  fi

  printf 'waiting: end=%s guard=%s NAS latest=%s\n' \
    "$END" "$GUARD" "$(jq -r '.logical_slot' <<<"$P_END")"
  sleep 15
done

jq -n \
  --argjson progress "$P_END" \
  --argjson status "$S_END" \
  --argjson guard "$GUARD" \
  '{progress:$progress, reader_status:$status, guard_slot:$guard}' \
  > "$AUDIT_DIR/end.json.tmp"
mv "$AUDIT_DIR/end.json.tmp" "$AUDIT_DIR/end.json"
chmod 0444 "$AUDIT_DIR/end.json"
```

## 5. Fetch finalized slot membership

Keep the request beside its response: a bare result array does not prove which commitment or range
was requested.

```bash
while :; do
  SLOT_REQUEST='{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[{"commitment":"finalized"}]}'
  FINALIZED="$(curl -fsS --max-time 15 \
    -H 'content-type: application/json' "$RPC_URL" -d "$SLOT_REQUEST")"
  FINALIZED_SLOT="$(jq -er '.result' <<<"$FINALIZED")"
  [ "$FINALIZED_SLOT" -ge "$END" ] && break
  sleep 15
done

printf '%s\n' "$SLOT_REQUEST" > "$AUDIT_DIR/getSlot-request.json"
printf '%s\n' "$FINALIZED" > "$AUDIT_DIR/getSlot-finalized.json"

BLOCKS_REQUEST="$(jq -nc \
  --argjson start "$START" --argjson end "$END" \
  '{jsonrpc:"2.0",id:1,method:"getBlocks",
    params:[$start,$end,{commitment:"finalized"}]}')"

BLOCKS="$(curl -fsS --max-time 60 \
  -H 'content-type: application/json' "$RPC_URL" -d "$BLOCKS_REQUEST")"

jq -e --argjson start "$START" --argjson end "$END" '
  .error == null
  and (.result | type == "array")
  and all(.result[]; . >= $start and . <= $end)
  and ((.result | unique | length) == (.result | length))
' <<<"$BLOCKS" >/dev/null

printf '%s\n' "$BLOCKS_REQUEST" > "$AUDIT_DIR/getBlocks-request.json"
printf '%s\n' "$BLOCKS" > "$AUDIT_DIR/getBlocks-finalized.json"
chmod 0444 "$AUDIT_DIR"/get*.json
```

## 6. Run the frozen two-pass audit

The NAS currently has 7.5 GiB RAM. The 512 MiB active-buffer bound is conservative for 10,000
slots and matches the prior successful audits. `nice` and `ionice` reduce interference with live
workloads.

```bash
ASEQ="$(jq -r '.progress.through_sequence' "$AUDIT_DIR/baseline.json")"
ASEG="$(jq -r '.progress.spool_location.segment_id' "$AUDIT_DIR/baseline.json")"
AOFF="$(jq -r '.progress.spool_location.frame_offset' "$AUDIT_DIR/baseline.json")"
ALEN="$(jq -r '.progress.spool_location.frame_len' "$AUDIT_DIR/baseline.json")"
BSEQ="$(jq -r '.progress.through_sequence' "$AUDIT_DIR/end.json")"

test "$BSEQ" -gt "$ASEQ"
test "$(sha256sum "$AUDIT_BIN" | awk '{print $1}')" = \
  "$(jq -r '.audit_binary_sha256' "$AUDIT_DIR/baseline.json")"
test -r "$JDIR/segment-$(printf '%020d' "$ASEG").wal"

date -u +%FT%TZ > "$AUDIT_DIR/audit.started"

if ionice -c 2 -n 7 nice -n 10 "$AUDIT_BIN" \
  --spool-root "$ROOT" \
  --cluster-id "$CLUSTER" \
  --origin-node-id "$ORIGIN" \
  --source-id "$SOURCE" \
  --journal-id "$JID" \
  --durable-through-sequence "$BSEQ" \
  --min-slot "$START" \
  --max-slot "$END" \
  --coverage-start-slot "$START" \
  --coverage-end-slot "$END" \
  --after-segment-id "$ASEG" \
  --after-frame-offset "$AOFF" \
  --after-frame-len "$ALEN" \
  --assert-anchor-precedes-all-coverage-records \
  --canonical-get-blocks-json "$AUDIT_DIR/getBlocks-finalized.json" \
  --assert-canonical-manifest-complete-finalized \
  --max-record-bytes 4096 \
  --scan-chunk-records 1000000 \
  --max-resident-bytes 536870912 \
  > "$AUDIT_DIR/audit.json.tmp" \
  2> "$AUDIT_DIR/audit.log"; then
  mv "$AUDIT_DIR/audit.json.tmp" "$AUDIT_DIR/audit.json"
  printf 'complete\n' > "$AUDIT_DIR/audit.status"
else
  rc=$?
  printf 'failed rc=%s\n' "$rc" > "$AUDIT_DIR/audit.status"
  exit "$rc"
fi
```

## 7. Validate invariants and emit final numbers

Integrity failure and failure to reach 99.9% are different outcomes. The first command fails on a
broken audit; the summary records `target_met: false` without mislabeling a sound measurement as
corrupt.

```bash
jq -e --argjson a "$ASEQ" --argjson b "$BSEQ" '
  .scan_start.anchor_sequence == $a
  and .pass_one.first_sequence == ($a + 1)
  and .pass_one.last_sequence == $b
  and .pass_two.first_sequence == ($a + 1)
  and .pass_two.last_sequence == $b
  and .pass_one.reached_durable_tail
  and .pass_two.reached_durable_tail
  and .pass_one.prefix_sha256 == .pass_two.prefix_sha256
  and .classification_counts.not_recorded == 0
  and (
    .canonical.produced_slots_in_coverage
    == (.classification_counts.reconstructed
        + .classification_counts.missed_capture)
  )
' "$AUDIT_DIR/audit.json" >/dev/null

jq '
  .canonical.produced_slots_in_coverage as $produced
  | .classification_counts.reconstructed as $rebuilt
  | {
      schema_version: 1,
      source_scope: "raw_turbine_plus_local_fec_no_repair_wal",
      slot_range: .coverage_slot_range,
      produced_slots: $produced,
      reconstructed_slots: $rebuilt,
      missed_slots: .classification_counts.missed_capture,
      reconstruction_percent:
        (if $produced == 0 then null else (100 * $rebuilt / $produced) end),
      target_percent: 99.9,
      target_met:
        (if $produced == 0 then false else (100 * $rebuilt / $produced) >= 99.9 end),
      observed_noncanonical_slots: .classification_counts.observed_noncanonical,
      locally_recovered_data_shreds:
        ([.observed_slots[].reconstruction.recovered_data_shreds] | add // 0),
      remaining_under_threshold_fec_sets:
        ([.observed_slots[].reconstruction.under_threshold_fec_sets] | add // 0),
      total_remaining_threshold_deficit:
        ([.observed_slots[].reconstruction.total_threshold_deficit] | add // 0),
      failure_categories: .failure_category_counts,
      scanned_records_per_pass: .pass_one.records,
      frozen_prefix_sha256: .pass_one.prefix_sha256
    }
' "$AUDIT_DIR/audit.json" > "$AUDIT_DIR/summary.json"

(
  cd "$AUDIT_DIR"
  sha256sum \
    baseline.json end.json \
    getSlot-request.json getSlot-finalized.json \
    getBlocks-request.json getBlocks-finalized.json \
    audit.json audit.log summary.json \
    > SHA256SUMS
)

chmod 0444 "$AUDIT_DIR"/audit.json \
  "$AUDIT_DIR"/audit.log \
  "$AUDIT_DIR"/summary.json \
  "$AUDIT_DIR"/SHA256SUMS

jq . "$AUDIT_DIR/summary.json"
```

## Expected cost and artifacts

At approximately 400 ms per slot, 10,000 slots take about 67 minutes. The 512-slot guard adds
about 3.5 minutes. Based on the previous exact NAS audits, the two passes should take 18–25
minutes; low I/O priority or concurrent pipeline work may extend this. Allow 1.5–2 hours total.

With duplicate-preserving forwarding at roughly 4,100 packets per second, expect approximately
16–18 million raw records and 25–32 GB of additional NAS spool data. Audit JSON and logs should be
roughly 10–20 MB. The procedure does not delete any of that data.

The durable evidence set is:

- `baseline.json`: rollout, reader snapshot, anchor `A`, target range, and audit-binary hash;
- `end.json`: guarded reader snapshot and durable cursor `B`;
- `getSlot-request.json` and `getSlot-finalized.json`: finalization proof;
- `getBlocks-request.json` and `getBlocks-finalized.json`: canonical membership basis;
- `audit.json`: complete exact report;
- `audit.log`: per-pass cursor progress;
- `summary.json`: stable machine-readable headline numbers;
- `SHA256SUMS`: artifact hashes.

`getBlocks` membership establishes finalized slot coverage, not full fork identity. The existing
audit report remains provisional until every reconstructed candidate's final PoH hash is compared
with the canonical blockhash.
