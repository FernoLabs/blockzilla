#!/usr/bin/env bash
set -euo pipefail

# Replace an already-running legacy supervisor only after its capture child has
# exited naturally. This avoids killing buffered archive writers during a live
# supervisor rollout. The replacement either resumes the same epoch directory
# or starts the next epoch directory when the durable journal reached the exact
# epoch boundary.

OLD_SUPERVISOR_PID=${1:?usage: nas-live-supervisor-hot-handoff.sh OLD_PID CAPTURE_DIR}
CAPTURE_DIR=${2:?usage: nas-live-supervisor-hot-handoff.sh OLD_PID CAPTURE_DIR}
REPO=${BLOCKZILLA_REPO:-/volume1/@home/ach/dev/blockzilla-v1-registry-mphf-20260616}
SUPERVISOR=${BLOCKZILLA_LIVE_SUPERVISOR:-$REPO/scripts/nas-live-producer-supervisor.sh}
SLOTS_PER_EPOCH=${BLOCKZILLA_SLOTS_PER_EPOCH:-432000}
POLL_SECS=${BLOCKZILLA_HANDOFF_POLL_SECS:-1}

if [[ ! "$OLD_SUPERVISOR_PID" =~ ^[0-9]+$ ]]; then
  echo "invalid old supervisor PID: $OLD_SUPERVISOR_PID" >&2
  exit 2
fi
if [[ ! -x "$SUPERVISOR" ]]; then
  echo "missing supervisor: $SUPERVISOR" >&2
  exit 2
fi

capture_child_running() {
  local pid
  while read -r pid; do
    [[ -n "$pid" ]] || continue
    if tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null \
      | grep -Fq "blockzilla-live-producer capture-grpc" \
      && tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null \
        | grep -Fq -- "--archive-dir $CAPTURE_DIR"
    then
      return 0
    fi
  done < <(pgrep -P "$OLD_SUPERVISOR_PID" 2>/dev/null || true)
  return 1
}

latest_slot() {
  python3 - "$CAPTURE_DIR/journal/grpc-blocks.jsonl" <<'PY'
import json, sys
last = None
with open(sys.argv[1], "rb") as journal:
    for line in journal:
        try:
            row = json.loads(line)
        except Exception:
            continue
        slot = row.get("slot")
        if isinstance(slot, int):
            last = slot
if last is None:
    raise SystemExit(1)
print(last)
PY
}

while kill -0 "$OLD_SUPERVISOR_PID" 2>/dev/null; do
  if capture_child_running; then
    sleep "$POLL_SECS"
    continue
  fi

  # Avoid switching during a transient fork/exec window between the shell and
  # a newly launched capture child.
  sleep 0.25
  if capture_child_running; then
    continue
  fi
  if ! last_slot=$(latest_slot 2>/dev/null); then
    sleep "$POLL_SECS"
    continue
  fi

  pkill -TERM -P "$OLD_SUPERVISOR_PID" 2>/dev/null || true
  kill -TERM "$OLD_SUPERVISOR_PID" 2>/dev/null || true
  sleep 0.25
  pkill -KILL -P "$OLD_SUPERVISOR_PID" 2>/dev/null || true
  kill -KILL "$OLD_SUPERVISOR_PID" 2>/dev/null || true

  next_slot=$((last_slot + 1))
  epoch_end=$((((last_slot / SLOTS_PER_EPOCH) + 1) * SLOTS_PER_EPOCH - 1))
  if ((last_slot == epoch_end)); then
    printf '%s handoff=start_next_epoch last_slot=%s next_slot=%s\n' \
      "$(date -u +%FT%TZ)" "$last_slot" "$next_slot"
    exec env \
      BLOCKZILLA_REPO="$REPO" \
      BLOCKZILLA_LIVE_FROM_SLOT="$next_slot" \
      BLOCKZILLA_LIVE_RAW_BLOCK_STORAGE=all \
      BLOCKZILLA_LIVE_PUBKEY_INDEX_MODE=runs \
      BLOCKZILLA_LIVE_PUBKEY_HOT_REGISTRY=/volume1/@home/ach/dev/blockzilla-v2/epoch-999/registry.bin \
      BLOCKZILLA_LIVE_PUBKEY_HOT_COUNT=1000 \
      BLOCKZILLA_LIVE_REPLAY_UNAVAILABLE_MARGIN_SLOTS=20 \
      BLOCKZILLA_LIVE_RESTART_DELAY_SECS=1 \
      "$SUPERVISOR"
  fi

  printf '%s handoff=resume_current_epoch last_slot=%s next_slot=%s capture_dir=%s\n' \
    "$(date -u +%FT%TZ)" "$last_slot" "$next_slot" "$CAPTURE_DIR"
  exec env \
    BLOCKZILLA_REPO="$REPO" \
    BLOCKZILLA_LIVE_CAPTURE_DIR="$CAPTURE_DIR" \
    BLOCKZILLA_LIVE_FROM_SLOT="$next_slot" \
    BLOCKZILLA_LIVE_RAW_BLOCK_STORAGE=all \
    BLOCKZILLA_LIVE_PUBKEY_INDEX_MODE=runs \
    BLOCKZILLA_LIVE_PUBKEY_HOT_REGISTRY=/volume1/@home/ach/dev/blockzilla-v2/epoch-999/registry.bin \
    BLOCKZILLA_LIVE_PUBKEY_HOT_COUNT=1000 \
    BLOCKZILLA_LIVE_REPLAY_UNAVAILABLE_MARGIN_SLOTS=20 \
    BLOCKZILLA_LIVE_RESTART_DELAY_SECS=1 \
    "$SUPERVISOR"
done

echo "old supervisor exited before a safe handoff was possible" >&2
exit 1
