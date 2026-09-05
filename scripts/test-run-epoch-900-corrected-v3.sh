#!/bin/sh

set -eu
umask 077
LC_ALL=C
export LC_ALL

die() {
    echo "epoch-900 corrected V3 runner test: $*" >&2
    exit 1
}

script_dir=$(CDPATH= cd -P "$(dirname "$0")" && pwd -P)
runner=$script_dir/run-epoch-900-corrected-v3.sh

for required in grep sh; do
    command -v "$required" >/dev/null 2>&1 || die "missing command: $required"
done

[ -f "$runner" ] && [ ! -L "$runner" ] || die "runner is not one real file"
sh -n "$runner" || die "runner does not pass sh -n"
if command -v dash >/dev/null 2>&1; then
    dash -n "$runner" || die "runner does not pass dash -n"
fi
"$runner" --self-test >/dev/null || die "runner self-test failed"

require_text() {
    grep -F -- "$1" "$runner" >/dev/null \
        || die "runner is missing fixed text: $1"
}

require_text '/volume1/blockzilla/archive-metadata-normalization/staging/epoch-900-current-typed-errors-v1-20260828T124710CEST'
require_text '/volume1/blockzilla/index-archive-trial/foundation-optimized-split-v3-current-r1/epoch-900-full-2g-r2'
require_text '/volume1/blockzilla/scheduler-state/index-archive-bin/archive-v3-split-convert-2g-r2'
require_text '/volume1/blockzilla/scheduler-state/index-archive-bin/archive-v3-read-demo-network-r1'
require_text 'SPYX_PID=252572'
require_text 'WORKERS=12'
require_text 'MIN_LOGICAL_CPUS=12'
require_text 'MIN_MEM_AVAILABLE_KIB=3145728'
require_text 'MIN_FREE_BYTES=500000000000'
require_text 'archive-v2-generation.json'
require_text 'archive-v2-message-schema-post-unknown-fallbacks-v1.marker'
require_text 'archive-v2-metadata-schema-current-typed-errors-v1.marker'
require_text 'source_generation_digest == null'
require_text '.output_validation == "not-run"'
require_text '.content_hashing == "none"'
require_text '.optimized_split_v3.forward_projection == "not-created"'
require_text '.standalone_ledger.stats.directory_v3.varint_delta_checkpoint_blocks == 431858'
require_text '.required_cloud_upload.omitted_forward_total_bytes == 0'
require_text 'ready_for_fixed_r2_inventory: true'
require_text '.block.block_id == 2'
require_text '.transaction.tx_index == 1151'
require_text '.transaction.metadata.kind == "decoded"'
require_text '[ ! -L "$LOCK_FILE" ]'

if grep -F '/volume1/blockzilla/index-archive-trial/foundation-optimized-split-v3-r1/epoch-900-full-2g-r2' "$runner" >/dev/null; then
    die "runner contains the forbidden historical V3 path"
fi
if grep -E 'loadavg|uptime|load average' "$runner" >/dev/null; then
    die "runner contains an unapproved load threshold"
fi
if grep -E 'rm[[:space:]]|rmdir[[:space:]]|--delete' "$runner" >/dev/null; then
    die "runner contains a destructive command"
fi
echo "epoch-900 corrected V3 runner test: PASS"
