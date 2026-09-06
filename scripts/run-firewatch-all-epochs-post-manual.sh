#!/bin/sh
set -eu
umask 077

usage() {
    echo "usage: $0 PLAN_TSV OUTPUT_ROOT INDEXER THREADS STATE_ROOT FIRST_EPOCH LAST_EPOCH" >&2
    exit 2
}

[ "$#" -eq 7 ] || usage

plan=$1
output_root=$2
indexer=$3
threads=$4
state_root=$5
first_epoch=$6
last_epoch=$7

case "$plan:$output_root:$indexer:$state_root" in
    /*:/*:/*:/*) ;;
    *) usage ;;
esac
case "$threads:$first_epoch:$last_epoch" in
    *[!0-9:]*|:|*:|:*) usage ;;
esac
[ "$threads" -ge 1 ] && [ "$threads" -le 256 ] || usage
[ "$first_epoch" -le "$last_epoch" ] || usage

[ -f "$plan" ] && [ ! -L "$plan" ]
[ -d "$output_root" ] && [ ! -L "$output_root" ]
[ -x "$indexer" ] && [ -f "$indexer" ] && [ ! -L "$indexer" ]

hash_file() {
    sha256sum "$1" | awk 'NR == 1 { print $1; exit }'
}

hash_text() {
    printf '%s' "$1" | sha256sum | awk 'NR == 1 { print $1; exit }'
}

source_authority() {
    sa_root=$1
    sa_rows=
    for sa_name in \
        archive-v2-blocks.zstd \
        archive-v2-blocks.index \
        archive-v2-meta.wincode \
        registry.bin \
        registry.mphf \
        signatures.bin \
        genesis.bin
    do
        sa_path=$sa_root/$sa_name
        if [ -e "$sa_path" ] || [ -L "$sa_path" ]; then
            [ -f "$sa_path" ] && [ ! -L "$sa_path" ] || return 1
            sa_row=$(stat -c "$sa_name|%d|%i|%f|%u|%g|%h|%s|%Y|%Z|%y|%z" "$sa_path") || return 1
        else
            case "$sa_name" in
                archive-v2-blocks.zstd|archive-v2-blocks.index|archive-v2-meta.wincode|registry.bin|registry.mphf|signatures.bin)
                    return 1
                    ;;
            esac
            sa_row="$sa_name|absent"
        fi
        sa_rows=$sa_rows$sa_row'
'
    done
    hash_text "$sa_rows"
}

usage_sorted_generation() {
    us_receipt=$1
    us_epoch=$2
    us_source=$3
    python3 - "$us_receipt" "$us_epoch" "$us_source" <<'PY'
import hashlib
import json
import os
import stat
import struct
import sys

receipt_path, epoch_text, source = sys.argv[1:]
epoch = int(epoch_text)
canonical = os.path.join(os.path.dirname(os.path.dirname(source)), f"epoch-{epoch}")
value = os.lstat(receipt_path)
if not stat.S_ISREG(value.st_mode):
    raise SystemExit(1)
with open(receipt_path, "rb") as handle:
    receipt = json.load(handle)
if receipt.get("version") != 3 or receipt.get("algorithm") != "compact_v2_first_seen_v1_to_usage_sorted_staged_access_v3":
    raise SystemExit(1)
if receipt.get("epoch") != epoch or receipt.get("source_dir") != canonical or receipt.get("target_dir") != source:
    raise SystemExit(1)
if receipt.get("wire_profile") not in (None, "post-unknown-instruction-fallbacks-v1"):
    raise SystemExit(1)
files = receipt.get("target_files")
required = {"archive-v2-blocks.zstd", "archive-v2-blocks.index", "archive-v2-meta.wincode", "registry.bin", "registry.mphf", "signatures.bin"}
if not isinstance(files, dict) or not required.issubset(files):
    raise SystemExit(1)
digest = hashlib.sha256()
digest.update(b"blockzilla.registry-reprocess.generation.v1")
digest.update(struct.pack("<Q", len(files)))
for name in sorted(files):
    binding = files[name]
    if not name or "/" in name or "\\" in name or set(binding) != {"bytes", "sha256"}:
        raise SystemExit(1)
    size, sha = binding["bytes"], binding["sha256"]
    if not isinstance(size, int) or size < 0 or not isinstance(sha, str) or len(sha) != 64 or any(c not in "0123456789abcdef" for c in sha):
        raise SystemExit(1)
    target_stat = os.lstat(os.path.join(source, name))
    if not stat.S_ISREG(target_stat.st_mode) or target_stat.st_size != size:
        raise SystemExit(1)
    encoded = name.encode("utf-8")
    digest.update(struct.pack("<Q", len(encoded)))
    digest.update(encoded)
    digest.update(struct.pack("<Q", size))
    digest.update(sha.encode("ascii"))
generation = digest.hexdigest()
if receipt.get("target_generation_sha256") != generation:
    raise SystemExit(1)
print(generation)
PY
}

candidate_generation() {
    cg_descriptor=$1
    cg_epoch=$2
    cg_source=$3
    python3 - "$cg_descriptor" "$cg_epoch" "$cg_source" <<'PY'
import hashlib
import json
import os
import stat
import sys

descriptor_path, epoch_text, source = sys.argv[1:]
epoch = int(epoch_text)

def regular_json(path):
    value = os.lstat(path)
    if not stat.S_ISREG(value.st_mode):
        raise SystemExit(1)
    with open(path, "rb") as handle:
        raw = handle.read()
    return json.loads(raw), raw, hashlib.sha256(raw).hexdigest()

def binding(value):
    return (isinstance(value, dict) and set(value) == {"bytes", "sha256"}
            and isinstance(value["bytes"], int) and value["bytes"] >= 0
            and isinstance(value["sha256"], str) and len(value["sha256"]) == 64
            and all(c in "0123456789abcdef" for c in value["sha256"]))

def exact_identity(path, value):
    if not isinstance(value, dict) or set(value) != {"bytes", "device_id", "inode"}:
        raise SystemExit(1)
    actual = os.lstat(path)
    if (not stat.S_ISREG(actual.st_mode) or value["bytes"] != actual.st_size
            or value["device_id"] != actual.st_dev or value["inode"] != actual.st_ino):
        raise SystemExit(1)

descriptor, descriptor_raw, descriptor_sha = regular_json(descriptor_path)
if descriptor.get("schema_version") != 1 or descriptor.get("kind") != "archive-v2-pre-to-post-candidate":
    raise SystemExit(1)
if descriptor.get("state") != "unfinalized":
    raise SystemExit(1)
if descriptor.get("epoch") != epoch or descriptor.get("cluster_id") != "mainnet-beta":
    raise SystemExit(1)
if descriptor.get("source") != source or descriptor.get("candidate") != source or descriptor.get("canonical") is not False:
    raise SystemExit(1)
if descriptor.get("canonical_publication_deferred") is not True or descriptor.get("target_post_audit_performed") is not False:
    raise SystemExit(1)
if descriptor.get("expected_wire_profile_after_rewrite") != "post-unknown-instruction-fallbacks-v1":
    raise SystemExit(1)
for field in ("single_decode_rewrite_pass", "outer_block_bytes_preserved_verbatim_except_messages", "exact_message_length_preserved", "exact_message_delta_proved", "metadata_regions_copied_verbatim"):
    if descriptor.get(field) is not True:
        raise SystemExit(1)
for field in ("sidecars_copied", "sidecars_rewritten", "target_post_audit_performed", "canonical_manifest_written", "canonical_profile_marker_written", "canonical_migration_receipt_written"):
    if descriptor.get(field) is not False:
        raise SystemExit(1)
rewrite = descriptor.get("rewrite", {})
audit_ref = descriptor.get("source_audit_report", {})
audit_path = audit_ref.get("path")
audit_bytes = audit_ref.get("bytes")
audit_sha = audit_ref.get("sha256")
if not isinstance(audit_path, str) or not os.path.isabs(audit_path) or not isinstance(audit_bytes, int):
    raise SystemExit(1)
audit, audit_raw, actual_audit_sha = regular_json(audit_path)
if len(audit_raw) != audit_bytes or audit_sha != actual_audit_sha:
    raise SystemExit(1)
counts = audit.get("counts")
if audit.get("schema_version") != 1 or audit.get("kind") != "archive-v2-wire-profile-scan" or audit.get("epoch") != epoch or audit.get("archive") != source or audit.get("error") is not None:
    raise SystemExit(1)
if audit.get("classification") != "legacy-pre" or audit.get("action") != "convert-to-post" or not isinstance(counts, dict):
    raise SystemExit(1)
if counts.get("pre_only", 0) <= 0 or counts.get("owned_fallback_blocks") != 0 or counts.get("raw_transaction_fallbacks") != 0 or counts.get("post_only") != 0 or counts.get("both_divergent") != 0 or counts.get("invalid") != 0:
    raise SystemExit(1)
if counts.get("typed_messages") != counts.get("pre_only") + counts.get("both_equivalent") or audit_ref.get("counts") != counts:
    raise SystemExit(1)
if rewrite.get("blocks") != counts.get("blocks") or rewrite.get("typed_messages") != counts.get("typed_messages") or rewrite.get("message_input_bytes") != rewrite.get("message_output_bytes"):
    raise SystemExit(1)
if rewrite.get("raw_transaction_fallbacks", 0) != 0 or rewrite.get("owned_outer_fallbacks", 0) != 0:
    raise SystemExit(1)
generation = descriptor.get("prospective_generation_id")
if not isinstance(generation, str) or not generation or "\t" in generation or "\n" in generation:
    raise SystemExit(1)
backup = descriptor.get("backup")
if backup != os.path.join(os.path.dirname(source), f".epoch-{epoch}.pre-to-post.backup"):
    raise SystemExit(1)
staging = os.path.join(os.path.dirname(source), f".epoch-{epoch}.pre-to-post.staging")
if not stat.S_ISDIR(os.lstat(backup).st_mode) or os.path.lexists(staging):
    raise SystemExit(1)
intent, intent_raw, intent_sha = regular_json(os.path.join(backup, "archive-v2-pre-to-post.switch-intent.v1.json"))
complete, complete_raw, complete_sha = regular_json(os.path.join(backup, "archive-v2-pre-to-post.switch-complete.v1.json"))
if intent.get("schema_version") != 1 or intent.get("kind") != "archive-v2-pre-to-post-pair-swap-intent":
    raise SystemExit(1)
if complete.get("schema_version") != 1 or complete.get("kind") != "archive-v2-pre-to-post-pair-swap-complete":
    raise SystemExit(1)
for record in (intent, complete):
    if record.get("epoch") != epoch or record.get("candidate") != source or record.get("backup") != backup:
        raise SystemExit(1)
if intent.get("cluster_id") != "mainnet-beta" or intent.get("staging") != staging:
    raise SystemExit(1)
if complete.get("canonical") is not False or complete.get("intent_sha256") != intent_sha:
    raise SystemExit(1)
if intent.get("candidate_descriptor") != {"bytes": len(descriptor_raw), "sha256": descriptor_sha}:
    raise SystemExit(1)
if complete.get("candidate_descriptor_sha256") != descriptor_sha or intent.get("prospective_generation_id") != generation:
    raise SystemExit(1)
if intent.get("source_audit_report_path") != audit_path or intent.get("source_audit_report") != {"bytes": audit_bytes, "sha256": audit_sha}:
    raise SystemExit(1)
if complete.get("source_audit_report_sha256") != audit_sha:
    raise SystemExit(1)
old_files = descriptor.get("source_files", {})
new_files = descriptor.get("candidate_rewrite_files", {})
for name in ("archive-v2-blocks.zstd", "archive-v2-blocks.index"):
    if not binding(old_files.get(name)) or not binding(new_files.get(name)):
        raise SystemExit(1)
if intent.get("source_blocks_binding") != old_files["archive-v2-blocks.zstd"] or intent.get("source_index_binding") != old_files["archive-v2-blocks.index"]:
    raise SystemExit(1)
if intent.get("candidate_blocks_binding") != new_files["archive-v2-blocks.zstd"] or intent.get("candidate_index_binding") != new_files["archive-v2-blocks.index"]:
    raise SystemExit(1)
if intent.get("moved_to_backup") != descriptor.get("moved_to_backup") or intent.get("retained_edge_files") != descriptor.get("retained_edge_files"):
    raise SystemExit(1)
for label, expected in (("source_blocks_sha256", old_files["archive-v2-blocks.zstd"]["sha256"]), ("source_index_sha256", old_files["archive-v2-blocks.index"]["sha256"]), ("candidate_blocks_sha256", new_files["archive-v2-blocks.zstd"]["sha256"]), ("candidate_index_sha256", new_files["archive-v2-blocks.index"]["sha256"])):
    if complete.get(label) != expected:
        raise SystemExit(1)
for directory, files in ((source, new_files), (backup, old_files)):
    for name in ("archive-v2-blocks.zstd", "archive-v2-blocks.index"):
        live = os.lstat(os.path.join(directory, name))
        if not stat.S_ISREG(live.st_mode) or live.st_size != files[name]["bytes"]:
            raise SystemExit(1)
exact_identity(os.path.join(source, "archive-v2-blocks.zstd"), intent.get("candidate_blocks"))
exact_identity(os.path.join(source, "archive-v2-blocks.index"), intent.get("candidate_index"))
exact_identity(os.path.join(backup, "archive-v2-blocks.zstd"), intent.get("source_blocks"))
exact_identity(os.path.join(backup, "archive-v2-blocks.index"), intent.get("source_index"))
durable = descriptor.get("retained_durable_files")
required_durable = {"archive-v2-meta.wincode", "registry.bin", "registry.mphf", "signatures.bin"}
if not isinstance(durable, list) or len(durable) != len(set(durable)) or not required_durable.issubset(durable):
    raise SystemExit(1)
for name in descriptor.get("moved_to_backup", []):
    if not isinstance(name, str) or not name or "/" in name or "\\" in name:
        raise SystemExit(1)
    if os.path.lexists(os.path.join(source, name)) or not os.path.lexists(os.path.join(backup, "disabled", name)):
        raise SystemExit(1)
for name in descriptor.get("retained_edge_files", []):
    if not isinstance(name, str) or not name or "/" in name or "\\" in name or not os.path.lexists(os.path.join(source, name)):
        raise SystemExit(1)
bundle = hashlib.sha256()
bundle.update(b"blockzilla.firewatch.candidate-control-bundle.v1")
for value in (descriptor_sha, audit_sha, intent_sha, complete_sha):
    bundle.update(value.encode("ascii"))
print(generation + " " + bundle.hexdigest())
PY
}

registry_identity_matches() {
    rim_manifest=$1
    rim_source=$2
    rim_registry=$(stat -c '%d:%i:%s:%Y:%Z' "$rim_source/registry.bin") || return 1
    rim_expected=$(jq -er '.registry_file_identity | [.device,.inode,.size,.modified_seconds,.changed_seconds] | map(tostring) | join(":")' "$rim_manifest") || return 1
    [ "$rim_registry" = "$rim_expected" ] || return 1
    rim_mphf=$(stat -c '%d:%i:%s:%Y:%Z' "$rim_source/registry.mphf") || return 1
    rim_expected=$(jq -er '.registry_index_file_identity | [.device,.inode,.size,.modified_seconds,.changed_seconds] | map(tostring) | join(":")' "$rim_manifest") || return 1
    [ "$rim_mphf" = "$rim_expected" ]
}

validate_output() {
    vo_epoch=$1
    vo_source=$2
    vo_generation=$3
    vo_output=$4
    [ -d "$vo_output" ] && [ ! -L "$vo_output" ] \
        && [ -f "$vo_output/manifest.json" ] && [ ! -L "$vo_output/manifest.json" ] || return 1
    "$indexer" verify-index --index "$vo_output" || return 1
    jq -e \
        --argjson epoch "$vo_epoch" \
        --arg source "$vo_source" \
        --arg generation "$vo_generation" \
        '(.schema_version == 4)
         and (.complete == true)
         and (.epoch == $epoch)
         and (.archive_root == $source)
         and (.generation_id == $generation)
         and (.archive_wire_profile == "post-unknown-instruction-fallbacks-v1")
         and ((.binding_kind == "trusted_local_asserted_immutable") or (.binding_kind == "published_manifest"))
         and (.omissions.raw_transactions == 0)
         and (.omissions.raw_metadata == 0)
         and (.omissions.decode_errors == 0)
         and (.omissions.unresolved_required_pubkeys == 0)' \
        "$vo_output/manifest.json" >/dev/null || return 1
    registry_identity_matches "$vo_output/manifest.json" "$vo_source"
}

indexer_count() {
    ic_count=0
    for ic_proc in /proc/[0-9]*; do
        [ -e "$ic_proc/exe" ] || continue
        ic_exe=$(readlink "$ic_proc/exe" 2>/dev/null || true)
        case "$ic_exe" in
            */blockzilla-user-program-index|*/blockzilla-user-program-index-*|*/blockzilla-firebase-indexer|*/blockzilla-firebase-indexer-*) ic_count=$((ic_count + 1)) ;;
        esac
    done
    printf '%s' "$ic_count"
}

services_inactive() {
    [ "$(systemctl --user is-active blockzilla-archive.service 2>/dev/null || true)" = inactive ] \
        && [ "$(systemctl --user is-active blockzilla-firewatch-index-controller.service 2>/dev/null || true)" = inactive ]
}

require_quiescent() {
    services_inactive && [ "$(indexer_count)" -eq 0 ]
}

validate_control() {
    vc_epoch=$1
    vc_kind=$2
    vc_source=$3
    vc_generation=$4
    vc_control=$5
    vc_control_sha=$6
    case "$vc_kind" in
        direct)
            [ "$vc_control" = - ] && [ "$vc_control_sha" = - ] \
                && [ ! -e "$vc_source/registry-first-seen.manifest" ] \
                && [ ! -L "$vc_source/registry-first-seen.manifest" ] \
                && [ ! -e "$vc_source/archive-v2-pre-to-post.candidate.v1.json" ] \
                && [ ! -L "$vc_source/archive-v2-pre-to-post.candidate.v1.json" ]
            ;;
        candidate)
            [ -f "$vc_control" ] && [ ! -L "$vc_control" ] || return 1
            [ ! -e "$vc_source/registry-first-seen.manifest" ] \
                && [ ! -L "$vc_source/registry-first-seen.manifest" ] || return 1
            [ "$(candidate_generation "$vc_control" "$vc_epoch" "$vc_source")" = "$vc_generation $vc_control_sha" ]
            ;;
        usage_sorted)
            [ -f "$vc_control" ] && [ ! -L "$vc_control" ] || return 1
            vc_canonical=$(dirname "$(dirname "$vc_source")")/epoch-$vc_epoch
            vc_first_seen=$vc_canonical/registry-first-seen.manifest
            [ -f "$vc_first_seen" ] && [ ! -L "$vc_first_seen" ] || return 1
            vc_receipt_sha=$(hash_file "$vc_control") || return 1
            vc_first_seen_sha=$(hash_file "$vc_first_seen") || return 1
            vc_bundle=$(hash_text "blockzilla.firewatch.usage-sorted-control-bundle.v1
$vc_receipt_sha
$vc_first_seen_sha
") || return 1
            [ "$vc_bundle" = "$vc_control_sha" ] \
                && [ "$(usage_sorted_generation "$vc_control" "$vc_epoch" "$vc_source")" = "$vc_generation" ]
            ;;
        *) return 1 ;;
    esac
}

plan_sha=$(hash_file "$plan")
runner_sha=$(hash_file "$0")
indexer_sha=$(hash_file "$indexer")
[ "$first_epoch" -eq 0 ] && [ "$last_epoch" -eq 1018 ]
expected_rows=1019
[ "$(wc -l <"$plan" | tr -d '[:space:]')" -eq "$expected_rows" ]

awk -F '\t' -v first="$first_epoch" -v last="$last_epoch" '
    BEGIN { expected = first; ok = 1 }
    NF != 8 || $1 !~ /^[0-9]+$/ || ($1 + 0) != expected { ok = 0; exit }
    { expected++ }
    END { if (!ok || expected != last + 1) exit 1 }
' "$plan"
candidate_cohort_sha=$(awk -F '\t' '$2 == "candidate" {print $1}' "$plan" | sha256sum | awk 'NR == 1 {print $1; exit}')
usage_sorted_cohort_sha=$(awk -F '\t' '$2 == "usage_sorted" {print $1}' "$plan" | sha256sum | awk 'NR == 1 {print $1; exit}')
direct_cohort_sha=$(awk -F '\t' '$2 == "direct" {print $1}' "$plan" | sha256sum | awk 'NR == 1 {print $1; exit}')
[ "$candidate_cohort_sha" = 331f0601eacd7ed881e7e500c09db884886303ff625e7f41b11e6666dfe00f60 ] \
    && [ "$usage_sorted_cohort_sha" = d62fcd7a4eeff906bedddeec2656ae1c7e8324ba82804d0c8ae3f99a0f6c02e8 ] \
    && [ "$direct_cohort_sha" = 92250ee2923bfec8f46efe21c3e5e732db8012be6c4a49b7de0b3cd418c70de4 ]

if [ ! -e "$state_root" ] && [ ! -L "$state_root" ]; then
    mkdir "$state_root"
    chmod 700 "$state_root"
fi
[ -d "$state_root" ] && [ ! -L "$state_root" ]

lock=$output_root/.firewatch-all-epochs-post.lock
mkdir "$lock" || {
    echo "another Firewatch all-epoch publisher holds $lock" >&2
    if [ -f "$lock/owner.json" ] && [ ! -L "$lock/owner.json" ]; then
        sed -n '1,40p' "$lock/owner.json" >&2
    fi
    exit 1
}

child=
release_lock() {
    if [ -f "$lock/owner.json" ] && [ ! -L "$lock/owner.json" ]; then
        unlink "$lock/owner.json"
    fi
    rmdir "$lock" 2>/dev/null || true
}
stop_child() {
    if [ -n "$child" ] && kill -0 "$child" 2>/dev/null; then
        kill "$child" 2>/dev/null || true
        wait "$child" 2>/dev/null || true
    fi
    child=
}
cleanup() {
    cleanup_status=$?
    trap - HUP INT QUIT TERM EXIT
    stop_child
    release_lock
    exit "$cleanup_status"
}
interrupted() {
    interrupted_status=$1
    trap - HUP INT QUIT TERM EXIT
    stop_child
    release_lock
    exit "$interrupted_status"
}
trap 'interrupted 129' HUP
trap 'interrupted 130' INT
trap 'interrupted 131' QUIT
trap 'interrupted 143' TERM
trap cleanup EXIT

owner_tmp=$lock/.owner.json.tmp.$$
owner_start_ticks=$(awk '{print $22}' "/proc/$$/stat")
jq -n --argjson pid "$$" --arg start_ticks "$owner_start_ticks" \
    --arg runner "$0" --arg plan "$plan" --arg state_root "$state_root" \
    '{kind:"firewatch-all-epochs-post-lock-owner-v1",pid:$pid,start_ticks:$start_ticks,
      runner:$runner,plan:$plan,state_root:$state_root}' >"$owner_tmp"
chmod 400 "$owner_tmp"
ln "$owner_tmp" "$lock/owner.json"
unlink "$owner_tmp"

config=$state_root/config.json
config_tmp=$state_root/.config.json.tmp.$$
jq -n \
    --arg kind firewatch-all-epochs-post-v1 \
    --arg plan "$plan" --arg plan_sha256 "$plan_sha" \
    --arg runner "$0" --arg runner_sha256 "$runner_sha" \
    --arg indexer "$indexer" --arg indexer_sha256 "$indexer_sha" \
    --arg output_root "$output_root" --argjson threads "$threads" \
    --argjson first_epoch "$first_epoch" --argjson last_epoch "$last_epoch" \
    --arg candidate_cohort_sha256 "$candidate_cohort_sha" \
    --arg usage_sorted_cohort_sha256 "$usage_sorted_cohort_sha" \
    --arg direct_cohort_sha256 "$direct_cohort_sha" \
    '{kind:$kind,canonical_publication:false,plan:$plan,plan_sha256:$plan_sha256,
      runner:$runner,runner_sha256:$runner_sha256,indexer:$indexer,indexer_sha256:$indexer_sha256,
      output_root:$output_root,threads:$threads,first_epoch:$first_epoch,last_epoch:$last_epoch,
      candidate_cohort_sha256:$candidate_cohort_sha256,
      usage_sorted_cohort_sha256:$usage_sorted_cohort_sha256,
      direct_cohort_sha256:$direct_cohort_sha256}' \
    >"$config_tmp"
chmod 400 "$config_tmp"
if [ -e "$config" ] || [ -L "$config" ]; then
    cmp -s "$config_tmp" "$config" || {
        echo "Firewatch batch config changed" >&2
        exit 1
    }
    rm "$config_tmp"
else
    ln "$config_tmp" "$config"
    rm "$config_tmp"
fi

require_quiescent || {
    echo "archive service, controller, or another indexer is active" >&2
    exit 1
}

tab=$(printf '\t')
while IFS="$tab" read -r epoch kind source generation authority control control_sha output extra; do
    [ -z "${extra:-}" ]
    [ -d "$source" ] && [ ! -L "$source" ]
    [ "$(source_authority "$source")" = "$authority" ] || {
        echo "epoch $epoch: source authority changed" >&2
        exit 1
    }
    validate_control "$epoch" "$kind" "$source" "$generation" "$control" "$control_sha" || {
        echo "epoch $epoch: control authority changed" >&2
        exit 1
    }
    [ "$(hash_file "$plan")" = "$plan_sha" ] \
        && [ "$(hash_file "$0")" = "$runner_sha" ] \
        && [ "$(hash_file "$indexer")" = "$indexer_sha" ] || {
        echo "epoch $epoch: admitted tool or plan changed" >&2
        exit 1
    }

    epoch_root=$output_root/epoch-$epoch
    [ "$(dirname "$output")" = "$epoch_root" ] || {
        echo "epoch $epoch: output is outside its exact epoch root" >&2
        exit 1
    }
    if [ ! -e "$epoch_root" ] && [ ! -L "$epoch_root" ]; then
        mkdir "$epoch_root"
        chmod 700 "$epoch_root"
    fi
    [ -d "$epoch_root" ] && [ ! -L "$epoch_root" ]

    if [ -e "$output" ] || [ -L "$output" ]; then
        validate_output "$epoch" "$source" "$generation" "$output" || {
            echo "epoch $epoch: existing current index is invalid" >&2
            exit 1
        }
        [ "$(source_authority "$source")" = "$authority" ] \
            && validate_control "$epoch" "$kind" "$source" "$generation" "$control" "$control_sha" \
            && require_quiescent || {
            echo "epoch $epoch: authority changed while validating the existing index" >&2
            exit 1
        }
        echo "epoch $epoch: verified existing $output"
        continue
    fi

    base=${output##*/}
    for staging in "$epoch_root"/.$base.staging-*; do
        if [ -e "$staging" ] || [ -L "$staging" ]; then
            echo "epoch $epoch: stale index staging exists: $staging" >&2
            exit 1
        fi
    done

    require_quiescent || {
        echo "epoch $epoch: archive service, controller, or another indexer became active" >&2
        exit 1
    }
    echo "epoch $epoch: building with $threads threads"
    "$indexer" build-dense \
        --epoch "$epoch" \
        --archive "$source" \
        --out "$output" \
        --trust-local \
        --cluster-id mainnet-beta \
        --generation-id "$generation" \
        --wire-profile post-unknown-instruction-fallbacks-v1 \
        --threads "$threads" &
    child=$!
    while kill -0 "$child" 2>/dev/null; do
        child_state=$(awk '{print $3}' "/proc/$child/stat" 2>/dev/null || true)
        [ "$child_state" = Z ] && break
        sleep 30
        kill -0 "$child" 2>/dev/null || break
        child_state=$(awk '{print $3}' "/proc/$child/stat" 2>/dev/null || true)
        [ "$child_state" = Z ] && break
        child_exe=$(readlink "/proc/$child/exe" 2>/dev/null || true)
        if ! services_inactive \
            || [ "$(indexer_count)" -ne 1 ] \
            || [ "$child_exe" != "$(realpath "$indexer")" ] \
            || [ "$(source_authority "$source")" != "$authority" ] \
            || ! validate_control "$epoch" "$kind" "$source" "$generation" "$control" "$control_sha" \
            || [ "$(hash_file "$plan")" != "$plan_sha" ]; then
            echo "epoch $epoch: live authority or process guard failed" >&2
            stop_child
            exit 1
        fi
    done
    if wait "$child"; then
        :
    else
        child_status=$?
        child=
        exit "$child_status"
    fi
    child=

    validate_control "$epoch" "$kind" "$source" "$generation" "$control" "$control_sha" || {
        echo "epoch $epoch: control authority changed during build" >&2
        exit 1
    }
    validate_output "$epoch" "$source" "$generation" "$output" || {
        echo "epoch $epoch: new current index is invalid" >&2
        exit 1
    }
    [ "$(source_authority "$source")" = "$authority" ] || {
        echo "epoch $epoch: source authority changed during build" >&2
        exit 1
    }
    validate_control "$epoch" "$kind" "$source" "$generation" "$control" "$control_sha" || {
        echo "epoch $epoch: control authority changed during final validation" >&2
        exit 1
    }
    require_quiescent || {
        echo "epoch $epoch: an unexpected service or indexer is active after build" >&2
        exit 1
    }
    echo "epoch $epoch: verified new $output"
done <"$plan"

[ "$(hash_file "$plan")" = "$plan_sha" ] \
    && [ "$(hash_file "$0")" = "$runner_sha" ] \
    && [ "$(hash_file "$indexer")" = "$indexer_sha" ]

complete=$state_root/complete.json
complete_tmp=$state_root/.complete.json.tmp.$$
jq -n \
    --arg kind firewatch-all-epochs-post-complete-v1 \
    --arg plan_sha256 "$plan_sha" --arg runner_sha256 "$runner_sha" \
    --arg indexer_sha256 "$indexer_sha" --argjson threads "$threads" \
    --argjson first_epoch "$first_epoch" --argjson last_epoch "$last_epoch" \
    --argjson completed_epochs "$expected_rows" \
    '{kind:$kind,canonical_publication:false,plan_sha256:$plan_sha256,
      runner_sha256:$runner_sha256,indexer_sha256:$indexer_sha256,threads:$threads,
      first_epoch:$first_epoch,last_epoch:$last_epoch,completed_epochs:$completed_epochs}' >"$complete_tmp"
chmod 400 "$complete_tmp"
if [ -e "$complete" ] || [ -L "$complete" ]; then
    cmp -s "$complete_tmp" "$complete" || {
        echo "completion record already exists with different content" >&2
        exit 1
    }
    rm "$complete_tmp"
else
    ln "$complete_tmp" "$complete"
    rm "$complete_tmp"
fi

echo "Firewatch all-epoch Post batch complete"
