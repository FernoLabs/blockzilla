#!/bin/sh
set -eu

usage() {
    echo "usage: $0 ARCHIVE_ROOT OUTPUT_ROOT FIRST_EPOCH LAST_EPOCH" >&2
    exit 2
}

[ "$#" -eq 4 ] || usage

archive_root=$1
output_root=$2
first_epoch=$3
last_epoch=$4

case "$archive_root:$output_root" in
    /*:/*) ;;
    *) usage ;;
esac
case "$first_epoch:$last_epoch" in
    *[!0-9:]*|:|*:|:*) usage ;;
esac
[ "$first_epoch" -le "$last_epoch" ] || usage

archive_root=$(realpath "$archive_root")
output_root=$(realpath "$output_root")
[ -d "$archive_root" ] && [ ! -L "$archive_root" ]
[ -d "$output_root" ] && [ ! -L "$output_root" ]

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
            [ -f "$sa_path" ] && [ ! -L "$sa_path" ] || {
                echo "source input is not one real file: $sa_path" >&2
                return 1
            }
            sa_row=$(stat -c "$sa_name|%d|%i|%f|%u|%g|%h|%s|%Y|%Z|%y|%z" "$sa_path") || return 1
        else
            case "$sa_name" in
                archive-v2-blocks.zstd|archive-v2-blocks.index|archive-v2-meta.wincode|registry.bin|registry.mphf|signatures.bin)
                    echo "required source input is absent: $sa_path" >&2
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
    us_canonical=$3
    us_source=$4
    python3 - "$us_receipt" "$us_epoch" "$us_canonical" "$us_source" <<'PY'
import hashlib
import json
import os
import stat
import struct
import sys

receipt_path, epoch_text, canonical, source = sys.argv[1:]
epoch = int(epoch_text)
st = os.lstat(receipt_path)
if not stat.S_ISREG(st.st_mode):
    raise SystemExit("usage-sorted receipt is not a real regular file")
with open(receipt_path, "rb") as handle:
    receipt = json.load(handle)
if receipt.get("version") != 3:
    raise SystemExit("usage-sorted receipt version is not 3")
if receipt.get("algorithm") != "compact_v2_first_seen_v1_to_usage_sorted_staged_access_v3":
    raise SystemExit("usage-sorted receipt algorithm is invalid")
if receipt.get("epoch") != epoch:
    raise SystemExit("usage-sorted receipt epoch is invalid")
if receipt.get("source_dir") != canonical or receipt.get("target_dir") != source:
    raise SystemExit("usage-sorted receipt paths are invalid")
if receipt.get("wire_profile") not in (None, "post-unknown-instruction-fallbacks-v1"):
    raise SystemExit("usage-sorted receipt wire profile is invalid")
files = receipt.get("target_files")
if not isinstance(files, dict) or not files:
    raise SystemExit("usage-sorted target file map is invalid")
required = {
    "archive-v2-blocks.zstd",
    "archive-v2-blocks.index",
    "archive-v2-meta.wincode",
    "registry.bin",
    "registry.mphf",
    "signatures.bin",
}
if not required.issubset(files):
    raise SystemExit("usage-sorted receipt lacks a required Firewatch input")
digest = hashlib.sha256()
digest.update(b"blockzilla.registry-reprocess.generation.v1")
digest.update(struct.pack("<Q", len(files)))
for name in sorted(files):
    binding = files[name]
    if not name or "/" in name or "\\" in name or set(binding) != {"bytes", "sha256"}:
        raise SystemExit("usage-sorted target binding is invalid")
    size = binding["bytes"]
    sha = binding["sha256"]
    if not isinstance(size, int) or size < 0:
        raise SystemExit("usage-sorted target size is invalid")
    if not isinstance(sha, str) or len(sha) != 64 or any(c not in "0123456789abcdef" for c in sha):
        raise SystemExit("usage-sorted target digest is invalid")
    target = os.path.join(source, name)
    target_stat = os.lstat(target)
    if not stat.S_ISREG(target_stat.st_mode) or target_stat.st_size != size:
        raise SystemExit("usage-sorted live target size or type is invalid")
    encoded = name.encode("utf-8")
    digest.update(struct.pack("<Q", len(encoded)))
    digest.update(encoded)
    digest.update(struct.pack("<Q", size))
    digest.update(sha.encode("ascii"))
generation = digest.hexdigest()
if receipt.get("target_generation_sha256") != generation:
    raise SystemExit("usage-sorted generation digest is invalid")
print(generation)
PY
}

candidate_generation() {
    cg_descriptor=$1
    cg_epoch=$2
    cg_source=$3
    python3 - "$cg_descriptor" "$cg_epoch" "$cg_source" <<'PY'
import hashlib, json, os, stat, sys

descriptor_path, epoch_text, source = sys.argv[1:]
epoch = int(epoch_text)
hexchars = set("0123456789abcdef")

def fail(message):
    raise SystemExit(message)

def regular_json(path):
    value = os.lstat(path)
    if not stat.S_ISREG(value.st_mode):
        fail(f"control is not a real regular file: {path}")
    with open(path, "rb") as handle:
        raw = handle.read()
    return json.loads(raw), raw, hashlib.sha256(raw).hexdigest()

def binding(value):
    return (isinstance(value, dict) and set(value) == {"bytes", "sha256"}
            and isinstance(value["bytes"], int) and value["bytes"] >= 0
            and isinstance(value["sha256"], str) and len(value["sha256"]) == 64
            and set(value["sha256"]) <= hexchars)

def real_file_size(path, expected):
    value = os.lstat(path)
    if not stat.S_ISREG(value.st_mode) or value.st_size != expected:
        fail(f"candidate payload size or type is invalid: {path}")

def exact_identity(path, value):
    if not isinstance(value, dict) or set(value) != {"bytes", "device_id", "inode"}:
        fail("candidate file identity shape is invalid")
    actual = os.lstat(path)
    if (not stat.S_ISREG(actual.st_mode) or value["bytes"] != actual.st_size
            or value["device_id"] != actual.st_dev or value["inode"] != actual.st_ino):
        fail("candidate file identity changed")

descriptor, descriptor_raw, descriptor_sha = regular_json(descriptor_path)
generation = descriptor.get("prospective_generation_id")
backup = os.path.join(os.path.dirname(source), f".epoch-{epoch}.pre-to-post.backup")
staging = os.path.join(os.path.dirname(source), f".epoch-{epoch}.pre-to-post.staging")
if descriptor.get("schema_version") != 1 or descriptor.get("kind") != "archive-v2-pre-to-post-candidate": fail("candidate descriptor schema is invalid")
if descriptor.get("state") != "unfinalized" or descriptor.get("canonical") is not False: fail("candidate state is invalid")
if descriptor.get("epoch") != epoch or descriptor.get("cluster_id") != "mainnet-beta": fail("candidate epoch or cluster is invalid")
if descriptor.get("source") != source or descriptor.get("candidate") != source or descriptor.get("backup") != backup: fail("candidate paths are invalid")
if not isinstance(generation, str) or not generation or "\t" in generation or "\n" in generation: fail("candidate generation ID is invalid")
if descriptor.get("expected_wire_profile_after_rewrite") != "post-unknown-instruction-fallbacks-v1": fail("candidate wire profile is invalid")
for field in ("single_decode_rewrite_pass", "outer_block_bytes_preserved_verbatim_except_messages", "exact_message_length_preserved", "exact_message_delta_proved", "metadata_regions_copied_verbatim", "canonical_publication_deferred"):
    if descriptor.get(field) is not True: fail("candidate rewrite proof is incomplete")
for field in ("sidecars_copied", "sidecars_rewritten", "target_post_audit_performed", "canonical_manifest_written", "canonical_profile_marker_written", "canonical_migration_receipt_written"):
    if descriptor.get(field) is not False: fail("candidate deferred-publication contract is invalid")

audit_ref = descriptor.get("source_audit_report")
if not isinstance(audit_ref, dict): fail("candidate audit reference is invalid")
audit_path, audit_bytes, audit_sha = audit_ref.get("path"), audit_ref.get("bytes"), audit_ref.get("sha256")
if not isinstance(audit_path, str) or not os.path.isabs(audit_path) or not isinstance(audit_bytes, int): fail("candidate audit reference is invalid")
audit, audit_raw, actual_audit_sha = regular_json(audit_path)
if len(audit_raw) != audit_bytes or audit_sha != actual_audit_sha: fail("candidate audit binding is invalid")
counts = audit.get("counts")
if audit.get("schema_version") != 1 or audit.get("kind") != "archive-v2-wire-profile-scan" or audit.get("epoch") != epoch or audit.get("archive") != source or audit.get("error") is not None: fail("candidate audit identity is invalid")
if audit.get("classification") != "legacy-pre" or audit.get("action") != "convert-to-post" or not isinstance(counts, dict): fail("candidate audit decision is invalid")
if counts.get("pre_only", 0) <= 0 or counts.get("owned_fallback_blocks") != 0 or counts.get("raw_transaction_fallbacks") != 0 or counts.get("post_only") != 0 or counts.get("both_divergent") != 0 or counts.get("invalid") != 0: fail("candidate audit counts are invalid")
if counts.get("typed_messages") != counts.get("pre_only") + counts.get("both_equivalent"): fail("candidate audit typed count is invalid")
if audit_ref.get("counts") != counts: fail("candidate descriptor audit counts differ")
rewrite = descriptor.get("rewrite", {})
if rewrite.get("blocks") != counts.get("blocks") or rewrite.get("typed_messages") != counts.get("typed_messages") or rewrite.get("message_input_bytes") != rewrite.get("message_output_bytes"): fail("candidate rewrite counts are invalid")
if rewrite.get("raw_transaction_fallbacks", 0) != 0 or rewrite.get("owned_outer_fallbacks", 0) != 0: fail("candidate rewrite contains a fallback")

old_files = descriptor.get("source_files", {})
new_files = descriptor.get("candidate_rewrite_files", {})
for name in ("archive-v2-blocks.zstd", "archive-v2-blocks.index"):
    if not binding(old_files.get(name)) or not binding(new_files.get(name)): fail("candidate pair binding is invalid")
if not stat.S_ISDIR(os.lstat(backup).st_mode) or os.path.lexists(staging): fail("candidate backup or staging state is invalid")
intent_path = os.path.join(backup, "archive-v2-pre-to-post.switch-intent.v1.json")
complete_path = os.path.join(backup, "archive-v2-pre-to-post.switch-complete.v1.json")
intent, intent_raw, intent_sha = regular_json(intent_path)
complete, complete_raw, complete_sha = regular_json(complete_path)
if intent.get("schema_version") != 1 or intent.get("kind") != "archive-v2-pre-to-post-pair-swap-intent" or intent.get("epoch") != epoch or intent.get("cluster_id") != "mainnet-beta": fail("candidate switch intent is invalid")
if intent.get("prospective_generation_id") != generation or intent.get("candidate") != source or intent.get("staging") != staging or intent.get("backup") != backup: fail("candidate switch intent paths are invalid")
if intent.get("candidate_descriptor") != {"bytes": len(descriptor_raw), "sha256": descriptor_sha}: fail("candidate intent descriptor binding is invalid")
if intent.get("source_audit_report_path") != audit_path or intent.get("source_audit_report") != {"bytes": audit_bytes, "sha256": audit_sha}: fail("candidate intent audit binding is invalid")
if intent.get("source_blocks_binding") != old_files["archive-v2-blocks.zstd"] or intent.get("source_index_binding") != old_files["archive-v2-blocks.index"]: fail("candidate intent source binding is invalid")
if intent.get("candidate_blocks_binding") != new_files["archive-v2-blocks.zstd"] or intent.get("candidate_index_binding") != new_files["archive-v2-blocks.index"]: fail("candidate intent target binding is invalid")
if intent.get("moved_to_backup") != descriptor.get("moved_to_backup") or intent.get("retained_edge_files") != descriptor.get("retained_edge_files"): fail("candidate disposition lists differ")
if complete.get("schema_version") != 1 or complete.get("kind") != "archive-v2-pre-to-post-pair-swap-complete" or complete.get("epoch") != epoch or complete.get("canonical") is not False: fail("candidate switch completion is invalid")
if complete.get("candidate") != source or complete.get("backup") != backup or complete.get("intent_sha256") != intent_sha or complete.get("candidate_descriptor_sha256") != descriptor_sha or complete.get("source_audit_report_sha256") != audit_sha: fail("candidate completion control binding is invalid")
for label, expected in (("source_blocks_sha256", old_files["archive-v2-blocks.zstd"]["sha256"]), ("source_index_sha256", old_files["archive-v2-blocks.index"]["sha256"]), ("candidate_blocks_sha256", new_files["archive-v2-blocks.zstd"]["sha256"]), ("candidate_index_sha256", new_files["archive-v2-blocks.index"]["sha256"])):
    if complete.get(label) != expected: fail("candidate completion payload binding is invalid")
for name, value in (("archive-v2-blocks.zstd", new_files["archive-v2-blocks.zstd"]), ("archive-v2-blocks.index", new_files["archive-v2-blocks.index"])): real_file_size(os.path.join(source, name), value["bytes"])
for name, value in (("archive-v2-blocks.zstd", old_files["archive-v2-blocks.zstd"]), ("archive-v2-blocks.index", old_files["archive-v2-blocks.index"])): real_file_size(os.path.join(backup, name), value["bytes"])
exact_identity(os.path.join(source, "archive-v2-blocks.zstd"), intent.get("candidate_blocks"))
exact_identity(os.path.join(source, "archive-v2-blocks.index"), intent.get("candidate_index"))
exact_identity(os.path.join(backup, "archive-v2-blocks.zstd"), intent.get("source_blocks"))
exact_identity(os.path.join(backup, "archive-v2-blocks.index"), intent.get("source_index"))
durable = descriptor.get("retained_durable_files")
required_durable = {"archive-v2-meta.wincode", "registry.bin", "registry.mphf", "signatures.bin"}
if not isinstance(durable, list) or len(durable) != len(set(durable)) or not required_durable.issubset(durable): fail("candidate durable disposition is invalid")
for name in descriptor.get("moved_to_backup", []):
    if not isinstance(name, str) or not name or "/" in name or "\\" in name or os.path.lexists(os.path.join(source, name)) or not os.path.lexists(os.path.join(backup, "disabled", name)): fail("candidate moved-file evidence is invalid")
for name in descriptor.get("retained_edge_files", []):
    if not isinstance(name, str) or not name or "/" in name or "\\" in name or not os.path.lexists(os.path.join(source, name)): fail("candidate retained edge is invalid")
bundle = hashlib.sha256()
bundle.update(b"blockzilla.firewatch.candidate-control-bundle.v1")
for value in (descriptor_sha, audit_sha, intent_sha, complete_sha): bundle.update(value.encode("ascii"))
print(generation + " " + bundle.hexdigest())
PY
}

approved_reuse() {
    case "$1" in
        305) printf '%s' 'c5c8e832836dcec8f75f1ea5e6c126721c5243ab414990b7699916a93d8541a0 2df770f140ad589299f592d1014b4b5300cb46d6b290b0d9d827920b9cff6d5c' ;;
        404) printf '%s' 'c02bda03b1a4ab7c04d26638e1477214fcf4d67d3d7346b0c32423aad3e823bf 556535e1c0982debc83ca400738af626960e89dd14df0cb4451bffadb8724017' ;;
        405) printf '%s' '5dcca8353f1fe1106b2fd01ca4d0ec35755576ff19a52afb5524bd6f103da02e e115dbcfcedc5fafaf680f22abbb64842df9b6611604293117ad86820f0c9130' ;;
        501) printf '%s' '532b431f472e99a3fb98145a3dd5084b7bc5229124e3cb18d5c330235ff17bd4 06773ca0777c0f5c6079e43346dedec9de8d1d3a99f3a8cef586f58628569a23' ;;
        502) printf '%s' 'e76b854d535fddd9742dd1e925fea75874621479dee9b80a96e9e3bdbb68bbe2 07a3aa02645a2831146626068fa816ce386bf4f82bc11de33408a224f4f4f98e' ;;
        503) printf '%s' '7640a6a55790999d5594e313d9679116aa555c908df23774cf3621009ada1fac 8160572f36f9566339b05ae4c9ca3ba9d30b0189ea2bf708477b9db6b0aecaa2' ;;
        504) printf '%s' '45aad44a38fe9a7e273a1c106779101d8aa6e6fcc2417fa043c16e35ed06daf4 f710016e09e955e02d06eb9fd6a399be855ae5993c2a23369568f7e273b6734b' ;;
        505) printf '%s' '9a22c92fd8561dd3acf6471507932f885ce395855b6659f25fce6193bdfd7782 b69a3403a7a0ba89f461df3820046578bd24de660845138d44d7a033a467f337' ;;
        997) printf '%s' 'e8d0be7d7fec4d090fb31d91fca69288d53fb74e16d3eef83839cb261203af63 65dd72786a7abc3b127a8ca3906f8795a7b0ecf7aee67272811248c2e9516a9d' ;;
        1001) printf '%s' '9c6f6393ca57cd9500c6d611214e521cadac3576d38aae7acc952a878476e465 6c00c423cd0adf3faabb4dcd1024f04f1a5dab41dc798c2fcd6d562154b7b637' ;;
        1002) printf '%s' '6b0b8638ffd8749fe347660127e93b928548464b54912fa1928f83dbeba22d54 eed513169acd1f1d3e64b5902105524f4b520b6a796b3862974b23844c2e2c54' ;;
        1003) printf '%s' '81af17d18c9d3973c8a027a21b1f902be76b3b0309cd2307bfb20214f7b963ba b22cf4064e9d58b58ff2333142d0dec52b7338890cae3b5c5538d2327740bf2e' ;;
        1004) printf '%s' 'a53a6f81a7aca008856207e6bc51ffd121ca3a4edd53b6e09cdb59f6c9403ebe ae020d2befbf75344c8ef15ae92bae59de83d1f1a6f6794861e678976ee7b6cc' ;;
        1005) printf '%s' 'd0100be8bb8b37fd85517934743e42ec09d85edf12bf5f4109e1982fd0ceb352 f2c43fc2886d598162cd1b8ab8f0b3b7ab777c186475668d75a0d774bc2ef821' ;;
        1006) printf '%s' '04af4f4dcf2f70e92fe7da13382bc6175329c7304ec3dc2c41bfc68dbd64ecdc 928156eca9911f464edeeb6a8704cbaa00c68c9f4320dc3354c9f1c63b2e391d' ;;
        1008) printf '%s' '848a856846c153642306bc08a591228d510b01b7d19bc6300318d28e19eb10bb 1af2d43d8657c557654cc01f0ae55981469a0ce9d45ce695474cdb462e75910f' ;;
        *) return 1 ;;
    esac
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

manifest_matches() {
    mm_manifest=$1
    mm_epoch=$2
    mm_source=$3
    mm_generation=$4
    [ -f "$mm_manifest" ] && [ ! -L "$mm_manifest" ] || return 1
    jq -e \
        --argjson epoch "$mm_epoch" \
        --arg source "$mm_source" \
        --arg generation "$mm_generation" \
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
        "$mm_manifest" >/dev/null 2>&1 || return 1
    registry_identity_matches "$mm_manifest" "$mm_source"
}

epoch=$first_epoch
tab=$(printf '\t')
newline='
'
candidate_epochs=
usage_sorted_epochs=
direct_epochs=
while [ "$epoch" -le "$last_epoch" ]; do
    canonical=$archive_root/epoch-$epoch
    [ -d "$canonical" ] && [ ! -L "$canonical" ] || {
        echo "canonical epoch directory is absent or not real: $canonical" >&2
        exit 1
    }

    kind=direct
    source=$canonical
    control=-
    control_sha=-
    generation=

    first_seen=$canonical/registry-first-seen.manifest
    candidate_control=$canonical/archive-v2-pre-to-post.candidate.v1.json
    if [ -e "$first_seen" ] || [ -L "$first_seen" ]; then
        [ -f "$first_seen" ] && [ ! -L "$first_seen" ] || {
            echo "first-seen control is not a real regular file for epoch $epoch" >&2
            exit 1
        }
        kind=usage_sorted
        source=$archive_root/.usage-sorted-generations/epoch-$epoch
        control=$source/archive-v2-registry-reprocess.receipt.json
        [ -d "$source" ] && [ ! -L "$source" ] \
            && [ -f "$control" ] && [ ! -L "$control" ] || {
            echo "usage-sorted target or receipt is absent for epoch $epoch" >&2
            exit 1
        }
        source=$(realpath "$source")
        generation=$(usage_sorted_generation "$control" "$epoch" "$canonical" "$source") || {
            echo "invalid usage-sorted receipt for epoch $epoch" >&2
            exit 1
        }
        receipt_sha=$(hash_file "$control")
        first_seen_sha=$(hash_file "$first_seen")
        control_sha=$(hash_text "blockzilla.firewatch.usage-sorted-control-bundle.v1
$receipt_sha
$first_seen_sha
")
    elif [ -e "$candidate_control" ] || [ -L "$candidate_control" ]; then
        [ -f "$candidate_control" ] && [ ! -L "$candidate_control" ] || {
            echo "candidate control is not a real regular file for epoch $epoch" >&2
            exit 1
        }
        kind=candidate
        control=$candidate_control
        candidate_values=$(candidate_generation "$control" "$epoch" "$canonical") || {
            echo "invalid Pre-to-Post candidate descriptor for epoch $epoch" >&2
            exit 1
        }
        generation=${candidate_values%% *}
        control_sha=${candidate_values#* }
    fi

    source=$(realpath "$source")
    authority=$(source_authority "$source")

    if [ "$kind" = direct ]; then
        generation=firewatch-post-local-v2-epoch-$epoch-$authority
        output_key=$(hash_text "$source	$generation	$authority
")
        output=$output_root/epoch-$epoch/current-post-v2-$output_key
    elif [ "$kind" = usage_sorted ] && reuse=$(approved_reuse "$epoch"); then
        reuse_authority=${reuse%% *}
        reuse_generation=${reuse#* }
        [ "$authority" = "$reuse_authority" ] && [ "$generation" = "$reuse_generation" ] || {
            echo "the approved source authority changed for epoch $epoch" >&2
            exit 1
        }
        output=$output_root/epoch-$epoch/target-usage-sorted-$generation
        manifest_matches "$output/manifest.json" "$epoch" "$source" "$generation" || {
            echo "the approved current index is absent or invalid for epoch $epoch" >&2
            exit 1
        }
    else
        output_key=$(hash_text "$source	$generation	$authority
")
        output=$output_root/epoch-$epoch/current-post-v2-$output_key
    fi

    case "$kind:$source:$generation:$authority:$control:$control_sha:$output" in
        *"$tab"*|*"$newline"*)
            echo "plan value contains a forbidden delimiter at epoch $epoch" >&2
            exit 1
            ;;
    esac
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$epoch" "$kind" "$source" "$generation" "$authority" "$control" "$control_sha" "$output"
    case "$kind" in
        candidate) candidate_epochs=$candidate_epochs$epoch'
' ;;
        usage_sorted) usage_sorted_epochs=$usage_sorted_epochs$epoch'
' ;;
        direct) direct_epochs=$direct_epochs$epoch'
' ;;
        *) exit 1 ;;
    esac
    epoch=$((epoch + 1))
done

if [ "$first_epoch" -eq 0 ] && [ "$last_epoch" -eq 1018 ]; then
    [ "$(hash_text "$candidate_epochs")" = 331f0601eacd7ed881e7e500c09db884886303ff625e7f41b11e6666dfe00f60 ] \
        && [ "$(hash_text "$usage_sorted_epochs")" = d62fcd7a4eeff906bedddeec2656ae1c7e8324ba82804d0c8ae3f99a0f6c02e8 ] \
        && [ "$(hash_text "$direct_epochs")" = 92250ee2923bfec8f46efe21c3e5e732db8012be6c4a49b7de0b3cd418c70de4 ] || {
        echo "the exact all-epoch source classification changed" >&2
        exit 1
    }
fi
