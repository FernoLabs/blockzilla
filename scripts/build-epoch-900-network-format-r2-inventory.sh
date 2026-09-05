#!/bin/sh
# Build the fixed, manifest-free R2 publication inventory for epoch 900.
# This tool reads directory entries and file sizes only. It does not read file
# bodies, calculate hashes, use the network, or change remote state.

set -eu
umask 077
LC_ALL=C
export LC_ALL

program=${0##*/}
release_id=e900-current-typed-errors-v1
compact_scope=compact-v2/releases/$release_id
v3_scope=indexer-v3/releases/$release_id
tab=$(printf '\t')

die() {
    echo "$program: $*" >&2
    exit 1
}

usage() {
    cat >&2 <<EOF
Usage:
  $program \\
    --compact-dir /absolute/canonical/compact-directory \\
    --v3-dir /absolute/canonical/v3-directory \\
    --output /absolute/new/path/epoch-900-r2-inventory.tsv

This dedicated builder always writes release $release_id under:
  $compact_scope
  $v3_scope

The output is created once. An existing file is never replaced.
EOF
    exit 2
}

require_command() {
    command -v "$1" >/dev/null 2>&1 \
        || die "required command is not available: $1"
}

canonical_directory() (
    CDPATH=
    cd -P "$1" 2>/dev/null || exit 1
    pwd -P
)

file_size() {
    fs_path=$1
    if fs_bytes=$(stat -f %z "$fs_path" 2>/dev/null); then
        printf '%s\n' "$fs_bytes"
    elif fs_bytes=$(stat -c %s "$fs_path" 2>/dev/null); then
        printf '%s\n' "$fs_bytes"
    else
        return 1
    fi
}

require_absolute_directory() {
    rad_label=$1
    rad_path=$2
    case "$rad_path" in
        /*) ;;
        *) die "$rad_label must be an absolute path: $rad_path" ;;
    esac
    case "$rad_path" in
        *"$tab"*|*'
'*) die "$rad_label must not contain a tab or line break" ;;
    esac
    [ -d "$rad_path" ] && [ ! -L "$rad_path" ] \
        || die "$rad_label is not one real directory: $rad_path"
    rad_real=$(canonical_directory "$rad_path") \
        || die "cannot canonicalize $rad_label: $rad_path"
    [ "$rad_path" = "$rad_real" ] \
        || die "$rad_label path must already be canonical: $rad_real"
}

require_object_size() {
    ros_directory=$1
    ros_name=$2
    ros_expected=$3
    ros_requirement=$4
    ros_path=$ros_directory/$ros_name
    if [ ! -e "$ros_path" ] && [ ! -L "$ros_path" ]; then
        [ "$ros_requirement" = optional ] && return
        die "required object is absent: $ros_path"
    fi
    [ -f "$ros_path" ] && [ ! -L "$ros_path" ] \
        || die "object is not one real file: $ros_path"
    ros_actual=$(file_size "$ros_path") \
        || die "cannot read object size: $ros_path"
    [ "$ros_actual" = "$ros_expected" ] \
        || die "object has wrong size: $ros_path (expected $ros_expected, found $ros_actual)"
}

compact_dir=
v3_dir=
output=
while [ "$#" -gt 0 ]; do
    case "$1" in
        --compact-dir|--v3-dir|--output)
            [ "$#" -ge 2 ] || usage
            option=$1
            value=$2
            shift 2
            case "$option" in
                --compact-dir)
                    [ -z "$compact_dir" ] || usage
                    compact_dir=$value
                    ;;
                --v3-dir)
                    [ -z "$v3_dir" ] || usage
                    v3_dir=$value
                    ;;
                --output)
                    [ -z "$output" ] || usage
                    output=$value
                    ;;
            esac
            ;;
        -h|--help|help) usage ;;
        *) usage ;;
    esac
done

[ -n "$compact_dir" ] && [ -n "$v3_dir" ] && [ -n "$output" ] \
    || usage

for required in awk basename chmod dirname find ln mktemp rm stat; do
    require_command "$required"
done

require_absolute_directory COMPACT_DIR "$compact_dir"
require_absolute_directory V3_DIR "$v3_dir"
[ "$compact_dir" != "$v3_dir" ] \
    || die "COMPACT_DIR and V3_DIR must be different directories"

case "$output" in
    /*) ;;
    *) die "OUTPUT must be an absolute path: $output" ;;
esac
case "$output" in
    *"$tab"*|*'
'*) die "OUTPUT must not contain a tab or line break" ;;
esac
output_parent=$(dirname "$output")
output_base=$(basename "$output")
[ -d "$output_parent" ] && [ ! -L "$output_parent" ] \
    || die "OUTPUT parent is not one real directory: $output_parent"
output_parent_real=$(canonical_directory "$output_parent") \
    || die "cannot canonicalize OUTPUT parent: $output_parent"
if [ "$output_parent_real" = / ]; then
    expected_output=/$output_base
else
    expected_output=$output_parent_real/$output_base
fi
[ "$output" = "$expected_output" ] \
    || die "OUTPUT path must already be canonical: $expected_output"
case "$output" in
    "$compact_dir"/*|"$v3_dir"/*)
        die "OUTPUT must not be inside either release source directory"
        ;;
esac
[ ! -e "$output" ] && [ ! -L "$output" ] \
    || die "OUTPUT already exists; refusing to replace it: $output"

source_listing=$(mktemp "$output_parent/.epoch-900-source-list.XXXXXX") \
    || die "cannot create temporary source listing"
cleanup_source_listing() {
    cleanup_status=$?
    trap - 0 1 2 3 15
    rm -f "$source_listing"
    exit "$cleanup_status"
}
trap 'cleanup_source_listing' 0
trap 'exit 129' 1
trap 'exit 130' 2
trap 'exit 131' 3
trap 'exit 143' 15

# Private evidence directories and the two named normalization records can
# stay beside Compact files. They never become inventory rows.
find "$compact_dir" -mindepth 1 -maxdepth 1 -print >"$source_listing" \
    || die "cannot list COMPACT_DIR"
while IFS= read -r path; do
    name=${path##*/}
    [ ! -L "$path" ] || die "COMPACT_DIR contains a symbolic link: $path"
    if [ -d "$path" ]; then
        case "$name" in
            evidence|reports) ;;
            *) die "COMPACT_DIR contains an unexpected directory: $path" ;;
        esac
    elif [ -f "$path" ]; then
        case "$name" in
            archive-v2-blocks.index|archive-v2-blocks.zstd|archive-v2-meta.wincode|blockhash_registry.bin|poh.wincode|prev_blockhash_tail.bin|registry.bin|registry.mphf|registry_counts.bin|shredding.wincode|signatures.bin|vote_hash_registry.bin|archive-v2-metadata-normalization.candidate.v1.json|archive-v2-metadata-normalization.receipt.v1.json) ;;
            *) die "COMPACT_DIR contains an unexpected file: $path" ;;
        esac
    else
        die "COMPACT_DIR contains an unexpected object type: $path"
    fi
done <"$source_listing"

# V3 reports and evidence remain private. Optional local copies of the nine
# retained files are checked but are never uploaded from V3_DIR.
find "$v3_dir" -mindepth 1 -maxdepth 1 -print >"$source_listing" \
    || die "cannot list V3_DIR"
while IFS= read -r path; do
    name=${path##*/}
    [ ! -L "$path" ] || die "V3_DIR contains a symbolic link: $path"
    if [ -d "$path" ]; then
        case "$name" in
            evidence|reports) ;;
            *) die "V3_DIR contains an unexpected directory: $path" ;;
        esac
    elif [ -f "$path" ]; then
        case "$name" in
            archive-v2-meta.wincode|archive-v2-retained-sidecars.candidate.json|archive-v2-standalone-account-postings-adaptive-v3.control|archive-v2-standalone-account-postings-adaptive-v3.coverage|archive-v2-standalone-account-postings-adaptive-v3.pages|archive-v2-standalone-balances.wincode|archive-v2-standalone-block-rewards.wincode|archive-v2-standalone-blocks.index|archive-v2-standalone-inner-instructions.wincode|archive-v2-standalone-loaded-addresses.wincode|archive-v2-standalone-logs.wincode|archive-v2-standalone-messages.wincode|archive-v2-standalone-outcomes.wincode|archive-v2-standalone-raw-metadata-fallbacks.wincode|archive-v2-standalone-token-balances.wincode|archive-v2-standalone-transaction-directory.wincode|archive-v2-standalone-transaction-rewards.wincode|blockhash_registry.bin|poh.wincode|prev_blockhash_tail.bin|registry.bin|registry.mphf|shredding.wincode|signatures.bin|vote_hash_registry.bin|benchmark-report.json|archive-v2-standalone-account-postings-adaptive-v3.report.json) ;;
            *) die "V3_DIR contains an unexpected file: $path" ;;
        esac
    else
        die "V3_DIR contains an unexpected object type: $path"
    fi
done <"$source_listing"

rm -f "$source_listing"
source_listing=
trap - 0 1 2 3 15

# Compact V2: twelve fixed serving objects.
require_object_size "$compact_dir" archive-v2-blocks.index 22456652 required
require_object_size "$compact_dir" archive-v2-blocks.zstd 59899113036 required
require_object_size "$compact_dir" archive-v2-meta.wincode 66 required
require_object_size "$compact_dir" blockhash_registry.bin 13819456 required
require_object_size "$compact_dir" poh.wincode 9681441209 required
require_object_size "$compact_dir" prev_blockhash_tail.bin 12000 required
require_object_size "$compact_dir" registry.bin 889551808 required
require_object_size "$compact_dir" registry.mphf 341082690 required
require_object_size "$compact_dir" registry_counts.bin 28366914 required
require_object_size "$compact_dir" shredding.wincode 792857572 required
require_object_size "$compact_dir" signatures.bin 32380385536 required
require_object_size "$compact_dir" vote_hash_registry.bin 28070770 required

# V3: sixteen local files. The final nine keys come from staged Compact data.
require_object_size "$v3_dir" archive-v2-retained-sidecars.candidate.json 1527 required
require_object_size "$v3_dir" archive-v2-standalone-account-postings-adaptive-v3.control 120 required
require_object_size "$v3_dir" archive-v2-standalone-account-postings-adaptive-v3.coverage 46512 required
require_object_size "$v3_dir" archive-v2-standalone-account-postings-adaptive-v3.pages 4688130905 required
require_object_size "$v3_dir" archive-v2-standalone-balances.wincode 10248823687 required
require_object_size "$v3_dir" archive-v2-standalone-block-rewards.wincode 31853726 required
require_object_size "$v3_dir" archive-v2-standalone-blocks.index 107100848 required
require_object_size "$v3_dir" archive-v2-standalone-inner-instructions.wincode 12373908023 required
require_object_size "$v3_dir" archive-v2-standalone-loaded-addresses.wincode 990259848 required
require_object_size "$v3_dir" archive-v2-standalone-logs.wincode 13733124021 required
require_object_size "$v3_dir" archive-v2-standalone-messages.wincode 13201297110 required
require_object_size "$v3_dir" archive-v2-standalone-outcomes.wincode 1128420825 required
require_object_size "$v3_dir" archive-v2-standalone-raw-metadata-fallbacks.wincode 64 required
require_object_size "$v3_dir" archive-v2-standalone-token-balances.wincode 4350091593 required
require_object_size "$v3_dir" archive-v2-standalone-transaction-directory.wincode 1341913145 required
require_object_size "$v3_dir" archive-v2-standalone-transaction-rewards.wincode 64 required

require_object_size "$v3_dir" archive-v2-meta.wincode 66 optional
require_object_size "$v3_dir" blockhash_registry.bin 13819456 optional
require_object_size "$v3_dir" poh.wincode 9681441209 optional
require_object_size "$v3_dir" prev_blockhash_tail.bin 12000 optional
require_object_size "$v3_dir" registry.bin 889551808 optional
require_object_size "$v3_dir" registry.mphf 341082690 optional
require_object_size "$v3_dir" shredding.wincode 792857572 optional
require_object_size "$v3_dir" signatures.bin 32380385536 optional
require_object_size "$v3_dir" vote_hash_registry.bin 28070770 optional

candidate=$(mktemp "$output_parent/.epoch-900-r2-inventory.XXXXXX") \
    || die "cannot create temporary inventory beside OUTPUT"
cleanup_candidate() {
    cleanup_status=$?
    trap - 0 1 2 3 15
    rm -f "$candidate"
    exit "$cleanup_status"
}
trap 'cleanup_candidate' 0
trap 'exit 129' 1
trap 'exit 130' 2
trap 'exit 131' 3
trap 'exit 143' 15

printf 'role\tsource_kind\tsource\ttarget_key\tbytes\n' >"$candidate"

append_local() {
    al_role=$1
    al_directory=$2
    al_scope=$3
    al_name=$4
    al_bytes=$5
    printf '%s\tlocal\t%s/%s\t%s/%s\t%s\n' \
        "$al_role" "$al_directory" "$al_name" "$al_scope" "$al_name" "$al_bytes" \
        >>"$candidate"
}

append_staged_copy() {
    asc_name=$1
    asc_bytes=$2
    printf 'payload\tstaged-copy\t%s/%s\t%s/%s\t%s\n' \
        "$compact_scope" "$asc_name" "$v3_scope" "$asc_name" "$asc_bytes" \
        >>"$candidate"
}

# All payload rows precede both small V3 control rows.
append_local payload "$compact_dir" "$compact_scope" archive-v2-blocks.index 22456652
append_local payload "$compact_dir" "$compact_scope" archive-v2-blocks.zstd 59899113036
append_local payload "$compact_dir" "$compact_scope" archive-v2-meta.wincode 66
append_local payload "$compact_dir" "$compact_scope" blockhash_registry.bin 13819456
append_local payload "$compact_dir" "$compact_scope" poh.wincode 9681441209
append_local payload "$compact_dir" "$compact_scope" prev_blockhash_tail.bin 12000
append_local payload "$compact_dir" "$compact_scope" registry.bin 889551808
append_local payload "$compact_dir" "$compact_scope" registry.mphf 341082690
append_local payload "$compact_dir" "$compact_scope" registry_counts.bin 28366914
append_local payload "$compact_dir" "$compact_scope" shredding.wincode 792857572
append_local payload "$compact_dir" "$compact_scope" signatures.bin 32380385536
append_local payload "$compact_dir" "$compact_scope" vote_hash_registry.bin 28070770

append_local payload "$v3_dir" "$v3_scope" archive-v2-standalone-account-postings-adaptive-v3.coverage 46512
append_local payload "$v3_dir" "$v3_scope" archive-v2-standalone-account-postings-adaptive-v3.pages 4688130905
append_local payload "$v3_dir" "$v3_scope" archive-v2-standalone-balances.wincode 10248823687
append_local payload "$v3_dir" "$v3_scope" archive-v2-standalone-block-rewards.wincode 31853726
append_local payload "$v3_dir" "$v3_scope" archive-v2-standalone-blocks.index 107100848
append_local payload "$v3_dir" "$v3_scope" archive-v2-standalone-inner-instructions.wincode 12373908023
append_local payload "$v3_dir" "$v3_scope" archive-v2-standalone-loaded-addresses.wincode 990259848
append_local payload "$v3_dir" "$v3_scope" archive-v2-standalone-logs.wincode 13733124021
append_local payload "$v3_dir" "$v3_scope" archive-v2-standalone-messages.wincode 13201297110
append_local payload "$v3_dir" "$v3_scope" archive-v2-standalone-outcomes.wincode 1128420825
append_local payload "$v3_dir" "$v3_scope" archive-v2-standalone-raw-metadata-fallbacks.wincode 64
append_local payload "$v3_dir" "$v3_scope" archive-v2-standalone-token-balances.wincode 4350091593
append_local payload "$v3_dir" "$v3_scope" archive-v2-standalone-transaction-directory.wincode 1341913145
append_local payload "$v3_dir" "$v3_scope" archive-v2-standalone-transaction-rewards.wincode 64

append_staged_copy archive-v2-meta.wincode 66
append_staged_copy blockhash_registry.bin 13819456
append_staged_copy poh.wincode 9681441209
append_staged_copy prev_blockhash_tail.bin 12000
append_staged_copy registry.bin 889551808
append_staged_copy registry.mphf 341082690
append_staged_copy shredding.wincode 792857572
append_staged_copy signatures.bin 32380385536
append_staged_copy vote_hash_registry.bin 28070770

append_local control "$v3_dir" "$v3_scope" archive-v2-standalone-account-postings-adaptive-v3.control 120
append_local control "$v3_dir" "$v3_scope" archive-v2-retained-sidecars.candidate.json 1527

awk -F '\t' '
    NR == 1 {
        if ($0 != "role\tsource_kind\tsource\ttarget_key\tbytes") exit 1
        next
    }
    NF != 5 { exit 1 }
    {
        if ($1 == "payload") rank = 1
        else if ($1 == "control") rank = 2
        else if ($1 == "completion") rank = 3
        else exit 1
        if (rank < prior) exit 1
        prior = rank
        if ($2 != "local" && $2 != "staged-copy") exit 1
        if (seen[$4]++) exit 1
        count++
        roles[$1]++
        kinds[$2]++
        bytes += $5
        if ($4 ~ /^compact-v2\//) {
            compact_count++
            compact_bytes += $5
        } else if ($4 ~ /^indexer-v3\//) {
            v3_count++
            v3_bytes += $5
        } else exit 1
        if ($2 == "local") local_bytes += $5
        else copy_bytes += $5
    }
    END {
        if (count != 37 || roles["payload"] != 35 ||
            roles["control"] != 2 || roles["completion"] != 0 ||
            kinds["local"] != 28 || kinds["staged-copy"] != 9 ||
            compact_count != 12 || compact_bytes != 104077157709 ||
            v3_count != 25 || v3_bytes != 106322193125 ||
            bytes != 210399350834 || local_bytes != 166272129727 ||
            copy_bytes != 44127221107) exit 1
    }
' "$candidate" || die "internal inventory validation failed"

chmod 400 "$candidate" || die "cannot make candidate inventory read-only"
[ ! -e "$output" ] && [ ! -L "$output" ] \
    || die "OUTPUT appeared during validation; refusing to replace it: $output"
ln "$candidate" "$output" \
    || die "cannot publish OUTPUT with an atomic no-clobber hard link: $output"
[ -f "$output" ] && [ ! -L "$output" ] && [ "$output" -ef "$candidate" ] \
    || die "published OUTPUT is not the validated candidate: $output"

echo "created immutable manifest-free $release_id inventory: $output"
