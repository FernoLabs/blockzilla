#!/bin/sh

set -eu
umask 077
LC_ALL=C
export LC_ALL

die() {
    echo "epoch-900 R2 inventory builder test: $*" >&2
    exit 1
}

script_dir=$(CDPATH= cd -P "$(dirname "$0")" && pwd -P)
builder=$script_dir/build-epoch-900-network-format-r2-inventory.sh

sh -n "$builder" || die "builder does not pass sh -n"
if command -v dash >/dev/null 2>&1; then
    dash -n "$builder" || die "builder does not pass dash -n"
    test_shell=dash
else
    test_shell=sh
fi
command -v truncate >/dev/null 2>&1 || die "truncate is required"

test_root=$(mktemp -d "${TMPDIR:-/tmp}/blockzilla-e900-inventory-test.XXXXXX") \
    || die "cannot create test root"
test_root=$(CDPATH= cd -P "$test_root" && pwd -P)
case "${test_root##*/}" in
    blockzilla-e900-inventory-test.*) ;;
    *) die "unsafe test root" ;;
esac
cleanup() {
    cleanup_status=$?
    trap - 0 1 2 3 15
    rm -rf "$test_root"
    exit "$cleanup_status"
}
trap 'cleanup' 0
trap 'exit 129' 1
trap 'exit 130' 2
trap 'exit 131' 3
trap 'exit 143' 15

compact=$test_root/compact
v3=$test_root/v3
out=$test_root/out
mkdir "$compact" "$v3" "$out"

make_sparse() {
    truncate -s "$2" "$1"
}

make_compact_fixture() {
    make_sparse "$compact/archive-v2-blocks.index" 22456652
    make_sparse "$compact/archive-v2-blocks.zstd" 59899113036
    make_sparse "$compact/archive-v2-meta.wincode" 66
    make_sparse "$compact/blockhash_registry.bin" 13819456
    make_sparse "$compact/poh.wincode" 9681441209
    make_sparse "$compact/prev_blockhash_tail.bin" 12000
    make_sparse "$compact/registry.bin" 889551808
    make_sparse "$compact/registry.mphf" 341082690
    make_sparse "$compact/registry_counts.bin" 28366914
    make_sparse "$compact/shredding.wincode" 792857572
    make_sparse "$compact/signatures.bin" 32380385536
    make_sparse "$compact/vote_hash_registry.bin" 28070770
}

make_v3_fixture() {
    make_sparse "$v3/archive-v2-retained-sidecars.candidate.json" 1527
    make_sparse "$v3/archive-v2-standalone-account-postings-adaptive-v3.control" 120
    make_sparse "$v3/archive-v2-standalone-account-postings-adaptive-v3.coverage" 46512
    make_sparse "$v3/archive-v2-standalone-account-postings-adaptive-v3.pages" 4688130905
    make_sparse "$v3/archive-v2-standalone-balances.wincode" 10248823687
    make_sparse "$v3/archive-v2-standalone-block-rewards.wincode" 31853726
    make_sparse "$v3/archive-v2-standalone-blocks.index" 107100848
    make_sparse "$v3/archive-v2-standalone-inner-instructions.wincode" 12373908023
    make_sparse "$v3/archive-v2-standalone-loaded-addresses.wincode" 990259848
    make_sparse "$v3/archive-v2-standalone-logs.wincode" 13733124021
    make_sparse "$v3/archive-v2-standalone-messages.wincode" 13201297110
    make_sparse "$v3/archive-v2-standalone-outcomes.wincode" 1128420825
    make_sparse "$v3/archive-v2-standalone-raw-metadata-fallbacks.wincode" 64
    make_sparse "$v3/archive-v2-standalone-token-balances.wincode" 4350091593
    make_sparse "$v3/archive-v2-standalone-transaction-directory.wincode" 1341913145
    make_sparse "$v3/archive-v2-standalone-transaction-rewards.wincode" 64
}

run_builder() {
    "$test_shell" "$builder" \
        --compact-dir "$compact" \
        --v3-dir "$v3" \
        --output "$1"
}

make_compact_fixture
make_v3_fixture

# These named private records and directories are permitted but are excluded.
make_sparse "$compact/archive-v2-metadata-normalization.candidate.v1.json" 10
make_sparse "$compact/archive-v2-metadata-normalization.receipt.v1.json" 11
mkdir "$compact/evidence" "$v3/reports"
make_sparse "$v3/benchmark-report.json" 13
make_sparse "$v3/archive-v2-standalone-account-postings-adaptive-v3.report.json" 14

inventory=$out/epoch-900.tsv
run_builder "$inventory" >/dev/null

[ -f "$inventory" ] && [ ! -L "$inventory" ] \
    || die "builder did not create one real inventory"
[ "$(wc -l <"$inventory" | tr -d '[:space:]')" = 38 ] \
    || die "inventory does not have one header and 37 data rows"

awk -F '\t' '
    NR == 1 {
        if ($0 != "role\tsource_kind\tsource\ttarget_key\tbytes") exit 1
        next
    }
    $1 == "payload" { payload++ }
    $1 == "control" { control++ }
    $1 == "completion" { completion++ }
    $2 == "local" { local++; local_bytes += $5 }
    $2 == "staged-copy" { copies++; copy_bytes += $5 }
    $4 ~ /^compact-v2\/releases\/e900-current-typed-errors-v1\// {
        compact++
        compact_bytes += $5
    }
    $4 ~ /^indexer-v3\/releases\/e900-current-typed-errors-v1\// {
        v3++
        v3_bytes += $5
    }
    { total += $5 }
    END {
        exit !(payload == 35 && control == 2 && completion == 0 &&
            local == 28 && copies == 9 && compact == 12 && v3 == 25 &&
            compact_bytes == 104077157709 && v3_bytes == 106322193125 &&
            total == 210399350834 && local_bytes == 166272129727 &&
            copy_bytes == 44127221107)
    }
' "$inventory" || die "inventory counts or byte totals are wrong"

expected_copies=$(printf '%s\n' \
    archive-v2-meta.wincode \
    blockhash_registry.bin \
    poh.wincode \
    prev_blockhash_tail.bin \
    registry.bin \
    registry.mphf \
    shredding.wincode \
    signatures.bin \
    vote_hash_registry.bin)
actual_copies=$(awk -F '\t' '$2 == "staged-copy" {
    name = $4
    sub(/^indexer-v3\/releases\/e900-current-typed-errors-v1\//, "", name)
    if ($3 != "compact-v2/releases/e900-current-typed-errors-v1/" name) exit 1
    print name
}' "$inventory") || die "one staged copy has the wrong source"
[ "$actual_copies" = "$expected_copies" ] \
    || die "the exact nine staged copies are absent or out of order"

expected_controls=$(printf '%s\n' \
    indexer-v3/releases/e900-current-typed-errors-v1/archive-v2-standalone-account-postings-adaptive-v3.control \
    indexer-v3/releases/e900-current-typed-errors-v1/archive-v2-retained-sidecars.candidate.json)
[ "$(awk -F '\t' '$1 == "control" { print $4 }' "$inventory")" = "$expected_controls" ] \
    || die "the two exact control rows are absent or out of order"
[ "$(tail -n 2 "$inventory" | awk -F '\t' '$1 == "control" { count++ } END { print count + 0 }')" = 2 ] \
    || die "controls do not follow every payload row"

if grep -E 'manifest|sha256|\.marker|normalization|benchmark-report|/reports/|/evidence/' "$inventory" >/dev/null 2>&1; then
    die "a manifest, hash, marker, or private evidence name entered the inventory"
fi

# The builder cannot replace an existing output.
cp "$inventory" "$out/expected.tsv"
if run_builder "$inventory" >/dev/null 2>&1; then
    die "builder replaced an existing output"
fi
cmp "$inventory" "$out/expected.tsv" >/dev/null \
    || die "existing output changed after the no-clobber test"

# A wrong size and an absent fixed object must fail.
make_sparse "$v3/archive-v2-standalone-blocks.index" 107100847
if run_builder "$out/wrong-size.tsv" >/dev/null 2>&1; then
    die "builder accepted a wrong-size V3 object"
fi
make_sparse "$v3/archive-v2-standalone-blocks.index" 107100848

mv "$compact/registry_counts.bin" "$out/registry_counts.bin.saved"
if run_builder "$out/missing.tsv" >/dev/null 2>&1; then
    die "builder accepted an absent Compact object"
fi
mv "$out/registry_counts.bin.saved" "$compact/registry_counts.bin"

# A present retained V3 copy must have its fixed Compact size.
make_sparse "$v3/signatures.bin" 1
if run_builder "$out/wrong-retained.tsv" >/dev/null 2>&1; then
    die "builder accepted a wrong-size retained V3 copy"
fi
rm "$v3/signatures.bin"

# Unreviewed files, old manifests, hashes, and symbolic links fail closed.
for forbidden in unreviewed.json benchmark-manifest.json object.sha256; do
    make_sparse "$v3/$forbidden" 1
    if run_builder "$out/forbidden-$forbidden.tsv" >/dev/null 2>&1; then
        die "builder accepted forbidden file $forbidden"
    fi
    rm "$v3/$forbidden"
done

make_sparse "$compact/archive-v2-generation.json" 1
if run_builder "$out/compact-manifest.tsv" >/dev/null 2>&1; then
    die "builder accepted the old Compact manifest"
fi
rm "$compact/archive-v2-generation.json"

ln -s "$v3/archive-v2-standalone-blocks.index" "$v3/unreviewed-link"
if run_builder "$out/symlink.tsv" >/dev/null 2>&1; then
    die "builder accepted a symbolic link"
fi
rm "$v3/unreviewed-link"

# Inventory output cannot be placed in either source tree.
if "$test_shell" "$builder" \
    --compact-dir "$compact" \
    --v3-dir "$v3" \
    --output "$compact/inventory.tsv" >/dev/null 2>&1; then
    die "builder accepted output inside COMPACT_DIR"
fi

echo "epoch-900 manifest-free R2 inventory builder tests passed"
