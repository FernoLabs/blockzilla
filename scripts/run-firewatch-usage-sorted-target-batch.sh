#!/bin/sh
set -eu

usage() {
    echo "usage: $0 USAGE_SORTED_ROOT OUTPUT_ROOT INDEXER THREADS EPOCH..." >&2
    exit 2
}

[ "$#" -ge 5 ] || usage

usage_root=$1
output_root=$2
indexer=$3
threads=$4
shift 4

case "$usage_root:$output_root:$indexer" in
    /*:/*:/*) ;;
    *) usage ;;
esac
case "$threads" in
    ''|*[!0-9]*) usage ;;
esac
[ "$threads" -ge 1 ] && [ "$threads" -le 256 ] || usage
[ -x "$indexer" ] || {
    echo "Firewatch indexer is not executable: $indexer" >&2
    exit 1
}
[ -d "$usage_root" ] && [ -d "$output_root" ] || {
    echo "Firewatch input or output root is absent" >&2
    exit 1
}

child=
stop_child() {
    if [ -n "$child" ] && kill -0 "$child" 2>/dev/null; then
        kill "$child" 2>/dev/null || true
        wait "$child" 2>/dev/null || true
    fi
    exit 143
}
trap stop_child HUP INT QUIT TERM

for epoch do
    case "$epoch" in
        ''|*[!0-9]*) usage ;;
    esac

    archive=$usage_root/epoch-$epoch
    receipt=$archive/archive-v2-registry-reprocess.receipt.json
    epoch_root=$output_root/epoch-$epoch
    [ "$(systemctl --user is-active blockzilla-archive.service || true)" = inactive ]
    [ "$(systemctl --user is-active blockzilla-firewatch-index-controller.service || true)" = inactive ]
    [ -d "$archive" ] && [ ! -L "$archive" ] \
        && [ -f "$receipt" ] && [ ! -L "$receipt" ] \
        && [ -d "$epoch_root" ] && [ ! -L "$epoch_root" ] || {
        echo "epoch $epoch: required receipt or output parent is absent" >&2
        exit 1
    }

    generation=$(jq -er '.target_generation_sha256' "$receipt")
    case "$generation" in
        ''|*[!0-9a-f]*)
            echo "epoch $epoch: invalid target generation" >&2
            exit 1
            ;;
    esac
    [ "${#generation}" -eq 64 ] || {
        echo "epoch $epoch: invalid target generation length" >&2
        exit 1
    }

    output=$epoch_root/target-usage-sorted-$generation
    if [ -d "$output" ] && [ ! -L "$output" ]; then
        echo "epoch $epoch: validating existing $generation"
    else
        [ ! -e "$output" ] && [ ! -L "$output" ] || {
            echo "epoch $epoch: output is not one real directory" >&2
            exit 1
        }
        stale=$(find "$epoch_root" -mindepth 1 -maxdepth 1 -name ".target-usage-sorted-$generation.staging-*" -print -quit)
        [ -z "$stale" ] || {
            echo "epoch $epoch: stale index staging exists: $stale" >&2
            exit 1
        }

        echo "epoch $epoch: building $generation with $threads threads"
        "$indexer" build-dense \
            --epoch "$epoch" \
            --archive "$archive" \
            --out "$output" \
            --trust-local \
            --cluster-id mainnet-beta \
            --generation-id "$generation" \
            --wire-profile post-unknown-instruction-fallbacks-v1 \
            --threads "$threads" &
        child=$!
        wait "$child"
        child=
    fi

    "$indexer" verify-index --index "$output"
    jq -e \
        --argjson epoch "$epoch" \
        --arg generation "$generation" \
        --slurpfile receipt "$receipt" \
        '(.schema_version == 4)
         and (.complete == true)
         and (.epoch == $epoch)
         and (.generation_id == $generation)
         and (.archive_wire_profile == "post-unknown-instruction-fallbacks-v1")
         and (.blocks_scanned == $receipt[0].rewrite_stats.blocks)
         and (.transactions_scanned == $receipt[0].rewrite_stats.transactions)
         and (.registry.sha256 == $receipt[0].target_files["registry.bin"].sha256)
         and (.registry.size == $receipt[0].target_files["registry.bin"].bytes)
         and (.omissions.raw_transactions == 0)
         and (.omissions.raw_metadata == 0)
         and (.omissions.decode_errors == 0)
         and (.omissions.unresolved_required_pubkeys == 0)' \
        "$output/manifest.json" >/dev/null
    echo "epoch $epoch: verified"
done

echo "Firewatch usage-sorted target batch complete"
