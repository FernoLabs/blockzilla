#!/bin/sh
# Run the read-only Archive V2 instruction-profile scanner over a contiguous
# epoch range. Results are written outside the archive tree and can be resumed.

set -u

if [ "$#" -ne 6 ]; then
    echo "usage: $0 SCANNER ARCHIVE_ROOT RESULT_ROOT FIRST_EPOCH LAST_EPOCH WORKERS" >&2
    exit 2
fi

scanner=$1
archive_root=$2
result_root=$3
first_epoch=$4
last_epoch=$5
workers=$6

if [ ! -x "$scanner" ]; then
    echo "scanner is not executable: $scanner" >&2
    exit 2
fi
if [ ! -d "$archive_root" ]; then
    echo "archive root is not a directory: $archive_root" >&2
    exit 2
fi

mkdir -p "$result_root" || exit 2

epoch=$first_epoch
while [ "$epoch" -le "$last_epoch" ]; do
    archive="$archive_root/epoch-$epoch"
    result="$result_root/epoch-$epoch.json"
    log="$result_root/epoch-$epoch.log"
    if [ -s "$result" ] \
        && grep -Fq '"kind":"archive-v2-wire-profile-scan"' "$result" \
        && grep -Fq '"epoch":'"$epoch"',' "$result"; then
        echo "epoch $epoch: existing result"
        epoch=$((epoch + 1))
        continue
    fi

    temporary_result="$result.building-$$"
    temporary_log="$log.building-$$"
    started=$(date +%s)
    if nice -n 19 ionice -c 3 "$scanner" \
        --archive "$archive" \
        --epoch "$epoch" \
        --workers "$workers" \
        --progress-blocks 50000 \
        >"$temporary_result" 2>"$temporary_log"; then
        if [ "$(wc -l <"$temporary_result")" -eq 1 ] \
            && grep -Fq '"kind":"archive-v2-wire-profile-scan"' "$temporary_result" \
            && grep -Fq '"epoch":'"$epoch"',' "$temporary_result"; then
            mv "$temporary_result" "$result"
            mv "$temporary_log" "$log"
            finished=$(date +%s)
            echo "epoch $epoch: result in $((finished - started)) seconds"
        else
            mv "$temporary_result" "$temporary_result.invalid"
            mv "$temporary_log" "$temporary_log.invalid"
            echo "epoch $epoch: scanner produced an invalid report" >&2
        fi
    else
        status=$?
        mv "$temporary_result" "$temporary_result.failed-$status"
        mv "$temporary_log" "$temporary_log.failed-$status"
        echo "epoch $epoch: scanner exited with status $status" >&2
    fi
    epoch=$((epoch + 1))
done

date -u +%Y-%m-%dT%H:%M:%SZ >"$result_root/complete.txt"
