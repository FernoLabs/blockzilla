#!/bin/sh
# Small orchestration fixture for the one-epoch multicore fast runner.

set -eu
umask 077

fail() {
    echo "fast multicore runner self-test: $*" >&2
    exit 1
}

sha256_file() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{print $1}'
    else
        shasum -a 256 "$1" | awk '{print $1}'
    fi
}

script_dir=$(CDPATH= cd -P "$(dirname "$0")" && pwd -P)
runner=$script_dir/run-archive-v2-pre-to-post-fast-multicore.sh
test_root=$(mktemp -d "${TMPDIR:-/tmp}/archive-v2-fast-multicore-test.XXXXXX") \
    || fail 'cannot create test root'
test_root=$(CDPATH= cd -P "$test_root" && pwd -P)
cleanup() {
    trap - 0 1 2 3 15
    if [ "${KEEP_FAST_MULTICORE_TEST_ROOT:-0}" = 1 ]; then
        echo "kept test root: $test_root" >&2
        return
    fi
    chmod -R u+w "$test_root" 2>/dev/null || :
    rm -rf "$test_root"
}
trap 'cleanup' 0
trap 'exit 129' 1
trap 'exit 130' 2
trap 'exit 131' 3
trap 'exit 143' 15

for command in jq awk sed wc tr dd; do
    command -v "$command" >/dev/null 2>&1 || fail "$command is required"
done

printf '%s\n' '#!/bin/sh' 'printf "  1 /sbin/init\n"' >"$test_root/ps"
printf '%s\n' '#!/bin/sh' \
    'echo "Filesystem 1024-blocks Used Available Capacity Mounted on"' \
    'echo "fake 999999999 1 999999998 1% /"' >"$test_root/df"
printf '%s\n' '#!/bin/sh' \
    'while [ "$#" -gt 0 ]; do' \
    '  case "$1" in -c|-n) shift 2 ;; *) break ;; esac' \
    'done' \
    'exec "$@"' >"$test_root/ionice"
chmod 700 "$test_root/ps" "$test_root/df" "$test_root/ionice"

fake_converter=$test_root/archive-v2-pre-to-post-fast-multicore-test
# shellcheck disable=SC2016
printf '%s\n' \
    '#!/bin/sh' \
    'set -eu' \
    'if [ "$#" -eq 1 ] && [ "$1" = --version ]; then echo "fake-fast-converter 1.0"; exit 0; fi' \
    'hash_file() { if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | awk '\''{print $1}'\''; else shasum -a 256 "$1" | awk '\''{print $1}'\''; fi; }' \
    'source_path= staging= epoch= cluster= generation= audit= audit_sha= threads=' \
    'while [ "$#" -gt 0 ]; do' \
    '  case "$1" in' \
    '    --fast-candidate) shift ;;' \
    '    --threads) threads=$2; shift 2 ;;' \
    '    --source) source_path=$2; shift 2 ;;' \
    '    --source-lease-id) shift 2 ;;' \
    '    --target) [ "$2" = "$source_path" ]; shift 2 ;;' \
    '    --staging) staging=$2; shift 2 ;;' \
    '    --epoch) epoch=$2; shift 2 ;;' \
    '    --cluster-id) cluster=$2; shift 2 ;;' \
    '    --generation-id) generation=$2; shift 2 ;;' \
    '    --source-audit-report) audit=$2; shift 2 ;;' \
    '    --source-audit-report-sha256) audit_sha=$2; shift 2 ;;' \
    '    *) exit 64 ;;' \
    '  esac' \
    'done' \
    '[ -n "$threads" ] && [ "$(hash_file "$audit")" = "$audit_sha" ]' \
    'control=${FAKE_CONTROL_ROOT:?}' \
    'mkdir -p "$control"' \
    'if ! mkdir "$control/active" 2>/dev/null; then : >"$control/overlap"; exit 91; fi' \
    'trap '\''rmdir "$control/active" 2>/dev/null || :'\'' 0' \
    'printf "%s\n" "$epoch" >>"$control/entered"' \
    'printf "%s\n" "$threads" >>"$control/threads-$epoch"' \
    'parent=${source_path%/*}' \
    'backup=$parent/.epoch-$epoch.pre-to-post.backup' \
    'descriptor=$source_path/archive-v2-pre-to-post.candidate.v1.json' \
    'intent=$backup/archive-v2-pre-to-post.switch-intent.v1.json' \
    'complete=$backup/archive-v2-pre-to-post.switch-complete.v1.json' \
    'audit_bytes=$(wc -c <"$audit" | tr -d "[:space:]")' \
    'counts=$(jq -c .counts "$audit")' \
    'if [ ! -d "$backup" ]; then' \
    '  mkdir "$staging"' \
    '  printf "post-%s\n" "$epoch" >"$staging/archive-v2-blocks.zstd"' \
    '  dd if=/dev/zero bs=88 count=1 of="$staging/archive-v2-blocks.index" 2>/dev/null' \
    '  old_blocks_sha=$(hash_file "$source_path/archive-v2-blocks.zstd")' \
    '  old_index_sha=$(hash_file "$source_path/archive-v2-blocks.index")' \
    '  new_blocks_sha=$(hash_file "$staging/archive-v2-blocks.zstd")' \
    '  new_index_sha=$(hash_file "$staging/archive-v2-blocks.index")' \
    '  old_blocks_bytes=$(wc -c <"$source_path/archive-v2-blocks.zstd" | tr -d "[:space:]")' \
    '  new_blocks_bytes=$(wc -c <"$staging/archive-v2-blocks.zstd" | tr -d "[:space:]")' \
    '  jq -n --argjson epoch "$epoch" --arg cluster "$cluster" --arg generation "$generation" --arg source "$source_path" --arg backup "$backup" --arg audit "$audit" --arg audit_sha "$audit_sha" --argjson audit_bytes "$audit_bytes" --argjson counts "$counts" --arg old_blocks_sha "$old_blocks_sha" --arg old_index_sha "$old_index_sha" --arg new_blocks_sha "$new_blocks_sha" --arg new_index_sha "$new_index_sha" --argjson old_blocks_bytes "$old_blocks_bytes" --argjson new_blocks_bytes "$new_blocks_bytes" '\''' \
    '    {schema_version:1,kind:"archive-v2-pre-to-post-candidate",state:"unfinalized",canonical:false,epoch:$epoch,cluster_id:$cluster,prospective_generation_id:$generation,source:$source,candidate:$source,backup:$backup,source_audit_report:{path:$audit,bytes:$audit_bytes,sha256:$audit_sha,counts:$counts},single_decode_rewrite_pass:true,outer_block_bytes_preserved_verbatim_except_messages:true,sidecars_copied:false,sidecars_rewritten:false,source_files:{"archive-v2-blocks.zstd":{bytes:$old_blocks_bytes,sha256:$old_blocks_sha},"archive-v2-blocks.index":{bytes:88,sha256:$old_index_sha}},candidate_rewrite_files:{"archive-v2-blocks.zstd":{bytes:$new_blocks_bytes,sha256:$new_blocks_sha},"archive-v2-blocks.index":{bytes:88,sha256:$new_index_sha}},rewrite:{blocks:$counts.blocks,typed_messages:$counts.typed_messages,message_input_bytes:1,message_output_bytes:1,message_mismatch_bytes:$counts.pre_only,source_instruction_data_tag_counts:[0,$counts.pre_only,0,0,0,0,0,0,0]},exact_message_length_preserved:true,exact_message_delta_proved:true,metadata_regions_copied_verbatim:true,canonical_publication_deferred:true,target_post_audit_performed:false,canonical_manifest_written:false,canonical_profile_marker_written:false,canonical_migration_receipt_written:false}'\'' >"$staging/archive-v2-pre-to-post.candidate.v1.json"' \
    '  descriptor_bytes=$(wc -c <"$staging/archive-v2-pre-to-post.candidate.v1.json" | tr -d "[:space:]")' \
    '  descriptor_sha=$(hash_file "$staging/archive-v2-pre-to-post.candidate.v1.json")' \
    '  jq -n --argjson epoch "$epoch" --arg generation "$generation" --arg source "$source_path" --arg staging "$staging" --arg backup "$backup" --arg audit "$audit" --arg audit_sha "$audit_sha" --arg descriptor_sha "$descriptor_sha" --argjson descriptor_bytes "$descriptor_bytes" '\''' \
    '    {schema_version:1,kind:"archive-v2-pre-to-post-pair-swap-intent",epoch:$epoch,prospective_generation_id:$generation,candidate:$source,staging:$staging,backup:$backup,candidate_descriptor:{bytes:$descriptor_bytes,sha256:$descriptor_sha},source_audit_report_path:$audit,source_audit_report:{sha256:$audit_sha}}'\'' >"$staging/archive-v2-pre-to-post.switch-intent.v1.json"' \
    '  mkdir "$backup"' \
    '  mv "$source_path/archive-v2-blocks.zstd" "$backup/archive-v2-blocks.zstd"' \
    '  mv "$source_path/archive-v2-blocks.index" "$backup/archive-v2-blocks.index"' \
    '  mv "$staging/archive-v2-blocks.zstd" "$source_path/archive-v2-blocks.zstd"' \
    '  mv "$staging/archive-v2-blocks.index" "$source_path/archive-v2-blocks.index"' \
    '  mv "$staging/archive-v2-pre-to-post.candidate.v1.json" "$descriptor"' \
    '  mv "$staging/archive-v2-pre-to-post.switch-intent.v1.json" "$intent"' \
    '  rmdir "$staging"' \
    '  intent_sha=$(hash_file "$intent")' \
    '  jq -n --argjson epoch "$epoch" --arg source "$source_path" --arg backup "$backup" --arg intent_sha "$intent_sha" --arg descriptor_sha "$descriptor_sha" --arg audit_sha "$audit_sha" '\''' \
    '    {schema_version:1,kind:"archive-v2-pre-to-post-pair-swap-complete",epoch:$epoch,canonical:false,candidate:$source,backup:$backup,intent_sha256:$intent_sha,candidate_descriptor_sha256:$descriptor_sha,source_audit_report_sha256:$audit_sha}'\'' >"$complete"' \
    '  if [ -e "$control/crash-$epoch" ] && [ ! -e "$control/crashed-$epoch" ]; then : >"$control/crashed-$epoch"; exit 23; fi' \
    '  report_kind=archive-v2-pre-to-post-candidate-report' \
    'else' \
    '  descriptor_sha=$(hash_file "$descriptor")' \
    '  descriptor_bytes=$(wc -c <"$descriptor" | tr -d "[:space:]")' \
    '  report_kind=archive-v2-pre-to-post-candidate-recovery-report' \
    'fi' \
    'if [ "$report_kind" = archive-v2-pre-to-post-candidate-report ]; then' \
    '  jq -n --arg kind "$report_kind" --argjson epoch "$epoch" --arg cluster "$cluster" --arg generation "$generation" --arg source "$source_path" --arg backup "$backup" --arg descriptor "$descriptor" --arg descriptor_sha "$descriptor_sha" --argjson descriptor_bytes "$descriptor_bytes" --arg audit "$audit" --arg audit_sha "$audit_sha" --argjson audit_bytes "$audit_bytes" '\''' \
    '    {schema_version:1,kind:$kind,state:"unfinalized",canonical:false,epoch:$epoch,cluster_id:$cluster,prospective_generation_id:$generation,source:$source,candidate:$source,backup:$backup,candidate_descriptor:$descriptor,candidate_descriptor_bytes:$descriptor_bytes,candidate_descriptor_sha256:$descriptor_sha,source_audit_report:$audit,source_audit_report_bytes:$audit_bytes,source_audit_report_sha256:$audit_sha,single_decode_rewrite_pass:true,sidecars_copied:false,sidecars_rewritten:false,canonical_publication_deferred:true}'\''' \
    'else' \
    '  jq -n --arg kind "$report_kind" --argjson epoch "$epoch" --arg cluster "$cluster" --arg generation "$generation" --arg source "$source_path" --arg backup "$backup" --arg descriptor "$descriptor" --arg descriptor_sha "$descriptor_sha" --argjson descriptor_bytes "$descriptor_bytes" --arg audit "$audit" --arg audit_sha "$audit_sha" --argjson audit_bytes "$audit_bytes" '\''' \
    '    {schema_version:1,kind:$kind,state:"unfinalized",canonical:false,epoch:$epoch,cluster_id:$cluster,prospective_generation_id:$generation,candidate:$source,backup:$backup,candidate_descriptor:$descriptor,candidate_descriptor_bytes:$descriptor_bytes,candidate_descriptor_sha256:$descriptor_sha,source_audit_report:$audit,source_audit_report_bytes:$audit_bytes,source_audit_report_sha256:$audit_sha,recovered_switch:true,already_complete:true}'\''' \
    'fi' >"$fake_converter"
chmod 700 "$fake_converter"

handoff=$test_root/handoff
archive=$test_root/archive
old_state=$handoff/fast-in-place-candidate
mkdir -p "$handoff" "$archive" "$old_state/results" "$old_state/reports" \
    "$old_state/logs" "$old_state/source-audit-reports" "$test_root/audits" "$test_root/control"
jq -n --arg archive "$archive" --arg state "$handoff" \
    '{archive_root:$archive,state_root:$state,cluster_id:"mainnet-beta",run_id:"test-run"}' \
    >"$handoff/request.json"
jq -n --arg archive "$archive" --arg state "$old_state" \
    '{schema_version:1,kind:"archive-v2-pre-to-post-fast-in-place-config",run_id:"test-run",cluster_id:"mainnet-beta",archive_root:$archive,state_root:$state,worker_count:2}' \
    >"$old_state/config.json"
printf '1\n2\n3\n4\n' >"$old_state/all-legacy-pre-epochs.txt"
: >"$old_state/all-legacy-pre-reports.tsv"

for epoch in 1 2 3 4; do
    source_path=$archive/epoch-$epoch
    mkdir "$source_path"
    dd if=/dev/zero bs=10 count=1 of="$source_path/archive-v2-blocks.zstd" 2>/dev/null
    dd if=/dev/zero bs=88 count=1 of="$source_path/archive-v2-blocks.index" 2>/dev/null
    audit=$test_root/audits/epoch-$epoch.json
    jq -n --argjson epoch "$epoch" --arg archive "$source_path" \
        '{schema_version:1,kind:"archive-v2-wire-profile-scan",epoch:$epoch,archive:$archive,
          workers:2,elapsed_seconds:1,completed_unix_seconds:1,error:null,
          classification:"legacy-pre",action:"convert-to-post",
          counts:{blocks:1,owned_fallback_blocks:0,compressed_block_bytes:10,
            uncompressed_block_bytes:20,typed_messages:1,raw_transaction_fallbacks:0,
            post_only:0,pre_only:1,both_equivalent:0,both_divergent:0,invalid:0}}' >"$audit"
    audit_sha=$(sha256_file "$audit")
    printf '%s\t%s\t%s\t1\t10\t20\t1\t1\t0\n' "$epoch" "$audit" "$audit_sha" \
        >>"$old_state/all-legacy-pre-reports.tsv"
done

for epoch in 1 2; do
    audit_source=$test_root/audits/epoch-$epoch.json
    audit_pinned=$old_state/source-audit-reports/epoch-$epoch.json
    cp "$audit_source" "$audit_pinned"
    audit_sha=$(sha256_file "$audit_pinned")
    generation=archive-v2-pre-to-post-fast-test-run-epoch-$epoch
    report=$old_state/reports/epoch-$epoch.json
    log=$old_state/logs/epoch-$epoch.log
    FAKE_CONTROL_ROOT=$test_root/control "$fake_converter" \
        --fast-candidate --threads 1 \
        --source "$archive/epoch-$epoch" --source-lease-id old \
        --target "$archive/epoch-$epoch" \
        --staging "$archive/.epoch-$epoch.pre-to-post.staging" \
        --epoch "$epoch" --cluster-id mainnet-beta --generation-id "$generation" \
        --source-audit-report "$audit_pinned" \
        --source-audit-report-sha256 "$audit_sha" >"$report" 2>"$log"
    jq -n --argjson epoch "$epoch" --arg source "$archive/epoch-$epoch" \
        --arg backup "$archive/.epoch-$epoch.pre-to-post.backup" \
        --arg generation "$generation" --arg audit_source "$audit_source" \
        --arg audit "$audit_pinned" --arg audit_sha "$audit_sha" \
        --arg report "$report" --arg log "$log" \
        '{schema_version:1,kind:"archive-v2-pre-to-post-fast-in-place-result",
          run_id:"test-run",epoch:$epoch,source:$source,backup:$backup,
          prospective_generation_id:$generation,
          source_audit:{source_path:$audit_source,pinned_path:$audit,sha256:$audit_sha},
          converter_report:$report,converter_log:$log,
          target_post_audit_deferred:true,canonical_manifest_deferred:true,
          canonical_publication:false}' >"$old_state/results/epoch-$epoch.json"
done

# Ignore prefix setup calls when checking new suffix execution.
: >"$test_root/control/entered"
rm -f "$test_root/control/threads-1" "$test_root/control/threads-2"
: >"$test_root/control/crash-4"
run_runner() {
    FAKE_CONTROL_ROOT=$test_root/control PATH="$test_root:$PATH" \
        "$runner" "$handoff" "$fake_converter" test-maintenance
}

if run_runner >"$test_root/first.out" 2>"$test_root/first.err"; then
    fail 'post-switch crash returned success'
fi
[ -s "$handoff/fast-in-place-multicore/results/epoch-3.json" ] \
    || fail 'epoch 3 did not publish before epoch 4 failed'
[ ! -e "$handoff/fast-in-place-multicore/results/epoch-4.json" ] \
    || fail 'failed epoch 4 published a result'
run_runner >"$test_root/resume.out" 2>"$test_root/resume.err" \
    || fail 'resume did not recover and complete'

state=$handoff/fast-in-place-multicore
[ "$(cat "$state/old-prefix-epochs.txt")" = "1
2" ] || fail 'old prefix is not exact'
[ "$(cat "$state/remaining-epochs.txt")" = "3
4" ] || fail 'remaining list is not exact'
[ "$(wc -l <"$state/remaining-audits.tsv" | tr -d '[:space:]')" -eq 2 ] \
    || fail 'remaining audit table count differs'
[ "$(grep -c '^3$' "$test_root/control/entered")" -eq 1 ] \
    || fail 'resume reran validated epoch 3'
[ "$(grep -c '^4$' "$test_root/control/entered")" -eq 2 ] \
    || fail 'epoch 4 did not use converter recovery'
[ "$(sort -u "$test_root/control/threads-3")" = 8 ] \
    && [ "$(sort -u "$test_root/control/threads-4")" = 8 ] \
    || fail 'runner did not pass the default 8 threads'
jq -e '
    .kind == "archive-v2-pre-to-post-fast-multicore-batch-complete" and
    .threads == 8 and .old_prefix.count == 2 and .old_prefix.last_epoch == 2 and
    .remaining.count == 2 and .new_results.count == 2 and
    .one_converter_child_at_a_time == true and .canonical_publication == false
' "$state/batch-complete.json" >/dev/null || fail 'completion record is invalid'
[ ! -e "$test_root/control/overlap" ] || fail 'two converter children overlapped'
while IFS=$(printf '\t') read -r bound_epoch bound_path bound_sha _rest; do
    [ "$bound_path" = "$state/source-audit-reports/epoch-$bound_epoch.json" ] \
        && [ "$(sha256_file "$bound_path")" = "$bound_sha" ] \
        || fail "audit binding changed for epoch $bound_epoch"
done <"$state/remaining-audits.tsv"

entered_before_lock=$(wc -l <"$test_root/control/entered" | tr -d '[:space:]')
mkdir -p "$handoff/conversion/.archive-v2-pre-to-post-manual.lock"
if run_runner >"$test_root/strict-lock.out" 2>"$test_root/strict-lock.err"; then
    fail 'strict converter lock was accepted'
fi
rmdir "$handoff/conversion/.archive-v2-pre-to-post-manual.lock"
[ "$(wc -l <"$test_root/control/entered" | tr -d '[:space:]')" -eq "$entered_before_lock" ] \
    || fail 'strict lock started a converter'

mkdir "$archive/.archive-v2-pre-to-post-fast-candidate.lock"
if run_runner >"$test_root/lock.out" 2>"$test_root/lock.err"; then
    fail 'duplicate archive-root lock was accepted'
fi
rmdir "$archive/.archive-v2-pre-to-post-fast-candidate.lock"
[ "$(wc -l <"$test_root/control/entered" | tr -d '[:space:]')" -eq "$entered_before_lock" ] \
    || fail 'duplicate lock started a converter'

live_blocks=$archive/epoch-3/archive-v2-blocks.zstd
cp "$live_blocks" "$test_root/epoch-3.blocks.saved"
printf 'changed-size\n' >>"$live_blocks"
if run_runner >"$test_root/payload-size.out" 2>"$test_root/payload-size.err"; then
    fail 'changed candidate payload size was accepted'
fi
cp "$test_root/epoch-3.blocks.saved" "$live_blocks"
[ "$(wc -l <"$test_root/control/entered" | tr -d '[:space:]')" -eq "$entered_before_lock" ] \
    || fail 'changed candidate payload started another converter'

entered_before=$(wc -l <"$test_root/control/entered" | tr -d '[:space:]')
chmod 600 "$state/results/epoch-3.json"
printf '{"tampered":true}\n' >"$state/results/epoch-3.json"
if run_runner >"$test_root/tamper.out" 2>"$test_root/tamper.err"; then
    fail 'tampered completed result was skipped'
fi
[ "$(wc -l <"$test_root/control/entered" | tr -d '[:space:]')" -eq "$entered_before" ] \
    || fail 'tampered result started another converter'

echo 'fast multicore runner self-test: PASS'
