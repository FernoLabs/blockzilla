#!/bin/sh
# Preserve the two interrupted, pre-journal staging trees and redo epochs
# 103 and 104 with the already reviewed one-epoch fast canary runner.

set -eu
umask 077

die() {
    echo "archive-v2 redo 103/104: $*" >&2
    exit 1
}

usage() {
    echo "usage: $0 HANDOFF_STATE_ROOT CANARY_RUNNER CONVERTER" >&2
    exit 2
}

directory() (
    CDPATH=
    cd -P "$1" 2>/dev/null || exit 1
    pwd -P
)

regular_file_path() (
    parent=$(directory "$(dirname "$1")") || exit 1
    printf '%s/%s\n' "$parent" "$(basename "$1")"
)

sha256_file() {
    sha256sum "$1" | awk '
        NR == 1 && length($1) == 64 && $1 !~ /[^0-9a-f]/ {
            print $1; ok = 1
        }
        END { if (!ok) exit 1 }
    '
}

json_string() {
    jq -er "$1 | select(type == \"string\" and length > 0)" "$2" 2>/dev/null \
        || die "invalid control field $1 in $2"
}

publish_file() {
    candidate=$1
    final=$2
    chmod 400 "$candidate" || die "cannot protect $candidate"
    if [ -e "$final" ] || [ -L "$final" ]; then
        [ -f "$final" ] && [ ! -L "$final" ] && cmp -s "$candidate" "$final" \
            || die "published control differs: $final"
    else
        ln -n "$candidate" "$final" || die "cannot publish $final"
    fi
    rm "$candidate" || die "cannot remove publication candidate"
}

require_absent() {
    [ ! -e "$1" ] && [ ! -L "$1" ] || die "path must be absent: $1"
}

require_regular() {
    [ -f "$1" ] && [ ! -L "$1" ] || die "required regular file is absent: $1"
}

service_is_quiet() {
    for service in \
        blockzilla-archive.service \
        blockzilla-gateway-internal.service \
        blockzilla-live-indexer.service \
        blockzilla-firewatch-index-controller.service \
        blockzilla-raw-live-fallback.service \
        blockzilla-watcher-runtime-operations.service \
        blockzilla-pre-post-fast-canary-batch-700-t10-r2.service; do
        state=$(systemctl --user is-active "$service" 2>/dev/null || true)
        case "$state" in
            active|activating|reloading) die "service is not stopped: $service ($state)" ;;
        esac
    done
}

processes_are_quiet() {
    snapshot=$recovery_root/processes-$$
    ps -eo pid=,args= >"$snapshot" || die 'cannot read the process table'
    if awk -v self="$$" -v archive="$archive_root" '
        function names_archive(line, path, at, tail) {
            while ((at = index(line, path)) != 0) {
                tail = substr(line, at + length(path), 1)
                if (tail == "" || tail == "/" || tail == " " || tail == "\t") return 1
                line = substr(line, at + length(path))
            }
            return 0
        }
        $1 != self &&
        (names_archive($0, archive) ||
         (index($0, "archive-v2-pre-to-post") && index($0, "--fast-candidate"))) {
            print > "/dev/stderr"; found = 1
        }
        END { exit found ? 0 : 1 }
    ' "$snapshot"; then
        rm "$snapshot" || :
        die 'an archive reader or converter is active'
    fi
    rm "$snapshot" || die 'cannot remove the process snapshot'
}

validate_old_staging() {
    epoch=$1
    staging=$archive_root/.epoch-$epoch.pre-to-post.staging
    [ -d "$staging" ] && [ ! -L "$staging" ] \
        || die "old staging is absent for epoch $epoch"
    entries=$(find "$staging" -mindepth 1 -maxdepth 1 -printf '%f\t%y\n') \
        || die "cannot inspect old staging for epoch $epoch"
    expected_entries=$(printf 'archive-v2-blocks.zstd\tf')
    [ "$entries" = "$expected_entries" ] \
        || die "old staging has unexpected entries for epoch $epoch"
    require_regular "$staging/archive-v2-blocks.zstd"
    require_absent "$staging/archive-v2-pre-to-post.switch-intent.v1.json"
    require_absent "$archive_root/.epoch-$epoch.pre-to-post.backup"
    require_absent "$archive_root/epoch-$epoch/archive-v2-pre-to-post.candidate.v1.json"
}

validate_shadow() {
    jq -e --arg archive "$archive_root" --arg state "$shadow" \
        --arg cluster "$cluster_id" --arg run "$new_run_id" '
        .schema_version == 1 and
        .kind == "archive-v2-pre-to-post-manual-handoff-request" and
        .archive_root == $archive and .state_root == $state and
        .cluster_id == $cluster and .run_id == $run
    ' "$shadow/request.json" >/dev/null 2>&1 || die 'shadow request is invalid'
    jq -e --arg archive "$archive_root" --arg state "$shadow_fast" \
        --arg cluster "$cluster_id" --arg run "$new_run_id" \
        --arg table "$shadow_table" --arg audits "$shadow_audits" '
        .schema_version == 1 and
        .kind == "archive-v2-pre-to-post-fast-in-place-config" and
        .archive_root == $archive and .state_root == $state and
        .cluster_id == $cluster and .run_id == $run and
        .source_audit_table == $table and
        .pinned_source_audit_directory == $audits
    ' "$shadow_fast/config.json" >/dev/null 2>&1 || die 'shadow config is invalid'
    [ "$(awk -F '\t' '$1 == 103 || $1 == 104 { n++ } END { print n + 0 }' "$shadow_table")" -eq 2 ] \
        || die 'shadow audit table is invalid'
}

validate_admission_bindings() {
    [ "$(jq -er '.shadow.request_sha256' "$admission")" = "$(sha256_file "$shadow/request.json")" ] \
        || die 'shadow request differs from admission'
    [ "$(jq -er '.shadow.config_sha256' "$admission")" = "$(sha256_file "$shadow_fast/config.json")" ] \
        || die 'shadow config differs from admission'
    [ "$(jq -er '.shadow.audit_table_sha256' "$admission")" = "$(sha256_file "$shadow_table")" ] \
        || die 'shadow audit table differs from admission'
    for binding_epoch in 103 104; do
        expected=$(jq -er --argjson epoch "$binding_epoch" '
            .epochs[] | select(.epoch == $epoch) | .shadow_audit_sha256
        ' "$admission") || die "admission has no shadow audit for epoch $binding_epoch"
        [ "$expected" = "$(sha256_file "$shadow_audits/epoch-$binding_epoch.json")" ] \
            || die "shadow audit differs from admission for epoch $binding_epoch"
        expected=$(jq -er --argjson epoch "$binding_epoch" '
            .epochs[] | select(.epoch == $epoch) | .old_report_sha256
        ' "$admission") || die "admission has no old report binding for epoch $binding_epoch"
        [ "$expected" = "$(sha256_file "$old_fast/attempts/epoch-$binding_epoch/attempt-1/report.json")" ] \
            || die "old report differs from admission for epoch $binding_epoch"
    done
}

move_old_staging() {
    epoch=$1
    staging=$archive_root/.epoch-$epoch.pre-to-post.staging
    evidence=$evidence_root/epoch-$epoch.interrupted-staging
    expected_identity=$(jq -er --argjson epoch "$epoch" '
        .epochs[] | select(.epoch == $epoch) | .staging_identity
    ' "$admission") || die "admission has no staging identity for epoch $epoch"
    expected_file_identity=$(jq -er --argjson epoch "$epoch" '
        .epochs[] | select(.epoch == $epoch) | .partial_file_identity
    ' "$admission") || die "admission has no partial file identity for epoch $epoch"
    if [ -d "$staging" ] && [ ! -L "$staging" ]; then
        require_absent "$evidence"
        [ "$(stat -c '%d:%i' "$staging")" = "$expected_identity" ] \
            || die "old staging identity changed for epoch $epoch"
        require_regular "$staging/archive-v2-blocks.zstd"
        [ "$(stat -c '%d:%i:%f:%u:%g:%h:%s:%Y:%Z' "$staging/archive-v2-blocks.zstd")" = "$expected_file_identity" ] \
            || die "old partial file identity changed for epoch $epoch"
        mv -T -n "$staging" "$evidence" || die "cannot preserve old staging for epoch $epoch"
    fi
    require_absent "$staging"
    [ -d "$evidence" ] && [ ! -L "$evidence" ] \
        || die "preserved evidence is absent for epoch $epoch"
    [ "$(stat -c '%d:%i' "$evidence")" = "$expected_identity" ] \
        || die "preserved staging identity differs for epoch $epoch"
    require_regular "$evidence/archive-v2-blocks.zstd"
    [ "$(stat -c '%d:%i:%f:%u:%g:%h:%s:%Y:%Z' "$evidence/archive-v2-blocks.zstd")" = "$expected_file_identity" ] \
        || die "preserved partial file identity differs for epoch $epoch"
}

run_epoch() {
    epoch=$1
    service_is_quiet
    validate_shadow
    validate_admission_bindings
    require_absent "$archive_root/.archive-v2-pre-to-post-fast-candidate.lock"

    attempt_root=$recovery_root/attempts/epoch-$epoch
    mkdir -p "$attempt_root" || die "cannot create attempt root for epoch $epoch"
    sequence=1
    while ! mkdir "$attempt_root/attempt-$sequence" 2>/dev/null; do
        sequence=$((sequence + 1))
        [ "$sequence" -le 100 ] || die "too many attempts for epoch $epoch"
    done
    attempt=$attempt_root/attempt-$sequence
    stdout=$attempt/canary.stdout
    stderr=$attempt/canary.stderr

    "$canary_runner" "$shadow" "$epoch" "$converter" 10 \
        >"$stdout" 2>"$stderr" &
    child_pid=$!
    if wait "$child_pid"; then status=0; else status=$?; fi
    child_pid=
    [ "$status" -eq 0 ] || die "fresh epoch $epoch conversion failed with status $status"
    chmod 400 "$stdout" "$stderr" || die "cannot protect epoch $epoch logs"

    result=$shadow/fast-canary-epoch-$epoch/result.json
    descriptor=$archive_root/epoch-$epoch/archive-v2-pre-to-post.candidate.v1.json
    backup=$archive_root/.epoch-$epoch.pre-to-post.backup
    require_regular "$result"
    require_regular "$descriptor"
    [ -d "$backup" ] && [ ! -L "$backup" ] || die "backup is absent for epoch $epoch"
    require_regular "$backup/archive-v2-pre-to-post.switch-intent.v1.json"
    require_regular "$backup/archive-v2-pre-to-post.switch-complete.v1.json"
    require_absent "$archive_root/.epoch-$epoch.pre-to-post.staging"
    jq -e --argjson epoch "$epoch" '
        .schema_version == 1 and
        .kind == "archive-v2-pre-to-post-fast-canary-result" and
        .epoch == $epoch and .threads == 10 and .canonical == false
    ' "$result" >/dev/null 2>&1 || die "result is invalid for epoch $epoch"

    result_copy=$recovery_root/results/epoch-$epoch.json
    if [ -e "$result_copy" ] || [ -L "$result_copy" ]; then
        [ -f "$result_copy" ] && [ ! -L "$result_copy" ] && cmp -s "$result" "$result_copy" \
            || die "recovery result differs for epoch $epoch"
    else
        ln -n "$result" "$result_copy" || die "cannot bind result for epoch $epoch"
    fi
}

[ "$#" -eq 3 ] || usage
handoff=$1
canary_runner=$2
converter=$3
for tool in jq sha256sum awk find stat wc tr cmp ln mv cp chmod mkdir rmdir rm \
    dirname basename date ps systemctl; do
    command -v "$tool" >/dev/null 2>&1 || die "required command is absent: $tool"
done
case "$handoff:$canary_runner:$converter" in *'
'*) die 'arguments must be one line' ;; esac
case "$handoff" in /*) ;; *) die 'handoff must be absolute' ;; esac
case "$canary_runner" in /*) ;; *) die 'canary runner must be absolute' ;; esac
case "$converter" in /*) ;; *) die 'converter must be absolute' ;; esac
[ -d "$handoff" ] && [ ! -L "$handoff" ] && [ "$(directory "$handoff")" = "$handoff" ] \
    || die 'handoff must be one canonical real directory'
[ -f "$canary_runner" ] && [ ! -L "$canary_runner" ] && [ -x "$canary_runner" ] \
    && [ "$(regular_file_path "$canary_runner")" = "$canary_runner" ] \
    || die 'canary runner must be one canonical executable'
[ -f "$converter" ] && [ ! -L "$converter" ] && [ -x "$converter" ] \
    && [ "$(regular_file_path "$converter")" = "$converter" ] \
    || die 'converter must be one canonical executable'

request=$handoff/request.json
old_fast=$handoff/fast-in-place-candidate
old_table=$old_fast/all-legacy-pre-reports.tsv
clean_complete=$handoff/fast-canary-batch-start-700/complete.json
require_regular "$request"
require_regular "$old_fast/config.json"
require_regular "$old_table"
require_regular "$clean_complete"
[ "$(sha256_file "$clean_complete")" = c725960511ecd0d7136368826cd40ad28422d303a7163fca4ee926d1d770e69c ] \
    || die 'clean-batch completion changed'
archive_root=$(json_string '.archive_root' "$request")
cluster_id=$(json_string '.cluster_id' "$request")
base_run_id=$(json_string '.run_id' "$request")
[ -d "$archive_root" ] && [ ! -L "$archive_root" ] \
    && [ "$(directory "$archive_root")" = "$archive_root" ] \
    || die 'archive root is invalid'

recovery_id=redo-103-104-20260824-v1
recovery_root=$handoff/fast-in-place-reprocess-103-104
shadow=$recovery_root/shadow-handoff
shadow_fast=$shadow/fast-in-place-candidate
shadow_audits=$shadow_fast/source-audit-reports
shadow_table=$shadow_fast/all-legacy-pre-reports.tsv
new_run_id=$base_run_id-reprocess-103-104-$recovery_id
evidence_root=$archive_root/.archive-v2-pre-to-post-interrupted-evidence-103-104-$recovery_id
admission=$recovery_root/admission.json
recovery_lock=$handoff/.archive-v2-pre-to-post-reprocess-103-104.lock
supervisor_lock=$handoff/.archive-v2-pre-to-post-fast-canary-batch.lock
root_lock=$archive_root/.archive-v2-pre-to-post-fast-candidate.lock
strict_lock=$handoff/conversion/.archive-v2-pre-to-post-manual.lock
lock_recovery=0
lock_supervisor=0
lock_root=0
child_pid=

release_locks() {
    if [ "$lock_root" -eq 1 ]; then rmdir "$root_lock" 2>/dev/null || :; lock_root=0; fi
    if [ "$lock_supervisor" -eq 1 ]; then rmdir "$supervisor_lock" 2>/dev/null || :; lock_supervisor=0; fi
    if [ "$lock_recovery" -eq 1 ]; then rmdir "$recovery_lock" 2>/dev/null || :; lock_recovery=0; fi
}
stop() {
    code=$1
    trap - 0 1 2 3 15
    if [ -n "$child_pid" ]; then
        kill -TERM "$child_pid" 2>/dev/null || :
        wait "$child_pid" 2>/dev/null || :
        child_pid=
    fi
    release_locks
    exit "$code"
}
trap 'release_locks' 0
trap 'stop 129' 1
trap 'stop 130' 2
trap 'stop 131' 3
trap 'stop 143' 15

mkdir "$recovery_lock" 2>/dev/null || die 'recovery lock exists'
lock_recovery=1
mkdir "$supervisor_lock" 2>/dev/null || die 'canary supervisor lock exists'
lock_supervisor=1
require_absent "$root_lock"
require_absent "$strict_lock"
service_is_quiet

mkdir -p "$recovery_root/results" "$recovery_root/attempts" \
    "$shadow_fast/results" "$shadow_fast/claims" "$shadow_audits" \
    || die 'cannot create private recovery state'
chmod 700 "$recovery_root" "$recovery_root/results" "$recovery_root/attempts" \
    "$shadow" "$shadow_fast" "$shadow_fast/results" "$shadow_fast/claims" "$shadow_audits" \
    || die 'cannot protect private recovery state'
processes_are_quiet

for epoch in 103 104; do
    require_regular "$old_fast/claims/epoch-$epoch.json"
    require_regular "$old_fast/reprocess-required/epoch-$epoch.json"
    require_regular "$old_fast/attempts/epoch-$epoch/attempt-1/converter.log"
    require_regular "$old_fast/attempts/epoch-$epoch/attempt-1/report.json"
    require_regular "$old_fast/source-audit-reports/epoch-$epoch.json"
    [ "$(wc -c <"$old_fast/attempts/epoch-$epoch/attempt-1/report.json" | tr -d '[:space:]')" -eq 0 ] \
        || die "old report is not empty for epoch $epoch"
done

if [ ! -e "$admission" ] && [ ! -L "$admission" ]; then
    validate_old_staging 103
    validate_old_staging 104

    for epoch in 103 104; do
        cp "$old_fast/source-audit-reports/epoch-$epoch.json" "$shadow_audits/epoch-$epoch.json" \
            || die "cannot copy audit for epoch $epoch"
        chmod 400 "$shadow_audits/epoch-$epoch.json" \
            || die "cannot protect audit for epoch $epoch"
    done
    awk -F '\t' '$1 == 103 || $1 == 104' "$old_table" >"$shadow_table" \
        || die 'cannot create shadow audit table'
    chmod 400 "$shadow_table" || die 'cannot protect shadow audit table'

    request_candidate=$shadow/request.json.building-$$
    jq -cn --arg archive "$archive_root" --arg state "$shadow" \
        --arg cluster "$cluster_id" --arg run "$new_run_id" '
        {schema_version:1,kind:"archive-v2-pre-to-post-manual-handoff-request",
         archive_root:$archive,state_root:$state,cluster_id:$cluster,run_id:$run,
         canonical:false,recovery_epochs:[103,104]}
    ' >"$request_candidate" || die 'cannot create shadow request'
    publish_file "$request_candidate" "$shadow/request.json"

    config_candidate=$shadow_fast/config.json.building-$$
    jq -cn --arg archive "$archive_root" --arg state "$shadow_fast" \
        --arg cluster "$cluster_id" --arg run "$new_run_id" \
        --arg table "$shadow_table" --arg audits "$shadow_audits" '
        {schema_version:1,kind:"archive-v2-pre-to-post-fast-in-place-config",
         run_id:$run,cluster_id:$cluster,archive_root:$archive,state_root:$state,
         source_audit_table:$table,pinned_source_audit_directory:$audits,
         epoch_count:2,threads:10,canonical_publication:false}
    ' >"$config_candidate" || die 'cannot create shadow config'
    publish_file "$config_candidate" "$shadow_fast/config.json"
    validate_shadow

    stage103=$archive_root/.epoch-103.pre-to-post.staging
    stage104=$archive_root/.epoch-104.pre-to-post.staging
    admission_candidate=$admission.building-$$
    jq -cn \
        --arg recovery "$recovery_id" --arg run "$new_run_id" \
        --arg clean "$clean_complete" --arg clean_sha "$(sha256_file "$clean_complete")" \
        --arg canary "$canary_runner" --arg canary_sha "$(sha256_file "$canary_runner")" \
        --arg converter "$converter" --arg converter_sha "$(sha256_file "$converter")" \
        --arg evidence "$evidence_root" \
        --arg stage103 "$stage103" --arg id103 "$(stat -c '%d:%i' "$stage103")" \
        --arg file103 "$(stat -c '%d:%i:%f:%u:%g:%h:%s:%Y:%Z' "$stage103/archive-v2-blocks.zstd")" \
        --arg claim103 "$(sha256_file "$old_fast/claims/epoch-103.json")" \
        --arg flag103 "$(sha256_file "$old_fast/reprocess-required/epoch-103.json")" \
        --arg log103 "$(sha256_file "$old_fast/attempts/epoch-103/attempt-1/converter.log")" \
        --arg report103 "$(sha256_file "$old_fast/attempts/epoch-103/attempt-1/report.json")" \
        --arg audit103 "$(sha256_file "$old_fast/source-audit-reports/epoch-103.json")" \
        --arg shadow_audit103 "$(sha256_file "$shadow_audits/epoch-103.json")" \
        --arg stage104 "$stage104" --arg id104 "$(stat -c '%d:%i' "$stage104")" \
        --arg file104 "$(stat -c '%d:%i:%f:%u:%g:%h:%s:%Y:%Z' "$stage104/archive-v2-blocks.zstd")" \
        --arg claim104 "$(sha256_file "$old_fast/claims/epoch-104.json")" \
        --arg flag104 "$(sha256_file "$old_fast/reprocess-required/epoch-104.json")" \
        --arg log104 "$(sha256_file "$old_fast/attempts/epoch-104/attempt-1/converter.log")" \
        --arg report104 "$(sha256_file "$old_fast/attempts/epoch-104/attempt-1/report.json")" \
        --arg audit104 "$(sha256_file "$old_fast/source-audit-reports/epoch-104.json")" \
        --arg shadow_audit104 "$(sha256_file "$shadow_audits/epoch-104.json")" \
        --arg shadow_request "$(sha256_file "$shadow/request.json")" \
        --arg shadow_config "$(sha256_file "$shadow_fast/config.json")" \
        --arg shadow_table "$(sha256_file "$shadow_table")" '
        {schema_version:1,kind:"archive-v2-pre-to-post-reprocess-admission",
         recovery_id:$recovery,run_id:$run,epochs:[
          {epoch:103,staging:$stage103,staging_identity:$id103,partial_file_identity:$file103,
           old_claim_sha256:$claim103,reprocess_flag_sha256:$flag103,
           old_log_sha256:$log103,old_report_sha256:$report103,
           source_audit_sha256:$audit103,shadow_audit_sha256:$shadow_audit103},
          {epoch:104,staging:$stage104,staging_identity:$id104,partial_file_identity:$file104,
           old_claim_sha256:$claim104,reprocess_flag_sha256:$flag104,
           old_log_sha256:$log104,old_report_sha256:$report104,
           source_audit_sha256:$audit104,shadow_audit_sha256:$shadow_audit104}],
         clean_batch:{path:$clean,sha256:$clean_sha},
         canary_runner:{path:$canary,sha256:$canary_sha},
         converter:{path:$converter,sha256:$converter_sha},threads:10,
         shadow:{request_sha256:$shadow_request,config_sha256:$shadow_config,
                 audit_table_sha256:$shadow_table},
         evidence_root:$evidence,old_partial_output_used:false,
         canonical:false,canonical_checks_deferred:true}
    ' >"$admission_candidate" || die 'cannot create recovery admission'
    publish_file "$admission_candidate" "$admission"
fi

require_regular "$admission"
[ "$(jq -er '.recovery_id' "$admission")" = "$recovery_id" ] || die 'admission changed'
[ "$(jq -er '.converter.sha256' "$admission")" = "$(sha256_file "$converter")" ] \
    || die 'converter differs from admission'
[ "$(jq -er '.canary_runner.sha256' "$admission")" = "$(sha256_file "$canary_runner")" ] \
    || die 'canary differs from admission'
validate_shadow
validate_admission_bindings

mkdir "$root_lock" 2>/dev/null || die 'archive-root fast lock exists'
lock_root=1
processes_are_quiet
if [ ! -e "$evidence_root" ] && [ ! -L "$evidence_root" ]; then
    mkdir "$evidence_root" || die 'cannot create evidence root'
    chmod 700 "$evidence_root" || die 'cannot protect evidence root'
fi
[ -d "$evidence_root" ] && [ ! -L "$evidence_root" ] || die 'evidence root is invalid'
move_old_staging 103
move_old_staging 104
release_root=$lock_root
rmdir "$root_lock" || die 'cannot release archive-root lock after evidence move'
lock_root=0

evidence_complete=$recovery_root/evidence-complete.json
evidence_candidate=$evidence_complete.building-$$
jq -cn --arg admission "$admission" --arg admission_sha "$(sha256_file "$admission")" \
    --arg root "$evidence_root" \
    --arg id103 "$(stat -c '%d:%i' "$evidence_root/epoch-103.interrupted-staging")" \
    --arg id104 "$(stat -c '%d:%i' "$evidence_root/epoch-104.interrupted-staging")" '
    {schema_version:1,kind:"archive-v2-pre-to-post-interrupted-staging-evidence",
     admission:{path:$admission,sha256:$admission_sha},evidence_root:$root,
     epochs:[{epoch:103,identity:$id103},{epoch:104,identity:$id104}],
     old_partial_output_used:false,canonical:false}
' >"$evidence_candidate" || die 'cannot create evidence completion record'
publish_file "$evidence_candidate" "$evidence_complete"

run_epoch 103
run_epoch 104

candidate_count=0
while IFS= read -r epoch; do
    case "$epoch" in ''|*[!0-9]*) die 'cohort epoch list is invalid' ;; esac
    require_regular "$archive_root/epoch-$epoch/archive-v2-pre-to-post.candidate.v1.json"
    [ -d "$archive_root/.epoch-$epoch.pre-to-post.backup" ] \
        && [ ! -L "$archive_root/.epoch-$epoch.pre-to-post.backup" ] \
        || die "backup is absent after recovery for epoch $epoch"
    candidate_count=$((candidate_count + 1))
done <"$old_fast/all-legacy-pre-epochs.txt"
[ "$candidate_count" -eq 211 ] || die "candidate count is $candidate_count, expected 211"

complete=$recovery_root/complete.json
complete_candidate=$complete.building-$$
completed_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
jq -cn --arg completed "$completed_at" \
    --arg admission "$admission" --arg admission_sha "$(sha256_file "$admission")" \
    --arg result103 "$recovery_root/results/epoch-103.json" \
    --arg result103_sha "$(sha256_file "$recovery_root/results/epoch-103.json")" \
    --arg result104 "$recovery_root/results/epoch-104.json" \
    --arg result104_sha "$(sha256_file "$recovery_root/results/epoch-104.json")" '
    {schema_version:1,kind:"archive-v2-pre-to-post-reprocess-complete",
     epochs:[103,104],candidate_count:211,threads:10,
     admission:{path:$admission,sha256:$admission_sha},
     results:[{epoch:103,path:$result103,sha256:$result103_sha},
              {epoch:104,path:$result104,sha256:$result104_sha}],
     completed_at_utc:$completed,old_partial_output_used:false,
     canonical:false,canonical_checks_deferred:true,backups_retained:true}
' >"$complete_candidate" || die 'cannot create recovery completion record'
publish_file "$complete_candidate" "$complete"

all_complete=$old_fast/all-211-complete.json
all_candidate=$all_complete.building-$$
jq -cn --arg completed "$completed_at" \
    --arg recovery "$complete" --arg recovery_sha "$(sha256_file "$complete")" \
    --arg clean "$clean_complete" --arg clean_sha "$(sha256_file "$clean_complete")" '
    {schema_version:1,kind:"archive-v2-pre-to-post-all-candidates-complete",
     completed_epoch_count:211,reprocessed_epochs:[103,104],
     clean_batch:{path:$clean,sha256:$clean_sha},
     recovery:{path:$recovery,sha256:$recovery_sha},
     completed_at_utc:$completed,canonical:false,
     canonical_checks_deferred:true,backups_retained:true}
' >"$all_candidate" || die 'cannot create all-211 completion record'
publish_file "$all_candidate" "$all_complete"

release_locks
trap - 0 1 2 3 15
jq -c . "$all_complete"
