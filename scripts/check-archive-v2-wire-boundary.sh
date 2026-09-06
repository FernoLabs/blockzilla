#!/usr/bin/env bash
set -euo pipefail

# Archive V2 readers must select one generation-bound wire profile through
# blockzilla-compact-v2-reader. Keep this small allowlist for existing migration debt;
# a new direct decoder, including one added to an allowlisted file, changes the
# exact report and fails CI.
pattern='ArchiveV2HotMessagePayload\s*=\s*wincode::config::deserialize|deserialize(?:_exact)?::<\s*ArchiveV2HotMessagePayload|fn\s+decode_message[\s\S]{0,1200}wincode::config::deserialize'

actual="$({
  rg --count-matches -U "$pattern" \
    --glob '*.rs' \
    --glob '!crates/blockzilla-format/**' \
    --glob '!crates/compact-v2/blockzilla-compact-v2-reader/**' \
    . || true
} | sed 's#^\./##' | LC_ALL=C sort)"

expected="$(LC_ALL=C sort <<'EOF'
blockzilla/cli/src/archive_v2.rs:3
blockzilla/cli/src/archive_v2/registry_reprocess.rs:2
blockzilla/cli/src/bin/upgrade_block_access_vote_hashes.rs:2
blockzilla/cli/src/bin/verify_access_vote_hashes.rs:1
blockzilla/cli/src/token_events.rs:5
indexer/blockzilla-token-transaction-dump/src/consolidate_v3.rs:1
examples/token-api/src/indexer.rs:1
edgezilla/get-block/src/worker.rs:1
EOF
)"

if [[ "$actual" != "$expected" ]]; then
  echo 'Archive V2 wire boundary changed.' >&2
  echo 'New readers and indexers must use the format reader with generation-bound schema validation.' >&2
  echo 'Expected legacy exceptions:' >&2
  printf '%s\n' "$expected" >&2
  echo 'Found:' >&2
  printf '%s\n' "$actual" >&2
  exit 1
fi

