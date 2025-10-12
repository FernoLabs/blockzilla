#!/usr/bin/env bash
set -euo pipefail

SRC_DIR="${1:-optimized}"
DEST_DB="${2:-merged_pubkeys.sqlite}"

echo "🚀 Fast sequential merge from $SRC_DIR → $DEST_DB (tracking first + last epoch)"

# --- Prepare destination DB ---
sqlite3 "$DEST_DB" <<'SQL'
PRAGMA journal_mode = WAL;
PRAGMA synchronous = OFF;
PRAGMA temp_store = MEMORY;
PRAGMA cache_size = -200000;        -- ~200MB cache
PRAGMA locking_mode = EXCLUSIVE;
PRAGMA defer_foreign_keys = ON;

CREATE TABLE IF NOT EXISTS pubkeys (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    pubkey       BLOB UNIQUE,
    first_epoch  INTEGER,
    last_epoch   INTEGER
);

-- Drop indexes during merge for speed
DROP INDEX IF EXISTS idx_pubkeys;
DROP INDEX IF EXISTS idx_first_epoch;
DROP INDEX IF EXISTS idx_last_epoch;
SQL

# --- Merge loop ---
for DB in $(ls "$SRC_DIR"/pubkeys-*.sqlite | sort); do
  [ -e "$DB" ] || continue
  FILE=$(basename "$DB")
  EPOCH=$(echo "$FILE" | grep -oE '[0-9]+' | tail -1)
  echo "→ Merging $FILE (epoch $EPOCH)"

  sqlite3 "$DEST_DB" <<SQL
PRAGMA synchronous = OFF;
PRAGMA temp_store = MEMORY;
PRAGMA locking_mode = EXCLUSIVE;
BEGIN TRANSACTION;

ATTACH DATABASE '$DB' AS src;

-- Insert new pubkeys (first + last epoch = current)
INSERT OR IGNORE INTO pubkeys (pubkey, first_epoch, last_epoch)
SELECT pubkey, $EPOCH, $EPOCH FROM src.pubkeys;

-- Update last_epoch for existing pubkeys
UPDATE pubkeys
SET last_epoch = $EPOCH
WHERE pubkey IN (SELECT pubkey FROM src.pubkeys);

DETACH DATABASE src;
COMMIT;
SQL

  echo "   ✅ Epoch $EPOCH merged."
done

# --- Final optimization phase ---
echo "🧹 Optimizing and checkpointing..."
sqlite3 "$DEST_DB" <<'SQL'
PRAGMA wal_checkpoint(TRUNCATE);
PRAGMA journal_mode = DELETE;
PRAGMA synchronous = NORMAL;

CREATE INDEX IF NOT EXISTS idx_pubkeys ON pubkeys(pubkey);
CREATE INDEX IF NOT EXISTS idx_first_epoch ON pubkeys(first_epoch);
CREATE INDEX IF NOT EXISTS idx_last_epoch ON pubkeys(last_epoch);

VACUUM;
SQL

# --- Summary ---
echo "📊 Summary:"
sqlite3 "$DEST_DB" 'SELECT COUNT(*) AS total_pubkeys, MIN(first_epoch), MAX(last_epoch) FROM pubkeys;'

echo "✅ Merge complete — optimized for large datasets."
