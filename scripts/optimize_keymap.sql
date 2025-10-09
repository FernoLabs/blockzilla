-- Rebuild keymap with compact sequential IDs, smallest ID = most used pubkey
-- Preserves views, min_slot, and max_slot if present.

PRAGMA synchronous = OFF;
PRAGMA journal_mode = WAL;
PRAGMA temp_store = MEMORY;
PRAGMA cache_size = 200000;

BEGIN TRANSACTION;

-- 1️⃣ Drop old ranked table if it exists (safety)
DROP TABLE IF EXISTS keymap_ranked;

-- 2️⃣ Create new ranked mapping table with min/max slot support
CREATE TABLE keymap_ranked (
    id        INTEGER PRIMARY KEY,   -- smallest id = most used pubkey
    pubkey    BLOB UNIQUE NOT NULL,
    views     INTEGER NOT NULL,
    min_slot  INTEGER,
    max_slot  INTEGER
);

-- 3️⃣ Insert ranked data, preserving slot bounds if columns exist
INSERT INTO keymap_ranked (id, pubkey, views, min_slot, max_slot)
SELECT
    ROW_NUMBER() OVER (ORDER BY views DESC) - 1 AS id,
    pubkey,
    views,
    CASE
        WHEN EXISTS (SELECT 1 FROM pragma_table_info('keymap') WHERE name='min_slot') THEN min_slot
        ELSE NULL
    END,
    CASE
        WHEN EXISTS (SELECT 1 FROM pragma_table_info('keymap') WHERE name='max_slot') THEN max_slot
        ELSE NULL
    END
FROM keymap
ORDER BY views DESC;

COMMIT;

-- 4️⃣ (Optional) swap in the new table once verified
-- DROP TABLE keymap;
-- ALTER TABLE keymap_ranked RENAME TO keymap;

VACUUM;
