ALTER TABLE keymap ADD COLUMN id INTEGER;
WITH ranked AS (
  SELECT pubkey, ROW_NUMBER() OVER (ORDER BY views DESC) AS rid
  FROM keymap
)
UPDATE keymap
SET id = (
  SELECT rid FROM ranked WHERE ranked.pubkey = keymap.pubkey
);
VACUUM;