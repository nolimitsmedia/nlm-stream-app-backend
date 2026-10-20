-- NLM Streaming App — Phase 2: public playback IDs
-- Date: 2026-10-20
--
-- The backend applies this automatically at startup (playback_id_schema.js,
-- ensurePlaybackIdColumns). This file documents the exact change and can be
-- run by hand for review or on a fresh database. Every statement is
-- idempotent and additive: no existing column, row value or constraint is
-- modified or removed.
--
-- channels.stream_key  = ingest credential (secret; unchanged)
-- channels.playback_id = public viewer identifier ("pb_" + 32 hex)
--
-- Requires PostgreSQL 13+ for gen_random_uuid() (production: 18.6).

BEGIN;

-- 1. Nullable column with no default: metadata-only, no table rewrite.
ALTER TABLE channels ADD COLUMN IF NOT EXISTS playback_id VARCHAR(40);

-- 2. Every future channel row gets an id automatically (no INSERT changes).
ALTER TABLE channels
  ALTER COLUMN playback_id
  SET DEFAULT ('pb_' || replace(gen_random_uuid()::text, '-', ''));

-- 3. Backfill existing channels.
UPDATE channels
   SET playback_id = 'pb_' || replace(gen_random_uuid()::text, '-', '')
 WHERE playback_id IS NULL;

-- 4. Uniqueness (also the lookup index for playback-ID resolution).
CREATE UNIQUE INDEX IF NOT EXISTS channels_playback_id_key
  ON channels (playback_id);

COMMIT;

-- Verification:
--   SELECT COUNT(*) AS channels,
--          COUNT(playback_id) AS with_playback_id,
--          COUNT(DISTINCT playback_id) AS distinct_ids
--     FROM channels;
--   -- all three numbers must be equal
--
-- Rollback (normally unnecessary — older code ignores the column):
--   Reverting the application code is sufficient. Dropping the column is
--   destructive (watch links built from playback IDs stop working for good)
--   and should only be done deliberately:
--     DROP INDEX IF EXISTS channels_playback_id_key;
--     ALTER TABLE channels DROP COLUMN IF EXISTS playback_id;
