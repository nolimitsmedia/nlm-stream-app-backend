// Phase 2 (2026-10-20) — public playback identifiers.
//
// channels.stream_key is the INGEST credential: whoever knows it can publish
// to the channel (SRS on_publish authorizes on it). Before Phase 2 the same
// value was also the public watch identifier, so every watch link, manifest
// URL, segment URL and embed status response handed viewers the publishing
// credential.
//
// channels.playback_id is a separate, public, non-secret identifier
// ("pb_" + 32 hex chars from gen_random_uuid()). Viewer-facing routes accept it
// and translate it to the stream key server-side; viewers never receive the
// key. It is stable across stream-key rotation, so rotating a compromised key
// no longer breaks shared watch links.
//
// This migration is additive and idempotent:
//   1. ADD COLUMN IF NOT EXISTS (nullable, no default => no table rewrite)
//   2. SET DEFAULT so every future INSERT gets an id with no code changes
//   3. backfill existing rows that are still NULL
//   4. unique index
// Rollback: older code simply ignores the column. Nothing existing is altered.
// gen_random_uuid() is built into PostgreSQL 13+ (production runs 18.6).

const PLAYBACK_ID_SQL_EXPRESSION =
  "('pb_' || replace(gen_random_uuid()::text, '-', ''))";

// Exactly what the SQL expression produces: "pb_" + 32 lowercase hex chars.
const PLAYBACK_ID_PATTERN = /^pb_[0-9a-f]{32}$/;
const PLAYBACK_ID_SEARCH_PATTERN = /pb_[0-9a-f]{32}/g;

async function ensurePlaybackIdColumns(pool) {
  await pool.query(
    `ALTER TABLE channels ADD COLUMN IF NOT EXISTS playback_id VARCHAR(40)`,
  );
  await pool.query(
    `ALTER TABLE channels ALTER COLUMN playback_id SET DEFAULT ${PLAYBACK_ID_SQL_EXPRESSION}`,
  );
  const backfill = await pool.query(
    `UPDATE channels
        SET playback_id = ${PLAYBACK_ID_SQL_EXPRESSION}
      WHERE playback_id IS NULL`,
  );
  await pool.query(
    `CREATE UNIQUE INDEX IF NOT EXISTS channels_playback_id_key
       ON channels (playback_id)`,
  );

  if (backfill?.rowCount) {
    console.log(
      `[PLAYBACK-ID] Assigned playback IDs to ${backfill.rowCount} existing channel(s).`,
    );
  }
}

function isPlaybackId(value) {
  return PLAYBACK_ID_PATTERN.test(String(value || ""));
}

module.exports = {
  ensurePlaybackIdColumns,
  isPlaybackId,
  PLAYBACK_ID_PATTERN,
  PLAYBACK_ID_SEARCH_PATTERN,
  PLAYBACK_ID_SQL_EXPRESSION,
};
