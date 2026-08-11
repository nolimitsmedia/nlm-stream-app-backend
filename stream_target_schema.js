// stream_target_schema.js
// Phase 2 — Generic Stream Targets
//
// Extends the existing social_destinations table instead of replacing it.
// This preserves every existing Facebook/YouTube/Instagram destination and
// its OAuth foreign-key linkage while allowing the same rows to represent
// generic RTMP/RTMPS/SRT/NLM/CDN targets.

async function ensureStreamTargetColumns(pool) {
  await pool.query(`
    ALTER TABLE social_destinations
      ADD COLUMN IF NOT EXISTS name VARCHAR(255),
      ADD COLUMN IF NOT EXISTS target_type VARCHAR(40),
      ADD COLUMN IF NOT EXISTS destination_url TEXT,
      ADD COLUMN IF NOT EXISTS protocol VARCHAR(20),
      ADD COLUMN IF NOT EXISTS enabled BOOLEAN DEFAULT TRUE,
      ADD COLUMN IF NOT EXISTS auto_start BOOLEAN DEFAULT FALSE,
      ADD COLUMN IF NOT EXISTS auto_reconnect BOOLEAN DEFAULT TRUE,
      ADD COLUMN IF NOT EXISTS status VARCHAR(30) DEFAULT 'stopped',
      ADD COLUMN IF NOT EXISTS reconnect_count INTEGER DEFAULT 0,
      ADD COLUMN IF NOT EXISTS dropped_frames BIGINT DEFAULT 0,
      ADD COLUMN IF NOT EXISTS current_bitrate_kbps INTEGER DEFAULT 0,
      ADD COLUMN IF NOT EXISTS last_error TEXT,
      ADD COLUMN IF NOT EXISTS last_connected_at TIMESTAMPTZ,
      ADD COLUMN IF NOT EXISTS last_disconnected_at TIMESTAMPTZ,
      ADD COLUMN IF NOT EXISTS active_destination_url TEXT,
      ADD COLUMN IF NOT EXISTS target_metadata JSONB DEFAULT '{}'::jsonb
  `);

  // Backfill existing social rows into the generic target model.
  await pool.query(`
    UPDATE social_destinations
    SET
      target_type = COALESCE(target_type, platform),
      name = COALESCE(
        NULLIF(name, ''),
        CASE platform
          WHEN 'facebook' THEN 'Facebook'
          WHEN 'youtube' THEN 'YouTube'
          WHEN 'instagram' THEN 'Instagram'
          ELSE INITCAP(REPLACE(COALESCE(platform, 'Stream Target'), '_', ' '))
        END
      ),
      protocol = COALESCE(
        protocol,
        CASE
          WHEN platform IN ('facebook', 'instagram') THEN 'rtmps'
          ELSE 'rtmp'
        END
      ),
      enabled = COALESCE(enabled, TRUE),
      auto_start = COALESCE(auto_start, automation_mode = 'oauth'),
      auto_reconnect = COALESCE(auto_reconnect, TRUE),
      status = COALESCE(status, CASE WHEN is_running THEN 'streaming' ELSE 'stopped' END),
      reconnect_count = COALESCE(reconnect_count, 0),
      dropped_frames = COALESCE(dropped_frames, 0),
      current_bitrate_kbps = COALESCE(current_bitrate_kbps, 0),
      target_metadata = COALESCE(target_metadata, '{}'::jsonb)
  `);

  // Existing OAuth targets auto-fired on every publish before Phase 2. Preserve
  // that behavior explicitly through the new auto_start flag.
  await pool.query(`
    UPDATE social_destinations
    SET auto_start = TRUE
    WHERE automation_mode = 'oauth'
      AND oauth_account_id IS NOT NULL
      AND auto_start IS DISTINCT FROM TRUE
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_stream_targets_channel_enabled
    ON social_destinations (channel_id, enabled)
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_stream_targets_channel_autostart
    ON social_destinations (channel_id, auto_start)
  `);
}

module.exports = { ensureStreamTargetColumns };
