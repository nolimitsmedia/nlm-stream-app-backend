// media_node_migration_schema.js
// Phase 5F.2 — durable Media Node migration state and audit history.
//
// This module provides migration persistence only. It does not move live media,
// change channels.media_node_id, or execute work on a Media Node.

async function ensureMediaNodeMigrationTables(pool) {
  if (!pool) {
    throw new Error("Media Node migration schema requires a database pool");
  }

  await pool.query(`
    CREATE TABLE IF NOT EXISTS channel_media_node_migrations (
      id BIGSERIAL PRIMARY KEY,

      channel_id INTEGER NOT NULL
        REFERENCES channels(id) ON DELETE CASCADE,

      organization_id INTEGER NOT NULL
        REFERENCES organizations(id) ON DELETE CASCADE,

      source_media_node_id BIGINT
        REFERENCES media_nodes(id) ON DELETE SET NULL,

      destination_media_node_id BIGINT
        REFERENCES media_nodes(id) ON DELETE SET NULL,

      requested_by_admin_id INTEGER
        REFERENCES admins(id) ON DELETE SET NULL,

      status VARCHAR(32) NOT NULL DEFAULT 'requested',
      reason TEXT,

      started_at TIMESTAMPTZ,
      prepared_at TIMESTAMPTZ,
      ownership_committed_at TIMESTAMPTZ,
      completed_at TIMESTAMPTZ,
      failed_at TIMESTAMPTZ,

      failure_code VARCHAR(64),
      failure_message TEXT,

      metadata JSONB NOT NULL DEFAULT '{}'::jsonb,

      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

      CONSTRAINT channel_media_node_migrations_status_check
        CHECK (
          status IN (
            'requested',
            'validating',
            'prepared',
            'stopping_source',
            'ownership_committing',
            'completed',
            'failed',
            'cancelled'
          )
        ),

      CONSTRAINT channel_media_node_migrations_distinct_nodes_check
        CHECK (
          source_media_node_id IS NULL
          OR destination_media_node_id IS NULL
          OR source_media_node_id <> destination_media_node_id
        )
    )
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_channel_media_node_migrations_channel
    ON channel_media_node_migrations (channel_id, created_at DESC)
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_channel_media_node_migrations_org
    ON channel_media_node_migrations (organization_id, created_at DESC)
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_channel_media_node_migrations_source_node
    ON channel_media_node_migrations (source_media_node_id)
    WHERE source_media_node_id IS NOT NULL
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_channel_media_node_migrations_destination_node
    ON channel_media_node_migrations (destination_media_node_id)
    WHERE destination_media_node_id IS NOT NULL
  `);

  // A channel may have migration history, but only one unfinished migration
  // may own the migration workflow at a time.
  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_channel_media_node_migrations_one_active
    ON channel_media_node_migrations (channel_id)
    WHERE status IN (
      'requested',
      'validating',
      'prepared',
      'stopping_source',
      'ownership_committing'
    )
  `);
}

module.exports = {
  ensureMediaNodeMigrationTables,
};
