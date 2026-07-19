// social_oauth_schema.js
//
// New table for OAuth-connected Facebook Pages / YouTube channels, plus the
// columns social_destinations needs to know a destination is automated
// rather than a manually-pasted stream key.
//
// Follows the same ensureXTable() + IF NOT EXISTS pattern as the rest of the
// app's schema (see ensureRecordingLibraryTable, ensureFeatureFlagsTable,
// etc. in server.js) so it's safe to run on every boot.

async function ensureSocialOAuthTables(pool) {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS social_oauth_accounts (
      id SERIAL PRIMARY KEY,
      organization_id INTEGER REFERENCES organizations(id) ON DELETE CASCADE,
      platform VARCHAR(20) NOT NULL, -- 'facebook' | 'youtube'
      external_account_id VARCHAR(255) NOT NULL, -- Facebook Page ID or YouTube Channel ID
      external_account_name VARCHAR(255),
      access_token TEXT NOT NULL,
      refresh_token TEXT, -- YouTube only; Facebook page tokens are long-lived, no refresh token
      token_expires_at TIMESTAMPTZ, -- null for Facebook page tokens (no fixed expiry)
      connected_by_admin_id INTEGER REFERENCES admins(id) ON DELETE SET NULL,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE (organization_id, platform, external_account_id)
    )
  `);

  // Link a social_destinations row to the OAuth account used to automate it,
  // and track the platform-side broadcast so we can end it from our side too.
  await pool.query(`
    ALTER TABLE social_destinations
    ADD COLUMN IF NOT EXISTS oauth_account_id INTEGER REFERENCES social_oauth_accounts(id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS platform_broadcast_id VARCHAR(255),
    ADD COLUMN IF NOT EXISTS platform_stream_id VARCHAR(255),
    ADD COLUMN IF NOT EXISTS automation_mode VARCHAR(20) DEFAULT 'manual'
  `);
  // automation_mode: 'manual' (existing pasted-key behavior, untouched) |
  // 'oauth' (new — key comes from a freshly created platform broadcast)

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_social_oauth_accounts_org
    ON social_oauth_accounts (organization_id, platform)
  `);
}

module.exports = { ensureSocialOAuthTables };
