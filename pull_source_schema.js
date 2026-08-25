// pull_source_schema.js
// External Pull Sources — durable configuration + encrypted source URL storage.
// Phase 1 Input Failover adds multiple sources per channel plus channel-level
// failover/failback configuration while preserving existing encrypted URLs.

const crypto = require("crypto");

const ENCRYPTION_PREFIX = "enc:v1:";

function getEncryptionKey() {
  const raw =
    process.env.PULL_SOURCE_ENCRYPTION_KEY ||
    process.env.OAUTH_TOKEN_ENCRYPTION_KEY ||
    "";

  if (!raw) {
    throw new Error(
      "PULL_SOURCE_ENCRYPTION_KEY (or OAUTH_TOKEN_ENCRYPTION_KEY fallback) is not configured",
    );
  }

  if (/^[0-9a-f]{64}$/i.test(raw)) return Buffer.from(raw, "hex");
  return crypto.createHash("sha256").update(raw).digest();
}

function isEncryptedSourceUrl(value) {
  return typeof value === "string" && value.startsWith(ENCRYPTION_PREFIX);
}

function encryptSourceUrl(value) {
  if (value === null || value === undefined || value === "") return null;
  const text = String(value);
  if (isEncryptedSourceUrl(text)) return text;

  const key = getEncryptionKey();
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv("aes-256-gcm", key, iv);
  const ciphertext = Buffer.concat([
    cipher.update(text, "utf8"),
    cipher.final(),
  ]);
  const tag = cipher.getAuthTag();

  return `${ENCRYPTION_PREFIX}${Buffer.concat([iv, tag, ciphertext]).toString("base64url")}`;
}

function decryptSourceUrl(value) {
  if (value === null || value === undefined || value === "") return null;
  const text = String(value);
  if (!isEncryptedSourceUrl(text)) return text;

  const payload = Buffer.from(
    text.slice(ENCRYPTION_PREFIX.length),
    "base64url",
  );
  if (payload.length < 29)
    throw new Error("Encrypted pull-source URL is invalid");

  const iv = payload.subarray(0, 12);
  const tag = payload.subarray(12, 28);
  const ciphertext = payload.subarray(28);
  const decipher = crypto.createDecipheriv(
    "aes-256-gcm",
    getEncryptionKey(),
    iv,
  );
  decipher.setAuthTag(tag);

  return Buffer.concat([
    decipher.update(ciphertext),
    decipher.final(),
  ]).toString("utf8");
}

function maskSourceUrl(value) {
  if (!value) return null;
  try {
    const parsed = new URL(String(value));
    if (parsed.username) parsed.username = "***";
    if (parsed.password) parsed.password = "***";

    for (const key of [
      "token",
      "key",
      "stream_key",
      "passphrase",
      "password",
      "auth",
      "sig",
      "signature",
    ]) {
      if (parsed.searchParams.has(key)) parsed.searchParams.set(key, "***");
    }

    return parsed.toString();
  } catch {
    return String(value).replace(/([^/?#]{6})[^/?#]{4,}/g, "$1***");
  }
}

async function ensurePullSourceTables(pool) {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS channel_pull_sources (
      id SERIAL PRIMARY KEY,
      channel_id INTEGER NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
      organization_id INTEGER NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
      name VARCHAR(255) NOT NULL DEFAULT 'Pull Source',
      protocol VARCHAR(20) NOT NULL,
      source_url TEXT NOT NULL,
      source_url_display TEXT,
      enabled BOOLEAN NOT NULL DEFAULT TRUE,
      auto_start BOOLEAN NOT NULL DEFAULT FALSE,
      auto_reconnect BOOLEAN NOT NULL DEFAULT TRUE,
      status VARCHAR(32) NOT NULL DEFAULT 'stopped',
      is_running BOOLEAN NOT NULL DEFAULT FALSE,
      started_at TIMESTAMPTZ,
      stopped_at TIMESTAMPTZ,
      last_error TEXT,
      last_error_code VARCHAR(64),
      reconnect_count INTEGER NOT NULL DEFAULT 0,
      role VARCHAR(16) NOT NULL DEFAULT 'primary',
      priority INTEGER NOT NULL DEFAULT 1,
      is_active_source BOOLEAN NOT NULL DEFAULT FALSE,
      health_status VARCHAR(24) NOT NULL DEFAULT 'unknown',
      last_health_check_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  // Rolling migration from the original one-source-per-channel design.
  await pool.query(`
    ALTER TABLE channel_pull_sources
      ADD COLUMN IF NOT EXISTS source_url_display TEXT,
      ADD COLUMN IF NOT EXISTS last_error_code VARCHAR(64),
      ADD COLUMN IF NOT EXISTS reconnect_count INTEGER NOT NULL DEFAULT 0,
      ADD COLUMN IF NOT EXISTS stopped_at TIMESTAMPTZ,
      ADD COLUMN IF NOT EXISTS role VARCHAR(16) NOT NULL DEFAULT 'primary',
      ADD COLUMN IF NOT EXISTS priority INTEGER NOT NULL DEFAULT 1,
      ADD COLUMN IF NOT EXISTS is_active_source BOOLEAN NOT NULL DEFAULT FALSE,
      ADD COLUMN IF NOT EXISTS health_status VARCHAR(24) NOT NULL DEFAULT 'unknown',
      ADD COLUMN IF NOT EXISTS last_health_check_at TIMESTAMPTZ
  `);

  // The legacy schema enforced UNIQUE(channel_id). Drop that constraint so a
  // channel can own Primary + Backup sources. PostgreSQL's default constraint
  // name is used by the original CREATE TABLE statement.
  await pool.query(`
    ALTER TABLE channel_pull_sources
      DROP CONSTRAINT IF EXISTS channel_pull_sources_channel_id_key
  `);

  // Existing single-source channels become Primary / priority 1 automatically.
  await pool.query(`
    UPDATE channel_pull_sources
    SET role='primary', priority=1
    WHERE role IS NULL OR role NOT IN ('primary','backup') OR priority IS NULL
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_channel_pull_sources_org
    ON channel_pull_sources (organization_id)
  `);
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_channel_pull_sources_channel_priority
    ON channel_pull_sources (channel_id, priority, id)
  `);
  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_channel_pull_sources_one_active
    ON channel_pull_sources (channel_id)
    WHERE is_active_source=TRUE
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS channel_source_failover (
      channel_id INTEGER PRIMARY KEY REFERENCES channels(id) ON DELETE CASCADE,
      organization_id INTEGER NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
      enabled BOOLEAN NOT NULL DEFAULT FALSE,
      failure_threshold_seconds INTEGER NOT NULL DEFAULT 5,
      failback_enabled BOOLEAN NOT NULL DEFAULT TRUE,
      failback_stability_seconds INTEGER NOT NULL DEFAULT 15,
      active_source_id INTEGER REFERENCES channel_pull_sources(id) ON DELETE SET NULL,
      last_switch_at TIMESTAMPTZ,
      last_switch_reason VARCHAR(64),
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_channel_source_failover_org
    ON channel_source_failover (organization_id)
  `);

  // Keep old plaintext development rows encrypted in-place.
  const rows = await pool.query(`
    SELECT id, source_url
    FROM channel_pull_sources
    WHERE source_url IS NOT NULL
  `);

  for (const row of rows.rows) {
    if (isEncryptedSourceUrl(row.source_url)) continue;
    const encrypted = encryptSourceUrl(row.source_url);
    const display = maskSourceUrl(row.source_url);
    await pool.query(
      `UPDATE channel_pull_sources
       SET source_url=$1,
           source_url_display=COALESCE(source_url_display,$2),
           updated_at=NOW()
       WHERE id=$3`,
      [encrypted, display, row.id],
    );
  }
}

module.exports = {
  ensurePullSourceTables,
  encryptSourceUrl,
  decryptSourceUrl,
  isEncryptedSourceUrl,
  maskSourceUrl,
};
