// pull_source_schema.js
// External Pull Sources — durable configuration + encrypted source URL storage.

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

  // Accept a 64-char hex key or derive a stable 32-byte key from any secret.
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
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE(channel_id)
    )
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_channel_pull_sources_org
    ON channel_pull_sources (organization_id)
  `);

  // Rolling migration for early development copies of the table.
  await pool.query(`
    ALTER TABLE channel_pull_sources
      ADD COLUMN IF NOT EXISTS source_url_display TEXT,
      ADD COLUMN IF NOT EXISTS last_error_code VARCHAR(64),
      ADD COLUMN IF NOT EXISTS reconnect_count INTEGER NOT NULL DEFAULT 0,
      ADD COLUMN IF NOT EXISTS stopped_at TIMESTAMPTZ
  `);

  // Encrypt any plaintext URLs left by a pre-encryption development build.
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
       SET source_url=$1, source_url_display=COALESCE(source_url_display,$2), updated_at=NOW()
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
