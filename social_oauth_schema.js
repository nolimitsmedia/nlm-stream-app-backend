// social_oauth_schema.js
//
// OAuth account schema + token-at-rest helpers for Facebook/YouTube automation.
// Tokens remain in the existing TEXT columns for backwards compatibility, but
// are stored as AES-256-GCM envelopes instead of plaintext.
//
// Preferred secret: OAUTH_TOKEN_ENCRYPTION_KEY
// Rolling-deploy fallback: JWT_SECRET (keeps existing installs working until a
// dedicated encryption key is added to .env). For best security, configure a
// separate random OAUTH_TOKEN_ENCRYPTION_KEY in production.

const crypto = require("crypto");

const TOKEN_PREFIX = "enc:v1:";

function getTokenEncryptionSecrets() {
  const values = [
    process.env.OAUTH_TOKEN_ENCRYPTION_KEY,
    process.env.JWT_SECRET,
  ]
    .map((value) => String(value || "").trim())
    .filter(Boolean);
  return [...new Set(values)];
}

function deriveTokenEncryptionKey(secret) {
  return crypto.createHash("sha256").update(secret).digest();
}

function getPrimaryTokenEncryptionKey() {
  const secrets = getTokenEncryptionSecrets();
  if (!secrets.length) {
    throw new Error(
      "OAuth token encryption is not configured. Set OAUTH_TOKEN_ENCRYPTION_KEY (preferred) or JWT_SECRET.",
    );
  }
  return deriveTokenEncryptionKey(secrets[0]);
}

function isEncryptedToken(value) {
  return typeof value === "string" && value.startsWith(TOKEN_PREFIX);
}

function encryptOAuthToken(value) {
  if (value === null || value === undefined || value === "")
    return value || null;
  const text = String(value);
  if (isEncryptedToken(text)) return text;

  const key = getPrimaryTokenEncryptionKey();
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv("aes-256-gcm", key, iv);
  const ciphertext = Buffer.concat([
    cipher.update(text, "utf8"),
    cipher.final(),
  ]);
  const tag = cipher.getAuthTag();

  return [
    TOKEN_PREFIX.slice(0, -1),
    iv.toString("base64url"),
    tag.toString("base64url"),
    ciphertext.toString("base64url"),
  ].join(":");
}

function decryptOAuthToken(value) {
  if (value === null || value === undefined || value === "")
    return value || null;
  const text = String(value);

  // Backwards compatibility for rows written before token encryption shipped.
  if (!isEncryptedToken(text)) return text;

  const parts = text.split(":");
  if (parts.length !== 5 || parts[0] !== "enc" || parts[1] !== "v1") {
    throw new Error("Unsupported OAuth token envelope");
  }

  const iv = Buffer.from(parts[2], "base64url");
  const tag = Buffer.from(parts[3], "base64url");
  const ciphertext = Buffer.from(parts[4], "base64url");
  const secrets = getTokenEncryptionSecrets();
  if (!secrets.length) {
    throw new Error("OAuth token encryption key is unavailable");
  }

  let lastError = null;
  for (const secret of secrets) {
    try {
      const decipher = crypto.createDecipheriv(
        "aes-256-gcm",
        deriveTokenEncryptionKey(secret),
        iv,
      );
      decipher.setAuthTag(tag);
      return Buffer.concat([
        decipher.update(ciphertext),
        decipher.final(),
      ]).toString("utf8");
    } catch (error) {
      lastError = error;
    }
  }

  throw new Error(
    `Unable to decrypt OAuth token with configured key(s): ${lastError?.message || "authentication failed"}`,
  );
}

function decryptOAuthAccount(account) {
  if (!account) return account;
  return {
    ...account,
    access_token: decryptOAuthToken(account.access_token),
    refresh_token: decryptOAuthToken(account.refresh_token),
  };
}

async function encryptExistingPlaintextTokens(pool) {
  const result = await pool.query(
    `SELECT id, access_token, refresh_token
     FROM social_oauth_accounts
     WHERE access_token IS NOT NULL OR refresh_token IS NOT NULL`,
  );

  for (const row of result.rows) {
    const nextAccess = row.access_token
      ? encryptOAuthToken(row.access_token)
      : row.access_token;
    const nextRefresh = row.refresh_token
      ? encryptOAuthToken(row.refresh_token)
      : row.refresh_token;

    if (nextAccess === row.access_token && nextRefresh === row.refresh_token) {
      continue;
    }

    await pool.query(
      `UPDATE social_oauth_accounts
       SET access_token = $1,
           refresh_token = $2,
           token_encryption_version = 1,
           updated_at = NOW()
       WHERE id = $3`,
      [nextAccess, nextRefresh, row.id],
    );
  }
}

async function ensureSocialOAuthTables(pool) {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS social_oauth_accounts (
      id SERIAL PRIMARY KEY,
      organization_id INTEGER REFERENCES organizations(id) ON DELETE CASCADE,
      platform VARCHAR(20) NOT NULL,
      external_account_id VARCHAR(255) NOT NULL,
      external_account_name VARCHAR(255),
      access_token TEXT NOT NULL,
      refresh_token TEXT,
      token_expires_at TIMESTAMPTZ,
      connected_by_admin_id INTEGER REFERENCES admins(id) ON DELETE SET NULL,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE (organization_id, platform, external_account_id)
    )
  `);

  await pool.query(`
    ALTER TABLE social_oauth_accounts
    ADD COLUMN IF NOT EXISTS token_encryption_version INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS connection_status VARCHAR(32) DEFAULT 'connected',
    ADD COLUMN IF NOT EXISTS token_last_validated_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS token_last_error TEXT,
    ADD COLUMN IF NOT EXISTS reconnect_required_at TIMESTAMPTZ
  `);

  await pool.query(`
    ALTER TABLE social_destinations
    ADD COLUMN IF NOT EXISTS oauth_account_id INTEGER REFERENCES social_oauth_accounts(id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS platform_broadcast_id VARCHAR(255),
    ADD COLUMN IF NOT EXISTS platform_stream_id VARCHAR(255),
    ADD COLUMN IF NOT EXISTS automation_mode VARCHAR(20) DEFAULT 'manual'
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_social_oauth_accounts_org
    ON social_oauth_accounts (organization_id, platform)
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_social_destinations_oauth_account
    ON social_destinations (oauth_account_id)
  `);

  // One-time/rolling migration: encrypt any old plaintext credentials in place.
  await encryptExistingPlaintextTokens(pool);
}

module.exports = {
  ensureSocialOAuthTables,
  encryptOAuthToken,
  decryptOAuthToken,
  decryptOAuthAccount,
  isEncryptedToken,
};
