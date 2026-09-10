// stream_target_schema.js
// Phase 5A.4b — Generic Stream Target credential encryption at rest.
//
// Extends the existing social_destinations table instead of replacing it.
// This preserves every existing Facebook/YouTube/Instagram destination and
// its OAuth foreign-key linkage while allowing the same rows to represent
// generic RTMP/RTMPS/SRT/NLM/CDN targets.

const crypto = require("crypto");

const TARGET_CREDENTIAL_PREFIX = "stenc:v1:";

function getTargetCredentialSecrets() {
  const values = [
    process.env.STREAM_TARGET_CREDENTIAL_ENCRYPTION_KEY,
    process.env.OAUTH_TOKEN_ENCRYPTION_KEY,
    process.env.JWT_SECRET,
  ]
    .map((value) => String(value || "").trim())
    .filter(Boolean);
  return [...new Set(values)];
}

function deriveTargetCredentialKey(secret) {
  return crypto
    .createHash("sha256")
    .update(`nlm-stream-target-credential:v1:${secret}`)
    .digest();
}

function getPrimaryTargetCredentialKey() {
  const secrets = getTargetCredentialSecrets();
  if (!secrets.length) {
    throw new Error(
      "Stream Target credential encryption is not configured. Set STREAM_TARGET_CREDENTIAL_ENCRYPTION_KEY (preferred), OAUTH_TOKEN_ENCRYPTION_KEY, or JWT_SECRET.",
    );
  }
  return deriveTargetCredentialKey(secrets[0]);
}

function isEncryptedTargetCredential(value) {
  return (
    typeof value === "string" && value.startsWith(TARGET_CREDENTIAL_PREFIX)
  );
}

function encryptTargetCredential(value) {
  if (value === null || value === undefined || value === "") {
    return value || null;
  }
  const text = String(value);
  if (isEncryptedTargetCredential(text)) return text;

  const key = getPrimaryTargetCredentialKey();
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv("aes-256-gcm", key, iv);
  const ciphertext = Buffer.concat([
    cipher.update(text, "utf8"),
    cipher.final(),
  ]);
  const tag = cipher.getAuthTag();

  return [
    TARGET_CREDENTIAL_PREFIX.slice(0, -1),
    iv.toString("base64url"),
    tag.toString("base64url"),
    ciphertext.toString("base64url"),
  ].join(":");
}

function decryptTargetCredential(value) {
  if (value === null || value === undefined || value === "") {
    return value || null;
  }
  const text = String(value);

  // Rolling-deploy/backward compatibility for rows that pre-date 5A.4b.
  if (!isEncryptedTargetCredential(text)) return text;

  const parts = text.split(":");
  if (parts.length !== 5 || parts[0] !== "stenc" || parts[1] !== "v1") {
    throw new Error("Unsupported Stream Target credential envelope");
  }

  const iv = Buffer.from(parts[2], "base64url");
  const tag = Buffer.from(parts[3], "base64url");
  const ciphertext = Buffer.from(parts[4], "base64url");
  const secrets = getTargetCredentialSecrets();
  if (!secrets.length) {
    throw new Error("Stream Target credential encryption key is unavailable");
  }

  let lastError = null;
  for (const secret of secrets) {
    try {
      const decipher = crypto.createDecipheriv(
        "aes-256-gcm",
        deriveTargetCredentialKey(secret),
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
    `Unable to decrypt Stream Target credential with configured key(s): ${lastError?.message || "authentication failed"}`,
  );
}

function decryptTargetCredentials(target) {
  if (!target) return target;
  return {
    ...target,
    destination_url: decryptTargetCredential(target.destination_url),
    stream_key: decryptTargetCredential(target.stream_key),
    active_destination_url: decryptTargetCredential(
      target.active_destination_url,
    ),
  };
}

function targetCredentialLookupHash(value) {
  if (value === null || value === undefined || value === "") return null;
  const plaintext = decryptTargetCredential(value);
  if (!plaintext) return null;
  const secrets = getTargetCredentialSecrets();
  if (!secrets.length) {
    throw new Error("Stream Target credential lookup key is unavailable");
  }
  return crypto
    .createHmac("sha256", deriveTargetCredentialKey(secrets[0]))
    .update(`stream-key-lookup:v1\0${String(plaintext)}`)
    .digest("hex");
}

async function encryptExistingTargetCredentials(pool) {
  const result = await pool.query(
    `SELECT id, destination_url, stream_key, active_destination_url,
            stream_key_lookup_hash, credential_encryption_version
     FROM social_destinations
     WHERE destination_url IS NOT NULL
        OR stream_key IS NOT NULL
        OR active_destination_url IS NOT NULL`,
  );

  for (const row of result.rows) {
    const destinationUrl = decryptTargetCredential(row.destination_url);
    const streamKey = decryptTargetCredential(row.stream_key);
    const activeDestinationUrl = decryptTargetCredential(
      row.active_destination_url,
    );

    const nextDestinationUrl = destinationUrl
      ? encryptTargetCredential(destinationUrl)
      : row.destination_url;
    const nextStreamKey = streamKey
      ? encryptTargetCredential(streamKey)
      : row.stream_key;
    const nextActiveDestinationUrl = activeDestinationUrl
      ? encryptTargetCredential(activeDestinationUrl)
      : row.active_destination_url;
    const nextLookupHash = streamKey
      ? targetCredentialLookupHash(streamKey)
      : null;

    const changed =
      nextDestinationUrl !== row.destination_url ||
      nextStreamKey !== row.stream_key ||
      nextActiveDestinationUrl !== row.active_destination_url ||
      nextLookupHash !== row.stream_key_lookup_hash ||
      Number(row.credential_encryption_version || 0) !== 1;

    if (!changed) continue;

    await pool.query(
      `UPDATE social_destinations
       SET destination_url = $1,
           stream_key = $2,
           active_destination_url = $3,
           stream_key_lookup_hash = $4,
           credential_encryption_version = 1,
           updated_at = NOW()
       WHERE id = $5`,
      [
        nextDestinationUrl,
        nextStreamKey,
        nextActiveDestinationUrl,
        nextLookupHash,
        row.id,
      ],
    );
  }
}

async function ensureStreamTargetColumns(pool) {
  // Phase 2 generic targets use the legacy `platform` column as a unique
  // internal key for custom destinations (for example custom_rtm_ab12cd).
  // The pre-Phase-2 CHECK constraint only allowed a fixed social-platform
  // list and therefore rejects valid generic targets. Remove it idempotently
  // on every deployment/startup so fresh/older databases migrate safely.
  await pool.query(`
    ALTER TABLE social_destinations
    DROP CONSTRAINT IF EXISTS social_destinations_platform_check
  `);

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
      ADD COLUMN IF NOT EXISTS target_metadata JSONB DEFAULT '{}'::jsonb,
      ADD COLUMN IF NOT EXISTS stream_key_lookup_hash VARCHAR(64),
      ADD COLUMN IF NOT EXISTS credential_encryption_version INTEGER DEFAULT 0
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

  // One-time/rolling migration: encrypt legacy plaintext Stream Target
  // credentials in-place and keep the deterministic HMAC lookup index current.
  await encryptExistingTargetCredentials(pool);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_stream_targets_channel_enabled
    ON social_destinations (channel_id, enabled)
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_stream_targets_channel_autostart
    ON social_destinations (channel_id, auto_start)
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_stream_targets_stream_key_lookup
    ON social_destinations (stream_key_lookup_hash)
  `);
}

module.exports = {
  ensureStreamTargetColumns,
  encryptTargetCredential,
  decryptTargetCredential,
  decryptTargetCredentials,
  isEncryptedTargetCredential,
  targetCredentialLookupHash,
};
