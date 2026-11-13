// webhook_service.js
//
// Outbound webhooks. Customers register HTTPS endpoints and choose events;
// every matching platform event becomes a signed POST.
//
// Request:
//   POST <endpoint url>
//   Content-Type: application/json
//   NLM-Webhook-Id: evt_…            (same id on every retry: dedupe on it)
//   NLM-Webhook-Timestamp: 1794540000
//   NLM-Signature: t=1794540000,v1=<hex HMAC-SHA256(secret, "<t>.<raw body>")>
//
// Body: { id, type, api_version, created_at, organization_id, channel_id, data }
//
// Delivery: 2xx = success. Anything else (or a timeout after 10s) retries
// after 30s, 2m, 10m, 30m, 2h, 6h, 12h, then gives up (8 attempts total).
// An endpoint that fails 25 deliveries in a row is switched off so a dead
// URL doesn't pile up retries; the customer can re-enable it.
//
// Safety: only https:// URLs (http allowed only when WEBHOOKS_ALLOW_HTTP=true,
// e.g. for local testing), no redirects, and URLs that resolve to private,
// loopback or cloud-metadata addresses are refused.

const crypto = require("crypto");
const dns = require("dns").promises;
const net = require("net");
const { EVENT_CATALOG, registerConsumer } = require("./platform_events");
const {
  encryptOAuthToken,
  decryptOAuthToken,
} = require("./social_oauth_schema");

const API_VERSION = "2026-11-01";
const RETRY_DELAYS_S = [30, 120, 600, 1800, 7200, 21600, 43200];
const MAX_ATTEMPTS = RETRY_DELAYS_S.length + 1;
const DISABLE_AFTER_FAILURES = 25;
const WORKER_INTERVAL_MS = 3000;
const BATCH_SIZE = 20;
const TIMEOUT_MS = 10000;

let pool = null;
let workerTimer = null;
let workerBusy = false;

// ── URL safety ──────────────────────────────────────────────
function isPrivateAddress(address) {
  if (net.isIPv4(address)) {
    const [a, b] = address.split(".").map(Number);
    return (
      a === 0 ||
      a === 10 ||
      a === 127 ||
      (a === 100 && b >= 64 && b <= 127) ||
      (a === 169 && b === 254) ||
      (a === 172 && b >= 16 && b <= 31) ||
      (a === 192 && b === 168) ||
      a >= 224
    );
  }
  const v6 = address.toLowerCase();
  if (v6 === "::1" || v6 === "::") return true;
  if (v6.startsWith("fc") || v6.startsWith("fd") || v6.startsWith("fe80"))
    return true;
  const mapped = v6.match(/^::ffff:(\d+\.\d+\.\d+\.\d+)$/);
  return mapped ? isPrivateAddress(mapped[1]) : false;
}

async function assertSafeUrl(rawUrl) {
  let url;
  try {
    url = new URL(String(rawUrl || "").trim());
  } catch {
    throw new Error("Enter a full URL, like https://example.com/webhooks/nlm");
  }
  const allowHttp = process.env.WEBHOOKS_ALLOW_HTTP === "true";
  if (url.protocol !== "https:" && !(allowHttp && url.protocol === "http:"))
    throw new Error("Webhook URLs must use https://");
  if (url.username || url.password)
    throw new Error(
      "Don't put a username or password in the URL; use the signing secret instead.",
    );
  if (process.env.WEBHOOKS_ALLOW_PRIVATE === "true") return url;

  const host = url.hostname.replace(/^\[|\]$/g, "");
  if (
    host === "localhost" ||
    host.endsWith(".local") ||
    host.endsWith(".internal")
  )
    throw new Error("Webhook URLs must be reachable on the public internet.");
  const addresses = net.isIP(host)
    ? [{ address: host }]
    : await dns.lookup(host, { all: true }).catch(() => {
        throw new Error(`Could not resolve ${host}`);
      });
  if (addresses.some((entry) => isPrivateAddress(entry.address)))
    throw new Error("Webhook URLs must be reachable on the public internet.");
  return url;
}

// ── signing ─────────────────────────────────────────────────
function newSecret() {
  return `whsec_${crypto.randomBytes(24).toString("base64url")}`;
}

function sign(secret, timestamp, body) {
  return crypto
    .createHmac("sha256", secret)
    .update(`${timestamp}.${body}`)
    .digest("hex");
}

// For customers/docs: verify an incoming request (raw body string).
function verifySignature({ secret, header, body, toleranceSeconds = 300 }) {
  const parts = Object.fromEntries(
    String(header || "")
      .split(",")
      .map((part) => part.split("=", 2).map((s) => s.trim())),
  );
  const timestamp = Number(parts.t);
  if (!timestamp || !parts.v1) return false;
  if (Math.abs(Date.now() / 1000 - timestamp) > toleranceSeconds) return false;
  const expected = Buffer.from(sign(secret, timestamp, body), "hex");
  const actual = Buffer.from(parts.v1, "hex");
  return (
    expected.length === actual.length &&
    crypto.timingSafeEqual(expected, actual)
  );
}

// ── endpoint CRUD ───────────────────────────────────────────
function normalizeEvents(events) {
  const list = Array.isArray(events) ? events.map(String) : ["*"];
  if (list.includes("*") || list.length === 0) return ["*"];
  const valid = list.filter(
    (type) => EVENT_CATALOG[type] && type !== "webhook.test",
  );
  if (!valid.length) throw new Error("Choose at least one event.");
  return [...new Set(valid)];
}

function publicEndpoint(row) {
  return {
    id: row.id,
    url: row.url,
    description: row.description || "",
    events: row.events || ["*"],
    enabled: row.enabled,
    disabledReason: row.disabled_reason || null,
    consecutiveFailures: row.consecutive_failures,
    lastSuccessAt: row.last_success_at,
    lastFailureAt: row.last_failure_at,
    createdAt: row.created_at,
    secretHint: (() => {
      try {
        const s = decryptOAuthToken(row.secret);
        return `${s.slice(0, 10)}…${s.slice(-4)}`;
      } catch {
        return null;
      }
    })(),
  };
}

async function listEndpoints(organizationId) {
  const result = await pool.query(
    `SELECT * FROM webhook_endpoints WHERE organization_id = $1 ORDER BY id`,
    [organizationId],
  );
  return result.rows.map(publicEndpoint);
}

async function createEndpoint(organizationId, adminId, input = {}) {
  const count = await pool.query(
    `SELECT COUNT(*)::int AS n FROM webhook_endpoints WHERE organization_id = $1`,
    [organizationId],
  );
  if (count.rows[0].n >= 10)
    throw new Error("You can add up to 10 webhook endpoints.");
  const url = await assertSafeUrl(input.url);
  const events = normalizeEvents(input.events);
  const secret = newSecret();
  const result = await pool.query(
    `INSERT INTO webhook_endpoints
       (organization_id, url, description, secret, events, created_by_admin_id)
     VALUES ($1,$2,$3,$4,$5,$6) RETURNING *`,
    [
      organizationId,
      url.toString(),
      String(input.description || "").slice(0, 255) || null,
      encryptOAuthToken(secret),
      JSON.stringify(events),
      adminId || null,
    ],
  );
  // The full secret is returned only now.
  return { endpoint: publicEndpoint(result.rows[0]), secret };
}

async function updateEndpoint(organizationId, endpointId, input = {}) {
  const sets = [];
  const values = [];
  if (input.url !== undefined) {
    const url = await assertSafeUrl(input.url);
    values.push(url.toString());
    sets.push(`url = $${values.length}`);
  }
  if (input.description !== undefined) {
    values.push(String(input.description || "").slice(0, 255) || null);
    sets.push(`description = $${values.length}`);
  }
  if (input.events !== undefined) {
    values.push(JSON.stringify(normalizeEvents(input.events)));
    sets.push(`events = $${values.length}`);
  }
  if (input.enabled !== undefined) {
    values.push(Boolean(input.enabled));
    sets.push(`enabled = $${values.length}`);
    if (input.enabled)
      sets.push(`disabled_reason = NULL, consecutive_failures = 0`);
  }
  if (!sets.length) return null;
  values.push(organizationId, endpointId);
  const result = await pool.query(
    `UPDATE webhook_endpoints SET ${sets.join(", ")}, updated_at = NOW()
     WHERE organization_id = $${values.length - 1} AND id = $${values.length}
     RETURNING *`,
    values,
  );
  return result.rows[0] ? publicEndpoint(result.rows[0]) : null;
}

async function deleteEndpoint(organizationId, endpointId) {
  const result = await pool.query(
    `DELETE FROM webhook_endpoints WHERE organization_id = $1 AND id = $2`,
    [organizationId, endpointId],
  );
  return result.rowCount > 0;
}

async function rotateSecret(organizationId, endpointId) {
  const secret = newSecret();
  const result = await pool.query(
    `UPDATE webhook_endpoints SET secret = $1, updated_at = NOW()
     WHERE organization_id = $2 AND id = $3 RETURNING *`,
    [encryptOAuthToken(secret), organizationId, endpointId],
  );
  return result.rows[0]
    ? { endpoint: publicEndpoint(result.rows[0]), secret }
    : null;
}

async function listDeliveries(organizationId, endpointId, limit = 50) {
  const result = await pool.query(
    `SELECT id, event_id, event_type, status, attempts, next_attempt_at,
            response_status, response_body, error, duration_ms,
            delivered_at, created_at, payload
     FROM webhook_deliveries
     WHERE organization_id = $1 AND endpoint_id = $2
     ORDER BY id DESC LIMIT $3`,
    [
      organizationId,
      endpointId,
      Math.min(200, Math.max(1, Number(limit) || 50)),
    ],
  );
  return result.rows;
}

async function redeliver(organizationId, deliveryId) {
  const result = await pool.query(
    `UPDATE webhook_deliveries
     SET status = 'pending', next_attempt_at = NOW(), error = NULL
     WHERE organization_id = $1 AND id = $2
     RETURNING id`,
    [organizationId, deliveryId],
  );
  kickWorker();
  return result.rowCount > 0;
}

// ── queueing ────────────────────────────────────────────────
function buildPayload(event) {
  return {
    id: event.id,
    type: event.type,
    api_version: API_VERSION,
    created_at: event.created_at,
    organization_id: event.organization_id,
    channel_id: event.channel_id,
    data: event.data || {},
  };
}

async function enqueue(event, endpointIds = null) {
  const result = await pool.query(
    `SELECT id, events FROM webhook_endpoints
     WHERE organization_id = $1 AND enabled = TRUE
       ${endpointIds ? "AND id = ANY($2::int[])" : ""}`,
    endpointIds
      ? [event.organization_id, endpointIds]
      : [event.organization_id],
  );
  const payload = JSON.stringify(buildPayload(event));
  let queued = 0;
  for (const endpoint of result.rows) {
    const events = Array.isArray(endpoint.events) ? endpoint.events : ["*"];
    if (!endpointIds && !events.includes("*") && !events.includes(event.type))
      continue;
    await pool.query(
      `INSERT INTO webhook_deliveries
         (endpoint_id, organization_id, event_id, event_type, payload)
       VALUES ($1,$2,$3,$4,$5)`,
      [endpoint.id, event.organization_id, event.id, event.type, payload],
    );
    queued += 1;
  }
  if (queued) kickWorker();
  return queued;
}

async function sendTest(organizationId, endpointId) {
  const event = {
    id: `evt_test_${crypto.randomBytes(8).toString("hex")}`,
    type: "webhook.test",
    created_at: new Date().toISOString(),
    organization_id: Number(organizationId),
    channel_id: null,
    data: { message: "This is a test event from NLM Streaming." },
  };
  const queued = await enqueue(event, [Number(endpointId)]);
  return { queued, eventId: event.id };
}

// ── worker ──────────────────────────────────────────────────
async function attempt(delivery) {
  const endpointResult = await pool.query(
    `SELECT * FROM webhook_endpoints WHERE id = $1`,
    [delivery.endpoint_id],
  );
  const endpoint = endpointResult.rows[0];
  if (!endpoint || !endpoint.enabled) {
    await pool.query(
      `UPDATE webhook_deliveries SET status = 'canceled', error = 'Endpoint disabled or deleted' WHERE id = $1`,
      [delivery.id],
    );
    return;
  }

  const body = JSON.stringify(delivery.payload);
  const timestamp = Math.floor(Date.now() / 1000);
  const started = Date.now();
  let status = 0;
  let responseBody = "";
  let error = null;

  try {
    await assertSafeUrl(endpoint.url);
    const secret = decryptOAuthToken(endpoint.secret);
    const response = await fetch(endpoint.url, {
      method: "POST",
      redirect: "manual",
      headers: {
        "Content-Type": "application/json",
        "User-Agent": "NLM-Webhooks/1.0",
        "NLM-Webhook-Id": delivery.event_id,
        "NLM-Webhook-Timestamp": String(timestamp),
        "NLM-Signature": `t=${timestamp},v1=${sign(secret, timestamp, body)}`,
      },
      body,
      signal: AbortSignal.timeout(TIMEOUT_MS),
    });
    status = response.status;
    responseBody = (await response.text().catch(() => "")).slice(0, 500);
    if (status >= 300 && status < 400)
      error = `Redirects aren't followed (HTTP ${status})`;
  } catch (err) {
    error =
      err.name === "TimeoutError"
        ? `Timed out after ${TIMEOUT_MS / 1000}s`
        : err.message;
  }

  const duration = Date.now() - started;
  const ok = !error && status >= 200 && status < 300;
  const attempts = delivery.attempts + 1;

  if (ok) {
    await pool.query(
      `UPDATE webhook_deliveries
       SET status = 'succeeded', attempts = $2, response_status = $3,
           response_body = $4, error = NULL, duration_ms = $5, delivered_at = NOW()
       WHERE id = $1`,
      [delivery.id, attempts, status, responseBody, duration],
    );
    await pool.query(
      `UPDATE webhook_endpoints
       SET consecutive_failures = 0, last_success_at = NOW() WHERE id = $1`,
      [endpoint.id],
    );
    return;
  }

  const finalFailure = attempts >= MAX_ATTEMPTS;
  const delay =
    RETRY_DELAYS_S[Math.min(attempts - 1, RETRY_DELAYS_S.length - 1)];
  await pool.query(
    `UPDATE webhook_deliveries
     SET status = $2, attempts = $3, response_status = $4, response_body = $5,
         error = $6, duration_ms = $7,
         next_attempt_at = NOW() + ($8 || ' seconds')::interval
     WHERE id = $1`,
    [
      delivery.id,
      finalFailure ? "failed" : "pending",
      attempts,
      status || null,
      responseBody || null,
      error || `HTTP ${status}`,
      duration,
      String(delay),
    ],
  );
  const updated = await pool.query(
    `UPDATE webhook_endpoints
     SET consecutive_failures = consecutive_failures + 1, last_failure_at = NOW()
     WHERE id = $1 RETURNING consecutive_failures`,
    [endpoint.id],
  );
  if ((updated.rows[0]?.consecutive_failures || 0) >= DISABLE_AFTER_FAILURES) {
    await pool.query(
      `UPDATE webhook_endpoints
       SET enabled = FALSE,
           disabled_reason = 'Switched off after ${DISABLE_AFTER_FAILURES} failed deliveries in a row. Fix the endpoint, then turn it back on.'
       WHERE id = $1`,
      [endpoint.id],
    );
    console.warn(
      `[WEBHOOKS] Endpoint #${endpoint.id} disabled after repeated failures.`,
    );
  }
}

async function runWorker() {
  if (workerBusy || !pool) return;
  workerBusy = true;
  try {
    // Recover deliveries stuck in "sending" (process restart mid-request).
    await pool.query(
      `UPDATE webhook_deliveries SET status = 'pending'
       WHERE status = 'sending' AND next_attempt_at < NOW() - INTERVAL '2 minutes'`,
    );
    const claimed = await pool.query(
      `UPDATE webhook_deliveries SET status = 'sending', next_attempt_at = NOW()
       WHERE id IN (
         SELECT id FROM webhook_deliveries
         WHERE status = 'pending' AND next_attempt_at <= NOW()
         ORDER BY next_attempt_at
         LIMIT ${BATCH_SIZE}
         FOR UPDATE SKIP LOCKED)
       RETURNING *`,
    );
    await Promise.all(
      claimed.rows.map((delivery) =>
        attempt(delivery).catch((error) =>
          console.error(
            `[WEBHOOKS] Delivery #${delivery.id} crashed:`,
            error.message,
          ),
        ),
      ),
    );
  } catch (error) {
    console.error("[WEBHOOKS] Worker error:", error.message);
  } finally {
    workerBusy = false;
  }
}

function kickWorker() {
  setImmediate(() => runWorker());
}

function initWebhookService(db) {
  pool = db;
  registerConsumer("webhooks", (event) => {
    if (event.type === "webhook.test" || event.type === "alert.test") return;
    return enqueue(event);
  });
  if (!workerTimer) {
    workerTimer = setInterval(runWorker, WORKER_INTERVAL_MS);
    workerTimer.unref?.();
  }
}

module.exports = {
  initWebhookService,
  listEndpoints,
  createEndpoint,
  updateEndpoint,
  deleteEndpoint,
  rotateSecret,
  sendTest,
  listDeliveries,
  redeliver,
  verifySignature,
  assertSafeUrl,
  API_VERSION,
};
