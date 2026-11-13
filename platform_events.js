// platform_events.js
//
// Shared event core for Stream Health Alerts, Encoder Pre-flight and
// Outbound Webhooks.
//
//   stream_monitor_service  ──detects──▶  emitEvent()  ──▶  platform_events (log)
//                                              │
//                                              ├──▶ alert_service    (email / Slack / SMS)
//                                              └──▶ webhook_service  (signed HTTP POST + retries)
//
// Every event is stored once in platform_events, then fanned out. Consumers
// never throw back into the emitter: an alert or webhook failure can't break
// streaming.

const crypto = require("crypto");

// Public event catalog. `alert` = default severity used by alerting (null =
// webhook-only, never alerts). Webhook subscribers can pick any of these.
const EVENT_CATALOG = {
  "stream.started": {
    label: "Stream started",
    description: "An encoder connected and the channel went live.",
    alert: "info",
  },
  "stream.ended": {
    label: "Stream ended",
    description: "The encoder disconnected and the channel went offline.",
    alert: "info",
  },
  "stream.dropped": {
    label: "Stream dropped unexpectedly",
    description:
      "The channel went offline while a scheduled service or stream target was active.",
    alert: "critical",
  },
  "stream.degraded": {
    label: "Incoming bitrate dropped",
    description:
      "The encoder is still connected but its bitrate fell far below normal for this stream.",
    alert: "warning",
  },
  "stream.recovered": {
    label: "Incoming bitrate recovered",
    description: "The encoder bitrate is back to normal.",
    alert: "info",
  },
  "stream.preflight.completed": {
    label: "Encoder pre-flight check completed",
    description:
      "Encoder settings were checked shortly after going live. Includes any problems found.",
    alert: "warning", // only alerts when the report has problems
  },
  "target.failed": {
    label: "Stream target failed",
    description:
      "Delivery to Facebook, YouTube or another destination failed or needs attention.",
    alert: "critical",
  },
  "target.recovered": {
    label: "Stream target recovered",
    description: "A destination that had failed is delivering again.",
    alert: "info",
  },
  "target.delivering": {
    label: "Stream target delivering",
    description: "A destination confirmed it is receiving video.",
    alert: null,
  },
  "target.reconnecting": {
    label: "Stream target reconnecting",
    description: "A destination lost its connection and NLM is retrying.",
    alert: null,
  },
  "target.stopped": {
    label: "Stream target stopped",
    description: "Delivery to a destination stopped.",
    alert: null,
  },
  "recording.ready": {
    label: "Recording ready",
    description: "A recording of a live stream finished and is available.",
    alert: null,
  },
  "webhook.test": {
    label: "Test event",
    description: "Sent when you click Send test event.",
    alert: null,
  },
};

const consumers = [];
let pool = null;
let tablesReady = null;

function registerConsumer(name, handler) {
  consumers.push({ name, handler });
}

function newEventId() {
  return `evt_${crypto.randomBytes(12).toString("hex")}`;
}

async function ensurePlatformTables(db) {
  pool = db;
  if (tablesReady) return tablesReady;
  tablesReady = (async () => {
    await db.query(`
      CREATE TABLE IF NOT EXISTS platform_events (
        id BIGSERIAL PRIMARY KEY,
        event_id VARCHAR(40) NOT NULL UNIQUE,
        organization_id INTEGER NOT NULL,
        channel_id INTEGER,
        event_type VARCHAR(60) NOT NULL,
        severity VARCHAR(20) NOT NULL DEFAULT 'info',
        title TEXT,
        data JSONB NOT NULL DEFAULT '{}'::jsonb,
        alert_summary JSONB,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )`);
    await db.query(`
      CREATE INDEX IF NOT EXISTS idx_platform_events_org_created
      ON platform_events (organization_id, created_at DESC)`);

    await db.query(`
      CREATE TABLE IF NOT EXISTS alert_settings (
        organization_id INTEGER PRIMARY KEY,
        enabled BOOLEAN NOT NULL DEFAULT TRUE,
        email_enabled BOOLEAN NOT NULL DEFAULT TRUE,
        email_recipients TEXT,
        slack_enabled BOOLEAN NOT NULL DEFAULT FALSE,
        slack_webhook_url TEXT,
        sms_enabled BOOLEAN NOT NULL DEFAULT FALSE,
        sms_numbers TEXT,
        events JSONB NOT NULL DEFAULT '{}'::jsonb,
        degraded_threshold_percent INTEGER NOT NULL DEFAULT 40,
        cooldown_minutes INTEGER NOT NULL DEFAULT 10,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )`);

    await db.query(`
      CREATE TABLE IF NOT EXISTS webhook_endpoints (
        id SERIAL PRIMARY KEY,
        organization_id INTEGER NOT NULL,
        url TEXT NOT NULL,
        description VARCHAR(255),
        secret TEXT NOT NULL,
        events JSONB NOT NULL DEFAULT '["*"]'::jsonb,
        enabled BOOLEAN NOT NULL DEFAULT TRUE,
        consecutive_failures INTEGER NOT NULL DEFAULT 0,
        disabled_reason TEXT,
        last_success_at TIMESTAMPTZ,
        last_failure_at TIMESTAMPTZ,
        created_by_admin_id INTEGER,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )`);
    await db.query(`
      CREATE INDEX IF NOT EXISTS idx_webhook_endpoints_org
      ON webhook_endpoints (organization_id)`);

    await db.query(`
      CREATE TABLE IF NOT EXISTS webhook_deliveries (
        id BIGSERIAL PRIMARY KEY,
        endpoint_id INTEGER NOT NULL REFERENCES webhook_endpoints(id) ON DELETE CASCADE,
        organization_id INTEGER NOT NULL,
        event_id VARCHAR(40) NOT NULL,
        event_type VARCHAR(60) NOT NULL,
        payload JSONB NOT NULL,
        status VARCHAR(20) NOT NULL DEFAULT 'pending',
        attempts INTEGER NOT NULL DEFAULT 0,
        next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        response_status INTEGER,
        response_body TEXT,
        error TEXT,
        duration_ms INTEGER,
        delivered_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )`);
    await db.query(`
      CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_due
      ON webhook_deliveries (status, next_attempt_at)`);
    await db.query(`
      CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_endpoint
      ON webhook_deliveries (endpoint_id, created_at DESC)`);

    await db.query(`
      CREATE TABLE IF NOT EXISTS channel_preflight_reports (
        id BIGSERIAL PRIMARY KEY,
        organization_id INTEGER NOT NULL,
        channel_id INTEGER NOT NULL,
        live_session VARCHAR(64),
        grade VARCHAR(20) NOT NULL,
        summary TEXT,
        checks JSONB NOT NULL DEFAULT '[]'::jsonb,
        measured JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )`);
    await db.query(`
      CREATE INDEX IF NOT EXISTS idx_preflight_channel_created
      ON channel_preflight_reports (channel_id, created_at DESC)`);

    // Cleanup of old rows keeps these tables small.
    await db
      .query(
        `DELETE FROM platform_events WHERE created_at < NOW() - INTERVAL '90 days'`,
      )
      .catch(() => {});
    await db
      .query(
        `DELETE FROM webhook_deliveries WHERE created_at < NOW() - INTERVAL '30 days'`,
      )
      .catch(() => {});
  })().catch((error) => {
    tablesReady = null;
    throw error;
  });
  return tablesReady;
}

// Emit one event. Returns the stored event (or null if storing failed).
//   emitEvent({ organizationId, channelId, type, severity, title, data })
async function emitEvent({
  organizationId,
  channelId = null,
  type,
  severity,
  title,
  data = {},
}) {
  if (!pool || !organizationId || !type) return null;
  const catalog = EVENT_CATALOG[type] || {};
  const event = {
    id: newEventId(),
    type,
    created_at: new Date().toISOString(),
    organization_id: Number(organizationId),
    channel_id: channelId ? Number(channelId) : null,
    severity: severity || catalog.alert || "info",
    title: title || catalog.label || type,
    data,
  };

  try {
    await pool.query(
      `INSERT INTO platform_events
         (event_id, organization_id, channel_id, event_type, severity, title, data)
       VALUES ($1,$2,$3,$4,$5,$6,$7)`,
      [
        event.id,
        event.organization_id,
        event.channel_id,
        event.type,
        event.severity,
        event.title,
        JSON.stringify(event.data),
      ],
    );
  } catch (error) {
    console.error("[EVENTS] Failed to store event:", error.message);
    return null;
  }

  console.log(
    `[EVENTS] ${event.type} org=${event.organization_id} channel=${event.channel_id ?? "-"} ${event.title}`,
  );

  for (const consumer of consumers) {
    Promise.resolve()
      .then(() => consumer.handler(event))
      .catch((error) =>
        console.error(
          `[EVENTS] consumer ${consumer.name} failed:`,
          error.message,
        ),
      );
  }
  return event;
}

async function recordAlertSummary(eventId, summary) {
  if (!pool) return;
  await pool
    .query(
      `UPDATE platform_events SET alert_summary = $1 WHERE event_id = $2`,
      [JSON.stringify(summary), eventId],
    )
    .catch(() => {});
}

module.exports = {
  EVENT_CATALOG,
  ensurePlatformTables,
  emitEvent,
  registerConsumer,
  recordAlertSummary,
};
