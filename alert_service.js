// alert_service.js
//
// Stream Health Alerts. Consumes platform events and notifies the
// organization by email (Mailgun), Slack (incoming webhook) and SMS (Twilio).
//
// Noise control:
//   - each event type can be switched on/off per organization;
//   - the same alert (type + channel + target) is not repeated within the
//     organization's cooldown window;
//   - "recovered" alerts are only sent when the matching problem alert was
//     sent, so people never get an "all good" for a problem they never saw;
//   - SMS is reserved for critical events (a dropped stream, a failed
//     Facebook/YouTube target), because texts cost money and wake people up.
//
// Env (all optional; a channel without config is skipped):
//   MAILGUN_ENABLED=true, MAILGUN_API_KEY, MAILGUN_DOMAIN, MAILGUN_FROM_EMAIL,
//   MAILGUN_FROM_NAME, MAILGUN_REGION            (same as server.js)
//   TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_FROM_NUMBER
//   CLIENT_URL                                   dashboard link in alerts

const {
  EVENT_CATALOG,
  registerConsumer,
  recordAlertSummary,
} = require("./platform_events");
const {
  encryptOAuthToken,
  decryptOAuthToken,
} = require("./social_oauth_schema");

// Which events alert by default. Start/end are off: they're useful as
// webhooks but too chatty as emails every Sunday.
const DEFAULT_ALERT_EVENTS = {
  "stream.started": false,
  "stream.ended": false,
  "stream.dropped": true,
  "stream.degraded": true,
  "stream.recovered": true,
  "stream.preflight.completed": true,
  "target.failed": true,
  "target.recovered": true,
};
const ALERTABLE_EVENTS = Object.keys(DEFAULT_ALERT_EVENTS);
const RECOVERY_PAIRS = {
  "stream.recovered": "stream.degraded",
  "target.recovered": "target.failed",
};

const sentAt = new Map(); // dedupe key -> ms
const openProblems = new Set(); // keys of problem alerts that were sent

let pool = null;

function dashboardUrl() {
  return String(
    process.env.CLIENT_URL || "https://dev.nolimitsmedia.com/nlm-stream/",
  ).replace(/\/?$/, "/");
}

function splitList(value) {
  return String(value || "")
    .split(/[,;\n]/)
    .map((item) => item.trim())
    .filter(Boolean);
}

function isValidEmail(value) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value);
}

function isValidPhone(value) {
  return /^\+[1-9]\d{7,14}$/.test(value);
}

function isValidSlackUrl(value) {
  try {
    const url = new URL(value);
    const allowHttp = process.env.WEBHOOKS_ALLOW_HTTP === "true";
    return (
      (url.protocol === "https:" || (allowHttp && url.protocol === "http:")) &&
      url.hostname.length > 3
    );
  } catch {
    return false;
  }
}

// ── settings ────────────────────────────────────────────────
async function getSettings(organizationId, { includeSecrets = false } = {}) {
  const result = await pool.query(
    `SELECT * FROM alert_settings WHERE organization_id = $1`,
    [organizationId],
  );
  const row = result.rows[0] || null;
  const events = { ...DEFAULT_ALERT_EVENTS, ...(row?.events || {}) };
  let slackUrl = null;
  if (row?.slack_webhook_url) {
    try {
      slackUrl = decryptOAuthToken(row.slack_webhook_url);
    } catch {
      slackUrl = null;
    }
  }
  return {
    organizationId: Number(organizationId),
    enabled: row ? row.enabled : true,
    emailEnabled: row ? row.email_enabled : true,
    emailRecipients: row?.email_recipients || "",
    slackEnabled: row ? row.slack_enabled : false,
    slackConfigured: Boolean(slackUrl),
    slackWebhookUrl: includeSecrets ? slackUrl : undefined,
    slackWebhookHint: slackUrl ? `…${slackUrl.slice(-6)}` : null,
    smsEnabled: row ? row.sms_enabled : false,
    smsNumbers: row?.sms_numbers || "",
    events,
    degradedThresholdPercent: row?.degraded_threshold_percent ?? 40,
    cooldownMinutes: row?.cooldown_minutes ?? 10,
    channelsAvailable: {
      email: process.env.MAILGUN_ENABLED === "true",
      sms: Boolean(
        process.env.TWILIO_ACCOUNT_SID &&
        process.env.TWILIO_AUTH_TOKEN &&
        process.env.TWILIO_FROM_NUMBER,
      ),
    },
  };
}

async function saveSettings(organizationId, input = {}) {
  const current = await getSettings(organizationId, { includeSecrets: true });
  const errors = [];

  const emailRecipients =
    input.emailRecipients !== undefined
      ? splitList(input.emailRecipients)
      : splitList(current.emailRecipients);
  const badEmails = emailRecipients.filter((e) => !isValidEmail(e));
  if (badEmails.length) errors.push(`Invalid email: ${badEmails.join(", ")}`);

  const smsNumbers =
    input.smsNumbers !== undefined
      ? splitList(input.smsNumbers)
      : splitList(current.smsNumbers);
  const badPhones = smsNumbers.filter((p) => !isValidPhone(p));
  if (badPhones.length)
    errors.push(
      `Phone numbers must use international format like +639171234567: ${badPhones.join(", ")}`,
    );

  let slackStored;
  if (input.slackWebhookUrl !== undefined) {
    const value = String(input.slackWebhookUrl || "").trim();
    if (value && !isValidSlackUrl(value))
      errors.push("Slack webhook URL must start with https://");
    slackStored = value ? encryptOAuthToken(value) : null;
  }
  if (errors.length) return { ok: false, errors };

  const events = { ...current.events };
  if (input.events && typeof input.events === "object") {
    for (const key of ALERTABLE_EVENTS) {
      if (input.events[key] !== undefined)
        events[key] = Boolean(input.events[key]);
    }
  }
  const clamp = (value, min, max, fallback) => {
    const n = Number(value);
    return Number.isFinite(n)
      ? Math.min(max, Math.max(min, Math.round(n)))
      : fallback;
  };

  await pool.query(
    `INSERT INTO alert_settings (
       organization_id, enabled, email_enabled, email_recipients,
       slack_enabled, slack_webhook_url, sms_enabled, sms_numbers,
       events, degraded_threshold_percent, cooldown_minutes, updated_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,NOW())
     ON CONFLICT (organization_id) DO UPDATE SET
       enabled = EXCLUDED.enabled,
       email_enabled = EXCLUDED.email_enabled,
       email_recipients = EXCLUDED.email_recipients,
       slack_enabled = EXCLUDED.slack_enabled,
       slack_webhook_url = ${
         slackStored === undefined
           ? "alert_settings.slack_webhook_url"
           : "EXCLUDED.slack_webhook_url"
       },
       sms_enabled = EXCLUDED.sms_enabled,
       sms_numbers = EXCLUDED.sms_numbers,
       events = EXCLUDED.events,
       degraded_threshold_percent = EXCLUDED.degraded_threshold_percent,
       cooldown_minutes = EXCLUDED.cooldown_minutes,
       updated_at = NOW()`,
    [
      organizationId,
      input.enabled !== undefined ? Boolean(input.enabled) : current.enabled,
      input.emailEnabled !== undefined
        ? Boolean(input.emailEnabled)
        : current.emailEnabled,
      emailRecipients.join(", ") || null,
      input.slackEnabled !== undefined
        ? Boolean(input.slackEnabled)
        : current.slackEnabled,
      slackStored === undefined ? null : slackStored,
      input.smsEnabled !== undefined
        ? Boolean(input.smsEnabled)
        : current.smsEnabled,
      smsNumbers.join(", ") || null,
      JSON.stringify(events),
      clamp(
        input.degradedThresholdPercent,
        10,
        90,
        current.degradedThresholdPercent,
      ),
      clamp(input.cooldownMinutes, 1, 240, current.cooldownMinutes),
    ],
  );
  return { ok: true, settings: await getSettings(organizationId) };
}

async function getOrganizationAdminEmails(organizationId) {
  const result = await pool.query(
    `SELECT DISTINCT a.email
     FROM organization_users ou
     JOIN admins a ON a.id = ou.admin_id
     WHERE ou.organization_id = $1
       AND ou.role IN ('owner', 'admin')
       AND a.email IS NOT NULL`,
    [organizationId],
  );
  return result.rows.map((row) => row.email).filter(Boolean);
}

// ── message text ────────────────────────────────────────────
function describe(event) {
  const d = event.data || {};
  const channel = d.channel_name ? `“${d.channel_name}”` : "your channel";
  const target = d.target_label || "a stream target";
  const when = new Date(event.created_at).toLocaleString("en-US", {
    dateStyle: "medium",
    timeStyle: "short",
    timeZone: process.env.BROADCAST_TIMEZONE || undefined,
  });
  switch (event.type) {
    case "stream.dropped":
      return {
        headline: `${channel} went offline unexpectedly`,
        body: `NLM stopped receiving video for ${channel} at ${when}${
          d.active_targets
            ? ` while ${d.active_targets} stream target(s) were live`
            : ""
        }. Check the encoder (OBS, vMix or hardware) and the venue's internet connection. Facebook and YouTube lives are held open for about 90 seconds, so reconnecting quickly continues the same broadcast.`,
      };
    case "stream.degraded":
      return {
        headline: `Weak signal on ${channel}`,
        body: `The incoming bitrate for ${channel} dropped to ${d.current_kbps} kbps (normal for this stream: about ${d.baseline_kbps} kbps). Viewers may see buffering. Check the venue's upload speed, or lower the encoder bitrate.`,
      };
    case "stream.recovered":
      return {
        headline: `Signal recovered on ${channel}`,
        body: `The incoming bitrate for ${channel} is back to ${d.current_kbps} kbps.`,
      };
    case "stream.started":
      return {
        headline: `${channel} is live`,
        body: `NLM started receiving video for ${channel} at ${when}.`,
      };
    case "stream.ended":
      return {
        headline: `${channel} ended`,
        body: `The stream for ${channel} ended at ${when}${
          d.duration_seconds
            ? ` after ${Math.round(d.duration_seconds / 60)} minutes`
            : ""
        }.`,
      };
    case "stream.preflight.completed": {
      const problems = (d.checks || []).filter((c) => c.status !== "pass");
      return {
        headline: `Encoder check for ${channel}: ${problems.length} thing(s) to fix`,
        body: `${d.summary || ""}\n\n${problems
          .map(
            (c) => `• ${c.label}: ${c.message}${c.fix ? ` Fix: ${c.fix}` : ""}`,
          )
          .join("\n")}`,
      };
    }
    case "target.failed":
      return {
        headline: `${target} failed on ${channel}`,
        body: `Delivery to ${target} for ${channel} stopped working: ${
          d.message || "no details"
        }. Open the channel's Stream Targets to see the status and restart it.`,
      };
    case "target.recovered":
      return {
        headline: `${target} is delivering again on ${channel}`,
        body: `${target} for ${channel} is receiving video again.`,
      };
    case "webhook.test":
    case "alert.test":
      return {
        headline: "Test alert from NLM Streaming",
        body: "Alerts are set up correctly. You'll receive messages like this when a stream drops, a stream target fails, or the encoder check finds a problem.",
      };
    default:
      return {
        headline: EVENT_CATALOG[event.type]?.label || event.type,
        body: JSON.stringify(d),
      };
  }
}

// ── channels ────────────────────────────────────────────────
async function sendEmail(to, subject, text) {
  if (process.env.MAILGUN_ENABLED !== "true")
    return { ok: false, skipped: "Mailgun not enabled" };
  const apiKey = process.env.MAILGUN_API_KEY;
  const domain = process.env.MAILGUN_DOMAIN;
  if (!apiKey || !domain)
    return { ok: false, skipped: "Mailgun not configured" };
  const fromEmail = process.env.MAILGUN_FROM_EMAIL || `no-reply@${domain}`;
  const fromName = process.env.MAILGUN_FROM_NAME || "NLM Streaming Cloud";
  const base =
    process.env.MAILGUN_REGION === "eu"
      ? "https://api.eu.mailgun.net"
      : "https://api.mailgun.net";
  const response = await fetch(`${base}/v3/${domain}/messages`, {
    method: "POST",
    headers: {
      Authorization: `Basic ${Buffer.from(`api:${apiKey}`).toString("base64")}`,
      "Content-Type": "application/x-www-form-urlencoded",
    },
    body: new URLSearchParams({
      from: `${fromName} <${fromEmail}>`,
      to,
      subject,
      text,
    }),
    signal: AbortSignal.timeout(10000),
  });
  return response.ok
    ? { ok: true }
    : { ok: false, error: `HTTP ${response.status}` };
}

async function sendSlack(url, event, text) {
  const icon =
    event.severity === "critical"
      ? "🔴"
      : event.severity === "warning"
        ? "🟠"
        : "🟢";
  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      text: `${icon} *${text.headline}*\n${text.body}\n<${dashboardUrl()}|Open NLM Streaming>`,
    }),
    signal: AbortSignal.timeout(10000),
  });
  return response.ok
    ? { ok: true }
    : { ok: false, error: `HTTP ${response.status}` };
}

async function sendSms(numbers, message) {
  const sid = process.env.TWILIO_ACCOUNT_SID;
  const token = process.env.TWILIO_AUTH_TOKEN;
  const from = process.env.TWILIO_FROM_NUMBER;
  if (!sid || !token || !from)
    return { ok: false, skipped: "Twilio not configured" };
  let sent = 0;
  const errors = [];
  for (const to of numbers) {
    const response = await fetch(
      `https://api.twilio.com/2010-04-01/Accounts/${sid}/Messages.json`,
      {
        method: "POST",
        headers: {
          Authorization: `Basic ${Buffer.from(`${sid}:${token}`).toString("base64")}`,
          "Content-Type": "application/x-www-form-urlencoded",
        },
        body: new URLSearchParams({
          From: from,
          To: to,
          Body: message.slice(0, 600),
        }),
        signal: AbortSignal.timeout(10000),
      },
    ).catch((error) => ({ ok: false, status: error.message }));
    if (response.ok) sent += 1;
    else errors.push(`${to}: ${response.status}`);
  }
  return sent
    ? { ok: true, sent, errors }
    : { ok: false, error: errors.join("; ") };
}

// ── dispatch ────────────────────────────────────────────────
function problemKey(event) {
  const d = event.data || {};
  const base = RECOVERY_PAIRS[event.type] || event.type;
  return `${event.organization_id}:${base}:${event.channel_id || "-"}:${d.target_id || "-"}`;
}

async function deliver(event, settings, { bypassRules = false } = {}) {
  const text = describe(event);
  const summary = { channels: {}, at: new Date().toISOString() };
  const subjectPrefix =
    event.severity === "critical"
      ? "[Action needed] "
      : event.severity === "warning"
        ? "[Warning] "
        : "";

  if (settings.emailEnabled) {
    const recipients = splitList(settings.emailRecipients);
    const to = recipients.length
      ? recipients
      : await getOrganizationAdminEmails(settings.organizationId);
    summary.channels.email = to.length
      ? await sendEmail(
          to.join(","),
          `${subjectPrefix}${text.headline}`,
          `${text.body}\n\nOpen NLM Streaming: ${dashboardUrl()}\n\nYou can change which alerts you receive in Settings → Alerts & Webhooks.`,
        ).catch((error) => ({ ok: false, error: error.message }))
      : { ok: false, skipped: "No recipients" };
  }

  if (settings.slackEnabled && settings.slackWebhookUrl) {
    summary.channels.slack = await sendSlack(
      settings.slackWebhookUrl,
      event,
      text,
    ).catch((error) => ({ ok: false, error: error.message }));
  }

  const numbers = splitList(settings.smsNumbers);
  if (
    settings.smsEnabled &&
    numbers.length &&
    (event.severity === "critical" || bypassRules)
  ) {
    summary.channels.sms = await sendSms(
      numbers,
      `NLM: ${text.headline}. ${text.body}`,
    ).catch((error) => ({ ok: false, error: error.message }));
  }
  return summary;
}

async function handleEvent(event) {
  if (!ALERTABLE_EVENTS.includes(event.type)) return;
  const settings = await getSettings(event.organization_id, {
    includeSecrets: true,
  });
  if (!settings.enabled || !settings.events[event.type]) return;

  // Pre-flight only alerts when something needs fixing.
  if (event.type === "stream.preflight.completed") {
    const problems = (event.data?.checks || []).filter(
      (c) => c.status !== "pass",
    );
    if (!problems.length) return;
  }

  const key = problemKey(event);
  if (RECOVERY_PAIRS[event.type]) {
    if (!openProblems.has(key)) return; // never alerted the problem
    openProblems.delete(key);
  } else {
    const dedupeKey = `${event.type}:${key}`;
    const last = sentAt.get(dedupeKey) || 0;
    if (Date.now() - last < settings.cooldownMinutes * 60 * 1000) return;
    sentAt.set(dedupeKey, Date.now());
    if (Object.values(RECOVERY_PAIRS).includes(event.type))
      openProblems.add(key);
  }

  const summary = await deliver(event, settings);
  await recordAlertSummary(event.id, summary);
}

async function sendTestAlert(organizationId) {
  const settings = await getSettings(organizationId, { includeSecrets: true });
  const event = {
    id: "test",
    type: "alert.test",
    severity: "info",
    organization_id: Number(organizationId),
    created_at: new Date().toISOString(),
    data: {},
  };
  return deliver(event, { ...settings, enabled: true }, { bypassRules: true });
}

function initAlertService(db) {
  pool = db;
  registerConsumer("alerts", handleEvent);
  setInterval(
    () => {
      const cutoff = Date.now() - 24 * 60 * 60 * 1000;
      for (const [key, at] of sentAt) if (at < cutoff) sentAt.delete(key);
    },
    60 * 60 * 1000,
  ).unref?.();
}

module.exports = {
  initAlertService,
  getSettings,
  saveSettings,
  sendTestAlert,
  ALERTABLE_EVENTS,
  DEFAULT_ALERT_EVENTS,
};
