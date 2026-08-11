// server/server.js
const express = require("express");
const rateLimit = require("express-rate-limit");
const cors = require("cors");
const http = require("http");
const { Server } = require("socket.io");
const { exec, spawn, execSync } = require("child_process");
const os = require("os");
const crypto = require("crypto");
require("dotenv").config({ override: true });

const fs = require("fs");
const path = require("path");

const bcrypt = require("bcryptjs");
const jwt = require("jsonwebtoken");
const whmcs = require("./whmcs_client");
const bunny = require("./bunny_client");
const embedRoutes = require("./embed_routes"); // Phase 1 — embedded player (Copy Embed Code)
const {
  getCachedStreamAnalysis,
  scheduleLiveStreamAnalysis,
} = require("./stream_health_service");

let UAParser = null;
try {
  const uaParserModule = require("ua-parser-js");
  UAParser = uaParserModule.UAParser || uaParserModule;
} catch {
  UAParser = null;
}

const { AsyncLocalStorage } = require("async_hooks");
const pool = require("./db");

const dbRetrySleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const isRetryableDatabaseError = (error) => {
  const code = String(error?.code || "").toUpperCase();
  const message = String(error?.message || "");

  return (
    [
      "ETIMEDOUT",
      "ECONNRESET",
      "ECONNREFUSED",
      "EPIPE",
      "57P01",
      "57P02",
      "57P03",
    ].includes(code) ||
    /timeout|connection terminated|connection reset|terminated unexpectedly|server closed the connection/i.test(
      message,
    )
  );
};

// Use for dashboard/background reads where one short Railway proxy hiccup
// should not turn into a user-visible 500. Writes keep their existing direct
// pool.query behavior so they are never accidentally repeated.
const queryWithRetry = async (text, params = [], options = {}) => {
  const retries = Number.isInteger(options.retries) ? options.retries : 1;
  const retryDelayMs = Number(options.retryDelayMs || 750);
  let lastError;

  for (let attempt = 0; attempt <= retries; attempt += 1) {
    try {
      return await pool.query(text, params);
    } catch (error) {
      lastError = error;
      if (!isRetryableDatabaseError(error) || attempt >= retries) throw error;

      console.warn(
        `[DATABASE] Temporary query connection failure; retrying (${attempt + 1}/${retries}) in ${retryDelayMs}ms: ${error.message}`,
      );
      await dbRetrySleep(retryDelayMs);
    }
  }

  throw lastError;
};
const { ensureSocialOAuthTables } = require("./social_oauth_schema");
const facebookGraph = require("./facebook_graph_service");
const youtubeApi = require("./youtube_api_service");
const { clear } = require("console");

// Tracks which organization (if any) the current request is scoped to,
// so errors logged deep inside async route handlers can still be tagged
// with the right organization_id for support-mode filtering.
const requestContext = new AsyncLocalStorage();

const app = express();

// The API is served behind nginx/Bunny. Trust the first proxy hop so
// express-rate-limit and request IP handling use the forwarded client IP
// without raising ERR_ERL_UNEXPECTED_X_FORWARDED_FOR warnings.
app.set("trust proxy", 1);

const server = http.createServer(app);

const PORT = process.env.PORT || 5000;
const CLIENT_URL = process.env.CLIENT_URL || "http://localhost:5174";
const CORS_ORIGINS = (process.env.CORS_ORIGINS || CLIENT_URL)
  .split(",")
  .map((origin) => origin.trim())
  .filter(Boolean);

const SRS_API_URL = process.env.SRS_API_URL || "http://localhost:1985";
const HLS_BASE_URL = process.env.HLS_BASE_URL || "http://localhost:8080";
// Internal SRS endpoints. RTMP remains the publish/output transport for
// encoded renditions, while FFmpeg consumers read the raw broadcast through
// SRS's local HLS endpoint. Live production testing on 2026-08-07 confirmed
// that local/public RTMP playback can complete the TCP/RTMP connection yet
// deliver no media frames, while the raw SRS HLS feed immediately exposes
// healthy H.264/AAC media to ffprobe/FFmpeg.
const SRS_INTERNAL_HLS_BASE_URL =
  process.env.SRS_INTERNAL_HLS_BASE_URL || "http://127.0.0.1:8080";
const API_PUBLIC_URL = process.env.API_PUBLIC_URL || `http://localhost:${PORT}`;
const RECORDINGS_ROOT = process.env.RECORDINGS_ROOT || "C:/nlm-srs/recordings";
const RECORDINGS_LIVE_ROOT = path.join(RECORDINGS_ROOT, "live");
// Where DVR writes the bitrate-capped transcode's recordings (see
// autoCapBitrateStream) — this is now the OFFICIAL source for archived
// recordings, since it reflects what viewers actually watched (the
// enforced/capped bitrate), not whatever the raw encoder pushed.
const RECORDINGS_LIVE_CAPPED_ROOT = path.join(RECORDINGS_ROOT, "live_capped");
const BUNNY_STORAGE_ZONE = process.env.BUNNY_STORAGE_ZONE || "";
const BUNNY_STORAGE_HOSTNAME = process.env.BUNNY_STORAGE_HOSTNAME || "";
const BUNNY_STORAGE_API_KEY = process.env.BUNNY_STORAGE_API_KEY || "";
const BUNNY_RECORDINGS_CDN_URL = process.env.BUNNY_RECORDINGS_CDN_URL || "";

// Dedicated Bunny pull zone for HLS/ABR delivery ONLY (separate from
// whatever fronts the general /api/* surface) — Bunny's Token
// Authentication is zone-wide (blocks every unsigned request through a
// zone), so it can't be turned on for a zone that also carries unrelated,
// unsigned API traffic like login/admin. See bunny-signed-urls.md.
const HLS_CDN_HOSTNAME = process.env.HLS_CDN_HOSTNAME || "";
const BUNNY_HLS_TOKEN_KEY = process.env.BUNNY_HLS_TOKEN_KEY || "";
const HLS_TOKEN_TTL_SECONDS = 6 * 60 * 60; // not a hard security boundary —
// bounds how long a dead broadcast's old links keep working; real access
// control is still whatever gates who receives a playbackUrl at all.

// Billing now runs through WHMCS (see whmcs_client.js) — WHMCS_* env vars
// are read directly by that module. Stripe has been fully retired: no
// Stripe keys, webhook, or SDK client remain in this file.
const WHMCS_POLL_INTERVAL_MS = Number(
  process.env.WHMCS_POLL_INTERVAL_MS || 2 * 60 * 1000,
);

// SUPER ADMIN — recent error log (in-memory ring buffer)
// Captures console.error calls so the super-admin dashboard can show
// recent server-side errors without needing external log tooling.
// Resets on server restart — this is a lightweight recent-activity
// view, not a durable audit log.

const RECENT_ERROR_LOG_LIMIT = 100;
const recentErrorLog = [];
const originalConsoleError = console.error.bind(console);
console.error = (...args) => {
  try {
    const message = args
      .map((a) =>
        a instanceof Error
          ? a.message
          : typeof a === "string"
            ? a
            : JSON.stringify(a),
      )
      .join(" ")
      .slice(0, 2000);

    const store = requestContext.getStore();

    recentErrorLog.push({
      message,
      at: new Date().toISOString(),
      organization_id: store?.organization_id ?? null,
      organization_name: store?.organization_name ?? null,
    });
    if (recentErrorLog.length > RECENT_ERROR_LOG_LIMIT) {
      recentErrorLog.shift();
    }
  } catch {
    // never let logging itself crash the app
  }
  originalConsoleError(...args);
};

// ══════════════════════════════════════════
// SLACK ALERTS — for terminal (give-up) failures only, not retryable warns.
// No-op if SLACK_ALERT_WEBHOOK_URL isn't set. Never throws — a failed
// Slack post must never break the actual error-handling path that called it.
// ══════════════════════════════════════════
async function notifySlack(message, context = {}) {
  const webhookUrl = process.env.SLACK_ALERT_WEBHOOK_URL;
  if (!webhookUrl) return;

  try {
    await fetch(webhookUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        text: `:warning: *${message}*\n\`\`\`${JSON.stringify(context, null, 2)}\`\`\``,
      }),
    });
  } catch (err) {
    console.warn("[SLACK] Alert failed to send:", err.message);
  }
}

// ══════════════════════════════════════════
// MAILGUN — outbound email to org admins (plan/quota notices, etc).
// Separate from the existing external Mailgun-based disk/log-error cron
// monitors (which alert websupport@ internally) — this sends TO the
// client organization's own admins, from inside the app. Same
// never-throw, no-op-if-unconfigured philosophy as notifySlack above: a
// failed or unconfigured email must never break the request/job that
// triggered it. Uses the same MAILGUN_ENABLED/MAILGUN_DOMAIN env vars
// already established for this project — confirm MAILGUN_API_KEY and
// MAILGUN_FROM_EMAIL are also set before relying on this in production;
// MAILGUN_REGION defaults to "us" (api.mailgun.net) — set to "eu" for
// api.eu.mailgun.net if this account is EU-region.
async function sendMailgunEmail({ to, subject, text }) {
  if (process.env.MAILGUN_ENABLED !== "true") return false;

  const apiKey = process.env.MAILGUN_API_KEY;
  const domain = process.env.MAILGUN_DOMAIN;
  const fromEmail =
    process.env.MAILGUN_FROM_EMAIL || `no-reply@${domain || ""}`;
  const fromName = process.env.MAILGUN_FROM_NAME || "NLM Streaming Cloud";
  const region = process.env.MAILGUN_REGION === "eu" ? "eu" : "us";
  const baseUrl =
    region === "eu" ? "https://api.eu.mailgun.net" : "https://api.mailgun.net";

  if (!apiKey || !domain || !to) return false;

  try {
    const body = new URLSearchParams({
      from: `${fromName} <${fromEmail}>`,
      to,
      subject,
      text,
    });

    const response = await fetch(`${baseUrl}/v3/${domain}/messages`, {
      method: "POST",
      headers: {
        Authorization: `Basic ${Buffer.from(`api:${apiKey}`).toString("base64")}`,
        "Content-Type": "application/x-www-form-urlencoded",
      },
      body,
      signal: AbortSignal.timeout(10000),
    });

    if (!response.ok) {
      console.warn(`[MAILGUN] Send failed (HTTP ${response.status}) to ${to}`);
      return false;
    }

    return true;
  } catch (err) {
    console.warn("[MAILGUN] Send failed:", err.message);
    return false;
  }
}

// Fetches the email address(es) of an organization's owner/admin
// users — the audience for org-facing notices like storage/bandwidth
// quota alerts. Returns a comma-separated string ready for Mailgun's
// `to` field (Mailgun accepts multiple recipients this way), or null if
// the org somehow has no admin users yet.
async function getOrganizationAdminEmails(organizationId) {
  const result = await pool.query(
    `
    SELECT DISTINCT a.email
    FROM organization_users ou
    JOIN admins a ON a.id = ou.admin_id
    WHERE ou.organization_id = $1
      AND ou.role IN ('owner', 'admin')
      AND a.email IS NOT NULL
    `,
    [organizationId],
  );

  const emails = result.rows.map((r) => r.email).filter(Boolean);
  return emails.length ? emails.join(",") : null;
}

const corsOptions = {
  origin(origin, callback) {
    if (!origin || CORS_ORIGINS.includes(origin)) {
      return callback(null, true);
    }

    // Passing an Error here makes Express treat a simple CORS mismatch as
    // an uncaught server error -- producing a raw, unstyled "Internal
    // Server Error" page instead of just omitting CORS headers. Reject
    // gracefully instead; the browser's own same-origin policy already
    // blocks the response from being read by JS, no need to crash.
    console.error(`CORS blocked origin: ${origin}`);
    return callback(null, false);
  },
  credentials: true,
};

app.use(cors(corsOptions));

// Any CDN or proxy sitting in front of this API (e.g. a Bunny CDN pull
// zone used as an SSL front door) must never cache these responses —
// most of them are per-user/per-organization and caching one response
// could leak it to a different logged-in user. This header tells any
// CDN in the chain not to store or reuse responses.
app.use((req, res, next) => {
  if (req.path.startsWith("/api/")) {
    res.setHeader("Cache-Control", "no-store, private");
  }
  next();
});

// ══════════════════════════════════════════
// SECURITY HEADERS
// Deliberately does NOT set X-Frame-Options or a frame-ancestors CSP
// directive anywhere in this app — the embed player (see embed_routes.js
// and /embed/:token) is specifically designed to be iframed on arbitrary
// third-party client websites; a blanket frame-blocking header here would
// break that feature entirely, not just harden something. If a
// frame-blocking policy is ever wanted for the dashboard/admin surface
// specifically, that belongs on the frontend's own static hosting config
// (DirectAdmin/nginx), not here — this backend can't tell "a dashboard
// request" from "an embed request" at this generic response-header layer.
// HSTS deliberately omits includeSubDomains — not every subdomain under
// nolimitsmedia.com has been confirmed HTTPS-ready; safe to add later
// once that's verified for all of them at once.
app.use((req, res, next) => {
  res.setHeader("X-Content-Type-Options", "nosniff");
  res.setHeader("Referrer-Policy", "strict-origin-when-cross-origin");
  res.setHeader("Strict-Transport-Security", "max-age=31536000");
  res.setHeader(
    "Permissions-Policy",
    "camera=(), microphone=(), geolocation=()",
  );
  next();
});

// ══════════════════════════════════════════
// RATE LIMITING — protects the endpoints most exposed to abuse.
// Keyed by IP by default (express-rate-limit's default), which is
// fine here since these all sit behind Bunny/nginx and the real
// client IP is what matters for throttling abuse.
// ══════════════════════════════════════════
const loginLimiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: 10,
  message: {
    ok: false,
    message: "Too many login attempts. Please try again in a few minutes.",
  },
  standardHeaders: true,
  legacyHeaders: false,
});

const signupLimiter = rateLimit({
  windowMs: 60 * 60 * 1000, // 1 hour
  max: 5,
  message: {
    ok: false,
    message: "Too many signup attempts. Please try again later.",
  },
  standardHeaders: true,
  legacyHeaders: false,
});

const publicEngagementLimiter = rateLimit({
  windowMs: 60 * 1000, // 1 minute
  max: 30,
  message: {
    ok: false,
    message: "Too many requests. Please slow down.",
  },
  standardHeaders: true,
  legacyHeaders: false,
});
// Status-polling endpoints — legitimately hit every few seconds by every
// active player (see POLL_WHEN_LIVE_MS/heartbeat intervals elsewhere in
// this file). Deliberately much more generous than publicEngagementLimiter
// (a per-user-action limiter) so several real viewers behind the same
// office/NAT IP don't get falsely throttled just for watching.
const statusPollLimiter = rateLimit({
  windowMs: 60 * 1000, // 1 minute
  max: 120,
  message: {
    ok: false,
    message: "Too many requests. Please slow down.",
  },
  standardHeaders: true,
  legacyHeaders: false,
});

app.use(express.json());

const io = new Server(server, {
  cors: {
    origin: CORS_ORIGINS,
    methods: ["GET", "POST"],
    credentials: true,
  },
});

app.get("/", (req, res) => {
  res.json({ message: "NLM Streaming Manager API is running" });
});

app.get("/api/health", (req, res) => {
  res.json({
    ok: true,
    service: "nlm-streaming-manager",
    time: new Date().toISOString(),
  });
});

/*
|--------------------------------------------------------------------------
| AUTH
|--------------------------------------------------------------------------
*/

const generateToken = (admin) => {
  return jwt.sign(
    {
      id: admin.id,
      email: admin.email,
      role: admin.role,
    },
    process.env.JWT_SECRET,
    { expiresIn: "24h" },
  );
};

const authenticateAdmin = (req, res, next) => {
  try {
    const authHeader = req.headers.authorization;

    if (!authHeader) {
      return res.status(401).json({
        ok: false,
        message: "No token provided",
      });
    }

    const token = authHeader.split(" ")[1];
    const decoded = jwt.verify(token, process.env.JWT_SECRET, {
      algorithms: ["HS256"],
    });

    req.admin = decoded;
    next();
  } catch (error) {
    return res.status(401).json({
      ok: false,
      message: "Invalid or expired token",
    });
  }
};

const requireRole = (...allowedRoles) => {
  return (req, res, next) => {
    if (!req.admin || !allowedRoles.includes(req.admin.role)) {
      return res.status(403).json({
        ok: false,
        message: "You do not have permission to perform this action",
      });
    }

    next();
  };
};

app.post(
  "/api/auth/register",
  authenticateAdmin,
  requireRole("super_admin"),
  async (req, res) => {
    try {
      const { name, email, password, role } = req.body;

      if (!name || !email || !password) {
        return res.status(400).json({
          ok: false,
          message: "Name, email, and password are required",
        });
      }

      const passwordHash = await bcrypt.hash(password, 10);

      const result = await pool.query(
        `
        INSERT INTO admins (name, email, password_hash, role)
        VALUES ($1, $2, $3, $4)
        RETURNING id, name, email, role, created_at
        `,
        [name, email, passwordHash, role || "admin"],
      );

      res.json({
        ok: true,
        admin: result.rows[0],
      });
    } catch (error) {
      console.error("Register error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to register admin",
        error: error.message,
      });
    }
  },
);

app.post("/api/auth/login", loginLimiter, async (req, res) => {
  try {
    const { email, password } = req.body;

    const result = await pool.query(
      `
      SELECT *
      FROM admins
      WHERE email = $1
      `,
      [email],
    );

    const admin = result.rows[0];

    if (!admin) {
      return res.status(401).json({
        ok: false,
        message: "Invalid email or password",
      });
    }

    const isMatch = await bcrypt.compare(password, admin.password_hash);

    if (!isMatch) {
      return res.status(401).json({
        ok: false,
        message: "Invalid email or password",
      });
    }

    const token = generateToken(admin);

    res.json({
      ok: true,
      token,
      admin: {
        id: admin.id,
        name: admin.name,
        email: admin.email,
        role: admin.role,
      },
    });
  } catch (error) {
    console.error("Login error:", error);

    res.status(500).json({
      ok: false,
      message: "Failed to login",
      error: error.message,
    });
  }
});

app.get("/api/auth/me", authenticateAdmin, async (req, res) => {
  try {
    const result = await pool.query(
      `
      SELECT id, name, email, role, created_at
      FROM admins
      WHERE id = $1
      `,
      [req.admin.id],
    );

    if (!result.rows[0]) {
      return res.status(404).json({
        ok: false,
        message: "Admin not found",
      });
    }

    const organizations = await getAdminOrganizations(req.admin.id);

    res.json({
      ok: true,
      admin: result.rows[0],
      organizations,
      currentOrganization: organizations[0] || null,
    });
  } catch (error) {
    console.error("Auth me error:", error);

    res.status(500).json({
      ok: false,
      message: "Failed to fetch admin profile",
    });
  }
});

/*
|--------------------------------------------------------------------------
| ORGANIZATIONS / MULTI-TENANT FOUNDATION
|--------------------------------------------------------------------------
*/

const cleanOrgText = (value, maxLength = 255) => {
  return String(value || "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, maxLength);
};

const slugifyOrganization = (value) => {
  const base = String(value || "organization")
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");

  return base || "organization";
};

// Generates a cryptographically random, guaranteed-unique stream key.
//
// PHASE 2 (secure stream keys): previously this derived the key from the
// org/channel name (e.g. "one-church-d690c4"), which is predictable —
// anyone who knows (or guesses) an org's name is most of the way to
// guessing its ingest credential. Keys are now a random alphanumeric
// string (letters + numbers only — no dashes/underscores, matching the
// "sk_<random letters and numbers>" format from the roadmap spec) built
// from crypto.randomBytes()/crypto.randomInt() and never derived from any
// human-readable input.
//
// Already auto-generated at the moment an account/channel is created — all
// three existing call sites (public signup, WHMCS/client onboarding, and
// manual "Create Channel") call this same function immediately when the
// row is inserted, so no separate "generate after signup" step is needed;
// this just changes what that automatic generation produces. The
// `baseText` parameter is kept (but ignored) so those call sites don't
// need to change.
const STREAM_KEY_ALPHABET =
  "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

const randomAlphanumeric = (length) => {
  let out = "";
  for (let i = 0; i < length; i++) {
    out += STREAM_KEY_ALPHABET[crypto.randomInt(STREAM_KEY_ALPHABET.length)];
  }
  return out;
};

const generateUniqueStreamKey = async (_baseText) => {
  for (let attempt = 0; attempt < 10; attempt++) {
    const candidate = `sk_${randomAlphanumeric(32)}`;

    const existing = await pool.query(
      `SELECT id FROM channels WHERE stream_key = $1 LIMIT 1`,
      [candidate],
    );

    if (!existing.rows[0]) {
      return candidate;
    }
  }

  // Astronomically unlikely to be reached, but fall back to a longer
  // random key rather than ever returning a colliding one.
  return `sk_${randomAlphanumeric(48)}`;
};

const slugifyRecording = (value) => {
  const base = String(value || "recording")
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");

  return base || "recording";
};

const ensureUniqueRecordingSlug = async (baseValue, existingId = null) => {
  const baseSlug = slugifyRecording(baseValue);
  let slug = baseSlug;
  let counter = 2;

  while (true) {
    const result = existingId
      ? await pool.query(
          `SELECT id FROM recordings WHERE public_slug = $1 AND id <> $2 LIMIT 1`,
          [slug, existingId],
        )
      : await pool.query(
          `SELECT id FROM recordings WHERE public_slug = $1 LIMIT 1`,
          [slug],
        );

    if (!result.rows[0]) return slug;

    slug = `${baseSlug}-${counter}`;
    counter += 1;
  }
};

const ensureUniqueOrganizationSlug = async (name, existingId = null) => {
  const baseSlug = slugifyOrganization(name);
  let slug = baseSlug;
  let counter = 2;

  while (true) {
    const params = existingId ? [slug, existingId] : [slug];
    const query = existingId
      ? "SELECT id FROM organizations WHERE slug = $1 AND id <> $2 LIMIT 1"
      : "SELECT id FROM organizations WHERE slug = $1 LIMIT 1";

    const result = await pool.query(query, params);

    if (!result.rows[0]) return slug;

    slug = `${baseSlug}-${counter}`;
    counter += 1;
  }
};

// Provisions a dedicated Bunny pull zone + storage zone for a brand-new
// organization, and saves the results onto its organizations row. Called
// AFTER an organization already exists and its creation transaction has
// committed — deliberately not inside that transaction, since an external
// API call has no place holding a DB transaction open.
//
// Deliberately non-fatal: if Bunny provisioning fails (account API not
// configured, Bunny outage, etc.), this logs and returns without
// throwing — a brand-new customer's signup should never be blocked by a
// CDN-provisioning hiccup. The organization simply falls back to the
// shared platform zone (same as all pre-existing/grandfathered orgs)
// until provisioning is retried.
const provisionBunnyZonesForNewOrganization = async (organization) => {
  if (!bunny.isBunnyAccountConfigured()) {
    console.log(
      `[BUNNY-PROVISION] Skipping org ${organization.id} (${organization.name}) — BUNNY_ACCOUNT_API_KEY not configured, will use the shared platform zone.`,
    );
    return;
  }

  try {
    const zones = await bunny.provisionBunnyZonesForOrganization(
      organization.slug,
    );

    await pool.query(
      `
      UPDATE organizations
      SET bunny_pull_zone_id = $1,
          bunny_pull_zone_hostname = $2,
          bunny_storage_zone_id = $3,
          bunny_storage_zone_name = $4,
          bunny_storage_zone_hostname = $5,
          bunny_storage_zone_password = $6,
          bunny_recordings_pull_zone_id = $7,
          bunny_recordings_cdn_url = $8,
          bunny_provisioned_at = NOW(),
          updated_at = NOW()
      WHERE id = $9
      `,
      [
        zones.pullZoneId,
        zones.pullZoneHostname,
        zones.storageZoneId,
        zones.storageZoneName,
        zones.storageZoneHostname,
        zones.storageZonePassword,
        zones.recordingsPullZoneId,
        zones.recordingsCdnUrl,
        organization.id,
      ],
    );

    console.log(
      `[BUNNY-PROVISION] Created dedicated zones for org ${organization.id} (${organization.name}): pull=${zones.pullZoneHostname}, storage=${zones.storageZoneName}, recordingsCdn=${zones.recordingsCdnUrl}`,
    );
  } catch (err) {
    console.error(
      `[BUNNY-PROVISION] Failed to provision Bunny zones for org ${organization.id} (${organization.name}):`,
      err.message,
    );
  }
};

const ensureOrganizationTables = async () => {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS organizations (
      id SERIAL PRIMARY KEY,
      name VARCHAR(255) NOT NULL,
      slug VARCHAR(255) UNIQUE NOT NULL,
      logo_url TEXT,
      primary_color VARCHAR(40) DEFAULT '#0d6efd',
      custom_domain VARCHAR(255),
      subscription_plan VARCHAR(80) DEFAULT 'starter',
      is_active BOOLEAN DEFAULT TRUE,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS organization_users (
      id SERIAL PRIMARY KEY,
      organization_id INTEGER NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
      admin_id INTEGER NOT NULL REFERENCES admins(id) ON DELETE CASCADE,
      role VARCHAR(40) DEFAULT 'owner',
      created_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE (organization_id, admin_id)
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS organization_settings (
      id SERIAL PRIMARY KEY,
      organization_id INTEGER NOT NULL UNIQUE REFERENCES organizations(id) ON DELETE CASCADE,
      watch_page_title VARCHAR(255),
      logo_url TEXT,
      primary_color VARCHAR(40) DEFAULT '#0d6efd',
      secondary_color VARCHAR(40) DEFAULT '#fd9d00',
      donation_url TEXT,
      custom_css TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  await pool.query(`
    ALTER TABLE organizations
    ADD COLUMN IF NOT EXISTS bunny_pull_zone_id VARCHAR(40),
    ADD COLUMN IF NOT EXISTS bunny_pull_zone_hostname VARCHAR(255),
    ADD COLUMN IF NOT EXISTS bunny_storage_zone_id VARCHAR(40),
    ADD COLUMN IF NOT EXISTS bunny_storage_zone_name VARCHAR(255),
    ADD COLUMN IF NOT EXISTS bunny_storage_zone_hostname VARCHAR(255),
    ADD COLUMN IF NOT EXISTS bunny_storage_zone_password TEXT,
    ADD COLUMN IF NOT EXISTS bunny_recordings_pull_zone_id VARCHAR(40),
    ADD COLUMN IF NOT EXISTS bunny_recordings_cdn_url VARCHAR(255),
    ADD COLUMN IF NOT EXISTS bunny_provisioned_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS transcoding_override BOOLEAN
  `);
  // transcoding_override: NULL (default) means "use the plan's default";
  // TRUE/FALSE lets a specific org be opted in or out of ABR/transcoding
  // independent of its plan — for staging a rollout to one or two willing
  // orgs before widening it to every Deluxe/Premium org at once, without
  // needing to change anything about their actual plan/billing.

  const defaultOrgResult = await pool.query(`
    INSERT INTO organizations (name, slug, primary_color, subscription_plan)
    VALUES ('No Limits Media', 'no-limits-media', '#0d6efd', 'internal')
    ON CONFLICT (slug) DO UPDATE
      SET updated_at = NOW()
    RETURNING *
  `);

  const defaultOrg = defaultOrgResult.rows[0];

  await pool.query(
    `
    INSERT INTO organization_settings (
      organization_id,
      watch_page_title,
      primary_color,
      secondary_color
    )
    VALUES ($1, 'NLM Streaming', '#0d6efd', '#fd9d00')
    ON CONFLICT (organization_id) DO NOTHING
    `,
    [defaultOrg.id],
  );

  await pool.query(`
    ALTER TABLE channels
    ADD COLUMN IF NOT EXISTS organization_id INTEGER REFERENCES organizations(id)
  `);

  await pool.query(`
    ALTER TABLE recordings
    ADD COLUMN IF NOT EXISTS organization_id INTEGER REFERENCES organizations(id)
  `);

  await pool.query(`
    ALTER TABLE scheduled_streams
    ADD COLUMN IF NOT EXISTS organization_id INTEGER REFERENCES organizations(id)
  `);

  await pool.query(
    `UPDATE channels SET organization_id = $1 WHERE organization_id IS NULL`,
    [defaultOrg.id],
  );
  await pool.query(
    `UPDATE recordings SET organization_id = $1 WHERE organization_id IS NULL`,
    [defaultOrg.id],
  );
  await pool.query(
    `UPDATE scheduled_streams SET organization_id = $1 WHERE organization_id IS NULL`,
    [defaultOrg.id],
  );

  return defaultOrg;
};

const getDefaultOrganization = async () => {
  const result = await pool.query(
    `
    SELECT *
    FROM organizations
    WHERE slug = 'no-limits-media'
    LIMIT 1
    `,
  );

  return result.rows[0];
};

const getAdminOrganizations = async (adminId) => {
  const result = await pool.query(
    `
    SELECT
      o.*,
      ou.role AS membership_role
    FROM organization_users ou
    JOIN organizations o ON o.id = ou.organization_id
    WHERE ou.admin_id = $1
      AND o.is_active = TRUE
    ORDER BY o.name ASC
    `,
    [adminId],
  );

  return result.rows;
};

const resolveOrganizationForRequest = async (req, res, next) => {
  try {
    const requestedId =
      req.headers["x-organization-id"] || req.query.organization_id;

    if (!requestedId) {
      const defaultOrg = await getDefaultOrganization();
      req.organization = defaultOrg || null;
      return requestContext.run(
        {
          organization_id: req.organization?.id ?? null,
          organization_name: req.organization?.name ?? null,
        },
        next,
      );
    }

    // super_admin can step into any organization's view for support
    // purposes — they aren't a real member of every client org, so the
    // membership join below would otherwise 403 them out of every
    // per-org page (Channels, Recordings, Live Monitor, etc.).
    if (req.admin?.role === "super_admin") {
      const orgResult = await pool.query(
        `SELECT o.*, 'owner' AS membership_role
         FROM organizations o
         WHERE o.id = $1 AND o.is_active = TRUE
         LIMIT 1`,
        [requestedId],
      );

      if (!orgResult.rows[0]) {
        return res.status(404).json({
          ok: false,
          message: "Organization not found",
        });
      }

      req.organization = orgResult.rows[0];
      return requestContext.run(
        {
          organization_id: req.organization?.id ?? null,
          organization_name: req.organization?.name ?? null,
        },
        next,
      );
    }

    const result = await pool.query(
      `
      SELECT o.*, ou.role AS membership_role
      FROM organizations o
      JOIN organization_users ou ON ou.organization_id = o.id
      WHERE o.id = $1
        AND ou.admin_id = $2
        AND o.is_active = TRUE
      LIMIT 1
      `,
      [requestedId, req.admin.id],
    );

    if (!result.rows[0]) {
      return res.status(403).json({
        ok: false,
        message: "You do not have access to this organization",
      });
    }

    req.organization = result.rows[0];
    requestContext.run(
      {
        organization_id: req.organization?.id ?? null,
        organization_name: req.organization?.name ?? null,
      },
      next,
    );
  } catch (error) {
    console.error("Resolve organization error:", error);
    res.status(500).json({
      ok: false,
      message: "Failed to resolve organization",
      error: error.message,
    });
  }
};

const requireOrganizationRole = (...allowedRoles) => {
  return (req, res, next) => {
    if (req.admin?.role === "super_admin") {
      return next();
    }

    const membershipRole = req.organization?.membership_role;

    if (!membershipRole || !allowedRoles.includes(membershipRole)) {
      return res.status(403).json({
        ok: false,
        message: "You do not have permission for this tenant",
      });
    }

    return next();
  };
};

const getOrganizationIdForStreamKey = async (streamKey) => {
  const cleanStreamKey = String(streamKey || "")
    .trim()
    .slice(0, 255);

  if (!cleanStreamKey) {
    const defaultOrg = await getDefaultOrganization();
    return defaultOrg?.id || null;
  }

  const lookupQueries = [
    {
      sql: "SELECT organization_id FROM channels WHERE stream_key = $1 AND organization_id IS NOT NULL LIMIT 1",
      values: [cleanStreamKey],
    },
    {
      sql: "SELECT organization_id FROM scheduled_streams WHERE stream_key = $1 AND organization_id IS NOT NULL ORDER BY created_at DESC LIMIT 1",
      values: [cleanStreamKey],
    },
  ];

  for (const query of lookupQueries) {
    const result = await pool.query(query.sql, query.values);
    if (result.rows[0]?.organization_id) return result.rows[0].organization_id;
  }

  const defaultOrg = await getDefaultOrganization();
  return defaultOrg?.id || null;
};

const organizationScopedRoom = (prefix, organizationId, streamKey) => {
  return `${prefix}:org:${organizationId || "default"}:${streamKey}`;
};

/*
|--------------------------------------------------------------------------
| SAAS PLANS / SUBSCRIPTIONS
|--------------------------------------------------------------------------
*/

// Feature values below were corrected against the business's actual
// marketing feature-grid (confirmed with the user), which differed from
// the numbers this array previously held on several dimensions (channels,
// storage, bitrate were all wrong; CDN bandwidth, egress bandwidth, TV
// channel, recording, 30-min rewind, and reduced latency weren't tracked
// at all). These are our own internal entitlement flags — unlike price/
// name/description, WHMCS has no concept of them, so PLAN_DEFINITIONS
// stays the authoritative source and these ARE re-synced to the DB on
// every restart (see the DO UPDATE SET below), unlike the WHMCS-owned
// fields.
const PLAN_DEFINITIONS = [
  {
    key: "starter",
    name: "Essential",
    // This price is ONLY used the very first time this plan_key is
    // inserted into the DB — see the comment above the upsert in
    // ensureSubscriptionTables. Set to match WHMCS's actual currently
    // configured price (Essential Streaming Solution, confirmed in
    // WHMCS's Pricing tab) rather than an arbitrary display number, so
    // the fallback is correct even before the first successful live
    // pricing fetch.
    monthly_price_cents: 7999,
    // Hardcoded fallback description, used until a real one is written
    // into WHMCS's Product Short Description field and live-fetched (see
    // the whmcs_description handling in ensureSubscriptionTables — this
    // only backfills a NULL/empty value, never overwrites a real fetched
    // one).
    description:
      "Get live in minutes with 1 channel, 250GB CDN bandwidth, and everything you need for reliable HLS streaming.",
    max_channels: 1,
    max_admins: 2,
    max_storage_gb: 0,
    max_cdn_bandwidth_gb: 250,
    max_egress_bandwidth_gb: 100,
    max_bitrate_kbps: 4000,
    tv_channel_enabled: false,
    recording_enabled: false,
    rewind_enabled: false,
    reduced_latency_enabled: false,
    transcoding_enabled: false,
    analytics_enabled: false,
    custom_domain_enabled: false,
    priority_support_enabled: false,
  },
  {
    key: "pro",
    name: "Deluxe",
    // Matches WHMCS's Deluxe Streaming Solution price — see note above.
    monthly_price_cents: 9999,
    description:
      "Scale up with 3 channels, TV channel delivery, recording, 30-minute rewind, and reduced-latency streaming.",
    max_channels: 3,
    max_admins: 8,
    max_storage_gb: 50,
    max_cdn_bandwidth_gb: 500,
    max_egress_bandwidth_gb: 250,
    max_bitrate_kbps: 10000,
    tv_channel_enabled: true,
    recording_enabled: true,
    rewind_enabled: true,
    reduced_latency_enabled: true,
    transcoding_enabled: true,
    analytics_enabled: true,
    custom_domain_enabled: false,
    priority_support_enabled: false,
  },
  {
    key: "enterprise",
    name: "Premium",
    // Matches WHMCS's Premium Streaming Solution price — see note above.
    monthly_price_cents: 13999,
    description:
      "Our full-featured tier — 5 channels, 1TB CDN bandwidth, recording, rewind, reduced latency, and priority support.",
    max_channels: 5,
    max_admins: 50,
    max_storage_gb: 100,
    max_cdn_bandwidth_gb: 1000,
    max_egress_bandwidth_gb: 350,
    max_bitrate_kbps: 15000,
    tv_channel_enabled: true,
    recording_enabled: true,
    rewind_enabled: true,
    reduced_latency_enabled: true,
    transcoding_enabled: true,
    analytics_enabled: true,
    custom_domain_enabled: true,
    priority_support_enabled: true,
  },
  {
    key: "internal",
    name: "Internal",
    monthly_price_cents: 0,
    description: "",
    max_channels: 999,
    max_admins: 999,
    max_storage_gb: 9999,
    max_cdn_bandwidth_gb: 99999,
    max_egress_bandwidth_gb: 99999,
    max_bitrate_kbps: 50000,
    tv_channel_enabled: true,
    recording_enabled: true,
    rewind_enabled: true,
    reduced_latency_enabled: true,
    transcoding_enabled: true,
    analytics_enabled: true,
    custom_domain_enabled: true,
    priority_support_enabled: true,
  },
];

const ensureSubscriptionTables = async () => {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS plans (
      id SERIAL PRIMARY KEY,
      plan_key VARCHAR(80) UNIQUE NOT NULL,
      name VARCHAR(120) NOT NULL,
      monthly_price_cents INTEGER DEFAULT 0,
      max_channels INTEGER DEFAULT 1,
      max_admins INTEGER DEFAULT 2,
      max_storage_gb INTEGER DEFAULT 25,
      max_cdn_bandwidth_gb INTEGER DEFAULT 0,
      max_egress_bandwidth_gb INTEGER DEFAULT 0,
      max_bitrate_kbps INTEGER DEFAULT 6000,
      tv_channel_enabled BOOLEAN DEFAULT FALSE,
      recording_enabled BOOLEAN DEFAULT FALSE,
      rewind_enabled BOOLEAN DEFAULT FALSE,
      reduced_latency_enabled BOOLEAN DEFAULT FALSE,
      transcoding_enabled BOOLEAN DEFAULT FALSE,
      analytics_enabled BOOLEAN DEFAULT FALSE,
      custom_domain_enabled BOOLEAN DEFAULT FALSE,
      priority_support_enabled BOOLEAN DEFAULT FALSE,
      stripe_price_id VARCHAR(255),
      is_active BOOLEAN DEFAULT TRUE,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS subscriptions (
      id SERIAL PRIMARY KEY,
      organization_id INTEGER NOT NULL UNIQUE REFERENCES organizations(id) ON DELETE CASCADE,
      plan_key VARCHAR(80) NOT NULL DEFAULT 'starter',
      status VARCHAR(40) NOT NULL DEFAULT 'trialing',
      trial_ends_at TIMESTAMPTZ DEFAULT (NOW() + INTERVAL '14 days'),
      current_period_start TIMESTAMPTZ DEFAULT NOW(),
      current_period_end TIMESTAMPTZ DEFAULT (NOW() + INTERVAL '30 days'),
      stripe_customer_id VARCHAR(255),
      stripe_subscription_id VARCHAR(255),
      cancel_at_period_end BOOLEAN DEFAULT FALSE,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  // Stripe columns above are kept as-is (harmless, historical) rather than
  // dropped — this is a "replace going forward" migration, not a data-loss
  // one. All new writes go to the whmcs_* columns added below.
  await pool.query(`
    ALTER TABLE plans
    ADD COLUMN IF NOT EXISTS stripe_price_id VARCHAR(255),
    ADD COLUMN IF NOT EXISTS whmcs_product_id VARCHAR(40),
    ADD COLUMN IF NOT EXISTS whmcs_description TEXT,
    ADD COLUMN IF NOT EXISTS max_cdn_bandwidth_gb INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS max_egress_bandwidth_gb INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS tv_channel_enabled BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS recording_enabled BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS rewind_enabled BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS reduced_latency_enabled BOOLEAN DEFAULT FALSE
  `);

  await pool.query(`
    ALTER TABLE subscriptions
    ADD COLUMN IF NOT EXISTS whmcs_client_id VARCHAR(40),
    ADD COLUMN IF NOT EXISTS whmcs_order_id VARCHAR(40),
    ADD COLUMN IF NOT EXISTS whmcs_invoice_id VARCHAR(40)
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS processed_whmcs_orders (
      whmcs_order_id VARCHAR(40) PRIMARY KEY,
      organization_id INTEGER REFERENCES organizations(id) ON DELETE SET NULL,
      outcome VARCHAR(40) NOT NULL,
      processed_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  // Records both the in-app notification shown to admins AND the history
  // used to decide when a repeat bitrate violation escalates to a kick
  // (see BITRATE_ESCALATION_THRESHOLD / pollBitrateCompliance below) — one
  // table serves both purposes rather than keeping separate ones in sync.
  await pool.query(`
    CREATE TABLE IF NOT EXISTS plan_alerts (
      id SERIAL PRIMARY KEY,
      organization_id INTEGER REFERENCES organizations(id) ON DELETE CASCADE,
      channel_id INTEGER REFERENCES channels(id) ON DELETE SET NULL,
      alert_type VARCHAR(60) NOT NULL,
      message TEXT NOT NULL,
      observed_bitrate_kbps INTEGER,
      plan_bitrate_kbps INTEGER,
      acknowledged BOOLEAN DEFAULT FALSE,
      acknowledged_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  // overage_bytes: only set for the 100%+ storage_quota_exceeded tier —
  // this is the queryable field a billing report pulls from to know how
  // much to charge, since the storage policy is warn+allow-overage+bill,
  // not block or auto-delete. email_sent/email_sent_at track whether the
  // Mailgun send actually succeeded for this alert row, independent of
  // whether it's been acknowledged in-app — a billing report should trust
  // this over "acknowledged", since an org can acknowledge without ever
  // having received the email (Mailgun down, bad address, etc).
  await pool.query(`
    ALTER TABLE plan_alerts
    ADD COLUMN IF NOT EXISTS overage_bytes BIGINT,
    ADD COLUMN IF NOT EXISTS email_sent BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS email_sent_at TIMESTAMPTZ
  `);

  // One-time correction: earlier deploys seeded plans with an arbitrary
  // $29/$79/$199 pricing scheme that was never actually reconciled against
  // WHMCS's real configured prices. Since the upsert below no longer
  // touches monthly_price_cents on conflict (see comment above the loop),
  // those wrong values would otherwise stay stuck on existing rows forever
  // — this brings them in line with WHMCS once, the first time this runs
  // against a DB that still has the old numbers. Safe to run repeatedly:
  // a no-op once the values already match.
  await pool.query(`
    UPDATE plans SET monthly_price_cents = 7999, updated_at = NOW()
    WHERE plan_key = 'starter' AND monthly_price_cents = 2900
  `);
  await pool.query(`
    UPDATE plans SET monthly_price_cents = 9999, updated_at = NOW()
    WHERE plan_key = 'pro' AND monthly_price_cents = 7900
  `);
  await pool.query(`
    UPDATE plans SET monthly_price_cents = 13999, updated_at = NOW()
    WHERE plan_key = 'enterprise' AND monthly_price_cents = 19900
  `);

  for (const plan of PLAN_DEFINITIONS) {
    const stripePriceId = getStripePriceIdForPlan(plan.key);
    const whmcsProductId = whmcs.getWhmcsProductIdForPlan(plan.key);

    // monthly_price_cents is intentionally NOT re-seeded from
    // PLAN_DEFINITIONS on conflict below (see `= plans.monthly_price_cents`
    // in the DO UPDATE) — WHMCS is the real source of truth for price, and
    // /api/public/plans keeps this column updated as a write-through cache
    // of the last known live price. If this upsert overwrote it with the
    // hardcoded fallback on every restart, that cache would be wiped
    // constantly. PLAN_DEFINITIONS' price is only ever used for a brand
    // new plan_key's first insert, or if the row doesn't exist yet.
    await pool.query(
      `
      INSERT INTO plans (
        plan_key,
        name,
        monthly_price_cents,
        max_channels,
        max_admins,
        max_storage_gb,
        max_cdn_bandwidth_gb,
        max_egress_bandwidth_gb,
        max_bitrate_kbps,
        tv_channel_enabled,
        recording_enabled,
        rewind_enabled,
        reduced_latency_enabled,
        transcoding_enabled,
        analytics_enabled,
        custom_domain_enabled,
        priority_support_enabled,
        stripe_price_id,
        whmcs_product_id,
        is_active
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, TRUE)
      ON CONFLICT (plan_key)
      DO UPDATE SET
        name = EXCLUDED.name,
        monthly_price_cents = plans.monthly_price_cents,
        max_channels = EXCLUDED.max_channels,
        max_admins = EXCLUDED.max_admins,
        max_storage_gb = EXCLUDED.max_storage_gb,
        max_cdn_bandwidth_gb = EXCLUDED.max_cdn_bandwidth_gb,
        max_egress_bandwidth_gb = EXCLUDED.max_egress_bandwidth_gb,
        max_bitrate_kbps = EXCLUDED.max_bitrate_kbps,
        tv_channel_enabled = EXCLUDED.tv_channel_enabled,
        recording_enabled = EXCLUDED.recording_enabled,
        rewind_enabled = EXCLUDED.rewind_enabled,
        reduced_latency_enabled = EXCLUDED.reduced_latency_enabled,
        transcoding_enabled = EXCLUDED.transcoding_enabled,
        analytics_enabled = EXCLUDED.analytics_enabled,
        custom_domain_enabled = EXCLUDED.custom_domain_enabled,
        priority_support_enabled = EXCLUDED.priority_support_enabled,
        stripe_price_id = EXCLUDED.stripe_price_id,
        whmcs_product_id = EXCLUDED.whmcs_product_id,
        is_active = TRUE,
        updated_at = NOW()
      `,
      [
        plan.key,
        plan.name,
        plan.monthly_price_cents,
        plan.max_channels,
        plan.max_admins,
        plan.max_storage_gb,
        plan.max_cdn_bandwidth_gb,
        plan.max_egress_bandwidth_gb,
        plan.max_bitrate_kbps,
        plan.tv_channel_enabled,
        plan.recording_enabled,
        plan.rewind_enabled,
        plan.reduced_latency_enabled,
        plan.transcoding_enabled,
        plan.analytics_enabled,
        plan.custom_domain_enabled,
        plan.priority_support_enabled,
        stripePriceId || null,
        whmcsProductId || null,
      ],
    );

    // Hardcoded fallback description ("for the meantime", per user) — only
    // fills in an empty value. Never overwrites a real description once
    // one has been fetched live from WHMCS (see the /api/public/plans
    // overlay), so this naturally stops mattering the moment real WHMCS
    // copy exists.
    if (plan.description) {
      await pool.query(
        `
        UPDATE plans
        SET whmcs_description = $1, updated_at = NOW()
        WHERE plan_key = $2 AND (whmcs_description IS NULL OR whmcs_description = '')
        `,
        [plan.description, plan.key],
      );
    }
  }

  await pool.query(`
    INSERT INTO subscriptions (organization_id, plan_key, status)
    SELECT id, COALESCE(subscription_plan, 'starter'), 'active'
    FROM organizations
    ON CONFLICT (organization_id) DO NOTHING
  `);
};

const ensurePendingSignupsTable = async () => {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS pending_signups (
      id SERIAL PRIMARY KEY,
      checkout_session_id VARCHAR(255) UNIQUE,
      stripe_customer_id VARCHAR(255),
      plan_key VARCHAR(80) NOT NULL,
      organization_name VARCHAR(255) NOT NULL,
      client_name VARCHAR(255) NOT NULL,
      client_email VARCHAR(255) NOT NULL,
      password_hash TEXT NOT NULL,
      stream_key VARCHAR(255) NOT NULL,
      primary_color VARCHAR(40) DEFAULT '#0d6efd',
      secondary_color VARCHAR(40) DEFAULT '#fd9d00',
      donation_url TEXT,
      status VARCHAR(40) DEFAULT 'pending',
      organization_id INTEGER REFERENCES organizations(id) ON DELETE SET NULL,
      admin_id INTEGER REFERENCES admins(id) ON DELETE SET NULL,
      channel_id INTEGER REFERENCES channels(id) ON DELETE SET NULL,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      completed_at TIMESTAMPTZ,
      updated_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  await pool.query(`
    ALTER TABLE pending_signups
    ADD COLUMN IF NOT EXISTS stripe_customer_id VARCHAR(255),
    ADD COLUMN IF NOT EXISTS organization_id INTEGER REFERENCES organizations(id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS admin_id INTEGER REFERENCES admins(id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS channel_id INTEGER REFERENCES channels(id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()
  `);
};

// Known feature flags and their defaults. Any flag not yet in the DB
// falls back to this default, so existing hardcoded behavior is
// preserved until a super_admin explicitly changes it.
const FEATURE_FLAG_DEFAULTS = {
  members_page: {
    enabled: false,
    description: "Show the Members page in the sidebar nav.",
  },
  public_replay_features: {
    enabled: false,
    description:
      "Public replay watch page, library access, and Publish/Unpublish/Edit Metadata on recordings.",
  },
};

const ensureFeatureFlagsTable = async () => {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS feature_flags (
      key VARCHAR(80) PRIMARY KEY,
      enabled BOOLEAN NOT NULL DEFAULT FALSE,
      description TEXT,
      updated_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);
};

const ensureNotificationPreferencesTable = async () => {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS notification_preferences (
      admin_id INTEGER PRIMARY KEY REFERENCES admins(id) ON DELETE CASCADE,
      stream_live BOOLEAN NOT NULL DEFAULT TRUE,
      chat_needs_moderation BOOLEAN NOT NULL DEFAULT FALSE,
      recording_processed BOOLEAN NOT NULL DEFAULT FALSE,
      updated_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);
};

const getFeatureFlags = async () => {
  const result = await pool.query(`SELECT key, enabled FROM feature_flags`);
  const stored = new Map(result.rows.map((r) => [r.key, r.enabled]));

  const flags = {};
  for (const [key, def] of Object.entries(FEATURE_FLAG_DEFAULTS)) {
    flags[key] = stored.has(key) ? stored.get(key) : def.enabled;
  }
  return flags;
};

// Completes a pending_signups row once WHMCS confirms the matching order
// is active/paid. Unlike the old Stripe flow (keyed off a checkout_session_id
// baked into the session before payment), WHMCS's hosted order form means
// the customer never touches our backend before paying — so matching is
// done by the poller on client_email + plan_key instead (see
// pollWhmcsBilling below), and this function is called with the already
// -matched pending row plus the WHMCS identifiers it should be stamped with.
const completePendingSignupFromWhmcs = async (
  pending,
  { whmcsClientId, whmcsOrderId, whmcsInvoiceId, nextDueDate } = {},
) => {
  if (!pending) return null;

  if (pending.status === "completed" && pending.organization_id) {
    return pending;
  }

  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const existingAdmin = await client.query(
      `SELECT id FROM admins WHERE email = $1 LIMIT 1`,
      [pending.client_email],
    );

    if (existingAdmin.rows[0]) {
      await client.query(
        `
        UPDATE pending_signups
        SET status = 'failed',
            updated_at = NOW()
        WHERE id = $1
        `,
        [pending.id],
      );

      await client.query("COMMIT");

      console.error(
        `Pending signup failed because email already exists: ${pending.client_email}`,
      );
      return null;
    }

    const slug = await ensureUniqueOrganizationSlug(pending.organization_name);

    const organizationResult = await client.query(
      `
      INSERT INTO organizations (
        name,
        slug,
        primary_color,
        subscription_plan,
        is_active
      )
      VALUES ($1, $2, $3, $4, TRUE)
      RETURNING *
      `,
      [
        pending.organization_name,
        slug,
        pending.primary_color || "#0d6efd",
        pending.plan_key,
      ],
    );

    const organization = organizationResult.rows[0];

    await client.query(
      `
      INSERT INTO organization_settings (
        organization_id,
        watch_page_title,
        primary_color,
        secondary_color,
        donation_url
      )
      VALUES ($1, $2, $3, $4, $5)
      ON CONFLICT (organization_id) DO NOTHING
      `,
      [
        organization.id,
        organization.name,
        pending.primary_color || "#0d6efd",
        pending.secondary_color || "#fd9d00",
        pending.donation_url || null,
      ],
    );

    const adminResult = await client.query(
      `
      INSERT INTO admins (name, email, password_hash, role)
      VALUES ($1, $2, $3, 'operator')
      RETURNING id, name, email, role, created_at
      `,
      [pending.client_name, pending.client_email, pending.password_hash],
    );

    const admin = adminResult.rows[0];

    await client.query(
      `
      INSERT INTO organization_users (organization_id, admin_id, role)
      VALUES ($1, $2, 'owner')
      ON CONFLICT (organization_id, admin_id) DO NOTHING
      `,
      [organization.id, admin.id],
    );

    const channelResult = await client.query(
      `
      INSERT INTO channels (organization_id, name, stream_key, description)
      VALUES ($1, $2, $3, $4)
      RETURNING *
      `,
      [
        organization.id,
        `${organization.name} Main`,
        pending.stream_key,
        "Primary livestream channel",
      ],
    );

    const periodStart = new Date();
    const periodEnd = nextDueDate ? new Date(nextDueDate) : null;

    await client.query(
      `
      INSERT INTO subscriptions (
        organization_id,
        plan_key,
        status,
        current_period_start,
        current_period_end,
        whmcs_client_id,
        whmcs_order_id,
        whmcs_invoice_id
      )
      VALUES ($1, $2, 'active', $3, $4, $5, $6, $7)
      ON CONFLICT (organization_id)
      DO UPDATE SET
        plan_key = EXCLUDED.plan_key,
        status = 'active',
        current_period_start = EXCLUDED.current_period_start,
        current_period_end = COALESCE(EXCLUDED.current_period_end, subscriptions.current_period_end),
        whmcs_client_id = EXCLUDED.whmcs_client_id,
        whmcs_order_id = EXCLUDED.whmcs_order_id,
        whmcs_invoice_id = EXCLUDED.whmcs_invoice_id,
        updated_at = NOW()
      `,
      [
        organization.id,
        pending.plan_key,
        periodStart,
        periodEnd,
        whmcsClientId || null,
        whmcsOrderId || null,
        whmcsInvoiceId || null,
      ],
    );

    const completedResult = await client.query(
      `
      UPDATE pending_signups
      SET status = 'completed',
          organization_id = $1,
          admin_id = $2,
          channel_id = $3,
          completed_at = NOW(),
          updated_at = NOW()
      WHERE id = $4
      RETURNING *
      `,
      [organization.id, admin.id, channelResult.rows[0].id, pending.id],
    );

    await client.query("COMMIT");

    console.log(
      `Completed paid signup for ${pending.client_email} / ${organization.name}`,
    );

    // Fire-and-forget from the caller's perspective (this function itself
    // still awaits it so provisioning happens promptly, but a failure here
    // is logged and swallowed, not thrown — see the function's own
    // comment for why).
    await provisionBunnyZonesForNewOrganization(organization);

    return completedResult.rows[0];
  } catch (error) {
    await client.query("ROLLBACK");
    console.error("Complete pending signup error:", error);
    throw error;
  } finally {
    client.release();
  }
};

const getOrganizationSubscriptionSummary = async (organizationId) => {
  const result = await pool.query(
    `
    SELECT
      s.id AS subscription_id,
      o.id AS organization_id,
      COALESCE(s.plan_key, o.subscription_plan, 'starter') AS plan_key,
      s.status,
      s.trial_ends_at,
      s.current_period_start,
      s.current_period_end,
      s.cancel_at_period_end,
      s.stripe_customer_id,
      s.stripe_subscription_id,
      s.whmcs_client_id,
      s.whmcs_order_id,
      s.whmcs_invoice_id,
      p.name AS plan_name,
      p.monthly_price_cents,
      p.max_channels,
      p.max_admins,
      p.max_storage_gb,
      p.max_cdn_bandwidth_gb,
      p.max_egress_bandwidth_gb,
      p.max_bitrate_kbps,
      p.tv_channel_enabled,
      p.recording_enabled,
      p.rewind_enabled,
      p.reduced_latency_enabled,
      p.transcoding_enabled,
      p.analytics_enabled,
      p.custom_domain_enabled,
      p.priority_support_enabled,
      COALESCE(channel_usage.count, 0)::int AS used_channels,
      COALESCE(member_usage.count, 0)::int AS used_admins,
      COALESCE(storage_usage.total_bytes, 0)::bigint AS used_storage_bytes
    FROM organizations o
    LEFT JOIN subscriptions s ON s.organization_id = o.id
    JOIN plans p ON p.plan_key = COALESCE(s.plan_key, o.subscription_plan, 'starter')
    LEFT JOIN (
      SELECT organization_id, COUNT(*) AS count
      FROM channels
      WHERE organization_id = $1
      GROUP BY organization_id
    ) channel_usage ON channel_usage.organization_id = o.id
    LEFT JOIN (
      SELECT organization_id, COUNT(*) AS count
      FROM organization_users
      WHERE organization_id = $1
      GROUP BY organization_id
    ) member_usage ON member_usage.organization_id = o.id
    LEFT JOIN (
      SELECT organization_id, SUM(file_size_bytes) AS total_bytes
      FROM recordings
      WHERE organization_id = $1
      GROUP BY organization_id
    ) storage_usage ON storage_usage.organization_id = o.id
    WHERE o.id = $1
    LIMIT 1
    `,
    [organizationId],
  );

  return result.rows[0] || null;
};

const ensureSubscriptionForOrganization = async (
  organizationId,
  planKey = "starter",
) => {
  await pool.query(
    `
    INSERT INTO subscriptions (organization_id, plan_key, status)
    VALUES ($1, $2, 'active')
    ON CONFLICT (organization_id) DO NOTHING
    `,
    [organizationId, planKey || "starter"],
  );

  return getOrganizationSubscriptionSummary(organizationId);
};

const getStripePriceIdForPlan = (planKey) => {
  const normalized = String(planKey || "").toLowerCase();

  const priceMap = {
    starter: process.env.STRIPE_STARTER_PRICE_ID,
    pro: process.env.STRIPE_PRO_PRICE_ID,
    enterprise: process.env.STRIPE_ENTERPRISE_PRICE_ID,
  };

  return priceMap[normalized] || null;
};

const getPlanKeyForStripePriceId = (priceId) => {
  if (!priceId) return null;

  const entries = [
    ["starter", process.env.STRIPE_STARTER_PRICE_ID],
    ["pro", process.env.STRIPE_PRO_PRICE_ID],
    ["enterprise", process.env.STRIPE_ENTERPRISE_PRICE_ID],
  ];

  const match = entries.find(([, configuredPriceId]) => {
    return configuredPriceId && configuredPriceId === priceId;
  });

  return match?.[0] || null;
};

// Applies a WHMCS order's current state to an already-existing
// organization's subscription row (the "existing client upgrades/renews"
// path — as opposed to completePendingSignupFromWhmcs, which is the
// "brand new signup" path). Used by both the poller and the manual
// /api/subscription/refresh endpoint.
const syncWhmcsOrgSubscription = async (
  organizationId,
  {
    planKey,
    whmcsStatus,
    whmcsClientId,
    whmcsOrderId,
    whmcsInvoiceId,
    nextDueDate,
  },
) => {
  if (!organizationId) return null;

  const status = whmcs.mapWhmcsStatusToSubscriptionStatus(whmcsStatus);

  if (planKey) {
    await pool.query(
      `
      UPDATE organizations
      SET subscription_plan = $1,
          updated_at = NOW()
      WHERE id = $2
      `,
      [planKey, organizationId],
    );
  }

  const result = await pool.query(
    `
    INSERT INTO subscriptions (
      organization_id,
      plan_key,
      status,
      current_period_start,
      current_period_end,
      whmcs_client_id,
      whmcs_order_id,
      whmcs_invoice_id
    )
    VALUES ($1, $2, $3, NOW(), $4, $5, $6, $7)
    ON CONFLICT (organization_id)
    DO UPDATE SET
      plan_key = COALESCE($2, subscriptions.plan_key),
      status = $3,
      current_period_end = COALESCE($4, subscriptions.current_period_end),
      whmcs_client_id = COALESCE($5, subscriptions.whmcs_client_id),
      whmcs_order_id = COALESCE($6, subscriptions.whmcs_order_id),
      whmcs_invoice_id = COALESCE($7, subscriptions.whmcs_invoice_id),
      updated_at = NOW()
    RETURNING *
    `,
    [
      organizationId,
      planKey || null,
      status,
      nextDueDate ? new Date(nextDueDate) : null,
      whmcsClientId || null,
      whmcsOrderId || null,
      whmcsInvoiceId || null,
    ],
  );

  return result.rows[0];
};

// ══════════════════════════════════════════
// WHMCS BILLING POLLER
// Since checkout happens entirely on WHMCS's own hosted cart, our app has
// no inbound signal when someone pays. This runs on an interval (see the
// setInterval call near server startup) and pulls WHMCS's own order list
// instead, matching each new active order to either a pending_signups row
// (brand new customer) or an existing admin's organization (plan
// upgrade/renewal), and provisioning/syncing accordingly.
//
// Idempotency: processed_whmcs_orders records every order id this poller
// has already acted on, so re-running the same order on the next cycle
// (which will happen constantly, since GetOrders always returns recent
// orders) is a no-op.
// ══════════════════════════════════════════
const pollWhmcsBilling = async () => {
  if (!whmcs.isWhmcsConfigured()) return;

  try {
    const orders = await whmcs.getOrders({ limitNum: 50, status: "Active" });

    for (const order of orders) {
      const orderId = String(order.id);

      const alreadyProcessed = await pool.query(
        `SELECT 1 FROM processed_whmcs_orders WHERE whmcs_order_id = $1 LIMIT 1`,
        [orderId],
      );
      if (alreadyProcessed.rows[0]) continue;

      // GetOrders nests line items differently across WHMCS versions —
      // handle both a single product object and an array of them.
      const rawProducts = order.lineitems?.lineitem
        ? Array.isArray(order.lineitems.lineitem)
          ? order.lineitems.lineitem
          : [order.lineitems.lineitem]
        : [];

      const productLine = rawProducts.find((item) =>
        whmcs.getPlanKeyForWhmcsProductId(item.relid || item.pid),
      );

      const planKey = productLine
        ? whmcs.getPlanKeyForWhmcsProductId(
            productLine.relid || productLine.pid,
          )
        : null;

      if (!planKey) {
        // Not one of our recognized plan products (could be a one-off
        // WHMCS product unrelated to this app) — nothing for us to do.
        await pool.query(
          `INSERT INTO processed_whmcs_orders (whmcs_order_id, outcome) VALUES ($1, 'skipped_unrecognized_product') ON CONFLICT DO NOTHING`,
          [orderId],
        );
        continue;
      }

      let clientEmail = null;
      try {
        const clientDetails = await whmcs.getClientDetails(order.userid);
        clientEmail = String(clientDetails.email || "").toLowerCase();
      } catch (clientErr) {
        console.error(
          `[WHMCS-POLL] Failed to fetch client ${order.userid} for order ${orderId}:`,
          clientErr.message,
        );
      }

      if (!clientEmail) {
        await pool.query(
          `INSERT INTO processed_whmcs_orders (whmcs_order_id, outcome) VALUES ($1, 'skipped_no_client_email') ON CONFLICT DO NOTHING`,
          [orderId],
        );
        continue;
      }

      // 1) Brand-new signup: does a pending_signups row match this email + plan?
      const pendingResult = await pool.query(
        `
        SELECT * FROM pending_signups
        WHERE client_email = $1 AND plan_key = $2 AND status = 'pending'
        LIMIT 1
        `,
        [clientEmail, planKey],
      );

      if (pendingResult.rows[0]) {
        try {
          const completed = await completePendingSignupFromWhmcs(
            pendingResult.rows[0],
            {
              whmcsClientId: order.userid,
              whmcsOrderId: orderId,
              nextDueDate: productLine?.nextduedate,
            },
          );

          await pool.query(
            `INSERT INTO processed_whmcs_orders (whmcs_order_id, organization_id, outcome) VALUES ($1, $2, 'provisioned_new_signup') ON CONFLICT DO NOTHING`,
            [orderId, completed?.organization_id || null],
          );
        } catch (provisionErr) {
          console.error(
            `[WHMCS-POLL] Failed to provision pending signup for order ${orderId}:`,
            provisionErr.message,
          );
        }
        continue;
      }

      // 2) Existing customer upgrading/renewing: match by admin email.
      const adminResult = await pool.query(
        `
        SELECT ou.organization_id
        FROM admins a
        JOIN organization_users ou ON ou.admin_id = a.id AND ou.role = 'owner'
        WHERE a.email = $1
        LIMIT 1
        `,
        [clientEmail],
      );

      const organizationId = adminResult.rows[0]?.organization_id;

      if (organizationId) {
        await syncWhmcsOrgSubscription(organizationId, {
          planKey,
          whmcsStatus: order.status,
          whmcsClientId: order.userid,
          whmcsOrderId: orderId,
          nextDueDate: productLine?.nextduedate,
        });

        await pool.query(
          `INSERT INTO processed_whmcs_orders (whmcs_order_id, organization_id, outcome) VALUES ($1, $2, 'synced_existing_org') ON CONFLICT DO NOTHING`,
          [orderId, organizationId],
        );
        continue;
      }

      // Neither a pending signup nor an existing org owner matched this
      // email — likely a manual/unrelated WHMCS sale. Flag it and move on;
      // don't loop retrying forever on something we can't resolve.
      console.error(
        `[WHMCS-POLL] Order ${orderId} (plan ${planKey}, client ${clientEmail}) has no matching pending signup or organization — needs manual review.`,
      );
      await pool.query(
        `INSERT INTO processed_whmcs_orders (whmcs_order_id, outcome) VALUES ($1, 'unmatched_needs_manual_review') ON CONFLICT DO NOTHING`,
        [orderId],
      );
    }
  } catch (error) {
    console.error("[WHMCS-POLL] Billing poll failed:", error.message);
  }
};

// ══════════════════════════════════════════
// BITRATE COMPLIANCE MONITOR
// Church/ministry live streaming is the primary use case here, so a hard
// disconnect over a first-time bitrate mistake (e.g. a volunteer's OBS
// misconfigured) is the wrong customer experience — this monitors and
// WARNS first, only escalating to an actual kick after repeat sustained
// violations. See the chat discussion this was built from for the
// reasoning behind that choice over a harder transcode-based cap.
//
// Polls SRS's stream list ONCE per cycle (not once per stream — SRS
// returns every active stream's stats in a single call), so this scales
// to many concurrent orgs streaming at once (e.g. a Sunday morning peak)
// without added per-stream cost.
// ══════════════════════════════════════════
const BITRATE_POLL_INTERVAL_MS = 20 * 1000;
const BITRATE_GRACE_MULTIPLIER = 1.1; // 10% grace over the plan's cap
const BITRATE_SUSTAINED_MS = 5 * 60 * 1000; // must stay over cap this long to count as one violation
const BITRATE_VIOLATION_WINDOW_DAYS = 30; // just for informational "seen N times" context now

// In-memory only — tracks how long each currently-live stream has been
// continuously over its cap. Deliberately not persisted: only a violation
// that actually crosses the sustained-duration threshold gets written to
// plan_alerts, so a brief blip never touches the DB at all.
const bitrateOverCapTracker = new Map(); // stream_key -> { since: ms timestamp, recorded: bool }
const unmappedStreamsLogged = new Set(); // stream_key -> already logged "no matching plan" once

// This is now INFORMATIONAL ONLY — no kick, ever. Now that the hard cap
// (autoCapBitrateStream) actually enforces the ceiling regardless of what
// the source encoder pushes, there's nothing left for a raw-source
// bitrate check to protect against: a customer literally cannot exceed
// their plan's delivered bitrate anymore, so disconnecting them for their
// encoder setting alone would serve no purpose — and would kill the
// working capped transcode too, since it reads from that same raw
// connection. This just lets them know their encoder is set higher than
// their plan needs, so they can save their own upload bandwidth.
const recordBitrateViolation = async ({
  organizationId,
  channelId,
  streamName,
  observedKbps,
  capKbps,
}) => {
  const priorCountResult = await pool.query(
    `
    SELECT COUNT(*)::int AS count
    FROM plan_alerts
    WHERE organization_id = $1
      AND alert_type = 'bitrate_warning'
      AND created_at > NOW() - INTERVAL '${BITRATE_VIOLATION_WINDOW_DAYS} days'
    `,
    [organizationId],
  );

  const timesSeen = (priorCountResult.rows[0]?.count || 0) + 1;

  const message = `Stream "${streamName}"'s encoder is set to ~${Math.round(observedKbps / 1000)}Mbps, higher than your plan's ${Math.round(capKbps / 1000)}Mbps limit. We're automatically capping the delivered stream to stay within your plan, so viewers aren't affected — but lowering your encoder's bitrate to match would save your own upload bandwidth.${timesSeen > 1 ? ` (Seen ${timesSeen} times in the last ${BITRATE_VIOLATION_WINDOW_DAYS} days.)` : ""}`;

  await pool.query(
    `
    INSERT INTO plan_alerts (
      organization_id, channel_id, alert_type, message,
      observed_bitrate_kbps, plan_bitrate_kbps
    )
    VALUES ($1, $2, 'bitrate_warning', $3, $4, $5)
    `,
    [
      organizationId,
      channelId || null,
      message,
      Math.round(observedKbps),
      capKbps,
    ],
  );

  console.log(
    `[BITRATE] Informational notice recorded for org ${organizationId}, stream ${streamName} (seen ${timesSeen}x in ${BITRATE_VIOLATION_WINDOW_DAYS}d)`,
  );
};

// Real bug found and fixed 2026-08-03: the three call sites below each
// hardcoded a check for streamKey.endsWith("_720p")/"_480p" specifically —
// when the bitrate-cap fold introduced a THIRD rendition label ("top"),
// none of them were updated, so ffmpeg's own republish of the "top"
// rendition fell through to normal on_publish validation, found no
// matching channel row for "{streamKey}_top", and got REJECTED (403) by
// our own webhook — meaning it could never actually start, so
// master.m3u8's checkPlaylist never found it and viewers got a manifest
// with zero levels ("no levels found in manifest", confirmed live via an
// Essential-tier org). Centralizing this check in one place so adding or
// renaming a rendition label in the future can't silently break this
// again the same way.
const ABR_RENDITION_SUFFIX_PATTERN = /_(top|720p|480p)$/;
const isAbrRenditionStreamKey = (streamKey) =>
  ABR_RENDITION_SUFFIX_PATTERN.test(streamKey || "");

const pollBitrateCompliance = async () => {
  try {
    const response = await fetch(`${SRS_API_URL}/api/v1/streams`);
    if (!response.ok) return;

    const data = await response.json();
    const activeStreams = (data.streams || []).filter((s) => {
      if (!s.publish?.active) return false;
      // Only check the raw ingest app — live_capped (retired) and the
      // ABR rendition variants (top/720p/480p) all re-publish under the
      // same base stream name and would otherwise be double-counted or
      // confuse which bitrate is "real". NOTE: the exact field SRS uses
      // for app name in this response (`s.app`) isn't confirmed against a
      // live payload — if this ever silently stops catching real streams,
      // check the actual field name here.
      if (s.app && s.app !== "live") return false;
      if (isAbrRenditionStreamKey(s.name)) return false;
      return true;
    });

    if (!activeStreams.length) {
      bitrateOverCapTracker.clear();
      return;
    }

    const streamNames = activeStreams.map((s) => s.name);
    const channelResult = await pool.query(
      `
      SELECT c.stream_key, c.id AS channel_id, c.organization_id, p.max_bitrate_kbps
      FROM channels c
      JOIN organizations o ON o.id = c.organization_id
      LEFT JOIN subscriptions s ON s.organization_id = c.organization_id
      JOIN plans p ON p.plan_key = COALESCE(s.plan_key, o.subscription_plan, 'starter')
      WHERE c.stream_key = ANY($1::text[])
      `,
      [streamNames],
    );

    const infoByStreamKey = new Map(
      channelResult.rows.map((row) => [row.stream_key, row]),
    );

    const liveStreamNames = new Set(streamNames);
    // Clear tracking for any stream that's no longer live, so a fresh
    // session always starts its sustained-duration count from zero.
    for (const key of bitrateOverCapTracker.keys()) {
      if (!liveStreamNames.has(key)) bitrateOverCapTracker.delete(key);
    }
    for (const key of unmappedStreamsLogged) {
      if (!liveStreamNames.has(key)) unmappedStreamsLogged.delete(key);
    }

    for (const stream of activeStreams) {
      const info = infoByStreamKey.get(stream.name);
      if (!info || !info.max_bitrate_kbps) {
        if (!unmappedStreamsLogged.has(stream.name)) {
          console.log(
            `[BITRATE] Stream "${stream.name}" is live but has no matching channel/plan cap — skipping compliance check for it.`,
          );
          unmappedStreamsLogged.add(stream.name);
        }
        continue;
      }

      const observedKbps = Number(stream.kbps?.recv_30s || 0);
      const capKbps = Number(info.max_bitrate_kbps);
      const isOverCap = observedKbps > capKbps * BITRATE_GRACE_MULTIPLIER;

      if (!isOverCap) {
        bitrateOverCapTracker.delete(stream.name);
        continue;
      }

      const tracked = bitrateOverCapTracker.get(stream.name);

      if (!tracked) {
        bitrateOverCapTracker.set(stream.name, {
          since: Date.now(),
          recorded: false,
        });
        continue;
      }

      if (tracked.recorded) continue; // already actioned this sustained streak

      if (Date.now() - tracked.since < BITRATE_SUSTAINED_MS) continue; // not sustained long enough yet

      tracked.recorded = true;

      await recordBitrateViolation({
        organizationId: info.organization_id,
        channelId: info.channel_id,
        streamName: stream.name,
        observedKbps,
        capKbps,
      });
    }
  } catch (err) {
    console.error("[BITRATE] Compliance poll failed:", err.message);
  }
};

const ACTIVE_SUBSCRIPTION_STATUSES = new Set(["active", "trialing"]);

const requireActiveSubscription = async (req, res, next) => {
  try {
    if (!req.organization?.id) {
      return res.status(400).json({
        ok: false,
        code: "ORGANIZATION_REQUIRED",
        message: "Please select an organization.",
      });
    }

    const subscription = await ensureSubscriptionForOrganization(
      req.organization.id,
      req.organization.subscription_plan || "starter",
    );

    if (!subscription) {
      return res.status(402).json({
        ok: false,
        code: "SUBSCRIPTION_REQUIRED",
        message: "No subscription was found for this organization.",
      });
    }

    if (
      subscription.status &&
      !ACTIVE_SUBSCRIPTION_STATUSES.has(subscription.status)
    ) {
      return res.status(402).json({
        ok: false,
        code: "SUBSCRIPTION_INACTIVE",
        message: "This organization’s subscription is not active.",
        subscription,
      });
    }

    req.subscription = subscription;
    next();
  } catch (error) {
    console.error("Active subscription check error:", error);
    res.status(500).json({
      ok: false,
      code: "SUBSCRIPTION_CHECK_FAILED",
      message: "Failed to verify the organization subscription.",
      error: error.message,
    });
  }
};

const requirePlanFeature = (featureKey) => {
  return async (req, res, next) => {
    try {
      if (!req.organization?.id) {
        return res.status(400).json({
          ok: false,
          code: "ORGANIZATION_REQUIRED",
          message: "Please select an organization.",
        });
      }

      const subscription =
        req.subscription ||
        (await ensureSubscriptionForOrganization(
          req.organization.id,
          req.organization.subscription_plan || "starter",
        ));

      if (!subscription) {
        return res.status(402).json({
          ok: false,
          code: "SUBSCRIPTION_REQUIRED",
          message: "No subscription was found for this organization.",
        });
      }

      if (
        subscription.status &&
        !ACTIVE_SUBSCRIPTION_STATUSES.has(subscription.status)
      ) {
        return res.status(402).json({
          ok: false,
          code: "SUBSCRIPTION_INACTIVE",
          message: "This organization’s subscription is not active.",
          subscription,
        });
      }

      if (!subscription[featureKey]) {
        return res.status(403).json({
          ok: false,
          code: "PLAN_FEATURE_REQUIRED",
          feature: featureKey,
          message: `Your ${subscription.plan_name} plan does not include this feature.`,
          subscription,
        });
      }

      req.subscription = subscription;
      next();
    } catch (error) {
      console.error(`Plan feature check failed (${featureKey}):`, error);
      res.status(500).json({
        ok: false,
        code: "SUBSCRIPTION_CHECK_FAILED",
        message: "Failed to verify the plan entitlement.",
        error: error.message,
      });
    }
  };
};

const enforceChannelLimit = async (req, res, next) => {
  try {
    const summary = await ensureSubscriptionForOrganization(
      req.organization.id,
      req.organization.subscription_plan || "starter",
    );

    if (!summary) return next();

    if (summary.status && !ACTIVE_SUBSCRIPTION_STATUSES.has(summary.status)) {
      return res.status(402).json({
        ok: false,
        code: "SUBSCRIPTION_INACTIVE",
        message: "This tenant subscription is not active.",
        subscription: summary,
      });
    }

    if (
      Number(summary.used_channels || 0) >= Number(summary.max_channels || 0)
    ) {
      return res.status(402).json({
        ok: false,
        code: "CHANNEL_LIMIT_REACHED",
        message: `Your ${summary.plan_name} plan allows ${summary.max_channels} channel(s). Upgrade the plan to add more channels.`,
        subscription: summary,
      });
    }

    req.subscription = summary;
    next();
  } catch (error) {
    console.error("Plan limit check error:", error);
    res.status(500).json({
      ok: false,
      message: "Failed to check subscription limits",
      error: error.message,
    });
  }
};

app.get("/api/public/plans", async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT
        *,
        CASE
          WHEN whmcs_product_id IS NOT NULL AND whmcs_product_id <> '' THEN TRUE
          ELSE FALSE
        END AS whmcs_configured
      FROM plans
      WHERE is_active = TRUE
        AND plan_key <> 'internal'
      ORDER BY monthly_price_cents ASC
    `);

    let plans = result.rows;

    // WHMCS is the source of truth for price, not our own plans table —
    // that table previously drifted out of sync with what WHMCS actually
    // charges (a real incident: this app advertised $29/$79/$199 while
    // WHMCS billed $79.99/$99.99/$139.99 for the same products). Fetch
    // live pricing here and write it back to the DB so monthly_price_cents
    // always reflects "last known real price" — used as the fallback on
    // any request where the live WHMCS call fails (e.g. while its IP
    // allow-list is still pending for this server).
    if (whmcs.isWhmcsConfigured()) {
      try {
        const pids = plans.map((p) => p.whmcs_product_id).filter(Boolean);
        const liveDetails = await whmcs.getProductsDetails(pids);

        plans = await Promise.all(
          plans.map(async (plan) => {
            const details = liveDetails[String(plan.whmcs_product_id)];
            if (!details) return plan;

            const liveCents = details.monthlyPriceCents;
            // Prefer the short description for display; fall back to the
            // long description if only that's filled in. Both are blank
            // strings, not null, when nothing's configured in WHMCS yet.
            const liveDescription =
              details.shortDescription || details.description || "";

            // WHMCS's real product name, e.g. "Essential Streaming
            // Solution" — strip the repetitive "Streaming Solution" suffix
            // to match the short-form naming already used in the
            // business's own marketing (Essential/Deluxe/Premium), rather
            // than showing the full WHMCS string verbatim on the card.
            const liveName = details.name
              ? details.name.replace(/\s+Streaming Solution$/i, "").trim()
              : "";

            const priceChanged =
              liveCents && liveCents !== plan.monthly_price_cents;
            const descriptionChanged =
              liveDescription !== (plan.whmcs_description || "");
            const nameChanged = liveName && liveName !== plan.name;

            if (!priceChanged && !descriptionChanged && !nameChanged)
              return plan;

            await pool.query(
              `
              UPDATE plans
              SET monthly_price_cents = COALESCE($1, monthly_price_cents),
                  whmcs_description = $2,
                  name = COALESCE(NULLIF($3, ''), name),
                  updated_at = NOW()
              WHERE plan_key = $4
              `,
              [liveCents || null, liveDescription, liveName, plan.plan_key],
            );

            return {
              ...plan,
              monthly_price_cents: liveCents || plan.monthly_price_cents,
              whmcs_description: liveDescription,
              name: liveName || plan.name,
            };
          }),
        );
      } catch (whmcsError) {
        console.error(
          "[WHMCS] Live pricing/description fetch failed, serving last-known values:",
          whmcsError.message,
        );
      }
    }

    res.json({ ok: true, plans });
  } catch (error) {
    console.error("Get public plans error:", error);
    res.status(500).json({ ok: false, message: "Failed to load plans" });
  }
});

app.get(
  "/api/subscription/current",
  authenticateAdmin,
  resolveOrganizationForRequest,
  async (req, res) => {
    try {
      const subscription = await ensureSubscriptionForOrganization(
        req.organization.id,
        req.organization.subscription_plan || "starter",
      );

      res.json({ ok: true, subscription });
    } catch (error) {
      console.error("Get current subscription error:", error);
      res.status(500).json({
        ok: false,
        message: "Failed to fetch subscription",
        error: error.message,
      });
    }
  },
);

app.post(
  "/api/subscription/checkout",
  authenticateAdmin,
  resolveOrganizationForRequest,
  async (req, res) => {
    try {
      const planKey = cleanOrgText(req.body.plan_key || "starter", 80);

      if (!whmcs.isWhmcsCheckoutReadyForPlan(planKey)) {
        return res.status(501).json({
          ok: false,
          message:
            "WHMCS checkout is not configured for this plan. Add WHMCS API keys and product IDs to the server .env.",
        });
      }

      const planResult = await pool.query(
        `SELECT * FROM plans WHERE plan_key = $1 AND is_active = TRUE AND plan_key <> 'internal' LIMIT 1`,
        [planKey],
      );

      const plan = planResult.rows[0];

      if (!plan) {
        return res
          .status(400)
          .json({ ok: false, message: "Invalid plan selected" });
      }

      // Nothing to create ahead of time here — WHMCS's own hosted cart
      // collects payment. The poller (pollWhmcsBilling) picks up the paid
      // order afterwards and matches it back to this organization by the
      // logged-in admin's email address.
      const checkoutUrl = whmcs.buildWhmcsCheckoutUrl(plan.plan_key, {
        email: req.admin.email,
        firstName: req.admin.name,
      });

      if (!checkoutUrl) {
        return res.status(501).json({
          ok: false,
          message: "WHMCS_CART_URL is not configured in the server .env.",
        });
      }

      res.json({ ok: true, checkout_url: checkoutUrl });
    } catch (error) {
      console.error("Create subscription checkout error:", error);
      res.status(500).json({
        ok: false,
        message: "Failed to build WHMCS checkout link",
        error: error.message,
      });
    }
  },
);

app.post(
  "/api/subscription/portal",
  authenticateAdmin,
  resolveOrganizationForRequest,
  async (req, res) => {
    try {
      const portalUrl = whmcs.getWhmcsClientAreaUrl();

      if (!portalUrl) {
        return res.status(501).json({
          ok: false,
          message:
            "WHMCS_CLIENT_AREA_URL is not configured in the server .env.",
        });
      }

      // WHMCS's client area handles its own login (by the client's email),
      // so we just send them there rather than minting a session ourselves.
      res.json({ ok: true, portal_url: portalUrl });
    } catch (error) {
      console.error("Create billing portal error:", error);
      res.status(500).json({
        ok: false,
        message: "Failed to open WHMCS client area",
        error: error.message,
      });
    }
  },
);

app.get(
  "/api/subscription/invoices",
  authenticateAdmin,
  resolveOrganizationForRequest,
  async (req, res) => {
    try {
      if (!whmcs.isWhmcsConfigured()) {
        return res.status(501).json({
          ok: false,
          message: "WHMCS invoice history is not configured.",
        });
      }

      const subscription = await ensureSubscriptionForOrganization(
        req.organization.id,
        req.organization.subscription_plan || "starter",
      );

      if (!subscription?.whmcs_client_id) {
        return res.json({ ok: true, invoices: [] });
      }

      const whmcsInvoices = await whmcs.getInvoices({
        userId: subscription.whmcs_client_id,
        limitNum: 12,
      });

      const clientAreaUrl = whmcs.getWhmcsClientAreaUrl();

      const formattedInvoices = (whmcsInvoices || []).map((invoice) => ({
        id: invoice.id,
        number: invoice.id,
        status: String(invoice.status || "").toLowerCase(),
        amount_paid:
          invoice.status === "Paid"
            ? Math.round(Number(invoice.total || 0) * 100)
            : 0,
        currency: "usd",
        hosted_invoice_url: clientAreaUrl
          ? `${clientAreaUrl.replace(/\/clientarea\.php$/, "")}/viewinvoice.php?id=${invoice.id}`
          : null,
        invoice_pdf: null,
        created: invoice.date || null,
        due_date: invoice.duedate || null,
        period_start: invoice.date || null,
        period_end: invoice.duedate || null,
      }));

      res.json({ ok: true, invoices: formattedInvoices });
    } catch (error) {
      console.error("Get subscription invoices error:", error);
      res.status(500).json({
        ok: false,
        message: "Failed to load invoice history",
        error: error.message,
      });
    }
  },
);

app.post(
  "/api/subscription/refresh",
  authenticateAdmin,
  resolveOrganizationForRequest,
  async (req, res) => {
    try {
      const subscription = await ensureSubscriptionForOrganization(
        req.organization.id,
        req.organization.subscription_plan || "starter",
      );

      if (whmcs.isWhmcsConfigured() && subscription?.whmcs_client_id) {
        const products = await whmcs.getClientsProducts(
          subscription.whmcs_client_id,
        );

        // If this client has more than one product, prefer whichever one
        // matches a known plan and is currently active.
        const match =
          products.find(
            (p) =>
              whmcs.getPlanKeyForWhmcsProductId(p.pid) &&
              String(p.status).toLowerCase() === "active",
          ) || products.find((p) => whmcs.getPlanKeyForWhmcsProductId(p.pid));

        if (match) {
          await syncWhmcsOrgSubscription(req.organization.id, {
            planKey: whmcs.getPlanKeyForWhmcsProductId(match.pid),
            whmcsStatus: match.status,
            whmcsClientId: subscription.whmcs_client_id,
            whmcsOrderId: match.orderid,
            nextDueDate: match.nextduedate,
          });
        }
      }

      const refreshed = await getOrganizationSubscriptionSummary(
        req.organization.id,
      );

      res.json({ ok: true, subscription: refreshed });
    } catch (error) {
      console.error("Refresh subscription error:", error);
      res.status(500).json({
        ok: false,
        message: "Failed to refresh subscription from WHMCS",
        error: error.message,
      });
    }
  },
);

app.put(
  "/api/organizations/:id/subscription",
  authenticateAdmin,
  requireRole("super_admin"),
  async (req, res) => {
    try {
      const { id } = req.params;
      const planKey = cleanOrgText(req.body.plan_key || "starter", 80);
      const status = ["active", "trialing", "past_due", "canceled"].includes(
        req.body.status,
      )
        ? req.body.status
        : "active";

      const planResult = await pool.query(
        `SELECT plan_key FROM plans WHERE plan_key = $1 AND is_active = TRUE LIMIT 1`,
        [planKey],
      );

      if (!planResult.rows[0]) {
        return res
          .status(400)
          .json({ ok: false, message: "Invalid plan selected" });
      }

      await pool.query(
        `
        UPDATE organizations
        SET subscription_plan = $1,
            updated_at = NOW()
        WHERE id = $2
        `,
        [planKey, id],
      );

      await pool.query(
        `
        INSERT INTO subscriptions (organization_id, plan_key, status)
        VALUES ($1, $2, $3)
        ON CONFLICT (organization_id)
        DO UPDATE SET
          plan_key = EXCLUDED.plan_key,
          status = EXCLUDED.status,
          updated_at = NOW()
        `,
        [id, planKey, status],
      );

      const subscription = await getOrganizationSubscriptionSummary(id);
      res.json({ ok: true, subscription });
    } catch (error) {
      console.error("Update subscription error:", error);
      res.status(500).json({
        ok: false,
        message: "Failed to update subscription",
        error: error.message,
      });
    }
  },
);

app.post("/api/public/signup", signupLimiter, async (req, res) => {
  try {
    const planKey = cleanOrgText(req.body.plan_key || "starter", 80);
    const organizationName = cleanOrgText(req.body.organization_name, 255);
    const clientName = cleanOrgText(req.body.client_name, 255);
    const clientEmail = cleanOrgText(req.body.client_email, 255).toLowerCase();
    const clientPassword = String(req.body.client_password || "");
    const primaryColor = cleanOrgText(req.body.primary_color || "#0d6efd", 40);
    const secondaryColor = cleanOrgText(
      req.body.secondary_color || "#fd9d00",
      40,
    );
    const donationUrl = cleanOrgText(req.body.donation_url, 1000) || null;

    if (!organizationName || !clientName || !clientEmail || !clientPassword) {
      return res.status(400).json({
        ok: false,
        message: "Organization name, name, email, and password are required",
      });
    }

    if (!clientEmail.includes("@")) {
      return res
        .status(400)
        .json({ ok: false, message: "Please enter a valid email address" });
    }

    if (clientPassword.length < 6) {
      return res
        .status(400)
        .json({ ok: false, message: "Password must be at least 6 characters" });
    }

    const planResult = await pool.query(
      `SELECT * FROM plans WHERE plan_key = $1 AND is_active = TRUE AND plan_key <> 'internal' LIMIT 1`,
      [planKey],
    );

    const plan = planResult.rows[0];

    if (!plan) {
      return res
        .status(400)
        .json({ ok: false, message: "Invalid plan selected" });
    }

    if (!whmcs.isWhmcsCheckoutReadyForPlan(plan.plan_key)) {
      return res.status(501).json({
        ok: false,
        message:
          "WHMCS checkout is not configured for this plan. Add WHMCS API keys and product IDs to the server .env.",
      });
    }

    const existingAdmin = await pool.query(
      `SELECT id FROM admins WHERE email = $1 LIMIT 1`,
      [clientEmail],
    );

    if (existingAdmin.rows[0]) {
      return res.status(409).json({
        ok: false,
        message:
          "An account with this email already exists. Please log in or use a different email.",
      });
    }

    const streamKey = await generateUniqueStreamKey(organizationName);
    const passwordHash = await bcrypt.hash(clientPassword, 10);

    // Unlike the old Stripe flow, there's no checkout_session_id to key
    // off of yet — the customer is about to leave our app entirely and
    // pay on WHMCS's hosted cart. pollWhmcsBilling() matches this row back
    // up afterwards by client_email + plan_key once WHMCS reports the
    // order active/paid, so we key this upsert on the email instead.
    await pool.query(
      `
      INSERT INTO pending_signups (
        checkout_session_id,
        plan_key,
        organization_name,
        client_name,
        client_email,
        password_hash,
        stream_key,
        primary_color,
        secondary_color,
        donation_url,
        status
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'pending')
      ON CONFLICT (checkout_session_id)
      DO UPDATE SET
        plan_key = EXCLUDED.plan_key,
        organization_name = EXCLUDED.organization_name,
        client_name = EXCLUDED.client_name,
        password_hash = EXCLUDED.password_hash,
        stream_key = EXCLUDED.stream_key,
        primary_color = EXCLUDED.primary_color,
        secondary_color = EXCLUDED.secondary_color,
        donation_url = EXCLUDED.donation_url,
        status = 'pending',
        updated_at = NOW()
      `,
      [
        // client_email doubles as our synthetic "checkout session id" key
        // since it's unique per pending signup and known before checkout.
        `whmcs:${clientEmail}`,
        plan.plan_key,
        organizationName,
        clientName,
        clientEmail,
        passwordHash,
        streamKey,
        primaryColor,
        secondaryColor,
        donationUrl,
      ],
    );

    const checkoutUrl = whmcs.buildWhmcsCheckoutUrl(plan.plan_key, {
      email: clientEmail,
      firstName: clientName,
    });

    res.json({
      ok: true,
      requires_checkout: true,
      checkout_url: checkoutUrl,
    });
  } catch (error) {
    console.error("Public signup error:", error);

    res.status(500).json({
      ok: false,
      message: "Failed to start paid signup checkout",
      error: error.message,
    });
  }
});

app.get("/api/organizations", authenticateAdmin, async (req, res) => {
  try {
    let organizations;

    if (req.admin.role === "super_admin") {
      const result = await pool.query(`
        SELECT o.*, 'owner' AS membership_role
        FROM organizations o
        ORDER BY o.created_at DESC
      `);

      organizations = result.rows;
    } else {
      organizations = await getAdminOrganizations(req.admin.id);
    }

    res.json({
      ok: true,
      organizations,
    });
  } catch (error) {
    console.error("Get organizations error:", error);

    res.status(500).json({
      ok: false,
      message: "Failed to fetch organizations",
      error: error.message,
    });
  }
});

app.post(
  "/api/organizations",
  authenticateAdmin,
  requireRole("super_admin"),
  async (req, res) => {
    try {
      const name = cleanOrgText(req.body.name, 255);
      const logoUrl = cleanOrgText(req.body.logo_url, 1000);
      const primaryColor = cleanOrgText(
        req.body.primary_color || "#0d6efd",
        40,
      );
      const customDomain = cleanOrgText(req.body.custom_domain, 255);
      const subscriptionPlan = cleanOrgText(
        req.body.subscription_plan || "starter",
        80,
      );

      if (!name) {
        return res.status(400).json({
          ok: false,
          message: "Organization name is required",
        });
      }

      const slug = await ensureUniqueOrganizationSlug(req.body.slug || name);

      const result = await pool.query(
        `
        INSERT INTO organizations (
          name,
          slug,
          logo_url,
          primary_color,
          custom_domain,
          subscription_plan,
          is_active
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING *
        `,
        [
          name,
          slug,
          logoUrl || null,
          primaryColor,
          customDomain || null,
          subscriptionPlan,
          req.body.is_active !== false,
        ],
      );

      const organization = result.rows[0];

      await pool.query(
        `
        INSERT INTO organization_settings (
          organization_id,
          watch_page_title,
          logo_url,
          primary_color,
          secondary_color,
          donation_url
        )
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (organization_id) DO NOTHING
        `,
        [
          organization.id,
          organization.name,
          organization.logo_url,
          organization.primary_color,
          req.body.secondary_color || "#fd9d00",
          req.body.donation_url || null,
        ],
      );

      await ensureSubscriptionForOrganization(
        organization.id,
        subscriptionPlan,
      );

      await pool.query(
        `
        INSERT INTO organization_users (organization_id, admin_id, role)
        VALUES ($1, $2, 'owner')
        ON CONFLICT (organization_id, admin_id) DO NOTHING
        `,
        [organization.id, req.admin.id],
      );

      await provisionBunnyZonesForNewOrganization(organization);

      res.json({
        ok: true,
        organization,
      });
    } catch (error) {
      console.error("Create organization error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to create organization",
        error: error.message,
      });
    }
  },
);

app.put(
  "/api/organizations/:id",
  authenticateAdmin,
  requireRole("super_admin"),
  async (req, res) => {
    try {
      const { id } = req.params;
      const name = cleanOrgText(req.body.name, 255);
      const logoUrl = cleanOrgText(req.body.logo_url, 1000);
      const primaryColor = cleanOrgText(
        req.body.primary_color || "#0d6efd",
        40,
      );
      const customDomain = cleanOrgText(req.body.custom_domain, 255);
      const subscriptionPlan = cleanOrgText(
        req.body.subscription_plan || "starter",
        80,
      );

      if (!name) {
        return res.status(400).json({
          ok: false,
          message: "Organization name is required",
        });
      }

      const slug = await ensureUniqueOrganizationSlug(
        req.body.slug || name,
        id,
      );

      const result = await pool.query(
        `
        UPDATE organizations
        SET name = $1,
            slug = $2,
            logo_url = $3,
            primary_color = $4,
            custom_domain = $5,
            subscription_plan = $6,
            is_active = $7,
            updated_at = NOW()
        WHERE id = $8
        RETURNING *
        `,
        [
          name,
          slug,
          logoUrl || null,
          primaryColor,
          customDomain || null,
          subscriptionPlan,
          req.body.is_active !== false,
          id,
        ],
      );

      if (!result.rows[0]) {
        return res.status(404).json({
          ok: false,
          message: "Organization not found",
        });
      }

      res.json({
        ok: true,
        organization: result.rows[0],
      });
    } catch (error) {
      console.error("Update organization error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to update organization",
        error: error.message,
      });
    }
  },
);

app.get(
  "/api/organizations/:id/settings",
  authenticateAdmin,
  async (req, res) => {
    try {
      const { id } = req.params;

      if (req.admin.role !== "super_admin") {
        const access = await pool.query(
          `
          SELECT id
          FROM organization_users
          WHERE organization_id = $1
            AND admin_id = $2
          LIMIT 1
          `,
          [id, req.admin.id],
        );

        if (!access.rows[0]) {
          return res.status(403).json({
            ok: false,
            message: "You do not have access to this organization",
          });
        }
      }

      const result = await pool.query(
        `
        SELECT *
        FROM organization_settings
        WHERE organization_id = $1
        LIMIT 1
        `,
        [id],
      );

      res.json({
        ok: true,
        settings: result.rows[0] || null,
      });
    } catch (error) {
      console.error("Get organization settings error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to fetch organization settings",
        error: error.message,
      });
    }
  },
);

app.put(
  "/api/organizations/:id/settings",
  authenticateAdmin,
  // ← REMOVED: requireRole("super_admin", "admin")
  async (req, res) => {
    try {
      const { id } = req.params;

      if (req.admin.role !== "super_admin") {
        const access = await pool.query(
          `
          SELECT id
          FROM organization_users
          WHERE organization_id = $1
            AND admin_id = $2
            AND role IN ('owner', 'admin', 'operator')
          `, // ← CHANGED: added 'operator' to the role list
          [id, req.admin.id],
        );

        if (!access.rows[0]) {
          return res.status(403).json({
            ok: false,
            message: "You do not have permission to update this organization",
          });
        }
      }

      // ↓ Everything below is UNCHANGED — copy from your existing file ↓
      const result = await pool.query(
        `
        INSERT INTO organization_settings (
          organization_id,
          watch_page_title,
          logo_url,
          primary_color,
          secondary_color,
          donation_url,
          custom_css
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (organization_id)
        DO UPDATE SET
          watch_page_title = EXCLUDED.watch_page_title,
          logo_url = EXCLUDED.logo_url,
          primary_color = EXCLUDED.primary_color,
          secondary_color = EXCLUDED.secondary_color,
          donation_url = EXCLUDED.donation_url,
          custom_css = EXCLUDED.custom_css,
          updated_at = NOW()
        RETURNING *
        `,
        [
          id,
          cleanOrgText(req.body.watch_page_title, 255),
          cleanOrgText(req.body.logo_url, 1000) || null,
          cleanOrgText(req.body.primary_color || "#0d6efd", 40),
          cleanOrgText(req.body.secondary_color || "#fd9d00", 40),
          cleanOrgText(req.body.donation_url, 1000) || null,
          req.body.custom_css || null,
        ],
      );

      res.json({
        ok: true,
        settings: result.rows[0],
      });
    } catch (error) {
      console.error("Update organization settings error:", error);
      res.status(500).json({
        ok: false,
        message: "Failed to update organization settings",
        error: error.message,
      });
    }
  },
);

// Lets a super_admin opt one specific org into (or out of) ABR/transcoding
// independent of its plan — for staging a rollout to a willing test org
// before widening transcoding_enabled to every Deluxe/Premium org at once.
// override: true/false pins it explicitly; null clears it back to
// following the plan's default (see transcoding_override column).
app.patch(
  "/api/admin/organizations/:id/transcoding-override",
  authenticateAdmin,
  async (req, res) => {
    if (req.admin.role !== "super_admin") {
      return res.status(403).json({
        ok: false,
        message: "Only super admins can set a per-org transcoding override",
      });
    }

    const { id } = req.params;
    const { override } = req.body;

    if (override !== null && typeof override !== "boolean") {
      return res.status(400).json({
        ok: false,
        message:
          "override must be true, false, or null (null clears it and falls back to the plan default)",
      });
    }

    try {
      const result = await pool.query(
        `UPDATE organizations SET transcoding_override = $1, updated_at = NOW()
         WHERE id = $2 RETURNING id, name, transcoding_override`,
        [override, id],
      );

      if (!result.rows[0]) {
        return res
          .status(404)
          .json({ ok: false, message: "Organization not found" });
      }

      console.log(
        `[ADMIN] transcoding_override for org ${result.rows[0].name} (${id}) set to ${
          override === null ? "null (plan default)" : override
        } by ${req.admin.email || req.admin.id}`,
      );

      res.json({ ok: true, organization: result.rows[0] });
    } catch (err) {
      res.status(500).json({ ok: false, message: err.message });
    }
  },
);

app.get("/api/organizations/:id/users", authenticateAdmin, async (req, res) => {
  try {
    const { id } = req.params;

    if (req.admin.role !== "super_admin") {
      const callerMembership = await pool.query(
        `SELECT role FROM organization_users WHERE organization_id = $1 AND admin_id = $2`,
        [id, req.admin.id],
      );
      if (!callerMembership.rows[0]) {
        return res.status(403).json({
          ok: false,
          message: "You do not have access to this organization's team",
        });
      }
    }

    const result = await pool.query(
      `
        SELECT
          ou.id,
          ou.organization_id,
          ou.admin_id,
          ou.role,
          ou.created_at,
          a.name,
          a.email,
          a.role AS global_role
        FROM organization_users ou
        JOIN admins a ON a.id = ou.admin_id
        WHERE ou.organization_id = $1
        ORDER BY ou.created_at DESC
        `,
      [id],
    );

    res.json({
      ok: true,
      users: result.rows,
    });
  } catch (error) {
    console.error("Get organization users error:", error);

    res.status(500).json({
      ok: false,
      message: "Failed to fetch organization users",
      error: error.message,
    });
  }
});

app.post(
  "/api/organizations/:id/users",
  authenticateAdmin,
  async (req, res) => {
    try {
      const { id } = req.params;
      const email = cleanOrgText(req.body.email, 255).toLowerCase();
      const name = cleanOrgText(req.body.name, 150);
      const role = ["owner", "admin", "operator", "viewer"].includes(
        req.body.role,
      )
        ? req.body.role
        : "operator";

      if (!email) {
        return res.status(400).json({
          ok: false,
          message: "User email is required",
        });
      }

      // Only super_admin, or an owner/admin OF THIS SPECIFIC organization,
      // may invite teammates into it.
      if (req.admin.role !== "super_admin") {
        const callerMembership = await pool.query(
          `SELECT role FROM organization_users WHERE organization_id = $1 AND admin_id = $2`,
          [id, req.admin.id],
        );
        if (!["owner", "admin"].includes(callerMembership.rows[0]?.role)) {
          return res.status(403).json({
            ok: false,
            message: "You do not have permission to manage this team",
          });
        }
      }

      const existingMembership = await pool.query(
        `SELECT ou.id
         FROM organization_users ou
         JOIN admins a ON a.id = ou.admin_id
         WHERE ou.organization_id = $1
           AND a.email = $2
         LIMIT 1`,
        [id, email],
      );

      // Role changes for an existing member do not consume another seat.
      if (!existingMembership.rows[0]) {
        const organizationResult = await pool.query(
          `SELECT subscription_plan FROM organizations WHERE id = $1 LIMIT 1`,
          [id],
        );

        const subscription = await ensureSubscriptionForOrganization(
          id,
          organizationResult.rows[0]?.subscription_plan || "starter",
        );

        if (
          subscription?.status &&
          !ACTIVE_SUBSCRIPTION_STATUSES.has(subscription.status)
        ) {
          return res.status(402).json({
            ok: false,
            code: "SUBSCRIPTION_INACTIVE",
            message: "This organization’s subscription is not active.",
            subscription,
          });
        }

        if (
          Number(subscription?.used_admins || 0) >=
          Number(subscription?.max_admins || 0)
        ) {
          return res.status(402).json({
            ok: false,
            code: "ADMIN_LIMIT_REACHED",
            message: `Your ${subscription?.plan_name || "current"} plan allows ${subscription?.max_admins || 0} organization user(s). Upgrade the plan to add another teammate.`,
            subscription,
          });
        }
      }

      let adminResult = await pool.query(
        `SELECT id FROM admins WHERE email = $1 LIMIT 1`,
        [email],
      );

      let tempPassword = null;

      if (!adminResult.rows[0]) {
        // No account exists yet for this email — create one, since
        // there's no email-invite flow. Temp password is returned once
        // so the inviter can share it directly with their teammate.
        tempPassword = Math.random().toString(36).slice(-10);
        const passwordHash = await bcrypt.hash(tempPassword, 10);

        adminResult = await pool.query(
          `
          INSERT INTO admins (name, email, password_hash, role)
          VALUES ($1, $2, $3, 'admin')
          RETURNING id
          `,
          [name || email.split("@")[0], email, passwordHash],
        );
      }

      const result = await pool.query(
        `
        INSERT INTO organization_users (organization_id, admin_id, role)
        VALUES ($1, $2, $3)
        ON CONFLICT (organization_id, admin_id)
        DO UPDATE SET role = EXCLUDED.role
        RETURNING *
        `,
        [id, adminResult.rows[0].id, role],
      );

      res.json({
        ok: true,
        membership: result.rows[0],
        temp_password: tempPassword,
      });
    } catch (error) {
      console.error("Add organization user error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to add organization user",
        error: error.message,
      });
    }
  },
);

app.delete(
  "/api/organizations/:organizationId/users/:adminId",
  authenticateAdmin,
  async (req, res) => {
    try {
      const { organizationId, adminId } = req.params;

      if (String(adminId) === String(req.admin.id)) {
        return res.status(400).json({
          ok: false,
          message: "You cannot remove yourself from the team",
        });
      }

      if (req.admin.role !== "super_admin") {
        const callerMembership = await pool.query(
          `SELECT role FROM organization_users WHERE organization_id = $1 AND admin_id = $2`,
          [organizationId, req.admin.id],
        );
        if (!["owner", "admin"].includes(callerMembership.rows[0]?.role)) {
          return res.status(403).json({
            ok: false,
            message: "You do not have permission to manage this team",
          });
        }
      }

      await pool.query(
        `
        DELETE FROM organization_users
        WHERE organization_id = $1
          AND admin_id = $2
        `,
        [organizationId, adminId],
      );

      res.json({
        ok: true,
        message: "Organization user removed",
      });
    } catch (error) {
      console.error("Remove organization user error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to remove organization user",
        error: error.message,
      });
    }
  },
);

/*
|--------------------------------------------------------------------------
| CLIENT ONBOARDING WIZARD
|--------------------------------------------------------------------------
*/

app.post(
  "/api/onboarding/client",
  authenticateAdmin,
  requireRole("super_admin"),
  async (req, res) => {
    const client = await pool.connect();

    try {
      const organizationName = cleanOrgText(req.body.organization_name, 255);
      const organizationSlugInput = cleanOrgText(
        req.body.organization_slug,
        255,
      );
      const logoUrl = cleanOrgText(req.body.logo_url, 1000);
      const primaryColor = cleanOrgText(
        req.body.primary_color || "#0d6efd",
        40,
      );
      const secondaryColor = cleanOrgText(
        req.body.secondary_color || "#fd9d00",
        40,
      );
      const customDomain = cleanOrgText(req.body.custom_domain, 255);
      const subscriptionPlan = cleanOrgText(
        req.body.subscription_plan || "starter",
        80,
      );

      const clientName = cleanOrgText(req.body.client_name, 255);
      const clientEmail = cleanOrgText(
        req.body.client_email,
        255,
      ).toLowerCase();
      const clientPassword = String(req.body.client_password || "");
      const clientGlobalRole = ["admin", "operator"].includes(
        req.body.client_global_role,
      )
        ? req.body.client_global_role
        : "operator";
      const organizationRole = [
        "owner",
        "admin",
        "operator",
        "viewer",
      ].includes(req.body.organization_role)
        ? req.body.organization_role
        : "operator";

      const channelName = cleanOrgText(req.body.channel_name, 255);
      const channelDescription = cleanOrgText(
        req.body.channel_description,
        1000,
      );

      if (!organizationName || !clientName || !clientEmail) {
        return res.status(400).json({
          ok: false,
          message:
            "Organization name, client name, and client email are required",
        });
      }

      if (!clientEmail.includes("@")) {
        return res.status(400).json({
          ok: false,
          message: "Please enter a valid client email address",
        });
      }

      await client.query("BEGIN");

      const slug = await ensureUniqueOrganizationSlug(
        organizationSlugInput || organizationName,
      );

      const organizationResult = await client.query(
        `
        INSERT INTO organizations (
          name,
          slug,
          logo_url,
          primary_color,
          custom_domain,
          subscription_plan,
          is_active
        )
        VALUES ($1, $2, $3, $4, $5, $6, TRUE)
        RETURNING *
        `,
        [
          organizationName,
          slug,
          logoUrl || null,
          primaryColor,
          customDomain || null,
          subscriptionPlan,
        ],
      );

      const organization = organizationResult.rows[0];

      const settingsResult = await client.query(
        `
        INSERT INTO organization_settings (
          organization_id,
          watch_page_title,
          logo_url,
          primary_color,
          secondary_color,
          donation_url
        )
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (organization_id)
        DO UPDATE SET
          watch_page_title = EXCLUDED.watch_page_title,
          logo_url = EXCLUDED.logo_url,
          primary_color = EXCLUDED.primary_color,
          secondary_color = EXCLUDED.secondary_color,
          donation_url = EXCLUDED.donation_url,
          updated_at = NOW()
        RETURNING *
        `,
        [
          organization.id,
          req.body.watch_page_title || organization.name,
          logoUrl || null,
          primaryColor,
          secondaryColor,
          cleanOrgText(req.body.donation_url, 1000) || null,
        ],
      );

      await client.query(
        `
        INSERT INTO subscriptions (organization_id, plan_key, status)
        VALUES ($1, $2, 'active')
        ON CONFLICT (organization_id) DO UPDATE SET
          plan_key = EXCLUDED.plan_key,
          status = EXCLUDED.status,
          updated_at = NOW()
        `,
        [organization.id, subscriptionPlan],
      );

      const existingAdminResult = await client.query(
        `
        SELECT id, name, email, role
        FROM admins
        WHERE email = $1
        LIMIT 1
        `,
        [clientEmail],
      );

      let clientAdmin = existingAdminResult.rows[0];
      let adminCreated = false;

      if (!clientAdmin) {
        if (!clientPassword || clientPassword.length < 6) {
          await client.query("ROLLBACK");

          return res.status(400).json({
            ok: false,
            message:
              "Client password is required and must be at least 6 characters",
          });
        }

        const passwordHash = await bcrypt.hash(clientPassword, 10);

        const adminResult = await client.query(
          `
          INSERT INTO admins (name, email, password_hash, role)
          VALUES ($1, $2, $3, $4)
          RETURNING id, name, email, role, created_at
          `,
          [clientName, clientEmail, passwordHash, clientGlobalRole],
        );

        clientAdmin = adminResult.rows[0];
        adminCreated = true;
      }

      const membershipResult = await client.query(
        `
        INSERT INTO organization_users (organization_id, admin_id, role)
        VALUES ($1, $2, $3)
        ON CONFLICT (organization_id, admin_id)
        DO UPDATE SET role = EXCLUDED.role
        RETURNING *
        `,
        [organization.id, clientAdmin.id, organizationRole],
      );

      const ownerMembershipResult = await client.query(
        `
        INSERT INTO organization_users (organization_id, admin_id, role)
        VALUES ($1, $2, 'owner')
        ON CONFLICT (organization_id, admin_id)
        DO UPDATE SET role = 'owner'
        RETURNING *
        `,
        [organization.id, req.admin.id],
      );

      const streamKey = await generateUniqueStreamKey(
        channelName || organizationName,
      );

      const channelResult = await client.query(
        `
        INSERT INTO channels (
          organization_id,
          name,
          stream_key,
          description
        )
        VALUES ($1, $2, $3, $4)
        RETURNING *
        `,
        [
          organization.id,
          channelName || `${organization.name} Main`,
          streamKey,
          channelDescription || "Primary livestream channel",
        ],
      );

      await client.query("COMMIT");

      await provisionBunnyZonesForNewOrganization(organization);

      const watchUrl = `${CLIENT_URL.replace(/\/$/, "")}/watch/${streamKey}`;
      const playbackUrl = `${HLS_BASE_URL.replace(/\/$/, "")}/live/${streamKey}.m3u8`;

      res.json({
        ok: true,
        message: "Client onboarded successfully",
        organization,
        settings: settingsResult.rows[0],
        admin: clientAdmin,
        adminCreated,
        membership: membershipResult.rows[0],
        ownerMembership: ownerMembershipResult.rows[0],
        channel: channelResult.rows[0],
        links: {
          watch_url: watchUrl,
          playback_url: playbackUrl,
          rtmp_server: "rtmp://localhost/live",
          stream_key: streamKey,
          srt_url: `srt://localhost:10080?streamid=#!::r=live/${streamKey},m=publish`,
        },
      });
    } catch (error) {
      await client.query("ROLLBACK");
      console.error("Client onboarding error:", error);

      if (error.code === "23505") {
        return res.status(409).json({
          ok: false,
          message:
            "A record already exists with the same organization slug, email, or stream key",
          error: error.message,
        });
      }

      res.status(500).json({
        ok: false,
        message: "Failed to onboard client",
        error: error.message,
      });
    } finally {
      client.release();
    }
  },
);

/*
|--------------------------------------------------------------------------
| ADMINS / USERS MANAGEMENT
|--------------------------------------------------------------------------
*/

const ADMIN_ROLES = ["super_admin", "admin", "operator"];

const normalizeAdminRole = (role) => {
  if (!role) return "admin";
  return ADMIN_ROLES.includes(role) ? role : null;
};

const getSuperAdminCount = async () => {
  const result = await pool.query(
    `
    SELECT COUNT(*)::int AS count
    FROM admins
    WHERE role = 'super_admin'
    `,
  );

  return result.rows[0]?.count || 0;
};

// ══════════════════════════════════════════
// SUPER ADMIN DASHBOARD — cross-organization overview
// super_admin only. Not scoped to a single organization on purpose.
// ══════════════════════════════════════════
app.get(
  "/api/admin/overview",
  authenticateAdmin,
  requireRole("super_admin"),
  async (req, res) => {
    try {
      // Run the independent summary reads concurrently. Each query retries one
      // transient Railway proxy failure before the request is allowed to fail.
      const [
        orgCounts,
        channelCount,
        adminsByRole,
        recordingsTotals,
        channelsResult,
        recentRecordings,
      ] = await Promise.all([
        queryWithRetry(
          `SELECT COUNT(*)::int AS total,
                    COUNT(*) FILTER (WHERE is_active)::int AS active
             FROM organizations`,
        ),
        queryWithRetry(`SELECT COUNT(*)::int AS total FROM channels`),
        queryWithRetry(
          `SELECT role, COUNT(*)::int AS count FROM admins GROUP BY role`,
        ),
        queryWithRetry(
          `SELECT COUNT(*)::int AS total_recordings,
                    COALESCE(SUM(file_size_bytes), 0)::bigint AS total_bytes
             FROM recordings`,
        ),
        queryWithRetry(
          `SELECT c.stream_key, c.name AS channel_name, c.live_started_at,
                    o.id AS organization_id, o.name AS organization_name
             FROM channels c
             JOIN organizations o ON o.id = c.organization_id`,
        ),
        queryWithRetry(
          `SELECT r.id, r.filename, r.mp4_filename, r.created_at,
                    r.file_size_bytes, c.name AS channel_name,
                    o.name AS organization_name
             FROM recordings r
             LEFT JOIN channels c ON c.id = r.channel_id
             LEFT JOIN organizations o ON o.id = r.organization_id
             ORDER BY r.created_at DESC
             LIMIT 10`,
        ),
      ]);

      const channelByStreamKey = new Map(
        channelsResult.rows.map((row) => [String(row.stream_key), row]),
      );

      let liveStreams = [];
      let srsAvailable = true;

      try {
        const srsResponse = await fetch(`${SRS_API_URL}/api/v1/streams`, {
          signal: AbortSignal.timeout(5000),
        });
        if (!srsResponse.ok) {
          throw new Error(`SRS responded ${srsResponse.status}`);
        }
        const srsData = await srsResponse.json();

        liveStreams = await Promise.all(
          (srsData.streams || [])
            .filter((stream) => channelByStreamKey.has(stream.name))
            .map(async (stream) => {
              const channel = channelByStreamKey.get(stream.name);
              const viewerMetrics = await getViewerMetricsForStream(
                stream.name,
                null,
              );
              const uptimeSeconds =
                stream.publish?.active && channel.live_started_at
                  ? Math.max(
                      0,
                      Math.floor(
                        (Date.now() -
                          new Date(channel.live_started_at).getTime()) /
                          1000,
                      ),
                    )
                  : 0;

              return {
                stream_key: stream.name,
                channel_name: channel.channel_name,
                organization_id: channel.organization_id,
                organization_name: channel.organization_name,
                active: Boolean(stream.publish?.active),
                viewers: viewerMetrics.active_viewers,
                kbps: Number(stream.kbps?.recv_30s || 0),
                uptime_seconds: uptimeSeconds,
              };
            }),
        );
      } catch (srsError) {
        console.warn(
          "Super admin overview: SRS temporarily unavailable:",
          srsError.message,
        );
        srsAvailable = false;
      }

      const activeLiveStreams = liveStreams.filter((stream) => stream.active);

      return res.json({
        ok: true,
        srs_available: srsAvailable,
        organizations: orgCounts.rows[0],
        channels: { total: channelCount.rows[0].total },
        admins_by_role: adminsByRole.rows,
        storage: {
          total_recordings: recordingsTotals.rows[0].total_recordings,
          total_bytes: Number(recordingsTotals.rows[0].total_bytes),
        },
        live_streams: activeLiveStreams,
        totals: {
          active_streams: activeLiveStreams.length,
          live_viewers: activeLiveStreams.reduce(
            (sum, stream) => sum + Number(stream.viewers || 0),
            0,
          ),
          incoming_kbps: activeLiveStreams.reduce(
            (sum, stream) => sum + Number(stream.kbps || 0),
            0,
          ),
        },
        recent_recordings: recentRecordings.rows,
      });
    } catch (error) {
      console.error("Get admin overview error after retry:", error);

      return res.status(503).json({
        ok: false,
        message:
          "The platform overview is temporarily unavailable. Please try again shortly.",
        error: error.message,
      });
    }
  },
);

// ══════════════════════════════════════════
// SUPER ADMIN DASHBOARD — recent server error log
// ══════════════════════════════════════════
app.get(
  "/api/admin/error-log",
  authenticateAdmin,
  requireRole("super_admin"),
  (req, res) => {
    const filterOrgId = req.query.organization_id
      ? String(req.query.organization_id)
      : null;

    const filtered = filterOrgId
      ? recentErrorLog.filter((e) => String(e.organization_id) === filterOrgId)
      : recentErrorLog;

    res.json({
      ok: true,
      errors: [...filtered].reverse(),
      total_unfiltered: recentErrorLog.length,
    });
  },
);

// ══════════════════════════════════════════
// SUPER ADMIN DASHBOARD — server health
// Real data from Node's os module + a disk usage check.
// ══════════════════════════════════════════
function getServerStatusSnapshot() {
  const totalMem = os.totalmem();
  const freeMem = os.freemem();
  const usedMem = totalMem - freeMem;
  const loadAvg = os.loadavg();
  const cpuCount = os.cpus().length;

  let disk = null;
  try {
    const dfOutput = execSync("df -k / | tail -1").toString().trim();
    const parts = dfOutput.split(/\s+/);
    const totalKb = Number(parts[1] || 0);
    const usedKb = Number(parts[2] || 0);
    const availKb = Number(parts[3] || 0);
    disk = {
      total_bytes: totalKb * 1024,
      used_bytes: usedKb * 1024,
      available_bytes: availKb * 1024,
      used_percent: totalKb ? Math.round((usedKb / totalKb) * 100) : null,
    };
  } catch (diskError) {
    console.error("Server status: disk check failed:", diskError.message);
  }

  return {
    uptime_seconds: Math.floor(os.uptime()),
    process_uptime_seconds: Math.floor(process.uptime()),
    cpu_count: cpuCount,
    load_avg: { "1m": loadAvg[0], "5m": loadAvg[1], "15m": loadAvg[2] },
    memory: {
      total_bytes: totalMem,
      used_bytes: usedMem,
      free_bytes: freeMem,
      used_percent: Math.round((usedMem / totalMem) * 100),
    },
    disk,
  };
}

app.get(
  "/api/admin/server-status",
  authenticateAdmin,
  requireRole("super_admin"),
  (req, res) => {
    try {
      res.json({ ok: true, ...getServerStatusSnapshot() });
    } catch (error) {
      console.error("Get server status error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to load server status",
        error: error.message,
      });
    }
  },
);

// ══════════════════════════════════════════
// SUPER ADMIN DASHBOARD — trend history (Platform Overview graphs)
// A periodic snapshot job, separate from the live getServerStatusSnapshot()
// above — that one only ever answers "right now"; this is what lets the
// dashboard show a trend line instead of a single current-moment gauge.
// ══════════════════════════════════════════
async function ensureServerMetricsHistoryTable(pool) {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS server_metrics_history (
      id SERIAL PRIMARY KEY,
      recorded_at TIMESTAMPTZ DEFAULT NOW(),
      cpu_percent INTEGER,
      mem_used_percent INTEGER,
      disk_used_percent INTEGER,
      live_stream_count INTEGER,
      live_viewer_count INTEGER,
      incoming_kbps INTEGER
    )
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_server_metrics_history_recorded_at
    ON server_metrics_history (recorded_at)
  `);
}

const SERVER_METRICS_COLLECTION_INTERVAL_MS = 5 * 60 * 1000; // 5 minutes
const SERVER_METRICS_RETENTION_DAYS = 30;

async function collectServerMetricsSnapshot() {
  try {
    const status = getServerStatusSnapshot();
    // Same formula the dashboard frontend already uses for the CPU gauge —
    // computed here too so the stored history matches what a viewer sees
    // live, rather than drifting from load-average alone.
    const cpuPercent = Math.min(
      100,
      Math.round(
        ((status.load_avg?.["1m"] || 0) / (status.cpu_count || 1)) * 100,
      ),
    );

    // Live stream count + incoming bitrate, straight from SRS — mirrors
    // getActiveStreamsSnapshot()'s approach (channel-scoped, not raw SRS
    // noise) without paying for a full per-stream viewer-metrics lookup on
    // every 5-minute tick.
    let liveStreamCount = 0;
    let incomingKbps = 0;
    try {
      const channelsResult = await pool.query(
        `SELECT stream_key FROM channels`,
      );
      const knownStreamKeys = new Set(
        channelsResult.rows.map((row) => String(row.stream_key)),
      );
      const srsResponse = await fetch(`${SRS_API_URL}/api/v1/streams`, {
        signal: AbortSignal.timeout(5000),
      });
      if (srsResponse.ok) {
        const srsData = await srsResponse.json();
        const active = (srsData.streams || []).filter(
          (s) => s.publish?.active && knownStreamKeys.has(s.name),
        );
        liveStreamCount = active.length;
        incomingKbps = active.reduce(
          (sum, s) => sum + Number(s.kbps?.recv_30s || 0),
          0,
        );
      }
    } catch (srsError) {
      console.warn(
        "Server metrics snapshot: SRS unavailable:",
        srsError.message,
      );
    }

    // Platform-wide concurrent viewers — same 45-second "active" window
    // used everywhere else viewer counts are computed, so this stays
    // consistent with what the rest of the dashboard already shows.
    let liveViewerCount = 0;
    try {
      const viewerResult = await pool.query(
        `SELECT COUNT(*)::int AS count
         FROM viewer_sessions
         WHERE ended_at IS NULL AND last_seen_at >= NOW() - INTERVAL '45 seconds'`,
      );
      liveViewerCount = viewerResult.rows[0]?.count || 0;
    } catch (viewerError) {
      console.warn(
        "Server metrics snapshot: viewer count query failed:",
        viewerError.message,
      );
    }

    await pool.query(
      `INSERT INTO server_metrics_history
         (cpu_percent, mem_used_percent, disk_used_percent, live_stream_count, live_viewer_count, incoming_kbps)
       VALUES ($1, $2, $3, $4, $5, $6)`,
      [
        cpuPercent,
        status.memory?.used_percent ?? null,
        status.disk?.used_percent ?? null,
        liveStreamCount,
        liveViewerCount,
        Math.round(incomingKbps),
      ],
    );

    // Prune in the same tick rather than a separate cron — cheap at this
    // volume (one row per 5 minutes) and keeps retention self-contained.
    await pool.query(
      `DELETE FROM server_metrics_history
       WHERE recorded_at < NOW() - INTERVAL '${SERVER_METRICS_RETENTION_DAYS} days'`,
    );
  } catch (error) {
    console.error("collectServerMetricsSnapshot failed:", error.message);
  }
}

app.get(
  "/api/admin/server-metrics-history",
  authenticateAdmin,
  requireRole("super_admin"),
  async (req, res) => {
    try {
      const rangeParam = String(req.query.range || "24h");
      const rangeInterval =
        rangeParam === "7d"
          ? "7 days"
          : rangeParam === "30d"
            ? "30 days"
            : "24 hours";

      const result = await pool.query(
        `SELECT recorded_at, cpu_percent, mem_used_percent, disk_used_percent,
                live_stream_count, live_viewer_count, incoming_kbps
         FROM server_metrics_history
         WHERE recorded_at >= NOW() - INTERVAL '${rangeInterval}'
         ORDER BY recorded_at ASC`,
      );

      res.json({ ok: true, range: rangeParam, points: result.rows });
    } catch (error) {
      console.error("Get server metrics history error:", error);
      res.status(500).json({
        ok: false,
        message: "Failed to load server metrics history",
        error: error.message,
      });
    }
  },
);

// ══════════════════════════════════════════
// FEATURE FLAGS
// Read: any authenticated user, since the app's own UI needs to know
// current flag states to decide what to render.
// Write: super_admin only.
// ══════════════════════════════════════════
app.get("/api/feature-flags", authenticateAdmin, async (req, res) => {
  try {
    const flags = await getFeatureFlags();
    res.json({ ok: true, flags });
  } catch (error) {
    console.error("Get feature flags error:", error);
    res.status(500).json({
      ok: false,
      message: "Failed to load feature flags",
      error: error.message,
    });
  }
});

app.get(
  "/api/admin/feature-flags",
  authenticateAdmin,
  requireRole("super_admin"),
  async (req, res) => {
    try {
      const flags = await getFeatureFlags();
      const withMeta = Object.entries(FEATURE_FLAG_DEFAULTS).map(
        ([key, def]) => ({
          key,
          enabled: flags[key],
          description: def.description,
        }),
      );
      res.json({ ok: true, flags: withMeta });
    } catch (error) {
      console.error("Get admin feature flags error:", error);
      res.status(500).json({
        ok: false,
        message: "Failed to load feature flags",
        error: error.message,
      });
    }
  },
);

app.put(
  "/api/admin/feature-flags/:key",
  authenticateAdmin,
  requireRole("super_admin"),
  async (req, res) => {
    try {
      const { key } = req.params;
      const enabled = Boolean(req.body.enabled);

      if (!FEATURE_FLAG_DEFAULTS[key]) {
        return res.status(404).json({
          ok: false,
          message: "Unknown feature flag",
        });
      }

      await pool.query(
        `
        INSERT INTO feature_flags (key, enabled, description)
        VALUES ($1, $2, $3)
        ON CONFLICT (key) DO UPDATE
        SET enabled = EXCLUDED.enabled, updated_at = NOW()
        `,
        [key, enabled, FEATURE_FLAG_DEFAULTS[key].description],
      );

      res.json({ ok: true, key, enabled });
    } catch (error) {
      console.error("Update feature flag error:", error);
      res.status(500).json({
        ok: false,
        message: "Failed to update feature flag",
        error: error.message,
      });
    }
  },
);

// ══════════════════════════════════════════
// INTEGRATION HEALTH — live checks, not just static config display
// ══════════════════════════════════════════
async function getIntegrationHealthSnapshot() {
  const results = {};

  // SRS
  try {
    const started = Date.now();
    const srsRes = await fetch(`${SRS_API_URL}/api/v1/streams`, {
      signal: AbortSignal.timeout(5000),
    });
    results.srs = {
      ok: srsRes.ok,
      status: srsRes.ok ? "Online" : `HTTP ${srsRes.status}`,
      latency_ms: Date.now() - started,
    };
  } catch (err) {
    results.srs = { ok: false, status: err.message, latency_ms: null };
  }

  // Bunny Storage
  if (!BUNNY_STORAGE_API_KEY || !BUNNY_STORAGE_HOSTNAME) {
    results.bunny = { ok: false, status: "Not configured", latency_ms: null };
  } else {
    try {
      const started = Date.now();
      const bunnyRes = await fetch(
        `https://${BUNNY_STORAGE_HOSTNAME}/${BUNNY_STORAGE_ZONE}/`,
        {
          method: "GET",
          headers: { AccessKey: BUNNY_STORAGE_API_KEY },
          signal: AbortSignal.timeout(5000),
        },
      );
      // Bunny returns 200/401/404 depending on zone contents — any
      // response (not a network failure) means the credentials and
      // hostname are at least reachable.
      results.bunny = {
        ok: bunnyRes.status !== 401 && bunnyRes.status !== 403,
        status:
          bunnyRes.status === 401 || bunnyRes.status === 403
            ? "Auth failed — check API key"
            : "Online",
        latency_ms: Date.now() - started,
      };
    } catch (err) {
      results.bunny = { ok: false, status: err.message, latency_ms: null };
    }
  }

  // WHMCS
  if (!whmcs.isWhmcsConfigured()) {
    results.whmcs = {
      ok: false,
      status: "Not configured",
      latency_ms: null,
    };
  } else {
    try {
      const started = Date.now();
      await whmcs.callWhmcsApi("GetOrders", { limitnum: 1 });
      results.whmcs = {
        ok: true,
        status: "Online",
        latency_ms: Date.now() - started,
      };
    } catch (err) {
      results.whmcs = { ok: false, status: err.message, latency_ms: null };
    }
  }

  // Database (if we got this far, pool is working, but confirm with a
  // trivial query so this stays consistent with the others)
  try {
    const started = Date.now();
    await pool.query("SELECT 1");
    results.database = {
      ok: true,
      status: "Online",
      latency_ms: Date.now() - started,
    };
  } catch (err) {
    results.database = { ok: false, status: err.message, latency_ms: null };
  }

  return results;
}

app.get(
  "/api/admin/integration-health",
  authenticateAdmin,
  requireRole("super_admin"),
  async (req, res) => {
    const results = await getIntegrationHealthSnapshot();
    res.json({ ok: true, integrations: results });
  },
);

// Org-scoped: an org's own admins see their own plan-limit alerts
// (bitrate warnings/kicks, and any future plan-enforcement notices) on
// their dashboard.
app.get(
  "/api/organization/alerts",
  authenticateAdmin,
  resolveOrganizationForRequest,
  async (req, res) => {
    try {
      const result = await pool.query(
        `
        SELECT * FROM plan_alerts
        WHERE organization_id = $1 AND acknowledged = FALSE
        ORDER BY created_at DESC
        LIMIT 20
        `,
        [req.organization.id],
      );

      res.json({ ok: true, alerts: result.rows });
    } catch (error) {
      console.error("Get organization alerts error:", error);
      res.status(500).json({ ok: false, message: "Failed to load alerts" });
    }
  },
);

app.post(
  "/api/organization/alerts/:id/acknowledge",
  authenticateAdmin,
  resolveOrganizationForRequest,
  async (req, res) => {
    try {
      await pool.query(
        `
        UPDATE plan_alerts
        SET acknowledged = TRUE, acknowledged_at = NOW()
        WHERE id = $1 AND organization_id = $2
        `,
        [req.params.id, req.organization.id],
      );

      res.json({ ok: true });
    } catch (error) {
      console.error("Acknowledge alert error:", error);
      res
        .status(500)
        .json({ ok: false, message: "Failed to acknowledge alert" });
    }
  },
);

// Platform-wide: super_admin sees recent alerts across every organization,
// for the Super Admin Dashboard.
app.get(
  "/api/admin/plan-alerts",
  authenticateAdmin,
  requireRole("super_admin"),
  async (req, res) => {
    try {
      const result = await pool.query(
        `
        SELECT pa.*, o.name AS organization_name
        FROM plan_alerts pa
        JOIN organizations o ON o.id = pa.organization_id
        ORDER BY pa.created_at DESC
        LIMIT 50
        `,
      );

      res.json({ ok: true, alerts: result.rows });
    } catch (error) {
      console.error("Get admin plan alerts error:", error);
      res
        .status(500)
        .json({ ok: false, message: "Failed to load plan alerts" });
    }
  },
);

// Storage overage report for billing — per-org peak overage within a date
// range, for whoever's cutting invoices to reference when billing for
// storage used past an org's plan limit. This is deliberately a report,
// not an automated charge: no WHMCS write happens here, since usage-based
// billing wasn't set up as part of the storage policy (warn + allow
// overage + bill manually later). `days` defaults to 30 (a billing-cycle
// window); peak (not sum) overage per org is reported, since overage_bytes
// on each alert row already reflects usage AT THAT MOMENT, not a delta —
// summing them would double-count the same overage across multiple
// cooldown-spaced alerts.
app.get(
  "/api/admin/storage-overage-report",
  authenticateAdmin,
  requireRole("super_admin"),
  async (req, res) => {
    try {
      const days = Math.max(1, Number(req.query.days) || 30);

      const result = await pool.query(
        `
        SELECT
          pa.organization_id,
          o.name AS organization_name,
          MAX(pa.overage_bytes) AS peak_overage_bytes,
          COUNT(*) AS alert_count,
          MIN(pa.created_at) AS first_alert_at,
          MAX(pa.created_at) AS last_alert_at,
          BOOL_OR(pa.email_sent) AS was_notified
        FROM plan_alerts pa
        JOIN organizations o ON o.id = pa.organization_id
        WHERE pa.alert_type = 'storage_quota_warning'
          AND pa.created_at > NOW() - ($1::text || ' days')::interval
        GROUP BY pa.organization_id, o.name
        ORDER BY peak_overage_bytes DESC
        `,
        [days],
      );

      res.json({
        ok: true,
        days,
        organizations: result.rows.map((row) => ({
          organization_id: row.organization_id,
          organization_name: row.organization_name,
          peak_overage_gb: Number(
            (Number(row.peak_overage_bytes || 0) / 1024 ** 3).toFixed(2),
          ),
          alert_count: Number(row.alert_count),
          first_alert_at: row.first_alert_at,
          last_alert_at: row.last_alert_at,
          was_notified: row.was_notified,
        })),
      });
    } catch (error) {
      console.error("Storage overage report error:", error);
      res
        .status(500)
        .json({ ok: false, message: "Failed to load storage overage report" });
    }
  },
);

app.get(
  "/api/admins",
  authenticateAdmin,
  requireRole("super_admin"),
  async (req, res) => {
    try {
      const result = await pool.query(
        `
        SELECT id, name, email, role, created_at
        FROM admins
        ORDER BY created_at DESC
        `,
      );

      res.json({
        ok: true,
        admins: result.rows,
      });
    } catch (error) {
      console.error("Get admins error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to fetch admins",
        error: error.message,
      });
    }
  },
);

app.post(
  "/api/admins",
  authenticateAdmin,
  requireRole("super_admin"),
  async (req, res) => {
    try {
      const { name, email, password, role } = req.body;
      const normalizedRole = normalizeAdminRole(role);

      if (!name || !email || !password) {
        return res.status(400).json({
          ok: false,
          message: "Name, email, and password are required",
        });
      }

      if (!normalizedRole) {
        return res.status(400).json({
          ok: false,
          message: "Invalid role selected",
        });
      }

      const passwordHash = await bcrypt.hash(password, 10);

      const result = await pool.query(
        `
        INSERT INTO admins (name, email, password_hash, role)
        VALUES ($1, $2, $3, $4)
        RETURNING id, name, email, role, created_at
        `,
        [name.trim(), email.trim().toLowerCase(), passwordHash, normalizedRole],
      );

      res.json({
        ok: true,
        admin: result.rows[0],
      });
    } catch (error) {
      console.error("Create admin error:", error);

      if (error.code === "23505") {
        return res.status(409).json({
          ok: false,
          message: "An admin with this email already exists",
        });
      }

      res.status(500).json({
        ok: false,
        message: "Failed to create admin",
        error: error.message,
      });
    }
  },
);

app.put("/api/admins/me/profile", authenticateAdmin, async (req, res) => {
  try {
    const { name, email, password } = req.body;
    const adminId = req.admin.id;

    if (!name || !email) {
      return res.status(400).json({
        ok: false,
        message: "Name and email are required",
      });
    }

    let result;

    if (password) {
      if (password.length < 6) {
        return res.status(400).json({
          ok: false,
          message: "Password must be at least 6 characters",
        });
      }

      const passwordHash = await bcrypt.hash(password, 10);

      result = await pool.query(
        `
          UPDATE admins
          SET name = $1,
              email = $2,
              password_hash = $3
          WHERE id = $4
          RETURNING id, name, email, role, created_at
          `,
        [name.trim(), email.trim().toLowerCase(), passwordHash, adminId],
      );
    } else {
      result = await pool.query(
        `
          UPDATE admins
          SET name = $1,
              email = $2
          WHERE id = $3
          RETURNING id, name, email, role, created_at
          `,
        [name.trim(), email.trim().toLowerCase(), adminId],
      );
    }

    res.json({
      ok: true,
      admin: result.rows[0],
    });
  } catch (error) {
    console.error("Self-update profile error:", error);

    if (error.code === "23505") {
      return res.status(409).json({
        ok: false,
        message: "An admin with this email already exists",
      });
    }

    res.status(500).json({
      ok: false,
      message: "Failed to update profile",
      error: error.message,
    });
  }
});

// ══════════════════════════════════════════
// NOTIFICATION PREFERENCES
// Self-scoped — every admin manages their own. NOTE: this stores the
// preference only. There is no email-sending integration wired up yet
// (no SMTP/SendGrid/etc. configured on this server), so toggling these
// on does not currently trigger any emails — it just records intent
// for when that integration is added.
// ══════════════════════════════════════════
app.get(
  "/api/admins/me/notification-preferences",
  authenticateAdmin,
  async (req, res) => {
    try {
      const result = await pool.query(
        `SELECT * FROM notification_preferences WHERE admin_id = $1`,
        [req.admin.id],
      );

      const prefs = result.rows[0] || {
        stream_live: true,
        chat_needs_moderation: false,
        recording_processed: false,
      };

      res.json({ ok: true, preferences: prefs });
    } catch (error) {
      console.error("Get notification preferences error:", error);
      res.status(500).json({
        ok: false,
        message: "Failed to load notification preferences",
        error: error.message,
      });
    }
  },
);

app.put(
  "/api/admins/me/notification-preferences",
  authenticateAdmin,
  async (req, res) => {
    try {
      const streamLive = Boolean(req.body.stream_live);
      const chatNeedsModeration = Boolean(req.body.chat_needs_moderation);
      const recordingProcessed = Boolean(req.body.recording_processed);

      const result = await pool.query(
        `
        INSERT INTO notification_preferences
          (admin_id, stream_live, chat_needs_moderation, recording_processed)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (admin_id) DO UPDATE
        SET stream_live = EXCLUDED.stream_live,
            chat_needs_moderation = EXCLUDED.chat_needs_moderation,
            recording_processed = EXCLUDED.recording_processed,
            updated_at = NOW()
        RETURNING *
        `,
        [req.admin.id, streamLive, chatNeedsModeration, recordingProcessed],
      );

      res.json({ ok: true, preferences: result.rows[0] });
    } catch (error) {
      console.error("Update notification preferences error:", error);
      res.status(500).json({
        ok: false,
        message: "Failed to update notification preferences",
        error: error.message,
      });
    }
  },
);

app.put(
  "/api/admins/:id",
  authenticateAdmin,
  requireRole("super_admin"),
  async (req, res) => {
    try {
      const { id } = req.params;
      const { name, email, password, role } = req.body;
      const normalizedRole = normalizeAdminRole(role);

      if (!name || !email) {
        return res.status(400).json({
          ok: false,
          message: "Name and email are required",
        });
      }

      if (!normalizedRole) {
        return res.status(400).json({
          ok: false,
          message: "Invalid role selected",
        });
      }

      const existingResult = await pool.query(
        `
        SELECT id, role
        FROM admins
        WHERE id = $1
        `,
        [id],
      );

      const existingAdmin = existingResult.rows[0];

      if (!existingAdmin) {
        return res.status(404).json({
          ok: false,
          message: "Admin not found",
        });
      }

      if (
        existingAdmin.role === "super_admin" &&
        normalizedRole !== "super_admin"
      ) {
        const superAdminCount = await getSuperAdminCount();

        if (superAdminCount <= 1) {
          return res.status(400).json({
            ok: false,
            message: "You must keep at least one super admin account",
          });
        }
      }

      let result;

      if (password) {
        const passwordHash = await bcrypt.hash(password, 10);

        result = await pool.query(
          `
          UPDATE admins
          SET name = $1,
              email = $2,
              password_hash = $3,
              role = $4
          WHERE id = $5
          RETURNING id, name, email, role, created_at
          `,
          [
            name.trim(),
            email.trim().toLowerCase(),
            passwordHash,
            normalizedRole,
            id,
          ],
        );
      } else {
        result = await pool.query(
          `
          UPDATE admins
          SET name = $1,
              email = $2,
              role = $3
          WHERE id = $4
          RETURNING id, name, email, role, created_at
          `,
          [name.trim(), email.trim().toLowerCase(), normalizedRole, id],
        );
      }

      res.json({
        ok: true,
        admin: result.rows[0],
      });
    } catch (error) {
      console.error("Update admin error:", error);

      if (error.code === "23505") {
        return res.status(409).json({
          ok: false,
          message: "An admin with this email already exists",
        });
      }

      res.status(500).json({
        ok: false,
        message: "Failed to update admin",
        error: error.message,
      });
    }
  },
);

app.delete(
  "/api/admins/:id",
  authenticateAdmin,
  requireRole("super_admin"),
  async (req, res) => {
    try {
      const { id } = req.params;

      if (Number(id) === Number(req.admin.id)) {
        return res.status(400).json({
          ok: false,
          message: "You cannot delete your own account while logged in",
        });
      }

      const existingResult = await pool.query(
        `
        SELECT id, role
        FROM admins
        WHERE id = $1
        `,
        [id],
      );

      const existingAdmin = existingResult.rows[0];

      if (!existingAdmin) {
        return res.status(404).json({
          ok: false,
          message: "Admin not found",
        });
      }

      if (existingAdmin.role === "super_admin") {
        const superAdminCount = await getSuperAdminCount();

        if (superAdminCount <= 1) {
          return res.status(400).json({
            ok: false,
            message: "You must keep at least one super admin account",
          });
        }
      }

      await pool.query(
        `
        DELETE FROM admins
        WHERE id = $1
        `,
        [id],
      );

      res.json({
        ok: true,
        message: "Admin deleted successfully",
      });
    } catch (error) {
      console.error("Delete admin error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to delete admin",
        error: error.message,
      });
    }
  },
);

/*
|--------------------------------------------------------------------------
| SCHEDULED STREAMS
|--------------------------------------------------------------------------
*/

const ensureScheduledStreamsTable = async () => {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS scheduled_streams (
      id SERIAL PRIMARY KEY,
      channel_id INTEGER NULL,
      title VARCHAR(255) NOT NULL,
      stream_key VARCHAR(255) NOT NULL,
      description TEXT,
      scheduled_start TIMESTAMPTZ NOT NULL,
      scheduled_end TIMESTAMPTZ NULL,
      timezone VARCHAR(100) DEFAULT 'America/Los_Angeles',
      status VARCHAR(40) DEFAULT 'scheduled',
      created_by INTEGER NULL,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);
};

const getPublicWatchBranding = async (organizationId) => {
  if (!organizationId) {
    return {
      organization: null,
      settings: null,
      branding: {
        name: "NLM Streaming",
        title: "NLM Streaming",
        logo_url: null,
        primary_color: "#0d6efd",
        secondary_color: "#fd9d00",
        donation_url: null,
        custom_css: null,
      },
    };
  }

  const result = await pool.query(
    `
    SELECT
      o.id,
      o.name,
      o.slug,
      o.logo_url AS organization_logo_url,
      o.primary_color AS organization_primary_color,
      o.custom_domain,
      os.watch_page_title,
      os.logo_url AS settings_logo_url,
      os.primary_color AS settings_primary_color,
      os.secondary_color,
      os.donation_url,
      os.custom_css
    FROM organizations o
    LEFT JOIN organization_settings os ON os.organization_id = o.id
    WHERE o.id = $1
      AND o.is_active = TRUE
    LIMIT 1
    `,
    [organizationId],
  );

  const row = result.rows[0];

  if (!row) {
    return {
      organization: null,
      settings: null,
      branding: {
        name: "NLM Streaming",
        title: "NLM Streaming",
        logo_url: null,
        primary_color: "#0d6efd",
        secondary_color: "#fd9d00",
        donation_url: null,
        custom_css: null,
      },
    };
  }

  const logoUrl = row.settings_logo_url || row.organization_logo_url || null;
  const primaryColor =
    row.settings_primary_color || row.organization_primary_color || "#0d6efd";
  const secondaryColor = row.secondary_color || "#fd9d00";
  const title = row.watch_page_title || row.name || "NLM Streaming";

  return {
    organization: {
      id: row.id,
      name: row.name,
      slug: row.slug,
      custom_domain: row.custom_domain,
    },
    settings: {
      watch_page_title: row.watch_page_title,
      logo_url: logoUrl,
      primary_color: primaryColor,
      secondary_color: secondaryColor,
      donation_url: row.donation_url,
      custom_css: row.custom_css,
    },
    branding: {
      name: row.name,
      title,
      logo_url: logoUrl,
      primary_color: primaryColor,
      secondary_color: secondaryColor,
      donation_url: row.donation_url,
      custom_css: row.custom_css,
    },
  };
};

/*
|--------------------------------------------------------------------------
| VIEWER ANALYTICS
|--------------------------------------------------------------------------
*/

const ensureViewerAnalyticsTables = async () => {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS viewer_sessions (
      id SERIAL PRIMARY KEY,
      organization_id INTEGER REFERENCES organizations(id) ON DELETE SET NULL,
      stream_key VARCHAR(255) NOT NULL,
      viewer_id VARCHAR(255) NOT NULL,
      session_token VARCHAR(255) UNIQUE NOT NULL,
      ip_address VARCHAR(100),
      user_agent TEXT,
      referrer TEXT,
      started_at TIMESTAMPTZ DEFAULT NOW(),
      last_seen_at TIMESTAMPTZ DEFAULT NOW(),
      ended_at TIMESTAMPTZ,
      duration_seconds INTEGER DEFAULT 0,
      device_type VARCHAR(80),
      browser_name VARCHAR(120),
      os_name VARCHAR(120),
      country_code VARCHAR(10),
      country_name VARCHAR(120),
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  await pool.query(`
    ALTER TABLE viewer_sessions
    ADD COLUMN IF NOT EXISTS device_type VARCHAR(80),
    ADD COLUMN IF NOT EXISTS browser_name VARCHAR(120),
    ADD COLUMN IF NOT EXISTS os_name VARCHAR(120),
    ADD COLUMN IF NOT EXISTS country_code VARCHAR(10),
    ADD COLUMN IF NOT EXISTS country_name VARCHAR(120)
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_viewer_sessions_stream_key
    ON viewer_sessions (stream_key)
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_viewer_sessions_organization_id
    ON viewer_sessions (organization_id)
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_viewer_sessions_last_seen_at
    ON viewer_sessions (last_seen_at)
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_viewer_sessions_tab_lookup
    ON viewer_sessions (organization_id, stream_key, viewer_id, last_seen_at DESC)
  `);
};

const ensureReplayAnalyticsTables = async () => {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS replay_sessions (
      id SERIAL PRIMARY KEY,
      organization_id INTEGER REFERENCES organizations(id) ON DELETE SET NULL,
      recording_id INTEGER REFERENCES recordings(id) ON DELETE CASCADE,
      public_slug VARCHAR(255) NOT NULL,
      viewer_id VARCHAR(255) NOT NULL,
      session_token VARCHAR(255) UNIQUE NOT NULL,
      ip_address VARCHAR(100),
      user_agent TEXT,
      referrer TEXT,
      started_at TIMESTAMPTZ DEFAULT NOW(),
      last_seen_at TIMESTAMPTZ DEFAULT NOW(),
      ended_at TIMESTAMPTZ,
      current_time_seconds INTEGER DEFAULT 0,
      max_position_seconds INTEGER DEFAULT 0,
      watched_seconds INTEGER DEFAULT 0,
      completed BOOLEAN DEFAULT FALSE,
      device_type VARCHAR(80),
      browser_name VARCHAR(120),
      os_name VARCHAR(120),
      country_code VARCHAR(10),
      country_name VARCHAR(120),
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  await pool.query(`
    ALTER TABLE replay_sessions
    ADD COLUMN IF NOT EXISTS device_type VARCHAR(80),
    ADD COLUMN IF NOT EXISTS browser_name VARCHAR(120),
    ADD COLUMN IF NOT EXISTS os_name VARCHAR(120),
    ADD COLUMN IF NOT EXISTS country_code VARCHAR(10),
    ADD COLUMN IF NOT EXISTS country_name VARCHAR(120),
    ADD COLUMN IF NOT EXISTS last_event_type VARCHAR(80),
    ADD COLUMN IF NOT EXISTS last_playback_rate NUMERIC DEFAULT 1,
    ADD COLUMN IF NOT EXISTS heartbeat_count INTEGER DEFAULT 0
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_replay_sessions_recording_id
    ON replay_sessions (recording_id)
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_replay_sessions_organization_id
    ON replay_sessions (organization_id)
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_replay_sessions_public_slug
    ON replay_sessions (public_slug)
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_replay_sessions_started_at
    ON replay_sessions (started_at)
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_replay_sessions_viewer_progress
    ON replay_sessions (viewer_id, recording_id, last_seen_at DESC)
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_replay_sessions_continue_lookup
    ON replay_sessions (viewer_id, last_seen_at DESC)
  `);
};

const makeSessionToken = () => {
  return `${Date.now()}-${Math.random().toString(36).slice(2)}-${Math.random()
    .toString(36)
    .slice(2)}`;
};

const getRequestIpAddress = (req) => {
  return (
    req.headers["cf-connecting-ip"] ||
    req.headers["x-real-ip"] ||
    req.headers["x-forwarded-for"]?.split(",")?.[0]?.trim() ||
    req.socket?.remoteAddress ||
    null
  );
};

const getCountryNameFromCode = (countryCode) => {
  const code = String(countryCode || "")
    .trim()
    .toUpperCase();

  const countries = {
    PH: "Philippines",
    US: "United States",
    CA: "Canada",
    GB: "United Kingdom",
    AU: "Australia",
    NZ: "New Zealand",
    SG: "Singapore",
    AE: "United Arab Emirates",
    SA: "Saudi Arabia",
    JP: "Japan",
    KR: "South Korea",
    IN: "India",
    ID: "Indonesia",
    MY: "Malaysia",
  };

  if (!code) return "Local / Unknown";
  if (["127", "::1", "LOCAL"].includes(code)) return "Local / Unknown";
  return countries[code] || code;
};

const getRequestCountryCode = (req, ipAddress = "") => {
  const headerCountry =
    req.headers["cf-ipcountry"] ||
    req.headers["x-vercel-ip-country"] ||
    req.headers["x-country-code"] ||
    req.headers["cloudfront-viewer-country"];

  if (headerCountry) return String(headerCountry).trim().toUpperCase();

  const ip = String(ipAddress || "").toLowerCase();
  if (
    !ip ||
    ip.includes("127.0.0.1") ||
    ip.includes("::1") ||
    ip.includes("localhost")
  ) {
    return "LOCAL";
  }

  return "UNKNOWN";
};

const getDeviceInfoFromUserAgent = (userAgent = "") => {
  const ua = String(userAgent || "");

  if (UAParser && ua) {
    try {
      const parser = new UAParser(ua);
      const parsed = parser.getResult();
      const deviceTypeRaw = parsed?.device?.type || "";
      const browserName = parsed?.browser?.name || "Unknown";
      const browserVersion = parsed?.browser?.major || "";
      const osName = parsed?.os?.name || "Unknown";
      const osVersion = parsed?.os?.version || "";

      let deviceType = "Desktop";
      if (deviceTypeRaw === "mobile") deviceType = "Mobile";
      else if (deviceTypeRaw === "tablet") deviceType = "Tablet";
      else if (deviceTypeRaw === "smarttv") deviceType = "Smart TV";
      else if (deviceTypeRaw === "console") deviceType = "Console";
      else if (deviceTypeRaw === "wearable") deviceType = "Wearable";
      else if (/bot|crawler|spider|preview/i.test(ua))
        deviceType = "Bot / Preview";

      return {
        deviceType,
        browserName: browserVersion
          ? `${browserName} ${browserVersion}`
          : browserName,
        osName: osVersion ? `${osName} ${osVersion}` : osName,
      };
    } catch {
      // Fallback below.
    }
  }

  const lower = ua.toLowerCase();

  let deviceType = "Desktop";
  if (/ipad|tablet|kindle|silk|playbook/i.test(ua)) deviceType = "Tablet";
  else if (/mobi|iphone|android.*mobile|windows phone/i.test(ua))
    deviceType = "Mobile";
  else if (/smart-tv|smarttv|appletv|roku|crkey|hbbtv/i.test(ua))
    deviceType = "Smart TV";
  else if (/bot|crawler|spider|preview/i.test(ua)) deviceType = "Bot / Preview";
  else if (!ua) deviceType = "Unknown";

  let browserName = "Unknown";
  if (lower.includes("edg/")) browserName = "Edge";
  else if (lower.includes("opr/") || lower.includes("opera"))
    browserName = "Opera";
  else if (lower.includes("samsungbrowser")) browserName = "Samsung Internet";
  else if (lower.includes("chrome/") && !lower.includes("chromium"))
    browserName = "Chrome";
  else if (lower.includes("safari/") && !lower.includes("chrome/"))
    browserName = "Safari";
  else if (lower.includes("firefox/")) browserName = "Firefox";

  let osName = "Unknown";
  if (lower.includes("windows")) osName = "Windows";
  else if (lower.includes("android")) osName = "Android";
  else if (/iphone|ipad|ipod/i.test(ua)) osName = "iOS / iPadOS";
  else if (lower.includes("mac os") || lower.includes("macintosh"))
    osName = "macOS";
  else if (lower.includes("linux")) osName = "Linux";

  return { deviceType, browserName, osName };
};

const getViewerMetricsForStream = async (streamKey, organizationId = null) => {
  const params = organizationId ? [streamKey, organizationId] : [streamKey];

  const activeWhere = organizationId
    ? `
      stream_key = $1
      AND organization_id = $2
      AND ended_at IS NULL
      AND last_seen_at >= NOW() - INTERVAL '45 seconds'
    `
    : `
      stream_key = $1
      AND ended_at IS NULL
      AND last_seen_at >= NOW() - INTERVAL '45 seconds'
    `;

  const totalWhere = organizationId
    ? `stream_key = $1 AND organization_id = $2`
    : `stream_key = $1`;

  const [activeResult, totalResult, peakResult] = await Promise.all([
    pool.query(
      `
      SELECT COUNT(*)::int AS count
      FROM viewer_sessions
      WHERE ${activeWhere}
      `,
      params,
    ),
    pool.query(
      `
      SELECT COUNT(*)::int AS total_sessions,
             COUNT(DISTINCT viewer_id)::int AS unique_viewers,
             COALESCE(SUM(duration_seconds), 0)::int AS total_watch_seconds
      FROM viewer_sessions
      WHERE ${totalWhere}
      `,
      params,
    ),
    pool.query(
      `
      SELECT COALESCE(MAX(active_viewers), 0)::int AS peak_viewers
      FROM (
        SELECT
          DATE_TRUNC('minute', last_seen_at) AS minute_bucket,
          COUNT(*)::int AS active_viewers
        FROM viewer_sessions
        WHERE ${totalWhere}
        GROUP BY minute_bucket
      ) minute_peaks
      `,
      params,
    ),
  ]);

  return {
    active_viewers: activeResult.rows[0]?.count || 0,
    total_sessions: totalResult.rows[0]?.total_sessions || 0,
    unique_viewers: totalResult.rows[0]?.unique_viewers || 0,
    total_watch_seconds: totalResult.rows[0]?.total_watch_seconds || 0,
    peak_viewers: peakResult.rows[0]?.peak_viewers || 0,
  };
};

const closeStaleViewerSessions = async () => {
  await pool.query(`
    UPDATE viewer_sessions
    SET ended_at = last_seen_at,
        duration_seconds = GREATEST(
          duration_seconds,
          EXTRACT(EPOCH FROM (last_seen_at - started_at))::int
        )
    WHERE ended_at IS NULL
      AND last_seen_at < NOW() - INTERVAL '2 minutes'
  `);
};

app.post(
  "/api/public/viewers/start",
  publicEngagementLimiter,
  async (req, res) => {
    try {
      const streamKey = cleanOrgText(req.body.stream_key, 255);
      const viewerId =
        cleanOrgText(req.body.viewer_id, 255) || makeSessionToken();

      if (!streamKey) {
        return res.status(400).json({
          ok: false,
          message: "Stream key is required",
        });
      }

      await closeStaleViewerSessions();

      const organizationId = await getOrganizationIdForStreamKey(streamKey);
      const userAgent = req.headers["user-agent"] || null;
      const referrer = req.headers.referer || req.headers.referrer || null;
      const ipAddress = getRequestIpAddress(req);
      const deviceInfo = getDeviceInfoFromUserAgent(userAgent);
      const countryCode = getRequestCountryCode(req, ipAddress);
      const countryName = getCountryNameFromCode(countryCode);

      /*
       * Reuse only a currently-active browser-tab session.
       * Do not revive old ended/inactive rows because that can inflate watch time
       * after a stream stops, a test reset runs, or a viewer returns much later.
       */
      const existingSession = await pool.query(
        `
      SELECT *
      FROM viewer_sessions
      WHERE stream_key = $1
        AND viewer_id = $2
        AND organization_id = $3
        AND ended_at IS NULL
        AND last_seen_at >= NOW() - INTERVAL '2 minutes'
      ORDER BY last_seen_at DESC
      LIMIT 1
      `,
        [streamKey, viewerId, organizationId],
      );

      let session = existingSession.rows[0];

      if (session) {
        const refreshedSession = await pool.query(
          `
        UPDATE viewer_sessions
        SET last_seen_at = NOW(),
            ended_at = NULL,
            duration_seconds = EXTRACT(EPOCH FROM (NOW() - started_at))::int,
            ip_address = COALESCE($2, ip_address),
            user_agent = COALESCE($3, user_agent),
            referrer = COALESCE($4, referrer),
            device_type = COALESCE($5, device_type),
            browser_name = COALESCE($6, browser_name),
            os_name = COALESCE($7, os_name),
            country_code = COALESCE($8, country_code),
            country_name = COALESCE($9, country_name)
        WHERE id = $1
        RETURNING *
        `,
          [
            session.id,
            ipAddress,
            userAgent,
            referrer,
            deviceInfo.deviceType,
            deviceInfo.browserName,
            deviceInfo.osName,
            countryCode,
            countryName,
          ],
        );

        session = refreshedSession.rows[0];
      } else {
        const sessionToken = makeSessionToken();

        const result = await pool.query(
          `
        INSERT INTO viewer_sessions (
          organization_id,
          stream_key,
          viewer_id,
          session_token,
          ip_address,
          user_agent,
          referrer,
          device_type,
          browser_name,
          os_name,
          country_code,
          country_name
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        RETURNING *
        `,
          [
            organizationId,
            streamKey,
            viewerId,
            sessionToken,
            ipAddress,
            userAgent,
            referrer,
            deviceInfo.deviceType,
            deviceInfo.browserName,
            deviceInfo.osName,
            countryCode,
            countryName,
          ],
        );

        session = result.rows[0];
      }

      const metrics = await getViewerMetricsForStream(
        streamKey,
        organizationId,
      );

      io.to(
        organizationScopedRoom("analytics", organizationId, streamKey),
      ).emit("analytics:viewers", {
        stream_key: streamKey,
        ...metrics,
      });

      res.json({
        ok: true,
        session: {
          id: session.id,
          session_token: session.session_token,
          viewer_id: session.viewer_id,
        },
        metrics,
      });
    } catch (error) {
      console.error("Start viewer session error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to start viewer session",
        error: error.message,
      });
    }
  },
);

app.post(
  "/api/public/viewers/heartbeat",
  statusPollLimiter,
  async (req, res) => {
    try {
      const sessionToken = cleanOrgText(req.body.session_token, 255);

      if (!sessionToken) {
        return res.status(400).json({
          ok: false,
          message: "Session token is required",
        });
      }

      const result = await pool.query(
        `
      UPDATE viewer_sessions
      SET last_seen_at = NOW(),
          duration_seconds = EXTRACT(EPOCH FROM (NOW() - started_at))::int
      WHERE session_token = $1
        AND ended_at IS NULL
      RETURNING organization_id, stream_key
      `,
        [sessionToken],
      );

      if (!result.rows[0]) {
        return res.status(404).json({
          ok: false,
          message: "Viewer session not found",
        });
      }

      const row = result.rows[0];
      const metrics = await getViewerMetricsForStream(
        row.stream_key,
        row.organization_id,
      );

      res.json({ ok: true, metrics });
    } catch (error) {
      console.error("Viewer heartbeat error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to update viewer heartbeat",
        error: error.message,
      });
    }
  },
);

app.post(
  "/api/public/viewers/end",
  publicEngagementLimiter,
  async (req, res) => {
    try {
      const sessionToken = cleanOrgText(req.body.session_token, 255);

      if (!sessionToken) {
        return res.json({ ok: true });
      }

      const result = await pool.query(
        `
      UPDATE viewer_sessions
      SET ended_at = NOW(),
          last_seen_at = NOW(),
          duration_seconds = EXTRACT(EPOCH FROM (NOW() - started_at))::int
      WHERE session_token = $1
        AND ended_at IS NULL
      RETURNING organization_id, stream_key
      `,
        [sessionToken],
      );

      if (result.rows[0]) {
        const row = result.rows[0];
        const metrics = await getViewerMetricsForStream(
          row.stream_key,
          row.organization_id,
        );

        io.to(
          organizationScopedRoom(
            "analytics",
            row.organization_id,
            row.stream_key,
          ),
        ).emit("analytics:viewers", {
          stream_key: row.stream_key,
          ...metrics,
        });
      }

      res.json({ ok: true });
    } catch (error) {
      console.error("End viewer session error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to end viewer session",
        error: error.message,
      });
    }
  },
);

const getRequestedTenantIdForAnalytics = (req) => {
  return req.headers["x-organization-id"] || req.query.organization_id || null;
};

const requireAnalyticsTenant = (req, res) => {
  if (!req.organization?.id) {
    res.status(400).json({
      ok: false,
      message: "Please select a tenant before loading analytics.",
    });
    return false;
  }

  if (
    req.admin?.role === "super_admin" &&
    !getRequestedTenantIdForAnalytics(req)
  ) {
    res.status(400).json({
      ok: false,
      message: "Please select a tenant before loading analytics.",
    });
    return false;
  }

  return true;
};

app.get(
  "/api/analytics/summary",
  authenticateAdmin,
  resolveOrganizationForRequest,
  requireActiveSubscription,
  requirePlanFeature("analytics_enabled"),
  async (req, res) => {
    try {
      if (!requireAnalyticsTenant(req, res)) return;

      await closeStaleViewerSessions();

      const days = Math.min(Math.max(Number(req.query.days || 7), 1), 90);

      const analyticsParams = [req.organization.id, days];

      const summaryResult = await pool.query(
        `
        WITH scoped_sessions AS (
          SELECT vs.*
          FROM viewer_sessions vs
          JOIN channels c
            ON c.stream_key = vs.stream_key
           AND c.organization_id = $1
          WHERE vs.organization_id = $1
            AND vs.started_at >= NOW() - ($2::text || ' days')::interval
        )
        SELECT
          COUNT(*)::int AS total_sessions,
          COUNT(DISTINCT viewer_id)::int AS unique_viewers,
          COALESCE(SUM(duration_seconds), 0)::int AS total_watch_seconds,
          COALESCE(AVG(NULLIF(duration_seconds, 0)), 0)::int AS avg_watch_seconds
        FROM scoped_sessions
        `,
        analyticsParams,
      );

      const streamsResult = await pool.query(
        `
        WITH scoped_sessions AS (
          SELECT vs.*
          FROM viewer_sessions vs
          JOIN channels c
            ON c.stream_key = vs.stream_key
           AND c.organization_id = $1
          WHERE vs.organization_id = $1
            AND vs.started_at >= NOW() - ($2::text || ' days')::interval
        ),
        stream_totals AS (
          SELECT
            stream_key,
            COUNT(*)::int AS total_sessions,
            COUNT(DISTINCT viewer_id)::int AS unique_viewers,
            COALESCE(SUM(duration_seconds), 0)::int AS total_watch_seconds,
            COALESCE(AVG(NULLIF(duration_seconds, 0)), 0)::int AS avg_watch_seconds,
            MAX(started_at) AS last_viewed_at
          FROM scoped_sessions
          GROUP BY stream_key
        ),
        stream_peaks AS (
          SELECT
            stream_key,
            COALESCE(MAX(active_viewers), 0)::int AS peak_viewers
          FROM (
            SELECT
              stream_key,
              DATE_TRUNC('minute', last_seen_at) AS minute_bucket,
              COUNT(*)::int AS active_viewers
            FROM scoped_sessions
            GROUP BY stream_key, minute_bucket
          ) minute_peaks
          GROUP BY stream_key
        )
        SELECT
          st.stream_key,
          st.total_sessions,
          st.unique_viewers,
          st.total_watch_seconds,
          st.avg_watch_seconds,
          st.last_viewed_at,
          COALESCE(sp.peak_viewers, 0)::int AS peak_viewers
        FROM stream_totals st
        LEFT JOIN stream_peaks sp ON sp.stream_key = st.stream_key
        ORDER BY st.total_sessions DESC, st.last_viewed_at DESC
        LIMIT 25
        `,
        analyticsParams,
      );

      const dailyResult = await pool.query(
        `
        WITH scoped_sessions AS (
          SELECT vs.*
          FROM viewer_sessions vs
          JOIN channels c
            ON c.stream_key = vs.stream_key
           AND c.organization_id = $1
          WHERE vs.organization_id = $1
            AND vs.started_at >= NOW() - ($2::text || ' days')::interval
        )
        SELECT
          DATE_TRUNC('day', started_at)::date AS day,
          COUNT(*)::int AS total_sessions,
          COUNT(DISTINCT viewer_id)::int AS unique_viewers,
          COALESCE(SUM(duration_seconds), 0)::int AS total_watch_seconds
        FROM scoped_sessions
        GROUP BY day
        ORDER BY day ASC
        `,
        analyticsParams,
      );

      const activeResult = await pool.query(
        `
        SELECT
          vs.stream_key,
          COUNT(*)::int AS active_viewers
        FROM viewer_sessions vs
        JOIN channels c
          ON c.stream_key = vs.stream_key
         AND c.organization_id = $1
        WHERE vs.organization_id = $1
          AND vs.ended_at IS NULL
          AND vs.last_seen_at >= NOW() - INTERVAL '45 seconds'
        GROUP BY vs.stream_key
        ORDER BY active_viewers DESC
        `,
        [req.organization.id],
      );

      const timelineResult = await pool.query(
        `
        WITH scoped_sessions AS (
          SELECT vs.*
          FROM viewer_sessions vs
          JOIN channels c
            ON c.stream_key = vs.stream_key
           AND c.organization_id = $1
          WHERE vs.organization_id = $1
            AND vs.started_at >= NOW() - ($2::text || ' days')::interval
        ),
        buckets AS (
          SELECT generate_series(
            DATE_TRUNC('hour', NOW() - ($2::text || ' days')::interval),
            DATE_TRUNC('hour', NOW()),
            INTERVAL '1 hour'
          ) AS bucket
        )
        SELECT
          b.bucket,
          COALESCE(COUNT(s.id), 0)::int AS active_viewers,
          COALESCE(COUNT(DISTINCT s.viewer_id), 0)::int AS unique_viewers
        FROM buckets b
        LEFT JOIN scoped_sessions s
          ON s.started_at <= b.bucket
         AND COALESCE(s.ended_at, s.last_seen_at, NOW()) >= b.bucket
        GROUP BY b.bucket
        ORDER BY b.bucket ASC
        `,
        analyticsParams,
      );

      const watchTrendResult = await pool.query(
        `
        WITH scoped_sessions AS (
          SELECT vs.*
          FROM viewer_sessions vs
          JOIN channels c
            ON c.stream_key = vs.stream_key
           AND c.organization_id = $1
          WHERE vs.organization_id = $1
            AND vs.started_at >= NOW() - ($2::text || ' days')::interval
        )
        SELECT
          DATE_TRUNC('day', started_at)::date AS day,
          COALESCE(SUM(duration_seconds), 0)::int AS total_watch_seconds,
          COALESCE(AVG(NULLIF(duration_seconds, 0)), 0)::int AS avg_watch_seconds
        FROM scoped_sessions
        GROUP BY day
        ORDER BY day ASC
        `,
        analyticsParams,
      );

      const retentionResult = await pool.query(
        `
        WITH scoped_sessions AS (
          SELECT vs.*
          FROM viewer_sessions vs
          JOIN channels c
            ON c.stream_key = vs.stream_key
           AND c.organization_id = $1
          WHERE vs.organization_id = $1
            AND vs.started_at >= NOW() - ($2::text || ' days')::interval
        )
        SELECT
          CASE
            WHEN duration_seconds < 30 THEN 'Under 30s'
            WHEN duration_seconds < 60 THEN '30s–1m'
            WHEN duration_seconds < 300 THEN '1m–5m'
            WHEN duration_seconds < 900 THEN '5m–15m'
            WHEN duration_seconds < 1800 THEN '15m–30m'
            ELSE '30m+'
          END AS bucket,
          CASE
            WHEN duration_seconds < 30 THEN 1
            WHEN duration_seconds < 60 THEN 2
            WHEN duration_seconds < 300 THEN 3
            WHEN duration_seconds < 900 THEN 4
            WHEN duration_seconds < 1800 THEN 5
            ELSE 6
          END AS sort_order,
          COUNT(*)::int AS sessions
        FROM scoped_sessions
        GROUP BY bucket, sort_order
        ORDER BY sort_order ASC
        `,
        analyticsParams,
      );

      const rawDeviceSessionResult = await pool.query(
        `
        SELECT
          COALESCE(NULLIF(vs.device_type, ''), 'Unknown') AS device_type,
          COALESCE(NULLIF(vs.browser_name, ''), 'Unknown') AS browser_name,
          COALESCE(NULLIF(vs.os_name, ''), 'Unknown') AS os_name,
          vs.user_agent,
          vs.viewer_id,
          COALESCE(vs.duration_seconds, 0)::int AS watch_seconds
        FROM viewer_sessions vs
        JOIN channels c
          ON c.stream_key = vs.stream_key
         AND c.organization_id = $1
        WHERE vs.organization_id = $1
          AND vs.started_at >= NOW() - ($2::text || ' days')::interval
        `,
        analyticsParams,
      );

      const deviceBuckets = new Map();

      for (const row of rawDeviceSessionResult.rows) {
        const parsedInfo = getDeviceInfoFromUserAgent(row.user_agent || "");
        const hasUsableDevice =
          row.device_type &&
          row.browser_name &&
          row.os_name &&
          row.device_type !== "Unknown" &&
          row.browser_name !== "Unknown" &&
          row.os_name !== "Unknown";

        const deviceType = hasUsableDevice
          ? row.device_type
          : parsedInfo.deviceType;
        const browserName = hasUsableDevice
          ? row.browser_name
          : parsedInfo.browserName;
        const osName = hasUsableDevice ? row.os_name : parsedInfo.osName;

        const key = `${deviceType}|||${browserName}|||${osName}`;
        const existing = deviceBuckets.get(key) || {
          device_type: deviceType || "Unknown",
          browser_name: browserName || "Unknown",
          os_name: osName || "Unknown",
          sessions: 0,
          unique_viewers_set: new Set(),
          total_watch_seconds: 0,
        };

        existing.sessions += 1;
        if (row.viewer_id) existing.unique_viewers_set.add(row.viewer_id);
        existing.total_watch_seconds += Number(row.watch_seconds || 0);
        deviceBuckets.set(key, existing);
      }

      const deviceResult = {
        rows: Array.from(deviceBuckets.values())
          .map((item) => ({
            device_type: item.device_type,
            browser_name: item.browser_name,
            os_name: item.os_name,
            sessions: item.sessions,
            unique_viewers: item.unique_viewers_set.size,
            total_watch_seconds: item.total_watch_seconds,
          }))
          .sort((a, b) => {
            if (b.sessions !== a.sessions) return b.sessions - a.sessions;
            return b.total_watch_seconds - a.total_watch_seconds;
          })
          .slice(0, 12),
      };

      const rawGeographySessionResult = await pool.query(
        `
        SELECT
          COALESCE(NULLIF(vs.country_code, ''), 'UNKNOWN') AS country_code,
          COALESCE(NULLIF(vs.country_name, ''), 'Local / Unknown') AS country_name,
          vs.viewer_id,
          COALESCE(vs.duration_seconds, 0)::int AS watch_seconds
        FROM viewer_sessions vs
        JOIN channels c
          ON c.stream_key = vs.stream_key
         AND c.organization_id = $1
        WHERE vs.organization_id = $1
          AND vs.started_at >= NOW() - ($2::text || ' days')::interval
        `,
        analyticsParams,
      );

      const geographyBuckets = new Map();

      for (const row of rawGeographySessionResult.rows) {
        const countryCode = row.country_code || "UNKNOWN";
        const countryName = row.country_name || "Local / Unknown";
        const key = `${countryCode}|||${countryName}`;

        const existing = geographyBuckets.get(key) || {
          country_code: countryCode,
          country_name: countryName,
          sessions: 0,
          unique_viewers_set: new Set(),
          total_watch_seconds: 0,
        };

        existing.sessions += 1;
        if (row.viewer_id) existing.unique_viewers_set.add(row.viewer_id);
        existing.total_watch_seconds += Number(row.watch_seconds || 0);
        geographyBuckets.set(key, existing);
      }

      const geographyResult = {
        rows: Array.from(geographyBuckets.values())
          .map((item) => ({
            country_code: item.country_code,
            country_name: item.country_name,
            sessions: item.sessions,
            unique_viewers: item.unique_viewers_set.size,
            total_watch_seconds: item.total_watch_seconds,
          }))
          .sort((a, b) => {
            if (b.sessions !== a.sessions) return b.sessions - a.sessions;
            return b.total_watch_seconds - a.total_watch_seconds;
          })
          .slice(0, 12),
      };

      res.json({
        ok: true,
        days,
        summary: summaryResult.rows[0] || {
          total_sessions: 0,
          unique_viewers: 0,
          total_watch_seconds: 0,
          avg_watch_seconds: 0,
        },
        streams: streamsResult.rows,
        daily: dailyResult.rows,
        active_streams: activeResult.rows,
        viewer_timeline: timelineResult.rows,
        watch_time_trend: watchTrendResult.rows,
        retention: retentionResult.rows,
        devices: deviceResult.rows,
        geography: geographyResult.rows,
      });
    } catch (error) {
      console.error("Analytics summary error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to load analytics summary",
        error: error.message,
      });
    }
  },
);

const escapeCsvValue = (value) => {
  if (value === null || value === undefined) return "";
  const text = String(value);
  if (/[",\n\r]/.test(text)) {
    return `"${text.replace(/"/g, '""')}"`;
  }
  return text;
};

app.get(
  "/api/analytics/export.csv",
  authenticateAdmin,
  resolveOrganizationForRequest,
  requireActiveSubscription,
  requirePlanFeature("analytics_enabled"),
  async (req, res) => {
    try {
      if (!requireAnalyticsTenant(req, res)) return;

      await closeStaleViewerSessions();

      const days = Math.min(Math.max(Number(req.query.days || 7), 1), 90);

      const result = await pool.query(
        `
        SELECT
          vs.stream_key,
          vs.viewer_id,
          vs.started_at,
          vs.last_seen_at,
          vs.ended_at,
          vs.duration_seconds,
          vs.device_type,
          vs.browser_name,
          vs.os_name,
          vs.country_code,
          vs.country_name,
          vs.ip_address,
          vs.referrer
        FROM viewer_sessions vs
        JOIN channels c
          ON c.stream_key = vs.stream_key
         AND c.organization_id = $1
        WHERE vs.organization_id = $1
          AND vs.started_at >= NOW() - ($2::text || ' days')::interval
        ORDER BY vs.started_at DESC
        `,
        [req.organization.id, days],
      );

      const rows = [
        [
          "stream_key",
          "viewer_id",
          "started_at",
          "last_seen_at",
          "ended_at",
          "duration_seconds",
          "device_type",
          "browser_name",
          "os_name",
          "country_code",
          "country_name",
          "ip_address",
          "referrer",
        ],
        ...result.rows.map((row) => [
          row.stream_key,
          row.viewer_id,
          row.started_at ? new Date(row.started_at).toISOString() : "",
          row.last_seen_at ? new Date(row.last_seen_at).toISOString() : "",
          row.ended_at ? new Date(row.ended_at).toISOString() : "",
          row.duration_seconds || 0,
          row.device_type || "",
          row.browser_name || "",
          row.os_name || "",
          row.country_code || "",
          row.country_name || "",
          row.ip_address || "",
          row.referrer || "",
        ]),
      ];

      const csv = rows
        .map((row) => row.map(escapeCsvValue).join(","))
        .join("\n");

      res.setHeader("Content-Type", "text/csv; charset=utf-8");
      res.setHeader(
        "Content-Disposition",
        `attachment; filename="viewer-analytics-${days}d.csv"`,
      );
      res.send(csv);
    } catch (error) {
      console.error("Analytics CSV export error:", error);
      res.status(500).json({
        ok: false,
        message: "Failed to export analytics CSV",
        error: error.message,
      });
    }
  },
);

const getPublicWatchStatus = async (streamKey) => {
  let activeStream = null;
  const organizationId = await getOrganizationIdForStreamKey(streamKey);

  // Reduced-latency and ABR/transcoding plan flags — previously wired into
  // this response but found missing on a fresh audit against the actual
  // deployed server.js (WatchPage.jsx defaults both to false when the API
  // doesn't send them, so this silently disabled reduced latency and ABR
  // for every organization, not just some). Uses the same LEFT JOIN +
  // COALESCE(s.plan_key, o.subscription_plan, 'starter') pattern already
  // proven correct in getOrgMaxBitrateKbps, rather than
  // getOrganizationSubscriptionSummary's plain JOIN, since that function
  // returns null entirely for an org with no subscriptions row yet.
  let reducedLatencyEnabled = false;
  let transcodingEnabled = false;
  if (organizationId) {
    try {
      const planFlagsResult = await pool.query(
        `
        SELECT p.reduced_latency_enabled,
               COALESCE(o.transcoding_override, p.transcoding_enabled) AS transcoding_enabled
        FROM organizations o
        LEFT JOIN subscriptions s ON s.organization_id = o.id
        JOIN plans p ON p.plan_key = COALESCE(s.plan_key, o.subscription_plan, 'starter')
        WHERE o.id = $1
        `,
        [organizationId],
      );
      reducedLatencyEnabled = Boolean(
        planFlagsResult.rows[0]?.reduced_latency_enabled,
      );
      transcodingEnabled = Boolean(
        planFlagsResult.rows[0]?.transcoding_enabled,
      );
      // Essential-tier orgs don't have the `transcoding_enabled` plan flag
      // set, but as of the ABR-rendition-ladder fold (2026-08-03) they DO
      // get a real single-rung rendition (see getRenditionPlanForOrg) —
      // route them through /api/abr/ same as Deluxe/Premium rather than
      // the plain /api/hls/ path, which no longer has anything special
      // publishing to it for a plan'd org.
      if (!transcodingEnabled) {
        const capKbps = await getOrgMaxBitrateKbps(organizationId);
        transcodingEnabled = Boolean(capKbps);
      }
    } catch (planErr) {
      console.error("Watch status plan flags lookup error:", planErr.message);
    }
  }

  // SRS is the authoritative source for whether a broadcast is live.
  // The database flag is bookkeeping and may become stale if an unpublish
  // webhook is delayed or missed. Keeping SRS authoritative prevents the
  // Watch Page and embed player from remaining on "Preparing stream" while
  // no raw publisher actually exists.
  let channelState = null;
  try {
    const channelResult = await pool.query(
      `SELECT stream_key, name, is_live, live_started_at,
              EXTRACT(EPOCH FROM (NOW() - live_started_at))::int AS uptime_seconds
       FROM channels
       WHERE stream_key = $1 AND organization_id = $2
       LIMIT 1`,
      [streamKey, organizationId],
    );
    channelState = channelResult.rows[0] || null;
  } catch (dbErr) {
    console.warn("DB watch-state lookup failed:", dbErr.message);
  }

  let srsReachable = false;
  let rawSrsStream = null;

  try {
    const response = await fetch(
      `${SRS_API_URL.replace(/\/$/, "")}/api/v1/streams/`,
      { signal: AbortSignal.timeout(4000) },
    );

    if (response.ok) {
      srsReachable = true;
      const data = await response.json();
      rawSrsStream = (data.streams || []).find(
        (stream) =>
          stream.name === streamKey &&
          stream.publish?.active &&
          !isAbrRenditionStreamKey(stream.name),
      );
    }
  } catch (srsErr) {
    console.debug(
      `[WATCH-STATUS] Unable to verify SRS state for ${streamKey}:`,
      srsErr.message,
    );
  }

  if (rawSrsStream) {
    const liveStartedAtMs = channelState?.live_started_at
      ? new Date(channelState.live_started_at).getTime()
      : Date.now();

    activeStream = {
      ...rawSrsStream,
      encoderGeneration: bitrateCapEncoderGeneration.get(streamKey) || 0,
      broadcastGeneration: bitrateCapGeneration.get(streamKey) || 0,
      liveStartedAtMs,
    };

    if (!channelState?.is_live) {
      pool
        .query(
          `UPDATE channels
           SET is_live = TRUE,
               live_started_at = COALESCE(live_started_at, NOW())
           WHERE stream_key = $1 AND organization_id = $2`,
          [streamKey, organizationId],
        )
        .catch((error) =>
          console.debug(
            `[WATCH-STATUS] Failed to repair live DB state for ${streamKey}:`,
            error.message,
          ),
        );
    }
  } else if (srsReachable) {
    // SRS answered successfully and proved that the raw source is offline.
    // Repair a stale DB live marker asynchronously.
    if (channelState?.is_live) {
      pool
        .query(
          `UPDATE channels
           SET is_live = FALSE, live_started_at = NULL
           WHERE stream_key = $1 AND organization_id = $2`,
          [streamKey, organizationId],
        )
        .catch((error) =>
          console.debug(
            `[WATCH-STATUS] Failed to clear stale DB state for ${streamKey}:`,
            error.message,
          ),
        );
    }
  } else if (channelState?.is_live) {
    // Only fall back to the DB when SRS itself could not be reached. This
    // preserves service during a brief SRS API/network hiccup without letting
    // a stale DB flag override a successful SRS "offline" result.
    activeStream = {
      name: channelState.stream_key,
      publish: {
        active: true,
        active_age: channelState.uptime_seconds || 0,
      },
      clients: 0,
      kbps: { recv_30s: 0 },
      source: "db_fallback",
      encoderGeneration: bitrateCapEncoderGeneration.get(streamKey) || 0,
      broadcastGeneration: bitrateCapGeneration.get(streamKey) || 0,
      liveStartedAtMs: channelState.live_started_at
        ? new Date(channelState.live_started_at).getTime()
        : 0,
    };
  }

  const scheduleResult = await pool.query(
    `
    SELECT *
    FROM scheduled_streams
    WHERE stream_key = $1
      AND organization_id = $2
      AND status IN ('scheduled', 'live')
      AND scheduled_start >= NOW() - INTERVAL '6 hours'
    ORDER BY scheduled_start ASC
    LIMIT 1
    `,
    [streamKey, organizationId],
  );

  const brandingData = await getPublicWatchBranding(organizationId);
  const viewerMetrics = await getViewerMetricsForStream(
    streamKey,
    organizationId,
  );

  // If this organization has its own dedicated Bunny pull zone (see the
  // per-org zone provisioning work), the watch page should route through
  // it instead of the shared platform CDN — same backend /api/hls/ proxy
  // logic either way (it's origin-agnostic), just a different front door.
  // null here means "use the shared default", which the frontend already
  // falls back to.
  // hlsBaseUrl: where HLS/ABR playback URLs are built against. Defaults to
  // the shared dedicated HLS_CDN_HOSTNAME zone (Token Authentication lives
  // here); an org with its OWN dedicated Bunny zone overrides it.
  //
  // rtcBaseUrl: WHEP (WebRTC) signaling base — deliberately NOT defaulted
  // to HLS_CDN_HOSTNAME. That zone has Token Authentication on, and WHEP's
  // POST to /rtc/v1/whep/ isn't signed (out of scope for this pass) — if
  // WhepPlayer inherited the HLS zone's hostname, its signaling requests
  // would start getting 403'd by Bunny. null here means "frontend falls
  // back to the general API host", same as hlsBaseUrl always used to.
  let hlsBaseUrl = HLS_CDN_HOSTNAME ? `https://${HLS_CDN_HOSTNAME}` : null;
  let rtcBaseUrl = null;
  try {
    const orgZoneResult = await pool.query(
      `SELECT bunny_pull_zone_hostname FROM organizations WHERE id = $1`,
      [organizationId],
    );
    const hostname = orgZoneResult.rows[0]?.bunny_pull_zone_hostname;
    if (hostname) {
      hlsBaseUrl = `https://${hostname}`;
      rtcBaseUrl = `https://${hostname}`;
    }
  } catch (zoneErr) {
    console.error("Watch status zone lookup error:", zoneErr.message);
  }

  // Signed query string for the TOP-LEVEL manifest URL the frontend builds
  // (WatchPage.jsx appends this to /api/hls/:streamKey.m3u8 or
  // /api/abr/:streamKey/master.m3u8) — everything downstream of that
  // request (nested manifests, segments) is signed by the manifest-rewrite
  // logic in the proxy routes themselves, not here.
  const hlsUrlPath = transcodingEnabled
    ? `/api/abr/${streamKey}/master.m3u8`
    : `/api/hls/${streamKey}.m3u8`;
  const hlsAuthQs = appendBunnyToken(hlsUrlPath);

  // Report readiness through the same public status object used by both the
  // React Watch Page and the standalone iframe. This keeps both surfaces from
  // requesting the ABR master while the raw source is live but renditions are
  // still initializing.
  let abrReady = !transcodingEnabled && Boolean(activeStream);
  let playbackReady = Boolean(activeStream);

  if (activeStream && transcodingEnabled && organizationId) {
    try {
      const renditionPlan = await getRenditionPlanForOrg(organizationId);
      for (const rendition of renditionPlan) {
        const upstreamUrl =
          `${SRS_INTERNAL_HLS_BASE_URL.replace(/\/$/, "")}/live/` +
          `${streamKey}_${rendition.label}.m3u8`;
        const initialized = await fetchInitializedHlsPlaylist(upstreamUrl);
        if (initialized.ok) {
          abrReady = true;
          break;
        }
      }
    } catch (readinessError) {
      console.debug(
        `[WATCH-STATUS] ABR readiness check failed for ${streamKey}:`,
        readinessError.message,
      );
    }
    playbackReady = abrReady;
  }

  return {
    organization_id: organizationId,
    organization: brandingData.organization,
    settings: brandingData.settings,
    branding: brandingData.branding,
    isLive: Boolean(activeStream),
    abrReady,
    playbackReady,
    stream: activeStream || null,
    schedule: scheduleResult.rows[0] || null,
    viewerMetrics,
    hlsBaseUrl,
    rtcBaseUrl,
    hlsAuthQs,
    reducedLatencyEnabled,
    transcodingEnabled,
  };
};

app.get("/api/public/watch/:streamKey", statusPollLimiter, async (req, res) => {
  try {
    const { streamKey } = req.params;
    const status = await getPublicWatchStatus(streamKey);

    res.json({
      ok: true,
      ...status,
    });
  } catch (error) {
    console.error("Public watch status error:", error);

    res.status(500).json({
      ok: false,
      message: "Failed to load stream status",
      error: error.message,
    });
  }
});

app.get(
  "/api/schedules",
  authenticateAdmin,
  resolveOrganizationForRequest,
  async (req, res) => {
    try {
      const result = await pool.query(
        `
      SELECT ss.*, c.name AS channel_name
      FROM scheduled_streams ss
      LEFT JOIN channels c ON c.id = ss.channel_id
      WHERE ss.organization_id = $1
      ORDER BY ss.scheduled_start ASC
      `,
        [req.organization.id],
      );

      res.json({
        ok: true,
        schedules: result.rows,
      });
    } catch (error) {
      console.error("Get schedules error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to fetch scheduled streams",
        error: error.message,
      });
    }
  },
);

app.post(
  "/api/schedules",
  authenticateAdmin,
  resolveOrganizationForRequest,
  requireRole("super_admin", "admin", "operator"),
  async (req, res) => {
    try {
      const {
        channel_id,
        title,
        stream_key,
        description,
        scheduled_start,
        scheduled_end,
        timezone,
        status,
      } = req.body;

      if (!title || !stream_key || !scheduled_start) {
        return res.status(400).json({
          ok: false,
          message: "Title, stream key, and scheduled start are required",
        });
      }

      const result = await pool.query(
        `
        INSERT INTO scheduled_streams (
          organization_id,
          channel_id,
          title,
          stream_key,
          description,
          scheduled_start,
          scheduled_end,
          timezone,
          status,
          created_by
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        RETURNING *
        `,
        [
          req.organization.id,
          channel_id || null,
          title.trim(),
          stream_key.trim(),
          description || null,
          scheduled_start,
          scheduled_end || null,
          timezone || "America/Los_Angeles",
          status || "scheduled",
          req.admin.id,
        ],
      );

      res.json({
        ok: true,
        schedule: result.rows[0],
      });
    } catch (error) {
      console.error("Create schedule error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to create scheduled stream",
        error: error.message,
      });
    }
  },
);

app.put(
  "/api/schedules/:id",
  authenticateAdmin,
  resolveOrganizationForRequest,
  requireRole("super_admin", "admin", "operator"),
  async (req, res) => {
    try {
      const { id } = req.params;
      const {
        channel_id,
        title,
        stream_key,
        description,
        scheduled_start,
        scheduled_end,
        timezone,
        status,
      } = req.body;

      if (!title || !stream_key || !scheduled_start) {
        return res.status(400).json({
          ok: false,
          message: "Title, stream key, and scheduled start are required",
        });
      }

      const result = await pool.query(
        `
        UPDATE scheduled_streams
        SET channel_id = $1,
            title = $2,
            stream_key = $3,
            description = $4,
            scheduled_start = $5,
            scheduled_end = $6,
            timezone = $7,
            status = $8,
            updated_at = NOW()
        WHERE id = $9
          AND organization_id = $10
        RETURNING *
        `,
        [
          channel_id || null,
          title.trim(),
          stream_key.trim(),
          description || null,
          scheduled_start,
          scheduled_end || null,
          timezone || "America/Los_Angeles",
          status || "scheduled",
          id,
          req.organization.id,
        ],
      );

      if (!result.rows[0]) {
        return res.status(404).json({
          ok: false,
          message: "Scheduled stream not found",
        });
      }

      res.json({
        ok: true,
        schedule: result.rows[0],
      });
    } catch (error) {
      console.error("Update schedule error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to update scheduled stream",
        error: error.message,
      });
    }
  },
);

app.delete(
  "/api/schedules/:id",
  authenticateAdmin,
  resolveOrganizationForRequest,
  requireRole("super_admin", "admin"),
  async (req, res) => {
    try {
      const { id } = req.params;

      await pool.query(
        `
        DELETE FROM scheduled_streams
        WHERE id = $1
          AND organization_id = $2
        `,
        [id, req.organization.id],
      );

      res.json({
        ok: true,
        message: "Scheduled stream deleted successfully",
      });
    } catch (error) {
      console.error("Delete schedule error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to delete scheduled stream",
        error: error.message,
      });
    }
  },
);

/*
|--------------------------------------------------------------------------
| SRS STREAMS
|--------------------------------------------------------------------------
*/

app.get(
  "/api/srs/streams",
  authenticateAdmin,
  resolveOrganizationForRequest,
  async (req, res) => {
    try {
      const response = await fetch(`${SRS_API_URL}/api/v1/streams`);

      if (!response.ok) {
        throw new Error(`SRS API responded with ${response.status}`);
      }

      const data = await response.json();

      const allowedResult = await pool.query(
        "SELECT stream_key, live_started_at FROM channels WHERE organization_id = $1",
        [req.organization.id],
      );

      const allowedStreamKeys = new Set(
        allowedResult.rows.map((row) => String(row.stream_key)),
      );

      const liveStartedAtByKey = new Map(
        allowedResult.rows.map((row) => [
          String(row.stream_key),
          row.live_started_at,
        ]),
      );

      const filteredStreams = (data.streams || []).filter((stream) => {
        return (
          allowedStreamKeys.size === 0 || allowedStreamKeys.has(stream.name)
        );
      });

      const streams = await Promise.all(
        filteredStreams.map(async (stream) => {
          const viewerMetrics = await getViewerMetricsForStream(
            stream.name,
            req.organization.id,
          );

          const liveStartedAt = liveStartedAtByKey.get(String(stream.name));
          const uptimeSeconds =
            stream.publish?.active && liveStartedAt
              ? Math.max(
                  0,
                  Math.floor(
                    (Date.now() - new Date(liveStartedAt).getTime()) / 1000,
                  ),
                )
              : 0;

          const isPrimaryLiveInput =
            stream.publish?.active && String(stream.app || "live") === "live";

          let mediaAnalysis = null;
          if (isPrimaryLiveInput) {
            const analyzerStream = {
              ...stream,
              uptime_seconds: uptimeSeconds,
            };
            scheduleLiveStreamAnalysis({
              stream: analyzerStream,
              internalHlsBaseUrl: SRS_INTERNAL_HLS_BASE_URL,
            });
            mediaAnalysis = getCachedStreamAnalysis(analyzerStream);
          }

          return {
            ...stream,
            srs_clients: Number(stream.clients || 0),
            clients: viewerMetrics.active_viewers,
            viewerMetrics,
            uptime_seconds: uptimeSeconds,
            mediaAnalysis,
            encoderGeneration:
              bitrateCapEncoderGeneration.get(stream.name) || 0,
            liveStartedAtMs: liveStartedAt
              ? new Date(liveStartedAt).getTime()
              : 0,
          };
        }),
      );

      res.json({
        ok: true,
        srs_available: true,
        streams,
      });
    } catch (error) {
      // SRS not reachable - fall back to DB is_live
      try {
        const liveResult = await pool.query(
          `SELECT stream_key, name, live_started_at,
                  EXTRACT(EPOCH FROM (NOW() - live_started_at))::int AS uptime_seconds
           FROM channels WHERE organization_id = $1 AND is_live = TRUE`,
          [req.organization.id],
        );
        const dbStreams = liveResult.rows.map((ch) => ({
          id: ch.stream_key,
          name: ch.stream_key,
          publish: { active: true, active_age: ch.uptime_seconds || 0 },
          clients: 0,
          kbps: { recv_30s: 0 },
          frames: 0,
          source: "db_webhook",
          encoderGeneration:
            bitrateCapEncoderGeneration.get(ch.stream_key) || 0,
          liveStartedAtMs: ch.live_started_at
            ? new Date(ch.live_started_at).getTime()
            : 0,
        }));
        return res.json({
          ok: true,
          srs_available: false,
          streams: dbStreams,
          message: "Stream status from DB webhook.",
        });
      } catch (dbErr) {
        console.warn("SRS and DB fallback both failed:", dbErr.message);
      }
      res.json({
        ok: true,
        srs_available: false,
        streams: [],
        message: "SRS not reachable.",
      });
    }
  },
);

// ── Helper: get plan limits for an org ────────────────────────────
async function getOrgStreamingPlan(organizationId) {
  try {
    // Previously queried a `subscription_plans` table that — confirmed via
    // a direct DB check — does not exist at all ("relation does not
    // exist"). Its error was silently swallowed by the catch below,
    // meaning EVERY organization got transcoding_enabled: false and
    // max_concurrent_streams: 1 regardless of actual plan, on every single
    // broadcast — a real, live bug (e.g. a Premium org with 5 channels
    // could never have more than 1 simultaneously live). Fixed to use the
    // real `plans` table with the same safe LEFT JOIN + COALESCE fallback
    // used elsewhere. max_channels doubles as the concurrent-live-stream
    // ceiling since there's no separate "concurrent streams" concept
    // anywhere else in the plan model — a plan's channel count is what it
    // can have live at once.
    const result = await pool.query(
      `
      SELECT
        COALESCE(s.plan_key, o.subscription_plan, 'starter') AS plan_key,
        COALESCE(o.transcoding_override, p.transcoding_enabled) AS transcoding_enabled,
        p.max_channels,
        p.max_channels AS max_concurrent_streams
      FROM organizations o
      LEFT JOIN subscriptions s ON s.organization_id = o.id
      JOIN plans p ON p.plan_key = COALESCE(s.plan_key, o.subscription_plan, 'starter')
      WHERE o.id = $1
      `,
      [organizationId],
    );
    return (
      result.rows[0] || {
        transcoding_enabled: false,
        max_concurrent_streams: 1,
      }
    );
  } catch (err) {
    console.error(
      `[PLAN] Failed to look up streaming plan for org ${organizationId}, defaulting to most restrictive limits:`,
      err.message,
    );
    return { transcoding_enabled: false, max_concurrent_streams: 1 };
  }
}

// ── Helper: count currently live streams for an org ───────────────
async function getActiveLiveCount(organizationId, excludeStreamKey = null) {
  const result = await pool.query(
    `SELECT COUNT(*)::int AS count
     FROM channels
     WHERE organization_id = $1
       AND is_live = TRUE
       AND ($2::text IS NULL OR stream_key <> $2)`,
    [organizationId, excludeStreamKey],
  );
  return result.rows[0]?.count || 0;
}

// ── Helper: auto-transcode using FFmpeg ───────────────────────────
// Same exec()-to-spawn() fix as autoCapBitrateStream below applies here
// too — exec() buffers all output in memory and kills the process once
// that buffer fills, which any long-running ffmpeg process will
// eventually hit. This pre-existed the bitrate-cap work but shares the
// identical flaw, so it's fixed here at the same time.
const activeTranscodeProcesses = new Map(); // key: `${streamKey}:${label}`
const transcodeStartupLocks = new Map(); // key -> broadcast generation currently starting
const transcodeRetryCount = new Map(); // key: `${streamKey}:${label}`
const MAX_TRANSCODE_RETRIES = 10;

// NOTE: bitrateCapGeneration, bitrateCapEncoderGeneration, and
// isServerLoadTooHighForNewTranscode are declared further down this file
// but are only ever referenced here from inside function bodies invoked at
// runtime (never at module-load time), so by the time any of these actually
// run, every top-level const below has already been initialized.
// Real bug found via ChatGPT review (2026-08-03): this was called a "full"
// crash dump, but it only ever received `stderrTail` — already capped to
// 50KB in spawnFfmpegVariant below — so on a fast crash it could still
// miss ffmpeg's actual first error line if a lot of encoder-stats output
// came before it. Replaced with a real streaming-to-disk approach: every
// byte ffmpeg writes to stderr is written to this file AS IT ARRIVES, not
// reconstructed from an in-memory tail afterward.
const FFMPEG_CRASH_LOG_DIR = "/tmp/ffmpeg-crash-logs";

const createFfmpegLogFile = (label, streamKey) => {
  try {
    fs.mkdirSync(FFMPEG_CRASH_LOG_DIR, { recursive: true });
    const safeLabel = String(label).replace(/[^a-zA-Z0-9_-]/g, "_");
    const safeStreamKey = String(streamKey).replace(/[^a-zA-Z0-9_-]/g, "_");
    const filePath = path.join(
      FFMPEG_CRASH_LOG_DIR,
      `${safeLabel}-${safeStreamKey}-${Date.now()}.log`,
    );
    return { filePath, stream: fs.createWriteStream(filePath, { flags: "a" }) };
  } catch (err) {
    console.error(
      `[FFMPEG-LOG] Unable to create log file for ${label}/${streamKey}:`,
      err.message,
    );
    return { filePath: null, stream: null };
  }
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// ══════════════════════════════════════════
// SRS SOURCE READINESS GATE (added 2026-08-03, per ChatGPT review)
// Real gap found: retry decisions only ever checked `channels.is_live` in
// our own DB — which stays true through brief encoder reconnects, SRS
// source-setup delays, and other states where the raw stream isn't
// actually readable yet. That's a plausible contributor to ffmpeg's
// "Input/output error" crashes on this input — spawning against a source
// that LOOKS live in our DB but isn't actually delivering media yet.
// This asks SRS itself, not our DB, whether the raw stream is genuinely
// active and has actually received media before we spawn/retry ffmpeg
// against it.
// ══════════════════════════════════════════
async function getSrsRawStream(streamKey) {
  const response = await fetch(`${SRS_API_URL}/api/v1/streams/`, {
    signal: AbortSignal.timeout(5000),
  });
  if (!response.ok) {
    throw new Error(`SRS streams API returned HTTP ${response.status}`);
  }
  const data = await response.json();
  return (data.streams || []).find(
    (stream) =>
      stream.name === streamKey &&
      (!stream.app || stream.app === "live") &&
      stream.publish?.active,
  );
}

async function waitForSrsRawStreamReady(
  streamKey,
  generation,
  { timeoutMs = 30000, pollMs = 1000 } = {},
) {
  const deadline = Date.now() + timeoutMs;
  let lastReason = "stream not found";
  let previousFrames = null;
  let previousRecvBytes = null;
  let observedAdvancement = false;

  while (Date.now() < deadline) {
    if (bitrateCapGeneration.get(streamKey) !== generation) {
      return {
        ready: false,
        superseded: true,
        reason: "broadcast generation was superseded",
      };
    }

    try {
      const stream = await getSrsRawStream(streamKey);

      if (!stream) {
        previousFrames = null;
        previousRecvBytes = null;
        observedAdvancement = false;
        lastReason = "raw stream is not actively published in SRS";
      } else {
        const recv30s = Number(
          stream.kbps?.recv_30s ?? stream.kbps?.recv30s ?? 0,
        );
        const frames = Number(stream.frames || 0);
        const recvBytes = Number(stream.recv_bytes || 0);
        const hasVideoMetadata = Boolean(stream.video?.codec);
        const hasAudioMetadata = Boolean(stream.audio?.codec);

        if (
          previousFrames !== null &&
          previousRecvBytes !== null &&
          (frames > previousFrames || recvBytes > previousRecvBytes)
        ) {
          observedAdvancement = true;
        }

        previousFrames = frames;
        previousRecvBytes = recvBytes;

        // SRS does not guarantee that its public counters refresh on every
        // one-second API poll. Requiring three consecutive counter increases
        // can therefore keep a healthy OBS source blocked forever. Instead,
        // require valid codec metadata, non-zero media counters/bitrate, and
        // at least one observed advancement during this readiness window.
        const mediaReady =
          hasVideoMetadata &&
          hasAudioMetadata &&
          recv30s > 0 &&
          frames > 0 &&
          recvBytes > 0 &&
          observedAdvancement;

        if (mediaReady) {
          return { ready: true, stream };
        }

        lastReason =
          `waiting for usable codec/media sample ` +
          `(advanced=${observedAdvancement}, recv_30s=${recv30s}kbps, ` +
          `frames=${frames}, video=${stream.video?.codec || "none"}, ` +
          `audio=${stream.audio?.codec || "none"})`;
      }
    } catch (err) {
      lastReason = err.message;
    }

    await sleep(pollMs);
  }

  return { ready: false, superseded: false, reason: lastReason };
}

// SRS may fire on_publish before codec metadata and the first keyframe are
// fully available. waitForSrsRawStreamReady() remains the startup gate; FFmpeg
// then reads the raw broadcast from SRS's local HLS endpoint. This avoids the
// production failure where RTMP playback connected successfully but delivered
// no media frames, while keeping readiness tied to the authoritative SRS
// publisher state rather than merely trusting the database flag.

// Detects an ffmpeg process that spawned successfully but never actually
// starts publishing to SRS — the exact failure mode found 2026-08-05: three
// ffmpeg processes sat alive with 0:00 CPU time for 15+ minutes, never
// exiting and never producing output, which made every existing check
// ("is the process object still alive?") report them as healthy forever.
// Only killing the zombie lets the periodic reconciler notice the rendition
// is genuinely missing and retry — this function does NOT retry itself, to
// avoid duplicating that logic.
const RENDITION_STARTUP_WATCHDOG_MS = 45000;
const RENDITION_STARTUP_POLL_MS = 2000;

async function watchRenditionStartupOrKill(
  retryKey,
  label,
  streamKey,
  proc,
  generation,
) {
  const renditionStreamName = `${streamKey}_${label}`;
  const deadline = Date.now() + RENDITION_STARTUP_WATCHDOG_MS;

  while (Date.now() < deadline) {
    await sleep(RENDITION_STARTUP_POLL_MS);

    // Stop watching if something else already superseded/replaced this attempt.
    if (activeTranscodeProcesses.get(retryKey) !== proc) return;
    if (bitrateCapGeneration.get(streamKey) !== generation) return;
    if (proc.exitCode !== null || proc.killed) return; // exited on its own — normal close handler deals with it

    const activeNow = await getSrsRawStream(renditionStreamName).catch(
      () => null,
    );
    if (activeNow) {
      console.log(
        `[Transcode] ${label} confirmed publishing for ${streamKey}; watchdog standing down.`,
      );
      return;
    }
  }

  if (activeTranscodeProcesses.get(retryKey) !== proc) return;
  if (bitrateCapGeneration.get(streamKey) !== generation) return;
  if (proc.exitCode !== null || proc.killed) return;

  console.warn(
    `[Transcode] ${label} for ${streamKey} produced no publish within ${RENDITION_STARTUP_WATCHDOG_MS}ms of spawning (pid ${proc.pid}) — treating as hung and killing it so the reconciler can retry.`,
  );
  activeTranscodeProcesses.delete(retryKey);
  proc.kill("SIGTERM");
  const forceKillTimer = setTimeout(() => {
    if (proc.exitCode === null) {
      console.warn(
        `[Transcode] Force-killing ${retryKey} after watchdog SIGTERM timeout.`,
      );
      proc.kill("SIGKILL");
    }
  }, 5000);
  forceKillTimer.unref?.();
}

const spawnFfmpegVariant = async (label, streamKey, args, generation) => {
  const retryKey = `${streamKey}:${label}`;

  // The initial on_publish path, the periodic reconciler, and a delayed crash
  // retry can all notice the same missing rendition at nearly the same time.
  // Only one startup attempt may own a rendition for a broadcast generation.
  // Without this lock, two attempts can both pass readiness, and the later one
  // kills/replaces the healthy process started by the first. SRS then rejects
  // the duplicate publisher with an RTMP Input/output error, creating a retry
  // storm and eventually leaving WatchPage stuck on "Preparing stream".
  const startupGeneration = transcodeStartupLocks.get(retryKey);
  if (startupGeneration === generation) {
    console.debug(
      `[Transcode] ${label} startup already in progress for ${streamKey}; skipping duplicate request.`,
    );
    return;
  }

  const existingProcess = activeTranscodeProcesses.get(retryKey);
  if (
    existingProcess &&
    existingProcess.exitCode === null &&
    !existingProcess.killed
  ) {
    console.debug(
      `[Transcode] ${label} is already running for ${streamKey}; skipping duplicate startup.`,
    );
    return;
  }

  transcodeStartupLocks.set(retryKey, generation);

  try {
    // Abandon delayed work from an older OBS/broadcast session.
    if (bitrateCapGeneration.get(streamKey) !== generation) {
      console.log(
        `[Transcode] Skipping superseded ${label} session for ${streamKey} (a newer broadcast session has since started).`,
      );
      return;
    }

    const readiness = await waitForSrsRawStreamReady(streamKey, generation);
    if (!readiness.ready) {
      if (!readiness.superseded) {
        // This is a recoverable startup race. The periodic ABR reconciler will
        // try again while the raw publisher remains live, so do not place a
        // transient condition in the Super Admin Recent Errors panel.
        console.warn(
          `[Transcode] ${label} startup deferred for ${streamKey}: raw SRS source is not ready yet — ${readiness.reason}`,
        );
      }
      return;
    }

    // Re-check after the asynchronous readiness wait. Another path may have
    // completed startup while this attempt was waiting.
    const processAfterReadiness = activeTranscodeProcesses.get(retryKey);
    if (
      processAfterReadiness &&
      processAfterReadiness.exitCode === null &&
      !processAfterReadiness.killed
    ) {
      console.debug(
        `[Transcode] ${label} became active for ${streamKey} while readiness was being checked; skipping duplicate spawn.`,
      );
      return;
    }

    if (isServerLoadTooHighForNewTranscode()) {
      console.warn(
        `[Transcode] Server load too high — deferring ${label} for ${streamKey}.`,
      );
      return;
    }

    // Close the final readiness-to-spawn race. The source may unpublish after
    // waitForSrsRawStreamReady() succeeds but before FFmpeg is spawned.
    const sourceStillLive = await getSrsRawStream(streamKey).catch((err) => {
      console.warn(
        `[Transcode] Final SRS source check failed for ${label}/${streamKey}:`,
        err.message,
      );
      return null;
    });

    if (
      !sourceStillLive ||
      bitrateCapGeneration.get(streamKey) !== generation
    ) {
      console.warn(
        `[Transcode] Aborting ${label} startup for ${streamKey}: source disappeared or broadcast was superseded before FFmpeg spawn.`,
      );
      return;
    }

    // The readiness gate observed the raw source actively delivering media
    // (a single passing check, not repeated confirmations — see
    // waitForSrsRawStreamReady's hasMedia check above). Keep a short buffer
    // so all three rendition subscribers do not attach on the exact same
    // millisecond, without adding unnecessary delay before spawn now that
    // analyzeduration/probesize are back to their smaller, fast-start values.
    await sleep(500);

    if (bitrateCapGeneration.get(streamKey) !== generation) return;

    const processBeforeSpawn = activeTranscodeProcesses.get(retryKey);
    if (
      processBeforeSpawn &&
      processBeforeSpawn.exitCode === null &&
      !processBeforeSpawn.killed
    ) {
      console.debug(
        `[Transcode] ${label} is already active for ${streamKey}; cancelling duplicate FFmpeg spawn.`,
      );
      return;
    }

    const inputIndex = args.indexOf("-i");
    const configuredInput =
      inputIndex >= 0 && inputIndex + 1 < args.length
        ? String(args[inputIndex + 1])
        : "";

    console.log(
      `[Transcode] Spawning ${label} for ${streamKey} with media input ` +
        `${JSON.stringify(configuredInput)}.`,
    );

    const proc = spawn("ffmpeg", args, {
      stdio: ["ignore", "ignore", "pipe"],
    });
    activeTranscodeProcesses.set(retryKey, proc);

    // The startup lock is no longer needed after the process has been recorded.
    if (transcodeStartupLocks.get(retryKey) === generation) {
      transcodeStartupLocks.delete(retryKey);
    }

    // Guards against the exact hang found 2026-08-05: ffmpeg spawning but
    // never actually publishing, which every other check in this file reads
    // as "healthy" since the process object never exits on its own.
    watchRenditionStartupOrKill(
      retryKey,
      label,
      streamKey,
      proc,
      generation,
    ).catch((err) =>
      console.error(
        `[Transcode] Startup watchdog failed for ${retryKey}:`,
        err.message,
      ),
    );

    bitrateCapEncoderGeneration.set(
      streamKey,
      (bitrateCapEncoderGeneration.get(streamKey) || 0) + 1,
    );

    const { filePath: ffmpegLogPath, stream: ffmpegLogStream } =
      createFfmpegLogFile(label, streamKey);

    let stderrTail = "";
    proc.stderr.on("data", (chunk) => {
      const chunkText = chunk.toString();
      if (ffmpegLogStream && !ffmpegLogStream.destroyed) {
        ffmpegLogStream.write(chunkText);
      }
      stderrTail += chunkText;
      if (stderrTail.length > 50000) stderrTail = stderrTail.slice(-50000);
    });

    proc.stderr.on("error", (err) => {
      console.error(
        `[FFMPEG-LOG] stderr read failed for ${label}/${streamKey}:`,
        err.message,
      );
    });

    proc.on("error", (err) => {
      console.error(
        `[Transcode] ${label} failed to spawn for ${streamKey}:`,
        err.message,
      );
    });

    proc.on("close", async (code, signal) => {
      if (ffmpegLogStream && !ffmpegLogStream.destroyed) {
        ffmpegLogStream.end();
      }

      if (activeTranscodeProcesses.get(retryKey) === proc) {
        activeTranscodeProcesses.delete(retryKey);
      }

      if (code === 0 || code === null) {
        if (ffmpegLogPath) fs.promises.unlink(ffmpegLogPath).catch(() => {});
        return;
      }

      // A newer broadcast session owns this stream now.
      if (bitrateCapGeneration.get(streamKey) !== generation) return;

      // If another legitimate path already installed a replacement process,
      // this old process closing must not start a second retry chain.
      const replacementProcess = activeTranscodeProcesses.get(retryKey);
      if (
        replacementProcess &&
        replacementProcess !== proc &&
        replacementProcess.exitCode === null &&
        !replacementProcess.killed
      ) {
        console.debug(
          `[Transcode] ${label} replacement is already running for ${streamKey}; suppressing stale retry.`,
        );
        return;
      }

      console.warn(
        `[Transcode] ${label} exited with code ${code}${signal ? ` (signal ${signal})` : ""} for ${streamKey} — evaluating retry` +
          (ffmpegLogPath ? ` (full stderr written to ${ffmpegLogPath})` : "") +
          `\n--- ffmpeg stderr (last ~1500 chars) ---\n${stderrTail.slice(-1500)}`,
      );

      try {
        const readinessForRetry = await waitForSrsRawStreamReady(
          streamKey,
          generation,
          { timeoutMs: 15000, pollMs: 1000 },
        );

        if (!readinessForRetry.ready) {
          if (!readinessForRetry.superseded) {
            console.warn(
              `[Transcode] Not retrying ${label} for ${streamKey}: raw source unavailable in SRS — ${readinessForRetry.reason}`,
            );
          }
          return;
        }

        const processBeforeRetry = activeTranscodeProcesses.get(retryKey);
        if (
          processBeforeRetry &&
          processBeforeRetry.exitCode === null &&
          !processBeforeRetry.killed
        ) {
          console.debug(
            `[Transcode] ${label} recovered through another path for ${streamKey}; retry cancelled.`,
          );
          return;
        }

        const attempts = (transcodeRetryCount.get(retryKey) || 0) + 1;
        transcodeRetryCount.set(retryKey, attempts);

        if (attempts > MAX_TRANSCODE_RETRIES) {
          console.error(
            `[Transcode] Giving up on ${label} for ${streamKey} after ${attempts} failed attempts — the ABR reconciler will continue periodic recovery checks.`,
          );
          notifySlack(`ffmpeg gave up on ABR transcode (${label})`, {
            streamKey,
            label,
            attempts,
            timestamp: new Date().toISOString(),
          });
          return;
        }

        console.warn(
          `[Transcode] ${label} ended unexpectedly while source is still live — retrying (attempt ${attempts}/${MAX_TRANSCODE_RETRIES}) for ${streamKey}`,
        );

        const retryTimer = setTimeout(
          () => spawnFfmpegVariant(label, streamKey, args, generation),
          3000,
        );
        retryTimer.unref?.();
      } catch (err) {
        console.error(
          `[Transcode] Failed readiness check for ${streamKey} before retrying ${label}:`,
          err.message,
        );
      }
    });
  } finally {
    // Clear only the lock belonging to this exact broadcast generation. A
    // newer OBS session may already have installed its own startup lock.
    if (transcodeStartupLocks.get(retryKey) === generation) {
      transcodeStartupLocks.delete(retryKey);
    }
  }
};

// Input-side resilience for the local SRS HLS source. +genpts/discardcorrupt and
// avoid_negative_ts guard against timestamp irregularities inherited from OBS
// that could otherwise create downstream HLS/MSE discontinuities.
//
// Deliberately NO -rw_timeout here: an internal ffmpeg read/write timeout
// races against the external watchRenditionStartupOrKill() watchdog
// (RENDITION_STARTUP_WATCHDOG_MS, currently 45s) — whichever fires first
// wins, and a shorter internal timeout silently pre-empts the external one
// every time, defeating its clearer logging/reconciler handoff. Confirmed
// live 2026-08-06: adding "-rw_timeout"/"30000000" here caused ffmpeg to
// self-exit at exactly 30s (no SIGTERM in its log) well before the 45s
// external watchdog ever got a chance to act. One hang-detection mechanism
// should own this decision, not two independent timers.
const inputResilienceFlags = [
  "-analyzeduration",
  "3000000",
  "-probesize",
  "1000000",
  "-fflags",
  "+genpts+discardcorrupt",
  "-avoid_negative_ts",
  "make_zero",
];

// Produce a closed, deterministic two-second GOP for the platform's
// supported 30 fps live-ingest profile. This aligns encoder keyframes with
// SRS's two-second HLS fragments and avoids DTS/PTS complexity caused by
// B-frame reordering. The timestamp-based force remains as an additional
// safeguard if an incoming source briefly drifts.
const keyframeAlignmentFlags = [
  "-g",
  "60",
  "-keyint_min",
  "60",
  "-sc_threshold",
  "0",
  "-force_key_frames",
  "expr:gte(t,n_forced*2)",
  "-bf",
  "0",
];

// ══════════════════════════════════════════
// PLAN-DRIVEN ABR RENDITION LADDER (2026-08-03)
// Replaces the old split between autoTranscodeStream (fixed 720p/480p,
// Deluxe/Premium only) and autoCapBitrateStream (a separate bespoke hard-
// cap ffmpeg process for Essential, republishing to a distinct
// "live_capped" app). That split was also the single largest source of
// incidents in this codebase's history (crash-loop investigation, the
// sticky-app-not-resetting bug, the race-tolerance tuning all traced back
// to the bespoke bitrate-cap process specifically).
//
// Every plan now gets a real, bitrate-BOUNDED (-maxrate/-bufsize, not just
// -b:v) rendition ladder built from the exact same proven
// spawnFfmpegVariant path, deliberately matching how Wowza's own
// Transcoder works (every rung is a genuine encode at a paired
// resolution+bitrate) rather than the old "unbounded raw passthrough as
// top rendition" behavior, which also silently didn't enforce the
// bitrate ceilings advertised on the pricing page for Deluxe/Premium.
// Deliberately NO raw/unbounded fallback rendition for any plan — if
// every rendition's ffmpeg process dies at once (has happened — see the
// 2026-08-02 incident notes), that stream is briefly unavailable until
// they recover, same as Wowza, rather than quietly falling back to an
// unbounded stream that undercuts the whole point of a bitrate ceiling.
// ══════════════════════════════════════════

// Essential-tier's single rendition downscales resolution to match its
// bitrate ceiling (rather than keeping full source resolution at a
// starved bitrate) — deliberate choice, matches how a real ABR rung
// pairs resolution+bitrate together instead of the old bitrate-cap
// behavior of capping bitrate alone at whatever resolution the source
// happened to be.
function pickResolutionForBitrate(bitrateKbps) {
  if (bitrateKbps >= 6000) return "1920x1080";
  if (bitrateKbps >= 1800) return "1280x720";
  return "854x480";
}

async function getRenditionPlanForOrg(organizationId) {
  const plan = await getOrgStreamingPlan(organizationId);
  const capKbps = await getOrgMaxBitrateKbps(organizationId);

  if (plan.transcoding_enabled) {
    // Deluxe/Premium: full 3-rung ladder. Top rung is now bounded at the
    // org's actual plan bitrate ceiling (10Mb Deluxe / 15Mb Premium)
    // instead of being an unbounded raw passthrough; 720p/480p stay at
    // their existing fixed presets underneath it.
    return [
      {
        label: "top",
        bitrateKbps: capKbps || 3500,
        resolution: "1920x1080",
      },
      { label: "720p", bitrateKbps: 2500, resolution: "1280x720" },
      { label: "480p", bitrateKbps: 1200, resolution: "854x480" },
    ];
  }

  if (capKbps) {
    // Essential: single rendition, resolution matched to its bitrate
    // ceiling — a one-rung ABR ladder rather than a separate mechanism.
    return [
      {
        label: "top",
        bitrateKbps: capKbps,
        resolution: pickResolutionForBitrate(capKbps),
      },
    ];
  }

  return []; // no plan/cap resolved — nothing to spawn
}

// Diagnostic logging (added 2026-08-03, per ChatGPT review): verbose by
// default (cheap, no packet-level spam via -nostats), full debug only for
// streams explicitly listed in FFMPEG_DEBUG_STREAMS (comma-separated
// stream keys) — set via .env + `pm2 restart <name> --update-env`, not
// left on globally, since debug-level ffmpeg output is heavy for a
// long-running healthy stream.
function getFfmpegLogLevel(streamKey) {
  const debugStreams = new Set(
    String(process.env.FFMPEG_DEBUG_STREAMS || "")
      .split(",")
      .map((value) => value.trim())
      .filter(Boolean),
  );
  return debugStreams.has(streamKey)
    ? "repeat+level+debug"
    : "repeat+level+verbose";
}

function buildRenditionFfmpegArgs(
  streamKey,
  input,
  output,
  { bitrateKbps, resolution },
) {
  return [
    "-y",
    "-hide_banner",
    "-nostats",
    "-loglevel",
    getFfmpegLogLevel(streamKey),
    ...inputResilienceFlags,
    "-i",
    input,
    "-map",
    "0:v:0",
    "-map",
    "0:a:0?",
    "-c:v",
    "libx264",
    "-preset",
    "veryfast",
    ...keyframeAlignmentFlags,
    "-b:v",
    `${bitrateKbps}k`,
    // -maxrate/-bufsize make this a genuine HARD ceiling, not just a
    // target — this is what the old bitrate-cap process did that the
    // original 720p/480p renditions didn't; applying it uniformly here
    // means every rung's advertised bitrate is now actually enforced.
    "-maxrate",
    `${bitrateKbps}k`,
    "-bufsize",
    `${bitrateKbps * 2}k`,
    "-s",
    resolution,
    "-c:a",
    "aac",
    "-b:a",
    bitrateKbps >= 2000 ? "128k" : "96k",

    // Smooth minor source-clock drift instead of carrying timestamp gaps
    // into the republished RTMP rendition.
    "-af",
    "aresample=async=1:first_pts=0",

    // Keep the output timeline constant at the supported 30 fps profile.
    // This gives SRS predictable frame timing and segment boundaries.
    "-fps_mode",
    "cfr",
    "-r",
    "30",

    "-f",
    "flv",
    output,
  ];
}

function getInternalHlsSourceUrl(streamKey) {
  return (
    `${SRS_INTERNAL_HLS_BASE_URL.replace(/\/$/, "")}/live/` +
    `${encodeURIComponent(streamKey)}.m3u8`
  );
}

function spawnRenditionsForStream(streamKey, renditions, generation) {
  // Consume the raw source through SRS's local HLS endpoint. Production
  // verification showed this path immediately exposes valid H.264/AAC media,
  // while local RTMP playback can connect yet remain frame-starved indefinitely.
  const input = getInternalHlsSourceUrl(streamKey);

  renditions.forEach((rendition, index) => {
    const output = `rtmp://127.0.0.1:1935/live/${streamKey}_${rendition.label}`;
    const args = buildRenditionFfmpegArgs(streamKey, input, output, rendition);

    // Stagger subscribers slightly. Starting three HLS readers at precisely
    // the same instant was correlated with the first-session startup failure
    // on this SRS host. Each variant still performs its own readiness check,
    // lock, watchdog, and retry.
    const startDelayMs = index * 1250;
    const timer = setTimeout(() => {
      console.log(
        `[ABR] Starting ${rendition.label} (${rendition.bitrateKbps}kbps, ${rendition.resolution}) for: ${streamKey}`,
      );
      spawnFfmpegVariant(rendition.label, streamKey, args, generation);
    }, startDelayMs);
    timer.unref?.();
  });
}

// Periodic ABR reconciliation. This recovers the exact failure mode where a
// raw source is live but the initial startup path did not leave a running
// rendition process (for example after an SRS/backend restart race). It also
// recovers active broadcasts after the Node process itself restarts.
const abrRecoveryLastAttempt = new Map();
const ABR_RECOVERY_COOLDOWN_MS = 15000;

async function reconcileAbrTranscoders() {
  try {
    const response = await fetch(
      `${SRS_API_URL.replace(/\/$/, "")}/api/v1/streams/`,
      {
        signal: AbortSignal.timeout(5000),
      },
    );
    if (!response.ok) {
      throw new Error(`SRS streams API returned HTTP ${response.status}`);
    }

    const payload = await response.json();
    const activeStreams = (payload.streams || []).filter(
      (item) => item.publish?.active,
    );
    const activeNames = new Set(activeStreams.map((item) => item.name));
    const rawStreams = activeStreams.filter(
      (item) => !isAbrRenditionStreamKey(item.name),
    );

    for (const raw of rawStreams) {
      const streamKey = raw.name;
      if (!streamKey) continue;

      const channelResult = await pool.query(
        `SELECT organization_id FROM channels WHERE stream_key = $1 LIMIT 1`,
        [streamKey],
      );
      const organizationId = channelResult.rows[0]?.organization_id;
      if (!organizationId) continue;

      const renditions = await getRenditionPlanForOrg(organizationId);
      if (!renditions.length) continue;

      let generation = bitrateCapGeneration.get(streamKey);
      if (!generation) {
        generation = 1;
        bitrateCapGeneration.set(streamKey, generation);
      }

      for (const rendition of renditions) {
        const retryKey = `${streamKey}:${rendition.label}`;
        const proc = activeTranscodeProcesses.get(retryKey);
        const processRunning = proc && proc.exitCode === null && !proc.killed;
        const startupInProgress =
          transcodeStartupLocks.get(retryKey) === generation;
        const renditionPublishing = activeNames.has(
          `${streamKey}_${rendition.label}`,
        );

        if (processRunning || startupInProgress || renditionPublishing)
          continue;

        const now = Date.now();
        const lastAttempt = abrRecoveryLastAttempt.get(retryKey) || 0;
        if (now - lastAttempt < ABR_RECOVERY_COOLDOWN_MS) continue;
        abrRecoveryLastAttempt.set(retryKey, now);

        // A reconciler-driven restart begins a fresh retry budget for this
        // missing rendition instead of inheriting a previously exhausted one.
        transcodeRetryCount.delete(retryKey);

        const input = getInternalHlsSourceUrl(streamKey);
        const output = `rtmp://127.0.0.1:1935/live/${streamKey}_${rendition.label}`;
        const args = buildRenditionFfmpegArgs(
          streamKey,
          input,
          output,
          rendition,
        );

        console.warn(
          `[ABR-RECOVERY] Raw source ${streamKey} is live but ${rendition.label} is missing — starting recovery.`,
        );
        spawnFfmpegVariant(rendition.label, streamKey, args, generation);
      }
    }
  } catch (error) {
    console.error("[ABR-RECOVERY] Reconciliation failed:", error.message);
  }
}

// ══════════════════════════════════════════
// BITRATE HARD CAP (real-time transcoding)
// Chosen over the softer "monitor and warn" approach — every stream gets
// forcibly re-encoded down to its plan's exact bitrate ceiling, published
// back into SRS as a second stream under the "live_capped" app. Viewers
// are served this capped copy (see the /api/hls/ proxy above, which tries
// live_capped first and falls back to raw live/ if no cap is running).
//
// Deliberately does NOT track/kill the spawned ffmpeg process explicitly:
// like the existing 720p/480p auto-transcode above, ffmpeg reading from
// the live source exits/retries through shared lifecycle handling once the source
// disconnects or its HLS input becomes unavailable.
// (on_unpublish), so no separate process-lifecycle management is needed.
//
// SAFETY FALLBACK: before spawning a new transcode, checks current server
// load. If the server is already heavily loaded (many concurrent
// transcodes), skips capping for this NEW stream rather than adding more
// load on top of an already-strained server — that stream is served
// uncapped (raw) instead of failing to start at all. This check only runs
// at stream-start; it does not kill already-running transcodes if load
// spikes afterward, since forcibly interrupting an active broadcast to
// relieve load is a worse failure mode than not capping one stream.
const MAX_LOAD_PER_CORE_FOR_NEW_TRANSCODE = 0.85;

const isServerLoadTooHighForNewTranscode = () => {
  const oneMinuteLoad = os.loadavg()[0];
  const coreCount = os.cpus().length || 1;
  const loadPerCore = oneMinuteLoad / coreCount;

  return loadPerCore > MAX_LOAD_PER_CORE_FOR_NEW_TRANSCODE;
};

// Reliable max-bitrate lookup — deliberately NOT reusing
// getOrgStreamingPlan() above, since that queries a `subscription_plans`
// table that's separate from the `plans` table used everywhere else in
// this integration, and isn't confirmed to be kept in sync. Uses the same
// LEFT JOIN + COALESCE fallback already proven correct in the bitrate
// compliance monitor.
const getOrgMaxBitrateKbps = async (organizationId) => {
  const result = await pool.query(
    `
    SELECT p.max_bitrate_kbps
    FROM organizations o
    LEFT JOIN subscriptions s ON s.organization_id = o.id
    JOIN plans p ON p.plan_key = COALESCE(s.plan_key, o.subscription_plan, 'starter')
    WHERE o.id = $1
    `,
    [organizationId],
  );

  return Number(result.rows[0]?.max_bitrate_kbps || 0);
};

// ══════════════════════════════════════════
// Shared generation counters, used by every rendition (regardless of
// plan/label) spawned via spawnFfmpegVariant above.
// ══════════════════════════════════════════

// Tracks which "broadcast session" is current for a given stream_key. A
// fresh on_publish bumps this; any retry chain from a PRIOR session
// checks this before acting and abandons itself if superseded. Without
// this, a brief encoder reconnect (a new on_publish) while an old failed
// transcode's retry chain is still in-flight would let both chains spawn
// ffmpeg processes publishing to the same output name concurrently — a
// real race that was actually happening (evidenced by the same stream
// logging "giving up" at both attempt 4 AND attempt 5, meaning two
// independent chains were both writing to the same counter).
const bitrateCapGeneration = new Map();
// Bumped every time a NEW ffmpeg process actually spawns for ANY of a
// stream_key's renditions (a crash+retry within the SAME broadcast
// counts — this is deliberately NOT the same thing as bitrateCapGeneration
// above, which only changes on a fresh on_publish). A restart here means
// the underlying encoded video has a genuine cut — new keyframe
// timing/GOP structure — that SRS's HLS output has no way to mark as
// EXT-X-DISCONTINUITY, so hls.js has no warning before it tries (and
// fails) to append across it. Exposed to the frontend via
// /api/public/watch and /api/srs/streams so a viewer's player can
// proactively remount (fresh MediaSource) instead of hitting a
// bufferAppendError reactively.
const bitrateCapEncoderGeneration = new Map();

// ── Helper: auto-sync recordings after stream ends ────────────────
// ══════════════════════════════════════════
// BUNNY STORAGE ARCHIVAL
// Uploads finished recordings to Bunny Storage, organized by
// organization slug and date, then removes the local copy to
// save server disk space.
// ══════════════════════════════════════════
// Resolves which Bunny storage zone credentials to use for a given
// organization — its own dedicated zone if one was provisioned, otherwise
// the shared platform zone (grandfathered orgs, or a new org where
// provisioning didn't succeed). Accepts either a full organization row or
// just the relevant bunny_* fields.
const getOrgBunnyZoneCreds = (org = {}) => {
  const hasOwnZone = Boolean(
    org.bunny_storage_zone_name && org.bunny_storage_zone_password,
  );

  return {
    hasOwnZone,
    zoneCreds: hasOwnZone
      ? {
          zoneName: org.bunny_storage_zone_name,
          hostname: org.bunny_storage_zone_hostname,
          apiKey: org.bunny_storage_zone_password,
        }
      : {},
    cdnBaseUrl: hasOwnZone
      ? org.bunny_recordings_cdn_url
      : BUNNY_RECORDINGS_CDN_URL,
  };
};

const uploadFileToBunnyStorage = async (
  localFilePath,
  remotePath,
  zoneCreds = {},
) => {
  const hostname = zoneCreds.hostname || BUNNY_STORAGE_HOSTNAME;
  const zoneName = zoneCreds.zoneName || BUNNY_STORAGE_ZONE;
  const apiKey = zoneCreds.apiKey || BUNNY_STORAGE_API_KEY;

  const stats = fs.statSync(localFilePath);
  const url = `https://${hostname}/${zoneName}/${remotePath}`;

  const response = await fetch(url, {
    method: "PUT",
    headers: {
      AccessKey: apiKey,
      "Content-Type": "application/octet-stream",
      "Content-Length": String(stats.size),
    },
    body: fs.createReadStream(localFilePath),
    duplex: "half",
  });

  if (!response.ok) {
    const text = await response.text().catch(() => "");
    throw new Error(`Bunny upload failed: ${response.status} ${text}`);
  }

  return true;
};

const deleteFileFromBunnyStorage = async (remotePath, zoneCreds = {}) => {
  const hostname = zoneCreds.hostname || BUNNY_STORAGE_HOSTNAME;
  const zoneName = zoneCreds.zoneName || BUNNY_STORAGE_ZONE;
  const apiKey = zoneCreds.apiKey || BUNNY_STORAGE_API_KEY;

  if (!remotePath || !apiKey) return;

  const url = `https://${hostname}/${zoneName}/${remotePath}`;

  try {
    const response = await fetch(url, {
      method: "DELETE",
      headers: { AccessKey: apiKey },
    });

    if (!response.ok && response.status !== 404) {
      const text = await response.text().catch(() => "");
      console.error(`[BUNNY] Delete failed: ${response.status} ${text}`);
    }
  } catch (err) {
    console.error("[BUNNY] Delete request error:", err.message);
  }
};

const archiveRecordingRow = async (recording) => {
  try {
    await pool.query(
      `UPDATE recordings SET archive_status = 'archiving' WHERE id = $1`,
      [recording.id],
    );

    const orgResult = await pool.query(
      `SELECT slug, bunny_storage_zone_name, bunny_storage_zone_hostname,
              bunny_storage_zone_password, bunny_recordings_cdn_url
       FROM organizations WHERE id = $1`,
      [recording.organization_id],
    );
    const org = orgResult.rows[0] || {};
    const orgSlug = org.slug || `org-${recording.organization_id}`;

    const { hasOwnZone, zoneCreds, cdnBaseUrl } = getOrgBunnyZoneCreds(org);

    const dateSource =
      recording.started_at || recording.created_at || new Date();
    const date = new Date(dateSource);
    const yyyy = date.getFullYear();
    const mm = String(date.getMonth() + 1).padStart(2, "0");
    const dd = String(date.getDate()).padStart(2, "0");

    const fileToArchive = recording.mp4_filepath || recording.filepath;
    const fileNameToArchive = recording.mp4_filename || recording.filename;

    if (!fileToArchive || !fs.existsSync(fileToArchive)) {
      throw new Error("Local file not found for archival");
    }

    const channelSlug = recording.stream_key || "unknown-channel";
    const isManualClip = recording.source === "manual_clip";

    // Manual clips get their own top-level folder so they're easy to
    // find/manage separately from regular auto-archived recordings —
    // requested explicitly, not the existing structure. Own-zone
    // recordings still don't need the org slug (the zone itself is
    // already org-specific); shared-zone ones still need it to keep
    // different orgs' files separated within one zone, same reasoning
    // as the existing path below.
    const remotePath = isManualClip
      ? hasOwnZone
        ? `manual recording/${channelSlug}/${yyyy}/${mm}/${dd}/${fileNameToArchive}`
        : `${orgSlug}/manual recording/${channelSlug}/${yyyy}/${mm}/${dd}/${fileNameToArchive}`
      : hasOwnZone
        ? `${channelSlug}/${yyyy}/${mm}/${dd}/${fileNameToArchive}`
        : `${orgSlug}/${channelSlug}/${yyyy}/${mm}/${dd}/${fileNameToArchive}`;

    await uploadFileToBunnyStorage(fileToArchive, remotePath, zoneCreds);

    // Delete local files now that upload succeeded
    const filesToRemove = [
      recording.filepath,
      recording.mp4_filepath,
      recording.thumbnail_filepath,
    ].filter(Boolean);

    for (const filePath of filesToRemove) {
      if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
    }

    const cdnUrl = `${cdnBaseUrl.replace(/\/$/, "")}/${remotePath}`;

    await pool.query(
      `UPDATE recordings
       SET archive_status = 'archived',
           bunny_storage_path = $1,
           bunny_cdn_url = $2,
           bunny_archived_at = NOW()
       WHERE id = $3`,
      [remotePath, cdnUrl, recording.id],
    );

    console.log(
      `[BUNNY] Archived recording #${recording.id} -> ${remotePath} (${hasOwnZone ? "own zone" : "shared zone"})`,
    );
  } catch (err) {
    console.error(
      `[BUNNY] Failed to archive recording #${recording.id}:`,
      err.message,
    );
    await pool
      .query(`UPDATE recordings SET archive_status = 'failed' WHERE id = $1`, [
        recording.id,
      ])
      .catch(() => {});
  }
};

const archiveReadyRecordingsForOrganization = async (organizationId) => {
  if (!BUNNY_STORAGE_API_KEY) return; // Bunny not configured, skip silently

  const result = await pool.query(
    `SELECT * FROM recordings
     WHERE organization_id = $1
       AND archive_status = 'local'
       AND (file_type = 'mp4' OR mp4_filename IS NOT NULL)
       AND processing_status = 'ready'
    `,
    [organizationId],
  );

  for (const recording of result.rows) {
    await archiveRecordingRow(recording);
  }
};

// Deletes raw local recording files for an organization's channels without
// ever creating `recordings` DB rows or archiving to Bunny — used when the
// org's plan doesn't include recording_enabled, so they don't accumulate
// local disk usage for a feature they haven't paid for. Mirrors the same
// "delete after we're done with it" pattern archiveRecordingRow already
// uses for the paid path.
// Same cleanup as cleanupUnrecordedFilesForOrganization above, but scoped
// to ONE channel's raw files only — used when a channel's own
// auto_record_enabled toggle is off, so it doesn't touch other channels
// in the same org that still have recording on.
const cleanupUnrecordedFilesForChannel = async (streamKey) => {
  try {
    for (const root of [RECORDINGS_LIVE_ROOT, RECORDINGS_LIVE_CAPPED_ROOT]) {
      const streamFolder = path.join(root, streamKey);
      if (
        !fs.existsSync(streamFolder) ||
        !fs.statSync(streamFolder).isDirectory()
      ) {
        continue;
      }

      for (const file of fs.readdirSync(streamFolder)) {
        if (file.endsWith(".tmp") || file.endsWith(".part")) continue;
        const filePath = path.join(streamFolder, file);
        try {
          if (fs.statSync(filePath).isFile()) fs.unlinkSync(filePath);
        } catch (fileErr) {
          console.error(
            `[RECORDING-GATE] Failed to remove ${filePath}:`,
            fileErr.message,
          );
        }
      }
    }
  } catch (err) {
    console.error(
      `[RECORDING-GATE] Per-channel cleanup failed for ${streamKey}:`,
      err.message,
    );
  }
};

const cleanupUnrecordedFilesForOrganization = async (organizationId) => {
  try {
    const allowedChannels = await getAllowedChannelMap(organizationId);

    for (const root of [RECORDINGS_LIVE_ROOT, RECORDINGS_LIVE_CAPPED_ROOT]) {
      if (!fs.existsSync(root)) continue;

      for (const streamName of allowedChannels.keys()) {
        const streamFolder = path.join(root, streamName);
        if (
          !fs.existsSync(streamFolder) ||
          !fs.statSync(streamFolder).isDirectory()
        ) {
          continue;
        }

        for (const file of fs.readdirSync(streamFolder)) {
          if (file.endsWith(".tmp") || file.endsWith(".part")) continue; // still being written
          const filePath = path.join(streamFolder, file);
          try {
            if (fs.statSync(filePath).isFile()) fs.unlinkSync(filePath);
          } catch (fileErr) {
            console.error(
              `[RECORDING-GATE] Failed to remove ${filePath}:`,
              fileErr.message,
            );
          }
        }
      }
    }
  } catch (err) {
    console.error(
      `[RECORDING-GATE] Cleanup failed for org ${organizationId}:`,
      err.message,
    );
  }
};

const STORAGE_QUOTA_ALERT_COOLDOWN_HOURS = 24;
const STORAGE_QUOTA_WARNING_THRESHOLD = 0.8; // 80% — "approaching" tier

// Warns an org as they approach (80%) and then exceed (100%+) their plan's
// storage cap. Does NOT delete or block anything — recorded church/
// ministry services are real content, not a technical setting like
// bitrate, so this only notifies; overage is tracked for billing to
// invoice later, not enforced here. A cooldown per tier prevents
// re-alerting on every single new recording once already past a
// threshold. Each tier fires at most once per cooldown window, and both
// an in-app alert (plan_alerts, surfaced via /api/organization/alerts)
// and an email to the org's admins go out together — acknowledging the
// in-app alert does not mean the email was ever received, so email
// success is tracked separately via email_sent/email_sent_at.
const checkStorageQuota = async (organizationId, summary) => {
  if (!summary) return;

  const maxBytes = Number(summary.max_storage_gb || 0) * 1024 ** 3;
  const usedBytes = Number(summary.used_storage_bytes || 0);

  if (!maxBytes || usedBytes < maxBytes * STORAGE_QUOTA_WARNING_THRESHOLD) {
    return;
  }

  const isOverQuota = usedBytes >= maxBytes;
  const alertType = isOverQuota
    ? "storage_quota_warning"
    : "storage_quota_approaching";

  const recentAlert = await pool.query(
    `
    SELECT 1 FROM plan_alerts
    WHERE organization_id = $1
      AND alert_type = $2
      AND created_at > NOW() - INTERVAL '${STORAGE_QUOTA_ALERT_COOLDOWN_HOURS} hours'
    LIMIT 1
    `,
    [organizationId, alertType],
  );

  if (recentAlert.rows[0]) return; // already warned recently, don't spam

  const usedGb = (usedBytes / 1024 ** 3).toFixed(1);
  const maxGb = Number(summary.max_storage_gb || 0);
  const overageBytes = isOverQuota ? usedBytes - maxBytes : null;

  const message = isOverQuota
    ? `You've used ${usedGb}GB of your plan's ${maxGb}GB recording storage limit. New recordings will keep archiving normally — the overage will be billed separately. Delete old recordings or upgrade your plan to bring usage back within your limit.`
    : `You've used ${usedGb}GB of your plan's ${maxGb}GB recording storage limit (${Math.round((usedBytes / maxBytes) * 100)}%). No action needed yet — this is a heads-up before you reach your limit.`;

  const insertResult = await pool.query(
    `
    INSERT INTO plan_alerts (organization_id, alert_type, message, overage_bytes)
    VALUES ($1, $2, $3, $4)
    RETURNING id
    `,
    [organizationId, alertType, message, overageBytes],
  );

  console.log(
    `[STORAGE] ${isOverQuota ? "Overage" : "Approaching-quota"} alert recorded for org ${organizationId} (${usedGb}GB / ${maxGb}GB)`,
  );

  const adminEmails = await getOrganizationAdminEmails(organizationId);
  if (adminEmails) {
    const subject = isOverQuota
      ? "Storage limit reached — recording storage overage"
      : "Approaching your recording storage limit";

    const sent = await sendMailgunEmail({
      to: adminEmails,
      subject,
      text: message,
    });

    if (sent) {
      await pool.query(
        `UPDATE plan_alerts SET email_sent = TRUE, email_sent_at = NOW() WHERE id = $1`,
        [insertResult.rows[0].id],
      );
    }
  }
};

// ══════════════════════════════════════════
// BANDWIDTH QUOTA MONITOR (CDN + egress)
// Only meaningful for orgs with their own dedicated Bunny zones (see
// per-org zone provisioning) — Bunny's statistics are per-zone, so a
// grandfathered org sharing the platform zone can't be measured
// individually at all. Runs on a slow interval (bandwidth is a monthly
// quota, not a real-time concern like bitrate), checking usage since the
// start of the current calendar month.
//
// Same non-destructive philosophy as storage: WARNS only. Unlike
// storage, bandwidth already delivered can't be undone, and the only
// real "hard" enforcement option (disabling the pull zone) would take a
// church's public watch page and replay library offline entirely until
// next month or an upgrade — a much harsher action than anything else
// built so far. Not doing that without an explicit decision to.
// ══════════════════════════════════════════
const BANDWIDTH_POLL_INTERVAL_MS = 6 * 60 * 60 * 1000; // every 6 hours
const BANDWIDTH_ALERT_COOLDOWN_HOURS = 24;

const checkBandwidthQuotaForZone = async ({
  organizationId,
  pullZoneId,
  usedBytes,
  maxGb,
  alertType,
  label,
}) => {
  const maxBytes = Number(maxGb || 0) * 1024 ** 3;
  if (!pullZoneId || !maxBytes || usedBytes <= maxBytes) return;

  const recentAlert = await pool.query(
    `
    SELECT 1 FROM plan_alerts
    WHERE organization_id = $1
      AND alert_type = $2
      AND created_at > NOW() - INTERVAL '${BANDWIDTH_ALERT_COOLDOWN_HOURS} hours'
    LIMIT 1
    `,
    [organizationId, alertType],
  );

  if (recentAlert.rows[0]) return;

  const usedGb = (usedBytes / 1024 ** 3).toFixed(1);

  await pool.query(
    `INSERT INTO plan_alerts (organization_id, alert_type, message) VALUES ($1, $2, $3)`,
    [
      organizationId,
      alertType,
      `You've used ${usedGb}GB of your plan's ${maxGb}GB monthly ${label} limit. This resets at the start of next month, or upgrade your plan for a higher limit.`,
    ],
  );

  console.log(
    `[BANDWIDTH] ${label} quota warning recorded for org ${organizationId} (${usedGb}GB / ${maxGb}GB)`,
  );
};

const getBunnyBandwidthWithRetry = async (
  pullZoneId,
  dateFrom,
  dateTo,
  retries = 1,
) => {
  let lastError;

  for (let attempt = 0; attempt <= retries; attempt += 1) {
    try {
      return await bunny.getTotalBandwidthUsedBytes(
        pullZoneId,
        dateFrom,
        dateTo,
      );
    } catch (error) {
      lastError = error;
      if (attempt >= retries) throw error;

      console.warn(
        `[BANDWIDTH] Bunny statistics request for zone ${pullZoneId} failed; retrying in 2s: ${error?.cause?.message || error?.cause?.code || error.message}`,
      );
      await sleep(2000);
    }
  }

  throw lastError;
};

const pollBandwidthCompliance = async () => {
  if (!bunny.isBunnyAccountConfigured()) return;

  try {
    const result = await pool.query(
      `
      SELECT o.id AS organization_id, o.bunny_pull_zone_id,
             o.bunny_recordings_pull_zone_id,
             p.max_cdn_bandwidth_gb, p.max_egress_bandwidth_gb
      FROM organizations o
      LEFT JOIN subscriptions s ON s.organization_id = o.id
      JOIN plans p ON p.plan_key = COALESCE(s.plan_key, o.subscription_plan, 'starter')
      WHERE o.bunny_pull_zone_id IS NOT NULL
      `,
    );

    const now = new Date();
    const monthStart = new Date(now.getFullYear(), now.getMonth(), 1);

    for (const org of result.rows) {
      try {
        if (org.bunny_pull_zone_id) {
          const cdnBytes = await getBunnyBandwidthWithRetry(
            org.bunny_pull_zone_id,
            monthStart,
            now,
          );
          await checkBandwidthQuotaForZone({
            organizationId: org.organization_id,
            pullZoneId: org.bunny_pull_zone_id,
            usedBytes: cdnBytes,
            maxGb: org.max_cdn_bandwidth_gb,
            alertType: "cdn_bandwidth_warning",
            label: "CDN bandwidth",
          });
        }

        if (org.bunny_recordings_pull_zone_id) {
          const egressBytes = await getBunnyBandwidthWithRetry(
            org.bunny_recordings_pull_zone_id,
            monthStart,
            now,
          );
          await checkBandwidthQuotaForZone({
            organizationId: org.organization_id,
            pullZoneId: org.bunny_recordings_pull_zone_id,
            usedBytes: egressBytes,
            maxGb: org.max_egress_bandwidth_gb,
            alertType: "egress_bandwidth_warning",
            label: "recording egress bandwidth",
          });
        }
      } catch (orgErr) {
        console.warn(
          `[BANDWIDTH] Temporary check failure for org ${org.organization_id}:`,
          orgErr?.cause?.message || orgErr?.cause?.code || orgErr.message,
        );
      }
    }
  } catch (err) {
    console.warn(
      "[BANDWIDTH] Compliance poll temporarily unavailable:",
      err?.cause?.message || err?.cause?.code || err.message,
    );
  }
};

// Cleans up just the raw (uncapped) DVR output for an org's channels —
// used both for orgs without recording_enabled (cleanupUnrecordedFilesForOrganization
// cleans both roots) and for orgs WITH recording enabled, where the raw
// copy still gets written by SRS's DVR (it fires for both the "live" and
// "live_capped" apps) but is never archived — only the capped copy is.
// Left uncleaned, raw DVR output would otherwise accumulate disk forever
// with no purpose.
const cleanupRawDvrFiles = async (organizationId) => {
  try {
    const allowedChannels = await getAllowedChannelMap(organizationId);
    if (!fs.existsSync(RECORDINGS_LIVE_ROOT)) return;

    for (const streamName of allowedChannels.keys()) {
      const streamFolder = path.join(RECORDINGS_LIVE_ROOT, streamName);
      if (
        !fs.existsSync(streamFolder) ||
        !fs.statSync(streamFolder).isDirectory()
      ) {
        continue;
      }

      for (const file of fs.readdirSync(streamFolder)) {
        if (file.endsWith(".tmp") || file.endsWith(".part")) continue;
        const filePath = path.join(streamFolder, file);
        try {
          if (fs.statSync(filePath).isFile()) fs.unlinkSync(filePath);
        } catch (fileErr) {
          console.error(
            `[RAW-DVR-CLEANUP] Failed to remove ${filePath}:`,
            fileErr.message,
          );
        }
      }
    }
  } catch (err) {
    console.error(
      `[RAW-DVR-CLEANUP] Failed for org ${organizationId}:`,
      err.message,
    );
  }
};

async function autoSyncRecordingsDelayed(
  organizationId,
  delayMs = 8000,
  channelId = null,
  streamKey = null,
) {
  setTimeout(async () => {
    try {
      const summary = await getOrganizationSubscriptionSummary(organizationId);

      if (!summary?.recording_enabled) {
        console.log(
          `[RECORDING-GATE] Org ${organizationId}'s plan (${summary?.plan_key || "unknown"}) does not include recording — cleaning up raw files instead of archiving.`,
        );
        await cleanupUnrecordedFilesForOrganization(organizationId);
        return;
      }

      // Plan allows recording org-wide, but this specific channel may
      // have its own "record all incoming streams" toggle off (Wowza-
      // parity, 2026-08-10) — clean up just this channel's raw files so
      // the scan below finds nothing to archive for it, while leaving
      // every other channel in the org completely unaffected.
      if (channelId && streamKey) {
        const channelResult = await pool.query(
          `SELECT auto_record_enabled FROM channels WHERE id = $1`,
          [channelId],
        );
        if (channelResult.rows[0]?.auto_record_enabled === false) {
          console.log(
            `[RECORDING-GATE] Channel ${channelId} has recording toggled off — cleaning up its raw files instead of archiving.`,
          );
          await cleanupUnrecordedFilesForChannel(streamKey);
        }
      }

      console.log(`[DVR] Auto-syncing recordings for org: ${organizationId}`);
      await scanRecordingFilesForOrganization(organizationId, {
        processReady: true,
      });

      // Archive newly-ready recordings to Bunny Storage (if configured)
      await archiveReadyRecordingsForOrganization(organizationId);

      // Raw (uncapped) DVR output is never archived — only the capped
      // copy is — so clean it up here to avoid it silently filling disk.
      await cleanupRawDvrFiles(organizationId);

      // Storage quota check — re-fetch the summary so used_storage_bytes
      // reflects the recording we just archived. Deliberately a WARNING
      // only, same philosophy as the bitrate monitor: don't delete or
      // block a church's actual recorded content over a storage cap,
      // just flag it so they can clean up old recordings or upgrade.
      // (This is my judgment call, not something explicitly decided in
      // chat — flag if a harder cap is wanted, e.g. blocking new
      // recordings once significantly over quota.)
      const updatedSummary =
        await getOrganizationSubscriptionSummary(organizationId);
      await checkStorageQuota(organizationId, updatedSummary);

      // Notify dashboard via socket
      if (io)
        io.emit("recordings:updated", { organization_id: organizationId });
    } catch (err) {
      console.error("[DVR] Auto-sync error:", err.message);
    }
  }, delayMs);
}

// ══════════════════════════════════════════

// HLS PROXY - forwards viewer HLS requests to local SRS via public URL
const SRS_HLS_ORIGIN = process.env.SRS_HLS_ORIGIN || "http://localhost:8080";

// Segments are immutable once SRS finishes writing them. Without this,
// N concurrent viewers means N separate round trips to the home-network
// SRS origin (through ngrok) for the exact same .ts file, which is the
// main cause of stalling/buffer errors as viewer count grows. Cache each
// segment briefly so only the first request per segment hits the origin.
const segmentCache = new Map(); // segment filename -> { buffer, contentType, cachedAt }
const SEGMENT_CACHE_TTL_MS = 15000;

// CDN-facing (Bunny edge) cache duration for segment responses — separate
// from SEGMENT_CACHE_TTL_MS above, which only controls how long THIS
// Node process holds a segment in memory to absorb a concurrent-viewer
// burst. This constant controls how long Bunny's edge nodes themselves
// cache the response before re-checking with us. Since segments are
// immutable once written AND the cache key already includes the `_s`
// session tag (a filename reused by a NEW broadcast can never resolve to
// a PRIOR session's bytes — see the cacheKey comment below), there's no
// staleness risk from caching much longer than the in-memory TTL above.
// Longer edge caching directly reduces repeat origin round-trips for
// actively-watched streams. Bumped from 30s (2026-08-10, Storage & CDN
// review) — 30s was a safe-but-conservative default from before the
// session-tag staleness fix existed to justify going longer.
const SEGMENT_EDGE_CACHE_SECONDS = 300;

// Same idea for the manifest itself: if N viewers are each polling the
// playlist every ~1-2s, that's N separate ngrok round trips per interval.
// We cache the RAW upstream text (not the per-viewer rewritten output,
// since SRS tags each viewer's lines with their own hls_ctx — caching
// the rewritten version would leak one viewer's session id into
// another's player). Each request still re-applies its own query string
// on top of the cached raw text.
const manifestCache = new Map(); // streamKey -> { text, cachedAt }
const MANIFEST_CACHE_TTL_MS = 1000;

// ══════════════════════════════════════════
// BUNNY SIGNED PLAYBACK URLS — Advanced (HMAC-SHA256) token auth on the
// dedicated HLS_CDN_HOSTNAME pull zone only (never the general API zone).
// Algorithm per Bunny's current docs (docs.bunny.net/cdn/security/
// token-authentication/advanced):
//   token = "HS256-" + base64url(HMAC-SHA256(security_key,
//              url_path + expires + "token_ignore_params=true"))
// We always sign with token_ignore_params=true so the signed message never
// depends on which OTHER query params happen to be on a given URL (e.g.
// our own `_s` session tag on segment URLs) — one consistent formula for
// every path, regardless of what else gets appended to it now or later.
// No-op (returns input unchanged) whenever BUNNY_HLS_TOKEN_KEY isn't set,
// so this is safe to deploy before the Bunny zone/key exist.
// ══════════════════════════════════════════
function signBunnyUrlPath(urlPath, expiresUnix) {
  const message = `${urlPath}${expiresUnix}token_ignore_params=true`;
  const hmac = crypto
    .createHmac("sha256", BUNNY_HLS_TOKEN_KEY)
    .update(message)
    .digest("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=/g, "");
  return `HS256-${hmac}`;
}

// urlPath must be the exact request path Bunny will see (leading slash,
// no domain, no query string) — e.g. "/api/hls/mystream.m3u8" or
// "/live/mystream_720p-0042.ts". Returns a query-string fragment
// ("?token=...&expires=...&token_ignore_params=true" or "&token=..." if
// existingQs already has a "?") to append to that path.
function appendBunnyToken(urlPath, existingQs = "") {
  if (!BUNNY_HLS_TOKEN_KEY) return existingQs;
  const expires = Math.floor(Date.now() / 1000) + HLS_TOKEN_TTL_SECONDS;
  const token = signBunnyUrlPath(urlPath, expires);
  const sep = existingQs ? "&" : "?";
  return `${existingQs}${sep}token=${encodeURIComponent(token)}&expires=${expires}&token_ignore_params=true`;
}

// AES-128 HLS content encryption (2026-08-10, Wowza-parity item 3) — SRS
// writes an #EXT-X-KEY line into every media playlist once hls_keys is on
// (see srs.conf), with a URI SRS builds from hls_key_url + the raw key
// filename (e.g. "http://host.docker.internal:5000/api/hls/key/live/
// mystream-0.key"). That internal-only host is never reachable by a real
// viewer's player — same reason every .ts segment line gets rewritten
// before a manifest reaches a client. This does the equivalent rewrite
// for the key line specifically: swap in the public, Bunny-Token-Auth-
// signed /api/hls/key/... route, leaving METHOD=/IV= untouched. Every
// manifest-serving route that rewrites segment URIs must also call this
// on each line, or playback breaks the moment SRS emits an encrypted
// stream — segments would load but never decrypt.
function rewriteHlsKeyLine(line) {
  const trimmed = line.trim();
  if (!trimmed.startsWith("#EXT-X-KEY")) return line;

  const uriMatch = trimmed.match(/URI="([^"]+)"/);
  if (!uriMatch) return line; // no URI to rewrite (e.g. METHOD=NONE), leave as-is

  const rawUri = uriMatch[1];
  const questionIndex = rawUri.indexOf("?");
  const uriPath = questionIndex >= 0 ? rawUri.slice(0, questionIndex) : rawUri;
  const keyFilename = uriPath.split("/").pop();
  // hls_key_file is "[app]/[stream]-[seq].key" — the app segment is
  // whatever directory SRS placed it under (matches hls_ts_file's own
  // [app] token), so take the path segment right before the filename
  // rather than assuming a fixed value.
  const pathParts = uriPath.split("/").filter(Boolean);
  const app = pathParts.length >= 2 ? pathParts[pathParts.length - 2] : "live";

  const keyPath = `/api/hls/key/${encodeURIComponent(app)}/${encodeURIComponent(keyFilename)}`;
  const signedKeyUrl = `${keyPath}${appendBunnyToken(keyPath)}`;

  return line.replace(rawUri, signedKeyUrl);
}

// Once a broadcast has successfully resolved to either live_capped or
// live, stick with that SAME app for the rest of that broadcast rather
// than re-deciding on every poll. Silently flipping between the two apps
// mid-session — even from a single transient blip, not just a real
// crash — causes a genuine HLS playback discontinuity (different segment
// timeline/numbering), which real testing confirmed as a bufferStalledError
// in the player. Keyed by stream_key; invalidated whenever the channel
// starts a genuinely NEW broadcast (different live_started_at), so a
// fresh stream always gets to try live_capped again from scratch.
const stickyHlsApp = new Map(); // streamKey -> { app, liveStartedAtMs }

// How long a brand-new broadcast gets to bring its bitrate-cap transcode
// online before its startup lag is treated as a genuine failure and we
// commit to the uncapped fallback for the rest of that session. Comfortably
// covers the ~3s spawn delay plus time for ffmpeg to connect and produce
// its first HLS segment.
const HLS_CAPPED_STARTUP_GRACE_MS = 20 * 1000;

setInterval(() => {
  const now = Date.now();
  for (const [key, val] of segmentCache) {
    if (now - val.cachedAt > SEGMENT_CACHE_TTL_MS) segmentCache.delete(key);
  }
  for (const [key, val] of manifestCache) {
    if (now - val.cachedAt > MANIFEST_CACHE_TTL_MS) manifestCache.delete(key);
  }
}, 30000).unref();

// Lightweight, SRS-free session-info endpoint — purely for
// LivePlayer.jsx's fast discontinuity-detection poll (see
// SESSION_MARKER_POLL_MS there). Deliberately does NOT touch SRS or the
// manifest proxy at all: an earlier version of this fix had LivePlayer
// re-fetch the actual .m3u8 manifest every 1.5s to check this, but that
// created a genuinely new problem — each of those fetches was an
// anonymous, session-less request indistinguishable from a real new HLS
// viewer connecting and immediately disconnecting, confirmed via a flood
// of on_stop events in the SRS logs every ~1.5-10s for the entire
// broadcast. This endpoint answers directly from in-memory state instead,
// so polling it has zero effect on SRS, zero effect on viewer/session
// tracking, and is far cheaper besides.
// Viewer-side HLS error reporting — see LivePlayer.jsx's reportPlayerError.
// Turns any real playback error (bufferAppendError and friends) into a
// server-side log entry automatically, for any viewer, on any org, at any
// time — previously the ONLY way to see one of these was someone happening
// to have DevTools open at the exact moment it happened. Deliberately just
// logs (no new DB table) to match the existing BITRATE-CAP/Transcode]
// logging pattern, and is de-duped per stream+error type so a persistent
// issue doesn't flood the logs — but always logs immediately on a fatal
// error, since those are rare enough to want zero delay on.
const lastPlayerErrorLoggedAt = new Map(); // key: `${streamKey}:${errorType}`
const PLAYER_ERROR_DEDUPE_MS = 10000;

// These are normal hls.js recovery actions, not terminal playback failures.
// Keep them available as debug telemetry without placing them in the
// Super Admin Recent Errors ring buffer.
const INFORMATIONAL_PLAYER_EVENTS = new Set([
  "bufferStalledError",
  "bufferNudgeOnStall",
  "bufferSeekOverHole",
]);

app.post("/api/player/error-report", (req, res) => {
  try {
    const { streamKey, errorType, details, fatal, sessionInfo, playerKind } =
      req.body || {};

    if (!streamKey || !errorType) {
      return res
        .status(400)
        .json({ ok: false, message: "streamKey and errorType are required" });
    }

    if (!fatal && INFORMATIONAL_PLAYER_EVENTS.has(errorType)) {
      console.debug(
        `[PLAYER-RECOVERY] ${errorType} for ${streamKey}`,
        JSON.stringify({ details, sessionInfo, playerKind }),
      );

      return res.json({
        ok: true,
        ignored: true,
        classification: "recoverable_player_event",
      });
    }

    const dedupeKey = `${streamKey}:${errorType}`;
    const now = Date.now();
    const lastLoggedAt = lastPlayerErrorLoggedAt.get(dedupeKey) || 0;
    const shouldLog = fatal || now - lastLoggedAt > PLAYER_ERROR_DEDUPE_MS;

    if (shouldLog) {
      lastPlayerErrorLoggedAt.set(dedupeKey, now);
      console.error(
        `[PLAYER-ERROR] ${errorType} for ${streamKey} (fatal: ${Boolean(fatal)}, page: ${playerKind || "unknown"})`,
        JSON.stringify({ details, sessionInfo }),
      );
    }

    res.json({ ok: true });
  } catch (err) {
    console.error(
      "[PLAYER-ERROR] Failed to process error report:",
      err.message,
    );
    res.status(500).json({ ok: false });
  }
});

app.get("/api/hls/session/:streamKey", async (req, res) => {
  try {
    const { streamKey } = req.params;

    const result = await pool.query(
      `SELECT live_started_at FROM channels WHERE stream_key = $1`,
      [streamKey],
    );

    const liveStartedAtMs = result.rows[0]?.live_started_at
      ? new Date(result.rows[0].live_started_at).getTime()
      : 0;
    const encoderGeneration = bitrateCapEncoderGeneration.get(streamKey) || 0;

    res.setHeader("Cache-Control", "no-store");
    res.json({ ok: true, liveStartedAtMs, encoderGeneration });
  } catch (err) {
    res.status(500).json({ ok: false, message: err.message });
  }
});

app.get("/api/hls/:streamKey.m3u8", async (req, res) => {
  const { streamKey } = req.params;

  const qs = req.originalUrl.includes("?")
    ? "?" + req.originalUrl.split("?")[1]
    : "";

  try {
    let text;

    const cacheKey = `${streamKey}${qs}`;
    const cached = manifestCache.get(cacheKey);
    let resolvedApp;
    let resolvedLiveStartedAtMs;

    if (cached && Date.now() - cached.cachedAt < MANIFEST_CACHE_TTL_MS) {
      text = cached.text;
      resolvedApp = cached.resolvedApp;
      resolvedLiveStartedAtMs = cached.liveStartedAtMs;
    } else {
      const channelResult = await pool.query(
        `SELECT live_started_at FROM channels WHERE stream_key = $1`,
        [streamKey],
      );
      const liveStartedAt = channelResult.rows[0]?.live_started_at;
      const liveStartedAtMs = liveStartedAt
        ? new Date(liveStartedAt).getTime()
        : null;

      const sticky = stickyHlsApp.get(streamKey);
      const stickyValid =
        sticky && liveStartedAtMs && sticky.liveStartedAtMs === liveStartedAtMs;

      // live_capped is fully retired (2026-08-03 ABR-fold) — nothing
      // publishes to it anymore, so there's no second app to race
      // against here. Real bug found live (2026-08-03): this block used
      // to also check bitrateCapRetryCount/MAX_BITRATE_CAP_RETRIES to
      // decide whether live_capped had "given up" yet — both were deleted
      // when autoCapBitrateStream was retired, so that check threw a
      // ReferenceError on nearly every request through this route
      // (crashing the whole HLS proxy for any org still on this fallback
      // path). Simplified to just resolving directly to "live".
      const candidateApps = ["live"];

      let upstream = null;
      let upstreamUrl = null;

      for (const app of candidateApps) {
        const candidateUrl = `${SRS_HLS_ORIGIN}/${app}/${streamKey}.m3u8${qs}`;

        try {
          const candidateResponse = await fetch(candidateUrl, {
            signal: AbortSignal.timeout(20000),
            redirect: "follow",
            headers: {
              "ngrok-skip-browser-warning": "1",
              "User-Agent": "NLM-Streaming-Backend/1.0",
              Accept:
                "application/vnd.apple.mpegurl, application/x-mpegURL, */*",
            },
          });

          if (candidateResponse.ok) {
            upstream = candidateResponse;
            upstreamUrl = candidateUrl;
            resolvedApp = app;
            break;
          }
        } catch {
          // try the next candidate app (none left now, but keeps the loop
          // shape intact in case a second candidate is ever reintroduced)
        }
      }

      console.log("[HLS REQUEST]", {
        streamKey,
        resolvedApp,
        upstreamUrl,
        sticky: stickyValid ? sticky.app : null,
      });

      if (!upstream) {
        // A fresh broadcast whose bitrate-cap transcode hasn't come
        // online yet is NOT a genuine failure — it just needs a moment.
        // During this grace window, never commit to the raw fallback;
        // instead tell the player to wait and retry shortly (it already
        // handles this gracefully, showing "Waiting for stream
        // segments..."), so no viewer starts on the raw stream only to
        // get yanked over to live_capped moments later.
        const withinStartupGrace =
          !stickyValid &&
          liveStartedAtMs &&
          Date.now() - liveStartedAtMs < HLS_CAPPED_STARTUP_GRACE_MS;

        if (withinStartupGrace) {
          console.log(
            "[HLS] Within startup grace window, asking player to retry shortly",
            { streamKey },
          );
          return res.status(503).send("Stream starting, please retry shortly");
        }

        console.error("[HLS UPSTREAM ERROR]", {
          streamKey,
          triedApps: candidateApps,
        });

        return res.status(502).send("HLS unavailable");
      }

      // Commit to this app for the rest of the broadcast.
      if (liveStartedAtMs) {
        stickyHlsApp.set(streamKey, { app: resolvedApp, liveStartedAtMs });
      }

      resolvedLiveStartedAtMs = liveStartedAtMs;

      console.log("[HLS RESPONSE]", {
        status: upstream.status,
        contentType: upstream.headers.get("content-type"),
        finalUrl: upstream.url,
      });

      text = await upstream.text();

      const normalizedManifest = text.trimStart();
      const isMasterPlaylist =
        normalizedManifest.startsWith("#EXTM3U") &&
        normalizedManifest.includes("#EXT-X-STREAM-INF");
      const isMediaPlaylist =
        normalizedManifest.startsWith("#EXTM3U") &&
        normalizedManifest.includes("#EXT-X-TARGETDURATION") &&
        (normalizedManifest.includes("#EXTINF:") ||
          normalizedManifest.includes("#EXT-X-MAP:"));

      // SRS may briefly answer 200 with an incomplete playlist while a
      // publish/session is being established or torn down. Returning that
      // body as a successful manifest makes hls.js fail fatally with
      // "no levels found in manifest". Treat it as transient instead so the
      // player's normal manifest retry path can recover cleanly.
      if (!isMasterPlaylist && !isMediaPlaylist) {
        console.warn("[INCOMPLETE HLS PLAYLIST]", {
          streamKey,
          resolvedApp,
          finalUrl: upstream.url,
          preview: text.substring(0, 500),
        });
        res.setHeader("Retry-After", "2");
        return res
          .status(503)
          .send("HLS playlist is not ready yet, please retry shortly");
      }

      manifestCache.set(cacheKey, {
        text,
        resolvedApp,
        liveStartedAtMs: resolvedLiveStartedAtMs,
        cachedAt: Date.now(),
      });
    }

    // Revalidate cached manifests too. This prevents a transient incomplete
    // 200 response captured just before this deploy (or during a race inside
    // the cache TTL) from being served repeatedly as a valid playlist.
    const normalizedManifest = text.trimStart();
    const isMasterPlaylist =
      normalizedManifest.startsWith("#EXTM3U") &&
      normalizedManifest.includes("#EXT-X-STREAM-INF");
    const isMediaPlaylist =
      normalizedManifest.startsWith("#EXTM3U") &&
      normalizedManifest.includes("#EXT-X-TARGETDURATION") &&
      (normalizedManifest.includes("#EXTINF:") ||
        normalizedManifest.includes("#EXT-X-MAP:"));

    if (!isMasterPlaylist && !isMediaPlaylist) {
      manifestCache.delete(cacheKey);
      res.setHeader("Retry-After", "2");
      return res
        .status(503)
        .send("HLS playlist is not ready yet, please retry shortly");
    }

    res.setHeader("Content-Type", "application/vnd.apple.mpegurl");
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate");

    const rewritten = text
      .split("\n")
      .map((line) => {
        const t = line.trim();

        if (t.startsWith("#EXT-X-KEY")) {
          return rewriteHlsKeyLine(line);
        }

        if (!t || t.startsWith("#")) {
          return line;
        }

        const questionIndex = t.indexOf("?");

        const pathPart = questionIndex >= 0 ? t.slice(0, questionIndex) : t;

        const suffix = questionIndex >= 0 ? t.slice(questionIndex) : "";

        if (pathPart.endsWith(".ts")) {
          const segmentName = pathPart.split("/").pop();
          // Tags each segment URL with this broadcast's liveStartedAt so the
          // segment proxy's cache (below) can never serve bytes from a PRIOR
          // session for a filename SRS happens to reuse (its HLS sequence
          // numbering resets per publish) — confirmed as a real risk since a
          // 10s OBS stop/restart is well inside the segment cache's 15s TTL.
          const sessionToken = resolvedLiveStartedAtMs || 0;
          const sessionSuffix = suffix
            ? `${suffix}&_s=${sessionToken}`
            : `?_s=${sessionToken}`;

          const segPath = `/api/hls/seg/${encodeURIComponent(resolvedApp)}/${encodeURIComponent(
            streamKey,
          )}/${encodeURIComponent(segmentName)}`;

          return `${segPath}${appendBunnyToken(segPath, sessionSuffix)}`;
        }

        if (pathPart.endsWith(".m3u8")) {
          const fileName = pathPart.split("/").pop();

          const variantKey = fileName.replace(/\.m3u8$/i, "");

          const subManifestPath = `/api/hls/${encodeURIComponent(variantKey)}.m3u8`;

          return `${subManifestPath}${appendBunnyToken(subManifestPath, suffix)}`;
        }

        return line;
      })
      .join("\n");

    return res.send(rewritten);
  } catch (err) {
    console.error("[HLS PROXY FAILED]", {
      name: err.name,
      message: err.message,
      cause: err.cause,
      origin: SRS_HLS_ORIGIN,
      streamKey,
    });

    return res.status(503).json({
      ok: false,
      message: "HLS unavailable: " + err.message,
    });
  }
});

app.get("/api/hls/key/:app/:filename", async (req, res) => {
  const { app, filename } = req.params;

  // Reject anything that isn't the .key filename shape SRS itself
  // generates (hls_key_file: [app]/[stream]-[seq].key in srs.conf) —
  // this route exists to proxy exactly one thing, not act as a general
  // file fetcher into SRS's internal http_server.
  if (!/^[\w-]+\.key$/.test(filename)) {
    return res.status(400).send("Invalid key filename");
  }

  try {
    const upstream = await fetch(`${SRS_HLS_ORIGIN}/${app}/${filename}`, {
      signal: AbortSignal.timeout(10000),
    });

    if (!upstream.ok) {
      console.error("[HLS KEY UPSTREAM ERROR]", {
        app,
        filename,
        status: upstream.status,
      });
      return res.status(upstream.status).send("Key unavailable");
    }

    const buffer = Buffer.from(await upstream.arrayBuffer());

    res.setHeader("Content-Type", "application/octet-stream");
    res.setHeader("Access-Control-Allow-Origin", "*");
    // Same edge-cache reasoning as segments: a given key file is static
    // for its whole hls_fragments_per_key window once SRS writes it, and
    // Bunny Token Auth already gates the initial signed request — no
    // staleness risk from caching this at the edge.
    res.setHeader(
      "Cache-Control",
      `public, max-age=${SEGMENT_EDGE_CACHE_SECONDS}`,
    );
    res.send(buffer);
  } catch (err) {
    res.status(503).send("Key temporarily unavailable, please retry shortly");
  }
});

app.get("/api/hls/seg/:app/:streamKey/:segment", async (req, res) => {
  const { app, streamKey, segment } = req.params;

  // _s is our own session tag (see the manifest rewrite above), not
  // something SRS understands — strip it before forwarding upstream, and
  // use it (not the raw filename alone) as the cache key so a filename
  // reused by a NEW broadcast session can never resolve to a PRIOR
  // session's cached bytes.
  const sessionToken = req.query._s || "0";
  const upstreamParams = new URLSearchParams(req.query);
  upstreamParams.delete("_s");
  const upstreamQs = upstreamParams.toString()
    ? `?${upstreamParams.toString()}`
    : "";

  const cacheKey = `${app}/${streamKey}/${sessionToken}/${segment}`;
  const cached = segmentCache.get(cacheKey);
  if (cached) {
    res.setHeader("Content-Type", cached.contentType);
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader(
      "Cache-Control",
      `public, max-age=${SEGMENT_EDGE_CACHE_SECONDS}`,
    );
    res.setHeader("X-Segment-Cache", "HIT");
    return res.send(cached.buffer);
  }

  try {
    const upstream = await fetch(
      `${SRS_HLS_ORIGIN}/${app}/${segment}${upstreamQs}`,
      {
        signal: AbortSignal.timeout(10000),
      },
    );
    if (!upstream.ok) {
      console.error("[HLS SEGMENT UPSTREAM ERROR]", {
        app,
        streamKey,
        segment,
        status: upstream.status,
        upstreamUrl: `${SRS_HLS_ORIGIN}/${app}/${segment}${upstreamQs}`,
      });
      return res.status(upstream.status).send("Segment unavailable");
    }
    const contentType = "video/mp2t";
    res.setHeader("Content-Type", contentType);
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader(
      "Cache-Control",
      `public, max-age=${SEGMENT_EDGE_CACHE_SECONDS}`,
    );
    res.setHeader("X-Segment-Cache", "MISS");
    const buffer = Buffer.from(await upstream.arrayBuffer());
    segmentCache.set(cacheKey, {
      buffer,
      contentType,
      cachedAt: Date.now(),
    });
    res.send(buffer);
  } catch (err) {
    res
      .status(503)
      .json({ ok: false, message: "Segment unavailable: " + err.message });
  }
});
// POST /api/srs/on_publish
// SRS fires this when a broadcaster connects
// Return code 0 = allow, code 403 = reject
// ══════════════════════════════════════════
app.post("/api/srs/on_publish", async (req, res) => {
  const streamKey = req.body?.stream || req.body?.name || "";
  const publishApp = req.body?.app || "";
  console.log(
    `[SRS] on_publish — app: ${publishApp}, stream key: ${streamKey}`,
  );

  // Skip transcoded variant streams (they re-publish to SRS too) — this
  // is any of our own ABR rendition outputs (top/720p/480p), an internal
  // re-publish of an already-validated stream, not a new broadcaster
  // connecting — running the full validation/plan-resolution logic again
  // here would at best be redundant and at worst spawn a duplicate (or
  // recursive) transcode.
  if (isAbrRenditionStreamKey(streamKey)) {
    return res.json({ code: 0 });
  }

  try {
    // 0. Auto-reset stale is_live flags older than 2 hours (cleanup)
    await pool.query(
      `UPDATE channels SET is_live = FALSE, live_started_at = NULL
       WHERE is_live = TRUE
         AND live_started_at < NOW() - INTERVAL '2 hours'`,
    );

    // 1. Validate stream key exists and is active in our DB
    const channelResult = await pool.query(
      `
      SELECT c.*, o.id AS org_id, o.name AS org_name, o.is_active AS org_active
      FROM channels c
      JOIN organizations o ON o.id = c.organization_id
      WHERE c.stream_key = $1
        AND c.is_active = TRUE
        AND o.is_active = TRUE
      LIMIT 1
      `,
      [streamKey],
    );

    if (!channelResult.rows[0]) {
      console.warn(
        `[SRS] REJECTED — Unknown or inactive stream key: ${streamKey}`,
      );
      return res.json({ code: 403 }); // SRS will kick the connection
    }

    const channel = channelResult.rows[0];

    // 2. Check concurrent stream limit for this org's plan
    const plan = await getOrgStreamingPlan(channel.org_id);
    const liveCount = await getActiveLiveCount(channel.org_id, streamKey);

    if (liveCount >= plan.max_concurrent_streams) {
      console.warn(
        `[SRS] REJECTED — Org ${channel.org_name} exceeded max concurrent streams (${plan.max_concurrent_streams})`,
      );
      return res.json({ code: 403 });
    }

    // 3. Mark channel as live in DB
    await pool.query(
      `UPDATE channels SET is_live = TRUE, live_started_at = NOW() WHERE stream_key = $1`,
      [streamKey],
    );

    // New broadcast session — bump the shared generation so any OLD retry
    // chain still in-flight for this stream_key (bitrate cap OR ABR
    // transcode, e.g. from a brief encoder reconnect blip) recognizes it's
    // been superseded and abandons itself, instead of racing this new
    // attempt to publish to the same output names.
    const generation = (bitrateCapGeneration.get(streamKey) || 0) + 1;
    bitrateCapGeneration.set(streamKey, generation);
    // Fresh broadcast session — clear retry budgets for every rendition
    // label that might be in flight for this stream_key (labels are now
    // plan-driven, not a fixed "720p"/"480p" pair, so this clears by
    // prefix rather than a hardcoded list).
    for (const key of transcodeRetryCount.keys()) {
      if (key.startsWith(`${streamKey}:`)) transcodeRetryCount.delete(key);
    }

    // 4. Spawn whatever rendition ladder this org's plan calls for. A short
    // delay allows SRS to expose codec metadata/keyframes; the deeper SRS
    // readiness gate still verifies the source before FFmpeg starts.
    getRenditionPlanForOrg(channel.org_id)
      .then((renditions) => {
        if (!renditions.length) return;
        const startupTimer = setTimeout(
          () => spawnRenditionsForStream(streamKey, renditions, generation),
          3000,
        );
        startupTimer.unref?.();
      })
      .catch((err) =>
        console.error(
          `[ABR] Failed to resolve rendition plan for org ${channel.org_id}:`,
          err.message,
        ),
      );

    // 4b. SOCIAL-OAUTH AUTO GO-LIVE — Wowza-style auto-fire for any
    // connected (automation_mode = 'oauth') Facebook/YouTube destination on
    // this channel. Fire-and-forget, same shape as the rendition spawn
    // above: must never block or throw into the on_publish response SRS is
    // waiting on to allow/reject the connection. Delayed 5s (longer than
    // the 3s rendition delay) since this pulls from our own internal HLS
    // source, which needs a couple of real segments written before it's
    // pull-able — a bare RTMP passthrough doesn't need that buffer, HLS
    // does. startOauthSimulcast()'s own socialProcesses.has() guard is what
    // absorbs a quick reconnect/blip without spinning up a duplicate
    // platform broadcast — see the function for that reasoning.
    pool
      .query(
        `SELECT * FROM social_destinations
         WHERE channel_id = $1 AND automation_mode = 'oauth' AND oauth_account_id IS NOT NULL`,
        [channel.id],
      )
      .then((destResult) => {
        for (const destination of destResult.rows) {
          const autoGoLiveTimer = setTimeout(() => {
            startOauthSimulcast(channel, destination, channel.org_id)
              .then((result) => {
                if (!result.ok) {
                  console.log(
                    `[SOCIAL-OAUTH] Auto go-live skipped for ${destination.platform} #${destination.id}: ${result.message}`,
                  );
                }
              })
              .catch((err) =>
                console.error(
                  `[SOCIAL-OAUTH] Auto go-live failed for ${destination.platform} #${destination.id}:`,
                  err.message,
                ),
              );
          }, 5000);
          autoGoLiveTimer.unref?.();
        }
      })
      .catch((err) =>
        console.error(
          "[SOCIAL-OAUTH] Failed to load social destinations for auto go-live:",
          err.message,
        ),
      );

    // 5. Notify all connected dashboard clients via socket
    if (io) {
      io.emit("stream:live", {
        stream_key: streamKey,
        channel_id: channel.id,
        organization_id: channel.org_id,
        organization_name: channel.org_name,
      });
    }

    console.log(`[SRS] ALLOWED — ${streamKey} (org: ${channel.org_name})`);
    res.json({ code: 0 });
  } catch (err) {
    console.error("[SRS] on_publish error:", err.message);
    // Allow even on DB error so a server glitch doesn't cut a live broadcast
    res.json({ code: 0 });
  }
});

// ══════════════════════════════════════════
// POST /api/srs/on_unpublish
// SRS fires this when a broadcaster disconnects
// ══════════════════════════════════════════
app.post("/api/srs/on_unpublish", async (req, res) => {
  const streamKey = req.body?.stream || req.body?.name || "";
  const publishApp = req.body?.app || "";
  console.log(
    `[SRS] on_unpublish — app: ${publishApp}, stream key: ${streamKey}`,
  );

  if (isAbrRenditionStreamKey(streamKey)) {
    return res.json({ code: 0 });
  }

  try {
    // Invalidate every delayed startup and retry that belongs to the broadcast
    // which just ended. A pending startup timer may otherwise spawn
    // ffmpeg after SRS has already removed the raw source.
    const endedGeneration = (bitrateCapGeneration.get(streamKey) || 0) + 1;
    bitrateCapGeneration.set(streamKey, endedGeneration);

    // Clear all retry budgets and recovery cooldowns for this source stream.
    for (const key of transcodeRetryCount.keys()) {
      if (key.startsWith(`${streamKey}:`)) {
        transcodeRetryCount.delete(key);
      }
    }
    for (const key of abrRecoveryLastAttempt.keys()) {
      if (key.startsWith(`${streamKey}:`)) {
        abrRecoveryLastAttempt.delete(key);
      }
    }
    for (const key of transcodeStartupLocks.keys()) {
      if (key.startsWith(`${streamKey}:`)) {
        transcodeStartupLocks.delete(key);
      }
    }

    // Stop any active rendition process that is still consuming this source.
    for (const [key, proc] of activeTranscodeProcesses.entries()) {
      if (!key.startsWith(`${streamKey}:`)) continue;

      if (proc && proc.exitCode === null) {
        console.log(
          `[Transcode] Stopping ${key} because raw source ${streamKey} unpublished.`,
        );

        proc.kill("SIGTERM");

        const forceKillTimer = setTimeout(() => {
          if (proc.exitCode === null) {
            console.warn(
              `[Transcode] Force-killing ${key} after graceful shutdown timeout.`,
            );
            proc.kill("SIGKILL");
          }
        }, 5000);
        forceKillTimer.unref();
      }

      activeTranscodeProcesses.delete(key);
    }

    // Remove every manifest/segment cache entry tied to the ended source or
    // one of its suffixed ABR rendition streams. This prevents stale manifests
    // from referencing SRS segment files that were already deleted.
    stickyHlsApp.delete(streamKey);

    for (const key of manifestCache.keys()) {
      if (
        key === streamKey ||
        key.startsWith(`${streamKey}?`) ||
        key.startsWith(`${streamKey}_`)
      ) {
        manifestCache.delete(key);
      }
    }

    for (const key of segmentCache.keys()) {
      if (key.includes(`/${streamKey}/`) || key.includes(`/${streamKey}_`)) {
        segmentCache.delete(key);
      }
    }

    // Mark channel offline
    const channelResult = await pool.query(
      `UPDATE channels SET is_live = FALSE, live_started_at = NULL
       WHERE stream_key = $1
       RETURNING id, organization_id`,
      [streamKey],
    );

    const orgId = channelResult.rows[0]?.organization_id;
    const endedChannelId = channelResult.rows[0]?.id;

    // SOCIAL-OAUTH AUTO END-LIVE — mirror of the auto go-live hook in
    // on_publish. Without this, our ffmpeg push dies on its own once the
    // source disappears, but the platform-side broadcast (Facebook
    // especially) is left showing "live" indefinitely until endLiveVideo/
    // transitionBroadcast is explicitly called. Fire-and-forget, same
    // reasoning as above — never block this webhook response.
    if (endedChannelId) {
      pool
        .query(
          `SELECT * FROM social_destinations
           WHERE channel_id = $1 AND automation_mode = 'oauth' AND is_running = true`,
          [endedChannelId],
        )
        .then((destResult) => {
          for (const destination of destResult.rows) {
            endOauthSimulcast(destination).catch((err) =>
              console.error(
                `[SOCIAL-OAUTH] Auto end-live failed for ${destination.platform} #${destination.id}:`,
                err.message,
              ),
            );
          }
        })
        .catch((err) =>
          console.error(
            "[SOCIAL-OAUTH] Failed to load social destinations for auto end-live:",
            err.message,
          ),
        );
    }

    // Notify dashboard
    if (io) {
      io.emit("stream:offline", {
        stream_key: streamKey,
        organization_id: orgId,
      });
    }

    // Auto-sync recordings after stream ends (wait for SRS to write files)
    if (orgId) {
      autoSyncRecordingsDelayed(orgId, 8000, endedChannelId, streamKey);
    }

    console.log(`[SRS] Stream offline: ${streamKey}`);
    res.json({ code: 0 });
  } catch (err) {
    console.error("[SRS] on_unpublish error:", err.message);
    res.json({ code: 0 });
  }
});

// ══════════════════════════════════════════
// POST /api/srs/on_play
// SRS fires this when a viewer starts watching
// ══════════════════════════════════════════
app.post("/api/srs/on_play", async (req, res) => {
  const streamKey = req.body?.stream || req.body?.name || "";
  const clientId = req.body?.client_id || "";
  const ip = req.body?.ip || req.ip || "";

  // Optional: log viewer for analytics
  console.log(`[SRS] on_play — stream: ${streamKey}, client: ${clientId}`);

  res.json({ code: 0 }); // Always allow (HLS token auth can add restriction here later)
});

// ══════════════════════════════════════════
// POST /api/srs/on_stop
// SRS fires this when a viewer stops watching
// ══════════════════════════════════════════
app.post("/api/srs/on_stop", async (req, res) => {
  const streamKey = req.body?.stream || req.body?.name || "";
  console.log(`[SRS] on_stop — stream: ${streamKey}`);
  res.json({ code: 0 });
});

// ══════════════════════════════════════════
// GET /api/srs/live-status
// Quick endpoint for dashboard to poll without
// hitting the SRS API directly
// ══════════════════════════════════════════
app.get(
  "/api/srs/live-status",
  authenticateAdmin,
  resolveOrganizationForRequest,
  async (req, res) => {
    try {
      const result = await pool.query(
        `SELECT stream_key, name, is_live, live_started_at,
              EXTRACT(EPOCH FROM (NOW() - live_started_at))::int AS uptime_seconds
       FROM channels
       WHERE organization_id = $1 AND is_live = TRUE`,
        [req.organization.id],
      );

      res.json({
        ok: true,
        live_channels: result.rows,
        count: result.rows.length,
      });
    } catch (err) {
      res
        .status(500)
        .json({ ok: false, message: "Failed to get live status." });
    }
  },
);

/*
|--------------------------------------------------------------------------
| CHANNELS
|--------------------------------------------------------------------------
*/

app.get(
  "/api/channels",
  authenticateAdmin,
  resolveOrganizationForRequest,
  async (req, res) => {
    try {
      const result = await pool.query(
        `
      SELECT *
      FROM channels
      WHERE organization_id = $1
      ORDER BY created_at DESC
      `,
        [req.organization.id],
      );

      res.json({
        ok: true,
        channels: result.rows,
      });
    } catch (error) {
      console.error("Get Channels Error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to fetch channels",
      });
    }
  },
);

app.post(
  "/api/channels",
  authenticateAdmin,
  resolveOrganizationForRequest,
  requireRole("super_admin", "admin", "operator"),
  requireOrganizationRole("owner", "admin"),
  enforceChannelLimit,
  async (req, res) => {
    try {
      const { name, description } = req.body;

      if (!name) {
        return res.status(400).json({
          ok: false,
          message: "Channel name is required",
        });
      }

      const streamKey = await generateUniqueStreamKey(name);

      const result = await pool.query(
        `
        INSERT INTO channels (
          organization_id,
          name,
          stream_key,
          description
        )
        VALUES ($1, $2, $3, $4)
        RETURNING *
        `,
        [req.organization.id, name, streamKey, description || null],
      );

      res.json({
        ok: true,
        channel: result.rows[0],
      });
    } catch (error) {
      console.error("Create Channel Error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to create channel",
        error: error.message,
      });
    }
  },
);

app.delete(
  "/api/channels/:id",
  authenticateAdmin,
  resolveOrganizationForRequest,
  requireRole("super_admin", "admin", "operator"),
  requireOrganizationRole("owner", "admin"),
  async (req, res) => {
    try {
      const { id } = req.params;

      await pool.query(
        `
        DELETE FROM channels
        WHERE id = $1
          AND organization_id = $2
        `,
        [id, req.organization.id],
      );

      res.json({
        ok: true,
        message: "Channel deleted successfully",
      });
    } catch (error) {
      console.error("Delete Channel Error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to delete channel",
      });
    }
  },
);

// ══════════════════════════════════════════
// PHASE 2 — secure stream key rotation.
// Regenerates a channel's RTMP/SRT ingest key using the same
// generateUniqueStreamKey() used at creation time. Recordings are keyed by
// channel_id (not stream_key), so past recordings stay intact and browsable
// after a rotation — only the live ingest credential changes. Blocked while
// the channel is currently live so we never rotate out from under an
// actively-publishing encoder; the client must stop streaming first, update
// OBS/their encoder with the new key, then go live again.
// super_admin only (matches the /force-offline pattern above) — this is a
// support/recovery-grade action (e.g. a leaked key), not something an
// org's own owner/admin/operator can trigger on themselves.
// ══════════════════════════════════════════
app.post(
  "/api/channels/:id/regenerate-key",
  authenticateAdmin,
  requireRole("super_admin"),
  async (req, res) => {
    try {
      const channelResult = await pool.query(
        `SELECT * FROM channels WHERE id = $1`,
        [req.params.id],
      );
      const channel = channelResult.rows[0];

      if (!channel) {
        return res.status(404).json({
          ok: false,
          message: "Channel not found",
        });
      }

      if (channel.is_live) {
        return res.status(400).json({
          ok: false,
          message:
            "Cannot rotate the stream key while this channel is live. Stop the broadcast first.",
        });
      }

      const newStreamKey = await generateUniqueStreamKey();

      await pool.query(`UPDATE channels SET stream_key = $1 WHERE id = $2`, [
        newStreamKey,
        channel.id,
      ]);

      res.json({
        ok: true,
        message:
          "Stream key regenerated. Update OBS/your encoder with the new key before going live again.",
        stream_key: newStreamKey,
      });
    } catch (error) {
      console.error("Regenerate Stream Key Error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to regenerate stream key",
      });
    }
  },
);

// ══════════════════════════════════════════
// SUPPORT TOOL — clear a stuck "live" flag
// If SRS crashes or the on_unpublish webhook never fires (e.g. the
// server restarted mid-stream), a channel's is_live/live_started_at
// can stay stuck "on" in the database even though nothing is actually
// streaming. super_admin only — this is a recovery action, not a
// routine client one.
// ══════════════════════════════════════════
app.post(
  "/api/channels/:id/force-offline",
  authenticateAdmin,
  requireRole("super_admin"),
  async (req, res) => {
    try {
      const { id } = req.params;

      const result = await pool.query(
        `
        UPDATE channels
        SET is_live = FALSE,
            live_started_at = NULL
        WHERE id = $1
        RETURNING id, name, stream_key
        `,
        [id],
      );

      if (!result.rows[0]) {
        return res.status(404).json({
          ok: false,
          message: "Channel not found",
        });
      }

      res.json({
        ok: true,
        message: `Cleared stuck live status for ${result.rows[0].name}`,
        channel: result.rows[0],
      });
    } catch (error) {
      console.error("Force channel offline error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to clear live status",
        error: error.message,
      });
    }
  },
);

// "Record all incoming streams" toggle (Wowza-parity, 2026-08-10) —
// layered on top of the org's plan-level recording_enabled gate, not a
// replacement for it. See ensureChannelRecordingColumn and
// autoSyncRecordingsDelayed for how this is actually enforced at
// broadcast-end time.
app.put(
  "/api/channels/:id/recording-toggle",
  authenticateAdmin,
  resolveOrganizationForRequest,
  requireRole("super_admin", "admin", "operator"),
  requireOrganizationRole("owner", "admin"),
  async (req, res) => {
    try {
      const channel = await getOwnedChannel(req.params.id, req.organization.id);
      if (!channel) {
        return res
          .status(404)
          .json({ ok: false, message: "Channel not found" });
      }

      const enabled = req.body.auto_record_enabled !== false;

      const result = await pool.query(
        `
        UPDATE channels
        SET auto_record_enabled = $1
        WHERE id = $2
        RETURNING id, name, auto_record_enabled
        `,
        [enabled, channel.id],
      );

      res.json({
        ok: true,
        message: enabled
          ? "This channel will now record every broadcast automatically."
          : "Automatic recording turned off for this channel.",
        channel: result.rows[0],
      });
    } catch (error) {
      console.error("Recording toggle error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to update recording setting",
        error: error.message,
      });
    }
  },
);

// ══════════════════════════════════════════
// TEMPORARY — Slack alert webhook test route. Hit once to confirm
// SLACK_ALERT_WEBHOOK_URL is wired correctly, then remove this route
// in a follow-up deploy. Does not touch any stream, org, or ffmpeg
// process — purely a notifySlack() smoke test.
// ══════════════════════════════════════════
app.get(
  "/api/admin/test-slack-alert",
  authenticateAdmin,
  requireRole("super_admin"),
  async (req, res) => {
    await notifySlack("Test alert — Slack webhook wiring check", {
      triggeredBy: req.admin?.email || "unknown",
      timestamp: new Date().toISOString(),
    });
    res.json({ ok: true, message: "Test alert sent (check Slack channel)" });
  },
);

// ══════════════════════════════════════════
// SUPER ADMIN DASHBOARD — per-organization force refresh
// Reconciles this org's channels/social destinations against the actual
// state of SRS and our own ffmpeg process table. Does NOT restart anything
// or drop any active stream -- it only corrects stale DB flags (e.g. a
// channel stuck "live" after a webhook was missed, or a simulcast stuck
// "running" after ffmpeg died without us noticing). Safe to run anytime.
// ══════════════════════════════════════════
app.post(
  "/api/admin/organizations/:organizationId/refresh",
  authenticateAdmin,
  requireRole("super_admin"),
  async (req, res) => {
    try {
      const { organizationId } = req.params;

      const orgResult = await pool.query(
        `SELECT id, name FROM organizations WHERE id = $1`,
        [organizationId],
      );
      const organization = orgResult.rows[0];
      if (!organization) {
        return res
          .status(404)
          .json({ ok: false, message: "Organization not found" });
      }

      const channelsResult = await pool.query(
        `SELECT id, name, stream_key, is_live, live_started_at
         FROM channels WHERE organization_id = $1`,
        [organizationId],
      );

      let srsAvailable = true;
      let srsStreamsByKey = new Map();
      try {
        const srsResponse = await fetch(`${SRS_API_URL}/api/v1/streams`);
        if (!srsResponse.ok)
          throw new Error(`SRS responded ${srsResponse.status}`);
        const srsData = await srsResponse.json();
        srsStreamsByKey = new Map(
          (srsData.streams || []).map((s) => [s.name, s]),
        );
      } catch (srsError) {
        console.error("Force refresh: SRS unavailable:", srsError.message);
        srsAvailable = false;
      }

      const channelCorrections = [];
      if (srsAvailable) {
        for (const channel of channelsResult.rows) {
          const srsStream = srsStreamsByKey.get(channel.stream_key);
          const actuallyLive = Boolean(srsStream?.publish?.active);
          if (Boolean(channel.is_live) !== actuallyLive) {
            await pool.query(
              `UPDATE channels SET is_live = $1, live_started_at = $2 WHERE id = $3`,
              [actuallyLive, actuallyLive ? new Date() : null, channel.id],
            );
            channelCorrections.push({
              channel_id: channel.id,
              channel_name: channel.name,
              was_live: Boolean(channel.is_live),
              now_live: actuallyLive,
            });
          }
        }
      }

      const socialResult = await pool.query(
        `SELECT sd.id, sd.platform, sd.is_running, c.name AS channel_name
         FROM social_destinations sd
         JOIN channels c ON c.id = sd.channel_id
         WHERE c.organization_id = $1`,
        [organizationId],
      );

      const socialCorrections = [];
      for (const dest of socialResult.rows) {
        const actuallyRunning = socialProcesses.has(dest.id);
        if (Boolean(dest.is_running) !== actuallyRunning) {
          await pool.query(
            `UPDATE social_destinations SET is_running = $1, ffmpeg_pid = $2 WHERE id = $3`,
            [
              actuallyRunning,
              actuallyRunning
                ? (socialProcesses.get(dest.id)?.pid ?? null)
                : null,
              dest.id,
            ],
          );
          socialCorrections.push({
            destination_id: dest.id,
            channel_name: dest.channel_name,
            platform: dest.platform,
            was_running: Boolean(dest.is_running),
            now_running: actuallyRunning,
          });
        }
      }

      const totalCorrections =
        channelCorrections.length + socialCorrections.length;

      res.json({
        ok: true,
        organization: { id: organization.id, name: organization.name },
        srs_available: srsAvailable,
        corrections_made: totalCorrections,
        channel_corrections: channelCorrections,
        social_corrections: socialCorrections,
        message: srsAvailable
          ? totalCorrections > 0
            ? `Fixed ${totalCorrections} stale status flag${totalCorrections === 1 ? "" : "s"} for ${organization.name}`
            : `Everything for ${organization.name} already matches actual server state`
          : `Reconciled ffmpeg/simulcast state for ${organization.name}, but SRS was unreachable so live-stream status couldn't be checked`,
      });
    } catch (error) {
      console.error("Organization force refresh error:", error);
      res.status(500).json({
        ok: false,
        message: "Failed to refresh organization",
        error: error.message,
      });
    }
  },
);

/*
|--------------------------------------------------------------------------
| SUPER ADMIN DASHBOARD — gated server / SRS restart
|--------------------------------------------------------------------------
| Two distinct actions with very different blast radii, so they are kept
| separate rather than one "restart server" button:
|   - restart-backend: restarts the PM2-managed Node process. Drops active
|     Socket.io connections (chat/reactions/viewer counts) for a few
|     seconds; does NOT touch RTMP publishing, since SRS runs independently
|     in its own Docker container.
|   - restart-srs: restarts the SRS Docker container itself. This DOES drop
|     every active live stream platform-wide immediately -- every publisher
|     has to reconnect and every viewer's playback breaks momentarily.
| Both require typed confirmation when there are active streams (matching
| the server hostname, so it can't be fat-fingered or scripted blindly),
| and both are logged to restart_audit_log before the restart is attempted
| -- logged even if the restart command itself later fails, since the
| *decision* to restart is the thing worth auditing.
*/

/*
|--------------------------------------------------------------------------
| SYSTEM STATUS — Wowza-parity item 4, 2026-08-10
|--------------------------------------------------------------------------
| One shared health-check engine, two views (Claude's recommendation) —
| a public coarse view (Operational/Degraded/Outage per category, no
| internal detail) for client trust/self-serve "is it us or is it them,"
| and an admin detailed view (real latency numbers, raw errors) off the
| exact same underlying checks. Runs on a background interval and caches
| the result rather than live-checking on every request — this matters
| most for the public endpoint, which could be hit frequently and should
| never itself become a load source or a way to hammer third-party APIs
| (WHMCS especially) on every page view.
*/

const systemHealthSnapshot = {
  updatedAt: null,
  checks: {
    database: { ok: null, latencyMs: null, error: null },
    srs: { ok: null, latencyMs: null, error: null },
    bunny: { ok: null, latencyMs: null, error: null, configured: false },
    whmcs: { ok: null, configured: false },
  },
};

const SYSTEM_HEALTH_CHECK_INTERVAL_MS = 30 * 1000;

async function runSystemHealthChecks() {
  // Database
  const dbStart = Date.now();
  try {
    await pool.query("SELECT 1");
    systemHealthSnapshot.checks.database = {
      ok: true,
      latencyMs: Date.now() - dbStart,
      error: null,
    };
  } catch (err) {
    systemHealthSnapshot.checks.database = {
      ok: false,
      latencyMs: null,
      error: err.message,
    };
  }

  // SRS — same /api/v1/streams endpoint already used elsewhere in this
  // file as the standard "is SRS up" check, reused here for consistency
  // rather than inventing a second pattern.
  const srsStart = Date.now();
  try {
    const res = await fetch(`${SRS_API_URL}/api/v1/streams`, {
      signal: AbortSignal.timeout(5000),
    });
    systemHealthSnapshot.checks.srs = {
      ok: res.ok,
      latencyMs: Date.now() - srsStart,
      error: res.ok ? null : `HTTP ${res.status}`,
    };
  } catch (err) {
    systemHealthSnapshot.checks.srs = {
      ok: false,
      latencyMs: null,
      error: err.message,
    };
  }

  // Bunny — a real (but lightweight, short-timeout) reachability check.
  // Bunny's API is high-availability infrastructure, not a fragile
  // third-party billing system, so pinging it every 30s is low-risk —
  // unlike WHMCS below, which deliberately is NOT live-pinged.
  if (bunny.isBunnyAccountConfigured()) {
    const bunnyStart = Date.now();
    try {
      await fetch("https://api.bunny.net/", {
        signal: AbortSignal.timeout(5000),
      });
      systemHealthSnapshot.checks.bunny = {
        ok: true,
        latencyMs: Date.now() - bunnyStart,
        error: null,
        configured: true,
      };
    } catch (err) {
      systemHealthSnapshot.checks.bunny = {
        ok: false,
        latencyMs: null,
        error: err.message,
        configured: true,
      };
    }
  } else {
    systemHealthSnapshot.checks.bunny = {
      ok: null,
      latencyMs: null,
      error: null,
      configured: false,
    };
  }

  // WHMCS — deliberately config-presence only, NOT a live API call.
  // WHMCS is a third-party billing system with its own rate limits;
  // pinging it every 30 seconds purely for a status page is the kind of
  // thing that looks harmless until it isn't. If WHMCS-specific
  // incidents ever need surfacing here, add a real check deliberately,
  // with its own longer interval — not by loosening this one.
  systemHealthSnapshot.checks.whmcs = {
    ok: whmcs.isWhmcsConfigured() ? true : null,
    configured: whmcs.isWhmcsConfigured(),
  };

  systemHealthSnapshot.updatedAt = new Date().toISOString();
}

// Maps the raw checks above into the 4 coarse, public-facing categories.
// "operational" is the default for anything not configured/not
// applicable — an unconfigured optional integration isn't a platform
// outage. "outage" only for the two checks core to the product actually
// working (database, srs); "degraded" for auxiliary ones (bunny, whmcs).
function getPublicStatusCategories() {
  const {
    database,
    srs,
    bunny: bunnyCheck,
    whmcs: whmcsCheck,
  } = systemHealthSnapshot.checks;

  const statusFor = (ok, criticalIfDown) => {
    if (ok === false) return criticalIfDown ? "outage" : "degraded";
    return "operational";
  };

  const categories = [
    {
      name: "Live Streaming",
      status: statusFor(srs.ok, true),
    },
    {
      name: "Dashboard & API",
      status: statusFor(database.ok, true),
    },
    {
      name: "Recordings",
      status: statusFor(bunnyCheck.ok, false),
    },
    {
      name: "Billing",
      status: statusFor(whmcsCheck.ok, false),
    },
  ];

  const overall = categories.some((c) => c.status === "outage")
    ? "outage"
    : categories.some((c) => c.status === "degraded")
      ? "degraded"
      : "operational";

  return { overall, categories };
}

app.get("/api/public/status", async (req, res) => {
  const { overall, categories } = getPublicStatusCategories();

  res.set("Cache-Control", "no-store");
  res.json({
    ok: true,
    status: overall,
    categories,
    updated_at: systemHealthSnapshot.updatedAt,
  });
});

app.get(
  "/api/admin/status",
  authenticateAdmin,
  requireRole("super_admin"),
  async (req, res) => {
    res.json({
      ok: true,
      updated_at: systemHealthSnapshot.updatedAt,
      checks: systemHealthSnapshot.checks,
      public_view: getPublicStatusCategories(),
    });
  },
);

/*
|--------------------------------------------------------------------------
| PUBLIC API (v1) — Wowza-parity item 5, 2026-08-10
|--------------------------------------------------------------------------
| Read-only for v1, deliberately: the actual demand here is orgs pulling
| their own stream/recording/analytics data into their own ChMS or
| dashboards, not remote-controlling broadcasts — and read-only means a
| leaked or misused key can never disrupt a live broadcast, delete a
| recording, or touch billing. Write operations (create/manage streams)
| can be added later once this read surface has proven itself; starting
| narrow and expanding is a lot safer than the reverse.
|
| Keys are per-organization, generated by that org's own owner/admin —
| same self-serve model as everything else on this platform, not
| something the platform team issues by hand. The actual key is only ever
| shown once, at creation — only its SHA-256 hash is stored, same
| principle as a password, appropriate here because the key itself
| already has ~190 bits of entropy (not a low-entropy human password
| needing bcrypt's deliberate slowness).
*/

// Reuses the same alphanumeric alphabet/generator already established
// for stream keys (randomAlphanumeric, defined near STREAM_KEY_ALPHABET
// above) — no need for a second one.
const generateApiKey = () => `nlmapi_${randomAlphanumeric(40)}`;

const hashApiKey = (key) =>
  crypto.createHash("sha256").update(key).digest("hex");

async function ensureApiKeysTable() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS api_keys (
      id SERIAL PRIMARY KEY,
      organization_id INTEGER NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
      key_hash VARCHAR(64) NOT NULL UNIQUE,
      key_prefix VARCHAR(20) NOT NULL,
      label VARCHAR(255),
      created_by_admin_id INTEGER REFERENCES admins(id) ON DELETE SET NULL,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      last_used_at TIMESTAMPTZ,
      revoked_at TIMESTAMPTZ
    )
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_api_keys_organization_id
    ON api_keys (organization_id)
  `);
}

// Auth for the /api/v1/* public routes — completely separate from
// authenticateAdmin's JWT session auth below. Reads a bearer API key,
// resolves it to an organization, and scopes everything downstream to
// that organization only. Deliberately does NOT touch req.admin/
// req.organization (the JWT-auth shapes) so the two auth systems can
// never be accidentally cross-used.
async function authenticateApiKey(req, res, next) {
  const authHeader = req.headers.authorization || "";
  const key = authHeader.startsWith("Bearer ")
    ? authHeader.slice(7).trim()
    : "";

  if (!key) {
    return res.status(401).json({
      ok: false,
      message: "Missing API key. Send it as: Authorization: Bearer <key>",
    });
  }

  try {
    const keyHash = hashApiKey(key);

    const result = await pool.query(
      `
      SELECT ak.id, ak.organization_id, o.is_active AS org_active
      FROM api_keys ak
      JOIN organizations o ON o.id = ak.organization_id
      WHERE ak.key_hash = $1 AND ak.revoked_at IS NULL
      LIMIT 1
      `,
      [keyHash],
    );

    const apiKeyRow = result.rows[0];

    if (!apiKeyRow || !apiKeyRow.org_active) {
      return res
        .status(401)
        .json({ ok: false, message: "Invalid or revoked API key" });
    }

    req.apiKeyOrganizationId = apiKeyRow.organization_id;

    // Fire-and-forget — never let a slow/failed write to last_used_at
    // hold up or break the actual request.
    pool
      .query(`UPDATE api_keys SET last_used_at = NOW() WHERE id = $1`, [
        apiKeyRow.id,
      ])
      .catch(() => {});

    next();
  } catch (error) {
    console.error("API key auth error:", error);
    res.status(500).json({ ok: false, message: "Authentication failed" });
  }
}

const apiV1Limiter = rateLimit({
  windowMs: 60 * 1000,
  max: 300,
  message: {
    ok: false,
    message: "Too many requests. Please slow down.",
  },
  standardHeaders: true,
  legacyHeaders: false,
});

app.get(
  "/api/v1/streams",
  apiV1Limiter,
  authenticateApiKey,
  async (req, res) => {
    try {
      const result = await pool.query(
        `
        SELECT id, name, is_live, live_started_at, created_at
        FROM channels
        WHERE organization_id = $1
        ORDER BY created_at DESC
        `,
        [req.apiKeyOrganizationId],
      );

      res.json({ ok: true, streams: result.rows });
    } catch (error) {
      console.error("API v1 streams list error:", error);
      res.status(500).json({ ok: false, message: "Failed to load streams" });
    }
  },
);

app.get(
  "/api/v1/streams/:id",
  apiV1Limiter,
  authenticateApiKey,
  async (req, res) => {
    try {
      const result = await pool.query(
        `
        SELECT id, name, is_live, live_started_at, created_at
        FROM channels
        WHERE id = $1 AND organization_id = $2
        `,
        [req.params.id, req.apiKeyOrganizationId],
      );

      if (!result.rows[0]) {
        return res.status(404).json({ ok: false, message: "Stream not found" });
      }

      res.json({ ok: true, stream: result.rows[0] });
    } catch (error) {
      console.error("API v1 stream detail error:", error);
      res.status(500).json({ ok: false, message: "Failed to load stream" });
    }
  },
);

app.get(
  "/api/v1/recordings",
  apiV1Limiter,
  authenticateApiKey,
  async (req, res) => {
    try {
      const result = await pool.query(
        `
        SELECT r.*, c.name AS channel_name
        FROM recordings r
        LEFT JOIN channels c ON c.id = r.channel_id
        WHERE r.organization_id = $1
        ORDER BY r.created_at DESC
        LIMIT 100
        `,
        [req.apiKeyOrganizationId],
      );

      res.json({
        ok: true,
        recordings: result.rows.map((row) => mapRecordingRowToDto(row)),
      });
    } catch (error) {
      console.error("API v1 recordings list error:", error);
      res.status(500).json({ ok: false, message: "Failed to load recordings" });
    }
  },
);

app.get(
  "/api/v1/recordings/:id",
  apiV1Limiter,
  authenticateApiKey,
  async (req, res) => {
    try {
      const result = await pool.query(
        `
        SELECT r.*, c.name AS channel_name
        FROM recordings r
        LEFT JOIN channels c ON c.id = r.channel_id
        WHERE r.id = $1 AND r.organization_id = $2
        `,
        [req.params.id, req.apiKeyOrganizationId],
      );

      if (!result.rows[0]) {
        return res
          .status(404)
          .json({ ok: false, message: "Recording not found" });
      }

      res.json({
        ok: true,
        recording: mapRecordingRowToDto(result.rows[0]),
      });
    } catch (error) {
      console.error("API v1 recording detail error:", error);
      res.status(500).json({ ok: false, message: "Failed to load recording" });
    }
  },
);

app.get(
  "/api/v1/analytics/:streamId",
  apiV1Limiter,
  authenticateApiKey,
  async (req, res) => {
    try {
      const channelResult = await pool.query(
        `SELECT id, name FROM channels WHERE id = $1 AND organization_id = $2`,
        [req.params.streamId, req.apiKeyOrganizationId],
      );
      const channel = channelResult.rows[0];
      if (!channel) {
        return res.status(404).json({ ok: false, message: "Stream not found" });
      }

      const days = Math.max(1, Math.min(90, Number(req.query.days) || 30));

      const statsResult = await pool.query(
        `
        SELECT
          COUNT(*)::int AS total_sessions,
          COUNT(DISTINCT vs.viewer_id)::int AS unique_viewers,
          COALESCE(SUM(vs.duration_seconds), 0)::bigint AS total_watch_seconds
        FROM viewer_sessions vs
        JOIN channels c ON c.stream_key = vs.stream_key
        WHERE c.id = $1
          AND vs.started_at >= NOW() - ($2::text || ' days')::interval
        `,
        [channel.id, days],
      );

      const stats = statsResult.rows[0] || {};

      res.json({
        ok: true,
        stream_id: channel.id,
        stream_name: channel.name,
        period_days: days,
        total_sessions: Number(stats.total_sessions || 0),
        unique_viewers: Number(stats.unique_viewers || 0),
        total_watch_seconds: Number(stats.total_watch_seconds || 0),
      });
    } catch (error) {
      console.error("API v1 analytics error:", error);
      res.status(500).json({ ok: false, message: "Failed to load analytics" });
    }
  },
);

// ── Admin-facing key management — uses the EXISTING JWT session auth,
// not authenticateApiKey above. This is how an org's own owner/admin
// generates and revokes their own keys through the dashboard. ──

app.post(
  "/api/organizations/current/api-keys",
  authenticateAdmin,
  resolveOrganizationForRequest,
  requireRole("super_admin", "admin", "operator"),
  requireOrganizationRole("owner", "admin"),
  async (req, res) => {
    try {
      const label = String(req.body.label || "").slice(0, 255) || null;
      const rawKey = generateApiKey();
      const keyHash = hashApiKey(rawKey);
      const keyPrefix = rawKey.slice(0, 14);

      const result = await pool.query(
        `
        INSERT INTO api_keys
          (organization_id, key_hash, key_prefix, label, created_by_admin_id)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING id, key_prefix, label, created_at
        `,
        [req.organization.id, keyHash, keyPrefix, label, req.admin.id],
      );

      // The only time the real key is ever visible — not retrievable
      // again after this response, by design.
      res.json({
        ok: true,
        message:
          "Copy this key now — it will not be shown again after you leave this page.",
        api_key: rawKey,
        key: result.rows[0],
      });
    } catch (error) {
      console.error("Create API key error:", error);
      res.status(500).json({ ok: false, message: "Failed to create API key" });
    }
  },
);

app.get(
  "/api/organizations/current/api-keys",
  authenticateAdmin,
  resolveOrganizationForRequest,
  requireOrganizationRole("owner", "admin"),
  async (req, res) => {
    try {
      const result = await pool.query(
        `
        SELECT id, key_prefix, label, created_at, last_used_at, revoked_at
        FROM api_keys
        WHERE organization_id = $1
        ORDER BY created_at DESC
        `,
        [req.organization.id],
      );

      res.json({ ok: true, keys: result.rows });
    } catch (error) {
      console.error("List API keys error:", error);
      res.status(500).json({ ok: false, message: "Failed to load API keys" });
    }
  },
);

app.delete(
  "/api/organizations/current/api-keys/:id",
  authenticateAdmin,
  resolveOrganizationForRequest,
  requireRole("super_admin", "admin", "operator"),
  requireOrganizationRole("owner", "admin"),
  async (req, res) => {
    try {
      const result = await pool.query(
        `
        UPDATE api_keys
        SET revoked_at = NOW()
        WHERE id = $1 AND organization_id = $2 AND revoked_at IS NULL
        RETURNING id
        `,
        [req.params.id, req.organization.id],
      );

      if (!result.rows[0]) {
        return res
          .status(404)
          .json({ ok: false, message: "API key not found or already revoked" });
      }

      res.json({ ok: true, message: "API key revoked" });
    } catch (error) {
      console.error("Revoke API key error:", error);
      res.status(500).json({ ok: false, message: "Failed to revoke API key" });
    }
  },
);

const SERVER_HOSTNAME_CONFIRM =
  process.env.SERVER_HOSTNAME_CONFIRM || "bscapi54";
const SRS_DOCKER_CONTAINER = process.env.SRS_DOCKER_CONTAINER || "";

async function ensureRestartAuditTable() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS restart_audit_log (
      id SERIAL PRIMARY KEY,
      admin_id INTEGER REFERENCES admins(id) ON DELETE SET NULL,
      admin_email VARCHAR(255),
      action VARCHAR(20) NOT NULL, -- 'backend' | 'srs' | 'srs-auto' (watchdog-triggered)
      reason TEXT,
      active_streams_at_time INTEGER NOT NULL DEFAULT 0,
      affected_organizations TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);
}

// Shared by both restart endpoints and usable by anything else that needs
// to know "how much would this action actually break right now."
async function getActiveStreamsSnapshot() {
  const channelsResult = await pool.query(
    `SELECT c.stream_key, c.name AS channel_name, o.name AS organization_name
     FROM channels c
     JOIN organizations o ON o.id = c.organization_id`,
  );
  const channelByStreamKey = new Map(
    channelsResult.rows.map((row) => [String(row.stream_key), row]),
  );

  try {
    const srsResponse = await fetch(`${SRS_API_URL}/api/v1/streams`);
    if (!srsResponse.ok) throw new Error(`SRS responded ${srsResponse.status}`);
    const srsData = await srsResponse.json();

    const active = (srsData.streams || [])
      .filter((s) => s.publish?.active && channelByStreamKey.has(s.name))
      .map((s) => channelByStreamKey.get(s.name));

    return { srsAvailable: true, count: active.length, activeStreams: active };
  } catch (srsError) {
    console.error("Restart snapshot: SRS unavailable:", srsError.message);
    return { srsAvailable: false, count: 0, activeStreams: [] };
  }
}

async function logRestartAudit({ req, action, reason, snapshot }) {
  await pool.query(
    `INSERT INTO restart_audit_log
       (admin_id, admin_email, action, reason, active_streams_at_time, affected_organizations)
     VALUES ($1, $2, $3, $4, $5, $6)`,
    [
      req.admin.id,
      req.admin.email || null,
      action,
      reason || null,
      snapshot.count,
      snapshot.activeStreams.map((s) => s.organization_name).join(", ") || null,
    ],
  );
}

app.get(
  "/api/admin/restart-audit-log",
  authenticateAdmin,
  requireRole("super_admin"),
  async (req, res) => {
    try {
      const result = await pool.query(
        `SELECT id, admin_email, action, reason, active_streams_at_time,
                affected_organizations, created_at
         FROM restart_audit_log ORDER BY created_at DESC LIMIT 20`,
      );
      res.json({ ok: true, entries: result.rows });
    } catch (error) {
      console.error("Restart audit log error:", error);
      res
        .status(500)
        .json({ ok: false, message: "Failed to load restart audit log" });
    }
  },
);

// Snapshot endpoint the frontend calls right before showing the confirm
// dialog, so the person sees an accurate "this will drop N streams across:
// Org A, Org B" warning instead of a generic confirm prompt.
app.get(
  "/api/admin/active-streams-snapshot",
  authenticateAdmin,
  requireRole("super_admin"),
  async (req, res) => {
    try {
      const snapshot = await getActiveStreamsSnapshot();
      res.json({ ok: true, ...snapshot });
    } catch (error) {
      console.error("Active streams snapshot error:", error);
      res
        .status(500)
        .json({ ok: false, message: "Failed to check active streams" });
    }
  },
);

app.post(
  "/api/admin/restart-backend",
  authenticateAdmin,
  requireRole("super_admin"),
  async (req, res) => {
    try {
      const { confirm_text, reason } = req.body;
      const snapshot = await getActiveStreamsSnapshot();
      const requiredPhrase =
        snapshot.count > 0 ? SERVER_HOSTNAME_CONFIRM : "RESTART BACKEND";

      if (confirm_text !== requiredPhrase) {
        return res.status(400).json({
          ok: false,
          message: `Confirmation text didn't match. Type "${requiredPhrase}" to proceed.`,
          required_confirm_text: requiredPhrase,
          active_streams: snapshot.count,
          affected_organizations: snapshot.activeStreams.map(
            (s) => s.organization_name,
          ),
        });
      }

      await logRestartAudit({ req, action: "backend", reason, snapshot });

      res.json({
        ok: true,
        message:
          snapshot.count > 0
            ? `Restarting backend now. ${snapshot.count} active stream(s) will keep publishing to SRS uninterrupted, but chat/reactions/viewer counts will briefly disconnect and reconnect.`
            : "Restarting backend now — back online in a few seconds.",
      });

      // Respond before restarting -- this process is about to be killed and
      // relaunched by PM2, so the HTTP response must go out first.
      setTimeout(() => {
        exec("pm2 restart nlm-stream-backend", (err) => {
          if (err) console.error("PM2 restart command failed:", err.message);
        });
      }, 400);
    } catch (error) {
      console.error("Restart backend error:", error);
      res.status(500).json({ ok: false, message: "Failed to restart backend" });
    }
  },
);

app.post(
  "/api/admin/restart-srs",
  authenticateAdmin,
  requireRole("super_admin"),
  async (req, res) => {
    try {
      if (!SRS_DOCKER_CONTAINER) {
        return res.status(503).json({
          ok: false,
          message:
            "SRS_DOCKER_CONTAINER is not set in .env. Run `docker ps` to find the real container name and set it before this action can be used.",
        });
      }

      const { confirm_text, reason } = req.body;
      const snapshot = await getActiveStreamsSnapshot();
      const requiredPhrase =
        snapshot.count > 0 ? SERVER_HOSTNAME_CONFIRM : "RESTART SRS";

      if (confirm_text !== requiredPhrase) {
        return res.status(400).json({
          ok: false,
          message: `Confirmation text didn't match. Type "${requiredPhrase}" to proceed.`,
          required_confirm_text: requiredPhrase,
          active_streams: snapshot.count,
          affected_organizations: snapshot.activeStreams.map(
            (s) => s.organization_name,
          ),
        });
      }

      await logRestartAudit({ req, action: "srs", reason, snapshot });

      // SRS restarting doesn't kill this Node process, so we can await the
      // command and report real success/failure back, unlike restart-backend.
      exec(`docker restart ${SRS_DOCKER_CONTAINER}`, (err, stdout, stderr) => {
        if (err) {
          console.error("SRS restart command failed:", err.message, stderr);
          return res.status(500).json({
            ok: false,
            message: "docker restart command failed — check server logs",
            error: err.message,
          });
        }
        res.json({
          ok: true,
          message:
            snapshot.count > 0
              ? `SRS restarted. ${snapshot.count} active stream(s) were dropped and will need to republish: ${snapshot.activeStreams.map((s) => s.organization_name).join(", ")}.`
              : "SRS restarted — no active streams were affected.",
        });
      });
    } catch (error) {
      console.error("Restart SRS error:", error);
      res.status(500).json({ ok: false, message: "Failed to restart SRS" });
    }
  },
);

/*
|--------------------------------------------------------------------------
| SUPER ADMIN DASHBOARD — AI Support Assistant
|--------------------------------------------------------------------------
| Bundles real diagnostic data (recent errors, integration health, server
| status, active streams, restart history, and -- if an org is selected in
| Support Mode -- that org's specific channels/simulcasts/errors) and sends
| it to the Claude API alongside the admin's plain-English question. This
| does NOT give Claude any ability to take action -- it's read-only context
| in, plain-English diagnosis out. Nothing here can restart anything,
| modify data, or act on the admin's behalf.
*/

const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY || "";
const SUPPORT_ASSISTANT_MODEL = "claude-sonnet-5";

app.post(
  "/api/admin/support-assistant",
  authenticateAdmin,
  requireRole("super_admin"),
  async (req, res) => {
    try {
      const { question, organizationId } = req.body;
      if (!question || typeof question !== "string" || !question.trim()) {
        return res
          .status(400)
          .json({ ok: false, message: "A question is required" });
      }

      if (!ANTHROPIC_API_KEY) {
        return res.status(503).json({
          ok: false,
          message:
            "The AI Support Assistant isn't configured yet — set ANTHROPIC_API_KEY in .env (get one at console.anthropic.com).",
        });
      }

      // ── Gather context, all read-only ──
      const [serverStatus, integrationHealth, streamsSnapshot, restartHistory] =
        await Promise.all([
          Promise.resolve(getServerStatusSnapshot()),
          getIntegrationHealthSnapshot(),
          getActiveStreamsSnapshot(),
          pool.query(
            `SELECT admin_email, action, reason, active_streams_at_time, affected_organizations, created_at
             FROM restart_audit_log ORDER BY created_at DESC LIMIT 5`,
          ),
        ]);

      let orgContext = null;
      let errorEntries;

      if (organizationId) {
        const orgResult = await pool.query(
          `SELECT id, name FROM organizations WHERE id = $1`,
          [organizationId],
        );
        const organization = orgResult.rows[0];

        if (organization) {
          const channelsResult = await pool.query(
            `SELECT name, stream_key, is_live, live_started_at FROM channels WHERE organization_id = $1`,
            [organizationId],
          );
          const socialResult = await pool.query(
            `SELECT sd.platform, sd.is_running, c.name AS channel_name
             FROM social_destinations sd
             JOIN channels c ON c.id = sd.channel_id
             WHERE c.organization_id = $1`,
            [organizationId],
          );
          orgContext = {
            name: organization.name,
            channels: channelsResult.rows,
            social_destinations: socialResult.rows,
          };
        }

        errorEntries = recentErrorLog
          .filter((e) => String(e.organization_id) === String(organizationId))
          .slice(-15)
          .reverse();
      } else {
        errorEntries = [...recentErrorLog].slice(-15).reverse();
      }

      const contextBlock = `
PLATFORM SERVER STATUS:
${JSON.stringify(serverStatus, null, 2)}

INTEGRATION HEALTH:
${JSON.stringify(integrationHealth, null, 2)}

ACTIVE STREAMS RIGHT NOW:
SRS reachable: ${streamsSnapshot.srsAvailable}
Count: ${streamsSnapshot.count}
${streamsSnapshot.activeStreams.map((s) => `- ${s.organization_name} / ${s.channel_name}`).join("\n") || "(none)"}

RECENT RESTART HISTORY (last 5):
${restartHistory.rows.map((r) => `- ${r.created_at}: ${r.admin_email} restarted ${r.action}${r.reason ? ` (${r.reason})` : ""}`).join("\n") || "(none)"}

${
  organizationId
    ? `SELECTED ORGANIZATION: ${orgContext ? orgContext.name : `(id ${organizationId} — not found)`}
${
  orgContext
    ? `Channels:
${orgContext.channels.map((c) => `- ${c.name}: is_live=${c.is_live}, started_at=${c.live_started_at || "n/a"}`).join("\n") || "(none)"}
Social destinations:
${orgContext.social_destinations.map((s) => `- ${s.channel_name} → ${s.platform}: running=${s.is_running}`).join("\n") || "(none)"}`
    : ""
}
RECENT ERRORS FOR THIS ORGANIZATION (last 15):`
    : `RECENT PLATFORM-WIDE ERRORS (last 15):`
}
${errorEntries.map((e) => `- ${e.timestamp || e.created_at || "?"}: ${e.message || JSON.stringify(e)}`).join("\n") || "(none logged)"}
`.trim();

      const anthropicRes = await fetch(
        "https://api.anthropic.com/v1/messages",
        {
          method: "POST",
          headers: {
            "content-type": "application/json",
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
          },
          body: JSON.stringify({
            model: SUPPORT_ASSISTANT_MODEL,
            max_tokens: 1024,
            system:
              "You are a technical support assistant embedded in the NLM Streaming Manager's super-admin dashboard. " +
              "You are given real, read-only diagnostic data about the platform (server status, integration health, active streams, restart history, and error logs). " +
              "Answer the admin's question using ONLY the data provided -- never invent facts, stream keys, error details, or org info that isn't in the context. " +
              "If the data doesn't contain enough to answer confidently, say what's missing and suggest what to check next. " +
              "Be concise and plain-English -- this is read by a busy operator, not a report. Do not suggest destructive actions (restarts, deletions) unless the data clearly shows they're warranted, and even then, flag it as a suggestion for the admin to do manually, not something you can do.",
            messages: [
              {
                role: "user",
                content: `${contextBlock}\n\nQUESTION: ${question.trim()}`,
              },
            ],
          }),
          signal: AbortSignal.timeout(30000),
        },
      );

      if (!anthropicRes.ok) {
        const errBody = await anthropicRes.text().catch(() => "");
        console.error(
          "Support assistant: Anthropic API error:",
          anthropicRes.status,
          errBody,
        );
        return res.status(502).json({
          ok: false,
          message:
            anthropicRes.status === 401
              ? "AI Support Assistant is misconfigured — check ANTHROPIC_API_KEY"
              : "The AI Support Assistant couldn't get a response — try again in a moment",
        });
      }

      const data = await anthropicRes.json();
      const answer =
        data.content?.find((block) => block.type === "text")?.text || "";

      res.json({ ok: true, answer, organization: orgContext?.name || null });
    } catch (error) {
      console.error("Support assistant error:", error);
      res.status(500).json({
        ok: false,
        message: "Failed to get a response from the assistant",
      });
    }
  },
);

require("./oauth_routes")(app, pool, jwt, {
  authenticateAdmin,
  resolveOrganizationForRequest,
  requireRole,
  requireOrganizationRole,
});

// Phase 1 — embedded player (Copy Embed Code). Registered here (rather than
// right after getPublicWatchStatus's definition) purely so it sits next to
// the other feature-module mounts, but it does depend on getPublicWatchStatus
// already being defined above this line.
embedRoutes.register(app, pool, {
  getPublicWatchStatus,
  authenticateAdmin,
  resolveOrganizationForRequest,
  requireRole,
  requireOrganizationRole,
});

/*
|--------------------------------------------------------------------------
| SOCIAL DESTINATIONS (Facebook / YouTube simulcasting)
|--------------------------------------------------------------------------
*/
const socialProcesses = new Map(); // destinationId -> ChildProcess

const SOCIAL_PLATFORMS = {
  facebook: {
    label: "Facebook",
    rtmpBase: "rtmps://live-api-s.facebook.com:443/rtmp",
  },
  youtube: {
    label: "YouTube",
    rtmpBase: "rtmp://a.rtmp.youtube.com/live2",
  },
  // Instagram has no Graph-API equivalent to Facebook/YouTube's "create a
  // broadcast, get RTMP credentials back" flow — Meta only offers Live
  // Producer, a manual, browser-only tool (instagram.com, not the app)
  // that issues a fresh one-time Stream URL + Stream Key per broadcast,
  // requiring a Business or Creator account. There is no OAuth
  // automation to build here — this is deliberately manual-key-only,
  // same as Facebook/YouTube's existing 'manual' automation_mode.
  // rtmpBase below is Live Producer's documented ingest host; if a real
  // broadcast fails to connect, check whether Instagram handed back a
  // materially different host in that session's generated URL and
  // update this — Instagram doesn't publish a stable public spec for it
  // the way Facebook/YouTube do.
  instagram: {
    label: "Instagram",
    rtmpBase: "rtmps://live-upload.instagram.com:443/rtmp",
  },
};

async function isSrsStreamLive(streamKey) {
  try {
    const res = await fetch("http://127.0.0.1:1985/api/v1/streams/", {
      signal: AbortSignal.timeout(5000),
    });
    const data = await res.json();
    const streams = data.streams || [];
    return streams.some(
      (s) => s.name === streamKey && s.publish && s.publish.active,
    );
  } catch (err) {
    console.error("[SOCIAL] SRS stream check failed:", err.message);
    return false;
  }
}

async function getOwnedChannel(channelId, organizationId) {
  const result = await pool.query(
    `SELECT * FROM channels WHERE id = $1 AND organization_id = $2`,
    [channelId, organizationId],
  );
  return result.rows[0] || null;
}

app.get(
  "/api/channels/:channelId/social-destinations",
  authenticateAdmin,
  resolveOrganizationForRequest,
  async (req, res) => {
    try {
      const channel = await getOwnedChannel(
        req.params.channelId,
        req.organization.id,
      );
      if (!channel) {
        return res
          .status(404)
          .json({ ok: false, message: "Channel not found" });
      }

      const result = await pool.query(
        `SELECT * FROM social_destinations WHERE channel_id = $1 ORDER BY platform`,
        [channel.id],
      );

      res.json({ ok: true, destinations: result.rows });
    } catch (error) {
      console.error("Get Social Destinations Error:", error);
      res
        .status(500)
        .json({ ok: false, message: "Failed to fetch social destinations" });
    }
  },
);

app.post(
  "/api/channels/:channelId/social-destinations",
  authenticateAdmin,
  resolveOrganizationForRequest,
  requireRole("super_admin", "admin", "operator"),
  requireOrganizationRole("owner", "admin"),
  async (req, res) => {
    try {
      const { platform, stream_key } = req.body;

      if (!platform || !SOCIAL_PLATFORMS[platform]) {
        return res.status(400).json({
          ok: false,
          message: "platform must be 'facebook' or 'youtube'",
        });
      }
      if (!stream_key) {
        return res
          .status(400)
          .json({ ok: false, message: "stream_key is required" });
      }

      const channel = await getOwnedChannel(
        req.params.channelId,
        req.organization.id,
      );
      if (!channel) {
        return res
          .status(404)
          .json({ ok: false, message: "Channel not found" });
      }

      const result = await pool.query(
        `
        INSERT INTO social_destinations (channel_id, platform, stream_key)
        VALUES ($1, $2, $3)
        ON CONFLICT (channel_id, platform)
        DO UPDATE SET stream_key = EXCLUDED.stream_key, updated_at = now()
        RETURNING *
        `,
        [channel.id, platform, stream_key],
      );

      res.json({ ok: true, destination: result.rows[0] });
    } catch (error) {
      console.error("Save Social Destination Error:", error);
      res
        .status(500)
        .json({ ok: false, message: "Failed to save social destination" });
    }
  },
);

app.delete(
  "/api/channels/:channelId/social-destinations/:id",
  authenticateAdmin,
  resolveOrganizationForRequest,
  requireRole("super_admin", "admin", "operator"),
  requireOrganizationRole("owner", "admin"),
  async (req, res) => {
    try {
      const channel = await getOwnedChannel(
        req.params.channelId,
        req.organization.id,
      );
      if (!channel) {
        return res
          .status(404)
          .json({ ok: false, message: "Channel not found" });
      }

      const existingProc = socialProcesses.get(Number(req.params.id));
      if (existingProc) {
        existingProc.kill("SIGTERM");
        socialProcesses.delete(Number(req.params.id));
      }

      await pool.query(
        `DELETE FROM social_destinations WHERE id = $1 AND channel_id = $2`,
        [req.params.id, channel.id],
      );

      res.json({ ok: true, message: "Social destination removed" });
    } catch (error) {
      console.error("Delete Social Destination Error:", error);
      res
        .status(500)
        .json({ ok: false, message: "Failed to delete social destination" });
    }
  },
);

app.post(
  "/api/channels/:channelId/social-destinations/:id/start",
  authenticateAdmin,
  resolveOrganizationForRequest,
  requireRole("super_admin", "admin", "operator"),
  requireOrganizationRole("owner", "admin"),
  async (req, res) => {
    try {
      const channel = await getOwnedChannel(
        req.params.channelId,
        req.organization.id,
      );
      if (!channel) {
        return res
          .status(404)
          .json({ ok: false, message: "Channel not found" });
      }

      const destResult = await pool.query(
        `SELECT * FROM social_destinations WHERE id = $1 AND channel_id = $2`,
        [req.params.id, channel.id],
      );
      const destination = destResult.rows[0];
      if (!destination) {
        return res
          .status(404)
          .json({ ok: false, message: "Social destination not found" });
      }

      if (socialProcesses.has(destination.id)) {
        return res.status(400).json({
          ok: false,
          message: "Already simulcasting to this platform",
        });
      }

      const live = await isSrsStreamLive(channel.stream_key);
      if (!live) {
        return res.status(400).json({
          ok: false,
          message: "Main stream is not live yet. Start streaming first.",
        });
      }

      const platformConfig = SOCIAL_PLATFORMS[destination.platform];
      const destinationUrl = `${platformConfig.rtmpBase}/${destination.stream_key}`;
      const sourceUrl = getInternalHlsSourceUrl(channel.stream_key);

      // Same aac_adtstoasc fix as startOauthSimulcast below — see that
      // function's comment for the full explanation. Manual mode pushes
      // through this identical ffmpeg shape, so it was equally exposed.
      const proc = spawn("ffmpeg", [
        ...inputResilienceFlags,
        "-i",
        sourceUrl,
        "-c:v",
        "copy",
        "-c:a",
        "aac",
        "-b:a",
        "128k",
        "-err_detect",
        "ignore_err",
        "-f",
        "flv",
        destinationUrl,
      ]);

      proc.stderr.on("data", (data) => {
        console.log(
          `[SOCIAL ${destination.platform} #${destination.id}]`,
          data.toString().slice(0, 300),
        );
      });

      proc.on("exit", (code) => {
        console.log(
          `[SOCIAL ${destination.platform} #${destination.id}] exited with code ${code}`,
        );
        socialProcesses.delete(destination.id);
        pool
          .query(
            `UPDATE social_destinations SET is_running = false, ffmpeg_pid = NULL WHERE id = $1`,
            [destination.id],
          )
          .catch((err) =>
            console.error(
              "[SOCIAL] Failed to update state on exit:",
              err.message,
            ),
          );
      });

      socialProcesses.set(destination.id, proc);

      await pool.query(
        `UPDATE social_destinations SET is_running = true, ffmpeg_pid = $1, started_at = now() WHERE id = $2`,
        [proc.pid, destination.id],
      );

      res.json({
        ok: true,
        message: `Simulcasting to ${platformConfig.label} started`,
      });
    } catch (error) {
      console.error("Start Social Destination Error:", error);
      res.status(500).json({ ok: false, message: "Failed to start simulcast" });
    }
  },
);

app.post(
  "/api/channels/:channelId/social-destinations/:id/stop",
  authenticateAdmin,
  resolveOrganizationForRequest,
  requireRole("super_admin", "admin", "operator"),
  requireOrganizationRole("owner", "admin"),
  async (req, res) => {
    try {
      const channel = await getOwnedChannel(
        req.params.channelId,
        req.organization.id,
      );
      if (!channel) {
        return res
          .status(404)
          .json({ ok: false, message: "Channel not found" });
      }

      const destId = Number(req.params.id);
      const proc = socialProcesses.get(destId);

      if (proc) {
        proc.kill("SIGTERM");
        socialProcesses.delete(destId);
      }

      await pool.query(
        `UPDATE social_destinations SET is_running = false, ffmpeg_pid = NULL WHERE id = $1 AND channel_id = $2`,
        [destId, channel.id],
      );

      res.json({ ok: true, message: "Simulcast stopped" });
    } catch (error) {
      console.error("Stop Social Destination Error:", error);
      res.status(500).json({ ok: false, message: "Failed to stop simulcast" });
    }
  },
);

/*
|--------------------------------------------------------------------------
| SOCIAL DESTINATIONS — OAUTH-AUTOMATED GO LIVE
|--------------------------------------------------------------------------
| Same socialProcesses Map and ffmpeg spawn pattern as the manual start/stop
| routes above. The difference: instead of pushing to a client-pasted
| persistent stream key, this creates a fresh broadcast on the platform via
| the connected OAuth account and pushes to whatever RTMP URL it hands back.
| Gated behind automation_mode = 'oauth' on the destination row, so existing
| manual-key destinations are completely unaffected.
*/

// Core go-live logic, shared by the manual "Go Live" button route below AND
// the auto-fire hook in on_publish (see SOCIAL-OAUTH AUTO GO-LIVE section
// near on_publish). Returns a plain {ok, message} result instead of writing
// to res directly so both callers can use it — on_publish in particular must
// never be blocked by or throw from this, since SRS is waiting on that
// webhook's response to allow/reject the connection.
async function startOauthSimulcast(channel, destination, organizationId) {
  if (
    destination.automation_mode !== "oauth" ||
    !destination.oauth_account_id
  ) {
    return {
      ok: false,
      message:
        "This destination isn't linked to a connected account. Use the manual start instead, or connect an account first.",
    };
  }
  if (socialProcesses.has(destination.id)) {
    // Reconnect/blip guard: if a push is already running for this
    // destination, skip rather than spin up a second platform broadcast.
    return { ok: false, message: "Already simulcasting to this platform" };
  }

  const live = await isSrsStreamLive(channel.stream_key);
  if (!live) {
    return {
      ok: false,
      message: "Main stream is not live yet. Start streaming first.",
    };
  }

  const accountResult = await pool.query(
    `SELECT * FROM social_oauth_accounts WHERE id = $1 AND organization_id = $2`,
    [destination.oauth_account_id, organizationId],
  );
  const account = accountResult.rows[0];
  if (!account) {
    return { ok: false, message: "Connected account not found" };
  }

  const sourceUrl = getInternalHlsSourceUrl(channel.stream_key);
  let destinationUrl, platformBroadcastId, platformStreamId;

  try {
    if (destination.platform === "facebook") {
      const created = await facebookGraph.createLiveVideo({
        pageId: account.external_account_id,
        pageAccessToken: account.access_token,
        title: channel.name,
      });
      destinationUrl = created.rtmpUrl;
      platformBroadcastId = created.liveVideoId;
    } else if (destination.platform === "youtube") {
      // Access tokens are short-lived (~1hr) — refresh proactively rather
      // than waiting for a 401 mid-request.
      let accessToken = account.access_token;
      if (
        !account.token_expires_at ||
        new Date(account.token_expires_at) <
          new Date(Date.now() + 5 * 60 * 1000)
      ) {
        const refreshed = await youtubeApi.refreshAccessToken(
          account.refresh_token,
        );
        accessToken = refreshed.access_token;
        await pool.query(
          `UPDATE social_oauth_accounts SET access_token = $1, token_expires_at = $2, updated_at = now() WHERE id = $3`,
          [
            accessToken,
            refreshed.expiry_date ? new Date(refreshed.expiry_date) : null,
            account.id,
          ],
        );
      }
      const oauth2Client = youtubeApi.clientFromTokens({
        accessToken,
        refreshToken: account.refresh_token,
      });
      const created = await youtubeApi.createBroadcastAndStream(oauth2Client, {
        title: channel.name,
      });
      destinationUrl = created.rtmpUrl;
      platformBroadcastId = created.broadcastId;
      platformStreamId = created.streamId;
    } else {
      return { ok: false, message: "Unsupported platform for automation" };
    }
  } catch (platformErr) {
    console.error(
      `[SOCIAL-OAUTH] Failed to create broadcast on ${destination.platform}:`,
      platformErr.message,
    );
    return {
      ok: false,
      message: platformErr.message || "Failed to create platform broadcast",
    };
  }

  // Same bitstream-filter crash already found and fixed once for manual
  // mid-broadcast recording: copying AAC straight from an HLS/MPEG-TS
  // source into FLV requires ffmpeg's aac_adtstoasc filter to reframe it,
  // and that filter has zero tolerance for a single malformed frame — one
  // bad frame kills the whole process. Video stays copied (cheap, safe);
  // audio is re-encoded instead of copied to sidestep the fragile filter
  // entirely. inputResilienceFlags + err_detect ignore_err match the same
  // established pattern used everywhere else in this file that reads a
  // live HLS source (see buildRenditionFfmpegArgs / manual recording).
  const proc = spawn("ffmpeg", [
    ...inputResilienceFlags,
    "-i",
    sourceUrl,
    "-c:v",
    "copy",
    "-c:a",
    "aac",
    "-b:a",
    "128k",
    "-err_detect",
    "ignore_err",
    "-f",
    "flv",
    destinationUrl,
  ]);

  proc.stderr.on("data", (data) => {
    console.log(
      `[SOCIAL-OAUTH ${destination.platform} #${destination.id}]`,
      data.toString().slice(0, 300),
    );
  });

  proc.on("exit", (code) => {
    console.log(
      `[SOCIAL-OAUTH ${destination.platform} #${destination.id}] exited with code ${code}`,
    );
    socialProcesses.delete(destination.id);
    pool
      .query(
        `UPDATE social_destinations SET is_running = false, ffmpeg_pid = NULL WHERE id = $1`,
        [destination.id],
      )
      .catch((err) =>
        console.error(
          "[SOCIAL-OAUTH] Failed to update state on exit:",
          err.message,
        ),
      );
  });

  socialProcesses.set(destination.id, proc);

  await pool.query(
    `UPDATE social_destinations
     SET is_running = true, ffmpeg_pid = $1, started_at = now(),
         platform_broadcast_id = $2, platform_stream_id = $3
     WHERE id = $4`,
    [proc.pid, platformBroadcastId, platformStreamId || null, destination.id],
  );

  return {
    ok: true,
    message: `Went live on ${SOCIAL_PLATFORMS[destination.platform].label} automatically`,
  };
}

app.post(
  "/api/channels/:channelId/social-destinations/:id/go-live",
  authenticateAdmin,
  resolveOrganizationForRequest,
  requireRole("super_admin", "admin", "operator"),
  requireOrganizationRole("owner", "admin"),
  async (req, res) => {
    try {
      const channel = await getOwnedChannel(
        req.params.channelId,
        req.organization.id,
      );
      if (!channel) {
        return res
          .status(404)
          .json({ ok: false, message: "Channel not found" });
      }

      const destResult = await pool.query(
        `SELECT * FROM social_destinations WHERE id = $1 AND channel_id = $2`,
        [req.params.id, channel.id],
      );
      const destination = destResult.rows[0];
      if (!destination) {
        return res
          .status(404)
          .json({ ok: false, message: "Social destination not found" });
      }

      const result = await startOauthSimulcast(
        channel,
        destination,
        req.organization.id,
      );
      res.status(result.ok ? 200 : 400).json(result);
    } catch (error) {
      console.error("Automated Go-Live Error:", error);
      res.status(500).json({
        ok: false,
        message: error.message || "Failed to start automated broadcast",
      });
    }
  },
);

// Core end-live logic, shared by the manual "End Live" button route below
// AND the auto-end hook in on_unpublish (see SOCIAL-OAUTH AUTO END-LIVE
// section near on_unpublish).
async function endOauthSimulcast(destination) {
  const destId = destination.id;
  const proc = socialProcesses.get(destId);
  if (proc) {
    proc.kill("SIGTERM");
    socialProcesses.delete(destId);
  }

  // Kill our ffmpeg push first, then tell the platform to end the broadcast —
  // otherwise Facebook/YouTube can be left showing "live" with a dead feed.
  if (
    destination.automation_mode === "oauth" &&
    destination.platform_broadcast_id
  ) {
    try {
      const accountResult = await pool.query(
        `SELECT * FROM social_oauth_accounts WHERE id = $1`,
        [destination.oauth_account_id],
      );
      const account = accountResult.rows[0];
      if (account && destination.platform === "facebook") {
        await facebookGraph.endLiveVideo({
          liveVideoId: destination.platform_broadcast_id,
          pageAccessToken: account.access_token,
        });
      } else if (account && destination.platform === "youtube") {
        const oauth2Client = youtubeApi.clientFromTokens({
          accessToken: account.access_token,
          refreshToken: account.refresh_token,
        });
        await youtubeApi.transitionBroadcast(
          oauth2Client,
          destination.platform_broadcast_id,
          "complete",
        );
      }
    } catch (platformErr) {
      // Don't fail the whole call over this — our feed is already stopped,
      // which is the important part. Log it so a stuck "live" broadcast on
      // the platform side can be caught.
      console.error(
        "[SOCIAL-OAUTH] Failed to end broadcast on platform:",
        platformErr.message,
      );
    }
  }

  await pool.query(
    `UPDATE social_destinations SET is_running = false, ffmpeg_pid = NULL WHERE id = $1`,
    [destId],
  );

  return { ok: true, message: "Broadcast ended" };
}

app.post(
  "/api/channels/:channelId/social-destinations/:id/end-live",
  authenticateAdmin,
  resolveOrganizationForRequest,
  requireRole("super_admin", "admin", "operator"),
  requireOrganizationRole("owner", "admin"),
  async (req, res) => {
    try {
      const channel = await getOwnedChannel(
        req.params.channelId,
        req.organization.id,
      );
      if (!channel) {
        return res
          .status(404)
          .json({ ok: false, message: "Channel not found" });
      }

      const destResult = await pool.query(
        `SELECT * FROM social_destinations WHERE id = $1 AND channel_id = $2`,
        [req.params.id, channel.id],
      );
      const destination = destResult.rows[0];
      if (!destination) {
        return res
          .status(404)
          .json({ ok: false, message: "Social destination not found" });
      }

      const result = await endOauthSimulcast(destination);
      res.json(result);
    } catch (error) {
      console.error("End Automated Broadcast Error:", error);
      res.status(500).json({ ok: false, message: "Failed to end broadcast" });
    }
  },
);

/*
|--------------------------------------------------------------------------
| RECORDINGS DATABASE + ARCHIVE LIBRARY + PROCESSING PIPELINE
|--------------------------------------------------------------------------
*/

const safeRecordingSegment = (value) => {
  return String(value || "")
    .replace(/\\/g, "")
    .replace(/\//g, "")
    .replace(/\.\./g, "")
    .trim();
};

const quotePath = (value) => `"${String(value || "").replace(/"/g, '\\"')}"`;

const execCommand = (command) => {
  return new Promise((resolve, reject) => {
    exec(
      command,
      { windowsHide: true, maxBuffer: 1024 * 1024 * 8 },
      (error, stdout, stderr) => {
        if (error) {
          error.stdout = stdout;
          error.stderr = stderr;
          return reject(error);
        }

        resolve({ stdout, stderr });
      },
    );
  });
};

const formatRecordingUrl = (streamKey, fileName) => {
  return `${API_PUBLIC_URL}/api/recordings/files/download?stream=${encodeURIComponent(streamKey)}&file=${encodeURIComponent(fileName)}`;
};

const formatRecordingThumbnailUrl = (streamKey, fileName) => {
  return `${API_PUBLIC_URL}/api/public/recordings/thumbnail?stream=${encodeURIComponent(streamKey)}&file=${encodeURIComponent(fileName)}`;
};

const formatRecordingPlaybackUrl = (streamKey, fileName) => {
  return `${API_PUBLIC_URL}/api/public/recordings/media?stream=${encodeURIComponent(streamKey)}&file=${encodeURIComponent(fileName)}`;
};

const formatBunnyRecordingUrl = (storagePath) => {
  return `${BUNNY_RECORDINGS_CDN_URL.replace(/\/$/, "")}/${storagePath}`;
};

const getRecordingAbsolutePath = (streamKey, fileName) => {
  const cleanStream = safeRecordingSegment(streamKey);
  const cleanFile = safeRecordingSegment(fileName);

  if (!cleanStream || !cleanFile) return null;

  const basePath = path.resolve(RECORDINGS_LIVE_CAPPED_ROOT);
  const filePath = path.resolve(basePath, cleanStream, cleanFile);

  if (!filePath.startsWith(basePath)) return null;

  return filePath;
};

const getFileType = (fileName) => {
  const ext = path
    .extname(fileName || "")
    .replace(".", "")
    .toLowerCase();

  return ext || "unknown";
};

const isRecordingFile = (fileName) => {
  return [".mp4", ".flv", ".mkv", ".mov"].includes(
    path.extname(fileName || "").toLowerCase(),
  );
};

const getRecordingBaseName = (fileName) => {
  return path.basename(fileName || "", path.extname(fileName || ""));
};

const getDerivedRecordingFileNames = (fileName) => {
  const baseName = getRecordingBaseName(fileName);

  return {
    mp4File: `${baseName}.mp4`,
    thumbnailFile: `${baseName}.jpg`,
  };
};

const isFileStable = (filePath, minimumAgeSeconds = 20) => {
  if (!filePath || !fs.existsSync(filePath)) return false;

  const stats = fs.statSync(filePath);
  const ageSeconds = (Date.now() - stats.mtimeMs) / 1000;

  return ageSeconds >= minimumAgeSeconds && stats.size > 0;
};

const parseFfprobeJson = (stdout) => {
  try {
    return JSON.parse(String(stdout || "{}"));
  } catch {
    return {};
  }
};

const getRecordingMediaMetadata = async (filePath) => {
  try {
    const command = [
      "ffprobe",
      "-v error",
      "-print_format json",
      "-show_format",
      "-show_streams",
      quotePath(filePath),
    ].join(" ");

    const { stdout } = await execCommand(command);
    const data = parseFfprobeJson(stdout);
    const videoStream = (data.streams || []).find(
      (stream) => stream.codec_type === "video",
    );

    const duration = Number.parseFloat(
      data.format?.duration || videoStream?.duration || 0,
    );
    const bitrate = Number.parseInt(
      data.format?.bit_rate || videoStream?.bit_rate || 0,
      10,
    );

    return {
      duration_seconds:
        Number.isFinite(duration) && duration > 0 ? Math.round(duration) : null,
      width: videoStream?.width || null,
      height: videoStream?.height || null,
      bitrate_kbps:
        Number.isFinite(bitrate) && bitrate > 0
          ? Math.round(bitrate / 1000)
          : null,
      codec: videoStream?.codec_name || null,
    };
  } catch (error) {
    console.warn("ffprobe metadata unavailable:", error.message);
    return {
      duration_seconds: null,
      width: null,
      height: null,
      bitrate_kbps: null,
      codec: null,
    };
  }
};

const isPlayableMediaFile = async (filePath) => {
  if (!filePath || !fs.existsSync(filePath)) return false;

  try {
    const stats = fs.statSync(filePath);
    if (!stats.isFile() || stats.size <= 0) return false;

    const metadata = await getRecordingMediaMetadata(filePath);
    return Boolean(metadata.duration_seconds && metadata.duration_seconds > 0);
  } catch {
    return false;
  }
};

const removeFileIfExists = (filePath) => {
  try {
    if (filePath && fs.existsSync(filePath)) {
      fs.unlinkSync(filePath);
    }
  } catch (error) {
    console.warn("Failed to remove file:", filePath, error.message);
  }
};

const convertFlvToMp4 = async (inputPath, outputPath) => {
  if (!inputPath || !outputPath) {
    throw new Error("Input and output paths are required");
  }

  if (!fs.existsSync(inputPath)) {
    throw new Error("Source FLV file was not found");
  }

  if (fs.existsSync(outputPath)) {
    const existingIsValid = await isPlayableMediaFile(outputPath);

    if (existingIsValid) {
      return { skipped: true, outputPath };
    }

    console.warn(
      "Removing corrupted/incomplete MP4 before rebuilding:",
      outputPath,
    );
    removeFileIfExists(outputPath);
  }

  const tempOutputPath = `${outputPath}.processing-${Date.now()}.tmp.mp4`;

  removeFileIfExists(tempOutputPath);

  const command = [
    "ffmpeg",
    "-y",
    "-i",
    quotePath(inputPath),
    "-c:v copy",
    "-c:a aac",
    "-movflags +faststart",
    quotePath(tempOutputPath),
  ].join(" ");

  try {
    await execCommand(command);

    const convertedIsValid = await isPlayableMediaFile(tempOutputPath);

    if (!convertedIsValid) {
      removeFileIfExists(tempOutputPath);
      throw new Error("MP4 conversion failed validation");
    }

    fs.renameSync(tempOutputPath, outputPath);

    return { skipped: false, outputPath };
  } catch (error) {
    removeFileIfExists(tempOutputPath);
    throw error;
  }
};

const generateRecordingThumbnail = async (inputPath, thumbnailPath) => {
  if (!inputPath || !thumbnailPath) {
    throw new Error("Input and thumbnail paths are required");
  }

  if (!fs.existsSync(inputPath)) {
    throw new Error("Source video file was not found");
  }

  if (fs.existsSync(thumbnailPath)) {
    return { skipped: true, thumbnailPath };
  }

  const command = [
    "ffmpeg",
    "-y",
    "-ss 00:00:05",
    "-i",
    quotePath(inputPath),
    "-frames:v 1",
    "-q:v 2",
    quotePath(thumbnailPath),
  ].join(" ");

  await execCommand(command);

  return { skipped: false, thumbnailPath };
};

// "Record all incoming streams" toggle (Wowza-parity, 2026-08-10) —
// layered ON TOP OF the existing plan-level recording_enabled gate, not a
// replacement for it. The plan sets the ceiling (does this org's
// subscription include recording at all); this column lets an admin turn
// recording off for one specific channel even when the plan allows it.
// Defaults to TRUE so existing channels keep today's automatic-whenever-
// the-plan-allows-it behavior with no migration-time behavior change.
const ensureChannelRecordingColumn = async () => {
  await pool.query(`
    ALTER TABLE channels
    ADD COLUMN IF NOT EXISTS auto_record_enabled BOOLEAN DEFAULT TRUE
  `);
};

const ensureRecordingLibraryTable = async () => {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS recordings (
      id SERIAL PRIMARY KEY,
      organization_id INTEGER REFERENCES organizations(id) ON DELETE CASCADE,
      channel_id INTEGER REFERENCES channels(id) ON DELETE SET NULL,
      filename VARCHAR(255) NOT NULL,
      filepath TEXT NOT NULL,
      started_at TIMESTAMPTZ,
      ended_at TIMESTAMPTZ,
      duration INTEGER,
      size_mb NUMERIC,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  await pool.query(`
    ALTER TABLE recordings
    ADD COLUMN IF NOT EXISTS organization_id INTEGER REFERENCES organizations(id) ON DELETE CASCADE,
    ADD COLUMN IF NOT EXISTS channel_id INTEGER REFERENCES channels(id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS stream_key VARCHAR(255),
    ADD COLUMN IF NOT EXISTS file_type VARCHAR(40),
    ADD COLUMN IF NOT EXISTS file_size_bytes BIGINT,
    ADD COLUMN IF NOT EXISTS duration_seconds INTEGER,
    ADD COLUMN IF NOT EXISTS status VARCHAR(40) DEFAULT 'archived',
    ADD COLUMN IF NOT EXISTS source VARCHAR(80) DEFAULT 'local_srs',
    ADD COLUMN IF NOT EXISTS mp4_filename VARCHAR(255),
    ADD COLUMN IF NOT EXISTS mp4_filepath TEXT,
    ADD COLUMN IF NOT EXISTS thumbnail_filename VARCHAR(255),
    ADD COLUMN IF NOT EXISTS thumbnail_filepath TEXT,
    ADD COLUMN IF NOT EXISTS processing_status VARCHAR(40) DEFAULT 'pending',
    ADD COLUMN IF NOT EXISTS processing_error TEXT,
    ADD COLUMN IF NOT EXISTS converted_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS thumbnail_generated_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS width INTEGER,
    ADD COLUMN IF NOT EXISTS height INTEGER,
    ADD COLUMN IF NOT EXISTS bitrate_kbps INTEGER,
    ADD COLUMN IF NOT EXISTS codec VARCHAR(80),
    ADD COLUMN IF NOT EXISTS is_public BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS bunny_cdn_url TEXT,
    ADD COLUMN IF NOT EXISTS public_slug VARCHAR(255),
    ADD COLUMN IF NOT EXISTS public_title VARCHAR(255),
    ADD COLUMN IF NOT EXISTS public_description TEXT,
    ADD COLUMN IF NOT EXISTS replay_category VARCHAR(120),
    ADD COLUMN IF NOT EXISTS replay_tags TEXT,
    ADD COLUMN IF NOT EXISTS replay_visibility VARCHAR(40) DEFAULT 'public',
    ADD COLUMN IF NOT EXISTS published_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS replay_views INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()
  `);

  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS recordings_public_slug_unique
    ON recordings(public_slug)
    WHERE public_slug IS NOT NULL
  `);
};

const getAllowedChannelMap = async (organizationId) => {
  const result = await pool.query(
    `
    SELECT id, name, stream_key
    FROM channels
    WHERE organization_id = $1
    `,
    [organizationId],
  );

  const map = new Map();

  result.rows.forEach((channel) => {
    map.set(String(channel.stream_key), channel);
  });

  return map;
};

const mapRecordingRowToDto = (row, channelName = null) => {
  const streamKey = row.stream_key;
  const file = row.filename;
  const mp4File =
    row.mp4_filename || (row.file_type === "mp4" ? row.filename : null);
  const thumbnailFile = row.thumbnail_filename || null;
  const playable = Boolean(mp4File);

  return {
    id: row.id,
    organization_id: row.organization_id,
    channel_id: row.channel_id,
    stream: streamKey,
    stream_key: streamKey,
    channel_name: channelName || row.channel_name || streamKey,
    file,
    filename: file,
    filepath: row.filepath,
    size: Number(row.file_size_bytes || 0),
    size_mb: Number(row.size_mb || 0),
    created: row.created_at || row.started_at,
    updated: row.updated_at || row.ended_at,
    started_at: row.started_at,
    ended_at: row.ended_at,
    duration_seconds: row.duration_seconds || row.duration || null,
    type: row.file_type || getFileType(file),
    file_type: row.file_type || getFileType(file),
    playable,
    converted: playable,
    status: row.status || "archived",
    processing_status: row.processing_status || "pending",
    processing_error: row.processing_error || null,
    mp4_file: mp4File,
    mp4_filename: mp4File,
    thumbnail_file: thumbnailFile,
    thumbnail_url: thumbnailFile
      ? formatRecordingThumbnailUrl(streamKey, thumbnailFile)
      : null,
    width: row.width || null,
    height: row.height || null,
    bitrate_kbps: row.bitrate_kbps || null,
    codec: row.codec || null,
    is_public: Boolean(row.is_public),
    public_slug: row.public_slug || null,
    public_title:
      row.public_title || row.channel_name || row.stream_key || "Recording",
    public_description: row.public_description || "",
    replay_category: row.replay_category || "",
    replay_tags: row.replay_tags || "",
    replay_visibility: row.replay_visibility || "public",
    published_at: row.published_at || null,
    replay_views: Number(row.replay_views || 0),
    public_url:
      row.is_public &&
      row.public_slug &&
      ["public", "unlisted", "members_only"].includes(
        row.replay_visibility || "public",
      )
        ? `${CLIENT_URL.replace(/\/$/, "")}/replay/${row.public_slug}`
        : null,
    archive_status: row.archive_status || "local",
    bunny_storage_path: row.bunny_storage_path || null,
    archived_at: row.bunny_archived_at || null,
    url:
      row.archive_status === "archived" && row.bunny_storage_path
        ? row.bunny_cdn_url || formatBunnyRecordingUrl(row.bunny_storage_path)
        : playable
          ? formatRecordingPlaybackUrl(streamKey, mp4File)
          : formatRecordingUrl(streamKey, file),
    download_url:
      row.archive_status === "archived" && row.bunny_storage_path
        ? row.bunny_cdn_url || formatBunnyRecordingUrl(row.bunny_storage_path)
        : formatRecordingUrl(streamKey, playable ? mp4File : file),
    source_download_url:
      row.archive_status === "archived" && row.bunny_storage_path
        ? row.bunny_cdn_url || formatBunnyRecordingUrl(row.bunny_storage_path)
        : formatRecordingUrl(streamKey, file),
  };
};

const upsertRecordingRowFromFile = async ({
  organizationId,
  channel,
  streamName,
  file,
  filePath,
  stats,
  metadata = {},
  processingStatus = "pending",
}) => {
  const fileType = getFileType(file);
  const isMp4 = fileType === "mp4";
  const fileSizeMb = Number((stats.size / (1024 * 1024)).toFixed(2));
  const { mp4File, thumbnailFile } = getDerivedRecordingFileNames(file);

  const mp4Path = isMp4
    ? filePath
    : getRecordingAbsolutePath(streamName, mp4File);

  const thumbnailPath = getRecordingAbsolutePath(streamName, thumbnailFile);
  const mp4Exists = mp4Path && (await isPlayableMediaFile(mp4Path));
  const thumbnailExists = thumbnailPath && fs.existsSync(thumbnailPath);

  const existing = await pool.query(
    `
    SELECT *
    FROM recordings
    WHERE organization_id = $1
      AND stream_key = $2
      AND filename = $3
    LIMIT 1
    `,
    [organizationId, streamName, file],
  );

  const values = [
    organizationId,
    channel?.id || null,
    streamName,
    file,
    filePath,
    fileType,
    stats.size,
    metadata.duration_seconds || null,
    fileSizeMb,
    stats.birthtime,
    stats.mtime,
    isMp4 || mp4Exists ? mp4File : null,
    isMp4 || mp4Exists ? mp4Path : null,
    thumbnailExists ? thumbnailFile : null,
    thumbnailExists ? thumbnailPath : null,
    processingStatus,
    metadata.width || null,
    metadata.height || null,
    metadata.bitrate_kbps || null,
    metadata.codec || null,
  ];

  if (existing.rows[0]) {
    const existingRow = existing.rows[0];

    const updateValues = [
      organizationId || existingRow.organization_id,
      channel?.id || existingRow.channel_id || null,
      filePath,
      fileType,
      Number(stats.size || existingRow.file_size_bytes || 0),
      metadata.duration_seconds ||
        existingRow.duration_seconds ||
        existingRow.duration ||
        null,
      fileSizeMb,
      stats.mtime,
      isMp4 || mp4Exists ? mp4File : existingRow.mp4_filename || null,
      isMp4 || mp4Exists ? mp4Path : existingRow.mp4_filepath || null,
      thumbnailExists ? thumbnailFile : existingRow.thumbnail_filename || null,
      thumbnailExists ? thumbnailPath : existingRow.thumbnail_filepath || null,
      processingStatus || existingRow.processing_status || "pending",
      metadata.width || existingRow.width || null,
      metadata.height || existingRow.height || null,
      metadata.bitrate_kbps || existingRow.bitrate_kbps || null,
      metadata.codec || existingRow.codec || null,
      existingRow.id,
    ];

    const updateResult = await pool.query(
      `
      UPDATE recordings
      SET organization_id = $1,
          channel_id = $2,
          filepath = $3,
          file_type = $4,
          file_size_bytes = $5,
          duration_seconds = $6,
          duration = $6,
          size_mb = $7,
          ended_at = $8,
          mp4_filename = $9,
          mp4_filepath = $10,
          thumbnail_filename = $11,
          thumbnail_filepath = $12,
          processing_status = $13,
          width = $14,
          height = $15,
          bitrate_kbps = $16,
          codec = $17,
          status = 'archived',
          updated_at = NOW()
      WHERE id = $18
      RETURNING *
      `,
      updateValues,
    );

    return updateResult.rows[0];
  }

  const insertResult = await pool.query(
    `
    INSERT INTO recordings (
      organization_id,
      channel_id,
      stream_key,
      filename,
      filepath,
      file_type,
      file_size_bytes,
      duration_seconds,
      duration,
      size_mb,
      status,
      source,
      started_at,
      ended_at,
      created_at,
      updated_at,
      mp4_filename,
      mp4_filepath,
      thumbnail_filename,
      thumbnail_filepath,
      processing_status,
      width,
      height,
      bitrate_kbps,
      codec
    )
    VALUES (
      $1, $2, $3, $4, $5, $6, $7, $8, $8, $9,
      'archived', 'local_srs', $10, $11, $10, NOW(),
      $12, $13, $14, $15, $16, $17, $18, $19, $20
    )
    RETURNING *
    `,
    values,
  );

  return insertResult.rows[0];
};

const scanRecordingFilesForOrganization = async (
  organizationId,
  { processReady = false } = {},
) => {
  const recordingsPath = RECORDINGS_LIVE_CAPPED_ROOT;
  const allowedChannels = await getAllowedChannelMap(organizationId);
  const recordings = [];

  if (!fs.existsSync(recordingsPath)) return recordings;

  const streamFolders = fs.readdirSync(recordingsPath).filter((streamName) => {
    if (allowedChannels.size === 0) return false;
    return allowedChannels.has(streamName);
  });

  for (const streamName of streamFolders) {
    const streamFolder = path.join(recordingsPath, streamName);
    if (
      !fs.existsSync(streamFolder) ||
      !fs.statSync(streamFolder).isDirectory()
    )
      continue;

    const channel = allowedChannels.get(streamName) || null;
    const files = fs.readdirSync(streamFolder);
    const fileSet = new Set(files);

    for (const file of files) {
      if (file.endsWith(".tmp") || file.endsWith(".part")) continue;
      if (!isRecordingFile(file)) continue;

      const fileTypeForScan = getFileType(file);
      const baseNameForScan = getRecordingBaseName(file);

      /*
       * SRS saves the original recording as FLV. Our processor creates a derived
       * MP4 with the same base filename. The FLV row already points to the MP4,
       * so skip the derived MP4 as its own library item to avoid duplicates.
       */
      if (fileTypeForScan === "mp4" && fileSet.has(`${baseNameForScan}.flv`)) {
        await pool.query(
          `
          DELETE FROM recordings
          WHERE organization_id = $1
            AND stream_key = $2
            AND filename = $3
            AND file_type = 'mp4'
          `,
          [organizationId, streamName, file],
        );
        continue;
      }

      const filePath = path.join(streamFolder, file);
      const stats = fs.statSync(filePath);

      if (!stats.isFile() || stats.size <= 0) continue;

      let metadata = {};
      const fileType = getFileType(file);

      if (fileType === "mp4") {
        metadata = await getRecordingMediaMetadata(filePath);
      }

      const processingStatus =
        fileType === "mp4"
          ? "ready"
          : fs.existsSync(
                getRecordingAbsolutePath(
                  streamName,
                  getDerivedRecordingFileNames(file).mp4File,
                ),
              )
            ? "ready"
            : "pending";

      const row = await upsertRecordingRowFromFile({
        organizationId,
        channel,
        streamName,
        file,
        filePath,
        stats,
        metadata,
        processingStatus,
      });

      if (processReady && fileType === "flv" && isFileStable(filePath)) {
        await processRecordingFile({
          organizationId,
          stream: streamName,
          file,
        });
      }

      const refreshed = await pool.query(
        `
        SELECT r.*, c.name AS channel_name
        FROM recordings r
        LEFT JOIN channels c ON c.id = r.channel_id
        WHERE r.id = $1
        LIMIT 1
        `,
        [row.id],
      );

      recordings.push(
        mapRecordingRowToDto(refreshed.rows[0] || row, channel?.name),
      );
    }
  }

  // Include already-archived recordings too. Their local files were
  // deleted after a successful Bunny upload, so they no longer show up
  // in the filesystem scan above — pull them straight from the database
  // instead so they don't disappear from the library.
  const archivedResult = await pool.query(
    `
    SELECT r.*, c.name AS channel_name
    FROM recordings r
    LEFT JOIN channels c ON c.id = r.channel_id
    WHERE r.organization_id = $1
      AND r.archive_status = 'archived'
    `,
    [organizationId],
  );

  for (const archivedRow of archivedResult.rows) {
    recordings.push(
      mapRecordingRowToDto(archivedRow, archivedRow.channel_name),
    );
  }

  recordings.sort((a, b) => new Date(b.created) - new Date(a.created));
  return recordings;
};

const processRecordingFile = async ({
  organizationId,
  stream,
  file,
  skipStabilityCheck = false,
}) => {
  const cleanStream = safeRecordingSegment(stream);
  const cleanFile = safeRecordingSegment(file);

  if (!cleanStream || !cleanFile) {
    throw new Error("Stream and file are required");
  }

  const allowedChannels = await getAllowedChannelMap(organizationId);
  const channel = allowedChannels.get(cleanStream);

  if (!channel) {
    const error = new Error("You do not have access to this recording");
    error.statusCode = 403;
    throw error;
  }

  const lockResult = await pool.query(
    `
    SELECT *
    FROM recordings
    WHERE organization_id = $1
      AND stream_key = $2
      AND filename = $3
      AND processing_status = 'processing'
      AND updated_at > NOW() - INTERVAL '15 minutes'
    LIMIT 1
    `,
    [organizationId, cleanStream, cleanFile],
  );

  if (lockResult.rows[0]) {
    return mapRecordingRowToDto({
      ...lockResult.rows[0],
      channel_name: channel.name,
    });
  }

  const inputPath = getRecordingAbsolutePath(cleanStream, cleanFile);

  if (!inputPath || !fs.existsSync(inputPath)) {
    const error = new Error("Recording file not found");
    error.statusCode = 404;
    throw error;
  }

  const inputStats = fs.statSync(inputPath);

  if (!skipStabilityCheck && !isFileStable(inputPath)) {
    const row = await upsertRecordingRowFromFile({
      organizationId,
      channel,
      streamName: cleanStream,
      file: cleanFile,
      filePath: inputPath,
      stats: inputStats,
      metadata: {},
      processingStatus: "waiting",
    });

    return mapRecordingRowToDto({ ...row, channel_name: channel.name });
  }

  const fileType = getFileType(cleanFile);
  const { mp4File, thumbnailFile } = getDerivedRecordingFileNames(cleanFile);
  const mp4Path =
    fileType === "mp4"
      ? inputPath
      : getRecordingAbsolutePath(cleanStream, mp4File);
  const thumbnailPath = getRecordingAbsolutePath(cleanStream, thumbnailFile);

  let processingStatus = "processing";
  let processingError = null;

  const initialRow = await upsertRecordingRowFromFile({
    organizationId,
    channel,
    streamName: cleanStream,
    file: cleanFile,
    filePath: inputPath,
    stats: inputStats,
    metadata: {},
    processingStatus,
  });

  try {
    if (fileType === "flv") {
      await convertFlvToMp4(inputPath, mp4Path);
    }

    const mediaPath = fs.existsSync(mp4Path) ? mp4Path : inputPath;
    const mediaIsValid = await isPlayableMediaFile(mediaPath);

    if (!mediaIsValid) {
      throw new Error(
        "Processed recording is not playable yet. Please retry after the source file is fully written.",
      );
    }

    const metadata = await getRecordingMediaMetadata(mediaPath);

    if (mediaPath && fs.existsSync(mediaPath)) {
      await generateRecordingThumbnail(mediaPath, thumbnailPath).catch(
        (error) => {
          console.warn("Thumbnail generation failed:", error.message);
        },
      );
    }

    processingStatus = "ready";

    const mp4Stats = fs.existsSync(mp4Path) ? fs.statSync(mp4Path) : inputStats;
    const updateResult = await pool.query(
      `
      UPDATE recordings
      SET duration_seconds = COALESCE($1, duration_seconds),
          duration = COALESCE($1, duration),
          mp4_filename = $2,
          mp4_filepath = $3,
          thumbnail_filename = CASE WHEN $4::boolean THEN $5 ELSE thumbnail_filename END,
          thumbnail_filepath = CASE WHEN $4::boolean THEN $6 ELSE thumbnail_filepath END,
          processing_status = 'ready',
          processing_error = NULL,
          converted_at = COALESCE(converted_at, NOW()),
          thumbnail_generated_at = CASE WHEN $4::boolean THEN COALESCE(thumbnail_generated_at, NOW()) ELSE thumbnail_generated_at END,
          width = COALESCE($7, width),
          height = COALESCE($8, height),
          bitrate_kbps = COALESCE($9, bitrate_kbps),
          codec = COALESCE($10, codec),
          updated_at = NOW()
      WHERE id = $11
      RETURNING *
      `,
      [
        metadata.duration_seconds,
        mp4File,
        mp4Path,
        fs.existsSync(thumbnailPath),
        thumbnailFile,
        thumbnailPath,
        metadata.width,
        metadata.height,
        metadata.bitrate_kbps,
        metadata.codec,
        initialRow.id,
      ],
    );

    return mapRecordingRowToDto({
      ...updateResult.rows[0],
      channel_name: channel.name,
      file_size_bytes: inputStats.size,
      size_mb: Number((inputStats.size / (1024 * 1024)).toFixed(2)),
    });
  } catch (error) {
    processingStatus = "failed";
    processingError = error.message;

    const updateResult = await pool.query(
      `
      UPDATE recordings
      SET processing_status = 'failed',
          processing_error = $1,
          updated_at = NOW()
      WHERE id = $2
      RETURNING *
      `,
      [processingError, initialRow.id],
    );

    return mapRecordingRowToDto({
      ...updateResult.rows[0],
      channel_name: channel.name,
    });
  }
};

app.get(
  "/api/recordings",
  authenticateAdmin,
  resolveOrganizationForRequest,
  async (req, res) => {
    try {
      await scanRecordingFilesForOrganization(req.organization.id);

      const result = await pool.query(
        `
        SELECT
          r.*,
          c.name AS channel_name
        FROM recordings r
        LEFT JOIN channels c ON c.id = r.channel_id
        WHERE r.organization_id = $1
        ORDER BY COALESCE(r.created_at, r.started_at) DESC
        `,
        [req.organization.id],
      );

      res.json({
        ok: true,
        recordings: result.rows.map((row) => mapRecordingRowToDto(row)),
      });
    } catch (error) {
      console.error("Get Recordings Error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to fetch recordings",
      });
    }
  },
);

app.post(
  "/api/recordings",
  authenticateAdmin,
  resolveOrganizationForRequest,
  async (req, res) => {
    try {
      const {
        channel_id,
        filename,
        filepath,
        started_at,
        ended_at,
        duration,
        size_mb,
      } = req.body;

      if (!filename || !filepath) {
        return res.status(400).json({
          ok: false,
          message: "Filename and filepath are required",
        });
      }

      const result = await pool.query(
        `
        INSERT INTO recordings (
          organization_id,
          channel_id,
          filename,
          filepath,
          started_at,
          ended_at,
          duration,
          duration_seconds,
          size_mb,
          status,
          source
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $7, $8, 'archived', 'manual')
        RETURNING *
        `,
        [
          req.organization.id,
          channel_id || null,
          filename,
          filepath,
          started_at || null,
          ended_at || null,
          duration || null,
          size_mb || null,
        ],
      );

      res.json({
        ok: true,
        recording: result.rows[0],
      });
    } catch (error) {
      console.error("Create Recording Error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to create recording",
        error: error.message,
      });
    }
  },
);

// ══════════════════════════════════════════
// SUPPORT TOOL — retry a failed Bunny archive
// If the Bunny upload failed (network blip, disk issue, etc.), the
// recording is stuck with archive_status = 'failed' and its local
// file was never cleaned up. This lets a super_admin re-attempt the
// archive without needing shell access to the server.
// ══════════════════════════════════════════
app.post(
  "/api/recordings/:id/retry-archive",
  authenticateAdmin,
  requireRole("super_admin"),
  async (req, res) => {
    try {
      const { id } = req.params;

      const result = await pool.query(
        `SELECT * FROM recordings WHERE id = $1`,
        [id],
      );
      const recording = result.rows[0];

      if (!recording) {
        return res.status(404).json({
          ok: false,
          message: "Recording not found",
        });
      }

      if (!BUNNY_STORAGE_API_KEY) {
        return res.status(501).json({
          ok: false,
          message: "Bunny Storage is not configured on this server",
        });
      }

      await archiveRecordingRow(recording);

      const refreshed = await pool.query(
        `SELECT archive_status, bunny_storage_path FROM recordings WHERE id = $1`,
        [id],
      );

      res.json({
        ok: true,
        message: "Archive retry attempted",
        archive_status: refreshed.rows[0]?.archive_status,
        bunny_storage_path: refreshed.rows[0]?.bunny_storage_path,
      });
    } catch (error) {
      console.error("Retry archive error:", error);
      res.status(500).json({
        ok: false,
        message: "Failed to retry archive",
        error: error.message,
      });
    }
  },
);

app.delete(
  "/api/recordings/:id",
  authenticateAdmin,
  resolveOrganizationForRequest,
  requireOrganizationRole("owner", "admin"),
  async (req, res) => {
    try {
      const { id } = req.params;

      const recordingResult = await pool.query(
        `
        SELECT *
        FROM recordings
        WHERE id = $1
          AND organization_id = $2
        LIMIT 1
        `,
        [id, req.organization.id],
      );

      const recording = recordingResult.rows[0];

      if (!recording) {
        return res.status(404).json({
          ok: false,
          message: "Recording not found",
        });
      }

      const filesToDelete = [
        recording.filepath,
        recording.mp4_filepath,
        recording.thumbnail_filepath,
      ].filter(Boolean);

      for (const filePath of filesToDelete) {
        if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
      }

      if (
        recording.archive_status === "archived" &&
        recording.bunny_storage_path
      ) {
        const { zoneCreds } = getOrgBunnyZoneCreds(req.organization);
        await deleteFileFromBunnyStorage(
          recording.bunny_storage_path,
          zoneCreds,
        );
      }

      await pool.query(
        `
        DELETE FROM recordings
        WHERE id = $1
          AND organization_id = $2
        `,
        [id, req.organization.id],
      );

      res.json({
        ok: true,
        message: "Recording deleted successfully",
      });
    } catch (error) {
      console.error("Delete Recording Error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to delete recording",
      });
    }
  },
);

/*
|--------------------------------------------------------------------------
| RECORDING FILES
|--------------------------------------------------------------------------
*/

app.get(
  "/api/recordings/files",
  authenticateAdmin,
  resolveOrganizationForRequest,
  async (req, res) => {
    try {
      const autoProcess =
        req.query.process === "1" || req.query.process === "true";
      const recordings = await scanRecordingFilesForOrganization(
        req.organization.id,
        { processReady: autoProcess },
      );

      res.json({
        ok: true,
        recordings,
      });
    } catch (err) {
      console.error("Get recording files error:", err);

      res.status(500).json({
        ok: false,
        message: "Failed to load recording files",
        error: err.message,
      });
    }
  },
);

app.get(
  "/api/recordings/files/download",
  authenticateAdmin,
  resolveOrganizationForRequest,
  async (req, res) => {
    try {
      const stream = safeRecordingSegment(req.query.stream);
      const file = safeRecordingSegment(req.query.file);

      if (!stream || !file) {
        return res.status(400).json({
          ok: false,
          message: "Stream and file are required",
        });
      }

      const allowedChannels = await getAllowedChannelMap(req.organization.id);

      if (!allowedChannels.has(stream)) {
        return res.status(403).json({
          ok: false,
          message: "You do not have access to this recording",
        });
      }

      const filePath = getRecordingAbsolutePath(stream, file);

      if (!filePath || !fs.existsSync(filePath)) {
        return res.status(404).json({
          ok: false,
          message: "Recording file not found",
        });
      }

      res.download(filePath, file);
    } catch (error) {
      console.error("Download recording error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to download recording",
      });
    }
  },
);

// Public organizations list — used by member login gate
// Returns active organizations that have at least one public replay
app.get("/api/public/organizations", async (req, res) => {
  try {
    const result = await pool.query(
      `
      SELECT DISTINCT
        o.id,
        o.name,
        o.slug,
        o.logo_url,
        o.primary_color,
        os.watch_page_title,
        os.secondary_color
      FROM organizations o
      INNER JOIN recordings r ON r.organization_id = o.id
        AND r.is_public = TRUE
        AND r.mp4_filename IS NOT NULL
      LEFT JOIN organization_settings os ON os.organization_id = o.id
      WHERE o.is_active = TRUE
      ORDER BY o.name ASC
      `,
    );

    res.json({
      ok: true,
      organizations: result.rows.map((org) => ({
        id: org.id,
        name: org.watch_page_title || org.name,
        slug: org.slug,
        logo_url: org.logo_url || null,
        primary_color: org.primary_color || "#0d6efd",
        secondary_color: org.secondary_color || "#fd9d00",
      })),
    });
  } catch (error) {
    console.error("Public organizations error:", error);
    res.status(500).json({
      ok: false,
      message: "Failed to load organizations.",
      error: error.message,
    });
  }
});

app.get("/api/public/recordings/thumbnail", async (req, res) => {
  try {
    const stream = safeRecordingSegment(req.query.stream);
    const file = safeRecordingSegment(req.query.file);

    if (!stream || !file) {
      return res.status(400).send("Stream and file are required");
    }

    const filePath = getRecordingAbsolutePath(stream, file);

    if (!filePath || !fs.existsSync(filePath)) {
      return res.status(404).send("Thumbnail not found");
    }

    res.setHeader("Cache-Control", "public, max-age=300");
    res.sendFile(filePath);
  } catch (error) {
    console.error("Public thumbnail error:", error);
    res.status(500).send("Failed to load thumbnail");
  }
});

app.get("/api/public/recordings/media", async (req, res) => {
  try {
    const stream = safeRecordingSegment(req.query.stream);
    const file = safeRecordingSegment(req.query.file);

    if (!stream || !file) {
      return res.status(400).send("Stream and file are required");
    }

    const filePath = getRecordingAbsolutePath(stream, file);

    if (!filePath || !fs.existsSync(filePath)) {
      return res.status(404).send("Recording not found");
    }

    if (!(await isPlayableMediaFile(filePath))) {
      return res.status(409).send("Recording is not playable yet");
    }

    res.setHeader("Cache-Control", "public, max-age=60");
    res.sendFile(filePath);
  } catch (error) {
    console.error("Public recording media error:", error);
    res.status(500).send("Failed to load recording");
  }
});

app.get(
  "/api/recordings/files/thumbnail",
  authenticateAdmin,
  resolveOrganizationForRequest,
  async (req, res) => {
    try {
      const stream = safeRecordingSegment(req.query.stream);
      const file = safeRecordingSegment(req.query.file);

      if (!stream || !file) {
        return res.status(400).json({
          ok: false,
          message: "Stream and file are required",
        });
      }

      const allowedChannels = await getAllowedChannelMap(req.organization.id);

      if (!allowedChannels.has(stream)) {
        return res.status(403).json({
          ok: false,
          message: "You do not have access to this thumbnail",
        });
      }

      const filePath = getRecordingAbsolutePath(stream, file);

      if (!filePath || !fs.existsSync(filePath)) {
        return res.status(404).json({
          ok: false,
          message: "Thumbnail not found",
        });
      }

      res.sendFile(filePath);
    } catch (error) {
      console.error("Thumbnail error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to load thumbnail",
      });
    }
  },
);

app.use("/recordings", express.static(RECORDINGS_ROOT));

app.post(
  "/api/recordings/sync",
  authenticateAdmin,
  resolveOrganizationForRequest,
  async (req, res) => {
    try {
      const autoProcess =
        req.body?.process === true || req.query.process === "1";
      const recordings = await scanRecordingFilesForOrganization(
        req.organization.id,
        { processReady: autoProcess },
      );

      res.json({
        ok: true,
        message: autoProcess
          ? "Recording library synced and processed."
          : "Recording library synced.",
        recordings,
      });
    } catch (error) {
      console.error("Sync recordings error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to sync recordings",
        error: error.message,
      });
    }
  },
);

app.post(
  "/api/recordings/process",
  authenticateAdmin,
  resolveOrganizationForRequest,
  async (req, res) => {
    try {
      const stream = safeRecordingSegment(req.body.stream);
      const file = safeRecordingSegment(req.body.file);

      const recording = await processRecordingFile({
        organizationId: req.organization.id,
        stream,
        file,
      });

      const recordings = await scanRecordingFilesForOrganization(
        req.organization.id,
      );

      res.json({
        ok: true,
        message: "Recording processed successfully",
        recording,
        recordings,
      });
    } catch (error) {
      console.error("Process recording error:", error);

      res.status(error.statusCode || 500).json({
        ok: false,
        message: "Failed to process recording",
        error: error.message,
      });
    }
  },
);

app.post(
  "/api/recordings/process-all",
  authenticateAdmin,
  resolveOrganizationForRequest,
  async (req, res) => {
    try {
      const before = await scanRecordingFilesForOrganization(
        req.organization.id,
      );

      const processable = before.filter((recording) => {
        return (
          recording.file_type === "flv" &&
          ["pending", "waiting", "failed"].includes(
            recording.processing_status || "pending",
          )
        );
      });

      const results = [];

      for (const recording of processable) {
        try {
          const processed = await processRecordingFile({
            organizationId: req.organization.id,
            stream: recording.stream,
            file: recording.file,
          });

          results.push({
            ok: true,
            file: recording.file,
            recording: processed,
          });
        } catch (error) {
          results.push({
            ok: false,
            file: recording.file,
            error: error.message,
          });
        }
      }

      const recordings = await scanRecordingFilesForOrganization(
        req.organization.id,
      );

      res.json({
        ok: true,
        message: "Recording processing completed.",
        processed: results,
        recordings,
      });
    } catch (error) {
      console.error("Process all recordings error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to process recordings",
        error: error.message,
      });
    }
  },
);

app.post(
  "/api/recordings/convert",
  authenticateAdmin,
  resolveOrganizationForRequest,
  async (req, res) => {
    try {
      const stream = safeRecordingSegment(req.body.stream);
      const file = safeRecordingSegment(req.body.file);

      const recording = await processRecordingFile({
        organizationId: req.organization.id,
        stream,
        file,
      });

      const recordings = await scanRecordingFilesForOrganization(
        req.organization.id,
      );

      res.json({
        ok: true,
        message: "Recording converted and processed successfully",
        file: recording.mp4_file,
        recording,
        recordings,
      });
    } catch (error) {
      console.error("Convert recording error:", error);

      res.status(error.statusCode || 500).json({
        ok: false,
        message: "Failed to convert recording",
        error: error.message,
      });
    }
  },
);

app.delete(
  "/api/recordings/files",
  authenticateAdmin,
  resolveOrganizationForRequest,
  requireOrganizationRole("owner", "admin"),
  async (req, res) => {
    try {
      const stream = safeRecordingSegment(req.body.stream);
      const file = safeRecordingSegment(req.body.file);

      if (!stream || !file) {
        return res.status(400).json({
          ok: false,
          message: "Stream and file are required",
        });
      }

      const allowedChannels = await getAllowedChannelMap(req.organization.id);

      if (!allowedChannels.has(stream)) {
        return res.status(403).json({
          ok: false,
          message: "You do not have access to this recording",
        });
      }

      const recordingResult = await pool.query(
        `
        SELECT *
        FROM recordings
        WHERE organization_id = $1
          AND stream_key = $2
          AND filename = $3
        LIMIT 1
        `,
        [req.organization.id, stream, file],
      );

      const recording = recordingResult.rows[0];
      const filePath = getRecordingAbsolutePath(stream, file);

      if (!recording && (!filePath || !fs.existsSync(filePath))) {
        return res.status(404).json({
          ok: false,
          message: "Recording file not found",
        });
      }

      const filesToDelete = [
        recording?.filepath || filePath,
        recording?.mp4_filepath,
        recording?.thumbnail_filepath,
      ].filter(Boolean);

      for (const deletePath of filesToDelete) {
        if (fs.existsSync(deletePath)) fs.unlinkSync(deletePath);
      }

      if (
        recording?.archive_status === "archived" &&
        recording?.bunny_storage_path
      ) {
        const { zoneCreds } = getOrgBunnyZoneCreds(req.organization);
        await deleteFileFromBunnyStorage(
          recording.bunny_storage_path,
          zoneCreds,
        );
      }

      await pool.query(
        `
        DELETE FROM recordings
        WHERE organization_id = $1
          AND stream_key = $2
          AND filename = $3
        `,
        [req.organization.id, stream, file],
      );

      res.json({
        ok: true,
        message: "Recording deleted successfully",
      });
    } catch (error) {
      console.error("Delete recording file error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to delete recording file",
        error: error.message,
      });
    }
  },
);

/*
|--------------------------------------------------------------------------
| FILE-AS-LIVE BROADCAST
|--------------------------------------------------------------------------
| Streams a pre-uploaded recording out through the normal live pipeline as
| if it were a real broadcast — repeat airings, time-zone rebroadcasts, or
| filling a scheduled slot with pre-recorded content (Wowza-parity item 2,
| 2026-08-10). Publishes to the SAME RTMP ingest URL a real encoder would
| use, so everything downstream (ABR transcoding, AES-128 encryption,
| viewer analytics, recording archival of the rebroadcast itself) works
| identically to a genuine live broadcast — no separate code path needed
| for playback. Deliberately refuses to start if the channel already has
| a real live source publishing, to avoid two publishers fighting over the
| same stream_key.
*/

const fileBroadcastProcesses = new Map(); // channelId -> { proc, recordingId, startedAt }

app.get(
  "/api/channels/:channelId/file-broadcast/status",
  authenticateAdmin,
  resolveOrganizationForRequest,
  async (req, res) => {
    try {
      const channel = await getOwnedChannel(
        req.params.channelId,
        req.organization.id,
      );
      if (!channel) {
        return res
          .status(404)
          .json({ ok: false, message: "Channel not found" });
      }

      const running = fileBroadcastProcesses.get(channel.id);

      res.json({
        ok: true,
        running: Boolean(running),
        recordingId: running?.recordingId || null,
        startedAt: running?.startedAt || null,
      });
    } catch (error) {
      console.error("File broadcast status error:", error);
      res
        .status(500)
        .json({ ok: false, message: "Failed to load file broadcast status" });
    }
  },
);

app.post(
  "/api/channels/:channelId/file-broadcast/start",
  authenticateAdmin,
  resolveOrganizationForRequest,
  requireRole("super_admin", "admin", "operator"),
  requireOrganizationRole("owner", "admin"),
  async (req, res) => {
    try {
      const channel = await getOwnedChannel(
        req.params.channelId,
        req.organization.id,
      );
      if (!channel) {
        return res
          .status(404)
          .json({ ok: false, message: "Channel not found" });
      }

      if (fileBroadcastProcesses.has(channel.id)) {
        return res.status(400).json({
          ok: false,
          message: "A file broadcast is already running on this channel.",
        });
      }

      const alreadyLive = await isSrsStreamLive(channel.stream_key);
      if (alreadyLive) {
        return res.status(400).json({
          ok: false,
          message:
            "This channel already has a real broadcast live. Stop it before starting a file broadcast.",
        });
      }

      const recordingId = Number(req.body.recording_id || 0);
      if (!recordingId) {
        return res
          .status(400)
          .json({ ok: false, message: "recording_id is required" });
      }

      const recordingResult = await pool.query(
        `SELECT * FROM recordings WHERE id = $1 AND organization_id = $2`,
        [recordingId, req.organization.id],
      );
      const recording = recordingResult.rows[0];
      if (!recording) {
        return res
          .status(404)
          .json({ ok: false, message: "Recording not found" });
      }

      const sourceFile = recording.mp4_filepath || recording.filepath;
      if (!sourceFile || !fs.existsSync(sourceFile)) {
        return res.status(404).json({
          ok: false,
          message: "Recording file is missing from disk.",
        });
      }

      const loop = req.body.loop === true;
      const destinationUrl = `rtmp://127.0.0.1/live/${channel.stream_key}`;

      // -re: read the input at its native frame rate, so this genuinely
      // "streams" the file over the broadcast's real duration instead of
      // publishing it as fast as disk I/O allows. -stream_loop -1 repeats
      // indefinitely until manually stopped; without it, the process
      // exits (and the "live" broadcast ends) when the file finishes.
      const args = [
        "-re",
        ...(loop ? ["-stream_loop", "-1"] : []),
        "-i",
        sourceFile,
        "-c",
        "copy",
        "-f",
        "flv",
        destinationUrl,
      ];

      const proc = spawn("ffmpeg", args);

      proc.stderr.on("data", (data) => {
        console.log(
          `[FILE-BROADCAST ${channel.stream_key}]`,
          data.toString().slice(0, 300),
        );
      });

      proc.on("exit", (code) => {
        console.log(
          `[FILE-BROADCAST ${channel.stream_key}] exited with code ${code}`,
        );
        fileBroadcastProcesses.delete(channel.id);
      });

      fileBroadcastProcesses.set(channel.id, {
        proc,
        recordingId,
        startedAt: new Date().toISOString(),
      });

      res.json({ ok: true, message: "File broadcast started" });
    } catch (error) {
      console.error("Start file broadcast error:", error);
      res
        .status(500)
        .json({ ok: false, message: "Failed to start file broadcast" });
    }
  },
);

app.post(
  "/api/channels/:channelId/file-broadcast/stop",
  authenticateAdmin,
  resolveOrganizationForRequest,
  requireRole("super_admin", "admin", "operator"),
  requireOrganizationRole("owner", "admin"),
  async (req, res) => {
    try {
      const channel = await getOwnedChannel(
        req.params.channelId,
        req.organization.id,
      );
      if (!channel) {
        return res
          .status(404)
          .json({ ok: false, message: "Channel not found" });
      }

      const running = fileBroadcastProcesses.get(channel.id);
      if (running) {
        running.proc.kill("SIGTERM");
        fileBroadcastProcesses.delete(channel.id);
      }

      res.json({ ok: true, message: "File broadcast stopped" });
    } catch (error) {
      console.error("Stop file broadcast error:", error);
      res
        .status(500)
        .json({ ok: false, message: "Failed to stop file broadcast" });
    }
  },
);

/*
|--------------------------------------------------------------------------
| MANUAL RECORDING CLIPS (mid-broadcast start/stop)
|--------------------------------------------------------------------------
| Wowza Engine parity, deferred from the earlier feature batch, built last
| per user's own priority ranking. Deliberately NOT built by retroactively
| slicing the passively-collected HLS segments by timestamp — that's
| fragile (depends on segment boundaries lining up with whatever moment
| someone clicked start/stop) and duplicates what the existing "record
| all incoming streams" pipeline already does. Instead, clicking "Start
| Recording" spawns a DEDICATED ffmpeg process that actively captures the
| live output to its own MP4 file for exactly the [start, stop] window —
| a genuinely independent clip, not a filtered view of the continuous
| archive. This runs completely independently of the auto_record_enabled
| toggle — an org can have automatic recording off entirely and still
| pull manual clips, or have both running at once; they don't interact.
*/

const manualRecordingProcesses = new Map(); // channelId -> { proc, startedAt, outputPath }

app.get(
  "/api/channels/:channelId/manual-recording/status",
  authenticateAdmin,
  resolveOrganizationForRequest,
  async (req, res) => {
    try {
      const channel = await getOwnedChannel(
        req.params.channelId,
        req.organization.id,
      );
      if (!channel) {
        return res
          .status(404)
          .json({ ok: false, message: "Channel not found" });
      }

      const running = manualRecordingProcesses.get(channel.id);

      res.json({
        ok: true,
        recording: Boolean(running),
        startedAt: running?.startedAt || null,
      });
    } catch (error) {
      console.error("Manual recording status error:", error);
      res
        .status(500)
        .json({ ok: false, message: "Failed to load recording status" });
    }
  },
);

app.post(
  "/api/channels/:channelId/manual-recording/start",
  authenticateAdmin,
  resolveOrganizationForRequest,
  requireRole("super_admin", "admin", "operator"),
  requireOrganizationRole("owner", "admin"),
  async (req, res) => {
    try {
      const channel = await getOwnedChannel(
        req.params.channelId,
        req.organization.id,
      );
      if (!channel) {
        return res
          .status(404)
          .json({ ok: false, message: "Channel not found" });
      }

      if (manualRecordingProcesses.has(channel.id)) {
        return res.status(400).json({
          ok: false,
          message: "A manual recording is already running on this channel.",
        });
      }

      // The frontend button already hides for plans without recording
      // (planIncludesRecording in ChannelsPanel.jsx), but that's a UI
      // convenience, not security — this route needs its own real check,
      // or a direct API call could bypass it entirely and let an
      // Essential-plan org rack up real Bunny storage costs for a
      // feature they haven't paid for. Same summary/field this same
      // gate already uses in checkStorageQuota() and
      // autoSyncRecordingsDelayed() above.
      const summary = await getOrganizationSubscriptionSummary(
        req.organization.id,
      );
      if (!summary?.recording_enabled) {
        return res.status(403).json({
          ok: false,
          message:
            "Recording isn't included on your current plan. Upgrade to Deluxe or Premium to use manual recording.",
        });
      }

      const isLive = await isSrsStreamLive(channel.stream_key);
      if (!isLive) {
        return res.status(400).json({
          ok: false,
          message:
            "This channel isn't currently live — nothing to record. Start manual recording once the broadcast is live.",
        });
      }

      const startedAt = new Date();
      const outputFilename = `manual-${startedAt.getTime()}.flv`;
      const outputPath = getRecordingAbsolutePath(
        channel.stream_key,
        outputFilename,
      );

      if (!outputPath) {
        return res
          .status(500)
          .json({ ok: false, message: "Could not resolve recording path" });
      }

      const outputDir = path.dirname(outputPath);
      if (!fs.existsSync(outputDir)) {
        fs.mkdirSync(outputDir, { recursive: true });
      }

      // -re not needed here — this reads a LIVE HLS source (SRS keeps
      // producing new segments in real time regardless of how fast
      // ffmpeg reads them), unlike the file-broadcast feature above
      // which reads a static file and needs -re to pace itself.
      //
      // FLV output, NOT MP4 — this was the actual root cause of an
      // earlier bug: raw `-f mp4` from an open-ended/live source (no
      // known final duration) is a known-fragile combination for
      // ffmpeg's MP4 muxer without fragmented-mp4 flags, and was
      // observed live cutting recordings short after a few seconds
      // despite the UI still showing the timer counting up. FLV is the
      // ONLY format every other live-capture ffmpeg process in this
      // entire file uses (social simulcast, ABR renditions) — this now
      // matches that established, proven-working convention instead of
      // being a second, untested pattern. The existing convertFlvToMp4()
      // + processRecordingFile() pipeline (used for every other
      // recording in this system) handles turning this into a playable
      // MP4 afterward — see the stop handler below.
      const sourceUrl = `${SRS_HLS_ORIGIN}/live/${channel.stream_key}.m3u8`;

      // CONFIRMED LIVE BUG (2026-08-10), fixed via actual ffmpeg stderr
      // logs, not guessed: a blanket "-c copy" here killed the whole
      // process outright on a single malformed AAC frame —
      // "[aac_adtstoasc] Error parsing ADTS frame header! / Conversion
      // failed!" — because copying AAC from an MPEG-TS/HLS source into
      // FLV requires ffmpeg's aac_adtstoasc bitstream filter to reframe
      // it, and that filter has no tolerance for one bad frame. Video is
      // still safe to copy (-c:v copy, no re-encode needed), but audio
      // is now RE-ENCODED (-c:a aac) instead of copied — this sidesteps
      // the fragile bitstream filter entirely by producing clean AAC
      // frames of ffmpeg's own making rather than reformatting whatever
      // the source handed it. This also matches how audio is ALREADY
      // handled in buildRenditionFfmpegArgs() above for the ABR
      // transcoders (always -c:a aac, never -c:a copy) — should have
      // followed that same established pattern from the start.
      const args = [
        ...inputResilienceFlags,
        "-i",
        sourceUrl,
        "-c:v",
        "copy",
        "-c:a",
        "aac",
        "-b:a",
        "128k",
        "-err_detect",
        "ignore_err",
        "-f",
        "flv",
        outputPath,
      ];

      const proc = spawn("ffmpeg", args);

      proc.stderr.on("data", (data) => {
        console.log(
          `[MANUAL-RECORDING ${channel.stream_key}]`,
          data.toString().slice(0, 300),
        );
      });

      proc.on("exit", (code) => {
        console.log(
          `[MANUAL-RECORDING ${channel.stream_key}] exited with code ${code}`,
        );
        manualRecordingProcesses.delete(channel.id);
      });

      manualRecordingProcesses.set(channel.id, {
        proc,
        startedAt: startedAt.toISOString(),
        outputPath,
      });

      res.json({ ok: true, message: "Manual recording started" });
    } catch (error) {
      console.error("Start manual recording error:", error);
      res
        .status(500)
        .json({ ok: false, message: "Failed to start manual recording" });
    }
  },
);

app.post(
  "/api/channels/:channelId/manual-recording/stop",
  authenticateAdmin,
  resolveOrganizationForRequest,
  requireRole("super_admin", "admin", "operator"),
  requireOrganizationRole("owner", "admin"),
  async (req, res) => {
    try {
      const channel = await getOwnedChannel(
        req.params.channelId,
        req.organization.id,
      );
      if (!channel) {
        return res
          .status(404)
          .json({ ok: false, message: "Channel not found" });
      }

      const running = manualRecordingProcesses.get(channel.id);
      if (!running) {
        return res.status(400).json({
          ok: false,
          message: "No manual recording is running on this channel.",
        });
      }

      const { proc, startedAt, outputPath } = running;
      const endedAt = new Date();

      // SIGTERM (not SIGKILL) so ffmpeg gets to close the FLV file
      // cleanly before exiting — an abrupt kill here could leave a
      // truncated/corrupted file. Same graceful-then-force pattern
      // already used elsewhere in this file (social simulcast, ABR
      // transcoders) — force-kill only if it hasn't exited after 5s.
      proc.kill("SIGTERM");

      const registerRecording = async () => {
        try {
          if (!fs.existsSync(outputPath)) {
            console.error(
              `[MANUAL-RECORDING] Expected output file missing after stop: ${outputPath}`,
            );
            return;
          }

          const filename = path.basename(outputPath);

          // Reuses the SAME conversion pipeline every other recording in
          // this system goes through (FLV→MP4 via convertFlvToMp4,
          // thumbnail generation, DB row with correct width/height/
          // bitrate/codec) rather than a second hand-rolled INSERT —
          // this is also what makes the resulting clip show up and play
          // correctly in the normal Recordings list, not a special case.
          const recording = await processRecordingFile({
            organizationId: channel.organization_id,
            stream: channel.stream_key,
            file: filename,
            // We know this file is complete because we're calling this
            // from ffmpeg's own "exit" event handler, not guessing from
            // a passive filesystem scan — the mtime-age heuristic
            // isFileStable() normally uses doesn't apply here and would
            // otherwise leave this clip stuck in "waiting" until
            // something else re-triggers processing, which for a manual
            // clip taken mid-broadcast could be a very long time away
            // (not until the whole broadcast eventually ends).
            skipStabilityCheck: true,
          });

          // processRecordingFile doesn't have a param for this — tag it
          // as a manual clip afterward so it's distinguishable from
          // auto-archived recordings without touching that shared
          // function's signature (which the passive pipeline also uses).
          await pool.query(
            `UPDATE recordings SET source = 'manual_clip', started_at = $2, ended_at = $3 WHERE id = $1`,
            [recording.id, startedAt, endedAt.toISOString()],
          );

          // Archive to Bunny + delete the local file(s) IMMEDIATELY,
          // rather than waiting for archiveReadyRecordingsForOrganization
          // — that only ever runs 8s after the whole broadcast's
          // on_unpublish fires, which for a manual clip taken mid-
          // broadcast could be arbitrarily far in the future (the
          // broadcast may keep going for hours after this clip is
          // stopped). Recordings should never sit on local disk longer
          // than necessary to convert them — this makes manual clips
          // behave the same way auto-archived recordings already do,
          // instead of accumulating local disk usage until the source
          // broadcast happens to end.
          if (BUNNY_STORAGE_API_KEY) {
            const freshRow = await pool.query(
              `SELECT * FROM recordings WHERE id = $1`,
              [recording.id],
            );
            if (freshRow.rows[0]) {
              await archiveRecordingRow(freshRow.rows[0]);
            }
          }

          console.log(
            `[MANUAL-RECORDING] Registered clip for ${channel.stream_key}: ${filename}`,
          );
        } catch (err) {
          console.error(
            `[MANUAL-RECORDING] Failed to register clip after stop:`,
            err.message,
          );
        }
      };

      // ffmpeg needs a moment to actually finish writing after SIGTERM —
      // wait for its exit event rather than registering the file
      // immediately, or the stat/size read below could race a
      // still-finalizing file.
      proc.once("exit", () => {
        registerRecording();
      });

      const forceKillTimer = setTimeout(() => {
        if (proc.exitCode === null) {
          console.warn(
            `[MANUAL-RECORDING ${channel.stream_key}] Force-killing after graceful shutdown timeout.`,
          );
          proc.kill("SIGKILL");
        }
      }, 5000);
      forceKillTimer.unref();

      manualRecordingProcesses.delete(channel.id);

      res.json({ ok: true, message: "Manual recording stopped" });
    } catch (error) {
      console.error("Stop manual recording error:", error);
      res
        .status(500)
        .json({ ok: false, message: "Failed to stop manual recording" });
    }
  },
);

/*
|--------------------------------------------------------------------------
| TRANSCODING
|--------------------------------------------------------------------------
*/

app.post("/api/transcode/start", authenticateAdmin, async (req, res) => {
  try {
    const { stream } = req.body;

    if (!stream) {
      return res.status(400).json({
        ok: false,
        message: "Stream name is required",
      });
    }

    // Use the same verified local SRS HLS source path as automatic ABR.
    const input = getInternalHlsSourceUrl(stream);
    const output720 = `rtmp://127.0.0.1/live/${stream}_720p`;
    const output480 = `rtmp://127.0.0.1/live/${stream}_480p`;

    const command720 = `ffmpeg -y -i "${input}" -map 0:v:0 -map 0:a:0? -c:v libx264 -preset veryfast -b:v 2500k -s 1280x720 -c:a aac -b:a 128k -f flv "${output720}"`;

    const command480 = `ffmpeg -y -i "${input}" -map 0:v:0 -map 0:a:0? -c:v libx264 -preset veryfast -b:v 1200k -s 854x480 -c:a aac -b:a 96k -f flv "${output480}"`;

    exec(command720, (error, stdout, stderr) => {
      if (error) {
        console.error("720p transcode error:", error.message);
        console.error(stderr);
      }
    });

    exec(command480, (error, stdout, stderr) => {
      if (error) {
        console.error("480p transcode error:", error.message);
        console.error(stderr);
      }
    });

    res.json({
      ok: true,
      message: "Transcoding started",
      input,
      outputs: [output720, output480],
    });
  } catch (error) {
    console.error("Start transcode error:", error);

    res.status(500).json({
      ok: false,
      message: "Failed to start transcoding",
      error: error.message,
    });
  }
});

/*
|--------------------------------------------------------------------------
| ABR MASTER PLAYLIST - PUBLIC
|--------------------------------------------------------------------------
*/

// ══════════════════════════════════════════
// ABR rendition manifest proxy — gives 720p/480p the same viewer-facing
// protection the bitrate-cap path has always had (see /api/hls/:streamKey.m3u8
// above), for a completely different underlying failure mode.
//
// Real incident (2026-08-02): Maranatha's 480p transcode hit the
// already-characterized transient SRS-internal race (ffmpeg exits,
// auto-retries within its normal MAX_TRANSCODE_RETRIES budget, recovers)
// — the exact same harmless blip long since proven contained on the
// bitrate-cap path. But /api/abr/master.m3u8 was pointing renditions
// directly at SRS's raw HLS output with zero retry-tolerance, so viewers
// on that rendition hit the raw, transiently-invalid manifest immediately
// (hls.js levelParsingError: "Missing Target Duration") instead of the
// blip being absorbed the way it always has been elsewhere.
//
// First attempt at this fix (same day) held the HTTP connection open,
// polling upstream for up to 20s before responding — wrong shape, since
// hls.js's own client-side manifest-load timeout is well under 20s, so
// it gave up and retried before the server-side poll ever finished
// (repeated non-fatal levelLoadTimeOut). Corrected to match the already-
// proven pattern used by the bitrate-cap path: decide instantly (one
// quick upstream attempt, short timeout), serve if valid, otherwise 503
// — the CLIENT does the retrying on its own cadence, the server never
// blocks a response waiting things out.
//
// Attempted same-day to extend this to the "Original" (source) rendition
// too, based on live evidence it hit the same-looking error signature —
// reverted almost immediately when it turned out SRS serves Original's
// manifest via a session-context redirect (a stub response with
// ?hls_ctx=... that must be followed to reach the real manifest), which
// this proxy's single-fetch validity check doesn't handle. That mismatch
// made every request 503 permanently (not transient), leaving real
// viewers stuck on "loading" indefinitely. Original is back on the raw
// SRS URL directly (see master.m3u8 route below) pending a proper fix
// that actually follows the redirect. This route only covers 720p/480p.
// ══════════════════════════════════════════
const ABR_VALID_RENDITIONS = new Set(["top", "720p", "480p"]);

// SRS's raw/native HLS output (confirmed via live testing 2026-08-02) uses
// a one-level session-context redirect: the first request to a manifest
// returns a tiny single-variant stub (#EXTM3U + one #EXT-X-STREAM-INF
// pointing back to the same path with a fresh ?hls_ctx=... appended),
// and the REAL media playlist (with #EXT-X-TARGETDURATION and segments)
// only appears once that's followed. hls.js already follows this
// correctly on its own when a manifest URL is the TOP-LEVEL src it loads
// directly (which is why /api/hls/:streamKey.m3u8 has always worked
// standalone) — but it does NOT recursively resolve a nested "master-
// style" redirect found inside one variant slot of an already-parsed
// master playlist (our own /api/abr/master.m3u8), so embedding the raw
// redirect URL there produced an immediate, permanent "Missing Target
// Duration" failure for real viewers. This helper follows that one level
// server-side, so by the time hls.js sees this route's response, it's
// always the real flat manifest, regardless of rendition.
const isSessionRedirectStub = (text) =>
  text.includes("#EXT-X-STREAM-INF") && !text.includes("#EXT-X-TARGETDURATION");

const extractRedirectUri = (text) => {
  const lines = text.split("\n").map((l) => l.trim());
  const infIndex = lines.findIndex((l) => l.startsWith("#EXT-X-STREAM-INF"));
  if (infIndex === -1) return null;
  // The URI is the next non-empty, non-comment line after the STREAM-INF tag.
  for (let i = infIndex + 1; i < lines.length; i++) {
    if (lines[i] && !lines[i].startsWith("#")) return lines[i];
  }
  return null;
};

const fetchInitializedHlsPlaylist = async (
  initialUrl,
  { timeoutMs = 4000 } = {},
) => {
  const fetchOnce = async (playlistUrl) => {
    const response = await fetch(playlistUrl, {
      signal: AbortSignal.timeout(timeoutMs),
      headers: {
        "ngrok-skip-browser-warning": "1",
        "User-Agent": "NLM-Streaming-Backend/1.0",
        Accept: "application/vnd.apple.mpegurl, application/x-mpegURL, */*",
      },
    });

    if (!response.ok) {
      return {
        ok: false,
        status: response.status,
        text: "",
        finalUrl: playlistUrl,
      };
    }

    return {
      ok: true,
      status: response.status,
      text: await response.text(),
      finalUrl: response.url || playlistUrl,
    };
  };

  const first = await fetchOnce(initialUrl);
  if (!first.ok) return first;

  let playlistText = first.text;
  let playlistUrl = first.finalUrl;

  // SRS 6 can return a session bootstrap manifest whose only media URI
  // points back to the same playlist with ?hls_ctx=<session>. Follow that
  // URI exactly once to retrieve the actual media playlist.
  if (isSessionRedirectStub(playlistText)) {
    const redirectUri = extractRedirectUri(playlistText);

    if (!redirectUri) {
      return {
        ok: false,
        status: 502,
        text: "",
        finalUrl: playlistUrl,
      };
    }

    const redirectUrl = new URL(redirectUri, playlistUrl).toString();
    const second = await fetchOnce(redirectUrl);
    if (!second.ok) return second;

    playlistText = second.text;
    playlistUrl = second.finalUrl;
  }

  const initialized =
    playlistText.trimStart().startsWith("#EXTM3U") &&
    playlistText.includes("#EXT-X-TARGETDURATION") &&
    (playlistText.includes("#EXTINF:") || playlistText.includes("#EXT-X-MAP:"));

  return {
    ok: initialized,
    status: initialized ? 200 : 503,
    text: playlistText,
    finalUrl: playlistUrl,
  };
};

// SRS writes rendition manifests with bare relative segment filenames
// (e.g. "streamkey_720p-0042.ts"), correct only when the manifest itself
// is fetched from /live/ (matching nginx's existing ^/live/(.*\.ts)$
// block). Serving that same manifest text from /api/abr/ instead would
// silently break every segment fetch — hls.js would resolve them against
// /api/abr/:stream/ instead of /live/. Rewrite any line that isn't a
// #-comment and isn't already absolute (http/https/leading slash) to be
// rooted at /live/ instead, so segments keep resolving correctly
// regardless of which path served the manifest.
const rewriteRelativeSegmentUris = (manifestText, streamKey) =>
  manifestText
    .split("\n")
    .map((line) => {
      const trimmed = line.trim();

      if (trimmed.startsWith("#EXT-X-KEY")) {
        return rewriteHlsKeyLine(line);
      }

      if (!trimmed || trimmed.startsWith("#")) {
        return line;
      }

      const questionIndex = trimmed.indexOf("?");
      const uriPart =
        questionIndex >= 0 ? trimmed.slice(0, questionIndex) : trimmed;
      const existingQs = questionIndex >= 0 ? trimmed.slice(questionIndex) : "";
      const filename = uriPart.split("/").pop();

      // Route every ABR media fragment through the same backend segment
      // proxy used by the raw HLS path. The previous implementation emitted
      // direct /live/*.ts URLs on the Bunny hostname; dedicated pull zones
      // do not consistently expose that path and real viewers received 404
      // fragLoadError responses even though SRS had the segment locally.
      if (filename && filename.toLowerCase().endsWith(".ts")) {
        const segPath = `/api/hls/seg/live/${encodeURIComponent(
          streamKey,
        )}/${encodeURIComponent(filename)}`;
        return `${segPath}${appendBunnyToken(segPath, existingQs)}`;
      }

      // Leave uncommon absolute/non-TS URIs untouched. Current SRS ABR
      // output is MPEG-TS, so these are not expected in the normal path.
      return line;
    })
    .join("\n");

// Lightweight ABR startup/readiness endpoint.
//
// Unlike the actual HLS master route, this endpoint ALWAYS returns HTTP 200.
// The public Watch page polls this JSON endpoint while FFmpeg/SRS initialize,
// then mounts hls.js only after at least one planned rendition has a real
// initialized media playlist. This prevents expected startup 503 responses
// from appearing as red browser-console errors.
app.get("/api/abr/:stream/status", async (req, res) => {
  const { stream } = req.params;
  const baseUrl = `${SRS_INTERNAL_HLS_BASE_URL.replace(/\/$/, "")}/live`;

  res.setHeader("Cache-Control", "no-store");

  try {
    let renditionPlan = [];

    const channelResult = await pool.query(
      `SELECT organization_id, is_live, live_started_at
       FROM channels
       WHERE stream_key = $1
       LIMIT 1`,
      [stream],
    );

    const channel = channelResult.rows[0] || null;

    if (!channel) {
      return res.json({
        ok: true,
        stream,
        isLive: false,
        abrReady: false,
        plannedRenditions: [],
        readyRenditions: [],
        reason: "channel_not_found",
      });
    }

    if (channel.organization_id) {
      renditionPlan = await getRenditionPlanForOrg(channel.organization_id);
    }

    const readyRenditions = [];

    for (const rendition of renditionPlan) {
      const upstreamUrl = `${baseUrl}/${stream}_${rendition.label}.m3u8`;

      try {
        const result = await fetchInitializedHlsPlaylist(upstreamUrl);

        if (result.ok) {
          readyRenditions.push({
            label: rendition.label,
            bitrateKbps: rendition.bitrateKbps,
            resolution: rendition.resolution,
          });
        }
      } catch (error) {
        console.debug(
          `[ABR-STATUS] ${stream}/${rendition.label} not ready:`,
          error.message,
        );
      }
    }

    // The raw SRS publisher is the only authority for whether the broadcast
    // is live. Renditions are derived outputs and may briefly outlive the raw
    // source; the database marker can also be stale after a missed webhook.
    let srsStreamActive = false;

    try {
      const srsResponse = await fetch(
        `${SRS_API_URL.replace(/\/$/, "")}/api/v1/streams/`,
        { signal: AbortSignal.timeout(5000) },
      );

      if (srsResponse.ok) {
        const srsData = await srsResponse.json();
        srsStreamActive = (srsData.streams || []).some(
          (item) => item.name === stream && item.publish?.active,
        );
      }
    } catch (error) {
      // SRS lookup failure must not hide an otherwise healthy initialized
      // rendition. Keep this at debug level to avoid polluting Recent Errors.
      console.debug(
        `[ABR-STATUS] Unable to verify SRS publish state for ${stream}:`,
        error.message,
      );
    }

    const isLive = srsStreamActive;
    const abrReady = srsStreamActive && readyRenditions.length > 0;

    // Self-heal the database marker when SRS proves the raw publisher is live.
    // This keeps dashboard/API consumers consistent without making playback
    // depend on the database update succeeding.
    if (srsStreamActive && !channel.is_live) {
      pool
        .query(
          `UPDATE channels
           SET is_live = TRUE,
               live_started_at = COALESCE(live_started_at, NOW())
           WHERE stream_key = $1`,
          [stream],
        )
        .catch((error) =>
          console.debug(
            `[ABR-STATUS] Failed to repair DB live state for ${stream}:`,
            error.message,
          ),
        );
    }

    return res.json({
      ok: true,
      stream,
      isLive,
      abrReady,
      liveSource: srsStreamActive ? "srs" : "none",
      liveStartedAtMs: channel.live_started_at
        ? new Date(channel.live_started_at).getTime()
        : 0,
      encoderGeneration: bitrateCapEncoderGeneration.get(stream) || 0,
      broadcastGeneration: bitrateCapGeneration.get(stream) || 0,
      plannedRenditions: renditionPlan.map((rendition) => ({
        label: rendition.label,
        bitrateKbps: rendition.bitrateKbps,
        resolution: rendition.resolution,
      })),
      readyRenditions,
      reason: abrReady
        ? "ready"
        : isLive
          ? "renditions_initializing"
          : "offline",
    });
  } catch (error) {
    console.error(
      `[ABR-STATUS] Failed to resolve readiness for ${stream}:`,
      error.message,
    );

    return res.json({
      ok: false,
      stream,
      isLive: false,
      abrReady: false,
      plannedRenditions: [],
      readyRenditions: [],
      reason: "status_check_failed",
      message: error.message,
    });
  }
});

app.get("/api/abr/:stream/master.m3u8", async (req, res) => {
  const { stream } = req.params;
  const baseUrl = `${SRS_INTERNAL_HLS_BASE_URL.replace(/\/$/, "")}/live`;

  // Never serve stale rendition playlists after the raw broadcaster has
  // disconnected. Derived outputs are not authoritative proof of a live
  // broadcast and may remain visible briefly while SRS cleans them up.
  try {
    const rawSource = await getSrsRawStream(stream);
    if (!rawSource) {
      res.setHeader(
        "Cache-Control",
        "no-store, no-cache, must-revalidate, max-age=0",
      );
      res.setHeader("CDN-Cache-Control", "no-store");
      res.setHeader("Surrogate-Control", "no-store");
      res.setHeader("Retry-After", "2");
      return res.status(503).send("The source broadcast is offline");
    }
  } catch (error) {
    console.warn(
      `[ABR] Unable to verify raw source before serving master for ${stream}:`,
      error.message,
    );
    res.setHeader(
      "Cache-Control",
      "no-store, no-cache, must-revalidate, max-age=0",
    );
    res.setHeader("CDN-Cache-Control", "no-store");
    res.setHeader("Surrogate-Control", "no-store");
    res.setHeader("Retry-After", "2");
    return res.status(503).send("Unable to verify source broadcast status");
  }

  const checkPlaylist = async (url) => {
    try {
      const result = await fetchInitializedHlsPlaylist(url);
      return result.ok;
    } catch (error) {
      console.warn(
        `[ABR] Playlist readiness check failed for ${url}:`,
        error.message,
      );
      return false;
    }
  };

  // Rendition ladder is now entirely plan-driven (see
  // getRenditionPlanForOrg) — no more hardcoded 720p/480p bitrates or an
  // unconditional "Original" entry. Every rung, including the top one, is
  // checked for actual availability before being listed; deliberately no
  // raw/unbounded fallback rung (see the PLAN-DRIVEN ABR RENDITION LADDER
  // comment near getRenditionPlanForOrg for why).
  let renditionPlan = [];
  try {
    const channelResult = await pool.query(
      `SELECT organization_id FROM channels WHERE stream_key = $1`,
      [stream],
    );
    const organizationId = channelResult.rows[0]?.organization_id;
    if (organizationId) {
      renditionPlan = await getRenditionPlanForOrg(organizationId);
    }
  } catch (err) {
    console.error(
      `[ABR] Failed to resolve rendition plan for ${stream}:`,
      err.message,
    );
  }

  let masterPlaylist = `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-INDEPENDENT-SEGMENTS
`;

  let availableRenditions = 0;

  for (const rendition of renditionPlan) {
    const upstreamUrl = `${baseUrl}/${stream}_${rendition.label}.m3u8`;
    if (!(await checkPlaylist(upstreamUrl))) continue;

    const path = `/api/abr/${stream}/${rendition.label}.m3u8`;
    masterPlaylist += `#EXT-X-STREAM-INF:BANDWIDTH=${rendition.bitrateKbps * 1000},RESOLUTION=${rendition.resolution},NAME="${rendition.label}"
${path}${appendBunnyToken(path)}
`;
    availableRenditions += 1;
  }

  // Never return a syntactically header-only master playlist with HTTP 200.
  // hls.js interprets that as a fatal parse error: "no levels found in
  // manifest". A 503 correctly represents the temporary condition and lets
  // its configured manifest retry logic wait for ffmpeg/SRS to recover.
  if (availableRenditions === 0) {
    console.warn("[ABR] No initialized renditions available", {
      stream,
      planned: renditionPlan.map((rendition) => rendition.label),
    });
    res.setHeader(
      "Cache-Control",
      "no-store, no-cache, must-revalidate, max-age=0",
    );
    res.setHeader("CDN-Cache-Control", "no-store");
    res.setHeader("Surrogate-Control", "no-store");
    res.setHeader("Retry-After", "2");
    return res
      .status(503)
      .send("No ABR renditions are ready yet, please retry shortly");
  }

  res.setHeader("Content-Type", "application/vnd.apple.mpegurl");
  res.setHeader(
    "Cache-Control",
    "no-store, no-cache, must-revalidate, max-age=0",
  );
  res.setHeader("CDN-Cache-Control", "no-store");
  res.setHeader("Surrogate-Control", "no-store");
  return res.send(masterPlaylist);
});

// NOTE: this route MUST be registered after the /master.m3u8 route above.
// Express matches routes in registration order, and :rendition is a
// wildcard param — a request for .../master.m3u8 would otherwise match
// THIS route first (rendition="master"), get rejected by the
// ABR_VALID_RENDITIONS check, and never reach the real master route below.
// (Real regression hit and fixed same-day: 2026-08-02.)
//
// Also fixed same-day: this originally held the HTTP connection open,
// polling upstream for up to ABR_RENDITION_RETRY_TOLERANCE_MS (20s)
// before responding. That's the wrong shape — hls.js's own client-side
// manifest-load timeout is well under 20s, so it gave up and retried
// BEFORE our server-side poll loop ever finished, producing repeated
// non-fatal levelLoadTimeOut churn. The already-proven pattern elsewhere
// in this file (/api/hls/:streamKey.m3u8's live_capped handling) never
// blocks a response either — it decides instantly (serve, or 503) and
// lets the CLIENT retry on its own cadence. This route now matches that:
// one quick upstream attempt, instant decision, no internal poll loop.
app.get("/api/abr/:stream/:rendition.m3u8", async (req, res) => {
  const { stream, rendition } = req.params;

  if (!ABR_VALID_RENDITIONS.has(rendition)) {
    return res.status(404).send("Unknown rendition");
  }

  // Query SRS directly. SRS 6 may return a session bootstrap manifest
  // containing a same-playlist URI with ?hls_ctx=<session>; follow that
  // once server-side so hls.js always receives the real media playlist.
  const upstreamUrl =
    `${SRS_INTERNAL_HLS_BASE_URL.replace(/\/$/, "")}/live/` +
    `${encodeURIComponent(stream)}_${encodeURIComponent(rendition)}.m3u8`;

  try {
    const result = await fetchInitializedHlsPlaylist(upstreamUrl);

    if (result.ok) {
      res.setHeader("Content-Type", "application/vnd.apple.mpegurl");
      res.setHeader("Cache-Control", "no-store");
      return res.send(
        rewriteRelativeSegmentUris(result.text, `${stream}_${rendition}`),
      );
    }
  } catch (error) {
    console.warn(
      `[ABR] Rendition playlist fetch failed for ${stream}_${rendition}:`,
      error.message,
    );
    // Upstream unreachable (ffmpeg mid-restart, SRS momentarily not
    // serving this stream) — fall through to the 503 below.
  }

  res
    .status(503)
    .send("Rendition temporarily unavailable, please retry shortly");
});

/*
|--------------------------------------------------------------------------
| SOCKET.IO
|--------------------------------------------------------------------------
*/

io.on("connection", (socket) => {
  console.log("Realtime client connected:", socket.id);

  socket.emit("connected", {
    message: "Realtime connection active",
  });

  socket.on("disconnect", () => {
    console.log("Realtime client disconnected:", socket.id);
  });
});

/*
|--------------------------------------------------------------------------
| SERVER
|--------------------------------------------------------------------------
*/

(async () => {
  await ensureScheduledStreamsTable();
  await ensureOrganizationTables();
  await ensureRecordingLibraryTable();
  await ensureChannelRecordingColumn();
  await ensureViewerAnalyticsTables();
  await ensureReplayAnalyticsTables();
  await ensureSubscriptionTables();
  await ensurePendingSignupsTable();
  await ensureFeatureFlagsTable();
  await ensureNotificationPreferencesTable();
  await ensureSocialOAuthTables(pool);
  await ensureRestartAuditTable();
  await ensureApiKeysTable();
  await ensureServerMetricsHistoryTable(pool);
  await embedRoutes.ensureEmbedColumns(pool); // Phase 1 — embed_token/embed_settings columns
})()
  .then(() => {
    server.listen(PORT, () => {
      console.log(
        `NLM Streaming Manager API running on http://localhost:${PORT}`,
      );
    });

    // WHMCS billing poller — see pollWhmcsBilling() for why this is a poll
    // rather than a webhook. Runs once immediately, then on an interval.
    if (whmcs.isWhmcsConfigured()) {
      pollWhmcsBilling();
      setInterval(pollWhmcsBilling, WHMCS_POLL_INTERVAL_MS);
    } else {
      console.error(
        "[WHMCS-POLL] WHMCS is not configured (missing WHMCS_API_URL/IDENTIFIER/SECRET) — billing poller disabled.",
      );
    }

    // Bitrate compliance monitor — see pollBitrateCompliance() above.
    setInterval(pollBitrateCompliance, BITRATE_POLL_INTERVAL_MS);

    // System status page — see runSystemHealthChecks() above. Runs once
    // immediately so /api/public/status has real data from the moment
    // the server comes up, then on its own interval.
    runSystemHealthChecks();
    setInterval(runSystemHealthChecks, SYSTEM_HEALTH_CHECK_INTERVAL_MS);

    // Server metrics trend history — see collectServerMetricsSnapshot()
    // above. Runs once shortly after startup so the dashboard has at least
    // one data point immediately, then on its own interval.
    setTimeout(collectServerMetricsSnapshot, 15000);
    setInterval(
      collectServerMetricsSnapshot,
      SERVER_METRICS_COLLECTION_INTERVAL_MS,
    );

    // ABR self-healing: recover any live raw source that is missing one or
    // more plan-required rendition processes. Run shortly after startup and
    // continuously so the first OBS session works after backend/SRS restarts.
    const initialAbrRecoveryTimer = setTimeout(reconcileAbrTranscoders, 5000);
    initialAbrRecoveryTimer.unref?.();
    const abrRecoveryInterval = setInterval(reconcileAbrTranscoders, 10000);
    abrRecoveryInterval.unref?.();

    // Bandwidth quota monitor — see pollBandwidthCompliance() above. Runs
    // once immediately, then on its slower interval.
    if (bunny.isBunnyAccountConfigured()) {
      pollBandwidthCompliance();
      setInterval(pollBandwidthCompliance, BANDWIDTH_POLL_INTERVAL_MS);
    }

    // Proactively refresh YouTube access tokens before they expire (~1hr
    // lifetime), so a scheduled/automated go-live never fails mid-stream
    // waiting on a lazy refresh. Facebook Page tokens aren't on a refresh
    // schedule, so nothing to do for them here — see debugToken() in
    // facebook_graph_service.js if a periodic validity check is wanted later.
    setInterval(
      async () => {
        try {
          const expiringSoon = await pool.query(
            `SELECT * FROM social_oauth_accounts
           WHERE platform = 'youtube'
             AND refresh_token IS NOT NULL
             AND (token_expires_at IS NULL OR token_expires_at < now() + interval '15 minutes')`,
          );
          for (const account of expiringSoon.rows) {
            try {
              const refreshed = await youtubeApi.refreshAccessToken(
                account.refresh_token,
              );
              await pool.query(
                `UPDATE social_oauth_accounts SET access_token = $1, token_expires_at = $2, updated_at = now() WHERE id = $3`,
                [
                  refreshed.access_token,
                  refreshed.expiry_date
                    ? new Date(refreshed.expiry_date)
                    : null,
                  account.id,
                ],
              );
            } catch (refreshErr) {
              // Refresh token itself may have been revoked — log so a stale
              // connection can be caught before someone tries to go live with it.
              console.error(
                `[SOCIAL-OAUTH] Failed to refresh YouTube token for account ${account.id}:`,
                refreshErr.message,
              );
            }
          }
        } catch (sweepErr) {
          console.error(
            "[SOCIAL-OAUTH] Token refresh sweep failed:",
            sweepErr.message,
          );
        }
      },
      30 * 60 * 1000,
    ); // every 30 minutes

    // ══════════════════════════════════════════
    // SRS health watchdog — auto-recovery layer
    // Docker's own restart policy (restart: unless-stopped) only reacts if
    // the SRS container process actually dies. It does NOT notice if SRS is
    // still running but unresponsive (hung, deadlocked, network-partitioned)
    // -- that gap is what this watchdog covers. Requires 2+ CONSECUTIVE
    // failed health checks before acting (a single blip shouldn't trigger a
    // restart), and rate-limits itself to one auto-restart attempt per
    // 10 minutes so a persistently broken SRS can't cause a restart-loop.
    // ══════════════════════════════════════════
    let srsConsecutiveFailures = 0;
    let lastSrsAutoRestartAt = 0;
    const SRS_AUTO_RESTART_COOLDOWN_MS = 10 * 60 * 1000;

    setInterval(async () => {
      let healthy = false;
      try {
        const res = await fetch(`${SRS_API_URL}/api/v1/streams`, {
          signal: AbortSignal.timeout(5000),
        });
        healthy = res.ok;
      } catch {
        healthy = false;
      }

      if (healthy) {
        srsConsecutiveFailures = 0;
        return;
      }

      srsConsecutiveFailures += 1;
      console.error(
        `[SRS-WATCHDOG] Health check failed (${srsConsecutiveFailures} consecutive)`,
      );

      if (srsConsecutiveFailures < 2) return; // one blip isn't enough to act on

      const sinceLastRestart = Date.now() - lastSrsAutoRestartAt;
      if (sinceLastRestart < SRS_AUTO_RESTART_COOLDOWN_MS) {
        console.error(
          `[SRS-WATCHDOG] SRS unhealthy but skipping auto-restart -- last attempt was ${Math.round(sinceLastRestart / 1000)}s ago (cooldown: ${SRS_AUTO_RESTART_COOLDOWN_MS / 1000}s)`,
        );
        return;
      }

      if (!SRS_DOCKER_CONTAINER) {
        console.error(
          "[SRS-WATCHDOG] SRS unhealthy but SRS_DOCKER_CONTAINER is not set -- cannot auto-restart. Set it in .env.",
        );
        return;
      }

      lastSrsAutoRestartAt = Date.now();
      console.error(
        `[SRS-WATCHDOG] Attempting auto-restart of ${SRS_DOCKER_CONTAINER} after ${srsConsecutiveFailures} consecutive failed health checks`,
      );

      try {
        await pool.query(
          `INSERT INTO restart_audit_log
             (admin_id, admin_email, action, reason, active_streams_at_time, affected_organizations)
           VALUES (NULL, 'system (auto-recovery)', 'srs-auto', $1, 0, NULL)`,
          [
            `Auto-recovery: SRS failed ${srsConsecutiveFailures} consecutive health checks`,
          ],
        );
      } catch (auditErr) {
        console.error(
          "[SRS-WATCHDOG] Failed to write audit log:",
          auditErr.message,
        );
      }

      exec(`docker restart ${SRS_DOCKER_CONTAINER}`, (err) => {
        if (err) {
          console.error(
            "[SRS-WATCHDOG] Auto-restart command failed:",
            err.message,
          );
        } else {
          console.error(
            `[SRS-WATCHDOG] Auto-restart of ${SRS_DOCKER_CONTAINER} completed`,
          );
          srsConsecutiveFailures = 0;
        }
      });
    }, 30 * 1000); // check every 30 seconds
  })
  .catch((error) => {
    console.error("Failed to initialize database tables:", error);
    process.exit(1);
  });
