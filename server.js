// server/server.js

const express = require("express");
const cors = require("cors");
const http = require("http");
const { Server } = require("socket.io");
const { exec } = require("child_process");
require("dotenv").config();

const fs = require("fs");
const path = require("path");

const bcrypt = require("bcryptjs");
const jwt = require("jsonwebtoken");
const Stripe = require("stripe");

let UAParser = null;
try {
  const uaParserModule = require("ua-parser-js");
  UAParser = uaParserModule.UAParser || uaParserModule;
} catch {
  UAParser = null;
}

const pool = require("./db");

const app = express();
const server = http.createServer(app);

const PORT = process.env.PORT || 5000;
const CLIENT_URL = process.env.CLIENT_URL || "http://localhost:5174";
const CORS_ORIGINS = (process.env.CORS_ORIGINS || CLIENT_URL)
  .split(",")
  .map((origin) => origin.trim())
  .filter(Boolean);

const SRS_API_URL = process.env.SRS_API_URL || "http://localhost:1985";
const HLS_BASE_URL = process.env.HLS_BASE_URL || "http://localhost:8080";
const API_PUBLIC_URL = process.env.API_PUBLIC_URL || `http://localhost:${PORT}`;
const RECORDINGS_ROOT = process.env.RECORDINGS_ROOT || "C:/nlm-srs/recordings";
const RECORDINGS_LIVE_ROOT = path.join(RECORDINGS_ROOT, "live");

const STRIPE_SECRET_KEY = process.env.STRIPE_SECRET_KEY || "";
const STRIPE_WEBHOOK_SECRET = process.env.STRIPE_WEBHOOK_SECRET || "";
const stripe = STRIPE_SECRET_KEY ? new Stripe(STRIPE_SECRET_KEY) : null;

const corsOptions = {
  origin(origin, callback) {
    if (!origin || CORS_ORIGINS.includes(origin)) {
      return callback(null, true);
    }

    return callback(new Error(`CORS blocked origin: ${origin}`));
  },
  credentials: true,
};

app.use(cors(corsOptions));

app.post(
  "/api/stripe/webhook",
  express.raw({ type: "application/json" }),
  async (req, res) => {
    if (!stripe || !STRIPE_WEBHOOK_SECRET) {
      return res.status(501).json({
        ok: false,
        message: "Stripe webhook is not configured",
      });
    }

    let event;

    try {
      event = stripe.webhooks.constructEvent(
        req.body,
        req.headers["stripe-signature"],
        STRIPE_WEBHOOK_SECRET,
      );
    } catch (error) {
      console.error("Stripe webhook signature error:", error.message);
      return res.status(400).send(`Webhook Error: ${error.message}`);
    }

    try {
      if (event.type === "checkout.session.completed") {
        const session = event.data.object;

        if (session.mode === "subscription" && session.subscription) {
          const subscription = await stripe.subscriptions.retrieve(
            session.subscription,
          );

          const completedSignup =
            await completePendingSignupFromCheckoutSession(
              session,
              subscription,
            );

          await syncStripeSubscriptionToDatabase(subscription, {
            organizationId:
              completedSignup?.organization_id ||
              session.metadata?.organization_id,
            planKey: completedSignup?.plan_key || session.metadata?.plan_key,
            customerId: session.customer,
          });
        }
      }

      if (
        [
          "customer.subscription.created",
          "customer.subscription.updated",
          "customer.subscription.deleted",
        ].includes(event.type)
      ) {
        await syncStripeSubscriptionToDatabase(event.data.object);
      }

      res.json({ received: true });
    } catch (error) {
      console.error("Stripe webhook processing error:", error);
      res.status(500).json({ ok: false, message: "Webhook processing failed" });
    }
  },
);

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
    { expiresIn: "7d" },
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
    const decoded = jwt.verify(token, process.env.JWT_SECRET);

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

app.post("/api/auth/login", async (req, res) => {
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
| PUBLIC VIEWER / MEMBER AUTH
|--------------------------------------------------------------------------
*/

const generateViewerToken = (viewer) => {
  return jwt.sign(
    {
      id: viewer.id,
      email: viewer.email,
      name: viewer.name,
      role: "viewer_member",
      organization_id: viewer.organization_id || null,
    },
    process.env.JWT_SECRET,
    { expiresIn: "30d" },
  );
};

const extractBearerToken = (req) => {
  const queryToken =
    req.query?.member_token ||
    req.query?.viewer_token ||
    req.body?.member_token;
  if (queryToken) return String(queryToken);

  const authHeader = req.headers.authorization || "";
  if (!authHeader.startsWith("Bearer ")) return "";
  return authHeader.split(" ")[1] || "";
};

const authenticateViewerMemberOptional = async (req) => {
  try {
    const token = extractBearerToken(req);
    if (!token) return null;

    const decoded = jwt.verify(token, process.env.JWT_SECRET);

    if (decoded?.role !== "viewer_member" || !decoded?.id) {
      return null;
    }

    const result = await pool.query(
      `
      SELECT id, organization_id, name, email, status, created_at
      FROM replay_viewer_members
      WHERE id = $1
        AND status = 'active'
      LIMIT 1
      `,
      [decoded.id],
    );

    return result.rows[0] || null;
  } catch {
    return null;
  }
};

const requireViewerMember = async (req, res, next) => {
  const viewer = await authenticateViewerMemberOptional(req);

  if (!viewer) {
    return res.status(401).json({
      ok: false,
      code: "MEMBER_LOGIN_REQUIRED",
      message: "Member login is required.",
    });
  }

  req.viewerMember = viewer;
  return next();
};

const ensureReplayMemberTables = async () => {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS replay_viewer_members (
      id SERIAL PRIMARY KEY,
      organization_id INTEGER REFERENCES organizations(id) ON DELETE CASCADE,
      name VARCHAR(255) NOT NULL,
      email VARCHAR(255) NOT NULL,
      password_hash TEXT NOT NULL,
      status VARCHAR(40) DEFAULT 'active',
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE (organization_id, email)
    )
  `);

  await pool.query(`
    ALTER TABLE replay_sessions
    ADD COLUMN IF NOT EXISTS member_id INTEGER REFERENCES replay_viewer_members(id) ON DELETE SET NULL
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_replay_viewer_members_org_email
    ON replay_viewer_members (organization_id, email)
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_replay_sessions_member
    ON replay_sessions (member_id, recording_id, last_seen_at DESC)
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS replay_saved_replays (
      id SERIAL PRIMARY KEY,
      member_id INTEGER NOT NULL REFERENCES replay_viewer_members(id) ON DELETE CASCADE,
      recording_id INTEGER NOT NULL REFERENCES recordings(id) ON DELETE CASCADE,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE (member_id, recording_id)
    )
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_replay_saved_replays_member
    ON replay_saved_replays (member_id, created_at DESC)
  `);
};

const getOrganizationIdFromReplaySlug = async (slug) => {
  const cleanSlug = slugifyRecording(slug);

  const result = await pool.query(
    `
    SELECT organization_id
    FROM recordings
    WHERE public_slug = $1
      AND is_public = TRUE
    LIMIT 1
    `,
    [cleanSlug],
  );

  return result.rows[0]?.organization_id || null;
};

app.post("/api/public/members/register", async (req, res) => {
  try {
    const name = cleanOrgText(req.body?.name || "", 255);
    const email = cleanOrgText(req.body?.email || "", 255).toLowerCase();
    const password = String(req.body?.password || "");
    const replaySlug = cleanOrgText(req.body?.replay_slug || "", 255);
    const requestedOrg = req.body?.organization_id
      ? Number(req.body.organization_id)
      : null;

    if (!name || !email || !password) {
      return res.status(400).json({
        ok: false,
        message: "Name, email, and password are required.",
      });
    }

    if (password.length < 6) {
      return res.status(400).json({
        ok: false,
        message: "Password must be at least 6 characters.",
      });
    }

    const organizationId =
      requestedOrg ||
      (replaySlug ? await getOrganizationIdFromReplaySlug(replaySlug) : null);

    if (!organizationId) {
      return res.status(400).json({
        ok: false,
        message: "Unable to determine organization for this member account.",
      });
    }

    const passwordHash = await bcrypt.hash(password, 10);

    const result = await pool.query(
      `
      INSERT INTO replay_viewer_members (
        organization_id,
        name,
        email,
        password_hash,
        status
      )
      VALUES ($1, $2, $3, $4, 'active')
      RETURNING id, organization_id, name, email, status, created_at
      `,
      [organizationId, name, email, passwordHash],
    );

    const viewer = result.rows[0];
    const token = generateViewerToken(viewer);

    res.json({ ok: true, viewer, token });
  } catch (error) {
    if (error.code === "23505") {
      return res.status(409).json({
        ok: false,
        message: "A member account already exists for this email.",
      });
    }

    console.error("Member register error:", error);
    res.status(500).json({
      ok: false,
      message: "Failed to register member account.",
      error: error.message,
    });
  }
});

app.post("/api/public/members/login", async (req, res) => {
  try {
    const email = cleanOrgText(req.body?.email || "", 255).toLowerCase();
    const password = String(req.body?.password || "");
    const replaySlug = cleanOrgText(req.body?.replay_slug || "", 255);
    const requestedOrg = req.body?.organization_id
      ? Number(req.body.organization_id)
      : null;

    if (!email || !password) {
      return res.status(400).json({
        ok: false,
        message: "Email and password are required.",
      });
    }

    const organizationId =
      requestedOrg ||
      (replaySlug ? await getOrganizationIdFromReplaySlug(replaySlug) : null);

    if (!organizationId) {
      return res.status(400).json({
        ok: false,
        message: "Unable to determine organization for this member account.",
      });
    }

    const result = await pool.query(
      `
      SELECT *
      FROM replay_viewer_members
      WHERE organization_id = $1
        AND email = $2
        AND status = 'active'
      LIMIT 1
      `,
      [organizationId, email],
    );

    const viewer = result.rows[0];

    if (!viewer) {
      return res.status(401).json({
        ok: false,
        message: "Invalid email or password.",
      });
    }

    const isMatch = await bcrypt.compare(password, viewer.password_hash);

    if (!isMatch) {
      return res.status(401).json({
        ok: false,
        message: "Invalid email or password.",
      });
    }

    const safeViewer = {
      id: viewer.id,
      organization_id: viewer.organization_id,
      name: viewer.name,
      email: viewer.email,
      status: viewer.status,
      created_at: viewer.created_at,
    };

    const token = generateViewerToken(safeViewer);

    res.json({ ok: true, viewer: safeViewer, token });
  } catch (error) {
    console.error("Member login error:", error);
    res.status(500).json({
      ok: false,
      message: "Failed to login member.",
      error: error.message,
    });
  }
});

app.get("/api/public/members/me", requireViewerMember, async (req, res) => {
  res.json({ ok: true, viewer: req.viewerMember });
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

  await pool.query(`
    ALTER TABLE live_chat_messages
    ADD COLUMN IF NOT EXISTS organization_id INTEGER REFERENCES organizations(id)
  `);

  await pool.query(`
    ALTER TABLE prayer_requests
    ADD COLUMN IF NOT EXISTS organization_id INTEGER REFERENCES organizations(id)
  `);

  await pool.query(`
    ALTER TABLE overlay_states
    ADD COLUMN IF NOT EXISTS organization_id INTEGER REFERENCES organizations(id)
  `);

  await pool.query(`
    ALTER TABLE overlay_history
    ADD COLUMN IF NOT EXISTS organization_id INTEGER REFERENCES organizations(id)
  `);

  await pool.query(`
    ALTER TABLE cta_links
    ADD COLUMN IF NOT EXISTS organization_id INTEGER REFERENCES organizations(id)
  `);

  await pool.query(`
    ALTER TABLE viewer_reactions
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
  await pool.query(
    `UPDATE live_chat_messages SET organization_id = $1 WHERE organization_id IS NULL`,
    [defaultOrg.id],
  );
  await pool.query(
    `UPDATE prayer_requests SET organization_id = $1 WHERE organization_id IS NULL`,
    [defaultOrg.id],
  );
  await pool.query(
    `UPDATE overlay_states SET organization_id = $1 WHERE organization_id IS NULL`,
    [defaultOrg.id],
  );
  await pool.query(
    `UPDATE overlay_history SET organization_id = $1 WHERE organization_id IS NULL`,
    [defaultOrg.id],
  );
  await pool.query(
    `UPDATE cta_links SET organization_id = $1 WHERE organization_id IS NULL`,
    [defaultOrg.id],
  );
  await pool.query(
    `UPDATE viewer_reactions SET organization_id = $1 WHERE organization_id IS NULL`,
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
      return next();
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
    next();
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
    {
      sql: "SELECT organization_id FROM cta_links WHERE stream_key = $1 AND organization_id IS NOT NULL ORDER BY created_at DESC LIMIT 1",
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

const PLAN_DEFINITIONS = [
  {
    key: "starter",
    name: "Starter",
    monthly_price_cents: 2900,
    max_channels: 1,
    max_admins: 2,
    max_storage_gb: 25,
    max_bitrate_kbps: 6000,
    transcoding_enabled: false,
    analytics_enabled: false,
    custom_domain_enabled: false,
    priority_support_enabled: false,
  },
  {
    key: "pro",
    name: "Pro",
    monthly_price_cents: 7900,
    max_channels: 5,
    max_admins: 8,
    max_storage_gb: 150,
    max_bitrate_kbps: 12000,
    transcoding_enabled: true,
    analytics_enabled: true,
    custom_domain_enabled: false,
    priority_support_enabled: false,
  },
  {
    key: "enterprise",
    name: "Enterprise",
    monthly_price_cents: 19900,
    max_channels: 25,
    max_admins: 50,
    max_storage_gb: 1000,
    max_bitrate_kbps: 25000,
    transcoding_enabled: true,
    analytics_enabled: true,
    custom_domain_enabled: true,
    priority_support_enabled: true,
  },
  {
    key: "internal",
    name: "Internal",
    monthly_price_cents: 0,
    max_channels: 999,
    max_admins: 999,
    max_storage_gb: 9999,
    max_bitrate_kbps: 50000,
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
      max_bitrate_kbps INTEGER DEFAULT 6000,
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

  await pool.query(`
    ALTER TABLE plans
    ADD COLUMN IF NOT EXISTS stripe_price_id VARCHAR(255)
  `);

  for (const plan of PLAN_DEFINITIONS) {
    const stripePriceId = getStripePriceIdForPlan(plan.key);

    await pool.query(
      `
      INSERT INTO plans (
        plan_key,
        name,
        monthly_price_cents,
        max_channels,
        max_admins,
        max_storage_gb,
        max_bitrate_kbps,
        transcoding_enabled,
        analytics_enabled,
        custom_domain_enabled,
        priority_support_enabled,
        stripe_price_id,
        is_active
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, TRUE)
      ON CONFLICT (plan_key)
      DO UPDATE SET
        name = EXCLUDED.name,
        monthly_price_cents = EXCLUDED.monthly_price_cents,
        max_channels = EXCLUDED.max_channels,
        max_admins = EXCLUDED.max_admins,
        max_storage_gb = EXCLUDED.max_storage_gb,
        max_bitrate_kbps = EXCLUDED.max_bitrate_kbps,
        transcoding_enabled = EXCLUDED.transcoding_enabled,
        analytics_enabled = EXCLUDED.analytics_enabled,
        custom_domain_enabled = EXCLUDED.custom_domain_enabled,
        priority_support_enabled = EXCLUDED.priority_support_enabled,
        stripe_price_id = EXCLUDED.stripe_price_id,
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
        plan.max_bitrate_kbps,
        plan.transcoding_enabled,
        plan.analytics_enabled,
        plan.custom_domain_enabled,
        plan.priority_support_enabled,
        stripePriceId || null,
      ],
    );
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

const completePendingSignupFromCheckoutSession = async (
  session,
  stripeSubscription,
) => {
  const checkoutSessionId = session?.id;

  if (!checkoutSessionId) return null;

  const pendingResult = await pool.query(
    `
    SELECT *
    FROM pending_signups
    WHERE checkout_session_id = $1
    LIMIT 1
    `,
    [checkoutSessionId],
  );

  const pending = pendingResult.rows[0];

  if (!pending) {
    console.warn(
      `No pending signup found for checkout session ${checkoutSessionId}`,
    );
    return null;
  }

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

    const periodStart = stripeSubscription?.current_period_start
      ? new Date(stripeSubscription.current_period_start * 1000)
      : null;
    const periodEnd = stripeSubscription?.current_period_end
      ? new Date(stripeSubscription.current_period_end * 1000)
      : null;
    const trialEnd = stripeSubscription?.trial_end
      ? new Date(stripeSubscription.trial_end * 1000)
      : null;

    await client.query(
      `
      INSERT INTO subscriptions (
        organization_id,
        plan_key,
        status,
        trial_ends_at,
        current_period_start,
        current_period_end,
        stripe_customer_id,
        stripe_subscription_id,
        cancel_at_period_end
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
      ON CONFLICT (organization_id)
      DO UPDATE SET
        plan_key = EXCLUDED.plan_key,
        status = EXCLUDED.status,
        trial_ends_at = COALESCE(EXCLUDED.trial_ends_at, subscriptions.trial_ends_at),
        current_period_start = COALESCE(EXCLUDED.current_period_start, subscriptions.current_period_start),
        current_period_end = COALESCE(EXCLUDED.current_period_end, subscriptions.current_period_end),
        stripe_customer_id = COALESCE(EXCLUDED.stripe_customer_id, subscriptions.stripe_customer_id),
        stripe_subscription_id = COALESCE(EXCLUDED.stripe_subscription_id, subscriptions.stripe_subscription_id),
        cancel_at_period_end = EXCLUDED.cancel_at_period_end,
        updated_at = NOW()
      `,
      [
        organization.id,
        pending.plan_key,
        mapStripeSubscriptionStatus(stripeSubscription?.status || "active"),
        trialEnd,
        periodStart,
        periodEnd,
        session.customer || stripeSubscription?.customer || null,
        stripeSubscription?.id || null,
        Boolean(stripeSubscription?.cancel_at_period_end),
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
      s.organization_id,
      s.plan_key,
      s.status,
      s.trial_ends_at,
      s.current_period_start,
      s.current_period_end,
      s.cancel_at_period_end,
      s.stripe_customer_id,
      s.stripe_subscription_id,
      p.name AS plan_name,
      p.monthly_price_cents,
      p.max_channels,
      p.max_admins,
      p.max_storage_gb,
      p.max_bitrate_kbps,
      p.transcoding_enabled,
      p.analytics_enabled,
      p.custom_domain_enabled,
      p.priority_support_enabled,
      COALESCE(channel_usage.count, 0)::int AS used_channels,
      COALESCE(member_usage.count, 0)::int AS used_admins
    FROM subscriptions s
    JOIN plans p ON p.plan_key = s.plan_key
    LEFT JOIN (
      SELECT organization_id, COUNT(*) AS count
      FROM channels
      WHERE organization_id = $1
      GROUP BY organization_id
    ) channel_usage ON channel_usage.organization_id = s.organization_id
    LEFT JOIN (
      SELECT organization_id, COUNT(*) AS count
      FROM organization_users
      WHERE organization_id = $1
      GROUP BY organization_id
    ) member_usage ON member_usage.organization_id = s.organization_id
    WHERE s.organization_id = $1
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

const isStripeCheckoutReadyForPlan = (planKey) => {
  return Boolean(stripe && getStripePriceIdForPlan(planKey));
};

const mapStripeSubscriptionStatus = (status) => {
  if (
    ["active", "trialing", "past_due", "canceled", "incomplete"].includes(
      status,
    )
  ) {
    return status;
  }

  if (status === "unpaid") return "past_due";
  return status || "incomplete";
};

const syncStripeSubscriptionToDatabase = async (
  stripeSubscription,
  fallback = {},
) => {
  const item = stripeSubscription.items?.data?.[0];
  const priceId = item?.price?.id;
  const planKey =
    fallback.planKey ||
    stripeSubscription.metadata?.plan_key ||
    getPlanKeyForStripePriceId(priceId) ||
    "starter";

  const organizationId =
    fallback.organizationId || stripeSubscription.metadata?.organization_id;

  const customerId = fallback.customerId || stripeSubscription.customer;

  if (!organizationId && !customerId) {
    console.warn(
      "Stripe subscription sync skipped: no organization/customer id",
    );
    return null;
  }

  let resolvedOrganizationId = organizationId;

  if (!resolvedOrganizationId && customerId) {
    const existing = await pool.query(
      `
      SELECT organization_id
      FROM subscriptions
      WHERE stripe_customer_id = $1
      LIMIT 1
      `,
      [customerId],
    );

    resolvedOrganizationId = existing.rows[0]?.organization_id;
  }

  if (!resolvedOrganizationId) {
    console.warn("Stripe subscription sync skipped: organization not found");
    return null;
  }

  const periodStart = stripeSubscription.current_period_start
    ? new Date(stripeSubscription.current_period_start * 1000)
    : null;
  const periodEnd = stripeSubscription.current_period_end
    ? new Date(stripeSubscription.current_period_end * 1000)
    : null;
  const trialEnd = stripeSubscription.trial_end
    ? new Date(stripeSubscription.trial_end * 1000)
    : null;

  await pool.query(
    `
    UPDATE organizations
    SET subscription_plan = $1,
        updated_at = NOW()
    WHERE id = $2
    `,
    [planKey, resolvedOrganizationId],
  );

  const result = await pool.query(
    `
    INSERT INTO subscriptions (
      organization_id,
      plan_key,
      status,
      trial_ends_at,
      current_period_start,
      current_period_end,
      stripe_customer_id,
      stripe_subscription_id,
      cancel_at_period_end
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
    ON CONFLICT (organization_id)
    DO UPDATE SET
      plan_key = EXCLUDED.plan_key,
      status = EXCLUDED.status,
      trial_ends_at = COALESCE(EXCLUDED.trial_ends_at, subscriptions.trial_ends_at),
      current_period_start = COALESCE(EXCLUDED.current_period_start, subscriptions.current_period_start),
      current_period_end = COALESCE(EXCLUDED.current_period_end, subscriptions.current_period_end),
      stripe_customer_id = COALESCE(EXCLUDED.stripe_customer_id, subscriptions.stripe_customer_id),
      stripe_subscription_id = COALESCE(EXCLUDED.stripe_subscription_id, subscriptions.stripe_subscription_id),
      cancel_at_period_end = EXCLUDED.cancel_at_period_end,
      updated_at = NOW()
    RETURNING *
    `,
    [
      resolvedOrganizationId,
      planKey,
      mapStripeSubscriptionStatus(stripeSubscription.status),
      trialEnd,
      periodStart,
      periodEnd,
      customerId || null,
      stripeSubscription.id,
      Boolean(stripeSubscription.cancel_at_period_end),
    ],
  );

  return result.rows[0];
};

const enforceChannelLimit = async (req, res, next) => {
  try {
    const summary = await ensureSubscriptionForOrganization(
      req.organization.id,
      req.organization.subscription_plan || "starter",
    );

    if (!summary) return next();

    if (summary.status && !["active", "trialing"].includes(summary.status)) {
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
          WHEN stripe_price_id IS NOT NULL AND stripe_price_id <> '' THEN TRUE
          ELSE FALSE
        END AS stripe_configured
      FROM plans
      WHERE is_active = TRUE
        AND plan_key <> 'internal'
      ORDER BY monthly_price_cents ASC
    `);

    res.json({ ok: true, plans: result.rows });
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
      const priceId = getStripePriceIdForPlan(planKey);

      if (!stripe || !priceId) {
        return res.status(501).json({
          ok: false,
          message:
            "Stripe checkout is not configured for this plan. Add Stripe keys and price IDs to the server .env.",
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

      const subscription = await ensureSubscriptionForOrganization(
        req.organization.id,
        req.organization.subscription_plan || "starter",
      );

      let customerId = subscription?.stripe_customer_id;

      if (!customerId) {
        const customer = await stripe.customers.create({
          email: req.admin.email,
          name: req.organization.name,
          metadata: {
            organization_id: String(req.organization.id),
            admin_id: String(req.admin.id),
          },
        });

        customerId = customer.id;

        await pool.query(
          `
          UPDATE subscriptions
          SET stripe_customer_id = $1,
              updated_at = NOW()
          WHERE organization_id = $2
          `,
          [customerId, req.organization.id],
        );
      }

      const session = await stripe.checkout.sessions.create({
        mode: "subscription",
        customer: customerId,
        line_items: [{ price: priceId, quantity: 1 }],
        allow_promotion_codes: true,
        success_url: `${CLIENT_URL.replace(/\/$/, "")}/?billing=success`,
        cancel_url: `${CLIENT_URL.replace(/\/$/, "")}/?billing=cancelled`,
        metadata: {
          organization_id: String(req.organization.id),
          plan_key: plan.plan_key,
          admin_id: String(req.admin.id),
        },
        subscription_data: {
          metadata: {
            organization_id: String(req.organization.id),
            plan_key: plan.plan_key,
            admin_id: String(req.admin.id),
          },
        },
      });

      res.json({ ok: true, checkout_url: session.url, session_id: session.id });
    } catch (error) {
      console.error("Create subscription checkout error:", error);
      res.status(500).json({
        ok: false,
        message: "Failed to create Stripe checkout session",
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
      if (!stripe) {
        return res.status(501).json({
          ok: false,
          message: "Stripe customer portal is not configured.",
        });
      }

      const subscription = await ensureSubscriptionForOrganization(
        req.organization.id,
        req.organization.subscription_plan || "starter",
      );

      if (!subscription?.stripe_customer_id) {
        return res.status(400).json({
          ok: false,
          message: "This tenant does not have a Stripe customer yet.",
        });
      }

      const portalSession = await stripe.billingPortal.sessions.create({
        customer: subscription.stripe_customer_id,
        return_url: `${CLIENT_URL.replace(/\/$/, "")}/`,
      });

      res.json({ ok: true, portal_url: portalSession.url });
    } catch (error) {
      console.error("Create billing portal error:", error);
      res.status(500).json({
        ok: false,
        message: "Failed to open Stripe billing portal",
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
      if (!stripe) {
        return res.status(501).json({
          ok: false,
          message: "Stripe invoice history is not configured.",
        });
      }

      const subscription = await ensureSubscriptionForOrganization(
        req.organization.id,
        req.organization.subscription_plan || "starter",
      );

      if (!subscription?.stripe_customer_id) {
        return res.json({ ok: true, invoices: [] });
      }

      const invoices = await stripe.invoices.list({
        customer: subscription.stripe_customer_id,
        limit: 12,
      });

      const formattedInvoices = (invoices.data || []).map((invoice) => ({
        id: invoice.id,
        number: invoice.number,
        status: invoice.status,
        amount_due: invoice.amount_due,
        amount_paid: invoice.amount_paid,
        amount_remaining: invoice.amount_remaining,
        currency: invoice.currency,
        hosted_invoice_url: invoice.hosted_invoice_url,
        invoice_pdf: invoice.invoice_pdf,
        created: invoice.created
          ? new Date(invoice.created * 1000).toISOString()
          : null,
        due_date: invoice.due_date
          ? new Date(invoice.due_date * 1000).toISOString()
          : null,
        period_start: invoice.period_start
          ? new Date(invoice.period_start * 1000).toISOString()
          : null,
        period_end: invoice.period_end
          ? new Date(invoice.period_end * 1000).toISOString()
          : null,
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
      if (!stripe) {
        const subscription = await ensureSubscriptionForOrganization(
          req.organization.id,
          req.organization.subscription_plan || "starter",
        );

        return res.json({ ok: true, subscription });
      }

      const subscription = await ensureSubscriptionForOrganization(
        req.organization.id,
        req.organization.subscription_plan || "starter",
      );

      if (subscription?.stripe_subscription_id) {
        const stripeSubscription = await stripe.subscriptions.retrieve(
          subscription.stripe_subscription_id,
        );

        await syncStripeSubscriptionToDatabase(stripeSubscription, {
          organizationId: req.organization.id,
          planKey: subscription.plan_key,
          customerId: subscription.stripe_customer_id,
        });
      }

      const refreshed = await getOrganizationSubscriptionSummary(
        req.organization.id,
      );

      res.json({ ok: true, subscription: refreshed });
    } catch (error) {
      console.error("Refresh subscription error:", error);
      res.status(500).json({
        ok: false,
        message: "Failed to refresh subscription from Stripe",
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

app.post("/api/public/signup", async (req, res) => {
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

    if (!isStripeCheckoutReadyForPlan(plan.plan_key)) {
      return res.status(501).json({
        ok: false,
        message:
          "Stripe checkout is not configured for this plan. Add Stripe keys and price IDs to the server .env.",
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

    const slugPreview = slugifyOrganization(organizationName);
    const requestedStreamKey = cleanOrgText(req.body.stream_key, 255);
    const streamKey = requestedStreamKey || `${slugPreview}-main`;

    const existingChannel = await pool.query(
      `SELECT id FROM channels WHERE stream_key = $1 LIMIT 1`,
      [streamKey],
    );

    if (existingChannel.rows[0]) {
      return res.status(409).json({
        ok: false,
        message:
          "This stream key is already in use. Please choose another stream key.",
      });
    }

    const passwordHash = await bcrypt.hash(clientPassword, 10);

    const customer = await stripe.customers.create({
      email: clientEmail,
      name: organizationName,
      metadata: {
        client_name: clientName,
        plan_key: plan.plan_key,
      },
    });

    const session = await stripe.checkout.sessions.create({
      mode: "subscription",
      customer: customer.id,
      line_items: [
        {
          price: getStripePriceIdForPlan(plan.plan_key),
          quantity: 1,
        },
      ],
      allow_promotion_codes: true,
      success_url: `${CLIENT_URL.replace(/\/$/, "")}/login?signup=success`,
      cancel_url: `${CLIENT_URL.replace(/\/$/, "")}/signup?plan=${plan.plan_key}&checkout=cancelled`,
      metadata: {
        plan_key: plan.plan_key,
        client_email: clientEmail,
      },
      subscription_data: {
        metadata: {
          plan_key: plan.plan_key,
          client_email: clientEmail,
        },
      },
    });

    await pool.query(
      `
      INSERT INTO pending_signups (
        checkout_session_id,
        stripe_customer_id,
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
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'pending')
      ON CONFLICT (checkout_session_id)
      DO UPDATE SET
        stripe_customer_id = EXCLUDED.stripe_customer_id,
        plan_key = EXCLUDED.plan_key,
        organization_name = EXCLUDED.organization_name,
        client_name = EXCLUDED.client_name,
        client_email = EXCLUDED.client_email,
        password_hash = EXCLUDED.password_hash,
        stream_key = EXCLUDED.stream_key,
        primary_color = EXCLUDED.primary_color,
        secondary_color = EXCLUDED.secondary_color,
        donation_url = EXCLUDED.donation_url,
        status = 'pending',
        updated_at = NOW()
      `,
      [
        session.id,
        customer.id,
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

    res.json({
      ok: true,
      requires_checkout: true,
      checkout_url: session.url,
      stripe_session_id: session.id,
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

app.get(
  "/api/organizations/:id/users",
  authenticateAdmin,
  requireRole("super_admin", "admin"),
  async (req, res) => {
    try {
      const { id } = req.params;

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
  },
);

app.post(
  "/api/organizations/:id/users",
  authenticateAdmin,
  requireRole("super_admin"),
  async (req, res) => {
    try {
      const { id } = req.params;
      const email = cleanOrgText(req.body.email, 255).toLowerCase();
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

      const adminResult = await pool.query(
        `
        SELECT id
        FROM admins
        WHERE email = $1
        LIMIT 1
        `,
        [email],
      );

      if (!adminResult.rows[0]) {
        return res.status(404).json({
          ok: false,
          message: "Admin account with this email was not found",
        });
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
  requireRole("super_admin"),
  async (req, res) => {
    try {
      const { organizationId, adminId } = req.params;

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
      const streamKey = cleanOrgText(req.body.stream_key, 255);
      const channelDescription = cleanOrgText(
        req.body.channel_description,
        1000,
      );

      if (!organizationName || !clientName || !clientEmail || !streamKey) {
        return res.status(400).json({
          ok: false,
          message:
            "Organization name, client name, client email, and stream key are required",
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
    CREATE TABLE IF NOT EXISTS replay_session_events (
      id SERIAL PRIMARY KEY,
      organization_id INTEGER REFERENCES organizations(id) ON DELETE SET NULL,
      recording_id INTEGER REFERENCES recordings(id) ON DELETE CASCADE,
      replay_session_id INTEGER REFERENCES replay_sessions(id) ON DELETE CASCADE,
      public_slug VARCHAR(255) NOT NULL,
      viewer_id VARCHAR(255) NOT NULL,
      event_type VARCHAR(80) NOT NULL,
      current_time_seconds INTEGER DEFAULT 0,
      watched_seconds INTEGER DEFAULT 0,
      delta_seconds INTEGER DEFAULT 0,
      playback_rate NUMERIC DEFAULT 1,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_replay_session_events_recording
    ON replay_session_events (recording_id, created_at DESC)
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_replay_session_events_session
    ON replay_session_events (replay_session_id, created_at DESC)
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

const closeStaleReplaySessions = async () => {
  await pool.query(`
    UPDATE replay_sessions
    SET ended_at = last_seen_at,
        watched_seconds = GREATEST(
          watched_seconds,
          EXTRACT(EPOCH FROM (last_seen_at - started_at))::int
        )
    WHERE ended_at IS NULL
      AND last_seen_at < NOW() - INTERVAL '5 minutes'
  `);
};

const getReplaySessionMetrics = async (recordingId) => {
  const result = await pool.query(
    `
    SELECT
      COUNT(*)::int AS total_views,
      COUNT(DISTINCT viewer_id)::int AS unique_viewers,
      COALESCE(SUM(watched_seconds), 0)::int AS total_watch_seconds,
      COALESCE(AVG(NULLIF(watched_seconds, 0)), 0)::int AS avg_watch_seconds,
      COALESCE(MAX(max_position_seconds), 0)::int AS furthest_position_seconds,
      COALESCE(AVG(CASE WHEN completed THEN 1 ELSE 0 END), 0)::float AS completion_rate
    FROM replay_sessions
    WHERE recording_id = $1
    `,
    [recordingId],
  );

  return (
    result.rows[0] || {
      total_views: 0,
      unique_viewers: 0,
      total_watch_seconds: 0,
      avg_watch_seconds: 0,
      furthest_position_seconds: 0,
      completion_rate: 0,
    }
  );
};

const recordReplaySessionEvent = async ({
  organizationId,
  recordingId,
  replaySessionId,
  publicSlug,
  viewerId,
  eventType = "heartbeat",
  currentTime = 0,
  watchedSeconds = 0,
  deltaSeconds = 0,
  playbackRate = 1,
}) => {
  if (!recordingId || !replaySessionId || !publicSlug || !viewerId) return;

  await pool.query(
    `
    INSERT INTO replay_session_events (
      organization_id,
      recording_id,
      replay_session_id,
      public_slug,
      viewer_id,
      event_type,
      current_time_seconds,
      watched_seconds,
      delta_seconds,
      playback_rate
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
    `,
    [
      organizationId || null,
      recordingId,
      replaySessionId,
      publicSlug,
      viewerId,
      cleanOrgText(eventType || "heartbeat", 80),
      Math.max(0, Math.floor(Number(currentTime || 0))),
      Math.max(0, Math.floor(Number(watchedSeconds || 0))),
      Math.max(0, Math.floor(Number(deltaSeconds || 0))),
      Number(playbackRate || 1),
    ],
  );
};

const getReplayProgressExpression = () => `
  GREATEST(
    COALESCE(latest.current_time_seconds, 0),
    COALESCE(latest.max_position_seconds, 0),
    CASE
      WHEN COALESCE(r.duration_seconds, 0) > 0
      THEN LEAST(COALESCE(latest.watched_seconds, 0), COALESCE(r.duration_seconds, 0))
      ELSE COALESCE(latest.watched_seconds, 0)
    END
  )
`;

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

app.post("/api/public/viewers/start", async (req, res) => {
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

    const metrics = await getViewerMetricsForStream(streamKey, organizationId);

    io.to(organizationScopedRoom("analytics", organizationId, streamKey)).emit(
      "analytics:viewers",
      {
        stream_key: streamKey,
        ...metrics,
      },
    );

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
});

app.post("/api/public/viewers/heartbeat", async (req, res) => {
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
});

app.post("/api/public/viewers/end", async (req, res) => {
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
});

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

        UNION ALL

        SELECT
          COALESCE(NULLIF(rs.device_type, ''), 'Unknown') AS device_type,
          COALESCE(NULLIF(rs.browser_name, ''), 'Unknown') AS browser_name,
          COALESCE(NULLIF(rs.os_name, ''), 'Unknown') AS os_name,
          rs.user_agent,
          rs.viewer_id,
          COALESCE(rs.watched_seconds, 0)::int AS watch_seconds
        FROM replay_sessions rs
        JOIN recordings r
          ON r.id = rs.recording_id
         AND r.organization_id = $1
        WHERE rs.organization_id = $1
          AND rs.started_at >= NOW() - ($2::text || ' days')::interval
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

        UNION ALL

        SELECT
          COALESCE(NULLIF(rs.country_code, ''), 'UNKNOWN') AS country_code,
          COALESCE(NULLIF(rs.country_name, ''), 'Local / Unknown') AS country_name,
          rs.viewer_id,
          COALESCE(rs.watched_seconds, 0)::int AS watch_seconds
        FROM replay_sessions rs
        JOIN recordings r
          ON r.id = rs.recording_id
         AND r.organization_id = $1
        WHERE rs.organization_id = $1
          AND rs.started_at >= NOW() - ($2::text || ' days')::interval
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

  // Primary: check DB is_live flag (set by SRS on_publish webhook)
  // This works even when SRS is on a different machine than the backend
  try {
    const channelResult = await pool.query(
      `SELECT stream_key, name, is_live, live_started_at,
              EXTRACT(EPOCH FROM (NOW() - live_started_at))::int AS uptime_seconds
       FROM channels
       WHERE stream_key = $1
         AND organization_id = $2
         AND is_live = TRUE
       LIMIT 1`,
      [streamKey, organizationId],
    );

    if (channelResult.rows[0]) {
      // Build a stream object matching the SRS format
      const ch = channelResult.rows[0];
      activeStream = {
        name: ch.stream_key,
        publish: { active: true, active_age: ch.uptime_seconds || 0 },
        clients: 0,
        kbps: { recv_30s: 0 },
      };
    }
  } catch (dbErr) {
    console.error("DB is_live check error:", dbErr.message);
  }

  // Fallback: try polling SRS directly (works when SRS and backend are co-located)
  if (!activeStream) {
    try {
      const response = await fetch(`${SRS_API_URL}/api/v1/streams`, {
        signal: AbortSignal.timeout(3000),
      });
      const data = await response.json();
      activeStream = (data.streams || []).find((stream) => {
        return stream.name === streamKey && stream.publish?.active;
      });
    } catch (error) {
      // Silent - SRS not reachable from this environment (expected when SRS is local)
    }
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

  return {
    organization_id: organizationId,
    organization: brandingData.organization,
    settings: brandingData.settings,
    branding: brandingData.branding,
    isLive: Boolean(activeStream),
    stream: activeStream || null,
    schedule: scheduleResult.rows[0] || null,
    viewerMetrics,
  };
};

app.get("/api/public/watch/:streamKey", async (req, res) => {
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
        [id],
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
| LIVE CHAT
|--------------------------------------------------------------------------
*/

const ensureLiveChatTable = async () => {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS live_chat_messages (
      id SERIAL PRIMARY KEY,
      stream_key VARCHAR(255) NOT NULL,
      display_name VARCHAR(120) NOT NULL,
      message TEXT NOT NULL,
      is_pinned BOOLEAN DEFAULT FALSE,
      is_hidden BOOLEAN DEFAULT FALSE,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);
};

const cleanChatText = (value, maxLength = 500) => {
  return String(value || "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, maxLength);
};

app.get("/api/public/chat/:streamKey", async (req, res) => {
  try {
    const { streamKey } = req.params;

    const result = await pool.query(
      `
      SELECT id, stream_key, display_name, message, is_pinned, created_at
      FROM live_chat_messages
      WHERE stream_key = $1
        AND is_hidden = FALSE
      ORDER BY is_pinned DESC, created_at ASC
      LIMIT 100
      `,
      [streamKey],
    );

    res.json({
      ok: true,
      messages: result.rows,
    });
  } catch (error) {
    console.error("Get public chat error:", error);

    res.status(500).json({
      ok: false,
      message: "Failed to load chat messages",
      error: error.message,
    });
  }
});

app.get(
  "/api/chat/admin/streams",
  authenticateAdmin,
  resolveOrganizationForRequest,
  requireRole("super_admin", "admin", "operator"),
  async (req, res) => {
    try {
      const result = await pool.query(
        `
        SELECT
          stream_key,
          COUNT(*)::int AS total_messages,
          COUNT(*) FILTER (WHERE is_hidden = FALSE)::int AS visible_messages,
          COUNT(*) FILTER (WHERE is_pinned = TRUE AND is_hidden = FALSE)::int AS pinned_messages,
          MAX(created_at) AS last_message_at
        FROM live_chat_messages
        WHERE organization_id = $1
        GROUP BY stream_key
        ORDER BY last_message_at DESC NULLS LAST
        LIMIT 50
        `,
        [req.organization.id],
      );

      res.json({
        ok: true,
        streams: result.rows,
      });
    } catch (error) {
      console.error("Get chat streams error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to load chat streams",
        error: error.message,
      });
    }
  },
);

app.get(
  "/api/chat/admin/messages",
  authenticateAdmin,
  resolveOrganizationForRequest,
  requireRole("super_admin", "admin", "operator"),
  async (req, res) => {
    try {
      const streamKey = cleanChatText(req.query.stream_key, 255);
      const includeHidden = String(req.query.include_hidden || "") === "true";

      if (!streamKey) {
        return res.status(400).json({
          ok: false,
          message: "Stream key is required",
        });
      }

      const result = await pool.query(
        `
        SELECT
          id,
          stream_key,
          display_name,
          message,
          is_pinned,
          is_hidden,
          created_at
        FROM live_chat_messages
        WHERE stream_key = $1
          AND organization_id = $2
          AND ($3::boolean = TRUE OR is_hidden = FALSE)
        ORDER BY is_pinned DESC, created_at DESC
        LIMIT 200
        `,
        [streamKey, req.organization.id, includeHidden],
      );

      res.json({
        ok: true,
        messages: result.rows,
      });
    } catch (error) {
      console.error("Get admin chat messages error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to load chat messages",
        error: error.message,
      });
    }
  },
);

app.patch(
  "/api/chat/messages/:id/restore",
  authenticateAdmin,
  requireRole("super_admin", "admin"),
  async (req, res) => {
    try {
      const { id } = req.params;

      const result = await pool.query(
        `
        UPDATE live_chat_messages
        SET is_hidden = FALSE
        WHERE id = $1
        RETURNING id, stream_key, display_name, message, is_pinned, is_hidden, created_at
        `,
        [id],
      );

      if (!result.rows[0]) {
        return res.status(404).json({
          ok: false,
          message: "Chat message not found",
        });
      }

      io.to(`stream:${result.rows[0].stream_key}`).emit(
        "chat:new",
        result.rows[0],
      );

      res.json({
        ok: true,
        message: result.rows[0],
      });
    } catch (error) {
      console.error("Restore chat message error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to restore chat message",
        error: error.message,
      });
    }
  },
);

app.post(
  "/api/chat/:streamKey",
  authenticateAdmin,
  resolveOrganizationForRequest,
  async (req, res) => {
    try {
      const { streamKey } = req.params;
      const displayName = cleanChatText(
        req.body.display_name || req.admin.email,
        120,
      );
      const message = cleanChatText(req.body.message, 500);

      if (!message) {
        return res.status(400).json({
          ok: false,
          message: "Message is required",
        });
      }

      const result = await pool.query(
        `
      INSERT INTO live_chat_messages (organization_id, stream_key, display_name, message)
      VALUES ($1, $2, $3, $4)
      RETURNING id, stream_key, display_name, is_hidden, message, is_pinned, created_at
      `,
        [req.organization.id, streamKey, displayName, message],
      );

      io.to(`stream:${streamKey}`).emit("chat:new", result.rows[0]);

      res.json({
        ok: true,
        message: result.rows[0],
      });
    } catch (error) {
      console.error("Create admin chat message error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to send chat message",
        error: error.message,
      });
    }
  },
);

app.patch(
  "/api/chat/messages/:id/pin",
  authenticateAdmin,
  requireRole("super_admin", "admin"),
  async (req, res) => {
    try {
      const { id } = req.params;
      const { is_pinned } = req.body;

      const result = await pool.query(
        `
        UPDATE live_chat_messages
        SET is_pinned = $1
        WHERE id = $2
        RETURNING id, stream_key, display_name, message, is_pinned, created_at
        `,
        [Boolean(is_pinned), id],
      );

      if (!result.rows[0]) {
        return res.status(404).json({
          ok: false,
          message: "Chat message not found",
        });
      }

      io.to(`stream:${result.rows[0].stream_key}`).emit(
        "chat:updated",
        result.rows[0],
      );

      res.json({
        ok: true,
        message: result.rows[0],
      });
    } catch (error) {
      console.error("Pin chat message error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to update chat message",
        error: error.message,
      });
    }
  },
);

app.delete(
  "/api/chat/messages/:id",
  authenticateAdmin,
  requireRole("super_admin", "admin"),
  async (req, res) => {
    try {
      const { id } = req.params;

      const result = await pool.query(
        `
        UPDATE live_chat_messages
        SET is_hidden = TRUE
        WHERE id = $1
        RETURNING id, stream_key
        `,
        [id],
      );

      if (!result.rows[0]) {
        return res.status(404).json({
          ok: false,
          message: "Chat message not found",
        });
      }

      io.to(`stream:${result.rows[0].stream_key}`).emit("chat:deleted", {
        id: Number(id),
      });

      res.json({
        ok: true,
        message: "Chat message removed",
      });
    } catch (error) {
      console.error("Delete chat message error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to remove chat message",
        error: error.message,
      });
    }
  },
);

/*
|--------------------------------------------------------------------------
| PRAYER REQUESTS
|--------------------------------------------------------------------------
*/

const ensurePrayerRequestsTable = async () => {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS prayer_requests (
      id SERIAL PRIMARY KEY,
      stream_key VARCHAR(255) NOT NULL,
      display_name VARCHAR(120),
      request_text TEXT NOT NULL,
      is_anonymous BOOLEAN DEFAULT FALSE,
      status VARCHAR(40) DEFAULT 'new',
      prayed_at TIMESTAMPTZ NULL,
      reviewed_by INTEGER NULL,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);
};

const cleanPrayerText = (value, maxLength = 1200) => {
  return String(value || "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, maxLength);
};

app.post("/api/public/prayer-requests/:streamKey", async (req, res) => {
  try {
    const { streamKey } = req.params;
    const cleanStreamKey = cleanPrayerText(streamKey, 255);
    const displayName = cleanPrayerText(req.body.display_name, 120);
    const requestText = cleanPrayerText(req.body.request_text, 1200);
    const isAnonymous = Boolean(req.body.is_anonymous);

    if (!cleanStreamKey || !requestText) {
      return res.status(400).json({
        ok: false,
        message: "Prayer request is required",
      });
    }

    const organizationId = await getOrganizationIdForStreamKey(cleanStreamKey);

    const result = await pool.query(
      `
      INSERT INTO prayer_requests (
        organization_id,
        stream_key,
        display_name,
        request_text,
        is_anonymous
      )
      VALUES ($1, $2, $3, $4, $5)
      RETURNING id, organization_id, stream_key, display_name, request_text, is_anonymous, status, created_at
      `,
      [
        organizationId,
        cleanStreamKey,
        isAnonymous ? null : displayName || "Guest",
        requestText,
        isAnonymous,
      ],
    );

    io.to("admins:prayer-requests").emit("prayer:new", result.rows[0]);

    res.json({
      ok: true,
      prayerRequest: result.rows[0],
      message: "Prayer request submitted successfully",
    });
  } catch (error) {
    console.error("Create prayer request error:", error);

    res.status(500).json({
      ok: false,
      message: "Failed to submit prayer request",
      error: error.message,
    });
  }
});

app.get(
  "/api/prayer-requests",
  authenticateAdmin,
  resolveOrganizationForRequest,
  requireRole("super_admin", "admin", "operator"),
  async (req, res) => {
    try {
      const { status, stream_key } = req.query;

      const values = [req.organization.id];
      const filters = ["pr.organization_id = $1"];

      if (status && status !== "all") {
        values.push(status);
        filters.push(`status = $${values.length}`);
      }

      if (stream_key) {
        values.push(stream_key);
        filters.push(`stream_key = $${values.length}`);
      }

      const whereClause = filters.length
        ? `WHERE ${filters.join(" AND ")}`
        : "";

      const result = await pool.query(
        `
        SELECT
          pr.*,
          a.name AS reviewed_by_name
        FROM prayer_requests pr
        LEFT JOIN admins a ON a.id = pr.reviewed_by
        ${whereClause}
        ORDER BY
          CASE pr.status
            WHEN 'new' THEN 1
            WHEN 'reviewed' THEN 2
            WHEN 'prayed' THEN 3
            ELSE 4
          END,
          pr.created_at DESC
        LIMIT 300
        `,
        values,
      );

      res.json({
        ok: true,
        prayerRequests: result.rows,
      });
    } catch (error) {
      console.error("Get prayer requests error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to load prayer requests",
        error: error.message,
      });
    }
  },
);

app.patch(
  "/api/prayer-requests/:id",
  authenticateAdmin,
  resolveOrganizationForRequest,
  requireRole("super_admin", "admin", "operator"),
  async (req, res) => {
    try {
      const { id } = req.params;
      const allowedStatuses = ["new", "reviewed", "prayed", "archived"];
      const status = allowedStatuses.includes(req.body.status)
        ? req.body.status
        : null;

      if (!status) {
        return res.status(400).json({
          ok: false,
          message: "Invalid prayer request status",
        });
      }

      const result = await pool.query(
        `
        UPDATE prayer_requests
        SET status = $1,
            reviewed_by = $2,
            prayed_at = CASE WHEN $1 = 'prayed' THEN NOW() ELSE prayed_at END,
            updated_at = NOW()
        WHERE id = $3
          AND organization_id = $4
        RETURNING *
        `,
        [status, req.admin.id, id, req.organization.id],
      );

      if (!result.rows[0]) {
        return res.status(404).json({
          ok: false,
          message: "Prayer request not found",
        });
      }

      res.json({
        ok: true,
        prayerRequest: result.rows[0],
      });
    } catch (error) {
      console.error("Update prayer request error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to update prayer request",
        error: error.message,
      });
    }
  },
);

app.delete(
  "/api/prayer-requests/:id",
  authenticateAdmin,
  resolveOrganizationForRequest,
  requireRole("super_admin"),
  async (req, res) => {
    try {
      const { id } = req.params;

      await pool.query(
        `
        DELETE FROM prayer_requests
        WHERE id = $1
          AND organization_id = $2
        `,
        [id, req.organization.id],
      );

      res.json({
        ok: true,
        message: "Prayer request deleted successfully",
      });
    } catch (error) {
      console.error("Delete prayer request error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to delete prayer request",
        error: error.message,
      });
    }
  },
);

/*
|--------------------------------------------------------------------------
| LIVE OVERLAYS / LOWER THIRDS
|--------------------------------------------------------------------------
*/

const ensureOverlayTables = async () => {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS overlay_states (
      stream_key VARCHAR(255) PRIMARY KEY,
      overlay_json JSONB NOT NULL DEFAULT '{}'::jsonb,
      is_visible BOOLEAN DEFAULT FALSE,
      updated_by INTEGER NULL,
      updated_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS overlay_history (
      id SERIAL PRIMARY KEY,
      stream_key VARCHAR(255) NOT NULL,
      overlay_json JSONB NOT NULL DEFAULT '{}'::jsonb,
      action VARCHAR(40) NOT NULL DEFAULT 'show',
      created_by INTEGER NULL,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);
};

const cleanOverlayText = (value, maxLength = 500) => {
  return String(value || "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, maxLength);
};

const buildOverlayPayload = (body = {}) => {
  const type = [
    "lower_third",
    "bible_verse",
    "announcement",
    "speaker",
  ].includes(body.type)
    ? body.type
    : "lower_third";

  return {
    type,
    title: cleanOverlayText(body.title, 160),
    subtitle: cleanOverlayText(body.subtitle, 220),
    body: cleanOverlayText(body.body, 700),
    reference: cleanOverlayText(body.reference, 160),
    position: [
      "bottom_left",
      "bottom_center",
      "bottom_right",
      "center",
    ].includes(body.position)
      ? body.position
      : "bottom_left",
    duration_seconds: Math.max(
      0,
      Math.min(Number(body.duration_seconds || 0), 120),
    ),
    updated_at: new Date().toISOString(),
  };
};

const getOverlayState = async (streamKey, organizationId = null) => {
  const resolvedOrganizationId =
    organizationId || (await getOrganizationIdForStreamKey(streamKey));

  const result = await pool.query(
    `
    SELECT stream_key, overlay_json, is_visible, updated_at, organization_id
    FROM overlay_states
    WHERE stream_key = $1
      AND organization_id = $2
    `,
    [streamKey, resolvedOrganizationId],
  );

  if (!result.rows[0]) {
    return {
      stream_key: streamKey,
      organization_id: resolvedOrganizationId,
      overlay: null,
      is_visible: false,
      updated_at: null,
    };
  }

  return {
    stream_key: result.rows[0].stream_key,
    organization_id: result.rows[0].organization_id,
    overlay: result.rows[0].overlay_json,
    is_visible: result.rows[0].is_visible,
    updated_at: result.rows[0].updated_at,
  };
};

app.get("/api/public/overlays/:streamKey/current", async (req, res) => {
  try {
    const streamKey = cleanOverlayText(req.params.streamKey, 255);
    const state = await getOverlayState(streamKey);

    res.json({
      ok: true,
      ...state,
    });
  } catch (error) {
    console.error("Get public overlay state error:", error);

    res.status(500).json({
      ok: false,
      message: "Failed to load overlay state",
      error: error.message,
    });
  }
});

app.get(
  "/api/overlays/:streamKey/current",
  authenticateAdmin,
  resolveOrganizationForRequest,
  async (req, res) => {
    try {
      const streamKey = cleanOverlayText(req.params.streamKey, 255);
      const state = await getOverlayState(streamKey, req.organization.id);

      res.json({
        ok: true,
        ...state,
      });
    } catch (error) {
      console.error("Get overlay state error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to load overlay state",
        error: error.message,
      });
    }
  },
);

app.get(
  "/api/overlays/:streamKey/history",
  authenticateAdmin,
  resolveOrganizationForRequest,
  async (req, res) => {
    try {
      const streamKey = cleanOverlayText(req.params.streamKey, 255);

      const result = await pool.query(
        `
      SELECT id, stream_key, overlay_json, action, created_at
      FROM overlay_history
      WHERE stream_key = $1
        AND organization_id = $2
      ORDER BY created_at DESC
      LIMIT 30
      `,
        [streamKey, req.organization.id],
      );

      res.json({
        ok: true,
        history: result.rows,
      });
    } catch (error) {
      console.error("Get overlay history error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to load overlay history",
        error: error.message,
      });
    }
  },
);

app.post(
  "/api/overlays/:streamKey/show",
  authenticateAdmin,
  resolveOrganizationForRequest,
  requireRole("super_admin", "admin", "operator"),
  async (req, res) => {
    try {
      const streamKey = cleanOverlayText(req.params.streamKey, 255);
      const overlay = buildOverlayPayload(req.body);

      if (!streamKey) {
        return res.status(400).json({
          ok: false,
          message: "Stream key is required",
        });
      }

      if (
        !overlay.title &&
        !overlay.subtitle &&
        !overlay.body &&
        !overlay.reference
      ) {
        return res.status(400).json({
          ok: false,
          message: "Overlay content is required",
        });
      }

      const result = await pool.query(
        `
        INSERT INTO overlay_states (stream_key, organization_id, overlay_json, is_visible, updated_by, updated_at)
        VALUES ($1, $2, $3::jsonb, TRUE, $4, NOW())
        ON CONFLICT (stream_key)
        DO UPDATE SET
          organization_id = EXCLUDED.organization_id,
          overlay_json = EXCLUDED.overlay_json,
          is_visible = TRUE,
          updated_by = EXCLUDED.updated_by,
          updated_at = NOW()
        RETURNING stream_key, organization_id, overlay_json, is_visible, updated_at
        `,
        [streamKey, req.organization.id, JSON.stringify(overlay), req.admin.id],
      );

      await pool.query(
        `
        INSERT INTO overlay_history (stream_key, organization_id, overlay_json, action, created_by)
        VALUES ($1, $2, $3::jsonb, 'show', $4)
        `,
        [streamKey, req.organization.id, JSON.stringify(overlay), req.admin.id],
      );

      const state = {
        stream_key: result.rows[0].stream_key,
        organization_id: result.rows[0].organization_id,
        overlay: result.rows[0].overlay_json,
        is_visible: result.rows[0].is_visible,
        updated_at: result.rows[0].updated_at,
      };

      io.to(`overlay:${streamKey}`).emit("overlay:show", state);
      io.to(`overlay-admin:${streamKey}`).emit("overlay:state", state);

      if (overlay.duration_seconds > 0) {
        setTimeout(async () => {
          try {
            const current = await getOverlayState(
              streamKey,
              req.organization.id,
            );
            if (
              current.is_visible &&
              current.overlay?.updated_at === overlay.updated_at
            ) {
              await pool.query(
                `
                UPDATE overlay_states
                SET is_visible = FALSE,
                    updated_at = NOW()
                WHERE stream_key = $1
                  AND organization_id = $2
                `,
                [streamKey, req.organization.id],
              );

              const hiddenState = await getOverlayState(
                streamKey,
                req.organization.id,
              );
              io.to(`overlay:${streamKey}`).emit("overlay:hide", hiddenState);
              io.to(`overlay-admin:${streamKey}`).emit(
                "overlay:state",
                hiddenState,
              );
            }
          } catch (timerError) {
            console.error("Overlay auto-hide error:", timerError);
          }
        }, overlay.duration_seconds * 1000);
      }

      res.json({
        ok: true,
        ...state,
      });
    } catch (error) {
      console.error("Show overlay error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to show overlay",
        error: error.message,
      });
    }
  },
);

app.post(
  "/api/overlays/:streamKey/hide",
  authenticateAdmin,
  resolveOrganizationForRequest,
  requireRole("super_admin", "admin", "operator"),
  async (req, res) => {
    try {
      const streamKey = cleanOverlayText(req.params.streamKey, 255);

      const result = await pool.query(
        `
        INSERT INTO overlay_states (stream_key, organization_id, overlay_json, is_visible, updated_by, updated_at)
        VALUES ($1, $2, '{}'::jsonb, FALSE, $3, NOW())
        ON CONFLICT (stream_key)
        DO UPDATE SET
          organization_id = EXCLUDED.organization_id,
          is_visible = FALSE,
          updated_by = EXCLUDED.updated_by,
          updated_at = NOW()
        RETURNING stream_key, organization_id, overlay_json, is_visible, updated_at
        `,
        [streamKey, req.organization.id, req.admin.id],
      );

      await pool.query(
        `
        INSERT INTO overlay_history (stream_key, organization_id, overlay_json, action, created_by)
        VALUES ($1, $2, '{}'::jsonb, 'hide', $3)
        `,
        [streamKey, req.organization.id, req.admin.id],
      );

      const state = {
        stream_key: result.rows[0].stream_key,
        organization_id: result.rows[0].organization_id,
        overlay: result.rows[0].overlay_json,
        is_visible: result.rows[0].is_visible,
        updated_at: result.rows[0].updated_at,
      };

      io.to(`overlay:${streamKey}`).emit("overlay:hide", state);
      io.to(`overlay-admin:${streamKey}`).emit("overlay:state", state);

      res.json({
        ok: true,
        ...state,
      });
    } catch (error) {
      console.error("Hide overlay error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to hide overlay",
        error: error.message,
      });
    }
  },
);

/*
|--------------------------------------------------------------------------
| DONATION LINKS / CALL TO ACTIONS
|--------------------------------------------------------------------------
*/

const CTA_TYPES = ["donation", "website", "newsletter", "custom"];
const CTA_STATUSES = ["active", "inactive"];

const ensureCtaLinksTable = async () => {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS cta_links (
      id SERIAL PRIMARY KEY,
      label VARCHAR(120) NOT NULL,
      title VARCHAR(180) NOT NULL,
      description TEXT,
      url TEXT NOT NULL,
      button_text VARCHAR(80) DEFAULT 'Open Link',
      type VARCHAR(40) DEFAULT 'donation',
      stream_key VARCHAR(255) NULL,
      is_active BOOLEAN DEFAULT TRUE,
      sort_order INTEGER DEFAULT 0,
      created_by INTEGER NULL,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);
};

const cleanCtaText = (value, maxLength = 500) => {
  return String(value || "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, maxLength);
};

const normalizeCtaPayload = (body = {}) => {
  const type = CTA_TYPES.includes(body.type) ? body.type : "donation";
  const isActive =
    typeof body.is_active === "boolean"
      ? body.is_active
      : body.status !== "inactive";

  return {
    label: cleanCtaText(body.label, 120),
    title: cleanCtaText(body.title, 180),
    description: cleanCtaText(body.description, 700),
    url: cleanCtaText(body.url, 1000),
    button_text: cleanCtaText(body.button_text || "Open Link", 80),
    type,
    stream_key: cleanCtaText(body.stream_key, 255) || null,
    is_active: isActive,
    sort_order: Number.isFinite(Number(body.sort_order))
      ? Number(body.sort_order)
      : 0,
  };
};

const isValidCtaUrl = (value) => {
  try {
    const parsed = new URL(value);
    return ["http:", "https:"].includes(parsed.protocol);
  } catch {
    return false;
  }
};

app.get("/api/public/cta-links", async (req, res) => {
  try {
    const streamKey = cleanCtaText(req.query.stream_key, 255);

    const organizationId = await getOrganizationIdForStreamKey(streamKey);
    const params = [organizationId];
    let whereClause = "WHERE is_active = TRUE AND organization_id = $1";

    if (streamKey) {
      params.push(streamKey);
      whereClause += ` AND (stream_key IS NULL OR stream_key = $2)`;
    }

    const result = await pool.query(
      `
      SELECT id, label, title, description, url, button_text, type, stream_key, sort_order
      FROM cta_links
      ${whereClause}
      ORDER BY sort_order ASC, created_at DESC
      LIMIT 12
      `,
      params,
    );

    res.json({
      ok: true,
      links: result.rows,
    });
  } catch (error) {
    console.error("Get public CTA links error:", error);

    res.status(500).json({
      ok: false,
      message: "Failed to load call-to-action links",
      error: error.message,
    });
  }
});

app.get(
  "/api/cta-links",
  authenticateAdmin,
  resolveOrganizationForRequest,
  async (req, res) => {
    try {
      const result = await pool.query(
        `
      SELECT *
      FROM cta_links
      WHERE organization_id = $1
      ORDER BY sort_order ASC, created_at DESC
      `,
        [req.organization.id],
      );

      res.json({
        ok: true,
        links: result.rows,
      });
    } catch (error) {
      console.error("Get CTA links error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to fetch call-to-action links",
        error: error.message,
      });
    }
  },
);

app.post(
  "/api/cta-links",
  authenticateAdmin,
  resolveOrganizationForRequest,
  requireRole("super_admin", "admin", "operator"),
  async (req, res) => {
    try {
      const payload = normalizeCtaPayload(req.body);

      if (!payload.label || !payload.title || !payload.url) {
        return res.status(400).json({
          ok: false,
          message: "Label, title, and URL are required",
        });
      }

      if (!isValidCtaUrl(payload.url)) {
        return res.status(400).json({
          ok: false,
          message: "Please enter a valid http or https URL",
        });
      }

      const result = await pool.query(
        `
        INSERT INTO cta_links (
          organization_id,
          label,
          title,
          description,
          url,
          button_text,
          type,
          stream_key,
          is_active,
          sort_order,
          created_by
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        RETURNING *
        `,
        [
          req.organization.id,
          payload.label,
          payload.title,
          payload.description || null,
          payload.url,
          payload.button_text,
          payload.type,
          payload.stream_key,
          payload.is_active,
          payload.sort_order,
          req.admin.id,
        ],
      );

      res.json({
        ok: true,
        link: result.rows[0],
      });
    } catch (error) {
      console.error("Create CTA link error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to create call-to-action link",
        error: error.message,
      });
    }
  },
);

app.put(
  "/api/cta-links/:id",
  authenticateAdmin,
  resolveOrganizationForRequest,
  requireRole("super_admin", "admin", "operator"),
  async (req, res) => {
    try {
      const { id } = req.params;
      const payload = normalizeCtaPayload(req.body);

      if (!payload.label || !payload.title || !payload.url) {
        return res.status(400).json({
          ok: false,
          message: "Label, title, and URL are required",
        });
      }

      if (!isValidCtaUrl(payload.url)) {
        return res.status(400).json({
          ok: false,
          message: "Please enter a valid http or https URL",
        });
      }

      const result = await pool.query(
        `
        UPDATE cta_links
        SET label = $1,
            title = $2,
            description = $3,
            url = $4,
            button_text = $5,
            type = $6,
            stream_key = $7,
            is_active = $8,
            sort_order = $9,
            updated_at = NOW()
        WHERE id = $10
          AND organization_id = $11
        RETURNING *
        `,
        [
          payload.label,
          payload.title,
          payload.description || null,
          payload.url,
          payload.button_text,
          payload.type,
          payload.stream_key,
          payload.is_active,
          payload.sort_order,
          id,
          req.organization.id,
        ],
      );

      if (!result.rows[0]) {
        return res.status(404).json({
          ok: false,
          message: "Call-to-action link not found",
        });
      }

      res.json({
        ok: true,
        link: result.rows[0],
      });
    } catch (error) {
      console.error("Update CTA link error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to update call-to-action link",
        error: error.message,
      });
    }
  },
);

app.delete(
  "/api/cta-links/:id",
  authenticateAdmin,
  resolveOrganizationForRequest,
  requireRole("super_admin", "admin", "operator"),
  async (req, res) => {
    try {
      const { id } = req.params;

      await pool.query(
        `
        DELETE FROM cta_links
        WHERE id = $1
          AND organization_id = $2
        `,
        [id, req.organization.id],
      );

      res.json({
        ok: true,
        message: "Call-to-action link deleted successfully",
      });
    } catch (error) {
      console.error("Delete CTA link error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to delete call-to-action link",
        error: error.message,
      });
    }
  },
);

app.post(
  "/api/cta-links/:id/trigger-overlay",
  authenticateAdmin,
  resolveOrganizationForRequest,
  requireRole("super_admin", "admin", "operator"),
  async (req, res) => {
    try {
      const { id } = req.params;
      const streamKey = cleanCtaText(req.body.stream_key, 255);

      if (!streamKey) {
        return res.status(400).json({
          ok: false,
          message: "Stream key is required to show CTA overlay",
        });
      }

      const linkResult = await pool.query(
        `
        SELECT *
        FROM cta_links
        WHERE id = $1
          AND organization_id = $2
        `,
        [id, req.organization.id],
      );

      const link = linkResult.rows[0];

      if (!link) {
        return res.status(404).json({
          ok: false,
          message: "Call-to-action link not found",
        });
      }

      const overlay = {
        type: "announcement",
        title: cleanOverlayText(link.title, 160),
        subtitle: cleanOverlayText(link.button_text || link.label, 220),
        body: cleanOverlayText(link.url, 700),
        reference: cleanOverlayText(link.type, 160),
        position: "bottom_right",
        duration_seconds: Math.max(
          0,
          Math.min(Number(req.body.duration_seconds || 12), 120),
        ),
        updated_at: new Date().toISOString(),
      };

      const result = await pool.query(
        `
        INSERT INTO overlay_states (stream_key, overlay_json, is_visible, updated_by, updated_at)
        VALUES ($1, $2::jsonb, TRUE, $3, NOW())
        ON CONFLICT (stream_key)
        DO UPDATE SET
          overlay_json = EXCLUDED.overlay_json,
          is_visible = TRUE,
          updated_by = EXCLUDED.updated_by,
          updated_at = NOW()
        RETURNING stream_key, overlay_json, is_visible, updated_at
        `,
        [streamKey, JSON.stringify(overlay), req.admin.id],
      );

      await pool.query(
        `
        INSERT INTO overlay_history (stream_key, overlay_json, action, created_by)
        VALUES ($1, $2::jsonb, 'show', $3)
        `,
        [streamKey, JSON.stringify(overlay), req.admin.id],
      );

      const state = {
        stream_key: result.rows[0].stream_key,
        overlay: result.rows[0].overlay_json,
        is_visible: result.rows[0].is_visible,
        updated_at: result.rows[0].updated_at,
      };

      io.to(`overlay:${streamKey}`).emit("overlay:show", state);
      io.to(`overlay-admin:${streamKey}`).emit("overlay:state", state);

      if (overlay.duration_seconds > 0) {
        setTimeout(async () => {
          try {
            const current = await getOverlayState(streamKey);
            if (
              current.is_visible &&
              current.overlay?.updated_at === overlay.updated_at
            ) {
              await pool.query(
                `
                UPDATE overlay_states
                SET is_visible = FALSE,
                    updated_at = NOW()
                WHERE stream_key = $1
                `,
                [streamKey],
              );

              const hiddenState = await getOverlayState(streamKey);
              io.to(`overlay:${streamKey}`).emit("overlay:hide", hiddenState);
              io.to(`overlay-admin:${streamKey}`).emit(
                "overlay:state",
                hiddenState,
              );
            }
          } catch (timerError) {
            console.error("CTA overlay auto-hide error:", timerError);
          }
        }, overlay.duration_seconds * 1000);
      }

      res.json({
        ok: true,
        ...state,
      });
    } catch (error) {
      console.error("Trigger CTA overlay error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to trigger CTA overlay",
        error: error.message,
      });
    }
  },
);

/*
|--------------------------------------------------------------------------
| VIEWER REACTIONS
|--------------------------------------------------------------------------
*/

const REACTION_TYPES = ["amen", "praise", "pray", "love"];

const normalizeReactionType = (type) => {
  const cleanType = String(type || "")
    .trim()
    .toLowerCase();
  return REACTION_TYPES.includes(cleanType) ? cleanType : null;
};

const ensureViewerReactionsTable = async () => {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS viewer_reactions (
      id SERIAL PRIMARY KEY,
      stream_key VARCHAR(255) NOT NULL,
      reaction_type VARCHAR(40) NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_viewer_reactions_stream_key
    ON viewer_reactions(stream_key)
  `);
};

const getReactionSummary = async (streamKey, organizationId = null) => {
  const resolvedOrganizationId =
    organizationId || (await getOrganizationIdForStreamKey(streamKey));

  const result = await pool.query(
    `
    SELECT reaction_type, COUNT(*)::int AS count
    FROM viewer_reactions
    WHERE stream_key = $1
      AND organization_id = $2
    GROUP BY reaction_type
    `,
    [streamKey, resolvedOrganizationId],
  );

  const counts = REACTION_TYPES.reduce((acc, type) => {
    acc[type] = 0;
    return acc;
  }, {});

  result.rows.forEach((row) => {
    counts[row.reaction_type] = Number(row.count || 0);
  });

  const total = Object.values(counts).reduce((sum, count) => sum + count, 0);

  return {
    stream_key: streamKey,
    organization_id: resolvedOrganizationId,
    counts,
    total,
  };
};

app.get("/api/public/reactions/:streamKey", async (req, res) => {
  try {
    const { streamKey } = req.params;
    const summary = await getReactionSummary(streamKey);

    res.json({
      ok: true,
      summary,
    });
  } catch (error) {
    console.error("Get public reactions error:", error);

    res.status(500).json({
      ok: false,
      message: "Failed to load reactions",
      error: error.message,
    });
  }
});

app.post("/api/public/reactions/:streamKey", async (req, res) => {
  try {
    const { streamKey } = req.params;
    const reactionType = normalizeReactionType(req.body.reaction_type);

    if (!reactionType) {
      return res.status(400).json({
        ok: false,
        message: "Invalid reaction type",
      });
    }

    const organizationId = await getOrganizationIdForStreamKey(streamKey);

    const result = await pool.query(
      `
      INSERT INTO viewer_reactions (organization_id, stream_key, reaction_type)
      VALUES ($1, $2, $3)
      RETURNING id, organization_id, stream_key, reaction_type, created_at
      `,
      [organizationId, streamKey, reactionType],
    );

    const summary = await getReactionSummary(streamKey, organizationId);

    io.to(`reactions:${streamKey}`).emit("reaction:new", {
      reaction: result.rows[0],
      summary,
    });

    io.to("admins:reactions").emit("reaction:summary", summary);

    res.json({
      ok: true,
      reaction: result.rows[0],
      summary,
    });
  } catch (error) {
    console.error("Create public reaction error:", error);

    res.status(500).json({
      ok: false,
      message: "Failed to send reaction",
      error: error.message,
    });
  }
});

app.get(
  "/api/reactions/streams",
  authenticateAdmin,
  resolveOrganizationForRequest,
  async (req, res) => {
    try {
      const result = await pool.query(
        `
      SELECT stream_key,
             COUNT(*)::int AS total,
             MAX(created_at) AS last_reaction_at
      FROM viewer_reactions
      WHERE organization_id = $1
      GROUP BY stream_key
      ORDER BY last_reaction_at DESC NULLS LAST, stream_key ASC
      `,
        [req.organization.id],
      );

      res.json({
        ok: true,
        streams: result.rows,
      });
    } catch (error) {
      console.error("Get reaction streams error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to load reaction streams",
        error: error.message,
      });
    }
  },
);

app.get(
  "/api/reactions/:streamKey",
  authenticateAdmin,
  resolveOrganizationForRequest,
  async (req, res) => {
    try {
      const { streamKey } = req.params;
      const summary = await getReactionSummary(streamKey, req.organization.id);

      res.json({
        ok: true,
        summary,
      });
    } catch (error) {
      console.error("Get reaction summary error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to load reaction summary",
        error: error.message,
      });
    }
  },
);

app.delete(
  "/api/reactions/:streamKey/reset",
  authenticateAdmin,
  resolveOrganizationForRequest,
  requireRole("super_admin", "admin"),
  async (req, res) => {
    try {
      const { streamKey } = req.params;

      await pool.query(
        `
        DELETE FROM viewer_reactions
        WHERE stream_key = $1
          AND organization_id = $2
        `,
        [streamKey, req.organization.id],
      );

      const summary = await getReactionSummary(streamKey, req.organization.id);

      io.to(`reactions:${streamKey}`).emit("reaction:summary", summary);
      io.to("admins:reactions").emit("reaction:summary", summary);

      res.json({
        ok: true,
        message: "Reaction counts reset",
        summary,
      });
    } catch (error) {
      console.error("Reset reactions error:", error);

      res.status(500).json({
        ok: false,
        message: "Failed to reset reaction counts",
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
        "SELECT stream_key FROM channels WHERE organization_id = $1",
        [req.organization.id],
      );

      const allowedStreamKeys = new Set(
        allowedResult.rows.map((row) => String(row.stream_key)),
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

          return {
            ...stream,
            srs_clients: Number(stream.clients || 0),
            clients: viewerMetrics.active_viewers,
            viewerMetrics,
          };
        }),
      );

      res.json({
        ok: true,
        srs_available: true,
        streams,
      });
    } catch (error) {
      // SRS not reachable (expected when SRS is local, backend is cloud)
      // Fall back to DB is_live flag set by on_publish webhook
      try {
        const liveResult = await pool.query(
          `SELECT c.stream_key, c.name, c.is_live, c.live_started_at,
                  EXTRACT(EPOCH FROM (NOW() - c.live_started_at))::int AS uptime_seconds
           FROM channels c
           WHERE c.organization_id = $1 AND c.is_live = TRUE`,
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
        }));
        return res.json({
          ok: true,
          srs_available: false,
          streams: dbStreams,
          message:
            "Stream status from webhook DB (SRS not directly reachable).",
        });
      } catch (dbErr) {
        console.warn("SRS unavailable and DB fallback failed:", dbErr.message);
      }
      res.json({
        ok: true,
        srs_available: false,
        streams: [],
        message:
          "SRS server is not reachable from this backend environment yet.",
      });
    }
  },
);

// ── Helper: get plan limits for an org ────────────────────────────
async function getOrgStreamingPlan(organizationId) {
  try {
    const result = await pool.query(
      `
      SELECT
        sub.plan_key,
        sp.transcoding_enabled,
        sp.max_channels,
        COALESCE(sp.max_concurrent_streams, 999) AS max_concurrent_streams
      FROM subscriptions sub
      JOIN subscription_plans sp ON sp.plan_key = sub.plan_key
      WHERE sub.organization_id = $1
        AND sub.status IN ('active', 'trialing')
      ORDER BY sub.created_at DESC
      LIMIT 1
      `,
      [organizationId],
    );
    return (
      result.rows[0] || {
        transcoding_enabled: false,
        max_concurrent_streams: 1,
      }
    );
  } catch {
    return { transcoding_enabled: false, max_concurrent_streams: 1 };
  }
}

// ── Helper: count currently live streams for an org ───────────────
async function getActiveLiveCount(organizationId) {
  const result = await pool.query(
    `SELECT COUNT(*)::int AS count FROM channels
     WHERE organization_id = $1 AND is_live = TRUE`,
    [organizationId],
  );
  return result.rows[0]?.count || 0;
}

// ── Helper: auto-transcode using FFmpeg ───────────────────────────
function autoTranscodeStream(streamKey) {
  const input = `rtmp://localhost/live/${streamKey}`;
  const out720 = `rtmp://localhost/live/${streamKey}_720p`;
  const out480 = `rtmp://localhost/live/${streamKey}_480p`;

  console.log(`[Transcode] Starting 720p + 480p for: ${streamKey}`);

  const cmd720 = `ffmpeg -y -i "${input}" -map 0:v:0 -map 0:a:0? -c:v libx264 -preset veryfast -b:v 2500k -s 1280x720 -c:a aac -b:a 128k -f flv "${out720}"`;
  const cmd480 = `ffmpeg -y -i "${input}" -map 0:v:0 -map 0:a:0? -c:v libx264 -preset veryfast -b:v 1200k -s 854x480 -c:a aac -b:a 96k -f flv "${out480}"`;

  exec(cmd720, (err) => {
    if (err)
      console.error(`[Transcode] 720p error for ${streamKey}:`, err.message);
  });
  exec(cmd480, (err) => {
    if (err)
      console.error(`[Transcode] 480p error for ${streamKey}:`, err.message);
  });
}

// ── Helper: auto-sync recordings after stream ends ────────────────
async function autoSyncRecordingsDelayed(organizationId, delayMs = 8000) {
  setTimeout(async () => {
    try {
      console.log(`[DVR] Auto-syncing recordings for org: ${organizationId}`);
      await scanRecordingFilesForOrganization(organizationId, {
        processReady: true,
      });
      // Notify dashboard via socket
      if (io)
        io.emit("recordings:updated", { organization_id: organizationId });
    } catch (err) {
      console.error("[DVR] Auto-sync error:", err.message);
    }
  }, delayMs);
}

// ══════════════════════════════════════════
// POST /api/srs/on_publish
// SRS fires this when a broadcaster connects
// Return code 0 = allow, code 403 = reject
// ══════════════════════════════════════════
app.post("/api/srs/on_publish", async (req, res) => {
  const streamKey = req.body?.stream || req.body?.name || "";
  console.log(`[SRS] on_publish — stream key: ${streamKey}`);

  // Skip transcoded variant streams (they re-publish to SRS too)
  if (streamKey.endsWith("_720p") || streamKey.endsWith("_480p")) {
    return res.json({ code: 0 });
  }

  try {
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
    const liveCount = await getActiveLiveCount(channel.org_id);

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

    // 4. Auto-transcode if plan allows
    if (plan.transcoding_enabled) {
      // Small delay so SRS stream is stable before FFmpeg connects
      setTimeout(() => autoTranscodeStream(streamKey), 3000);
    }

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
  console.log(`[SRS] on_unpublish — stream key: ${streamKey}`);

  if (streamKey.endsWith("_720p") || streamKey.endsWith("_480p")) {
    return res.json({ code: 0 });
  }

  try {
    // Mark channel offline
    const channelResult = await pool.query(
      `UPDATE channels SET is_live = FALSE, live_started_at = NULL
       WHERE stream_key = $1
       RETURNING id, organization_id`,
      [streamKey],
    );

    const orgId = channelResult.rows[0]?.organization_id;

    // Notify dashboard
    if (io) {
      io.emit("stream:offline", {
        stream_key: streamKey,
        organization_id: orgId,
      });
    }

    // Auto-sync recordings after stream ends (wait for SRS to write files)
    if (orgId) {
      autoSyncRecordingsDelayed(orgId, 8000);
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
      const { name, stream_key, description } = req.body;

      if (!name || !stream_key) {
        return res.status(400).json({
          ok: false,
          message: "Name and stream_key are required",
        });
      }

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
        [req.organization.id, name, stream_key, description || null],
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

const getRecordingAbsolutePath = (streamKey, fileName) => {
  const cleanStream = safeRecordingSegment(streamKey);
  const cleanFile = safeRecordingSegment(fileName);

  if (!cleanStream || !cleanFile) return null;

  const basePath = path.resolve(RECORDINGS_LIVE_ROOT);
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
    url: playable
      ? formatRecordingPlaybackUrl(streamKey, mp4File)
      : formatRecordingUrl(streamKey, file),
    download_url: formatRecordingUrl(streamKey, playable ? mp4File : file),
    source_download_url: formatRecordingUrl(streamKey, file),
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
  const recordingsPath = RECORDINGS_LIVE_ROOT;
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

  recordings.sort((a, b) => new Date(b.created) - new Date(a.created));
  return recordings;
};

const processRecordingFile = async ({ organizationId, stream, file }) => {
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

  if (!isFileStable(inputPath)) {
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

const handleRecordingPublishRequest = async (req, res) => {
  try {
    const id = Number(req.params.id || 0);
    const shouldPublish = req.body?.is_public !== false;
    const bodyStream = safeRecordingSegment(
      req.body?.stream || req.body?.stream_key || "",
    );
    const bodyFile = safeRecordingSegment(
      req.body?.mp4_file || req.body?.file || req.body?.filename || "",
    );

    let existingResult = await pool.query(
      `
      SELECT r.*, c.name AS channel_name
      FROM recordings r
      LEFT JOIN channels c ON c.id = r.channel_id
      WHERE r.id = $1
        AND r.organization_id = $2
      LIMIT 1
      `,
      [id, req.organization.id],
    );

    let existing = existingResult.rows[0];

    if (!existing && bodyStream && bodyFile) {
      existingResult = await pool.query(
        `
        SELECT r.*, c.name AS channel_name
        FROM recordings r
        LEFT JOIN channels c ON c.id = r.channel_id
        WHERE r.organization_id = $1
          AND r.stream_key = $2
          AND (
            r.mp4_filename = $3
            OR r.filename = $3
            OR r.source_filename = $3
          )
        ORDER BY r.updated_at DESC NULLS LAST, r.created_at DESC NULLS LAST
        LIMIT 1
        `,
        [req.organization.id, bodyStream, bodyFile],
      );

      existing = existingResult.rows[0];
    }

    if (!existing) {
      return res.status(404).json({
        ok: false,
        message:
          "Recording not found for this tenant. Sync Library first, then try publishing again.",
      });
    }

    if (shouldPublish && !existing.mp4_filename) {
      return res.status(400).json({
        ok: false,
        message: "Process this recording to MP4 before publishing.",
      });
    }

    const title = cleanOrgText(
      req.body?.public_title ||
        existing.public_title ||
        existing.channel_name ||
        existing.stream_key,
      255,
    );
    const description = cleanOrgText(
      req.body?.public_description || existing.public_description || "",
      2000,
    );
    const category = cleanOrgText(
      req.body?.replay_category || existing.replay_category || "",
      120,
    );
    const tags = cleanOrgText(
      Array.isArray(req.body?.replay_tags)
        ? req.body.replay_tags.join(", ")
        : req.body?.replay_tags || existing.replay_tags || "",
      500,
    );
    const requestedVisibility = cleanOrgText(
      req.body?.replay_visibility || existing.replay_visibility || "public",
      40,
    ).toLowerCase();
    const replayVisibility = [
      "public",
      "unlisted",
      "private",
      "members_only",
    ].includes(requestedVisibility)
      ? requestedVisibility
      : "public";

    let publicSlug = existing.public_slug;

    if (shouldPublish && !publicSlug) {
      publicSlug = await ensureUniqueRecordingSlug(
        `${req.organization.slug || req.organization.name}-${existing.stream_key}-${existing.id}`,
        existing.id,
      );
    }

    const updateResult = await pool.query(
      `
      UPDATE recordings
      SET is_public = $1,
          public_slug = CASE WHEN $1 = TRUE THEN $2 ELSE public_slug END,
          public_title = $3,
          public_description = $4,
          replay_category = $5,
          replay_tags = $6,
          replay_visibility = $7,
          published_at = CASE
            WHEN $1 = TRUE AND published_at IS NULL THEN NOW()
            WHEN $1 = FALSE THEN NULL
            ELSE published_at
          END,
          updated_at = NOW()
      WHERE id = $8
        AND organization_id = $9
      RETURNING *
      `,
      [
        shouldPublish,
        publicSlug,
        title,
        description,
        category,
        tags,
        replayVisibility,
        existing.id,
        req.organization.id,
      ],
    );

    const row = updateResult.rows[0];

    res.json({
      ok: true,
      recording: mapRecordingRowToDto({
        ...row,
        channel_name: existing.channel_name,
      }),
    });
  } catch (error) {
    console.error("Publish recording error:", error);
    res.status(500).json({
      ok: false,
      message: "Failed to update replay publishing",
      error: error.message,
    });
  }
};

app.put(
  "/api/recordings/:id/publish",
  authenticateAdmin,
  resolveOrganizationForRequest,
  requireOrganizationRole("owner", "admin"),
  handleRecordingPublishRequest,
);

app.post(
  "/api/recordings/:id/publish",
  authenticateAdmin,
  resolveOrganizationForRequest,
  requireOrganizationRole("owner", "admin"),
  handleRecordingPublishRequest,
);

app.put(
  "/api/recordings/:id/metadata",
  authenticateAdmin,
  resolveOrganizationForRequest,
  requireOrganizationRole("owner", "admin"),
  async (req, res) => {
    try {
      const id = Number(req.params.id || 0);
      const title = cleanOrgText(req.body?.public_title || "", 255);
      const description = cleanOrgText(
        req.body?.public_description || "",
        2000,
      );
      const category = cleanOrgText(req.body?.replay_category || "", 120);
      const tags = cleanOrgText(
        Array.isArray(req.body?.replay_tags)
          ? req.body.replay_tags.join(", ")
          : req.body?.replay_tags || "",
        500,
      );
      const existingResult = await pool.query(
        `
        SELECT r.*, c.name AS channel_name
        FROM recordings r
        LEFT JOIN channels c ON c.id = r.channel_id
        WHERE r.id = $1
          AND r.organization_id = $2
        LIMIT 1
        `,
        [id, req.organization.id],
      );

      const existing = existingResult.rows[0];

      if (!existing) {
        return res.status(404).json({
          ok: false,
          message: "Recording not found for this tenant.",
        });
      }

      const requestedVisibility = cleanOrgText(
        req.body?.replay_visibility || existing?.replay_visibility || "public",
        40,
      ).toLowerCase();
      const replayVisibility = [
        "public",
        "unlisted",
        "private",
        "members_only",
      ].includes(requestedVisibility)
        ? requestedVisibility
        : "public";

      const updateResult = await pool.query(
        `
        UPDATE recordings
        SET public_title = COALESCE(NULLIF($1, ''), public_title),
            public_description = $2,
            replay_category = $3,
            replay_tags = $4,
            replay_visibility = $5,
            updated_at = NOW()
        WHERE id = $6
          AND organization_id = $7
        RETURNING *
        `,
        [
          title,
          description,
          category,
          tags,
          replayVisibility,
          existing.id,
          req.organization.id,
        ],
      );

      res.json({
        ok: true,
        recording: mapRecordingRowToDto({
          ...updateResult.rows[0],
          channel_name: existing.channel_name,
        }),
      });
    } catch (error) {
      console.error("Update recording metadata error:", error);
      res.status(500).json({
        ok: false,
        message: "Failed to update replay metadata",
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

const mapReplayLibraryItem = (row) => ({
  id: row.id,
  slug: row.public_slug,
  title: row.public_title || row.channel_name || row.stream_key || "Replay",
  description: row.public_description || "",
  replay_category: row.replay_category || "",
  replay_tags: row.replay_tags || "",
  replay_visibility: row.replay_visibility || "public",
  stream: row.stream_key,
  channel_name: row.channel_name || row.stream_key,
  organization_name: row.organization_name || "NLM Streaming",
  organization_slug: row.organization_slug || "",
  created: row.created_at,
  published_at: row.published_at,
  duration_seconds: row.duration_seconds,
  width: row.width,
  height: row.height,
  bitrate_kbps: row.bitrate_kbps,
  codec: row.codec,
  replay_views: Number(row.replay_views || 0),
  url: `${CLIENT_URL.replace(/\/$/, "")}/replay/${row.public_slug}`,
  thumbnail_url: row.thumbnail_filename
    ? `${API_PUBLIC_URL.replace(/\/$/, "")}/api/public/replays/${row.public_slug}/thumbnail`
    : null,
});

const getSavedReplayIdsForMember = async (memberId, recordingIds = []) => {
  if (!memberId || !Array.isArray(recordingIds) || recordingIds.length === 0) {
    return new Set();
  }

  const cleanIds = recordingIds.map((id) => Number(id)).filter(Boolean);
  if (cleanIds.length === 0) return new Set();

  const result = await pool.query(
    `
    SELECT recording_id
    FROM replay_saved_replays
    WHERE member_id = $1
      AND recording_id = ANY($2::int[])
    `,
    [memberId, cleanIds],
  );

  return new Set((result.rows || []).map((row) => Number(row.recording_id)));
};

const getReplayProgressForViewer = async (recordingId, viewerId) => {
  if (!recordingId || !viewerId) return null;

  const result = await pool.query(
    `
    SELECT
      current_time_seconds,
      max_position_seconds,
      watched_seconds,
      completed,
      last_seen_at,
      ended_at
    FROM replay_sessions
    WHERE recording_id = $1
      AND viewer_id = $2
    ORDER BY last_seen_at DESC
    LIMIT 1
    `,
    [recordingId, viewerId],
  );

  const row = result.rows[0];
  if (!row) return null;

  return {
    current_time_seconds: Number(row.current_time_seconds || 0),
    max_position_seconds: Number(row.max_position_seconds || 0),
    watched_seconds: Number(row.watched_seconds || 0),
    completed: Boolean(row.completed),
    last_seen_at: row.last_seen_at,
    ended_at: row.ended_at,
  };
};

const getContinueWatchingForViewer = async ({
  viewerId,
  search = "",
  organizationSlug = "",
  limit = 12,
} = {}) => {
  if (!viewerId) return [];

  const values = [viewerId];
  const where = [
    "r.is_public = TRUE",
    "COALESCE(r.replay_visibility, 'public') = 'public'",
    "r.mp4_filename IS NOT NULL",
    "(o.id IS NULL OR o.is_active = TRUE)",
    "latest.current_time_seconds > 5",
    "COALESCE(latest.completed, FALSE) = FALSE",
    "(r.duration_seconds IS NULL OR r.duration_seconds <= 0 OR latest.current_time_seconds < GREATEST(r.duration_seconds - 10, 1))",
  ];

  if (organizationSlug) {
    values.push(organizationSlug);
    where.push(`o.slug = $${values.length}`);
  }

  if (search) {
    values.push(`%${search}%`);
    where.push(`(
      LOWER(COALESCE(r.public_title, '')) LIKE $${values.length}
      OR LOWER(COALESCE(r.public_description, '')) LIKE $${values.length}
      OR LOWER(COALESCE(r.replay_category, '')) LIKE $${values.length}
      OR LOWER(COALESCE(r.replay_tags, '')) LIKE $${values.length}
      OR LOWER(COALESCE(c.name, '')) LIKE $${values.length}
      OR LOWER(COALESCE(r.stream_key, '')) LIKE $${values.length}
      OR LOWER(COALESCE(o.name, '')) LIKE $${values.length}
    )`);
  }

  values.push(limit);

  const result = await pool.query(
    `
    WITH latest AS (
      SELECT DISTINCT ON (recording_id)
        recording_id,
        current_time_seconds,
        max_position_seconds,
        watched_seconds,
        completed,
        last_seen_at
      FROM replay_sessions
      WHERE viewer_id = $1
      ORDER BY recording_id, last_seen_at DESC
    )
    SELECT
      r.*,
      c.name AS channel_name,
      o.name AS organization_name,
      o.slug AS organization_slug,
      o.logo_url AS organization_logo_url,
      o.primary_color AS organization_primary_color,
      os.watch_page_title,
      os.donation_url,
      os.secondary_color,
      latest.current_time_seconds,
      latest.max_position_seconds,
      latest.watched_seconds,
      latest.completed,
      latest.last_seen_at AS progress_updated_at,
      GREATEST(
        COALESCE(latest.current_time_seconds, 0),
        COALESCE(latest.max_position_seconds, 0),
        CASE
          WHEN COALESCE(r.duration_seconds, 0) > 0
          THEN LEAST(COALESCE(latest.watched_seconds, 0), COALESCE(r.duration_seconds, 0))
          ELSE COALESCE(latest.watched_seconds, 0)
        END
      )::int AS progress_position_seconds,
      CASE
        WHEN COALESCE(r.duration_seconds, 0) > 0
        THEN LEAST(
          100,
          ROUND((
            GREATEST(
              COALESCE(latest.current_time_seconds, 0),
              COALESCE(latest.max_position_seconds, 0),
              LEAST(COALESCE(latest.watched_seconds, 0), COALESCE(r.duration_seconds, 0))
            )::numeric / r.duration_seconds::numeric
          ) * 100)
        )::int
        ELSE 0
      END AS progress_percent
    FROM latest
    JOIN recordings r ON r.id = latest.recording_id
    LEFT JOIN channels c ON c.id = r.channel_id
    LEFT JOIN organizations o ON o.id = r.organization_id
    LEFT JOIN organization_settings os ON os.organization_id = r.organization_id
    WHERE ${where.join(" AND ")}
    ORDER BY latest.last_seen_at DESC
    LIMIT $${values.length}
    `,
    values,
  );

  return result.rows.map((row) => ({
    ...mapReplayLibraryItem(row),
    resume_seconds: Number(
      row.progress_position_seconds ||
        row.max_position_seconds ||
        row.current_time_seconds ||
        0,
    ),
    progress_percent: Number(row.progress_percent || 0),
    progress_updated_at: row.progress_updated_at,
  }));
};

const getPublicReplayBySlug = async (slug) => {
  const cleanSlug = slugifyRecording(slug);

  const result = await pool.query(
    `
    SELECT
      r.*,
      c.name AS channel_name,
      o.name AS organization_name,
      o.slug AS organization_slug,
      o.logo_url AS organization_logo_url,
      o.primary_color AS organization_primary_color,
      os.watch_page_title,
      os.donation_url,
      os.secondary_color
    FROM recordings r
    LEFT JOIN channels c ON c.id = r.channel_id
    LEFT JOIN organizations o ON o.id = r.organization_id
    LEFT JOIN organization_settings os ON os.organization_id = r.organization_id
    WHERE r.public_slug = $1
      AND r.is_public = TRUE
      AND r.mp4_filename IS NOT NULL
    LIMIT 1
    `,
    [cleanSlug],
  );

  return result.rows[0] || null;
};

const getReplayAccessStatus = (row, viewerMember = null) => {
  if (!row) {
    return {
      allowed: false,
      status: 404,
      code: "REPLAY_NOT_FOUND",
      message: "Replay not found or not published",
    };
  }

  const visibility = String(row.replay_visibility || "public").toLowerCase();

  if (["public", "unlisted"].includes(visibility)) {
    return { allowed: true, visibility };
  }

  if (visibility === "members_only") {
    const replayOrgId = row.organization_id
      ? Number(row.organization_id)
      : null;
    const memberOrgId = viewerMember?.organization_id
      ? Number(viewerMember.organization_id)
      : null;

    if (viewerMember && replayOrgId && memberOrgId === replayOrgId) {
      return { allowed: true, visibility, viewerMember };
    }

    return {
      allowed: false,
      status: 401,
      code: "MEMBERS_ONLY_REPLAY",
      visibility,
      organization_id: row.organization_id || null,
      organization_name: row.organization_name || "this organization",
      replay_title:
        row.public_title || row.channel_name || row.stream_key || "Replay",
      message: "This replay is available to members only.",
    };
  }

  return {
    allowed: false,
    status: 403,
    code: "PRIVATE_REPLAY",
    visibility,
    message: "This replay is private.",
  };
};

const mapPublicReplayDto = (row) => {
  const dto = mapRecordingRowToDto(row, row.channel_name);

  return {
    ok: true,
    recording: {
      ...dto,
      title: row.public_title || row.channel_name || dto.channel_name,
      description: row.public_description || "",
      replay_category: row.replay_category || "",
      replay_tags: row.replay_tags || "",
      replay_visibility: row.replay_visibility || "public",
      media_url: `${API_PUBLIC_URL.replace(/\/$/, "")}/api/public/replays/${row.public_slug}/media`,
      replay_views: Number(row.replay_views || 0),
      replay_unique_viewers: Number(row.replay_unique_viewers || 0),
      replay_total_watch_seconds: Number(row.replay_total_watch_seconds || 0),
      replay_avg_watch_seconds: Number(row.replay_avg_watch_seconds || 0),
      replay_completion_rate: Number(row.replay_completion_rate || 0),
      thumbnail_url: row.thumbnail_filename
        ? `${API_PUBLIC_URL.replace(/\/$/, "")}/api/public/replays/${row.public_slug}/thumbnail`
        : null,
    },
    organization: {
      id: row.organization_id,
      name: row.organization_name || "NLM Streaming",
      slug: row.organization_slug || "",
      logo_url: row.organization_logo_url || null,
      primary_color: row.organization_primary_color || "#0d6efd",
      secondary_color: row.secondary_color || "#fd9d00",
      donation_url: row.donation_url || null,
      watch_page_title:
        row.watch_page_title || row.organization_name || "Replay",
    },
  };
};

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

app.get("/api/public/organizations", async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT DISTINCT
        o.id,
        o.name,
        o.slug,
        o.logo_url,
        o.primary_color,
        COALESCE(os.watch_page_title, o.name) AS display_name,
        os.secondary_color
      FROM organizations o
      INNER JOIN recordings r ON r.organization_id = o.id
        AND r.is_public = TRUE
        AND r.mp4_filename IS NOT NULL
      LEFT JOIN organization_settings os ON os.organization_id = o.id
      WHERE o.is_active = TRUE
      ORDER BY o.name ASC
    `);

    res.json({
      ok: true,
      organizations: result.rows.map((org) => ({
        id: org.id,
        name: org.display_name || org.name,
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

app.get(
  "/api/public/members/library",
  requireViewerMember,
  async (req, res) => {
    try {
      const limit = Math.min(
        Math.max(Number.parseInt(req.query.limit || "24", 10) || 24, 1),
        100,
      );

      const result = await pool.query(
        `
      WITH latest AS (
        SELECT DISTINCT ON (s.recording_id)
          s.recording_id,
          s.current_time_seconds,
          s.max_position_seconds,
          s.watched_seconds,
          s.completed,
          s.last_seen_at,
          s.ended_at
        FROM replay_sessions s
        WHERE s.member_id = $1
          OR s.viewer_id = $2
        ORDER BY s.recording_id, s.last_seen_at DESC
      )
      SELECT
        r.*,
        c.name AS channel_name,
        o.name AS organization_name,
        o.slug AS organization_slug,
        o.logo_url AS organization_logo_url,
        o.primary_color AS organization_primary_color,
        os.watch_page_title,
        os.donation_url,
        os.secondary_color,
        latest.current_time_seconds,
        latest.max_position_seconds,
        latest.watched_seconds,
        latest.completed,
        latest.last_seen_at AS progress_updated_at,
        latest.ended_at,
        GREATEST(
          COALESCE(latest.current_time_seconds, 0),
          COALESCE(latest.max_position_seconds, 0),
          CASE
            WHEN COALESCE(r.duration_seconds, 0) > 0
            THEN LEAST(COALESCE(latest.watched_seconds, 0), COALESCE(r.duration_seconds, 0))
            ELSE COALESCE(latest.watched_seconds, 0)
          END
        )::int AS progress_position_seconds,
        CASE
          WHEN COALESCE(r.duration_seconds, 0) > 0
          THEN LEAST(
            100,
            ROUND((
              GREATEST(
                COALESCE(latest.current_time_seconds, 0),
                COALESCE(latest.max_position_seconds, 0),
                LEAST(COALESCE(latest.watched_seconds, 0), COALESCE(r.duration_seconds, 0))
              )::numeric / r.duration_seconds::numeric
            ) * 100)
          )::int
          ELSE 0
        END AS progress_percent
      FROM latest
      JOIN recordings r ON r.id = latest.recording_id
      LEFT JOIN channels c ON c.id = r.channel_id
      LEFT JOIN organizations o ON o.id = r.organization_id
      LEFT JOIN organization_settings os ON os.organization_id = r.organization_id
      WHERE r.is_public = TRUE
        AND r.mp4_filename IS NOT NULL
        AND (o.id IS NULL OR o.is_active = TRUE)
        AND (
          COALESCE(r.replay_visibility, 'public') IN ('public', 'unlisted', 'members_only')
          OR r.organization_id = $3
        )
      ORDER BY latest.last_seen_at DESC
      LIMIT $4
      `,
        [
          req.viewerMember.id,
          req.viewerMember.email,
          req.viewerMember.organization_id,
          limit,
        ],
      );

      const rows = result.rows || [];

      const mapped = rows.map((row) => {
        const duration = Number(row.duration_seconds || 0);
        const progressPosition = Number(
          row.progress_position_seconds ||
            row.max_position_seconds ||
            row.current_time_seconds ||
            0,
        );
        const progressPercent = Number(row.progress_percent || 0);
        const isCompleted =
          Boolean(row.completed) || (duration > 0 && progressPercent >= 90);

        return {
          ...mapReplayLibraryItem(row),
          resume_seconds: isCompleted ? 0 : progressPosition,
          progress_percent: progressPercent,
          progress_updated_at: row.progress_updated_at,
          completed: isCompleted,
          watched_seconds: Number(row.watched_seconds || 0),
          visibility: row.replay_visibility || "public",
        };
      });

      const savedResult = await pool.query(
        `
      SELECT
        r.*,
        c.name AS channel_name,
        o.name AS organization_name,
        o.slug AS organization_slug,
        o.logo_url AS organization_logo_url,
        o.primary_color AS organization_primary_color,
        os.watch_page_title,
        os.donation_url,
        os.secondary_color,
        sr.created_at AS saved_at,
        latest.current_time_seconds,
        latest.max_position_seconds,
        latest.watched_seconds,
        latest.completed,
        latest.last_seen_at AS progress_updated_at,
        GREATEST(
          COALESCE(latest.current_time_seconds, 0),
          COALESCE(latest.max_position_seconds, 0),
          CASE
            WHEN COALESCE(r.duration_seconds, 0) > 0
            THEN LEAST(COALESCE(latest.watched_seconds, 0), COALESCE(r.duration_seconds, 0))
            ELSE COALESCE(latest.watched_seconds, 0)
          END
        )::int AS progress_position_seconds,
        CASE
          WHEN COALESCE(r.duration_seconds, 0) > 0
          THEN LEAST(
            100,
            ROUND((
              GREATEST(
                COALESCE(latest.current_time_seconds, 0),
                COALESCE(latest.max_position_seconds, 0),
                LEAST(COALESCE(latest.watched_seconds, 0), COALESCE(r.duration_seconds, 0))
              )::numeric / r.duration_seconds::numeric
            ) * 100)
          )::int
          ELSE 0
        END AS progress_percent
      FROM replay_saved_replays sr
      JOIN recordings r ON r.id = sr.recording_id
      LEFT JOIN channels c ON c.id = r.channel_id
      LEFT JOIN organizations o ON o.id = r.organization_id
      LEFT JOIN organization_settings os ON os.organization_id = r.organization_id
      LEFT JOIN LATERAL (
        SELECT
          s.current_time_seconds,
          s.max_position_seconds,
          s.watched_seconds,
          s.completed,
          s.last_seen_at
        FROM replay_sessions s
        WHERE s.recording_id = r.id
          AND (s.member_id = $1 OR s.viewer_id = $2)
        ORDER BY s.last_seen_at DESC
        LIMIT 1
      ) latest ON TRUE
      WHERE sr.member_id = $1
        AND r.is_public = TRUE
        AND r.mp4_filename IS NOT NULL
        AND (o.id IS NULL OR o.is_active = TRUE)
      ORDER BY sr.created_at DESC
      LIMIT $3
      `,
        [req.viewerMember.id, req.viewerMember.email, limit],
      );

      const saved = (savedResult.rows || []).map((row) => {
        const duration = Number(row.duration_seconds || 0);
        const progressPercent = Number(row.progress_percent || 0);
        return {
          ...mapReplayLibraryItem(row),
          resume_seconds: Number(row.progress_position_seconds || 0),
          progress_percent: progressPercent,
          progress_updated_at: row.progress_updated_at,
          saved_at: row.saved_at,
          completed:
            Boolean(row.completed) || (duration > 0 && progressPercent >= 90),
          watched_seconds: Number(row.watched_seconds || 0),
          visibility: row.replay_visibility || "public",
          is_saved: true,
        };
      });

      const continueWatching = mapped.filter((item) => {
        const duration = Number(item.duration_seconds || 0);
        const resume = Number(item.resume_seconds || 0);
        return (
          resume > 5 &&
          !item.completed &&
          (!duration || resume < Math.max(duration * 0.9, 1))
        );
      });

      const completed = mapped.filter((item) => item.completed);
      const recent = mapped;

      const stats = {
        total_watched: recent.length,
        continue_count: continueWatching.length,
        completed_count: completed.length,
        saved_count: saved.length,
        total_watch_seconds: recent.reduce(
          (sum, item) => sum + Number(item.watched_seconds || 0),
          0,
        ),
      };

      res.json({
        ok: true,
        viewer: req.viewerMember,
        stats,
        continue_watching: continueWatching,
        completed,
        saved,
        recent,
      });
    } catch (error) {
      console.error("Member library error:", error);
      res.status(500).json({
        ok: false,
        message: "Failed to load member watch history.",
        error: error.message,
      });
    }
  },
);

app.get("/api/public/members/saved", requireViewerMember, async (req, res) => {
  try {
    const limit = Math.min(
      Math.max(Number.parseInt(req.query.limit || "48", 10) || 48, 1),
      100,
    );

    const result = await pool.query(
      `
      SELECT
        r.*,
        c.name AS channel_name,
        o.name AS organization_name,
        o.slug AS organization_slug,
        sr.created_at AS saved_at
      FROM replay_saved_replays sr
      JOIN recordings r ON r.id = sr.recording_id
      LEFT JOIN channels c ON c.id = r.channel_id
      LEFT JOIN organizations o ON o.id = r.organization_id
      WHERE sr.member_id = $1
        AND r.is_public = TRUE
        AND r.mp4_filename IS NOT NULL
        AND (o.id IS NULL OR o.is_active = TRUE)
      ORDER BY sr.created_at DESC
      LIMIT $2
      `,
      [req.viewerMember.id, limit],
    );

    res.json({
      ok: true,
      saved: (result.rows || []).map((row) => ({
        ...mapReplayLibraryItem(row),
        saved_at: row.saved_at,
        is_saved: true,
      })),
    });
  } catch (error) {
    console.error("Saved replays error:", error);
    res.status(500).json({
      ok: false,
      message: "Failed to load saved replays.",
      error: error.message,
    });
  }
});

app.post(
  "/api/public/members/saved/:slug",
  requireViewerMember,
  async (req, res) => {
    try {
      const row = await getPublicReplayBySlug(req.params.slug);
      const access = getReplayAccessStatus(row, req.viewerMember);

      if (!access.allowed) {
        return res.status(access.status).json({
          ok: false,
          code: access.code,
          message: access.message,
        });
      }

      await pool.query(
        `
      INSERT INTO replay_saved_replays (member_id, recording_id)
      VALUES ($1, $2)
      ON CONFLICT (member_id, recording_id) DO NOTHING
      `,
        [req.viewerMember.id, row.id],
      );

      res.json({ ok: true, saved: true, recording_id: row.id });
    } catch (error) {
      console.error("Save replay error:", error);
      res.status(500).json({
        ok: false,
        message: "Failed to save replay.",
        error: error.message,
      });
    }
  },
);

app.delete(
  "/api/public/members/saved/:slug",
  requireViewerMember,
  async (req, res) => {
    try {
      const row = await getPublicReplayBySlug(req.params.slug);

      await pool.query(
        `
      DELETE FROM replay_saved_replays
      WHERE member_id = $1
        AND recording_id = $2
      `,
        [req.viewerMember.id, row.id],
      );

      res.json({ ok: true, saved: false, recording_id: row.id });
    } catch (error) {
      console.error("Remove saved replay error:", error);
      res.status(500).json({
        ok: false,
        message: "Failed to remove saved replay.",
        error: error.message,
      });
    }
  },
);

app.get("/api/public/replays/:slug/recommendations", async (req, res) => {
  try {
    const currentReplay = await getPublicReplayBySlug(req.params.slug);
    const viewerMember = await authenticateViewerMemberOptional(req);
    const access = getReplayAccessStatus(currentReplay, viewerMember);

    if (!access.allowed) {
      return res.status(access.status).json({
        ok: false,
        code: access.code,
        message: access.message,
      });
    }

    const memberOrgId = viewerMember?.organization_id
      ? Number(viewerMember.organization_id)
      : null;

    const limit = Math.min(
      Math.max(Number.parseInt(req.query.limit || "12", 10) || 12, 1),
      24,
    );

    const result = await pool.query(
      `
      SELECT
        rec.*,
        c.name AS channel_name,
        o.name AS organization_name,
        o.slug AS organization_slug,
        o.logo_url AS organization_logo_url,
        o.primary_color AS organization_primary_color,
        os.watch_page_title,
        os.donation_url,
        os.secondary_color,
        CASE
          WHEN rec.organization_id = $2 THEN 1
          ELSE 0
        END AS same_organization_score,
        CASE
          WHEN LOWER(COALESCE(rec.replay_category, '')) =
               LOWER(COALESCE($3, ''))
               AND COALESCE(rec.replay_category, '') <> ''
          THEN 1
          ELSE 0
        END AS same_category_score
      FROM recordings rec
      LEFT JOIN channels c ON c.id = rec.channel_id
      LEFT JOIN organizations o ON o.id = rec.organization_id
      LEFT JOIN organization_settings os ON os.organization_id = rec.organization_id
      WHERE rec.id <> $1
        AND rec.is_public = TRUE
        AND rec.mp4_filename IS NOT NULL
        AND (o.id IS NULL OR o.is_active = TRUE)
        AND (
          COALESCE(rec.replay_visibility, 'public') = 'public'
          OR (
            COALESCE(rec.replay_visibility, 'public') = 'members_only'
            AND $4::int IS NOT NULL
            AND rec.organization_id = $4::int
          )
        )
      ORDER BY
        same_category_score DESC,
        same_organization_score DESC,
        rec.published_at DESC NULLS LAST,
        rec.created_at DESC NULLS LAST
      LIMIT $5
      `,
      [
        currentReplay.id,
        currentReplay.organization_id || null,
        currentReplay.replay_category || "",
        memberOrgId,
        limit,
      ],
    );

    const items = (result.rows || []).map((row) => ({
      ...mapReplayLibraryItem(row),
      reason:
        row.same_category_score > 0
          ? "Related by category"
          : row.same_organization_score > 0
            ? `More from ${row.organization_name || "this organization"}`
            : "Recently published",
    }));

    const sameOrganization = items.filter(
      (item) =>
        item.organization_slug === (currentReplay.organization_slug || ""),
    );

    const sameCategory = items.filter(
      (item) =>
        String(item.replay_category || "").toLowerCase() ===
          String(currentReplay.replay_category || "").toLowerCase() &&
        String(item.replay_category || "").trim(),
    );

    res.json({
      ok: true,
      recommendations: items,
      more_from_organization: sameOrganization.slice(0, 8),
      related_by_category: sameCategory.slice(0, 8),
      recently_published: items.slice(0, 8),
    });
  } catch (error) {
    console.error("Replay recommendations error:", error);
    res.status(500).json({
      ok: false,
      message: "Failed to load replay recommendations.",
      error: error.message,
    });
  }
});

app.get("/api/public/replays/:slug", async (req, res) => {
  try {
    const row = await getPublicReplayBySlug(req.params.slug);
    const viewerMember = await authenticateViewerMemberOptional(req);

    const access = getReplayAccessStatus(row, viewerMember);

    if (!access.allowed) {
      return res.status(access.status).json({
        ok: false,
        code: access.code,
        visibility: access.visibility || null,
        organization_id: access.organization_id || null,
        organization_name: access.organization_name || null,
        replay_title: access.replay_title || null,
        message: access.message,
      });
    }

    const metrics = await getReplaySessionMetrics(row.id);
    const viewerId =
      viewerMember?.email || cleanOrgText(req.query.viewer_id || "", 255);
    const progress = await getReplayProgressForViewer(row.id, viewerId);
    const payload = mapPublicReplayDto({
      ...row,
      replay_views: Number(metrics.total_views || row.replay_views || 0),
      replay_unique_viewers: Number(metrics.unique_viewers || 0),
      replay_total_watch_seconds: Number(metrics.total_watch_seconds || 0),
      replay_avg_watch_seconds: Number(metrics.avg_watch_seconds || 0),
      replay_completion_rate: Number(metrics.completion_rate || 0),
    });

    if (viewerMember?.id) {
      const savedReplayIds = await getSavedReplayIdsForMember(viewerMember.id, [
        row.id,
      ]);
      payload.recording.is_saved = savedReplayIds.has(Number(row.id));
    } else {
      payload.recording.is_saved = false;
    }

    if (progress) {
      payload.recording.progress = {
        ...progress,
        progress_percent:
          Number(payload.recording.duration_seconds || 0) > 0
            ? Math.min(
                100,
                Math.round(
                  (Number(progress.current_time_seconds || 0) /
                    Number(payload.recording.duration_seconds || 1)) *
                    100,
                ),
              )
            : 0,
      };
    }

    res.json(payload);
  } catch (error) {
    console.error("Public replay error:", error);
    res.status(500).json({ ok: false, message: "Failed to load replay" });
  }
});

app.get("/api/public/replays/:slug/progress", async (req, res) => {
  try {
    const row = await getPublicReplayBySlug(req.params.slug);
    const viewerMember = await authenticateViewerMemberOptional(req);

    const access = getReplayAccessStatus(row, viewerMember);

    if (!access.allowed) {
      return res.status(access.status).json({
        ok: false,
        code: access.code,
        visibility: access.visibility || null,
        organization_id: access.organization_id || null,
        organization_name: access.organization_name || null,
        replay_title: access.replay_title || null,
        message: access.message,
      });
    }

    const viewerId =
      viewerMember?.email || cleanOrgText(req.query.viewer_id || "", 255);
    const progress = await getReplayProgressForViewer(row.id, viewerId);

    res.json({ ok: true, progress });
  } catch (error) {
    console.error("Get replay progress error:", error);
    res
      .status(500)
      .json({ ok: false, message: "Failed to load replay progress" });
  }
});

app.post("/api/public/replays/:slug/session/start", async (req, res) => {
  try {
    const row = await getPublicReplayBySlug(req.params.slug);
    const viewerMember = await authenticateViewerMemberOptional(req);

    const access = getReplayAccessStatus(row, viewerMember);

    if (!access.allowed) {
      return res.status(access.status).json({
        ok: false,
        code: access.code,
        visibility: access.visibility || null,
        organization_id: access.organization_id || null,
        organization_name: access.organization_name || null,
        replay_title: access.replay_title || null,
        message: access.message,
      });
    }

    await closeStaleReplaySessions();

    const viewerId =
      viewerMember?.email ||
      cleanOrgText(req.body.viewer_id, 255) ||
      makeSessionToken();
    const currentTime = Math.max(
      0,
      Math.floor(Number(req.body.current_time || 0)),
    );
    const watchedSeconds = Math.max(
      0,
      Math.floor(Number(req.body.watched_seconds || 0)),
    );
    const playbackRate = Number(req.body.playback_rate || 1);
    const eventType = cleanOrgText(req.body.event_type || "start", 80);
    const userAgent = req.headers["user-agent"] || null;
    const referrer = req.headers.referer || req.headers.referrer || null;
    const ipAddress = getRequestIpAddress(req);
    const deviceInfo = getDeviceInfoFromUserAgent(userAgent);
    const countryCode = getRequestCountryCode(req, ipAddress);
    const countryName = getCountryNameFromCode(countryCode);

    const active = await pool.query(
      `
      SELECT *
      FROM replay_sessions
      WHERE recording_id = $1
        AND viewer_id = $2
        AND (
          (ended_at IS NULL AND last_seen_at >= NOW() - INTERVAL '5 minutes')
          OR (ended_at IS NOT NULL AND last_seen_at >= NOW() - INTERVAL '30 minutes')
        )
      ORDER BY ended_at IS NULL DESC, last_seen_at DESC
      LIMIT 1
      `,
      [row.id, viewerId],
    );

    let session = active.rows[0];

    if (session) {
      const refreshed = await pool.query(
        `
        UPDATE replay_sessions
        SET last_seen_at = NOW(),
            ended_at = NULL,
            current_time_seconds = CASE WHEN $2 > 0 THEN $2 ELSE current_time_seconds END,
            max_position_seconds = GREATEST(max_position_seconds, $2),
            ip_address = COALESCE($3, ip_address),
            user_agent = COALESCE($4, user_agent),
            referrer = COALESCE($5, referrer),
            device_type = COALESCE($6, device_type),
            browser_name = COALESCE($7, browser_name),
            os_name = COALESCE($8, os_name),
            country_code = COALESCE($9, country_code),
            country_name = COALESCE($10, country_name),
            watched_seconds = GREATEST(watched_seconds, $11),
            last_event_type = $12,
            last_playback_rate = $13
        WHERE id = $1
        RETURNING *
        `,
        [
          session.id,
          currentTime,
          ipAddress,
          userAgent,
          referrer,
          deviceInfo.deviceType,
          deviceInfo.browserName,
          deviceInfo.osName,
          countryCode,
          countryName,
          watchedSeconds,
          eventType,
          playbackRate,
        ],
      );
      session = refreshed.rows[0];
    } else {
      const sessionToken = makeSessionToken();
      const inserted = await pool.query(
        `
        INSERT INTO replay_sessions (
          organization_id,
          recording_id,
          public_slug,
          viewer_id,
          session_token,
          ip_address,
          user_agent,
          referrer,
          current_time_seconds,
          max_position_seconds,
          member_id,
          device_type,
          browser_name,
          os_name,
          country_code,
          country_name,
          watched_seconds,
          last_event_type,
          last_playback_rate
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
        RETURNING *
        `,
        [
          row.organization_id,
          row.id,
          row.public_slug,
          viewerId,
          sessionToken,
          ipAddress,
          userAgent,
          referrer,
          currentTime,
          viewerMember?.id || null,
          deviceInfo.deviceType,
          deviceInfo.browserName,
          deviceInfo.osName,
          countryCode,
          countryName,
          watchedSeconds,
          eventType,
          playbackRate,
        ],
      );
      session = inserted.rows[0];

      await pool.query(
        `UPDATE recordings SET replay_views = COALESCE(replay_views, 0) + 1 WHERE id = $1`,
        [row.id],
      );
    }

    await recordReplaySessionEvent({
      organizationId: row.organization_id,
      recordingId: row.id,
      replaySessionId: session.id,
      publicSlug: row.public_slug,
      viewerId,
      eventType,
      currentTime,
      watchedSeconds,
      deltaSeconds: 0,
      playbackRate,
    });

    const metrics = await getReplaySessionMetrics(row.id);

    res.json({
      ok: true,
      session: {
        session_token: session.session_token,
        viewer_id: session.viewer_id,
      },
      metrics,
    });
  } catch (error) {
    console.error("Start replay session error:", error);
    res
      .status(500)
      .json({ ok: false, message: "Failed to start replay session" });
  }
});

app.post("/api/public/replays/:slug/session/heartbeat", async (req, res) => {
  try {
    const sessionToken = cleanOrgText(req.body.session_token, 255);

    if (!sessionToken) {
      return res
        .status(400)
        .json({ ok: false, message: "Session token is required" });
    }

    const currentTime = Math.max(
      0,
      Math.floor(Number(req.body.current_time || 0)),
    );
    const watchedSeconds = Math.max(
      0,
      Math.floor(Number(req.body.watched_seconds || 0)),
    );
    const completed = Boolean(req.body.completed);
    const eventType = cleanOrgText(req.body.event_type || "heartbeat", 80);
    const playbackRate = Number(req.body.playback_rate || 1);
    const deltaSeconds = Math.max(
      0,
      Math.floor(Number(req.body.delta_seconds || 0)),
    );

    const result = await pool.query(
      `
      UPDATE replay_sessions
      SET last_seen_at = NOW(),
          current_time_seconds = $2,
          max_position_seconds = GREATEST(max_position_seconds, $2),
          watched_seconds = GREATEST(watched_seconds, $3),
          last_event_type = $5,
          last_playback_rate = $6,
          heartbeat_count = COALESCE(heartbeat_count, 0) + 1,
          completed = completed OR $4 OR (
            SELECT CASE
              WHEN COALESCE(r.duration_seconds, 0) > 0
              THEN GREATEST($2, $3) >= GREATEST(1, FLOOR(r.duration_seconds * 0.9))
              ELSE FALSE
            END
            FROM recordings r
            WHERE r.id = replay_sessions.recording_id
          )
      WHERE session_token = $1
        AND ended_at IS NULL
      RETURNING id, organization_id, recording_id, public_slug, viewer_id
      `,
      [
        sessionToken,
        currentTime,
        watchedSeconds,
        completed,
        eventType,
        playbackRate,
      ],
    );

    if (!result.rows[0]) {
      return res
        .status(404)
        .json({ ok: false, message: "Replay session not found" });
    }

    const updatedSession = result.rows[0];

    await recordReplaySessionEvent({
      organizationId: updatedSession.organization_id,
      recordingId: updatedSession.recording_id,
      replaySessionId: updatedSession.id,
      publicSlug: updatedSession.public_slug,
      viewerId: updatedSession.viewer_id,
      eventType,
      currentTime,
      watchedSeconds,
      deltaSeconds,
      playbackRate,
    });

    const metrics = await getReplaySessionMetrics(updatedSession.recording_id);
    res.json({ ok: true, metrics });
  } catch (error) {
    console.error("Replay heartbeat error:", error);
    res
      .status(500)
      .json({ ok: false, message: "Failed to update replay heartbeat" });
  }
});

app.post("/api/public/replays/:slug/session/end", async (req, res) => {
  try {
    const sessionToken = cleanOrgText(req.body.session_token, 255);

    if (!sessionToken) return res.json({ ok: true });

    const currentTime = Math.max(
      0,
      Math.floor(Number(req.body.current_time || 0)),
    );
    const watchedSeconds = Math.max(
      0,
      Math.floor(Number(req.body.watched_seconds || 0)),
    );
    const completed = Boolean(req.body.completed);
    const eventType = cleanOrgText(req.body.event_type || "end", 80);
    const playbackRate = Number(req.body.playback_rate || 1);
    const deltaSeconds = Math.max(
      0,
      Math.floor(Number(req.body.delta_seconds || 0)),
    );

    const result = await pool.query(
      `
      UPDATE replay_sessions
      SET ended_at = NOW(),
          last_seen_at = NOW(),
          current_time_seconds = $2,
          max_position_seconds = GREATEST(max_position_seconds, $2),
          watched_seconds = GREATEST(watched_seconds, $3),
          last_event_type = $5,
          last_playback_rate = $6,
          heartbeat_count = COALESCE(heartbeat_count, 0) + 1,
          completed = completed OR $4 OR (
            SELECT CASE
              WHEN COALESCE(r.duration_seconds, 0) > 0
              THEN GREATEST($2, $3) >= GREATEST(1, FLOOR(r.duration_seconds * 0.9))
              ELSE FALSE
            END
            FROM recordings r
            WHERE r.id = replay_sessions.recording_id
          )
      WHERE session_token = $1
        AND ended_at IS NULL
      RETURNING id, organization_id, recording_id, public_slug, viewer_id
      `,
      [
        sessionToken,
        currentTime,
        watchedSeconds,
        completed,
        eventType,
        playbackRate,
      ],
    );

    const endedSession = result.rows[0];

    if (endedSession) {
      await recordReplaySessionEvent({
        organizationId: endedSession.organization_id,
        recordingId: endedSession.recording_id,
        replaySessionId: endedSession.id,
        publicSlug: endedSession.public_slug,
        viewerId: endedSession.viewer_id,
        eventType,
        currentTime,
        watchedSeconds,
        deltaSeconds,
        playbackRate,
      });
    }

    const metrics = endedSession
      ? await getReplaySessionMetrics(endedSession.recording_id)
      : null;

    res.json({ ok: true, metrics });
  } catch (error) {
    console.error("End replay session error:", error);
    res
      .status(500)
      .json({ ok: false, message: "Failed to end replay session" });
  }
});

app.get("/api/public/replays/:slug/media", async (req, res) => {
  try {
    const row = await getPublicReplayBySlug(req.params.slug);
    const viewerMember = await authenticateViewerMemberOptional(req);

    const access = getReplayAccessStatus(row, viewerMember);

    if (!access.allowed) {
      return res.status(access.status).send(access.message);
    }

    const mediaFile = row.mp4_filename || row.filename;
    const mediaPath =
      row.mp4_filepath || getRecordingAbsolutePath(row.stream_key, mediaFile);

    if (!mediaPath || !fs.existsSync(mediaPath)) {
      return res.status(404).send("Replay media not found");
    }

    if (!(await isPlayableMediaFile(mediaPath))) {
      return res.status(409).send("Replay is not playable yet");
    }

    res.setHeader("Cache-Control", "public, max-age=60");
    res.sendFile(mediaPath);
  } catch (error) {
    console.error("Public replay media error:", error);
    res.status(500).send("Failed to load replay media");
  }
});

app.get("/api/public/replays/:slug/thumbnail", async (req, res) => {
  try {
    const row = await getPublicReplayBySlug(req.params.slug);
    const viewerMember = await authenticateViewerMemberOptional(req);

    const access = getReplayAccessStatus(row, viewerMember);

    if (!access.allowed) {
      return res.status(access.status).send(access.message);
    }

    if (
      !row ||
      !row.thumbnail_filepath ||
      !fs.existsSync(row.thumbnail_filepath)
    ) {
      return res.status(404).send("Thumbnail not found");
    }

    res.setHeader("Cache-Control", "public, max-age=300");
    res.sendFile(row.thumbnail_filepath);
  } catch (error) {
    console.error("Public replay thumbnail error:", error);
    res.status(500).send("Failed to load thumbnail");
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

    const input = `rtmp://localhost/live/${stream}`;
    const output720 = `rtmp://localhost/live/${stream}_720p`;
    const output480 = `rtmp://localhost/live/${stream}_480p`;

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

app.get("/api/abr/:stream/master.m3u8", async (req, res) => {
  const { stream } = req.params;
  const baseUrl = `${HLS_BASE_URL}/live`;

  const checkPlaylist = async (url) => {
    try {
      const response = await fetch(url);
      if (!response.ok) return false;

      const text = await response.text();
      return text.includes("#EXTM3U");
    } catch {
      return false;
    }
  };

  const originalUrl = `${baseUrl}/${stream}.m3u8`;
  const url720 = `${baseUrl}/${stream}_720p.m3u8`;
  const url480 = `${baseUrl}/${stream}_480p.m3u8`;

  let masterPlaylist = `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-INDEPENDENT-SEGMENTS
`;

  masterPlaylist += `#EXT-X-STREAM-INF:BANDWIDTH=3500000,RESOLUTION=1920x1080,NAME="Original"
${originalUrl}
`;

  if (await checkPlaylist(url720)) {
    masterPlaylist += `#EXT-X-STREAM-INF:BANDWIDTH=2500000,RESOLUTION=1280x720,NAME="720p"
${url720}
`;
  }

  if (await checkPlaylist(url480)) {
    masterPlaylist += `#EXT-X-STREAM-INF:BANDWIDTH=1200000,RESOLUTION=854x480,NAME="480p"
${url480}
`;
  }

  res.setHeader("Content-Type", "application/vnd.apple.mpegurl");
  res.setHeader("Cache-Control", "no-store");
  res.send(masterPlaylist);
});

app.get(
  "/api/analytics/replays/engagement",
  authenticateAdmin,
  resolveOrganizationForRequest,
  async (req, res) => {
    try {
      if (!requireAnalyticsTenant(req, res)) return;

      const days = Math.min(
        Math.max(Number.parseInt(req.query.days || "7", 10) || 7, 1),
        365,
      );
      const recordingId = Number(req.query.recording_id || 0) || null;

      const params = recordingId
        ? [req.organization.id, days, recordingId]
        : [req.organization.id, days];

      const recordingFilter = recordingId ? "AND rse.recording_id = $3" : "";

      const dropoffResult = await pool.query(
        `
        SELECT
          FLOOR(COALESCE(rse.current_time_seconds, 0) / 60)::int AS minute,
          COUNT(*)::int AS events,
          COUNT(DISTINCT rse.viewer_id)::int AS viewers
        FROM replay_session_events rse
        JOIN recordings r ON r.id = rse.recording_id
        WHERE rse.organization_id = $1
          AND r.organization_id = $1
          AND rse.created_at >= NOW() - ($2::text || ' days')::interval
          ${recordingFilter}
        GROUP BY minute
        ORDER BY minute ASC
        LIMIT 240
        `,
        params,
      );

      const eventResult = await pool.query(
        `
        SELECT
          event_type,
          COUNT(*)::int AS total_events,
          COUNT(DISTINCT viewer_id)::int AS unique_viewers
        FROM replay_session_events rse
        JOIN recordings r ON r.id = rse.recording_id
        WHERE rse.organization_id = $1
          AND r.organization_id = $1
          AND rse.created_at >= NOW() - ($2::text || ' days')::interval
          ${recordingFilter}
        GROUP BY event_type
        ORDER BY total_events DESC
        `,
        params,
      );

      const topMomentsResult = await pool.query(
        `
        SELECT
          FLOOR(COALESCE(rse.current_time_seconds, 0) / 30)::int * 30 AS second_bucket,
          COUNT(*)::int AS events,
          COUNT(DISTINCT rse.viewer_id)::int AS viewers
        FROM replay_session_events rse
        JOIN recordings r ON r.id = rse.recording_id
        WHERE rse.organization_id = $1
          AND r.organization_id = $1
          AND rse.created_at >= NOW() - ($2::text || ' days')::interval
          ${recordingFilter}
        GROUP BY second_bucket
        ORDER BY viewers DESC, events DESC
        LIMIT 20
        `,
        params,
      );

      res.json({
        ok: true,
        days,
        recording_id: recordingId,
        dropoff: dropoffResult.rows,
        events: eventResult.rows,
        top_moments: topMomentsResult.rows,
      });
    } catch (error) {
      console.error("Replay engagement analytics error:", error);
      res.status(500).json({
        ok: false,
        message: "Failed to load replay engagement analytics",
        error: error.message,
      });
    }
  },
);

app.get(
  "/api/analytics/replays",
  authenticateAdmin,
  resolveOrganizationForRequest,
  async (req, res) => {
    try {
      if (!requireAnalyticsTenant(req, res)) return;

      const days = Math.min(
        Math.max(Number.parseInt(req.query.days || "7", 10) || 7, 1),
        365,
      );

      await closeStaleReplaySessions();

      const result = await pool.query(
        `
        WITH replay_scope AS (
          SELECT
            r.id,
            r.public_slug,
            COALESCE(r.public_title, c.name, r.stream_key, r.filename) AS title,
            r.stream_key,
            r.duration_seconds,
            r.published_at,
            r.organization_id
          FROM recordings r
          LEFT JOIN channels c ON c.id = r.channel_id
          WHERE r.organization_id = $1
            AND r.is_public = TRUE
            AND r.mp4_filename IS NOT NULL
        ),
        session_stats AS (
          SELECT
            rs.recording_id,
            COUNT(rs.id)::int AS total_views,
            COUNT(DISTINCT rs.viewer_id)::int AS unique_viewers,
            COUNT(DISTINCT rs.member_id)::int AS member_viewers,
            COALESCE(SUM(rs.watched_seconds), 0)::int AS total_watch_seconds,
            COALESCE(AVG(NULLIF(rs.watched_seconds, 0)), 0)::int AS avg_watch_seconds,
            COUNT(*) FILTER (WHERE rs.completed)::int AS completed_views,
            COALESCE(AVG(CASE WHEN rs.completed THEN 1 ELSE 0 END), 0)::float AS completion_rate,
            COALESCE(MAX(rs.max_position_seconds), 0)::int AS furthest_position_seconds,
            MAX(rs.started_at) AS last_viewed_at
          FROM replay_sessions rs
          JOIN replay_scope r ON r.id = rs.recording_id
          WHERE rs.organization_id = $1
            AND rs.started_at >= NOW() - ($2::text || ' days')::interval
          GROUP BY rs.recording_id
        ),
        event_stats AS (
          SELECT
            rse.recording_id,
            COUNT(*) FILTER (WHERE rse.event_type IN ('share', 'copy_link', 'copy-link'))::int AS shares,
            COUNT(*) FILTER (WHERE rse.event_type IN ('like', 'reaction'))::int AS reactions,
            COUNT(*) FILTER (WHERE rse.event_type IN ('play', 'start'))::int AS plays,
            COUNT(*) FILTER (WHERE rse.event_type IN ('complete', 'ended', 'end'))::int AS end_events
          FROM replay_session_events rse
          JOIN replay_scope r ON r.id = rse.recording_id
          WHERE rse.organization_id = $1
            AND rse.created_at >= NOW() - ($2::text || ' days')::interval
          GROUP BY rse.recording_id
        ),
        save_stats AS (
          SELECT
            s.recording_id,
            COUNT(*)::int AS saves
          FROM replay_saved_replays s
          JOIN replay_viewer_members m ON m.id = s.member_id
          WHERE m.organization_id = $1
            AND s.created_at >= NOW() - ($2::text || ' days')::interval
          GROUP BY s.recording_id
        )
        SELECT
          r.id,
          r.public_slug,
          r.title,
          r.stream_key,
          r.duration_seconds,
          r.published_at,
          COALESCE(ss.total_views, 0)::int AS total_views,
          COALESCE(ss.unique_viewers, 0)::int AS unique_viewers,
          COALESCE(ss.member_viewers, 0)::int AS member_viewers,
          COALESCE(ss.total_watch_seconds, 0)::int AS total_watch_seconds,
          COALESCE(ss.avg_watch_seconds, 0)::int AS avg_watch_seconds,
          COALESCE(ss.completed_views, 0)::int AS completed_views,
          COALESCE(ss.completion_rate, 0)::float AS completion_rate,
          COALESCE(ss.furthest_position_seconds, 0)::int AS furthest_position_seconds,
          COALESCE(es.shares, 0)::int AS shares,
          COALESCE(es.reactions, 0)::int AS reactions,
          COALESCE(es.plays, 0)::int AS plays,
          COALESCE(es.end_events, 0)::int AS end_events,
          COALESCE(sv.saves, 0)::int AS saves,
          (
            COALESCE(ss.total_views, 0)
            + (COALESCE(ss.completed_views, 0) * 3)
            + (COALESCE(sv.saves, 0) * 4)
            + (COALESCE(es.shares, 0) * 5)
            + (COALESCE(es.reactions, 0) * 2)
          )::int AS engagement_score,
          ss.last_viewed_at
        FROM replay_scope r
        LEFT JOIN session_stats ss ON ss.recording_id = r.id
        LEFT JOIN event_stats es ON es.recording_id = r.id
        LEFT JOIN save_stats sv ON sv.recording_id = r.id
        ORDER BY total_views DESC, engagement_score DESC, last_viewed_at DESC NULLS LAST, r.published_at DESC NULLS LAST
        LIMIT 50
        `,
        [req.organization.id, days],
      );

      const summary = result.rows.reduce(
        (acc, row) => {
          acc.total_views += Number(row.total_views || 0);
          acc.unique_viewers += Number(row.unique_viewers || 0);
          acc.member_viewers += Number(row.member_viewers || 0);
          acc.total_watch_seconds += Number(row.total_watch_seconds || 0);
          acc.completed_views += Number(row.completed_views || 0);
          acc.saves += Number(row.saves || 0);
          acc.shares += Number(row.shares || 0);
          acc.reactions += Number(row.reactions || 0);
          acc.engagement_score += Number(row.engagement_score || 0);
          return acc;
        },
        {
          total_views: 0,
          unique_viewers: 0,
          member_viewers: 0,
          total_watch_seconds: 0,
          completed_views: 0,
          saves: 0,
          shares: 0,
          reactions: 0,
          engagement_score: 0,
        },
      );

      summary.avg_watch_seconds = summary.total_views
        ? Math.round(summary.total_watch_seconds / summary.total_views)
        : 0;
      summary.completion_rate = summary.total_views
        ? summary.completed_views / summary.total_views
        : 0;

      res.json({ ok: true, days, summary, replays: result.rows });
    } catch (error) {
      console.error("Replay analytics error:", error);
      res
        .status(500)
        .json({ ok: false, message: "Failed to load replay analytics" });
    }
  },
);

app.get(
  "/api/analytics/replays/retention",
  authenticateAdmin,
  resolveOrganizationForRequest,
  async (req, res) => {
    try {
      if (!requireAnalyticsTenant(req, res)) return;

      const days = Math.min(
        Math.max(Number.parseInt(req.query.days || "7", 10) || 7, 1),
        365,
      );
      const recordingId = Number(req.query.recording_id || 0) || null;
      const params = recordingId
        ? [req.organization.id, days, recordingId]
        : [req.organization.id, days];
      const recordingFilter = recordingId ? "AND rs.recording_id = $3" : "";

      const result = await pool.query(
        `
        WITH scoped AS (
          SELECT
            rs.recording_id,
            COALESCE(r.public_title, r.stream_key, r.filename) AS title,
            GREATEST(
              COALESCE(rs.max_position_seconds, 0),
              COALESCE(rs.current_time_seconds, 0),
              COALESCE(rs.watched_seconds, 0)
            )::int AS reached_seconds,
            COALESCE(r.duration_seconds, 0)::int AS duration_seconds
          FROM replay_sessions rs
          JOIN recordings r ON r.id = rs.recording_id
          WHERE rs.organization_id = $1
            AND r.organization_id = $1
            AND rs.started_at >= NOW() - ($2::text || ' days')::interval
            ${recordingFilter}
        ),
        checkpoints AS (
          SELECT * FROM (VALUES
            (0, 'Start'),
            (60, '1m'),
            (300, '5m'),
            (600, '10m'),
            (900, '15m'),
            (1800, '30m'),
            (2700, '45m'),
            (3600, '60m'),
            (5400, '90m'),
            (7200, '120m')
          ) AS c(second_mark, label)
        ),
        totals AS (
          SELECT COUNT(*)::int AS total_sessions FROM scoped
        )
        SELECT
          c.second_mark,
          c.label,
          COALESCE(COUNT(s.*), 0)::int AS viewers,
          COALESCE(t.total_sessions, 0)::int AS total_sessions,
          CASE
            WHEN COALESCE(t.total_sessions, 0) > 0
            THEN (COUNT(s.*)::float / t.total_sessions)
            ELSE 0
          END AS retention_rate
        FROM checkpoints c
        CROSS JOIN totals t
        LEFT JOIN scoped s
          ON s.reached_seconds >= c.second_mark
         AND (s.duration_seconds = 0 OR c.second_mark <= s.duration_seconds)
        GROUP BY c.second_mark, c.label, t.total_sessions
        ORDER BY c.second_mark ASC
        `,
        params,
      );

      res.json({
        ok: true,
        days,
        recording_id: recordingId,
        retention: result.rows,
      });
    } catch (error) {
      console.error("Replay retention analytics error:", error);
      res.status(500).json({
        ok: false,
        message: "Failed to load replay retention analytics",
        error: error.message,
      });
    }
  },
);

app.get(
  "/api/analytics/members",
  authenticateAdmin,
  resolveOrganizationForRequest,
  async (req, res) => {
    try {
      if (!requireAnalyticsTenant(req, res)) return;

      const days = Math.min(
        Math.max(Number.parseInt(req.query.days || "7", 10) || 7, 1),
        365,
      );

      const result = await pool.query(
        `
        WITH member_sessions AS (
          SELECT
            m.id,
            m.name,
            m.email,
            m.status,
            m.created_at,
            COUNT(rs.id)::int AS replay_views,
            COUNT(DISTINCT rs.recording_id)::int AS replays_watched,
            COALESCE(SUM(rs.watched_seconds), 0)::int AS total_watch_seconds,
            COALESCE(AVG(NULLIF(rs.watched_seconds, 0)), 0)::int AS avg_watch_seconds,
            COUNT(*) FILTER (WHERE rs.completed)::int AS completed_replays,
            MAX(rs.last_seen_at) AS last_active_at
          FROM replay_viewer_members m
          LEFT JOIN replay_sessions rs
            ON rs.member_id = m.id
           AND rs.organization_id = m.organization_id
           AND rs.started_at >= NOW() - ($2::text || ' days')::interval
          WHERE m.organization_id = $1
          GROUP BY m.id
        )
        SELECT
          *,
          CASE
            WHEN replay_views > 0 THEN completed_replays::float / replay_views
            ELSE 0
          END AS completion_rate
        FROM member_sessions
        ORDER BY total_watch_seconds DESC, replay_views DESC, last_active_at DESC NULLS LAST
        LIMIT 50
        `,
        [req.organization.id, days],
      );

      const summary = result.rows.reduce(
        (acc, row) => {
          acc.total_members += 1;
          if (row.status === "active") acc.active_members += 1;
          if (Number(row.replay_views || 0) > 0) acc.watching_members += 1;
          acc.total_replay_views += Number(row.replay_views || 0);
          acc.total_watch_seconds += Number(row.total_watch_seconds || 0);
          acc.completed_replays += Number(row.completed_replays || 0);
          return acc;
        },
        {
          total_members: 0,
          active_members: 0,
          watching_members: 0,
          total_replay_views: 0,
          total_watch_seconds: 0,
          completed_replays: 0,
        },
      );

      summary.avg_watch_seconds = summary.total_replay_views
        ? Math.round(summary.total_watch_seconds / summary.total_replay_views)
        : 0;

      res.json({ ok: true, days, summary, members: result.rows });
    } catch (error) {
      console.error("Member analytics error:", error);
      res.status(500).json({
        ok: false,
        message: "Failed to load member analytics",
        error: error.message,
      });
    }
  },
);

app.get(
  "/api/analytics/replays/export.csv",
  authenticateAdmin,
  resolveOrganizationForRequest,
  async (req, res) => {
    try {
      if (!requireAnalyticsTenant(req, res)) return;

      const days = Math.min(
        Math.max(Number.parseInt(req.query.days || "7", 10) || 7, 1),
        365,
      );

      const result = await pool.query(
        `
        SELECT
          COALESCE(r.public_title, r.stream_key, r.filename) AS replay_title,
          r.public_slug,
          rs.viewer_id,
          m.email AS member_email,
          m.name AS member_name,
          rs.started_at,
          rs.last_seen_at,
          rs.ended_at,
          rs.current_time_seconds,
          rs.max_position_seconds,
          rs.watched_seconds,
          rs.completed,
          rs.device_type,
          rs.browser_name,
          rs.os_name,
          rs.country_code,
          rs.country_name,
          rs.referrer
        FROM replay_sessions rs
        JOIN recordings r ON r.id = rs.recording_id
        LEFT JOIN replay_viewer_members m ON m.id = rs.member_id
        WHERE rs.organization_id = $1
          AND r.organization_id = $1
          AND rs.started_at >= NOW() - ($2::text || ' days')::interval
        ORDER BY rs.started_at DESC
        `,
        [req.organization.id, days],
      );

      const rows = [
        [
          "replay_title",
          "public_slug",
          "viewer_id",
          "member_email",
          "member_name",
          "started_at",
          "last_seen_at",
          "ended_at",
          "current_time_seconds",
          "max_position_seconds",
          "watched_seconds",
          "completed",
          "device_type",
          "browser_name",
          "os_name",
          "country_code",
          "country_name",
          "referrer",
        ],
        ...result.rows.map((row) => [
          row.replay_title || "",
          row.public_slug || "",
          row.viewer_id || "",
          row.member_email || "",
          row.member_name || "",
          row.started_at ? new Date(row.started_at).toISOString() : "",
          row.last_seen_at ? new Date(row.last_seen_at).toISOString() : "",
          row.ended_at ? new Date(row.ended_at).toISOString() : "",
          row.current_time_seconds || 0,
          row.max_position_seconds || 0,
          row.watched_seconds || 0,
          row.completed ? "true" : "false",
          row.device_type || "",
          row.browser_name || "",
          row.os_name || "",
          row.country_code || "",
          row.country_name || "",
          row.referrer || "",
        ]),
      ];

      const csv = rows
        .map((row) => row.map(escapeCsvValue).join(","))
        .join("\n");
      res.setHeader("Content-Type", "text/csv; charset=utf-8");
      res.setHeader(
        "Content-Disposition",
        `attachment; filename="replay-analytics-${days}d.csv"`,
      );
      res.send(csv);
    } catch (error) {
      console.error("Replay analytics CSV export error:", error);
      res.status(500).json({
        ok: false,
        message: "Failed to export replay analytics CSV",
        error: error.message,
      });
    }
  },
);

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

  socket.on("overlay:join", ({ streamKey } = {}) => {
    const cleanStreamKey = cleanOverlayText(streamKey, 255);

    if (!cleanStreamKey) return;

    socket.join(`overlay:${cleanStreamKey}`);

    socket.emit("overlay:joined", {
      streamKey: cleanStreamKey,
    });
  });

  socket.on("overlay-admin:join", ({ streamKey } = {}) => {
    const cleanStreamKey = cleanOverlayText(streamKey, 255);

    if (!cleanStreamKey) return;

    socket.join(`overlay-admin:${cleanStreamKey}`);

    socket.emit("overlay-admin:joined", {
      streamKey: cleanStreamKey,
    });
  });

  socket.on("admin:join-prayer-requests", () => {
    socket.join("admins:prayer-requests");
  });

  socket.on("chat:join", ({ streamKey } = {}) => {
    const cleanStreamKey = cleanChatText(streamKey, 255);

    if (!cleanStreamKey) return;

    socket.join(`stream:${cleanStreamKey}`);

    socket.emit("chat:joined", {
      streamKey: cleanStreamKey,
    });
  });

  socket.on("chat:send", async (payload = {}) => {
    try {
      const streamKey = cleanChatText(payload.streamKey, 255);
      const displayName = cleanChatText(payload.displayName || "Guest", 120);
      const message = cleanChatText(payload.message, 500);

      if (!streamKey || !message) {
        socket.emit("chat:error", {
          message: "Name and message are required.",
        });
        return;
      }

      const result = await pool.query(
        `
        INSERT INTO live_chat_messages (stream_key, display_name, message)
        VALUES ($1, $2, $3)
        RETURNING id, stream_key, display_name, message, is_pinned, created_at
        `,
        [streamKey, displayName || "Guest", message],
      );

      io.to(`stream:${streamKey}`).emit("chat:new", result.rows[0]);
    } catch (error) {
      console.error("Socket chat send error:", error);

      socket.emit("chat:error", {
        message: "Failed to send chat message.",
      });
    }
  });

  socket.on("reactions:join", ({ streamKey } = {}) => {
    const cleanStreamKey = String(streamKey || "")
      .trim()
      .slice(0, 255);

    if (!cleanStreamKey) return;

    socket.join(`reactions:${cleanStreamKey}`);

    socket.emit("reactions:joined", {
      streamKey: cleanStreamKey,
    });
  });

  socket.on("admin:join-reactions", () => {
    socket.join("admins:reactions");
  });

  socket.on("disconnect", () => {
    console.log("Realtime client disconnected:", socket.id);
  });
});

/*
|--------------------------------------------------------------------------
| ADMIN MEMBER MANAGEMENT
|--------------------------------------------------------------------------
*/

const mapAdminMemberReplayHistoryItem = (row) => {
  const duration = Number(row.duration_seconds || 0);
  const currentTime = Math.max(
    Number(row.current_time_seconds || 0),
    Number(row.max_position_seconds || 0),
    duration > 0
      ? Math.min(Number(row.watched_seconds || 0), duration)
      : Number(row.watched_seconds || 0),
  );

  const progressPercent =
    duration > 0
      ? Math.min(100, Math.round((currentTime / duration) * 100))
      : 0;

  return {
    id: row.id,
    slug: row.public_slug,
    title: row.public_title || row.channel_name || row.stream_key || "Replay",
    description: row.public_description || "",
    stream: row.stream_key,
    replay_category: row.replay_category || "",
    replay_tags: row.replay_tags || "",
    replay_visibility: row.replay_visibility || "public",
    duration_seconds: row.duration_seconds || 0,
    thumbnail_url: row.thumbnail_filename
      ? `${API_PUBLIC_URL.replace(/\/$/, "")}/api/public/replays/${row.public_slug}/thumbnail`
      : null,
    replay_url: row.public_slug
      ? `${CLIENT_URL.replace(/\/$/, "")}/replay/${row.public_slug}`
      : null,
    current_time_seconds: Number(row.current_time_seconds || 0),
    max_position_seconds: Number(row.max_position_seconds || 0),
    watched_seconds: Number(row.watched_seconds || 0),
    progress_percent: progressPercent,
    completed: Boolean(row.completed) || progressPercent >= 90,
    last_seen_at: row.last_seen_at,
    started_at: row.started_at,
    ended_at: row.ended_at,
  };
};

app.get(
  "/api/members",
  authenticateAdmin,
  resolveOrganizationForRequest,
  requireOrganizationRole("owner", "admin", "operator"),
  async (req, res) => {
    try {
      const search = cleanOrgText(req.query.search || "", 255).toLowerCase();
      const status = cleanOrgText(req.query.status || "all", 40).toLowerCase();
      const limit = Math.min(
        Math.max(Number.parseInt(req.query.limit || "100", 10) || 100, 1),
        250,
      );

      const result = await pool.query(
        `
        SELECT
          m.id,
          m.organization_id,
          m.name,
          m.email,
          m.status,
          m.created_at,
          m.updated_at,
          COUNT(DISTINCT s.recording_id)::int AS watched_replays,
          COUNT(DISTINCT CASE WHEN s.completed THEN s.recording_id END)::int AS completed_replays,
          COALESCE(SUM(s.watched_seconds), 0)::int AS total_watch_seconds,
          MAX(s.last_seen_at) AS last_seen_at,
          COUNT(DISTINCT sr.recording_id)::int AS saved_replays
        FROM replay_viewer_members m
        LEFT JOIN replay_sessions s ON s.member_id = m.id
        LEFT JOIN replay_saved_replays sr ON sr.member_id = m.id
        WHERE m.organization_id = $1
          AND (
            $2 = ''
            OR LOWER(m.name) LIKE '%' || $2 || '%'
            OR LOWER(m.email) LIKE '%' || $2 || '%'
          )
          AND (
            $3 = 'all'
            OR $3 = ''
            OR LOWER(m.status) = $3
          )
        GROUP BY m.id
        ORDER BY
          CASE WHEN MAX(s.last_seen_at) IS NULL THEN 1 ELSE 0 END,
          MAX(s.last_seen_at) DESC NULLS LAST,
          m.created_at DESC
        LIMIT $4
        `,
        [req.organization.id, search, status, limit],
      );

      const statsResult = await pool.query(
        `
        SELECT
          COUNT(*)::int AS total_members,
          COUNT(*) FILTER (WHERE status = 'active')::int AS active_members,
          COUNT(*) FILTER (WHERE status <> 'active')::int AS inactive_members
        FROM replay_viewer_members
        WHERE organization_id = $1
        `,
        [req.organization.id],
      );

      const watchStatsResult = await pool.query(
        `
        SELECT
          COUNT(DISTINCT s.member_id)::int AS watching_members,
          COUNT(DISTINCT s.recording_id)::int AS watched_replays,
          COALESCE(SUM(s.watched_seconds), 0)::int AS total_watch_seconds
        FROM replay_sessions s
        JOIN replay_viewer_members m ON m.id = s.member_id
        WHERE m.organization_id = $1
        `,
        [req.organization.id],
      );

      res.json({
        ok: true,
        organization: req.organization,
        stats: {
          ...(statsResult.rows[0] || {}),
          ...(watchStatsResult.rows[0] || {}),
        },
        members: result.rows || [],
      });
    } catch (error) {
      console.error("Admin members list error:", error);
      res.status(500).json({
        ok: false,
        message: "Failed to load members.",
        error: error.message,
      });
    }
  },
);

app.get(
  "/api/members/:memberId/history",
  authenticateAdmin,
  resolveOrganizationForRequest,
  requireOrganizationRole("owner", "admin", "operator"),
  async (req, res) => {
    try {
      const memberId = Number(req.params.memberId);
      const limit = Math.min(
        Math.max(Number.parseInt(req.query.limit || "50", 10) || 50, 1),
        150,
      );

      const memberResult = await pool.query(
        `
        SELECT id, organization_id, name, email, status, created_at, updated_at
        FROM replay_viewer_members
        WHERE id = $1
          AND organization_id = $2
        LIMIT 1
        `,
        [memberId, req.organization.id],
      );

      const member = memberResult.rows[0];

      if (!member) {
        return res.status(404).json({
          ok: false,
          message: "Member not found for this organization.",
        });
      }

      const historyResult = await pool.query(
        `
        WITH latest AS (
          SELECT DISTINCT ON (s.recording_id)
            s.*
          FROM replay_sessions s
          WHERE s.member_id = $1
          ORDER BY s.recording_id, s.last_seen_at DESC NULLS LAST, s.started_at DESC NULLS LAST
        )
        SELECT
          r.*,
          c.name AS channel_name,
          latest.current_time_seconds,
          latest.max_position_seconds,
          latest.watched_seconds,
          latest.completed,
          latest.started_at,
          latest.last_seen_at,
          latest.ended_at,
          CASE WHEN sr.id IS NULL THEN FALSE ELSE TRUE END AS is_saved
        FROM latest
        JOIN recordings r ON r.id = latest.recording_id
        LEFT JOIN channels c ON c.id = r.channel_id
        LEFT JOIN replay_saved_replays sr
          ON sr.recording_id = r.id
         AND sr.member_id = $1
        WHERE r.organization_id = $2
        ORDER BY latest.last_seen_at DESC NULLS LAST, latest.started_at DESC NULLS LAST
        LIMIT $3
        `,
        [memberId, req.organization.id, limit],
      );

      const savedResult = await pool.query(
        `
        SELECT
          r.*,
          c.name AS channel_name,
          sr.created_at AS saved_at
        FROM replay_saved_replays sr
        JOIN recordings r ON r.id = sr.recording_id
        LEFT JOIN channels c ON c.id = r.channel_id
        WHERE sr.member_id = $1
          AND r.organization_id = $2
        ORDER BY sr.created_at DESC
        LIMIT $3
        `,
        [memberId, req.organization.id, limit],
      );

      const history = (historyResult.rows || []).map(
        mapAdminMemberReplayHistoryItem,
      );
      const saved = (savedResult.rows || []).map((row) => ({
        ...mapAdminMemberReplayHistoryItem(row),
        saved_at: row.saved_at,
        is_saved: true,
      }));

      res.json({
        ok: true,
        member,
        history,
        saved,
        stats: {
          watched_replays: history.length,
          completed_replays: history.filter((item) => item.completed).length,
          saved_replays: saved.length,
          total_watch_seconds: history.reduce(
            (sum, item) => sum + Number(item.watched_seconds || 0),
            0,
          ),
        },
      });
    } catch (error) {
      console.error("Admin member history error:", error);
      res.status(500).json({
        ok: false,
        message: "Failed to load member history.",
        error: error.message,
      });
    }
  },
);

app.patch(
  "/api/members/:memberId/status",
  authenticateAdmin,
  resolveOrganizationForRequest,
  requireOrganizationRole("owner", "admin", "operator"),
  async (req, res) => {
    try {
      const memberId = Number(req.params.memberId);
      const requestedStatus = cleanOrgText(
        req.body?.status || "active",
        40,
      ).toLowerCase();
      const status = ["active", "inactive", "blocked"].includes(requestedStatus)
        ? requestedStatus
        : "active";

      const result = await pool.query(
        `
        UPDATE replay_viewer_members
        SET status = $1,
            updated_at = NOW()
        WHERE id = $2
          AND organization_id = $3
        RETURNING id, organization_id, name, email, status, created_at, updated_at
        `,
        [status, memberId, req.organization.id],
      );

      if (!result.rows[0]) {
        return res.status(404).json({
          ok: false,
          message: "Member not found for this organization.",
        });
      }

      res.json({ ok: true, member: result.rows[0] });
    } catch (error) {
      console.error("Update member status error:", error);
      res.status(500).json({
        ok: false,
        message: "Failed to update member status.",
        error: error.message,
      });
    }
  },
);

app.delete(
  "/api/members/:memberId",
  authenticateAdmin,
  resolveOrganizationForRequest,
  requireOrganizationRole("owner", "admin", "operator"),
  async (req, res) => {
    try {
      const memberId = Number(req.params.memberId);

      const result = await pool.query(
        `
        DELETE FROM replay_viewer_members
        WHERE id = $1
          AND organization_id = $2
        RETURNING id, name, email
        `,
        [memberId, req.organization.id],
      );

      if (!result.rows[0]) {
        return res.status(404).json({
          ok: false,
          message: "Member not found for this organization.",
        });
      }

      res.json({ ok: true, deleted: result.rows[0] });
    } catch (error) {
      console.error("Delete member error:", error);
      res.status(500).json({
        ok: false,
        message: "Failed to delete member.",
        error: error.message,
      });
    }
  },
);

/*
|--------------------------------------------------------------------------
| SERVER
|--------------------------------------------------------------------------
*/

(async () => {
  await ensureScheduledStreamsTable();
  await ensureLiveChatTable();
  await ensurePrayerRequestsTable();
  await ensureOverlayTables();
  await ensureCtaLinksTable();
  await ensureViewerReactionsTable();
  await ensureOrganizationTables();
  await ensureRecordingLibraryTable();
  await ensureViewerAnalyticsTables();
  await ensureReplayAnalyticsTables();
  await ensureReplayMemberTables();
  await ensureSubscriptionTables();
  await ensurePendingSignupsTable();
})()
  .then(() => {
    server.listen(PORT, () => {
      console.log(
        `NLM Streaming Manager API running on http://localhost:${PORT}`,
      );
    });
  })
  .catch((error) => {
    console.error("Failed to initialize database tables:", error);
    process.exit(1);
  });
