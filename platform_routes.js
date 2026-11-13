// platform_routes.js
//
// Stream Health Alerts, Encoder Pre-flight and Outbound Webhooks.
// Mounted from oauth_routes.js (same pattern as facebook_page_routes.js),
// so server.js does not change.
//
//   GET    /api/organizations/current/alerts                 settings + event list
//   PUT    /api/organizations/current/alerts                 save settings
//   POST   /api/organizations/current/alerts/test            send a test alert
//   GET    /api/organizations/current/events                 recent events (alert history)
//
//   GET    /api/organizations/current/webhooks               endpoints + event catalog
//   POST   /api/organizations/current/webhooks               create (returns secret once)
//   PATCH  /api/organizations/current/webhooks/:id           edit / enable / disable
//   DELETE /api/organizations/current/webhooks/:id
//   POST   /api/organizations/current/webhooks/:id/rotate-secret
//   POST   /api/organizations/current/webhooks/:id/test
//   GET    /api/organizations/current/webhooks/:id/deliveries
//   POST   /api/organizations/current/webhooks/deliveries/:deliveryId/redeliver
//
//   GET    /api/channels/:channelId/preflight                latest encoder report
//   POST   /api/channels/:channelId/preflight/run            check now
//
// Env:
//   STREAM_MONITOR_ENABLED=false  turn the background monitor off on this
//                                 instance (run it on exactly one instance
//                                 if you ever scale to several backends).

const { EVENT_CATALOG, ensurePlatformTables } = require("./platform_events");
const alertService = require("./alert_service");
const webhookService = require("./webhook_service");
const {
  initStreamMonitor,
  startStreamMonitor,
  runPreflight,
  latestPreflight,
} = require("./stream_monitor_service");

module.exports = function registerPlatformRoutes(app, pool, mw) {
  const {
    authenticateAdmin,
    resolveOrganizationForRequest,
    requireRole,
    requireOrganizationRole,
  } = mw;
  const readMw = [authenticateAdmin, resolveOrganizationForRequest];
  const manageMw = [
    authenticateAdmin,
    resolveOrganizationForRequest,
    requireRole("super_admin", "admin", "operator"),
    requireOrganizationRole("owner", "admin"),
  ];

  initStreamMonitor(pool);
  ensurePlatformTables(pool)
    .then(() => {
      alertService.initAlertService(pool);
      webhookService.initWebhookService(pool);
      if (process.env.STREAM_MONITOR_ENABLED !== "false")
        startStreamMonitor(pool);
    })
    .catch((error) =>
      console.error(
        "[PLATFORM] Could not start alerts/webhooks:",
        error.message,
      ),
    );

  const fail = (res, error, fallback, status = 500) => {
    console.error(`[PLATFORM] ${fallback}:`, error.message);
    res
      .status(status)
      .json({ ok: false, message: error.publicMessage || fallback });
  };
  const badRequest = (res, message) =>
    res.status(400).json({ ok: false, message });

  const catalog = Object.entries(EVENT_CATALOG)
    .filter(([type]) => type !== "webhook.test")
    .map(([type, info]) => ({
      type,
      label: info.label,
      description: info.description,
    }));

  // ── alerts ──────────────────────────────────────────────────
  app.get("/api/organizations/current/alerts", ...readMw, async (req, res) => {
    try {
      const settings = await alertService.getSettings(req.organization.id);
      res.json({
        ok: true,
        settings,
        events: alertService.ALERTABLE_EVENTS.map((type) => ({
          type,
          label: EVENT_CATALOG[type]?.label || type,
          description: EVENT_CATALOG[type]?.description || "",
          severity: EVENT_CATALOG[type]?.alert || "info",
        })),
      });
    } catch (error) {
      fail(res, error, "Could not load alert settings");
    }
  });

  app.put(
    "/api/organizations/current/alerts",
    ...manageMw,
    async (req, res) => {
      try {
        const result = await alertService.saveSettings(
          req.organization.id,
          req.body || {},
        );
        if (!result.ok) return badRequest(res, result.errors.join(" "));
        res.json({ ok: true, settings: result.settings });
      } catch (error) {
        fail(res, error, "Could not save alert settings");
      }
    },
  );

  app.post(
    "/api/organizations/current/alerts/test",
    ...manageMw,
    async (req, res) => {
      try {
        const summary = await alertService.sendTestAlert(req.organization.id);
        res.json({ ok: true, summary });
      } catch (error) {
        fail(res, error, "Could not send the test alert");
      }
    },
  );

  app.get("/api/organizations/current/events", ...readMw, async (req, res) => {
    try {
      const limit = Math.min(200, Math.max(1, Number(req.query.limit) || 50));
      const values = [req.organization.id, limit];
      let filter = "";
      if (req.query.alertsOnly === "1")
        filter = "AND e.alert_summary IS NOT NULL";
      if (req.query.channelId) {
        values.push(Number(req.query.channelId));
        filter += ` AND e.channel_id = $${values.length}`;
      }
      const result = await pool.query(
        `SELECT e.event_id, e.event_type, e.severity, e.title, e.channel_id,
                c.name AS channel_name, e.data, e.alert_summary, e.created_at
         FROM platform_events e
         LEFT JOIN channels c ON c.id = e.channel_id
         WHERE e.organization_id = $1 ${filter}
         ORDER BY e.id DESC LIMIT $2`,
        values,
      );
      res.json({ ok: true, events: result.rows });
    } catch (error) {
      fail(res, error, "Could not load events");
    }
  });

  // ── webhooks ────────────────────────────────────────────────
  app.get(
    "/api/organizations/current/webhooks",
    ...readMw,
    async (req, res) => {
      try {
        res.json({
          ok: true,
          apiVersion: webhookService.API_VERSION,
          endpoints: await webhookService.listEndpoints(req.organization.id),
          events: catalog,
        });
      } catch (error) {
        fail(res, error, "Could not load webhooks");
      }
    },
  );

  app.post(
    "/api/organizations/current/webhooks",
    ...manageMw,
    async (req, res) => {
      try {
        const result = await webhookService.createEndpoint(
          req.organization.id,
          req.admin?.id,
          req.body || {},
        );
        res.status(201).json({ ok: true, ...result });
      } catch (error) {
        badRequest(res, error.message || "Could not add the webhook");
      }
    },
  );

  app.patch(
    "/api/organizations/current/webhooks/:id",
    ...manageMw,
    async (req, res) => {
      try {
        const endpoint = await webhookService.updateEndpoint(
          req.organization.id,
          Number(req.params.id),
          req.body || {},
        );
        if (!endpoint)
          return res
            .status(404)
            .json({ ok: false, message: "Webhook not found" });
        res.json({ ok: true, endpoint });
      } catch (error) {
        badRequest(res, error.message || "Could not update the webhook");
      }
    },
  );

  app.delete(
    "/api/organizations/current/webhooks/:id",
    ...manageMw,
    async (req, res) => {
      try {
        const deleted = await webhookService.deleteEndpoint(
          req.organization.id,
          Number(req.params.id),
        );
        if (!deleted)
          return res
            .status(404)
            .json({ ok: false, message: "Webhook not found" });
        res.json({ ok: true });
      } catch (error) {
        fail(res, error, "Could not delete the webhook");
      }
    },
  );

  app.post(
    "/api/organizations/current/webhooks/:id/rotate-secret",
    ...manageMw,
    async (req, res) => {
      try {
        const result = await webhookService.rotateSecret(
          req.organization.id,
          Number(req.params.id),
        );
        if (!result)
          return res
            .status(404)
            .json({ ok: false, message: "Webhook not found" });
        res.json({ ok: true, ...result });
      } catch (error) {
        fail(res, error, "Could not rotate the secret");
      }
    },
  );

  app.post(
    "/api/organizations/current/webhooks/:id/test",
    ...manageMw,
    async (req, res) => {
      try {
        const result = await webhookService.sendTest(
          req.organization.id,
          Number(req.params.id),
        );
        if (!result.queued)
          return badRequest(
            res,
            "This webhook is switched off. Turn it on to send a test.",
          );
        res.json({ ok: true, ...result });
      } catch (error) {
        fail(res, error, "Could not send the test event");
      }
    },
  );

  app.get(
    "/api/organizations/current/webhooks/:id/deliveries",
    ...readMw,
    async (req, res) => {
      try {
        res.json({
          ok: true,
          deliveries: await webhookService.listDeliveries(
            req.organization.id,
            Number(req.params.id),
            req.query.limit,
          ),
        });
      } catch (error) {
        fail(res, error, "Could not load deliveries");
      }
    },
  );

  app.post(
    "/api/organizations/current/webhooks/deliveries/:deliveryId/redeliver",
    ...manageMw,
    async (req, res) => {
      try {
        const ok = await webhookService.redeliver(
          req.organization.id,
          Number(req.params.deliveryId),
        );
        if (!ok)
          return res
            .status(404)
            .json({ ok: false, message: "Delivery not found" });
        res.json({ ok: true });
      } catch (error) {
        fail(res, error, "Could not redeliver");
      }
    },
  );

  // ── encoder pre-flight ──────────────────────────────────────
  async function ownChannel(req, res) {
    const result = await pool.query(
      `SELECT id FROM channels WHERE id = $1 AND organization_id = $2`,
      [req.params.channelId, req.organization.id],
    );
    if (!result.rows[0]) {
      res.status(404).json({ ok: false, message: "Channel not found" });
      return null;
    }
    return result.rows[0].id;
  }

  app.get("/api/channels/:channelId/preflight", ...readMw, async (req, res) => {
    try {
      const channelId = await ownChannel(req, res);
      if (!channelId) return;
      const live = await pool.query(
        `SELECT is_live, live_started_at FROM channels WHERE id = $1`,
        [channelId],
      );
      const report = await latestPreflight(channelId);
      const session = live.rows[0]?.live_started_at
        ? new Date(live.rows[0].live_started_at).toISOString()
        : null;
      res.json({
        ok: true,
        live: Boolean(live.rows[0]?.is_live),
        report,
        reportIsCurrent: Boolean(
          report && session && report.live_session === session,
        ),
      });
    } catch (error) {
      fail(res, error, "Could not load the encoder check");
    }
  });

  const lastManualRun = new Map();
  app.post(
    "/api/channels/:channelId/preflight/run",
    ...readMw,
    async (req, res) => {
      try {
        const channelId = await ownChannel(req, res);
        if (!channelId) return;
        const last = lastManualRun.get(channelId) || 0;
        if (Date.now() - last < 15000)
          return res
            .status(429)
            .json({
              ok: false,
              message: "Give it a few seconds before checking again.",
            });
        lastManualRun.set(channelId, Date.now());
        const result = await runPreflight(channelId, { emit: false });
        if (!result.ok) return res.status(409).json(result);
        res.json(result);
      } catch (error) {
        fail(res, error, "Could not run the encoder check");
      }
    },
  );
};
