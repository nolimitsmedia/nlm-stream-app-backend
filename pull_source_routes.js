// pull_source_routes.js
// Organization-scoped HTTP API for multiple external Pull Sources per channel.
// Phase 1 Input Failover: Primary + Backup warm-standby switching.

const {
  createPullSourceManager,
  SUPPORTED_PROTOCOLS,
} = require("./pull_source_service");
const {
  encryptSourceUrl,
  decryptSourceUrl,
  maskSourceUrl,
} = require("./pull_source_schema");

function sanitize(row, runtime = null) {
  if (!row) return row;
  const merged = { ...row, ...(runtime || {}) };
  delete merged.source_url;
  if (!merged.source_url_display) {
    try {
      merged.source_url_display = maskSourceUrl(
        decryptSourceUrl(row.source_url),
      );
    } catch {}
  }
  return merged;
}

function normalizeRole(value, fallback = "backup") {
  const role = String(value || fallback)
    .trim()
    .toLowerCase();
  return role === "primary" ? "primary" : "backup";
}

function positiveInt(value, fallback, min = 1, max = 3600) {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(min, Math.min(max, parsed));
}

module.exports = function registerPullSourceRoutes(app, pool, deps) {
  const {
    authenticateAdmin,
    resolveOrganizationForRequest,
    requireRole,
    requireOrganizationRole,
  } = deps;

  const manager = createPullSourceManager({ pool });

  const manageMw = [
    authenticateAdmin,
    resolveOrganizationForRequest,
    requireRole("super_admin", "admin", "operator"),
    requireOrganizationRole("owner", "admin"),
  ];

  async function getOwnedChannel(channelId, organizationId) {
    const result = await pool.query(
      `SELECT * FROM channels WHERE id=$1 AND organization_id=$2`,
      [channelId, organizationId],
    );
    return result.rows[0] || null;
  }

  async function loadSource(id, channelId, organizationId) {
    const result = await pool.query(
      `SELECT * FROM channel_pull_sources
       WHERE id=$1 AND channel_id=$2 AND organization_id=$3`,
      [id, channelId, organizationId],
    );
    return result.rows[0] || null;
  }

  async function listSources(channelId, organizationId) {
    const result = await pool.query(
      `SELECT * FROM channel_pull_sources
       WHERE channel_id=$1 AND organization_id=$2
       ORDER BY CASE WHEN role='primary' THEN 0 ELSE 1 END, priority ASC, id ASC`,
      [channelId, organizationId],
    );
    return result.rows;
  }

  async function ensureFailoverRow(channel) {
    const result = await pool.query(
      `INSERT INTO channel_source_failover
         (channel_id, organization_id)
       VALUES ($1,$2)
       ON CONFLICT (channel_id)
       DO UPDATE SET organization_id=EXCLUDED.organization_id
       RETURNING *`,
      [channel.id, channel.organization_id],
    );
    return result.rows[0];
  }

  async function loadFailover(channel) {
    const result = await pool.query(
      `SELECT * FROM channel_source_failover
       WHERE channel_id=$1 AND organization_id=$2`,
      [channel.id, channel.organization_id],
    );
    return result.rows[0] || ensureFailoverRow(channel);
  }

  // Backward-compatible endpoint name. Response now includes `sources`, while
  // `source` remains populated with active/primary/first source for older UIs.
  app.get(
    "/api/channels/:channelId/pull-source",
    authenticateAdmin,
    resolveOrganizationForRequest,
    async (req, res) => {
      try {
        const channel = await getOwnedChannel(
          req.params.channelId,
          req.organization.id,
        );
        if (!channel)
          return res
            .status(404)
            .json({ ok: false, message: "Channel not found" });

        const rows = await listSources(channel.id, req.organization.id);
        const sources = rows.map((row) =>
          sanitize(row, manager.getRuntimeState(row.id)),
        );
        const failover = await loadFailover(channel);
        const legacySource =
          sources.find((item) => item.is_active_source) ||
          sources.find((item) => item.role === "primary") ||
          sources[0] ||
          null;

        res.json({
          ok: true,
          source: legacySource,
          sources,
          failover,
          supported_protocols: SUPPORTED_PROTOCOLS,
        });
      } catch (error) {
        console.error("Get Pull Sources Error:", error);
        res
          .status(500)
          .json({ ok: false, message: "Failed to fetch Pull Sources" });
      }
    },
  );

  app.post(
    "/api/channels/:channelId/pull-source",
    ...manageMw,
    async (req, res) => {
      const client = await pool.connect();
      try {
        const channel = await getOwnedChannel(
          req.params.channelId,
          req.organization.id,
        );
        if (!channel)
          return res
            .status(404)
            .json({ ok: false, message: "Channel not found" });

        const protocol = manager.normalizeProtocol(req.body.protocol);
        const sourceUrl = String(
          req.body.source_url || req.body.sourceUrl || "",
        ).trim();
        if (!protocol)
          return res
            .status(400)
            .json({ ok: false, message: "Unsupported Pull Source protocol" });
        if (!sourceUrl)
          return res
            .status(400)
            .json({ ok: false, message: "Source URL is required" });

        await manager.validateSourceUrl(sourceUrl, protocol);
        const encrypted = encryptSourceUrl(sourceUrl);
        const display = maskSourceUrl(sourceUrl);

        await client.query("BEGIN");
        const existing = await client.query(
          `SELECT id, role, priority FROM channel_pull_sources
           WHERE channel_id=$1 AND organization_id=$2
           ORDER BY priority,id`,
          [channel.id, req.organization.id],
        );

        const requestedRole = req.body.role
          ? normalizeRole(req.body.role)
          : existing.rows.length === 0
            ? "primary"
            : "backup";

        if (
          requestedRole === "primary" &&
          existing.rows.some((item) => item.role === "primary")
        ) {
          await client.query("ROLLBACK");
          return res.status(409).json({
            ok: false,
            message:
              "This channel already has a Primary source. Edit the existing Primary or add this source as Backup.",
          });
        }

        const maxPriority = existing.rows.reduce(
          (max, item) => Math.max(max, Number(item.priority || 0)),
          0,
        );
        const priority = positiveInt(
          req.body.priority,
          requestedRole === "primary" ? 1 : Math.max(2, maxPriority + 1),
          1,
          999,
        );

        const result = await client.query(
          `INSERT INTO channel_pull_sources
             (channel_id, organization_id, name, protocol, source_url,
              source_url_display, enabled, auto_start, auto_reconnect,
              role, priority, status, is_running, health_status, updated_at)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'stopped',FALSE,'unknown',NOW())
           RETURNING *`,
          [
            channel.id,
            req.organization.id,
            String(req.body.name || "Pull Source").trim() || "Pull Source",
            protocol,
            encrypted,
            display,
            req.body.enabled !== false,
            Boolean(req.body.auto_start),
            req.body.auto_reconnect !== false,
            requestedRole,
            priority,
          ],
        );

        await client.query(
          `INSERT INTO channel_source_failover (channel_id, organization_id)
           VALUES ($1,$2)
           ON CONFLICT (channel_id) DO NOTHING`,
          [channel.id, req.organization.id],
        );
        await client.query("COMMIT");

        res.json({ ok: true, source: sanitize(result.rows[0]) });
      } catch (error) {
        try {
          await client.query("ROLLBACK");
        } catch {}
        console.error("Create Pull Source Error:", error);
        res.status(400).json({
          ok: false,
          message: error.message || "Failed to save Pull Source",
          code: error.code || null,
        });
      } finally {
        client.release();
      }
    },
  );

  app.put(
    "/api/channels/:channelId/pull-source/:id",
    ...manageMw,
    async (req, res) => {
      const client = await pool.connect();
      try {
        const channel = await getOwnedChannel(
          req.params.channelId,
          req.organization.id,
        );
        if (!channel)
          return res
            .status(404)
            .json({ ok: false, message: "Channel not found" });

        const existing = await loadSource(
          req.params.id,
          channel.id,
          req.organization.id,
        );
        if (!existing)
          return res
            .status(404)
            .json({ ok: false, message: "Pull Source not found" });

        const protocol = manager.normalizeProtocol(
          req.body.protocol || existing.protocol,
        );
        const currentUrl = decryptSourceUrl(existing.source_url);
        const sourceUrl = String(
          req.body.source_url || req.body.sourceUrl || currentUrl || "",
        ).trim();
        await manager.validateSourceUrl(sourceUrl, protocol);

        const nextEnabled =
          req.body.enabled !== undefined
            ? Boolean(req.body.enabled)
            : existing.enabled;
        const nextRole = normalizeRole(
          req.body.role,
          existing.role || "backup",
        );
        const nextPriority = positiveInt(
          req.body.priority,
          Number(existing.priority || 1),
          1,
          999,
        );

        const configurationChanged =
          protocol !== existing.protocol ||
          sourceUrl !== currentUrl ||
          nextRole !== existing.role ||
          nextPriority !== Number(existing.priority || 1) ||
          (req.body.name !== undefined &&
            String(req.body.name).trim() !==
              String(existing.name || "").trim());

        if (existing.is_running && configurationChanged && nextEnabled) {
          return res.status(409).json({
            ok: false,
            message:
              "Stop the Pull Source before changing its URL, protocol, role, priority, or name.",
          });
        }

        if (!nextEnabled) await manager.stopSource(existing);

        await client.query("BEGIN");
        if (nextRole === "primary") {
          await client.query(
            `UPDATE channel_pull_sources
             SET role='backup',
                 priority=CASE WHEN priority <= 1 THEN 2 ELSE priority END,
                 updated_at=NOW()
             WHERE channel_id=$1 AND organization_id=$2 AND id<>$3 AND role='primary'`,
            [channel.id, req.organization.id, existing.id],
          );
        }

        const result = await client.query(
          `UPDATE channel_pull_sources
           SET name=$1, protocol=$2, source_url=$3, source_url_display=$4,
               enabled=$5, auto_start=$6, auto_reconnect=$7,
               role=$8, priority=$9, updated_at=NOW()
           WHERE id=$10 AND channel_id=$11 AND organization_id=$12
           RETURNING *`,
          [
            String(req.body.name ?? existing.name).trim() || "Pull Source",
            protocol,
            encryptSourceUrl(sourceUrl),
            maskSourceUrl(sourceUrl),
            nextEnabled,
            req.body.auto_start !== undefined
              ? Boolean(req.body.auto_start)
              : existing.auto_start,
            req.body.auto_reconnect !== undefined
              ? Boolean(req.body.auto_reconnect)
              : existing.auto_reconnect,
            nextRole,
            nextPriority,
            existing.id,
            channel.id,
            req.organization.id,
          ],
        );
        await client.query("COMMIT");

        res.json({
          ok: true,
          source: sanitize(
            result.rows[0],
            manager.getRuntimeState(existing.id),
          ),
        });
      } catch (error) {
        try {
          await client.query("ROLLBACK");
        } catch {}
        console.error("Update Pull Source Error:", error);
        res.status(400).json({
          ok: false,
          message: error.message || "Failed to update Pull Source",
          code: error.code || null,
        });
      } finally {
        client.release();
      }
    },
  );

  app.delete(
    "/api/channels/:channelId/pull-source/:id",
    ...manageMw,
    async (req, res) => {
      try {
        const channel = await getOwnedChannel(
          req.params.channelId,
          req.organization.id,
        );
        if (!channel)
          return res
            .status(404)
            .json({ ok: false, message: "Channel not found" });

        const source = await loadSource(
          req.params.id,
          channel.id,
          req.organization.id,
        );
        if (!source)
          return res
            .status(404)
            .json({ ok: false, message: "Pull Source not found" });

        await manager.stopSource(source);
        await pool.query(
          `UPDATE channel_source_failover
           SET active_source_id=NULL, updated_at=NOW()
           WHERE channel_id=$1 AND active_source_id=$2`,
          [channel.id, source.id],
        );
        await pool.query(`DELETE FROM channel_pull_sources WHERE id=$1`, [
          source.id,
        ]);
        res.json({ ok: true, message: "Pull Source removed" });
      } catch (error) {
        console.error("Delete Pull Source Error:", error);
        res
          .status(500)
          .json({ ok: false, message: "Failed to remove Pull Source" });
      }
    },
  );

  app.patch(
    "/api/channels/:channelId/pull-source/failover",
    ...manageMw,
    async (req, res) => {
      try {
        const channel = await getOwnedChannel(
          req.params.channelId,
          req.organization.id,
        );
        if (!channel)
          return res
            .status(404)
            .json({ ok: false, message: "Channel not found" });

        const current = await loadFailover(channel);
        const enabled =
          req.body.enabled !== undefined
            ? Boolean(req.body.enabled)
            : current.enabled;
        const failbackEnabled =
          req.body.failback_enabled !== undefined
            ? Boolean(req.body.failback_enabled)
            : current.failback_enabled;
        const failureThreshold = positiveInt(
          req.body.failure_threshold_seconds,
          Number(current.failure_threshold_seconds || 5),
          1,
          120,
        );
        const failbackStability = positiveInt(
          req.body.failback_stability_seconds,
          Number(current.failback_stability_seconds || 15),
          3,
          600,
        );

        const result = await pool.query(
          `UPDATE channel_source_failover
           SET enabled=$1,
               failure_threshold_seconds=$2,
               failback_enabled=$3,
               failback_stability_seconds=$4,
               updated_at=NOW()
           WHERE channel_id=$5 AND organization_id=$6
           RETURNING *`,
          [
            enabled,
            failureThreshold,
            failbackEnabled,
            failbackStability,
            channel.id,
            req.organization.id,
          ],
        );

        res.json({ ok: true, failover: result.rows[0] });
      } catch (error) {
        console.error("Update Pull Source Failover Error:", error);
        res.status(400).json({
          ok: false,
          message: error.message || "Failed to update failover settings",
        });
      }
    },
  );

  app.post(
    "/api/channels/:channelId/pull-source/:id/preflight",
    ...manageMw,
    async (req, res) => {
      try {
        const channel = await getOwnedChannel(
          req.params.channelId,
          req.organization.id,
        );
        if (!channel)
          return res
            .status(404)
            .json({ ok: false, message: "Channel not found" });
        const source = await loadSource(
          req.params.id,
          channel.id,
          req.organization.id,
        );
        if (!source)
          return res
            .status(404)
            .json({ ok: false, message: "Pull Source not found" });
        const result = await manager.preflightSource(source);
        await manager.recordHealth(
          source.id,
          result.ok ? "ready" : "unhealthy",
        );
        res
          .status(result.ok ? 200 : 400)
          .json({ ok: result.ok, preflight: result });
      } catch (error) {
        res.status(400).json({
          ok: false,
          message: error.message,
          code: error.code || null,
        });
      }
    },
  );

  // Manual start is a safe source switch: the manager stops the current Pull
  // Source worker before giving this source ownership of the canonical key.
  app.post(
    "/api/channels/:channelId/pull-source/:id/start",
    ...manageMw,
    async (req, res) => {
      try {
        const channel = await getOwnedChannel(
          req.params.channelId,
          req.organization.id,
        );
        if (!channel)
          return res
            .status(404)
            .json({ ok: false, message: "Channel not found" });
        const source = await loadSource(
          req.params.id,
          channel.id,
          req.organization.id,
        );
        if (!source)
          return res
            .status(404)
            .json({ ok: false, message: "Pull Source not found" });

        const result = await manager.activateSource(source, channel, {
          reason: "manual_start",
        });
        res.status(result.ok ? 200 : 409).json(result);
      } catch (error) {
        console.error("Start Pull Source Error:", error);
        res.status(500).json({
          ok: false,
          message: error.message || "Failed to start Pull Source",
          code: error.code || null,
        });
      }
    },
  );

  app.post(
    "/api/channels/:channelId/pull-source/:id/switch",
    ...manageMw,
    async (req, res) => {
      try {
        const channel = await getOwnedChannel(
          req.params.channelId,
          req.organization.id,
        );
        if (!channel)
          return res
            .status(404)
            .json({ ok: false, message: "Channel not found" });
        const source = await loadSource(
          req.params.id,
          channel.id,
          req.organization.id,
        );
        if (!source)
          return res
            .status(404)
            .json({ ok: false, message: "Pull Source not found" });

        const result = await manager.activateSource(source, channel, {
          reason: "manual_switch",
        });
        res.status(result.ok ? 200 : 409).json(result);
      } catch (error) {
        console.error("Switch Pull Source Error:", error);
        res.status(500).json({
          ok: false,
          message: error.message || "Failed to switch Pull Source",
        });
      }
    },
  );

  // Intentional Stop never triggers automatic failover. Automatic failover is
  // reserved for transport/source failures detected by the manager.
  app.post(
    "/api/channels/:channelId/pull-source/:id/stop",
    ...manageMw,
    async (req, res) => {
      try {
        const channel = await getOwnedChannel(
          req.params.channelId,
          req.organization.id,
        );
        if (!channel)
          return res
            .status(404)
            .json({ ok: false, message: "Channel not found" });
        const source = await loadSource(
          req.params.id,
          channel.id,
          req.organization.id,
        );
        if (!source)
          return res
            .status(404)
            .json({ ok: false, message: "Pull Source not found" });

        const result = await manager.stopSource(source, {
          clearActive: true,
          reason: "manual_stop",
        });
        res.json(result);
      } catch (error) {
        console.error("Stop Pull Source Error:", error);
        res
          .status(500)
          .json({ ok: false, message: "Failed to stop Pull Source" });
      }
    },
  );

  return manager;
};
