// pull_source_routes.js
// Organization-scoped HTTP API for one external Pull Source per channel.

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
      `SELECT * FROM channel_pull_sources WHERE id=$1 AND channel_id=$2 AND organization_id=$3`,
      [id, channelId, organizationId],
    );
    return result.rows[0] || null;
  }

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

        const result = await pool.query(
          `SELECT * FROM channel_pull_sources WHERE channel_id=$1 AND organization_id=$2 LIMIT 1`,
          [channel.id, req.organization.id],
        );
        const source = result.rows[0] || null;
        res.json({
          ok: true,
          source: source
            ? sanitize(source, manager.getRuntimeState(source.id))
            : null,
          supported_protocols: SUPPORTED_PROTOCOLS,
        });
      } catch (error) {
        console.error("Get Pull Source Error:", error);
        res
          .status(500)
          .json({ ok: false, message: "Failed to fetch Pull Source" });
      }
    },
  );

  app.post(
    "/api/channels/:channelId/pull-source",
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

        const result = await pool.query(
          `INSERT INTO channel_pull_sources
             (channel_id, organization_id, name, protocol, source_url, source_url_display,
              enabled, auto_start, auto_reconnect, status, is_running, updated_at)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'stopped',FALSE,NOW())
           ON CONFLICT (channel_id)
           DO UPDATE SET
             name=EXCLUDED.name,
             protocol=EXCLUDED.protocol,
             source_url=EXCLUDED.source_url,
             source_url_display=EXCLUDED.source_url_display,
             enabled=EXCLUDED.enabled,
             auto_start=EXCLUDED.auto_start,
             auto_reconnect=EXCLUDED.auto_reconnect,
             updated_at=NOW()
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
          ],
        );

        res.json({ ok: true, source: sanitize(result.rows[0]) });
      } catch (error) {
        console.error("Create Pull Source Error:", error);
        res
          .status(400)
          .json({
            ok: false,
            message: error.message || "Failed to save Pull Source",
            code: error.code || null,
          });
      }
    },
  );

  app.put(
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

        const configurationChanged =
          protocol !== existing.protocol ||
          sourceUrl !== currentUrl ||
          (req.body.name !== undefined &&
            String(req.body.name).trim() !==
              String(existing.name || "").trim());

        if (existing.is_running && configurationChanged && nextEnabled) {
          return res.status(409).json({
            ok: false,
            message:
              "Stop the Pull Source before changing its URL, protocol, or name.",
          });
        }

        // Always call stop when disabling, even if DB currently says not running:
        // the manager may own a reconnect/wait timer that also needs cancelling.
        if (!nextEnabled) await manager.stopSource(existing);

        const result = await pool.query(
          `UPDATE channel_pull_sources
           SET name=$1, protocol=$2, source_url=$3, source_url_display=$4,
               enabled=$5, auto_start=$6, auto_reconnect=$7, updated_at=NOW()
           WHERE id=$8 AND channel_id=$9 AND organization_id=$10
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
            existing.id,
            channel.id,
            req.organization.id,
          ],
        );
        res.json({
          ok: true,
          source: sanitize(
            result.rows[0],
            manager.getRuntimeState(existing.id),
          ),
        });
      } catch (error) {
        console.error("Update Pull Source Error:", error);
        res
          .status(400)
          .json({
            ok: false,
            message: error.message || "Failed to update Pull Source",
            code: error.code || null,
          });
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
        res
          .status(result.ok ? 200 : 400)
          .json({ ok: result.ok, preflight: result });
      } catch (error) {
        res
          .status(400)
          .json({
            ok: false,
            message: error.message,
            code: error.code || null,
          });
      }
    },
  );

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
        const result = await manager.startSource(source, channel);
        res.status(result.ok ? 200 : 409).json(result);
      } catch (error) {
        console.error("Start Pull Source Error:", error);
        res
          .status(500)
          .json({
            ok: false,
            message: error.message || "Failed to start Pull Source",
            code: error.code || null,
          });
      }
    },
  );

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
        res.json(await manager.stopSource(source));
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
