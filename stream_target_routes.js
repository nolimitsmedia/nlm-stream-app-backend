// stream_target_routes.js
// Phase 2 — Generic Stream Targets HTTP API.

const {
  createStreamTargetManager,
  TARGET_TYPES,
} = require("./stream_target_service");

function firstDefined(...values) {
  return values.find((value) => value !== undefined && value !== null);
}

function normalizeTargetRequestBody(body = {}) {
  const name = String(
    firstDefined(body.name, body.targetName, body.target_name, "") || "",
  ).trim();
  const destinationUrl = String(
    firstDefined(
      body.destination_url,
      body.destinationUrl,
      body.url,
      body.server_url,
      body.serverUrl,
      "",
    ) || "",
  ).trim();
  const streamKey = String(
    firstDefined(body.stream_key, body.streamKey, body.key, "") || "",
  ).trim();

  return {
    ...body,
    name,
    destination_url: destinationUrl,
    stream_key: streamKey,
  };
}

function sanitizeTarget(row, runtimeState = null) {
  if (!row) return row;
  const merged = { ...row, ...(runtimeState || {}) };
  // Do not echo active OAuth ingest URLs back to the browser; they contain
  // per-broadcast credentials. Manual stream_key remains available because the
  // existing UI already manages it and admins need to edit it.
  if (merged.automation_mode === "oauth") merged.active_destination_url = null;
  const runtimeUptime = Number(runtimeState?.uptime_seconds);
  const startedAt = merged.started_at
    ? new Date(merged.started_at).getTime()
    : null;

  if (runtimeState && Number.isFinite(runtimeUptime)) {
    merged.uptime_seconds = Math.max(0, Math.floor(runtimeUptime));
  } else {
    merged.uptime_seconds =
      merged.is_running && startedAt
        ? Math.max(0, Math.floor((Date.now() - startedAt) / 1000))
        : 0;
  }
  return merged;
}

module.exports = function registerStreamTargetRoutes(app, pool, deps) {
  const {
    authenticateAdmin,
    resolveOrganizationForRequest,
    requireRole,
    requireOrganizationRole,
    getInternalHlsSourceUrl,
    inputResilienceFlags,
  } = deps;

  const manager = createStreamTargetManager({
    pool,
    getInternalHlsSourceUrl,
    inputResilienceFlags,
  });

  async function getOwnedChannel(channelId, organizationId) {
    const result = await pool.query(
      `SELECT * FROM channels WHERE id = $1 AND organization_id = $2`,
      [channelId, organizationId],
    );
    return result.rows[0] || null;
  }

  async function loadTarget(id, channelId) {
    const result = await pool.query(
      `SELECT * FROM social_destinations WHERE id = $1 AND channel_id = $2`,
      [id, channelId],
    );
    return result.rows[0] || null;
  }

  const manageMw = [
    authenticateAdmin,
    resolveOrganizationForRequest,
    requireRole("super_admin", "admin", "operator"),
    requireOrganizationRole("owner", "admin"),
  ];

  async function listHandler(req, res) {
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
        `SELECT * FROM social_destinations WHERE channel_id = $1 ORDER BY id`,
        [channel.id],
      );
      const targets = result.rows.map((row) =>
        sanitizeTarget(row, manager.getRuntimeState(row.id)),
      );
      res.json({
        ok: true,
        targets,
        destinations: targets,
        target_types: TARGET_TYPES,
      });
    } catch (error) {
      console.error("Get Stream Targets Error:", error);
      res
        .status(500)
        .json({ ok: false, message: "Failed to fetch stream targets" });
    }
  }

  app.get(
    "/api/channels/:channelId/stream-targets",
    authenticateAdmin,
    resolveOrganizationForRequest,
    listHandler,
  );
  // Compatibility for an older frontend during rolling deployment.
  app.get(
    "/api/channels/:channelId/social-destinations",
    authenticateAdmin,
    resolveOrganizationForRequest,
    listHandler,
  );

  async function createHandler(req, res) {
    try {
      const body = normalizeTargetRequestBody(req.body);
      const channel = await getOwnedChannel(
        req.params.channelId,
        req.organization.id,
      );
      if (!channel)
        return res
          .status(404)
          .json({ ok: false, message: "Channel not found" });

      const targetType = manager.normalizeTargetType(
        body.target_type || body.platform,
      );
      if (!targetType)
        return res
          .status(400)
          .json({ ok: false, message: "Unsupported target type" });
      const config = TARGET_TYPES[targetType];
      const automationMode =
        body.automation_mode === "oauth" ? "oauth" : "manual";
      if (automationMode === "oauth" && !config.oauth) {
        return res.status(400).json({
          ok: false,
          message:
            "OAuth automation is only available for Facebook and YouTube",
        });
      }

      const protocol = manager.normalizeProtocol(body.protocol, targetType);
      const targetDraft = {
        target_type: targetType,
        platform: targetType,
        automation_mode: automationMode,
        protocol,
        destination_url: body.destination_url || null,
        stream_key: body.stream_key || null,
      };
      const validationError = manager.validateManualTarget(targetDraft);
      if (validationError && automationMode !== "oauth") {
        return res.status(400).json({ ok: false, message: validationError });
      }

      // Preserve the legacy (channel_id, platform) unique constraint without
      // preventing multiple generic/custom targets. Native Facebook/YouTube
      // keep their canonical platform key so existing OAuth code still finds
      // them; everything else receives a short internal unique key.
      const nativePlatformTypes = [
        "facebook",
        "youtube",
        "instagram",
        "twitch",
        "linkedin",
        "x",
      ];
      const internalPlatform = nativePlatformTypes.includes(targetType)
        ? targetType
        : manager.makeInternalPlatformKey(targetType);

      let result;
      if (nativePlatformTypes.includes(targetType)) {
        result = await pool.query(
          `INSERT INTO social_destinations
             (channel_id, platform, stream_key, name, target_type, destination_url,
              protocol, automation_mode, enabled, auto_start, auto_reconnect,
              status, updated_at)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'stopped',now())
           ON CONFLICT (channel_id, platform)
           DO UPDATE SET
             name = EXCLUDED.name,
             target_type = EXCLUDED.target_type,
             destination_url = EXCLUDED.destination_url,
             stream_key = EXCLUDED.stream_key,
             protocol = EXCLUDED.protocol,
             automation_mode = EXCLUDED.automation_mode,
             enabled = EXCLUDED.enabled,
             auto_start = EXCLUDED.auto_start,
             auto_reconnect = EXCLUDED.auto_reconnect,
             updated_at = now()
           RETURNING *`,
          [
            channel.id,
            internalPlatform,
            body.stream_key ||
              (automationMode === "oauth" ? "oauth-managed" : null),
            body.name || config.label,
            targetType,
            body.destination_url || null,
            protocol,
            automationMode,
            body.enabled !== false,
            Boolean(body.auto_start),
            body.auto_reconnect !== false,
          ],
        );
      } else {
        result = await pool.query(
          `INSERT INTO social_destinations
             (channel_id, platform, stream_key, name, target_type, destination_url,
              protocol, automation_mode, enabled, auto_start, auto_reconnect,
              status, updated_at)
           VALUES ($1,$2,$3,$4,$5,$6,$7,'manual',$8,$9,$10,'stopped',now())
           RETURNING *`,
          [
            channel.id,
            internalPlatform,
            body.stream_key || (protocol === "srt" ? "url-managed" : null),
            body.name || config.label,
            targetType,
            body.destination_url || null,
            protocol,
            body.enabled !== false,
            Boolean(body.auto_start),
            body.auto_reconnect !== false,
          ],
        );
      }
      res.json({
        ok: true,
        target: sanitizeTarget(result.rows[0]),
        destination: sanitizeTarget(result.rows[0]),
      });
    } catch (error) {
      console.error("Create Stream Target Error:", error);
      res.status(500).json({
        ok: false,
        message: "Failed to create stream target",
        error: error.message,
      });
    }
  }

  app.post(
    "/api/channels/:channelId/stream-targets",
    ...manageMw,
    createHandler,
  );
  app.post(
    "/api/channels/:channelId/social-destinations",
    ...manageMw,
    createHandler,
  );

  app.put(
    "/api/channels/:channelId/stream-targets/:id",
    ...manageMw,
    async (req, res) => {
      try {
        const body = normalizeTargetRequestBody(req.body);
        const channel = await getOwnedChannel(
          req.params.channelId,
          req.organization.id,
        );
        if (!channel)
          return res
            .status(404)
            .json({ ok: false, message: "Channel not found" });
        const existing = await loadTarget(req.params.id, channel.id);
        if (!existing)
          return res
            .status(404)
            .json({ ok: false, message: "Stream target not found" });

        const targetType = manager.normalizeTargetType(
          body.target_type || existing.target_type || existing.platform,
        );
        const protocol = manager.normalizeProtocol(
          body.protocol || existing.protocol,
          targetType,
        );
        const next = {
          ...existing,
          target_type: targetType,
          protocol,
          name: body.name !== "" ? body.name : existing.name,
          destination_url:
            body.destination_url !== ""
              ? body.destination_url
              : existing.destination_url,
          stream_key:
            body.stream_key !== "" ? body.stream_key : existing.stream_key,
          enabled:
            req.body.enabled !== undefined
              ? Boolean(req.body.enabled)
              : existing.enabled,
          auto_start:
            req.body.auto_start !== undefined
              ? Boolean(body.auto_start)
              : existing.auto_start,
          auto_reconnect:
            req.body.auto_reconnect !== undefined
              ? Boolean(req.body.auto_reconnect)
              : existing.auto_reconnect,
        };
        const validationError = manager.validateManualTarget(next);
        if (validationError && next.automation_mode !== "oauth")
          return res.status(400).json({ ok: false, message: validationError });

        if (existing.is_running && next.enabled === false)
          await manager.stopTarget(existing);

        const result = await pool.query(
          `UPDATE social_destinations
         SET name=$1, target_type=$2, destination_url=$3, stream_key=$4,
             protocol=$5, enabled=$6, auto_start=$7, auto_reconnect=$8, updated_at=now()
         WHERE id=$9 AND channel_id=$10
         RETURNING *`,
          [
            next.name,
            targetType,
            next.destination_url || null,
            next.stream_key || null,
            protocol,
            next.enabled,
            next.auto_start,
            next.auto_reconnect,
            existing.id,
            channel.id,
          ],
        );
        res.json({
          ok: true,
          target: sanitizeTarget(
            result.rows[0],
            manager.getRuntimeState(existing.id),
          ),
        });
      } catch (error) {
        console.error("Update Stream Target Error:", error);
        res
          .status(500)
          .json({ ok: false, message: "Failed to update stream target" });
      }
    },
  );

  async function deleteHandler(req, res) {
    try {
      const channel = await getOwnedChannel(
        req.params.channelId,
        req.organization.id,
      );
      if (!channel)
        return res
          .status(404)
          .json({ ok: false, message: "Channel not found" });
      const target = await loadTarget(req.params.id, channel.id);
      if (!target)
        return res
          .status(404)
          .json({ ok: false, message: "Stream target not found" });
      if (target.is_running) await manager.stopTarget(target);
      await pool.query(
        `DELETE FROM social_destinations WHERE id=$1 AND channel_id=$2`,
        [target.id, channel.id],
      );
      res.json({ ok: true, message: "Stream target removed" });
    } catch (error) {
      console.error("Delete Stream Target Error:", error);
      res
        .status(500)
        .json({ ok: false, message: "Failed to remove stream target" });
    }
  }

  app.delete(
    "/api/channels/:channelId/stream-targets/:id",
    ...manageMw,
    deleteHandler,
  );
  app.delete(
    "/api/channels/:channelId/social-destinations/:id",
    ...manageMw,
    deleteHandler,
  );

  async function startHandler(req, res) {
    try {
      const channel = await getOwnedChannel(
        req.params.channelId,
        req.organization.id,
      );
      if (!channel)
        return res
          .status(404)
          .json({ ok: false, message: "Channel not found" });
      const target = await loadTarget(req.params.id, channel.id);
      if (!target)
        return res
          .status(404)
          .json({ ok: false, message: "Stream target not found" });
      const result = await manager.startTarget(
        target,
        channel,
        req.organization.id,
      );
      res.status(result.ok ? 200 : 400).json(result);
    } catch (error) {
      console.error("Start Stream Target Error:", error);
      res.status(500).json({
        ok: false,
        message: error.message || "Failed to start stream target",
      });
    }
  }

  async function stopHandler(req, res) {
    try {
      const channel = await getOwnedChannel(
        req.params.channelId,
        req.organization.id,
      );
      if (!channel)
        return res
          .status(404)
          .json({ ok: false, message: "Channel not found" });
      const target = await loadTarget(req.params.id, channel.id);
      if (!target)
        return res
          .status(404)
          .json({ ok: false, message: "Stream target not found" });
      const result = await manager.stopTarget(target);
      res.json(result);
    } catch (error) {
      console.error("Stop Stream Target Error:", error);
      res
        .status(500)
        .json({ ok: false, message: "Failed to stop stream target" });
    }
  }

  app.post(
    "/api/channels/:channelId/stream-targets/:id/start",
    ...manageMw,
    startHandler,
  );
  app.post(
    "/api/channels/:channelId/stream-targets/:id/stop",
    ...manageMw,
    stopHandler,
  );
  // Compatibility aliases for existing SocialDestinations.jsx.
  app.post(
    "/api/channels/:channelId/social-destinations/:id/start",
    ...manageMw,
    startHandler,
  );
  app.post(
    "/api/channels/:channelId/social-destinations/:id/stop",
    ...manageMw,
    stopHandler,
  );
  app.post(
    "/api/channels/:channelId/social-destinations/:id/go-live",
    ...manageMw,
    startHandler,
  );
  app.post(
    "/api/channels/:channelId/social-destinations/:id/end-live",
    ...manageMw,
    stopHandler,
  );

  app.get("/api/stream-target-types", authenticateAdmin, (req, res) => {
    res.json({ ok: true, target_types: TARGET_TYPES });
  });

  return manager;
};
