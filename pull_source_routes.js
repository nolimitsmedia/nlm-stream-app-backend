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

const { createHaEventService } = require("./ha_event_service");

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
  const haEvents = createHaEventService(pool);

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

  // Read-only HA / failover event history.
  // Tenant isolation is enforced by getOwnedChannel() before history is read.
  app.get(
    "/api/channels/:channelId/ha-health",
    authenticateAdmin,
    resolveOrganizationForRequest,
    async (req, res) => {
      try {
        const channel = await getOwnedChannel(
          req.params.channelId,
          req.organization.id,
        );

        if (!channel) {
          return res.status(404).json({
            ok: false,
            message: "Channel not found",
          });
        }

        const channelId = channel.id;
        const organizationId = req.organization.id;

        const summaryResult = await pool.query(
          `
          SELECT
            COUNT(*) FILTER (
              WHERE event_type = 'primary_failed'
            )::int AS primary_failures,

            COUNT(*) FILTER (
              WHERE event_type = 'failover_started'
            )::int AS failovers_started,

            COUNT(*) FILTER (
              WHERE event_type = 'backup_activated'
            )::int AS backup_activations,

            COUNT(*) FILTER (
              WHERE event_type = 'failback_started'
            )::int AS failbacks_started,

            COUNT(*) FILTER (
              WHERE event_type = 'primary_restored'
            )::int AS primary_restorations,

            COUNT(*) FILTER (
              WHERE event_type = 'switch_failed'
            )::int AS failed_switches,

            COUNT(*) FILTER (
              WHERE event_type IN (
                'source_recovered',
                'backend_restart_recovery'
              )
            )::int AS recoveries,

            MAX(created_at) FILTER (
              WHERE event_type = 'primary_failed'
            ) AS last_primary_failure,

            MAX(created_at) FILTER (
              WHERE event_type = 'backup_activated'
            ) AS last_failover,

            MAX(created_at) FILTER (
              WHERE event_type = 'primary_restored'
            ) AS last_failback,

            MAX(created_at) AS last_ha_event

          FROM channel_ha_events

          WHERE channel_id = $1
            AND organization_id = $2
          `,
          [channelId, organizationId],
        );

        const incidentMetricsResult = await pool.query(
          `
          WITH failures AS (
            SELECT
              id,
              created_at AS failed_at
            FROM channel_ha_events
            WHERE channel_id = $1
              AND organization_id = $2
              AND event_type IN (
                'primary_failed',
                'active_source_failed'
              )
            ORDER BY created_at
          ),

          paired_failovers AS (
            SELECT
              f.id AS failure_event_id,
              f.failed_at,
              b.id AS backup_event_id,
              b.created_at AS backup_activated_at,

              EXTRACT(
                EPOCH FROM (b.created_at - f.failed_at)
              ) * 1000 AS failover_ms

            FROM failures f

            LEFT JOIN LATERAL (
              SELECT
                id,
                created_at
              FROM channel_ha_events e
              WHERE e.channel_id = $1
                AND e.organization_id = $2
                AND e.event_type = 'backup_activated'
                AND e.created_at >= f.failed_at
              ORDER BY e.created_at
              LIMIT 1
            ) b ON TRUE
          ),

          paired_switches AS (
            SELECT
              fs.id AS failover_started_event_id,
              fs.created_at AS failover_started_at,
              ba.id AS backup_activated_event_id,
              ba.created_at AS backup_activated_at,

              EXTRACT(
                EPOCH FROM (
                  ba.created_at - fs.created_at
                )
              ) * 1000 AS switch_ms

            FROM channel_ha_events fs

            LEFT JOIN LATERAL (
              SELECT
                id,
                created_at
              FROM channel_ha_events e
              WHERE e.channel_id = $1
                AND e.organization_id = $2
                AND e.event_type = 'backup_activated'
                AND e.created_at >= fs.created_at
              ORDER BY e.created_at
              LIMIT 1
            ) ba ON TRUE

            WHERE fs.channel_id = $1
              AND fs.organization_id = $2
              AND fs.event_type = 'failover_started'
          ),

          paired_failbacks AS (
            SELECT
              fb.id AS failback_started_event_id,
              fb.created_at AS failback_started_at,
              pr.id AS primary_restored_event_id,
              pr.created_at AS primary_restored_at,

              EXTRACT(
                EPOCH FROM (
                  pr.created_at - fb.created_at
                )
              ) * 1000 AS failback_ms

            FROM channel_ha_events fb

            LEFT JOIN LATERAL (
              SELECT
                id,
                created_at
              FROM channel_ha_events e
              WHERE e.channel_id = $1
                AND e.organization_id = $2
                AND e.event_type = 'primary_restored'
                AND e.created_at >= fb.created_at
              ORDER BY e.created_at
              LIMIT 1
            ) pr ON TRUE

            WHERE fb.channel_id = $1
              AND fb.organization_id = $2
              AND fb.event_type = 'failback_started'
          )

          SELECT
            (
              SELECT COUNT(*)
              FROM paired_failovers
              WHERE backup_activated_at IS NOT NULL
            )::int AS successful_failovers,

            (
              SELECT COUNT(*)
              FROM paired_failovers
              WHERE backup_activated_at IS NULL
            )::int AS incomplete_failovers,

            (
              SELECT ROUND(AVG(failover_ms))::bigint
              FROM paired_failovers
              WHERE failover_ms IS NOT NULL
            ) AS avg_failover_incident_ms,

            (
              SELECT ROUND(MAX(failover_ms))::bigint
              FROM paired_failovers
              WHERE failover_ms IS NOT NULL
            ) AS worst_failover_incident_ms,

            (
              SELECT ROUND(failover_ms)::bigint
              FROM paired_failovers
              WHERE failover_ms IS NOT NULL
              ORDER BY failed_at DESC
              LIMIT 1
            ) AS last_failover_incident_ms,

            (
              SELECT ROUND(AVG(switch_ms))::bigint
              FROM paired_switches
              WHERE switch_ms IS NOT NULL
            ) AS avg_switch_ms,

            (
              SELECT ROUND(switch_ms)::bigint
              FROM paired_switches
              WHERE switch_ms IS NOT NULL
              ORDER BY failover_started_at DESC
              LIMIT 1
            ) AS last_switch_ms,

            (
              SELECT ROUND(AVG(failback_ms))::bigint
              FROM paired_failbacks
              WHERE failback_ms IS NOT NULL
            ) AS avg_failback_ms,

            (
              SELECT ROUND(failback_ms)::bigint
              FROM paired_failbacks
              WHERE failback_ms IS NOT NULL
              ORDER BY failback_started_at DESC
              LIMIT 1
            ) AS last_failback_ms
          `,
          [channelId, organizationId],
        );

        const stateResult = await pool.query(
          `
          SELECT
            f.channel_id,
            f.enabled,
            f.active_source_id,
            f.last_switch_reason,
            f.last_switch_at,

            s.name AS active_source_name,
            s.role AS active_source_role,
            s.priority AS active_source_priority,
            s.status AS active_source_status,
            s.health_status AS active_source_health,
            s.is_running AS active_source_running

          FROM channel_source_failover f

          LEFT JOIN channel_pull_sources s
            ON s.id = f.active_source_id
           AND s.organization_id = $2

          WHERE f.channel_id = $1
          `,
          [channelId, organizationId],
        );

        const sourcesResult = await pool.query(
          `
          SELECT
            id,
            name,
            role,
            priority,
            status,
            health_status,
            is_running,
            is_active_source,
            reconnect_count
          FROM channel_pull_sources
          WHERE channel_id = $1
            AND organization_id = $2
            AND enabled = TRUE
          ORDER BY
            CASE WHEN role = 'primary' THEN 0 ELSE 1 END,
            priority ASC,
            id ASC
          `,
          [channelId, organizationId],
        );

        const summary = summaryResult.rows[0] || {};
        const incidentMetrics = incidentMetricsResult.rows[0] || {};
        const state = stateResult.rows[0] || null;

        const sources = (sourcesResult.rows || []).map((source) => {
          const runtime = manager.getRuntimeState(source.id);

          return {
            ...source,
            runtime: runtime || null,
            bitrate_kbps: Number(runtime?.bitrate_kbps || 0),
            uptime_seconds: Number(runtime?.uptime_seconds || 0),
            out_time_ms: Number(runtime?.out_time_ms || 0),
            runtime_status: runtime?.status || null,
            runtime_last_error: runtime?.last_error || null,
          };
        });

        const activeHealthy =
          Boolean(state?.active_source_running) &&
          String(state?.active_source_status || "").toLowerCase() === "streaming" &&
          String(state?.active_source_health || "").toLowerCase() === "healthy";

        const systemStatus = !state?.enabled
          ? "disabled"
          : activeHealthy
            ? "healthy"
            : state?.active_source_id
              ? "degraded"
              : "unavailable";

        res.json({
          ok: true,
          channel: {
            id: channel.id,
            name: channel.name,
            stream_key: channel.stream_key,
          },
          health: {
            status: systemStatus,
            failover_enabled: Boolean(state?.enabled),
            active_source: state
              ? {
                  id: state.active_source_id,
                  name: state.active_source_name,
                  role: state.active_source_role,
                  priority: state.active_source_priority,
                  status: state.active_source_status,
                  health_status: state.active_source_health,
                  is_running: Boolean(state.active_source_running),
                }
              : null,
            last_switch_reason: state?.last_switch_reason || null,
            last_switch_at: state?.last_switch_at || null,
          },
          metrics: {
            primary_failures: Number(summary.primary_failures || 0),
            failovers_started: Number(summary.failovers_started || 0),
            backup_activations: Number(summary.backup_activations || 0),
            failbacks_started: Number(summary.failbacks_started || 0),
            primary_restorations: Number(summary.primary_restorations || 0),
            failed_switches: Number(summary.failed_switches || 0),
            recoveries: Number(summary.recoveries || 0),
            last_primary_failure: summary.last_primary_failure || null,
            last_failover: summary.last_failover || null,
            last_failback: summary.last_failback || null,
            last_ha_event: summary.last_ha_event || null,

            successful_failovers: Number(
              incidentMetrics.successful_failovers || 0
            ),

            incomplete_failovers: Number(
              incidentMetrics.incomplete_failovers || 0
            ),

            avg_failover_incident_ms:
              incidentMetrics.avg_failover_incident_ms == null
                ? null
                : Number(incidentMetrics.avg_failover_incident_ms),

            worst_failover_incident_ms:
              incidentMetrics.worst_failover_incident_ms == null
                ? null
                : Number(incidentMetrics.worst_failover_incident_ms),

            last_failover_incident_ms:
              incidentMetrics.last_failover_incident_ms == null
                ? null
                : Number(incidentMetrics.last_failover_incident_ms),

            avg_switch_ms:
              incidentMetrics.avg_switch_ms == null
                ? null
                : Number(incidentMetrics.avg_switch_ms),

            last_switch_ms:
              incidentMetrics.last_switch_ms == null
                ? null
                : Number(incidentMetrics.last_switch_ms),

            avg_failback_ms:
              incidentMetrics.avg_failback_ms == null
                ? null
                : Number(incidentMetrics.avg_failback_ms),

            last_failback_ms:
              incidentMetrics.last_failback_ms == null
                ? null
                : Number(incidentMetrics.last_failback_ms),
          },
          sources,
        });
      } catch (error) {
        console.error("Get HA Health Error:", error);

        res.status(500).json({
          ok: false,
          message: "Failed to fetch HA health",
        });
      }
    },
  );

  app.get(
    "/api/channels/:channelId/ha-events",
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

        const limit = positiveInt(req.query.limit, 50, 1, 500);

        const eventType = String(
          req.query.event_type || req.query.eventType || "",
        ).trim() || null;

        const status =
          String(req.query.status || "").trim() || null;

        const events = await haEvents.getChannelHistory(
          channel.id,
          req.organization.id,
          {
            limit,
            eventType,
            status,
          },
        );

        res.json({
          ok: true,
          channel: {
            id: channel.id,
            name: channel.name,
            stream_key: channel.stream_key,
          },
          count: events.length,
          events,
        });
      } catch (error) {
        console.error("Get HA Event History Error:", error);

        res.status(500).json({
          ok: false,
          message: "Failed to fetch HA event history",
        });
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
