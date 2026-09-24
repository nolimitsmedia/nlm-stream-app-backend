// media_node_migration_service.js
// Phase 5F.2 — controlled channel ownership migration between Media Nodes.
//
// Safety contract:
//   1. Source ownership remains authoritative while workers are stopped.
//   2. channels.media_node_id changes only after source workers are verified stopped.
//   3. Pull Source HA transitions are held for the migration critical section.
//   4. Stream Target platform sessions are preserved (endPlatform:false).
//   5. A failed post-commit recovery attempts a compensating rollback.

const ACTIVE_MIGRATION_STATUSES = [
  "requested",
  "validating",
  "prepared",
  "stopping_source",
  "ownership_committing",
];

function migrationError(code, message, details = null) {
  const error = new Error(message);
  error.code = code;
  if (details) error.details = details;
  return error;
}

function safeError(error) {
  return {
    code: String(error?.code || "media_node_migration_failed").slice(0, 64),
    message: String(error?.message || "Media Node migration failed").slice(
      0,
      2000,
    ),
  };
}

function createMediaNodeMigrationManager({
  pool,
  pullSourceManager,
  streamTargetManager,
  heartbeatIntervalMs = 30000,
}) {
  if (!pool) throw new Error("Media Node migration manager requires pool");
  if (!pullSourceManager)
    throw new Error("Media Node migration manager requires pullSourceManager");
  if (!streamTargetManager)
    throw new Error(
      "Media Node migration manager requires streamTargetManager",
    );

  const staleAfterSeconds = Math.max(
    60,
    Math.ceil(Number(heartbeatIntervalMs || 30000) / 1000) * 3,
  );

  async function getChannel(channelId, client = pool) {
    const result = await client.query(
      `SELECT id, organization_id, name, stream_key, is_live, media_node_id
       FROM channels WHERE id=$1 LIMIT 1`,
      [channelId],
    );
    return result.rows[0] || null;
  }

  async function getDestinationNode(nodeId, client = pool) {
    const result = await client.query(
      `SELECT id, name, hostname, host(public_ip) AS public_ip,
              region, status, is_enabled, is_draining,
              max_streams, active_streams, srs_healthy, api_healthy,
              last_heartbeat_at
       FROM media_nodes WHERE id=$1 LIMIT 1`,
      [nodeId],
    );
    return result.rows[0] || null;
  }

  function validateDestinationNode(node) {
    if (!node)
      throw migrationError(
        "destination_not_found",
        "Destination Media Node was not found.",
      );
    if (!node.is_enabled)
      throw migrationError(
        "destination_disabled",
        "Destination Media Node is disabled.",
      );
    if (node.is_draining)
      throw migrationError(
        "destination_draining",
        "Destination Media Node is draining.",
      );

    const heartbeatAgeSeconds = node.last_heartbeat_at
      ? Math.max(
          0,
          Math.floor(
            (Date.now() - new Date(node.last_heartbeat_at).getTime()) / 1000,
          ),
        )
      : null;

    if (
      node.status !== "online" ||
      node.srs_healthy !== true ||
      node.api_healthy !== true ||
      heartbeatAgeSeconds === null ||
      heartbeatAgeSeconds > staleAfterSeconds
    ) {
      throw migrationError(
        "destination_unhealthy",
        "Destination Media Node is offline, stale, or unhealthy.",
        {
          heartbeat_age_seconds: heartbeatAgeSeconds,
          stale_after_seconds: staleAfterSeconds,
        },
      );
    }

    const maxStreams = Number(node.max_streams || 0);
    const activeStreams = Number(node.active_streams || 0);
    if (maxStreams > 0 && activeStreams >= maxStreams) {
      throw migrationError(
        "destination_at_capacity",
        "Destination Media Node is at capacity.",
      );
    }

    return { heartbeatAgeSeconds, maxStreams, activeStreams };
  }

  async function createMigration({
    channel,
    destinationMediaNodeId,
    requestedByAdminId = null,
    reason = null,
    metadata = {},
  }) {
    try {
      const result = await pool.query(
        `INSERT INTO channel_media_node_migrations (
           channel_id, organization_id, source_media_node_id,
           destination_media_node_id, requested_by_admin_id,
           status, reason, metadata
         ) VALUES ($1,$2,$3,$4,$5,'requested',$6,$7::jsonb)
         RETURNING *`,
        [
          channel.id,
          channel.organization_id,
          channel.media_node_id,
          destinationMediaNodeId,
          requestedByAdminId,
          reason || null,
          JSON.stringify(metadata || {}),
        ],
      );
      return result.rows[0];
    } catch (error) {
      if (error?.code === "23505") {
        throw migrationError(
          "migration_already_active",
          "This channel already has an active Media Node migration.",
        );
      }
      throw error;
    }
  }

  async function updateMigration(migrationId, status, patch = {}) {
    const metadata = patch.metadata || {};
    const result = await pool.query(
      `UPDATE channel_media_node_migrations
       SET status=$2,
           started_at=CASE WHEN $2='validating' THEN COALESCE(started_at,NOW()) ELSE started_at END,
           prepared_at=CASE WHEN $2='prepared' THEN COALESCE(prepared_at,NOW()) ELSE prepared_at END,
           ownership_committed_at=CASE WHEN $2='ownership_committing' AND $3::boolean THEN COALESCE(ownership_committed_at,NOW()) ELSE ownership_committed_at END,
           completed_at=CASE WHEN $2='completed' THEN COALESCE(completed_at,NOW()) ELSE completed_at END,
           failed_at=CASE WHEN $2='failed' THEN COALESCE(failed_at,NOW()) ELSE failed_at END,
           failure_code=COALESCE($4,failure_code),
           failure_message=COALESCE($5,failure_message),
           metadata=COALESCE(metadata,'{}'::jsonb) || $6::jsonb,
           updated_at=NOW()
       WHERE id=$1
       RETURNING *`,
      [
        migrationId,
        status,
        Boolean(patch.ownershipCommitted),
        patch.failureCode || null,
        patch.failureMessage || null,
        JSON.stringify(metadata),
      ],
    );
    return result.rows[0] || null;
  }

  async function snapshotWorkloads(channel) {
    const [targetsResult, sourceResult] = await Promise.all([
      pool.query(
        `SELECT * FROM social_destinations
         WHERE channel_id=$1
           AND (is_running=TRUE OR status IN ('streaming','connecting','reconnecting'))
         ORDER BY id`,
        [channel.id],
      ),
      pool.query(
        `SELECT * FROM channel_pull_sources
         WHERE channel_id=$1 AND organization_id=$2
           AND is_active_source=TRUE
         ORDER BY id LIMIT 1`,
        [channel.id, channel.organization_id],
      ),
    ]);

    const activeSource = sourceResult.rows[0] || null;
    if (channel.is_live && !activeSource) {
      throw migrationError(
        "direct_ingest_routing_unavailable",
        "Live direct OBS/SRT publisher migration remains locked in Phase 5F.5. A stable ingest hostname alone cannot hand off an already-connected publisher; the routing layer must be physically validated before this guard can be relaxed.",
      );
    }

    return {
      targets: targetsResult.rows,
      activeSource,
      metadata: {
        source_id: activeSource?.id || null,
        target_ids: targetsResult.rows.map((row) => Number(row.id)),
        channel_was_live: Boolean(channel.is_live),
      },
    };
  }

  async function stopSourceWorkers(snapshot, stopped) {
    for (const target of snapshot.targets) {
      const result = await streamTargetManager.stopTarget(target, {
        endPlatform: false,
      });
      if (!result?.ok) {
        throw migrationError(
          "stream_target_stop_failed",
          `Stream Target #${target.id} did not stop cleanly.`,
        );
      }
      stopped.target_ids.push(Number(target.id));
    }

    if (snapshot.activeSource) {
      const result = await pullSourceManager.stopSource(snapshot.activeSource, {
        clearActive: false,
        reason: "media_node_migration",
      });
      if (!result?.ok) {
        throw migrationError(
          "pull_source_stop_failed",
          `Pull Source #${snapshot.activeSource.id} did not stop cleanly.`,
        );
      }
      stopped.pull_source = true;
    }
  }

  async function commitOwnership({ channel, destinationMediaNodeId }) {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      const locked = await getChannel(channel.id, client);
      if (!locked)
        throw migrationError("channel_not_found", "Channel no longer exists.");
      if (Number(locked.media_node_id) !== Number(channel.media_node_id)) {
        throw migrationError(
          "source_ownership_changed",
          "Channel Media Node ownership changed while migration was preparing.",
        );
      }
      const node = await getDestinationNode(destinationMediaNodeId, client);
      validateDestinationNode(node);
      await client.query(`UPDATE channels SET media_node_id=$1 WHERE id=$2`, [
        destinationMediaNodeId,
        channel.id,
      ]);
      await client.query("COMMIT");
    } catch (error) {
      await client.query("ROLLBACK").catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  }

  async function setOwnership(channelId, expectedNodeId, nextNodeId) {
    const result = await pool.query(
      `UPDATE channels SET media_node_id=$1
       WHERE id=$2 AND media_node_id=$3
       RETURNING id, organization_id, name, stream_key, is_live, media_node_id`,
      [nextNodeId, channelId, expectedNodeId],
    );
    if (!result.rows[0]) {
      throw migrationError(
        "rollback_ownership_conflict",
        "Channel ownership changed before rollback could be committed.",
      );
    }
    return result.rows[0];
  }

  const targetVerifyTimeoutMs = Math.max(
    5000,
    Number(process.env.MEDIA_NODE_MIGRATION_TARGET_VERIFY_TIMEOUT_MS || 20000),
  );

  async function waitForTargetDelivery(targetId) {
    const startedAt = Date.now();
    while (Date.now() - startedAt < targetVerifyTimeoutMs) {
      const runtime = streamTargetManager.getRuntimeState?.(Number(targetId));
      if (runtime?.delivery_verified === true && runtime?.is_running === true) {
        return runtime;
      }
      if (runtime && runtime.worker_running === false) {
        throw migrationError(
          "stream_target_recovery_failed",
          `Stream Target #${targetId} worker stopped before delivery was verified.`,
        );
      }
      await new Promise((resolve) => setTimeout(resolve, 500));
    }
    throw migrationError(
      "stream_target_delivery_not_verified",
      `Stream Target #${targetId} did not verify destination delivery within ${targetVerifyTimeoutMs}ms.`,
    );
  }

  async function recoverWorkloads(
    snapshot,
    channel,
    requestedBy,
    recovered = { pull_source: false, target_ids: [] },
    restoreOnly = null,
  ) {
    const restorePullSource = restoreOnly
      ? Boolean(restoreOnly.pull_source)
      : Boolean(snapshot.activeSource);
    const restoreTargetIds = restoreOnly
      ? new Set((restoreOnly.target_ids || []).map(Number))
      : null;

    if (snapshot.activeSource && restorePullSource) {
      const sourceResult = await pool.query(
        `SELECT * FROM channel_pull_sources
         WHERE id=$1 AND channel_id=$2 AND organization_id=$3 LIMIT 1`,
        [snapshot.activeSource.id, channel.id, channel.organization_id],
      );
      const source = sourceResult.rows[0];
      if (!source)
        throw migrationError(
          "pull_source_missing",
          "Active Pull Source disappeared during migration.",
        );

      const started = await pullSourceManager.startSource(source, channel, {
        reason: "media_node_migration",
        reconnecting: true,
      });
      if (!started?.ok) {
        throw migrationError(
          started?.code || "pull_source_recovery_failed",
          started?.message ||
            "Pull Source failed to recover on destination Media Node.",
        );
      }
      recovered.pull_source = true;
    }

    for (const original of snapshot.targets) {
      if (restoreTargetIds && !restoreTargetIds.has(Number(original.id)))
        continue;

      const targetResult = await pool.query(
        `SELECT * FROM social_destinations WHERE id=$1 AND channel_id=$2 LIMIT 1`,
        [original.id, channel.id],
      );
      const target = targetResult.rows[0];
      if (!target)
        throw migrationError(
          "stream_target_missing",
          `Stream Target #${original.id} disappeared during migration.`,
        );

      const started = await streamTargetManager.startTarget(
        target,
        channel,
        channel.organization_id,
        {
          reconnect: true,
          reuseRuntimeUrl: true,
          requestedBy: requestedBy || "media-node-migration",
        },
      );
      if (!started?.ok) {
        throw migrationError(
          started?.failure?.code || "stream_target_recovery_failed",
          started?.message ||
            `Stream Target #${target.id} failed to recover on destination Media Node.`,
        );
      }

      // Track immediately after start succeeds so a later verification failure
      // still knows which destination worker must be stopped during rollback.
      recovered.target_ids.push(Number(target.id));
      await waitForTargetDelivery(target.id);
    }

    return recovered;
  }

  async function stopRecoveredDestinationWorkers(snapshot, recovered) {
    for (const targetId of [...(recovered?.target_ids || [])].reverse()) {
      const result = await pool.query(
        `SELECT * FROM social_destinations WHERE id=$1 LIMIT 1`,
        [targetId],
      );
      if (result.rows[0]) {
        await streamTargetManager
          .stopTarget(result.rows[0], { endPlatform: false })
          .catch(() => {});
      }
    }
    if (recovered?.pull_source && snapshot.activeSource) {
      const result = await pool.query(
        `SELECT * FROM channel_pull_sources WHERE id=$1 LIMIT 1`,
        [snapshot.activeSource.id],
      );
      if (result.rows[0]) {
        await pullSourceManager
          .stopSource(result.rows[0], {
            clearActive: false,
            reason: "media_node_migration_rollback",
          })
          .catch(() => {});
      }
    }
  }

  async function migrateChannel({
    channelId,
    destinationMediaNodeId,
    requestedByAdminId = null,
    reason = null,
    dryRun = false,
  }) {
    const cleanChannelId = Number(channelId);
    const destinationId = Number(destinationMediaNodeId);
    if (!Number.isInteger(cleanChannelId) || cleanChannelId <= 0)
      throw migrationError(
        "invalid_channel_id",
        "channelId must be a positive integer.",
      );
    if (!Number.isInteger(destinationId) || destinationId <= 0)
      throw migrationError(
        "invalid_destination_id",
        "destinationMediaNodeId must be a positive integer.",
      );

    const channel = await getChannel(cleanChannelId);
    if (!channel)
      throw migrationError("channel_not_found", "Channel not found.");
    const sourceNodeId = Number(channel.media_node_id);
    if (!Number.isInteger(sourceNodeId) || sourceNodeId <= 0)
      throw migrationError(
        "source_node_missing",
        "Channel has no assigned source Media Node.",
      );
    if (sourceNodeId === destinationId)
      throw migrationError(
        "same_media_node",
        "Source and destination Media Nodes must be different.",
      );

    const destination = await getDestinationNode(destinationId);
    const destinationValidation = validateDestinationNode(destination);
    const snapshot = await snapshotWorkloads(channel);

    if (dryRun) {
      return {
        ok: true,
        dry_run: true,
        channel_id: cleanChannelId,
        source_media_node_id: sourceNodeId,
        destination_media_node_id: destinationId,
        destination: {
          id: destination.id,
          name: destination.name,
          region: destination.region,
          heartbeat_age_seconds: destinationValidation.heartbeatAgeSeconds,
        },
        workload: snapshot.metadata,
      };
    }

    const migration = await createMigration({
      channel,
      destinationMediaNodeId: destinationId,
      requestedByAdminId,
      reason,
      metadata: { workload: snapshot.metadata },
    });

    let holdAcquired = false;
    let ownershipCommitted = false;
    const stopped = { pull_source: false, target_ids: [] };
    let recovered = { pull_source: false, target_ids: [] };
    let rollback = null;

    try {
      await updateMigration(migration.id, "validating", {
        metadata: { destination_validation: destinationValidation },
      });

      holdAcquired = pullSourceManager.beginMediaNodeMigration(cleanChannelId);
      if (!holdAcquired) {
        throw migrationError(
          "ha_transition_busy",
          "Another Pull Source HA transition is already active for this channel.",
        );
      }

      await updateMigration(migration.id, "prepared", {
        metadata: { workload: snapshot.metadata },
      });
      await updateMigration(migration.id, "stopping_source");
      await stopSourceWorkers(snapshot, stopped);

      await updateMigration(migration.id, "ownership_committing");
      await commitOwnership({ channel, destinationMediaNodeId: destinationId });
      ownershipCommitted = true;
      await updateMigration(migration.id, "ownership_committing", {
        ownershipCommitted: true,
      });

      const destinationChannel = await getChannel(cleanChannelId);
      await recoverWorkloads(
        snapshot,
        destinationChannel,
        requestedByAdminId
          ? `admin:${requestedByAdminId}`
          : "media-node-migration",
        recovered,
      );

      const completed = await updateMigration(migration.id, "completed", {
        metadata: { recovered },
      });

      return {
        ok: true,
        migration: completed,
        channel: destinationChannel,
        recovered,
      };
    } catch (error) {
      const failure = safeError(error);

      if (ownershipCommitted) {
        rollback = { attempted: true, ok: false };
        try {
          await stopRecoveredDestinationWorkers(snapshot, recovered);
          const sourceChannel = await setOwnership(
            cleanChannelId,
            destinationId,
            sourceNodeId,
          );
          const restored = await recoverWorkloads(
            snapshot,
            sourceChannel,
            "media-node-migration-rollback",
          );
          rollback = { attempted: true, ok: true, restored };
        } catch (rollbackError) {
          rollback = {
            attempted: true,
            ok: false,
            error: safeError(rollbackError),
          };
        }
      } else if (stopped.pull_source || stopped.target_ids.length > 0) {
        // Ownership never moved. Restore any source workers already stopped.
        try {
          const sourceChannel = await getChannel(cleanChannelId);
          const restored = await recoverWorkloads(
            snapshot,
            sourceChannel,
            "media-node-migration-abort-recovery",
            { pull_source: false, target_ids: [] },
            stopped,
          );
          rollback = {
            attempted: true,
            ok: true,
            restored,
            ownership_unchanged: true,
          };
        } catch (rollbackError) {
          rollback = {
            attempted: true,
            ok: false,
            ownership_unchanged: true,
            error: safeError(rollbackError),
          };
        }
      } else {
        rollback = { attempted: false, ownership_unchanged: true };
      }

      const failed = await updateMigration(migration.id, "failed", {
        failureCode: failure.code,
        failureMessage: failure.message,
        metadata: { rollback },
      }).catch(() => null);

      error.migration = failed;
      error.rollback = rollback;
      throw error;
    } finally {
      if (holdAcquired) pullSourceManager.endMediaNodeMigration(cleanChannelId);
    }
  }

  async function listMigrations(channelId, limit = 25) {
    const safeLimit = Math.max(1, Math.min(100, Number(limit || 25)));
    const result = await pool.query(
      `SELECT * FROM channel_media_node_migrations
       WHERE channel_id=$1 ORDER BY id DESC LIMIT $2`,
      [Number(channelId), safeLimit],
    );
    return result.rows;
  }

  async function getActiveMigration(channelId) {
    const result = await pool.query(
      `SELECT * FROM channel_media_node_migrations
       WHERE channel_id=$1 AND status = ANY($2::varchar[])
       ORDER BY id DESC LIMIT 1`,
      [Number(channelId), ACTIVE_MIGRATION_STATUSES],
    );
    return result.rows[0] || null;
  }

  async function listClusterMigrations(options = {}) {
    const safeLimit = Math.max(1, Math.min(200, Number(options.limit || 50)));
    const activeOnly = options.activeOnly === true;
    const nodeId = Number(options.nodeId || 0);
    const values = [];
    const where = [];

    if (activeOnly) {
      values.push(ACTIVE_MIGRATION_STATUSES);
      where.push(`m.status = ANY($${values.length}::varchar[])`);
    }
    if (Number.isInteger(nodeId) && nodeId > 0) {
      values.push(nodeId);
      where.push(
        `(m.source_media_node_id=$${values.length} OR m.destination_media_node_id=$${values.length})`,
      );
    }

    values.push(safeLimit);
    const result = await pool.query(
      `SELECT m.*,
              c.name AS channel_name,
              c.media_node_id AS current_media_node_id,
              c.is_live AS channel_is_live,
              src.name AS source_media_node_name,
              dst.name AS destination_media_node_name
       FROM channel_media_node_migrations m
       LEFT JOIN channels c ON c.id=m.channel_id
       LEFT JOIN media_nodes src ON src.id=m.source_media_node_id
       LEFT JOIN media_nodes dst ON dst.id=m.destination_media_node_id
       ${where.length ? `WHERE ${where.join(" AND ")}` : ""}
       ORDER BY m.id DESC
       LIMIT $${values.length}`,
      values,
    );
    return result.rows;
  }

  async function getNodeOperations(nodeId) {
    const cleanNodeId = Number(nodeId);
    if (!Number.isInteger(cleanNodeId) || cleanNodeId <= 0) {
      throw codedError(
        "invalid_media_node_id",
        "Media Node id must be a positive integer.",
      );
    }

    const nodeResult = await pool.query(
      `SELECT id, name, status, is_enabled, is_draining, max_streams,
              active_streams, srs_healthy, api_healthy, last_heartbeat_at,
              last_error
       FROM media_nodes WHERE id=$1 LIMIT 1`,
      [cleanNodeId],
    );
    const node = nodeResult.rows[0];
    if (!node)
      throw codedError("media_node_not_found", "Media Node was not found.");

    const channelsResult = await pool.query(
      `SELECT id, organization_id, name, is_live, media_node_id
       FROM channels
       WHERE media_node_id=$1
       ORDER BY is_live DESC, name ASC, id ASC`,
      [cleanNodeId],
    );
    const migrations = await listClusterMigrations({
      nodeId: cleanNodeId,
      activeOnly: true,
      limit: 200,
    });

    const assigned = channelsResult.rows.map((row) => ({
      ...row,
      id: Number(row.id),
      organization_id: Number(row.organization_id),
      media_node_id: Number(row.media_node_id),
    }));
    const liveAssigned = assigned.filter((row) => row.is_live === true);

    return {
      node: {
        ...node,
        id: Number(node.id),
        max_streams: Number(node.max_streams || 0),
        active_streams: Number(node.active_streams || 0),
      },
      assigned_channels: assigned,
      active_migrations: migrations,
      evacuation: {
        requested: node.is_draining === true,
        assigned_channels: assigned.length,
        live_assigned_channels: liveAssigned.length,
        active_migrations: migrations.length,
        ready_to_disable:
          node.is_draining === true &&
          assigned.length === 0 &&
          Number(node.active_streams || 0) === 0 &&
          migrations.length === 0,
        note:
          node.is_draining === true && assigned.length > 0
            ? "Node is draining but still owns channels. Existing assignments are intentionally preserved until controlled migration or reassignment."
            : null,
      },
    };
  }

  async function reconcileIncompleteMigrations(options = {}) {
    const rows = await listClusterMigrations({ activeOnly: true, limit: 200 });
    const results = [];

    for (const migration of rows) {
      const channelId = Number(migration.channel_id);
      const sourceNodeId = Number(migration.source_media_node_id || 0);
      const destinationNodeId = Number(
        migration.destination_media_node_id || 0,
      );
      const currentNodeId = Number(migration.current_media_node_id || 0);
      const ownershipCommitted = Boolean(migration.ownership_committed_at);

      let classification;
      let failureCode;
      let message;

      if (currentNodeId > 0 && currentNodeId === sourceNodeId) {
        classification = ownershipCommitted
          ? "ownership_rolled_back_before_restart"
          : "precommit_abandoned";
        failureCode = ownershipCommitted
          ? "backend_restart_ownership_rolled_back"
          : "backend_restart_precommit_abandoned";
        message = ownershipCommitted
          ? "Backend restarted during migration; channel ownership is on the source node. Existing media reconcilers remain authoritative for worker recovery."
          : "Backend restarted before migration ownership committed. Channel ownership remains on the source node; existing media reconcilers remain authoritative for worker recovery.";
      } else if (currentNodeId > 0 && currentNodeId === destinationNodeId) {
        classification = "postcommit_requires_review";
        failureCode = "backend_restart_postcommit_requires_review";
        message =
          "Backend restarted after channel ownership moved to the destination node. Ownership is preserved; worker state is not guessed and must be verified through existing media reconciliation/operations tooling.";
      } else {
        classification = "ownership_ambiguous_requires_review";
        failureCode = "backend_restart_ownership_ambiguous";
        message =
          "Backend restarted with migration ownership that does not match the recorded source or destination. No ownership or media-worker action was taken.";
      }

      const reconciliation = {
        version: 1,
        trigger: options.trigger || "backend_startup",
        classification,
        previous_status: migration.status,
        current_media_node_id: currentNodeId || null,
        source_media_node_id: sourceNodeId || null,
        destination_media_node_id: destinationNodeId || null,
        ownership_committed_at: migration.ownership_committed_at || null,
        media_actions_taken: false,
        ownership_changed: false,
        reconciled_at: new Date().toISOString(),
      };

      const updated = await updateMigration(migration.id, "failed", {
        failureCode,
        failureMessage: message,
        metadata: { reconciliation },
      });

      results.push({
        migration_id: Number(migration.id),
        channel_id: channelId,
        classification,
        failure_code: failureCode,
        current_media_node_id: currentNodeId || null,
        migration: updated,
      });
    }

    return {
      ok: true,
      inspected: rows.length,
      reconciled: results.length,
      results,
    };
  }

  return {
    migrateChannel,
    listMigrations,
    getActiveMigration,
    listClusterMigrations,
    getNodeOperations,
    reconcileIncompleteMigrations,
    validateDestinationNode,
  };
}

module.exports = { createMediaNodeMigrationManager, ACTIVE_MIGRATION_STATUSES };
