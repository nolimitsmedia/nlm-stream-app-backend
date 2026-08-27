function createHaEventService(pool) {
  async function recordEvent({
    organizationId,
    channelId,
    eventType,
    sourceId = null,
    previousSourceId = null,
    newSourceId = null,
    reason = null,
    status = "completed",
    durationMs = null,
    metadata = {},
  }) {
    if (!organizationId || !channelId || !eventType) {
      console.warn("[HA-EVENT] Skipping invalid event", {
        organizationId,
        channelId,
        eventType,
      });
      return null;
    }

    try {
      const result = await pool.query(
        `
        INSERT INTO channel_ha_events (
          organization_id,
          channel_id,
          event_type,
          source_id,
          previous_source_id,
          new_source_id,
          reason,
          status,
          duration_ms,
          metadata
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb)
        RETURNING *
        `,
        [
          organizationId,
          channelId,
          eventType,
          sourceId,
          previousSourceId,
          newSourceId,
          reason,
          status,
          durationMs,
          JSON.stringify(metadata || {}),
        ],
      );

      const event = result.rows[0];

      console.log(
        `[HA-EVENT] ${event.event_type} channel=${event.channel_id}` +
          ` source=${event.source_id ?? "-"}` +
          ` previous=${event.previous_source_id ?? "-"}` +
          ` new=${event.new_source_id ?? "-"}` +
          ` reason=${event.reason ?? "-"}`,
      );

      return event;
    } catch (error) {
      // HA event history must never interrupt actual stream failover.
      console.error("[HA-EVENT] Failed to record event:", error.message);
      return null;
    }
  }

  async function getChannelHistory(
    channelId,
    organizationId,
    {
      limit = 100,
      eventType = null,
      status = null,
    } = {},
  ) {
    const safeLimit = Math.min(Math.max(Number(limit) || 100, 1), 500);

    const params = [channelId, organizationId];
    const filters = [
      "e.channel_id=$1",
      "e.organization_id=$2",
    ];

    if (eventType) {
      params.push(String(eventType).trim());
      filters.push(`e.event_type=$${params.length}`);
    }

    if (status) {
      params.push(String(status).trim());
      filters.push(`e.status=$${params.length}`);
    }

    params.push(safeLimit);
    const limitParam = `$${params.length}`;

    const result = await pool.query(
      `
      SELECT
        e.id,
        e.organization_id,
        e.channel_id,
        e.event_type,
        e.source_id,
        e.previous_source_id,
        e.new_source_id,
        e.reason,
        e.status,
        e.duration_ms,
        e.metadata,
        e.created_at,

        source.name AS source_name,
        previous_source.name AS previous_source_name,
        new_source.name AS new_source_name

      FROM channel_ha_events e

      LEFT JOIN channel_pull_sources source
        ON source.id=e.source_id

      LEFT JOIN channel_pull_sources previous_source
        ON previous_source.id=e.previous_source_id

      LEFT JOIN channel_pull_sources new_source
        ON new_source.id=e.new_source_id

      WHERE ${filters.join(" AND ")}

      ORDER BY e.created_at DESC, e.id DESC
      LIMIT ${limitParam}
      `,
      params,
    );

    return result.rows;
  }

  return {
    recordEvent,
    getChannelHistory,
  };
}

module.exports = {
  createHaEventService,
};
