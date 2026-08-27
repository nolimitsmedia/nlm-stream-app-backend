const os = require("os");
const { execSync } = require("child_process");

const DEFAULT_INTERVAL_MS = 30 * 1000;

/**
 * Collect local system metrics for this media node.
 *
 * CPU percentage intentionally follows the same basic calculation used
 * elsewhere in the NLM Streaming backend:
 *
 *   1-minute load average / CPU count * 100
 *
 * This keeps node-level telemetry consistent with the existing dashboard.
 */
function getLocalSystemMetrics() {
  const totalMem = os.totalmem();
  const freeMem = os.freemem();
  const usedMem = totalMem - freeMem;

  const loadAvg = os.loadavg();
  const cpuCount = os.cpus().length || 1;

  const cpuPercent = Math.min(
    100,
    Math.max(0, Math.round(((loadAvg[0] || 0) / cpuCount) * 100)),
  );

  const memoryPercent = totalMem
    ? Math.min(100, Math.max(0, Math.round((usedMem / totalMem) * 100)))
    : 0;

  let diskPercent = null;

  try {
    const output = execSync("df -k / | tail -1", {
      encoding: "utf8",
      timeout: 3000,
    }).trim();

    const parts = output.split(/\s+/);

    const totalKb = Number(parts[1] || 0);
    const usedKb = Number(parts[2] || 0);

    if (Number.isFinite(totalKb) && totalKb > 0) {
      diskPercent = Math.min(
        100,
        Math.max(0, Math.round((usedKb / totalKb) * 100)),
      );
    }
  } catch (error) {
    console.warn("[MediaNode] Disk metric unavailable:", error.message);
  }

  return {
    cpu_percent: cpuPercent,
    memory_percent: memoryPercent,
    disk_percent: diskPercent,
    load_1m: Number(loadAvg[0] || 0),
  };
}

/**
 * Check the local SRS API and count active published streams.
 */
async function getSrsHealth(srsApiUrl) {
  const baseUrl = String(srsApiUrl || "http://localhost:1985").replace(
    /\/$/,
    "",
  );

  try {
    const controller = new AbortController();

    const timeout = setTimeout(() => {
      controller.abort();
    }, 5000);

    timeout.unref?.();

    let response;

    try {
      response = await fetch(`${baseUrl}/api/v1/streams`, {
        signal: controller.signal,
      });
    } finally {
      clearTimeout(timeout);
    }

    if (!response.ok) {
      return {
        healthy: false,
        active_streams: 0,
        error: `SRS HTTP ${response.status}`,
      };
    }

    const data = await response.json();

    const streams = Array.isArray(data?.streams) ? data.streams : [];

    const activeStreams = streams.filter(
      (stream) => stream?.publish?.active === true,
    );

    return {
      healthy: true,
      active_streams: activeStreams.length,
      error: null,
    };
  } catch (error) {
    const message =
      error?.name === "AbortError"
        ? "SRS health check timed out"
        : error?.message || "SRS health check failed";

    return {
      healthy: false,
      active_streams: 0,
      error: message,
    };
  }
}

/**
 * Count all FFmpeg processes currently running on this media node.
 *
 * NOTE:
 * This is infrastructure telemetry only. It should not be interpreted
 * as a count of customer transcodes because FFmpeg may also be used for
 * Pull Sources, recording, stream targets, probes, and other workloads.
 */
function getFfmpegProcessCount() {
  try {
    const output = execSync("pgrep -c -x ffmpeg || true", {
      encoding: "utf8",
      timeout: 3000,
    }).trim();

    const count = Number(output || 0);

    return Number.isFinite(count) ? Math.max(0, count) : 0;
  } catch (error) {
    console.warn(
      "[MediaNode] FFmpeg process count unavailable:",
      error.message,
    );

    return 0;
  }
}

/**
 * Perform one heartbeat and persist the current media-node state.
 */
async function updateMediaNodeHeartbeat({
  pool,
  nodeId,
  srsApiUrl = "http://localhost:1985",
}) {
  if (!pool) {
    throw new Error("Media node heartbeat requires a database pool");
  }

  if (!nodeId) {
    throw new Error("Media node heartbeat requires nodeId");
  }

  const system = getLocalSystemMetrics();

  const srs = await getSrsHealth(srsApiUrl);

  const ffmpegProcesses = getFfmpegProcessCount();

  const nodeStatus = srs.healthy ? "online" : "degraded";

  const result = await pool.query(
    `
    UPDATE media_nodes
    SET
      hostname = $2,
      status = $3,
      active_streams = $4,
      cpu_percent = $5,
      memory_percent = $6,
      disk_percent = $7,
      load_1m = $8,
      ffmpeg_processes = $9,
      srs_healthy = $10,
      api_healthy = TRUE,
      last_heartbeat_at = NOW(),
      last_error = $11,
      updated_at = NOW()
    WHERE id = $1
      AND is_enabled = TRUE
    RETURNING
      id,
      name,
      hostname,
      public_ip,
      private_ip,
      region,
      role,
      status,
      is_enabled,
      is_draining,
      max_streams,
      active_streams,
      cpu_percent,
      memory_percent,
      disk_percent,
      load_1m,
      ffmpeg_processes,
      srs_healthy,
      api_healthy,
      last_heartbeat_at,
      last_error,
      metadata,
      created_at,
      updated_at
    `,
    [
      nodeId,
      os.hostname(),
      nodeStatus,
      srs.active_streams,
      system.cpu_percent,
      system.memory_percent,
      system.disk_percent,
      system.load_1m,
      ffmpegProcesses,
      srs.healthy,
      srs.error,
    ],
  );

  if (!result.rows.length) {
    throw new Error(`Media node ${nodeId} was not found or is disabled`);
  }

  return {
    node_id: nodeId,
    status: nodeStatus,

    cpu_percent: system.cpu_percent,
    memory_percent: system.memory_percent,
    disk_percent: system.disk_percent,
    load_1m: system.load_1m,

    active_streams: srs.active_streams,
    ffmpeg_processes: ffmpegProcesses,

    srs_healthy: srs.healthy,
    api_healthy: true,

    last_error: srs.error,

    last_heartbeat_at: result.rows[0].last_heartbeat_at,
  };
}

/**
 * Start the recurring heartbeat loop.
 *
 * The heartbeat runs immediately once and then repeats using the
 * configured interval.
 *
 * A lock prevents overlapping heartbeat executions if one check takes
 * longer than expected.
 */
function startMediaNodeHeartbeat({
  pool,
  nodeId,
  srsApiUrl = "http://localhost:1985",
  intervalMs = DEFAULT_INTERVAL_MS,
}) {
  if (!pool) {
    throw new Error("Media node heartbeat requires a database pool");
  }

  if (!nodeId) {
    throw new Error("Media node heartbeat requires nodeId");
  }

  const safeIntervalMs = Math.max(
    5000,
    Number(intervalMs) || DEFAULT_INTERVAL_MS,
  );

  let running = false;
  let stopped = false;

  const tick = async () => {
    if (running || stopped) {
      return;
    }

    running = true;

    try {
      const result = await updateMediaNodeHeartbeat({
        pool,
        nodeId,
        srsApiUrl,
      });

      console.log(
        `[MediaNode] heartbeat` +
          ` node=${result.node_id}` +
          ` status=${result.status}` +
          ` streams=${result.active_streams}` +
          ` ffmpeg=${result.ffmpeg_processes}` +
          ` cpu=${result.cpu_percent}%` +
          ` mem=${result.memory_percent}%` +
          ` disk=${
            result.disk_percent == null ? "n/a" : `${result.disk_percent}%`
          }` +
          ` srs=${result.srs_healthy ? "healthy" : "unhealthy"}`,
      );
    } catch (error) {
      console.error("[MediaNode] heartbeat failed:", error);

      try {
        await pool.query(
          `
          UPDATE media_nodes
          SET
            status = 'degraded',
            api_healthy = TRUE,
            last_error = $2,
            updated_at = NOW()
          WHERE id = $1
          `,
          [nodeId, error?.message || "Media node heartbeat failed"],
        );
      } catch (updateError) {
        console.error(
          "[MediaNode] failed to record heartbeat error:",
          updateError,
        );
      }
    } finally {
      running = false;
    }
  };

  /*
   * Run the first heartbeat immediately.
   * Do not await it here because this function returns the controller
   * synchronously.
   */
  void tick();

  const timer = setInterval(tick, safeIntervalMs);

  timer.unref?.();

  return {
    nodeId,
    intervalMs: safeIntervalMs,

    stop() {
      if (stopped) {
        return;
      }

      stopped = true;
      clearInterval(timer);

      console.log(`[MediaNode] heartbeat stopped node=${nodeId}`);
    },

    async runNow() {
      if (stopped) {
        throw new Error(`Media node heartbeat ${nodeId} is stopped`);
      }

      return updateMediaNodeHeartbeat({
        pool,
        nodeId,
        srsApiUrl,
      });
    },
  };
}

module.exports = {
  DEFAULT_INTERVAL_MS,
  getLocalSystemMetrics,
  getSrsHealth,
  getFfmpegProcessCount,
  updateMediaNodeHeartbeat,
  startMediaNodeHeartbeat,
};
