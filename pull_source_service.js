// pull_source_service.js
// Pulls external live sources and republishes them into the channel's normal
// SRS /live/<stream_key> ingest path so the existing NLM lifecycle (ABR, DVR,
// recording, analytics and Stream Targets) remains the single source of truth.

const { spawn } = require("child_process");
const dns = require("dns").promises;
const net = require("net");
const { decryptSourceUrl, maskSourceUrl } = require("./pull_source_schema");

const SUPPORTED_PROTOCOLS = Object.freeze({
  rtmp: { label: "RTMP", schemes: ["rtmp:"] },
  rtmps: { label: "RTMPS", schemes: ["rtmps:"] },
  rtsp: { label: "RTSP", schemes: ["rtsp:"] },
  srt: { label: "SRT", schemes: ["srt:"] },
  hls: { label: "HLS", schemes: ["http:", "https:"] },
  http_flv: { label: "HTTP-FLV", schemes: ["http:", "https:"] },
});

const BASE_RECONNECT_MS = Math.max(
  1000,
  Number(process.env.PULL_SOURCE_RECONNECT_DELAY_MS || 3000),
);
const MAX_RECONNECT_MS = Math.max(
  BASE_RECONNECT_MS,
  Number(process.env.PULL_SOURCE_MAX_RECONNECT_DELAY_MS || 30000),
);
const RECONNECT_JITTER_PERCENT = Math.max(
  0,
  Math.min(
    0.5,
    Number(process.env.PULL_SOURCE_RECONNECT_JITTER_PERCENT || 0.15),
  ),
);
const PREFLIGHT_TIMEOUT_MS = Math.max(
  3000,
  Number(process.env.PULL_SOURCE_PREFLIGHT_TIMEOUT_MS || 12000),
);
const DELIVERY_VERIFY_TIMEOUT_MS = Math.max(
  5000,
  Number(process.env.PULL_SOURCE_DELIVERY_VERIFY_TIMEOUT_MS || 15000),
);
const ALLOW_PRIVATE_NETWORKS =
  String(
    process.env.PULL_SOURCE_ALLOW_PRIVATE_NETWORKS || "false",
  ).toLowerCase() === "true";
const INTERNAL_RTMP_BASE = String(
  process.env.PULL_SOURCE_INTERNAL_RTMP_BASE || "rtmp://127.0.0.1:1935/live",
).replace(/\/+$/, "");
const SRS_API_URL = String(
  process.env.SRS_API_URL || "http://127.0.0.1:1985",
).replace(/\/+$/, "");

const FAILOVER_MONITOR_INTERVAL_MS = Math.max(
  2000,
  Number(process.env.PULL_SOURCE_FAILOVER_MONITOR_INTERVAL_MS || 5000),
);

const RTSP_TRANSPORT = (() => {
  const value = String(process.env.PULL_SOURCE_RTSP_TRANSPORT || "tcp")
    .trim()
    .toLowerCase();
  return ["tcp", "udp", "udp_multicast", "http", "https"].includes(value)
    ? value
    : "tcp";
})();

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function normalizeProtocol(value) {
  const key = String(value || "")
    .trim()
    .toLowerCase()
    .replace(/-/g, "_");
  return SUPPORTED_PROTOCOLS[key] ? key : null;
}

function isPrivateIp(address) {
  const ip = String(address || "").toLowerCase();
  if (!ip) return true;

  if (net.isIP(ip) === 4) {
    const parts = ip.split(".").map(Number);
    return (
      parts[0] === 10 ||
      parts[0] === 127 ||
      (parts[0] === 169 && parts[1] === 254) ||
      (parts[0] === 172 && parts[1] >= 16 && parts[1] <= 31) ||
      (parts[0] === 192 && parts[1] === 168) ||
      parts[0] === 0 ||
      parts[0] >= 224
    );
  }

  if (net.isIP(ip) === 6) {
    return (
      ip === "::1" ||
      ip === "::" ||
      ip.startsWith("fc") ||
      ip.startsWith("fd") ||
      ip.startsWith("fe8") ||
      ip.startsWith("fe9") ||
      ip.startsWith("fea") ||
      ip.startsWith("feb")
    );
  }

  return true;
}

async function validateExternalHostname(hostname) {
  if (ALLOW_PRIVATE_NETWORKS) return;
  if (!hostname) throw new Error("Source URL host is missing");

  if (hostname.toLowerCase() === "localhost") {
    const error = new Error(
      "Private/loopback pull-source addresses are blocked",
    );
    error.code = "private_network_blocked";
    throw error;
  }

  let addresses;
  try {
    addresses = await dns.lookup(hostname, { all: true, verbatim: true });
  } catch (error) {
    const wrapped = new Error(`Source DNS lookup failed: ${error.message}`);
    wrapped.code = "dns_failure";
    throw wrapped;
  }

  if (
    !addresses.length ||
    addresses.some((item) => isPrivateIp(item.address))
  ) {
    const error = new Error(
      "Source resolves to a private, loopback, link-local or reserved network address",
    );
    error.code = "private_network_blocked";
    throw error;
  }
}

async function validateSourceUrl(sourceUrl, protocol) {
  const normalized = normalizeProtocol(protocol);
  if (!normalized) {
    const error = new Error("Unsupported pull-source protocol");
    error.code = "unsupported_protocol";
    throw error;
  }

  let parsed;
  try {
    parsed = new URL(String(sourceUrl || "").trim());
  } catch {
    const error = new Error("Source URL is invalid");
    error.code = "invalid_source_url";
    throw error;
  }

  if (
    !SUPPORTED_PROTOCOLS[normalized].schemes.includes(
      parsed.protocol.toLowerCase(),
    )
  ) {
    const error = new Error(
      `${SUPPORTED_PROTOCOLS[normalized].label} source requires ${SUPPORTED_PROTOCOLS[
        normalized
      ].schemes.join(" or ")}`,
    );
    error.code = "protocol_mismatch";
    throw error;
  }

  if (
    normalized === "hls" &&
    !/\.m3u8(?:$|\?)/i.test(parsed.pathname + parsed.search)
  ) {
    // Not a hard technical requirement, but catches accidental web-page URLs.
    const error = new Error("HLS source URL must point to an .m3u8 playlist");
    error.code = "invalid_hls_url";
    throw error;
  }

  if (
    normalized === "http_flv" &&
    !/\.flv(?:$|\?)/i.test(parsed.pathname + parsed.search)
  ) {
    const error = new Error("HTTP-FLV source URL must point to a .flv stream");
    error.code = "invalid_http_flv_url";
    throw error;
  }

  await validateExternalHostname(parsed.hostname);
  return parsed;
}

function reconnectDelay(attempt) {
  const exponent = Math.min(
    MAX_RECONNECT_MS,
    BASE_RECONNECT_MS * 2 ** Math.max(0, Number(attempt || 1) - 1),
  );
  const jitter = exponent * RECONNECT_JITTER_PERCENT;
  return Math.max(
    BASE_RECONNECT_MS,
    Math.round(exponent + (Math.random() * 2 - 1) * jitter),
  );
}

function classifyFailure(text, protocol) {
  const value = String(text || "");
  const lower = value.toLowerCase();

  if (
    /401|403|unauthorized|forbidden|authentication failed|invalid key|bad name/i.test(
      value,
    )
  ) {
    return {
      code: "authentication_rejected",
      retryable: false,
      message: "Source rejected authentication",
    };
  }
  if (
    /name or service not known|temporary failure in name resolution|enotfound|eai_again/i.test(
      value,
    )
  ) {
    return {
      code: "dns_failure",
      retryable: true,
      message: "Source DNS lookup failed",
    };
  }
  if (/certificate|unknown ca|self signed|unable to verify/i.test(lower)) {
    return {
      code: "tls_certificate_error",
      retryable: false,
      message: "Source TLS certificate validation failed",
    };
  }
  if (/method describe failed:\s*401|401 unauthorized/i.test(lower)) {
    return {
      code: "authentication_rejected",
      retryable: false,
      message: "RTSP source rejected authentication",
    };
  }
  if (/method describe failed:\s*404|404 not found/i.test(lower)) {
    return {
      code: "source_not_found",
      retryable: true,
      message: "RTSP source path is not currently available",
    };
  }
  if (/461 unsupported transport|unsupported transport/i.test(lower)) {
    return {
      code: "rtsp_transport_rejected",
      retryable: false,
      message: `RTSP source rejected the ${RTSP_TRANSPORT.toUpperCase()} transport`,
    };
  }
  if (/method (describe|setup|play) failed/i.test(lower)) {
    return {
      code: "rtsp_handshake_failed",
      retryable: true,
      message: "RTSP session negotiation failed",
    };
  }
  if (/connection refused/i.test(value)) {
    return {
      code: "connection_refused",
      retryable: true,
      message: "Source refused the connection",
    };
  }
  if (/timed out|timeout/i.test(value)) {
    return {
      code: "connection_timeout",
      retryable: true,
      message: "Source connection timed out",
    };
  }
  if (
    /connection reset|broken pipe|network is unreachable|input\/output error/i.test(
      value,
    )
  ) {
    return {
      code: "connection_lost",
      retryable: true,
      message: "Source connection was interrupted",
    };
  }
  if (
    /invalid data|could not find codec parameters|error parsing|conversion failed/i.test(
      value,
    )
  ) {
    return {
      code: "invalid_media",
      retryable: true,
      message: "Source media could not be decoded",
    };
  }

  // Pull Sources are live inputs regardless of ingest protocol. A transport
  // EOF must never be treated as a clean, terminal end for RTMP/RTMPS/RTSP/
  // SRT/HLS/HTTP-FLV Pull Sources. In particular, SRT can report EOF after the
  // downstream SRS publisher disappears during an SRS restart. Treat that as
  // a retryable connection loss so Auto Reconnect can respawn the worker.
  if (/end of file/i.test(value)) {
    const normalizedProtocol = normalizeProtocol(protocol);
    const label = SUPPORTED_PROTOCOLS[normalizedProtocol]?.label || "Source";

    return {
      code: "connection_lost",
      retryable: true,
      clean: false,
      message: `${label} source stream ended unexpectedly`,
    };
  }

  // FFmpeg may emit FLV trailer warnings while closing NLM's republished
  // output after SRS restarts or the upstream connection disappears. These
  // Pull Sources are live inputs, so this is also a retryable interruption for
  // every supported protocol rather than a terminal source_ended condition.
  if (
    /failed to update header with correct duration/i.test(value) &&
    /failed to update header with correct filesize/i.test(value)
  ) {
    const normalizedProtocol = normalizeProtocol(protocol);
    const label = SUPPORTED_PROTOCOLS[normalizedProtocol]?.label || "Source";

    return {
      code: "connection_lost",
      retryable: true,
      clean: false,
      message: `${label} source stream ended unexpectedly`,
    };
  }

  return {
    code: "ffmpeg_error",
    retryable: true,
    message:
      value.trim().slice(-600) || "Pull-source worker stopped unexpectedly",
  };
}

function inputArgs(protocol) {
  const common = [
    "-hide_banner",
    "-nostats",
    "-loglevel",
    "warning",
    "-fflags",
    "+genpts+discardcorrupt",
    "-avoid_negative_ts",
    "make_zero",
  ];

  if (protocol === "rtmp" || protocol === "rtmps") {
    return [...common, "-rw_timeout", "15000000"];
  }
  if (protocol === "rtsp") {
    return [...common, "-rtsp_transport", RTSP_TRANSPORT];
  }
  if (protocol === "hls" || protocol === "http_flv") {
    return [
      ...common,
      "-rw_timeout",
      "15000000",
      "-reconnect",
      "1",
      "-reconnect_streamed",
      "1",
      "-reconnect_delay_max",
      "5",
    ];
  }
  return common;
}

function buildWorkerArgs(sourceUrl, protocol, streamKey) {
  const normalized = normalizeProtocol(protocol);
  const normalizeVideo =
    normalized === "hls" || normalized === "http_flv" || normalized === "rtsp";

  const videoArgs = normalizeVideo
    ? [
        // HLS/CMAF, HTTP-FLV and RTSP inputs are not guaranteed to be directly
        // RTMP/FLV-safe (RTSP cameras may expose H.265/MPEG-4). Normalize
        // them to a conservative H.264 profile so
        // SRS receives stable timestamps, frame cadence and keyframes.
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-pix_fmt",
        "yuv420p",
        "-r",
        "25",
        "-g",
        "50",
        "-keyint_min",
        "50",
        "-sc_threshold",
        "0",
        "-bf",
        "0",
      ]
    : ["-c:v", "copy"];

  return [
    ...inputArgs(normalized),
    "-i",
    sourceUrl,
    "-map",
    "0:v:0",
    "-map",
    "0:a:0?",
    ...videoArgs,
    "-c:a",
    "aac",
    "-b:a",
    "128k",
    "-ar",
    "48000",
    "-af",
    "aresample=async=1:first_pts=0",
    "-progress",
    "pipe:1",
    "-f",
    "flv",
    `${INTERNAL_RTMP_BASE}/${streamKey}`,
  ];
}

function createPullSourceManager({ pool }) {
  const states = new Map();
  const failbackStableSince = new Map();
  let failoverMonitorBusy = false;

  function getState(id) {
    return states.get(Number(id)) || null;
  }

  function publicRuntimeState(id) {
    const state = getState(id);
    if (!state) return null;
    return {
      status: state.status,
      is_running: state.isRunning,
      bitrate_kbps: Math.max(0, Math.round(state.bitrateKbps || 0)),
      uptime_seconds: state.startedAt
        ? Math.max(0, Math.floor((Date.now() - state.startedAt) / 1000))
        : 0,
      reconnect_count: state.reconnectCount || 0,
      last_error: state.lastError || null,
      last_error_code: state.lastErrorCode || null,
      out_time_ms: state.outTimeMs || 0,
    };
  }

  async function updateDb(id, fields) {
    const entries = Object.entries(fields).filter(
      ([, value]) => value !== undefined,
    );
    if (!entries.length) return;
    const assignments = entries.map(([key], index) => `${key}=$${index + 1}`);
    const values = entries.map(([, value]) => value);
    values.push(id);
    await pool.query(
      `UPDATE channel_pull_sources SET ${assignments.join(", ")}, updated_at=NOW() WHERE id=$${values.length}`,
      values,
    );
  }

  async function isSrsStreamLive(streamKey) {
    try {
      const response = await fetch(`${SRS_API_URL}/api/v1/streams/`, {
        signal: AbortSignal.timeout(4000),
        cache: "no-store",
      });
      if (!response.ok) return false;
      const data = await response.json();
      return (data.streams || []).some(
        (stream) =>
          stream.name === streamKey &&
          stream.app === "live" &&
          stream.publish?.active === true,
      );
    } catch {
      return false;
    }
  }

  async function preflightSource(source) {
    const sourceUrl = decryptSourceUrl(source.source_url);
    const protocol = normalizeProtocol(source.protocol);
    const parsed = await validateSourceUrl(sourceUrl, protocol);

    return new Promise((resolve) => {
      const preflightInputArgs =
        protocol === "rtsp" ? ["-rtsp_transport", RTSP_TRANSPORT] : [];

      const args = [
        "-v",
        "error",
        ...preflightInputArgs,
        "-show_entries",
        "stream=index,codec_type,codec_name,width,height,avg_frame_rate,sample_rate,channels",
        "-of",
        "json",
        sourceUrl,
      ];
      const proc = spawn("ffprobe", args, {
        stdio: ["ignore", "pipe", "pipe"],
      });
      let stdout = "";
      let stderr = "";
      let settled = false;

      const finish = (result) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        try {
          proc.kill("SIGKILL");
        } catch {}
        resolve(result);
      };

      proc.stdout.on("data", (chunk) => {
        stdout += chunk.toString();
      });
      proc.stderr.on("data", (chunk) => {
        stderr += chunk.toString();
      });
      proc.on("error", (error) =>
        finish({ ok: false, code: "ffprobe_error", message: error.message }),
      );
      proc.on("exit", (code) => {
        if (code !== 0) {
          const failure = classifyFailure(stderr, protocol);
          return finish({ ok: false, ...failure });
        }
        try {
          const data = JSON.parse(stdout || "{}");
          const streams = data.streams || [];
          const video =
            streams.find((item) => item.codec_type === "video") || null;
          const audio =
            streams.find((item) => item.codec_type === "audio") || null;
          if (!video) {
            return finish({
              ok: false,
              code: "no_video",
              message: "Source does not expose a video stream",
            });
          }
          finish({
            ok: true,
            code: "source_ready",
            protocol,
            source: maskSourceUrl(parsed.toString()),
            video,
            audio,
          });
        } catch (error) {
          finish({
            ok: false,
            code: "probe_parse_error",
            message: error.message,
          });
        }
      });

      const timer = setTimeout(() => {
        finish({
          ok: false,
          code: "probe_timeout",
          retryable: true,
          message: `Source probe timed out after ${PREFLIGHT_TIMEOUT_MS}ms`,
        });
      }, PREFLIGHT_TIMEOUT_MS);
      timer.unref?.();
    });
  }

  async function recordHealth(id, healthStatus) {
    await updateDb(id, {
      health_status: healthStatus || "unknown",
      last_health_check_at: new Date(),
    });
  }

  async function getFailoverConfig(channelId, organizationId = null) {
    const params = [channelId];
    let where = "channel_id=$1";
    if (organizationId !== null && organizationId !== undefined) {
      params.push(organizationId);
      where += " AND organization_id=$2";
    }
    const result = await pool.query(
      `SELECT * FROM channel_source_failover WHERE ${where} LIMIT 1`,
      params,
    );
    return result.rows[0] || null;
  }

  async function getChannelSources(channelId, organizationId) {
    const result = await pool.query(
      `SELECT * FROM channel_pull_sources
       WHERE channel_id=$1 AND organization_id=$2
       ORDER BY CASE WHEN role='primary' THEN 0 ELSE 1 END, priority ASC, id ASC`,
      [channelId, organizationId],
    );
    return result.rows;
  }

  async function isActiveSource(source) {
    const result = await pool.query(
      `SELECT is_active_source FROM channel_pull_sources WHERE id=$1`,
      [source.id],
    );
    return Boolean(result.rows[0]?.is_active_source);
  }

  async function setActiveSource(source, reason = "source_switch") {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      await client.query(
        `UPDATE channel_pull_sources
         SET is_active_source=FALSE, updated_at=NOW()
         WHERE channel_id=$1 AND organization_id=$2`,
        [source.channel_id, source.organization_id],
      );
      await client.query(
        `UPDATE channel_pull_sources
         SET is_active_source=TRUE, updated_at=NOW()
         WHERE id=$1`,
        [source.id],
      );
      await client.query(
        `INSERT INTO channel_source_failover
           (channel_id, organization_id, active_source_id, last_switch_at, last_switch_reason)
         VALUES ($1,$2,$3,NOW(),$4)
         ON CONFLICT (channel_id)
         DO UPDATE SET
           organization_id=EXCLUDED.organization_id,
           active_source_id=EXCLUDED.active_source_id,
           last_switch_at=NOW(),
           last_switch_reason=EXCLUDED.last_switch_reason,
           updated_at=NOW()`,
        [source.channel_id, source.organization_id, source.id, reason],
      );
      await client.query("COMMIT");
    } catch (error) {
      try {
        await client.query("ROLLBACK");
      } catch {}
      throw error;
    } finally {
      client.release();
    }
  }

  async function clearActiveSource(source, reason = "source_stopped") {
    await pool.query(
      `UPDATE channel_pull_sources
       SET is_active_source=FALSE, updated_at=NOW()
       WHERE id=$1`,
      [source.id],
    );
    await pool.query(
      `UPDATE channel_source_failover
       SET active_source_id=NULL,
           last_switch_at=NOW(),
           last_switch_reason=$2,
           updated_at=NOW()
       WHERE channel_id=$1 AND active_source_id=$3`,
      [source.channel_id, reason, source.id],
    );
  }

  async function waitForCanonicalOffline(streamKey, timeoutMs = 6000) {
    const started = Date.now();
    while (Date.now() - started < timeoutMs) {
      if (!(await isSrsStreamLive(streamKey))) return true;
      await sleep(250);
    }
    return !(await isSrsStreamLive(streamKey));
  }

  async function activateSource(source, channel, options = {}) {
    if (!source?.id) return { ok: false, message: "Pull source is missing" };
    if (!source.enabled)
      return { ok: false, message: "Pull source is disabled" };

    const freshResult = await pool.query(
      `SELECT * FROM channel_pull_sources
       WHERE id=$1 AND channel_id=$2 AND organization_id=$3`,
      [source.id, source.channel_id, source.organization_id],
    );
    const freshSource = freshResult.rows[0];
    if (!freshSource) return { ok: false, message: "Pull source not found" };

    const activeResult = await pool.query(
      `SELECT * FROM channel_pull_sources
       WHERE channel_id=$1 AND organization_id=$2 AND is_active_source=TRUE
       ORDER BY id LIMIT 1`,
      [freshSource.channel_id, freshSource.organization_id],
    );
    const current = activeResult.rows[0] || null;

    if (current && Number(current.id) !== Number(freshSource.id)) {
      await stopSource(current, {
        clearActive: true,
        reason: options.reason || "source_switch",
      });
      await waitForCanonicalOffline(channel.stream_key, 6000);
    }

    // Clean up any orphan runtime worker for another source on this channel.
    const siblings = await getChannelSources(
      freshSource.channel_id,
      freshSource.organization_id,
    );
    for (const sibling of siblings) {
      if (Number(sibling.id) === Number(freshSource.id)) continue;
      const siblingState = getState(sibling.id);

      // A sibling can have a pending reconnect timer even when it has no live
      // FFmpeg process. If we promote another source without cancelling that
      // timer, the old source may wake up later and repeatedly contend with
      // the now-active canonical publisher. Stop/cancel any managed sibling
      // runtime before assigning ownership to the selected source.
      if (
        siblingState &&
        ((siblingState.proc && siblingState.proc.exitCode === null) ||
          siblingState.retryTimer)
      ) {
        await stopSource(sibling, {
          clearActive: false,
          reason: "source_switch_cleanup",
        });
      }
    }

    // Direct push ingest (OBS/SRT push) still wins over Pull Sources. The only
    // canonical publisher we are allowed to replace is another managed Pull Source.
    if (await isSrsStreamLive(channel.stream_key)) {
      return {
        ok: false,
        code: "channel_already_live",
        message:
          "This channel already has a live publisher that is not the selected Pull Source.",
      };
    }

    await setActiveSource(freshSource, options.reason || "source_start");
    const result = await startSource(freshSource, channel, {
      reconnecting: Boolean(options.reconnecting),
      activated: true,
    });

    if (!result.ok) {
      await clearActiveSource(freshSource, "source_start_failed");
    } else if (String(freshSource.role || "backup") === "backup") {
      failbackStableSince.delete(Number(freshSource.channel_id));
    }
    return result;
  }

  async function chooseFailoverCandidate(source, channel) {
    const sources = await getChannelSources(
      source.channel_id,
      source.organization_id,
    );
    const candidates = sources.filter(
      (candidate) =>
        candidate.enabled && Number(candidate.id) !== Number(source.id),
    );

    for (const candidate of candidates) {
      try {
        const probe = await preflightSource(candidate);
        await recordHealth(candidate.id, probe.ok ? "ready" : "unhealthy");
        if (probe.ok) return candidate;
      } catch {
        await recordHealth(candidate.id, "unhealthy").catch(() => {});
      }
    }
    return null;
  }

  async function scheduleFailover(source, channel, state, failure, config) {
    state.reconnectCount += 1;
    state.status = "reconnecting";
    state.lastError = failure.message;
    state.lastErrorCode = failure.code;
    const thresholdMs = Math.max(
      1000,
      Number(config.failure_threshold_seconds || 5) * 1000,
    );

    await updateDb(source.id, {
      status: "reconnecting",
      is_running: false,
      reconnect_count: state.reconnectCount,
      last_error: failure.message,
      last_error_code: failure.code,
      health_status: "unhealthy",
      last_health_check_at: new Date(),
      stopped_at: new Date(),
    });

    console.warn(
      `[PULL-SOURCE-FAILOVER] Active #${source.id} failed. Waiting ${thresholdMs}ms before selecting backup: ${failure.message}`,
    );

    state.retryTimer = setTimeout(async () => {
      state.retryTimer = null;
      if (state.manualStop) return;
      try {
        const stillActive = await isActiveSource(source);
        if (!stillActive) return;

        const candidate = await chooseFailoverCandidate(source, channel);
        if (candidate) {
          console.warn(
            `[PULL-SOURCE-FAILOVER] Switching channel ${channel.id} from #${source.id} to #${candidate.id} (${candidate.name}).`,
          );
          await activateSource(candidate, channel, {
            reason: "automatic_failover",
            reconnecting: true,
          });
          return;
        }

        // No healthy backup is available. Retry the current active source so a
        // one-source channel still recovers exactly as it did before failover.
        console.warn(
          `[PULL-SOURCE-FAILOVER] No healthy backup available for channel ${channel.id}; retrying #${source.id}.`,
        );
        await clearActiveSource(source, "retry_current_source");
        await activateSource(source, channel, {
          reason: "retry_current_source",
          reconnecting: true,
        });
      } catch (error) {
        console.error(
          `[PULL-SOURCE-FAILOVER] Recovery failed for channel ${channel.id}:`,
          error.message,
        );
      }
    }, thresholdMs);
    state.retryTimer.unref?.();
  }

  async function scheduleReconnect(source, channel, state, failure) {
    if (
      state.manualStop ||
      !source.enabled ||
      !source.auto_reconnect ||
      failure.retryable === false
    ) {
      state.status = "stopped";
      state.isRunning = false;

      const cleanStop =
        state.manualStop ||
        failure.clean === true ||
        failure.code === "source_ended";

      state.lastError = cleanStop ? null : failure.message;
      state.lastErrorCode = cleanStop ? null : failure.code;

      await updateDb(source.id, {
        status: "stopped",
        is_running: false,
        stopped_at: new Date(),
        health_status: cleanStop ? "unknown" : "unhealthy",
        last_health_check_at: new Date(),
        last_error: cleanStop ? null : failure.message,
        last_error_code: cleanStop ? null : failure.code,
      });
      return;
    }

    const config = await getFailoverConfig(
      source.channel_id,
      source.organization_id,
    );
    const active = await isActiveSource(source);
    if (config?.enabled && active) {
      await scheduleFailover(source, channel, state, failure, config);
      return;
    }

    state.reconnectCount += 1;
    state.status = "reconnecting";
    state.lastError = failure.message;
    state.lastErrorCode = failure.code;
    const delay = reconnectDelay(state.reconnectCount);

    await updateDb(source.id, {
      status: "reconnecting",
      is_running: false,
      reconnect_count: state.reconnectCount,
      last_error: failure.message,
      last_error_code: failure.code,
      health_status: "unhealthy",
      last_health_check_at: new Date(),
      stopped_at: new Date(),
    });

    console.warn(
      `[PULL-SOURCE] #${source.id} reconnect ${state.reconnectCount} in ${delay}ms: ${failure.message}`,
    );

    state.retryTimer = setTimeout(async () => {
      state.retryTimer = null;
      if (state.manualStop) return;
      try {
        await startSource(source, channel, { reconnecting: true });
      } catch (error) {
        console.error(
          `[PULL-SOURCE] #${source.id} reconnect start failed:`,
          error.message,
        );
      }
    }, delay);
    state.retryTimer.unref?.();
  }

  async function startSource(source, channel, options = {}) {
    const id = Number(source.id);
    const existing = getState(id);
    if (existing?.proc && existing.proc.exitCode === null) {
      return {
        ok: true,
        message: "Pull source is already running",
        runtime: publicRuntimeState(id),
      };
    }

    if (!source.enabled)
      return { ok: false, message: "Pull source is disabled" };
    if (!channel?.stream_key)
      return { ok: false, message: "Channel stream key is missing" };

    const sourceUrl = decryptSourceUrl(source.source_url);
    const protocol = normalizeProtocol(source.protocol);
    await validateSourceUrl(sourceUrl, protocol);

    // First publisher wins. Never collide with OBS/another source using the same channel key.
    if (await isSrsStreamLive(channel.stream_key)) {
      const state = existing || {
        proc: null,
        retryTimer: null,
        reconnectCount: Number(source.reconnect_count || 0),
        startedAt: null,
        bitrateKbps: 0,
        outTimeMs: 0,
        manualStop: false,
      };
      state.status = "waiting";
      state.isRunning = false;
      state.lastError = "Channel is already receiving another publisher";
      state.lastErrorCode = "channel_already_live";
      states.set(id, state);
      await updateDb(id, {
        status: "waiting",
        is_running: false,
        last_error: state.lastError,
        last_error_code: state.lastErrorCode,
      });

      if (source.auto_reconnect && !state.manualStop) {
        await scheduleReconnect(source, channel, state, {
          code: "channel_already_live",
          retryable: true,
          message: "Channel is already receiving another publisher",
        });
      }

      return {
        ok: false,
        code: "channel_already_live",
        message:
          "This channel already has a live publisher. Stop OBS/the existing source before starting the Pull Source.",
      };
    }

    const state = existing || {
      proc: null,
      retryTimer: null,
      reconnectCount: Number(source.reconnect_count || 0),
      startedAt: null,
      bitrateKbps: 0,
      outTimeMs: 0,
      manualStop: false,
    };
    if (!options.reconnecting) state.reconnectCount = 0;
    state.manualStop = false;
    state.status = "starting";
    state.isRunning = false;
    state.lastError = null;
    state.lastErrorCode = null;
    states.set(id, state);

    await updateDb(id, {
      status: "starting",
      is_running: false,
      last_error: null,
      last_error_code: null,
      reconnect_count: state.reconnectCount,
    });

    const args = buildWorkerArgs(sourceUrl, protocol, channel.stream_key);
    console.log(
      `[PULL-SOURCE] Starting #${id} (${protocol}) ${maskSourceUrl(sourceUrl)} -> ${channel.stream_key}`,
    );
    const proc = spawn("ffmpeg", args, { stdio: ["ignore", "pipe", "pipe"] });
    state.proc = proc;
    state.startedAt = Date.now();
    state.stderr = "";

    proc.stdout.setEncoding("utf8");
    let progressBuffer = "";
    proc.stdout.on("data", (chunk) => {
      progressBuffer += chunk;
      const lines = progressBuffer.split(/\r?\n/);
      progressBuffer = lines.pop() || "";
      for (const line of lines) {
        const [key, ...rest] = line.split("=");
        const value = rest.join("=");
        if (key === "bitrate") {
          const match = value.match(/([0-9.]+)kbits\/s/i);
          if (match) state.bitrateKbps = Number(match[1]);
        } else if (key === "out_time_ms") {
          state.outTimeMs = Number(value || 0);
        }
      }
    });

    proc.stderr.setEncoding("utf8");
    proc.stderr.on("data", (chunk) => {
      state.stderr = `${state.stderr}${chunk}`.slice(-12000);
    });

    proc.on("error", async (error) => {
      state.stderr = `${state.stderr}\n${error.message}`.slice(-12000);
    });

    proc.on("exit", async (code, signal) => {
      if (state.proc !== proc) return;
      state.proc = null;
      state.isRunning = false;
      state.bitrateKbps = 0;
      const failure = state.manualStop
        ? {
            code: "manual_stop",
            retryable: false,
            message: "Pull source stopped",
          }
        : classifyFailure(
            state.stderr ||
              `FFmpeg exited code=${code} signal=${signal || "none"}`,
            protocol,
          );
      await scheduleReconnect(source, channel, state, failure).catch((error) =>
        console.error(
          `[PULL-SOURCE] #${id} exit handling failed:`,
          error.message,
        ),
      );
    });

    // Confirm the republished stream actually appeared in SRS before declaring success.
    const verifyStarted = Date.now();
    while (Date.now() - verifyStarted < DELIVERY_VERIFY_TIMEOUT_MS) {
      if (proc.exitCode !== null) break;
      if (await isSrsStreamLive(channel.stream_key)) {
        state.status = "streaming";
        state.isRunning = true;
        await updateDb(id, {
          status: "streaming",
          is_running: true,
          started_at: new Date(state.startedAt),
          stopped_at: null,
          last_error: null,
          last_error_code: null,
          reconnect_count: state.reconnectCount,
          health_status: "healthy",
          last_health_check_at: new Date(),
        });
        return {
          ok: true,
          message: "Pull source is streaming",
          runtime: publicRuntimeState(id),
        };
      }
      await sleep(500);
    }

    if (proc.exitCode === null) {
      try {
        proc.kill("SIGTERM");
      } catch {}
    }
    return {
      ok: false,
      code: "publish_verify_timeout",
      message:
        "External source connected, but NLM did not receive the republished stream in time",
    };
  }

  async function stopSource(source, options = {}) {
    const id = Number(source.id);
    const state = getState(id);
    if (state) {
      state.manualStop = true;
      if (state.retryTimer) {
        clearTimeout(state.retryTimer);
        state.retryTimer = null;
      }
      if (state.proc && state.proc.exitCode === null) {
        state.proc.kill("SIGTERM");
        const proc = state.proc;
        const timer = setTimeout(() => {
          if (proc.exitCode === null) proc.kill("SIGKILL");
        }, 5000);
        timer.unref?.();
      }
      state.status = "stopped";
      state.isRunning = false;
      state.bitrateKbps = 0;
    }

    await updateDb(id, {
      status: "stopped",
      is_running: false,
      stopped_at: new Date(),
      health_status: "unknown",
      last_error: null,
      last_error_code: null,
    });

    if (options.clearActive !== false) {
      await clearActiveSource(source, options.reason || "source_stopped");
    }
    return { ok: true, message: "Pull source stopped" };
  }

  async function runFailbackMonitor() {
    if (failoverMonitorBusy) return;
    failoverMonitorBusy = true;
    try {
      // Recovery path for a channel that currently has NO active Pull Source.
      // This is the state reached when the active source fails and every other
      // configured source is unavailable at that moment. Previously the code
      // retried only the failed source, so a backup that came online later
      // could remain permanently unused. Probe enabled sources in preference
      // order and promote the first source that has actually recovered.
      const noActiveChannels = await pool.query(`
        SELECT
          f.*,
          c.stream_key
        FROM channel_source_failover f
        JOIN channels c ON c.id=f.channel_id
          AND c.organization_id=f.organization_id
        JOIN organizations o ON o.id=f.organization_id
        WHERE f.enabled=TRUE
          AND c.is_active=TRUE
          AND o.is_active=TRUE
          AND f.active_source_id IS NULL
          AND NOT EXISTS (
            SELECT 1
            FROM channel_pull_sources active_ps
            WHERE active_ps.channel_id=f.channel_id
              AND active_ps.organization_id=f.organization_id
              AND active_ps.is_active_source=TRUE
          )
      `);

      for (const row of noActiveChannels.rows) {
        const channelId = Number(row.channel_id);
        const channel = { id: row.channel_id, stream_key: row.stream_key };

        // Direct push ingest (OBS/SRT push) still wins. If SRS already has the
        // canonical stream while no Pull Source owns it, do not compete with it.
        if (await isSrsStreamLive(row.stream_key)) continue;

        const sources = await getChannelSources(
          row.channel_id,
          row.organization_id,
        );
        const candidates = sources.filter((source) => source.enabled);

        let recovered = null;
        for (const candidate of candidates) {
          let probe;
          try {
            probe = await preflightSource(candidate);
          } catch (error) {
            probe = { ok: false, message: error.message };
          }

          await recordHealth(
            candidate.id,
            probe.ok ? "ready" : "unhealthy",
          ).catch(() => {});

          if (probe.ok) {
            recovered = candidate;
            break;
          }
        }

        if (!recovered) {
          failbackStableSince.delete(channelId);
          continue;
        }

        console.warn(
          `[PULL-SOURCE-RECOVERY] Channel ${channelId} has no active source; recovered #${recovered.id} (${recovered.name}).`,
        );

        const result = await activateSource(recovered, channel, {
          reason: "automatic_source_recovery",
          reconnecting: true,
        });

        if (!result?.ok) {
          console.warn(
            `[PULL-SOURCE-RECOVERY] Could not activate recovered #${recovered.id} for channel ${channelId}: ${result?.message || "unknown error"}`,
          );
        }
      }

      // Existing failback behavior: when a backup owns the canonical stream,
      // continuously probe the preferred primary and switch back only after it
      // has remained healthy for the configured stability window.
      const activeBackups = await pool.query(`
        SELECT
          f.*,
          c.stream_key,
          ps.id AS active_id,
          ps.role AS active_role
        FROM channel_source_failover f
        JOIN channels c ON c.id=f.channel_id
        JOIN organizations o ON o.id=f.organization_id
        LEFT JOIN channel_pull_sources ps ON ps.id=f.active_source_id
        WHERE f.enabled=TRUE
          AND f.failback_enabled=TRUE
          AND c.is_active=TRUE
          AND o.is_active=TRUE
          AND ps.role='backup'
      `);

      for (const row of activeBackups.rows) {
        const primaryResult = await pool.query(
          `SELECT * FROM channel_pull_sources
           WHERE channel_id=$1 AND organization_id=$2
             AND role='primary' AND enabled=TRUE
           ORDER BY priority,id LIMIT 1`,
          [row.channel_id, row.organization_id],
        );
        const primary = primaryResult.rows[0];
        if (!primary) continue;

        let probe;
        try {
          probe = await preflightSource(primary);
        } catch (error) {
          probe = { ok: false, message: error.message };
        }
        await recordHealth(primary.id, probe.ok ? "ready" : "unhealthy").catch(
          () => {},
        );

        const channelId = Number(row.channel_id);
        if (!probe.ok) {
          failbackStableSince.delete(channelId);
          continue;
        }

        const firstHealthyAt = failbackStableSince.get(channelId) || Date.now();
        failbackStableSince.set(channelId, firstHealthyAt);
        const stableForMs = Date.now() - firstHealthyAt;
        const requiredMs =
          Math.max(3, Number(row.failback_stability_seconds || 15)) * 1000;
        if (stableForMs < requiredMs) continue;

        console.warn(
          `[PULL-SOURCE-FAILBACK] Primary #${primary.id} has been healthy for ${Math.round(stableForMs / 1000)}s; switching channel ${channelId} back.`,
        );
        await activateSource(
          primary,
          { id: row.channel_id, stream_key: row.stream_key },
          { reason: "automatic_failback", reconnecting: true },
        );
        failbackStableSince.delete(channelId);
      }
    } catch (error) {
      console.error("[PULL-SOURCE-FAILOVER] Monitor failed:", error.message);
    } finally {
      failoverMonitorBusy = false;
    }
  }

  const failoverMonitorTimer = setInterval(
    runFailbackMonitor,
    FAILOVER_MONITOR_INTERVAL_MS,
  );
  failoverMonitorTimer.unref?.();

  async function reconcileDatabaseState() {
    await pool.query(`
      UPDATE channel_pull_sources
      SET status='stopped',
          is_running=FALSE,
          is_active_source=FALSE,
          started_at=NULL,
          updated_at=NOW()
      WHERE is_running=TRUE
         OR is_active_source=TRUE
         OR status IN ('starting','streaming','reconnecting','waiting')
    `);
    await pool.query(`
      UPDATE channel_source_failover
      SET active_source_id=NULL, updated_at=NOW()
      WHERE active_source_id IS NOT NULL
    `);

    const result = await pool.query(`
      SELECT ps.*, c.stream_key, c.is_active AS channel_active
      FROM channel_pull_sources ps
      JOIN channels c ON c.id=ps.channel_id AND c.organization_id=ps.organization_id
      JOIN organizations o ON o.id=ps.organization_id
      WHERE ps.enabled=TRUE AND ps.auto_start=TRUE
        AND c.is_active=TRUE AND o.is_active=TRUE
      ORDER BY ps.channel_id,
               CASE WHEN ps.role='primary' THEN 0 ELSE 1 END,
               ps.priority,
               ps.id
    `);

    // Only one Pull Source may own a channel's canonical stream. Even if older
    // data accidentally has Auto Start enabled on multiple sources, start only
    // the highest-priority source for each channel.
    const chosenByChannel = new Map();
    for (const row of result.rows) {
      if (!chosenByChannel.has(Number(row.channel_id))) {
        chosenByChannel.set(Number(row.channel_id), row);
      }
    }

    let delay = 1000;
    for (const row of chosenByChannel.values()) {
      const channel = { id: row.channel_id, stream_key: row.stream_key };
      setTimeout(() => {
        activateSource(row, channel, {
          reason: "backend_restart",
          reconnecting: true,
        }).catch((error) =>
          console.error(
            `[PULL-SOURCE] Startup reconcile failed for #${row.id}:`,
            error.message,
          ),
        );
      }, delay).unref?.();
      delay += 250;
    }
  }

  async function shutdown() {
    clearInterval(failoverMonitorTimer);
    for (const state of states.values()) {
      state.manualStop = true;
      if (state.retryTimer) clearTimeout(state.retryTimer);
      if (state.proc && state.proc.exitCode === null)
        state.proc.kill("SIGTERM");
    }
  }

  return {
    SUPPORTED_PROTOCOLS,
    normalizeProtocol,
    validateSourceUrl,
    preflightSource,
    startSource,
    activateSource,
    stopSource,
    recordHealth,
    getRuntimeState: publicRuntimeState,
    isSrsStreamLive,
    reconcileDatabaseState,
    shutdown,
  };
}

module.exports = {
  SUPPORTED_PROTOCOLS,
  createPullSourceManager,
};
