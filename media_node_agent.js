"use strict";

require("dotenv").config();

const http = require("http");
const https = require("https");
const fs = require("fs");
const os = require("os");
const crypto = require("crypto");
const { execFileSync, spawn } = require("child_process");

const {
  getLocalSystemMetrics,
  getSrsHealth,
  getFfmpegProcessCount,
} = require("./media_node_service");

const AGENT_VERSION = "4D.4D";
const DEFAULT_HOST = "127.0.0.1";
const DEFAULT_PORT = 5091;
const DEFAULT_SRS_API_URL = "http://127.0.0.1:1985";
const DEFAULT_SRS_HLS_BASE_URL = "http://127.0.0.1:8080";
const MAX_RESPONSE_BYTES = 1024 * 1024;
const MAX_REQUEST_BYTES = 16 * 1024;
const JOB_RETENTION_MS = 60 * 60 * 1000;
const JOB_MAX_RUNTIME_MS = 15 * 1000;
const jobs = new Map();
const requestIds = new Map();
const PULL_SOURCE_PROBE_PROTOCOLS = new Set([
  "rtmp",
  "rtmps",
  "rtsp",
  "srt",
  "hls",
  "http_flv",
]);
const PULL_SOURCE_START_PROTOCOLS = PULL_SOURCE_PROBE_PROTOCOLS;
const INTERNAL_RTMP_BASE = String(
  process.env.PULL_SOURCE_INTERNAL_RTMP_BASE || "rtmp://127.0.0.1:1935/live",
).replace(/\/+$/, "");
const RTSP_TRANSPORT = (() => {
  const value = String(process.env.PULL_SOURCE_RTSP_TRANSPORT || "tcp")
    .trim()
    .toLowerCase();
  return ["tcp", "udp", "udp_multicast", "http", "https"].includes(value)
    ? value
    : "tcp";
})();

const host = String(process.env.MEDIA_NODE_AGENT_HOST || DEFAULT_HOST).trim();
const port = Math.max(
  1,
  Math.min(65535, Number(process.env.MEDIA_NODE_AGENT_PORT || DEFAULT_PORT)),
);
const srsApiUrl = String(
  process.env.MEDIA_NODE_AGENT_SRS_API_URL ||
    process.env.SRS_API_URL ||
    DEFAULT_SRS_API_URL,
).replace(/\/$/, "");
const srsHlsBaseUrl = String(
  process.env.MEDIA_NODE_AGENT_SRS_HLS_BASE_URL || DEFAULT_SRS_HLS_BASE_URL,
).replace(/\/$/, "");
const nodeId = String(process.env.MEDIA_NODE_ID || "").trim();
const nodeName = String(process.env.MEDIA_NODE_NAME || os.hostname()).trim();
const agentToken = String(process.env.MEDIA_NODE_AGENT_TOKEN || "").trim();
const tlsCertFile = String(
  process.env.MEDIA_NODE_AGENT_TLS_CERT_FILE || "",
).trim();
const tlsKeyFile = String(
  process.env.MEDIA_NODE_AGENT_TLS_KEY_FILE || "",
).trim();
const allowInsecureRemote =
  String(process.env.MEDIA_NODE_AGENT_ALLOW_INSECURE_REMOTE || "false")
    .trim()
    .toLowerCase() === "true";
const useTls = Boolean(tlsCertFile && tlsKeyFile);

if (!agentToken || agentToken.length < 32) {
  console.error(
    "[MediaNodeAgent] MEDIA_NODE_AGENT_TOKEN is required and must be at least 32 characters.",
  );
  process.exit(1);
}

if (Boolean(tlsCertFile) !== Boolean(tlsKeyFile)) {
  console.error(
    "[MediaNodeAgent] MEDIA_NODE_AGENT_TLS_CERT_FILE and MEDIA_NODE_AGENT_TLS_KEY_FILE must be configured together.",
  );
  process.exit(1);
}

function isLoopbackBind(value) {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  return (
    normalized === "127.0.0.1" ||
    normalized === "localhost" ||
    normalized === "::1" ||
    normalized.startsWith("127.")
  );
}

if (!useTls && !isLoopbackBind(host) && !allowInsecureRemote) {
  console.error(
    "[MediaNodeAgent] Refusing non-loopback plaintext listener. Configure TLS or explicitly set MEDIA_NODE_AGENT_ALLOW_INSECURE_REMOTE=true for an isolated private-network test.",
  );
  process.exit(1);
}

function timingSafeTokenEqual(receivedToken) {
  const expected = Buffer.from(agentToken, "utf8");
  const received = Buffer.from(String(receivedToken || ""), "utf8");

  if (expected.length !== received.length) {
    return false;
  }

  return crypto.timingSafeEqual(expected, received);
}

function isAuthorized(req) {
  const header = String(req.headers.authorization || "");
  const match = header.match(/^Bearer\s+(.+)$/i);
  return Boolean(match && timingSafeTokenEqual(match[1].trim()));
}

function sendJson(res, statusCode, payload) {
  const body = Buffer.from(JSON.stringify(payload));

  if (body.length > MAX_RESPONSE_BYTES) {
    const overflow = Buffer.from(
      JSON.stringify({
        ok: false,
        error: "Agent response exceeded safety limit",
      }),
    );
    res.writeHead(500, {
      "Content-Type": "application/json; charset=utf-8",
      "Content-Length": overflow.length,
      "Cache-Control": "no-store",
      "X-Content-Type-Options": "nosniff",
    });
    res.end(overflow);
    return;
  }

  res.writeHead(statusCode, {
    "Content-Type": "application/json; charset=utf-8",
    "Content-Length": body.length,
    "Cache-Control": "no-store",
    "X-Content-Type-Options": "nosniff",
  });
  res.end(body);
}

function getDiskFreeBytes() {
  try {
    const output = execFileSync("df", ["-Pk", "/"], {
      encoding: "utf8",
      timeout: 3000,
    })
      .trim()
      .split("\n");

    const row = output[output.length - 1] || "";
    const parts = row.trim().split(/\s+/);
    const availableKb = Number(parts[3] || 0);

    return Number.isFinite(availableKb) ? availableKb * 1024 : null;
  } catch {
    return null;
  }
}

async function fetchJson(url, timeoutMs = 5000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  timer.unref?.();

  try {
    const response = await fetch(url, { signal: controller.signal });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    return await response.json();
  } finally {
    clearTimeout(timer);
  }
}

async function getSanitizedStreams() {
  const data = await fetchJson(`${srsApiUrl}/api/v1/streams`);
  const streams = Array.isArray(data?.streams) ? data.streams : [];

  return streams.map((stream) => ({
    id: stream?.id ?? null,
    vhost: stream?.vhost ?? null,
    app: stream?.app ?? null,
    name: stream?.name ?? null,
    publish_active: stream?.publish?.active === true,
    clients: Number(stream?.clients || 0),
    frames: Number(stream?.frames || 0),
    send_bytes: Number(stream?.send_bytes || 0),
    recv_bytes: Number(stream?.recv_bytes || 0),
    kbps: {
      recv_30s: Number(stream?.kbps?.recv_30s || 0),
      send_30s: Number(stream?.kbps?.send_30s || 0),
    },
  }));
}

function baseIdentity() {
  return {
    node_id: nodeId || null,
    node_name: nodeName,
    hostname: os.hostname(),
    agent_version: AGENT_VERSION,
    transport: useTls ? "https" : "http",
    pid: process.pid,
    uptime_seconds: Math.floor(process.uptime()),
    timestamp: new Date().toISOString(),
  };
}

function readJsonBody(req) {
  return new Promise((resolve, reject) => {
    let size = 0;
    const chunks = [];
    req.on("data", (chunk) => {
      size += chunk.length;
      if (size > MAX_REQUEST_BYTES) {
        const error = new Error("Request body too large");
        error.status = 413;
        reject(error);
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });
    req.on("end", () => {
      try {
        const text = Buffer.concat(chunks).toString("utf8");
        resolve(text ? JSON.parse(text) : {});
      } catch {
        const error = new Error("Invalid JSON body");
        error.status = 400;
        reject(error);
      }
    });
    req.on("error", reject);
  });
}

function sanitizeJobError(job, value) {
  if (!value) return null;
  let text = String(value);
  if (job?.type === "pull_source_probe" || job?.type === "pull_source_start") {
    for (const sensitive of job.sensitive_values || []) {
      if (sensitive)
        text = text.split(String(sensitive)).join("[source-url-redacted]");
    }
    text = text.replace(
      /(?:rtmps?|rtsp|srt|https?):\/\/[^\s]+/gi,
      "[source-url-redacted]",
    );
  }
  return text;
}

function publicJob(job) {
  const startedMs = job.started_at ? Date.parse(job.started_at) : 0;
  const finishedMs = job.finished_at ? Date.parse(job.finished_at) : Date.now();
  return {
    id: job.id,
    request_id: job.request_id,
    type: job.type,
    channel_id: job.channel_id ?? null,
    source_id: job.source_id ?? null,
    protocol: job.protocol ?? null,
    persistent: job.persistent === true,
    status: job.status,
    bitrate_kbps: Math.max(0, Math.round(Number(job.bitrate_kbps || 0))),
    uptime_seconds:
      startedMs && ["starting", "running", "stopping"].includes(job.status)
        ? Math.max(0, Math.floor((finishedMs - startedMs) / 1000))
        : 0,
    created_at: job.created_at,
    started_at: job.started_at,
    finished_at: job.finished_at,
    exit_code: job.exit_code,
    signal: job.signal,
    error: sanitizeJobError(job, job.error),
  };
}

function cleanupJobs() {
  const cutoff = Date.now() - JOB_RETENTION_MS;
  for (const [id, job] of jobs) {
    const finished = job.finished_at ? Date.parse(job.finished_at) : 0;
    if (finished && finished < cutoff) {
      jobs.delete(id);
      if (job.request_id) requestIds.delete(job.request_id);
    }
  }
}

function makeJob(requestId, jobType, metadata = {}) {
  const id = crypto.randomUUID();
  const now = new Date().toISOString();
  const job = {
    id,
    request_id: requestId || null,
    type: jobType,
    status: "starting",
    created_at: now,
    started_at: now,
    finished_at: null,
    exit_code: null,
    signal: null,
    error: null,
    child: null,
    timer: null,
    ...metadata,
  };
  jobs.set(id, job);
  if (requestId) requestIds.set(requestId, id);
  return job;
}

function attachJobProcess(job, child) {
  job.child = child;
  job.status = "running";
  let stderr = "";

  child.stderr.on("data", (chunk) => {
    stderr = (stderr + chunk.toString("utf8")).slice(-2048);
  });

  child.on("error", (error) => {
    job.status = "failed";
    job.error = error.message;
    job.finished_at = new Date().toISOString();
    job.child = null;
    clearTimeout(job.timer);
  });

  child.on("exit", (code, signal) => {
    job.exit_code = code;
    job.signal = signal || null;
    job.finished_at = new Date().toISOString();
    job.child = null;
    clearTimeout(job.timer);

    if (job.status === "stopping") {
      job.status = "stopped";
    } else if (code === 0) {
      job.status = "succeeded";
    } else {
      job.status = "failed";
      job.error = stderr || `FFmpeg exited with code ${code}`;
    }
  });

  job.timer = setTimeout(() => {
    if (job.child && job.status === "running") {
      job.status = "stopping";
      job.error = "Job exceeded maximum runtime";
      job.child.kill("SIGTERM");
    }
  }, JOB_MAX_RUNTIME_MS);
  job.timer.unref?.();
  return job;
}

function startFfmpegProbe(requestId, jobType = "ffmpeg_probe") {
  cleanupJobs();
  if (requestId && requestIds.has(requestId))
    return jobs.get(requestIds.get(requestId));

  const job = makeJob(requestId, jobType);
  const durationSeconds = jobType === "ffmpeg_stop_probe" ? "10" : "3";
  const args = [
    "-hide_banner",
    "-loglevel",
    "error",
    ...(jobType === "ffmpeg_stop_probe" ? ["-re"] : []),
    "-f",
    "lavfi",
    "-i",
    "testsrc=size=320x180:rate=10",
    "-t",
    durationSeconds,
    "-f",
    "null",
    "-",
  ];

  const child = spawn("ffmpeg", args, {
    stdio: ["ignore", "ignore", "pipe"],
    shell: false,
  });
  return attachJobProcess(job, child);
}

function isValidStreamKey(value) {
  return /^[A-Za-z0-9_-]{1,255}$/.test(String(value || ""));
}

function startLiveStreamProbe(requestId, channelId, streamKey) {
  cleanupJobs();
  if (requestId && requestIds.has(requestId)) {
    return jobs.get(requestIds.get(requestId));
  }

  if (!Number.isInteger(channelId) || channelId <= 0) {
    throw new Error("Invalid channel_id");
  }
  if (!isValidStreamKey(streamKey)) {
    throw new Error("Invalid stream key");
  }

  const job = makeJob(requestId, "live_stream_probe", {
    channel_id: channelId,
  });

  const inputUrl = `${srsHlsBaseUrl}/live/${encodeURIComponent(streamKey)}.m3u8`;
  const args = [
    "-hide_banner",
    "-loglevel",
    "error",
    "-i",
    inputUrl,
    "-t",
    "5",
    "-map",
    "0:v:0?",
    "-map",
    "0:a:0?",
    "-c",
    "copy",
    "-f",
    "null",
    "-",
  ];

  const child = spawn("ffmpeg", args, {
    stdio: ["ignore", "ignore", "pipe"],
    shell: false,
  });
  return attachJobProcess(job, child);
}

function normalizeProbeProtocol(value) {
  const protocol = String(value || "")
    .trim()
    .toLowerCase()
    .replace(/-/g, "_");
  return PULL_SOURCE_PROBE_PROTOCOLS.has(protocol) ? protocol : null;
}

function validatePullProbeUrl(sourceUrl, protocol) {
  const normalized = normalizeProbeProtocol(protocol);
  if (!normalized) throw new Error("Unsupported pull-source probe protocol");

  let parsed;
  try {
    parsed = new URL(String(sourceUrl || "").trim());
  } catch {
    throw new Error("Invalid pull-source probe URL");
  }

  const schemes = {
    rtmp: ["rtmp:"],
    rtmps: ["rtmps:"],
    rtsp: ["rtsp:"],
    srt: ["srt:"],
    hls: ["http:", "https:"],
    http_flv: ["http:", "https:"],
  }[normalized];

  if (!schemes.includes(parsed.protocol.toLowerCase())) {
    throw new Error("Pull-source probe protocol does not match source URL");
  }
  return parsed.toString();
}

function buildPersistentPullSourceArgs(sourceUrl, protocol, streamKey) {
  const normalized = normalizeProbeProtocol(protocol);
  if (!normalized) throw new Error("Unsupported Pull Source start protocol");
  if (!isValidStreamKey(streamKey)) throw new Error("Invalid stream key");

  const input = [
    "-hide_banner",
    "-loglevel",
    "error",
    "-analyzeduration",
    "3000000",
    "-probesize",
    "1000000",
    "-fflags",
    "+genpts+discardcorrupt",
    "-avoid_negative_ts",
    "make_zero",
  ];

  if (normalized === "rtmp" || normalized === "rtmps") {
    input.push("-rw_timeout", "15000000");
  } else if (normalized === "rtsp") {
    input.push("-rtsp_transport", RTSP_TRANSPORT);
  } else if (normalized === "hls" || normalized === "http_flv") {
    input.push(
      "-rw_timeout",
      "15000000",
      "-reconnect",
      "1",
      "-reconnect_streamed",
      "1",
      "-reconnect_delay_max",
      "5",
    );
  }

  const normalizeVideo =
    normalized === "hls" || normalized === "http_flv" || normalized === "rtsp";
  const video = normalizeVideo
    ? [
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
    ...input,
    "-i",
    sourceUrl,
    "-map",
    "0:v:0",
    "-map",
    "0:a:0?",
    ...video,
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

function findActivePullSourceJob(sourceId, channelId) {
  for (const job of jobs.values()) {
    if (
      job.type === "pull_source_start" &&
      (job.source_id === sourceId || job.channel_id === channelId) &&
      ["starting", "running", "stopping"].includes(job.status)
    ) {
      return job;
    }
  }
  return null;
}

function attachPersistentPullSourceProcess(job, child) {
  job.child = child;
  job.persistent = true;
  job.status = "running";
  job.bitrate_kbps = 0;
  let stderr = "";
  let progressBuffer = "";

  child.stdout.setEncoding("utf8");
  child.stdout.on("data", (chunk) => {
    progressBuffer += chunk;
    const lines = progressBuffer.split(/\r?\n/);
    progressBuffer = lines.pop() || "";
    for (const line of lines) {
      const [key, ...rest] = line.split("=");
      if (key !== "bitrate") continue;
      const match = rest.join("=").match(/([0-9.]+)kbits\/s/i);
      if (match) job.bitrate_kbps = Number(match[1]);
    }
  });

  child.stderr.setEncoding("utf8");
  child.stderr.on("data", (chunk) => {
    stderr = `${stderr}${chunk}`.slice(-4096);
  });

  child.on("error", (error) => {
    job.status = "failed";
    job.error = error.message;
    job.finished_at = new Date().toISOString();
    job.child = null;
  });

  child.on("exit", (code, signal) => {
    job.exit_code = code;
    job.signal = signal || null;
    job.finished_at = new Date().toISOString();
    job.child = null;
    job.bitrate_kbps = 0;
    if (job.stop_timer) {
      clearTimeout(job.stop_timer);
      job.stop_timer = null;
    }

    if (job.status === "stopping") {
      job.status = "stopped";
      job.error = null;
    } else {
      job.status = "failed";
      job.error =
        stderr ||
        `Persistent Pull Source worker exited code=${code} signal=${signal || "none"}`;
    }
  });

  return job;
}

function startPersistentPullSource(
  requestId,
  sourceId,
  channelId,
  protocol,
  sourceUrl,
  streamKey,
) {
  cleanupJobs();
  if (requestId && requestIds.has(requestId)) {
    return jobs.get(requestIds.get(requestId));
  }

  const normalized = normalizeProbeProtocol(protocol);
  if (!normalized || !PULL_SOURCE_START_PROTOCOLS.has(normalized)) {
    throw new Error("Unsupported Pull Source start protocol");
  }
  if (!Number.isInteger(sourceId) || sourceId <= 0)
    throw new Error("Invalid source_id");
  if (!Number.isInteger(channelId) || channelId <= 0)
    throw new Error("Invalid channel_id");
  if (!isValidStreamKey(streamKey)) throw new Error("Invalid stream key");

  const existing = findActivePullSourceJob(sourceId, channelId);
  if (existing) {
    const error = new Error(
      "A persistent Pull Source worker is already active for this source or channel",
    );
    error.status = 409;
    error.existingJob = existing;
    throw error;
  }

  const cleanUrl = validatePullProbeUrl(sourceUrl, normalized);
  const job = makeJob(requestId, "pull_source_start", {
    source_id: sourceId,
    channel_id: channelId,
    protocol: normalized,
    persistent: true,
    bitrate_kbps: 0,
    stop_timer: null,
    sensitive_values: [cleanUrl],
    stream_key: streamKey,
  });
  const args = buildPersistentPullSourceArgs(cleanUrl, normalized, streamKey);
  const child = spawn("ffmpeg", args, {
    stdio: ["ignore", "pipe", "pipe"],
    shell: false,
  });
  return attachPersistentPullSourceProcess(job, child);
}

function startPullSourceProbe(
  requestId,
  sourceId,
  channelId,
  protocol,
  sourceUrl,
) {
  cleanupJobs();
  if (requestId && requestIds.has(requestId)) {
    return jobs.get(requestIds.get(requestId));
  }

  const normalized = normalizeProbeProtocol(protocol);
  if (!normalized) throw new Error("Unsupported pull-source probe protocol");

  if (!Number.isInteger(sourceId) || sourceId <= 0) {
    throw new Error("Invalid source_id");
  }
  if (!Number.isInteger(channelId) || channelId <= 0) {
    throw new Error("Invalid channel_id");
  }

  const cleanUrl = validatePullProbeUrl(sourceUrl, normalized);
  const job = makeJob(requestId, "pull_source_probe", {
    source_id: sourceId,
    channel_id: channelId,
    protocol: normalized,
    sensitive_values: [cleanUrl],
  });

  // Read-only real Pull Source validation. Nothing is published or written.
  const inputArgs = ["-hide_banner", "-loglevel", "error"];
  if (normalized === "rtsp") {
    inputArgs.push("-rtsp_transport", "tcp");
  } else if (
    normalized === "rtmp" ||
    normalized === "rtmps" ||
    normalized === "hls" ||
    normalized === "http_flv"
  ) {
    inputArgs.push("-rw_timeout", "15000000");
  }

  const args = [
    ...inputArgs,
    "-i",
    cleanUrl,
    "-t",
    "5",
    "-map",
    "0:v:0?",
    "-map",
    "0:a:0?",
    "-c",
    "copy",
    "-f",
    "null",
    "-",
  ];

  const child = spawn("ffmpeg", args, {
    stdio: ["ignore", "ignore", "pipe"],
    shell: false,
  });
  return attachJobProcess(job, child);
}

async function handleCreateJob(req, res) {
  const body = await readJsonBody(req);
  if (!body || typeof body !== "object" || Array.isArray(body)) {
    sendJson(res, 400, { ok: false, error: "Job body must be an object" });
    return;
  }

  const allowedTypes = new Set([
    "ffmpeg_probe",
    "ffmpeg_stop_probe",
    "live_stream_probe",
    "pull_source_probe",
    "pull_source_start",
  ]);
  if (!allowedTypes.has(body.type)) {
    sendJson(res, 400, { ok: false, error: "Unsupported job type" });
    return;
  }

  const allowedKeys =
    body.type === "live_stream_probe"
      ? new Set(["type", "request_id", "channel_id", "stream_key"])
      : body.type === "pull_source_probe" || body.type === "pull_source_start"
        ? new Set([
            "type",
            "request_id",
            "source_id",
            "channel_id",
            "protocol",
            "source_url",
            ...(body.type === "pull_source_start" ? ["stream_key"] : []),
          ])
        : new Set(["type", "request_id"]);

  const unknownKeys = Object.keys(body).filter((key) => !allowedKeys.has(key));
  if (unknownKeys.length) {
    sendJson(res, 400, {
      ok: false,
      error: `Unsupported job fields: ${unknownKeys.join(", ")}`,
    });
    return;
  }

  const requestId =
    body.request_id == null ? null : String(body.request_id).trim();
  if (requestId && !/^[A-Za-z0-9._:-]{1,128}$/.test(requestId)) {
    sendJson(res, 400, { ok: false, error: "Invalid request_id" });
    return;
  }

  if (requestId && requestIds.has(requestId)) {
    const existing = jobs.get(requestIds.get(requestId));
    if (existing && existing.type !== body.type) {
      sendJson(res, 409, {
        ok: false,
        error: "request_id is already associated with a different job type",
        job: publicJob(existing),
      });
      return;
    }
  }

  let job;
  if (body.type === "pull_source_probe" || body.type === "pull_source_start") {
    const sourceId = Number(body.source_id);
    const channelId = Number(body.channel_id);
    const protocol = normalizeProbeProtocol(body.protocol);
    const sourceUrl = String(body.source_url || "").trim();

    if (!Number.isInteger(sourceId) || sourceId <= 0) {
      sendJson(res, 400, { ok: false, error: "Invalid source_id" });
      return;
    }
    if (!Number.isInteger(channelId) || channelId <= 0) {
      sendJson(res, 400, { ok: false, error: "Invalid channel_id" });
      return;
    }
    if (!protocol) {
      sendJson(res, 400, {
        ok: false,
        error: "Unsupported Pull Source protocol",
      });
      return;
    }
    if (!sourceUrl || sourceUrl.length > 4096) {
      sendJson(res, 400, { ok: false, error: "Invalid Pull Source URL" });
      return;
    }

    if (body.type === "pull_source_start") {
      const streamKey = String(body.stream_key || "").trim();
      if (!isValidStreamKey(streamKey)) {
        sendJson(res, 400, { ok: false, error: "Invalid stream key" });
        return;
      }
      try {
        job = startPersistentPullSource(
          requestId,
          sourceId,
          channelId,
          protocol,
          sourceUrl,
          streamKey,
        );
      } catch (error) {
        if (error?.status === 409) {
          sendJson(res, 409, {
            ok: false,
            ...baseIdentity(),
            error: error.message,
            job: error.existingJob ? publicJob(error.existingJob) : null,
          });
          return;
        }
        throw error;
      }
    } else {
      job = startPullSourceProbe(
        requestId,
        sourceId,
        channelId,
        protocol,
        sourceUrl,
      );
    }
  } else if (body.type === "live_stream_probe") {
    const channelId = Number(body.channel_id);
    const streamKey = String(body.stream_key || "").trim();
    if (!Number.isInteger(channelId) || channelId <= 0) {
      sendJson(res, 400, { ok: false, error: "Invalid channel_id" });
      return;
    }
    if (!isValidStreamKey(streamKey)) {
      sendJson(res, 400, { ok: false, error: "Invalid stream key" });
      return;
    }
    job = startLiveStreamProbe(requestId, channelId, streamKey);
  } else {
    job = startFfmpegProbe(requestId, body.type);
  }

  sendJson(res, 202, { ok: true, ...baseIdentity(), job: publicJob(job) });
}

function handleGetJob(res, id) {
  cleanupJobs();
  const job = jobs.get(id);
  if (!job) return sendJson(res, 404, { ok: false, error: "Job not found" });
  sendJson(res, 200, { ok: true, ...baseIdentity(), job: publicJob(job) });
}

function handleStopJob(res, id) {
  const job = jobs.get(id);
  if (!job) return sendJson(res, 404, { ok: false, error: "Job not found" });
  if (job.type === "pull_source_start") {
    return sendJson(res, 409, {
      ok: false,
      error:
        "Persistent Pull Source jobs must be stopped through the authoritative Pull Source stop endpoint",
      job: publicJob(job),
    });
  }
  if (!["starting", "running"].includes(job.status) || !job.child) {
    return sendJson(res, 409, {
      ok: false,
      error: `Job is already ${job.status}`,
      job: publicJob(job),
    });
  }
  job.status = "stopping";
  const child = job.child;
  child.kill("SIGTERM");
  if (job.persistent === true) {
    job.stop_timer = setTimeout(() => {
      if (job.child === child && child.exitCode === null) child.kill("SIGKILL");
    }, 5000);
    job.stop_timer.unref?.();
  }
  sendJson(res, 202, { ok: true, ...baseIdentity(), job: publicJob(job) });
}

function latestPullSourceJob(sourceId, channelId = null) {
  return (
    Array.from(jobs.values())
      .filter(
        (job) =>
          job.type === "pull_source_start" &&
          job.source_id === sourceId &&
          (channelId == null || job.channel_id === channelId),
      )
      .sort(
        (a, b) => Date.parse(b.created_at || 0) - Date.parse(a.created_at || 0),
      )[0] || null
  );
}

function requestPersistentPullSourceStop(job) {
  if (!job) return { accepted: false, already_stopped: true };

  if (job.status === "stopping") {
    return { accepted: true, already_stopped: false };
  }

  if (!["starting", "running"].includes(job.status) || !job.child) {
    return { accepted: false, already_stopped: true };
  }

  job.status = "stopping";
  const child = job.child;
  child.kill("SIGTERM");
  if (job.stop_timer) clearTimeout(job.stop_timer);
  job.stop_timer = setTimeout(() => {
    if (job.child === child && child.exitCode === null) child.kill("SIGKILL");
  }, 5000);
  job.stop_timer.unref?.();
  return { accepted: true, already_stopped: false };
}

async function handlePullSourceStop(req, res, sourceId) {
  cleanupJobs();
  if (!Number.isInteger(sourceId) || sourceId <= 0) {
    return sendJson(res, 400, { ok: false, error: "Invalid source_id" });
  }

  const body = await readJsonBody(req);
  if (!body || typeof body !== "object" || Array.isArray(body)) {
    return sendJson(res, 400, {
      ok: false,
      error: "Pull Source stop body must be an object",
    });
  }
  const unknownKeys = Object.keys(body).filter((key) => key !== "channel_id");
  if (unknownKeys.length) {
    return sendJson(res, 400, {
      ok: false,
      error: `Unsupported Pull Source stop fields: ${unknownKeys.join(", ")}`,
    });
  }

  const channelId = Number(body.channel_id);
  if (!Number.isInteger(channelId) || channelId <= 0) {
    return sendJson(res, 400, { ok: false, error: "Invalid channel_id" });
  }

  const job = latestPullSourceJob(sourceId, channelId);
  if (!job) {
    return sendJson(res, 200, {
      ok: true,
      ...baseIdentity(),
      source_id: sourceId,
      channel_id: channelId,
      result: "not_running",
      stop_accepted: false,
      already_stopped: true,
      job: null,
    });
  }

  const stop = requestPersistentPullSourceStop(job);
  sendJson(res, stop.accepted ? 202 : 200, {
    ok: true,
    ...baseIdentity(),
    source_id: sourceId,
    channel_id: channelId,
    result: stop.accepted ? "stopping" : "already_stopped",
    stop_accepted: stop.accepted,
    already_stopped: stop.already_stopped,
    job: publicJob(job),
  });
}

async function handlePullSourceRuntimeStatus(res, sourceId) {
  cleanupJobs();
  if (!Number.isInteger(sourceId) || sourceId <= 0) {
    return sendJson(res, 400, { ok: false, error: "Invalid source_id" });
  }

  const matching = Array.from(jobs.values())
    .filter(
      (job) => job.type === "pull_source_start" && job.source_id === sourceId,
    )
    .sort(
      (a, b) => Date.parse(b.created_at || 0) - Date.parse(a.created_at || 0),
    );
  const job = matching[0] || null;

  let canonical = {
    expected: false,
    publish_active: false,
    clients: 0,
    frames: 0,
    recv_kbps: 0,
    send_kbps: 0,
    error: null,
  };

  if (job?.stream_key) {
    canonical.expected = ["starting", "running", "stopping"].includes(
      job.status,
    );
    try {
      const streams = await getSanitizedStreams();
      const stream = streams.find(
        (item) => item.app === "live" && item.name === job.stream_key,
      );
      if (stream) {
        canonical = {
          ...canonical,
          publish_active: stream.publish_active === true,
          clients: Number(stream.clients || 0),
          frames: Number(stream.frames || 0),
          recv_kbps: Number(stream?.kbps?.recv_30s || 0),
          send_kbps: Number(stream?.kbps?.send_30s || 0),
        };
      }
    } catch (error) {
      canonical.error = error?.message || "Unable to read SRS stream inventory";
    }
  }

  const active =
    !!job && ["starting", "running", "stopping"].includes(job.status);
  const processAlive =
    !!job?.child && job.child.exitCode === null && !job.child.killed;

  sendJson(res, 200, {
    ok: true,
    ...baseIdentity(),
    source_id: sourceId,
    runtime: {
      found: !!job,
      active,
      process_alive: processAlive,
      child_pid: processAlive ? job.child.pid : null,
      job: job ? publicJob(job) : null,
      canonical_publish: canonical,
      healthy:
        !!job &&
        job.status === "running" &&
        processAlive &&
        canonical.publish_active === true,
    },
  });
}

async function handleHealth(res) {
  const startedAt = Date.now();
  const system = getLocalSystemMetrics();
  const srs = await getSrsHealth(srsApiUrl);
  const ffmpegProcesses = getFfmpegProcessCount();

  sendJson(res, srs.healthy ? 200 : 503, {
    ok: srs.healthy,
    ...baseIdentity(),
    api_healthy: true,
    srs_healthy: srs.healthy,
    active_streams: srs.active_streams,
    ffmpeg_processes: ffmpegProcesses,
    cpu_percent: system.cpu_percent,
    memory_percent: system.memory_percent,
    disk_percent: system.disk_percent,
    load_1m: system.load_1m,
    disk_free_bytes: getDiskFreeBytes(),
    last_error: srs.error,
    response_ms: Date.now() - startedAt,
  });
}

function handleCapabilities(res) {
  sendJson(res, 200, {
    ok: true,
    ...baseIdentity(),
    mode: "controlled_jobs",
    capabilities: {
      health: true,
      system_metrics: true,
      srs_health: true,
      stream_inventory: true,
      ffmpeg_process_count: true,
      remote_job_start: true,
      remote_job_stop: true,
      allowed_job_types: [
        "ffmpeg_probe",
        "ffmpeg_stop_probe",
        "live_stream_probe",
        "pull_source_probe",
        "pull_source_start",
      ],
      persistent_pull_source_start: true,
      pull_source_runtime_status: true,
      authoritative_pull_source_stop: true,
      stream_migration: false,
      automatic_load_balancing: false,
      secure_remote_transport: useTls,
      node_identity_response: true,
    },
    protocols: {
      ingest: ["rtmp", "rtmps", "srt"],
      pull_foundation: ["rtmp", "rtmps", "rtsp", "srt", "hls", "http-flv"],
      playback: ["hls"],
    },
  });
}

async function handleStreams(res) {
  try {
    const streams = await getSanitizedStreams();
    sendJson(res, 200, {
      ok: true,
      ...baseIdentity(),
      count: streams.length,
      active_count: streams.filter((stream) => stream.publish_active).length,
      streams,
    });
  } catch (error) {
    sendJson(res, 503, {
      ok: false,
      ...baseIdentity(),
      error:
        error?.name === "AbortError"
          ? "SRS stream inventory timed out"
          : error?.message || "Unable to read SRS streams",
    });
  }
}

function handleFfmpeg(res) {
  sendJson(res, 200, {
    ok: true,
    ...baseIdentity(),
    process_count: getFfmpegProcessCount(),
    note: "Phase 4A intentionally does not expose FFmpeg command lines or secrets.",
  });
}

const requestHandler = async (req, res) => {
  try {
    const requestProtocol = useTls ? "https" : "http";
    const url = new URL(
      req.url || "/",
      `${requestProtocol}://${req.headers.host || "localhost"}`,
    );

    if (!isAuthorized(req)) {
      res.setHeader("WWW-Authenticate", 'Bearer realm="nlm-media-node-agent"');
      sendJson(res, 401, { ok: false, error: "Unauthorized" });
      return;
    }

    if (req.method === "POST" && url.pathname === "/v1/jobs") {
      await handleCreateJob(req, res);
      return;
    }
    const jobMatch = url.pathname.match(/^\/v1\/jobs\/([0-9a-f-]{36})$/i);
    if (jobMatch && req.method === "GET") {
      handleGetJob(res, jobMatch[1]);
      return;
    }
    if (jobMatch && req.method === "POST") {
      handleStopJob(res, jobMatch[1]);
      return;
    }
    const pullSourceStopMatch = url.pathname.match(
      /^\/v1\/pull-sources\/(\d+)\/stop$/,
    );
    if (pullSourceStopMatch && req.method === "POST") {
      await handlePullSourceStop(req, res, Number(pullSourceStopMatch[1]));
      return;
    }
    const pullSourceStatusMatch = url.pathname.match(
      /^\/v1\/pull-sources\/(\d+)\/status$/,
    );
    if (pullSourceStatusMatch && req.method === "GET") {
      await handlePullSourceRuntimeStatus(
        res,
        Number(pullSourceStatusMatch[1]),
      );
      return;
    }
    if (req.method !== "GET") {
      sendJson(res, 405, { ok: false, error: "Method not allowed" });
      return;
    }

    switch (url.pathname) {
      case "/v1/health":
        await handleHealth(res);
        return;
      case "/v1/capabilities":
        handleCapabilities(res);
        return;
      case "/v1/streams":
        await handleStreams(res);
        return;
      case "/v1/processes/ffmpeg":
        handleFfmpeg(res);
        return;
      default:
        sendJson(res, 404, { ok: false, error: "Not found" });
    }
  } catch (error) {
    console.error("[MediaNodeAgent] request failed:", error);
    if (!res.headersSent) {
      sendJson(res, 500, { ok: false, error: "Internal agent error" });
    } else {
      res.end();
    }
  }
};

let server;
if (useTls) {
  try {
    server = https.createServer(
      {
        cert: fs.readFileSync(tlsCertFile),
        key: fs.readFileSync(tlsKeyFile),
        minVersion: "TLSv1.2",
      },
      requestHandler,
    );
  } catch (error) {
    console.error(
      "[MediaNodeAgent] Failed to load TLS certificate/key:",
      error.message,
    );
    process.exit(1);
  }
} else {
  server = http.createServer(requestHandler);
}

server.requestTimeout = 10_000;
server.headersTimeout = 12_000;
server.keepAliveTimeout = 5_000;
server.maxRequestsPerSocket = 100;

server.on("clientError", (error, socket) => {
  console.warn("[MediaNodeAgent] client error:", error.message);
  if (socket.writable) {
    socket.end("HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n");
  }
});

server.listen(port, host, () => {
  console.log(
    `[MediaNodeAgent] v${AGENT_VERSION} listening on ${useTls ? "https" : "http"}://${host}:${port} node=${nodeId || "unset"} mode=controlled-jobs`,
  );
});

function shutdown(signal) {
  console.log(`[MediaNodeAgent] ${signal} received; shutting down`);

  // 4D.4B does not yet implement persistent-job reconciliation after an agent
  // restart. Stop agent-owned workers deliberately so no orphan FFmpeg process
  // can continue publishing without a control-plane owner.
  for (const job of jobs.values()) {
    if (job.child && ["starting", "running", "stopping"].includes(job.status)) {
      try {
        job.status = "stopping";
        job.child.kill("SIGTERM");
      } catch {}
    }
  }

  server.close(() => process.exit(0));

  const timer = setTimeout(() => {
    for (const job of jobs.values()) {
      if (job.child && job.child.exitCode === null) {
        try {
          job.child.kill("SIGKILL");
        } catch {}
      }
    }
    process.exit(1);
  }, 5000);
  timer.unref?.();
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));
