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

const AGENT_VERSION = "4D.2";
const DEFAULT_HOST = "127.0.0.1";
const DEFAULT_PORT = 5091;
const DEFAULT_SRS_API_URL = "http://127.0.0.1:1985";
const MAX_RESPONSE_BYTES = 1024 * 1024;
const MAX_REQUEST_BYTES = 16 * 1024;
const JOB_RETENTION_MS = 60 * 60 * 1000;
const JOB_MAX_RUNTIME_MS = 15 * 1000;
const jobs = new Map();
const requestIds = new Map();

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

function publicJob(job) {
  return {
    id: job.id,
    request_id: job.request_id,
    type: job.type,
    status: job.status,
    created_at: job.created_at,
    started_at: job.started_at,
    finished_at: job.finished_at,
    exit_code: job.exit_code,
    signal: job.signal,
    error: job.error,
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

function startFfmpegProbe(requestId, jobType = "ffmpeg_probe") {
  cleanupJobs();
  if (requestId && requestIds.has(requestId))
    return jobs.get(requestIds.get(requestId));
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
  };
  jobs.set(id, job);
  if (requestId) requestIds.set(requestId, id);

  // Fixed executable and fixed arguments: callers cannot supply commands, paths, URLs or shell text.
  const durationSeconds = jobType === "ffmpeg_stop_probe" ? "10" : "3";
  const args = [
    "-hide_banner",
    "-loglevel",
    "error",
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
    clearTimeout(job.timer);
  });
  child.on("exit", (code, signal) => {
    job.exit_code = code;
    job.signal = signal || null;
    job.finished_at = new Date().toISOString();
    job.child = null;
    clearTimeout(job.timer);
    if (job.status === "stopping") job.status = "stopped";
    else if (code === 0) job.status = "succeeded";
    else {
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

async function handleCreateJob(req, res) {
  const body = await readJsonBody(req);
  const allowedKeys = new Set(["type", "request_id"]);
  const unknownKeys = Object.keys(body || {}).filter(
    (key) => !allowedKeys.has(key),
  );
  if (unknownKeys.length) {
    sendJson(res, 400, {
      ok: false,
      error: `Unsupported job fields: ${unknownKeys.join(", ")}`,
    });
    return;
  }

  const allowedTypes = new Set(["ffmpeg_probe", "ffmpeg_stop_probe"]);
  if (!allowedTypes.has(body.type)) {
    sendJson(res, 400, { ok: false, error: "Unsupported job type" });
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

  const job = startFfmpegProbe(requestId, body.type);
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
  if (!["starting", "running"].includes(job.status) || !job.child) {
    return sendJson(res, 409, {
      ok: false,
      error: `Job is already ${job.status}`,
      job: publicJob(job),
    });
  }
  job.status = "stopping";
  job.child.kill("SIGTERM");
  sendJson(res, 202, { ok: true, ...baseIdentity(), job: publicJob(job) });
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
      allowed_job_types: ["ffmpeg_probe", "ffmpeg_stop_probe"],
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
  server.close(() => process.exit(0));

  const timer = setTimeout(() => process.exit(1), 5000);
  timer.unref?.();
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));
