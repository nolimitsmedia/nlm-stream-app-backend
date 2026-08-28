"use strict";

// Phase 4D.1 — secure multi-node transport plus strictly controlled job requests.

const net = require("net");

const DEFAULT_TIMEOUT_MS = 2500;
const ALLOWED_GET_PATHS = new Set([
  "/v1/health",
  "/v1/capabilities",
  "/v1/streams",
  "/v1/processes/ffmpeg",
]);

function cleanHost(value) {
  return String(value || "")
    .trim()
    .replace(/^\[/, "")
    .replace(/\]$/, "")
    .replace(/\/\d+$/, "")
    .toLowerCase();
}

function isLoopbackHost(hostname) {
  const host = cleanHost(hostname);
  return (
    host === "localhost" ||
    host === "127.0.0.1" ||
    host === "::1" ||
    host.startsWith("127.")
  );
}

function normalizeBaseUrl(value, options = {}) {
  const raw = String(value || "").trim();
  if (!raw) return null;

  const parsed = new URL(raw);
  if (!["http:", "https:"].includes(parsed.protocol)) {
    throw new Error("Media Node Agent URL must use http or https");
  }
  if (parsed.username || parsed.password) {
    throw new Error("Media Node Agent URL must not contain credentials");
  }
  if (parsed.search || parsed.hash) {
    throw new Error(
      "Media Node Agent URL must not contain query parameters or fragments",
    );
  }
  if (parsed.pathname && parsed.pathname !== "/") {
    throw new Error("Media Node Agent URL must not contain a path");
  }

  const allowLoopbackHttp = options.allowLoopbackHttp === true;
  const allowInsecureRemote = options.allowInsecureRemote === true;
  const loopback = isLoopbackHost(parsed.hostname);

  if (
    parsed.protocol !== "https:" &&
    !(allowLoopbackHttp && loopback) &&
    !allowInsecureRemote
  ) {
    throw new Error("Remote Media Node Agent URLs must use HTTPS");
  }

  parsed.pathname = "";
  parsed.search = "";
  parsed.hash = "";
  return parsed.toString().replace(/\/$/, "");
}

function getAllowedNodeHosts(node) {
  return new Set(
    [node?.hostname, node?.public_ip, node?.private_ip]
      .map(cleanHost)
      .filter(Boolean),
  );
}

function validateAgentBaseUrlForNode(node, value, options = {}) {
  const nodeId = Number(node?.id || 0);
  const localNodeId = Number(options.localNodeId || 0);
  const isLocalNode = localNodeId > 0 && nodeId === localNodeId;

  const normalized = normalizeBaseUrl(value, {
    allowLoopbackHttp: isLocalNode,
    allowInsecureRemote: options.allowInsecureRemote === true,
  });
  if (!normalized) return null;

  const parsed = new URL(normalized);
  const urlHost = cleanHost(parsed.hostname);

  // A co-located node may use loopback. A remote node must resolve through one
  // of the node's registered identities to prevent the agent setting from
  // becoming an arbitrary server-side request target.
  if (isLocalNode && isLoopbackHost(urlHost)) {
    return normalized;
  }

  const allowedHosts = getAllowedNodeHosts(node);
  if (!allowedHosts.has(urlHost)) {
    throw new Error(
      "Media Node Agent host must match the node hostname, public IP, or private IP",
    );
  }

  if (
    !isLocalNode &&
    parsed.protocol !== "https:" &&
    options.allowInsecureRemote !== true
  ) {
    throw new Error("Remote Media Node Agent transport must use HTTPS");
  }

  return normalized;
}

function resolveAgentBaseUrl(node, options = {}) {
  const localNodeId = Number(options.localNodeId || 0);
  const localPort = Number(options.localPort || 5091);
  const nodeId = Number(node?.id || 0);

  // Keep Media Node 01 private and backward-compatible. No public listener is
  // needed while the control plane and agent are co-located.
  if (localNodeId > 0 && nodeId === localNodeId) {
    const explicit = node?.metadata?.agent_url;
    if (explicit) {
      return validateAgentBaseUrlForNode(node, explicit, options);
    }
    return `http://127.0.0.1:${localPort}`;
  }

  const metadataUrl = node?.metadata?.agent_url;
  return metadataUrl
    ? validateAgentBaseUrlForNode(node, metadataUrl, options)
    : null;
}

function resolveAgentToken(node, options = {}) {
  const nodeId = Number(node?.id || 0);
  const localNodeId = Number(options.localNodeId || 0);
  if (!Number.isInteger(nodeId) || nodeId <= 0) return null;

  // Remote nodes receive a unique control-plane credential. This intentionally
  // avoids reusing Media Node 01's bearer token across the cluster.
  const specificToken = String(
    process.env[`MEDIA_NODE_AGENT_TOKEN_${nodeId}`] || "",
  ).trim();
  if (specificToken) return specificToken;

  if (localNodeId > 0 && nodeId === localNodeId) {
    return String(options.localToken || "").trim() || null;
  }

  return null;
}

function resolveAgentConnection(node, options = {}) {
  const baseUrl = resolveAgentBaseUrl(node, options);
  const token = resolveAgentToken(node, options);
  const parsed = baseUrl ? new URL(baseUrl) : null;

  return {
    configured: Boolean(baseUrl),
    baseUrl,
    token,
    tokenConfigured: Boolean(token && token.length >= 32),
    transport: parsed
      ? isLoopbackHost(parsed.hostname)
        ? "loopback"
        : parsed.protocol === "https:"
          ? "https"
          : "http"
      : null,
  };
}

function verifyAgentIdentity(data, expectedNodeId) {
  if (expectedNodeId == null) return;
  const expected = String(expectedNodeId);
  const received = data?.node_id == null ? "" : String(data.node_id);
  if (!received || received !== expected) {
    const error = new Error(
      `Media Node Agent identity mismatch: expected node ${expected}, received ${received || "unset"}`,
    );
    error.code = "AGENT_IDENTITY_MISMATCH";
    throw error;
  }
}

async function requestMediaNodeAgent({
  baseUrl,
  token,
  path,
  method = "GET",
  body = null,
  timeoutMs = DEFAULT_TIMEOUT_MS,
  expectedNodeId = null,
}) {
  const normalizedMethod = String(method).toUpperCase();
  const jobPath = /^\/v1\/jobs(?:\/[0-9a-f-]{36})?$/i.test(path);
  const allowed =
    (normalizedMethod === "GET" && (ALLOWED_GET_PATHS.has(path) || jobPath)) ||
    (normalizedMethod === "POST" && jobPath);
  if (!allowed)
    throw new Error(
      `Unsupported Media Node Agent request: ${normalizedMethod} ${path}`,
    );
  if (!baseUrl) throw new Error("Media Node Agent URL is not configured");
  if (!token || String(token).length < 32) {
    throw new Error("Media Node Agent token is not configured for this node");
  }

  const controller = new AbortController();
  const safeTimeout = Math.max(500, Number(timeoutMs) || DEFAULT_TIMEOUT_MS);
  const timer = setTimeout(() => controller.abort(), safeTimeout);
  timer.unref?.();
  const startedAt = Date.now();

  try {
    const response = await fetch(
      `${normalizeBaseUrl(baseUrl, {
        allowLoopbackHttp: true,
      })}${path}`,
      {
        method: normalizedMethod,
        headers: {
          Authorization: `Bearer ${token}`,
          Accept: "application/json",
          ...(body != null ? { "Content-Type": "application/json" } : {}),
          "X-NLM-Control-Plane": "1",
        },
        ...(body != null ? { body: JSON.stringify(body) } : {}),
        signal: controller.signal,
      },
    );

    const text = await response.text();
    let body = null;
    try {
      body = text ? JSON.parse(text) : null;
    } catch {
      body = null;
    }

    if (!response.ok) {
      const error = new Error(
        body?.error ||
          body?.message ||
          `Media Node Agent returned HTTP ${response.status}`,
      );
      error.status = response.status;
      throw error;
    }

    verifyAgentIdentity(body, expectedNodeId);

    return {
      ok: true,
      status: response.status,
      response_ms: Date.now() - startedAt,
      data: body,
    };
  } catch (error) {
    if (error?.name === "AbortError") {
      const timeoutError = new Error(
        `Media Node Agent request timed out after ${safeTimeout}ms`,
      );
      timeoutError.code = "AGENT_TIMEOUT";
      throw timeoutError;
    }
    throw error;
  } finally {
    clearTimeout(timer);
  }
}

async function getAgentHealth(options) {
  return requestMediaNodeAgent({ ...options, path: "/v1/health" });
}

module.exports = {
  DEFAULT_TIMEOUT_MS,
  isLoopbackHost,
  normalizeBaseUrl,
  validateAgentBaseUrlForNode,
  resolveAgentBaseUrl,
  resolveAgentToken,
  resolveAgentConnection,
  verifyAgentIdentity,
  requestMediaNodeAgent,
  getAgentHealth,
};
