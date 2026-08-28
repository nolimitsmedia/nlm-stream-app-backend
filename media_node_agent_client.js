// server/media_node_agent_client.js
// Phase 4B — authenticated, read-only control-plane client for Media Node Agent.

const DEFAULT_TIMEOUT_MS = 2500;
const ALLOWED_PATHS = new Set([
  "/v1/health",
  "/v1/capabilities",
  "/v1/streams",
  "/v1/processes/ffmpeg",
]);

function normalizeBaseUrl(value) {
  const raw = String(value || "").trim();
  if (!raw) return null;
  const parsed = new URL(raw);
  if (!["http:", "https:"].includes(parsed.protocol)) {
    throw new Error("Media Node Agent URL must use http or https");
  }
  parsed.pathname = parsed.pathname.replace(/\/+$/, "");
  parsed.search = "";
  parsed.hash = "";
  return parsed.toString().replace(/\/$/, "");
}

function resolveAgentBaseUrl(node, options = {}) {
  const localNodeId = Number(options.localNodeId || 0);
  const localPort = Number(options.localPort || 5091);
  const nodeId = Number(node?.id || 0);

  // Phase 4B safely supports the co-located production node without exposing
  // the agent port publicly. Future remote nodes may explicitly advertise an
  // agent_url in metadata once a private/TLS transport is configured.
  if (localNodeId > 0 && nodeId === localNodeId) {
    return `http://127.0.0.1:${localPort}`;
  }

  const metadataUrl = node?.metadata?.agent_url;
  return metadataUrl ? normalizeBaseUrl(metadataUrl) : null;
}

async function requestMediaNodeAgent({
  baseUrl,
  token,
  path,
  timeoutMs = DEFAULT_TIMEOUT_MS,
}) {
  if (!ALLOWED_PATHS.has(path)) {
    throw new Error(`Unsupported Media Node Agent path: ${path}`);
  }
  if (!baseUrl) throw new Error("Media Node Agent URL is not configured");
  if (!token || String(token).length < 32) {
    throw new Error("MEDIA_NODE_AGENT_TOKEN is not configured");
  }

  const controller = new AbortController();
  const timer = setTimeout(
    () => controller.abort(),
    Math.max(500, Number(timeoutMs) || DEFAULT_TIMEOUT_MS),
  );
  timer.unref?.();
  const startedAt = Date.now();

  try {
    const response = await fetch(`${normalizeBaseUrl(baseUrl)}${path}`, {
      method: "GET",
      headers: {
        Authorization: `Bearer ${token}`,
        Accept: "application/json",
      },
      signal: controller.signal,
    });

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

    return {
      ok: true,
      status: response.status,
      response_ms: Date.now() - startedAt,
      data: body,
    };
  } catch (error) {
    if (error?.name === "AbortError") {
      const timeoutError = new Error(
        `Media Node Agent request timed out after ${timeoutMs}ms`,
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
  resolveAgentBaseUrl,
  requestMediaNodeAgent,
  getAgentHealth,
};
