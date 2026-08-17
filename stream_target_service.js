// stream_target_service.js
// Phase 2 — Generic Stream Targets process/orchestration engine.

const { spawn } = require("child_process");
const crypto = require("crypto");
const net = require("net");
const tls = require("tls");
const dns = require("dns").promises;
const facebookGraph = require("./facebook_graph_service");
const youtubeApi = require("./youtube_api_service");
const {
  encryptOAuthToken,
  decryptOAuthAccount,
} = require("./social_oauth_schema");

const TARGET_TYPES = {
  facebook: {
    label: "Facebook",
    protocol: "rtmps",
    oauth: true,
    baseUrl: "rtmps://live-api-s.facebook.com:443/rtmp",
  },
  youtube: {
    label: "YouTube",
    protocol: "rtmp",
    oauth: true,
    baseUrl: "rtmp://a.rtmp.youtube.com/live2",
  },
  instagram: {
    label: "Instagram",
    protocol: "rtmps",
    baseUrl: "rtmps://live-upload.instagram.com:443/rtmp",
  },
  twitch: { label: "Twitch", protocol: "rtmps" },
  linkedin: { label: "LinkedIn Live", protocol: "rtmps" },
  x: { label: "X / Custom Social", protocol: "rtmps" },
  custom_rtmp: { label: "Custom RTMP", protocol: "rtmp" },
  custom_rtmps: { label: "Custom RTMPS", protocol: "rtmps" },
  srt: { label: "SRT Destination", protocol: "srt" },
  nlm_server: { label: "Another NLM Streaming Server", protocol: "rtmp" },
  cdn_origin: { label: "CDN / Origin", protocol: "rtmp" },
};

const RECONNECT_DELAY_MS = Number(
  process.env.STREAM_TARGET_RECONNECT_DELAY_MS || 5000,
);
const MAX_RECONNECT_DELAY_MS = Number(
  process.env.STREAM_TARGET_MAX_RECONNECT_DELAY_MS || 30000,
);

const STREAM_TARGET_MAX_RECONNECT_ATTEMPTS = Math.max(
  1,
  Number(process.env.STREAM_TARGET_MAX_RECONNECT_ATTEMPTS || 8),
);

const STREAM_TARGET_RECONNECT_JITTER_PERCENT = Math.max(
  0,
  Math.min(
    0.5,
    Number(process.env.STREAM_TARGET_RECONNECT_JITTER_PERCENT || 0.15),
  ),
);

const STREAM_TARGET_PREFLIGHT_TIMEOUT_MS = Math.max(
  1000,
  Number(process.env.STREAM_TARGET_PREFLIGHT_TIMEOUT_MS || 5000),
);

const STREAM_TARGET_PREFLIGHT_ENABLED =
  String(
    process.env.STREAM_TARGET_PREFLIGHT_ENABLED || "true",
  ).toLowerCase() !== "false";

const STREAM_TARGET_DELIVERY_VERIFY_TIMEOUT_MS = Math.max(
  5000,
  Number(process.env.STREAM_TARGET_DELIVERY_VERIFY_TIMEOUT_MS || 15000),
);

const STREAM_TARGET_MIN_VERIFIED_BYTES = Math.max(
  1,
  Number(process.env.STREAM_TARGET_MIN_VERIFIED_BYTES || 1024),
);

const STREAM_TARGET_MIN_VERIFIED_OUT_TIME_MS = Math.max(
  0,
  Number(process.env.STREAM_TARGET_MIN_VERIFIED_OUT_TIME_MS || 500),
);

// Source HLS produced by SRS is already AAC in the current NLM pipeline.
// Re-encoding that AAC for every simulcast target needlessly invokes the AAC
// decoder and was producing intermittent "invalid band type" decode warnings
// on encrypted HLS segment boundaries. Default to stream-copying audio.
//
// Set STREAM_TARGET_AUDIO_MODE=transcode only if a future source/destination
// combination requires AAC normalization.
const STREAM_TARGET_AUDIO_MODE =
  String(process.env.STREAM_TARGET_AUDIO_MODE || "transcode").toLowerCase() ===
  "copy"
    ? "copy"
    : "transcode";

const STREAM_TARGET_SRT_AUDIO_SAMPLE_RATE = Math.max(
  8000,
  Number(process.env.STREAM_TARGET_SRT_AUDIO_SAMPLE_RATE || 44100),
);
const STREAM_TARGET_SRT_AUDIO_CHANNELS = Math.max(
  1,
  Math.min(2, Number(process.env.STREAM_TARGET_SRT_AUDIO_CHANNELS || 2)),
);

function getTargetAudioArgs(protocol) {
  if (STREAM_TARGET_AUDIO_MODE === "copy") {
    return ["-c:a", "copy"];
  }

  const args = ["-c:a", "aac", "-b:a", "128k"];

  if (protocol === "srt") {
    args.push(
      "-ar",
      String(STREAM_TARGET_SRT_AUDIO_SAMPLE_RATE),
      "-ac",
      String(STREAM_TARGET_SRT_AUDIO_CHANNELS),
    );
  }

  args.push("-af", "aresample=async=1:first_pts=0");

  return args;
}

const STREAM_TARGET_HLS_READY_TIMEOUT_MS = Math.max(
  5000,
  Number(process.env.STREAM_TARGET_HLS_READY_TIMEOUT_MS || 20000),
);
const STREAM_TARGET_HLS_READY_POLL_MS = Math.max(
  250,
  Number(process.env.STREAM_TARGET_HLS_READY_POLL_MS || 750),
);
const STREAM_TARGET_HLS_READY_CONFIRMATIONS = Math.max(
  1,
  Number(process.env.STREAM_TARGET_HLS_READY_CONFIRMATIONS || 2),
);

const STREAM_TARGET_SOURCE_MODE =
  String(process.env.STREAM_TARGET_SOURCE_MODE || "rtmp").toLowerCase() ===
  "hls"
    ? "hls"
    : "rtmp";

const STREAM_TARGET_INTERNAL_RTMP_BASE = String(
  process.env.STREAM_TARGET_INTERNAL_RTMP_BASE || "rtmp://127.0.0.1:1935/live",
).replace(/\/+$/, "");

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function looksLikePlayableHlsPlaylist(text) {
  const body = String(text || "");
  if (!body.includes("#EXTM3U")) return false;

  // SRS HLS segment URIs can vary when encryption/hls_ctx is active.
  // Requiring a .ts/.m4s/.mp4 filename caused valid HTTP 200 playlists to be
  // rejected even while NLM's ABR FFmpeg workers were already consuming them.
  // EXTINF proves this is a media playlist; EXT-X-STREAM-INF proves a valid
  // master playlist.
  return body.includes("#EXTINF:") || body.includes("#EXT-X-STREAM-INF:");
}

function safeNumber(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function redactTargetUrl(url) {
  if (!url) return null;
  const text = String(url);
  try {
    const parsed = new URL(text);
    if (parsed.password) parsed.password = "***";
    const pieces = parsed.pathname.split("/");
    if (pieces.length > 2) pieces[pieces.length - 1] = "***";
    parsed.pathname = pieces.join("/");
    return parsed.toString();
  } catch {
    return text.replace(/\/[^/\s?]{8,}(?=\?|$)/, "/***");
  }
}

function destinationDefaultPort(protocol) {
  if (protocol === "rtmps") return 443;
  if (protocol === "rtmp") return 1935;
  if (protocol === "srt") return null;
  return null;
}

function parseDestinationEndpoint(destinationUrl, protocol) {
  const value = String(destinationUrl || "").trim();
  if (!value) {
    return {
      ok: false,
      code: "missing_destination",
      message: "Destination URL is required",
    };
  }

  try {
    const parsed = new URL(value);
    const expected = `${protocol}:`;
    if (parsed.protocol.toLowerCase() !== expected) {
      return {
        ok: false,
        code: "protocol_mismatch",
        message: `Expected ${protocol.toUpperCase()} destination URL`,
      };
    }

    const port = parsed.port
      ? Number(parsed.port)
      : destinationDefaultPort(protocol);

    if (!parsed.hostname) {
      return {
        ok: false,
        code: "missing_host",
        message: "Destination host is missing",
      };
    }

    if (!port && protocol === "srt") {
      return {
        ok: false,
        code: "missing_port",
        message: "SRT destinations must include an explicit port",
      };
    }

    return {
      ok: true,
      hostname: parsed.hostname,
      port,
      protocol,
    };
  } catch {
    return {
      ok: false,
      code: "invalid_destination_url",
      message: "Destination URL is invalid",
    };
  }
}

async function tcpPreflight(
  hostname,
  port,
  { tlsMode = false, timeoutMs } = {},
) {
  const startedAt = Date.now();
  const timeout = Math.max(
    1000,
    Number(timeoutMs || STREAM_TARGET_PREFLIGHT_TIMEOUT_MS),
  );

  return new Promise((resolve) => {
    let settled = false;
    let socket = null;

    const finish = (result) => {
      if (settled) return;
      settled = true;
      try {
        socket?.destroy();
      } catch {
        // non-fatal
      }
      resolve({
        ...result,
        latency_ms: Math.max(0, Date.now() - startedAt),
      });
    };

    const timer = setTimeout(() => {
      finish({
        ok: false,
        code: "connection_timeout",
        category: "network",
        retryable: true,
        message: `Connection timed out after ${timeout}ms`,
      });
    }, timeout);

    const onReady = () => {
      clearTimeout(timer);
      finish({
        ok: true,
        code: "reachable",
        category: tlsMode ? "tls" : "network",
        retryable: true,
        message: tlsMode
          ? "TLS connection established"
          : "TCP connection established",
      });
    };

    const onError = (error) => {
      clearTimeout(timer);
      const code = String(error?.code || "").toUpperCase();
      const tlsFailure = /CERT|TLS|SSL|HANDSHAKE/i.test(
        `${code} ${error?.message || ""}`,
      );

      finish({
        ok: false,
        code:
          code === "ECONNREFUSED"
            ? "connection_refused"
            : code === "ENOTFOUND" || code === "EAI_AGAIN"
              ? "dns_failure"
              : tlsFailure
                ? "tls_failure"
                : "connection_failed",
        category: tlsFailure ? "tls" : code.includes("DNS") ? "dns" : "network",
        retryable:
          !tlsFailure &&
          !/CERT_HAS_EXPIRED|SELF_SIGNED|UNABLE_TO_VERIFY/i.test(
            `${code} ${error?.message || ""}`,
          ),
        message: String(
          error?.message || "Destination connection failed",
        ).slice(0, 500),
      });
    };

    try {
      socket = tlsMode
        ? tls.connect(
            {
              host: hostname,
              port,
              servername: net.isIP(hostname) ? undefined : hostname,
              rejectUnauthorized: true,
            },
            onReady,
          )
        : net.createConnection({ host: hostname, port }, onReady);

      socket.once("error", onError);
    } catch (error) {
      onError(error);
    }
  });
}

function classifyTargetFailure(
  text,
  { sourceUrl = "", destinationUrl = "" } = {},
) {
  const value = String(text || "");
  const lower = value.toLowerCase();
  const source = String(sourceUrl || "");
  const destination = String(destinationUrl || "");

  const result = {
    scope: "worker",
    code: "ffmpeg_error",
    category: "media",
    retryable: true,
    message: value.trim().slice(-1200) || "FFmpeg target worker failed",
  };

  if (
    /404 not found|failed to reload playlist|when parsing playlist|error when loading first segment|failed to open segment/i.test(
      value,
    ) ||
    (source &&
      value.includes(source) &&
      /connection refused|connection reset by peer|input\/output error|end of file|timed out|server error/i.test(
        value,
      ))
  ) {
    return {
      scope: "source",
      code: "source_unavailable",
      category: "source",
      retryable: true,
      message: "Internal media source temporarily unavailable",
    };
  }

  if (
    /name or service not known|temporary failure in name resolution|enotfound|eai_again/i.test(
      value,
    )
  ) {
    return {
      scope: "destination",
      code: "dns_failure",
      category: "dns",
      retryable: true,
      message: "Destination DNS lookup failed",
    };
  }

  if (
    /certificate verify failed|certificate has expired|self signed certificate|unable to verify|unknown ca/i.test(
      lower,
    )
  ) {
    return {
      scope: "destination",
      code: "tls_certificate_error",
      category: "tls",
      retryable: false,
      message: "Destination TLS certificate validation failed",
    };
  }

  if (/tls|ssl|handshake/i.test(value)) {
    return {
      scope: "destination",
      code: "tls_handshake_failed",
      category: "tls",
      retryable: true,
      message: "Destination TLS handshake failed",
    };
  }

  if (
    /401|403|unauthorized|forbidden|permission denied|authentication failed|invalid stream key|bad name|rejected/i.test(
      value,
    )
  ) {
    return {
      scope: "destination",
      code: "authentication_rejected",
      category: "authentication",
      retryable: false,
      message: "Destination rejected authentication or the stream key",
    };
  }

  if (
    /already publishing|already exists|another publisher|stream is busy|duplicate publisher/i.test(
      value,
    )
  ) {
    return {
      scope: "destination",
      code: "duplicate_publisher",
      category: "destination",
      retryable: false,
      message: "Another publisher may already be using this destination stream",
    };
  }

  if (/connection refused/i.test(value)) {
    return {
      scope: "destination",
      code: "connection_refused",
      category: "network",
      retryable: true,
      message: "Destination refused the connection",
    };
  }

  if (/timed out|timeout/i.test(value)) {
    return {
      scope: "destination",
      code: "connection_timeout",
      category: "network",
      retryable: true,
      message: "Destination connection timed out",
    };
  }

  if (
    /connection reset by peer|broken pipe|network is unreachable|i\/o error|input\/output error/i.test(
      value,
    )
  ) {
    return {
      scope: "destination",
      code: "connection_lost",
      category: "network",
      retryable: true,
      message: "Destination connection was interrupted",
    };
  }

  if (
    /invalid argument|conversion failed|error initializing output|could not write header|muxer does not support/i.test(
      value,
    )
  ) {
    return {
      scope:
        destination && value.includes(destination) ? "destination" : "worker",
      code: "media_conversion_failed",
      category: "media",
      retryable: false,
      message: "FFmpeg could not initialize the destination media output",
    };
  }

  if (destination && value.includes(destination)) {
    result.scope = "destination";
    result.category = "destination";
  }

  return result;
}

function calculateReconnectDelay(attempt) {
  const safeAttempt = Math.max(1, Number(attempt || 1));
  const exponential = Math.min(
    RECONNECT_DELAY_MS * 2 ** Math.max(0, safeAttempt - 1),
    MAX_RECONNECT_DELAY_MS,
  );

  const jitterRange = exponential * STREAM_TARGET_RECONNECT_JITTER_PERCENT;
  const jitter = jitterRange > 0 ? (Math.random() * 2 - 1) * jitterRange : 0;

  return Math.max(
    RECONNECT_DELAY_MS,
    Math.round(Math.min(MAX_RECONNECT_DELAY_MS, exponential + jitter)),
  );
}

function makeInternalPlatformKey(targetType) {
  const clean = String(targetType || "target")
    .replace(/[^a-z0-9_]/gi, "_")
    .slice(0, 12);
  return `${clean.slice(0, 10)}_${crypto.randomBytes(3).toString("hex")}`.slice(
    0,
    18,
  );
}

function normalizeTargetType(value) {
  const key = String(value || "")
    .trim()
    .toLowerCase();
  return TARGET_TYPES[key] ? key : null;
}

function normalizeProtocol(value, targetType) {
  const requested = String(value || "")
    .trim()
    .toLowerCase();
  if (["rtmp", "rtmps", "srt"].includes(requested)) return requested;
  return TARGET_TYPES[targetType]?.protocol || "rtmp";
}

function combineDestinationUrl(destinationUrl, streamKey, protocol) {
  let base = String(destinationUrl || "").trim();
  const key = String(streamKey || "").trim();
  if (!base) return "";
  if (base.includes("{stream_key}"))
    return base.replaceAll("{stream_key}", encodeURIComponent(key));
  if (!key || protocol === "srt") return base;
  return `${base.replace(/\/+$/, "")}/${key.replace(/^\/+/, "")}`;
}

function validateManualTarget(target) {
  const type = normalizeTargetType(target.target_type || target.platform);
  if (!type) return "Unsupported target type";

  const protocol = normalizeProtocol(target.protocol, type);
  const config = TARGET_TYPES[type];
  const destinationUrl = String(target.destination_url || "").trim();
  const streamKey = String(target.stream_key || "").trim();
  const targetName = String(target.name || "").trim();

  if (target.automation_mode === "oauth") return null;
  if (!destinationUrl && !config?.baseUrl)
    return "Destination URL is required for this target";

  if (protocol === "srt") {
    const full = destinationUrl || config?.baseUrl || "";
    if (!full.toLowerCase().startsWith("srt://"))
      return "SRT targets require an srt:// destination URL";
    return null;
  }

  if (protocol === "rtmp" || protocol === "rtmps") {
    const full = destinationUrl || config?.baseUrl || "";
    const requiredPrefix = protocol === "rtmps" ? "rtmps://" : "rtmp://";

    if (!full.toLowerCase().startsWith(requiredPrefix)) {
      return `${protocol.toUpperCase()} targets require a ${requiredPrefix} destination URL`;
    }

    // Catch the exact class of field-crossing bug that previously stored
    // "Phase 2 RTMPS Test" as the stream key.
    if (
      streamKey &&
      targetName &&
      streamKey.trim().toLowerCase() === targetName.trim().toLowerCase()
    ) {
      return "Stream key cannot be the same value as the target name. Check the Destination URL and Stream Key fields.";
    }

    if (
      !streamKey &&
      !destinationUrl.includes("{stream_key}") &&
      ["facebook", "youtube", "instagram"].includes(type)
    ) {
      return "Stream key is required for this target";
    }
  }

  return null;
}

function createStreamTargetManager({
  pool,
  getInternalHlsSourceUrl,
  inputResilienceFlags = [],
}) {
  const processStates = new Map();

  async function isSrsStreamLive(streamKey) {
    try {
      const res = await fetch(
        `${process.env.SRS_API_URL || "http://127.0.0.1:1985"}/api/v1/streams/`,
        {
          signal: AbortSignal.timeout(5000),
        },
      );
      const data = await res.json();
      return (data.streams || []).some(
        (s) =>
          s.name === streamKey &&
          s.publish &&
          s.publish.active &&
          String(s.app || "live") === "live",
      );
    } catch (error) {
      console.error("[STREAM-TARGET] SRS stream check failed:", error.message);
      return false;
    }
  }

  function getInternalRtmpSourceUrl(streamKey) {
    return `${STREAM_TARGET_INTERNAL_RTMP_BASE}/${encodeURIComponent(String(streamKey || "").trim())}`;
  }

  function getPreferredSource(streamKey, mode = STREAM_TARGET_SOURCE_MODE) {
    if (String(mode).toLowerCase() === "hls") {
      return {
        mode: "hls",
        url: getInternalHlsSourceUrl(streamKey),
      };
    }
    return {
      mode: "rtmp",
      url: getInternalRtmpSourceUrl(streamKey),
    };
  }

  async function waitForPreferredSource(
    streamKey,
    timeoutMs = STREAM_TARGET_HLS_READY_TIMEOUT_MS,
  ) {
    const live = await isSrsStreamLive(streamKey);
    if (live && STREAM_TARGET_SOURCE_MODE === "rtmp") {
      return {
        ok: true,
        mode: "rtmp",
        sourceUrl: getInternalRtmpSourceUrl(streamKey),
      };
    }

    const hls = await waitForPlayableHls(streamKey, timeoutMs);
    return {
      ...hls,
      mode: "hls",
    };
  }

  async function waitForPlayableHls(
    streamKey,
    timeoutMs = STREAM_TARGET_HLS_READY_TIMEOUT_MS,
  ) {
    const sourceUrl = getInternalHlsSourceUrl(streamKey);
    const deadline = Date.now() + Math.max(1000, Number(timeoutMs || 0));
    let confirmations = 0;
    let lastReason = "HLS playlist is not ready";

    while (Date.now() < deadline) {
      try {
        const response = await fetch(sourceUrl, {
          cache: "no-store",
          headers: { "Cache-Control": "no-cache" },
          signal: AbortSignal.timeout(4000),
        });

        if (!response.ok) {
          confirmations = 0;
          lastReason = `HLS returned HTTP ${response.status}`;
        } else {
          const body = await response.text();
          if (looksLikePlayableHlsPlaylist(body)) {
            confirmations += 1;
            if (confirmations >= STREAM_TARGET_HLS_READY_CONFIRMATIONS) {
              return { ok: true, sourceUrl };
            }
          } else {
            confirmations = 0;
            lastReason = "HLS playlist does not contain playable media yet";
          }
        }
      } catch (error) {
        confirmations = 0;
        lastReason = error?.message || "HLS readiness check failed";
      }

      await sleep(STREAM_TARGET_HLS_READY_POLL_MS);
    }

    return { ok: false, sourceUrl, message: lastReason };
  }

  function classifyTransientSourceFailure(text, sourceUrl) {
    const value = String(text || "");
    const source = String(sourceUrl || "");

    // HLS parser/read failures are inherently source-side.
    if (
      /HTTP error 404|404 Not Found|Failed to reload playlist|keepalive request failed|when parsing playlist|Error when loading first segment|Failed to open segment/i.test(
        value,
      )
    ) {
      return true;
    }

    // Generic transport errors can belong to either FFmpeg input or output.
    // Only call them source failures when the stderr chunk identifies our
    // configured source URL. This prevents RTMP/RTMPS destination failures
    // from being shown in the UI as "Internal RTMP source unavailable".
    if (
      source &&
      value.includes(source) &&
      /Connection refused|Connection reset by peer|Server error|Input\/output error|End of file|timed out/i.test(
        value,
      )
    ) {
      return true;
    }

    return false;
  }

  function classifyDestinationFailure(text, destinationUrl) {
    const value = String(text || "");
    const destination = String(destinationUrl || "");
    return Boolean(
      destination &&
      value.includes(destination) &&
      /Connection refused|Connection reset by peer|Server error|Input\/output error|Broken pipe|timed out|TLS|handshake|403|401|denied|rejected/i.test(
        value,
      ),
    );
  }

  async function getDestinationAndChannel(destinationId) {
    const result = await pool.query(
      `SELECT sd.*, c.stream_key AS channel_stream_key, c.name AS channel_name,
              c.organization_id
       FROM social_destinations sd
       JOIN channels c ON c.id = sd.channel_id
       WHERE sd.id = $1
       LIMIT 1`,
      [destinationId],
    );
    return result.rows[0] || null;
  }

  async function ensureYoutubeAccessToken(rawAccount) {
    const account = decryptOAuthAccount(rawAccount);
    if (!account) throw new Error("Connected YouTube account not found");

    if (account.connection_status === "reconnect_required") {
      throw new Error("YouTube connection needs to be reconnected");
    }

    let accessToken = account.access_token;
    if (
      !account.token_expires_at ||
      new Date(account.token_expires_at) < new Date(Date.now() + 5 * 60 * 1000)
    ) {
      if (!account.refresh_token)
        throw new Error("YouTube connection needs to be reconnected");

      try {
        const refreshed = await youtubeApi.refreshAccessToken(
          account.refresh_token,
        );
        accessToken = refreshed.access_token;
        await pool.query(
          `UPDATE social_oauth_accounts
           SET access_token = $1,
               token_expires_at = $2,
               token_encryption_version = 1,
               connection_status = 'connected',
               token_last_validated_at = NOW(),
               token_last_error = NULL,
               reconnect_required_at = NULL,
               updated_at = NOW()
           WHERE id = $3`,
          [
            encryptOAuthToken(accessToken),
            refreshed.expiry_date ? new Date(refreshed.expiry_date) : null,
            account.id,
          ],
        );
      } catch (error) {
        await pool
          .query(
            `UPDATE social_oauth_accounts
           SET connection_status = 'reconnect_required',
               token_last_validated_at = NOW(),
               token_last_error = $1,
               reconnect_required_at = COALESCE(reconnect_required_at, NOW()),
               updated_at = NOW()
           WHERE id = $2`,
            [
              String(error.message || "YouTube token refresh failed").slice(
                0,
                1000,
              ),
              account.id,
            ],
          )
          .catch(() => {});
        throw new Error("YouTube connection needs to be reconnected");
      }
    }
    return accessToken;
  }

  async function createOAuthDestination(target, channel, organizationId) {
    const accountResult = await pool.query(
      `SELECT * FROM social_oauth_accounts WHERE id = $1 AND organization_id = $2`,
      [target.oauth_account_id, organizationId],
    );
    const account = decryptOAuthAccount(accountResult.rows[0]);
    if (!account) throw new Error("Connected account not found");

    const targetPlatform = normalizeTargetType(
      target.target_type || target.platform,
    );
    if (targetPlatform !== String(account.platform || "").toLowerCase()) {
      throw new Error(
        `OAuth account platform mismatch: ${account.platform} account cannot be used for ${targetPlatform || "unknown"} target`,
      );
    }
    if (account.connection_status === "reconnect_required") {
      throw new Error(`${account.platform} connection needs to be reconnected`);
    }

    if (target.target_type === "facebook" || target.platform === "facebook") {
      const created = await facebookGraph.createLiveVideo({
        pageId: account.external_account_id,
        pageAccessToken: account.access_token,
        title: channel.name,
      });
      await pool
        .query(
          `UPDATE social_oauth_accounts
         SET connection_status='connected', token_last_validated_at=NOW(),
             token_last_error=NULL, reconnect_required_at=NULL, updated_at=NOW()
         WHERE id=$1`,
          [account.id],
        )
        .catch(() => {});
      return {
        destinationUrl: created.rtmpUrl,
        platformBroadcastId: created.liveVideoId,
        platformStreamId: null,
      };
    }

    if (target.target_type === "youtube" || target.platform === "youtube") {
      const accessToken = await ensureYoutubeAccessToken(account);
      const oauth2Client = youtubeApi.clientFromTokens({
        accessToken,
        refreshToken: account.refresh_token,
      });
      const created = await youtubeApi.createBroadcastAndStream(oauth2Client, {
        title: channel.name,
      });
      return {
        destinationUrl: created.rtmpUrl,
        platformBroadcastId: created.broadcastId,
        platformStreamId: created.streamId,
      };
    }

    throw new Error(
      "OAuth automation is only available for Facebook and YouTube",
    );
  }

  function buildManualDestinationUrl(target) {
    const type = normalizeTargetType(target.target_type || target.platform);
    const config = TARGET_TYPES[type] || {};
    const protocol = normalizeProtocol(target.protocol, type);
    const base = String(target.destination_url || config.baseUrl || "").trim();
    return combineDestinationUrl(base, target.stream_key, protocol);
  }

  async function preflightDestination(destinationUrl, protocol) {
    const normalizedProtocol = String(protocol || "").toLowerCase();
    const endpoint = parseDestinationEndpoint(
      destinationUrl,
      normalizedProtocol,
    );

    if (!endpoint.ok) {
      return {
        ok: false,
        protocol: normalizedProtocol,
        stage: "validation",
        ...endpoint,
      };
    }

    const startedAt = Date.now();

    try {
      const records = await Promise.race([
        dns.lookup(endpoint.hostname, { all: true }),
        new Promise((_, reject) =>
          setTimeout(
            () => reject(new Error("DNS lookup timed out")),
            STREAM_TARGET_PREFLIGHT_TIMEOUT_MS,
          ),
        ),
      ]);

      if (!records?.length) {
        return {
          ok: false,
          protocol: normalizedProtocol,
          stage: "dns",
          code: "dns_failure",
          category: "dns",
          retryable: true,
          message: "Destination hostname did not resolve",
        };
      }
    } catch (error) {
      return {
        ok: false,
        protocol: normalizedProtocol,
        stage: "dns",
        code: "dns_failure",
        category: "dns",
        retryable: true,
        message: String(
          error?.message || "Destination DNS lookup failed",
        ).slice(0, 500),
      };
    }

    // SRT is UDP-based, so a TCP connect test would be misleading. DNS +
    // endpoint validation is the safe preflight; FFmpeg performs the actual
    // SRT handshake when the target starts.
    if (normalizedProtocol === "srt") {
      return {
        ok: true,
        protocol: normalizedProtocol,
        stage: "ready",
        code: "dns_resolved",
        category: "network",
        retryable: true,
        latency_ms: Math.max(0, Date.now() - startedAt),
        message: "SRT endpoint syntax and DNS resolution are valid",
      };
    }

    const connection = await tcpPreflight(endpoint.hostname, endpoint.port, {
      tlsMode: normalizedProtocol === "rtmps",
      timeoutMs: STREAM_TARGET_PREFLIGHT_TIMEOUT_MS,
    });

    return {
      ...connection,
      protocol: normalizedProtocol,
      stage: connection.ok ? "ready" : "connect",
    };
  }

  async function preflightTarget(target) {
    if (!target) {
      return {
        ok: false,
        stage: "validation",
        code: "missing_target",
        retryable: false,
        message: "Stream target was not found",
      };
    }

    if (target.automation_mode === "oauth") {
      return {
        ok: true,
        skipped: true,
        stage: "oauth",
        code: "oauth_runtime_destination",
        retryable: true,
        message:
          "OAuth destination is created at broadcast start; network preflight is deferred until then.",
      };
    }

    const type = normalizeTargetType(target.target_type || target.platform);
    const protocol = normalizeProtocol(target.protocol, type);
    const validationError = validateManualTarget(target);
    if (validationError) {
      return {
        ok: false,
        stage: "validation",
        code: "invalid_target",
        category: "configuration",
        retryable: false,
        protocol,
        message: validationError,
      };
    }

    const destinationUrl = buildManualDestinationUrl(target);
    return preflightDestination(destinationUrl, protocol);
  }

  function buildFfmpegArgs(sourceUrl, destinationUrl, protocol) {
    const audioArgs = getTargetAudioArgs(protocol);
    const output =
      protocol === "srt"
        ? [
            "-c:v",
            "copy",
            ...audioArgs,
            "-err_detect",
            "ignore_err",
            "-pes_payload_size",
            "0",
            "-flush_packets",
            "1",
            "-f",
            "mpegts",
            destinationUrl,
          ]
        : [
            "-c:v",
            "copy",
            ...audioArgs,
            "-err_detect",
            "ignore_err",
            "-f",
            "flv",
            destinationUrl,
          ];

    return [
      ...inputResilienceFlags,
      "-i",
      sourceUrl,
      "-map",
      "0:v:0",
      "-map",
      "0:a:0?",
      "-nostats",
      "-progress",
      "pipe:1",
      ...output,
    ];
  }

  function hasVerifiedDelivery(state) {
    const bytes = safeNumber(state.outputBytes, 0);
    const outTimeMs = safeNumber(state.outputTimeMs, 0);
    const frames = safeNumber(state.outputFrames, 0);
    const bitrate = safeNumber(state.currentBitrateKbps, 0);

    return Boolean(
      bytes >= STREAM_TARGET_MIN_VERIFIED_BYTES &&
      outTimeMs >= STREAM_TARGET_MIN_VERIFIED_OUT_TIME_MS &&
      (frames > 0 || bitrate > 0),
    );
  }

  function markDeliveryVerified(state) {
    if (state.deliveryVerified) return;

    state.deliveryVerified = true;
    state.deliveryVerifiedAt = Date.now();
    state.lastProgressAt = Date.now();
    state.retryAttempt = 0;
    state.nextRetryAt = null;
    state.sourceHlsFault = false;
    state.lastError = null;
    state.failureCode = null;
    state.failureCategory = null;
    state.failureScope = null;
    state.failureRetryable = true;
    state.lastFailureAt = null;
    state.status = "streaming";

    if (state.deliveryVerifyTimer) {
      clearTimeout(state.deliveryVerifyTimer);
      state.deliveryVerifyTimer = null;
    }
  }

  function parseProgressLine(state, line) {
    const idx = line.indexOf("=");
    if (idx <= 0) return;

    const key = line.slice(0, idx).trim();
    const value = line.slice(idx + 1).trim();

    if (key === "bitrate") {
      const match = value.match(/([0-9.]+)kbits\/s/i);
      state.currentBitrateKbps = match
        ? Math.max(0, Math.round(Number(match[1])))
        : 0;
    } else if (key === "drop_frames") {
      state.droppedFrames = safeNumber(value, state.droppedFrames || 0);
    } else if (key === "frame") {
      state.outputFrames = safeNumber(value, state.outputFrames || 0);
    } else if (key === "total_size") {
      state.outputBytes = safeNumber(value, state.outputBytes || 0);
    } else if (key === "out_time_ms") {
      // FFmpeg's progress protocol calls this out_time_ms although builds
      // commonly report microseconds. Treat values >= 100000 as microseconds.
      const raw = safeNumber(value, state.outputTimeMs || 0);
      state.outputTimeMs = raw >= 100000 ? Math.floor(raw / 1000) : raw;
    } else if (key === "out_time_us") {
      state.outputTimeMs = Math.floor(
        safeNumber(value, state.outputTimeMs || 0) / 1000,
      );
    } else if (
      key === "progress" &&
      (value === "continue" || value === "end")
    ) {
      state.lastProgressAt = Date.now();

      // A progress heartbeat by itself is not proof that the destination is
      // receiving media. Require output bytes + media time + frame/bitrate.
      if (hasVerifiedDelivery(state)) {
        markDeliveryVerified(state);
      }
    }
  }

  async function persistMetrics(destinationId, state) {
    try {
      await pool.query(
        `UPDATE social_destinations
         SET current_bitrate_kbps = $1,
             dropped_frames = $2,
             status = CASE WHEN is_running THEN $3 ELSE status END,
             last_error = CASE WHEN $3 = 'streaming' THEN NULL ELSE last_error END,
             updated_at = now()
         WHERE id = $4`,
        [
          state.currentBitrateKbps || 0,
          state.droppedFrames || 0,
          state.status || "connecting",
          destinationId,
        ],
      );
    } catch (error) {
      console.warn(
        `[STREAM-TARGET #${destinationId}] metric update failed:`,
        error.message,
      );
    }
  }

  function clearStateTimers(state) {
    if (!state) return;
    if (state.metricsTimer) clearInterval(state.metricsTimer);
    if (state.reconnectTimer) clearTimeout(state.reconnectTimer);
    if (state.deliveryVerifyTimer) clearTimeout(state.deliveryVerifyTimer);
    state.metricsTimer = null;
    state.reconnectTimer = null;
    state.deliveryVerifyTimer = null;
  }

  async function spawnPush(
    target,
    channel,
    destinationUrl,
    { reconnect = false } = {},
  ) {
    const destinationId = Number(target.id);
    const targetType = normalizeTargetType(
      target.target_type || target.platform,
    );
    const protocol = normalizeProtocol(target.protocol, targetType);
    const preferredSource = getPreferredSource(
      channel.stream_key,
      target.source_mode || STREAM_TARGET_SOURCE_MODE,
    );
    const sourceUrl = preferredSource.url;

    const existing = processStates.get(destinationId);
    if (existing?.proc && !existing.proc.killed) {
      return { ok: false, message: "Target is already streaming" };
    }

    const state = existing || {
      destinationId,
      currentBitrateKbps: 0,
      droppedFrames: safeNumber(target.dropped_frames),
      reconnectCount: safeNumber(target.reconnect_count),
      retryAttempt: 0,
      nextRetryAt: null,
      maxReconnectAttempts: STREAM_TARGET_MAX_RECONNECT_ATTEMPTS,
      sourceHlsFault: false,
      intentionalStop: false,
      failureCode: null,
      failureCategory: null,
      failureScope: null,
      failureRetryable: true,
      lastFailureAt: null,
      preflight: null,
      outputBytes: 0,
      outputTimeMs: 0,
      outputFrames: 0,
      deliveryVerified: false,
      deliveryVerifiedAt: null,
      deliveryVerifyTimer: null,
    };
    clearStateTimers(state);
    state.intentionalStop = false;
    state.destinationUrl = destinationUrl;
    state.targetType = targetType;
    state.protocol = protocol;
    state.channelStreamKey = channel.stream_key;
    state.sourceMode = preferredSource.mode;
    state.sourceUrl = sourceUrl;
    state.currentBitrateKbps = 0;
    state.outputBytes = 0;
    state.outputTimeMs = 0;
    state.outputFrames = 0;
    state.deliveryVerified = false;
    state.deliveryVerifiedAt = null;
    if (!state.startedAt || !reconnect) {
      state.startedAt =
        reconnect && target.started_at
          ? new Date(target.started_at).getTime()
          : Date.now();
    }

    const proc = spawn(
      "ffmpeg",
      buildFfmpegArgs(sourceUrl, destinationUrl, protocol),
    );
    state.proc = proc;
    state.pid = proc.pid;
    state.status = "connecting";
    processStates.set(destinationId, state);

    let progressBuffer = "";
    proc.stdout.on("data", (chunk) => {
      progressBuffer += chunk.toString();
      const lines = progressBuffer.split(/\r?\n/);
      progressBuffer = lines.pop() || "";
      for (const line of lines) parseProgressLine(state, line);
    });

    proc.stderr.on("data", (chunk) => {
      const text = chunk.toString();

      if (
        /error|failed|refused|timed out|broken pipe|denied|rejected|handshake|401|403/i.test(
          text,
        )
      ) {
        const failure = classifyTargetFailure(text, {
          sourceUrl,
          destinationUrl,
        });

        state.sourceHlsFault = failure.scope === "source";
        state.failureCode = failure.code;
        state.failureCategory = failure.category;
        state.failureScope = failure.scope;
        state.failureRetryable = failure.retryable !== false;
        state.lastFailureAt = Date.now();

        state.lastError =
          failure.scope === "source"
            ? `Internal ${String(state.sourceMode || "media").toUpperCase()} source temporarily unavailable; reconnecting automatically.`
            : failure.message;
      }

      console.log(
        `[STREAM-TARGET ${targetType} #${destinationId}]`,
        text.slice(0, 300),
      );
    });

    state.metricsTimer = setInterval(
      () => persistMetrics(destinationId, state),
      5000,
    );
    state.metricsTimer.unref?.();

    await pool.query(
      `UPDATE social_destinations
       SET is_running = true,
           ffmpeg_pid = $1,
           status = 'connecting',
           started_at = CASE WHEN $2::boolean THEN COALESCE(started_at, now()) ELSE now() END,
           last_connected_at = now(),
           last_error = NULL,
           active_destination_url = $3,
           current_bitrate_kbps = 0,
           updated_at = now()
       WHERE id = $4`,
      [proc.pid, reconnect, destinationUrl, destinationId],
    );
    // Remain in CONNECTING until actual output delivery is verified.
    // A live FFmpeg PID or progress heartbeat alone is insufficient.
    state.deliveryVerifyTimer = setTimeout(() => {
      if (
        state.proc !== proc ||
        state.intentionalStop ||
        state.deliveryVerified
      ) {
        return;
      }

      state.status = "reconnecting";
      state.lastError =
        "Destination delivery could not be verified: FFmpeg produced no confirmed output media.";
      state.failureCode = "delivery_not_verified";
      state.failureCategory = "delivery";
      state.failureScope = "destination";
      state.failureRetryable = true;
      state.lastFailureAt = Date.now();

      // Let the existing exit/reconnect path perform bounded backoff and
      // persistence. Killing only this worker prevents false STREAMING state.
      try {
        proc.kill("SIGTERM");
      } catch {
        // Process may already be exiting.
      }
    }, STREAM_TARGET_DELIVERY_VERIFY_TIMEOUT_MS);
    state.deliveryVerifyTimer.unref?.();

    proc.on("error", (error) => {
      const failure = classifyTargetFailure(
        error?.message || "FFmpeg spawn failed",
        {
          sourceUrl,
          destinationUrl,
        },
      );
      state.lastError = failure.message;
      state.failureCode = failure.code;
      state.failureCategory = failure.category;
      state.failureScope = failure.scope;
      state.failureRetryable = failure.retryable !== false;
      state.lastFailureAt = Date.now();
    });

    proc.on("exit", async (code, signal) => {
      if (state.proc !== proc) return;
      state.proc = null;
      if (state.metricsTimer) clearInterval(state.metricsTimer);
      if (state.deliveryVerifyTimer) clearTimeout(state.deliveryVerifyTimer);
      state.metricsTimer = null;
      state.deliveryVerifyTimer = null;

      const wasIntentional = state.intentionalStop;
      const lastError =
        state.lastError ||
        (code === 0
          ? null
          : `FFmpeg exited with code ${code}${signal ? ` (${signal})` : ""}`);

      if (wasIntentional) {
        try {
          await pool.query(
            `UPDATE social_destinations
             SET is_running = false,
                 ffmpeg_pid = NULL,
                 status = 'stopped',
                 current_bitrate_kbps = 0,
                 last_disconnected_at = now(),
                 last_error = NULL,
                 updated_at = now()
             WHERE id = $1`,
            [destinationId],
          );
        } catch (error) {
          console.error(
            `[STREAM-TARGET #${destinationId}] stop state update failed:`,
            error.message,
          );
        }
        processStates.delete(destinationId);
        return;
      }

      try {
        const fresh = await getDestinationAndChannel(destinationId);
        const sourceStillLive = fresh
          ? await isSrsStreamLive(fresh.channel_stream_key)
          : false;

        const retryable = state.failureRetryable !== false;
        const attemptsExhausted =
          safeNumber(state.retryAttempt) >=
          STREAM_TARGET_MAX_RECONNECT_ATTEMPTS;

        const canReconnect = Boolean(
          fresh &&
          fresh.enabled &&
          fresh.auto_reconnect &&
          sourceStillLive &&
          retryable &&
          !attemptsExhausted,
        );

        if (!canReconnect) {
          let terminalStatus = "disconnected";
          let terminalError = lastError;

          if (!retryable) {
            terminalStatus = "failed";
            terminalError =
              state.lastError ||
              "Target requires operator attention before it can reconnect.";
          } else if (attemptsExhausted) {
            terminalStatus = "failed";
            terminalError =
              `Automatic reconnect stopped after ${STREAM_TARGET_MAX_RECONNECT_ATTEMPTS} attempts. ${lastError || ""}`.trim();
          }

          state.status = terminalStatus;
          state.nextRetryAt = null;

          await pool.query(
            `UPDATE social_destinations
             SET is_running = false,
                 ffmpeg_pid = NULL,
                 status = $1,
                 current_bitrate_kbps = 0,
                 last_disconnected_at = now(),
                 last_error = $2,
                 updated_at = now()
             WHERE id = $3`,
            [terminalStatus, terminalError, destinationId],
          );

          processStates.set(destinationId, state);
          return;
        }

        state.reconnectCount = safeNumber(fresh.reconnect_count) + 1;
        state.retryAttempt = safeNumber(state.retryAttempt) + 1;
        state.status = "reconnecting";

        const delay = calculateReconnectDelay(state.retryAttempt);
        state.nextRetryAt = Date.now() + delay;

        await pool.query(
          `UPDATE social_destinations
           SET is_running = false,
               ffmpeg_pid = NULL,
               status = 'reconnecting',
               reconnect_count = $1,
               current_bitrate_kbps = 0,
               last_disconnected_at = now(),
               last_error = $2,
               updated_at = now()
           WHERE id = $3`,
          [
            state.reconnectCount,
            state.sourceHlsFault
              ? `Internal ${String(state.sourceMode || "media").toUpperCase()} source temporarily unavailable; reconnecting automatically.`
              : lastError,
            destinationId,
          ],
        );

        const scheduleNextAttempt = () => {
          if (state.reconnectTimer) clearTimeout(state.reconnectTimer);

          state.reconnectTimer = setTimeout(async () => {
            state.reconnectTimer = null;
            state.nextRetryAt = null;

            try {
              const latest = await getDestinationAndChannel(destinationId);
              if (!latest || !latest.enabled || !latest.auto_reconnect) {
                processStates.delete(destinationId);
                return;
              }

              const stillLive = await isSrsStreamLive(
                latest.channel_stream_key,
              );
              if (!stillLive) {
                state.status = "disconnected";
                await pool.query(
                  `UPDATE social_destinations
                   SET status='disconnected', is_running=false, ffmpeg_pid=NULL,
                       current_bitrate_kbps=0, updated_at=now()
                   WHERE id=$1`,
                  [destinationId],
                );
                return;
              }

              const readiness = await waitForPreferredSource(
                latest.channel_stream_key,
                STREAM_TARGET_HLS_READY_TIMEOUT_MS,
              );

              if (!readiness.ok) {
                state.failureCode = "source_not_ready";
                state.failureCategory = "source";
                state.failureScope = "source";
                state.failureRetryable = true;
                state.lastFailureAt = Date.now();
                state.lastError = `Source media is not ready yet: ${readiness.message}`;

                // Trigger a synthetic exit-style retry decision by increasing
                // the attempt counter and scheduling the next bounded backoff.
                if (
                  safeNumber(state.retryAttempt) >=
                  STREAM_TARGET_MAX_RECONNECT_ATTEMPTS
                ) {
                  state.status = "failed";
                  state.nextRetryAt = null;
                  await pool.query(
                    `UPDATE social_destinations
                     SET status='failed', is_running=false, ffmpeg_pid=NULL,
                         current_bitrate_kbps=0,
                         last_error=$1, updated_at=now()
                     WHERE id=$2`,
                    [
                      `Automatic reconnect stopped after ${STREAM_TARGET_MAX_RECONNECT_ATTEMPTS} attempts. ${state.lastError}`,
                      destinationId,
                    ],
                  );
                  return;
                }

                state.retryAttempt = safeNumber(state.retryAttempt) + 1;
                state.reconnectCount = safeNumber(state.reconnectCount) + 1;
                state.status = "reconnecting";
                const retryDelay = calculateReconnectDelay(state.retryAttempt);
                state.nextRetryAt = Date.now() + retryDelay;

                await pool.query(
                  `UPDATE social_destinations
                   SET status='reconnecting', reconnect_count=$1,
                       last_error=$2, updated_at=now()
                   WHERE id=$3`,
                  [state.reconnectCount, state.lastError, destinationId],
                );

                state.reconnectTimer = setTimeout(
                  scheduleNextAttempt,
                  retryDelay,
                );
                state.reconnectTimer.unref?.();
                return;
              }

              const url =
                latest.active_destination_url ||
                state.destinationUrl ||
                buildManualDestinationUrl(latest);

              if (
                STREAM_TARGET_PREFLIGHT_ENABLED &&
                latest.automation_mode !== "oauth"
              ) {
                const preflight = await preflightDestination(
                  url,
                  normalizeProtocol(
                    latest.protocol,
                    normalizeTargetType(latest.target_type || latest.platform),
                  ),
                );
                state.preflight = {
                  ...preflight,
                  checked_at: new Date().toISOString(),
                };

                if (!preflight.ok) {
                  state.failureCode = preflight.code || "preflight_failed";
                  state.failureCategory = preflight.category || "destination";
                  state.failureScope = "destination";
                  state.failureRetryable = preflight.retryable !== false;
                  state.lastFailureAt = Date.now();
                  state.lastError =
                    preflight.message || "Destination preflight failed";

                  if (
                    preflight.retryable === false ||
                    safeNumber(state.retryAttempt) >=
                      STREAM_TARGET_MAX_RECONNECT_ATTEMPTS
                  ) {
                    state.status = "failed";
                    state.nextRetryAt = null;
                    await pool.query(
                      `UPDATE social_destinations
                       SET status='failed', is_running=false, ffmpeg_pid=NULL,
                           current_bitrate_kbps=0, last_error=$1, updated_at=now()
                       WHERE id=$2`,
                      [state.lastError, destinationId],
                    );
                    return;
                  }

                  state.retryAttempt = safeNumber(state.retryAttempt) + 1;
                  state.reconnectCount = safeNumber(state.reconnectCount) + 1;
                  const retryDelay = calculateReconnectDelay(
                    state.retryAttempt,
                  );
                  state.nextRetryAt = Date.now() + retryDelay;

                  await pool.query(
                    `UPDATE social_destinations
                     SET status='reconnecting', reconnect_count=$1,
                         last_error=$2, updated_at=now()
                     WHERE id=$3`,
                    [state.reconnectCount, state.lastError, destinationId],
                  );

                  state.reconnectTimer = setTimeout(
                    scheduleNextAttempt,
                    retryDelay,
                  );
                  state.reconnectTimer.unref?.();
                  return;
                }
              }

              await spawnPush(
                latest,
                {
                  id: latest.channel_id,
                  name: latest.channel_name,
                  stream_key: latest.channel_stream_key,
                },
                url,
                { reconnect: true },
              );
            } catch (error) {
              state.failureCode = "reconnect_worker_failed";
              state.failureCategory = "worker";
              state.failureScope = "worker";
              state.failureRetryable = true;
              state.lastFailureAt = Date.now();
              state.lastError = error.message || "Reconnect worker failed";

              if (
                safeNumber(state.retryAttempt) >=
                STREAM_TARGET_MAX_RECONNECT_ATTEMPTS
              ) {
                state.status = "failed";
                state.nextRetryAt = null;
                await pool
                  .query(
                    `UPDATE social_destinations
                   SET status='failed', is_running=false, ffmpeg_pid=NULL,
                       current_bitrate_kbps=0, last_error=$1, updated_at=now()
                   WHERE id=$2`,
                    [
                      `Automatic reconnect stopped after ${STREAM_TARGET_MAX_RECONNECT_ATTEMPTS} attempts. ${state.lastError}`,
                      destinationId,
                    ],
                  )
                  .catch(() => {});
                return;
              }

              state.retryAttempt = safeNumber(state.retryAttempt) + 1;
              state.reconnectCount = safeNumber(state.reconnectCount) + 1;
              const retryDelay = calculateReconnectDelay(state.retryAttempt);
              state.status = "reconnecting";
              state.nextRetryAt = Date.now() + retryDelay;

              await pool
                .query(
                  `UPDATE social_destinations
                 SET status='reconnecting', reconnect_count=$1,
                     last_error=$2, updated_at=now()
                 WHERE id=$3`,
                  [state.reconnectCount, state.lastError, destinationId],
                )
                .catch(() => {});

              state.reconnectTimer = setTimeout(
                scheduleNextAttempt,
                retryDelay,
              );
              state.reconnectTimer.unref?.();
            }
          }, delay);

          state.reconnectTimer.unref?.();
        };

        scheduleNextAttempt();
      } catch (error) {
        console.error(
          `[STREAM-TARGET #${destinationId}] reconnect decision failed:`,
          error.message,
        );
        processStates.delete(destinationId);
      }
    });

    return {
      ok: true,
      message: reconnect ? "Target reconnected" : "Stream target started",
    };
  }

  async function startTarget(target, channel, organizationId, options = {}) {
    if (!target.enabled)
      return { ok: false, message: "This stream target is disabled" };
    if (processStates.get(Number(target.id))?.proc)
      return { ok: false, message: "Target is already streaming" };

    const live = await isSrsStreamLive(channel.stream_key);
    if (!live)
      return {
        ok: false,
        message: "Main stream is not live yet. Start streaming first.",
      };

    await pool.query(
      `UPDATE social_destinations
       SET status = 'connecting',
           last_error = NULL,
           current_bitrate_kbps = 0,
           reconnect_count = CASE WHEN $2::boolean THEN reconnect_count ELSE 0 END,
           updated_at = now()
       WHERE id = $1`,
      [target.id, Boolean(options.reconnect)],
    );

    if (!options.reconnect) {
      target.reconnect_count = 0;
      const existingState = processStates.get(Number(target.id));
      if (existingState) {
        existingState.reconnectCount = 0;
        existingState.retryAttempt = 0;
      }
    }

    const readiness = await waitForPreferredSource(
      channel.stream_key,
      STREAM_TARGET_HLS_READY_TIMEOUT_MS,
    );
    if (!readiness.ok) {
      await pool.query(
        `UPDATE social_destinations
         SET is_running = false,
             ffmpeg_pid = NULL,
             status = 'connecting',
             last_error = NULL,
             current_bitrate_kbps = 0,
             updated_at = now()
         WHERE id = $1`,
        [target.id],
      );

      return {
        ok: false,
        message: `Source is live but target media is not ready yet: ${readiness.message}`,
      };
    }

    const validationError = validateManualTarget(target);
    if (validationError) return { ok: false, message: validationError };

    let destinationUrl = target.active_destination_url || null;
    let platformBroadcastId = target.platform_broadcast_id || null;
    let platformStreamId = target.platform_stream_id || null;

    if (target.automation_mode === "oauth") {
      if (!target.oauth_account_id)
        return {
          ok: false,
          message: "Connect an account to this target first",
        };
      if (!destinationUrl || !options.reuseRuntimeUrl) {
        try {
          const created = await createOAuthDestination(
            target,
            channel,
            organizationId,
          );
          destinationUrl = created.destinationUrl;
          platformBroadcastId = created.platformBroadcastId;
          platformStreamId = created.platformStreamId;
        } catch (error) {
          const message =
            error.message || "Failed to create platform broadcast";
          await pool
            .query(
              `UPDATE social_destinations
             SET is_running=false, ffmpeg_pid=NULL, status='failed',
                 current_bitrate_kbps=0, last_error=$1, updated_at=NOW()
             WHERE id=$2`,
              [message, target.id],
            )
            .catch(() => {});
          return {
            ok: false,
            message,
            failure: {
              code: /reconnected|reconnect/i.test(message)
                ? "oauth_reconnect_required"
                : "oauth_broadcast_create_failed",
              category: "oauth",
              retryable: !/needs to be reconnected/i.test(message),
              stage: "platform_broadcast",
            },
          };
        }
      }
    } else {
      destinationUrl = buildManualDestinationUrl(target);
    }

    if (!destinationUrl)
      return { ok: false, message: "Target destination URL is not configured" };

    if (STREAM_TARGET_PREFLIGHT_ENABLED && target.automation_mode !== "oauth") {
      const type = normalizeTargetType(target.target_type || target.platform);
      const protocol = normalizeProtocol(target.protocol, type);
      const preflight = await preflightDestination(destinationUrl, protocol);

      const existingState = processStates.get(Number(target.id));
      if (existingState) {
        existingState.preflight = {
          ...preflight,
          checked_at: new Date().toISOString(),
        };
      }

      if (!preflight.ok) {
        await pool.query(
          `UPDATE social_destinations
           SET is_running=false, ffmpeg_pid=NULL, status='failed',
               current_bitrate_kbps=0, last_error=$1, updated_at=now()
           WHERE id=$2`,
          [preflight.message || "Destination preflight failed", target.id],
        );

        return {
          ok: false,
          message: preflight.message || "Destination preflight failed",
          failure: {
            code: preflight.code || "preflight_failed",
            category: preflight.category || "destination",
            retryable: preflight.retryable !== false,
            stage: preflight.stage || "connect",
          },
          preflight,
        };
      }
    }

    await pool.query(
      `UPDATE social_destinations
       SET platform_broadcast_id = $1,
           platform_stream_id = $2,
           active_destination_url = $3,
           last_error = NULL,
           updated_at = now()
       WHERE id = $4`,
      [platformBroadcastId, platformStreamId, destinationUrl, target.id],
    );

    return spawnPush(target, channel, destinationUrl, {
      reconnect: Boolean(options.reconnect),
    });
  }

  async function endPlatformBroadcast(target) {
    if (
      target.automation_mode !== "oauth" ||
      !target.platform_broadcast_id ||
      !target.oauth_account_id
    )
      return;
    try {
      const accountResult = await pool.query(
        `SELECT * FROM social_oauth_accounts WHERE id = $1`,
        [target.oauth_account_id],
      );
      const account = decryptOAuthAccount(accountResult.rows[0]);
      if (!account) return;
      if ((target.target_type || target.platform) === "facebook") {
        await facebookGraph.endLiveVideo({
          liveVideoId: target.platform_broadcast_id,
          pageAccessToken: account.access_token,
        });
      } else if ((target.target_type || target.platform) === "youtube") {
        const accessToken = await ensureYoutubeAccessToken(account);
        const oauth2Client = youtubeApi.clientFromTokens({
          accessToken,
          refreshToken: account.refresh_token,
        });
        await youtubeApi.transitionBroadcast(
          oauth2Client,
          target.platform_broadcast_id,
          "complete",
        );
      }
    } catch (error) {
      console.error(
        `[STREAM-TARGET #${target.id}] platform end failed:`,
        error.message,
      );
    }
  }

  async function stopTarget(target, { endPlatform = true } = {}) {
    const destinationId = Number(target.id);
    const state = processStates.get(destinationId);
    if (state) {
      state.intentionalStop = true;
      clearStateTimers(state);
      if (state.proc && !state.proc.killed) state.proc.kill("SIGTERM");
      state.proc = null;
      processStates.delete(destinationId);
    } else if (target.ffmpeg_pid) {
      // Never blindly kill an arbitrary PID recovered from a prior backend
      // process. The DB state is reconciled below; process ownership belongs
      // to the current Node instance only.
    }

    if (endPlatform) await endPlatformBroadcast(target);

    await pool.query(
      `UPDATE social_destinations
       SET is_running = false,
           ffmpeg_pid = NULL,
           status = 'stopped',
           current_bitrate_kbps = 0,
           last_disconnected_at = now(),
           active_destination_url = NULL,
           platform_broadcast_id = CASE WHEN $2::boolean THEN NULL ELSE platform_broadcast_id END,
           platform_stream_id = CASE WHEN $2::boolean THEN NULL ELSE platform_stream_id END,
           updated_at = now()
       WHERE id = $1`,
      [destinationId, endPlatform],
    );
    return { ok: true, message: "Stream target stopped" };
  }

  async function startAutoTargets(channel, organizationId) {
    const result = await pool.query(
      `SELECT * FROM social_destinations
       WHERE channel_id = $1 AND enabled = true AND auto_start = true`,
      [channel.id],
    );
    for (const target of result.rows) {
      const timer = setTimeout(() => {
        startTarget(target, channel, organizationId)
          .then((result) => {
            if (result.ok) return;

            console.log(
              `[STREAM-TARGET] Auto-start attempt for #${target.id} did not start: ${result.message}`,
            );

            const shouldRetry =
              /HLS media is not ready|HLS playlist|HLS returned HTTP/i.test(
                String(result.message || ""),
              );

            if (!shouldRetry) return;

            const retryTimer = setTimeout(() => {
              startTarget(target, channel, organizationId)
                .then((retryResult) => {
                  if (!retryResult.ok) {
                    console.log(
                      `[STREAM-TARGET] Delayed auto-start retry for #${target.id} did not start: ${retryResult.message}`,
                    );
                  }
                })
                .catch((error) =>
                  console.error(
                    `[STREAM-TARGET] Delayed auto-start retry failed for #${target.id}:`,
                    error.message,
                  ),
                );
            }, 5000);
            retryTimer.unref?.();
          })
          .catch((error) =>
            console.error(
              `[STREAM-TARGET] Auto-start failed for #${target.id}:`,
              error.message,
            ),
          );
      }, 5000);
      timer.unref?.();
    }
  }

  async function stopChannelTargets(channelId) {
    const result = await pool.query(
      `SELECT * FROM social_destinations WHERE channel_id = $1 AND is_running = true`,
      [channelId],
    );
    await Promise.all(
      result.rows.map((target) =>
        stopTarget(target).catch((error) => {
          console.error(
            `[STREAM-TARGET] Auto-stop failed for #${target.id}:`,
            error.message,
          );
        }),
      ),
    );
  }

  async function reconcileDatabaseState() {
    // PIDs from a prior Node process cannot be trusted as ownership state.
    // Mark them stopped at backend startup; auto-start will recreate them on
    // the next publisher event, and auto-reconnect handles current-process exits.
    await pool.query(
      `UPDATE social_destinations
       SET is_running = false, ffmpeg_pid = NULL, current_bitrate_kbps = 0,
           status = CASE WHEN status = 'streaming' THEN 'stopped' ELSE COALESCE(status, 'stopped') END
       WHERE is_running = true OR ffmpeg_pid IS NOT NULL`,
    );
  }

  function getRuntimeState(destinationId) {
    const state = processStates.get(Number(destinationId));
    if (!state) return null;

    const startedAtMs = Number(state.startedAt || 0);
    const uptimeSeconds =
      state.proc && startedAtMs > 0
        ? Math.max(0, Math.floor((Date.now() - startedAtMs) / 1000))
        : 0;

    return {
      pid: state.proc?.pid || null,
      is_running: Boolean(state.proc && state.deliveryVerified),
      worker_running: Boolean(state.proc),
      delivery_verified: Boolean(state.deliveryVerified),
      delivery_verified_at: state.deliveryVerifiedAt
        ? new Date(state.deliveryVerifiedAt).toISOString()
        : null,
      output_bytes: safeNumber(state.outputBytes),
      output_time_ms: safeNumber(state.outputTimeMs),
      output_frames: safeNumber(state.outputFrames),
      current_bitrate_kbps: state.currentBitrateKbps || 0,
      dropped_frames: state.droppedFrames || 0,
      reconnect_count: state.reconnectCount || 0,
      runtime_status: state.proc
        ? state.deliveryVerified
          ? "streaming"
          : state.status === "reconnecting"
            ? "reconnecting"
            : "connecting"
        : state.status || "disconnected",
      status: state.proc
        ? state.deliveryVerified
          ? "streaming"
          : state.status === "reconnecting"
            ? "reconnecting"
            : "connecting"
        : state.status || "disconnected",
      uptime_seconds: uptimeSeconds,
      started_at: startedAtMs > 0 ? new Date(startedAtMs).toISOString() : null,
      source_hls_fault: Boolean(state.sourceHlsFault),
      retry_attempt: safeNumber(state.retryAttempt),
      max_reconnect_attempts: STREAM_TARGET_MAX_RECONNECT_ATTEMPTS,
      next_retry_at:
        state.nextRetryAt && state.nextRetryAt > Date.now()
          ? new Date(state.nextRetryAt).toISOString()
          : null,
      next_retry_in_seconds:
        state.nextRetryAt && state.nextRetryAt > Date.now()
          ? Math.max(0, Math.ceil((state.nextRetryAt - Date.now()) / 1000))
          : 0,
      failure_code: state.failureCode || null,
      failure_category: state.failureCategory || null,
      failure_scope: state.failureScope || null,
      failure_retryable:
        state.failureRetryable == null ? true : Boolean(state.failureRetryable),
      last_failure_at: state.lastFailureAt
        ? new Date(state.lastFailureAt).toISOString()
        : null,
      preflight: state.preflight || null,
    };
  }

  return {
    TARGET_TYPES,
    makeInternalPlatformKey,
    normalizeTargetType,
    normalizeProtocol,
    validateManualTarget,
    redactTargetUrl,
    startTarget,
    stopTarget,
    startAutoTargets,
    stopChannelTargets,
    reconcileDatabaseState,
    getRuntimeState,
    isSrsStreamLive,
    waitForPlayableHls,
    preflightDestination,
    preflightTarget,
  };
}

module.exports = { createStreamTargetManager, TARGET_TYPES };
