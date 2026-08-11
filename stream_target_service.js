// stream_target_service.js
// Phase 2 — Generic Stream Targets process/orchestration engine.

const { spawn } = require("child_process");
const crypto = require("crypto");
const facebookGraph = require("./facebook_graph_service");
const youtubeApi = require("./youtube_api_service");

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

function getTargetAudioArgs() {
  return STREAM_TARGET_AUDIO_MODE === "copy"
    ? ["-c:a", "copy"]
    : ["-c:a", "aac", "-b:a", "128k", "-af", "aresample=async=1:first_pts=0"];
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

  if (target.automation_mode === "oauth") return null;
  if (!destinationUrl && !config?.baseUrl)
    return "Destination URL is required for this target";
  if (protocol === "srt") {
    const full = destinationUrl || config?.baseUrl || "";
    if (!full.toLowerCase().startsWith("srt://"))
      return "SRT targets require an srt:// destination URL";
  }
  if (["rtmp", "rtmps"].includes(protocol)) {
    const full = destinationUrl || config?.baseUrl || "";
    if (!/^rtmps?:\/\//i.test(full))
      return "RTMP targets require an rtmp:// or rtmps:// destination URL";
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

  function classifyTransientSourceFailure(text) {
    const value = String(text || "");
    return /HTTP error 404|404 Not Found|Failed to reload playlist|keepalive request failed|when parsing playlist|Error when loading first segment|Failed to open segment/i.test(
      value,
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

  async function ensureYoutubeAccessToken(account) {
    let accessToken = account.access_token;
    if (
      !account.token_expires_at ||
      new Date(account.token_expires_at) < new Date(Date.now() + 5 * 60 * 1000)
    ) {
      if (!account.refresh_token)
        throw new Error("YouTube connection needs to be reconnected");
      const refreshed = await youtubeApi.refreshAccessToken(
        account.refresh_token,
      );
      accessToken = refreshed.access_token;
      await pool.query(
        `UPDATE social_oauth_accounts
         SET access_token = $1, token_expires_at = $2, updated_at = now()
         WHERE id = $3`,
        [
          accessToken,
          refreshed.expiry_date ? new Date(refreshed.expiry_date) : null,
          account.id,
        ],
      );
    }
    return accessToken;
  }

  async function createOAuthDestination(target, channel, organizationId) {
    const accountResult = await pool.query(
      `SELECT * FROM social_oauth_accounts WHERE id = $1 AND organization_id = $2`,
      [target.oauth_account_id, organizationId],
    );
    const account = accountResult.rows[0];
    if (!account) throw new Error("Connected account not found");

    if (target.target_type === "facebook" || target.platform === "facebook") {
      const created = await facebookGraph.createLiveVideo({
        pageId: account.external_account_id,
        pageAccessToken: account.access_token,
        title: channel.name,
      });
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

  function buildFfmpegArgs(sourceUrl, destinationUrl, protocol) {
    const audioArgs = getTargetAudioArgs();
    const output =
      protocol === "srt"
        ? [
            "-c:v",
            "copy",
            ...audioArgs,
            "-err_detect",
            "ignore_err",
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

  function parseProgressLine(state, line) {
    const idx = line.indexOf("=");
    if (idx <= 0) return;
    const key = line.slice(0, idx).trim();
    const value = line.slice(idx + 1).trim();
    if (key === "bitrate") {
      const match = value.match(/([0-9.]+)kbits\/s/i);
      if (match) state.currentBitrateKbps = Math.round(Number(match[1]));
    } else if (key === "drop_frames") {
      state.droppedFrames = safeNumber(value, state.droppedFrames || 0);
    } else if (
      key === "progress" &&
      (value === "continue" || value === "end")
    ) {
      state.lastProgressAt = Date.now();
      state.retryAttempt = 0;
      state.sourceHlsFault = false;
      state.lastError = null;
      // Do not advertise STREAMING merely because FFmpeg spawned. The first
      // real progress heartbeat proves media is actually flowing through the
      // target worker.
      state.status = "streaming";
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
    state.metricsTimer = null;
    state.reconnectTimer = null;
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
    const sourceUrl = getInternalHlsSourceUrl(channel.stream_key);

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
      sourceHlsFault: false,
      intentionalStop: false,
    };
    clearStateTimers(state);
    state.intentionalStop = false;
    state.destinationUrl = destinationUrl;
    state.targetType = targetType;
    state.protocol = protocol;
    state.channelStreamKey = channel.stream_key;
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
      if (classifyTransientSourceFailure(text)) {
        state.sourceHlsFault = true;
        state.lastError =
          "Internal HLS source temporarily unavailable; reconnecting automatically.";
      } else if (/error|failed|refused|timed out|broken pipe/i.test(text)) {
        state.lastError = text.trim().slice(-1200);
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
    // Remain in CONNECTING until FFmpeg emits its first progress heartbeat.
    // parseProgressLine() promotes the runtime state to STREAMING once media
    // is demonstrably flowing.

    proc.on("exit", async (code, signal) => {
      if (state.proc !== proc) return;
      state.proc = null;
      if (state.metricsTimer) clearInterval(state.metricsTimer);
      state.metricsTimer = null;

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
        const canReconnect = Boolean(
          fresh &&
          fresh.enabled &&
          fresh.auto_reconnect &&
          (await isSrsStreamLive(fresh.channel_stream_key)),
        );

        if (!canReconnect) {
          await pool.query(
            `UPDATE social_destinations
             SET is_running = false,
                 ffmpeg_pid = NULL,
                 status = 'disconnected',
                 current_bitrate_kbps = 0,
                 last_disconnected_at = now(),
                 last_error = $1,
                 updated_at = now()
             WHERE id = $2`,
            [lastError, destinationId],
          );
          processStates.delete(destinationId);
          return;
        }

        state.reconnectCount = safeNumber(fresh.reconnect_count) + 1;
        state.retryAttempt = safeNumber(state.retryAttempt) + 1;
        state.status = "reconnecting";

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
              ? "Internal HLS source temporarily unavailable; reconnecting automatically."
              : lastError,
            destinationId,
          ],
        );

        const delay = Math.min(
          RECONNECT_DELAY_MS * Math.max(1, state.retryAttempt),
          MAX_RECONNECT_DELAY_MS,
        );
        state.reconnectTimer = setTimeout(async () => {
          try {
            const latest = await getDestinationAndChannel(destinationId);
            if (!latest || !latest.enabled)
              return processStates.delete(destinationId);
            const readiness = await waitForPlayableHls(
              latest.channel_stream_key,
              STREAM_TARGET_HLS_READY_TIMEOUT_MS,
            );
            if (!readiness.ok) {
              throw new Error(
                `Source HLS is not ready yet: ${readiness.message}`,
              );
            }

            const url =
              latest.active_destination_url ||
              state.destinationUrl ||
              buildManualDestinationUrl(latest);
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
            state.lastError = error.message;
            console.error(
              `[STREAM-TARGET #${destinationId}] reconnect failed:`,
              error.message,
            );
            // A failed respawn doesn't produce an exit event, so schedule another
            // attempt through the same exit-style recovery path by marking this
            // state disconnected and recursively scheduling with a short timer.
            state.reconnectTimer = setTimeout(async () => {
              const latest = await getDestinationAndChannel(
                destinationId,
              ).catch(() => null);
              if (!latest || !latest.enabled || !latest.auto_reconnect)
                return processStates.delete(destinationId);
              const readiness = await waitForPlayableHls(
                latest.channel_stream_key,
                STREAM_TARGET_HLS_READY_TIMEOUT_MS,
              );
              if (!readiness.ok) {
                state.retryAttempt = safeNumber(state.retryAttempt) + 1;
                state.lastError = `Source HLS is not ready yet: ${readiness.message}`;
                return;
              }

              const url =
                latest.active_destination_url ||
                state.destinationUrl ||
                buildManualDestinationUrl(latest);
              spawnPush(
                latest,
                {
                  id: latest.channel_id,
                  name: latest.channel_name,
                  stream_key: latest.channel_stream_key,
                },
                url,
                { reconnect: true },
              ).catch((err) =>
                console.error(
                  `[STREAM-TARGET #${destinationId}] repeated reconnect failed:`,
                  err.message,
                ),
              );
            }, MAX_RECONNECT_DELAY_MS);
            state.reconnectTimer.unref?.();
          }
        }, delay);
        state.reconnectTimer.unref?.();
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
           updated_at = now()
       WHERE id = $1`,
      [target.id],
    );

    const readiness = await waitForPlayableHls(
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
        message: `Source is live but HLS media is not ready yet: ${readiness.message}`,
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
          return {
            ok: false,
            message: error.message || "Failed to create platform broadcast",
          };
        }
      }
    } else {
      destinationUrl = buildManualDestinationUrl(target);
    }

    if (!destinationUrl)
      return { ok: false, message: "Target destination URL is not configured" };

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
      const account = accountResult.rows[0];
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
      is_running: Boolean(state.proc),
      current_bitrate_kbps: state.currentBitrateKbps || 0,
      dropped_frames: state.droppedFrames || 0,
      reconnect_count: state.reconnectCount || 0,
      runtime_status: state.proc
        ? state.status || "connecting"
        : state.status || "disconnected",
      status: state.proc
        ? state.status || "connecting"
        : state.status || "disconnected",
      uptime_seconds: uptimeSeconds,
      started_at: startedAtMs > 0 ? new Date(startedAtMs).toISOString() : null,
      source_hls_fault: Boolean(state.sourceHlsFault),
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
  };
}

module.exports = { createStreamTargetManager, TARGET_TYPES };
