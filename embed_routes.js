// embed_routes.js
//
// Phase 1 — Embedded Player.
//
// Manages a per-channel, revocable "embed token" that is separate from the
// channel's RTMP stream_key, plus the public routes it gates:
//   - GET  /embed/:embedToken              the standalone, iframe-able player page
//   - GET  /api/public/embed/:embedToken/status   JSON the page's own JS polls
//
// Why a separate token instead of just using stream_key in the iframe URL:
// stream_key is the ingest credential (what OBS/an encoder authenticates
// with). Treating it as also-public embed-page identifier would mean any
// client who wants to stop showing an old/leaked embed URL would have to
// rotate their RTMP key too — reconnecting every encoder. This token can be
// regenerated independently (see /embed/regenerate below), instantly killing
// any iframe placed with the old link, without touching ingest at all.
//
// The actual video security still rests on the existing Bunny Token
// Authentication already wired into getPublicWatchStatus()/appendBunnyToken()
// in server.js — this token's job is narrower: gate *where the embed lives*,
// not the segment/manifest requests themselves.
//
// Mounted from server.js with:
//   const embedRoutes = require("./embed_routes");
//   embedRoutes.register(app, pool, {
//     getPublicWatchStatus,
//     authenticateAdmin, resolveOrganizationForRequest,
//     requireRole, requireOrganizationRole,
//   });
//   // ...and in the boot sequence, alongside the other ensure*Table() calls:
//   await embedRoutes.ensureEmbedColumns(pool);

const crypto = require("crypto");

// Where the iframe's src should point. Defaults to the API's own public URL
// since this route is served directly by this backend (not the separately
// hosted static frontend) — same pattern HLS_BASE_URL/CLIENT_URL already
// follow elsewhere in server.js. Override with EMBED_PLAYER_HOST if the
// production embed link should live on a different host/CDN zone than the
// general API base.
const EMBED_PLAYER_HOST =
  process.env.EMBED_PLAYER_HOST || process.env.API_PUBLIC_URL || "";

// 32 URL-safe characters (base64url of 24 random bytes) — long enough to be
// unguessable, short enough to paste into a query string without fuss.
const generateEmbedToken = () => crypto.randomBytes(24).toString("base64url");

async function ensureEmbedColumns(pool) {
  await pool.query(`
    ALTER TABLE channels
    ADD COLUMN IF NOT EXISTS embed_token VARCHAR(64) UNIQUE,
    ADD COLUMN IF NOT EXISTS embed_settings JSONB DEFAULT '{}'::jsonb
  `);

  // Backfill any channel created before this feature existed. Done as one
  // UPDATE per row (not a single bulk statement) so every channel gets its
  // own cryptographically random token instead of sharing one value or
  // anything derived from the channel's existing data.
  const missing = await pool.query(
    `SELECT id FROM channels WHERE embed_token IS NULL`,
  );
  for (const row of missing.rows) {
    await pool.query(`UPDATE channels SET embed_token = $1 WHERE id = $2`, [
      generateEmbedToken(),
      row.id,
    ]);
  }
}

function buildEmbedUrl(embedToken) {
  const base = EMBED_PLAYER_HOST.replace(/\/$/, "");
  return `${base}/embed/${embedToken}`;
}

function escapeHtml(value) {
  return String(value ?? "").replace(
    /[&<>"']/g,
    (c) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[
        c
      ],
  );
}

// Minimal shell used for 404/error cases — never actually reached by a
// correctly-copied embed code, but a client's site should never show a raw
// Express error page inside their iframe if a token is ever revoked.
function shellHtml(message) {
  return `<!DOCTYPE html>
<html><head><meta charset="utf-8" /><meta name="viewport" content="width=device-width, initial-scale=1" />
<style>
  html,body{margin:0;height:100%;background:#0b0b0d;color:#cfd2d6;font-family:system-ui,-apple-system,Segoe UI,sans-serif;}
  .wrap{height:100%;display:flex;align-items:center;justify-content:center;text-align:center;padding:24px;}
</style></head>
<body><div class="wrap">${escapeHtml(message)}</div></body></html>`;
}

function embedPageHtml(embedToken) {
  return `<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Live Stream</title>
<style>
  :root { --accent: #0d6efd; --accent-2: #fd9d00; }
  * { box-sizing: border-box; }
  html, body {
    margin: 0; padding: 0; height: 100%; background: #000;
    font-family: system-ui, -apple-system, "Segoe UI", sans-serif;
    overflow: hidden;
  }
  #nlm-embed-root {
    position: relative; width: 100%; height: 100%;
    display: flex; flex-direction: column; background: #000;
  }
  #nlm-video-wrap { position: relative; flex: 1 1 auto; min-height: 0; background: #000; }
  #nlm-video { width: 100%; height: 100%; display: block; background: #000; }
  #nlm-offline {
    position: absolute; inset: 0; display: flex; flex-direction: column;
    align-items: center; justify-content: center; gap: 14px; text-align: center;
    background: radial-gradient(circle at 50% 30%, #1a1a1e 0%, #0b0b0d 70%);
    color: #e7e9ec; padding: 24px;
  }
  #nlm-offline img { max-width: 140px; max-height: 70px; object-fit: contain; }
  #nlm-offline h2 { margin: 0; font-size: 17px; font-weight: 600; }
  #nlm-offline p { margin: 0; font-size: 13px; color: #9aa0a6; }
  #nlm-offline.hidden, #nlm-video-wrap.hidden { display: none; }
  #nlm-badge {
    position: absolute; top: 10px; left: 10px; z-index: 3;
    background: rgba(0,0,0,0.55); color: #fff; font-size: 11px; font-weight: 700;
    letter-spacing: 0.05em; padding: 4px 8px; border-radius: 4px;
    display: flex; align-items: center; gap: 6px;
  }
  #nlm-badge .dot { width: 7px; height: 7px; border-radius: 50%; background: #e0263f; }
  #nlm-chat-toggle {
    position: absolute; bottom: 10px; right: 10px; z-index: 3;
    background: rgba(0,0,0,0.55); color: #fff; border: none; border-radius: 6px;
    font-size: 12px; padding: 6px 10px; cursor: pointer;
  }
  #nlm-chat-panel {
    flex: 0 0 auto; height: 34%; min-height: 160px; max-height: 320px;
    background: #111214; border-top: 1px solid #232428;
    display: flex; flex-direction: column;
  }
  #nlm-chat-panel.hidden { display: none; }
  #nlm-chat-log { flex: 1 1 auto; overflow-y: auto; padding: 8px 10px; font-size: 12.5px; color: #d7dadd; }
  #nlm-chat-log .msg { margin-bottom: 6px; line-height: 1.35; word-break: break-word; }
  #nlm-chat-log .msg b { color: var(--accent); }
  #nlm-chat-form { display: flex; gap: 6px; padding: 8px; border-top: 1px solid #232428; }
  #nlm-chat-form input {
    flex: 1 1 auto; background: #1a1b1e; border: 1px solid #2a2b2f; border-radius: 6px;
    color: #eee; padding: 7px 9px; font-size: 12.5px; outline: none;
  }
  #nlm-chat-form button {
    background: var(--accent); color: #fff; border: none; border-radius: 6px;
    padding: 0 12px; font-size: 12.5px; font-weight: 600; cursor: pointer;
  }
  #nlm-playback-error {
    position: absolute; left: 10px; right: 10px; bottom: 44px; z-index: 3;
    background: rgba(20,20,22,0.9); color: #e7e9ec; border: 1px solid #3a3b3f;
    border-radius: 8px; padding: 10px 12px; font-size: 12px; line-height: 1.4;
  }
  #nlm-playback-error.hidden { display: none; }
</style>
</head>
<body>
  <div id="nlm-embed-root">
    <div id="nlm-video-wrap">
      <div id="nlm-badge" style="display:none;"><span class="dot"></span>LIVE</div>
      <video id="nlm-video" playsinline></video>
      <div id="nlm-offline">
        <img id="nlm-offline-logo" style="display:none;" />
        <h2 id="nlm-offline-title">Stream is offline</h2>
        <p id="nlm-offline-sub">Check back soon.</p>
      </div>
      <button id="nlm-chat-toggle" style="display:none;">Chat</button>
      <div id="nlm-playback-error" class="hidden">
        Playback couldn't start. If you use an ad blocker or a privacy
        browser extension, try disabling it for this site and refresh —
        some block video CDN requests by default.
      </div>
    </div>
    <div id="nlm-chat-panel" class="hidden">
      <div id="nlm-chat-log"></div>
      <form id="nlm-chat-form">
        <input id="nlm-chat-name" placeholder="Name" style="flex:0 0 90px;" maxlength="40" />
        <input id="nlm-chat-input" placeholder="Say something…" maxlength="500" />
        <button type="submit">Send</button>
      </form>
    </div>
  </div>

<script src="https://cdnjs.cloudflare.com/ajax/libs/hls.js/1.5.15/hls.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.7.5/socket.io.min.js"></script>
<script>
(function () {
  var EMBED_TOKEN = ${JSON.stringify(embedToken)};
  var params = new URLSearchParams(window.location.search);

  function boolParam(name, fallback) {
    if (!params.has(name)) return fallback;
    return ["1", "true", "yes"].includes(params.get(name).toLowerCase());
  }

  var videoEl = document.getElementById("nlm-video");
  var offlineEl = document.getElementById("nlm-offline");
  var videoWrapEl = document.getElementById("nlm-video-wrap");
  var badgeEl = document.getElementById("nlm-badge");
  var offlineLogo = document.getElementById("nlm-offline-logo");
  var offlineTitle = document.getElementById("nlm-offline-title");
  var offlineSub = document.getElementById("nlm-offline-sub");
  var chatToggleBtn = document.getElementById("nlm-chat-toggle");
  var playbackErrorEl = document.getElementById("nlm-playback-error");
  var chatPanel = document.getElementById("nlm-chat-panel");
  var chatLog = document.getElementById("nlm-chat-log");
  var chatForm = document.getElementById("nlm-chat-form");
  var chatNameInput = document.getElementById("nlm-chat-name");
  var chatMsgInput = document.getElementById("nlm-chat-input");

  var hls = null;
  var socket = null;
  var chatJoined = false;
  var wasLive = false;
  var currentSessionKey = null;
  var pollTimer = null;

  // Startup/recovery retry budget for a SINGLE session. A brand new
  // broadcast (or one just recovered by the server's ABR reconciler) can
  // legitimately 503 on its first manifest fetch for a few seconds while
  // renditions spin up — that's normal, not a real error. Without this,
  // hls.js gives up after its own internal retries, and since the session
  // key hasn't changed (it's still the same broadcast, just not ready yet
  // on the very first attempt), the page would sit dead until either the
  // session key changes or someone manually refreshes — which is exactly
  // the "sometimes it just doesn't work" pattern reported. This retries
  // playback a handful of times with backoff before showing a real error.
  var STARTUP_RETRY_DELAYS_MS = [2000, 3000, 4000, 5000, 6000, 8000]; // ~28s total
  var startupRetryCount = 0;
  var startupRetryTimer = null;

  function clearStartupRetry() {
    if (startupRetryTimer) {
      clearTimeout(startupRetryTimer);
      startupRetryTimer = null;
    }
  }

  // A broadcast "session" is identified by encoderGeneration + when it
  // started — not just a live/offline boolean. Restarting the server/SRS
  // mid-broadcast (or a bitrate-cap encoder respawn) can produce a brand
  // new HLS/ABR session while isLive stays true the whole time from the
  // page's perspective; without this, an already-open embed never rebuilds
  // its player for the new session and just sits on a dead manifest until
  // someone manually refreshes the page. Mirrors the same
  // encoderGeneration+liveStartedAtMs remount key LivePlayer.jsx/
  // WatchPage.jsx already use for this exact reason.
  function sessionKeyFor(data) {
    var stream = data && data.stream;
    if (!stream) return null;
    var startedAt = stream.liveStartedAtMs || stream.live_ms || 0;
    var generation = stream.encoderGeneration || 0;
    return generation + ":" + startedAt;
  }

  function showOffline(title, sub) {
    videoWrapEl.style.background = "#000";
    offlineEl.classList.remove("hidden");
    badgeEl.style.display = "none";
    hidePlaybackError();
    clearStartupRetry();
    if (title) offlineTitle.textContent = title;
    if (sub !== undefined) offlineSub.textContent = sub;
    if (hls) { try { hls.destroy(); } catch (e) {} hls = null; }
    videoEl.removeAttribute("src");
    videoEl.load();
  }

  function showLive() {
    offlineEl.classList.add("hidden");
    badgeEl.style.display = "flex";
  }

  function applyBranding(data) {
    var branding = data.branding || {};
    document.documentElement.style.setProperty("--accent", branding.primary_color || "#0d6efd");
    document.documentElement.style.setProperty("--accent-2", branding.secondary_color || "#fd9d00");
    if (branding.logo_url) {
      offlineLogo.src = branding.logo_url;
      offlineLogo.style.display = "block";
    }
    document.title = branding.title || branding.name || "Live Stream";
  }

  function startPlayback(data) {
    var manifestPath = data.transcodingEnabled
      ? "/api/abr/" + data.streamKey + "/master.m3u8"
      : "/api/hls/" + data.streamKey + ".m3u8";
    var origin = (data.hlsBaseUrl || window.location.origin).replace(/\\/$/, "");
    var manifestUrl = origin + manifestPath + (data.hlsAuthQs || "");

    var autoplay = boolParam("autoplay", data.embedSettings && data.embedSettings.autoplay !== undefined ? !!data.embedSettings.autoplay : true);
    var muted = boolParam("muted", data.embedSettings && data.embedSettings.muted !== undefined ? !!data.embedSettings.muted : true);
    var controls = boolParam("controls", data.embedSettings && data.embedSettings.controls !== undefined ? !!data.embedSettings.controls : true);

    videoEl.muted = muted;
    videoEl.controls = controls;
    hidePlaybackError();

    if (window.Hls && window.Hls.isSupported()) {
      if (hls) { try { hls.destroy(); } catch (e) {} }
      hls = new window.Hls({ enableWorker: true });
      hls.loadSource(manifestUrl);
      hls.attachMedia(videoEl);
      hls.on(window.Hls.Events.MANIFEST_PARSED, function () {
        startupRetryCount = 0;
        clearStartupRetry();
        hidePlaybackError();
        if (autoplay) videoEl.play().catch(function () {});
      });
      hls.on(window.Hls.Events.ERROR, function (evt, errData) {
        if (errData && errData.fatal) {
          // Logged with the real, unmasked manifest URL — some browser
          // ad-blocking/privacy extensions rewrite third-party hostnames
          // shown in the Network/Console panels (e.g. to something like
          // "…io_orrdns") to defeat anti-adblock detection scripts, which
          // can make a blocked CDN request look like an unrelated 404.
          // Logging it here from our own code gives the real target.
          console.warn("[embed] fatal HLS error", errData.type, manifestUrl);
          scheduleStartupRetry(data);
        }
      });
    } else if (videoEl.canPlayType("application/vnd.apple.mpegurl")) {
      // Safari/iOS native HLS
      videoEl.src = manifestUrl;
      videoEl.addEventListener("error", function () {
        console.warn("[embed] native HLS playback error", manifestUrl);
        scheduleStartupRetry(data);
      });
      if (autoplay) videoEl.play().catch(function () {});
    }
  }

  // Shows a calm "stream is starting" message and retries shortly, up to a
  // small budget — only escalates to the real error message (ad blocker
  // etc.) once that budget is exhausted, so a normal few-second startup or
  // reconciler-driven recovery window never looks like a broken embed.
  function scheduleStartupRetry(data) {
    if (startupRetryCount >= STARTUP_RETRY_DELAYS_MS.length) {
      showPlaybackError(true);
      return;
    }
    showPlaybackError(false);
    var delay = STARTUP_RETRY_DELAYS_MS[startupRetryCount];
    startupRetryCount += 1;
    clearStartupRetry();
    startupRetryTimer = setTimeout(function () {
      startPlayback(data);
    }, delay);
  }

  function showPlaybackError(isFinal) {
    playbackErrorEl.textContent = isFinal
      ? "Playback couldn't start. If you use an ad blocker or a privacy browser extension, try disabling it for this site and refresh \u2014 some block video CDN requests by default."
      : "Stream is starting \u2014 this can take a few seconds.";
    playbackErrorEl.classList.remove("hidden");
  }

  function hidePlaybackError() {
    playbackErrorEl.classList.add("hidden");
  }

  function setupChat(data) {
    var chatEnabled = boolParam("chat", data.embedSettings && data.embedSettings.chat !== undefined ? !!data.embedSettings.chat : false);
    if (!chatEnabled || !window.io) return;

    chatToggleBtn.style.display = "block";
    chatToggleBtn.onclick = function () {
      chatPanel.classList.toggle("hidden");
    };

    if (chatJoined) return;
    chatJoined = true;

    socket = window.io(window.location.origin, { transports: ["websocket", "polling"] });
    socket.on("connect", function () {
      socket.emit("chat:join", { streamKey: data.streamKey });
    });
    socket.on("chat:new", function (msg) {
      var row = document.createElement("div");
      row.className = "msg";
      var b = document.createElement("b");
      b.textContent = (msg.display_name || "Guest") + ": ";
      row.appendChild(b);
      row.appendChild(document.createTextNode(msg.message || ""));
      chatLog.appendChild(row);
      chatLog.scrollTop = chatLog.scrollHeight;
    });
    socket.on("chat:error", function (err) {
      console.warn("[embed chat]", err && err.message);
    });

    chatForm.addEventListener("submit", function (e) {
      e.preventDefault();
      var message = chatMsgInput.value.trim();
      if (!message) return;
      socket.emit("chat:send", {
        streamKey: data.streamKey,
        displayName: chatNameInput.value.trim() || "Guest",
        message: message,
      });
      chatMsgInput.value = "";
    });
  }

  function refresh() {
    fetch("/api/public/embed/" + encodeURIComponent(EMBED_TOKEN) + "/status")
      .then(function (r) {
        if (!r.ok) throw new Error("status " + r.status);
        return r.json();
      })
      .then(function (data) {
        if (!data.ok) throw new Error(data.message || "embed error");
        applyBranding(data);
        setupChat(data);

        if (data.isLive) {
          showLive();
          var sessionKey = sessionKeyFor(data);
          // Rebuild playback whenever we weren't live before, OR the
          // broadcast session itself changed (server/SRS restart,
          // encoder respawn) even though isLive never flipped to false —
          // see sessionKeyFor() above for why a plain wasLive boolean
          // isn't enough here.
          if (!wasLive || sessionKey !== currentSessionKey) {
            // Genuinely new/changed session — this is not a retry, so give
            // it a fresh startup-retry budget.
            startupRetryCount = 0;
            clearStartupRetry();
            startPlayback(data);
          }
          wasLive = true;
          currentSessionKey = sessionKey;
        } else {
          wasLive = false;
          currentSessionKey = null;
          var nextTitle = "Stream is offline";
          var nextSub = "Check back soon.";
          if (data.schedule && data.schedule.scheduled_start) {
            var when = new Date(data.schedule.scheduled_start);
            nextSub = "Next scheduled: " + when.toLocaleString();
          }
          showOffline(nextTitle, nextSub);
        }
      })
      .catch(function (err) {
        console.warn("[embed] status poll failed", err.message);
        // A single missed poll (transient network blip, brief server
        // restart) shouldn't tear down playback that's otherwise fine —
        // only show the offline shell if we weren't already live, so a
        // hiccup on the status endpoint doesn't interrupt real viewers.
        if (!wasLive) {
          showOffline("Stream unavailable", "Please try again shortly.");
        }
      });
  }

  refresh();
  pollTimer = setInterval(refresh, 8000);
  window.addEventListener("beforeunload", function () {
    if (pollTimer) clearInterval(pollTimer);
    clearStartupRetry();
    if (socket) socket.disconnect();
  });
})();
</script>
</body>
</html>`;
}

module.exports = {
  ensureEmbedColumns,
  generateEmbedToken,

  register(app, pool, deps) {
    const {
      getPublicWatchStatus,
      authenticateAdmin,
      resolveOrganizationForRequest,
      requireRole,
      requireOrganizationRole,
    } = deps;

    async function getOwnedChannel(channelId, organizationId) {
      const result = await pool.query(
        `SELECT * FROM channels WHERE id = $1 AND organization_id = $2`,
        [channelId, organizationId],
      );
      return result.rows[0] || null;
    }

    // ── Admin: fetch embed info + snippet for a channel ──
    app.get(
      "/api/channels/:channelId/embed",
      authenticateAdmin,
      resolveOrganizationForRequest,
      async (req, res) => {
        try {
          const channel = await getOwnedChannel(
            req.params.channelId,
            req.organization.id,
          );
          if (!channel) {
            return res
              .status(404)
              .json({ ok: false, message: "Channel not found" });
          }
          res.json({
            ok: true,
            embed_token: channel.embed_token,
            embed_url: buildEmbedUrl(channel.embed_token),
            embed_settings: channel.embed_settings || {},
          });
        } catch (error) {
          console.error("Get Channel Embed Error:", error);
          res
            .status(500)
            .json({ ok: false, message: "Failed to fetch embed info" });
        }
      },
    );

    // ── Admin: persist default embed params so "Copy Embed Code" pre-fills
    // them next time (does not affect iframes already placed with explicit
    // query-string overrides — those always win over the saved defaults) ──
    app.patch(
      "/api/channels/:channelId/embed/settings",
      authenticateAdmin,
      resolveOrganizationForRequest,
      requireRole("super_admin", "admin", "operator"),
      requireOrganizationRole("owner", "admin"),
      async (req, res) => {
        try {
          const channel = await getOwnedChannel(
            req.params.channelId,
            req.organization.id,
          );
          if (!channel) {
            return res
              .status(404)
              .json({ ok: false, message: "Channel not found" });
          }
          const { autoplay, muted, controls, chat } = req.body || {};
          const settings = {
            autoplay: Boolean(autoplay),
            muted: muted !== false, // most browsers block unmuted autoplay
            controls: controls !== false,
            chat: Boolean(chat),
          };
          const result = await pool.query(
            `UPDATE channels SET embed_settings = $1 WHERE id = $2 RETURNING embed_settings`,
            [JSON.stringify(settings), channel.id],
          );
          res.json({ ok: true, embed_settings: result.rows[0].embed_settings });
        } catch (error) {
          console.error("Update Embed Settings Error:", error);
          res
            .status(500)
            .json({ ok: false, message: "Failed to save embed settings" });
        }
      },
    );

    // ── Admin: rotate the embed token — instantly revokes every iframe
    // placed with the old link, without touching the RTMP stream_key ──
    app.post(
      "/api/channels/:channelId/embed/regenerate",
      authenticateAdmin,
      resolveOrganizationForRequest,
      requireRole("super_admin", "admin", "operator"),
      requireOrganizationRole("owner", "admin"),
      async (req, res) => {
        try {
          const channel = await getOwnedChannel(
            req.params.channelId,
            req.organization.id,
          );
          if (!channel) {
            return res
              .status(404)
              .json({ ok: false, message: "Channel not found" });
          }
          const newToken = generateEmbedToken();
          await pool.query(
            `UPDATE channels SET embed_token = $1 WHERE id = $2`,
            [newToken, channel.id],
          );
          res.json({
            ok: true,
            embed_token: newToken,
            embed_url: buildEmbedUrl(newToken),
          });
        } catch (error) {
          console.error("Regenerate Embed Token Error:", error);
          res
            .status(500)
            .json({ ok: false, message: "Failed to regenerate embed link" });
        }
      },
    );

    // ── Public: status JSON the embed page's own JS polls ──
    app.get("/api/public/embed/:embedToken/status", async (req, res) => {
      try {
        const channelResult = await pool.query(
          `SELECT c.id, c.name, c.stream_key, c.organization_id, c.embed_settings
           FROM channels c
           JOIN organizations o ON o.id = c.organization_id
           WHERE c.embed_token = $1 AND o.is_active = TRUE
           LIMIT 1`,
          [req.params.embedToken],
        );
        const channel = channelResult.rows[0];
        if (!channel) {
          return res
            .status(404)
            .json({ ok: false, message: "Embed not found" });
        }

        const status = await getPublicWatchStatus(channel.stream_key);

        res.set("Cache-Control", "no-store, private");
        res.json({
          ok: true,
          channelName: channel.name,
          streamKey: channel.stream_key,
          embedSettings: channel.embed_settings || {},
          ...status,
        });
      } catch (error) {
        console.error("Public Embed Status Error:", error);
        res
          .status(500)
          .json({ ok: false, message: "Failed to load embed status" });
      }
    });

    // ── Public: the standalone, iframe-able embed page ──
    app.get("/embed/:embedToken", async (req, res) => {
      const { embedToken } = req.params;

      try {
        const channelResult = await pool.query(
          `SELECT c.id FROM channels c
           JOIN organizations o ON o.id = c.organization_id
           WHERE c.embed_token = $1 AND o.is_active = TRUE
           LIMIT 1`,
          [embedToken],
        );

        res.set("Cache-Control", "no-store, private");
        // Intentionally no X-Frame-Options / frame-ancestors restriction —
        // this route exists specifically to be iframed on a client's own
        // website. Playback itself stays secured via the existing
        // Bunny-signed HLS/ABR URLs, not via a frame-ancestors allowlist.
        if (!channelResult.rows[0]) {
          return res
            .status(404)
            .send(shellHtml("This stream embed could not be found."));
        }

        res.send(embedPageHtml(embedToken));
      } catch (error) {
        console.error("Embed Page Error:", error);
        res.status(500).send(shellHtml("This stream could not be loaded."));
      }
    });
  },
};
