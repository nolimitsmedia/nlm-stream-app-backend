// facebook_page_routes.js
//
// Backend for Facebook Live Studio (client: FacebookLiveStudio.jsx).
// Mounted from oauth_routes.js, so server.js does not need to change.
//
//   GET   /api/channels/:channelId/facebook/studio      one call that drives the studio UI
//   GET   /api/channels/:channelId/facebook/content     Page posts / videos / live videos
//   POST  /api/channels/:channelId/facebook/go-live     create the Page live video + start pushing
//   POST  /api/channels/:channelId/facebook/end-live    stop pushing + end the live on Facebook
//   PATCH /api/channels/:channelId/facebook/settings    broadcast title/description, auto go-live
//
//   POST  /api/oauth/facebook/data-deletion             Meta "Data Deletion Request" callback
//   GET   /api/oauth/facebook/data-deletion/status      status page Meta links people to
//   POST  /api/oauth/facebook/deauthorize               Meta "Deauthorize" callback
//
// Page access tokens never leave the server.

const crypto = require("crypto");
const express = require("express");
const facebookGraph = require("./facebook_graph_service");
const { decryptOAuthAccount } = require("./social_oauth_schema");
const { decryptTargetCredentials } = require("./stream_target_schema");

const PROFILE_CACHE_MS = 60 * 1000;
const LIVE_STATUS_CACHE_MS = 4 * 1000;
const CONTENT_CACHE_MS = 20 * 1000;

module.exports = function registerFacebookPageRoutes(app, pool, mw) {
  const {
    authenticateAdmin,
    resolveOrganizationForRequest,
    requireRole,
    requireOrganizationRole,
    streamTargetManager = null,
  } = mw;

  const readMw = [authenticateAdmin, resolveOrganizationForRequest];
  const manageMw = [
    authenticateAdmin,
    resolveOrganizationForRequest,
    requireRole("super_admin", "admin", "operator"),
    requireOrganizationRole("owner", "admin"),
  ];

  // ── schema ──────────────────────────────────────────────────
  pool
    .query(
      `ALTER TABLE social_destinations
       ADD COLUMN IF NOT EXISTS broadcast_title VARCHAR(255),
       ADD COLUMN IF NOT EXISTS broadcast_description TEXT,
       ADD COLUMN IF NOT EXISTS last_platform_broadcast_id VARCHAR(255)`,
    )
    .catch((error) =>
      console.error("[FB-STUDIO] schema update failed:", error.message),
    );

  // ── tiny TTL cache (keeps Graph API usage low while the UI polls) ──
  const cache = new Map();
  async function cached(key, ttlMs, loader) {
    const hit = cache.get(key);
    if (hit && hit.expires > Date.now()) return hit.value;
    const value = await loader();
    cache.set(key, { value, expires: Date.now() + ttlMs });
    if (cache.size > 2000) {
      const now = Date.now();
      for (const [k, v] of cache) if (v.expires <= now) cache.delete(k);
    }
    return value;
  }
  function invalidate(prefix) {
    for (const key of cache.keys())
      if (key.startsWith(prefix)) cache.delete(key);
  }

  // ── helpers ─────────────────────────────────────────────────
  async function getOwnedChannel(channelId, organizationId) {
    const result = await pool.query(
      `SELECT * FROM channels WHERE id = $1 AND organization_id = $2`,
      [channelId, organizationId],
    );
    return result.rows[0] || null;
  }

  async function loadFacebookTarget(channelId) {
    const result = await pool.query(
      `SELECT * FROM social_destinations
       WHERE channel_id = $1 AND platform = 'facebook'
       ORDER BY (automation_mode = 'oauth') DESC, id
       LIMIT 1`,
      [channelId],
    );
    return result.rows[0] ? decryptTargetCredentials(result.rows[0]) : null;
  }

  async function loadAccount(accountId, organizationId) {
    if (!accountId) return null;
    const result = await pool.query(
      `SELECT * FROM social_oauth_accounts WHERE id = $1 AND organization_id = $2`,
      [accountId, organizationId],
    );
    return decryptOAuthAccount(result.rows[0]) || null;
  }

  async function loadFacebookUserRow(organizationId, adminId) {
    const result = await pool.query(
      `SELECT id, external_account_name, connection_status, token_expires_at
       FROM social_oauth_accounts
       WHERE organization_id = $1 AND platform = 'facebook_user'
       ORDER BY (connected_by_admin_id = $2) DESC, updated_at DESC NULLS LAST, id DESC
       LIMIT 1`,
      [organizationId, adminId],
    );
    return result.rows[0] || null;
  }

  async function markReconnect(accountId, error) {
    await pool
      .query(
        `UPDATE social_oauth_accounts
         SET connection_status = 'reconnect_required',
             token_last_error = $1,
             reconnect_required_at = COALESCE(reconnect_required_at, NOW()),
             updated_at = NOW()
         WHERE id = $2`,
        [
          String(error?.message || "Facebook token invalid").slice(0, 1000),
          accountId,
        ],
      )
      .catch(() => {});
  }

  function reconnectResponse(res) {
    return res.status(409).json({
      ok: false,
      code: "facebook_reconnect_required",
      message:
        "Facebook access for this Page expired or was removed. Continue with Facebook again.",
    });
  }

  function targetSummary(target) {
    if (!target) return null;
    const runtime = streamTargetManager?.getRuntimeState?.(target.id) || null;
    const workerRunning = Boolean(
      runtime?.worker_running ?? target.is_running ?? false,
    );
    const startedAt = target.started_at
      ? new Date(target.started_at).getTime()
      : null;
    return {
      id: target.id,
      enabled: target.enabled !== false,
      autoStart: Boolean(target.auto_start),
      status: runtime?.runtime_status || target.status || "stopped",
      workerRunning,
      deliveryVerified: Boolean(runtime?.delivery_verified),
      bitrateKbps: Number(
        runtime?.current_bitrate_kbps ?? target.current_bitrate_kbps ?? 0,
      ),
      droppedFrames: Number(runtime?.dropped_frames ?? 0),
      reconnects: Number(
        runtime?.reconnect_count ?? target.reconnect_count ?? 0,
      ),
      uptimeSeconds: Number.isFinite(Number(runtime?.uptime_seconds))
        ? Number(runtime.uptime_seconds)
        : workerRunning && startedAt
          ? Math.max(0, Math.floor((Date.now() - startedAt) / 1000))
          : 0,
      lastError: target.last_error || null,
      lastErrorAt: target.last_error ? target.updated_at || null : null,
      failed: (target.status || "") === "failed",
      broadcastTitle: target.broadcast_title || "",
      broadcastDescription: target.broadcast_description || "",
      liveVideoId: target.platform_broadcast_id || null,
      lastLiveVideoId: target.last_platform_broadcast_id || null,
    };
  }

  async function liveStatus(liveVideoId, account) {
    if (!liveVideoId || !account) return null;
    try {
      return await cached(`live:${liveVideoId}`, LIVE_STATUS_CACHE_MS, () =>
        facebookGraph.getLiveVideoStatus({
          liveVideoId,
          pageAccessToken: account.access_token,
        }),
      );
    } catch (error) {
      if (facebookGraph.isReconnectError(error))
        await markReconnect(account.id, error);
      return { id: liveVideoId, status: "UNKNOWN", error: error.message };
    }
  }

  // ── GET studio ──────────────────────────────────────────────
  app.get(
    "/api/channels/:channelId/facebook/studio",
    ...readMw,
    async (req, res) => {
      try {
        const channel = await getOwnedChannel(
          req.params.channelId,
          req.organization.id,
        );
        if (!channel)
          return res
            .status(404)
            .json({ ok: false, message: "Channel not found" });

        const userRow = await loadFacebookUserRow(
          req.organization.id,
          req.admin.id,
        );
        const target = await loadFacebookTarget(channel.id);
        const account =
          target?.automation_mode === "oauth" && target?.oauth_account_id
            ? await loadAccount(target.oauth_account_id, req.organization.id)
            : null;

        let page = null;
        if (account && account.platform === "facebook") {
          page = {
            accountId: account.id,
            pageId: account.external_account_id,
            name: account.external_account_name,
            connectionStatus: account.connection_status || "connected",
            category: null,
            followers: null,
            pictureUrl: null,
            coverUrl: null,
            link: `https://www.facebook.com/${account.external_account_id}`,
          };
          if (account.connection_status !== "reconnect_required") {
            try {
              const profile = await cached(
                `profile:${account.external_account_id}`,
                PROFILE_CACHE_MS,
                () =>
                  facebookGraph.getPageProfile({
                    pageId: account.external_account_id,
                    pageAccessToken: account.access_token,
                  }),
              );
              page = { ...page, ...profile, name: profile.name || page.name };
            } catch (error) {
              if (facebookGraph.isReconnectError(error)) {
                await markReconnect(account.id, error);
                page.connectionStatus = "reconnect_required";
              }
            }
          }
        }

        const summary = targetSummary(target);
        const live = summary?.liveVideoId
          ? await liveStatus(summary.liveVideoId, account)
          : null;
        const lastBroadcast =
          !live && summary?.lastLiveVideoId
            ? await liveStatus(summary.lastLiveVideoId, account)
            : null;

        res.json({
          ok: true,
          configured: facebookGraph.isConfigured(),
          channel: {
            id: channel.id,
            name: channel.name,
            sourceLive: Boolean(channel.is_live),
          },
          facebook: {
            signedIn: Boolean(userRow),
            signedInAs: userRow?.external_account_name || null,
            reconnectRequired:
              userRow?.connection_status === "reconnect_required" ||
              page?.connectionStatus === "reconnect_required",
          },
          page,
          target: summary,
          live,
          lastBroadcast,
        });
      } catch (error) {
        console.error("Facebook Studio Error:", error);
        res
          .status(500)
          .json({ ok: false, message: "Failed to load Facebook Live Studio" });
      }
    },
  );

  // ── GET content (pages_read_engagement) ─────────────────────
  app.get(
    "/api/channels/:channelId/facebook/content",
    ...readMw,
    async (req, res) => {
      try {
        const tab = ["posts", "videos", "live"].includes(req.query.tab)
          ? req.query.tab
          : "posts";
        const channel = await getOwnedChannel(
          req.params.channelId,
          req.organization.id,
        );
        if (!channel)
          return res
            .status(404)
            .json({ ok: false, message: "Channel not found" });
        const target = await loadFacebookTarget(channel.id);
        const account = await loadAccount(
          target?.oauth_account_id,
          req.organization.id,
        );
        if (!account || account.platform !== "facebook") {
          return res
            .status(409)
            .json({
              ok: false,
              code: "no_page",
              message: "Choose a Page first.",
            });
        }
        if (req.query.refresh)
          invalidate(`content:${account.external_account_id}:`);

        const args = {
          pageId: account.external_account_id,
          pageAccessToken: account.access_token,
          limit: 6,
        };
        const loader =
          tab === "videos"
            ? () => facebookGraph.getPageVideos(args)
            : tab === "live"
              ? () => facebookGraph.getPageLiveVideos(args)
              : () => facebookGraph.getPagePosts(args);

        let items;
        try {
          items = await cached(
            `content:${account.external_account_id}:${tab}`,
            CONTENT_CACHE_MS,
            loader,
          );
        } catch (error) {
          if (facebookGraph.isReconnectError(error)) {
            await markReconnect(account.id, error);
            return reconnectResponse(res);
          }
          throw error;
        }

        res.json({
          ok: true,
          tab,
          page: {
            pageId: account.external_account_id,
            name: account.external_account_name,
          },
          fetchedAt: new Date().toISOString(),
          items,
        });
      } catch (error) {
        console.error("Facebook Content Error:", error);
        res.status(502).json({
          ok: false,
          message: error.message || "Could not read content from Facebook",
        });
      }
    },
  );

  // ── PATCH settings ──────────────────────────────────────────
  app.patch(
    "/api/channels/:channelId/facebook/settings",
    ...manageMw,
    async (req, res) => {
      try {
        const channel = await getOwnedChannel(
          req.params.channelId,
          req.organization.id,
        );
        if (!channel)
          return res
            .status(404)
            .json({ ok: false, message: "Channel not found" });
        const target = await loadFacebookTarget(channel.id);
        if (!target)
          return res
            .status(409)
            .json({ ok: false, message: "Choose a Page first." });

        const body = req.body || {};
        const sets = [];
        const values = [];
        if (body.title !== undefined) {
          values.push(String(body.title).slice(0, 254));
          sets.push(`broadcast_title = $${values.length}`);
        }
        if (body.description !== undefined) {
          values.push(String(body.description).slice(0, 5000));
          sets.push(`broadcast_description = $${values.length}`);
        }
        if (body.autoStart !== undefined) {
          values.push(Boolean(body.autoStart));
          sets.push(`auto_start = $${values.length}`);
        }
        if (!sets.length) return res.json({ ok: true });
        values.push(target.id);
        await pool.query(
          `UPDATE social_destinations SET ${sets.join(", ")}, updated_at = NOW()
           WHERE id = $${values.length}`,
          values,
        );

        let started = null;
        if (
          body.autoStart === true &&
          channel.is_live &&
          target.automation_mode === "oauth" &&
          target.oauth_account_id &&
          !target.is_running &&
          !target.platform_broadcast_id &&
          streamTargetManager
        ) {
          const fresh = await loadFacebookTarget(channel.id);
          sweepAttempts.delete(Number(fresh.id));
          started = await startFacebookTargetNow(
            fresh,
            channel,
            "Automatic mode switched on while encoder is live",
          );
        }
        res.json({
          ok: true,
          started: started ? Boolean(started.ok) : undefined,
          message: started && !started.ok ? started.message : undefined,
        });
      } catch (error) {
        console.error("Facebook Settings Error:", error);
        res.status(500).json({ ok: false, message: "Failed to save settings" });
      }
    },
  );

  // ── POST go-live (Live Video API + publish_video + pages_manage_posts) ──
  app.post(
    "/api/channels/:channelId/facebook/go-live",
    ...manageMw,
    async (req, res) => {
      try {
        if (!streamTargetManager) {
          return res.status(503).json({
            ok: false,
            message: "Stream target engine is not available",
          });
        }
        const channel = await getOwnedChannel(
          req.params.channelId,
          req.organization.id,
        );
        if (!channel)
          return res
            .status(404)
            .json({ ok: false, message: "Channel not found" });

        let target = await loadFacebookTarget(channel.id);
        if (
          !target ||
          target.automation_mode !== "oauth" ||
          !target.oauth_account_id
        ) {
          return res.status(409).json({
            ok: false,
            code: "no_page",
            message: "Choose a Facebook Page first.",
          });
        }
        if (target.platform_broadcast_id || target.is_running) {
          return res.status(409).json({
            ok: false,
            code: "already_live",
            message: "This channel is already live on Facebook.",
          });
        }

        const force = Boolean(req.body?.force);
        if (!channel.is_live && !force) {
          return res.status(409).json({
            ok: false,
            code: "source_offline",
            message:
              "NLM is not receiving video for this channel yet. Start your encoder (OBS, vMix, hardware encoder) first, then go live.",
          });
        }

        const title = String(req.body?.title || "")
          .trim()
          .slice(0, 254);
        const description = String(req.body?.description || "")
          .trim()
          .slice(0, 5000);
        await pool.query(
          `UPDATE social_destinations
           SET broadcast_title = $1, broadcast_description = $2,
               enabled = TRUE, updated_at = NOW()
           WHERE id = $3`,
          [title || null, description || null, target.id],
        );

        target = await loadFacebookTarget(channel.id);
        sweepAttempts.delete(Number(target.id));
        const result = await streamTargetManager.startTarget(
          target,
          channel,
          req.organization.id,
        );
        if (!result?.ok) {
          return res.status(400).json({
            ok: false,
            code: result?.failure?.code || "go_live_failed",
            message: result?.message || "Could not start the Facebook live",
          });
        }

        const fresh = await loadFacebookTarget(channel.id);
        const account = await loadAccount(
          fresh?.oauth_account_id,
          req.organization.id,
        );
        invalidate(`content:${account?.external_account_id}:`);
        const live = fresh?.platform_broadcast_id
          ? await liveStatus(fresh.platform_broadcast_id, account)
          : null;

        res.json({
          ok: true,
          message: "You are live on Facebook",
          live,
          target: targetSummary(fresh),
        });
      } catch (error) {
        console.error("Facebook Go Live Error:", error);
        res.status(500).json({
          ok: false,
          message: error.message || "Could not start the Facebook live",
        });
      }
    },
  );

  // ── POST end-live ───────────────────────────────────────────
  app.post(
    "/api/channels/:channelId/facebook/end-live",
    ...manageMw,
    async (req, res) => {
      try {
        const channel = await getOwnedChannel(
          req.params.channelId,
          req.organization.id,
        );
        if (!channel)
          return res
            .status(404)
            .json({ ok: false, message: "Channel not found" });
        const target = await loadFacebookTarget(channel.id);
        if (!target)
          return res
            .status(404)
            .json({ ok: false, message: "No Facebook target on this channel" });

        const lastId =
          target.platform_broadcast_id || target.last_platform_broadcast_id;
        await streamTargetManager.stopTarget(target);
        if (lastId) {
          cache.delete(`live:${lastId}`);
          await pool
            .query(
              `UPDATE social_destinations SET last_platform_broadcast_id = $1 WHERE id = $2`,
              [lastId, target.id],
            )
            .catch(() => {});
        }
        const account = await loadAccount(
          target.oauth_account_id,
          req.organization.id,
        );
        if (account) invalidate(`content:${account.external_account_id}:`);

        res.json({
          ok: true,
          message: "Facebook live ended",
          lastLiveVideoId: lastId,
        });
      } catch (error) {
        console.error("Facebook End Live Error:", error);
        res
          .status(500)
          .json({ ok: false, message: "Could not end the Facebook live" });
      }
    },
  );

  // ── POST diagnostics: "Check Facebook setup" ───────────────
  // Read-only checks plus one UNPUBLISHED live-video test that is deleted
  // immediately, so nothing appears on the Page.
  app.post(
    "/api/channels/:channelId/facebook/diagnostics",
    ...manageMw,
    async (req, res) => {
      const checks = [];
      const add = (id, label, ok, detail) =>
        checks.push({ id, label, ok, detail: detail || "" });
      try {
        const channel = await getOwnedChannel(
          req.params.channelId,
          req.organization.id,
        );
        if (!channel)
          return res
            .status(404)
            .json({ ok: false, message: "Channel not found" });

        add(
          "config",
          "Facebook app is configured on the server",
          facebookGraph.isConfigured(),
          facebookGraph.isConfigured()
            ? ""
            : "Set FACEBOOK_APP_ID, FACEBOOK_APP_SECRET and FACEBOOK_CONFIG_ID.",
        );

        const target = await loadFacebookTarget(channel.id);
        const account = await loadAccount(
          target?.oauth_account_id,
          req.organization.id,
        );
        add(
          "page",
          "A Page is selected for this channel",
          Boolean(account && account.platform === "facebook"),
          account ? account.external_account_name : "Choose a Page first.",
        );
        if (!account || account.platform !== "facebook")
          return res.json({ ok: true, checks });

        // Token + granted permissions for this Page
        const required = [
          "pages_show_list",
          "pages_read_engagement",
          "pages_manage_posts",
          "publish_video",
        ];
        try {
          const info = await facebookGraph.debugToken(account.access_token);
          add(
            "token",
            "Page access token is valid",
            Boolean(info?.is_valid),
            info?.is_valid
              ? `Type: ${info.type || "PAGE"}`
              : info?.error?.message || "Token invalid. Reconnect Facebook.",
          );
          const scopes = new Set(info?.scopes || []);
          const granular = Array.isArray(info?.granular_scopes)
            ? info.granular_scopes
            : [];
          for (const scope of required) {
            const entry = granular.find((g) => g.scope === scope);
            const coversPage =
              !entry?.target_ids ||
              entry.target_ids
                .map(String)
                .includes(String(account.external_account_id));
            const ok = scopes.has(scope) && coversPage;
            add(
              `scope:${scope}`,
              `Permission granted: ${scope}`,
              ok,
              ok
                ? ""
                : scopes.has(scope)
                  ? "Granted, but not for this Page. Use Edit Page access and tick this Page."
                  : "Not granted. Add it to the Facebook Login for Business configuration, then reconnect.",
            );
          }
        } catch (error) {
          add("token", "Page access token is valid", false, error.message);
        }

        // Page role (CREATE_CONTENT task) via the signed-in user's Page list
        try {
          const userRow = (
            await pool.query(
              `SELECT * FROM social_oauth_accounts
               WHERE organization_id = $1 AND platform = 'facebook_user'
               ORDER BY updated_at DESC NULLS LAST, id DESC LIMIT 1`,
              [req.organization.id],
            )
          ).rows[0];
          const user = decryptOAuthAccount(userRow);
          if (user) {
            const pages = await facebookGraph.listUserPages(user.access_token);
            const page = pages.find(
              (p) => String(p.pageId) === String(account.external_account_id),
            );
            add(
              "role",
              "Your Page role can create content",
              Boolean(page?.canGoLive),
              page
                ? `Tasks: ${page.tasks.join(", ") || "not reported"}`
                : "Page not in your shared Pages list.",
            );
          }
        } catch (error) {
          add(
            "role",
            "Your Page role can create content",
            false,
            error.message,
          );
        }

        // The real test: can this token create a live video on the Page?
        try {
          await facebookGraph.testCreateLiveVideo({
            pageId: account.external_account_id,
            pageAccessToken: account.access_token,
          });
          add(
            "live",
            "Facebook allows creating a live video on this Page",
            true,
            "Test video created unpublished and deleted.",
          );
        } catch (error) {
          add(
            "live",
            "Facebook allows creating a live video on this Page",
            false,
            error.message,
          );
        }

        res.json({ ok: true, checks });
      } catch (error) {
        console.error("Facebook Diagnostics Error:", error);
        res
          .status(500)
          .json({ ok: false, message: "Diagnostics failed", checks });
      }
    },
  );

  // ── Automatic go-live safety net ───────────────────────────
  // Auto-start normally fires from SRS on_publish (encoder connects). That
  // single trigger is missed when: the Page was chosen or automatic mode was
  // switched on while OBS was already streaming, the backend restarted
  // mid-stream, or Facebook briefly refused the first attempt. This sweep
  // starts any enabled auto-start Facebook target whose channel is live but
  // which isn't running. Attempts are limited per encoder session so a Page
  // that Facebook keeps refusing doesn't loop.
  const SWEEP_MS = 30 * 1000;
  const MIN_SESSION_AGE_MS = 30 * 1000;
  const RETRY_GAP_MS = 2 * 60 * 1000;
  const MAX_ATTEMPTS_PER_SESSION = 3;
  const sweepAttempts = new Map(); // targetId -> { session, count, lastAt }

  async function startFacebookTargetNow(target, channel, reason) {
    const key = Number(target.id);
    const session = String(channel.live_started_at || "unknown");
    const entry = sweepAttempts.get(key);
    const state =
      entry && entry.session === session
        ? entry
        : { session, count: 0, lastAt: 0 };
    if (state.count >= MAX_ATTEMPTS_PER_SESSION) return null;
    if (Date.now() - state.lastAt < RETRY_GAP_MS) return null;
    state.count += 1;
    state.lastAt = Date.now();
    sweepAttempts.set(key, state);

    console.log(
      `[FB-STUDIO] ${reason}: starting Facebook target #${target.id} for channel ${channel.id} (attempt ${state.count}/${MAX_ATTEMPTS_PER_SESSION}).`,
    );
    const result = await streamTargetManager.startTarget(
      decryptTargetCredentials(target),
      channel,
      channel.organization_id,
      { requestedBy: "facebook-auto-sweep" },
    );
    if (!result?.ok) {
      console.log(
        `[FB-STUDIO] Facebook target #${target.id} did not start: ${result?.message || "unknown error"}`,
      );
    }
    return result;
  }

  async function sweepAutoFacebookTargets() {
    if (!streamTargetManager) return;
    const rows = await pool.query(
      `SELECT sd.*, row_to_json(c.*) AS channel_row
       FROM social_destinations sd
       JOIN channels c ON c.id = sd.channel_id
       WHERE sd.platform = 'facebook'
         AND sd.automation_mode = 'oauth'
         AND sd.oauth_account_id IS NOT NULL
         AND sd.enabled = TRUE
         AND sd.auto_start = TRUE
         AND COALESCE(sd.is_running, FALSE) = FALSE
         AND sd.platform_broadcast_id IS NULL
         AND c.is_live = TRUE`,
    );
    for (const row of rows.rows) {
      const { channel_row: channel, ...target } = row;
      const startedAt = channel?.live_started_at
        ? new Date(channel.live_started_at).getTime()
        : 0;
      // Leave the first ~30s to the normal on_publish auto-start.
      if (startedAt && Date.now() - startedAt < MIN_SESSION_AGE_MS) continue;
      const runtime = streamTargetManager.getRuntimeState?.(target.id);
      if (runtime?.worker_running) continue;
      await startFacebookTargetNow(
        target,
        channel,
        "Auto go-live safety net",
      ).catch((error) =>
        console.error(
          `[FB-STUDIO] safety-net start failed for #${target.id}:`,
          error.message,
        ),
      );
    }
  }

  const sweepTimer = setInterval(() => {
    sweepAutoFacebookTargets().catch((error) =>
      console.error("[FB-STUDIO] auto go-live sweep failed:", error.message),
    );
  }, SWEEP_MS);
  sweepTimer.unref?.();

  // ────────────────────────────────────────────────────────────
  // Meta platform callbacks (set these URLs in App settings → Basic and
  // Facebook Login for Business → Settings):
  //   Data Deletion Request URL: {API_PUBLIC_URL}/api/oauth/facebook/data-deletion
  //   Deauthorize callback URL:  {API_PUBLIC_URL}/api/oauth/facebook/deauthorize
  // ────────────────────────────────────────────────────────────

  function parseSignedRequest(signedRequest) {
    const secret = facebookGraph.getAppSecret();
    if (!signedRequest || !secret) return null;
    const [encodedSig, payload] = String(signedRequest).split(".", 2);
    if (!encodedSig || !payload) return null;
    const fromB64Url = (value) =>
      Buffer.from(value.replace(/-/g, "+").replace(/_/g, "/"), "base64");
    const expected = crypto
      .createHmac("sha256", secret)
      .update(payload)
      .digest();
    const actual = fromB64Url(encodedSig);
    if (
      actual.length !== expected.length ||
      !crypto.timingSafeEqual(actual, expected)
    )
      return null;
    try {
      const data = JSON.parse(fromB64Url(payload).toString("utf8"));
      return data?.algorithm?.toUpperCase() === "HMAC-SHA256" ? data : null;
    } catch {
      return null;
    }
  }

  // Removes every Facebook credential tied to this Facebook user: the
  // identity row and the Page rows that same NLM admin connected.
  async function removeFacebookUserData(facebookUserId) {
    const users = await pool.query(
      `SELECT id, organization_id, connected_by_admin_id
       FROM social_oauth_accounts
       WHERE platform = 'facebook_user' AND external_account_id = $1`,
      [String(facebookUserId)],
    );
    let removed = 0;
    for (const user of users.rows) {
      const accounts = await pool.query(
        `SELECT id FROM social_oauth_accounts
         WHERE organization_id = $1
           AND (id = $2 OR (platform = 'facebook' AND connected_by_admin_id = $3))`,
        [user.organization_id, user.id, user.connected_by_admin_id],
      );
      const ids = accounts.rows.map((row) => row.id);
      if (!ids.length) continue;

      const targets = await pool.query(
        `SELECT * FROM social_destinations WHERE oauth_account_id = ANY($1::int[])`,
        [ids],
      );
      for (const target of targets.rows) {
        if (
          streamTargetManager &&
          (target.is_running || target.platform_broadcast_id)
        ) {
          await streamTargetManager
            .stopTarget(target, { endPlatform: false })
            .catch(() => {});
        }
      }
      await pool.query(
        `UPDATE social_destinations
         SET oauth_account_id = NULL, enabled = FALSE, auto_start = FALSE,
             is_running = FALSE, ffmpeg_pid = NULL, status = 'stopped',
             active_destination_url = NULL, platform_broadcast_id = NULL,
             platform_stream_id = NULL, last_platform_broadcast_id = NULL,
             last_error = 'Facebook access was removed by the account owner',
             updated_at = NOW()
         WHERE oauth_account_id = ANY($1::int[])`,
        [ids],
      );
      const del = await pool.query(
        `DELETE FROM social_oauth_accounts WHERE id = ANY($1::int[])`,
        [ids],
      );
      removed += del.rowCount;
    }
    return removed;
  }

  const metaForm = express.urlencoded({ extended: false });

  app.post("/api/oauth/facebook/data-deletion", metaForm, async (req, res) => {
    const data = parseSignedRequest(req.body?.signed_request);
    if (!data?.user_id)
      return res.status(400).json({ error: "Invalid signed_request" });
    const confirmationCode = crypto.randomBytes(8).toString("hex");
    try {
      const removed = await removeFacebookUserData(data.user_id);
      console.log(
        `[FB-DATA-DELETION] ${confirmationCode}: removed ${removed} credential row(s)`,
      );
    } catch (error) {
      console.error("[FB-DATA-DELETION] failed:", error.message);
    }
    const base = String(process.env.API_PUBLIC_URL || "").replace(/\/$/, "");
    res.json({
      url: `${base}/api/oauth/facebook/data-deletion/status?code=${confirmationCode}`,
      confirmation_code: confirmationCode,
    });
  });

  app.get("/api/oauth/facebook/data-deletion/status", (req, res) => {
    const code = String(req.query.code || "")
      .replace(/[^a-f0-9]/gi, "")
      .slice(0, 32);
    res.send(`<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Data deletion status — NLM Streaming</title></head>
<body style="font-family: system-ui, sans-serif; max-width: 560px; margin: 4rem auto; padding: 0 1rem; line-height: 1.55;">
  <h1 style="font-size: 1.4rem;">Your Facebook data has been deleted</h1>
  <p>NLM Streaming removed the Facebook sign-in and Page access tokens linked to your account and stopped any Facebook broadcasts that used them.</p>
  <p>Confirmation code: <strong>${code || "unknown"}</strong></p>
  <p>Questions? Contact privacy@nolimitsmedia.com and include the confirmation code.</p>
</body></html>`);
  });

  app.post("/api/oauth/facebook/deauthorize", metaForm, async (req, res) => {
    const data = parseSignedRequest(req.body?.signed_request);
    if (!data?.user_id) return res.status(400).json({ ok: false });
    try {
      await removeFacebookUserData(data.user_id);
    } catch (error) {
      console.error("[FB-DEAUTHORIZE] failed:", error.message);
    }
    res.json({ ok: true });
  });
};
