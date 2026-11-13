// oauth_routes.js
//
// Handles the *connection* side of OAuth automation: letting an org admin
// link a Facebook Page or YouTube channel to a stream destination.
//
// Deliberately does NOT touch the ffmpeg/socialProcesses logic that already
// lives inline in server.js next to the existing manual start/stop routes —
// the new "go-live"/"end-live" handlers that actually use these connected
// accounts belong there instead, so they can share the same socialProcesses
// Map. This file only manages: init -> platform OAuth dialog -> callback ->
// (Facebook only) page picker -> stored social_oauth_accounts row.
//
// Mounted from server.js with:
//   require("./oauth_routes")(app, pool, jwt, {
//     authenticateAdmin, resolveOrganizationForRequest,
//     requireRole, requireOrganizationRole,
//   });

const express = require("express");
const facebookGraph = require("./facebook_graph_service");
const youtubeApi = require("./youtube_api_service");
const {
  encryptOAuthToken,
  decryptOAuthToken,
} = require("./social_oauth_schema");

const STATE_TTL = "10m"; // OAuth dialogs are usually completed within a minute or two

module.exports = function registerOAuthRoutes(app, pool, jwt, mw) {
  const {
    authenticateAdmin,
    resolveOrganizationForRequest,
    requireRole,
    requireOrganizationRole,
    streamTargetManager = null,
  } = mw;

  function signState(payload) {
    return jwt.sign(payload, process.env.JWT_SECRET, { expiresIn: STATE_TTL });
  }

  function verifyState(token) {
    return jwt.verify(token, process.env.JWT_SECRET);
  }

  // Small HTML helper for the popup window's final step — posts a message to
  // the opener (the main app tab) and closes itself. The opener listens for
  // this in the React destinations component to refresh its list.
  function closePopupHtml({
    ok,
    message,
    platform,
    accountId = null,
    identityOnly = false,
    needsPageSelection = false,
  }) {
    return `<!DOCTYPE html>
<html><body style="font-family: system-ui; padding: 2rem; text-align: center;">
  <p>${ok ? "Facebook connected." : "Facebook connection was not completed"} ${message ? `— ${String(message).replace(/[&<>"']/g, (ch) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[ch])}` : ""}</p>
  <p>${ok ? "Return to NLM Streaming to choose your Page. " : ""}This window closes automatically.</p>
  <script>
    if (window.opener) {
      window.opener.postMessage(
        { type: "social-oauth-result", ok: ${JSON.stringify(!!ok)}, platform: ${JSON.stringify(platform || "")}, accountId: ${JSON.stringify(accountId)}, identityOnly: ${JSON.stringify(identityOnly)}, needsPageSelection: ${JSON.stringify(!!needsPageSelection)} },
        "*"
      );
    }
    setTimeout(() => window.close(), ${ok ? 1200 : 4000});
  </script>
</body></html>`;
  }

  async function getOwnedChannel(channelId, organizationId) {
    const result = await pool.query(
      `SELECT * FROM channels WHERE id = $1 AND organization_id = $2`,
      [channelId, organizationId],
    );
    return result.rows[0] || null;
  }

  async function ensureOAuthTargetForChannel({
    channelId,
    organizationId,
    account,
  }) {
    if (!account || !["facebook", "youtube"].includes(account.platform)) {
      throw new Error("Unsupported OAuth platform");
    }

    const channel = await getOwnedChannel(channelId, organizationId);
    if (!channel) throw new Error("Channel not found for OAuth connection");

    const protocol = account.platform === "facebook" ? "rtmps" : "rtmp";
    const result = await pool.query(
      `INSERT INTO social_destinations
         (channel_id, platform, stream_key, name, target_type, destination_url,
          protocol, automation_mode, oauth_account_id, enabled, auto_start,
          auto_reconnect, status, updated_at)
       VALUES ($1,$2,'oauth-managed',$3,$2,NULL,$4,'oauth',$5,TRUE,TRUE,TRUE,'stopped',NOW())
       ON CONFLICT (channel_id, platform)
       DO UPDATE SET
         name = EXCLUDED.name,
         target_type = EXCLUDED.target_type,
         destination_url = NULL,
         stream_key = 'oauth-managed',
         protocol = EXCLUDED.protocol,
         automation_mode = 'oauth',
         oauth_account_id = EXCLUDED.oauth_account_id,
         enabled = TRUE,
         auto_start = TRUE,
         auto_reconnect = TRUE,
         last_error = NULL,
         updated_at = NOW()
       RETURNING *`,
      [
        channel.id,
        account.platform,
        account.external_account_name ||
          (account.platform === "youtube" ? "YouTube" : "Facebook"),
        protocol,
        account.id,
      ],
    );

    return result.rows[0];
  }

  // ────────────────────────────────────────────────────────────
  // List / detach connected accounts for an org
  // ────────────────────────────────────────────────────────────

  app.get(
    "/api/organizations/current/oauth-accounts",
    authenticateAdmin,
    resolveOrganizationForRequest,
    async (req, res) => {
      try {
        const result = await pool.query(
          `SELECT id, platform, external_account_id, external_account_name,
                  connection_status, token_last_validated_at, token_last_error,
                  reconnect_required_at, created_at
           FROM social_oauth_accounts WHERE organization_id = $1 ORDER BY platform`,
          [req.organization.id],
        );
        res.json({ ok: true, accounts: result.rows });
      } catch (error) {
        console.error("List OAuth Accounts Error:", error);
        res
          .status(500)
          .json({ ok: false, message: "Failed to list connected accounts" });
      }
    },
  );

  app.delete(
    "/api/organizations/current/oauth-accounts/:id",
    authenticateAdmin,
    resolveOrganizationForRequest,
    requireRole("super_admin", "admin", "operator"),
    requireOrganizationRole("owner", "admin"),
    async (req, res) => {
      try {
        const accountResult = await pool.query(
          `SELECT * FROM social_oauth_accounts WHERE id = $1 AND organization_id = $2`,
          [req.params.id, req.organization.id],
        );
        const account = accountResult.rows[0];
        if (!account) {
          return res
            .status(404)
            .json({ ok: false, message: "Connected account not found" });
        }

        const targetsResult = await pool.query(
          `SELECT sd.*
           FROM social_destinations sd
           JOIN channels c ON c.id = sd.channel_id
           WHERE sd.oauth_account_id = $1 AND c.organization_id = $2`,
          [account.id, req.organization.id],
        );

        // Stop active workers/platform broadcasts before severing the account
        // relationship so endPlatformBroadcast still has access to credentials.
        if (streamTargetManager) {
          for (const target of targetsResult.rows) {
            if (
              target.is_running ||
              target.ffmpeg_pid ||
              target.platform_broadcast_id ||
              streamTargetManager.getRuntimeState?.(target.id)?.worker_running
            ) {
              await streamTargetManager
                .stopTarget(target)
                .catch((stopError) => {
                  console.error(
                    `[SOCIAL-OAUTH] Failed to stop target #${target.id} before disconnect:`,
                    stopError.message,
                  );
                });
            }
          }
        }

        await pool.query(
          `UPDATE social_destinations
           SET oauth_account_id = NULL,
               enabled = FALSE,
               auto_start = FALSE,
               auto_reconnect = FALSE,
               is_running = FALSE,
               ffmpeg_pid = NULL,
               status = 'stopped',
               current_bitrate_kbps = 0,
               active_destination_url = NULL,
               platform_broadcast_id = NULL,
               platform_stream_id = NULL,
               last_error = 'Connected account was disconnected',
               updated_at = NOW()
           WHERE oauth_account_id = $1`,
          [account.id],
        );

        await pool.query(
          `DELETE FROM social_oauth_accounts WHERE id = $1 AND organization_id = $2`,
          [account.id, req.organization.id],
        );
        res.json({
          ok: true,
          message: "Account disconnected and linked targets disabled",
        });
      } catch (error) {
        console.error("Delete OAuth Account Error:", error);
        res
          .status(500)
          .json({ ok: false, message: "Failed to disconnect account" });
      }
    },
  );

  // ────────────────────────────────────────────────────────────
  // Facebook
  // ────────────────────────────────────────────────────────────

  app.post(
    "/api/channels/:channelId/oauth/facebook/init",
    authenticateAdmin,
    resolveOrganizationForRequest,
    requireRole("super_admin", "admin", "operator"),
    requireOrganizationRole("owner", "admin"),
    async (req, res) => {
      try {
        if (!facebookGraph.isConfigured()) {
          return res.status(503).json({
            ok: false,
            message:
              "Facebook integration is not configured on this server yet",
          });
        }
        const channel = await getOwnedChannel(
          req.params.channelId,
          req.organization.id,
        );
        if (!channel) {
          return res
            .status(404)
            .json({ ok: false, message: "Channel not found" });
        }
        const state = signState({
          purpose: "fb_connect",
          adminId: req.admin.id,
          organizationId: req.organization.id,
          channelId: channel.id,
        });
        res.json({
          ok: true,
          authUrl: facebookGraph.getAuthUrl(state, {
            rerequest: Boolean(req.body?.rerequest || req.query?.rerequest),
          }),
        });
      } catch (error) {
        console.error("Facebook OAuth Init Error:", error);
        res
          .status(500)
          .json({ ok: false, message: "Failed to start Facebook connection" });
      }
    },
  );

  // Public — no authenticateAdmin here. Identity comes from the signed `state`.
  app.get("/api/oauth/facebook/callback", async (req, res) => {
    const { code, state, error: fbError } = req.query;
    try {
      if (fbError) {
        return res.send(
          closePopupHtml({
            ok: false,
            message: String(fbError),
            platform: "facebook",
          }),
        );
      }
      const claims = verifyState(state);
      if (claims.purpose !== "fb_connect") throw new Error("Invalid state");

      const shortLivedToken =
        await facebookGraph.exchangeCodeForUserToken(code);
      const { accessToken: longLivedUserToken, expiresInSeconds } =
        await facebookGraph.getLongLivedUserToken(shortLivedToken);
      const user = await facebookGraph.getCurrentUser(longLivedUserToken);
      const facebookUser = await upsertFacebookUser(
        claims.organizationId,
        claims.adminId,
        user,
        longLivedUserToken,
        expiresInSeconds,
      );
      // Page selection happens inside NLM Streaming (Live Studio), not in
      // this popup. That keeps the step visible to the person (and to Meta
      // App Review) and lets them switch Pages later without logging in again.
      return res.send(
        closePopupHtml({
          ok: true,
          platform: "facebook",
          accountId: facebookUser?.id || null,
          identityOnly: true,
          needsPageSelection: true,
        }),
      );
    } catch (error) {
      console.error("Facebook OAuth Callback Error:", error);
      res.send(
        closePopupHtml({
          ok: false,
          message: "Connection failed",
          platform: "facebook",
        }),
      );
    }
  });

  app.post(
    "/api/oauth/facebook/select-page",
    express.urlencoded({ extended: false }),
    async (req, res) => {
      try {
        const claims = verifyState(req.body.state);
        if (claims.purpose !== "fb_pick_page") throw new Error("Invalid state");
        const chosen = claims.pages[Number(req.body.pageIndex)];
        if (!chosen) throw new Error("Invalid selection");
        chosen.pageAccessToken = decryptOAuthToken(chosen.pageAccessToken);

        const account = await upsertFacebookAccount(
          claims.organizationId,
          claims.adminId,
          chosen,
        );
        await ensureOAuthTargetForChannel({
          channelId: claims.channelId,
          organizationId: claims.organizationId,
          account,
        });
        res.send(
          closePopupHtml({
            ok: true,
            platform: "facebook",
            accountId: account.id,
          }),
        );
      } catch (error) {
        console.error("Facebook Page Selection Error:", error);
        res.send(
          closePopupHtml({
            ok: false,
            message: "Selection failed",
            platform: "facebook",
          }),
        );
      }
    },
  );

  // Personal account is identity-only: never attach it to a stream target.
  async function upsertFacebookUser(
    organizationId,
    adminId,
    user,
    userToken,
    expiresInSeconds,
  ) {
    const expiresAt = new Date(
      Date.now() + Math.max(0, Number(expiresInSeconds) || 0) * 1000,
    );
    const result = await pool.query(
      `INSERT INTO social_oauth_accounts
         (organization_id, platform, external_account_id, external_account_name,
          access_token, token_expires_at, connected_by_admin_id, token_encryption_version,
          connection_status, token_last_validated_at, token_last_error, reconnect_required_at)
       VALUES ($1, 'facebook_user', $2, $3, $4, $5, $6, 1, 'connected', NOW(), NULL, NULL)
       ON CONFLICT (organization_id, platform, external_account_id)
       DO UPDATE SET external_account_name=EXCLUDED.external_account_name,
         access_token=EXCLUDED.access_token, token_expires_at=EXCLUDED.token_expires_at,
         connected_by_admin_id=EXCLUDED.connected_by_admin_id,
         token_encryption_version=1, connection_status='connected',
         token_last_validated_at=NOW(), token_last_error=NULL,
         reconnect_required_at=NULL, updated_at=NOW()
       RETURNING id`,
      [
        organizationId,
        user.id,
        user.name,
        encryptOAuthToken(userToken),
        expiresAt,
        adminId,
      ],
    );
    return result.rows[0];
  }

  async function upsertFacebookAccount(organizationId, adminId, page) {
    const encryptedAccessToken = encryptOAuthToken(page.pageAccessToken);
    const result = await pool.query(
      `INSERT INTO social_oauth_accounts
         (organization_id, platform, external_account_id, external_account_name,
          access_token, connected_by_admin_id, token_encryption_version,
          connection_status, token_last_validated_at, token_last_error,
          reconnect_required_at)
       VALUES ($1, 'facebook', $2, $3, $4, $5, 1, 'connected', NOW(), NULL, NULL)
       ON CONFLICT (organization_id, platform, external_account_id)
       DO UPDATE SET access_token = EXCLUDED.access_token,
                     external_account_name = EXCLUDED.external_account_name,
                     connected_by_admin_id = EXCLUDED.connected_by_admin_id,
                     token_encryption_version = 1,
                     connection_status = 'connected',
                     token_last_validated_at = NOW(),
                     token_last_error = NULL,
                     reconnect_required_at = NULL,
                     updated_at = NOW()
       RETURNING *`,
      [
        organizationId,
        page.pageId,
        page.pageName,
        encryptedAccessToken,
        adminId,
      ],
    );
    return result.rows[0];
  }

  // ────────────────────────────────────────────────────────────
  // Facebook in-app Page picker (pages_show_list)
  // The studio lists the Pages the signed-in person granted, then stores the
  // chosen Page's token and links it to this channel's Facebook target.
  // ────────────────────────────────────────────────────────────

  async function loadFacebookUser(organizationId, adminId) {
    const result = await pool.query(
      `SELECT * FROM social_oauth_accounts
       WHERE organization_id = $1 AND platform = 'facebook_user'
       ORDER BY (connected_by_admin_id = $2) DESC, updated_at DESC NULLS LAST, id DESC
       LIMIT 1`,
      [organizationId, adminId],
    );
    const row = result.rows[0];
    if (!row) return null;
    return { ...row, access_token: decryptOAuthToken(row.access_token) };
  }

  async function markFacebookUserReconnect(accountId, error) {
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

  app.get(
    "/api/channels/:channelId/facebook/pages",
    authenticateAdmin,
    resolveOrganizationForRequest,
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

        const user = await loadFacebookUser(req.organization.id, req.admin.id);
        if (!user) {
          return res.status(409).json({
            ok: false,
            code: "facebook_not_connected",
            message: "Continue with Facebook first.",
          });
        }

        let pages;
        try {
          pages = await facebookGraph.listUserPages(user.access_token);
        } catch (error) {
          if (facebookGraph.isReconnectError(error)) {
            await markFacebookUserReconnect(user.id, error);
            return res.status(409).json({
              ok: false,
              code: "facebook_reconnect_required",
              message:
                "Your Facebook sign-in expired or access was removed. Continue with Facebook again.",
            });
          }
          throw error;
        }

        const selected = await pool.query(
          `SELECT a.external_account_id
           FROM social_destinations sd
           JOIN social_oauth_accounts a ON a.id = sd.oauth_account_id
           WHERE sd.channel_id = $1 AND sd.platform = 'facebook'
             AND sd.automation_mode = 'oauth'
           LIMIT 1`,
          [channel.id],
        );
        const selectedPageId = selected.rows[0]?.external_account_id || null;

        res.json({
          ok: true,
          signedInAs: user.external_account_name,
          selectedPageId,
          // Never send Page tokens to the browser.
          pages: pages.map(({ pageAccessToken, tasks, ...page }) => ({
            ...page,
            selected: String(page.pageId) === String(selectedPageId),
          })),
        });
      } catch (error) {
        console.error("List Facebook Pages Error:", error);
        res
          .status(502)
          .json({ ok: false, message: "Could not load your Facebook Pages" });
      }
    },
  );

  app.post(
    "/api/channels/:channelId/facebook/select-page",
    authenticateAdmin,
    resolveOrganizationForRequest,
    requireRole("super_admin", "admin", "operator"),
    requireOrganizationRole("owner", "admin"),
    async (req, res) => {
      try {
        const pageId = String(req.body?.pageId || "").trim();
        if (!pageId)
          return res
            .status(400)
            .json({ ok: false, message: "Choose a Page first" });

        const channel = await getOwnedChannel(
          req.params.channelId,
          req.organization.id,
        );
        if (!channel)
          return res
            .status(404)
            .json({ ok: false, message: "Channel not found" });

        const live = await pool.query(
          `SELECT id FROM social_destinations
           WHERE channel_id = $1 AND platform = 'facebook'
             AND (is_running = TRUE OR platform_broadcast_id IS NOT NULL)
           LIMIT 1`,
          [channel.id],
        );
        if (live.rows[0]) {
          return res.status(409).json({
            ok: false,
            code: "facebook_live_active",
            message: "End the current Facebook live before changing Page.",
          });
        }

        const user = await loadFacebookUser(req.organization.id, req.admin.id);
        if (!user)
          return res.status(409).json({
            ok: false,
            code: "facebook_not_connected",
            message: "Continue with Facebook first.",
          });

        const pages = await facebookGraph.listUserPages(user.access_token);
        const chosen = pages.find((page) => String(page.pageId) === pageId);
        if (!chosen) {
          return res.status(404).json({
            ok: false,
            message:
              "That Page was not shared with NLM Streaming. Use Edit Page access to add it.",
          });
        }
        if (!chosen.canGoLive) {
          return res.status(403).json({
            ok: false,
            message:
              "Your role on this Page cannot create content. Ask a Page admin for full control or content access.",
          });
        }

        const account = await upsertFacebookAccount(
          req.organization.id,
          req.admin.id,
          chosen,
        );
        const target = await ensureOAuthTargetForChannel({
          channelId: channel.id,
          organizationId: req.organization.id,
          account,
        });

        res.json({
          ok: true,
          message: `${chosen.pageName} selected`,
          page: {
            accountId: account.id,
            pageId: chosen.pageId,
            pageName: chosen.pageName,
            pictureUrl: chosen.pictureUrl,
            category: chosen.category,
          },
          targetId: target.id,
        });
      } catch (error) {
        console.error("Select Facebook Page Error:", error);
        res
          .status(500)
          .json({ ok: false, message: "Could not select that Page" });
      }
    },
  );

  // ────────────────────────────────────────────────────────────
  // YouTube
  // ────────────────────────────────────────────────────────────

  app.post(
    "/api/channels/:channelId/oauth/youtube/init",
    authenticateAdmin,
    resolveOrganizationForRequest,
    requireRole("super_admin", "admin", "operator"),
    requireOrganizationRole("owner", "admin"),
    async (req, res) => {
      try {
        if (!youtubeApi.isConfigured()) {
          return res.status(503).json({
            ok: false,
            message: "YouTube integration is not configured on this server yet",
          });
        }
        const channel = await getOwnedChannel(
          req.params.channelId,
          req.organization.id,
        );
        if (!channel) {
          return res
            .status(404)
            .json({ ok: false, message: "Channel not found" });
        }
        const state = signState({
          purpose: "yt_connect",
          adminId: req.admin.id,
          organizationId: req.organization.id,
          channelId: channel.id,
        });
        res.json({ ok: true, authUrl: youtubeApi.getAuthUrl(state) });
      } catch (error) {
        console.error("YouTube OAuth Init Error:", error);
        res
          .status(500)
          .json({ ok: false, message: "Failed to start YouTube connection" });
      }
    },
  );

  app.get("/api/oauth/youtube/callback", async (req, res) => {
    const { code, state, error: googleError } = req.query;
    try {
      if (googleError) {
        return res.send(
          closePopupHtml({
            ok: false,
            message: String(googleError),
            platform: "youtube",
          }),
        );
      }
      const claims = verifyState(state);
      if (claims.purpose !== "yt_connect") throw new Error("Invalid state");

      const tokens = await youtubeApi.exchangeCodeForTokens(code);
      if (!tokens.refresh_token) {
        // Happens if the user had already granted consent before and Google
        // didn't re-issue a refresh_token. prompt: "consent" in getAuthUrl
        // is meant to prevent this, but it's worth guarding explicitly.
        return res.send(
          closePopupHtml({
            ok: false,
            platform: "youtube",
            message:
              "Google didn't return a refresh token — try disconnecting the app in your Google account settings and reconnecting",
          }),
        );
      }

      const oauth2Client = youtubeApi.clientFromTokens({
        accessToken: tokens.access_token,
        refreshToken: tokens.refresh_token,
      });
      const channelInfo = await youtubeApi.getMyChannel(oauth2Client);
      if (!channelInfo) {
        return res.send(
          closePopupHtml({
            ok: false,
            platform: "youtube",
            message: "No YouTube channel found on this account",
          }),
        );
      }

      // Validate the durable refresh token independently. A transient access
      // token is not sufficient proof of where future broadcasts will go.
      const refreshed = await youtubeApi.refreshAccessToken(
        tokens.refresh_token,
      );
      if (!refreshed.access_token) {
        throw new Error(
          "YouTube refresh token did not provide an access token",
        );
      }
      const refreshedClient = youtubeApi.clientFromTokens({
        accessToken: refreshed.access_token,
        refreshToken: tokens.refresh_token,
      });
      const refreshedChannel = await youtubeApi.getMyChannel(refreshedClient);
      if (
        !refreshedChannel ||
        refreshedChannel.channelId !== channelInfo.channelId
      ) {
        return res.send(
          closePopupHtml({
            ok: false,
            platform: "youtube",
            message:
              "YouTube channel verification failed. Please select the intended YouTube channel during Google authorization and reconnect.",
          }),
        );
      }

      const accountResult = await pool.query(
        `INSERT INTO social_oauth_accounts
           (organization_id, platform, external_account_id, external_account_name,
            access_token, refresh_token, token_expires_at, connected_by_admin_id,
            token_encryption_version, connection_status, token_last_validated_at,
            token_last_error, reconnect_required_at)
         VALUES ($1, 'youtube', $2, $3, $4, $5, $6, $7, 1, 'connected', NOW(), NULL, NULL)
         ON CONFLICT (organization_id, platform, external_account_id)
         DO UPDATE SET access_token = EXCLUDED.access_token,
                       refresh_token = EXCLUDED.refresh_token,
                       token_expires_at = EXCLUDED.token_expires_at,
                       external_account_name = EXCLUDED.external_account_name,
                       connected_by_admin_id = EXCLUDED.connected_by_admin_id,
                       token_encryption_version = 1,
                       connection_status = 'connected',
                       token_last_validated_at = NOW(),
                       token_last_error = NULL,
                       reconnect_required_at = NULL,
                       updated_at = NOW()
         RETURNING *`,
        [
          claims.organizationId,
          channelInfo.channelId,
          channelInfo.channelTitle,
          encryptOAuthToken(refreshed.access_token),
          encryptOAuthToken(tokens.refresh_token),
          refreshed.expiry_date ? new Date(refreshed.expiry_date) : null,
          claims.adminId,
        ],
      );

      await ensureOAuthTargetForChannel({
        channelId: claims.channelId,
        organizationId: claims.organizationId,
        account: accountResult.rows[0],
      });

      res.send(
        closePopupHtml({
          ok: true,
          platform: "youtube",
          accountId: accountResult.rows[0].id,
        }),
      );
    } catch (error) {
      console.error("YouTube OAuth Callback Error:", error);
      res.send(
        closePopupHtml({
          ok: false,
          message: "Connection failed",
          platform: "youtube",
        }),
      );
    }
  });

  // ────────────────────────────────────────────────────────────
  // Attach a connected account to a Stream Target.
  // The legacy social-destinations route is kept as an alias so a frontend
  // and backend can be deployed in either order without breaking OAuth.
  // ────────────────────────────────────────────────────────────

  const linkOAuthTarget = async (req, res) => {
    try {
      const { oauthAccountId } = req.body;
      const channel = await getOwnedChannel(
        req.params.channelId,
        req.organization.id,
      );
      if (!channel) {
        return res
          .status(404)
          .json({ ok: false, message: "Channel not found" });
      }
      const accountResult = await pool.query(
        `SELECT * FROM social_oauth_accounts WHERE id = $1 AND organization_id = $2`,
        [oauthAccountId, req.organization.id],
      );
      const account = accountResult.rows[0];
      if (!account) {
        return res
          .status(404)
          .json({ ok: false, message: "Connected account not found" });
      }
      if (account.platform === "facebook_user") {
        return res.status(400).json({
          ok: false,
          message:
            "Personal Facebook accounts are identity-only and cannot be linked to OAuth livestream targets",
        });
      }

      const targetResult = await pool.query(
        `SELECT * FROM social_destinations WHERE id = $1 AND channel_id = $2`,
        [req.params.id, channel.id],
      );
      const target = targetResult.rows[0];
      if (!target) {
        return res
          .status(404)
          .json({ ok: false, message: "Stream target not found" });
      }
      const targetPlatform = String(
        target.target_type || target.platform || "",
      ).toLowerCase();
      if (targetPlatform !== String(account.platform || "").toLowerCase()) {
        return res.status(400).json({
          ok: false,
          message: `A ${account.platform} account can only be linked to a ${account.platform} target`,
        });
      }

      const result = await pool.query(
        `UPDATE social_destinations
         SET oauth_account_id = $1,
             automation_mode = 'oauth',
             target_type = COALESCE(target_type, $2),
             name = COALESCE(NULLIF(name, ''), $3),
             enabled = TRUE,
             auto_start = TRUE,
             auto_reconnect = TRUE,
             updated_at = now()
         WHERE id = $4 AND channel_id = $5
         RETURNING *`,
        [
          oauthAccountId,
          account.platform,
          account.external_account_name || account.platform,
          req.params.id,
          channel.id,
        ],
      );
      if (!result.rows[0]) {
        return res
          .status(404)
          .json({ ok: false, message: "Stream target not found" });
      }
      res.json({
        ok: true,
        target: result.rows[0],
        destination: result.rows[0],
      });
    } catch (error) {
      console.error("Link OAuth Account Error:", error);
      res
        .status(500)
        .json({ ok: false, message: "Failed to link connected account" });
    }
  };

  app.post(
    "/api/channels/:channelId/stream-targets/:id/link-oauth",
    authenticateAdmin,
    resolveOrganizationForRequest,
    requireRole("super_admin", "admin", "operator"),
    requireOrganizationRole("owner", "admin"),
    linkOAuthTarget,
  );

  app.post(
    "/api/channels/:channelId/social-destinations/:id/link-oauth",
    authenticateAdmin,
    resolveOrganizationForRequest,
    requireRole("super_admin", "admin", "operator"),
    requireOrganizationRole("owner", "admin"),
    linkOAuthTarget,
  );

  // Facebook Live Studio: Page content, live status, go-live, data deletion.
  require("./facebook_page_routes")(app, pool, {
    authenticateAdmin,
    resolveOrganizationForRequest,
    requireRole,
    requireOrganizationRole,
    streamTargetManager,
  });

  // Stream health alerts, encoder pre-flight and outbound webhooks.
  require("./platform_routes")(app, pool, {
    authenticateAdmin,
    resolveOrganizationForRequest,
    requireRole,
    requireOrganizationRole,
  });
};
