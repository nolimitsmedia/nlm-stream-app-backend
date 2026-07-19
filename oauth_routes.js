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

const STATE_TTL = "10m"; // OAuth dialogs are usually completed within a minute or two

module.exports = function registerOAuthRoutes(app, pool, jwt, mw) {
  const {
    authenticateAdmin,
    resolveOrganizationForRequest,
    requireRole,
    requireOrganizationRole,
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
  function closePopupHtml({ ok, message, platform }) {
    return `<!DOCTYPE html>
<html><body style="font-family: system-ui; padding: 2rem; text-align: center;">
  <p>${ok ? "Connected!" : "Something went wrong"} ${message ? `— ${message}` : ""}</p>
  <p>This window will close automatically.</p>
  <script>
    if (window.opener) {
      window.opener.postMessage(
        { type: "social-oauth-result", ok: ${JSON.stringify(!!ok)}, platform: ${JSON.stringify(platform || "")} },
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
          `SELECT id, platform, external_account_id, external_account_name, created_at
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
        await pool.query(
          `DELETE FROM social_oauth_accounts WHERE id = $1 AND organization_id = $2`,
          [req.params.id, req.organization.id],
        );
        res.json({ ok: true, message: "Account disconnected" });
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
        res.json({ ok: true, authUrl: facebookGraph.getAuthUrl(state) });
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
      const { accessToken: longLivedUserToken } =
        await facebookGraph.getLongLivedUserToken(shortLivedToken);
      const pages = await facebookGraph.listUserPages(longLivedUserToken);

      if (pages.length === 0) {
        return res.send(
          closePopupHtml({
            ok: false,
            platform: "facebook",
            message:
              "No Pages found — you need to manage at least one Facebook Page",
          }),
        );
      }

      if (pages.length === 1) {
        await upsertFacebookAccount(
          claims.organizationId,
          claims.adminId,
          pages[0],
        );
        return res.send(closePopupHtml({ ok: true, platform: "facebook" }));
      }

      // Multiple Pages — ask which one before storing anything.
      const pickerState = signState({
        purpose: "fb_pick_page",
        adminId: claims.adminId,
        organizationId: claims.organizationId,
        channelId: claims.channelId,
        pages, // small list, fine to round-trip through a signed token
      });

      const optionsHtml = pages
        .map(
          (p, i) =>
            `<button name="pageIndex" value="${i}" style="display:block;width:100%;margin:6px 0;padding:10px;">${p.pageName}</button>`,
        )
        .join("");

      res.send(`<!DOCTYPE html>
<html><body style="font-family: system-ui; padding: 2rem;">
  <p>Which Page should this channel simulcast to?</p>
  <form method="POST" action="/api/oauth/facebook/select-page">
    <input type="hidden" name="state" value="${pickerState}" />
    ${optionsHtml}
  </form>
</body></html>`);
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

        await upsertFacebookAccount(
          claims.organizationId,
          claims.adminId,
          chosen,
        );
        res.send(closePopupHtml({ ok: true, platform: "facebook" }));
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

  async function upsertFacebookAccount(organizationId, adminId, page) {
    await pool.query(
      `INSERT INTO social_oauth_accounts
         (organization_id, platform, external_account_id, external_account_name, access_token, connected_by_admin_id)
       VALUES ($1, 'facebook', $2, $3, $4, $5)
       ON CONFLICT (organization_id, platform, external_account_id)
       DO UPDATE SET access_token = EXCLUDED.access_token,
                     external_account_name = EXCLUDED.external_account_name,
                     updated_at = now()`,
      [
        organizationId,
        page.pageId,
        page.pageName,
        page.pageAccessToken,
        adminId,
      ],
    );
  }

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

      await pool.query(
        `INSERT INTO social_oauth_accounts
           (organization_id, platform, external_account_id, external_account_name, access_token, refresh_token, token_expires_at, connected_by_admin_id)
         VALUES ($1, 'youtube', $2, $3, $4, $5, $6, $7)
         ON CONFLICT (organization_id, platform, external_account_id)
         DO UPDATE SET access_token = EXCLUDED.access_token,
                       refresh_token = EXCLUDED.refresh_token,
                       token_expires_at = EXCLUDED.token_expires_at,
                       external_account_name = EXCLUDED.external_account_name,
                       updated_at = now()`,
        [
          claims.organizationId,
          channelInfo.channelId,
          channelInfo.channelTitle,
          tokens.access_token,
          tokens.refresh_token,
          tokens.expiry_date ? new Date(tokens.expiry_date) : null,
          claims.adminId,
        ],
      );

      res.send(closePopupHtml({ ok: true, platform: "youtube" }));
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
  // Attach a connected account to a channel's social destination
  // ────────────────────────────────────────────────────────────

  app.post(
    "/api/channels/:channelId/social-destinations/:id/link-oauth",
    authenticateAdmin,
    resolveOrganizationForRequest,
    requireRole("super_admin", "admin", "operator"),
    requireOrganizationRole("owner", "admin"),
    async (req, res) => {
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
        if (!accountResult.rows[0]) {
          return res
            .status(404)
            .json({ ok: false, message: "Connected account not found" });
        }
        const result = await pool.query(
          `UPDATE social_destinations
           SET oauth_account_id = $1, automation_mode = 'oauth', updated_at = now()
           WHERE id = $2 AND channel_id = $3
           RETURNING *`,
          [oauthAccountId, req.params.id, channel.id],
        );
        if (!result.rows[0]) {
          return res
            .status(404)
            .json({ ok: false, message: "Social destination not found" });
        }
        res.json({ ok: true, destination: result.rows[0] });
      } catch (error) {
        console.error("Link OAuth Account Error:", error);
        res
          .status(500)
          .json({ ok: false, message: "Failed to link connected account" });
      }
    },
  );
};
