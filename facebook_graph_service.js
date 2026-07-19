// facebook_graph_service.js
//
// Wraps the Meta Graph API calls needed to automate "Go Live" on Facebook Pages.
// No SDK dependency — plain fetch, matching the rest of this codebase's style.
//
// Docs this maps to:
//   - OAuth dialog:            https://developers.facebook.com/docs/facebook-login/manually-build-a-login-flow
//   - Long-lived tokens:       https://developers.facebook.com/docs/facebook-login/access-tokens/refreshing
//   - Live Video API:          https://developers.facebook.com/docs/video-api/guides/live
//
// Required permissions (submitted for App Review): pages_show_list,
// pages_read_engagement, pages_manage_posts, publish_video

const GRAPH_VERSION = "v21.0";
const GRAPH_BASE = `https://graph.facebook.com/${GRAPH_VERSION}`;

const FACEBOOK_APP_ID = process.env.FACEBOOK_APP_ID || "";
const FACEBOOK_APP_SECRET = process.env.FACEBOOK_APP_SECRET || "";
const FACEBOOK_REDIRECT_URI =
  process.env.FACEBOOK_REDIRECT_URI ||
  `${process.env.API_PUBLIC_URL || ""}/api/oauth/facebook/callback`;

const FACEBOOK_SCOPES = [
  "pages_show_list",
  "pages_read_engagement",
  "pages_manage_posts",
  "publish_video",
].join(",");

function isConfigured() {
  return Boolean(FACEBOOK_APP_ID && FACEBOOK_APP_SECRET);
}

// Step 1: build the URL the browser popup is sent to.
function getAuthUrl(state) {
  const params = new URLSearchParams({
    client_id: FACEBOOK_APP_ID,
    redirect_uri: FACEBOOK_REDIRECT_URI,
    state,
    scope: FACEBOOK_SCOPES,
    response_type: "code",
  });
  return `https://www.facebook.com/${GRAPH_VERSION}/dialog/oauth?${params.toString()}`;
}

async function graphFetch(pathAndQuery, options = {}) {
  const res = await fetch(`${GRAPH_BASE}${pathAndQuery}`, {
    ...options,
    signal: AbortSignal.timeout(15000),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok || data.error) {
    const message = data.error?.message || `Graph API error (${res.status})`;
    const err = new Error(message);
    err.graphError = data.error;
    err.status = res.status;
    throw err;
  }
  return data;
}

// Step 2: exchange the ?code= from the callback for a short-lived user token.
async function exchangeCodeForUserToken(code) {
  const params = new URLSearchParams({
    client_id: FACEBOOK_APP_ID,
    client_secret: FACEBOOK_APP_SECRET,
    redirect_uri: FACEBOOK_REDIRECT_URI,
    code,
  });
  const data = await graphFetch(`/oauth/access_token?${params.toString()}`);
  return data.access_token; // short-lived, ~1-2 hours
}

// Step 3: swap the short-lived user token for a long-lived one (~60 days).
async function getLongLivedUserToken(shortLivedToken) {
  const params = new URLSearchParams({
    grant_type: "fb_exchange_token",
    client_id: FACEBOOK_APP_ID,
    client_secret: FACEBOOK_APP_SECRET,
    fb_exchange_token: shortLivedToken,
  });
  const data = await graphFetch(`/oauth/access_token?${params.toString()}`);
  return {
    accessToken: data.access_token,
    expiresInSeconds: data.expires_in || 5184000, // ~60 days if not returned
  };
}

// Step 4: list the Pages this user manages. Page tokens returned here are
// already long-lived as long as the parent user token is long-lived.
async function listUserPages(longLivedUserToken) {
  const params = new URLSearchParams({
    access_token: longLivedUserToken,
    fields: "id,name,access_token,picture",
  });
  const data = await graphFetch(`/me/accounts?${params.toString()}`);
  return (data.data || []).map((page) => ({
    pageId: page.id,
    pageName: page.name,
    pageAccessToken: page.access_token,
    pictureUrl: page.picture?.data?.url || null,
  }));
}

// Creates a live video object on the Page and returns the RTMP ingest details.
// This is the API call that replaces the client manually clicking "Go Live"
// and copy-pasting a stream key into our app.
async function createLiveVideo({
  pageId,
  pageAccessToken,
  title,
  description,
}) {
  const data = await graphFetch(`/${pageId}/live_videos`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      access_token: pageAccessToken,
      title: title || "Live Stream",
      description: description || "",
      status: "LIVE_NOW",
    }),
  });
  // data: { id, stream_url, secure_stream_url }
  // secure_stream_url is rtmps://...  and already contains the per-broadcast stream key.
  return {
    liveVideoId: data.id,
    rtmpUrl: data.secure_stream_url || data.stream_url,
  };
}

// Ends the broadcast on Facebook's side (separate from killing our ffmpeg process —
// both need to happen, since Facebook will otherwise leave the video "live" indefinitely).
async function endLiveVideo({ liveVideoId, pageAccessToken }) {
  return graphFetch(`/${liveVideoId}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      access_token: pageAccessToken,
      end_live_video: true,
    }),
  });
}

// Page tokens derived from a long-lived user token don't expire on their own
// schedule the way user tokens do, but Meta can still invalidate them (password
// change, permission revocation, app review changes). This is a cheap way to
// check validity before relying on one during a scheduled stream.
async function debugToken(inputToken) {
  const appToken = `${FACEBOOK_APP_ID}|${FACEBOOK_APP_SECRET}`;
  const params = new URLSearchParams({
    input_token: inputToken,
    access_token: appToken,
  });
  const data = await graphFetch(`/debug_token?${params.toString()}`);
  return data.data; // { is_valid, expires_at, scopes, ... }
}

module.exports = {
  isConfigured,
  getAuthUrl,
  exchangeCodeForUserToken,
  getLongLivedUserToken,
  listUserPages,
  createLiveVideo,
  endLiveVideo,
  debugToken,
};
