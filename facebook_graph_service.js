// facebook_graph_service.js
//
// Wraps the Meta Graph API calls used by NLM Streaming's Facebook Live Studio.
// No SDK dependency — plain fetch, matching the rest of this codebase's style.
//
// Docs this maps to:
//   - Facebook Login for Business: https://developers.facebook.com/docs/facebook-login/facebook-login-for-business
//   - Long-lived tokens:           https://developers.facebook.com/docs/facebook-login/guides/access-tokens/get-long-lived
//   - Live Video API:              https://developers.facebook.com/docs/live-video-api
//   - Page posts / videos:         https://developers.facebook.com/docs/pages-api
//
// Permission -> where it is used (keep this in sync with the App Review notes):
//   pages_show_list        listUserPages()            Page picker in Live Studio
//   pages_read_engagement  getPageProfile(),          Page header (followers, category),
//                          getPagePosts(),            "Recent posts" tab (reactions/comments),
//                          getPageVideos(),           "Videos" tab,
//                          getPageLiveVideos(),       "Live videos" tab,
//                          getLiveVideoStatus()       live indicator + viewer count
//   pages_manage_posts     createLiveVideo()          the live broadcast is published as a Page post
//   publish_video          createLiveVideo(),         publishes the live video to the Page
//                          endLiveVideo()
//   Live Video API         createLiveVideo(), endLiveVideo(), getLiveVideoStatus()

const GRAPH_VERSION = process.env.FACEBOOK_GRAPH_VERSION || "v21.0";
const GRAPH_BASE = `https://graph.facebook.com/${GRAPH_VERSION}`;
const FACEBOOK_WEB = "https://www.facebook.com";

const FACEBOOK_APP_ID = process.env.FACEBOOK_APP_ID || "";
const FACEBOOK_APP_SECRET = process.env.FACEBOOK_APP_SECRET || "";
const FACEBOOK_REDIRECT_URI =
  process.env.FACEBOOK_REDIRECT_URI ||
  `${process.env.API_PUBLIC_URL || ""}/api/oauth/facebook/callback`;

const FACEBOOK_CONFIG_ID = process.env.FACEBOOK_CONFIG_ID || "";

function isConfigured() {
  return Boolean(
    FACEBOOK_APP_ID &&
    FACEBOOK_APP_SECRET &&
    FACEBOOK_REDIRECT_URI &&
    FACEBOOK_CONFIG_ID,
  );
}

function getAppSecret() {
  return FACEBOOK_APP_SECRET;
}

// Step 1: build the URL the browser popup is sent to.
// Facebook Login for Business uses a saved configuration (config_id) to define
// the permissions requested during login.
function getAuthUrl(state, { rerequest = false } = {}) {
  if (!FACEBOOK_CONFIG_ID) {
    throw new Error("Facebook Login for Business configuration is missing");
  }

  const params = new URLSearchParams({
    client_id: FACEBOOK_APP_ID,
    redirect_uri: FACEBOOK_REDIRECT_URI,
    state,
    config_id: FACEBOOK_CONFIG_ID,
    response_type: "code",
  });
  // "rerequest" re-opens the Page/permission chooser so a person can add a
  // Page they skipped the first time ("Edit Page access" in the studio).
  if (rerequest) params.set("auth_type", "rerequest");

  return `${FACEBOOK_WEB}/${GRAPH_VERSION}/dialog/oauth?${params.toString()}`;
}

async function graphFetch(pathAndQuery, options = {}) {
  const res = await fetch(`${GRAPH_BASE}${pathAndQuery}`, {
    ...options,
    signal: AbortSignal.timeout(15000),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok || data.error) {
    const e = data.error || {};
    // Keep Facebook's code/subcode and user-facing text: "Permissions error"
    // alone doesn't say which permission or Page condition failed.
    const codePart = e.code
      ? ` (#${e.code}${e.error_subcode ? `/${e.error_subcode}` : ""})`
      : "";
    const detail = [e.error_user_title, e.error_user_msg]
      .filter(Boolean)
      .join(": ");
    const message =
      `${e.message || `Graph API error (${res.status})`}${codePart}` +
      `${detail ? ` — ${detail}` : ""}` +
      `${e.fbtrace_id ? ` [fbtrace ${e.fbtrace_id}]` : ""}`;
    const err = new Error(message);
    err.graphError = data.error;
    err.status = res.status;
    err.code = e.code;
    err.subcode = e.error_subcode;
    throw err;
  }
  return data;
}

// Graph returns some permalinks as relative paths ("/123/videos/456/").
function absoluteFacebookUrl(url) {
  if (!url) return null;
  const value = String(url);
  if (/^https?:\/\//i.test(value)) return value;
  return `${FACEBOOK_WEB}${value.startsWith("/") ? "" : "/"}${value}`;
}

// Token errors that mean the person must reconnect (expired, revoked,
// password changed, permission removed).
function isReconnectError(error) {
  const code = Number(error?.code || error?.graphError?.code);
  return code === 190 || code === 102 || code === 10 || code === 200;
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

// Identity of the person who completed Facebook Login. Used only to show
// "Signed in as …" in the studio; it never publishes to a personal timeline.
async function getCurrentUser(userAccessToken) {
  const params = new URLSearchParams({
    fields: "id,name,picture.type(small){url}",
    access_token: userAccessToken,
  });
  const user = await graphFetch(`/me?${params.toString()}`);
  if (!user.id || !user.name)
    throw new Error("Meta did not return an account identity");
  return {
    id: String(user.id),
    name: String(user.name),
    pictureUrl: user.picture?.data?.url || null,
  };
}

function mapPage(page) {
  const tasks = Array.isArray(page.tasks) ? page.tasks : [];
  return {
    pageId: String(page.id),
    pageName: page.name,
    pageAccessToken: page.access_token,
    category: page.category || null,
    pictureUrl: page.picture?.data?.url || null,
    followers: Number.isFinite(Number(page.followers_count))
      ? Number(page.followers_count)
      : Number.isFinite(Number(page.fan_count))
        ? Number(page.fan_count)
        : null,
    link: page.link || `${FACEBOOK_WEB}/${page.id}`,
    tasks,
    // Creating a live video on a Page requires the CREATE_CONTENT task.
    // Older tokens may not return tasks; treat that as allowed and let the
    // Graph API be the final judge when the broadcast is created.
    canGoLive: tasks.length === 0 || tasks.includes("CREATE_CONTENT"),
  };
}

// Step 4 (pages_show_list): list the Pages this person granted to the app.
// Page tokens returned here are long-lived when the user token is long-lived.
async function listUserPages(longLivedUserToken) {
  const richFields =
    "id,name,access_token,category,tasks,link,fan_count,followers_count,picture.type(large){url}";
  const basicFields = "id,name,access_token,picture{url}";
  const fetchAll = async (fields) => {
    const pages = [];
    let next = `/me/accounts?${new URLSearchParams({
      access_token: longLivedUserToken,
      fields,
      limit: "100",
    }).toString()}`;
    // Follow paging so people who manage many Pages see all of them.
    for (let i = 0; next && i < 10; i += 1) {
      const data = await graphFetch(next);
      pages.push(...(data.data || []));
      const nextUrl = data.paging?.next;
      next = nextUrl ? nextUrl.replace(GRAPH_BASE, "") : null;
    }
    return pages;
  };

  let raw;
  try {
    raw = await fetchAll(richFields);
  } catch (error) {
    if (isReconnectError(error) && Number(error.code) === 190) throw error;
    // Some fields need pages_read_engagement; fall back so the picker still
    // works while that permission is pending review.
    raw = await fetchAll(basicFields);
  }

  // Facebook Login for Business: Pages owned by a business portfolio (and
  // reached through it) are often missing from /me/accounts even though the
  // person ticked them in the login dialog. The Page IDs they ticked are on
  // the token itself (granular_scopes.target_ids), so fetch those directly.
  const seen = new Set(raw.map((page) => String(page.id)));
  let grantedIds = [];
  try {
    grantedIds = await getGrantedPageIds(longLivedUserToken);
  } catch (error) {
    console.warn("[FB] Could not read granted Page IDs:", error.message);
  }
  for (const pageId of grantedIds) {
    if (seen.has(String(pageId))) continue;
    try {
      const page = await fetchPageWithUserToken(pageId, longLivedUserToken);
      if (page?.access_token) {
        raw.push(page);
        seen.add(String(page.id));
      } else {
        console.warn(
          `[FB] Page ${pageId} was granted but Facebook returned no Page token; the person may lack a task on that Page.`,
        );
      }
    } catch (error) {
      console.warn(
        `[FB] Could not load granted Page ${pageId}:`,
        error.message,
      );
    }
  }

  console.log(
    `[FB] listUserPages: ${raw.length} Page(s) available (granted IDs: ${grantedIds.length}).`,
  );
  return raw.map(mapPage);
}

// Page IDs the person selected in the Facebook Login for Business dialog.
async function getGrantedPageIds(userAccessToken) {
  const info = await debugToken(userAccessToken);
  const pageScopes = new Set([
    "pages_show_list",
    "pages_manage_posts",
    "pages_read_engagement",
    "pages_manage_metadata",
  ]);
  const ids = new Set();
  for (const entry of info?.granular_scopes || []) {
    if (!pageScopes.has(entry.scope)) continue;
    for (const id of entry.target_ids || []) ids.add(String(id));
  }
  return [...ids];
}

async function fetchPageWithUserToken(pageId, userAccessToken) {
  const run = (fields) =>
    graphFetch(
      `/${pageId}?${new URLSearchParams({
        access_token: userAccessToken,
        fields,
      }).toString()}`,
    );
  try {
    return await run(
      "id,name,access_token,category,link,fan_count,followers_count,picture.type(large){url}",
    );
  } catch (error) {
    if (Number(error.code) === 190) throw error;
    return run("id,name,access_token,picture{url}");
  }
}

// pages_read_engagement: Page identity shown at the top of Live Studio.
async function getPageProfile({ pageId, pageAccessToken }) {
  const params = new URLSearchParams({
    access_token: pageAccessToken,
    fields:
      "id,name,category,link,about,fan_count,followers_count,picture.type(large){url},cover{source}",
  });
  const page = await graphFetch(`/${pageId}?${params.toString()}`);
  return {
    pageId: String(page.id),
    name: page.name,
    category: page.category || null,
    about: page.about || null,
    link: page.link || `${FACEBOOK_WEB}/${page.id}`,
    followers: Number.isFinite(Number(page.followers_count))
      ? Number(page.followers_count)
      : Number.isFinite(Number(page.fan_count))
        ? Number(page.fan_count)
        : null,
    pictureUrl: page.picture?.data?.url || null,
    coverUrl: page.cover?.source || null,
  };
}

// pages_read_engagement: the Page's own recent posts with engagement counts.
async function getPagePosts({ pageId, pageAccessToken, limit = 6 }) {
  const baseFields =
    "id,message,story,created_time,permalink_url,full_picture,status_type";
  const engagementFields =
    "reactions.summary(total_count).limit(0),comments.summary(total_count).limit(0),shares";
  const run = (fields) =>
    graphFetch(
      `/${pageId}/posts?${new URLSearchParams({
        access_token: pageAccessToken,
        fields,
        limit: String(limit),
      }).toString()}`,
    );

  let data;
  try {
    data = await run(`${baseFields},${engagementFields}`);
  } catch (error) {
    if (Number(error.code) === 190) throw error;
    data = await run(baseFields);
  }

  return (data.data || []).map((post) => ({
    id: post.id,
    message: post.message || post.story || "",
    createdTime: post.created_time,
    permalinkUrl: absoluteFacebookUrl(post.permalink_url),
    imageUrl: post.full_picture || null,
    statusType: post.status_type || null,
    reactions: post.reactions?.summary?.total_count ?? null,
    comments: post.comments?.summary?.total_count ?? null,
    shares: post.shares?.count ?? null,
  }));
}

// pages_read_engagement: videos published on the Page (ended lives land here).
async function getPageVideos({ pageId, pageAccessToken, limit = 6 }) {
  const data = await graphFetch(
    `/${pageId}/videos?${new URLSearchParams({
      access_token: pageAccessToken,
      fields:
        "id,title,description,created_time,permalink_url,picture,length,live_status",
      limit: String(limit),
    }).toString()}`,
  );
  return (data.data || []).map((video) => ({
    id: video.id,
    title: video.title || video.description || "Untitled video",
    createdTime: video.created_time,
    permalinkUrl: absoluteFacebookUrl(video.permalink_url),
    thumbnailUrl: video.picture || null,
    lengthSeconds: Number(video.length) || null,
    liveStatus: video.live_status || null,
  }));
}

// pages_read_engagement + Live Video API: live broadcasts on the Page.
async function getPageLiveVideos({ pageId, pageAccessToken, limit = 6 }) {
  const data = await graphFetch(
    `/${pageId}/live_videos?${new URLSearchParams({
      access_token: pageAccessToken,
      fields:
        "id,title,description,status,creation_time,permalink_url,live_views,video{id,picture}",
      limit: String(limit),
    }).toString()}`,
  );
  return (data.data || []).map(mapLiveVideo);
}

function mapLiveVideo(live) {
  return {
    id: live.id,
    title: live.title || "Live video",
    description: live.description || "",
    status: live.status || null, // LIVE, LIVE_STOPPED, VOD, UNPUBLISHED, SCHEDULED_*, PROCESSING
    createdTime: live.creation_time || null,
    permalinkUrl: absoluteFacebookUrl(live.permalink_url),
    liveViews: Number.isFinite(Number(live.live_views))
      ? Number(live.live_views)
      : null,
    videoId: live.video?.id || null,
    thumbnailUrl: live.video?.picture || null,
  };
}

// Live Video API: status of one broadcast (drives the in-app live indicator).
async function getLiveVideoStatus({ liveVideoId, pageAccessToken }) {
  const live = await graphFetch(
    `/${liveVideoId}?${new URLSearchParams({
      access_token: pageAccessToken,
      fields:
        "id,title,description,status,creation_time,permalink_url,live_views,video{id,picture}",
    }).toString()}`,
  );
  return mapLiveVideo(live);
}

// Creates a live video object on the Page and returns the RTMPS ingest URL.
// Replaces the manual "open Live Producer and copy the stream key" step.
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
      title: String(title || "Live Stream").slice(0, 254),
      description: String(description || "").slice(0, 5000),
      status: "LIVE_NOW",
    }),
  });
  // data: { id, stream_url, secure_stream_url }
  // secure_stream_url is rtmps://… and already contains the per-broadcast key.
  return {
    liveVideoId: data.id,
    rtmpUrl: data.secure_stream_url || data.stream_url,
  };
}

// Ends the broadcast on Facebook's side (separate from stopping our ffmpeg
// worker — both need to happen, or Facebook keeps the video "live").
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

// Permission test used by Live Studio's "Check Facebook setup". Creates an
// UNPUBLISHED live video (never visible on the Page) and deletes it again.
async function testCreateLiveVideo({ pageId, pageAccessToken }) {
  const created = await graphFetch(`/${pageId}/live_videos`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      access_token: pageAccessToken,
      title: "NLM setup check (not published)",
      status: "UNPUBLISHED",
    }),
  });
  try {
    await graphFetch(
      `/${created.id}?${new URLSearchParams({ access_token: pageAccessToken })}`,
      { method: "DELETE" },
    );
  } catch {
    // Unpublished test videos are harmless if deletion fails.
  }
  return { liveVideoId: created.id };
}

// Cheap validity check before relying on a token during a scheduled stream.
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
  GRAPH_VERSION,
  isConfigured,
  getAppSecret,
  getAuthUrl,
  exchangeCodeForUserToken,
  getLongLivedUserToken,
  listUserPages,
  getCurrentUser,
  getPageProfile,
  getPagePosts,
  getPageVideos,
  getPageLiveVideos,
  getLiveVideoStatus,
  createLiveVideo,
  endLiveVideo,
  debugToken,
  testCreateLiveVideo,
  isReconnectError,
  absoluteFacebookUrl,
};
