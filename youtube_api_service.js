// youtube_api_service.js
//
// Wraps the YouTube Data API v3 calls needed to automate "Go Live."
// Uses the official googleapis client (add to package.json — see notes below).
//
// Docs this maps to:
//   - OAuth for web server apps: https://developers.google.com/identity/protocols/oauth2/web-server
//   - Live Streaming API:        https://developers.google.com/youtube/v3/live/docs
//
// Required scope (submitted for Google's sensitive-scope review):
//   https://www.googleapis.com/auth/youtube

const { google } = require("googleapis");

const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID || "";
const GOOGLE_CLIENT_SECRET = process.env.GOOGLE_CLIENT_SECRET || "";
const GOOGLE_REDIRECT_URI =
  process.env.GOOGLE_REDIRECT_URI ||
  `${process.env.API_PUBLIC_URL || ""}/api/oauth/youtube/callback`;

const YOUTUBE_SCOPES = ["https://www.googleapis.com/auth/youtube"];

function isConfigured() {
  return Boolean(GOOGLE_CLIENT_ID && GOOGLE_CLIENT_SECRET);
}

function newOAuthClient() {
  return new google.auth.OAuth2(
    GOOGLE_CLIENT_ID,
    GOOGLE_CLIENT_SECRET,
    GOOGLE_REDIRECT_URI,
  );
}

// Step 1: build the URL the browser popup is sent to.
function getAuthUrl(state) {
  const oauth2Client = newOAuthClient();
  return oauth2Client.generateAuthUrl({
    access_type: "offline", // required to get a refresh_token back
    prompt: "consent", // forces refresh_token on repeat connections too
    scope: YOUTUBE_SCOPES,
    state,
  });
}

// Step 2: exchange the ?code= from the callback for tokens.
async function exchangeCodeForTokens(code) {
  const oauth2Client = newOAuthClient();
  const { tokens } = await oauth2Client.getToken(code);
  // tokens: { access_token, refresh_token, expiry_date, scope, token_type }
  return tokens;
}

function clientFromTokens({ accessToken, refreshToken }) {
  const oauth2Client = newOAuthClient();
  oauth2Client.setCredentials({
    access_token: accessToken,
    refresh_token: refreshToken,
  });
  return oauth2Client;
}

// Refreshes an expired/expiring access token using the stored refresh_token.
// Google refresh tokens don't expire on a fixed schedule, but they can be
// revoked (user removes app access, 6 months of inactivity, etc.) — callers
// should be ready for this to throw and prompt a reconnect.
async function refreshAccessToken(refreshToken) {
  const oauth2Client = newOAuthClient();
  oauth2Client.setCredentials({ refresh_token: refreshToken });
  const { credentials } = await oauth2Client.refreshAccessToken();
  return credentials; // { access_token, expiry_date, ... }
}

// Fetches the channel tied to the authorized account, so we know what we
// connected to (YouTube ties one channel to one Google account in the
// common case — Brand Accounts with multiple managers are the exception).
async function getMyChannel(oauth2Client) {
  const youtube = google.youtube({ version: "v3", auth: oauth2Client });
  const res = await youtube.channels.list({ part: ["snippet"], mine: true });
  const channel = res.data.items?.[0];
  if (!channel) return null;
  return {
    channelId: channel.id,
    channelTitle: channel.snippet?.title || null,
    thumbnailUrl: channel.snippet?.thumbnails?.default?.url || null,
  };
}

// Creates a broadcast + a bound stream, and returns the RTMP ingest details.
// This is the pair of calls that replaces the client manually creating a
// stream on YouTube Studio and pasting the key into our app.
async function createBroadcastAndStream(oauth2Client, { title, description }) {
  const youtube = google.youtube({ version: "v3", auth: oauth2Client });

  const broadcastRes = await youtube.liveBroadcasts.insert({
    part: ["snippet", "status", "contentDetails"],
    requestBody: {
      snippet: {
        title: title || "Live Stream",
        description: description || "",
        scheduledStartTime: new Date().toISOString(),
      },
      status: { privacyStatus: "public", selfDeclaredMadeForKids: false },
      contentDetails: { enableAutoStart: true, enableAutoStop: true },
    },
  });
  const broadcast = broadcastRes.data;

  const streamRes = await youtube.liveStreams.insert({
    part: ["snippet", "cdn"],
    requestBody: {
      snippet: { title: title || "Live Stream" },
      cdn: {
        frameRate: "variable",
        ingestionType: "rtmp",
        resolution: "variable",
      },
    },
  });
  const stream = streamRes.data;

  await youtube.liveBroadcasts.bind({
    id: broadcast.id,
    part: ["id"],
    streamId: stream.id,
  });

  const ingestion = stream.cdn?.ingestionInfo;
  return {
    broadcastId: broadcast.id,
    streamId: stream.id,
    rtmpUrl: `${ingestion.ingestionAddress}/${ingestion.streamName}`,
  };
}

// enableAutoStart/enableAutoStop above mean YouTube flips the broadcast's
// status on its own once RTMP data arrives/stops — but calling transition
// explicitly is still exposed here for a manual "end stream now" action.
async function transitionBroadcast(oauth2Client, broadcastId, status) {
  const youtube = google.youtube({ version: "v3", auth: oauth2Client });
  return youtube.liveBroadcasts.transition({
    id: broadcastId,
    broadcastStatus: status, // "live" | "complete"
    part: ["status"],
  });
}

module.exports = {
  isConfigured,
  getAuthUrl,
  exchangeCodeForTokens,
  clientFromTokens,
  refreshAccessToken,
  getMyChannel,
  createBroadcastAndStream,
  transitionBroadcast,
};
