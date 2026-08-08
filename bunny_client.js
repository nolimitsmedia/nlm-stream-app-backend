// server/bunny_client.js
//
// Wraps Bunny.net's Core/Account API (api.bunny.net) — this is DIFFERENT
// from the existing shared-zone Storage API usage elsewhere in the app
// (which uses a single Storage Zone's own password to upload/download
// files). This module uses the account-level API key to CREATE new zones
// and pull account-wide statistics — capabilities the storage zone
// password cannot do.
//
// Built for the "separate Bunny pull+storage zone per organization"
// architecture decided in chat, so per-org CDN/egress bandwidth can
// eventually be measured via Bunny's per-zone statistics instead of one
// shared platform-wide total. Existing organizations are grandfathered
// onto the original shared zones (BUNNY_STORAGE_ZONE/HOSTNAME/API_KEY env
// vars, used elsewhere in server.js) — only NEW organizations get their
// own dedicated zones via this module.

const BUNNY_API_BASE = "https://api.bunny.net";
const BUNNY_ACCOUNT_API_KEY = process.env.BUNNY_ACCOUNT_API_KEY || "";

// The origin server pull zones fetch HLS segments from — same origin the
// existing shared "nlmstream" pull zone already points at.
const BUNNY_ORIGIN_URL = process.env.BUNNY_ORIGIN_URL || "";

// Storage zones need a region code, not a full hostname — the existing
// shared storage zone is in NY (ny.storage.bunnycdn.com), so default to
// that same region for consistency unless told otherwise.
const BUNNY_STORAGE_ZONE_REGION = process.env.BUNNY_STORAGE_ZONE_REGION || "NY";

// The single platform-wide key server.js's signBunnyUrlPath()/
// appendBunnyToken() already sign every HLS/ABR URL with. Every zone this
// module provisions gets its own real Token Authentication key FORCED to
// equal this value (via resetSecurityKey below) — rather than storing a
// different auto-generated key per organization and rewriting the signing
// code to look one up per request, every zone's actual key simply matches
// the one value already used everywhere. Must be the exact same env var
// server.js reads (BUNNY_HLS_TOKEN_KEY) or signed URLs will validate
// against the wrong key.
const BUNNY_HLS_TOKEN_KEY = process.env.BUNNY_HLS_TOKEN_KEY || "";

const isBunnyAccountConfigured = () => Boolean(BUNNY_ACCOUNT_API_KEY);

const callBunnyApi = async (method, path, body) => {
  if (!isBunnyAccountConfigured()) {
    throw new Error(
      "Bunny account API is not configured (missing BUNNY_ACCOUNT_API_KEY)",
    );
  }

  const response = await fetch(`${BUNNY_API_BASE}${path}`, {
    method,
    headers: {
      AccessKey: BUNNY_ACCOUNT_API_KEY,
      "Content-Type": "application/json",
      Accept: "application/json",
    },
    body: body ? JSON.stringify(body) : undefined,
    signal: AbortSignal.timeout(20000),
  });

  const text = await response.text();
  const data = text ? JSON.parse(text) : {};

  if (!response.ok) {
    throw new Error(
      `Bunny API ${method} ${path} failed (HTTP ${response.status}): ${data.Message || text || "unknown error"}`,
    );
  }

  return data;
};

// Enables Token Authentication on an existing pull zone and forces its
// real security key to equal our single platform-wide BUNNY_HLS_TOKEN_KEY
// — this is what makes Token Authentication enforcement compatible with
// the existing single-shared-key signing code in server.js without
// needing to touch that code, store a different key per organization, or
// look one up per request.
//
// Real incident, 2026-08-08: an auto-provisioned zone (nlm-one-church)
// was found via a live unsigned-request test to be serving live HLS
// manifests AND video segments with ZERO authentication — every zone
// createPullZoneForOrganization() created before this fix had the exact
// same gap, since Token Authentication defaults to OFF on a brand-new
// Bunny pull zone and was never explicitly turned on here.
//
// NOTE: field names below (ZoneSecurityEnabled on the update call,
// SecurityKey on resetSecurityKey) are the best-identified match for
// Bunny's current API shape — NOT yet confirmed against a real response
// from this account, consistent with the same caveat already given for
// createPullZoneForOrganization() below. After running this, verify in
// the Bunny dashboard (Pull Zone -> Security -> Token authentication)
// that it actually shows ON with the expected key before trusting it for
// a bulk backfill across every existing organization.
const enableTokenAuthForPullZone = async (pullZoneId) => {
  if (!BUNNY_HLS_TOKEN_KEY) {
    throw new Error(
      "BUNNY_HLS_TOKEN_KEY is not configured — refusing to enable Token Authentication with no key to set it to (that would just break playback for this zone)",
    );
  }

  await callBunnyApi("POST", `/pullzone/${pullZoneId}`, {
    ZoneSecurityEnabled: true,
  });

  await callBunnyApi("POST", `/pullzone/${pullZoneId}/resetSecurityKey`, {
    SecurityKey: BUNNY_HLS_TOKEN_KEY,
  });
};

// Creates a new Pull Zone for an organization's live HLS delivery. Returns
// the zone's id and its default CDN hostname ({name}.b-cdn.net).
//
// NOTE: field names below (OriginUrl, PullZone.Id/Hostnames, etc.) follow
// Bunny's documented Pull Zone API shape as of when this was written — if
// zone creation fails with an unexpected response shape, check Bunny's
// current API docs for any field renames before assuming the API key
// itself is the problem.
const createPullZoneForOrganization = async (orgSlug) => {
  if (!BUNNY_ORIGIN_URL) {
    throw new Error(
      "BUNNY_ORIGIN_URL is not configured — needed to know what origin server new pull zones should fetch from",
    );
  }

  const zoneName = `nlm-${orgSlug}`;

  const result = await callBunnyApi("POST", "/pullzone", {
    Name: zoneName,
    OriginUrl: BUNNY_ORIGIN_URL,
    Type: 0, // 0 = Standard/Premium tier pull zone
  });

  // Every new zone MUST have Token Authentication enabled before it's
  // handed back — otherwise its HLS manifests/segments are fetchable by
  // anyone who knows or guesses the org's stream key, completely
  // bypassing signed-URL playback gating. See enableTokenAuthForPullZone
  // above for the real incident this fixes. Deliberately NOT caught here
  // — if this fails, provisioning should fail loudly rather than hand
  // back an org with a silently unprotected pull zone.
  await enableTokenAuthForPullZone(result.Id);

  return {
    pullZoneId: result.Id,
    pullZoneName: result.Name,
    // Bunny's default CDN hostname for a pull zone is always
    // {name}.b-cdn.net — also present in result.Hostnames[0].Value, but
    // this is deterministic and doesn't require parsing that array.
    pullZoneHostname: `${zoneName}.b-cdn.net`,
  };
};

// Creates a new Storage Zone for an organization's own recording archive,
// AND a Pull Zone linked to it (origin = that storage zone, not a URL) so
// archived recordings are publicly servable via a CDN URL — mirroring
// exactly how the existing shared "nlm-stream-recordings" zone already
// works (a storage zone plus a same-named connected pull zone). Per
// Bunny's own docs, serving files directly from a Storage Zone without a
// connected Pull Zone breaches their Terms of Service, so this step isn't
// optional.
//
// NOTE: the exact field signaling "this pull zone's origin is a storage
// zone" (StorageZoneId on the create payload, omitting OriginUrl) is
// based on Bunny's documented response shape, not a confirmed request
// example — verify in the Bunny dashboard that the created pull zone
// actually shows "Storage Zone" as its origin type before trusting this
// for a real customer's recordings.
const createStorageZoneForOrganization = async (orgSlug) => {
  const zoneName = `nlm-${orgSlug}-recordings`;

  const storageResult = await callBunnyApi("POST", "/storagezone", {
    Name: zoneName,
    Region: BUNNY_STORAGE_ZONE_REGION,
  });

  const pullZoneResult = await callBunnyApi("POST", "/pullzone", {
    Name: zoneName,
    StorageZoneId: storageResult.Id,
  });

  return {
    storageZoneId: storageResult.Id,
    storageZoneName: storageResult.Name,
    storageZoneHostname:
      storageResult.StorageHostname ||
      `${BUNNY_STORAGE_ZONE_REGION.toLowerCase()}.storage.bunnycdn.com`,
    storageZonePassword: storageResult.Password,
    recordingsPullZoneId: pullZoneResult.Id,
    recordingsCdnUrl: `https://${zoneName}.b-cdn.net`,
  };
};

// Full provisioning for a brand-new organization: creates both zones and
// returns everything needed to store on the organizations row. Throws if
// either step fails — deliberately not partial/best-effort, since a
// half-provisioned org (e.g. pull zone created but storage zone failed)
// would be a confusing state to debug later.
const provisionBunnyZonesForOrganization = async (orgSlug) => {
  const pullZone = await createPullZoneForOrganization(orgSlug);
  const storageZone = await createStorageZoneForOrganization(orgSlug);

  return { ...pullZone, ...storageZone };
};

// Per-zone bandwidth statistics — will be used once quota ENFORCEMENT is
// built on top of this provisioning. dateFrom/dateTo are Date objects.
//
// NOTE: the query param is `pullZone`, NOT `pullZoneId` — confirmed this
// was the actual bug behind a real incident where two different orgs'
// bandwidth checks both returned the exact same (account-wide) total:
// Bunny was silently ignoring the unrecognized `pullZoneId` param and
// falling back to all-zones data instead of erroring, which is exactly
// why two different orgs got an identical number.
const getPullZoneStatistics = async (pullZoneId, dateFrom, dateTo) => {
  const params = new URLSearchParams({
    pullZone: String(pullZoneId),
    dateFrom: dateFrom.toISOString(),
    dateTo: dateTo.toISOString(),
  });

  return callBunnyApi("GET", `/statistics?${params.toString()}`);
};

// Total bandwidth (bytes) served by a pull zone over a date range.
//
// NOTE: this reads `TotalBandwidthUsed` from Bunny's /statistics response
// based on its commonly-documented shape — not confirmed against a real
// response from this account. If bandwidth quota checks always come back
// as 0 despite real traffic, check a raw response from this endpoint for
// the actual field name before assuming something else is broken.
const getTotalBandwidthUsedBytes = async (pullZoneId, dateFrom, dateTo) => {
  if (!pullZoneId) return 0;

  const stats = await getPullZoneStatistics(pullZoneId, dateFrom, dateTo);
  return Number(stats.TotalBandwidthUsed || 0);
};

module.exports = {
  isBunnyAccountConfigured,
  provisionBunnyZonesForOrganization,
  createPullZoneForOrganization,
  createStorageZoneForOrganization,
  enableTokenAuthForPullZone,
  getPullZoneStatistics,
  getTotalBandwidthUsedBytes,
};
