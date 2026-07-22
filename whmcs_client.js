// server/whmcs_client.js
//
// Thin wrapper around the WHMCS Local API (includes/api.php) using API
// Credentials (identifier/secret) — this is the modern replacement for the
// old admin username/password API auth. Docs: developers.whmcs.com/api/
//
// This module is intentionally dependency-free (uses global fetch, Node 18+)
// so it drops straight into the existing server.js without a new package.

const WHMCS_API_URL = process.env.WHMCS_API_URL || "";
const WHMCS_API_IDENTIFIER = process.env.WHMCS_API_IDENTIFIER || "";
const WHMCS_API_SECRET = process.env.WHMCS_API_SECRET || "";

const WHMCS_CART_URL = process.env.WHMCS_CART_URL || "";
const WHMCS_CLIENT_AREA_URL = process.env.WHMCS_CLIENT_AREA_URL || "";

const isWhmcsConfigured = () =>
  Boolean(WHMCS_API_URL && WHMCS_API_IDENTIFIER && WHMCS_API_SECRET);

// Every plan_key in PLAN_DEFINITIONS that is actually sold needs a matching
// WHMCS product id here. "internal" is intentionally excluded — that plan
// is never purchased, it's assigned manually to house/test orgs.
//
// Env var names match WHMCS's own product naming (Essential/Premium/
// Enterprise Streaming Solution) rather than our internal plan_key naming
// (starter/pro/enterprise) — the mapping below is what ties the two
// together, so plan_key stays stable for the DB/existing orgs while the
// .env reads naturally against the WHMCS product list.
const PLAN_TO_WHMCS_PRODUCT_ID = {
  starter: process.env.WHMCS_ESSENTIAL_PRODUCT_ID || "",
  pro: process.env.WHMCS_PREMIUM_PRODUCT_ID || "",
  enterprise: process.env.WHMCS_ENTERPRISE_PRODUCT_ID || "",
};

const getWhmcsProductIdForPlan = (planKey) => {
  const normalized = String(planKey || "").toLowerCase();
  return PLAN_TO_WHMCS_PRODUCT_ID[normalized] || null;
};

const getPlanKeyForWhmcsProductId = (productId) => {
  if (!productId) return null;
  const normalizedId = String(productId);

  const match = Object.entries(PLAN_TO_WHMCS_PRODUCT_ID).find(
    ([, configuredId]) => configuredId && String(configuredId) === normalizedId,
  );

  return match?.[0] || null;
};

const isWhmcsCheckoutReadyForPlan = (planKey) =>
  Boolean(isWhmcsConfigured() && getWhmcsProductIdForPlan(planKey));

// WHMCS order/service statuses -> our internal subscription status values
// (active / trialing / past_due / canceled) so the rest of the app doesn't
// need to know WHMCS's vocabulary.
const mapWhmcsStatusToSubscriptionStatus = (whmcsStatus) => {
  const normalized = String(whmcsStatus || "").toLowerCase();

  if (["active", "pending"].includes(normalized)) return "active";
  if (["suspended", "overdue"].includes(normalized)) return "past_due";
  if (["cancelled", "canceled", "terminated", "fraud"].includes(normalized)) {
    return "canceled";
  }

  return "active";
};

// Builds the URL a customer is sent to in order to buy / upgrade to a
// given plan on WHMCS's own hosted order form. WHMCS pre-fills the
// registration step from these GET params when the visitor isn't already
// logged into the client area.
const buildWhmcsCheckoutUrl = (
  planKey,
  { email, firstName, lastName } = {},
) => {
  const productId = getWhmcsProductIdForPlan(planKey);
  if (!productId || !WHMCS_CART_URL) return null;

  const url = new URL(WHMCS_CART_URL);
  url.searchParams.set("a", "add");
  url.searchParams.set("pid", productId);
  if (email) url.searchParams.set("email", email);
  if (firstName) url.searchParams.set("firstname", firstName);
  if (lastName) url.searchParams.set("lastname", lastName);

  return url.toString();
};

const getWhmcsClientAreaUrl = () => WHMCS_CLIENT_AREA_URL || null;

// Low-level call. `action` is any WHMCS API action name (GetOrders,
// GetClientsDetails, GetInvoices, etc). Returns the parsed JSON body.
// Throws on transport failure or when WHMCS reports result !== "success".
const callWhmcsApi = async (action, params = {}) => {
  if (!isWhmcsConfigured()) {
    throw new Error(
      "WHMCS API is not configured (missing WHMCS_API_URL / WHMCS_API_IDENTIFIER / WHMCS_API_SECRET)",
    );
  }

  const body = new URLSearchParams({
    action,
    identifier: WHMCS_API_IDENTIFIER,
    secret: WHMCS_API_SECRET,
    responsetype: "json",
    ...Object.fromEntries(
      Object.entries(params)
        .filter(([, v]) => v !== undefined && v !== null)
        .map(([k, v]) => [k, String(v)]),
    ),
  });

  const response = await fetch(WHMCS_API_URL, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body,
    signal: AbortSignal.timeout(15000),
  });

  if (!response.ok) {
    throw new Error(`WHMCS API HTTP ${response.status} for action ${action}`);
  }

  const data = await response.json();

  if (data.result !== "success") {
    throw new Error(
      `WHMCS API error for action ${action}: ${data.message || "unknown error"}`,
    );
  }

  return data;
};

// ── Convenience wrappers around the actions this integration needs ──

const getOrders = async ({ limitStart = 0, limitNum = 50, status } = {}) => {
  const data = await callWhmcsApi("GetOrders", {
    limitstart: limitStart,
    limitnum: limitNum,
    ...(status ? { status } : {}),
  });

  return data.orders?.order || [];
};

const getClientDetails = async (clientId) => {
  const data = await callWhmcsApi("GetClientsDetails", {
    clientid: clientId,
    stats: false,
  });

  return data;
};

const getClientsProducts = async (clientId) => {
  const data = await callWhmcsApi("GetClientsProducts", {
    clientid: clientId,
  });

  return data.products?.product || [];
};

const getInvoices = async ({
  userId,
  limitStart = 0,
  limitNum = 25,
  status,
} = {}) => {
  const data = await callWhmcsApi("GetInvoices", {
    ...(userId ? { userid: userId } : {}),
    limitstart: limitStart,
    limitnum: limitNum,
    ...(status ? { status } : {}),
  });

  return data.invoices?.invoice || [];
};

module.exports = {
  isWhmcsConfigured,
  getWhmcsProductIdForPlan,
  getPlanKeyForWhmcsProductId,
  isWhmcsCheckoutReadyForPlan,
  mapWhmcsStatusToSubscriptionStatus,
  buildWhmcsCheckoutUrl,
  getWhmcsClientAreaUrl,
  callWhmcsApi,
  getOrders,
  getClientDetails,
  getClientsProducts,
  getInvoices,
};
