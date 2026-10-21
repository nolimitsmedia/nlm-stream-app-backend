// link-whmcs-client.js — link an EXISTING app organization to an EXISTING
// WHMCS client's StreamControl service (2026-10-21).
//
// Why: the WHMCS poller only handles new orders (latest 50 active) placed by
// customers who signed up in the app or whose email matches an org owner.
// Clients who bought StreamControl in WHMCS earlier are never picked up, so
// their organization has to be linked explicitly.
//
// What --apply writes is exactly what server.js syncWhmcsOrgSubscription()
// writes for the poller, in one transaction:
//   organizations.subscription_plan  <- plan of the WHMCS product
//   subscriptions (upsert by organization_id): plan_key, status (mapped from
//     the WHMCS service status), current_period_end (= next due date),
//     whmcs_client_id, whmcs_order_id
// plus processed_whmcs_orders(order, org, 'linked_manually') so the poller
// never re-classifies that order. After linking, the Subscription page's
// "Refresh from WHMCS" (/api/subscription/refresh) keeps it in sync.
// Nothing is ever written to WHMCS.
//
// Usage (from the backend directory):
//   node link-whmcs-client.js --org <id|slug> --whmcs-client <id>             # preview only
//   node link-whmcs-client.js --org <id|slug> --whmcs-client <id> --apply     # write
// Options:
//   --service <id>   choose the WHMCS service when the client has several
//   --force          allow: re-linking an org already linked to a different
//                    WHMCS client, linking a client already linked to another
//                    org, or changing the house organization's plan
//   --show-emails    print emails unmasked (do not share that output)

const pool = require("./db");
const whmcs = require("./whmcs_client");

const args = process.argv.slice(2);
const opt = (name) => {
  const i = args.indexOf(`--${name}`);
  return i >= 0 && args[i + 1] && !args[i + 1].startsWith("--")
    ? args[i + 1]
    : null;
};
const flag = (name) => args.includes(`--${name}`);
const APPLY = flag("apply");
const FORCE = flag("force");
const SHOW_EMAILS = flag("show-emails");
const HOUSE_SLUG = "no-limits-media";

const mask = (email) => {
  const v = String(email || "").trim();
  if (!v) return "-";
  if (SHOW_EMAILS || !v.includes("@")) return v;
  const [u, d] = v.split("@");
  return `${u.slice(0, 1)}***@${d}`;
};
const validDate = (value) => {
  const s = String(value || "");
  if (!/^\d{4}-\d{2}-\d{2}/.test(s) || s.startsWith("0000")) return null;
  const d = new Date(s);
  return Number.isNaN(d.getTime()) ? null : d;
};

async function main() {
  const orgArg = opt("org");
  const clientArg = opt("whmcs-client");
  const serviceArg = opt("service");
  if (!orgArg || !clientArg) {
    throw new Error(
      "usage: node link-whmcs-client.js --org <id|slug> --whmcs-client <id> [--service <id>] [--apply] [--force]",
    );
  }
  if (!/^\d+$/.test(clientArg))
    throw new Error("--whmcs-client must be a numeric WHMCS client id");
  if (!whmcs.isWhmcsConfigured())
    throw new Error("WHMCS API is not configured in .env");

  // ---- organization ----
  const orgRes = await pool.query(
    /^\d+$/.test(orgArg)
      ? `SELECT id, name, slug, subscription_plan, is_active FROM organizations WHERE id = $1`
      : `SELECT id, name, slug, subscription_plan, is_active FROM organizations WHERE slug = $1`,
    [/^\d+$/.test(orgArg) ? Number(orgArg) : orgArg],
  );
  const org = orgRes.rows[0];
  if (!org) throw new Error(`organization "${orgArg}" not found`);
  const subRes = await pool.query(
    `SELECT plan_key, status, whmcs_client_id, whmcs_order_id, current_period_end FROM subscriptions WHERE organization_id = $1`,
    [org.id],
  );
  const currentSub = subRes.rows[0] || null;
  const ownerRes = await pool.query(
    `SELECT lower(a.email) AS email FROM admins a
       JOIN organization_users ou ON ou.admin_id = a.id AND ou.role = 'owner'
      WHERE ou.organization_id = $1`,
    [org.id],
  );
  const ownerEmails = ownerRes.rows.map((r) => r.email);

  // ---- WHMCS client + StreamControl service ----
  const details = await whmcs.getClientDetails(clientArg);
  const client = details.client || details;
  const clientEmail = String(client.email || details.email || "").toLowerCase();
  const clientName =
    client.companyname ||
    details.companyname ||
    [client.firstname || details.firstname, client.lastname || details.lastname]
      .filter(Boolean)
      .join(" ");
  const products = (await whmcs.getClientsProducts(clientArg)).filter((p) =>
    whmcs.getPlanKeyForWhmcsProductId(p.pid),
  );
  if (!products.length)
    throw new Error(
      `WHMCS client ${clientArg} has no StreamControl (Essential/Premium/Enterprise) service`,
    );
  let service;
  if (serviceArg) {
    service = products.find((p) => String(p.id) === String(serviceArg));
    if (!service)
      throw new Error(
        `service ${serviceArg} is not a StreamControl service of client ${clientArg}`,
      );
  } else {
    const active = products.filter(
      (p) => String(p.status).toLowerCase() === "active",
    );
    if (active.length > 1) {
      throw new Error(
        `client ${clientArg} has ${active.length} active StreamControl services (${active.map((p) => p.id).join(", ")}); choose one with --service <id>`,
      );
    }
    service = active[0] || products[0];
  }
  const planKey = whmcs.getPlanKeyForWhmcsProductId(service.pid);
  const status = whmcs.mapWhmcsStatusToSubscriptionStatus(service.status);
  const periodEnd = validDate(service.nextduedate);
  const orderId = service.orderid ? String(service.orderid) : null;

  // ---- safety checks ----
  const problems = [];
  const warnings = [];
  if (!org.is_active) problems.push(`organization ${org.id} is not active`);
  if (
    currentSub?.whmcs_client_id &&
    String(currentSub.whmcs_client_id) !== clientArg
  ) {
    problems.push(
      `organization is already linked to WHMCS client ${currentSub.whmcs_client_id} (use --force to re-link)`,
    );
  }
  const otherOrgs = (
    await pool.query(
      `SELECT s.organization_id, o.name FROM subscriptions s JOIN organizations o ON o.id = s.organization_id
        WHERE s.whmcs_client_id = $1 AND s.organization_id <> $2`,
      [clientArg, org.id],
    )
  ).rows;
  if (otherOrgs.length) {
    problems.push(
      `WHMCS client ${clientArg} is already linked to ${otherOrgs.map((o) => `org ${o.organization_id} (${o.name})`).join(", ")} (use --force to link it here as well)`,
    );
  }
  if (org.slug === HOUSE_SLUG) {
    problems.push(
      `"${org.name}" is the house organization; linking changes its plan from "${org.subscription_plan}" to "${planKey}" (use --force if that is intended)`,
    );
  }
  if (status === "canceled")
    warnings.push(
      `the WHMCS service is ${service.status}; the subscription will be marked canceled`,
    );
  if (!ownerEmails.length)
    warnings.push("the organization has no owner account yet");
  else if (clientEmail && !ownerEmails.includes(clientEmail)) {
    warnings.push(
      `owner email (${ownerEmails.map(mask).join(", ")}) differs from the WHMCS client email (${mask(clientEmail)})`,
    );
  }
  if (!periodEnd)
    warnings.push(
      `WHMCS next due date "${service.nextduedate || ""}" is not a valid date; the current period end is left unchanged`,
    );

  // ---- report ----
  console.log(
    `Organization : ${org.id} ${org.name} (slug ${org.slug}), plan ${org.subscription_plan}`,
  );
  console.log(
    `  current    : ${currentSub ? `${currentSub.plan_key}/${currentSub.status}, WHMCS client ${currentSub.whmcs_client_id || "none"}` : "no subscription row"}`,
  );
  console.log(`  owners     : ${ownerEmails.map(mask).join(", ") || "-"}`);
  console.log(
    `WHMCS client : ${clientArg} ${clientName} <${mask(clientEmail)}>`,
  );
  console.log(
    `  service    : ${service.id} — product ${service.pid} → plan "${planKey}", status ${service.status} → "${status}", next due ${service.nextduedate || "-"}, order ${orderId || "-"}`,
  );
  console.log(
    `Will set     : plan ${planKey}, status ${status}, period end ${periodEnd ? periodEnd.toISOString().slice(0, 10) : "(unchanged)"}, whmcs_client_id ${clientArg}, whmcs_order_id ${orderId || "(unchanged)"}`,
  );
  for (const w of warnings) console.log(`WARNING      : ${w}`);
  for (const p of problems)
    console.log(`${FORCE ? "FORCED       " : "BLOCKED      "}: ${p}`);

  if (problems.length && !FORCE) {
    console.log(
      "\nNot linked. Resolve the BLOCKED item(s) above, or re-run with --force if you are sure.",
    );
    return 1;
  }
  if (!APPLY) {
    console.log(
      "\nPreview only — nothing was changed. Re-run with --apply to link.",
    );
    return 0;
  }

  const db = await pool.connect();
  try {
    await db.query("BEGIN");
    await db.query(
      `UPDATE organizations SET subscription_plan = $1, updated_at = NOW() WHERE id = $2`,
      [planKey, org.id],
    );
    await db.query(
      `INSERT INTO subscriptions (organization_id, plan_key, status, current_period_start, current_period_end, whmcs_client_id, whmcs_order_id, whmcs_invoice_id)
       VALUES ($1, $2, $3, NOW(), $4, $5, $6, NULL)
       ON CONFLICT (organization_id) DO UPDATE SET
         plan_key = COALESCE($2, subscriptions.plan_key),
         status = $3,
         current_period_end = COALESCE($4, subscriptions.current_period_end),
         whmcs_client_id = COALESCE($5, subscriptions.whmcs_client_id),
         whmcs_order_id = COALESCE($6, subscriptions.whmcs_order_id),
         updated_at = NOW()`,
      [org.id, planKey, status, periodEnd, clientArg, orderId],
    );
    if (orderId) {
      await db.query(
        `INSERT INTO processed_whmcs_orders (whmcs_order_id, organization_id, outcome)
         VALUES ($1, $2, 'linked_manually') ON CONFLICT DO NOTHING`,
        [orderId, org.id],
      );
    }
    await db.query("COMMIT");
  } catch (error) {
    await db.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    db.release();
  }

  const after = (
    await pool.query(
      `SELECT o.subscription_plan, s.plan_key, s.status, s.whmcs_client_id, s.whmcs_order_id, s.current_period_end
         FROM organizations o JOIN subscriptions s ON s.organization_id = o.id WHERE o.id = $1`,
      [org.id],
    )
  ).rows[0];
  console.log(
    `\nLINKED ✓  org ${org.id} → WHMCS client ${after.whmcs_client_id}: plan ${after.plan_key} (org plan ${after.subscription_plan}), status ${after.status}, order ${after.whmcs_order_id || "-"}, period end ${after.current_period_end ? new Date(after.current_period_end).toISOString().slice(0, 10) : "-"}`,
  );
  console.log(
    'Next: open the organization\'s Subscription page and use "Refresh from WHMCS" to confirm the sync.',
  );
  return 0;
}

main()
  .then(async (code) => {
    await pool.end().catch(() => {});
    process.exit(code);
  })
  .catch(async (error) => {
    console.error(`ERROR: ${error.message}`);
    await pool.end().catch(() => {});
    process.exit(1);
  });
