// link-whmcs-client.js — link an EXISTING app organization to an EXISTING
// WHMCS client's StreamControl service (2026-10-21).
//
// Why: the WHMCS poller only handles new orders (latest 50 active) placed by
// customers who signed up in the app or whose email matches an org owner.
// Clients who bought StreamControl in WHMCS earlier are never picked up, so
// their organization has to be linked explicitly.
//
// The checks and writes live in whmcs_link_service.js (shared with the
// super-admin "Import from WHMCS" API). What --apply writes is exactly what
// server.js syncWhmcsOrgSubscription()
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
const linkService = require("./whmcs_link_service");

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

const mask = (email) => {
  const v = String(email || "").trim();
  if (!v) return "-";
  if (SHOW_EMAILS || !v.includes("@")) return v;
  const [u, d] = v.split("@");
  return `${u.slice(0, 1)}***@${d}`;
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

  let preview;
  try {
    preview = await linkService.previewLink({
      pool,
      whmcs,
      organization: orgArg,
      whmcsClientId: clientArg,
      serviceId: serviceArg,
    });
  } catch (error) {
    if (error.serviceIds)
      throw new Error(
        `${error.message} — re-run with --service <id> (one of ${error.serviceIds.join(", ")})`,
      );
    throw error;
  }
  const {
    organization: org,
    currentSubscription: currentSub,
    ownerEmails,
    client,
    service,
    planned,
    problems,
    warnings,
  } = preview;

  // ---- report ----
  console.log(
    `Organization : ${org.id} ${org.name} (slug ${org.slug}), plan ${org.subscription_plan}`,
  );
  console.log(
    `  current    : ${currentSub ? `${currentSub.plan_key}/${currentSub.status}, WHMCS client ${currentSub.whmcs_client_id || "none"}` : "no subscription row"}`,
  );
  console.log(`  owners     : ${ownerEmails.map(mask).join(", ") || "-"}`);
  console.log(
    `WHMCS client : ${client.id} ${client.name} <${mask(client.email)}>`,
  );
  console.log(
    `  service    : ${service.id} — product ${service.product_id} → plan "${planned.plan_key}", status ${service.status} → "${planned.status}", next due ${service.next_due || "-"}, order ${service.order_id || "-"}`,
  );
  console.log(
    `Will set     : plan ${planned.plan_key}, status ${planned.status}, period end ${planned.current_period_end || "(unchanged)"}, whmcs_client_id ${planned.whmcs_client_id}, whmcs_order_id ${planned.whmcs_order_id || "(unchanged)"}`,
  );
  for (const w of warnings) {
    console.log(
      `WARNING      : ${w}${/owner's email differs/.test(w) ? ` (owner ${ownerEmails.map(mask).join(", ")} vs WHMCS ${mask(client.email)})` : ""}`,
    );
  }
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

  const { after } = await linkService.applyLink({
    pool,
    whmcs,
    organization: orgArg,
    whmcsClientId: clientArg,
    serviceId: serviceArg,
    force: FORCE,
  });
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
