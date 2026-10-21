// whmcs-clients-report.js — READ-ONLY report (2026-10-21)
//
// Lists every WHMCS client that holds a StreamControl product (the product
// ids configured as WHMCS_ESSENTIAL/PREMIUM/ENTERPRISE_PRODUCT_ID) and shows
// whether each one is already connected to an organization in this app:
//
//   LINKED       a subscription row carries this WHMCS client id
//                (billing sync is connected)
//   EMAIL MATCH  an organization OWNER in the app uses the same email, so
//                the WHMCS poller can link them (once their order is
//                re-queued if it was marked for manual review)
//   NOT IN APP   no organization yet — needs to be brought in
//
// Also shows how the WHMCS poller classified each service's order
// (processed_whmcs_orders.outcome).
//
// Run from the backend directory:
//   node whmcs-clients-report.js                # emails masked (safe to share)
//   node whmcs-clients-report.js --show-emails  # full emails (do not share)
//
// Makes no changes to WHMCS or the database.

const pool = require("./db");
const whmcs = require("./whmcs_client");

const SHOW_EMAILS = process.argv.includes("--show-emails");
const PLANS = ["starter", "pro", "enterprise"];
const PAGE = 100;

const mask = (email) => {
  const value = String(email || "").trim();
  if (SHOW_EMAILS || !value.includes("@")) return value || "-";
  const [user, domain] = value.split("@");
  return `${user.slice(0, 1)}***@${domain}`;
};

async function servicesForProduct(pid) {
  const services = [];
  for (let start = 0; start < 10000; start += PAGE) {
    const data = await whmcs.callWhmcsApi("GetClientsProducts", {
      pid,
      limitstart: start,
      limitnum: PAGE,
    });
    const page = data.products?.product || [];
    services.push(...page);
    const total = Number(data.totalresults || 0);
    if (!page.length || services.length >= total) break;
  }
  return services;
}

(async () => {
  let exitCode = 0;
  try {
    if (!whmcs.isWhmcsConfigured())
      throw new Error("WHMCS API is not configured in .env");

    const rows = [];
    for (const plan of PLANS) {
      const pid = whmcs.getWhmcsProductIdForPlan(plan);
      if (!pid) {
        console.log(
          `(no WHMCS product id configured for plan "${plan}" — skipped)`,
        );
        continue;
      }
      for (const s of await servicesForProduct(pid)) {
        rows.push({ plan, pid: String(pid), service: s });
      }
    }

    const clientIds = [...new Set(rows.map((r) => String(r.service.clientid)))];
    const clients = new Map();
    for (const id of clientIds) {
      try {
        const d = await whmcs.getClientDetails(id);
        const c = d.client || d;
        clients.set(id, {
          email: String(c.email || d.email || "").toLowerCase(),
          name: [c.firstname || d.firstname, c.lastname || d.lastname]
            .filter(Boolean)
            .join(" "),
          company: c.companyname || d.companyname || "",
        });
      } catch (error) {
        clients.set(id, {
          email: "",
          name: `(lookup failed: ${error.message})`,
          company: "",
        });
      }
    }

    const emails = [
      ...new Set([...clients.values()].map((c) => c.email).filter(Boolean)),
    ];
    const owners = emails.length
      ? (
          await pool.query(
            `SELECT lower(a.email) AS email, o.id AS org_id, o.name AS org_name
               FROM admins a
               JOIN organization_users ou ON ou.admin_id = a.id AND ou.role = 'owner'
               JOIN organizations o ON o.id = ou.organization_id
              WHERE lower(a.email) = ANY($1)`,
            [emails],
          )
        ).rows
      : [];
    const subs = clientIds.length
      ? (
          await pool.query(
            `SELECT s.organization_id, s.whmcs_client_id, s.plan_key, s.status, o.name AS org_name
               FROM subscriptions s
               LEFT JOIN organizations o ON o.id = s.organization_id
              WHERE s.whmcs_client_id = ANY($1)`,
            [clientIds],
          )
        ).rows
      : [];
    const orderIds = [
      ...new Set(
        rows.map((r) => String(r.service.orderid || "")).filter(Boolean),
      ),
    ];
    const outcomes = orderIds.length
      ? (
          await pool.query(
            `SELECT whmcs_order_id, outcome FROM processed_whmcs_orders WHERE whmcs_order_id = ANY($1)`,
            [orderIds],
          )
        ).rows
      : [];
    const outcomeByOrder = new Map(
      outcomes.map((o) => [String(o.whmcs_order_id), o.outcome]),
    );

    const table = rows.map(({ plan, service: s }) => {
      const cid = String(s.clientid);
      const c = clients.get(cid) || {};
      const sub = subs.find((x) => String(x.whmcs_client_id) === cid);
      const owner = owners.find((o) => o.email === c.email);
      const link = sub
        ? `LINKED → org ${sub.organization_id} ${sub.org_name || ""} (${sub.plan_key}/${sub.status})`
        : owner
          ? `EMAIL MATCH → org ${owner.org_id} ${owner.org_name}`
          : "NOT IN APP";
      return {
        whmcs_client: cid,
        client: c.company || c.name || "-",
        email: mask(c.email),
        plan,
        service_status: s.status || "-",
        next_due: s.nextduedate || "-",
        order: s.orderid || "-",
        poller_outcome:
          outcomeByOrder.get(String(s.orderid)) || "(not seen by poller)",
        app_link: link,
      };
    });

    if (!table.length) {
      console.log(
        "No WHMCS clients hold a StreamControl product (Essential/Premium/Enterprise).",
      );
    } else {
      console.table(table);
      const notInApp = table.filter((t) => t.app_link === "NOT IN APP").length;
      const emailMatch = table.filter((t) =>
        t.app_link.startsWith("EMAIL MATCH"),
      ).length;
      const linked = table.filter((t) =>
        t.app_link.startsWith("LINKED"),
      ).length;
      console.log(
        `\n${table.length} StreamControl service(s): ${linked} linked, ${emailMatch} email match (linkable), ${notInApp} not in app.`,
      );
    }
    if (!SHOW_EMAILS)
      console.log(
        "(emails masked — use --show-emails to see them; do not share that output)",
      );
  } catch (error) {
    console.error(`ERROR: ${error.message}`);
    exitCode = 1;
  }
  await pool.end().catch(() => {});
  process.exit(exitCode);
})();
