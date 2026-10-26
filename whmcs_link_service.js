// whmcs_link_service.js — shared logic for connecting EXISTING WHMCS clients
// to organizations in this app (2026-10-26).
//
// Used by:
//   - the super-admin API (GET /api/admin/whmcs/clients, POST
//     /api/admin/whmcs/link/preview, POST /api/admin/whmcs/link) behind the
//     "Import from WHMCS" section of the Client Onboarding page
//   - link-whmcs-client.js (command-line equivalent)
//
// Why: the WHMCS poller only handles new orders (latest 50 active) placed by
// customers who signed up in the app or whose email matches an org owner.
// Clients who bought StreamControl in WHMCS earlier are never picked up.
//
// What applyLink() writes is exactly what server.js
// syncWhmcsOrgSubscription() writes for the poller, in one transaction:
//   organizations.subscription_plan  <- plan of the WHMCS product
//   subscriptions (upsert by organization_id): plan_key, status (mapped from
//     the WHMCS service status), current_period_end (= next due date),
//     whmcs_client_id, whmcs_order_id
//   processed_whmcs_orders(order, org, 'linked_manually') so the poller never
//     re-classifies that order.
// Nothing is ever written to WHMCS.

const HOUSE_SLUG = "no-limits-media";
const STREAMCONTROL_PLANS = ["starter", "pro", "enterprise"];

function validDate(value) {
  const s = String(value || "");
  if (!/^\d{4}-\d{2}-\d{2}/.test(s) || s.startsWith("0000")) return null;
  const d = new Date(s);
  return Number.isNaN(d.getTime()) ? null : d;
}

function clientDisplay(details) {
  const c = details?.client || details || {};
  const name =
    c.companyname ||
    details?.companyname ||
    [c.firstname || details?.firstname, c.lastname || details?.lastname]
      .filter(Boolean)
      .join(" ");
  return {
    email: String(c.email || details?.email || "").toLowerCase(),
    company: c.companyname || details?.companyname || "",
    fullName: [
      c.firstname || details?.firstname,
      c.lastname || details?.lastname,
    ]
      .filter(Boolean)
      .join(" "),
    name: name || "",
  };
}

// Every WHMCS service on a StreamControl product, with the app link state.
async function listStreamControlClients({ pool, whmcs }) {
  const services = [];
  for (const plan of STREAMCONTROL_PLANS) {
    const pid = whmcs.getWhmcsProductIdForPlan(plan);
    if (!pid) continue;
    for (let start = 0; start < 10000; start += 100) {
      const data = await whmcs.callWhmcsApi("GetClientsProducts", {
        pid,
        limitstart: start,
        limitnum: 100,
      });
      const page = data.products?.product || [];
      services.push(...page.map((s) => ({ plan, service: s })));
      if (!page.length || services.length >= Number(data.totalresults || 0))
        break;
    }
  }

  const clientIds = [
    ...new Set(services.map((x) => String(x.service.clientid))),
  ];
  const clients = new Map();
  for (const id of clientIds) {
    try {
      clients.set(id, clientDisplay(await whmcs.getClientDetails(id)));
    } catch (error) {
      clients.set(id, {
        email: "",
        company: "",
        fullName: "",
        name: `(lookup failed: ${error.message})`,
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

  return services.map(({ plan, service: s }) => {
    const cid = String(s.clientid);
    const c = clients.get(cid) || {};
    const sub = subs.find((x) => String(x.whmcs_client_id) === cid);
    const owner = owners.find((o) => o.email === c.email);
    let link = { state: "not_in_app" };
    if (sub) {
      link = {
        state: "linked",
        organization_id: sub.organization_id,
        organization_name: sub.org_name,
        plan_key: sub.plan_key,
        status: sub.status,
      };
    } else if (owner) {
      link = {
        state: "email_match",
        organization_id: owner.org_id,
        organization_name: owner.org_name,
      };
    }
    return {
      whmcs_client_id: cid,
      client_name: c.name || "-",
      company: c.company || "",
      contact_name: c.fullName || "",
      email: c.email || "",
      plan_key: plan,
      service_id: String(s.id),
      product_id: String(s.pid),
      service_status: s.status || "",
      next_due: s.nextduedate || "",
      order_id: s.orderid ? String(s.orderid) : "",
      link,
    };
  });
}

// Validates a link and describes exactly what applyLink() would write.
// Throws (Error with .status) only for invalid input; business-rule issues are
// returned as `problems` (blocking unless force) and `warnings`.
async function previewLink({
  pool,
  whmcs,
  organization,
  whmcsClientId,
  serviceId,
}) {
  const clientArg = String(whmcsClientId || "").trim();
  if (!/^\d+$/.test(clientArg)) {
    const e = new Error("whmcs_client_id must be a numeric WHMCS client id");
    e.status = 400;
    throw e;
  }
  const orgRef = String(organization ?? "").trim();
  if (!orgRef) {
    const e = new Error("organization (id or slug) is required");
    e.status = 400;
    throw e;
  }
  const byId = /^\d+$/.test(orgRef);
  const org = (
    await pool.query(
      byId
        ? `SELECT id, name, slug, subscription_plan, is_active FROM organizations WHERE id = $1`
        : `SELECT id, name, slug, subscription_plan, is_active FROM organizations WHERE slug = $1`,
      [byId ? Number(orgRef) : orgRef],
    )
  ).rows[0];
  if (!org) {
    const e = new Error(`organization "${orgRef}" not found`);
    e.status = 404;
    throw e;
  }
  const currentSubscription =
    (
      await pool.query(
        `SELECT plan_key, status, whmcs_client_id, whmcs_order_id, current_period_end
           FROM subscriptions WHERE organization_id = $1`,
        [org.id],
      )
    ).rows[0] || null;
  const owners = (
    await pool.query(
      `SELECT lower(a.email) AS email FROM admins a
         JOIN organization_users ou ON ou.admin_id = a.id AND ou.role = 'owner'
        WHERE ou.organization_id = $1`,
      [org.id],
    )
  ).rows.map((r) => r.email);

  const client = clientDisplay(await whmcs.getClientDetails(clientArg));
  const products = (await whmcs.getClientsProducts(clientArg)).filter((p) =>
    whmcs.getPlanKeyForWhmcsProductId(p.pid),
  );
  if (!products.length) {
    const e = new Error(
      `WHMCS client ${clientArg} has no StreamControl (Essential/Deluxe/Premium) service`,
    );
    e.status = 422;
    throw e;
  }
  let service;
  if (serviceId) {
    service = products.find((p) => String(p.id) === String(serviceId));
    if (!service) {
      const e = new Error(
        `service ${serviceId} is not a StreamControl service of client ${clientArg}`,
      );
      e.status = 422;
      throw e;
    }
  } else {
    const active = products.filter(
      (p) => String(p.status).toLowerCase() === "active",
    );
    if (active.length > 1) {
      const e = new Error(
        `client ${clientArg} has ${active.length} active StreamControl services (${active.map((p) => p.id).join(", ")}); choose one service`,
      );
      e.status = 422;
      e.serviceIds = active.map((p) => String(p.id));
      throw e;
    }
    service = active[0] || products[0];
  }

  const planKey = whmcs.getPlanKeyForWhmcsProductId(service.pid);
  const status = whmcs.mapWhmcsStatusToSubscriptionStatus(service.status);
  const periodEnd = validDate(service.nextduedate);
  const orderId = service.orderid ? String(service.orderid) : null;

  const problems = [];
  const warnings = [];
  if (!org.is_active) problems.push(`organization ${org.id} is not active`);
  if (
    currentSubscription?.whmcs_client_id &&
    String(currentSubscription.whmcs_client_id) !== clientArg
  ) {
    problems.push(
      `organization is already linked to WHMCS client ${currentSubscription.whmcs_client_id}`,
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
      `WHMCS client ${clientArg} is already linked to ${otherOrgs.map((o) => `org ${o.organization_id} (${o.name})`).join(", ")}`,
    );
  }
  if (org.slug === HOUSE_SLUG) {
    problems.push(
      `"${org.name}" is the house organization; linking changes its plan from "${org.subscription_plan}" to "${planKey}"`,
    );
  }
  if (status === "canceled")
    warnings.push(
      `the WHMCS service is ${service.status}; the subscription will be marked canceled`,
    );
  if (!owners.length)
    warnings.push("the organization has no owner account yet");
  else if (client.email && !owners.includes(client.email)) {
    warnings.push(
      "the organization owner's email differs from the WHMCS client email",
    );
  }
  if (!periodEnd)
    warnings.push(
      `WHMCS next due date "${service.nextduedate || ""}" is not a valid date; the current period end is left unchanged`,
    );

  return {
    organization: org,
    currentSubscription,
    ownerEmails: owners,
    client: { id: clientArg, ...client },
    service: {
      id: String(service.id),
      product_id: String(service.pid),
      status: service.status,
      next_due: service.nextduedate || "",
      order_id: orderId,
    },
    planned: {
      plan_key: planKey,
      status,
      current_period_end: periodEnd
        ? periodEnd.toISOString().slice(0, 10)
        : null,
      whmcs_client_id: clientArg,
      whmcs_order_id: orderId,
    },
    problems,
    warnings,
    blocked: problems.length > 0,
  };
}

// Applies a previewed link. Refuses when the preview has problems unless
// force is true. Returns { preview, after }.
async function applyLink({
  pool,
  whmcs,
  organization,
  whmcsClientId,
  serviceId,
  force = false,
}) {
  const preview = await previewLink({
    pool,
    whmcs,
    organization,
    whmcsClientId,
    serviceId,
  });
  if (preview.blocked && !force) {
    const e = new Error(`not linked: ${preview.problems.join("; ")}`);
    e.status = 409;
    e.preview = preview;
    throw e;
  }
  const org = preview.organization;
  const {
    plan_key: planKey,
    status,
    whmcs_client_id: clientId,
    whmcs_order_id: orderId,
  } = preview.planned;
  const periodEnd = preview.planned.current_period_end
    ? new Date(preview.planned.current_period_end)
    : null;

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
      [org.id, planKey, status, periodEnd, clientId, orderId],
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
      `SELECT o.id AS organization_id, o.subscription_plan, s.plan_key, s.status, s.whmcs_client_id, s.whmcs_order_id, s.current_period_end
         FROM organizations o JOIN subscriptions s ON s.organization_id = o.id WHERE o.id = $1`,
      [org.id],
    )
  ).rows[0];
  return { preview, after };
}

module.exports = {
  HOUSE_SLUG,
  listStreamControlClients,
  previewLink,
  applyLink,
};
