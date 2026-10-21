// Test preload: in-memory ./db answering the three report queries.
const Module = require("module");
const original = Module._load;
Module._load = function (request) {
  if (request === "./db" || request === "./db.js") {
    return {
      async query(sql, params) {
        const q = String(sql);
        if (/FROM admins a/.test(q)) return { rows: [{ email: "pastor@gracechurch.org", org_id: 3794, org_name: "New Church" }].filter((r) => params[0].includes(r.email)) };
        if (/FROM subscriptions s/.test(q)) return { rows: [{ organization_id: 3800, whmcs_client_id: "12", plan_key: "pro", status: "active", org_name: "One Church" }].filter((r) => params[0].includes(r.whmcs_client_id)) };
        if (/FROM processed_whmcs_orders/.test(q)) return { rows: [{ whmcs_order_id: "503", outcome: "unmatched_needs_manual_review" }].filter((r) => params[0].includes(r.whmcs_order_id)) };
        return { rows: [] };
      },
      async end() {},
    };
  }
  return original.apply(this, arguments);
};
