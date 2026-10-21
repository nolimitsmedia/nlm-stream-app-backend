// Tests whmcs-clients-report.js against a mock WHMCS API + mock DB. Read-only by design.
const http = require("http"); const path = require("path"); const { spawn } = require("child_process");
const ROOT = path.resolve(__dirname, "..", ".."); const PORT = Number(process.env.WHMCS_TEST_PORT || 19895);
const writes = [];
// pid 67 has 2 pages (limitnum honoured), pid 68 one service, pid 69 none
const services = { 67: [], 68: [{ id: 902, clientid: 12, orderid: 502, pid: 68, status: "Active", nextduedate: "2026-11-01" }], 69: [] };
for (let i = 0; i < 101; i++) services[67].push({ id: 1000 + i, clientid: i === 0 ? 11 : 13, orderid: i === 0 ? 501 : 503, pid: 67, status: "Active", nextduedate: "2026-11-15" });
const clients = { 11: { email: "Pastor@GraceChurch.org", firstname: "Gary", lastname: "Grace", companyname: "Grace Church" },
  12: { email: "admin@onechurch.org", firstname: "Olivia", lastname: "One", companyname: "One Church" },
  13: { email: "media@newlife.org", firstname: "Nate", lastname: "Life", companyname: "New Life Ministries" } };
const srv = http.createServer((req, res) => { let b = ""; req.on("data", (d) => (b += d)); req.on("end", () => {
  const p = new URLSearchParams(b); res.setHeader("content-type", "application/json");
  if (p.get("identifier") !== "id" || p.get("secret") !== "sec") return res.end(JSON.stringify({ result: "error", message: "auth" }));
  const a = p.get("action");
  if (a === "GetClientsProducts") { const all = services[p.get("pid")] || []; const s = Number(p.get("limitstart") || 0), n = Number(p.get("limitnum") || 25);
    return res.end(JSON.stringify({ result: "success", totalresults: all.length, products: { product: all.slice(s, s + n) } })); }
  if (a === "GetClientsDetails") return res.end(JSON.stringify({ result: "success", client: clients[p.get("clientid")], ...clients[p.get("clientid")] }));
  writes.push(a); res.end(JSON.stringify({ result: "error", message: "unexpected action " + a }));
}); });
const run = (args) => new Promise((resolve) => { const c = spawn(process.execPath, ["-r", path.join(__dirname, "db-mock.js"), path.join(ROOT, "whmcs-clients-report.js"), ...args],
  { cwd: ROOT, env: { PATH: process.env.PATH, WHMCS_API_URL: `http://127.0.0.1:${PORT}/includes/api.php`, WHMCS_API_IDENTIFIER: "id", WHMCS_API_SECRET: "sec",
    WHMCS_ESSENTIAL_PRODUCT_ID: "67", WHMCS_PREMIUM_PRODUCT_ID: "68", WHMCS_ENTERPRISE_PRODUCT_ID: "69" } });
  let out = ""; c.stdout.on("data", (d) => (out += d)); c.stderr.on("data", (d) => (out += d)); c.on("exit", (code) => resolve({ code, out })); });
const results = []; const check = (n, c, d) => results.push({ n, pass: !!c, d });
(async () => {
  await new Promise((r) => srv.listen(PORT, "127.0.0.1", r));
  const r = await run([]);
  check("R1 exits 0", r.code === 0, r.code);
  check("R2 all 102 services found (pagination beyond 100 works)", /102 StreamControl service\(s\)/.test(r.out), (r.out.match(/\d+ StreamControl service\(s\)[^\n]*/) || [""])[0]);
  check("R3 LINKED via subscription whmcs_client_id", /LINKED → org 3800 One Church \(pro\/active\)/.test(r.out), "");
  check("R4 EMAIL MATCH via org owner (case-insensitive email)", /EMAIL MATCH → org 3794 New Church/.test(r.out), "");
  check("R5 NOT IN APP for unknown client", /New Life Ministries.*NOT IN APP/.test(r.out), "");
  check("R6 poller outcome shown (manual review)", /unmatched_needs_manual_review/.test(r.out), "");
  check("R7 emails masked by default", /p\*\*\*@gracechurch\.org/.test(r.out) && !/pastor@gracechurch/i.test(r.out), "");
  check("R8 summary counts", /1 linked, 1 email match \(linkable\), 100 not in app/.test(r.out), "");
  const full = await run(["--show-emails"]);
  check("R9 --show-emails reveals emails", /pastor@gracechurch\.org/.test(full.out), "");
  check("R10 read-only: no write actions sent to WHMCS", writes.length === 0, writes.join(","));
  srv.close();
  for (const x of results) console.log(`${x.pass ? "PASS" : "FAIL"}  ${x.n}  |  ${String(x.d ?? "")}`);
  const f = results.filter((x) => !x.pass); console.log(`\n${results.length - f.length}/${results.length} passed`); process.exit(f.length ? 1 : 0);
})();
