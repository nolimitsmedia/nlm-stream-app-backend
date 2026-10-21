// NLM Streaming App — Bunny origin-key Edge Rule automation tests.
// Usage (from the backend root):   node tests/bunny-origin-key/run.js
//
// Runs bunny_client.js and backfill-origin-key-rule.js against a local mock
// of Bunny's Core API that validates request bodies against the documented
// EdgeRuleV2Model (ActionType 6 = SetRequestHeader, Trigger Type 0 = Url,
// MatchAny = 0, Guid updates in place). No real Bunny account, database or
// network is used. Port 127.0.0.1:19890 (override BUNNY_TEST_PORT).

const http = require("http");
const path = require("path");
const { spawn } = require("child_process");

const ROOT = path.resolve(__dirname, "..", "..");
const PORT = Number(process.env.BUNNY_TEST_PORT || 19890);
const API_KEY = "test-account-key";
const ORIGIN = "https://192.211.56.162";
const KEY = "a".repeat(32) + "b".repeat(32);
const OLD_KEY = "c".repeat(64);

// ---- mock Bunny API --------------------------------------------------------
let nextZoneId = 200;
let guidSeq = 0;
const calls = [];
const schemaErrors = [];
const zones = new Map();
const rule = (value, pattern, enabled = true) => ({
  Guid: `g-${++guidSeq}`, ActionType: 6, ActionParameter1: "X-NLM-Origin-Key",
  ActionParameter2: value, Triggers: [{ Type: 0, PatternMatches: [pattern], PatternMatchingType: 0 }],
  TriggerMatchingType: 0, Description: "NLM origin key", Enabled: enabled,
});
zones.set(101, { Id: 101, Name: "nlmstream-hls", OriginUrl: ORIGIN, EdgeRules: [rule(KEY, "https://nlmstream-hls.b-cdn.net/*")] });
zones.set(102, { Id: 102, Name: "nlmstream", OriginUrl: ORIGIN + "/", EdgeRules: [rule(OLD_KEY, "https://nlmstream.b-cdn.net/*")] });
zones.set(103, { Id: 103, Name: "nlm-one-church", OriginUrl: ORIGIN, EdgeRules: [] });
zones.set(104, { Id: 104, Name: "nlm-one-church-recordings", OriginUrl: "", StorageZoneId: 9, EdgeRules: [] });
zones.set(105, { Id: 105, Name: "unrelated-site", OriginUrl: "https://example.org", EdgeRules: [] });

function validateRuleBody(b) {
  const e = [];
  if (b.ActionType !== 6) e.push("ActionType must be 6 (SetRequestHeader)");
  if (b.ActionParameter1 !== "X-NLM-Origin-Key") e.push("ActionParameter1 must be the header name");
  if (typeof b.ActionParameter2 !== "string" || !b.ActionParameter2) e.push("ActionParameter2 must be the value");
  if (!Array.isArray(b.Triggers) || !b.Triggers.length) e.push("Triggers required");
  for (const t of b.Triggers || []) {
    if (t.Type !== 0) e.push("Trigger Type must be 0 (Url)");
    if (t.PatternMatchingType !== 0) e.push("PatternMatchingType must be 0 (MatchAny)");
    if (!Array.isArray(t.PatternMatches) || !t.PatternMatches.length) e.push("PatternMatches required");
  }
  if (b.TriggerMatchingType !== 0) e.push("TriggerMatchingType must be 0 (MatchAny)");
  if (b.Enabled !== true) e.push("Enabled must be true");
  const allowed = new Set(["Guid", "ActionType", "ActionParameter1", "ActionParameter2", "ActionParameter3", "Triggers", "ExtraActions", "TriggerMatchingType", "Description", "Enabled", "OrderIndex", "ReadOnly"]);
  for (const k of Object.keys(b)) if (!allowed.has(k)) e.push(`unknown property ${k}`); // additionalProperties: false
  return e;
}

const server = http.createServer((req, res) => {
  let raw = "";
  req.on("data", (d) => (raw += d));
  req.on("end", () => {
    const url = new URL(req.url, "http://x");
    const body = raw ? JSON.parse(raw) : null;
    calls.push({ method: req.method, path: url.pathname, body });
    const send = (code, obj) => { res.statusCode = code; res.setHeader("content-type", "application/json"); res.end(obj === undefined ? "" : JSON.stringify(obj)); };
    if (req.headers.accesskey !== API_KEY) return send(401, { Message: "bad key" });
    let m;
    if (req.method === "GET" && url.pathname === "/pullzone") {
      return send(200, { Items: [...zones.values()].map(({ EdgeRules, ...z }) => z), CurrentPage: 1, TotalItems: zones.size, HasMoreItems: false });
    }
    if ((m = url.pathname.match(/^\/pullzone\/(\d+)$/))) {
      const z = zones.get(Number(m[1]));
      if (!z) return send(404, { Message: "not found" });
      if (req.method === "GET") return send(200, z);
      Object.assign(z, body); return send(200, z);
    }
    if (req.method === "POST" && url.pathname === "/pullzone") {
      const id = ++nextZoneId;
      zones.set(id, { Id: id, Name: body.Name, OriginUrl: body.OriginUrl || "", StorageZoneId: body.StorageZoneId, EdgeRules: [] });
      return send(201, { Id: id, Name: body.Name });
    }
    if ((m = url.pathname.match(/^\/pullzone\/(\d+)\/resetSecurityKey$/))) {
      zones.get(Number(m[1])).SecurityKey = body.SecurityKey; return send(204);
    }
    if ((m = url.pathname.match(/^\/pullzone\/(\d+)\/edgerules\/addOrUpdate$/))) {
      const z = zones.get(Number(m[1]));
      const errs = validateRuleBody(body);
      if (errs.length) { schemaErrors.push(...errs); return send(400, { Message: errs.join("; ") }); }
      if (body.Guid) {
        const i = z.EdgeRules.findIndex((r) => r.Guid === body.Guid);
        if (i < 0) return send(400, { Message: "Guid not found" });
        z.EdgeRules[i] = { ...body };
      } else z.EdgeRules.push({ ...body, Guid: `g-${++guidSeq}` });
      return send(201);
    }
    if (req.method === "POST" && url.pathname === "/storagezone") return send(201, { Id: 9000 + nextZoneId, Name: body.Name, Password: "pw" });
    return send(404, { Message: `no mock for ${req.method} ${url.pathname}` });
  });
});

// ---- helpers ---------------------------------------------------------------
const results = [];
const check = (name, cond, detail) => results.push({ name, pass: !!cond, detail });
const originRules = (id) => zones.get(id).EdgeRules.filter((r) => r.ActionType === 6 && r.ActionParameter1 === "X-NLM-Origin-Key");
const addCalls = () => calls.filter((c) => /edgerules\/addOrUpdate$/.test(c.path));

function runBackfill(args, orgs) {
  return new Promise((resolve) => {
    const preload = path.join(__dirname, "db-mock.js");
    const child = spawn(process.execPath, ["-r", preload, path.join(ROOT, "backfill-origin-key-rule.js"), ...args], {
      cwd: ROOT,
      env: { PATH: process.env.PATH, BUNNY_API_BASE_URL: `http://127.0.0.1:${PORT}`, BUNNY_ACCOUNT_API_KEY: API_KEY,
        BUNNY_ORIGIN_URL: ORIGIN, BUNNY_ORIGIN_KEY: KEY, TEST_ORGS: JSON.stringify(orgs), DOTENV_CONFIG_PATH: "/nonexistent" },
    });
    let out = ""; child.stdout.on("data", (d) => (out += d)); child.stderr.on("data", (d) => (out += d));
    child.on("exit", (code) => resolve({ code, out }));
  });
}

(async () => {
  await new Promise((r) => server.listen(PORT, "127.0.0.1", r));
  process.env.BUNNY_API_BASE_URL = `http://127.0.0.1:${PORT}`;
  process.env.BUNNY_ACCOUNT_API_KEY = API_KEY;
  process.env.BUNNY_ORIGIN_URL = ORIGIN;
  process.env.BUNNY_HLS_TOKEN_KEY = "hls-token-key";
  process.env.BUNNY_ORIGIN_KEY = KEY;
  const bunny = require(path.join(ROOT, "bunny_client.js"));

  // ---- U: automatic rule on new zones + idempotency ----
  const created = await bunny.createPullZoneForOrganization("test-church");
  const nz = zones.get(created.pullZoneId);
  check("U1 new org zone gets Token Authentication (unchanged behaviour)", nz.ZoneSecurityEnabled === true && nz.SecurityKey === "hls-token-key", "");
  const r1 = originRules(created.pullZoneId);
  check("U2 new org zone gets exactly one origin-key Edge Rule", r1.length === 1, `${r1.length} rule(s)`);
  check("U3 rule: SetRequestHeader X-NLM-Origin-Key = current key, Url * MatchAny, enabled", r1[0] && r1[0].ActionParameter2 === KEY && r1[0].Triggers[0].PatternMatches[0] === "*" && r1[0].Enabled, "");
  const before = addCalls().length;
  const again = await bunny.ensureOriginKeyEdgeRule(created.pullZoneId);
  check("U4 re-running on a correct zone changes nothing", again.status === "ok" && addCalls().length === before, `${again.status}`);
  const guid102 = originRules(102)[0].Guid;
  const upd = await bunny.ensureOriginKeyEdgeRule(102);
  check("U5 zone with an OLD key is updated in place (same rule, no duplicate)", upd.status === "updated" && originRules(102).length === 1 && originRules(102)[0].Guid === guid102 && originRules(102)[0].ActionParameter2 === KEY, upd.status);
  originRules(102)[0].ActionParameter2 = OLD_KEY; // restore for the backfill tests
  check("U6 every request body matched Bunny's documented schema", schemaErrors.length === 0, schemaErrors.join("; ") || "no schema errors");
  const provisioned = await bunny.provisionBunnyZonesForOrganization("second-church");
  check("U7 full provisioning (WHMCS signup / admin / wizard path) adds the rule to the live zone", originRules(provisioned.pullZoneId).length === 1 && originRules(provisioned.recordingsPullZoneId).length === 0, "storage-backed recordings zone correctly left alone");

  // ---- B: backfill script ----
  const orgs = [{ id: 3800, name: "One Church", bunny_pull_zone_id: "103" }, { id: 9999, name: "Ghost Org", bunny_pull_zone_id: "777" }];
  const callsBeforeCheck = addCalls().length;
  const chk = await runBackfill([], orgs);
  check("B1 check mode reports each origin zone's state", /✓ nlmstream-hls \[101\] — OK/.test(chk.out) && /✗ nlmstream \[102\] — STALE KEY/.test(chk.out) && /✗ nlm-one-church \[103\] — MISSING/.test(chk.out), "");
  check("B2 check mode ignores storage-backed and unrelated zones", !/recordings \[104\]/.test(chk.out) && !/unrelated-site/.test(chk.out), "");
  check("B3 check mode changes nothing and exits 1 when work is needed", addCalls().length === callsBeforeCheck && chk.code === 1, `exit ${chk.code}`);
  check("B4 DB zone unknown to Bunny is flagged", /organization 9999 \(Ghost Org\) references pull zone 777/.test(chk.out), "");
  const app = await runBackfill(["--apply"], [orgs[0]]);
  check("B5 --apply fixes every origin zone and exits 0", app.code === 0 && /All origin zones carry the current origin key/.test(app.out) && originRules(102)[0].ActionParameter2 === KEY && originRules(103).length === 1, `exit ${app.code}`);
  const app2 = await runBackfill(["--apply"], [orgs[0]]);
  check("B6 --apply is idempotent (second run makes no changes)", app2.code === 0 && !/created|updated/.test(app2.out.replace(/All origin zones.*/, "")), "");
  check("B7 the key never appears in any script output", ![chk.out, app.out, app2.out].some((o) => o.includes(KEY) || o.includes(OLD_KEY)), "");
  check("B8 no request ever violated the documented schema", schemaErrors.length === 0, schemaErrors.join("; ") || "ok");

  // ---- S: safety when misconfigured ----
  const noKey = await runBackfill([], orgs).then(() => null);
  void noKey;
  const missing = await new Promise((resolve) => {
    const child = spawn(process.execPath, ["-e", `process.env.BUNNY_ORIGIN_KEY="";const b=require(${JSON.stringify(path.join(ROOT, "bunny_client.js"))});b.ensureOriginKeyEdgeRule(103).then(()=>console.log("NO-THROW")).catch(e=>console.log("THREW:"+e.message))`], {
      env: { PATH: process.env.PATH, BUNNY_API_BASE_URL: `http://127.0.0.1:${PORT}`, BUNNY_ACCOUNT_API_KEY: API_KEY, BUNNY_ORIGIN_URL: ORIGIN },
    });
    let out = ""; child.stdout.on("data", (d) => (out += d)); child.on("exit", () => resolve(out));
  });
  check("S1 missing BUNNY_ORIGIN_KEY fails loudly (no silent half-provisioned zone)", /THREW:BUNNY_ORIGIN_KEY is not configured/.test(missing), missing.trim().slice(0, 80));

  server.close();
  for (const r of results) console.log(`${r.pass ? "PASS" : "FAIL"}  ${r.name}  |  ${String(r.detail ?? "").slice(0, 120)}`);
  const failed = results.filter((r) => !r.pass);
  console.log(`\n${results.length - failed.length}/${results.length} passed`);
  process.exit(failed.length ? 1 : 0);
})().catch((e) => { console.error("RUNNER ERROR", e); process.exit(2); });
