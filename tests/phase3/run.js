// NLM Streaming App — Phase 3 reliability regression harness.
// Usage (from the backend root):   node tests/phase3/run.js
//
// Boots the real server.js in an isolated temp copy (mocked DB, recorded
// child_process, no .env, no outbound network) against a mock SRS that pages
// its stream list exactly like real SRS: 10 streams unless ?count= is given.
// Asserts:
//   S*  the stale is_live cleanup in on_publish never targets a channel whose
//       stream SRS reports as publishing, never targets the publishing stream
//       itself, is limited to this media node, and is skipped entirely when
//       the SRS publisher list cannot be read;
//   P*  a live channel listed beyond SRS's first page of 10 streams is still
//       seen as live (watch status, playback-ID resolution).
// Exit code 0 = all checks passed. Ports 127.0.0.1:19870 / 15075.

const http = require("http");
const { spawn } = require("child_process");
const fs = require("fs");
const os = require("os");
const path = require("path");

const ROOT = path.resolve(__dirname, "..", "..");
const FIXTURE = JSON.parse(fs.readFileSync(path.join(__dirname, "fixture.json"), "utf8"));
const WORK = fs.mkdtempSync(path.join(os.tmpdir(), "nlm-phase3-"));
const APP = path.join(WORK, "app");
fs.mkdirSync(APP);
for (const f of fs.readdirSync(ROOT)) {
  if (f.endsWith(".js")) fs.copyFileSync(path.join(ROOT, f), path.join(APP, f));
}
fs.symlinkSync(path.join(ROOT, "node_modules"), path.join(APP, "node_modules"), "dir");

const SECRET = "test-secret-not-production-0123456789abcdef";
const SRS_PORT = Number(process.env.PHASE3_SRS_PORT || 19870);
const APP_PORT = Number(process.env.PHASE3_APP_PORT || 15075);
const [LIVE] = FIXTURE.channels;
const K = LIVE.stream_key;
const P = LIVE.playback_id;
const LONG_SERVICE = "sk_LongRunningServiceKey000000000000";

// 12 OTHER publishers first (3 channels x source + 3 renditions), then the
// long-running service, then our live channel: both sit beyond the first 10.
const others = [];
for (let c = 1; c <= 3; c++) {
  const base = `sk_OtherChannel${c}`.padEnd(35, "0");
  others.push(base, `${base}_top`, `${base}_720p`, `${base}_480p`);
}
const publishers = [...others, LONG_SERVICE, K, `${K}_top`, `${K}_720p`, `${K}_480p`];

let srsDown = false;
let tick = 0;
const srs = http.createServer((req, res) => {
  tick++;
  const url = new URL(req.url, "http://x");
  res.setHeader("content-type", "application/json");
  if (url.pathname.startsWith("/api/v1/streams")) {
    if (srsDown) { res.statusCode = 500; return res.end('{"code":1}'); }
    // Real SRS: start=0, count=10 by default; count is raised to at least 10.
    const start = Number(url.searchParams.get("start") || 0);
    const count = Math.max(10, Number(url.searchParams.get("count") || 10));
    const page = publishers.slice(start, start + count);
    return res.end(JSON.stringify({
      code: 0,
      streams: page.map((name, i) => ({
        id: "s" + i, name, app: "live", vhost: "__defaultVhost__",
        publish: { active: true }, clients: 1,
        video: { codec: "H264" }, audio: { codec: "AAC" },
        kbps: { recv_30s: 3000 }, frames: 100 + tick * 30, recv_bytes: 100000 + tick * 50000,
      })),
    }));
  }
  if (url.pathname.startsWith("/api/v1/")) return res.end('{"code":0,"data":{"ok":true}}');
  res.statusCode = 404; res.end("not found");
});

let port = APP_PORT;
const results = [];
const check = (name, cond, detail) => results.push({ name, pass: !!cond, detail });
async function req(method, p, body) {
  const r = await fetch(`http://127.0.0.1:${port}${p}`, {
    method,
    headers: body ? { "content-type": "application/json" } : {},
    body: body ? JSON.stringify(body) : undefined,
  });
  const text = await r.text();
  let json = null; try { json = JSON.parse(text); } catch {}
  return { status: r.status, text, json };
}
function boot() {
  return new Promise((resolve, reject) => {
    const env = {
      PATH: process.env.PATH, HOME: WORK, NODE_ENV: "test", PORT: String(port),
      HARNESS_APP_DIR: APP + path.sep, JWT_SECRET: SECRET,
      DOTENV_CONFIG_PATH: path.join(WORK, "does-not-exist.env"),
      SRS_API_URL: `http://127.0.0.1:${SRS_PORT}`,
      SRS_INTERNAL_HLS_BASE_URL: `http://127.0.0.1:${SRS_PORT}`,
      SRS_HLS_ORIGIN: `http://127.0.0.1:${SRS_PORT}`,
      MEDIA_NODE_AGENT_SRS_API_URL: `http://127.0.0.1:${SRS_PORT}`,
      MOCK_SRS_PORT: String(SRS_PORT), TEST_FIXTURE: path.join(__dirname, "fixture.json"),
      RECORDINGS_ROOT: path.join(WORK, "rec"), CLIENT_URL: "http://localhost",
      CORS_ORIGINS: "http://localhost", MEDIA_NODE_ID: "1", MAILGUN_ENABLED: "false",
    };
    const child = spawn(process.execPath, ["-r", path.join(__dirname, "preload.js"), "server.js"], { cwd: APP, env });
    let log = ""; let done = false;
    const onData = (d) => { log += d; if (!done && /API running on/i.test(log)) { done = true; setTimeout(() => resolve({ child, getLog: () => log }), 1500); } };
    child.stdout.on("data", onData); child.stderr.on("data", onData);
    child.on("exit", (c) => { if (!done) reject(new Error("server exited " + c + "\n" + log.slice(-3000))); });
    setTimeout(() => { if (!done) { done = true; resolve({ child, getLog: () => log }); } }, 25000);
  });
}
const staleResets = (log) => log.split("\n").filter((l) => l.startsWith("[HARNESS-STALE-RESET] ")).map((l) => JSON.parse(l.slice(22)));

(async () => {
  await new Promise((r) => srs.listen(SRS_PORT, "127.0.0.1", r));
  const s = await boot();

  // ---------- S. stale is_live cleanup ----------
  const before = staleResets(s.getLog()).length;
  const pub = await req("POST", "/api/srs/on_publish", { action: "on_publish", app: "live", stream: K, vhost: "__defaultVhost__" });
  check("S1 on_publish answered (mock DB rejects the channel later; cleanup runs before validation)", pub.status === 200, `${pub.status} ${pub.text.slice(0, 60)}`);
  const resets = staleResets(s.getLog()).slice(before);
  check("S2 stale cleanup ran exactly once for this publish", resets.length === 1, `${resets.length}`);
  const r0 = resets[0] || { sql: "", params: [] };
  const [currentKey, activeList, nodeId] = r0.params || [];
  check("S3 publishing stream itself is excluded", currentKey === K, String(currentKey).slice(0, 12));
  check("S4 long-running service still in SRS is excluded (it was #13 in SRS's list)", Array.isArray(activeList) && activeList.includes(LONG_SERVICE), `${(activeList || []).length} active publishers passed`);
  check("S5 full SRS list used (all 17 publishers, not just the first page of 10)", Array.isArray(activeList) && activeList.length === publishers.length, `${(activeList || []).length}/${publishers.length}`);
  check("S6 cleanup limited to this media node (or unassigned)", /media_node_id IS NULL OR media_node_id = \$3/.test(r0.sql) && Number(nodeId) === 1, `node param ${nodeId}`);
  check("S7 cleanup excludes active publishers in SQL", /NOT \(stream_key = ANY\(\$2::text\[\]\)\)/.test(r0.sql) && /stream_key <> \$1/.test(r0.sql), "");

  srsDown = true;
  const beforeDown = staleResets(s.getLog()).length;
  const pubDown = await req("POST", "/api/srs/on_publish", { action: "on_publish", app: "live", stream: K, vhost: "__defaultVhost__" });
  await new Promise((r) => setTimeout(r, 300));
  const log = s.getLog();
  check("S8 SRS unreachable: NO stale cleanup issued (fail safe)", staleResets(log).length === beforeDown, `${staleResets(log).length - beforeDown} issued`);
  check("S9 SRS unreachable: skip is logged and on_publish still answers", /\[LIVE-STATE\] Skipped stale is_live cleanup/.test(log) && pubDown.status === 200, `${pubDown.status}`);
  srsDown = false;

  // ---------- P. SRS stream-list paging ----------
  const w = await req("GET", `/api/public/watch/${P}`);
  check("P1 channel listed beyond SRS's first 10 streams is reported LIVE", w.status === 200 && w.json?.isLive === true, `isLive=${w.json?.isLive}`);
  const srsHits = (s.getLog().match(/count=1000/g) || []).length; // not logged by server; informational only
  void srsHits;

  fs.writeFileSync(path.join(WORK, "boot.log"), s.getLog());
  s.child.kill("SIGKILL");
  srs.close();
  for (const r of results) console.log(`${r.pass ? "PASS" : "FAIL"}  ${r.name}  |  ${String(r.detail ?? "").slice(0, 150)}`);
  console.log(`\nWorkspace (logs): ${WORK}`);
  const failed = results.filter((r) => !r.pass);
  console.log(`\n${results.length - failed.length}/${results.length} passed`);
  process.exit(failed.length ? 1 : 0);
})().catch((e) => { console.error("RUNNER ERROR", e); process.exit(2); });
