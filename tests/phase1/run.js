// NLM Streaming App — Phase 1 security regression harness.
// Usage (from the backend root):   node tests/phase1/run.js
// Boots the real server.js twice in an isolated temp copy with a mocked DB,
// a recorded child_process and a local mock SRS, then asserts the Phase 1
// fixes over HTTP. Exit code 0 = all checks passed. Safe to run on a
// production host: it never reads .env and makes no outbound connections
// (ports 19850, 15055 and 15056 on 127.0.0.1 must be free; override with
// PHASE1_SRS_PORT / PHASE1_APP_PORT).
// Runner: mock SRS API, boot server.js in an isolated child, run HTTP assertions.
const http = require("http");
const { spawn } = require("child_process");
const fs = require("fs");
const os = require("os");
const path = require("path");

// ---- isolated workspace -------------------------------------------------
// Copies only the backend *.js files (never .env) into a temp directory and
// links node_modules, so dotenv inside db.js finds no real credentials.
const ROOT = path.resolve(__dirname, "..", "..");
const jwt = require(path.join(ROOT, "node_modules", "jsonwebtoken"));
const WORK = fs.mkdtempSync(path.join(os.tmpdir(), "nlm-phase1-"));
const APP = path.join(WORK, "app");
const REC = path.join(WORK, "rec");
fs.mkdirSync(APP);
for (const f of fs.readdirSync(ROOT)) {
  if (f.endsWith(".js")) fs.copyFileSync(path.join(ROOT, f), path.join(APP, f));
}
fs.symlinkSync(path.join(ROOT, "node_modules"), path.join(APP, "node_modules"), "dir");
fs.mkdirSync(path.join(REC, "live"), { recursive: true });
fs.writeFileSync(path.join(REC, "live", "sk_LIVEKEY0000000000000000000000000-1.key"), "KEYDATA\n");
fs.writeFileSync(path.join(REC, "live", "sk_LIVEKEY0000000000000000000000000.m3u8"), "#EXTM3U\n");
fs.writeFileSync(path.join(REC, "live", "replay.mp4"), "MP4DATA\n");
const SECRET = "test-secret-not-production-0123456789abcdef";
const SRS_PORT = Number(process.env.PHASE1_SRS_PORT || 19850);
const LIVE = ["sk_LIVEKEY0000000000000000000000000", "sk_HOUSEKEY000000000000000000000000", "sk_LOWKEY00000000000000000000000000"];

let tick = 0;
const srs = http.createServer((req, res) => {
  tick++;
  res.setHeader("content-type", "application/json");
  if (req.url.startsWith("/api/v1/streams")) {
    return res.end(JSON.stringify({ code: 0, streams: LIVE.map((name, i) => ({ id: "s" + i, name, app: "live", vhost: "__defaultVhost__", publish: { active: true }, clients: 1, video: { codec: "H264" }, audio: { codec: "AAC" }, kbps: { recv_30s: 3000 }, frames: 100 + tick * 30, recv_bytes: 100000 + tick * 50000 })) }));
  }
  if (req.url.startsWith("/api/v1/summaries") || req.url.startsWith("/api/v1/versions")) return res.end(JSON.stringify({ code: 0, data: { ok: true } }));
  res.end(JSON.stringify({ code: 0 }));
});

const tok = (id, role) => jwt.sign({ id, email: `u${id}@test.local`, role }, SECRET, { expiresIn: "1h", algorithm: "HS256" });
const T = { client: tok(20, "admin"), low: tok(30, "admin"), orphan: tok(40, "admin"), sa: tok(1, "super_admin") };

let results = []; let port;
async function req(method, p, { token, org, body } = {}) {
  const headers = { "content-type": "application/json" };
  if (token) headers.authorization = `Bearer ${token}`;
  if (org) headers["x-organization-id"] = String(org);
  const r = await fetch(`http://127.0.0.1:${port}${p}`, { method, headers, body: body ? JSON.stringify(body) : undefined });
  const text = await r.text(); let json = null; try { json = JSON.parse(text); } catch {}
  return { status: r.status, json, text };
}
function check(name, cond, detail) { results.push({ name, pass: !!cond, detail }); }

function boot(extraEnv) {
  return new Promise((resolve, reject) => {
    const env = {
      PATH: process.env.PATH, HOME: WORK, HARNESS_APP_DIR: APP + path.sep, NODE_ENV: "test", PORT: String(port),
      JWT_SECRET: SECRET, DOTENV_CONFIG_PATH: path.join(WORK, "does-not-exist.env"),
      SRS_API_URL: `http://127.0.0.1:${SRS_PORT}`, SRS_INTERNAL_HLS_BASE_URL: `http://127.0.0.1:${SRS_PORT}`, SRS_HLS_ORIGIN: `http://127.0.0.1:${SRS_PORT}`,
      MEDIA_NODE_AGENT_SRS_API_URL: `http://127.0.0.1:${SRS_PORT}`,
      MOCK_SRS_PORT: String(SRS_PORT), TEST_FIXTURE: path.join(__dirname, "fixture.json"),
      RECORDINGS_ROOT: REC, CLIENT_URL: "http://localhost", CORS_ORIGINS: "http://localhost",
      MEDIA_NODE_ID: "1", MAILGUN_ENABLED: "false", ...extraEnv,
    };
    const child = spawn(process.execPath, ["-r", path.join(__dirname, "preload.js"), "server.js"], { cwd: APP, env });
    let log = ""; let done = false;
    const onData = (d) => { log += d; if (!done && /running on|listening|Server running|port\s*\d+/i.test(log) ) { done = true; setTimeout(() => resolve({ child, getLog: () => log }), 1500); } };
    child.stdout.on("data", onData); child.stderr.on("data", onData);
    child.on("exit", (c) => { if (!done) reject(new Error("server exited " + c + "\n" + log.slice(-3000))); });
    setTimeout(() => { if (!done) { done = true; resolve({ child, getLog: () => log }); } }, 20000);
  });
}

(async () => {
  await new Promise((r) => srs.listen(SRS_PORT, "127.0.0.1", r));
  port = Number(process.env.PHASE1_APP_PORT || 15055);
  const s1 = await boot({});
  const health = await req("GET", "/api/health");
  check("Boot: /api/health responds", health.status === 200, health.status);

  // ---------- A. Transcode route ----------
  const sentinel = path.join(WORK, "PWNED_SENTINEL");
  try { fs.unlinkSync(sentinel); } catch {}
  const r0 = await req("POST", "/api/transcode/start", { body: { stream: "sk_LIVEKEY0000000000000000000000000" } });
  check("A1 unauthenticated → 401", r0.status === 401, r0.status);
  const inj = await req("POST", "/api/transcode/start", { token: T.client, body: { stream: `x$(touch ${sentinel})` } });
  check("A2 shell-injection payload → 400", inj.status === 400, inj.status + " " + inj.text);
  const inj2 = await req("POST", "/api/transcode/start", { token: T.client, body: { stream: 'sk_LIVE";id;"' } });
  check("A3 quote-breaking payload → 400", inj2.status === 400, inj2.status);
  const inj3 = await req("POST", "/api/transcode/start", { token: T.client, body: { stream: ["a", "b"] } });
  check("A4 non-string payload → 400", inj3.status === 400, inj3.status);
  const rend = await req("POST", "/api/transcode/start", { token: T.client, body: { stream: "sk_LIVEKEY0000000000000000000000000_720p" } });
  check("A5 rendition key as source → 400", rend.status === 400, rend.status);
  const cross = await req("POST", "/api/transcode/start", { token: T.client, body: { stream: "sk_HOUSEKEY000000000000000000000000" } });
  check("A6 other tenant's stream key → 404", cross.status === 404, cross.status);
  const idle = await req("POST", "/api/transcode/start", { token: T.client, body: { stream: "sk_IDLEKEY0000000000000000000000000" } });
  check("A7 own key, not live → 409", idle.status === 409, idle.status + " " + idle.json?.message);
  const low = await req("POST", "/api/transcode/start", { token: T.low, body: { stream: "sk_LOWKEY00000000000000000000000000" } });
  check("A8 plan without ABR → 409", low.status === 409, low.status + " " + low.json?.message);
  const ok1 = await req("POST", "/api/transcode/start", { token: T.client, body: { stream: "sk_LIVEKEY0000000000000000000000000" } });
  check("A9 own live key → 200, plan ladder started", ok1.status === 200 && JSON.stringify(ok1.json?.started) === JSON.stringify(["top", "720p", "480p"]), ok1.status + " " + JSON.stringify(ok1.json));
  check("A9b response keeps ok/message/input/outputs contract", ok1.json && "ok" in ok1.json && "message" in ok1.json && "input" in ok1.json && Array.isArray(ok1.json.outputs), Object.keys(ok1.json || {}).join(","));
  const sa = await req("POST", "/api/transcode/start", { token: T.sa, body: { stream: "sk_HOUSEKEY000000000000000000000000" } });
  check("A10 super_admin may start any org's stream", sa.status === 200, sa.status + " " + JSON.stringify(sa.json));
  // Wait for the managed workers to actually start (readiness gate + staggered
  // spawn) instead of a fixed sleep: up to 40s, polling the child's log.
  for (let waited = 0; waited < 40000; waited += 1000) {
    const spawned = (s1.getLog().match(/\[HARNESS-SPAWN\] \{"cmd":"ffmpeg"/g) || []).length;
    if (spawned >= 6) break;
    await new Promise((r) => setTimeout(r, 1000));
  }
  const ok2 = await req("POST", "/api/transcode/start", { token: T.client, body: { stream: "sk_LIVEKEY0000000000000000000000000" } });
  check("A11 repeat request does not duplicate workers", ok2.status === 200 && (ok2.json?.started || []).length === 0 && (ok2.json?.already_running || []).length === 3, JSON.stringify(ok2.json));
  check("A12 injection sentinel file was NOT created", !fs.existsSync(sentinel), fs.existsSync(sentinel));

  // ---------- B. Tenant resolution ----------
  const b1 = await req("GET", "/api/channels", { token: T.client });
  const b1ids = (b1.json?.channels || []).map((c) => c.id).sort();
  check("B1 client admin, no org header → own org only (not house org)", b1.status === 200 && JSON.stringify(b1ids) === "[201,202]", b1.status + " " + JSON.stringify(b1ids));
  const b2 = await req("GET", "/api/channels", { token: T.client, org: 1 });
  check("B2 client admin requesting house org → 403", b2.status === 403, b2.status);
  const b3 = await req("GET", "/api/channels", { token: T.client, org: 2 });
  check("B3 client admin with own org header → 200", b3.status === 200 && (b3.json?.channels || []).length === 2, b3.status);
  const b4 = await req("GET", "/api/channels", { token: T.sa });
  check("B4 super_admin, no header → house org (unchanged)", b4.status === 200 && (b4.json?.channels || []).map((c) => c.id).join() === "101", b4.status + " " + JSON.stringify((b4.json?.channels || []).map((c) => c.id)));
  const b5 = await req("GET", "/api/channels", { token: T.sa, org: 2 });
  check("B5 super_admin support mode into org 2 (unchanged)", b5.status === 200 && (b5.json?.channels || []).length === 2, b5.status);
  const b6 = await req("GET", "/api/channels", { token: T.orphan });
  check("B6 admin with no memberships → 403", b6.status === 403, b6.status);
  const b7 = await req("GET", "/api/channels", { token: tok(20, "admin") + "x" });
  check("B7 tampered JWT → 401", b7.status === 401, b7.status);
  const expired = jwt.sign({ id: 20, role: "admin", exp: Math.floor(Date.now() / 1000) - 60 }, SECRET);
  const b8 = await req("GET", "/api/channels", { token: expired });
  check("B8 expired JWT → 401", b8.status === 401, b8.status);

  // ---------- C. /recordings static ----------
  for (const [n, p] of [["key", "/recordings/live/sk_LIVEKEY0000000000000000000000000-1.key"], ["m3u8", "/recordings/live/sk_LIVEKEY0000000000000000000000000.m3u8"], ["mp4", "/recordings/live/replay.mp4"]]) {
    const r = await req("GET", p);
    check(`C1 default: /recordings ${n} not served`, r.status === 404 && !/KEYDATA|EXTM3U|MP4DATA/.test(r.text), r.status);
  }

  // ---------- D. isSrsStreamLive callers ----------
  const d1 = await req("POST", "/api/channels/201/file-broadcast/start", { token: T.client, org: 2, body: { recording_id: 1 } });
  check("D1 file-broadcast on live channel → 400 'already live' (was ReferenceError 500)", d1.status === 400 && /already has a real broadcast live/.test(d1.json?.message || ""), d1.status + " " + d1.json?.message);
  const d2 = await req("POST", "/api/channels/202/manual-recording/start", { token: T.client, org: 2 });
  check("D2 manual-recording on idle channel → 400 'not live' (was ReferenceError 500)", d2.status === 400 && /isn't currently live/.test(d2.json?.message || ""), d2.status + " " + d2.json?.message);
  const d3 = await req("POST", "/api/channels/101/manual-recording/start", { token: T.client, org: 2 });
  check("D3 manual-recording on other tenant's channel → 404", d3.status === 404, d3.status);

  // ---------- E. migration service codedError ----------
  const e1 = await req("GET", "/api/admin/media-nodes/abc/operations", { token: T.sa });
  check("E1 invalid node id → 400 invalid_media_node_id (was ReferenceError 500)", e1.status === 400 && e1.json?.code === "invalid_media_node_id", e1.status + " " + e1.json?.code);
  const e2 = await req("GET", "/api/admin/media-nodes/999/operations", { token: T.sa });
  check("E2 unknown node → 404 media_node_not_found", e2.status === 404 && e2.json?.code === "media_node_not_found", e2.status + " " + e2.json?.code);
  const e3 = await req("GET", "/api/admin/media-nodes/999/operations", { token: T.client });
  check("E3 non-super-admin → 403", e3.status === 403, e3.status);

  const log1 = s1.getLog();
  const spawnLines = log1.split("\n").filter((l) => l.startsWith('[HARNESS-SPAWN] {"cmd":"ffmpeg"'));
  const outputs = spawnLines.map((l) => (l.match(/rtmp:\/\/127\.0\.0\.1:1935\/live\/[A-Za-z0-9_]+/) || [])[0]).filter(Boolean);
  check("F1 ffmpeg launched via spawn() with an argument array", spawnLines.length > 0, spawnLines.length + " spawn calls");
  check("F2 exactly one worker per rendition output (no duplicates)", outputs.length > 0 && new Set(outputs).size === outputs.length, outputs.length + " outputs / " + new Set(outputs).size + " distinct");
  const execFfmpeg = log1.split("\n").filter((l) => l.startsWith("[HARNESS-EXEC]") && /ffmpeg/.test(l));
  check("F3 no ffmpeg command passed to a shell (exec)", execFfmpeg.length === 0, execFfmpeg.length);
  s1.child.kill("SIGKILL");
  fs.writeFileSync(path.join(WORK, "boot1.log"), log1);

  // ---------- C2: legacy flag rollback path ----------
  port = Number(process.env.PHASE1_APP_PORT || 15055) + 1;
  const s2 = await boot({ LEGACY_RECORDINGS_STATIC_ENABLED: "true" });
  const k = await req("GET", "/recordings/live/sk_LIVEKEY0000000000000000000000000-1.key");
  check("C2 legacy flag: .key still blocked", k.status === 404 && !/KEYDATA/.test(k.text), k.status);
  const m = await req("GET", "/recordings/live/sk_LIVEKEY0000000000000000000000000.m3u8");
  check("C3 legacy flag: .m3u8 still blocked", m.status === 404, m.status);
  const mp4 = await req("GET", "/recordings/live/replay.mp4");
  check("C4 legacy flag: .mp4 served (rollback works)", mp4.status === 200 && /MP4DATA/.test(mp4.text), mp4.status);
  const dot = await req("GET", "/recordings/.env");
  check("C5 legacy flag: dotfiles denied", dot.status === 404, dot.status);
  fs.writeFileSync(path.join(WORK, "boot2.log"), s2.getLog());
  s2.child.kill("SIGKILL");
  srs.close();

  // Spawn evidence is in the child's globals; we parse it from a log hook below.
  for (const r of results) console.log(`${r.pass ? "PASS" : "FAIL"}  ${r.name}  |  ${String(r.detail).slice(0, 140)}`);
  console.log(`\nWorkspace (logs): ${WORK}`);
  const failed = results.filter((r) => !r.pass);
  console.log(`\n${results.length - failed.length}/${results.length} passed`);
  process.exit(failed.length ? 1 : 0);
})().catch((e) => { console.error("RUNNER ERROR", e); process.exit(2); });
