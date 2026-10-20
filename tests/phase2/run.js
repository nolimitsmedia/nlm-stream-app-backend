// NLM Streaming App — Phase 2 playback-ID regression harness.
// Usage (from the backend root):   node tests/phase2/run.js
//
// Boots the real server.js (twice) in an isolated temp copy with a mocked DB
// (fixture.json), a recorded child_process, and a local mock SRS that serves
// live HLS playlists, ABR renditions, TS segments and WHEP answers which all
// CONTAIN THE STREAM KEY. It then asserts, over HTTP, that viewers only ever
// see the public playback ID, that every Bunny token signs the exact path the
// viewer requests, and that legacy links / recordings / WHEP behave as
// designed. Exit code 0 = all checks passed.
//
// Safe to run on a production host: never reads .env, makes no outbound
// connections. Ports on 127.0.0.1: 19860 (mock SRS), 15065 and 15066
// (override with PHASE2_SRS_PORT / PHASE2_APP_PORT).

const http = require("http");
const { spawn } = require("child_process");
const crypto = require("crypto");
const fs = require("fs");
const os = require("os");
const path = require("path");

const ROOT = path.resolve(__dirname, "..", "..");
const jwt = require(path.join(ROOT, "node_modules", "jsonwebtoken"));
const FIXTURE = JSON.parse(
  fs.readFileSync(path.join(__dirname, "fixture.json"), "utf8"),
);

const WORK = fs.mkdtempSync(path.join(os.tmpdir(), "nlm-phase2-"));
const APP = path.join(WORK, "app");
const REC = path.join(WORK, "rec");
fs.mkdirSync(APP);
for (const f of fs.readdirSync(ROOT)) {
  if (f.endsWith(".js")) fs.copyFileSync(path.join(ROOT, f), path.join(APP, f));
}
fs.symlinkSync(path.join(ROOT, "node_modules"), path.join(APP, "node_modules"), "dir");

const SECRET = "test-secret-not-production-0123456789abcdef";
const BUNNY_KEY = "test-bunny-token-key";
const SRS_PORT = Number(process.env.PHASE2_SRS_PORT || 19860);
const BASE_PORT = Number(process.env.PHASE2_APP_PORT || 15065);

const [LIVE, IDLE, SHORT] = FIXTURE.channels;
const K = LIVE.stream_key;
const P = LIVE.playback_id;
const UNKNOWN_P = "pb_" + "f".repeat(32);
const RENDITIONS = ["top", "720p", "480p"];

// ---- recordings fixture (RECORDINGS_ROOT/live/<stream>/<file>) ----
fs.mkdirSync(path.join(REC, "live", K), { recursive: true });
fs.writeFileSync(path.join(REC, "live", K, "thumb.jpg"), "JPEGDATA");

// ---- mock SRS ---------------------------------------------------------
const whepCalls = [];
let tick = 0;
const tsBytes = Buffer.alloc(188 * 20, 0xff);
for (let i = 0; i < tsBytes.length; i += 188) tsBytes[i] = 0x47;
const mediaPlaylist = (name) =>
  [
    "#EXTM3U",
    "#EXT-X-VERSION:3",
    "#EXT-X-TARGETDURATION:4",
    `#EXT-X-MEDIA-SEQUENCE:${100 + tick}`,
    "#EXTINF:4.000,",
    `${name}-${100 + tick}.ts`,
    "#EXTINF:4.000,",
    `${name}-${101 + tick}.ts`,
    "#EXTINF:4.000,",
    `${name}-${102 + tick}.ts`,
    "",
  ].join("\n");
const liveNames = () => [
  K,
  ...RENDITIONS.map((r) => `${K}_${r}`),
  SHORT.stream_key,
];

const srs = http.createServer((req, res) => {
  tick++;
  const url = new URL(req.url, "http://x");
  if (url.pathname.startsWith("/api/v1/streams")) {
    res.setHeader("content-type", "application/json");
    return res.end(
      JSON.stringify({
        code: 0,
        streams: liveNames().map((name, i) => ({
          id: "s" + i, name, app: "live", vhost: "__defaultVhost__",
          publish: { active: true }, clients: 1,
          video: { codec: "H264", width: 1280, height: 720 },
          audio: { codec: "AAC" },
          kbps: { recv_30s: 3000 }, frames: 100 + tick * 30,
          recv_bytes: 100000 + tick * 50000,
        })),
      }),
    );
  }
  if (url.pathname.startsWith("/api/v1/")) {
    res.setHeader("content-type", "application/json");
    return res.end(JSON.stringify({ code: 0, data: { ok: true } }));
  }
  if (url.pathname.startsWith("/rtc/v1/whep/")) {
    let body = "";
    req.on("data", (d) => (body += d));
    req.on("end", () => {
      whepCalls.push({ stream: url.searchParams.get("stream"), offer: body });
      res.statusCode = 201;
      res.setHeader("content-type", "application/sdp");
      // Real SRS answers can echo the stream name; include it on purpose.
      res.end(`v=0\r\no=SRS/6 1 2 IN IP4 0.0.0.0\r\ns=live/${url.searchParams.get("stream")}\r\nt=0 0\r\n`);
    });
    return;
  }
  const m3u8 = url.pathname.match(/^\/live\/([^/]+)\.m3u8$/);
  if (m3u8 && liveNames().includes(m3u8[1])) {
    res.setHeader("content-type", "application/vnd.apple.mpegurl");
    return res.end(mediaPlaylist(m3u8[1]));
  }
  const ts = url.pathname.match(/^\/live\/([^/]+)-\d+\.ts$/);
  if (ts && liveNames().includes(ts[1])) {
    res.setHeader("content-type", "video/mp2t");
    return res.end(tsBytes);
  }
  res.statusCode = 404;
  res.end("not found");
});

// ---- helpers ------------------------------------------------------------
const tok = (id, role) =>
  jwt.sign({ id, email: `u${id}@test.local`, role }, SECRET, {
    expiresIn: "1h",
    algorithm: "HS256",
  });
const T = { client: tok(20, "admin"), sa: tok(1, "super_admin") };

let port;
const results = [];
const seenBodies = []; // every viewer-facing response body, for the global scan
function check(name, cond, detail) {
  results.push({ name, pass: !!cond, detail });
}
async function req(method, p, { token, body, contentType, raw } = {}) {
  const headers = {};
  if (token) headers.authorization = `Bearer ${token}`;
  let payload;
  if (body !== undefined) {
    headers["content-type"] = contentType || "application/json";
    payload = typeof body === "string" ? body : JSON.stringify(body);
  }
  const r = await fetch(`http://127.0.0.1:${port}${p}`, { method, headers, body: payload });
  const buf = Buffer.from(await r.arrayBuffer());
  const text = buf.toString("utf8");
  let json = null;
  try { json = JSON.parse(text); } catch {}
  const out = { status: r.status, text, json, buf, headers: r.headers };
  if (!raw) seenBodies.push({ path: p, text });
  return out;
}
function bunnyToken(urlPath, expires) {
  return "HS256-" + crypto
    .createHmac("sha256", BUNNY_KEY)
    .update(`${urlPath}${expires}token_ignore_params=true`)
    .digest("base64")
    .replace(/\+/g, "-").replace(/\//g, "_").replace(/=/g, "");
}
// Verifies the token in a URL signs exactly that URL's path.
function tokenValidFor(urlString) {
  const u = new URL(urlString, "http://cdn.test");
  const t = u.searchParams.get("token");
  const e = u.searchParams.get("expires");
  if (!t || !e) return false;
  return t === bunnyToken(u.pathname, e);
}
function uriLines(manifest) {
  return manifest
    .split("\n")
    .map((l) => l.trim())
    .flatMap((l) => {
      if (!l) return [];
      if (l.startsWith("#")) {
        const m = l.match(/URI="([^"]+)"/);
        return m ? [m[1]] : [];
      }
      return [l];
    });
}
function signRecording(kind, stream, file, expires) {
  const key = crypto.createHmac("sha256", SECRET).update("nlm-public-recording-url-v1").digest();
  return crypto.createHmac("sha256", key).update(`${kind}\n${stream}\n${file}\n${expires}`).digest("base64url");
}

function boot(extraEnv) {
  return new Promise((resolve, reject) => {
    const env = {
      PATH: process.env.PATH, HOME: WORK, NODE_ENV: "test", PORT: String(port),
      HARNESS_APP_DIR: APP + path.sep, JWT_SECRET: SECRET,
      DOTENV_CONFIG_PATH: path.join(WORK, "does-not-exist.env"),
      SRS_API_URL: `http://127.0.0.1:${SRS_PORT}`,
      SRS_INTERNAL_HLS_BASE_URL: `http://127.0.0.1:${SRS_PORT}`,
      SRS_HLS_ORIGIN: `http://127.0.0.1:${SRS_PORT}`,
      MEDIA_NODE_AGENT_SRS_API_URL: `http://127.0.0.1:${SRS_PORT}`,
      MOCK_SRS_PORT: String(SRS_PORT),
      TEST_FIXTURE: path.join(__dirname, "fixture.json"),
      RECORDINGS_ROOT: REC, CLIENT_URL: "http://localhost",
      CORS_ORIGINS: "http://localhost", API_PUBLIC_URL: "https://api.test",
      BUNNY_HLS_TOKEN_KEY: BUNNY_KEY, HLS_CDN_HOSTNAME: "cdn.test",
      MEDIA_NODE_ID: "1", MAILGUN_ENABLED: "false",
      ...extraEnv,
    };
    const child = spawn(process.execPath, ["-r", path.join(__dirname, "preload.js"), "server.js"], { cwd: APP, env });
    let log = ""; let done = false;
    const onData = (d) => {
      log += d;
      if (!done && /API running on/i.test(log)) { done = true; setTimeout(() => resolve({ child, getLog: () => log }), 1500); }
    };
    child.stdout.on("data", onData); child.stderr.on("data", onData);
    child.on("exit", (c) => { if (!done) reject(new Error("server exited " + c + "\n" + log.slice(-3000))); });
    setTimeout(() => { if (!done) { done = true; resolve({ child, getLog: () => log }); } }, 25000);
  });
}

(async () => {
  await new Promise((r) => srs.listen(SRS_PORT, "127.0.0.1", r));
  port = BASE_PORT;
  const s1 = await boot({});
  check("Boot: /api/health", (await req("GET", "/api/health", { raw: true })).status === 200);

  // ---------- A. Watch status ----------
  const w = await req("GET", `/api/public/watch/${P}`);
  check("A1 watch status by playback ID → 200", w.status === 200 && w.json?.ok, w.status);
  check("A2 returns playbackId and channelName", w.json?.playbackId === P && w.json?.channelName === LIVE.name, `${w.json?.playbackId} / ${w.json?.channelName}`);
  check("A3 watch status body contains no stream key", !w.text.includes(K), w.text.includes(K) ? "LEAK" : "clean");
  const topPath = w.json?.transcodingEnabled ? `/api/abr/${P}/master.m3u8` : `/api/hls/${P}.m3u8`;
  check("A4 hlsAuthQs signs the playback-ID manifest path", tokenValidFor(topPath + (w.json?.hlsAuthQs || "")), `${topPath}${(w.json?.hlsAuthQs || "").slice(0, 30)}…`);

  const lw = await req("GET", `/api/public/watch/${K}`);
  check("A5 legacy key link still works → 200", lw.status === 200 && lw.json?.ok, lw.status);
  check("A6 legacy response returns playbackId, no key", lw.json?.playbackId === P && !lw.text.includes(K), lw.json?.playbackId);

  // Deploy safety: an OLD cached WatchPage on a legacy link builds the
  // top-level URL from the key and uses hlsAuthQs; the NEW page uses
  // playbackId + playbackHlsAuthQs. Both must be correctly signed.
  const legacyTop = lw.json?.transcodingEnabled ? `/api/abr/${K}/master.m3u8` : `/api/hls/${K}.m3u8`;
  const pidTop = lw.json?.transcodingEnabled ? `/api/abr/${P}/master.m3u8` : `/api/hls/${P}.m3u8`;
  check("A9 legacy link: hlsAuthQs signs the KEY path (old cached pages keep playing)", tokenValidFor(legacyTop + (lw.json?.hlsAuthQs || "")), "");
  check("A10 legacy link: playbackHlsAuthQs signs the playback-ID path (new page)", tokenValidFor(pidTop + (lw.json?.playbackHlsAuthQs || "")), "");
  check("A11 playback-ID link: playbackHlsAuthQs signs the playback-ID path", tokenValidFor(topPath + (w.json?.playbackHlsAuthQs || "")), "");
  {
    const old = await req("GET", legacyTop + (lw.json?.hlsAuthQs || ""));
    const oUris = uriLines(old.text);
    check("A12 old page following the legacy manifest gets playback-ID URIs with valid tokens, no key", old.status === 200 && !old.text.includes(K) && oUris.length > 0 && oUris.every((u) => u.includes(P) && tokenValidFor(u)), `${old.status} ${oUris.length} URIs`);
  }

  const unk = await req("GET", `/api/public/watch/${UNKNOWN_P}`);
  check("A7 unknown playback ID → 404", unk.status === 404, unk.status);
  const dbl = await req("GET", `/api/hls/seg/live/${P}/${IDLE.playback_id}-1.ts`);
  check("A8 two different playback IDs in one path → 400", dbl.status === 400, dbl.status);

  // ---------- B. Raw HLS chain ----------
  const hm = await req("GET", `/api/hls/${P}.m3u8`);
  check("B1 HLS manifest by playback ID → 200", hm.status === 200, hm.status + " " + hm.text.slice(0, 80));
  check("B2 HLS manifest contains no stream key", hm.status === 200 && !hm.text.includes(K), "");
  const hUris = uriLines(hm.text);
  check("B3 every HLS URI uses the playback ID", hUris.length > 0 && hUris.every((u) => u.includes(P)), hUris.slice(0, 2).join(" | "));
  check("B4 every HLS URI token signs its own path", hUris.length > 0 && hUris.every(tokenValidFor), hUris.length + " URIs");
  if (hUris[0]) {
    const u = new URL(hUris[0], "http://x");
    const seg = await req("GET", u.pathname + u.search, { raw: true });
    check("B5 segment fetched via playback-ID URL → 200 TS bytes", seg.status === 200 && seg.buf[0] === 0x47, `${seg.status} ${seg.buf.length}B`);
  } else check("B5 segment fetched via playback-ID URL", false, "no URI");

  // ---------- C. ABR chain ----------
  let master = null;
  for (let i = 0; i < 6; i++) {
    master = await req("GET", `/api/abr/${P}/master.m3u8`);
    if (master.status === 200) break;
    await new Promise((r) => setTimeout(r, 1500));
  }
  check("C1 ABR master by playback ID → 200", master.status === 200, master.status + " " + master.text.slice(0, 80));
  check("C2 ABR master contains no stream key", !master.text.includes(K), "");
  const mUris = uriLines(master.text);
  check("C3 every rendition URI uses the playback ID and a valid token", mUris.length > 0 && mUris.every((u) => u.includes(P) && tokenValidFor(u)), mUris.slice(0, 1).join(""));
  if (mUris[0]) {
    const ru = new URL(mUris[0], "http://x");
    const rp = await req("GET", ru.pathname + ru.search);
    const rUris = uriLines(rp.text);
    check("C4 rendition playlist → 200, no key, playback-ID URIs with valid tokens", rp.status === 200 && !rp.text.includes(K) && rUris.length > 0 && rUris.every((u) => u.includes(P) && tokenValidFor(u)), `${rp.status} ${rUris.slice(0, 1).join("")}`);
    if (rUris[0]) {
      const su = new URL(rUris[0], "http://x");
      const rs = await req("GET", su.pathname + su.search, { raw: true });
      check("C5 rendition segment via playback-ID URL → 200 TS bytes", rs.status === 200 && rs.buf[0] === 0x47, `${rs.status}`);
    } else check("C5 rendition segment", false, "no URI");
  } else { check("C4 rendition playlist", false, "no master URI"); check("C5 rendition segment", false, "skipped"); }
  const abrStatus = await req("GET", `/api/abr/${P}/status`);
  check("C6 ABR status by playback ID → 200, no key", abrStatus.status === 200 && !abrStatus.text.includes(K), abrStatus.status);
  const sess = await req("GET", `/api/hls/session/${P}`);
  check("C7 HLS session by playback ID → 2xx, no key", sess.status < 300 && !sess.text.includes(K), sess.status);

  // ---------- D. WHEP proxy ----------
  const offer = "v=0\r\no=- 1 2 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\n";
  const wh = await req("POST", `/api/whep/${P}`, { body: offer, contentType: "application/sdp" });
  check("D1 WHEP via playback ID → 201 SDP answer", wh.status === 201 && wh.text.startsWith("v=0"), wh.status);
  check("D2 SRS received the real stream key server-side", whepCalls.at(-1)?.stream === K, whepCalls.at(-1)?.stream === K ? "ok" : String(whepCalls.at(-1)?.stream));
  check("D3 WHEP answer contains no stream key", !wh.text.includes(K), "");
  const whBad = await req("POST", `/api/whep/${P}`, { body: "garbage", contentType: "application/sdp" });
  check("D4 WHEP with invalid SDP → 400", whBad.status === 400, whBad.status);
  const whUnk = await req("POST", `/api/whep/${UNKNOWN_P}`, { body: offer, contentType: "application/sdp" });
  check("D5 WHEP unknown playback ID → 404", whUnk.status === 404, whUnk.status);

  // ---------- E. Viewer session + error report ----------
  const vs = await req("POST", "/api/public/viewers/start", { body: { stream_key: P, viewer_id: "v-test" } });
  check("E1 viewer session start with playback ID → 2xx", vs.status < 300, vs.status + " " + vs.text.slice(0, 120));
  check("E2 viewer session response contains no stream key", !vs.text.includes(K), "");
  const er = await req("POST", "/api/player/error-report", { body: { streamKey: P, errorType: "harnessFatalError", fatal: true, details: "harness" } });
  check("E3 player error report with playback ID → 2xx", er.status < 300, er.status);

  // ---------- F. Embed ----------
  const em = await req("GET", `/api/public/embed/${LIVE.embed_token}/status`);
  check("F1 embed status → 200", em.status === 200 && em.json?.ok, em.status);
  check("F2 embed status has no stream key; streamKey/playbackId = playback ID", !em.text.includes(K) && em.json?.playbackId === P && em.json?.streamKey === P, `${em.json?.streamKey}`);
  const emTop = em.json?.transcodingEnabled ? `/api/abr/${P}/master.m3u8` : `/api/hls/${P}.m3u8`;
  check("F3 embed hlsAuthQs signs the playback-ID manifest path", tokenValidFor(emTop + (em.json?.hlsAuthQs || "")), "");

  // ---------- G. Short legacy key (< 12 chars): inbound only ----------
  const sw = await req("GET", `/api/public/watch/${SHORT.playback_id}`);
  check("G1 short-key channel reachable by playback ID → 200", sw.status === 200, sw.status);
  check("G2 short-key channel: playbackId null (client keeps its URL value)", sw.json && sw.json.playbackId === null, String(sw.json?.playbackId));

  // ---------- H. Signed recording URLs ----------
  const now = Math.floor(Date.now() / 1000);
  const base = `/api/public/recordings/thumbnail?stream=${encodeURIComponent(K)}&file=thumb.jpg`;
  const r0 = await req("GET", base, { raw: true });
  check("H1 recording URL without signature → 403", r0.status === 403, r0.status);
  const r1 = await req("GET", `${base}&expires=${now + 600}&sig=forged`, { raw: true });
  check("H2 forged signature → 403", r1.status === 403, r1.status);
  const r2 = await req("GET", `${base}&expires=${now - 5}&sig=${signRecording("thumbnail", K, "thumb.jpg", now - 5)}`, { raw: true });
  check("H3 expired signature → 403", r2.status === 403, r2.status);
  const r3 = await req("GET", `${base}&expires=${now + 600}&sig=${signRecording("media", K, "thumb.jpg", now + 600)}`, { raw: true });
  check("H4 signature for a different kind (media→thumbnail) → 403", r3.status === 403, r3.status);
  const r4 = await req("GET", `${base}&expires=${now + 600}&sig=${signRecording("thumbnail", K, "thumb.jpg", now + 600)}`, { raw: true });
  check("H5 valid signature → 200 file", r4.status === 200 && r4.text === "JPEGDATA", r4.status);

  // ---------- I. Legacy usage report ----------
  const lu = await req("GET", "/api/admin/playback/legacy-usage", { token: T.sa, raw: true });
  check("I1 legacy usage report (super_admin) counts legacy hits", lu.status === 200 && lu.json?.total >= 1 && lu.json?.legacy_stream_key_playback_enabled === true, JSON.stringify(lu.json?.by_route));
  const luc = await req("GET", "/api/admin/playback/legacy-usage", { token: T.client, raw: true });
  check("I2 legacy usage report denied to client admin → 403", luc.status === 403, luc.status);

  // ---------- J. Global scan ----------
  const leaks = seenBodies.filter((b) => b.text.includes(K)).map((b) => b.path);
  check("J1 stream key absent from EVERY viewer-facing response above", leaks.length === 0, leaks.length ? "LEAK in " + leaks.join(", ") : `${seenBodies.length} responses scanned`);

  fs.writeFileSync(path.join(WORK, "boot1.log"), s1.getLog());
  const vInsert = s1.getLog().split("\n").find((l) => l.startsWith("[HARNESS-VIEWER-INSERT]")) || "";
  const errLine = s1.getLog().split("\n").find((l) => l.includes("[PLAYER-ERROR] harnessFatalError")) || "";
  check("E5 player error logged server-side under the real stream key", errLine.includes(K) && !errLine.includes(P), errLine ? "ok" : "no log line");
  check("E4 viewer session stored with the real stream key (analytics intact)", vInsert.includes(K) && !vInsert.includes(P), vInsert ? "ok" : "no insert seen");
  s1.child.kill("SIGKILL");

  // ---------- K. Legacy playback switched off ----------
  port = BASE_PORT + 1;
  const s2 = await boot({ LEGACY_STREAM_KEY_PLAYBACK: "false" });
  const k1 = await req("GET", `/api/public/watch/${K}`, { raw: true });
  check("K1 legacy off: key link → 404", k1.status === 404, k1.status);
  const k2 = await req("GET", `/api/hls/${K}.m3u8`, { raw: true });
  check("K2 legacy off: key manifest → 404", k2.status === 404, k2.status);
  const k3 = await req("GET", `/api/public/watch/${P}`, { raw: true });
  check("K3 legacy off: playback ID still works → 200", k3.status === 200, k3.status);
  fs.writeFileSync(path.join(WORK, "boot2.log"), s2.getLog());
  s2.child.kill("SIGKILL");
  srs.close();

  for (const r of results) console.log(`${r.pass ? "PASS" : "FAIL"}  ${r.name}  |  ${String(r.detail ?? "").slice(0, 150)}`);
  console.log(`\nWorkspace (logs): ${WORK}`);
  const failed = results.filter((r) => !r.pass);
  console.log(`\n${results.length - failed.length}/${results.length} passed`);
  process.exit(failed.length ? 1 : 0);
})().catch((e) => { console.error("RUNNER ERROR", e); process.exit(2); });
