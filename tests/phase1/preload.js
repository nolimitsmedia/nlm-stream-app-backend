// NLM Streaming App — Phase 1 security regression harness: preload.
// Loaded with `node -r` into an ISOLATED copy of the backend (see run.js).
// It replaces ./db with an in-memory fake pool driven by fixture.json,
// replaces child_process with a recorder (nothing is ever executed), and
// blocks every outbound connection except the local mock SRS.
// It never reads .env and never touches a real database, CDN, SRS or API.
const fs = require("fs");
// Test preload: isolates server.js from every real external dependency.
const Module = require("module");
const path = require("path");
global.__calls = { spawn: [], exec: [], execSync: [], queries: [] };
global.__db = JSON.parse(fs.readFileSync(process.env.TEST_FIXTURE, "utf8"));

const fakePool = {
  async query(sql, params = []) {
    const q = String(sql);
    global.__calls.queries.push(q.slice(0, 120).replace(/\s+/g, " "));
    const D = global.__db;
    if (/^\s*INSERT INTO organizations[\s\S]*'no-limits-media'[\s\S]*RETURNING/.test(q)) return { rows: [D.orgs[1]], rowCount: 1 };
    if (/slug = 'no-limits-media'/.test(q)) return { rows: [D.orgs[1]], rowCount: 1 };
    if (/FROM organization_users ou\s+JOIN organizations o ON o.id = ou.organization_id\s+WHERE ou.admin_id = \$1/.test(q)) {
      const ids = D.memberships[params[0]] || [];
      return { rows: ids.map((id) => ({ ...D.orgs[id], membership_role: "owner" })), rowCount: ids.length };
    }
    if (/JOIN organization_users ou ON ou.organization_id = o.id\s+WHERE o.id = \$1\s+AND ou.admin_id = \$2/.test(q)) {
      const ok = (D.memberships[params[1]] || []).includes(Number(params[0]));
      return ok ? { rows: [{ ...D.orgs[params[0]], membership_role: "owner" }], rowCount: 1 } : { rows: [], rowCount: 0 };
    }
    if (/'owner' AS membership_role\s+FROM organizations o\s+WHERE o.id = \$1/.test(q)) {
      const o = D.orgs[params[0]]; return { rows: o ? [{ ...o, membership_role: "owner" }] : [], rowCount: o ? 1 : 0 };
    }
    if (/FROM channels c\s+LEFT JOIN media_nodes mn ON mn.id = c.media_node_id\s+WHERE c.organization_id = \$1/.test(q)) {
      const rows = D.channels.filter((c) => c.organization_id === Number(params[0])); return { rows, rowCount: rows.length };
    }
    if (/WHERE stream_key = \$1\s+AND \(\$2::boolean OR organization_id = \$3\)/.test(q)) {
      const rows = D.channels.filter((c) => c.stream_key === params[0] && (params[1] || c.organization_id === Number(params[2])));
      return { rows: rows.slice(0, 1), rowCount: rows.length ? 1 : 0 };
    }
    if (/transcoding_enabled/.test(q) && /WHERE o.id = \$1/.test(q) && !/p\.recording_enabled/.test(q)) {
      const p = D.plans[params[0]] || {}; return { rows: [{ transcoding_enabled: !!p.transcoding_enabled, plan_slug: "test" }], rowCount: 1 };
    }
    if (/SELECT p.max_bitrate_kbps/.test(q)) {
      const p = D.plans[params[0]] || {}; return { rows: [{ max_bitrate_kbps: p.max_bitrate_kbps || 0 }], rowCount: 1 };
    }
    if (/FROM channels\s+WHERE id = \$1\s+AND organization_id = \$2/.test(q)) {
      const rows = D.channels.filter((c) => c.id === Number(params[0]) && c.organization_id === Number(params[1]));
      return { rows, rowCount: rows.length };
    }
    if (/p\.recording_enabled/.test(q) && /LEFT JOIN subscriptions s/.test(q)) {
      return { rows: [{ ...D.orgs[params[0]], status: "active", recording_enabled: true, analytics_enabled: true, plan_key: "premium" }], rowCount: 1 };
    }
    return { rows: [], rowCount: 0 };
  },
  async connect() { return { query: fakePool.query, release() {} }; },
  on() {}, end: async () => {},
};

const realCp = require("child_process");
const { EventEmitter } = require("events");
const fakeChild = () => { const c = new EventEmitter(); c.stdout = new EventEmitter(); c.stderr = new EventEmitter(); c.stdin = { write() {}, end() {} }; c.exitCode = null; c.killed = false; c.pid = 99999; c.kill = () => { c.killed = true; }; return c; };
const fakeCp = {
  ...realCp,
  spawn: (cmd, args = [], opts) => { global.__calls.spawn.push({ cmd, args }); process.stdout.write("[HARNESS-SPAWN] " + JSON.stringify({ cmd, args }) + "\n"); return fakeChild(); },
  exec: (cmd, ...rest) => { global.__calls.exec.push(cmd); process.stdout.write("[HARNESS-EXEC] " + JSON.stringify(cmd) + "\n"); const cb = rest.find((r) => typeof r === "function"); if (cb) setImmediate(() => cb(null, "", "")); return fakeChild(); },
  execSync: (cmd) => { global.__calls.execSync.push(cmd); return Buffer.from("/dev/root 100 50 50 50% /\n"); },
  execFile: (cmd, args, ...rest) => { global.__calls.exec.push(cmd + " " + (args || []).join(" ")); const cb = rest.find((r) => typeof r === "function"); if (cb) setImmediate(() => cb(null, "", "")); return fakeChild(); },
};

const origLoad = Module._load;
Module._load = function (request, parent, isMain) {
  if (request === "child_process" || request === "node:child_process") return fakeCp;
  if (parent && process.env.HARNESS_APP_DIR && String(parent.filename || "").startsWith(process.env.HARNESS_APP_DIR) && (request === "./db" || request === "./db.js")) return fakePool;
  return origLoad.apply(this, arguments);
};

// Network lock-down: only the local mock SRS is reachable.
const realFetch = global.fetch;
global.fetch = async (url, opts) => {
  const u = String(url instanceof URL ? url.href : url?.url || url);
  if (u.startsWith(`http://127.0.0.1:${process.env.MOCK_SRS_PORT}/`)) return realFetch(url, opts);
  throw new Error("BLOCKED_BY_TEST_HARNESS " + u);
};
for (const modName of ["http", "https", "net", "tls"]) {
  const m = require(modName);
  const guard = (orig, name) => function (...a) {
    const opt = a[0];
    const host = typeof opt === "string" ? new URL(opt).hostname : opt?.hostname || opt?.host || (typeof a[1] === "string" ? a[1] : undefined);
    const port = typeof opt === "object" ? String(opt?.port || "") : typeof a[0] === "number" ? String(a[0]) : "";
    if (host && !["127.0.0.1", "localhost"].includes(String(host).split(":")[0])) throw new Error(`BLOCKED_BY_TEST_HARNESS ${modName}.${name} ${host}`);
    return orig.apply(this, a);
  };
  if (m.request) m.request = guard(m.request, "request");
  if (m.get) m.get = guard(m.get, "get");
  if (m.connect) m.connect = guard(m.connect, "connect");
}
