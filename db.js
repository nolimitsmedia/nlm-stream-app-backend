const { Pool } = require("pg");
require("dotenv").config();

const pool = new Pool({
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  database: process.env.DB_NAME,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  ssl:
    process.env.DB_SSL === "true" ? { rejectUnauthorized: false } : undefined,

  // Railway's connection proxy (zephyr.proxy.rlwy.net) silently drops idle
  // TCP connections after a while. Without these, a pooled client that's
  // gone stale throws on its next query and — critically — with no error
  // handler on the pool (see below), that crashes the entire Node process,
  // not just the one request.
  keepAlive: true, // OS-level TCP keepalive packets to prevent the proxy from treating the connection as idle
  idleTimeoutMillis: 30000, // recycle idle clients proactively instead of waiting for Railway to kill them first
  connectionTimeoutMillis: 10000, // fail fast on a hung connection attempt instead of hanging indefinitely
  max: 10,
});

// ══════════════════════════════════════════
// CRITICAL: without this handler, an error on an idle pooled client (e.g.
// Railway dropping the connection) is an unhandled 'error' event on the
// pool's EventEmitter — Node crashes the *entire process* over one dead
// connection. This has been happening repeatedly (visible in pm2 logs as
// the server restarting every few minutes). Logging it here instead lets
// `pg` quietly discard the bad client and grab a fresh one on the next
// query, so a flaky connection degrades one request instead of taking
// down every active stream and every connected admin dashboard.
// ══════════════════════════════════════════
pool.on("error", (err) => {
  console.error("Unexpected error on idle PG client:", err.message);
});

module.exports = pool;
