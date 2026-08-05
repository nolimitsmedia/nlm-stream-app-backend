const { Pool } = require("pg");
require("dotenv").config();

const pool = new Pool({
  host: process.env.DB_HOST,
  port: Number(process.env.DB_PORT || 5432),
  database: process.env.DB_NAME,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  ssl:
    process.env.DB_SSL === "true" ? { rejectUnauthorized: false } : undefined,

  // Railway's public proxy can retire idle TCP connections. Keep sockets
  // alive, recycle idle clients before they grow stale, and allow enough time
  // for a fresh TLS/database connection during short proxy slowdowns.
  keepAlive: true,
  keepAliveInitialDelayMillis: 10000,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 15000,
  query_timeout: 30000,
  statement_timeout: 30000,
  max: Number(process.env.DB_POOL_MAX || 10),
  allowExitOnIdle: false,
});

// An error emitted by an idle pooled client must never crash the Node process.
// pg-pool discards that client; the next query receives a fresh connection.
pool.on("error", (err) => {
  console.warn("[DATABASE] Idle pooled client was discarded:", err.message);
});

module.exports = pool;
