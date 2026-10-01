module.exports = {
  apps: [
    {
      name: "nlm-ingest-gateway-control",
      script: "./gateway.js",
      cwd: __dirname,
      instances: 1,
      exec_mode: "fork",
      autorestart: true,
      watch: false,
      max_memory_restart: "256M",
      env: { NODE_ENV: "production" },
    },
  ],
};
