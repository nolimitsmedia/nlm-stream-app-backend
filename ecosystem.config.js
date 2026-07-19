module.exports = {
  apps: [
    {
      name: "nlm-stream-backend",
      script: "server.js",
      instances: 1,        // IMPORTANT: keep at 1 — do not increase until Redis is added for Socket.io
      exec_mode: "fork",   // not "cluster"
      autorestart: true,
      watch: false,
      max_memory_restart: "500M",
      env: {
        NODE_ENV: "production",
      },
    },
  ],
};
