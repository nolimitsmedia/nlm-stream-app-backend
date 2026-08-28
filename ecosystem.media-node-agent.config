module.exports = {
  apps: [
    {
      name: "nlm-media-node-agent",
      script: "./media_node_agent.js",
      cwd: "/opt/nlm-stream/backend",
      instances: 1,
      exec_mode: "fork",
      autorestart: true,
      watch: false,
      max_memory_restart: "256M",
      kill_timeout: 5000,
      restart_delay: 3000,
      time: true,
      env: {
        NODE_ENV: "production",
      },
    },
  ],
};
