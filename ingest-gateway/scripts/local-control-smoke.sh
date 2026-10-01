#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
PORT="${GATEWAY_CONTROL_PORT:-15092}"
GATEWAY_CONTROL_PORT="$PORT" INGEST_GATEWAY_TOKEN='' node gateway.js >/tmp/nlm-gateway-smoke.log 2>&1 &
PID=$!
trap 'kill "$PID" 2>/dev/null || true; rm -f /tmp/nlm-gateway-smoke.log' EXIT
sleep 1
curl -fsS "http://127.0.0.1:${PORT}/health" | node -e 'let s="";process.stdin.on("data",d=>s+=d);process.stdin.on("end",()=>{const x=JSON.parse(s);if(!x.ok||x.service!=="nlm-ingest-gateway-control")process.exit(1);console.log("CONTROL_HEALTH=PASS");});'
