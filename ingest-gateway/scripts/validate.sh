#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
echo "== Node syntax =="
node --check gateway.js
echo "PASS"
echo "== Compose parse =="
if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  docker compose --env-file .env.example config >/dev/null
  echo "PASS"
else
  echo "SKIP: docker compose unavailable"
fi
echo "== Secret hygiene =="
if grep -RInE '(sk_[A-Za-z0-9]{20,}|INGEST_GATEWAY_TOKEN=[A-Za-z0-9_-]{32,})' . --exclude='.env.example' --exclude='validate.sh'; then
  echo "FAIL: possible credential material found"
  exit 1
fi
echo "PASS"
