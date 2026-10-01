# NLM Phase 5G.3B — Gateway Runtime / Transport

This package implements the runtime foundation for stable ingest routing while preserving the backend as the authoritative placement/control plane.

Components:
- `gateway.js`: small authenticated control service; receives SRS callbacks and queries the existing backend resolver.
- `srs/srs-gateway.conf`: SRS 6 ingress gateway configuration using `http_hooks` plus dynamic `forward.backend`.
- `docker-compose.yml`: isolated SRS gateway container.
- `ecosystem.config.js`: PM2 definition for the gateway control service.
- `.env.example`: names/placeholders only; no production secrets.

No production backend file changes are required for this package. Do not set `PUBLIC_INGEST_*` yet.
