# Production Rollout Gate

This package is NOT an instruction to activate public ingest yet.

Before activation:

1. Install the gateway control service on a dedicated gateway host or isolated validation host.
2. Install the same dedicated `INGEST_GATEWAY_TOKEN` as the backend without printing it.
3. Confirm HTTPS reachability from gateway control to the backend resolver.
4. Start Gateway SRS on non-conflicting validation ports first.
5. Validate RTMP publish -> resolver -> dynamic forward -> Node01.
6. Validate SRT publish -> resolver -> dynamic forward -> Node01.
7. Validate unknown-key rejection.
8. Validate resolver outage rejection.
9. Validate draining/unhealthy node rejection using controlled state only when appropriate.
10. Do not enable `PUBLIC_INGEST_*` until the gateway has passed physical validation.
11. Do not change DNS, OBS, production SRS listeners, NGINX, or firewall during local/static 5G.3B validation.
12. Node02 and cross-node handoff remain deferred until a real second node exists.
