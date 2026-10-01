# NLM Phase 5G.3B Gateway Runtime Architecture

## Purpose

Provide a protocol-aware stable ingest hop without making NGINX/HAProxy a second placement engine.

## Flow

Encoder (RTMP/RTMPS/SRT) -> Gateway SRS 6 -> HTTP on_publish -> Gateway Control -> Backend `/api/internal/ingest/resolve` -> assigned Media Node -> SRS dynamic forward -> assigned node RTMP `/live/<stream>`.

The backend remains authoritative for `media_node_id` and node eligibility. The gateway does not choose a node itself.

## Why SRS 6 dynamic forwarding

The deployed media stack already uses SRS 6. SRS 6 supports dynamic forwarding where a backend API returns the RTMP destination for a newly published stream. This allows the gateway to terminate RTMP/SRT using a real media server and forward the accepted stream to the assigned node without introducing a custom RTMP/SRT parser.

## Fail-closed behavior

`on_publish` calls the backend resolver before accepting the publisher. Unknown stream keys, invalid assignments, unhealthy/draining/stale/full nodes, resolver failure, or gateway authentication failure cause the callback to return a non-zero code and SRS rejects the publisher.

## SRT

SRS terminates SRT on the gateway. The gateway's forwarding hop to the assigned media node is RTMP. SRT ingress must be physically validated in 5G.3C before public activation.

## RTMPS

RTMPS termination is intentionally not included in this package. Production can place the existing NGINX stream/TLS pattern in front of the gateway's RTMP listener after 5G.3C validation. No current NGINX configuration should be changed during 5G.3B.

## Connection semantics

Routing is resolved when the publisher establishes a session. Changing `media_node_id` does not move an already-established publisher connection. Reconnect/handoff behavior remains part of physical multi-node acceptance.
