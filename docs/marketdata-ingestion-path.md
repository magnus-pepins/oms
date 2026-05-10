# Market data path for OMS (HTTP vs native MQTT)

## Decision (Phase 1)

| Mode | Status | Use when |
|------|--------|----------|
| **HTTP adapter** (`oms.marketdata.*`) | **Shipped** | Ops can reach marketdata-platform over TLS from OMS VPC; instruments cache + optional NBBO in `market_context` / control path. |
| **Native MQTT / Chronicle bridge** | **Deferred** | Sub-millisecond co-location or venue-adjacent OMS needs push quotes without HTTP polling; requires shared client library with **marketdata-platform** (sibling repo) and JetStream/MQTT topic contract. |

**Rationale:** HTTP keeps a single operational surface (timeouts, API keys, WireMock in tests) and matches Beard/Ops polling patterns. MQTT remains the publisher’s native transport; a sidecar or BFF may translate to HTTP if OMS cannot open MQTT egress.

## Configuration

See [configuration.md](configuration.md) — **Marketdata HTTP** and **`oms.routing.nbbo-reference-in-market-context-enabled`**.

## If product reopens MQTT

1. Publish a topic map (symbol → subject) from marketdata-platform docs.
2. Add optional **`oms.marketdata.mqtt-url`** (or reuse shared-telemetry NATS) + **`MarketdataMqttQuoteClient`** implementing the same cache interface as **`RestMarketdataPlatformHttpClient`**.
3. Keep HTTP as fallback when MQTT disconnects (explicit dual-path behaviour in ADR).
