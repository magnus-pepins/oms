# Customer BFF ↔ OMS integration

The **Customer API BFF** (Hono server in `customer-frontend`) is the governance
boundary between public clients and internal systems. OMS exposes an **internal**
HTTP surface only (`/internal/v1/**`, `X-OMS-Internal-Key`).

## Implemented proxy (customer-frontend)

- `POST /api/internal/oms/v1/orders` — `customer-frontend/app/api/internal/oms/v1/orders+api.ts` (authenticated; forwards to OMS `POST /internal/v1/orders` with `X-OMS-Internal-Key`). Env on the BFF: `OMS_INTERNAL_BASE_URL`, `OMS_INTERNAL_API_KEY`.
- `GET /api/internal/oms/v1/orders/{orderId}` — `customer-frontend/app/api/internal/oms/v1/orders/[orderId]+api.ts` (authenticated; forwards to OMS `GET /internal/v1/orders/{id}`; returns **404** `{"error":"not_found"}` if the order’s `accountId` does not match the session user, same as missing order; OMS **404** bodies are not passed through). OMS JSON includes **`settlementStatus`**: aggregate of all **`TRADE`** execution settlement enums for that order (`null` when there are no trade executions yet), for §12.9 retail chips.
- **`POST /api/broker-trade`** — `broker-trade+api.ts` validates/sanitizes the JSON body, maps HTTP error/status for OMS failures, then calls **`executeBrokerTrade`** (`lib/brokerTradeService.ts`): optional **`omsOrderId`** / **`oms_order_id`** (UUID) is verified via OMS **`GET /internal/v1/orders/{id}`** (`OMS_INTERNAL_*`), then **`oms_order_id`** is merged into **`alpaca_orders.raw_alpaca_response`** with the Alpaca response (settlement chip in **Trade information**). Supabase insert/update on that path use **`withSupabaseWriteRetry*`** in the service. **`PUT …/broker-trade?orderId=…&action=cancel`** requires a session and rejects cross-user access (**403**) before Alpaca; cancel persistence uses **`lib/brokerTradeAlpacaCancelPersist.ts`**. When **`EXPO_PUBLIC_OMS_BROKER_TRADE_LINK`** (or **`OMS_BROKER_TRADE_LINK`** on the BFF) is enabled, `TradeStockModal` / `TradeCryptoModal` pre-create an OMS order for supported shapes and pass **`omsOrderId`** automatically.
- **`POST /api/basket-trade`** — multi-leg trades call **`executeBrokerTrade`** (`customer-frontend/lib/brokerTradeService.ts`) per component. When the same link flags are on, `basket-trade+api.ts` pre-creates one OMS ingress order per eligible leg via **`POST /internal/v1/orders`** using **`X-OMS-Internal-Key`** (`lib/server/postOmsCreateOrderInternal.ts`; body from **`lib/omsBrokerTradeLink.ts`**), then passes the returned UUID as **`omsOrderId`** into **`executeBrokerTrade`**, which re-verifies ownership and merges **`oms_order_id`** into **`raw_alpaca_response`** (same persistence contract as broker-trade). Legs with **`stop`** / **`stop_limit`** (or other shapes where **`buildOmsIngressOrderBody`** returns null) skip OMS pre-create and Alpaca proceeds without a link for that leg, matching single-trade behaviour.
- Shared URL/key/timeout helpers — `customer-frontend/app/api/internal/oms/v1/omsInternalHttpConstants.ts` (pure) and `omsInternalProxy.ts` (metrics wrappers).
- **Standalone BFF:** `customer-frontend/server/src/routes/oms.ts` registers these paths on Hono so production `/api/*` traffic is **not** served only via the Metro proxy fallback — middleware, logs, and **`GET /metrics`** include OMS proxy calls. Prometheus: `customer_api_oms_proxy_requests_total`, `customer_api_oms_proxy_handler_duration_seconds` (see [Customer API BFF — Prometheus metrics](../../system-documentation/systems/customer-api-bff.md#prometheus-metrics) in system-documentation). **`POST /api/internal/oms/v1/orders`** is also subject to a **stricter per-IP rate limit** than the global `/api/*` bucket (`OMS_ORDERS_POST_RATE_LIMIT_*` on the BFF; does not consume the global counter).
- **Ledger balance binding (POST):** when the JSON body includes a non-empty `ledgerBalanceId`, the BFF loads `user_profiles.ledger_identity_id` (Supabase service role) and `GET {LEDGER_API_URL}/balances/{id}?with_queued=true` (`X-Ledger-Key`); the balance’s Ledger `identityId` must match. The BFF then **injects `ledgerIdentityId`** on the JSON sent to OMS (client-supplied `ledgerIdentityId` is ignored). **OMS** performs the same Ledger GET and compares `ledgerIdentityId` to the balance owner when `oms.ledger.enabled=true` (otherwise requests with `ledgerBalanceId` are rejected). **Anti-probing:** BFF responds with **404** `{"error":"not_found"}` for any outcome where a caller should not learn whether a balance id or identity binding exists (unknown balance, identity mismatch, missing profile identity, Ledger non-OK on balance GET); missing Ledger env → **503**; `accountId` mismatch → **403**. OMS aligns **ledger_identity_mismatch** and **ledger_balance_not_found** with **404**; BFF replaces any OMS **404** on POST and on GET single-order with the same generic body so internal codes are not echoed to clients.

## Intended wiring

1. **Ingress:** BFF validates the customer session, maps the request to OMS
   `CreateOrderRequest`, and calls OMS with the shared internal API key.
2. **Idempotency:** BFF forwards a stable `clientIdempotencyKey` per customer
   intent so OMS can return `200` on safe retries.
3. **Read path:** BFF may read order status from OMS `GET /internal/v1/orders/{id}`
   or from a **projection** (e.g. Supabase) fed by the same domain events; the
   product choice is documented in the platform plan, not enforced here.

## Domain events

Downstream desk / SSE / drop copy consumers should subscribe to the **envelope**
JSON from NATS (subject `oms.events.<Type>` when using default prefix), not to
ad-hoc HTTP-side publishes. The envelope carries `schemaVersion`, `type`,
`occurredAt`, `correlationId` (order UUID), and `payload`.

See [drop-copy-events.md](drop-copy-events.md) for payload field shapes.

**Optional BFF JetStream consumer:** when `BFF_OMS_DOMAIN_EVENTS_CONSUMER_ENABLED=true` on the standalone BFF (same `NATS_URL` as OMS; stream `OMS_EVENTS` must exist), `customer-frontend/lib/server/omsDomainEventsConsumer.ts` runs a durable pull consumer on `oms.events.>` — **metrics + logs** (`customer_api_oms_domain_events_*`); **GET** remains the OMS proxy. Dedupe: Redis `oms:fanout:dedup:*` when `REDIS_URL` is set.
