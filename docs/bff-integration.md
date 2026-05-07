# Customer BFF ↔ OMS integration

The **Customer API BFF** (Hono server in `customer-frontend`) is the governance
boundary between public clients and internal systems. OMS exposes an **internal**
HTTP surface only (`/internal/v1/**`, `X-OMS-Internal-Key`).

## Implemented proxy (customer-frontend)

- `POST /api/internal/oms/v1/orders` — `customer-frontend/app/api/internal/oms/v1/orders+api.ts` (authenticated; forwards to OMS `POST /internal/v1/orders` with `X-OMS-Internal-Key`). Env on the BFF: `OMS_INTERNAL_BASE_URL`, `OMS_INTERNAL_API_KEY`.
- `GET /api/internal/oms/v1/orders/{orderId}` — `customer-frontend/app/api/internal/oms/v1/orders/[orderId]+api.ts` (authenticated; forwards to OMS `GET /internal/v1/orders/{id}`; returns **404** if the order’s `accountId` does not match the session user, same as missing order).
- Shared URL/key/timeout helpers — `customer-frontend/app/api/internal/oms/v1/omsInternalProxy.ts`.

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
