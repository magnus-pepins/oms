-- Generic portfolio attribution on orders.
-- Threaded from order entry (REST/gRPC ingress, later FIX-in) through the Aeron cluster admit
-- wire (AcceptOrderCommand/OrderAdmittedEvent optional append-only tail) into this column by the
-- Postgres projector. Opaque to OMS (the BFF validates investment_portfolios ownership); NULL when
-- the order is unattributed. Generic across products (equities + prediction markets).
-- See system-documentation/plans/generic-portfolio-order-attribution.md.

ALTER TABLE orders
    ADD COLUMN portfolio_id TEXT NULL;

COMMENT ON COLUMN orders.portfolio_id IS
    'Opaque portfolio attribution id (investment_portfolios.id) the order belongs to; NULL when unattributed. Set by the projector from OrderAdmittedEvent.';

-- Per-portfolio order lookups (customer "my orders" filtered by portfolio). Partial index keeps it
-- small: only attributed rows are indexed.
CREATE INDEX idx_orders_portfolio_id
    ON orders (portfolio_id)
    WHERE portfolio_id IS NOT NULL;
