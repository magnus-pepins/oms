-- V48: Persist the in-process FxQuoteService quote cache so a quoteId
-- minted just before an OMS restart is still recallable after the
-- restart (P4.1 of system-documentation/plans/fx-tier-quotes-production.md).
--
-- Without this table the cache lived only in the JVM heap, so:
--   * `customer-frontend-api POST /api/internal/oms/v1/orders` could
--     mint a quoteId, hand it back to the client, the operator
--     restarts the ingress instance, the client confirms within the
--     30s validity window — and OMS rejects with
--     `RISK_FX_QUOTE_EXPIRED` because the recall map was empty after
--     boot. Money path failure with no underlying business problem.
--
-- Design:
--   * Same lifetime as the in-memory cache (validityMillis, default 30s)
--   * Wide rows are fine — payload is small, expiry is short
--   * Index on expires_at for the purge sweep
--   * No FK to fx_pair_markups — the cached row is a snapshot of the
--     numbers at mint time, so deleting / changing a markup row later
--     must not break recall
--
-- The in-memory cache stays as a hot read path. This table is the
-- durable mirror — write-through on mint, read-fallback on recall miss.
--
-- flyway:executeInTransaction=true

CREATE TABLE IF NOT EXISTS fx_quote_cache (
    quote_id        TEXT          PRIMARY KEY,
    pair            TEXT          NOT NULL,
    tier            TEXT          NOT NULL,
    bid             NUMERIC(28, 8) NOT NULL CHECK (bid > 0),
    ask             NUMERIC(28, 8) NOT NULL CHECK (ask > 0),
    mid             NUMERIC(28, 8) NOT NULL CHECK (mid > 0),
    captured_at     TIMESTAMPTZ   NOT NULL,
    expires_at      TIMESTAMPTZ   NOT NULL,
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    CONSTRAINT fx_quote_cache_window CHECK (expires_at > captured_at)
);

COMMENT ON TABLE fx_quote_cache IS
    'Persistent mirror of FxQuoteService.cache so a quoteId minted before an OMS restart is still recallable after. Rows expire at expires_at and are purged by FxQuoteService on its own schedule. Snapshots the bid/ask numbers at mint time so subsequent fx_pair_markups changes do not break recall.';

-- The purge sweep does a single index range scan: DELETE WHERE expires_at < NOW().
-- Recall picks one row by PK; no index needed beyond the implicit primary key.
CREATE INDEX IF NOT EXISTS idx_fx_quote_cache_expires
    ON fx_quote_cache (expires_at);
