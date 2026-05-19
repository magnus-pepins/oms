-- V36: indices supporting the operator-driven historical search endpoint
-- (DeskOrderSearchController, GET /internal/v1/desk/orders/search).
--
-- The search SQL's canonical sort is ORDER BY received_at DESC, id DESC with cursor predicate
-- (received_at, id) < (:cursor_received, :cursor_id). Without a matching index, Postgres walks
-- the heap for every page even on selective filter sets — which is fine at Pop's ~5M-row scale
-- but degrades super-linearly as the table grows in production.
--
-- We ship two indices:
--   1. idx_orders_received_at_id — global descending sort + cursor walks. Used when no symbol /
--      account filter is selective, and as the sort backbone for every query.
--   2. idx_orders_symbol_received_at — per-symbol history ("show me every AAPL order"). The
--      desk uses this whenever the operator types a symbol in the search form, which is the
--      single most common search dimension.
--
-- account-scoped queries already have idx_orders_account (V1), which is good enough for the
-- selectivity those queries have at our scale — adding (account_id, received_at DESC) gives
-- marginal improvement at the cost of a third hot-table index. Defer until a real account-page
-- query is shown to be slow.
--
-- CONCURRENTLY: cannot run inside a transaction. Flyway 10 honours the directive below by
-- switching this migration to autocommit. The migration is NOT atomic — if it fails partway,
-- an INVALID index can be left behind and must be DROPped manually before retry. Acceptable
-- because the alternative (plain CREATE INDEX) takes an AccessExclusiveLock on `orders` for
-- the duration of each build, which on a production-shape table can stall the OMS write path
-- for minutes.
-- flyway:executeInTransaction=false

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_orders_received_at_id
    ON orders (received_at DESC, id DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_orders_symbol_received_at
    ON orders (instrument_symbol, received_at DESC);
