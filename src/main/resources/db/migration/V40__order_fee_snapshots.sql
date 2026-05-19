-- V40: per-order fee snapshot pinned by the customer-frontend BFF at order accept.
--
-- WHY THIS TABLE EXISTS
-- ---------------------
-- The fee a customer is *quoted* at confirmation time is computed by the customer
-- BFF (lib/server/resolveStockFee.ts) using a 3-step lookup:
--   1. user_fee_overrides (per-user contract pricing)
--   2. fee_schedules tier row (basic / elite / etc.)
--   3. fee_schedules 'default' row (catch-all)
--
-- Today OMS at settlement time recomputes the fee from a hard-coded default
-- schedule (StockCommissionCalculator), which means the fee leg posted to
-- @Fees-<CCY> can disagree with what the customer was quoted — bad for an
-- elite-tier customer who was promised a lower commission, worse for a
-- per-user override negotiated by sales.
--
-- Threading the resolved fee through Aeron cluster admission (CreateOrderRequest
-- → AcceptOrderCommand → OrderAdmittedEvent → projector) would touch the wire
-- format and replay assumptions of the cluster substrate. Instead we pin it in
-- a dedicated Postgres table keyed by order_id, posted synchronously by OMS's
-- ingress controller (which already has the cluster-assigned order id after
-- admission). The settlement processor reads this row at fee-leg time; absence
-- falls back to StockCommissionCalculator, so deployments where the customer
-- BFF hasn't been upgraded yet still settle (with the default schedule).
--
-- IDEMPOTENCY
-- -----------
-- order_id is the PRIMARY KEY: the BFF retries on a transient OMS failure end
-- up as DO NOTHING. The (uncommon) case where the BFF sends a different fee on
-- a retry is intentionally ignored — first write wins, matching the quote the
-- user actually saw at confirmation.

CREATE TABLE order_fee_snapshots (
    order_id                UUID PRIMARY KEY REFERENCES orders (id) ON DELETE CASCADE,
    fee_amount              NUMERIC(20, 8) NOT NULL,
    fee_currency            TEXT NOT NULL,
    fee_balance_indicator   TEXT NOT NULL,
    fee_tier                TEXT NOT NULL,
    fee_source              TEXT NOT NULL,
    -- fee_source mirrors resolveStockFee.ts StockFeeSource: 'override' | 'tier'
    -- | 'default' | 'no-match'. Useful in operator audit (e.g. "why did this
    -- trade settle at $0.50 instead of $1?" → fee_source='override').
    fee_schedule_id         UUID,
    user_fee_override_id    UUID,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE order_fee_snapshots IS
    'BFF-pinned commission for each accepted order. SettlementConfirmProcessor '
    'reads from here at fee-leg time; absent rows fall back to '
    'StockCommissionCalculator default. See V40 file header for design notes.';
