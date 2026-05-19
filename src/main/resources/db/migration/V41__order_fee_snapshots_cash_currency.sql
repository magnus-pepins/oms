-- V41: extend V40 order_fee_snapshots with the cash currency + FX rate the BFF
-- pinned at order acceptance, so SettlementConfirmProcessor can route the cash
-- leg cross-currency (cash-base + cash-quote via @FX-Suspense-<ccy>) when the
-- customer pays in one currency for a trade in another (e.g. EUR-funded
-- customer buying USD-listed AAPL).
--
-- All three columns are nullable so existing rows (pinned before this migration)
-- continue to behave as single-currency cashCcy==tradeCcy at settlement; the
-- processor falls back to OmsConfig.Settlement.defaultCashCurrency when null,
-- matching pre-V41 behaviour.
--
-- fx_rate is the rate that was quoted to the customer at order time
-- (cashCurrency → tradeCurrency, i.e. how many tradeCcy units one cashCcy
-- unit buys). cash_amount is the customer-side notional in cashCcy and is
-- pre-computed by the BFF (cash_amount = notional / fx_rate for BUY,
-- cash_amount = notional * fx_rate for SELL — same convention as
-- FxQuoteService customer quotes). Persisting both keeps settlement
-- deterministic against what the customer agreed to, regardless of how the
-- spot mid moves between trade and settlement.
--
-- NOTE: First-write-wins on retries still applies (V40 PRIMARY KEY on
-- order_id with ON CONFLICT DO NOTHING is unchanged). A BFF retry that
-- arrives after settlement has already read the snapshot is a no-op.

ALTER TABLE order_fee_snapshots
    ADD COLUMN cash_currency TEXT,
    ADD COLUMN cash_amount   NUMERIC(20, 8),
    ADD COLUMN fx_rate       NUMERIC(20, 10);

COMMENT ON COLUMN order_fee_snapshots.cash_currency IS
    'Customer-side payment currency (null = use OmsConfig defaultCashCurrency, single-currency settlement).';
COMMENT ON COLUMN order_fee_snapshots.cash_amount IS
    'Customer-side notional in cash_currency, pre-computed by the BFF using the quoted fx_rate. Null when cash_currency is null.';
COMMENT ON COLUMN order_fee_snapshots.fx_rate IS
    'Rate quoted at order time, cash_currency → trade_currency. Null when cash_currency is null or single-currency.';
