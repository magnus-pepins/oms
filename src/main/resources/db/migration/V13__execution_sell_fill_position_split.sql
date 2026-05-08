-- Persist how a SELL trade fill was sourced from pending-buy vs settled buckets so
-- operator mark-failed can reverse the position row exactly (slice 6 / §12.7).

ALTER TABLE executions
    ADD COLUMN sell_position_from_pending_buy NUMERIC(28, 10),
    ADD COLUMN sell_position_from_settled NUMERIC(28, 10);

COMMENT ON COLUMN executions.sell_position_from_pending_buy IS
    'For TRADE rows on SELL orders: quantity taken from quantity_pending_buy_settle when the fill was applied (nullable for BUY / legacy rows).';

COMMENT ON COLUMN executions.sell_position_from_settled IS
    'For TRADE rows on SELL orders: quantity taken from quantity_settled when the fill was applied (nullable for BUY / legacy rows).';
