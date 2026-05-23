-- V58: executions.trade_date and executions.expected_settlement_date.
--
-- Stock-settlement gap plan §5.3 Slice 1.
--
-- These two date columns let the matcher (and any downstream consumer) reason
-- about the calendar correctness of broker_trade_confirm.settlementDate
-- against an OMS-computed expectation, without having to re-derive both dates
-- on every comparison. Both are nullable for backward compatibility — every
-- execution row inserted before this migration ran has NULL, and the matcher
-- currently treats NULL as "expectation unknown, do not raise a break on the
-- date axis". Slice 2 of §5.3 will populate these on backfill (only for trade
-- executions whose orders we still hold).
--
--   trade_date              — calendar date in UTC derived from venue_ts (the
--                             interim choice; the eventual rule moves to the
--                             venue's local timezone once we carry venue MIC
--                             on executions / orders).
--   expected_settlement_date — venue-business-day rollover of `trade_date`
--                             plus the configured settlement cycle
--                             (`OMS_SETTLEMENT_DEFAULT_CYCLE`, default `T+2`).
--                             Holiday awareness is OUT of scope for this
--                             slice and is captured as a TODO on
--                             SettlementDateCalculator. The eventual rule
--                             is per-instrument and lives in
--                             instrument_settlement_profile (§5.3 follow-up
--                             slice).
--
-- We do not add an index here — the columns are written on every TRADE
-- execution insert but the only short-term read consumer is the matcher,
-- which joins by `executions.id`. If a date-range scan emerges we add an
-- index then (gap plan §5.18 "observability and controls").

ALTER TABLE executions
    ADD COLUMN trade_date              DATE,
    ADD COLUMN expected_settlement_date DATE;

COMMENT ON COLUMN executions.trade_date IS
    'Trade date (calendar date the execution happened) — currently UTC; populated by SettlementDateCalculator at TRADE insert time (gap plan §5.3 Slice 1). NULL for executions written before V58.';
COMMENT ON COLUMN executions.expected_settlement_date IS
    'OMS-computed expected settlement date = trade_date + cycle business days (Mon-Fri only, no holiday calendar yet). Populated by SettlementDateCalculator at TRADE insert time (gap plan §5.3 Slice 1); NULL on CANCEL/REJECT/REPLACE rows and on executions written before V58.';
