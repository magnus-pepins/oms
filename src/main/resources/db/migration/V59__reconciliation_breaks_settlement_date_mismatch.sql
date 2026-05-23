-- V59: extend reconciliation_breaks.break_type CHECK to accept settlement_date_mismatch.
--
-- Stock-settlement gap plan §5.3 Slice 2a. The matcher
-- (BrokerTradeConfirmMatcher) starts comparing broker_trade_confirm.settlement_date
-- against executions.expected_settlement_date (populated by V58 +
-- SettlementDateCalculator). When both are non-null and disagree, and the rest
-- of the economic comparison matched, we open a side break of this type so
-- ops can investigate the calendar drift without blocking the actual trade
-- settlement — which the broker is authoritative on anyway.
--
-- ALTER ... DROP + ADD is fine here: the CHECK constraint is only enforced on
-- INSERT/UPDATE and the table holds no rows with break_type =
-- 'settlement_date_mismatch' yet (the type doesn't exist before this
-- migration). Postgres takes a short ACCESS EXCLUSIVE on
-- reconciliation_breaks for the rewrite — the table is tiny and only ops UI
-- reads from it, so the lock cost is negligible.

ALTER TABLE reconciliation_breaks
    DROP CONSTRAINT reconciliation_breaks_break_type_chk;

ALTER TABLE reconciliation_breaks
    ADD CONSTRAINT reconciliation_breaks_break_type_chk
    CHECK (break_type IN (
        'trade_mismatch',
        'unresolved_confirm',
        'unmatched_execution',
        'cash_mismatch',
        'position_mismatch',
        'corporate_action_mismatch',
        'settlement_date_mismatch'
    ));
