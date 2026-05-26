-- ISK Slice ISK-B (gap plan §5.10 / I3): BUY funding gate reject code.
--
-- Reject is emitted by IskFundingGate (in ControlRiskEvaluator) when an order
-- against an ISK-wrapped account carries a ledgerBalanceId that is not the
-- ISK's own ledger balance recorded in oms_account_tax_wrapper. Without this
-- gate the BFF could pick a non-ISK cash balance for an ISK BUY, silently
-- routing settlement cash through the wrong wrapper.
--
-- ADD VALUE IF NOT EXISTS is idempotent on PG 12+; mirrors V74 (RISK_ISK_
-- INSTRUMENT_NOT_ELIGIBLE) and V50 (RISK_FX_TIER_KILLED).

ALTER TYPE reject_code ADD VALUE IF NOT EXISTS 'RISK_ISK_FUNDING_MISMATCH';
