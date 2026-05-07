-- Optional Ledger balance_id (e.g. balance_...) for buying-power checks when
-- oms.ledger.enabled is true. NULL means the tailer skips the Ledger HTTP gate.
ALTER TABLE orders ADD COLUMN ledger_balance_id TEXT;

COMMENT ON COLUMN orders.ledger_balance_id IS
    'Ledger balance_id used for BUY buying-power checks; see oms.ledger.* and ControlTailer.';
