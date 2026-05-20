-- SELL pre-trade: order quantity exceeds available position.
ALTER TYPE reject_code ADD VALUE IF NOT EXISTS 'RISK_INSUFFICIENT_POSITION';
