-- Slice 5 §5.11: reserve additional reject codes for control / risk catalogue breadth.

ALTER TYPE reject_code ADD VALUE IF NOT EXISTS 'RISK_SYMBOL_HALT';
ALTER TYPE reject_code ADD VALUE IF NOT EXISTS 'RISK_CONCENTRATION_LIMIT';
ALTER TYPE reject_code ADD VALUE IF NOT EXISTS 'RISK_MARKET_SESSION_CLOSED';
