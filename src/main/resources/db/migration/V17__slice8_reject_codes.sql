-- Slice 8: additional canonical reject codes (control path + compliance hooks).
ALTER TYPE reject_code ADD VALUE IF NOT EXISTS 'RISK_COMPLIANCE_SANCTIONS';
ALTER TYPE reject_code ADD VALUE IF NOT EXISTS 'RISK_TICK_SIZE_VIOLATION';
ALTER TYPE reject_code ADD VALUE IF NOT EXISTS 'RISK_STP_GATE';
