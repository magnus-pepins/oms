-- Phase E Slice 12a: ISK instrument eligibility reject at control.
ALTER TYPE reject_code ADD VALUE IF NOT EXISTS 'RISK_ISK_INSTRUMENT_NOT_ELIGIBLE';
