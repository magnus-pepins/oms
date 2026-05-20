-- V46: FX-related reject codes for the auto-FX-on-trade slice
-- (plans/oms-multi-currency-invest-accounts.md §8.6).
--
--   * RISK_FX_REQUIRED        — trade currency differs from source-balance
--                                currency and the source has auto_fx_enabled=false
--                                (or no eligible auto-FX balance). The BFF
--                                picker fails closed; OMS emits this code if
--                                it ever sees a snapshot mismatch the BFF
--                                missed.
--   * RISK_FX_QUOTE_EXPIRED   — order arrived with a quoteId that
--                                FxQuoteService.recall returns null/expired.
--                                Customer-frontend "refresh silently and
--                                reconfirm" gets RATE_MOVED on the BFF side
--                                so OMS only sees this on a genuinely stale
--                                submit (rare).
--   * RISK_FX_STALE_QUOTE     — vendor mid feed older than the staleness
--                                window at quote / accept time. Loud signal
--                                that OmsFxMidSubscriber stopped seeing ticks.
--
-- All three are added with IF NOT EXISTS so re-running on a partially-applied
-- DB is safe (matches V44 pattern).

ALTER TYPE reject_code ADD VALUE IF NOT EXISTS 'RISK_FX_REQUIRED';
ALTER TYPE reject_code ADD VALUE IF NOT EXISTS 'RISK_FX_QUOTE_EXPIRED';
ALTER TYPE reject_code ADD VALUE IF NOT EXISTS 'RISK_FX_STALE_QUOTE';
