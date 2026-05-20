-- V45: extend fx_pair_markups with customer-role tier rows.
--
-- Background
-- ----------
-- V37 + V38 seeded the `fx_pair_markups` grid with **desk-operator** tier
-- vocabulary: `default | retail | affluent | institutional`. That works for
-- the trading-desk Treasury / FX console (operator picks a tier when
-- pricing a manual hedge) but it does NOT match the **customer** role
-- vocabulary used by the customer-frontend:
--   `default | basic | premium | elite | admin | business`
-- (see customer-frontend/supabase/migrations/20260519140000_fees_stock_tiers.sql:14
--  and public.fee_schedules.user_type).
--
-- §11.5.6 Phase 2 of plans/oms-fix-gateway-and-settlement.md and §8 of
-- plans/oms-multi-currency-invest-accounts.md land an MQTT publisher that
-- streams per-tier customer quotes on
--   fx/{BASE}/{QUOTE}/customer/{tier}/quote
-- where `{tier}` is the user's role string verbatim. So we need rows in
-- `fx_pair_markups` keyed by the role values, not the desk vocabulary.
--
-- Strategy
-- --------
-- Keep the desk-operator rows (retail/affluent/institutional) — the
-- trading-desk Treasury console still pulls them via FxQuoteService.quote()
-- with explicit `tier=retail|affluent|institutional` query params, and
-- removing them would silently change the price the desk has been seeing.
--
-- Add a NEW set of rows for the customer roles on the **six majors**
-- already covered by V37 (EURUSD, GBPUSD, USDEUR) and V38 (USDGBP, EURGBP,
-- GBPEUR). For every other pair, FxQuoteService.lookupMarkupBps falls back
-- to tier='default' via its existing waterfall — no migration churn needed
-- for exotic pairs.
--
-- Initial markup map (operators tune in DB after the fact):
--   basic    = same bps as retail        (e.g. EURUSD 20 bps)
--   premium  = midway retail<->affluent  (e.g. EURUSD 15 bps)
--   elite    = same bps as affluent      (e.g. EURUSD 10 bps)
--   admin    = same bps as default       (employee — no preferential v1;
--                                          deliberately conservative so an
--                                          employee placing a trade doesn't
--                                          get a tighter spread than a
--                                          retail customer until the
--                                          fee-desk explicitly decides to)
--   business = same bps as affluent      (e.g. EURUSD 10 bps)
--
-- Idempotent insert via WHERE NOT EXISTS so re-applying on a partially
-- populated DB does not double-insert (matches the V37/V38 pattern).
--
-- flyway:executeInTransaction=true

INSERT INTO fx_pair_markups
    (pair, side, tier, markup_bps, description)
SELECT v.pair, v.side, v.tier, v.markup_bps, v.description
FROM (VALUES
    -- ----------------------------------------------------------------
    -- EURUSD  (V37 had default=20, retail=20, affluent=10, institutional=3)
    -- ----------------------------------------------------------------
    ('EURUSD', 'BID', 'basic',    20.00::numeric(8,2), 'Basic EURUSD bid markup (= retail)'),
    ('EURUSD', 'ASK', 'basic',    20.00::numeric(8,2), 'Basic EURUSD ask markup (= retail)'),
    ('EURUSD', 'BID', 'premium',  15.00::numeric(8,2), 'Premium EURUSD bid markup (mid retail/affluent)'),
    ('EURUSD', 'ASK', 'premium',  15.00::numeric(8,2), 'Premium EURUSD ask markup (mid retail/affluent)'),
    ('EURUSD', 'BID', 'elite',    10.00::numeric(8,2), 'Elite EURUSD bid markup (= affluent)'),
    ('EURUSD', 'ASK', 'elite',    10.00::numeric(8,2), 'Elite EURUSD ask markup (= affluent)'),
    ('EURUSD', 'BID', 'admin',    20.00::numeric(8,2), 'Admin EURUSD bid markup (= default v1)'),
    ('EURUSD', 'ASK', 'admin',    20.00::numeric(8,2), 'Admin EURUSD ask markup (= default v1)'),
    ('EURUSD', 'BID', 'business', 10.00::numeric(8,2), 'Business EURUSD bid markup (= affluent)'),
    ('EURUSD', 'ASK', 'business', 10.00::numeric(8,2), 'Business EURUSD ask markup (= affluent)'),

    -- ----------------------------------------------------------------
    -- GBPUSD  (V37 had default=25, retail=25, affluent=12, institutional=4)
    -- ----------------------------------------------------------------
    ('GBPUSD', 'BID', 'basic',    25.00::numeric(8,2), 'Basic GBPUSD bid markup (= retail)'),
    ('GBPUSD', 'ASK', 'basic',    25.00::numeric(8,2), 'Basic GBPUSD ask markup (= retail)'),
    ('GBPUSD', 'BID', 'premium',  18.00::numeric(8,2), 'Premium GBPUSD bid markup (mid retail/affluent)'),
    ('GBPUSD', 'ASK', 'premium',  18.00::numeric(8,2), 'Premium GBPUSD ask markup (mid retail/affluent)'),
    ('GBPUSD', 'BID', 'elite',    12.00::numeric(8,2), 'Elite GBPUSD bid markup (= affluent)'),
    ('GBPUSD', 'ASK', 'elite',    12.00::numeric(8,2), 'Elite GBPUSD ask markup (= affluent)'),
    ('GBPUSD', 'BID', 'admin',    25.00::numeric(8,2), 'Admin GBPUSD bid markup (= default v1)'),
    ('GBPUSD', 'ASK', 'admin',    25.00::numeric(8,2), 'Admin GBPUSD ask markup (= default v1)'),
    ('GBPUSD', 'BID', 'business', 12.00::numeric(8,2), 'Business GBPUSD bid markup (= affluent)'),
    ('GBPUSD', 'ASK', 'business', 12.00::numeric(8,2), 'Business GBPUSD ask markup (= affluent)'),

    -- ----------------------------------------------------------------
    -- USDEUR  (V37 had default=20, retail=20, affluent=10, institutional=3)
    -- ----------------------------------------------------------------
    ('USDEUR', 'BID', 'basic',    20.00::numeric(8,2), 'Basic USDEUR bid markup (= retail)'),
    ('USDEUR', 'ASK', 'basic',    20.00::numeric(8,2), 'Basic USDEUR ask markup (= retail)'),
    ('USDEUR', 'BID', 'premium',  15.00::numeric(8,2), 'Premium USDEUR bid markup (mid retail/affluent)'),
    ('USDEUR', 'ASK', 'premium',  15.00::numeric(8,2), 'Premium USDEUR ask markup (mid retail/affluent)'),
    ('USDEUR', 'BID', 'elite',    10.00::numeric(8,2), 'Elite USDEUR bid markup (= affluent)'),
    ('USDEUR', 'ASK', 'elite',    10.00::numeric(8,2), 'Elite USDEUR ask markup (= affluent)'),
    ('USDEUR', 'BID', 'admin',    20.00::numeric(8,2), 'Admin USDEUR bid markup (= default v1)'),
    ('USDEUR', 'ASK', 'admin',    20.00::numeric(8,2), 'Admin USDEUR ask markup (= default v1)'),
    ('USDEUR', 'BID', 'business', 10.00::numeric(8,2), 'Business USDEUR bid markup (= affluent)'),
    ('USDEUR', 'ASK', 'business', 10.00::numeric(8,2), 'Business USDEUR ask markup (= affluent)'),

    -- ----------------------------------------------------------------
    -- USDGBP  (V38 had default=25, retail=25, affluent=12, institutional=4)
    -- ----------------------------------------------------------------
    ('USDGBP', 'BID', 'basic',    25.00::numeric(8,2), 'Basic USDGBP bid markup (= retail)'),
    ('USDGBP', 'ASK', 'basic',    25.00::numeric(8,2), 'Basic USDGBP ask markup (= retail)'),
    ('USDGBP', 'BID', 'premium',  18.00::numeric(8,2), 'Premium USDGBP bid markup (mid retail/affluent)'),
    ('USDGBP', 'ASK', 'premium',  18.00::numeric(8,2), 'Premium USDGBP ask markup (mid retail/affluent)'),
    ('USDGBP', 'BID', 'elite',    12.00::numeric(8,2), 'Elite USDGBP bid markup (= affluent)'),
    ('USDGBP', 'ASK', 'elite',    12.00::numeric(8,2), 'Elite USDGBP ask markup (= affluent)'),
    ('USDGBP', 'BID', 'admin',    25.00::numeric(8,2), 'Admin USDGBP bid markup (= default v1)'),
    ('USDGBP', 'ASK', 'admin',    25.00::numeric(8,2), 'Admin USDGBP ask markup (= default v1)'),
    ('USDGBP', 'BID', 'business', 12.00::numeric(8,2), 'Business USDGBP bid markup (= affluent)'),
    ('USDGBP', 'ASK', 'business', 12.00::numeric(8,2), 'Business USDGBP ask markup (= affluent)'),

    -- ----------------------------------------------------------------
    -- EURGBP  (V38 had default=30, retail=30, affluent=15, institutional=5)
    -- ----------------------------------------------------------------
    ('EURGBP', 'BID', 'basic',    30.00::numeric(8,2), 'Basic EURGBP bid markup (= retail)'),
    ('EURGBP', 'ASK', 'basic',    30.00::numeric(8,2), 'Basic EURGBP ask markup (= retail)'),
    ('EURGBP', 'BID', 'premium',  22.00::numeric(8,2), 'Premium EURGBP bid markup (mid retail/affluent)'),
    ('EURGBP', 'ASK', 'premium',  22.00::numeric(8,2), 'Premium EURGBP ask markup (mid retail/affluent)'),
    ('EURGBP', 'BID', 'elite',    15.00::numeric(8,2), 'Elite EURGBP bid markup (= affluent)'),
    ('EURGBP', 'ASK', 'elite',    15.00::numeric(8,2), 'Elite EURGBP ask markup (= affluent)'),
    ('EURGBP', 'BID', 'admin',    30.00::numeric(8,2), 'Admin EURGBP bid markup (= default v1)'),
    ('EURGBP', 'ASK', 'admin',    30.00::numeric(8,2), 'Admin EURGBP ask markup (= default v1)'),
    ('EURGBP', 'BID', 'business', 15.00::numeric(8,2), 'Business EURGBP bid markup (= affluent)'),
    ('EURGBP', 'ASK', 'business', 15.00::numeric(8,2), 'Business EURGBP ask markup (= affluent)'),

    -- ----------------------------------------------------------------
    -- GBPEUR  (V38 had default=30, retail=30, affluent=15, institutional=5)
    -- ----------------------------------------------------------------
    ('GBPEUR', 'BID', 'basic',    30.00::numeric(8,2), 'Basic GBPEUR bid markup (= retail)'),
    ('GBPEUR', 'ASK', 'basic',    30.00::numeric(8,2), 'Basic GBPEUR ask markup (= retail)'),
    ('GBPEUR', 'BID', 'premium',  22.00::numeric(8,2), 'Premium GBPEUR bid markup (mid retail/affluent)'),
    ('GBPEUR', 'ASK', 'premium',  22.00::numeric(8,2), 'Premium GBPEUR ask markup (mid retail/affluent)'),
    ('GBPEUR', 'BID', 'elite',    15.00::numeric(8,2), 'Elite GBPEUR bid markup (= affluent)'),
    ('GBPEUR', 'ASK', 'elite',    15.00::numeric(8,2), 'Elite GBPEUR ask markup (= affluent)'),
    ('GBPEUR', 'BID', 'admin',    30.00::numeric(8,2), 'Admin GBPEUR bid markup (= default v1)'),
    ('GBPEUR', 'ASK', 'admin',    30.00::numeric(8,2), 'Admin GBPEUR ask markup (= default v1)'),
    ('GBPEUR', 'BID', 'business', 15.00::numeric(8,2), 'Business GBPEUR bid markup (= affluent)'),
    ('GBPEUR', 'ASK', 'business', 15.00::numeric(8,2), 'Business GBPEUR ask markup (= affluent)')
) AS v(pair, side, tier, markup_bps, description)
WHERE NOT EXISTS (
    SELECT 1 FROM fx_pair_markups m
    WHERE m.pair = v.pair AND m.side = v.side AND m.tier = v.tier
);
