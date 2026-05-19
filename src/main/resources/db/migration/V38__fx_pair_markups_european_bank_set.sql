-- V38: extend fx_pair_markups to the European-bank pair set.
--
-- V37 seeded 3 pairs (EURUSD / GBPUSD / USDEUR) with a full tier grid.
-- That left FxQuoteService.lookupMarkupBps() throwing "no markup row for X"
-- on every other pair the trading-desk Treasury page now displays
-- (USDGBP, EURGBP, GBPEUR plus the broader European-bank set added in
-- ts/marketdata-platform/src/config/massive-fx-limits.ts).
--
-- This migration ships markup rows for the full 38-pair European-bank
-- catalogue advertised by FxQuoteService.STUB_MIDS:
--
--   * The 6 majors the demo actively transacts in (EURUSD, GBPUSD, USDEUR,
--     USDGBP, EURGBP, GBPEUR) get the full per-tier grid
--     (default + retail + affluent + institutional), letting the demo
--     show role-aware pricing on the customer side.
--   * All other pairs get a single `default`-tier row per side; the
--     lookup waterfall in lookupMarkupBps() falls back to `default` when
--     the requested tier has no row, so quotes always resolve without an
--     operator pre-populating the full grid for exotic pairs.
--
-- Markup levels follow the same shape as V37: tighter for liquid majors,
-- wider for crosses, widest for EM / CEE / TRY. Numbers are demo-realistic,
-- not a real PB sheet — the desk can tune individual rows in beard-admin
-- once the per-pair fee UI lands. retail = same as default so an
-- unauthenticated quote returns a sane price out of the box.
--
-- Idempotent insert via WHERE NOT EXISTS so re-applying on a partially
-- populated DB does not double-insert (matches V37's pattern).
--
-- flyway:executeInTransaction=true

INSERT INTO fx_pair_markups
    (pair, side, tier, markup_bps, description)
SELECT v.pair, v.side, v.tier, v.markup_bps, v.description
FROM (VALUES
    -- ----------------------------------------------------------------
    -- Full tier grid: the 3 majors V37 did not cover.
    -- ----------------------------------------------------------------
    ('USDGBP', 'BID', 'default',       25.00::numeric(8,2), 'Default USDGBP bid markup'),
    ('USDGBP', 'ASK', 'default',       25.00::numeric(8,2), 'Default USDGBP ask markup'),
    ('USDGBP', 'BID', 'retail',        25.00::numeric(8,2), 'Retail USDGBP bid markup'),
    ('USDGBP', 'ASK', 'retail',        25.00::numeric(8,2), 'Retail USDGBP ask markup'),
    ('USDGBP', 'BID', 'affluent',      12.00::numeric(8,2), 'Affluent USDGBP bid markup'),
    ('USDGBP', 'ASK', 'affluent',      12.00::numeric(8,2), 'Affluent USDGBP ask markup'),
    ('USDGBP', 'BID', 'institutional',  4.00::numeric(8,2), 'Institutional USDGBP bid markup'),
    ('USDGBP', 'ASK', 'institutional',  4.00::numeric(8,2), 'Institutional USDGBP ask markup'),

    ('EURGBP', 'BID', 'default',       30.00::numeric(8,2), 'Default EURGBP bid markup'),
    ('EURGBP', 'ASK', 'default',       30.00::numeric(8,2), 'Default EURGBP ask markup'),
    ('EURGBP', 'BID', 'retail',        30.00::numeric(8,2), 'Retail EURGBP bid markup'),
    ('EURGBP', 'ASK', 'retail',        30.00::numeric(8,2), 'Retail EURGBP ask markup'),
    ('EURGBP', 'BID', 'affluent',      15.00::numeric(8,2), 'Affluent EURGBP bid markup'),
    ('EURGBP', 'ASK', 'affluent',      15.00::numeric(8,2), 'Affluent EURGBP ask markup'),
    ('EURGBP', 'BID', 'institutional',  5.00::numeric(8,2), 'Institutional EURGBP bid markup'),
    ('EURGBP', 'ASK', 'institutional',  5.00::numeric(8,2), 'Institutional EURGBP ask markup'),

    ('GBPEUR', 'BID', 'default',       30.00::numeric(8,2), 'Default GBPEUR bid markup'),
    ('GBPEUR', 'ASK', 'default',       30.00::numeric(8,2), 'Default GBPEUR ask markup'),
    ('GBPEUR', 'BID', 'retail',        30.00::numeric(8,2), 'Retail GBPEUR bid markup'),
    ('GBPEUR', 'ASK', 'retail',        30.00::numeric(8,2), 'Retail GBPEUR ask markup'),
    ('GBPEUR', 'BID', 'affluent',      15.00::numeric(8,2), 'Affluent GBPEUR bid markup'),
    ('GBPEUR', 'ASK', 'affluent',      15.00::numeric(8,2), 'Affluent GBPEUR ask markup'),
    ('GBPEUR', 'BID', 'institutional',  5.00::numeric(8,2), 'Institutional GBPEUR bid markup'),
    ('GBPEUR', 'ASK', 'institutional',  5.00::numeric(8,2), 'Institutional GBPEUR ask markup'),

    -- ----------------------------------------------------------------
    -- Default-only rows for the rest of the European-bank catalogue.
    -- Per-tier overrides can be added in beard-admin without another
    -- migration.
    -- ----------------------------------------------------------------
    -- EUR base (excluding EURUSD + EURGBP already covered above / V37)
    ('EURCHF', 'BID', 'default', 25.00::numeric(8,2), 'Default EURCHF bid markup'),
    ('EURCHF', 'ASK', 'default', 25.00::numeric(8,2), 'Default EURCHF ask markup'),
    ('EURJPY', 'BID', 'default', 30.00::numeric(8,2), 'Default EURJPY bid markup'),
    ('EURJPY', 'ASK', 'default', 30.00::numeric(8,2), 'Default EURJPY ask markup'),
    ('EURSEK', 'BID', 'default', 35.00::numeric(8,2), 'Default EURSEK bid markup'),
    ('EURSEK', 'ASK', 'default', 35.00::numeric(8,2), 'Default EURSEK ask markup'),
    ('EURNOK', 'BID', 'default', 35.00::numeric(8,2), 'Default EURNOK bid markup'),
    ('EURNOK', 'ASK', 'default', 35.00::numeric(8,2), 'Default EURNOK ask markup'),
    ('EURDKK', 'BID', 'default', 15.00::numeric(8,2), 'Default EURDKK bid markup (pegged)'),
    ('EURDKK', 'ASK', 'default', 15.00::numeric(8,2), 'Default EURDKK ask markup (pegged)'),
    ('EURPLN', 'BID', 'default', 45.00::numeric(8,2), 'Default EURPLN bid markup'),
    ('EURPLN', 'ASK', 'default', 45.00::numeric(8,2), 'Default EURPLN ask markup'),
    ('EURCZK', 'BID', 'default', 50.00::numeric(8,2), 'Default EURCZK bid markup'),
    ('EURCZK', 'ASK', 'default', 50.00::numeric(8,2), 'Default EURCZK ask markup'),
    ('EURHUF', 'BID', 'default', 60.00::numeric(8,2), 'Default EURHUF bid markup'),
    ('EURHUF', 'ASK', 'default', 60.00::numeric(8,2), 'Default EURHUF ask markup'),
    ('EURCAD', 'BID', 'default', 30.00::numeric(8,2), 'Default EURCAD bid markup'),
    ('EURCAD', 'ASK', 'default', 30.00::numeric(8,2), 'Default EURCAD ask markup'),
    ('EURAUD', 'BID', 'default', 30.00::numeric(8,2), 'Default EURAUD bid markup'),
    ('EURAUD', 'ASK', 'default', 30.00::numeric(8,2), 'Default EURAUD ask markup'),

    -- GBP base (excluding GBPUSD + GBPEUR already covered above / V37)
    ('GBPCHF', 'BID', 'default', 30.00::numeric(8,2), 'Default GBPCHF bid markup'),
    ('GBPCHF', 'ASK', 'default', 30.00::numeric(8,2), 'Default GBPCHF ask markup'),
    ('GBPJPY', 'BID', 'default', 35.00::numeric(8,2), 'Default GBPJPY bid markup'),
    ('GBPJPY', 'ASK', 'default', 35.00::numeric(8,2), 'Default GBPJPY ask markup'),
    ('GBPSEK', 'BID', 'default', 40.00::numeric(8,2), 'Default GBPSEK bid markup'),
    ('GBPSEK', 'ASK', 'default', 40.00::numeric(8,2), 'Default GBPSEK ask markup'),
    ('GBPNOK', 'BID', 'default', 40.00::numeric(8,2), 'Default GBPNOK bid markup'),
    ('GBPNOK', 'ASK', 'default', 40.00::numeric(8,2), 'Default GBPNOK ask markup'),
    ('GBPAUD', 'BID', 'default', 35.00::numeric(8,2), 'Default GBPAUD bid markup'),
    ('GBPAUD', 'ASK', 'default', 35.00::numeric(8,2), 'Default GBPAUD ask markup'),
    ('GBPCAD', 'BID', 'default', 35.00::numeric(8,2), 'Default GBPCAD bid markup'),
    ('GBPCAD', 'ASK', 'default', 35.00::numeric(8,2), 'Default GBPCAD ask markup'),

    -- USD base (excluding USDEUR + USDGBP already covered above / V37)
    ('USDJPY', 'BID', 'default', 25.00::numeric(8,2), 'Default USDJPY bid markup'),
    ('USDJPY', 'ASK', 'default', 25.00::numeric(8,2), 'Default USDJPY ask markup'),
    ('USDCHF', 'BID', 'default', 25.00::numeric(8,2), 'Default USDCHF bid markup'),
    ('USDCHF', 'ASK', 'default', 25.00::numeric(8,2), 'Default USDCHF ask markup'),
    ('USDCAD', 'BID', 'default', 25.00::numeric(8,2), 'Default USDCAD bid markup'),
    ('USDCAD', 'ASK', 'default', 25.00::numeric(8,2), 'Default USDCAD ask markup'),
    ('USDSEK', 'BID', 'default', 35.00::numeric(8,2), 'Default USDSEK bid markup'),
    ('USDSEK', 'ASK', 'default', 35.00::numeric(8,2), 'Default USDSEK ask markup'),
    ('USDNOK', 'BID', 'default', 35.00::numeric(8,2), 'Default USDNOK bid markup'),
    ('USDNOK', 'ASK', 'default', 35.00::numeric(8,2), 'Default USDNOK ask markup'),
    ('USDDKK', 'BID', 'default', 35.00::numeric(8,2), 'Default USDDKK bid markup'),
    ('USDDKK', 'ASK', 'default', 35.00::numeric(8,2), 'Default USDDKK ask markup'),
    ('USDPLN', 'BID', 'default', 45.00::numeric(8,2), 'Default USDPLN bid markup'),
    ('USDPLN', 'ASK', 'default', 45.00::numeric(8,2), 'Default USDPLN ask markup'),
    ('USDCZK', 'BID', 'default', 50.00::numeric(8,2), 'Default USDCZK bid markup'),
    ('USDCZK', 'ASK', 'default', 50.00::numeric(8,2), 'Default USDCZK ask markup'),
    ('USDHUF', 'BID', 'default', 60.00::numeric(8,2), 'Default USDHUF bid markup'),
    ('USDHUF', 'ASK', 'default', 60.00::numeric(8,2), 'Default USDHUF ask markup'),
    ('USDSGD', 'BID', 'default', 30.00::numeric(8,2), 'Default USDSGD bid markup'),
    ('USDSGD', 'ASK', 'default', 30.00::numeric(8,2), 'Default USDSGD ask markup'),
    ('USDHKD', 'BID', 'default', 30.00::numeric(8,2), 'Default USDHKD bid markup'),
    ('USDHKD', 'ASK', 'default', 30.00::numeric(8,2), 'Default USDHKD ask markup'),
    ('USDCNH', 'BID', 'default', 40.00::numeric(8,2), 'Default USDCNH bid markup'),
    ('USDCNH', 'ASK', 'default', 40.00::numeric(8,2), 'Default USDCNH ask markup'),
    ('USDMXN', 'BID', 'default', 50.00::numeric(8,2), 'Default USDMXN bid markup'),
    ('USDMXN', 'ASK', 'default', 50.00::numeric(8,2), 'Default USDMXN ask markup'),
    ('USDZAR', 'BID', 'default', 60.00::numeric(8,2), 'Default USDZAR bid markup'),
    ('USDZAR', 'ASK', 'default', 60.00::numeric(8,2), 'Default USDZAR ask markup'),
    ('USDTRY', 'BID', 'default', 80.00::numeric(8,2), 'Default USDTRY bid markup (high-vol EM)'),
    ('USDTRY', 'ASK', 'default', 80.00::numeric(8,2), 'Default USDTRY ask markup (high-vol EM)'),

    -- *-USD parity rows
    ('AUDUSD', 'BID', 'default', 30.00::numeric(8,2), 'Default AUDUSD bid markup'),
    ('AUDUSD', 'ASK', 'default', 30.00::numeric(8,2), 'Default AUDUSD ask markup'),
    ('NZDUSD', 'BID', 'default', 30.00::numeric(8,2), 'Default NZDUSD bid markup'),
    ('NZDUSD', 'ASK', 'default', 30.00::numeric(8,2), 'Default NZDUSD ask markup')
) AS v(pair, side, tier, markup_bps, description)
WHERE NOT EXISTS (
    SELECT 1 FROM fx_pair_markups m
    WHERE m.pair = v.pair AND m.side = v.side AND m.tier = v.tier
);
