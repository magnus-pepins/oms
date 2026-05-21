-- V53: 'desk' tier markups for the auto-hedger pricing path.
--
-- See system-documentation/plans/fx-treasury-auto-hedger-and-publisher-controls.md
-- decision 5 ("Auto-hedger pricing tier → new DESK tier with its own
-- fx_pair_markups rows. Operator hedges price off the desk markup,
-- not the customer tier the drift originated from").
--
-- Why a dedicated tier rather than reusing 'admin' or 'elite':
--   * The auto-hedger price moves money between two house nostros
--     through Ledger suspense balances (V37 mechanics). There is no
--     external venue spread to recover here — the markup is purely
--     an internal P&L cushion the desk decides to apply (or not).
--   * Customer tiers shift over time as the rate-card is repriced via
--     beard-admin. A dedicated 'desk' tier lets the hedger pricing
--     evolve independently — e.g. set a tiny cushion for one currency
--     pair, zero everywhere else — without that change leaking back
--     onto customer-visible streams.
--   * The OMS customer-quote publisher (V45 tiers: basic / premium /
--     elite / admin / business) is unaffected: 'desk' is not in the
--     default publisher tier list, so the MQTT stream stays
--     unchanged.
--
-- Default value = 0 bps for every (pair, side). The auto-hedger
-- bookings come out at mid → the audit P&L per hedge is zero unless
-- the operator deliberately widens the desk tier via beard-admin.
-- Operators who want a small cushion can update individual rows
-- without re-running this migration.
--
-- Pairs covered: every (pair, side) that already has a 'basic' row in
-- fx_pair_markups. That set is the canonical list of pairs the
-- customer-quote publisher emits on, and matches the pairs the
-- auto-hedger can route through. Pairs added later need their own
-- desk row written via beard-admin (the FxAutoHedger engine logs a
-- warning if a policy.pair_route lacks a desk row and skips the
-- recommendation rather than silently re-pricing at a customer tier).
--
-- Idempotent insert via WHERE NOT EXISTS so re-applying on a partially
-- populated DB does not double-insert (matches the V45 pattern).
--
-- flyway:executeInTransaction=true

INSERT INTO fx_pair_markups (pair, side, tier, markup_bps, description)
SELECT
    src.pair,
    src.side,
    'desk'                AS tier,
    0.00::numeric(8, 2)   AS markup_bps,
    'Desk (auto-hedger / operator hedge) ' || lower(src.side)
        || ' markup — defaults to 0 bps; operator may widen per-pair via beard-admin'
        AS description
FROM (
    SELECT DISTINCT pair, side
    FROM fx_pair_markups
    WHERE tier = 'basic'
) AS src
WHERE NOT EXISTS (
    SELECT 1 FROM fx_pair_markups m
    WHERE m.pair = src.pair AND m.side = src.side AND m.tier = 'desk'
);
