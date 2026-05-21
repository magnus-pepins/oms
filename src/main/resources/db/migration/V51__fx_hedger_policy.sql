-- V51: Per-currency auto-hedger policy table.
--
-- See system-documentation/plans/fx-treasury-auto-hedger-and-publisher-controls.md
-- Phase B1 "Hedger policy + decision engine".
--
-- One row per currency we manage. The hedger loop reads
-- FxNostroSnapshotService.snapshot() each tick, sums available balance
-- per currency, compares against the per-currency target, and emits a
-- recommendation (or, in auto mode, fires a hedge through FxHedgeService)
-- when drift exceeds the configured threshold.
--
-- Operator workflow (plan B2):
--   - day one: every currency starts at mode='off'. Engine just emits
--     drift telemetry, never writes recommendations or fires hedges.
--   - promote to 'advisory' to start writing recommendations to
--     fx_hedger_recommendations (V52). Nothing fires.
--   - after a week of clean recommendations, promote to 'auto'.
--     Four-eyes is enforced by the CHECK constraint below: a row may
--     have mode='auto' only if auto_approved_by is non-null and was
--     set by an identity different from updated_by/created_by (the
--     service-layer enforces the distinct-identity rule; the column
--     pair lives here so re-applying a policy via API is auditable).
--
-- About the auto-hedger identity (plan decision 2): rather than
-- creating a service-account row in a user_profiles table that OMS
-- doesn't own (the supabase table is shared with beard-admin, not
-- referenced by FK from OMS), we use the canonical literal string
-- 'fx-auto-hedger' as submittedBy on auto-fired rows in
-- fx_hedge_actions. submitted_by is a free-text column there
-- (V37), so this works without a schema cross-cut. Operators can
-- filter automatic vs manual hedges in the trading-desk audit list by
-- this string.
--
-- Pair-route is strict 1:1 per currency (plan decision 3). If the
-- single route ever halts under load (e.g. SEKEUR cap on the venue),
-- a multi-route resolver lands as a follow-up; not a V1 problem.
--
-- flyway:executeInTransaction=true

CREATE TABLE IF NOT EXISTS fx_hedger_policy (
    currency              TEXT       PRIMARY KEY CHECK (length(currency) = 3),
    target_balance        NUMERIC(38, 8) NOT NULL CHECK (target_balance >= 0),
    -- Drift threshold: |current - target| must exceed
    -- max(threshold_abs, threshold_pct × target_balance) before the
    -- engine emits anything. At least one must be non-null.
    threshold_abs         NUMERIC(38, 8) CHECK (threshold_abs IS NULL OR threshold_abs > 0),
    threshold_pct         NUMERIC(8, 4)  CHECK (threshold_pct IS NULL OR (threshold_pct > 0 AND threshold_pct <= 100)),
    -- pair_route is the only pair we'll hedge this currency through
    -- in V1. Side of the hedge is derived: if currency = base(pair),
    -- drift > 0 means we have too much base → SELL pair_route. If
    -- currency = quote(pair), drift > 0 → BUY pair_route.
    pair_route            TEXT       NOT NULL CHECK (length(pair_route) = 6),
    -- Defence vs runaway: a single auto-fire cannot move more than this
    -- in the policy currency, even if the drift is larger. Multiple
    -- ticks at the cooldown cadence chip away at large drifts.
    max_per_action        NUMERIC(38, 8) NOT NULL CHECK (max_per_action > 0),
    -- Seconds between auto-fires for this currency. Engine still emits
    -- a "observed drift" telemetry tick more frequently (every
    -- evaluation cycle) so the UI sees the live drift even mid-cooldown.
    cooldown_s            INTEGER    NOT NULL DEFAULT 300 CHECK (cooldown_s >= 0),
    mode                  TEXT       NOT NULL DEFAULT 'off' CHECK (mode IN ('off', 'advisory', 'auto')),
    -- Operator decides which two nostros the hedge crosses. Explicit
    -- here rather than auto-resolved from omsConfig.nostroBalanceIds
    -- so a money-moving rule cannot silently shift to a different
    -- balance when the operator adds a new nostro to the ID list.
    base_nostro_id        TEXT       NOT NULL CHECK (length(btrim(base_nostro_id)) > 0),
    quote_nostro_id       TEXT       NOT NULL CHECK (length(btrim(quote_nostro_id)) > 0),
    created_by            TEXT       NOT NULL CHECK (length(btrim(created_by)) > 0),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by            TEXT,
    updated_at            TIMESTAMPTZ,
    -- Four-eyes columns for the advisory → auto promotion. Cleared on
    -- a demotion to advisory/off so a future re-promotion goes through
    -- the same review.
    auto_approved_by      TEXT,
    auto_approved_at      TIMESTAMPTZ,
    CONSTRAINT fx_hedger_policy_threshold_present CHECK (
        threshold_abs IS NOT NULL OR threshold_pct IS NOT NULL
    ),
    CONSTRAINT fx_hedger_policy_auto_needs_approver CHECK (
        mode <> 'auto' OR auto_approved_by IS NOT NULL
    ),
    CONSTRAINT fx_hedger_policy_nostros_differ CHECK (
        base_nostro_id <> quote_nostro_id
    )
);

COMMENT ON TABLE fx_hedger_policy IS
    'Per-currency rules for the auto-hedger engine. mode=off emits drift telemetry only; advisory writes recommendations to fx_hedger_recommendations; auto additionally fires through FxHedgeService.submit() with submittedBy="fx-auto-hedger". Plan B1 in system-documentation/plans/fx-treasury-auto-hedger-and-publisher-controls.md.';
