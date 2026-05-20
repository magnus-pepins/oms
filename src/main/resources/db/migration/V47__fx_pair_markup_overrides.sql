-- V47: Tactical, time-bounded markup overrides on top of fx_pair_markups.
--
-- See system-documentation/plans/fx-tier-quotes-production.md §Phase 3
-- "Treasury publisher overview + tactical overrides" (P3.6).
--
-- Why a separate table from fx_pair_markups
--   fx_pair_markups (V37/V38/V45) is the permanent rate-card — changed
--   slowly via beard-admin with four-eyes (P3.4). This table is the
--   tactical knob the trading-desk Treasury page (P3.8) uses to widen
--   spreads during vol spikes, news events, or illiquid windows.
--   Overrides:
--     * have a TTL and auto-expire (publisher cache stops applying them
--       on the next refresh tick after valid_until)
--     * are additive on top of the rate-card (positive bps widen,
--       negative bps tighten — the latter clamps at 0 in code)
--     * carry their own audit trail (created_by, reason, optional
--       approved_by for four-eyes per P3.9)
--   Keeping them out of fx_pair_markups means an override never silently
--   becomes "the new rate-card" and the permanent grid stays clean.
--
-- Wildcard scope
--   pair / side / tier are all nullable. NULL = wildcard, applies to all
--   values in that dimension. A widening "all USD pairs for retail tier
--   on the ASK side for 30 minutes" is one row with pair='USDXXX-...'
--   (but more typically the operator writes one row per pair). The
--   cache-merge in FxMarkupOverridesService sums additive_bps across all
--   matching rows so multiple overlapping overrides combine predictably.
--
-- flyway:executeInTransaction=true

CREATE TABLE IF NOT EXISTS fx_pair_markup_overrides (
    id              BIGSERIAL PRIMARY KEY,
    pair            TEXT,
    side            TEXT CHECK (side IS NULL OR side IN ('BID', 'ASK')),
    tier            TEXT,
    additive_bps    NUMERIC(8, 2) NOT NULL,
    reason          TEXT          NOT NULL CHECK (length(btrim(reason)) > 0),
    valid_from      TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    valid_until     TIMESTAMPTZ   NOT NULL,
    created_by      TEXT          NOT NULL CHECK (length(btrim(created_by)) > 0),
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    approved_by     TEXT,
    approved_at     TIMESTAMPTZ,
    revoked_by      TEXT,
    revoked_at      TIMESTAMPTZ,
    CONSTRAINT fx_pair_markup_overrides_window CHECK (valid_until > valid_from),
    CONSTRAINT fx_pair_markup_overrides_revoke_pair CHECK (
        (revoked_at IS NULL AND revoked_by IS NULL)
        OR (revoked_at IS NOT NULL AND revoked_by IS NOT NULL)
    ),
    CONSTRAINT fx_pair_markup_overrides_approval_pair CHECK (
        (approved_at IS NULL AND approved_by IS NULL)
        OR (approved_at IS NOT NULL AND approved_by IS NOT NULL)
    )
);

COMMENT ON TABLE fx_pair_markup_overrides IS
    'Tactical, time-bounded additive markup overrides on top of fx_pair_markups. Trading-desk Treasury (P3.8) writes these to widen/tighten spreads temporarily; rows expire automatically at valid_until. Wildcard scope via NULL pair/side/tier. Read by FxMarkupOverridesService and merged into both the streaming publisher and the HTTP /quote path so display and submit-time rates stay in lockstep.';

-- Lookup path: the cache refresh selects unrevoked rows whose window
-- overlaps "now". A single composite index on the window endpoints with
-- a partial WHERE on unrevoked rows is enough — the table is expected
-- to stay small (single-digit active rows at most) so we optimise for
-- compactness over coverage.
CREATE INDEX IF NOT EXISTS idx_fx_pair_markup_overrides_active
    ON fx_pair_markup_overrides (valid_until, valid_from)
    WHERE revoked_at IS NULL;
