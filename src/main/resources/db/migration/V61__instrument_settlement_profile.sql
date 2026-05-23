-- V61: instrument settlement profile (skeleton).
--
-- Stock-settlement gap plan §5.3 Slice 2b-1. Replaces the configured-default-cycle
-- placeholder in `SettlementDateCalculator` with a real per-instrument, effective-dated
-- lookup. v1 of this table only needs to drive the settlement cycle (T+0 / T+1 / T+2 /
-- T+3) used at trade-projection time; the rest of the columns are populated for ops /
-- forward use (settlement currency for ledger booking, ISK eligibility for §5.10, MIC
-- for the future trade-date timezone fix in `computeTradeDate`, calendar id for the
-- holiday-aware lookup in Slice 2b-3).
--
-- Effective dating: the plan's draft schema used `instrument_id TEXT PRIMARY KEY` but
-- that cannot represent the EU T+1 migration on 2027-10-11 (same instrument, two
-- effective rows). We use (instrument_id, effective_from) as the natural composite
-- key, exposed via a UNIQUE constraint on top of a surrogate BIGSERIAL so future
-- tables (settlement_calendar, instrument_corporate_action) can reference profiles
-- by id without re-deriving the composite key.
--
-- Population is operator-led: this migration creates the table empty. The Calculator
-- treats "no row" as "fall back to configured default cycle" — same behaviour as
-- Slice 1, just routed through the lookup so the fallback is explicit and testable.
-- The full population path (CSV ingest, marketdata-platform sync) lands later in
-- Phase B / Phase E.

CREATE TABLE instrument_settlement_profile (
    id                       BIGSERIAL    PRIMARY KEY,
    instrument_id            TEXT         NOT NULL,
    symbol                   TEXT         NOT NULL,
    isin                     TEXT,
    primary_mic              TEXT         NOT NULL,
    settlement_calendar_id   TEXT         NOT NULL,
    settlement_cycle         TEXT         NOT NULL,
    settlement_currency      TEXT         NOT NULL,
    isk_eligible             BOOLEAN      NOT NULL DEFAULT FALSE,
    effective_from           DATE         NOT NULL,
    effective_to             DATE,
    created_at               TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT instrument_settlement_profile_cycle_chk
        CHECK (settlement_cycle IN ('T+0', 'T+1', 'T+2', 'T+3')),
    CONSTRAINT instrument_settlement_profile_effective_window_chk
        CHECK (effective_to IS NULL OR effective_to > effective_from),
    CONSTRAINT instrument_settlement_profile_instrument_effective_uk
        UNIQUE (instrument_id, effective_from)
);

COMMENT ON TABLE instrument_settlement_profile IS
    'Per-instrument, effective-dated settlement metadata (gap plan §5.3 Slice 2b). v1 drives `SettlementDateCalculator` cycle lookup; future slices read settlement_currency for ledger booking, isk_eligible for ISK enforcement, and primary_mic for the trade-date timezone fix in `computeTradeDate`.';

-- Symbol is the matching key used by SettlementDateCalculator.resolveExpectedSettlementDate
-- (called from OmsPostgresProjector with order.instrumentSymbol). Index covers the
-- effective_from DESC ordering for the "latest active at asOf" query.
CREATE INDEX idx_instrument_settlement_profile_symbol_eff
    ON instrument_settlement_profile (symbol, effective_from DESC);

CREATE INDEX idx_instrument_settlement_profile_isin
    ON instrument_settlement_profile (isin)
    WHERE isin IS NOT NULL;
