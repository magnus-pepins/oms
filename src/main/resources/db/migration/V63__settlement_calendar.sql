-- V63: settlement_calendar + minimal Swedish/EU and US listed-equity 2026 seed.
--
-- Stock-settlement gap plan §5.3 Slice 2b-3. Replaces the weekend-only
-- business-day skip in SettlementDateCalculator with a calendar-aware lookup:
-- when an instrument_settlement_profile (V61) references a calendar_id whose row
-- exists for the date in question, that date is treated as non-business for the
-- T+N walk.
--
-- Population model: rows are operator-led. The seed below covers calendar year
-- 2026 only and is intentionally short — it gives the IT smoke real data without
-- locking in a 5-year corpus that operators would then have to keep in sync.
-- Full population (2026-2029, half-day handling, market-specific rules around
-- Boxing Day / Easter / Christmas Eve) lands via the V64 JSON ingest endpoint
-- (Slice 2b-4) plus marketdata-platform sync.
--
-- Half-day convention: Stockholm and several other EU venues run half-days on
-- Midsummer Eve / Christmas Eve / New Year's Eve where settlement processing
-- still happens but the trading session closes early. For T+N purposes we
-- conservatively treat these as non-business: an operator can override per
-- calendar via JSON ingest if a specific instrument settles same-day on a
-- half-day. The conservative choice matches the matcher's safety net — the
-- broker is authoritative on actual settlement date, and a wrong-direction
-- mismatch surfaces as a `settlement_date_mismatch` break (severity medium),
-- not silent drift.

CREATE TABLE settlement_calendar (
    calendar_id   TEXT NOT NULL,
    holiday_date  DATE NOT NULL,
    description   TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (calendar_id, holiday_date)
);

COMMENT ON TABLE settlement_calendar IS
    'Per-calendar non-business dates (gap plan §5.3 Slice 2b-3). Used by SettlementDateCalculator to skip holidays during T+N business-day arithmetic. Calendar IDs link to instrument_settlement_profile.settlement_calendar_id.';

CREATE INDEX idx_settlement_calendar_calendar_id_date
    ON settlement_calendar (calendar_id, holiday_date);

-- --------------------------------------------------------------------------
-- Seed: XSTO-CAL (NASDAQ Stockholm) 2026 holidays.
-- Source: NASDAQ Nordic Holiday Calendar 2026. Half-days (Midsummer Eve,
-- Christmas Eve, New Year's Eve) are listed because settlement is closed even
-- when trading runs a partial session.
-- --------------------------------------------------------------------------
INSERT INTO settlement_calendar (calendar_id, holiday_date, description) VALUES
    ('XSTO-CAL', DATE '2026-01-01', 'New Year''s Day'),
    ('XSTO-CAL', DATE '2026-01-06', 'Epiphany'),
    ('XSTO-CAL', DATE '2026-04-03', 'Good Friday'),
    ('XSTO-CAL', DATE '2026-04-06', 'Easter Monday'),
    ('XSTO-CAL', DATE '2026-05-01', 'Labour Day'),
    ('XSTO-CAL', DATE '2026-05-14', 'Ascension Day'),
    ('XSTO-CAL', DATE '2026-06-05', 'National Day of Sweden'),
    ('XSTO-CAL', DATE '2026-06-19', 'Midsummer Eve (half-day, settlement closed)'),
    ('XSTO-CAL', DATE '2026-12-24', 'Christmas Eve (half-day, settlement closed)'),
    ('XSTO-CAL', DATE '2026-12-25', 'Christmas Day'),
    ('XSTO-CAL', DATE '2026-12-31', 'New Year''s Eve (half-day, settlement closed)');

-- --------------------------------------------------------------------------
-- Seed: XNAS-CAL (NYSE / Nasdaq US) 2026 holidays.
-- Source: NYSE 2026 Holiday & Settlement Schedule. Observed-date adjustments
-- (Jul 3 in lieu of Sat Jul 4, Dec 25 falls on Friday) are encoded as the
-- observed date — that is what the venue actually closes on.
-- --------------------------------------------------------------------------
INSERT INTO settlement_calendar (calendar_id, holiday_date, description) VALUES
    ('XNAS-CAL', DATE '2026-01-01', 'New Year''s Day'),
    ('XNAS-CAL', DATE '2026-01-19', 'Martin Luther King Jr. Day'),
    ('XNAS-CAL', DATE '2026-02-16', 'Washington''s Birthday (Presidents'' Day)'),
    ('XNAS-CAL', DATE '2026-04-03', 'Good Friday'),
    ('XNAS-CAL', DATE '2026-05-25', 'Memorial Day'),
    ('XNAS-CAL', DATE '2026-06-19', 'Juneteenth National Independence Day'),
    ('XNAS-CAL', DATE '2026-07-03', 'Independence Day (observed; Jul 4 is Saturday)'),
    ('XNAS-CAL', DATE '2026-09-07', 'Labor Day'),
    ('XNAS-CAL', DATE '2026-11-26', 'Thanksgiving Day'),
    ('XNAS-CAL', DATE '2026-12-25', 'Christmas Day');
