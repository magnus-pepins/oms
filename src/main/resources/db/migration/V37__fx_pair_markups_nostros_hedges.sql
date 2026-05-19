-- V37: FX tier markup grid + nostro reference book + hedge action audit.
--
-- See system-documentation/plans/oms-fix-gateway-and-settlement.md §11.5
-- and trading-desk/systems/trading-desk.md "Treasury / FX console" section
-- for the broader story. This migration ships the demo-scope cut:
--
--   * fx_pair_markups       : per-tier basis-point markup on top of the PB
--                             mid for each (pair, side) pair the bank quotes.
--                             Consumed by FxQuotesController.quote() to build
--                             the bid/ask we show to a customer.
--   * nostros               : reference table mapping a (currency, ledger
--                             balance id) pair to the operator-friendly label
--                             we render in the trading-desk Treasury page.
--                             Augments OMS_FX_NOSTRO_BALANCE_IDS_CSV — the
--                             CSV stays the source of truth for what
--                             FxNostroSnapshotService aggregates; this table
--                             carries the human metadata (correspondent
--                             bank, status, demo notes) so the UI can render
--                             nice pills without a code change.
--   * fx_hedge_actions      : append-only audit of hedge submissions
--                             produced by FxHedgeHooksController.submitHedge().
--                             The Ledger transfer is the source of truth for
--                             the money movement; this table is the trader-
--                             facing audit / surveillance feed (last 100
--                             hedges, P&L attribution etc.). Idempotency on
--                             (action_key) so a UI double-submit cannot
--                             produce two ledger transfers.
--
-- Demo scope choices (do not bake more in without bumping V37):
--   * No netting / EOD flatten yet. Those will live in V38+ once a
--     PB session exists.
--   * No per-pair limit enforcement at write time. Limits are surfaced as
--     advisory `daily_cap_*` columns; the desk console will display them
--     and let a trader cancel an over-limit hedge before it goes through.
--   * No multi-leg atomic group yet. The hedge is a single 2-leg ledger
--     transfer (debit one nostro, credit the other at the quoted rate).
--
-- flyway:executeInTransaction=true

------------------------------------------------------------------------
-- 1. fx_pair_markups
------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fx_pair_markups (
    id                BIGSERIAL PRIMARY KEY,
    pair              TEXT      NOT NULL,
    side              TEXT      NOT NULL CHECK (side IN ('BID', 'ASK')),
    tier              TEXT      NOT NULL,
    markup_bps        NUMERIC(8, 2) NOT NULL CHECK (markup_bps >= 0),
    is_active         BOOLEAN   NOT NULL DEFAULT TRUE,
    description       TEXT      NOT NULL DEFAULT '',
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fx_pair_markups_unique UNIQUE (pair, side, tier)
);

COMMENT ON TABLE fx_pair_markups IS
    'Per-tier basis-point markup applied on top of the prime-broker mid to produce the BID/ASK we quote a customer for a given currency pair. tier matches user_profiles.role conventions ("default" is the wildcard fallback).';

CREATE INDEX IF NOT EXISTS idx_fx_pair_markups_lookup
    ON fx_pair_markups (pair, tier, side)
    WHERE is_active = TRUE;

------------------------------------------------------------------------
-- 2. nostros
------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS nostros (
    id                BIGSERIAL PRIMARY KEY,
    currency          TEXT NOT NULL,
    ledger_balance_id TEXT NOT NULL UNIQUE,
    correspondent     TEXT NOT NULL,
    label             TEXT NOT NULL,
    status            TEXT NOT NULL DEFAULT 'active'
                          CHECK (status IN ('active', 'closed', 'frozen', 'demo')),
    daily_cap_amount  NUMERIC(28, 2),
    notes             TEXT NOT NULL DEFAULT '',
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT nostros_currency_label_unique UNIQUE (currency, label)
);

COMMENT ON TABLE nostros IS
    'Operator-friendly reference for nostro accounts (the banks own currency accounts at correspondent banks). The ledger_balance_id is the canonical pointer; correspondent / label / daily_cap_amount are display + advisory only. The CSV env OMS_FX_NOSTRO_BALANCE_IDS_CSV still drives what FxNostroSnapshotService aggregates.';

CREATE INDEX IF NOT EXISTS idx_nostros_currency_active
    ON nostros (currency)
    WHERE status = 'active' OR status = 'demo';

------------------------------------------------------------------------
-- 3. fx_hedge_actions
------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fx_hedge_actions (
    id                BIGSERIAL PRIMARY KEY,
    action_key        TEXT NOT NULL UNIQUE,
    submitted_by      TEXT NOT NULL,
    pair              TEXT NOT NULL,
    side              TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
    base_currency     TEXT NOT NULL,
    quote_currency    TEXT NOT NULL,
    base_amount       NUMERIC(28, 8) NOT NULL CHECK (base_amount > 0),
    quote_amount      NUMERIC(28, 8) NOT NULL CHECK (quote_amount > 0),
    quoted_rate       NUMERIC(28, 8) NOT NULL CHECK (quoted_rate > 0),
    quote_id          TEXT,
    base_nostro_id    TEXT NOT NULL,
    quote_nostro_id   TEXT NOT NULL,
    ledger_transaction_id TEXT,
    status            TEXT NOT NULL DEFAULT 'pending'
                          CHECK (status IN ('pending', 'submitted', 'posted', 'failed', 'cancelled')),
    failure_reason    TEXT,
    submitted_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    posted_at         TIMESTAMPTZ,
    payload_json      JSONB NOT NULL DEFAULT '{}'::jsonb
);

COMMENT ON TABLE fx_hedge_actions IS
    'Append-only audit of manual hedge submissions from the trading-desk FX console. action_key carries the UI request id for idempotency. ledger_transaction_id points to the canonical Ledger txn; this row is the operator-facing journal.';

CREATE INDEX IF NOT EXISTS idx_fx_hedge_actions_submitted
    ON fx_hedge_actions (submitted_at DESC);
CREATE INDEX IF NOT EXISTS idx_fx_hedge_actions_pair_submitted
    ON fx_hedge_actions (pair, submitted_at DESC);
CREATE INDEX IF NOT EXISTS idx_fx_hedge_actions_status
    ON fx_hedge_actions (status)
    WHERE status IN ('pending', 'submitted', 'failed');

------------------------------------------------------------------------
-- 4. Seed: a markup grid for EURUSD / GBPUSD / USDEUR so the demo has
--    enough rows for the operator to pick between tiers without manual
--    setup. retail = 20 bps; affluent = 10 bps; institutional = 3 bps;
--    default = 20 bps fallback.
--
-- Idempotent insert via WHERE NOT EXISTS so re-applying the migration on
-- a partially-populated DB does not double-insert.
------------------------------------------------------------------------
INSERT INTO fx_pair_markups
    (pair, side, tier, markup_bps, description)
SELECT v.pair, v.side, v.tier, v.markup_bps, v.description
FROM (VALUES
    ('EURUSD', 'BID', 'default',       20.00::numeric(8,2), 'Default EURUSD bid markup'),
    ('EURUSD', 'ASK', 'default',       20.00::numeric(8,2), 'Default EURUSD ask markup'),
    ('EURUSD', 'BID', 'retail',        20.00::numeric(8,2), 'Retail EURUSD bid markup'),
    ('EURUSD', 'ASK', 'retail',        20.00::numeric(8,2), 'Retail EURUSD ask markup'),
    ('EURUSD', 'BID', 'affluent',      10.00::numeric(8,2), 'Affluent EURUSD bid markup'),
    ('EURUSD', 'ASK', 'affluent',      10.00::numeric(8,2), 'Affluent EURUSD ask markup'),
    ('EURUSD', 'BID', 'institutional',  3.00::numeric(8,2), 'Institutional EURUSD bid markup'),
    ('EURUSD', 'ASK', 'institutional',  3.00::numeric(8,2), 'Institutional EURUSD ask markup'),

    ('GBPUSD', 'BID', 'default',       25.00::numeric(8,2), 'Default GBPUSD bid markup'),
    ('GBPUSD', 'ASK', 'default',       25.00::numeric(8,2), 'Default GBPUSD ask markup'),
    ('GBPUSD', 'BID', 'retail',        25.00::numeric(8,2), 'Retail GBPUSD bid markup'),
    ('GBPUSD', 'ASK', 'retail',        25.00::numeric(8,2), 'Retail GBPUSD ask markup'),
    ('GBPUSD', 'BID', 'affluent',      12.00::numeric(8,2), 'Affluent GBPUSD bid markup'),
    ('GBPUSD', 'ASK', 'affluent',      12.00::numeric(8,2), 'Affluent GBPUSD ask markup'),
    ('GBPUSD', 'BID', 'institutional',  4.00::numeric(8,2), 'Institutional GBPUSD bid markup'),
    ('GBPUSD', 'ASK', 'institutional',  4.00::numeric(8,2), 'Institutional GBPUSD ask markup'),

    ('USDEUR', 'BID', 'default',       20.00::numeric(8,2), 'Default USDEUR bid markup'),
    ('USDEUR', 'ASK', 'default',       20.00::numeric(8,2), 'Default USDEUR ask markup'),
    ('USDEUR', 'BID', 'retail',        20.00::numeric(8,2), 'Retail USDEUR bid markup'),
    ('USDEUR', 'ASK', 'retail',        20.00::numeric(8,2), 'Retail USDEUR ask markup'),
    ('USDEUR', 'BID', 'affluent',      10.00::numeric(8,2), 'Affluent USDEUR bid markup'),
    ('USDEUR', 'ASK', 'affluent',      10.00::numeric(8,2), 'Affluent USDEUR ask markup'),
    ('USDEUR', 'BID', 'institutional',  3.00::numeric(8,2), 'Institutional USDEUR bid markup'),
    ('USDEUR', 'ASK', 'institutional',  3.00::numeric(8,2), 'Institutional USDEUR ask markup')
) AS v(pair, side, tier, markup_bps, description)
WHERE NOT EXISTS (
    SELECT 1 FROM fx_pair_markups m
    WHERE m.pair = v.pair AND m.side = v.side AND m.tier = v.tier
);
