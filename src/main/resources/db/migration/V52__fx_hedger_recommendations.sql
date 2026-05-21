-- V52: Auto-hedger recommendation log.
--
-- See system-documentation/plans/fx-treasury-auto-hedger-and-publisher-controls.md
-- Phase B1.
--
-- One row per recommendation the engine emits in 'advisory' or 'auto'
-- mode. In advisory, operators see the row in the Treasury panel and
-- click "Execute" or "Dismiss". In auto, the row is written for audit
-- BEFORE the fire and updated with auto_fired_action_id once the
-- corresponding fx_hedge_actions row is in. This ordering means a
-- crash between the write and the fire leaves an orphan row in
-- recommendations the operator can investigate, never a fire without
-- a paper trail.
--
-- action_key = canonical idempotency key the engine derives from the
-- currency + the evaluation-tick epoch second. The same drift across
-- two consecutive ticks within one cooldown cannot create a second
-- row; it can however expire via expires_at and be re-emitted on the
-- next qualifying tick — see plan decision 4 ("recommendation TTL =
-- cooldown_s × 2, then re-emit on the next qualifying tick").
--
-- flyway:executeInTransaction=true

CREATE TABLE IF NOT EXISTS fx_hedger_recommendations (
    id                    BIGSERIAL  PRIMARY KEY,
    action_key            TEXT       NOT NULL UNIQUE CHECK (length(btrim(action_key)) > 0),
    -- Source of the drift: which currency hit threshold this tick.
    currency              TEXT       NOT NULL CHECK (length(currency) = 3),
    -- Pair + side derived from the policy. Recommendations carry the
    -- already-resolved BUY/SELL so the operator-facing UI can render
    -- "BUY 12,500 EUR via EURSEK" without re-applying the base/quote
    -- mapping rule.
    pair                  TEXT       NOT NULL CHECK (length(pair) = 6),
    side                  TEXT       NOT NULL CHECK (side IN ('BUY', 'SELL')),
    -- Quote at recommendation time. base_amount is in the pair's base
    -- currency; quoted_rate is BUY=ask / SELL=bid; quote_id ties back
    -- to fx_quote_cache so the auto-fire (or operator execute) can
    -- recall the same price.
    base_amount           NUMERIC(38, 8) NOT NULL CHECK (base_amount > 0),
    quoted_rate           NUMERIC(38, 12) NOT NULL CHECK (quoted_rate > 0),
    quote_id              TEXT       NOT NULL,
    -- Drift telemetry captured at decision time. Useful for operators
    -- to understand "why did this row appear?" without cross-referring
    -- nostro snapshots.
    drift                 NUMERIC(38, 8) NOT NULL,
    target_balance        NUMERIC(38, 8) NOT NULL,
    current_balance       NUMERIC(38, 8) NOT NULL,
    -- Snapshot of the policy mode at recommendation time. Auto-mode
    -- rows are still written before the fire is attempted; advisory
    -- rows just sit until an operator acts.
    mode                  TEXT       NOT NULL CHECK (mode IN ('advisory', 'auto')),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- TTL = created_at + cooldown_s × 2 (decision 4). The engine treats
    -- rows past expires_at as "no longer active": a fresh qualifying
    -- drift re-emits a new row with a new action_key.
    expires_at            TIMESTAMPTZ NOT NULL CHECK (expires_at > created_at),
    -- Terminal-state columns. Mutually exclusive in practice: an
    -- advisory row is either dismissed by an operator, manually
    -- executed (linking to a fx_hedge_actions row), or expires
    -- untouched. An auto row gets auto_fired_action_id set the moment
    -- FxHedgeService.submit succeeds (or stays NULL on a fire-failure;
    -- audit trail still says "we tried").
    dismissed_by          TEXT,
    dismissed_at          TIMESTAMPTZ,
    executed_action_id    BIGINT     REFERENCES fx_hedge_actions(id),
    auto_fired_action_id  BIGINT     REFERENCES fx_hedge_actions(id),
    CONSTRAINT fx_hedger_recs_dismiss_pair CHECK (
        (dismissed_at IS NULL AND dismissed_by IS NULL)
        OR (dismissed_at IS NOT NULL AND dismissed_by IS NOT NULL)
    )
);

COMMENT ON TABLE fx_hedger_recommendations IS
    'Recommendation log emitted by FxAutoHedger. advisory rows wait for an operator click; auto rows are fired immediately and updated with auto_fired_action_id. Idempotent on action_key (currency + tick epoch second). TTL = policy.cooldown_s × 2 (plan decision 4).';

-- Lookup paths:
--   * Treasury panel list:   active rows = expires_at > now AND no dismissed_at AND no executed_action_id.
--   * Auto-fire idempotency: lookup by action_key.
--   * Per-currency cooldown: latest row per currency vs created_at.
CREATE INDEX IF NOT EXISTS idx_fx_hedger_recs_active
    ON fx_hedger_recommendations (expires_at DESC, created_at DESC)
    WHERE dismissed_at IS NULL AND executed_action_id IS NULL AND auto_fired_action_id IS NULL;

CREATE INDEX IF NOT EXISTS idx_fx_hedger_recs_currency_created
    ON fx_hedger_recommendations (currency, created_at DESC);
