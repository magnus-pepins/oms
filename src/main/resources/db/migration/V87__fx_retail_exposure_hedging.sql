-- V87: retail FX exposure hedging.
--
-- Adds the ability to supervise and hedge the *retail* FX conversion pools
-- (the plain @Nostro-<CCY> balances the customer-app "move money"
-- cross-currency path debits/credits) alongside the existing OMS-routed
-- @FX-Suspense-<CCY> book.
--
-- Two narrow column adds, both back-compatible (defaults preserve existing
-- behaviour for every current row):
--
--   1. fx_hedger_policy.exposure_source
--        'suspense' (default) — the auto-hedger reads the bank-nostro book
--                    (FxNostroSnapshotService) minus windowed invest flow,
--                    exactly as before V87, and hedge legs route through
--                    @FX-Suspense-<CCY>.
--        'retail'   — the auto-hedger reads the retail pool balance
--                    (@Nostro-<CCY> via FxRetailNostroSnapshotService) as
--                    the live drift and hedge legs route through
--                    @Nostro-<CCY>. target_balance is the flat target
--                    (normally 0). customer-flow netting is NOT applied
--                    (that bucket tracks invest order flow, a different
--                    book).
--
--   2. fx_hedge_actions.exposure
--        Same enum, stamped by FxHedgeService.submit() so the trading-desk
--        audit list and beard surveillance can tell a retail-pool hedge
--        from a suspense hedge. Default 'suspense' keeps historical rows
--        unambiguous.
--
-- flyway:executeInTransaction=true

ALTER TABLE fx_hedger_policy
    ADD COLUMN IF NOT EXISTS exposure_source TEXT NOT NULL DEFAULT 'suspense'
        CHECK (exposure_source IN ('suspense', 'retail'));

COMMENT ON COLUMN fx_hedger_policy.exposure_source IS
    'Which open-FX book this policy manages. suspense=bank-nostro drift minus windowed invest flow, hedged via @FX-Suspense-<CCY>; retail=@Nostro-<CCY> conversion pool balance, hedged via @Nostro-<CCY>. See V87 header + FxAutoHedger.';

ALTER TABLE fx_hedge_actions
    ADD COLUMN IF NOT EXISTS exposure TEXT NOT NULL DEFAULT 'suspense'
        CHECK (exposure IN ('suspense', 'retail'));

COMMENT ON COLUMN fx_hedge_actions.exposure IS
    'Exposure book the hedge legs crossed: suspense=@FX-Suspense-<CCY>, retail=@Nostro-<CCY>. Stamped by FxHedgeService.submit(). Default suspense for pre-V87 rows.';
