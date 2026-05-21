-- V50: RISK_FX_TIER_KILLED reject code for plan A2 in
-- system-documentation/plans/fx-treasury-auto-hedger-and-publisher-controls.md.
--
-- When the operator kills a (pair, tier) via fx_pair_tier_kills (V49), the
-- streaming publisher stops emitting that combination. The HTTP /fx/quote
-- path (FxQuoteService) must reject for the same combination too — without
-- that, the BFF can still mint a fresh quote and submit an order at the
-- killed price, defeating the kill. This reject code lets OMS map the
-- refusal to a stable wire-format value the BFF can act on (refresh +
-- reconfirm "this rate is not available right now") without conflating
-- it with stale-mid or expired-quote outcomes.
--
-- Separate migration from V49 because ALTER TYPE ... ADD VALUE cannot run
-- inside a transaction on Postgres (matches the V46 pattern that added the
-- prior RISK_FX_* codes).

ALTER TYPE reject_code ADD VALUE IF NOT EXISTS 'RISK_FX_TIER_KILLED';
