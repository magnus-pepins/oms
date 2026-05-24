-- V62: backfill executions.trade_date on pre-V58 TRADE rows.
--
-- Stock-settlement gap plan §5.3 Slice 2b-2. After V58 + Slice 1 every new TRADE
-- execution lands with `trade_date` and `expected_settlement_date` populated. Rows
-- projected before V58 have NULL in both columns. The matcher's settlement-date
-- axis (Slice 2a) already handles NULL gracefully — it treats the row as
-- "OMS expectation unknown" and does not open a settlement_date_mismatch break.
-- That preserves correctness on legacy rows, but the customer-facing UI would still
-- prefer a real `trade_date` so settled-date math works retroactively.
--
-- We backfill `trade_date` because it has a deterministic derivation that does not
-- depend on historical instrument profile data: it is the UTC calendar date of the
-- venue timestamp. Same rule the V58 projector path uses.
--
-- We deliberately do NOT backfill `expected_settlement_date`. That value depends on
-- the settlement cycle that was active for the instrument at the historical trade
-- date, which is information we do not have for pre-V58 trades. Inventing a value
-- (e.g. "always T+2") would risk:
--   - manufacturing settlement_date_mismatch breaks against truthful broker confirms
--     once Slice 2a runs over historical data (high signal:noise penalty for ops);
--   - lying to the customer UI about a delivered settlement date for trades that
--     actually settled on a different day.
-- Leaving expected_settlement_date NULL keeps the matcher's "OMS expectation unknown"
-- path active for legacy rows — which is the operator-friendly outcome until the
-- instrument_settlement_profile (V61) is populated and an operator decides to
-- recompute legacy expected dates via the resolver.
--
-- Scale: this UPDATE is a single statement so it runs inside Flyway's migration
-- transaction. For pop / staging volumes (low thousands of executions today) this
-- is comfortably sub-second. If/when production volume crosses ~1M unfilled-trade
-- rows this should be re-shaped as a batched Java migration; tracked in the plan's
-- §5.3 Slice 2b follow-up.

UPDATE executions
SET trade_date = (venue_ts AT TIME ZONE 'UTC')::date
WHERE exec_type = 'TRADE'::execution_exec_type
  AND trade_date IS NULL
  AND venue_ts IS NOT NULL;

-- venue_ts is declared NOT NULL on the executions table (see V6), so the
-- IS NOT NULL guard above is belt-and-braces; we keep it explicit so a future
-- schema change that relaxes the constraint surfaces here rather than silently
-- failing at the projection time of an offending row.
