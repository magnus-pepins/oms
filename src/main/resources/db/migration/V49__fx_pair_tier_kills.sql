-- V49: Per-tier kill-switches for the customer-quote publisher.
--
-- See system-documentation/plans/fx-treasury-auto-hedger-and-publisher-controls.md
-- Phase A2 "Per-tier kill-switch".
--
-- Why a separate table from fx_pair_markup_overrides
--   The override flow (V47) widens or tightens the spread by a number of
--   bps. A kill is binary: "do not publish (pair, tier) at all until
--   valid_until". The operator-approved design (plan, decision 1) is to
--   keep semantics clean: a kill row carries no additive_bps, has no
--   BID/ASK side (you cannot half-kill a stream), and the matcher answer
--   is boolean rather than a running sum. The two tables share the same
--   governance columns (created_by / approved_by / revoked_by) so the
--   four-eyes UI is structurally identical and the same NATS-style
--   cross-JVM invalidation pattern applies.
--
-- Wildcard scope
--   pair is nullable. NULL = "kill this tier across every pair we publish"
--   — primary use case (e.g. halt all `business` quotes for 15 min).
--   pair = 'EURUSD' = "only kill the business stream for that pair".
--   tier is NOT NULL — killing a wildcard tier is killing the publisher
--   entirely, for which we already have OMS_FX_CUSTOMER_QUOTE_PUBLISHER_ENABLED.
--
-- flyway:executeInTransaction=true

CREATE TABLE IF NOT EXISTS fx_pair_tier_kills (
    id              BIGSERIAL PRIMARY KEY,
    pair            TEXT,
    tier            TEXT          NOT NULL CHECK (length(btrim(tier)) > 0),
    reason          TEXT          NOT NULL CHECK (length(btrim(reason)) > 0),
    valid_from      TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    valid_until     TIMESTAMPTZ   NOT NULL,
    created_by      TEXT          NOT NULL CHECK (length(btrim(created_by)) > 0),
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    approved_by     TEXT,
    approved_at     TIMESTAMPTZ,
    revoked_by      TEXT,
    revoked_at      TIMESTAMPTZ,
    CONSTRAINT fx_pair_tier_kills_window CHECK (valid_until > valid_from),
    CONSTRAINT fx_pair_tier_kills_revoke_pair CHECK (
        (revoked_at IS NULL AND revoked_by IS NULL)
        OR (revoked_at IS NOT NULL AND revoked_by IS NOT NULL)
    ),
    CONSTRAINT fx_pair_tier_kills_approval_pair CHECK (
        (approved_at IS NULL AND approved_by IS NULL)
        OR (approved_at IS NOT NULL AND approved_by IS NOT NULL)
    )
);

COMMENT ON TABLE fx_pair_tier_kills IS
    'Per-tier publisher kill-switches. The customer-quote publisher skips a (pair, tier) while an approved, unrevoked row covers "now". Wildcard pair (NULL) kills the tier across every pair. Tier is required. Mirrors fx_pair_markup_overrides governance columns so the four-eyes UI is structurally identical. NATS-bus cache invalidation keeps every OMS JVM convergent within ~1s of the write.';

-- Lookup path: the cache refresh selects unrevoked rows whose window
-- overlaps "now". Same partial-index strategy as fx_pair_markup_overrides
-- — table is expected to stay tiny (one digit active rows at most).
CREATE INDEX IF NOT EXISTS idx_fx_pair_tier_kills_active
    ON fx_pair_tier_kills (valid_until, valid_from)
    WHERE revoked_at IS NULL;
