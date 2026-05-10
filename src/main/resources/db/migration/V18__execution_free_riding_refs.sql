-- Slice 8 / master plan 11.7: optional linkage for US free-riding attribution (OMS stores refs; finance rules TBD).
ALTER TABLE executions
    ADD COLUMN IF NOT EXISTS unsettled_funded_by_exec_ids BIGINT[] NOT NULL DEFAULT '{}'::bigint[];

COMMENT ON COLUMN executions.unsettled_funded_by_exec_ids IS
    'Optional peer TRADE execution ids whose unsettled proceeds funded this BUY; populated by policy engine when enabled.';
