-- Gap plan §5.9: record-date entitlement snapshot + voluntary-election workflow.

CREATE TABLE corporate_action_record_date_snapshot (
    id                          BIGSERIAL PRIMARY KEY,
    corporate_action_event_id   BIGINT      NOT NULL REFERENCES corporate_action_event (id) ON DELETE CASCADE,
    account_id                  UUID        NOT NULL,
    instrument_symbol           TEXT        NOT NULL,
    quantity_settled            NUMERIC(28, 10) NOT NULL,
    record_date                 DATE        NOT NULL,
    snapshot_source             TEXT        NOT NULL DEFAULT 'record_date_job',
    captured_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_ca_record_date_snapshot_event_account
        UNIQUE (corporate_action_event_id, account_id)
);

COMMENT ON TABLE corporate_action_record_date_snapshot IS
    'Settled quantity per holder frozen on corporate-action record date (gap plan §5.9).';

CREATE TABLE corporate_action_election (
    id                          BIGSERIAL PRIMARY KEY,
    corporate_action_event_id   BIGINT      NOT NULL REFERENCES corporate_action_event (id) ON DELETE CASCADE,
    account_id                  UUID        NOT NULL,
    election_choice             TEXT        NOT NULL,
    requested_by                TEXT        NOT NULL,
    approved_by                 TEXT,
    approved_at                 TIMESTAMPTZ,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_ca_election_event_account
        UNIQUE (corporate_action_event_id, account_id),
    CONSTRAINT ca_election_approval_pair_chk
        CHECK (
            (approved_at IS NULL AND approved_by IS NULL)
            OR (approved_at IS NOT NULL AND approved_by IS NOT NULL)
        )
);

COMMENT ON TABLE corporate_action_election IS
    'Customer/ops election for voluntary corporate actions (rights, tender offers). Four-eyes on approve.';

CREATE INDEX idx_ca_record_date_snapshot_event
    ON corporate_action_record_date_snapshot (corporate_action_event_id);

CREATE INDEX idx_ca_election_event
    ON corporate_action_election (corporate_action_event_id);
