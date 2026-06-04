-- Phase D (H6): registry of Ledger settlement templates keyed by (template_id, version).
-- Outbox tables stay separate (sibling pattern); payloads carry template + templateVersion for routing.

CREATE TABLE settlement_template (
    template_id     TEXT        NOT NULL,
    version         INT         NOT NULL,
    outbox_table    TEXT        NOT NULL,
    description     TEXT        NOT NULL,
    active          BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (template_id, version)
);

COMMENT ON TABLE settlement_template IS
    'OMS Ledger settlement template registry (H6). Reconcilers validate template_id/version before posting.';

INSERT INTO settlement_template (template_id, version, outbox_table, description, active)
VALUES
    (
        'prediction_market_binary_resolution',
        1,
        'prediction_market_ledger_outbox',
        'Binary PM contract resolution: collateral pool debit, customer payout credit',
        TRUE
    ),
    (
        'equity_broker_eod_v1',
        1,
        'ledger_settlement_outbox',
        'Broker EOD confirm → per-leg cash/fee Ledger postings on trade settle',
        TRUE
    );
