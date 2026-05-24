-- Phase E Slice 12a (gap plan §5.10): OMS account tax-wrapper mapping for ISK settlement metadata.

CREATE TABLE oms_account_tax_wrapper (
    account_id          UUID PRIMARY KEY,
    tax_wrapper         TEXT        NOT NULL,
    isk_account_id      UUID,
    ledger_balance_id   TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT oms_account_tax_wrapper_type_chk
        CHECK (tax_wrapper IN ('none', 'investment', 'isk'))
);

CREATE INDEX idx_oms_account_tax_wrapper_isk
    ON oms_account_tax_wrapper (isk_account_id)
    WHERE isk_account_id IS NOT NULL;

COMMENT ON TABLE oms_account_tax_wrapper IS
    'Maps retail account_id to tax wrapper (gap plan §5.10 / ledger-swedish-isk-account-type.md). Drives ISK instrument eligibility and settlement leg metadata.';
