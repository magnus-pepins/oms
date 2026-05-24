-- Gap plan §5.10 Phase E: schablon parameters + KU30 four-eyes approval columns.

CREATE TABLE isk_tax_parameters (
    tax_year                INT             NOT NULL PRIMARY KEY,
    statslaneranta          NUMERIC(8, 6)   NOT NULL,
    schablon_rate           NUMERIC(8, 6)   NOT NULL,
    source                  TEXT            NOT NULL DEFAULT 'manual',
    updated_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE isk_tax_parameters IS
    'Annual statslåneränta and derived schablon rate (IL 42 kap. 35 §).';

INSERT INTO isk_tax_parameters (tax_year, statslaneranta, schablon_rate, source)
VALUES
    (2025, 0.0188, 0.0288, 'seed'),
    (2026, 0.0188, 0.0288, 'seed')
ON CONFLICT (tax_year) DO NOTHING;

ALTER TABLE isk_tax_year_export
    ADD COLUMN approved_by TEXT,
    ADD COLUMN approved_at TIMESTAMPTZ,
    ADD COLUMN filed_by    TEXT,
    ADD COLUMN filed_at    TIMESTAMPTZ;

COMMENT ON COLUMN isk_tax_year_export.approved_by IS
    'Four-eyes approver for KU30 draft before filing (must differ from draft creator when enforced in OMS).';
