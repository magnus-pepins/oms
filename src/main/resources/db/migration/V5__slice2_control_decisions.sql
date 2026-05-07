-- Slice 2: audit trail for control-plane decisions + Postgres-backed kill switch
-- until Ops Console toggles Redis-backed flags (see plans/oms-fix-gateway-and-settlement.md §5.10).

ALTER TYPE reject_code ADD VALUE 'RISK_NOTIONAL_CAP';

CREATE TABLE oms_runtime_flags (
    flag_key        TEXT PRIMARY KEY,
    value_boolean   BOOLEAN      NOT NULL DEFAULT false,
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE oms_runtime_flags IS
    'Interim runtime toggles read on each control decision. Key global_halt=true rejects all orders at control (RISK_KILL_SWITCH).';

CREATE TABLE control_decisions (
    id                    BIGSERIAL PRIMARY KEY,
    order_id              UUID         NOT NULL REFERENCES orders (id),
    order_version_before  INTEGER      NOT NULL,
    outcome               TEXT         NOT NULL CHECK (outcome IN ('PASS', 'REJECT')),
    reject_code           reject_code,
    stage                 TEXT         NOT NULL DEFAULT 'CONTROL',
    detail                JSONB,
    decided_at            TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_control_decisions_order_time
    ON control_decisions (order_id, decided_at DESC);

COMMENT ON TABLE control_decisions IS
    'One row per ControlTailer.apply outcome (PASS → WORKING CAS, or REJECT with reject_code).';
