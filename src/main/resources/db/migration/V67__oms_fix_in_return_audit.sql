-- FIX-in return path cursor, send dedupe, message audit index, drop-copy entitlements.

CREATE TABLE oms_fix_in_return_cursor (
    egress_id TEXT PRIMARY KEY,
    replay_stream_id INT NOT NULL,
    last_applied_recording_id BIGINT NULL,
    last_applied_position BIGINT NOT NULL DEFAULT 0,
    high_water_recording_id BIGINT NULL,
    high_water_position BIGINT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE oms_fix_in_outbound_sent (
    session_id UUID NOT NULL REFERENCES oms_fix_in_session (id),
    dedupe_key TEXT NOT NULL,
    sent_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (session_id, dedupe_key)
);

CREATE TABLE oms_fix_message_audit (
    id UUID PRIMARY KEY,
    direction TEXT NOT NULL,
    session_role TEXT NOT NULL,
    fix_session_id UUID NULL REFERENCES oms_fix_in_session (id),
    broker_route_key TEXT NULL,
    msg_type TEXT NULL,
    msg_seq_num INT NULL,
    cl_ord_id TEXT NULL,
    orig_cl_ord_id TEXT NULL,
    oms_order_id UUID NULL,
    exec_id TEXT NULL,
    raw_store_ref TEXT NULL,
    summary TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_oms_fix_message_audit_session_created ON oms_fix_message_audit (fix_session_id, created_at DESC);
CREATE INDEX idx_oms_fix_message_audit_order ON oms_fix_message_audit (oms_order_id);

CREATE TABLE oms_fix_drop_copy_entitlement (
    drop_copy_session_id UUID NOT NULL REFERENCES oms_fix_in_session (id),
    oms_account_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (drop_copy_session_id, oms_account_id)
);
