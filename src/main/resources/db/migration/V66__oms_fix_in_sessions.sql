-- FIX-in counterparty / session / account binding / order map (system-documentation/plans/oms-fix-in-acceptor-plan.md).

CREATE TABLE oms_fix_in_counterparty (
    id UUID PRIMARY KEY,
    legal_name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    supabase_org_id UUID NULL,
    supabase_user_id UUID NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE oms_fix_in_session (
    id UUID PRIMARY KEY,
    counterparty_id UUID NOT NULL REFERENCES oms_fix_in_counterparty (id),
    environment TEXT NOT NULL DEFAULT 'UAT',
    session_mode TEXT NOT NULL DEFAULT 'ORDER_ENTRY',
    sender_comp_id TEXT NOT NULL,
    target_comp_id TEXT NOT NULL,
    session_qualifier TEXT NULL,
    logon_username TEXT NULL,
    password_hash TEXT NULL,
    heartbeat_seconds INT NOT NULL DEFAULT 30,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_oms_fix_in_session_comp_ids UNIQUE (sender_comp_id, target_comp_id, session_qualifier)
);

CREATE TABLE oms_fix_in_account_binding (
    id UUID PRIMARY KEY,
    session_id UUID NOT NULL REFERENCES oms_fix_in_session (id),
    fix_account_tag TEXT NOT NULL DEFAULT '',
    oms_account_id UUID NOT NULL,
    ledger_identity_id TEXT NULL,
    ledger_balance_id TEXT NULL,
    default_binding BOOLEAN NOT NULL DEFAULT FALSE,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_oms_fix_in_account_binding UNIQUE (session_id, fix_account_tag)
);

CREATE TABLE oms_fix_in_order_map (
    session_id UUID NOT NULL REFERENCES oms_fix_in_session (id),
    client_cl_ord_id TEXT NOT NULL,
    oms_order_id UUID NOT NULL,
    orig_client_cl_ord_id TEXT NULL,
    account_binding_id UUID NULL REFERENCES oms_fix_in_account_binding (id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (session_id, client_cl_ord_id)
);

CREATE INDEX idx_oms_fix_in_order_map_oms_order_id ON oms_fix_in_order_map (oms_order_id);

CREATE TABLE oms_fix_session_admin_actions (
    id UUID PRIMARY KEY,
    session_role TEXT NOT NULL,
    fix_session_id UUID NULL,
    broker_route_key TEXT NULL,
    action_type TEXT NOT NULL,
    requested_by TEXT NOT NULL,
    approved_by TEXT NULL,
    reason TEXT NOT NULL,
    counterparty_reference TEXT NULL,
    payload_json TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
