-- UAT / integration-test FIX-in counterparty (system-documentation/plans/oms-fix-in-acceptor-plan.md).

INSERT INTO oms_fix_in_counterparty (id, legal_name, status)
VALUES ('c0000001-0000-4000-8000-000000000001', 'Loopback FIX Client', 'ACTIVE')
ON CONFLICT (id) DO NOTHING;

INSERT INTO oms_fix_in_session (
    id, counterparty_id, environment, session_mode,
    sender_comp_id, target_comp_id, heartbeat_seconds, enabled)
VALUES (
    '00000001-0000-4000-8000-000000000001',
    'c0000001-0000-4000-8000-000000000001',
    'UAT',
    'ORDER_ENTRY',
    'LOOPBACK_CLIENT',
    'BALH_OMS',
    30,
    TRUE)
ON CONFLICT (id) DO NOTHING;

INSERT INTO oms_fix_in_account_binding (
    id, session_id, fix_account_tag, oms_account_id, default_binding, enabled)
VALUES (
    'b0000001-0000-4000-8000-000000000001',
    '00000001-0000-4000-8000-000000000001',
    '',
    'a0000001-0000-4000-8000-000000000002',
    TRUE,
    TRUE)
ON CONFLICT (id) DO NOTHING;

-- DROP_COPY session for conformance scenario 7 (loopback probe uses LOOPBACK_DROP → BALH_OMS).
INSERT INTO oms_fix_in_session (
    id, counterparty_id, environment, session_mode,
    sender_comp_id, target_comp_id, heartbeat_seconds, enabled)
VALUES (
    '00000002-0000-4000-8000-000000000002',
    'c0000001-0000-4000-8000-000000000001',
    'UAT',
    'DROP_COPY',
    'LOOPBACK_DROP',
    'BALH_OMS',
    30,
    TRUE)
ON CONFLICT (id) DO NOTHING;
