-- Slice 4+: single-row (per route_key) send gate for FIX outbound before broker UAT extras (§7.6).

CREATE TABLE fix_route_state (
    route_key    TEXT PRIMARY KEY,
    send_enabled BOOLEAN      NOT NULL DEFAULT TRUE,
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_by   TEXT,
    note         TEXT
);

COMMENT ON TABLE fix_route_state IS
    'Outbound FIX send enablement per route_key; ControlTailer still enqueues WORKING ids — worker skips send when send_enabled is false.';

INSERT INTO fix_route_state (route_key, send_enabled)
VALUES ('default', TRUE);
