CREATE TABLE oms_venue_egress_cursor (
    egress_id TEXT NOT NULL,
    stream_id INTEGER NOT NULL,
    last_applied_recording_id BIGINT,
    last_applied_position BIGINT NOT NULL,
    high_water_recording_id BIGINT,
    high_water_position BIGINT,
    last_applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (egress_id, stream_id)
);
