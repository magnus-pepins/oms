-- Phase B: cursor for oms-venue-resolver tail of balh-venue cluster events recording.

CREATE TABLE oms_venue_resolver_cursor (
    resolver_id                 TEXT        NOT NULL,
    venue_events_stream_id      INTEGER     NOT NULL,
    last_applied_recording_id   BIGINT,
    last_applied_position       BIGINT      NOT NULL,
    last_applied_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (resolver_id, venue_events_stream_id)
);

COMMENT ON TABLE oms_venue_resolver_cursor IS
    'Replay cursor for OmsVenueResolverService tailing venue VenueResolutionEvent fragments.';
