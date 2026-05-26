package com.balh.oms.cluster;

import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class VenueResolutionAppliedEventTest {

    @Test
    void encodeDecode_roundTrips() {
        VenueResolutionAppliedEvent ev =
                new VenueResolutionAppliedEvent(
                        "PREDMKT-TEST-1",
                        OmsClusterWireFormat.OUTCOME_YES,
                        "it-oracle",
                        1_700_000_000_000L,
                        "sha256-deadbeef",
                        "balh-internal-venue",
                        1_700_000_000_500L,
                        2);
        ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(OmsClusterWireFormat.MAX_COMMAND_BYTES);
        int len = ev.encode(buffer, 0);
        VenueResolutionAppliedEvent decoded = VenueResolutionAppliedEvent.decode(buffer, 0, len);
        assertThat(decoded).isEqualTo(ev);
    }
}
