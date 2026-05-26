package com.balh.oms.cluster;

import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ApplyVenueResolutionCommandTest {

    @Test
    void encodeDecode_roundTrips() {
        ApplyVenueResolutionCommand cmd =
                new ApplyVenueResolutionCommand(
                        42L,
                        "PREDMKT-TEST-1",
                        OmsClusterWireFormat.OUTCOME_YES,
                        "it-oracle",
                        1_700_000_000_000L,
                        "sha256-deadbeef",
                        "balh-internal-venue");
        ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(OmsClusterWireFormat.MAX_COMMAND_BYTES);
        int len = cmd.encode(buffer, 0);
        ApplyVenueResolutionCommand decoded = ApplyVenueResolutionCommand.decode(buffer, 0, len);
        assertThat(decoded).isEqualTo(cmd);
        assertThat(decoded.idempotencyKey()).isEqualTo("PREDMKT-TEST-1|sha256-deadbeef");
    }
}
