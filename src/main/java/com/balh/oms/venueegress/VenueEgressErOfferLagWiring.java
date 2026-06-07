package com.balh.oms.venueegress;

import com.balh.oms.cluster.OmsClusterIngressClient;
import com.balh.oms.config.OmsProfiles;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * After venue-egress beans are constructed, wires archive replay lag into the ER-offer daemon for
 * lag-aware cluster egress interleave ({@link OmsClusterIngressClient#setExcessEgressLagBytesSupplier}).
 */
@Component
@Profile(OmsProfiles.VENUE_EGRESS)
class VenueEgressErOfferLagWiring {

    VenueEgressErOfferLagWiring(
            OmsClusterIngressClient clusterIngressClient, OmsVenueEgressService venueEgressService) {
        clusterIngressClient.setExcessEgressLagBytesSupplier(venueEgressService::currentExcessReplayLagBytes);
    }
}
