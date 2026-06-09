package com.balh.oms.venueegress;

import com.balh.oms.config.OmsProfiles;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * Readiness signal for the OMS→venue Aeron replay bridge. The Spring Boot process can stay
 * {@code UP} while {@link OmsVenueEgressService}'s dedicated replay thread has exited (fatal
 * cursor error or unclassified {@link RuntimeException}); PM2 then keeps a zombie JVM that
 * accepts no new journal fragments. This indicator fails readiness so operators can restart.
 */
@Component("omsVenueEgressReplay")
@Profile(OmsProfiles.VENUE_EGRESS)
@ConditionalOnProperty(prefix = "oms.cluster.venue-egress", name = "enabled", havingValue = "true")
public class OmsVenueEgressReplayHealthIndicator implements HealthIndicator {

    private final OmsVenueEgressService egressService;

    public OmsVenueEgressReplayHealthIndicator(OmsVenueEgressService egressService) {
        this.egressService = egressService;
    }

    @Override
    public Health health() {
        if (egressService.isReplayLoopAlive()) {
            return Health.up().build();
        }
        return Health.down()
                .withDetail("reason", "oms-venue-egress replay thread not running")
                .withDetail("replayLoopRunningFlag", egressService.isReplayLoopRunning())
                .build();
    }
}
