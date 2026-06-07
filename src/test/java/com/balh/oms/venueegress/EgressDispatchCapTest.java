package com.balh.oms.venueegress;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link OmsVenueEgressService.EgressRoutePipeline#computeEffectiveDispatchCapacity}. */
class EgressDispatchCapTest {

    private static final int MAX_IN_FLIGHT = 576;
    private static final int MAX_PENDING = MAX_IN_FLIGHT * 7;
    private static final int HARD_FLOOR = 576;
    private static final int ROUTE_THRESHOLD = 2048;
    private static final int ER_THRESHOLD = 7168;
    private static final int SOFT_MULT = 4;
    private static final int EXHAUSTED_MULT = 3;

    @Test
    void normalPath_usesMaxPendingFragments() {
        assertThat(
                        cap(
                                /* inFlight */ 100,
                                /* erDepth */ 0,
                                /* availablePermits */ MAX_IN_FLIGHT))
                .isEqualTo(MAX_PENDING);
    }

    @Test
    void erDeepWithPermits_usesSoftCap() {
        int expected = Math.max(HARD_FLOOR, Math.min(MAX_PENDING, MAX_IN_FLIGHT * SOFT_MULT));
        assertThat(cap(100, ER_THRESHOLD, 64)).isEqualTo(expected);
    }

    @Test
    void erDeepPermitsExhausted_usesGraduatedExhaustedCap_notHardFloor() {
        int expected = Math.max(HARD_FLOOR, Math.min(MAX_PENDING, MAX_IN_FLIGHT * EXHAUSTED_MULT));
        assertThat(cap(1200, ER_THRESHOLD, 0)).isEqualTo(expected);
        assertThat(expected).isGreaterThan(HARD_FLOOR);
        assertThat(expected).isLessThan(MAX_IN_FLIGHT * SOFT_MULT);
    }

    @Test
    void routeBacklogged_usesHardFloor_evenWhenErDeep() {
        assertThat(cap(ROUTE_THRESHOLD, ER_THRESHOLD, 0)).isEqualTo(HARD_FLOOR);
    }

    @Test
    void erDeepPermitsExhausted_belowRouteThreshold_notHardFloor() {
        // 16k profile: permits exhausted + ER deep but inFlight < route threshold — must not clamp to 576.
        assertThat(cap(800, ER_THRESHOLD, 0)).isEqualTo(MAX_IN_FLIGHT * EXHAUSTED_MULT);
    }

    private static int cap(int inFlight, int erDepth, int availablePermits) {
        return OmsVenueEgressService.computeEffectiveDispatchCapacity(
                MAX_IN_FLIGHT,
                MAX_PENDING,
                HARD_FLOOR,
                ROUTE_THRESHOLD,
                ER_THRESHOLD,
                SOFT_MULT,
                EXHAUSTED_MULT,
                inFlight,
                erDepth,
                availablePermits);
    }
}
