package com.balh.oms.cluster;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class OmsClusterThreeNodeLeaderFailoverIT {

    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(45);

    @Test
    void leaderFailover_underRepeatedAdmits_recordsFailedOfferCount(@TempDir Path tempDir) throws Exception {
        AtomicInteger failed = new AtomicInteger();
        try (OmsThreeNodeClusterHarness cluster = OmsThreeNodeClusterHarness.start(tempDir)) {
            await().atMost(CONNECT_TIMEOUT).untilAsserted(() -> OmsClusterThreeNodeFailoverIT.admitOne(cluster, "pre-1"));

            cluster.stopMember(0);

            await().atMost(CONNECT_TIMEOUT).pollInterval(Duration.ofMillis(100)).untilAsserted(() -> {
                try {
                    OmsClusterThreeNodeFailoverIT.admitOne(cluster, "post-failover");
                } catch (Exception e) {
                    failed.incrementAndGet();
                    throw e;
                }
            });

            assertThat(failed.get())
                    .as("failed admits during leader failover — documents option A vs B")
                    .isLessThanOrEqualTo(0);
        }
    }
}
