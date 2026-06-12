package com.balh.oms.cluster;

import com.balh.oms.OmsClusterNodeBootstrap;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.codecs.EventCode;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class OmsClusterThreeNodeFailoverIT {

    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(45);
    private static final long MESSAGE_TIMEOUT_NS = Duration.ofSeconds(15).toNanos();

    @Test
    void leaderFailover_preservesOrderCountAndAcceptsNewOrders(@TempDir Path tempDir) throws Exception {
        try (OmsThreeNodeClusterHarness cluster = OmsThreeNodeClusterHarness.start(tempDir)) {
            await().atMost(CONNECT_TIMEOUT).pollInterval(Duration.ofMillis(200)).untilAsserted(() -> {
                admitOne(cluster, "idem-1");
                assertThat(maxAdmitted(cluster)).isGreaterThanOrEqualTo(1);
            });

            long before = maxAdmitted(cluster);
            cluster.stopMember(0);

            await().atMost(CONNECT_TIMEOUT).pollInterval(Duration.ofMillis(200)).untilAsserted(() -> {
                admitOne(cluster, "idem-2");
                assertThat(maxAdmitted(cluster)).isEqualTo(before + 1);
            });
        }
    }

    static void admitOne(OmsThreeNodeClusterHarness cluster, String idemKey) throws Exception {
        AtomicInteger acks = new AtomicInteger();
        OmsClusterNodeBootstrap.ClusterNodePaths paths = cluster.pathsForClient();
        ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(OmsClusterWireFormat.MAX_COMMAND_BYTES);
        UUID orderId = UUID.randomUUID();
        AcceptOrderCommand cmd =
                new AcceptOrderCommand(
                        1L,
                        orderId,
                        System.nanoTime(),
                        10_000_000_000L,
                        0L,
                        0,
                        AcceptOrderCommand.SIDE_BUY,
                        AcceptOrderCommand.TIF_DAY,
                        "failover-acct",
                        idemKey,
                        "hash-" + idemKey,
                        "AAPL",
                        null);
        int written = cmd.encode(buffer, 0);

        EgressListener listener =
                new EgressListener() {
                    @Override
                    public void onMessage(
                            long clusterSessionId,
                            long timestamp,
                            DirectBuffer buf,
                            int offset,
                            int length,
                            io.aeron.logbuffer.Header header) {
                        int typeId = buf.getInt(offset + OmsClusterWireFormat.HEADER_TYPE_ID_OFFSET);
                        if (typeId == OmsClusterWireFormat.TYPE_ID_ORDER_ACCEPTED) {
                            acks.incrementAndGet();
                        }
                    }

                    @Override
                    public void onSessionEvent(
                            long correlationId,
                            long clusterSessionId,
                            long leadershipTermId,
                            int leaderMemberId,
                            EventCode code,
                            String detail) {}
                };

        AeronCluster.Context ctx =
                new AeronCluster.Context()
                        .aeronDirectoryName(paths.aeronDirectory())
                        .ingressChannel("aeron:udp?endpoint=localhost:0")
                        .ingressEndpoints(OmsThreeNodeClusterHarness.INGRESS_ENDPOINTS)
                        .egressChannel("aeron:udp?endpoint=localhost:0")
                        .egressListener(listener)
                        .messageTimeoutNs(MESSAGE_TIMEOUT_NS);

        try (AeronCluster client =
                await().atMost(CONNECT_TIMEOUT)
                        .pollInterval(Duration.ofMillis(100))
                        .ignoreExceptions()
                        .until(() -> AeronCluster.connect(ctx.clone()), c -> c != null)) {

            long offerResult;
            do {
                offerResult = client.offer(buffer, 0, written);
            } while (offerResult < 0L);

            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                client.pollEgress();
                assertThat(acks.get()).isEqualTo(1);
            });
        }
    }

    private static int maxAdmitted(OmsThreeNodeClusterHarness cluster) {
        return Math.max(
                cluster.serviceOnMember(0).admittedOrderCount(),
                Math.max(
                        cluster.serviceOnMember(1).admittedOrderCount(),
                        cluster.serviceOnMember(2).admittedOrderCount()));
    }
}
