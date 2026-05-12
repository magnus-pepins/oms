package com.balh.oms.cluster;

import com.balh.oms.AbstractPostgresIntegrationTest;
import com.balh.oms.OmsClusterNodeBootstrap;
import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Phase 2 slice 2b-1: verifies the cluster service emits {@link OrderAdmittedEvent}s to the projector
 * event stream ({@link OmsClusterWireFormat#EVENTS_CHANNEL} / {@link OmsClusterWireFormat#EVENTS_STREAM_ID})
 * and that the cluster member's Aeron Archive captures them — i.e. the recording the projector will
 * replay from in slice 2b-2 actually has the events in it.
 *
 * <p>Connects to the JVM-wide {@link AbstractPostgresIntegrationTest} singleton cluster, looks up the
 * recording for the events stream via {@link AeronArchive#listRecordings}, opens a replay subscription
 * from position 0, and asserts the decoded {@code OrderAdmittedEvent} matches the order submitted via
 * {@link OmsClusterIngressClient}.
 */
class ClusterEventsRecordingIT extends AbstractPostgresIntegrationTest {

    private static final Duration RECORDING_WAIT = Duration.ofSeconds(20);
    private static final Duration EVENT_WAIT = Duration.ofSeconds(20);

    @Autowired
    OmsClusterIngressClient ingressClient;

    @Test
    void freshAdmission_isCapturedByEventsRecording() throws Exception {
        UUID orderId = UUID.randomUUID();
        AcceptOrderCommand cmd = new AcceptOrderCommand(
                ingressClient.nextCorrelationId(),
                orderId,
                /* clientTimestampNanos = */ System.nanoTime(),
                /* quantityScaled = */ 10_000_000_000L,
                /* limitPriceScaledOrZero = */ 0L,
                /* shardId = */ 0,
                AcceptOrderCommand.SIDE_BUY,
                AcceptOrderCommand.TIF_DAY,
                "events-rec-account",
                "events-rec-idem-" + orderId,
                "events-rec-hash",
                "AAPL",
                /* ledgerBalanceIdOrNull = */ null);

        AdmissionResult result = ingressClient.submitAcceptOrder(cmd, Duration.ofSeconds(10));
        assertThat(result).isInstanceOf(AdmissionResult.Accepted.class);
        AdmissionResult.Accepted accepted = (AdmissionResult.Accepted) result;
        assertThat(accepted.event().duplicate()).isFalse();
        assertThat(accepted.event().orderId()).isEqualTo(orderId);

        Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(testClusterAeronDirectory()));
        AeronArchive archive = AeronArchive.connect(new AeronArchive.Context()
                .aeron(aeron)
                .ownsAeronClient(false)
                .controlRequestChannel("aeron:ipc?term-length=64k")
                .controlResponseChannel("aeron:ipc?term-length=64k"));
        try {
            long recordingId = await()
                    .atMost(RECORDING_WAIT)
                    .pollInterval(Duration.ofMillis(100))
                    .until(() -> latestRecordingFor(archive), id -> id >= 0);

            Subscription replay = archive.replay(
                    recordingId,
                    /* position = */ 0L,
                    /* length = */ Long.MAX_VALUE,
                    /* replayChannel = */ "aeron:ipc?term-length=64k",
                    /* replayStreamId = */ 4321);

            try {
                AtomicReference<OrderAdmittedEvent> captured = new AtomicReference<>();
                await()
                        .atMost(EVENT_WAIT)
                        .pollInterval(Duration.ofMillis(50))
                        .untilAsserted(() -> {
                            replay.poll((buffer, offset, length, header) -> {
                                int typeId = buffer.getInt(offset + OmsClusterWireFormat.HEADER_TYPE_ID_OFFSET);
                                if (typeId == OmsClusterWireFormat.TYPE_ID_ORDER_ADMITTED) {
                                    byte[] copy = new byte[length];
                                    buffer.getBytes(offset, copy);
                                    OrderAdmittedEvent ev = OrderAdmittedEvent.decode(
                                            new UnsafeBuffer(copy), 0, length);
                                    if (ev.orderId().equals(orderId)) {
                                        captured.set(ev);
                                    }
                                }
                            }, /* fragmentLimit = */ 16);
                            assertThat(captured.get()).isNotNull();
                        });

                OrderAdmittedEvent ev = captured.get();
                assertThat(ev.accountId()).isEqualTo(cmd.accountId());
                assertThat(ev.clientIdempotencyKey()).isEqualTo(cmd.clientIdempotencyKey());
                assertThat(ev.instrumentSymbol()).isEqualTo(cmd.instrumentSymbol());
                assertThat(ev.quantityScaled()).isEqualTo(cmd.quantityScaled());
                assertThat(ev.shardId()).isEqualTo(cmd.shardId());
                assertThat(ev.side()).isEqualTo(cmd.side());
            } finally {
                CloseHelper.quietClose(replay);
            }
        } finally {
            CloseHelper.quietClose(archive);
            CloseHelper.quietClose(aeron);
        }
    }

    /**
     * Returns the recording id for the events stream, or {@code -1} if the Archive has not yet started
     * recording the publication. The cluster service opens the publication on {@code onStart}; the
     * Archive engages the previously-registered recording subscription on the next driver tick.
     */
    private static long latestRecordingFor(AeronArchive archive) {
        long[] result = {-1L};
        archive.listRecordingsForUri(
                /* fromRecordingId = */ 0L,
                /* recordCount = */ 1024,
                OmsClusterWireFormat.EVENTS_CHANNEL,
                OmsClusterWireFormat.EVENTS_STREAM_ID,
                (controlSessionId,
                        correlationId,
                        recordingId,
                        startTimestamp,
                        stopTimestamp,
                        startPosition,
                        stopPosition,
                        initialTermId,
                        segmentFileLength,
                        termBufferLength,
                        mtuLength,
                        sessionId,
                        streamId,
                        strippedChannel,
                        originalChannel,
                        sourceIdentity) -> {
                    if (streamId == OmsClusterWireFormat.EVENTS_STREAM_ID && recordingId > result[0]) {
                        result[0] = recordingId;
                    }
                });
        return result[0];
    }
}
