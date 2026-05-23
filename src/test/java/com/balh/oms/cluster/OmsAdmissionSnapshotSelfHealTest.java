package com.balh.oms.cluster;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.service.Cluster;
import io.aeron.logbuffer.FragmentHandler;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Phase 7 self-healing tests — {@code plans/oms-cluster-recovery-and-hardening.md} §8.
 *
 * <p>Proves the two write-/read-side invariants that close the 2026-05-23 "corrupt snapshot
 * bricks the cluster" failure shape:
 *
 * <ol>
 *   <li><b>Read-side self-heal.</b> A snapshot the decoder rejects (magic mismatch, version
 *       mismatch, truncated buffer, mid-decode RuntimeException) must NOT propagate out of
 *       {@link OmsAdmissionClusteredService#onStart} — instead the service clears partial state,
 *       flips an internal flag, increments a published counter, and continues. Archive replay
 *       then rebuilds state from the log.</li>
 *   <li><b>Write-side verify.</b> {@link OmsAdmissionClusteredService#onTakeSnapshot} runs an
 *       encode/decode round-trip on the bytes it just encoded BEFORE publishing them. A snapshot
 *       the same JVM's decoder can't read is aborted at the source — it never lands in the
 *       archive as a poison-pill the next restart inherits.</li>
 * </ol>
 *
 * <p>Unit-test scope: uses the same Mockito {@code Cluster}/{@code Aeron}/{@code Image} pattern
 * that {@link OmsAdmissionClusteredServiceTest} uses, so this is fast (no real cluster) and runs
 * inside the regular {@code ./gradlew test} graph alongside the other admission unit tests.
 */
class OmsAdmissionSnapshotSelfHealTest {

    /** Bytes that look like ASCII text — mirrors the {@code 0x345f6563} ("ce_4") payload pop saw. */
    private static final byte[] ASCII_GARBAGE = "ce_4-not-an-OMSA-snapshot-payload-at-all".getBytes();

    @Test
    void corruptMagic_isSelfHealedRatherThanThrown() {
        OmsAdmissionClusteredService svc = freshService();
        Image corrupt = mockSnapshotImage(ASCII_GARBAGE);

        // The ProductionBug™: snapshot decode throws IllegalStateException("snapshot magic mismatch ...").
        // After Phase 7 self-healing, onStart MUST NOT propagate that throw — the cluster boots
        // past it, partial state is wiped, archive replay will rebuild from the log.
        svc.onStart(clusterMockWithAeron(), corrupt);

        assertThat(svc.snapshotLoadFailedOnStartForTest())
                .as("self-heal flag must be set after corrupt-magic recovery")
                .isTrue();
        assertThat(svc.snapshotLoadedAtStartForTest())
                .as("loaded flag must be false when decode failed")
                .isFalse();
        assertThat(svc.snapshotLoadFailedCountForTest()).isEqualTo(1L);
        assertThat(svc.admittedOrderCount())
                .as("partial state from failed load must be cleared")
                .isZero();
    }

    @Test
    void truncatedSnapshot_isSelfHealedRatherThanThrown() {
        OmsAdmissionClusteredService svc = freshService();
        // Valid magic + version header, then nothing — the orderCount read will throw IndexOutOfBoundsException.
        UnsafeBuffer truncated = new UnsafeBuffer(new byte[Integer.BYTES * 2]);
        truncated.putInt(0, 0x4F4D5341); // "OMSA"
        truncated.putInt(Integer.BYTES, 4); // SNAPSHOT_SCHEMA_VERSION
        Image image = mockSnapshotImage(java.util.Arrays.copyOf(truncated.byteArray(), Integer.BYTES * 2));

        svc.onStart(clusterMockWithAeron(), image);

        assertThat(svc.snapshotLoadFailedOnStartForTest()).isTrue();
        assertThat(svc.snapshotLoadFailedCountForTest()).isEqualTo(1L);
    }

    @Test
    void unsupportedSchemaVersion_isSelfHealedRatherThanThrown() {
        OmsAdmissionClusteredService svc = freshService();
        UnsafeBuffer futureVersion = new UnsafeBuffer(new byte[Integer.BYTES * 3]);
        futureVersion.putInt(0, 0x4F4D5341);
        futureVersion.putInt(Integer.BYTES, /* version = */ 99);
        futureVersion.putInt(Integer.BYTES * 2, /* orderCount = */ 0);
        Image image = mockSnapshotImage(java.util.Arrays.copyOf(futureVersion.byteArray(), futureVersion.capacity()));

        svc.onStart(clusterMockWithAeron(), image);

        assertThat(svc.snapshotLoadFailedOnStartForTest())
                .as("a snapshot from a future code version must self-heal, not brick the cluster")
                .isTrue();
        assertThat(svc.snapshotLoadFailedCountForTest()).isEqualTo(1L);
    }

    @Test
    void emptyValidSnapshot_loadsCleanlyWithoutSelfHealFlag() {
        OmsAdmissionClusteredService svc = freshService();
        UnsafeBuffer empty = new UnsafeBuffer(new byte[Integer.BYTES * 5]);
        empty.putInt(0, 0x4F4D5341);
        empty.putInt(Integer.BYTES, 4);
        empty.putInt(Integer.BYTES * 2, /* orderCount = */ 0);
        empty.putInt(Integer.BYTES * 3, /* orderCountWithRefs = */ 0);
        empty.putInt(Integer.BYTES * 4, /* senderCountWithSeqs = */ 0);
        Image image = mockSnapshotImage(java.util.Arrays.copyOf(empty.byteArray(), empty.capacity()));

        svc.onStart(clusterMockWithAeron(), image);

        assertThat(svc.snapshotLoadFailedOnStartForTest())
                .as("a structurally valid empty snapshot must NOT trip self-heal")
                .isFalse();
        assertThat(svc.snapshotLoadedAtStartForTest()).isTrue();
        assertThat(svc.snapshotLoadFailedCountForTest()).isZero();
    }

    @Test
    void writeSideRoundTripVerify_passesForEmptyState() {
        OmsAdmissionClusteredService svc = freshService();
        svc.onStart(clusterMockWithAeron(), null);
        // No orders admitted — the encoder writes just the 5-int header. The round-trip verify
        // must accept it; if it didn't, every healthy snapshot would abort.
        byte[] bytes = takeSnapshotBytes(svc);
        assertThat(bytes.length)
                .as("empty snapshot is magic(4) + version(4) + orderCount(0) + execRefCount(0) + senderCount(0) = 20 bytes")
                .isEqualTo(Integer.BYTES * 5);
    }

    @Test
    void writeSideRoundTripVerify_rejectsWrongMagic() {
        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(64);
        buf.putInt(0, 0xDEADBEEF); // wrong magic
        buf.putInt(Integer.BYTES, 4);
        buf.putInt(Integer.BYTES * 2, 0);
        buf.putInt(Integer.BYTES * 3, 0);
        buf.putInt(Integer.BYTES * 4, 0);
        final int encodedLength = Integer.BYTES * 5;
        assertThatThrownBy(() -> OmsAdmissionClusteredService.verifySnapshotRoundTrip(buf, encodedLength))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("SNAPSHOT_LOAD_FAILED")
                .hasMessageContaining("write-side verify");
    }

    @Test
    void writeSideRoundTripVerify_rejectsWrongLengthDeclaration() {
        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(64);
        buf.putInt(0, 0x4F4D5341);
        buf.putInt(Integer.BYTES, 4);
        buf.putInt(Integer.BYTES * 2, /* orderCount = */ 0);
        buf.putInt(Integer.BYTES * 3, /* orderCountWithRefs = */ 0);
        buf.putInt(Integer.BYTES * 4, /* senderCountWithSeqs = */ 0);
        // Tell verify the buffer is longer than what we actually encoded. The decoder consumes
        // exactly 20 bytes; verify must catch the mismatch ("decoder consumed N but encode wrote M").
        assertThatThrownBy(() -> OmsAdmissionClusteredService.verifySnapshotRoundTrip(buf, /* encodedLength = */ 32))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("SNAPSHOT_LOAD_FAILED")
                .hasMessageContaining("out of sync");
    }

    @Test
    void writeSideRoundTripVerify_acceptsValidEmptySnapshot() {
        ExpandableArrayBuffer buf = new ExpandableArrayBuffer(64);
        buf.putInt(0, 0x4F4D5341);
        buf.putInt(Integer.BYTES, 4);
        buf.putInt(Integer.BYTES * 2, 0);
        buf.putInt(Integer.BYTES * 3, 0);
        buf.putInt(Integer.BYTES * 4, 0);
        // Should not throw.
        OmsAdmissionClusteredService.verifySnapshotRoundTrip(buf, Integer.BYTES * 5);
    }

    // ---- mock plumbing ----------------------------------------------------------------------

    private OmsAdmissionClusteredService freshService() {
        return new OmsAdmissionClusteredService(new SimpleMeterRegistry());
    }

    private Cluster clusterMockWithAeron() {
        Cluster cluster = mock(Cluster.class);
        when(cluster.role()).thenReturn(Cluster.Role.FOLLOWER);
        Aeron aeron = mock(Aeron.class);
        ExclusivePublication eventsPub = mock(ExclusivePublication.class);
        when(eventsPub.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenReturn(1L);
        OmsAdmissionClusteredServiceTestFixtures.wireClusterAeronMocks(aeron, eventsPub);
        when(cluster.aeron()).thenReturn(aeron);
        return cluster;
    }

    private static Image mockSnapshotImage(byte[] bytes) {
        Image image = mock(Image.class);
        AtomicBoolean delivered = new AtomicBoolean(false);
        when(image.isEndOfStream()).thenAnswer(inv -> delivered.get());
        when(image.poll(any(FragmentHandler.class), anyInt())).thenAnswer(inv -> {
            if (delivered.get()) {
                return 0;
            }
            FragmentHandler handler = inv.getArgument(0);
            UnsafeBuffer buf = new UnsafeBuffer(bytes);
            handler.onFragment(buf, 0, bytes.length, null);
            delivered.set(true);
            return 1;
        });
        return image;
    }

    private static byte[] takeSnapshotBytes(OmsAdmissionClusteredService svc) {
        ExpandableArrayBuffer accumulator = new ExpandableArrayBuffer(1024);
        int[] written = {0};
        ExclusivePublication snapshotPub = mock(ExclusivePublication.class);
        when(snapshotPub.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenAnswer(inv -> {
            DirectBuffer src = inv.getArgument(0);
            int off = inv.getArgument(1);
            int len = inv.getArgument(2);
            accumulator.putBytes(written[0], src, off, len);
            written[0] += len;
            return 1L;
        });
        svc.onTakeSnapshot(snapshotPub);
        byte[] copy = new byte[written[0]];
        accumulator.getBytes(0, copy);
        return copy;
    }
}
