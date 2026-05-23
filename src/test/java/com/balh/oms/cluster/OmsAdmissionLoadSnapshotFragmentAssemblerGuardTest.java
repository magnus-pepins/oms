package com.balh.oms.cluster;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.ImageFragmentAssembler;
import io.aeron.cluster.service.Cluster;
import io.aeron.logbuffer.FragmentHandler;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.agrona.DirectBuffer;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Structural regression guard for the 2026-05-23 "snapshot magic mismatch" incident.
 *
 * <p><b>Why this test exists.</b> The chaos integration test
 * {@code OmsClusterChaosIT#largeSnapshotSurvivesFragmentationAcrossRestart} proves end-to-end
 * that a &gt;MTU snapshot reassembles cleanly across restart. That test relies on a real Aeron
 * cluster fragmenting the snapshot on the way out — if Aeron's MTU defaults change, or if a
 * future refactor reduces the on-disk snapshot size below ~1376 bytes for some reason, the
 * functional IT would silently stop exercising the fragmentation path while still going green.
 *
 * <p>This test pins the <em>structural contract</em>: {@link OmsAdmissionClusteredService} MUST
 * wrap its snapshot loader in {@link ImageFragmentAssembler} before handing it to
 * {@link Image#poll(FragmentHandler, int)}. If anyone unwraps the assembler in a future cleanup
 * ("the loader handles fragments fine on its own"), this test fails immediately — well before
 * the bug reaches a real cluster where a 67-order admission snapshot bricks production.
 *
 * <p>Per {@code .cursor/rules/serious-debugging.mdc}: this is the falsifier for the hypothesis
 * "loadSnapshot still wraps in ImageFragmentAssembler". It runs at unit-test speed (no Aeron,
 * no Testcontainers, no PM2) so it's cheap to keep green forever.
 *
 * <p>Sibling pin in {@code ledger-cluster}:
 * {@code LedgerStateLoadSnapshotFragmentAssemblerGuardTest} — same root cause, same fix, same
 * regression guard. Both must stay in lock-step or one of them is lying.
 */
class OmsAdmissionLoadSnapshotFragmentAssemblerGuardTest {

    @Test
    void loadSnapshot_wrapsLoaderInImageFragmentAssembler() {
        OmsAdmissionClusteredService service = new OmsAdmissionClusteredService(new SimpleMeterRegistry());
        Cluster cluster = mock(Cluster.class);
        when(cluster.role()).thenReturn(Cluster.Role.FOLLOWER);
        Aeron aeron = mock(Aeron.class);
        ExclusivePublication eventsPub = mock(ExclusivePublication.class);
        when(eventsPub.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenReturn(1L);
        OmsAdmissionClusteredServiceTestFixtures.wireClusterAeronMocks(aeron, eventsPub);
        when(cluster.aeron()).thenReturn(aeron);

        Image snapshotImage = mock(Image.class);
        AtomicBoolean ended = new AtomicBoolean(false);
        // Empty snapshot path: first poll marks end-of-stream without delivering anything.
        // We don't care about decoded state here — we only care WHICH handler the service hands
        // to image.poll(...).
        when(snapshotImage.poll(any(FragmentHandler.class), anyInt())).thenAnswer(inv -> {
            ended.set(true);
            return 0;
        });
        when(snapshotImage.isEndOfStream()).thenAnswer(inv -> ended.get());

        service.onStart(cluster, snapshotImage);

        ArgumentCaptor<FragmentHandler> handlerCaptor = ArgumentCaptor.forClass(FragmentHandler.class);
        verify(snapshotImage, atLeastOnce()).poll(handlerCaptor.capture(), anyInt());
        assertThat(handlerCaptor.getValue())
                .as(
                        "OmsAdmissionClusteredService#loadSnapshot must wrap its SnapshotLoader in"
                                + " io.aeron.ImageFragmentAssembler. Without the wrapper a snapshot"
                                + " that exceeds MTU (~1376 bytes — i.e. ~70 admitted orders on the"
                                + " 2026-05-23 schema) delivers as N onFragment calls, the loader"
                                + " reads bytes from offset 0 of each fragment as if they were"
                                + " the magic header, and pop bricks on restart with `snapshot"
                                + " magic mismatch`. See plans/oms-cluster-recovery-and-hardening.md"
                                + " and the 2026-05-23 handover for the full failure narrative.")
                .isInstanceOf(ImageFragmentAssembler.class);
    }
}
