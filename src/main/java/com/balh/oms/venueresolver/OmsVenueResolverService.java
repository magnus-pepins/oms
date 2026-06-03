package com.balh.oms.venueresolver;

import com.balh.oms.cluster.ApplyExecutionReportCommand;
import com.balh.oms.cluster.ApplyVenueResolutionCommand;
import com.balh.oms.cluster.OmsClusterIngressClient;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.config.OmsProfiles;
import com.balh.venue.cluster.TradeMatchedEvent;
import com.balh.venue.cluster.VenueClusterWireFormat;
import com.balh.venue.cluster.VenueResolutionEvent;
import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.logbuffer.FragmentHandler;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.agrona.CloseHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Tails the balh-venue cluster events recording and bridges venue→OMS state into the OMS cluster:
 *
 * <ul>
 *   <li><b>{@link VenueResolutionEvent}</b> → {@link ApplyVenueResolutionCommand} (Phase B; idempotent
 *       on symbol + evidence hash).</li>
 *   <li><b>{@link TradeMatchedEvent}</b> → maker-side {@link ApplyExecutionReportCommand} (Phase G
 *       slice 2; idempotent on {@code (orderId, venueExecRef)}). The synchronous {@code routeOrder} ack
 *       only carries the taker's fill, so this is the only path by which a resting (maker) order learns
 *       it filled.</li>
 * </ul>
 *
 * <p>Single instance per venue stream (the cursor is single-writer). The cursor only advances past
 * fragments this service handled, so an unhandled event type is never skipped silently.
 */
@Component
@Profile(OmsProfiles.VENUE_EGRESS)
@ConditionalOnProperty(prefix = "oms.cluster.venue-resolver", name = "enabled", havingValue = "true")
public class OmsVenueResolverService {

    private static final Logger log = LoggerFactory.getLogger(OmsVenueResolverService.class);
    static final String RESOLVER_ID = "oms-venue-resolver-default";

    private final OmsConfig config;
    private final OmsVenueResolverCursorRepository cursorRepository;
    private final OmsClusterIngressClient clusterIngressClient;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong lastAppliedPosition = new AtomicLong(0L);
    private Thread replayThread;

    @Autowired
    public OmsVenueResolverService(
            OmsConfig config,
            OmsVenueResolverCursorRepository cursorRepository,
            @Autowired(required = false) OmsClusterIngressClient clusterIngressClient) {
        this.config = config;
        this.cursorRepository = cursorRepository;
        this.clusterIngressClient = clusterIngressClient;
    }

    /** Visible for integration tests awaiting resolver progress. */
    public long lastAppliedPosition() {
        return lastAppliedPosition.get();
    }

    @PostConstruct
    void init() {
        OmsConfig.Cluster.VenueResolver cfg = config.getCluster().getVenueResolver();
        if (clusterIngressClient == null) {
            log.warn("oms-venue-resolver disabled: OmsClusterIngressClient bean absent");
            return;
        }
        if (cfg.getVenueAeronDirectory().isBlank()) {
            log.warn("oms-venue-resolver disabled: oms.cluster.venue-resolver.venue-aeron-directory is empty");
            return;
        }
        running.set(true);
        replayThread = new Thread(this::replayLoop, "oms-venue-resolver-replay");
        replayThread.setDaemon(true);
        replayThread.start();
        log.info(
                "oms-venue-resolver started (venueAeronDir={}, venueEventsStreamId={}, replayStreamId={})",
                cfg.getVenueAeronDirectory(),
                VenueClusterWireFormat.EVENTS_STREAM_ID,
                cfg.getReplayStreamId());
    }

    @PreDestroy
    void close() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        if (replayThread != null) {
            replayThread.interrupt();
            try {
                replayThread.join(2_000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void replayLoop() {
        OmsConfig.Cluster.VenueResolver cfg = config.getCluster().getVenueResolver();
        Aeron aeron = null;
        AeronArchive archive = null;
        try {
            aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(cfg.getVenueAeronDirectory()));
            archive = AeronArchive.connect(new AeronArchive.Context()
                    .aeron(aeron)
                    .ownsAeronClient(false)
                    .controlRequestChannel(cfg.getArchiveControlRequestChannel())
                    .controlResponseChannel(cfg.getArchiveControlResponseChannel()));

            var savedCursor =
                    cursorRepository.findCursor(RESOLVER_ID, VenueClusterWireFormat.EVENTS_STREAM_ID);
            long recordingId;
            long startPosition;
            if (savedCursor.isPresent()) {
                recordingId = savedCursor.get().recordingId();
                startPosition = savedCursor.get().position();
            } else {
                recordingId = awaitRecordingAndBootstrapCursor(archive, cfg);
                if (recordingId < 0L) {
                    return;
                }
                startPosition = 0L;
            }

            Subscription replay = archive.replay(
                    recordingId,
                    startPosition,
                    Long.MAX_VALUE,
                    cfg.getReplayChannel(),
                    cfg.getReplayStreamId());

            FragmentHandler handler = (buffer, offset, length, header) -> {
                int typeId = buffer.getInt(offset + VenueClusterWireFormat.HEADER_TYPE_ID_OFFSET);
                try {
                    switch (typeId) {
                        case VenueClusterWireFormat.TYPE_ID_VENUE_RESOLUTION ->
                                submitResolution(VenueResolutionEvent.decode(buffer, offset, length));
                        case VenueClusterWireFormat.TYPE_ID_TRADE_MATCHED ->
                                submitMakerFill(TradeMatchedEvent.decode(buffer, offset, length));
                        default -> {
                            // Other venue events (quotes, rested, etc.) are not consumed here; leave the
                            // cursor where it is so we never record progress past an unhandled fragment.
                            return;
                        }
                    }
                    long position = header.position();
                    lastAppliedPosition.set(position);
                    cursorRepository.advance(
                            RESOLVER_ID, VenueClusterWireFormat.EVENTS_STREAM_ID, recordingId, position);
                } catch (Exception e) {
                    log.error("oms-venue-resolver failed to apply venue fragment typeId={}", typeId, e);
                }
            };

            while (running.get()) {
                int polled = replay.poll(handler, cfg.getFragmentLimit());
                if (polled == 0) {
                    LockSupport.parkNanos(cfg.getPollParkNanos());
                }
            }
        } catch (RuntimeException e) {
            log.error("oms-venue-resolver replay loop terminating", e);
        } finally {
            CloseHelper.quietClose(archive);
            CloseHelper.quietClose(aeron);
            log.info("oms-venue-resolver replay loop stopped");
        }
    }

    private long awaitRecordingAndBootstrapCursor(AeronArchive archive, OmsConfig.Cluster.VenueResolver cfg) {
        while (running.get()) {
            List<long[]> recordings = new ArrayList<>();
            archive.listRecordingsForUri(
                    0L,
                    1024,
                    VenueClusterWireFormat.EVENTS_CHANNEL,
                    VenueClusterWireFormat.EVENTS_STREAM_ID,
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
                            sourceIdentity) ->
                            recordings.add(new long[] {recordingId, startPosition}));
            if (!recordings.isEmpty()) {
                recordings.sort(Comparator.comparingLong(a -> a[0]));
                long recordingId = recordings.getFirst()[0];
                long startPosition = recordings.getFirst()[1];
                cursorRepository
                        .findCursor(RESOLVER_ID, VenueClusterWireFormat.EVENTS_STREAM_ID)
                        .ifPresentOrElse(
                                c -> {},
                                () -> cursorRepository.resetCursor(
                                        RESOLVER_ID,
                                        VenueClusterWireFormat.EVENTS_STREAM_ID,
                                        recordingId,
                                        startPosition));
                return recordingId;
            }
            LockSupport.parkNanos(cfg.getRecordingLookupParkMs() * 1_000_000L);
        }
        return -1L;
    }

    /**
     * Deliver the <b>maker</b> side of a venue trade to the OMS cluster. The synchronous
     * {@code routeOrder} ack only reports the taker's fill, so without this a resting order that fills
     * never learns it filled (latent today with thin two-book flow; load-bearing for the Phase G
     * one-book model where most fills are maker fills). Idempotent: the cluster dedupes on
     * {@code (orderId, venueExecRef)} ({@link com.balh.oms.cluster.OmsAdmissionClusteredService}), so
     * replays after a restart re-submit harmlessly. Legacy events (pre-maker-field) decode to
     * {@link TradeMatchedEvent#NO_MAKER} and are skipped.
     */
    void submitMakerFill(TradeMatchedEvent event)
            throws InterruptedException, java.util.concurrent.TimeoutException {
        java.util.UUID makerOrderId = event.makerOrderId();
        if (makerOrderId == null || makerOrderId.equals(TradeMatchedEvent.NO_MAKER)) {
            return;
        }
        String venueId = config.getVenue().getVenueId();
        ApplyExecutionReportCommand cmd =
                new ApplyExecutionReportCommand(
                        0L,
                        makerOrderId,
                        event.lastQtyScaled(),
                        event.makerPxScaled(),
                        event.venueTsNanos(),
                        0,
                        ApplyExecutionReportCommand.EXEC_TYPE_TRADE,
                        (byte) 0,
                        venueId,
                        event.venueExecRef(),
                        "",
                        "{\"source\":\"balh-venue-maker-fill\"}");
        clusterIngressClient.submitApplyExecutionReport(
                cmd, Duration.ofMillis(config.getCluster().getVenueResolver().getOfferTimeoutMs()));
        log.debug(
                "oms-venue-resolver applied maker fill makerOrderId={} qty={} makerPx={} execRef={}",
                makerOrderId,
                event.lastQtyScaled(),
                event.makerPxScaled(),
                event.venueExecRef());
    }

    private void submitResolution(VenueResolutionEvent event) throws InterruptedException, java.util.concurrent.TimeoutException {
        ApplyVenueResolutionCommand cmd =
                new ApplyVenueResolutionCommand(
                        System.nanoTime(),
                        event.instrumentSymbol(),
                        event.outcomeCode(),
                        event.resolutionSource(),
                        event.resolutionTimestampMillis(),
                        event.evidenceHash(),
                        config.getVenue().getVenueId());
        clusterIngressClient.submitApplyVenueResolution(
                cmd, Duration.ofMillis(config.getCluster().getVenueResolver().getOfferTimeoutMs()));
        log.info(
                "oms-venue-resolver submitted ApplyVenueResolutionCommand symbol={} outcome={} hash={}",
                event.instrumentSymbol(),
                event.outcomeCode(),
                event.evidenceHash());
    }
}
