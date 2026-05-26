package com.balh.oms.venueresolver;

import com.balh.oms.cluster.ApplyVenueResolutionCommand;
import com.balh.oms.cluster.OmsClusterIngressClient;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.config.OmsProfiles;
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
 * Phase B: tails balh-venue cluster events recording for {@link VenueResolutionEvent} and submits
 * {@link ApplyVenueResolutionCommand} to the OMS cluster (idempotent on symbol + evidence hash).
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
                if (typeId != VenueClusterWireFormat.TYPE_ID_VENUE_RESOLUTION) {
                    return;
                }
                try {
                    VenueResolutionEvent event = VenueResolutionEvent.decode(buffer, offset, length);
                    submitResolution(event);
                    long position = header.position();
                    lastAppliedPosition.set(position);
                    cursorRepository.advance(
                            RESOLVER_ID, VenueClusterWireFormat.EVENTS_STREAM_ID, recordingId, position);
                } catch (Exception e) {
                    log.error("oms-venue-resolver failed to apply venue resolution fragment", e);
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
