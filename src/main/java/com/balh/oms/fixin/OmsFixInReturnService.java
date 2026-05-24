package com.balh.oms.fixin;

import com.balh.oms.cluster.ExecutionAppliedEvent;
import com.balh.oms.cluster.OmsClusterWireFormat;
import com.balh.oms.cluster.OrderAdmittedEvent;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.config.OmsProfiles;
import com.balh.oms.fixin.persistence.FixInReturnCursorRepository;
import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Consumes the cluster events recording and publishes client-facing FIX 8/9 for FIX-in orders.
 * Mirrors {@link com.balh.oms.fixegress.OmsFixEgressService} replay shape on a separate stream id.
 */
@Component
@Profile(OmsProfiles.FIX_INGRESS)
@ConditionalOnProperty(prefix = "oms.fix-in.return-publisher", name = "enabled", havingValue = "true")
public class OmsFixInReturnService {

    private static final Logger log = LoggerFactory.getLogger(OmsFixInReturnService.class);
    public static final String RETURN_EGRESS_ID = "oms-fix-in-return-default";

    private final OmsConfig config;
    private final FixInReturnCursorRepository cursorRepository;
    private final FixInReturnPublisher returnPublisher;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong lastAppliedPosition = new AtomicLong(0L);
    private final AtomicLong currentRecordingId = new AtomicLong(-1L);
    /** Deferred until {@code orders} projection catches up; keyed by recording position. */
    private final ConcurrentHashMap<Long, ExecutionAppliedEvent> pendingExecutionApplied =
            new ConcurrentHashMap<>();
    private Thread replayThread;

    public OmsFixInReturnService(
            OmsConfig config,
            FixInReturnCursorRepository cursorRepository,
            FixInReturnPublisher returnPublisher) {
        this.config = config;
        this.cursorRepository = cursorRepository;
        this.returnPublisher = returnPublisher;
    }

    @PostConstruct
    void init() {
        OmsConfig.FixIn.ReturnPublisher cfg = config.getFixIn().getReturnPublisher();
        if (cfg.getAeronDirectory().isBlank()) {
            log.warn("oms-fix-in-return replay skipped: oms.fix-in.return-publisher.aeron-directory is empty");
            return;
        }
        cursorRepository
                .find(RETURN_EGRESS_ID)
                .ifPresent(c -> {
                    lastAppliedPosition.set(c.position());
                    currentRecordingId.set(c.recordingId());
                });
        running.set(true);
        replayThread = new Thread(this::replayLoop, "oms-fix-in-return-replay");
        replayThread.setDaemon(true);
        replayThread.start();
        log.info("oms-fix-in-return replay started (streamId={})", cfg.getReplayStreamId());
    }

    @PreDestroy
    void close() {
        running.set(false);
        if (replayThread != null) {
            replayThread.interrupt();
        }
    }

    boolean applyExecutionAppliedForTest(ExecutionAppliedEvent ev, long newPosition) {
        boolean sent = returnPublisher.publishExecutionApplied(ev);
        if (sent) {
            advanceCursor(newPosition);
        }
        return sent;
    }

    private void replayLoop() {
        OmsConfig.FixIn.ReturnPublisher cfg = config.getFixIn().getReturnPublisher();
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(cfg.getAeronDirectory()));
                AeronArchive archive =
                        AeronArchive.connect(new AeronArchive.Context()
                                .aeron(aeron)
                                .controlRequestChannel(cfg.getArchiveControlRequestChannel())
                                .controlResponseChannel(cfg.getArchiveControlResponseChannel()))) {
            long recordingId = waitForLatestRecordingId(archive, cfg);
            currentRecordingId.set(recordingId);
            long startPos = cursorRepository
                    .find(RETURN_EGRESS_ID)
                    .map(FixInReturnCursorRepository.RecordedCursor::position)
                    .orElse(lastAppliedPosition.get());
            lastAppliedPosition.set(startPos);
            try (Subscription replay = archive.replay(
                    recordingId,
                    startPos,
                    Long.MAX_VALUE,
                    cfg.getReplayChannel(),
                    cfg.getReplayStreamId())) {
                log.info(
                        "oms-fix-in-return replay open; recordingId={} startPos={} streamId={}",
                        recordingId,
                        startPos,
                        cfg.getReplayStreamId());
                io.aeron.logbuffer.FragmentHandler handler = (buffer, offset, length, header) -> {
                    int typeId = buffer.getInt(offset + OmsClusterWireFormat.HEADER_TYPE_ID_OFFSET);
                    long newPosition = header.position();
                    switch (typeId) {
                        case OmsClusterWireFormat.TYPE_ID_EXECUTION_APPLIED -> {
                            ExecutionAppliedEvent ev = ExecutionAppliedEvent.decode(buffer, offset, length);
                            if (returnPublisher.publishExecutionApplied(ev)) {
                                pendingExecutionApplied.remove(newPosition);
                                advanceCursor(newPosition);
                            } else {
                                pendingExecutionApplied.put(newPosition, ev);
                            }
                        }
                        case OmsClusterWireFormat.TYPE_ID_ORDER_ADMITTED -> {
                            OrderAdmittedEvent ev = OrderAdmittedEvent.decode(buffer, offset, length);
                            returnPublisher.publishOrderAdmitted(ev);
                            advanceCursor(newPosition);
                        }
                        default -> advanceCursor(newPosition);
                    }
                };
                while (running.get()) {
                    int fragments = replay.poll(handler, cfg.getFragmentLimit());
                    drainPendingExecutionApplied();
                    if (fragments == 0) {
                        LockSupport.parkNanos(cfg.getPollParkNanos());
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.info("oms-fix-in-return replay loop interrupted");
        } catch (Exception e) {
            log.error("oms-fix-in-return replay loop failed", e);
        } finally {
            running.set(false);
            log.info("oms-fix-in-return replay loop stopped");
        }
    }

    /** Blocks until the cluster events recording exists (mirrors fix-egress bootstrap). */
    private long waitForLatestRecordingId(AeronArchive archive, OmsConfig.FixIn.ReturnPublisher cfg)
            throws InterruptedException {
        while (running.get()) {
            Long latest = findLatestRecordingIdOrNull(archive);
            if (latest != null) {
                return latest;
            }
            Thread.sleep(cfg.getRecordingLookupParkMs());
        }
        throw new IllegalStateException("oms-fix-in-return shutdown before events recording appeared");
    }

    /** @return latest recording id, or {@code null} when none exist yet (recording ids may be {@code 0}). */
    private static Long findLatestRecordingIdOrNull(AeronArchive archive) {
        final long[] latest = {-1L};
        archive.listRecordingsForUri(
                0L,
                64,
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
                        sourceIdentity) -> latest[0] = Math.max(latest[0], recordingId));
        return latest[0] >= 0L ? latest[0] : null;
    }

    private void drainPendingExecutionApplied() {
        if (pendingExecutionApplied.isEmpty()) {
            return;
        }
        pendingExecutionApplied.forEach((position, ev) -> {
            if (returnPublisher.publishExecutionApplied(ev)) {
                pendingExecutionApplied.remove(position);
                advanceCursor(position);
            }
        });
    }

    private void advanceCursor(long newPosition) {
        lastAppliedPosition.set(newPosition);
        cursorRepository.advanceWithRecording(
                RETURN_EGRESS_ID,
                config.getFixIn().getReturnPublisher().getReplayStreamId(),
                currentRecordingId.get(),
                newPosition);
    }
}
