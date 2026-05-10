package com.balh.oms.routing;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.persistence.ControlRuntimeFlagsRepository;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.UUID;
import java.util.concurrent.BlockingQueue;

/**
 * Return-path projection worker for the simulated broker: drains the post-{@code WORKING}
 * queue and applies synthetic execution reports via {@link SimulatedExecutionProgram}.
 *
 * <p>Slice 4’s FIX inbound path will call the same {@link com.balh.oms.returnpath.ExecutionReportApplier}
 * from a QuickFIX/J {@code Application#fromApp} adapter instead of this worker.
 */
public final class SimulatedReturnPathProjectionWorker {

    private final BlockingQueue<UUID> orderQueue;
    private final SimulatedExecutionProgram executionProgram;
    private final OmsConfig config;
    private final ControlRuntimeFlagsRepository runtimeFlags;

    public SimulatedReturnPathProjectionWorker(
            BlockingQueue<UUID> orderQueue,
            SimulatedExecutionProgram executionProgram,
            OmsConfig config,
            ControlRuntimeFlagsRepository runtimeFlags) {
        this.orderQueue = orderQueue;
        this.executionProgram = executionProgram;
        this.config = config;
        this.runtimeFlags = runtimeFlags;
    }

    @Scheduled(
            initialDelayString = "${oms.routing.simulated.poll-interval-ms:50}",
            fixedDelayString = "${oms.routing.simulated.poll-interval-ms:50}")
    public void scheduledDrain() {
        if (!config.getRouting().getSimulated().isSchedulerEnabled()) {
            return;
        }
        processPendingQueueOnce();
    }

    /** Test hook: process at most one queued order through the programmed fill sequence. */
    public void processPendingQueueOnce() {
        if (runtimeFlags.isCanaryPauseSimulatedFills()) {
            return;
        }
        UUID id = orderQueue.poll();
        if (id == null) {
            return;
        }
        executionProgram.runProgrammedFills(id);
    }
}
