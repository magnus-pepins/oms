package com.balh.oms.fix;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.domain.Order;
import com.balh.oms.domain.OrderStatus;
import com.balh.oms.observability.metrics.OmsPipelineMetrics;
import com.balh.oms.observability.otel.IngressToFixNosLatencyRecorder;
import com.balh.oms.persistence.FixRouteStateRepository;
import com.balh.oms.persistence.FixRouteStateRow;
import com.balh.oms.persistence.OrdersRepository;
import com.balh.oms.returnpath.ExecutionReportApplier;
import com.balh.oms.routing.RouteDispatcher;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import quickfix.SessionNotFound;
import quickfix.fix44.NewOrderSingle;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

/**
 * Drains {@link FixOutboundOrderDequeue} after logon and sends outbound QuickFIX messages on the active session
 * ({@link NewOrderSingle} and {@link quickfix.fix44.OrderMassCancelRequest}).
 *
 * <p>Drain cadence is either Spring scheduling ({@link FixOutboundDriver#SCHEDULED}) or a dedicated thread
 * ({@link FixOutboundDriver#DEDICATED}); see {@code docs/fix-outbound-driver.md}.
 */
public class FixOutboundDispatchWorker implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(FixOutboundDispatchWorker.class);

    /** When {@code outbound-dedicated-not-ready-park-nanos} is 0, avoid hot-spinning while logon or route is disabled. */
    private static final long MIN_NOT_READY_PARK_WHEN_ZERO_NANOS = 1_000_000L;

    private static final long DEDICATED_THREAD_JOIN_TIMEOUT_MS = 5000L;

    private static final long DEDICATED_LOOP_ERROR_BACKOFF_MS = 1000L;

    /** Minimum wait when polling the queue with {@code idleParkNanos == 0} to avoid a zero-timeout spin storm. */
    private static final long MIN_QUEUE_POLL_WAIT_NANOS = 1L;

    private final RouteDispatcher routeEnqueue;
    private final FixOutboundOrderDequeue fixOutboundDequeue;
    private final FixOutboundSessionSend fixOutboundSessionSend;
    private final OrdersRepository ordersRepository;
    private final FixNewOrderSingleBuilder newOrderSingleBuilder;
    private final MeterRegistry meterRegistry;
    private final OmsConfig omsConfig;
    private final ExecutionReportApplier executionReportApplier;
    private final FixRouteStateRepository fixRouteStateRepository;
    private final FixOutboundTokenBucket fixOutboundTokenBucket;
    private final IngressToFixNosLatencyRecorder ingressToFixNosLatencyRecorder;

    private final AtomicBoolean dedicatedStop = new AtomicBoolean(false);

    private Thread dedicatedThread;

    /**
     * While {@link #isFixRouteSendEnabled()} is false, remove {@link FixOutboundWireJob.MassCancelWire} jobs from the
     * head of the queue and complete their waiters exceptionally so HTTP threads do not hang indefinitely.
     *
     * @return {@code true} if at least one mass-cancel waiter was released
     */
    private boolean drainMassCancelsAtHeadWhileRouteDisabled() {
        boolean any = false;
        while (!isFixRouteSendEnabled()) {
            FixOutboundWireJob head = fixOutboundDequeue.peekOrNull();
            if (!(head instanceof FixOutboundWireJob.MassCancelWire)) {
                break;
            }
            FixOutboundWireJob job = fixOutboundDequeue.pollOrNull();
            if (job instanceof FixOutboundWireJob.MassCancelWire mc) {
                log.warn("FIX outbound mass cancel rejected while route send is disabled");
                mc.completion()
                        .completeExceptionally(
                                new IllegalStateException(FixMetrics.FIX_ROUTE_SEND_DISABLED_MESSAGE));
                any = true;
            } else if (job == null) {
                break;
            }
        }
        return any;
    }

    public FixOutboundDispatchWorker(
            RouteDispatcher routeEnqueue,
            FixOutboundOrderDequeue fixOutboundDequeue,
            FixOutboundSessionSend fixOutboundSessionSend,
            OrdersRepository ordersRepository,
            FixNewOrderSingleBuilder newOrderSingleBuilder,
            MeterRegistry meterRegistry,
            OmsConfig omsConfig,
            ExecutionReportApplier executionReportApplier,
            FixRouteStateRepository fixRouteStateRepository,
            FixOutboundTokenBucket fixOutboundTokenBucket,
            IngressToFixNosLatencyRecorder ingressToFixNosLatencyRecorder) {
        this.routeEnqueue = routeEnqueue;
        this.fixOutboundDequeue = fixOutboundDequeue;
        this.fixOutboundSessionSend = fixOutboundSessionSend;
        this.ordersRepository = ordersRepository;
        this.newOrderSingleBuilder = newOrderSingleBuilder;
        this.meterRegistry = meterRegistry;
        this.omsConfig = omsConfig;
        this.executionReportApplier = executionReportApplier;
        this.fixRouteStateRepository = fixRouteStateRepository;
        this.fixOutboundTokenBucket = fixOutboundTokenBucket;
        this.ingressToFixNosLatencyRecorder = ingressToFixNosLatencyRecorder;
    }

    @PostConstruct
    void startDedicatedIfNeeded() {
        if (omsConfig.getFix().getOutboundDriver() != FixOutboundDriver.DEDICATED) {
            log.info(
                    "FIX outbound driver=SCHEDULED pollIntervalMs={}",
                    omsConfig.getFix().getOutboundPollIntervalMs());
            return;
        }
        this.dedicatedThread = new Thread(this::runDedicatedLoop, "oms-fix-outbound-dispatch");
        this.dedicatedThread.setDaemon(false);
        this.dedicatedThread.start();
        log.info(
                "FIX outbound driver=DEDICATED idleParkNanos={} notReadyParkNanos={}",
                omsConfig.getFix().getOutboundDedicatedIdleParkNanos(),
                omsConfig.getFix().getOutboundDedicatedNotReadyParkNanos());
    }

    /**
     * One scheduler tick: at most one outbound job when {@link FixOutboundDriver#SCHEDULED}
     * (see {@code FixAutoStartBeans#fixOutboundPollScheduling}).
     */
    public void drainPendingOutboundOnce() {
        if (!fixOutboundSessionSend.hasActiveSession()) {
            return;
        }
        boolean drainedMassCancel = drainMassCancelsAtHeadWhileRouteDisabled();
        if (!isFixRouteSendEnabled()) {
            if (!drainedMassCancel) {
                meterRegistry.counter(FixMetrics.METRIC_OUTBOUND_ROUTE_DISABLED_SKIPS).increment();
            }
            return;
        }
        FixOutboundWireJob job = fixOutboundDequeue.pollOrNull();
        if (job == null) {
            return;
        }
        dispatchJob(job);
    }

    private void dispatchJob(FixOutboundWireJob job) {
        switch (job) {
            case FixOutboundWireJob.NosOrder nos -> dispatchOutboundForOrderId(nos.orderId());
            case FixOutboundWireJob.MassCancelWire mc -> dispatchMassCancelWire(mc);
        }
    }

    private void dispatchMassCancelWire(FixOutboundWireJob.MassCancelWire mc) {
        try {
            fixOutboundSessionSend.send(mc.message());
            mc.completion().complete(null);
        } catch (SessionNotFound e) {
            mc.completion().completeExceptionally(e);
        } catch (Exception e) {
            mc.completion().completeExceptionally(e);
        }
    }

    private void runDedicatedLoop() {
        while (!dedicatedStop.get()) {
            try {
                if (!fixOutboundSessionSend.hasActiveSession()) {
                    LockSupport.parkNanos(notReadyParkNanos());
                    continue;
                }
                boolean drainedMassCancel = drainMassCancelsAtHeadWhileRouteDisabled();
                if (!isFixRouteSendEnabled()) {
                    if (!drainedMassCancel) {
                        meterRegistry.counter(FixMetrics.METRIC_OUTBOUND_ROUTE_DISABLED_SKIPS).increment();
                    }
                    LockSupport.parkNanos(notReadyParkNanos());
                    continue;
                }
                long waitNanos = Math.max(
                        MIN_QUEUE_POLL_WAIT_NANOS, omsConfig.getFix().getOutboundDedicatedIdleParkNanos());
                FixOutboundWireJob job = fixOutboundDequeue.poll(waitNanos, TimeUnit.NANOSECONDS);
                if (job == null) {
                    continue;
                }
                dispatchJob(job);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                if (dedicatedStop.get()) {
                    break;
                }
            } catch (Throwable t) {
                log.error("Dedicated FIX outbound loop failed; backing off", t);
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(DEDICATED_LOOP_ERROR_BACKOFF_MS));
            }
        }
    }

    private void dispatchOutboundForOrderId(UUID id) {
        Order order = ordersRepository.findById(id).orElse(null);
        if (order == null || order.status() != OrderStatus.WORKING) {
            log.debug("Skipping FIX outbound for orderId={} (missing or not WORKING)", id);
            return;
        }
        long maxAgeMs = omsConfig.getFix().getMaxOutboundJobAgeMs();
        if (maxAgeMs > 0 && order.acceptedAt() != null) {
            long ageMs = Duration.between(order.acceptedAt(), Instant.now()).toMillis();
            if (ageMs > maxAgeMs) {
                meterRegistry.counter(FixMetrics.METRIC_OUTBOUND_JOB_EXPIRED).increment();
                executionReportApplier.applyOutboundJobExpired(id);
                return;
            }
        }
        if (!fixOutboundTokenBucket.tryAcquire()) {
            meterRegistry.counter(FixMetrics.METRIC_OUTBOUND_THROTTLED_REQUEUES).increment();
            routeEnqueue.enqueueWorkingOrder(id);
            return;
        }
        Timer.Sample outboundSample = Timer.start(meterRegistry);
        try {
            NewOrderSingle nos = newOrderSingleBuilder.build(order);
            fixOutboundSessionSend.send(nos);
            meterRegistry.counter(FixMetrics.METRIC_NOS_SENT).increment();
            ingressToFixNosLatencyRecorder.recordNewOrderSingleSent(id);
            OmsPipelineMetrics.finishFixOutboundNos(meterRegistry, outboundSample, "success");
        } catch (SessionNotFound e) {
            OmsPipelineMetrics.finishFixOutboundNos(meterRegistry, outboundSample, "failure");
            log.warn("FIX sendToTarget failed (no session) orderId={}", id, e);
        } catch (Exception e) {
            OmsPipelineMetrics.finishFixOutboundNos(meterRegistry, outboundSample, "failure");
            log.error("FIX outbound send failed orderId={}", id, e);
        }
    }

    private boolean isFixRouteSendEnabled() {
        String routeKey = omsConfig.getFix().getRouteKey();
        return fixRouteStateRepository
                .findByRouteKey(routeKey)
                .map(FixRouteStateRow::sendEnabled)
                .orElse(true);
    }

    private long notReadyParkNanos() {
        long configured = omsConfig.getFix().getOutboundDedicatedNotReadyParkNanos();
        return configured > 0L ? configured : MIN_NOT_READY_PARK_WHEN_ZERO_NANOS;
    }

    @Override
    public void destroy() {
        if (dedicatedThread == null) {
            return;
        }
        dedicatedStop.set(true);
        dedicatedThread.interrupt();
        try {
            dedicatedThread.join(DEDICATED_THREAD_JOIN_TIMEOUT_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while joining dedicated FIX outbound thread");
        }
        if (dedicatedThread.isAlive()) {
            log.warn(
                    "Dedicated FIX outbound thread did not stop within {} ms",
                    DEDICATED_THREAD_JOIN_TIMEOUT_MS);
        }
    }
}
