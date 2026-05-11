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
import quickfix.Session;
import quickfix.SessionNotFound;
import quickfix.fix44.NewOrderSingle;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

/**
 * Drains {@link FixOutboundOrderDequeue} after logon and sends {@link NewOrderSingle} on the active session.
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
    private final FixSessionRegistry fixSessionRegistry;
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

    public FixOutboundDispatchWorker(
            RouteDispatcher routeEnqueue,
            FixOutboundOrderDequeue fixOutboundDequeue,
            FixSessionRegistry fixSessionRegistry,
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
        this.fixSessionRegistry = fixSessionRegistry;
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
        if (!fixSessionRegistry.hasLoggedOnSession()) {
            return;
        }
        if (!isFixRouteSendEnabled()) {
            meterRegistry.counter(FixMetrics.METRIC_OUTBOUND_ROUTE_DISABLED_SKIPS).increment();
            return;
        }
        UUID id = fixOutboundDequeue.pollOrNull();
        if (id == null) {
            return;
        }
        dispatchOutboundForOrderId(id);
    }

    private void runDedicatedLoop() {
        while (!dedicatedStop.get()) {
            try {
                if (!fixSessionRegistry.hasLoggedOnSession()) {
                    LockSupport.parkNanos(notReadyParkNanos());
                    continue;
                }
                if (!isFixRouteSendEnabled()) {
                    meterRegistry.counter(FixMetrics.METRIC_OUTBOUND_ROUTE_DISABLED_SKIPS).increment();
                    LockSupport.parkNanos(notReadyParkNanos());
                    continue;
                }
                long waitNanos = Math.max(
                        MIN_QUEUE_POLL_WAIT_NANOS, omsConfig.getFix().getOutboundDedicatedIdleParkNanos());
                UUID id = fixOutboundDequeue.poll(waitNanos, TimeUnit.NANOSECONDS);
                if (id == null) {
                    continue;
                }
                dispatchOutboundForOrderId(id);
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
            Session.sendToTarget(nos, fixSessionRegistry.sessionOrNull());
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
