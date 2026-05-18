package com.balh.oms.reconciler;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.ledger.LedgerInflightLifecycleClient;
import com.balh.oms.persistence.LedgerInflightOutboxRepository;
import com.balh.oms.persistence.LedgerInflightOutboxRepository.LifecycleSettleableRow;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Wed-demo (V32): commits or voids the Ledger inflight hold attached to an order whose status
 * has reached terminal in OMS. Closes the loop on the user-visible balance flip:
 *
 * <pre>
 *   place      → POST /transactions (sync, inflight=true)              [hold notional debited]
 *   fill       → ...this reconciler... → PUT /transactions/inflight {status:"commit"}   [posted]
 *   cancel/reject/expire → ...this reconciler... → PUT inflight {status:"void"} [released]
 * </pre>
 *
 * <h2>Why a scheduled reconciler vs an Aeron event subscriber</h2>
 *
 * <p>The cluster's events recording is the source of truth for terminal-state transitions and
 * could drive this directly, but the projector already writes {@code orders.status} into
 * Postgres after consuming the same events, and {@code ledger_inflight_outbox.order_id} keys
 * straight into that table. A polled join keeps the reconciler off the cluster's hot path and
 * matches the operational shape of {@link LedgerInflightOutboxReconciler} +
 * {@link LedgerInflightHoldFailureCompensator}, which both already run from this same JVM
 * profile. For the demo the latency is fine (subsecond from terminal → balance flip on the
 * UI); a follow-up slice can move this onto an Aeron subscription if the latency budget
 * tightens.
 *
 * <h2>Idempotency</h2>
 *
 * <ul>
 *   <li><strong>Postgres working-set</strong>: filter at fetch is "ledger_txn_id present,
 *       lifecycle_settled_at NULL, compensated_at NULL, order terminal, attempts under
 *       threshold, backoff elapsed". A row settled by an earlier tick (or by a peer
 *       reconciler JVM) cannot be re-selected.</li>
 *   <li><strong>Ledger side</strong>: a second PUT {commit|void} on an already-settled txn
 *       returns 2xx via the Ledger's "already in requested state" branch. So even a crash
 *       between the Ledger 2xx and our Postgres {@code markLifecycleSettled} is safe — restart
 *       re-fetches the row, the second call is a no-op at the Ledger, and we then stamp
 *       {@code lifecycle_settled_at} so the row drops out of the working set.</li>
 *   <li><strong>Concurrency</strong>: {@code FOR UPDATE OF lio SKIP LOCKED} on the working-set
 *       fetch lets multiple reconciler JVMs run without double-settling. Per-tick batch is
 *       small ({@link OmsConfig.Ledger#getInflightLifecycleBatchSize()} defaults to 50).</li>
 * </ul>
 *
 * <h2>Failure handling</h2>
 *
 * <p>A failed call bumps {@code lifecycle_attempts} and stamps {@code lifecycle_last_error}.
 * The next tick can re-pick the row up only after {@code lifecycle_last_attempt_at +
 * retryBackoffMs} has elapsed (so a 4xx config error doesn't busy-loop). After
 * {@code attemptsThreshold} the row is no longer eligible — operator signal via the
 * {@code oms_ledger_inflight_lifecycle_failed_total} counter (action=give_up tag pending; today
 * the row just stops being fetched and the Ledger's expiry sweep is the safety net).
 */
@Component
public class LedgerInflightLifecycleReconciler {

    private static final Logger log = LoggerFactory.getLogger(LedgerInflightLifecycleReconciler.class);

    private static final String METRIC_SETTLED = "oms_ledger_inflight_lifecycle_settled_total";
    private static final String METRIC_FAILED = "oms_ledger_inflight_lifecycle_failed_total";
    private static final String METRIC_TICK_DURATION = "oms_ledger_inflight_lifecycle_tick_seconds";
    private static final String TAG_ACTION = "action";
    static final String ACTION_COMMIT = RestLedgerInflightLifecycleClientActions.COMMIT;
    static final String ACTION_VOID = RestLedgerInflightLifecycleClientActions.VOID;

    private final LedgerInflightOutboxRepository outbox;
    private final ObjectProvider<LedgerInflightLifecycleClient> lifecycleClient;
    private final OmsConfig config;
    private final MeterRegistry meterRegistry;
    private final TransactionTemplate transactionTemplate;

    public LedgerInflightLifecycleReconciler(
            LedgerInflightOutboxRepository outbox,
            ObjectProvider<LedgerInflightLifecycleClient> lifecycleClient,
            OmsConfig config,
            MeterRegistry meterRegistry,
            PlatformTransactionManager transactionManager) {
        this.outbox = outbox;
        this.lifecycleClient = lifecycleClient;
        this.config = config;
        this.meterRegistry = meterRegistry;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    @Scheduled(fixedDelayString = "${oms.ledger.inflight-lifecycle-interval-ms:1000}")
    public void runOnce() {
        var ledger = config.getLedger();
        if (!ledger.isInflightLifecycleReconcilerEnabled() || !ledger.isInflightAsyncEnabled()) {
            return;
        }
        LedgerInflightLifecycleClient client = lifecycleClient.getIfAvailable();
        if (client == null) {
            return;
        }
        Timer.Sample tick = Timer.start(meterRegistry);
        try {
            transactionTemplate.executeWithoutResult(status -> {
                Instant now = Instant.now();
                Instant backoffFloor = now.minus(Duration.ofMillis(ledger.getInflightLifecycleRetryBackoffMs()));
                List<LifecycleSettleableRow> rows = outbox.fetchLifecycleSettleable(
                        ledger.getInflightLifecycleAttemptsThreshold(),
                        ledger.getInflightLifecycleBatchSize(),
                        backoffFloor);
                for (LifecycleSettleableRow row : rows) {
                    String action = actionForStatus(row.orderStatus());
                    if (action == null) {
                        log.warn(
                                "lifecycle reconciler: unexpected order status '{}' for row id={} orderId={}; skipping",
                                row.orderStatus(), row.id(), row.orderId());
                        continue;
                    }
                    try {
                        if (ACTION_COMMIT.equals(action)) {
                            client.commitHold(row.ledgerTxnId());
                        } else {
                            client.voidHold(row.ledgerTxnId());
                        }
                        outbox.markLifecycleSettled(row.id(), action, Instant.now());
                        meterRegistry.counter(METRIC_SETTLED, TAG_ACTION, action).increment();
                        if (log.isDebugEnabled()) {
                            log.debug(
                                    "lifecycle reconciler: settled row id={} orderId={} txnId={} action={}",
                                    row.id(), row.orderId(), row.ledgerTxnId(), action);
                        }
                    } catch (LedgerInflightLifecycleClient.LedgerLifecycleException e) {
                        outbox.markLifecycleFailed(row.id(), e.toString(), Instant.now());
                        meterRegistry.counter(METRIC_FAILED, TAG_ACTION, action).increment();
                        log.warn(
                                "lifecycle reconciler: {} failed for id={} orderId={} attempts={}",
                                action, row.id(), row.orderId(), row.lifecycleAttempts() + 1, e);
                    }
                }
            });
        } finally {
            tick.stop(Timer.builder(METRIC_TICK_DURATION)
                    .description("Per-tick wall time of the Ledger inflight lifecycle reconciler")
                    .register(meterRegistry));
        }
    }

    static String actionForStatus(String orderStatus) {
        return switch (orderStatus) {
            case "FILLED" -> ACTION_COMMIT;
            case "CANCELLED", "REJECTED", "EXPIRED" -> ACTION_VOID;
            default -> null;
        };
    }

    /**
     * Pinned constants for the lifecycle action strings, kept here so the reconciler and the
     * REST client agree on the wire-level vocabulary without a circular reference.
     */
    static final class RestLedgerInflightLifecycleClientActions {
        static final String COMMIT = "commit";
        static final String VOID = "void";

        private RestLedgerInflightLifecycleClientActions() {}
    }
}
