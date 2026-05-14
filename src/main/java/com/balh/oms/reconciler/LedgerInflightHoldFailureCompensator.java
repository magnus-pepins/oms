package com.balh.oms.reconciler;

import com.balh.oms.cluster.CancelOrderCommand;
import com.balh.oms.cluster.OmsClusterIngressClient;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.config.OmsProfiles;
import com.balh.oms.persistence.LedgerInflightOutboxRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * Phase 4 slice 4p ({@code system-documentation/plans/oms-aeron-cluster-substrate.md}, Tier 2.5):
 * compensating reconciler for the async {@code ledger_inflight_outbox} path. When a buying-power
 * hold has failed past
 * {@link OmsConfig.Ledger#getInflightCompensatorAttemptsThreshold() attempts-threshold}, this
 * reconciler treats the hold as un-acceptable (e.g. insufficient balance) and submits a
 * {@link CancelOrderCommand} to cancel the order in the cluster, then stamps
 * {@code ledger_inflight_outbox.compensated_at} so the row is excluded from further retries.
 *
 * <h3>Race window (documented limitation)</h3>
 *
 * <p>Between the hold-failure and the cancel landing, a venue fill can race the compensator and
 * leave the user with an unfunded position. The cluster service silently no-ops the cancel for an
 * already-terminal order ({@link com.balh.oms.cluster.OmsAdmissionClusteredService#applyCancelOrder
 * applyCancelOrder}); the compensator still stamps {@code compensated_at} because the goal of the
 * row is "we tried to cancel, do not retry". Slice 4q's
 * {@code LedgerInflightCoalescer} is the synchronous fix for callers that cannot tolerate this
 * race; slice 4p is the eventually-consistent backstop with the throughput shape Tier 2.5
 * targets.
 *
 * <h3>Idempotency</h3>
 *
 * <ul>
 *   <li>Cluster: {@link CancelOrderCommand} on an already-CANCELLED / FILLED / REJECTED order is a
 *       silent no-op; replay or a second compensator hit is safe.</li>
 *   <li>Postgres: {@code FOR UPDATE SKIP LOCKED} on the working-set fetch + the
 *       {@code compensated_at IS NULL} filter prevents a second compensator JVM from
 *       double-cancelling the same row.</li>
 * </ul>
 *
 * <h3>Profile / opt-in</h3>
 *
 * <p>Pinned to {@link OmsProfiles#CLUSTER_CLIENT_PROFILE} (excludes
 * {@code oms-postgres-projector} which has no cluster client). Per-deployment opt-in via
 * {@code oms.ledger.inflight-compensator-enabled=true} alongside
 * {@code inflight-async-enabled=true}; without both, the bean loads but {@link #runOnce()} is a
 * cheap config-check no-op.
 */
@Component
@Profile(OmsProfiles.CLUSTER_CLIENT_PROFILE)
public class LedgerInflightHoldFailureCompensator {

    private static final Logger log = LoggerFactory.getLogger(LedgerInflightHoldFailureCompensator.class);

    private static final String METRIC_COMPENSATED = "oms_ledger_inflight_hold_compensated_total";
    private static final String METRIC_COMPENSATE_FAILED = "oms_ledger_inflight_hold_compensate_failed_total";
    private static final String TAG_OUTCOME = "outcome";
    private static final String OUTCOME_CANCELLED = "cancelled";
    private static final String OUTCOME_SUBMIT_FAILED = "submit_failed";

    /** Reason string carried on the cluster cancel; capped at 256 bytes by the wire format. */
    private static final int MAX_REASON_BYTES = 240;
    private static final String REASON_PREFIX = "ledger_inflight_hold_failed:";

    private final LedgerInflightOutboxRepository outbox;
    private final ObjectProvider<OmsClusterIngressClient> clusterClient;
    private final OmsConfig config;
    private final MeterRegistry meterRegistry;
    private final TransactionTemplate transactionTemplate;

    public LedgerInflightHoldFailureCompensator(
            LedgerInflightOutboxRepository outbox,
            ObjectProvider<OmsClusterIngressClient> clusterClient,
            OmsConfig config,
            MeterRegistry meterRegistry,
            PlatformTransactionManager transactionManager) {
        this.outbox = outbox;
        this.clusterClient = clusterClient;
        this.config = config;
        this.meterRegistry = meterRegistry;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    @Scheduled(fixedDelayString = "${oms.ledger.inflight-compensator-interval-ms:1000}")
    public void runOnce() {
        OmsConfig.Ledger ledgerCfg = config.getLedger();
        if (!ledgerCfg.isInflightReservationEnabled()
                || !ledgerCfg.isInflightAsyncEnabled()
                || !ledgerCfg.isInflightCompensatorEnabled()) {
            return;
        }
        OmsClusterIngressClient client = clusterClient.getIfAvailable();
        if (client == null || !client.isConnected()) {
            return;
        }

        int threshold = ledgerCfg.getInflightCompensatorAttemptsThreshold();
        int batchSize = ledgerCfg.getInflightCompensatorBatchSize();
        Duration submitTimeout = Duration.ofMillis(ledgerCfg.getInflightCompensatorSubmitTimeoutMs());

        List<LedgerInflightOutboxRepository.FailedInflightRow> rows =
                transactionTemplate.execute(status ->
                        outbox.fetchFailedUncompensated(threshold, batchSize));
        if (rows == null || rows.isEmpty()) {
            return;
        }

        for (var row : rows) {
            try {
                long correlationId = client.nextCorrelationId();
                String reason = buildReason(row.lastError());
                CancelOrderCommand cmd = new CancelOrderCommand(
                        correlationId, row.orderId(), System.nanoTime(), reason);
                client.submitCancelOrder(cmd, submitTimeout);
                // Mark compensated only after cluster offer succeeds: the cluster log commit is
                // the durability boundary. If we crash between this point and the next line the
                // cluster will still apply the cancel on log replay (idempotent in the cluster
                // service); the row stays uncompensated and the next compensator tick re-fires
                // the cancel — also a no-op in the cluster, then this UPDATE catches up.
                transactionTemplate.executeWithoutResult(s ->
                        outbox.markCompensated(row.id(), correlationId, Instant.now()));
                meterRegistry.counter(METRIC_COMPENSATED,
                        Tags.of(TAG_OUTCOME, OUTCOME_CANCELLED)).increment();
                log.info(
                        "Compensated ledger_inflight_outbox id={} orderId={} (attempts={}) -> CancelOrderCommand correlationId={}",
                        row.id(), row.orderId(), row.attempts(), correlationId);
            } catch (TimeoutException e) {
                meterRegistry.counter(METRIC_COMPENSATE_FAILED,
                        Tags.of(TAG_OUTCOME, OUTCOME_SUBMIT_FAILED)).increment();
                log.warn(
                        "Compensator: cluster submitCancelOrder timed out for orderId={} id={}; will retry next tick.",
                        row.orderId(), row.id(), e);
                // Stop the batch on first timeout: the cluster is back-pressured or unreachable;
                // burning the rest of the batch on the same condition wastes a 2s budget per row.
                return;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn(
                        "Compensator: interrupted while submitting cancel for orderId={} id={}; deferring batch.",
                        row.orderId(), row.id());
                return;
            } catch (RuntimeException e) {
                meterRegistry.counter(METRIC_COMPENSATE_FAILED,
                        Tags.of(TAG_OUTCOME, OUTCOME_SUBMIT_FAILED)).increment();
                log.warn(
                        "Compensator: cluster submitCancelOrder failed for orderId={} id={}; will retry next tick.",
                        row.orderId(), row.id(), e);
                // Continue with next row — a runtime failure on a single submit (e.g. an Aeron
                // exception that does not affect other in-flight commands) shouldn't block the
                // rest of the batch.
            }
        }
    }

    /**
     * Truncates the recorded {@code last_error} so the cluster {@link CancelOrderCommand}
     * {@code reason} field stays within {@link com.balh.oms.cluster.OmsClusterWireFormat#MAX_STRING_BYTES}
     * bytes after UTF-8 encoding. Conservative byte budget (240 < 256) leaves headroom for the
     * {@link #REASON_PREFIX} and any multi-byte trailing character.
     */
    static String buildReason(String lastError) {
        String body = lastError == null ? "" : lastError;
        String full = REASON_PREFIX + body;
        if (full.length() <= MAX_REASON_BYTES) {
            return full;
        }
        return full.substring(0, MAX_REASON_BYTES);
    }
}
