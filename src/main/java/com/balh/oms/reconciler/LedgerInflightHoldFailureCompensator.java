package com.balh.oms.reconciler;

import com.balh.oms.cluster.ApplyExecutionReportCommand;
import com.balh.oms.cluster.CancelOrderCommand;
import com.balh.oms.cluster.OmsClusterIngressClient;
import com.balh.oms.cluster.OmsClusterShardRouter;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.config.OmsProfiles;
import com.balh.oms.domain.RejectCode;
import com.balh.oms.ledger.LedgerInflightReservationFailures;
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
 * {@link ApplyExecutionReportCommand} with {@link RejectCode#RISK_BUYING_POWER} for
 * insufficient-funds holds (or a {@link CancelOrderCommand} for other hold failures on
 * pre-fix admits), then stamps
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
    private static final String OUTCOME_REJECTED = "rejected";
    private static final String OUTCOME_CANCELLED = "cancelled";
    private static final String OUTCOME_SUBMIT_FAILED = "submit_failed";

    /** Reason string carried on the cluster cancel; capped at 256 bytes by the wire format. */
    private static final int MAX_REASON_BYTES = 240;
    private static final String REASON_PREFIX = "ledger_inflight_hold_failed:";

    private final LedgerInflightOutboxRepository outbox;
    /**
     * Phase 4 Tier 2.5 phase E-2 — replaces the pre-E-2
     * {@code ObjectProvider<OmsClusterIngressClient>} so the compensator can route each cancel
     * to the shard that owns the order. At {@code shardCount=1} the router resolves to the
     * single cluster client (byte-identical to E-1); at {@code shardCount>1} (E-3+) each row's
     * cancel lands on its owning cluster without changing this method.
     */
    private final ObjectProvider<OmsClusterShardRouter> clusterShardRouter;
    private final OmsConfig config;
    private final MeterRegistry meterRegistry;
    private final TransactionTemplate transactionTemplate;

    public LedgerInflightHoldFailureCompensator(
            LedgerInflightOutboxRepository outbox,
            ObjectProvider<OmsClusterShardRouter> clusterShardRouter,
            OmsConfig config,
            MeterRegistry meterRegistry,
            PlatformTransactionManager transactionManager) {
        this.outbox = outbox;
        this.clusterShardRouter = clusterShardRouter;
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
        OmsClusterShardRouter router = clusterShardRouter.getIfAvailable();
        if (router == null) {
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
            // Phase 4 Tier 2.5 phase E-2: pick the cluster client that owns this row's shard.
            // At shardCount=1 every row carries shardId=0 and the router returns the singleton
            // — same instance the pre-E-2 code held in clusterClient.
            OmsClusterIngressClient client;
            try {
                client = router.forShard(row.shardId());
            } catch (IllegalArgumentException e) {
                // Defensive: a row whose shardId is out of range would indicate either a stale
                // outbox row from a higher-shardCount deployment or a corrupt orders.shard_id.
                // Don't compensate this row this tick; surface the count via the same
                // submit_failed bucket so the operator's existing alert fires.
                meterRegistry.counter(METRIC_COMPENSATE_FAILED,
                        Tags.of(TAG_OUTCOME, OUTCOME_SUBMIT_FAILED)).increment();
                log.warn(
                        "Compensator: row shardId={} not in router (shardCount={}); skipping orderId={} id={}.",
                        row.shardId(), router.shardCount(), row.orderId(), row.id(), e);
                continue;
            }
            if (!client.isConnected()) {
                // The owning shard's client is currently down. Skip without burning the submit
                // timeout — the next tick will retry. Bookkeeping intentionally uses the same
                // submit_failed counter the timeout / runtime-error paths use; the operator's
                // existing alert fires regardless of which sub-cause kept the row alive.
                meterRegistry.counter(METRIC_COMPENSATE_FAILED,
                        Tags.of(TAG_OUTCOME, OUTCOME_SUBMIT_FAILED)).increment();
                log.debug(
                        "Compensator: shard {} client not connected; deferring orderId={} id={} to next tick.",
                        row.shardId(), row.orderId(), row.id());
                continue;
            }
            try {
                long correlationId = client.nextCorrelationId();
                String reason = buildReason(row.lastError());
                String outcome;
                if (LedgerInflightReservationFailures.isInsufficientFundsMessage(row.lastError())) {
                    ApplyExecutionReportCommand cmd = new ApplyExecutionReportCommand(
                            correlationId,
                            row.orderId(),
                            0L,
                            0L,
                            System.nanoTime(),
                            0,
                            ApplyExecutionReportCommand.EXEC_TYPE_VENUE_REJECT,
                            (byte) RejectCode.RISK_BUYING_POWER.ordinal(),
                            config.getVenue().getVenueId(),
                            "ledger-hold-compensate-" + row.orderId() + "-a" + row.attempts(),
                            "",
                            "{\"reason\":\"" + reason + "\",\"errorCode\":\"insufficient_funds\"}");
                    client.submitApplyExecutionReport(cmd, submitTimeout);
                    outcome = OUTCOME_REJECTED;
                } else {
                    CancelOrderCommand cmd = new CancelOrderCommand(
                            correlationId, row.orderId(), System.nanoTime(), reason);
                    client.submitCancelOrder(cmd, submitTimeout);
                    outcome = OUTCOME_CANCELLED;
                }
                // Mark compensated only after cluster offer succeeds: the cluster log commit is
                // the durability boundary. If we crash between this point and the next line the
                // cluster will still apply the reject/cancel on log replay (idempotent in the
                // cluster service); the row stays uncompensated and the next compensator tick
                // re-fires — also a no-op in the cluster, then this UPDATE catches up.
                transactionTemplate.executeWithoutResult(s ->
                        outbox.markCompensated(row.id(), correlationId, Instant.now()));
                meterRegistry.counter(METRIC_COMPENSATED, Tags.of(TAG_OUTCOME, outcome)).increment();
                log.info(
                        "Compensated ledger_inflight_outbox id={} orderId={} shardId={} (attempts={}) -> {} correlationId={}",
                        row.id(), row.orderId(), row.shardId(), row.attempts(), outcome, correlationId);
            } catch (TimeoutException e) {
                meterRegistry.counter(METRIC_COMPENSATE_FAILED,
                        Tags.of(TAG_OUTCOME, OUTCOME_SUBMIT_FAILED)).increment();
                log.warn(
                        "Compensator: cluster submitCancelOrder timed out for orderId={} id={} shardId={}; will retry next tick.",
                        row.orderId(), row.id(), row.shardId(), e);
                // E-2: stop the batch on first timeout only when the timeout came from the
                // shard the *next* row also targets. Different shards have independent
                // back-pressure budgets; making a shard-1 timeout also skip pending shard-0
                // rows would over-couple them. At shardCount=1 this preserves the pre-E-2
                // "abort batch" behaviour because every row's shardId is 0.
                if (allRemainingRowsTargetShard(rows, row, row.shardId())) {
                    return;
                }
                // Otherwise fall through to the next row; if THAT row's shard is also down its
                // own isConnected() / submit failure will skip it on its own.
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
     * Returns {@code true} when every row in {@code rows} after {@code currentRow} targets the
     * same {@code shardId}. Used to preserve the pre-E-2 "abort batch on first timeout" semantics
     * at {@code shardCount=1} (every remaining row hits the same back-pressured cluster, so
     * burning the rest is wasteful) without coupling shards in the multi-shard path.
     */
    private static boolean allRemainingRowsTargetShard(
            List<LedgerInflightOutboxRepository.FailedInflightRow> rows,
            LedgerInflightOutboxRepository.FailedInflightRow currentRow,
            int shardId) {
        boolean past = false;
        for (var r : rows) {
            if (!past) {
                if (r == currentRow) {
                    past = true;
                }
                continue;
            }
            if (r.shardId() != shardId) {
                return false;
            }
        }
        return true;
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
