package com.balh.oms.reconciler;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.ledger.LedgerSettlementPostingClient;
import com.balh.oms.ledger.LedgerSettlementPostingClient.LedgerSettlementPostingException;
import com.balh.oms.ledger.LedgerSettlementPostingClient.LedgerSettlementPostingException.Reason;
import com.balh.oms.settlement.LedgerSettlementOutboxRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.SimpleTransactionStatus;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit coverage for the {@code SKIPPED_UNFUNDED_BALANCE} vs generic-failure split in
 * {@link LedgerSettlementOutboxReconciler#runOnce()}. The regression this guards against:
 * conflating fresh-account / demo "customer balance not found" warnings with real Ledger HTTP
 * failures inflates the failed-rate so much that the genuine failure signal becomes invisible
 * on the SettlementPipelinePage (see plans/ops-console-aeron-and-oms-observability.md §6).
 *
 * <p>Asserts both metric branches and the shared invariant that the outbox row stays unposted
 * in either case — {@code posted_at} is only set on {@code postSettlementOutbox} returning
 * normally, so a skipped row must be re-attempted on the next reconciler tick.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class LedgerSettlementOutboxReconcilerSkippedReasonTest {

    private static final long OUTBOX_ID = 7L;
    private static final long EXECUTION_ID = 4242L;
    private static final String STATUS = "settled";
    private static final String LEG = "cash";
    private static final String PAYLOAD = "{}";
    private static final int BATCH_SIZE = 50;

    private static final String METRIC_PUBLISHED = "oms_ledger_settlement_outbox_published_total";
    private static final String METRIC_FAILED = "oms_ledger_settlement_outbox_failed_total";
    private static final String METRIC_SKIPPED = "oms_ledger_settlement_outbox_skipped_total";

    @Mock private LedgerSettlementOutboxRepository outbox;
    @Mock private LedgerSettlementPostingClient postingClient;
    @Mock private PlatformTransactionManager transactionManager;

    private OmsConfig config;
    private SimpleMeterRegistry meterRegistry;
    private LedgerSettlementOutboxReconciler reconciler;

    @BeforeEach
    void setUp() {
        config = new OmsConfig();
        config.getLedger().setSettlementOutboxReconcilerEnabled(true);
        config.getLedger().setSettlementOutboxReconcilerAgeMs(0);
        config.getLedger().setSettlementOutboxReconcilerBatchSize(BATCH_SIZE);

        meterRegistry = new SimpleMeterRegistry();

        // TransactionTemplate.executeWithoutResult invokes
        // PlatformTransactionManager.getTransaction(...) before running the callback;
        // hand back a no-op status so the inner Lambda actually runs.
        TransactionStatus status = new SimpleTransactionStatus();
        when(transactionManager.getTransaction(any())).thenReturn(status);

        when(outbox.lockUnpostedOlderThan(any(Instant.class), eq(BATCH_SIZE)))
                .thenReturn(List.of(new LedgerSettlementOutboxRepository.OutboxRow(
                        OUTBOX_ID, EXECUTION_ID, STATUS, LEG, PAYLOAD)));

        reconciler = new LedgerSettlementOutboxReconciler(
                outbox, postingClient, config, transactionManager, meterRegistry);
    }

    @Test
    void unfundedBalanceIncrementsSkippedCounterAndLeavesRowUnposted() throws Exception {
        doThrow(new LedgerSettlementPostingException(
                        Reason.SKIPPED_UNFUNDED_BALANCE,
                        "customer balance not found in Ledger: indicator=inv-x-USD currency=USD"))
                .when(postingClient)
                .postSettlementOutbox(eq(OUTBOX_ID), eq(EXECUTION_ID), eq(STATUS), eq(LEG), eq(PAYLOAD));

        reconciler.runOnce();

        assertThat(skippedCount("unfunded_balance")).isEqualTo(1.0);
        assertThat(counterTotal(METRIC_FAILED)).isZero();
        assertThat(counterTotal(METRIC_PUBLISHED)).isZero();
        verify(outbox, never()).markPosted(anyLong(), any(Instant.class));
    }

    @Test
    void genericPostingFailureIncrementsFailedCounterNotSkipped() throws Exception {
        doThrow(new LedgerSettlementPostingException("ledger /transactions HTTP 502: upstream down"))
                .when(postingClient)
                .postSettlementOutbox(eq(OUTBOX_ID), eq(EXECUTION_ID), eq(STATUS), eq(LEG), eq(PAYLOAD));

        reconciler.runOnce();

        assertThat(counterTotal(METRIC_FAILED)).isEqualTo(1.0);
        assertThat(skippedCount("unfunded_balance")).isZero();
        assertThat(counterTotal(METRIC_PUBLISHED)).isZero();
        verify(outbox, never()).markPosted(anyLong(), any(Instant.class));
    }

    @Test
    void runtimeExceptionStillIncrementsFailedCounter() throws Exception {
        doThrow(new RuntimeException("npe in payload parsing"))
                .when(postingClient)
                .postSettlementOutbox(anyLong(), anyLong(), anyString(), anyString(), anyString());

        reconciler.runOnce();

        assertThat(counterTotal(METRIC_FAILED)).isEqualTo(1.0);
        assertThat(skippedCount("unfunded_balance")).isZero();
        verify(outbox, never()).markPosted(anyLong(), any(Instant.class));
    }

    @Test
    void successfulPostMarksPostedAndIncrementsPublished() throws Exception {
        // postSettlementOutbox returns normally → markPosted + published++.
        reconciler.runOnce();

        verify(outbox).markPosted(eq(OUTBOX_ID), any(Instant.class));
        assertThat(counterTotal(METRIC_PUBLISHED)).isEqualTo(1.0);
        assertThat(counterTotal(METRIC_FAILED)).isZero();
        assertThat(skippedCount("unfunded_balance")).isZero();
    }

    private double counterTotal(String name) {
        var c = meterRegistry.find(name).counter();
        return c == null ? 0.0 : c.count();
    }

    private double skippedCount(String reasonTag) {
        var c = meterRegistry.find(METRIC_SKIPPED).tag("reason", reasonTag).counter();
        return c == null ? 0.0 : c.count();
    }
}
