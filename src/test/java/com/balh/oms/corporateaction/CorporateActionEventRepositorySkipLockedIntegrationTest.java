package com.balh.oms.corporateaction;

import com.balh.oms.AbstractPostgresIntegrationTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

class CorporateActionEventRepositorySkipLockedIntegrationTest extends AbstractPostgresIntegrationTest {

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private CorporateActionEventRepository corporateActionEventRepository;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @BeforeEach
    void clean() {
        jdbc.execute(SQL_TRUNCATE_ORDERS_AND_SETTLEMENT);
    }

    @Test
    void findUnprocessedSkipLockedSkipsRowsLockedByAnotherTransaction() throws Exception {
        jdbc.update(
                """
                INSERT INTO corporate_action_event (instrument_symbol, action_type, effective_date, payload_json)
                VALUES ('AAA', 'DIV', DATE '2026-06-01', '{}'::jsonb),
                       ('BBB', 'DIV', DATE '2026-06-01', '{}'::jsonb)
                """);

        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*) FROM corporate_action_event WHERE processed_at IS NULL", Long.class))
                .isEqualTo(2L);

        TransactionTemplate txTemplate = new TransactionTemplate(transactionManager);
        CountDownLatch firstTxHoldsLock = new CountDownLatch(1);
        CountDownLatch releaseFirstTx = new CountDownLatch(1);
        AtomicLong lockedId = new AtomicLong();

        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        try {
            Future<?> holder = executor.submit(() -> txTemplate.executeWithoutResult(status -> {
                var rows = corporateActionEventRepository.findUnprocessedForUpdateSkipLocked(1);
                assertThat(rows).hasSize(1);
                lockedId.set(rows.get(0).id());
                firstTxHoldsLock.countDown();
                try {
                    assertThat(releaseFirstTx.await(30, TimeUnit.SECONDS)).isTrue();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }));

            assertThat(firstTxHoldsLock.await(30, TimeUnit.SECONDS)).isTrue();

            txTemplate.executeWithoutResult(status -> {
                var rows = corporateActionEventRepository.findUnprocessedForUpdateSkipLocked(10);
                assertThat(rows).hasSize(1);
                assertThat(rows.get(0).id()).isNotEqualTo(lockedId.get());
            });

            releaseFirstTx.countDown();
            holder.get(30, TimeUnit.SECONDS);
        } finally {
            executor.shutdown();
            assertThat(executor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }
    }
}
