package com.balh.oms.persistence;

import com.balh.oms.AbstractPostgresIntegrationTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code control_outbox} pending fetch uses {@code FOR UPDATE SKIP LOCKED} so two concurrent transactions
 * cannot claim the same row (required for N {@link com.balh.oms.reconciler.OutboxReconciler} JVMs).
 */
class ControlOutboxRepositorySkipLockedIntegrationTest extends AbstractPostgresIntegrationTest {

    private static final Duration FAR_FUTURE_OFFSET = Duration.ofDays(1);

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate http;

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private ControlOutboxRepository controlOutboxRepository;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @BeforeEach
    void clean() {
        jdbc.execute(SQL_TRUNCATE_ORDERS_AND_SETTLEMENT);
    }

    @Test
    void fetchPendingSkipLockedSkipsRowsLockedByAnotherTransaction() throws Exception {
        UUID accountId = UUID.randomUUID();
        UUID orderA = postOrder(accountId, "skip-lock-a");
        UUID orderB = postOrder(accountId, "skip-lock-b");

        assertThat(jdbc.queryForObject(
                        "SELECT COUNT(*) FROM control_outbox WHERE chronicle_enqueued_at IS NULL",
                        Long.class))
                .isEqualTo(2L);

        TransactionTemplate txTemplate = new TransactionTemplate(transactionManager);
        Instant olderThan = Instant.now().plus(FAR_FUTURE_OFFSET);
        CountDownLatch firstTxHoldsLock = new CountDownLatch(1);
        CountDownLatch releaseFirstTx = new CountDownLatch(1);
        AtomicReference<UUID> lockedOrderId = new AtomicReference<>();

        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        try {
            Future<?> holder = executor.submit(() -> txTemplate.executeWithoutResult(status -> {
                var rows = controlOutboxRepository.fetchPendingOlderThan(olderThan, 1);
                assertThat(rows).hasSize(1);
                lockedOrderId.set(rows.get(0).orderId());
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
                var rows = controlOutboxRepository.fetchPendingOlderThan(olderThan, 10);
                assertThat(rows).hasSize(1);
                assertThat(rows.get(0).orderId()).isNotEqualTo(lockedOrderId.get());
            });

            releaseFirstTx.countDown();
            holder.get(30, TimeUnit.SECONDS);
        } finally {
            executor.shutdown();
            assertThat(executor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }
    }

    private UUID postOrder(UUID accountId, String idempotencyKey) {
        ResponseEntity<Map<String, Object>> res = http.exchange(
                "http://localhost:" + port + "/internal/v1/orders",
                HttpMethod.POST,
                new HttpEntity<>(jsonRequest(accountId, idempotencyKey), authHeaders()),
                new ParameterizedTypeReference<>() {});
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        return UUID.fromString((String) res.getBody().get("id"));
    }

    private static HttpHeaders authHeaders() {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        h.set("X-OMS-Internal-Key", "test-key");
        return h;
    }

    private static String jsonRequest(UUID accountId, String key) {
        return """
                {
                  "accountId": "%s",
                  "clientIdempotencyKey": "%s",
                  "side": "BUY",
                  "instrumentSymbol": "AAPL",
                  "quantity": "10",
                  "limitPrice": "150.00",
                  "timeInForce": "DAY"
                }
                """
                .formatted(accountId, key);
    }
}
