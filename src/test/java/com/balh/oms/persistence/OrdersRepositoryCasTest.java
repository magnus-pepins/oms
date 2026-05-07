package com.balh.oms.persistence;

import com.balh.oms.AbstractPostgresIntegrationTest;
import com.balh.oms.domain.Order;
import com.balh.oms.domain.OrderStatus;
import com.balh.oms.domain.RejectCode;
import com.balh.oms.domain.Side;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins the slice-1 invariants:
 * <ul>
 *   <li>UNIQUE (account_id, client_idempotency_key) prevents duplicate inserts.</li>
 *   <li>updateWithCas only succeeds when the caller holds the latest version.</li>
 *   <li>Stale CAS attempts are no-ops (no orphaned writes, no exceptions).</li>
 * </ul>
 */
class OrdersRepositoryCasTest extends AbstractPostgresIntegrationTest {

    @Autowired OrdersRepository orders;

    @Test
    void insertingTwiceWithSameIdempotencyKeyThrows() {
        UUID accountId = UUID.randomUUID();
        Order o = sampleOrder(accountId, "k1");
        orders.insert(o);

        Order duplicate = sampleOrder(accountId, "k1");
        assertThatThrownBy(() -> orders.insert(duplicate))
                .isInstanceOf(OrdersRepository.DuplicateOrderException.class);
    }

    @Test
    void casUpdateOnlyAppliesWithMatchingVersion() {
        Order o = sampleOrder(UUID.randomUUID(), "k1");
        orders.insert(o);

        boolean firstUpdate = orders.updateWithCas(
                o.id(), 0, OrderStatus.WORKING, null, Instant.now(), null);
        assertThat(firstUpdate).isTrue();

        // Stale version: no row affected, no exception, no overwrite.
        boolean staleUpdate = orders.updateWithCas(
                o.id(), 0, OrderStatus.REJECTED, RejectCode.INTERNAL_ERROR, null, Instant.now());
        assertThat(staleUpdate).isFalse();

        Order reloaded = orders.findById(o.id()).orElseThrow();
        assertThat(reloaded.status()).isEqualTo(OrderStatus.WORKING);
        assertThat(reloaded.version()).isEqualTo(1);
    }

    private static Order sampleOrder(UUID accountId, String key) {
        Instant now = Instant.now();
        return new Order(
                UUID.randomUUID(),
                accountId,
                key,
                0,
                0,
                OrderStatus.NEW,
                null,
                Side.BUY,
                "AAPL",
                new BigDecimal("10"),
                new BigDecimal("150.00"),
                "DAY",
                now,
                now,
                null,
                "deadbeef",
                null
        );
    }
}
