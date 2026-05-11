package com.balh.oms.fix;

import com.balh.oms.persistence.FixOutboundHandoffRepository;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Dequeue from {@code fix_outbound_handoff} using short transactions + {@link FixOutboundHandoffRepository#popNextOrderId()}.
 */
public final class PostgresFixOutboundOrderDequeue implements FixOutboundOrderDequeue {

    private final FixOutboundHandoffRepository repository;
    private final TransactionTemplate transactionTemplate;

    public PostgresFixOutboundOrderDequeue(
            FixOutboundHandoffRepository repository, TransactionTemplate transactionTemplate) {
        this.repository = repository;
        this.transactionTemplate = transactionTemplate;
    }

    @Override
    public UUID pollOrNull() {
        return transactionTemplate.execute(status -> repository.popNextOrderId().orElse(null));
    }

    @Override
    public UUID poll(long timeout, TimeUnit unit) throws InterruptedException {
        long deadlineNanos = System.nanoTime() + unit.toNanos(timeout);
        while (System.nanoTime() < deadlineNanos) {
            UUID id = pollOrNull();
            if (id != null) {
                return id;
            }
            long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0L) {
                return null;
            }
            Thread.sleep(1L);
        }
        return null;
    }
}
