package com.balh.oms.ledger;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.persistence.LedgerInflightOutboxRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Coalesces BUY inflight holds onto Ledger {@code POST /transactions/bulk?inflight=true&atomic=false}
 * (Phase 4 slice 4q of the Aeron Cluster substrate plan).
 *
 * <p><b>Status.</b> <em>Off by default and currently a regression vs slice 4p — do NOT enable
 * in production until slice 4r lands.</em> The class is preserved on {@code main} so the next
 * slice can iterate on the dispatch loop; the bulk dispatcher
 * ({@link LedgerInflightBulkDispatcher}) and the outbox-fallback wiring on the failure path
 * compose cleanly and slice 4r will reuse them. Pop! 2026-05-14 A/B (5000 orders / concurrency
 * 50, 2× ingress replicas, real Ledger HTTP, same Postgres state across A and B): outbox path
 * = 768 rps / HTTP RTT p50 16.7 ms, coalescer path = 95 rps / p50 431 ms. Root cause (verified
 * against {@code oms_ledger_inflight_coalescer_flush_seconds} and {@code _submit_seconds}):
 * the accept thread blocks on the per-item future returned by {@link #submit} and only one
 * daemon flush thread per JVM consumes the queue, so each JVM ceilings at "one bulk POST in
 * flight at a time" while slice 4p stays fire-and-forget at accept (the outbox row is
 * committed in the same tx as the accept; Ledger HTTP is entirely off the ingress critical
 * path). The intra-batch OCC reduction landed as designed; the regression is in the
 * accept-side coupling, not in the bulk dispatcher itself. Full numbers / math sanity-check /
 * slice 4r roadmap live in {@code oms/docs/runbooks/local-multi-jvm-bench.md}
 * {@code ## Slice 4q evidence} / {@code ## Slice 4q verdict + roadmap}.
 *
 * <p><b>Original design intent (preserved for slice 4r).</b> Slice 4p moved the per-order
 * Ledger HTTP off the ingress critical path by enqueuing each hold to
 * {@code ledger_inflight_outbox} and letting
 * {@link com.balh.oms.reconciler.LedgerInflightOutboxReconciler} drive Ledger after commit;
 * that lifted ingress rps from 279 → 764 on Pop!. The remaining bottleneck is server-side:
 * every hold mutates the source {@code balances.version} row, so cross-JVM bursts serialise
 * on Ledger's OCC retry. The bulk endpoint iterates the batch sequentially within a
 * <em>single</em> HTTP so the intra-batch OCC race surface drops by a factor of
 * {@code maxBatchSize}. What this slice missed: a synchronous per-item future + single flush
 * thread reintroduces the accept-side serialisation property that slice 4p removed.
 *
 * <p><b>Topology.</b> Operators choose between three paths in
 * {@link com.balh.oms.ingress.OrderIngressService#maybePlaceBuyLedgerInflightHold}:
 * <ul>
 *   <li><b>Outbox path (slice 4p) — current production default.</b>
 *       {@code oms.ledger.inflight-async-enabled=true},
 *       {@code oms.ledger.inflight-coalescer-enabled=false}. Each accept inserts a row into
 *       {@code ledger_inflight_outbox} <em>inside</em> the accept transaction; the reconciler
 *       drives Ledger after commit. Strongest durability (no in-memory gap), one Ledger HTTP
 *       per order, accept never blocks on Ledger.</li>
 *   <li><b>Coalescer path (slice 4q) — currently a regression, off by default.</b>
 *       {@code oms.ledger.inflight-async-enabled=false},
 *       {@code oms.ledger.inflight-coalescer-enabled=true}. Accept hands the hold to this
 *       coalescer's MPSC queue and waits on a per-item future; the daemon flush thread
 *       batches up to {@link OmsConfig.Ledger#getInflightCoalescerMaxBatchSize()} items and
 *       posts one bulk HTTP. <em>On any flush failure</em> (whole-batch HTTP failure, or
 *       per-item Ledger error in a partial-success response) the failed items are written
 *       to {@code ledger_inflight_outbox} so the existing reconciler + slice 4p compensator
 *       still cover them. The cost is a small in-memory gap: items in the queue at JVM crash
 *       time are lost; their orders are admitted at the cluster but the hold never reaches
 *       either Ledger or the outbox. The slice 4p compensator does not recover this gap
 *       because it only graduates rows that crossed
 *       {@link OmsConfig.Ledger#getInflightCompensatorAttemptsThreshold()}, and a row that
 *       never existed cannot graduate.</li>
 *   <li><b>Sync path</b> — both flags off, accept POSTs Ledger inline. Operationally the
 *       fallback for non-burst topologies; serialises on Ledger's OCC retry under load
 *       (slice 4p Bench A measured 279 rps / 92 % failures at 50-way concurrency on Pop!).</li>
 * </ul>
 *
 * <p><b>Lifecycle.</b> {@link #start()} spins up the daemon thread; {@link #stop()} signals
 * the loop, drains residual items to the outbox, and joins. Spring drives both via the
 * configuration's lifecycle bean.
 */
public final class LedgerInflightCoalescer {

    private static final Logger log = LoggerFactory.getLogger(LedgerInflightCoalescer.class);

    /** Bench-evidence-driven (slice 4q): coalescer flush latency, by outcome tag. */
    private static final String METRIC_FLUSH_SECONDS = "oms_ledger_inflight_coalescer_flush_seconds";
    /** Bench-evidence-driven (slice 4q): per-submit latency to first acknowledgement (success or fallback). */
    private static final String METRIC_SUBMIT_SECONDS = "oms_ledger_inflight_coalescer_submit_seconds";
    /** Counter: items submitted to the coalescer and their disposition. */
    private static final String METRIC_SUBMITTED_TOTAL = "oms_ledger_inflight_coalescer_submitted_total";
    /** Counter: per-item disposition once the flush thread has handled it. */
    private static final String METRIC_ITEMS_TOTAL = "oms_ledger_inflight_coalescer_items_total";

    private final OmsConfig config;
    private final LedgerInflightBulkDispatcher dispatcher;
    private final LedgerInflightOutboxRepository outbox;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;
    private final TransactionTemplate transactionTemplate;

    private final BlockingQueue<PendingHold> queue;
    private final int maxBatchSize;
    private final int maxInFlightFlushes;
    private final ExecutorService flushExecutor;
    private final Semaphore inFlightFlushes;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong drainedOnShutdown = new AtomicLong(0);
    private volatile Thread flushThread;

    public LedgerInflightCoalescer(
            OmsConfig config,
            LedgerInflightBulkDispatcher dispatcher,
            LedgerInflightOutboxRepository outbox,
            ObjectMapper objectMapper,
            MeterRegistry meterRegistry,
            PlatformTransactionManager transactionManager) {
        this.config = config;
        this.dispatcher = dispatcher;
        this.outbox = outbox;
        this.objectMapper = objectMapper;
        this.meterRegistry = meterRegistry;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.queue = new ArrayBlockingQueue<>(config.getLedger().getInflightCoalescerQueueCapacity());
        this.maxBatchSize = config.getLedger().getInflightCoalescerMaxBatchSize();
        this.maxInFlightFlushes = config.getLedger().getInflightCoalescerMaxInFlightFlushes();
        this.flushExecutor = Executors.newFixedThreadPool(maxInFlightFlushes, r -> {
            Thread t = new Thread(r, "oms-ledger-inflight-coalescer-flush");
            t.setDaemon(true);
            return t;
        });
        this.inFlightFlushes = new Semaphore(maxInFlightFlushes);
    }

    /**
     * Hands an item to the coalescer. Returns a future that completes when the item is either
     * acknowledged by Ledger (success) or written to the outbox (fallback). The future can
     * complete exceptionally only if the outbox fallback itself fails; in that case the caller
     * should propagate so the ingress controller returns 5xx and the client retries.
     *
     * <p>Throws {@link IllegalStateException} when the queue is full or the coalescer is not
     * running. The caller (ingress thread) translates that to 503, matching the cluster
     * back-pressure contract.
     */
    public CompletableFuture<Void> submit(UUID orderId, String sourceBalanceId, BigDecimal holdAmount) {
        if (!running.get()) {
            meterRegistry.counter(METRIC_SUBMITTED_TOTAL, "outcome", "stopped").increment();
            throw new IllegalStateException("ledger inflight coalescer is not running");
        }
        CompletableFuture<Void> future = new CompletableFuture<>();
        PendingHold pending = new PendingHold(orderId, sourceBalanceId, holdAmount, future, System.nanoTime());
        if (!queue.offer(pending)) {
            meterRegistry.counter(METRIC_SUBMITTED_TOTAL, "outcome", "queue_full").increment();
            throw new IllegalStateException("ledger inflight coalescer queue is full");
        }
        meterRegistry.counter(METRIC_SUBMITTED_TOTAL, "outcome", "accepted").increment();
        return future;
    }

    /** Spring lifecycle entry point — starts the daemon flush thread once. */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        Thread t = new Thread(this::runLoop, "oms-ledger-inflight-coalescer");
        t.setDaemon(true);
        flushThread = t;
        t.start();
        log.info(
                "LedgerInflightCoalescer started (maxBatchSize={}, flushIntervalMicros={}, queueCapacity={}, maxInFlightFlushes={})",
                maxBatchSize,
                config.getLedger().getInflightCoalescerFlushIntervalMicros(),
                config.getLedger().getInflightCoalescerQueueCapacity(),
                maxInFlightFlushes);
    }

    /**
     * Signals the daemon thread to stop, then drains any residual queue items to the outbox
     * (so a Ctrl-C / SIGTERM does not leak in-memory holds). Future of each drained item
     * completes successfully once the outbox row exists; if the outbox insert fails, the
     * future completes exceptionally and the order's downstream contract becomes "operator
     * intervention" — there is no other surface that could place the hold.
     */
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        Thread t = flushThread;
        if (t != null) {
            t.interrupt();
            try {
                t.join(TimeUnit.SECONDS.toMillis(5));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        flushExecutor.shutdown();
        try {
            if (!flushExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                flushExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            flushExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        // Final drain in case the flush thread exited before clearing the queue (e.g. due to
        // the interrupt hitting between poll() and flush()).
        List<PendingHold> residual = new ArrayList<>();
        queue.drainTo(residual);
        if (!residual.isEmpty()) {
            log.warn("LedgerInflightCoalescer stop(): draining {} residual items to outbox", residual.size());
            fallbackToOutbox(residual, "shutdown_drain");
            drainedOnShutdown.addAndGet(residual.size());
        }
    }

    /** Visible for tests. */
    long drainedOnShutdownCount() {
        return drainedOnShutdown.get();
    }

    /** Visible for tests. */
    int queueSize() {
        return queue.size();
    }

    private void runLoop() {
        long configuredFlushIntervalNanos =
                TimeUnit.MICROSECONDS.toNanos(config.getLedger().getInflightCoalescerFlushIntervalMicros());
        List<PendingHold> batch = new ArrayList<>(maxBatchSize);
        while (running.get()) {
            try {
                // Under backlog, skip the coalesce window so the first queued hold is not delayed
                // by flushIntervalMicros (10k/s soak tail-latency lever).
                long flushIntervalNanos = queue.size() >= maxBatchSize ? 0L : configuredFlushIntervalNanos;
                // Block for the first item up to one flush interval; then drain whatever else is
                // already queued without further waits, capped at maxBatchSize. Once running flips
                // false the next poll either returns a residual item or returns null promptly.
                PendingHold first = queue.poll(flushIntervalNanos, TimeUnit.NANOSECONDS);
                if (first == null) {
                    continue;
                }
                batch.add(first);
                queue.drainTo(batch, maxBatchSize - 1);
                submitFlushAsync(List.copyOf(batch));
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                if (!batch.isEmpty()) {
                    submitFlushAsync(List.copyOf(batch));
                }
                break;
            } catch (RuntimeException ex) {
                log.error("LedgerInflightCoalescer flush loop caught unexpected error; failing batch and continuing",
                        ex);
                failBatchExceptionally(batch, ex);
                batch.clear();
            } finally {
                batch.clear();
            }
        }
        // Drain whatever is left after the loop exits.
        List<PendingHold> tail = new ArrayList<>();
        queue.drainTo(tail);
        if (!tail.isEmpty()) {
            fallbackToOutbox(tail, "loop_exit_drain");
            drainedOnShutdown.addAndGet(tail.size());
        }
    }

    private void submitFlushAsync(List<PendingHold> batch) {
        if (batch.isEmpty()) {
            return;
        }
        try {
            inFlightFlushes.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fallbackToOutbox(batch, "loop_exit_drain");
            return;
        }
        flushExecutor.submit(() -> {
            try {
                flushBatch(batch);
            } finally {
                inFlightFlushes.release();
            }
        });
    }

    private void flushBatch(List<PendingHold> batch) {
        if (batch.isEmpty()) {
            return;
        }
        Timer.Sample sample = Timer.start(meterRegistry);
        List<LedgerInflightBulkDispatcher.HoldItem> items = new ArrayList<>(batch.size());
        for (PendingHold p : batch) {
            items.add(new LedgerInflightBulkDispatcher.HoldItem(p.orderId(), p.sourceBalanceId(), p.holdAmount()));
        }
        try {
            LedgerInflightBulkDispatcher.Result result = dispatcher.dispatch(items);
            String outcome = result.failedOrderIds().isEmpty() ? "applied" : "partial";
            stopFlushTimer(sample, outcome);
            java.util.Set<UUID> failedIds = result.failedOrderIds();
            List<PendingHold> failedItems = new ArrayList<>();
            // Iterate batch directly (not an orderId-keyed map) so duplicate orderIds within a
            // single batch are routed consistently — both copies count as failed if Ledger
            // reports the orderId as failed.
            for (PendingHold p : batch) {
                if (failedIds.contains(p.orderId())) {
                    failedItems.add(p);
                } else {
                    completeSuccess(p, "applied");
                }
            }
            if (!failedItems.isEmpty()) {
                fallbackToOutbox(failedItems, "ledger_partial_failure");
            }
        } catch (LedgerInflightBulkDispatcher.LedgerInflightBulkException e) {
            stopFlushTimer(sample, "failure");
            log.warn("LedgerInflightCoalescer bulk dispatch failed for batch size={}: {}",
                    batch.size(), e.getMessage());
            fallbackToOutbox(batch, "bulk_dispatch_failed");
        }
    }

    private void stopFlushTimer(Timer.Sample sample, String outcome) {
        sample.stop(Timer.builder(METRIC_FLUSH_SECONDS)
                .description("LedgerInflightCoalescer bulk flush latency")
                .tag("outcome", outcome)
                .register(meterRegistry));
    }

    private void completeSuccess(PendingHold pending, String outcome) {
        meterRegistry.counter(METRIC_ITEMS_TOTAL, "outcome", outcome).increment();
        recordSubmitLatency(pending, outcome);
        pending.future().complete(null);
    }

    private void recordSubmitLatency(PendingHold pending, String outcome) {
        long elapsedNanos = System.nanoTime() - pending.submittedAtNanos();
        if (elapsedNanos < 0) {
            elapsedNanos = 0;
        }
        Timer.builder(METRIC_SUBMIT_SECONDS)
                .description("LedgerInflightCoalescer per-item time-to-acknowledge")
                .tag("outcome", outcome)
                .register(meterRegistry)
                .record(elapsedNanos, TimeUnit.NANOSECONDS);
    }

    private void fallbackToOutbox(List<PendingHold> items, String reason) {
        if (items.isEmpty()) {
            return;
        }
        // Outbox writes happen in their own DB transaction (the original ingress accept tx
        // committed long ago). Each write is independent so a failure on one item does not
        // poison the rest of the batch.
        for (PendingHold pending : items) {
            try {
                String payload = serializeOutboxPayload(pending);
                transactionTemplate.executeWithoutResult(status ->
                        outbox.insertIfAbsent(pending.orderId(), payload));
                // D-9: the projector may have already inserted this row from OrderAdmittedEvent
                // before the coalescer fallback runs. insertIfAbsent treats that as success —
                // the outbox safety net exists either way and the reconciler can drive Ledger.
                meterRegistry.counter(METRIC_ITEMS_TOTAL, "outcome", "fallback_outbox", "reason", reason).increment();
                recordSubmitLatency(pending, "fallback_outbox");
                pending.future().complete(null);
            } catch (Exception ex) {
                log.error("LedgerInflightCoalescer outbox fallback failed for orderId={} reason={}",
                        pending.orderId(), reason, ex);
                meterRegistry.counter(METRIC_ITEMS_TOTAL, "outcome", "fallback_failed", "reason", reason).increment();
                recordSubmitLatency(pending, "fallback_failed");
                pending.future().completeExceptionally(
                        new RuntimeException("ledger inflight coalescer outbox fallback failed", ex));
            }
        }
    }

    private String serializeOutboxPayload(PendingHold pending) throws JsonProcessingException {
        ObjectNode node = objectMapper.createObjectNode();
        node.put("ledgerBalanceId", pending.sourceBalanceId());
        node.put("holdAmount", pending.holdAmount().toPlainString());
        return objectMapper.writeValueAsString(node);
    }

    private void failBatchExceptionally(List<PendingHold> items, Throwable ex) {
        for (PendingHold p : items) {
            meterRegistry.counter(METRIC_ITEMS_TOTAL, "outcome", "loop_error").increment();
            recordSubmitLatency(p, "loop_error");
            p.future().completeExceptionally(
                    new RuntimeException("ledger inflight coalescer loop error", ex));
        }
    }

    private record PendingHold(
            UUID orderId,
            String sourceBalanceId,
            BigDecimal holdAmount,
            CompletableFuture<Void> future,
            long submittedAtNanos) {}
}
