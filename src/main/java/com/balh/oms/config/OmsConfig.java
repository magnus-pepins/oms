package com.balh.oms.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Named limit and config keys for the OMS slice 1.
 *
 * <p>Per the Balh ecosystem config-and-limits rule, no bare numeric literals
 * appear in business logic for timeouts, queue sizes, batch sizes, or shard
 * counts. They are bound here from {@code application.yaml} / env.
 */
@Configuration
@ConfigurationProperties(prefix = "oms")
public class OmsConfig {

    private final Http http = new Http();
    private final Grpc grpc = new Grpc();
    private final Shard shard = new Shard();
    private final Control control = new Control();
    private final Events events = new Events();
    private final Ledger ledger = new Ledger();
    private final DomainEvents domainEvents = new DomainEvents();
    private final Pii pii = new Pii();
    private final Risk risk = new Risk();
    private final Routing routing = new Routing();
    private final Fix fix = new Fix();
    private final FixIn fixIn = new FixIn();
    private final Venue venue = new Venue();
    private final Settlement settlement = new Settlement();
    private final Marketdata marketdata = new Marketdata();
    private final CorporateAction corporateAction = new CorporateAction();
    private final IskTax iskTax = new IskTax();
    private final Desk desk = new Desk();
    private final Fx fx = new Fx();
    private final Otel otel = new Otel();
    private final Cluster cluster = new Cluster();
    private final Admin admin = new Admin();

    public Http getHttp() { return http; }
    public Grpc getGrpc() { return grpc; }
    public Shard getShard() { return shard; }
    public Control getControl() { return control; }
    public Events getEvents() { return events; }
    public Ledger getLedger() { return ledger; }
    public DomainEvents getDomainEvents() { return domainEvents; }
    public Pii getPii() { return pii; }
    public Risk getRisk() { return risk; }
    public Routing getRouting() { return routing; }
    public Fix getFix() { return fix; }
    public FixIn getFixIn() { return fixIn; }
    public Venue getVenue() { return venue; }
    public Settlement getSettlement() { return settlement; }
    public Marketdata getMarketdata() { return marketdata; }
    public CorporateAction getCorporateAction() { return corporateAction; }
    public IskTax getIskTax() { return iskTax; }
    public Desk getDesk() { return desk; }
    public Fx getFx() { return fx; }
    public Otel getOtel() { return otel; }
    public Cluster getCluster() { return cluster; }
    public Admin getAdmin() { return admin; }

    /**
     * Knobs for the internal admin surface ({@link com.balh.oms.ingress.AdminOrderController}).
     *
     * <p>Today this is just the cancel-observation timing for the force-cancel endpoint. The
     * polling loop watches the orders row for the projector to land the cluster-emitted
     * {@code OrderCancelAppliedEvent}; if no version bump + terminal status is observed within
     * the timeout the endpoint returns {@code 410 Gone} with {@code cluster_forgot_order}.
     * That status code is the operator-visible signal that the cluster swallowed the command
     * silently (typically: in-memory {@code orderIndex} no longer contains the order — e.g.
     * after a journal wipe and replay that didn't restore it), and manual Postgres cleanup
     * is needed.
     */
    public static class Admin {
        // The 50ms floor keeps the poll cheap; the 100ms timeout floor protects against
        // accidentally disabling the observation (e.g. setting it to 0 in a config typo).
        private static final long MIN_TIMEOUT_MS = 100L;
        private static final long MIN_POLL_INTERVAL_MS = 5L;

        /**
         * How long to wait after the cluster submit returns OK for the projector to write the
         * cancel-applied event to Postgres (visible as a version bump + terminal status on
         * the orders row).
         *
         * <p>Default {@code 2000} ms: comfortably above the steady-state projector lag of
         * 50–200 ms while still snappy enough for an interactive operator on the trading-desk
         * console. Tune up if the deployment routinely sees longer projector lag under load.
         */
        private long cancelObservationTimeoutMs = 2000L;

        /**
         * Polling cadence for the cancel-observation loop. Tight enough that a happy-path
         * response feels immediate ({@code orders.findById} is a primary-key lookup); not so
         * tight that we burn cycles on negative polls when the cluster is going to forget the
         * order anyway.
         */
        private long cancelObservationPollIntervalMs = 50L;

        public long getCancelObservationTimeoutMs() { return cancelObservationTimeoutMs; }
        public void setCancelObservationTimeoutMs(long v) {
            this.cancelObservationTimeoutMs = Math.max(MIN_TIMEOUT_MS, v);
        }
        public long getCancelObservationPollIntervalMs() { return cancelObservationPollIntervalMs; }
        public void setCancelObservationPollIntervalMs(long v) {
            this.cancelObservationPollIntervalMs = Math.max(MIN_POLL_INTERVAL_MS, v);
        }
    }

    public static class Http {
        private String internalApiKey = "";
        public String getInternalApiKey() { return internalApiKey; }
        public void setInternalApiKey(String v) { this.internalApiKey = v; }
    }

    /** Optional gRPC ingress (same order accept semantics as HTTP). */
    public static class Grpc {
        private boolean enabled = false;
        private int port = 9099;
        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean v) { this.enabled = v; }
        public int getPort() { return port; }
        public void setPort(int port) { this.port = Math.max(1, Math.min(port, 65535)); }
    }

    /**
     * Sharding knobs. Pinned in {@code oms/docs/decisions.md} §4: shard key is {@code account_id},
     * hash function is {@code xxh64} (via {@code net.openhft:zero-allocation-hashing}, encoded in
     * {@link com.balh.oms.domain.ShardKey}), and slice 1 ships at {@code count = 1}. The mapping
     * function exists from day one so call sites do not change when shards grow.
     *
     * <ul>
     *   <li>{@link #count} — number of shards in the OMS topology. Default {@code 1}. Phase 4
     *       Tier 2.5 phase E-3b lifted the previous E-1 single-shard cap; an ingress JVM now
     *       hosts {@code count} {@link com.balh.oms.cluster.OmsClusterIngressClient}s wired by
     *       {@link com.balh.oms.cluster.OmsClusterClientsConfiguration} and routed by
     *       {@link com.balh.oms.cluster.OmsClusterShardRouter}. {@code count > 1} is supported
     *       on ingress JVMs only; FIX-egress and projector JVMs still operate at
     *       {@code count = 1} (a follow-up slice will lift this for FIX-inbound).</li>
     *   <li>{@link #id} — this JVM's shard id (meaningful on per-shard JVM roles like
     *       {@code oms-cluster-node} / {@code oms-postgres-projector}). Used as a
     *       {@code shard_id} tag on Micrometer meters (see {@code MetricsConfig}) and as the
     *       expected shard for the projector's E-2 shard guard. Default {@code 0}.</li>
     * </ul>
     */
    public static class Shard {
        private int id = 0;
        private int count = 1;
        public int getId() { return id; }
        public void setId(int id) { this.id = id; }
        public int getCount() { return count; }
        public void setCount(int count) { this.count = count; }
    }

    /**
     * Control-plane admission knobs. Phase 3 slice 3g of the Aeron Cluster substrate plan removed
     * the {@code postgres-write-path} switch (the cluster admission service is now the only writer)
     * and the unused {@code tailer-batch-size} (only the deleted {@code ChronicleControlTailReader}
     * read it). {@link #maxJobAgeMs} survives — it still drives {@link com.balh.oms.tailer.StaleJobGuard}
     * inside {@link com.balh.oms.tailer.OrderControlAdmission}.
     */
    public static class Control {
        /** Default 5 minutes — see plans/oms-phase0-interim-decisions.md until trading-ops signs a stricter value. */
        private long maxJobAgeMs = 300_000L;
        public long getMaxJobAgeMs() { return maxJobAgeMs; }
        public void setMaxJobAgeMs(long v) { this.maxJobAgeMs = v; }
    }

    public static class Events {
        private final Nats nats = new Nats();
        public Nats getNats() { return nats; }

        public static class Nats {
            private boolean enabled = false;
            private String url = "nats://localhost:4222";
            private String subjectPrefix = "oms.events";
            private String streamName = "OMS_EVENTS";
            private long connectionTimeoutMs = 5000L;
            public boolean isEnabled() { return enabled; }
            public void setEnabled(boolean v) { this.enabled = v; }
            public String getUrl() { return url; }
            public void setUrl(String v) { this.url = v; }
            public String getSubjectPrefix() { return subjectPrefix; }
            public void setSubjectPrefix(String v) { this.subjectPrefix = v; }
            public String getStreamName() { return streamName; }
            public void setStreamName(String v) { this.streamName = v; }
            public long getConnectionTimeoutMs() { return connectionTimeoutMs; }
            public void setConnectionTimeoutMs(long v) { this.connectionTimeoutMs = v; }
        }
    }

    public static class Ledger {
        private boolean enabled = false;
        /** When true, OMS POSTs a Ledger sync inflight hold on BUY accept (see {@link LedgerInflightReservationClient}). */
        private boolean inflightReservationEnabled = false;
        /**
         * Ledger {@code balance_id} that receives the customer leg of the inflight hold
         * (bank-side suspense / OMS hold account). Required when {@link #inflightReservationEnabled} is true.
         *
         * <p>This is the legacy single-currency destination. When the BUY source balance
         * is in a currency that has an entry in {@link #inflightHoldDestinationBalanceIdByCurrency},
         * that per-currency entry wins and this field becomes the fallback (used for any
         * currency that doesn't yet have a dedicated nostro/hold account provisioned).
         * Operators keeping a single-currency stack continue setting only this field and
         * the per-currency map stays empty.
         */
        private String inflightHoldDestinationBalanceId = "";
        /**
         * Per-source-currency override map (uppercase ISO ccy → Ledger {@code balance_id}).
         * Spring binds this from {@code oms.ledger.inflight-hold-destination-balance-id-by-currency.USD=balance_xxx}
         * or environment {@code OMS_LEDGER_INFLIGHT_HOLD_DESTINATION_BALANCE_ID_BY_CURRENCY_USD=balance_xxx}.
         *
         * <p>{@link com.balh.oms.ledger.RestLedgerInflightReservationClient} consults this map
         * by the *resolved* source-balance currency (already pulled from Ledger). A missing
         * entry falls back to {@link #inflightHoldDestinationBalanceId} and emits a warn log
         * the first time it hits — so a misconfigured stack still attempts the hold instead
         * of refusing the order outright, and the Ledger will surface the real mismatch.
         *
         * <p>Plans/oms-multi-currency-invest-accounts.md §8.4. Unblocks the cross-currency
         * BUY path (e.g. EUR-funded customer buying a USD instrument) which would
         * otherwise be cancelled at accept with {@code CURRENCY_MISMATCH} on the Ledger
         * {@code POST /transactions} call.
         */
        private final java.util.Map<String, String> inflightHoldDestinationBalanceIdByCurrency =
                new java.util.LinkedHashMap<>();
        /** ISO currency code for the inflight hold (major unit in {@code amount}). */
        private String inflightReservationCurrency = "EUR";
        /** Ledger amount scaling (e.g. 100 = cents). */
        private int inflightReservationPrecision = 100;
        private String baseUrl = "http://localhost:5001";
        private String apiKey = "";
        /** Bearer token for elevated ISK read API ({@code /internal/v1/isk/*}). */
        private String elevatedApiKey = "";
        /** When true with {@link #elevatedApiKey}, wires {@link com.balh.oms.ledger.LedgerIskReadClient}. */
        private boolean iskReadEnabled = false;
        /** When true, wires {@link com.balh.oms.ledger.LedgerMetadataClient} for ISK metadata sync. */
        private boolean metadataSyncEnabled = false;
        private long connectTimeoutMs = 2000L;
        private long readTimeoutMs = 5000L;
        /**
         * When true with {@link #inflightReservationEnabled}, BUY inflight hold is enqueued to
         * {@code ledger_inflight_outbox} in the same DB transaction; {@link com.balh.oms.reconciler.LedgerInflightOutboxReconciler}
         * calls Ledger after commit.
         */
        private boolean inflightAsyncEnabled = false;
        /**
         * When {@code true} (default), BUY inflight holds are placed synchronously on the ingress
         * thread <em>before</em> Aeron cluster admission so {@code INSUFFICIENT_FUNDS} returns HTTP
         * 422 at accept and never reaches venue egress (b625f5d). When {@code false} with
         * {@link #inflightAsyncEnabled} and coalescer off, holds are projected into
         * {@code ledger_inflight_outbox} after admit and
         * {@link com.balh.oms.reconciler.LedgerInflightOutboxReconciler} drives Ledger off the
         * hot path (slice 4p bench throughput). Pop {@code ~/.oms-bench.env} sets this
         * {@code false} for ledger-on soak; production retail keeps the default {@code true}.
         */
        private boolean inflightPreAdmitHoldEnabled = true;
        private long inflightOutboxReconcilerAgeMs = 2000L;
        private int inflightOutboxReconcilerBatchSize = 50;
        private long inflightOutboxReconcilerIntervalMs = 500L;
        /**
         * Phase 4 slice 4p — controls for {@link com.balh.oms.reconciler.LedgerInflightHoldFailureCompensator}.
         * When enabled, rows on {@code ledger_inflight_outbox} that have failed past
         * {@link #inflightCompensatorAttemptsThreshold} are cancelled in the cluster (idempotent
         * {@code CancelOrderCommand}) and stamped {@code compensated_at} so the reconciler stops
         * retrying them. Disabled by default — operator opts in alongside
         * {@link #inflightAsyncEnabled} since the compensator is the correctness backstop for the
         * async path.
         */
        /**
         * Phase 4 slice 4q — when {@code true} with {@link #inflightReservationEnabled}, BUY
         * inflight holds are coalesced through {@link com.balh.oms.ledger.LedgerInflightCoalescer}
         * onto Ledger {@code POST /transactions/bulk?inflight=true&atomic=false} from a daemon
         * flush thread, instead of the per-order outbox path
         * ({@link #inflightAsyncEnabled}).
         *
         * <p><b>Off by default and currently a regression vs slice 4p — do NOT enable in
         * production until slice 4r lands.</b> Pop! 2026-05-14 A/B (5000 orders / concurrency 50,
         * 2× ingress replicas, real Ledger HTTP, same Postgres state): outbox path =
         * 768 rps / HTTP RTT p50 16.7 ms, coalescer path = 95 rps / p50 431 ms, i.e. 8.1× rps
         * regression / 25.8× p50 inflation. Root cause (verified against
         * {@code oms_ledger_inflight_coalescer_flush_seconds} and {@code _submit_seconds}): the
         * accept thread blocks on {@link com.balh.oms.ledger.LedgerInflightCoalescer#submit}'s
         * per-item future and only one daemon flush thread per JVM consumes the queue, so each
         * JVM is ceiling-limited at "one bulk POST in flight at a time" while the slice 4p outbox
         * path stays fire-and-forget at accept (the outbox row is committed in the same tx as the
         * accept; Ledger HTTP is entirely off the ingress critical path). The intra-batch OCC
         * reduction landed as designed; the loss came from the new accept-side serialisation
         * point, not from the bulk dispatcher itself. Full numbers + math sanity-check + the
         * slice 4r redesign roadmap (fire-and-forget at-accept dispatch reusing this slice's
         * bulk dispatcher + outbox-fallback wiring) live in
         * {@code oms/docs/runbooks/local-multi-jvm-bench.md} {@code ## Slice 4q evidence} /
         * {@code ## Slice 4q verdict + roadmap}.
         *
         * <p><b>What this flag still costs even when correctness is fine</b>: the coalescer reuses
         * {@code ledger_inflight_outbox} as its fallback path (any flush failure writes pending
         * items to the outbox, where the existing reconciler + compensator pick them up). On
         * the happy path, items in the in-memory queue at JVM crash time are lost (the order is
         * admitted at the cluster but the hold never reached either Ledger or the outbox). The
         * slice 4p compensator does NOT recover this gap because it only graduates rows that
         * crossed {@link #inflightCompensatorAttemptsThreshold} — a row that never existed
         * cannot graduate.
         *
         * <p>Until slice 4r the operator contract is: leave this flag {@code false} and run the
         * slice 4p outbox path ({@link #inflightAsyncEnabled} {@code = true}, this flag
         * {@code false}). The coalescer code is preserved on {@code main} so slice 4r can
         * iterate on the dispatch loop (the bulk dispatcher and outbox-fallback wiring compose
         * cleanly; only the accept-side coupling needs to change).
         */
        /**
         * Slice 4r — when {@code true} with {@link #inflightAsyncEnabled}, the outbox reconciler
         * drives holds via {@link com.balh.oms.ledger.LedgerInflightBulkDispatcher} instead of
         * one {@code POST /transactions} per row.
         */
        private boolean inflightOutboxBulkEnabled = false;

        private boolean inflightCoalescerEnabled = false;
        /**
         * Maximum items the coalescer will batch into a single Ledger bulk POST. Larger batches
         * amortise HTTP round-trip cost across more orders but widen the worst-case shutdown
         * drain (each in-flight batch must finish or be flushed to outbox before the JVM stops).
         */
        private int inflightCoalescerMaxBatchSize = 50;
        /**
         * Maximum time the daemon flush thread waits for the next item once the queue empties
         * before flushing whatever it has. Tuning lever between latency (lower = closer to sync
         * RTT for first item) and throughput (higher = larger batches when load is bursty).
         * Default 5 ms balances both for the slice 4p bench shape (HTTP RTT ≈ 25 ms; 5 ms wait
         * lets a 50-item batch fill in steady state without stalling first arrivals).
         */
        private long inflightCoalescerFlushIntervalMicros = 5_000L;
        /**
         * Bounded queue capacity. {@code submit} fails fast with {@code IllegalStateException}
         * when full, which propagates as 503 to the ingress caller — same operator contract as
         * cluster back-pressure.
         */
        private int inflightCoalescerQueueCapacity = 1_000;
        /**
         * Maximum concurrent bulk Ledger POSTs per ingress JVM. Slice 4q used a single flush
         * thread; pipelining lifts the per-JVM ceiling toward Ledger bulk throughput under 10k/s
         * soak without changing pre-admit semantics.
         */
        private int inflightCoalescerMaxInFlightFlushes = 8;
        /**
         * Caller (ingress thread) wait budget for the per-item future to complete. Includes the
         * worst-case flush wait + the bulk Ledger HTTP RTT. Sized at 2 s to absorb an entire
         * Ledger {@code readTimeoutMs} (5 s) at the bulk endpoint while still failing the order
         * fast on a stalled coalescer (operator should pair this with monitoring on the
         * {@code oms_ledger_inflight_coalescer_submit_seconds} histogram).
         */
        private long inflightCoalescerSubmitTimeoutMs = 2_000L;
        private boolean inflightCompensatorEnabled = false;
        /**
         * Wed-demo (V32): when {@code true}, {@code LedgerInflightLifecycleReconciler} polls
         * {@code ledger_inflight_outbox} for rows whose corresponding {@code orders.status} is
         * terminal ({@code FILLED} / {@code CANCELLED} / {@code REJECTED} / {@code EXPIRED}) and
         * issues {@code PUT /transactions/inflight/{txID}} {@code commit} (for FILLED) or
         * {@code void} (for the rest) against the Ledger. Off by default so production deploys
         * upgrade on opt-in; the demo turns it on alongside {@code inflight-async-enabled=true}.
         */
        private boolean inflightLifecycleReconcilerEnabled = false;
        /**
         * Bounds the per-row retry count for the lifecycle reconciler. After this many failed
         * attempts the row is no longer eligible (the Ledger's expiry sweep is the safety net).
         * Sized small (5) for the demo because the lifecycle call should be a strict no-op
         * after a single 2xx — failures here usually signal a config / version mismatch that
         * an operator should look at, not a transient blip to retry indefinitely.
         */
        private int inflightLifecycleAttemptsThreshold = 5;
        private int inflightLifecycleBatchSize = 50;
        private long inflightLifecycleIntervalMs = 1_000L;
        /**
         * Per-row minimum backoff between lifecycle attempts. A row that failed once must wait
         * this long before being re-fetched. Demo-sized to 2 s so a Ledger blip recovers
         * quickly; production should track Ledger's transient-failure SLO.
         */
        private long inflightLifecycleRetryBackoffMs = 2_000L;
        /** Read-timeout for the PUT /transactions/inflight/{txID} HTTP call. */
        private long inflightLifecycleSubmitTimeoutMs = 2_000L;
        /**
         * Number of failed Ledger hold attempts before the compensator graduates a row to a
         * {@code CancelOrderCommand}. Below this, transient blips stay on the reconciler retry
         * path; at-or-above, the row is treated as a hold the Ledger will not accept (e.g.
         * insufficient balance) and the order is cancelled. Operator should size this against
         * Ledger's transient-failure SLO so a brief Ledger outage does not turn into spurious
         * cancels.
         */
        private int inflightCompensatorAttemptsThreshold = 3;
        private int inflightCompensatorBatchSize = 50;
        private long inflightCompensatorIntervalMs = 1000L;
        /** Time budget for {@link com.balh.oms.cluster.OmsClusterIngressClient#submitCancelOrder}. */
        private long inflightCompensatorSubmitTimeoutMs = 2000L;
        /**
         * When {@code true}, a row is written to {@code ledger_settlement_outbox} in the same transaction as OMS
         * transition to {@code settled} on {@code TRADE} executions; {@link com.balh.oms.reconciler.LedgerSettlementOutboxReconciler}
         * delivers when {@link #settlementOutboxReconcilerEnabled} is {@code true}.
         */
        private boolean settlementOutboxEnabled = false;
        /**
         * When {@code true} with {@link #enabled}, {@link com.balh.oms.reconciler.LedgerSettlementOutboxReconciler} POSTs
         * unposted rows to Ledger and sets {@code posted_at}.
         */
        private boolean settlementOutboxReconcilerEnabled = false;
        private long settlementOutboxReconcilerAgeMs = 2000L;
        private int settlementOutboxReconcilerBatchSize = 50;
        private long settlementOutboxReconcilerIntervalMs = 500L;
        /**
         * After this many delivery attempts with a skippable reason ({@code SKIPPED_UNFUNDED_BALANCE},
         * {@code SKIPPED_INDICATOR_NOT_FOUND}), the reconciler tombstones the row ({@code skipped_at}) so it
         * is no longer locked every tick.
         */
        private int settlementOutboxSkipAfterAttempts = 10;
        /**
         * Minimum interval between identical settlement-outbox WARN log lines (same reason + indicator).
         * Suppressed repeats append a count suffix on the first line in each window.
         */
        private long settlementOutboxSkipWarnThrottleMs = 60_000L;
        /**
         * Path on the Ledger HTTP base URL for settlement outbox POST (must start with {@code /} or be relative segment;
         * normalized to a leading slash). Finance must align Ledger route with this value.
         */
        private String settlementPostingHttpPath = "/internal/v0/settlement-outbox";

        private final BalanceIdentityCache balanceIdentityCache = new BalanceIdentityCache();

        public BalanceIdentityCache getBalanceIdentityCache() { return balanceIdentityCache; }

        /**
         * Phase 4 Tier 2.5 phase D-8 — JVM-local cache for the
         * {@code (balanceId -> identityId)} binding read by
         * {@link com.balh.oms.ingress.OrderIngressService#maybeVerifyLedgerBalanceBinding}.
         *
         * <p>The binding is durable in Ledger (it only changes when an operator reassigns a
         * balance to a different identity, which is a manual action and rare in steady state).
         * Caching it locally removes a synchronous {@code GET /balances/{id}} round-trip from
         * the order accept hot path. Pop! 2026-05-14 D-3 jstack at c1600 / 11 629 rps showed
         * 187 / 200 ingress Tomcat threads parked in
         * {@code RestLedgerBalanceClient.fetchBalanceRoot} — i.e. the verify HTTP call was the
         * primary throughput wall after Postgres + Aeron came off the hot path.
         *
         * <p>Negative responses ({@code ledger_balance_not_found}, network errors) are
         * <strong>not</strong> cached: those should remain fast-retryable so transient blips
         * don't pin a 4xx/5xx for the whole TTL window. Mismatch (claimed identity != cached
         * identity) is also surfaced from the cached value, because the durable identity is
         * the cached value — a mismatch means the caller's claim is wrong, not the cache.
         *
         * <p>Disabled by default. When enabled, callers must accept up to {@link #ttlSeconds}
         * of staleness on the (balanceId -> identityId) mapping. Operators flipping
         * a balance reassignment that needs to be effective sooner can shrink {@link #ttlSeconds}
         * temporarily, or trigger a JVM restart on the affected ingresses.
         */
        public static class BalanceIdentityCache {

            /** Default 5 min — short enough to limit operator-reassignment staleness, long enough to absorb burst load. */
            private static final long DEFAULT_TTL_SECONDS = 300L;
            /**
             * Default 100k entries. Each entry is a small fixed-size pair of UUID-ish strings
             * (~80 B + Caffeine overhead, well under 200 B), so 100k * 200 B = 20 MB worst
             * case — comfortable on every ingress JVM. Operators with much larger balance
             * domains can raise this; the eviction policy is Caffeine's default Window-TinyLFU.
             */
            private static final long DEFAULT_MAX_SIZE = 100_000L;

            private boolean enabled = false;
            private long ttlSeconds = DEFAULT_TTL_SECONDS;
            private long maxSize = DEFAULT_MAX_SIZE;

            public boolean isEnabled() { return enabled; }
            public void setEnabled(boolean v) { this.enabled = v; }

            public long getTtlSeconds() { return ttlSeconds; }
            public void setTtlSeconds(long v) {
                // Floor at 1s — anything smaller is cheaper to just call Ledger every time.
                this.ttlSeconds = Math.max(1L, v);
            }

            public long getMaxSize() { return maxSize; }
            public void setMaxSize(long v) {
                // Floor at 1 — Caffeine rejects 0/negative; "0" means "off", which is what
                // BalanceIdentityCache.enabled=false expresses.
                this.maxSize = Math.max(1L, v);
            }
        }

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean v) { this.enabled = v; }
        public boolean isInflightReservationEnabled() { return inflightReservationEnabled; }
        public void setInflightReservationEnabled(boolean v) { this.inflightReservationEnabled = v; }
        public String getInflightHoldDestinationBalanceId() { return inflightHoldDestinationBalanceId; }
        public void setInflightHoldDestinationBalanceId(String v) { this.inflightHoldDestinationBalanceId = v; }
        /**
         * Returns the mutable per-currency dest map. Spring populates this map directly
         * (relaxed binding handles upper/lower case keys); we normalise to uppercase
         * inside {@link com.balh.oms.ledger.RestLedgerInflightReservationClient} so a
         * yaml entry like {@code eur: balance_x} works the same as {@code EUR: balance_x}.
         */
        public java.util.Map<String, String> getInflightHoldDestinationBalanceIdByCurrency() {
            return inflightHoldDestinationBalanceIdByCurrency;
        }
        public String getInflightReservationCurrency() { return inflightReservationCurrency; }
        public void setInflightReservationCurrency(String v) { this.inflightReservationCurrency = v; }
        public int getInflightReservationPrecision() { return inflightReservationPrecision; }
        public void setInflightReservationPrecision(int v) { this.inflightReservationPrecision = v; }
        public String getBaseUrl() { return baseUrl; }
        public void setBaseUrl(String v) { this.baseUrl = v; }
        public String getApiKey() { return apiKey; }
        public void setApiKey(String v) { this.apiKey = v; }
        public String getElevatedApiKey() { return elevatedApiKey; }
        public void setElevatedApiKey(String v) { this.elevatedApiKey = v; }
        public boolean isIskReadEnabled() { return iskReadEnabled; }
        public void setIskReadEnabled(boolean v) { this.iskReadEnabled = v; }
        public boolean isMetadataSyncEnabled() { return metadataSyncEnabled; }
        public void setMetadataSyncEnabled(boolean v) { this.metadataSyncEnabled = v; }
        public long getConnectTimeoutMs() { return connectTimeoutMs; }
        public void setConnectTimeoutMs(long v) { this.connectTimeoutMs = v; }
        public long getReadTimeoutMs() { return readTimeoutMs; }
        public void setReadTimeoutMs(long v) { this.readTimeoutMs = v; }
        public boolean isInflightAsyncEnabled() { return inflightAsyncEnabled; }
        public void setInflightAsyncEnabled(boolean v) { this.inflightAsyncEnabled = v; }
        public boolean isInflightPreAdmitHoldEnabled() { return inflightPreAdmitHoldEnabled; }
        public void setInflightPreAdmitHoldEnabled(boolean v) { this.inflightPreAdmitHoldEnabled = v; }
        public long getInflightOutboxReconcilerAgeMs() { return inflightOutboxReconcilerAgeMs; }
        public void setInflightOutboxReconcilerAgeMs(long v) { this.inflightOutboxReconcilerAgeMs = v; }
        public int getInflightOutboxReconcilerBatchSize() { return inflightOutboxReconcilerBatchSize; }
        public void setInflightOutboxReconcilerBatchSize(int v) { this.inflightOutboxReconcilerBatchSize = v; }
        public long getInflightOutboxReconcilerIntervalMs() { return inflightOutboxReconcilerIntervalMs; }
        public void setInflightOutboxReconcilerIntervalMs(long v) { this.inflightOutboxReconcilerIntervalMs = v; }

        public boolean isInflightOutboxBulkEnabled() { return inflightOutboxBulkEnabled; }
        public void setInflightOutboxBulkEnabled(boolean v) { this.inflightOutboxBulkEnabled = v; }

        public boolean isInflightCoalescerEnabled() { return inflightCoalescerEnabled; }
        public void setInflightCoalescerEnabled(boolean v) { this.inflightCoalescerEnabled = v; }
        public int getInflightCoalescerMaxBatchSize() { return inflightCoalescerMaxBatchSize; }
        public void setInflightCoalescerMaxBatchSize(int v) {
            this.inflightCoalescerMaxBatchSize = Math.max(1, v);
        }
        public long getInflightCoalescerFlushIntervalMicros() { return inflightCoalescerFlushIntervalMicros; }
        public void setInflightCoalescerFlushIntervalMicros(long v) {
            this.inflightCoalescerFlushIntervalMicros = Math.max(100L, v);
        }
        public int getInflightCoalescerQueueCapacity() { return inflightCoalescerQueueCapacity; }
        public void setInflightCoalescerQueueCapacity(int v) {
            this.inflightCoalescerQueueCapacity = Math.max(100, v);
        }
        public int getInflightCoalescerMaxInFlightFlushes() { return inflightCoalescerMaxInFlightFlushes; }
        public void setInflightCoalescerMaxInFlightFlushes(int v) {
            this.inflightCoalescerMaxInFlightFlushes = Math.max(1, v);
        }
        public long getInflightCoalescerSubmitTimeoutMs() { return inflightCoalescerSubmitTimeoutMs; }
        public void setInflightCoalescerSubmitTimeoutMs(long v) {
            this.inflightCoalescerSubmitTimeoutMs = Math.max(100L, v);
        }

        public boolean isInflightCompensatorEnabled() { return inflightCompensatorEnabled; }
        public void setInflightCompensatorEnabled(boolean v) { this.inflightCompensatorEnabled = v; }
        public int getInflightCompensatorAttemptsThreshold() { return inflightCompensatorAttemptsThreshold; }
        public void setInflightCompensatorAttemptsThreshold(int v) {
            this.inflightCompensatorAttemptsThreshold = Math.max(1, v);
        }
        public int getInflightCompensatorBatchSize() { return inflightCompensatorBatchSize; }
        public void setInflightCompensatorBatchSize(int v) {
            this.inflightCompensatorBatchSize = Math.max(1, v);
        }
        public long getInflightCompensatorIntervalMs() { return inflightCompensatorIntervalMs; }
        public void setInflightCompensatorIntervalMs(long v) {
            this.inflightCompensatorIntervalMs = Math.max(50L, v);
        }
        public long getInflightCompensatorSubmitTimeoutMs() { return inflightCompensatorSubmitTimeoutMs; }
        public void setInflightCompensatorSubmitTimeoutMs(long v) {
            this.inflightCompensatorSubmitTimeoutMs = Math.max(100L, v);
        }
        public boolean isInflightLifecycleReconcilerEnabled() { return inflightLifecycleReconcilerEnabled; }
        public void setInflightLifecycleReconcilerEnabled(boolean v) { this.inflightLifecycleReconcilerEnabled = v; }
        public int getInflightLifecycleAttemptsThreshold() { return inflightLifecycleAttemptsThreshold; }
        public void setInflightLifecycleAttemptsThreshold(int v) {
            this.inflightLifecycleAttemptsThreshold = Math.max(1, v);
        }
        public int getInflightLifecycleBatchSize() { return inflightLifecycleBatchSize; }
        public void setInflightLifecycleBatchSize(int v) {
            this.inflightLifecycleBatchSize = Math.max(1, v);
        }
        public long getInflightLifecycleIntervalMs() { return inflightLifecycleIntervalMs; }
        public void setInflightLifecycleIntervalMs(long v) {
            this.inflightLifecycleIntervalMs = Math.max(50L, v);
        }
        public long getInflightLifecycleRetryBackoffMs() { return inflightLifecycleRetryBackoffMs; }
        public void setInflightLifecycleRetryBackoffMs(long v) {
            this.inflightLifecycleRetryBackoffMs = Math.max(0L, v);
        }
        public long getInflightLifecycleSubmitTimeoutMs() { return inflightLifecycleSubmitTimeoutMs; }
        public void setInflightLifecycleSubmitTimeoutMs(long v) {
            this.inflightLifecycleSubmitTimeoutMs = Math.max(100L, v);
        }

        public boolean isSettlementOutboxEnabled() {
            return settlementOutboxEnabled;
        }

        public void setSettlementOutboxEnabled(boolean settlementOutboxEnabled) {
            this.settlementOutboxEnabled = settlementOutboxEnabled;
        }

        public boolean isSettlementOutboxReconcilerEnabled() {
            return settlementOutboxReconcilerEnabled;
        }

        public void setSettlementOutboxReconcilerEnabled(boolean settlementOutboxReconcilerEnabled) {
            this.settlementOutboxReconcilerEnabled = settlementOutboxReconcilerEnabled;
        }

        public long getSettlementOutboxReconcilerAgeMs() {
            return settlementOutboxReconcilerAgeMs;
        }

        public void setSettlementOutboxReconcilerAgeMs(long settlementOutboxReconcilerAgeMs) {
            this.settlementOutboxReconcilerAgeMs = Math.max(0L, settlementOutboxReconcilerAgeMs);
        }

        public int getSettlementOutboxReconcilerBatchSize() {
            return settlementOutboxReconcilerBatchSize;
        }

        public void setSettlementOutboxReconcilerBatchSize(int settlementOutboxReconcilerBatchSize) {
            this.settlementOutboxReconcilerBatchSize = Math.max(1, settlementOutboxReconcilerBatchSize);
        }

        public long getSettlementOutboxReconcilerIntervalMs() {
            return settlementOutboxReconcilerIntervalMs;
        }

        public void setSettlementOutboxReconcilerIntervalMs(long settlementOutboxReconcilerIntervalMs) {
            this.settlementOutboxReconcilerIntervalMs = Math.max(100L, settlementOutboxReconcilerIntervalMs);
        }

        public int getSettlementOutboxSkipAfterAttempts() {
            return settlementOutboxSkipAfterAttempts;
        }

        public void setSettlementOutboxSkipAfterAttempts(int settlementOutboxSkipAfterAttempts) {
            this.settlementOutboxSkipAfterAttempts = Math.max(1, settlementOutboxSkipAfterAttempts);
        }

        public long getSettlementOutboxSkipWarnThrottleMs() {
            return settlementOutboxSkipWarnThrottleMs;
        }

        public void setSettlementOutboxSkipWarnThrottleMs(long settlementOutboxSkipWarnThrottleMs) {
            this.settlementOutboxSkipWarnThrottleMs = Math.max(1_000L, settlementOutboxSkipWarnThrottleMs);
        }

        public String getSettlementPostingHttpPath() {
            return settlementPostingHttpPath;
        }

        public void setSettlementPostingHttpPath(String settlementPostingHttpPath) {
            this.settlementPostingHttpPath = settlementPostingHttpPath == null ? "" : settlementPostingHttpPath.trim();
        }
    }

    public static class DomainEvents {
        private long reconcilerAgeMs = 2000L;
        private int reconcilerBatchSize = 100;
        private long reconcilerIntervalMs = 500L;
        public long getReconcilerAgeMs() { return reconcilerAgeMs; }
        public void setReconcilerAgeMs(long v) { this.reconcilerAgeMs = v; }
        public int getReconcilerBatchSize() { return reconcilerBatchSize; }
        public void setReconcilerBatchSize(int v) { this.reconcilerBatchSize = v; }
        public long getReconcilerIntervalMs() { return reconcilerIntervalMs; }
        public void setReconcilerIntervalMs(long v) { this.reconcilerIntervalMs = v; }
    }

    public static class Pii {
        private boolean auditTraceEnabled = false;
        private String hashSecret = "dev-only-change-me";
        public boolean isAuditTraceEnabled() { return auditTraceEnabled; }
        public void setAuditTraceEnabled(boolean v) { this.auditTraceEnabled = v; }
        public String getHashSecret() { return hashSecret; }
        public void setHashSecret(String v) { this.hashSecret = v; }
    }

    /**
     * Pre-trade limits read on each control decision. Zero or negative numeric
     * thresholds mean "check disabled" for that dimension.
     */
    public static class Risk {
        private boolean instrumentAllowlistEnabled = false;
        private String allowedInstrumentSymbols = "";
        /**
         * When {@code true}, symbols must appear in {@link #tradableInstrumentSymbolSet()} or the order is rejected
         * with {@link com.balh.oms.domain.RejectCode#RISK_INSTRUMENT_NOT_ALLOWED} (slice 5 v1; replaces full
         * marketdata-backed {@code instruments} cache until that integration exists).
         */
        private boolean instrumentTradabilityCheckEnabled = false;
        private String tradableInstrumentSymbols = "";
        /**
         * When {@code true} with {@link #instrumentTradabilityCheckEnabled} and {@link OmsConfig#getMarketdata()}
         * {@code enabled}, {@link com.balh.oms.marketdata.MarketdataInstrumentsCache} symbols override CSV when non-empty;
         * otherwise CSV {@link #tradableInstrumentSymbols} applies.
         */
        private boolean instrumentTradabilityFromMarketdataEnabled = false;
        /**
         * When {@code true}, symbols in {@link #haltedInstrumentSymbolSet()} reject with {@link
         * com.balh.oms.domain.RejectCode#RISK_SYMBOL_HALT} (evaluated before allowlist / tradability).
         */
        private boolean instrumentSymbolHaltCheckEnabled = false;
        private String haltedInstrumentSymbols = "";
        private BigDecimal fatFingerMaxLimitPrice = BigDecimal.ZERO;
        private BigDecimal fatFingerMaxOrderQuantity = BigDecimal.ZERO;
        private BigDecimal maxOrderNotional = BigDecimal.ZERO;
        /**
         * When {@code true}, {@link com.balh.oms.risk.SanctionsExecutionGate} runs before other risk checks
         * (after global halt). Uses {@link #sanctionsCacheMaxAgeSeconds} for permissive cache; {@link
         * #sanctionsRecheckStrict} rejects every order until a real downstream client exists.
         */
        private boolean sanctionsRecheckEnabled = false;
        private boolean sanctionsRecheckStrict = false;
        private long sanctionsCacheMaxAgeSeconds = 3_600L;
        /**
         * Minimum wall-clock spacing between accepted control evaluations per {@code account_id}; {@code 0}
         * disables ({@link com.balh.oms.domain.RejectCode#RISK_RATE_LIMIT} on violation).
         */
        private long orderMinIntervalMsPerAccount = 0L;
        /** When {@code true}, limit prices must align to {@link #tickSizeIncrement} grid (zero disables increment). */
        private boolean tickSizeCheckEnabled = false;
        private java.math.BigDecimal tickSizeIncrement = java.math.BigDecimal.ZERO;
        /**
         * Reserved STP / venue-calendar gate (slice 8). When enabled without venue calendar wiring, evaluator
         * returns {@link com.balh.oms.domain.RejectCode#RISK_STP_GATE} only if {@link #stpGateRejectAll} is true
         * (testing knob; default false).
         */
        private boolean stpGateEnabled = false;
        private boolean stpGateRejectAll = false;
        /**
         * When {@code true} with {@code oms.routing.backend=fix}, rejects at control if {@code fix_route_state.send_enabled}
         * is false for {@code oms.fix.route-key} ({@link com.balh.oms.domain.RejectCode#RISK_MARKET_SESSION_CLOSED}).
         */
        private boolean fixRouteSendEnabledCheckEnabled = false;
        /**
         * When {@code true}, BUY orders where {@code positions.quantity_total + order.quantity} exceeds
         * {@link #maxAggregatePositionQuantity} reject with {@link com.balh.oms.domain.RejectCode#RISK_CONCENTRATION_LIMIT}.
         */
        private boolean maxAggregatePositionQuantityCheckEnabled = false;
        /** Max position quantity per account+symbol+custody (default omnibus); {@code 0} disables even when check enabled. */
        private java.math.BigDecimal maxAggregatePositionQuantity = java.math.BigDecimal.ZERO;
        /**
         * When {@code true}, SELL orders reject at control when {@code positions.quantity_total}
         * is below order quantity ({@link com.balh.oms.domain.RejectCode#RISK_INSUFFICIENT_POSITION}).
         */
        private boolean sellPositionCheckEnabled = true;
        /**
         * When {@code true}, ISK tax-wrapper accounts reject orders on instruments whose active
         * {@code instrument_settlement_profile.isk_eligible} is false
         * ({@link com.balh.oms.domain.RejectCode#RISK_ISK_INSTRUMENT_NOT_ELIGIBLE}).
         */
        private boolean iskInstrumentEligibilityCheckEnabled = false;
        /**
         * When {@code true}, an ISK tax-wrapper account BUY whose {@code order.ledgerBalanceId}
         * does not match the ISK's own ledger balance recorded in
         * {@code oms_account_tax_wrapper.ledger_balance_id} is rejected with
         * {@link com.balh.oms.domain.RejectCode#RISK_ISK_FUNDING_MISMATCH}. Gap plan §5.10 / I3.
         * Default off until the BFF picker is verified to always emit the ISK SEK cash balance
         * on ISK BUYs; flipping on prematurely would reject legitimate orders from older clients.
         */
        private boolean iskFundingCheckEnabled = false;

        public boolean isInstrumentAllowlistEnabled() { return instrumentAllowlistEnabled; }
        public void setInstrumentAllowlistEnabled(boolean v) { this.instrumentAllowlistEnabled = v; }
        public String getAllowedInstrumentSymbols() { return allowedInstrumentSymbols; }
        public void setAllowedInstrumentSymbols(String v) { this.allowedInstrumentSymbols = v == null ? "" : v; }

        public boolean isInstrumentTradabilityCheckEnabled() {
            return instrumentTradabilityCheckEnabled;
        }

        public void setInstrumentTradabilityCheckEnabled(boolean instrumentTradabilityCheckEnabled) {
            this.instrumentTradabilityCheckEnabled = instrumentTradabilityCheckEnabled;
        }

        public String getTradableInstrumentSymbols() {
            return tradableInstrumentSymbols;
        }

        public void setTradableInstrumentSymbols(String tradableInstrumentSymbols) {
            this.tradableInstrumentSymbols = tradableInstrumentSymbols == null ? "" : tradableInstrumentSymbols;
        }

        public boolean isInstrumentTradabilityFromMarketdataEnabled() {
            return instrumentTradabilityFromMarketdataEnabled;
        }

        public void setInstrumentTradabilityFromMarketdataEnabled(boolean instrumentTradabilityFromMarketdataEnabled) {
            this.instrumentTradabilityFromMarketdataEnabled = instrumentTradabilityFromMarketdataEnabled;
        }

        public boolean isInstrumentSymbolHaltCheckEnabled() {
            return instrumentSymbolHaltCheckEnabled;
        }

        public void setInstrumentSymbolHaltCheckEnabled(boolean instrumentSymbolHaltCheckEnabled) {
            this.instrumentSymbolHaltCheckEnabled = instrumentSymbolHaltCheckEnabled;
        }

        public String getHaltedInstrumentSymbols() {
            return haltedInstrumentSymbols;
        }

        public void setHaltedInstrumentSymbols(String haltedInstrumentSymbols) {
            this.haltedInstrumentSymbols = haltedInstrumentSymbols == null ? "" : haltedInstrumentSymbols;
        }

        /** Uppercased symbols from {@link #haltedInstrumentSymbols}, comma-separated. */
        public Set<String> haltedInstrumentSymbolSet() {
            if (!instrumentSymbolHaltCheckEnabled
                    || haltedInstrumentSymbols == null
                    || haltedInstrumentSymbols.isBlank()) {
                return Collections.emptySet();
            }
            return Arrays.stream(haltedInstrumentSymbols.split(","))
                    .map(s -> s.trim().toUpperCase(Locale.ROOT))
                    .filter(s -> !s.isEmpty())
                    .collect(Collectors.toUnmodifiableSet());
        }

        /** Uppercased symbols from {@link #tradableInstrumentSymbols}, comma-separated. */
        public Set<String> tradableInstrumentSymbolSet() {
            if (!instrumentTradabilityCheckEnabled
                    || tradableInstrumentSymbols == null
                    || tradableInstrumentSymbols.isBlank()) {
                return Collections.emptySet();
            }
            return Arrays.stream(tradableInstrumentSymbols.split(","))
                    .map(s -> s.trim().toUpperCase(Locale.ROOT))
                    .filter(s -> !s.isEmpty())
                    .collect(Collectors.toUnmodifiableSet());
        }

        public BigDecimal getFatFingerMaxLimitPrice() { return fatFingerMaxLimitPrice; }
        public void setFatFingerMaxLimitPrice(BigDecimal v) { this.fatFingerMaxLimitPrice = v == null ? BigDecimal.ZERO : v; }
        public BigDecimal getFatFingerMaxOrderQuantity() { return fatFingerMaxOrderQuantity; }
        public void setFatFingerMaxOrderQuantity(BigDecimal v) { this.fatFingerMaxOrderQuantity = v == null ? BigDecimal.ZERO : v; }
        public BigDecimal getMaxOrderNotional() { return maxOrderNotional; }
        public void setMaxOrderNotional(BigDecimal v) { this.maxOrderNotional = v == null ? BigDecimal.ZERO : v; }

        public boolean isSanctionsRecheckEnabled() {
            return sanctionsRecheckEnabled;
        }

        public void setSanctionsRecheckEnabled(boolean sanctionsRecheckEnabled) {
            this.sanctionsRecheckEnabled = sanctionsRecheckEnabled;
        }

        public boolean isSanctionsRecheckStrict() {
            return sanctionsRecheckStrict;
        }

        public void setSanctionsRecheckStrict(boolean sanctionsRecheckStrict) {
            this.sanctionsRecheckStrict = sanctionsRecheckStrict;
        }

        public long getSanctionsCacheMaxAgeSeconds() {
            return sanctionsCacheMaxAgeSeconds;
        }

        public void setSanctionsCacheMaxAgeSeconds(long sanctionsCacheMaxAgeSeconds) {
            this.sanctionsCacheMaxAgeSeconds = Math.max(0L, sanctionsCacheMaxAgeSeconds);
        }

        public long getOrderMinIntervalMsPerAccount() {
            return orderMinIntervalMsPerAccount;
        }

        public void setOrderMinIntervalMsPerAccount(long orderMinIntervalMsPerAccount) {
            this.orderMinIntervalMsPerAccount = Math.max(0L, orderMinIntervalMsPerAccount);
        }

        public boolean isTickSizeCheckEnabled() {
            return tickSizeCheckEnabled;
        }

        public void setTickSizeCheckEnabled(boolean tickSizeCheckEnabled) {
            this.tickSizeCheckEnabled = tickSizeCheckEnabled;
        }

        public java.math.BigDecimal getTickSizeIncrement() {
            return tickSizeIncrement;
        }

        public void setTickSizeIncrement(java.math.BigDecimal tickSizeIncrement) {
            this.tickSizeIncrement = tickSizeIncrement == null ? java.math.BigDecimal.ZERO : tickSizeIncrement;
        }

        public boolean isStpGateEnabled() {
            return stpGateEnabled;
        }

        public void setStpGateEnabled(boolean stpGateEnabled) {
            this.stpGateEnabled = stpGateEnabled;
        }

        public boolean isStpGateRejectAll() {
            return stpGateRejectAll;
        }

        public void setStpGateRejectAll(boolean stpGateRejectAll) {
            this.stpGateRejectAll = stpGateRejectAll;
        }

        public boolean isFixRouteSendEnabledCheckEnabled() {
            return fixRouteSendEnabledCheckEnabled;
        }

        public void setFixRouteSendEnabledCheckEnabled(boolean fixRouteSendEnabledCheckEnabled) {
            this.fixRouteSendEnabledCheckEnabled = fixRouteSendEnabledCheckEnabled;
        }

        public boolean isMaxAggregatePositionQuantityCheckEnabled() {
            return maxAggregatePositionQuantityCheckEnabled;
        }

        public void setMaxAggregatePositionQuantityCheckEnabled(boolean maxAggregatePositionQuantityCheckEnabled) {
            this.maxAggregatePositionQuantityCheckEnabled = maxAggregatePositionQuantityCheckEnabled;
        }

        public java.math.BigDecimal getMaxAggregatePositionQuantity() {
            return maxAggregatePositionQuantity;
        }

        public void setMaxAggregatePositionQuantity(java.math.BigDecimal maxAggregatePositionQuantity) {
            this.maxAggregatePositionQuantity =
                    maxAggregatePositionQuantity == null
                            ? java.math.BigDecimal.ZERO
                            : maxAggregatePositionQuantity.max(java.math.BigDecimal.ZERO);
        }

        public boolean isSellPositionCheckEnabled() {
            return sellPositionCheckEnabled;
        }

        public void setSellPositionCheckEnabled(boolean sellPositionCheckEnabled) {
            this.sellPositionCheckEnabled = sellPositionCheckEnabled;
        }

        public boolean isIskInstrumentEligibilityCheckEnabled() {
            return iskInstrumentEligibilityCheckEnabled;
        }

        public void setIskInstrumentEligibilityCheckEnabled(boolean iskInstrumentEligibilityCheckEnabled) {
            this.iskInstrumentEligibilityCheckEnabled = iskInstrumentEligibilityCheckEnabled;
        }

        public boolean isIskFundingCheckEnabled() {
            return iskFundingCheckEnabled;
        }

        public void setIskFundingCheckEnabled(boolean iskFundingCheckEnabled) {
            this.iskFundingCheckEnabled = iskFundingCheckEnabled;
        }

        /** Uppercased symbols from {@link #allowedInstrumentSymbols}, comma-separated. */
        public Set<String> allowedInstrumentSymbolSet() {
            if (!instrumentAllowlistEnabled || allowedInstrumentSymbols == null || allowedInstrumentSymbols.isBlank()) {
                return Collections.emptySet();
            }
            return Arrays.stream(allowedInstrumentSymbols.split(","))
                    .map(s -> s.trim().toUpperCase(Locale.ROOT))
                    .filter(s -> !s.isEmpty())
                    .collect(Collectors.toUnmodifiableSet());
        }
    }

    /**
     * Outbound routing / market-context evidence config. {@code backend=noop} is the default safe
     * mode; {@code fix} drives QuickFIX/J via the {@code oms-fix-egress} JVM (Phase 3 of the Aeron
     * Cluster substrate plan). The legacy {@code simulated} backend (along with {@code FixRouteDispatcher}
     * / {@code FixOutboundDispatchWorker} / {@code ExecutionReportApplier}) was removed in slice 3g.
     */
    public static class Routing {
        private String backend = "noop";
        private String marketContextStubJson = "{\"stub\":true}";
        private boolean nbboReferenceInMarketContextEnabled = false;
        private BigDecimal nbboStubBidPrice = BigDecimal.ZERO;
        private BigDecimal nbboStubAskPrice = BigDecimal.ZERO;
        /**
         * When {@code true}, {@code oms-venue-egress} routes only symbols matching
         * {@link #venueSymbolPrefix} and {@code oms-fix-egress} skips those symbols. Required when
         * both egress JVMs run on pop (equities → FIX, prediction markets → venue).
         */
        private boolean venueSymbolPrefixRoutingEnabled = false;
        private String venueSymbolPrefix = "PREDMKT";

        public String getBackend() { return backend; }
        public void setBackend(String v) { this.backend = v == null ? "noop" : v; }
        public String getMarketContextStubJson() { return marketContextStubJson; }
        public void setMarketContextStubJson(String v) {
            this.marketContextStubJson = v == null || v.isBlank() ? "{\"stub\":true}" : v;
        }

        /**
         * When {@code true} with positive {@link #getNbboStubBidPrice()} and {@link #getNbboStubAskPrice()}, each trade
         * apply merges an NBBO-class reference object into {@code market_context.snapshot_json} (slice 5 stub until
         * Marketdata Platform quotes are wired).
         */
        public boolean isNbboReferenceInMarketContextEnabled() {
            return nbboReferenceInMarketContextEnabled;
        }

        public void setNbboReferenceInMarketContextEnabled(boolean nbboReferenceInMarketContextEnabled) {
            this.nbboReferenceInMarketContextEnabled = nbboReferenceInMarketContextEnabled;
        }

        public BigDecimal getNbboStubBidPrice() {
            return nbboStubBidPrice;
        }

        public void setNbboStubBidPrice(BigDecimal nbboStubBidPrice) {
            this.nbboStubBidPrice = nbboStubBidPrice == null ? BigDecimal.ZERO : nbboStubBidPrice;
        }

        public BigDecimal getNbboStubAskPrice() {
            return nbboStubAskPrice;
        }

        public void setNbboStubAskPrice(BigDecimal nbboStubAskPrice) {
            this.nbboStubAskPrice = nbboStubAskPrice == null ? BigDecimal.ZERO : nbboStubAskPrice;
        }

        public boolean isVenueSymbolPrefixRoutingEnabled() {
            return venueSymbolPrefixRoutingEnabled;
        }

        public void setVenueSymbolPrefixRoutingEnabled(boolean venueSymbolPrefixRoutingEnabled) {
            this.venueSymbolPrefixRoutingEnabled = venueSymbolPrefixRoutingEnabled;
        }

        public String getVenueSymbolPrefix() {
            return venueSymbolPrefix;
        }

        public void setVenueSymbolPrefix(String venueSymbolPrefix) {
            this.venueSymbolPrefix =
                    venueSymbolPrefix == null || venueSymbolPrefix.isBlank()
                            ? "PREDMKT"
                            : venueSymbolPrefix.trim().toUpperCase(Locale.ROOT);
        }
    }

    /** gRPC client settings for {@code oms.routing.backend=internal-venue}. */
    public static class Venue {
        private static final long DEFAULT_RESOLUTION_DISPUTE_WINDOW_MS = 7_200_000L;

        private String grpcHost = "127.0.0.1";
        private int grpcPort = 50051;
        /** Blocking stub deadline for RouteOrder / RouteCancel / RouteReplace on oms-venue-egress. */
        private long grpcCallTimeoutMs = 15_000L;
        /**
         * Per-route ack deadline on the pipelined {@code RouteOrderStream} (demux by {@code oms_order_id}).
         * Align with venue gateway {@code BALH_VENUE_GRPC_ROUTE_TIMEOUT_S} (default 10s); shorter than
         * {@link #grpcCallTimeoutMs} so in-flight windows fail fast instead of piling up at 400+ routes/s.
         */
        private long grpcStreamAckTimeoutMs = 10_000L;
        private String venueId = "balh-internal-venue";
        /** Phase C: push catalog creates/updates to balh-venue registry over gRPC. */
        private boolean registrySyncEnabled = true;
        /**
         * On {@code oms-ingress-replica} / {@code oms-venue-egress} ready, sync all OPEN catalog rows
         * to the venue registry (retries until venue gRPC accepts or attempts exhaust).
         */
        private boolean registrySyncOnStartupEnabled = true;
        private int registrySyncOnStartupMaxAttempts = DEFAULT_REGISTRY_SYNC_ON_STARTUP_MAX_ATTEMPTS;
        private long registrySyncOnStartupRetryBackoffMs = DEFAULT_REGISTRY_SYNC_ON_STARTUP_RETRY_BACKOFF_MS;
        private long registrySyncOnStartupInitialDelayMs = DEFAULT_REGISTRY_SYNC_ON_STARTUP_INITIAL_DELAY_MS;

        private static final int DEFAULT_REGISTRY_SYNC_ON_STARTUP_MAX_ATTEMPTS = 12;
        private static final long DEFAULT_REGISTRY_SYNC_ON_STARTUP_RETRY_BACKOFF_MS = 5_000L;
        private static final long DEFAULT_REGISTRY_SYNC_ON_STARTUP_INITIAL_DELAY_MS = 2_000L;
        /** Phase B: hold Ledger posting until this window elapses (default 2h). */
        private long resolutionDisputeWindowMs = DEFAULT_RESOLUTION_DISPUTE_WINDOW_MS;
        /** Pre-admission circuit breaker: refuse venue-routed accepts when oms-venue-egress is behind. */
        private final AdmissionGate admissionGate = new AdmissionGate();

        public AdmissionGate getAdmissionGate() { return admissionGate; }

        public String getGrpcHost() { return grpcHost; }
        public void setGrpcHost(String grpcHost) {
            this.grpcHost = grpcHost == null || grpcHost.isBlank() ? "127.0.0.1" : grpcHost.trim();
        }

        public int getGrpcPort() { return grpcPort; }
        public void setGrpcPort(int grpcPort) {
            this.grpcPort = grpcPort <= 0 ? 50051 : grpcPort;
        }

        public long getGrpcCallTimeoutMs() {
            return grpcCallTimeoutMs;
        }

        public void setGrpcCallTimeoutMs(long grpcCallTimeoutMs) {
            this.grpcCallTimeoutMs = grpcCallTimeoutMs > 0 ? grpcCallTimeoutMs : 15_000L;
        }

        public long getGrpcStreamAckTimeoutMs() {
            return grpcStreamAckTimeoutMs;
        }

        public void setGrpcStreamAckTimeoutMs(long grpcStreamAckTimeoutMs) {
            this.grpcStreamAckTimeoutMs = grpcStreamAckTimeoutMs > 0 ? grpcStreamAckTimeoutMs : 10_000L;
        }

        public String getVenueId() { return venueId; }
        public void setVenueId(String venueId) {
            this.venueId = venueId == null || venueId.isBlank() ? "balh-internal-venue" : venueId.trim();
        }

        public boolean isRegistrySyncEnabled() { return registrySyncEnabled; }
        public void setRegistrySyncEnabled(boolean registrySyncEnabled) {
            this.registrySyncEnabled = registrySyncEnabled;
        }

        public boolean isRegistrySyncOnStartupEnabled() { return registrySyncOnStartupEnabled; }
        public void setRegistrySyncOnStartupEnabled(boolean registrySyncOnStartupEnabled) {
            this.registrySyncOnStartupEnabled = registrySyncOnStartupEnabled;
        }

        public int getRegistrySyncOnStartupMaxAttempts() { return registrySyncOnStartupMaxAttempts; }
        public void setRegistrySyncOnStartupMaxAttempts(int registrySyncOnStartupMaxAttempts) {
            this.registrySyncOnStartupMaxAttempts = Math.max(1, registrySyncOnStartupMaxAttempts);
        }

        public long getRegistrySyncOnStartupRetryBackoffMs() { return registrySyncOnStartupRetryBackoffMs; }
        public void setRegistrySyncOnStartupRetryBackoffMs(long registrySyncOnStartupRetryBackoffMs) {
            this.registrySyncOnStartupRetryBackoffMs = Math.max(1L, registrySyncOnStartupRetryBackoffMs);
        }

        public long getRegistrySyncOnStartupInitialDelayMs() { return registrySyncOnStartupInitialDelayMs; }
        public void setRegistrySyncOnStartupInitialDelayMs(long registrySyncOnStartupInitialDelayMs) {
            this.registrySyncOnStartupInitialDelayMs = Math.max(0L, registrySyncOnStartupInitialDelayMs);
        }

        public long getResolutionDisputeWindowMs() {
            return resolutionDisputeWindowMs;
        }

        public void setResolutionDisputeWindowMs(long resolutionDisputeWindowMs) {
            this.resolutionDisputeWindowMs =
                    resolutionDisputeWindowMs > 0
                            ? resolutionDisputeWindowMs
                            : DEFAULT_RESOLUTION_DISPUTE_WINDOW_MS;
        }

        /**
         * Venue-egress health gate evaluated on the order-accept path (HTTP + gRPC) for
         * venue-routed (e.g. {@code PREDMKT/*}) symbols only.
         *
         * <p><strong>Lag semantics (2026-06-05).</strong> Raw byte lag is
         * {@code projector_position − egress_position} on the shared cluster events stream. With
         * pipelined egress ({@code venue-route-max-in-flight > 1}) the projector legitimately leads
         * by up to {@link #pipelinedFloorBytes(int)} while ER offers drain — that window is
         * <em>not</em> actionable backlog. {@link #maxLagBytes} is the maximum <em>excess</em> lag
         * (raw minus the pipelined floor) before HTTP 503 {@code venue_unavailable}; between
         * {@link #throttleExcessLagBytes} and {@link #maxLagBytes} the gate applies a bounded
         * accept-path delay instead of shedding. Equities / FIX-routed flow is never gated. See
         * {@link com.balh.oms.ingress.VenueAdmissionGate} and
         * {@link com.balh.oms.ingress.OmsVenueEgressLagPublisher}.
         */
        public static class AdmissionGate {
            /** Max <em>excess</em> lag (beyond the pipelined floor) before hard 503. Serial egress uses this directly. */
            private static final long DEFAULT_MAX_LAG_BYTES = 4_096L;

            /** Begin soft throttling when excess lag exceeds this (defaults to half of {@link #maxLagBytes}). */
            private static final long DEFAULT_THROTTLE_EXCESS_LAG_BYTES = 2_048L;

            /** Base accept-path delay at the start of the soft-throttle band (nanos). */
            private static final long DEFAULT_THROTTLE_BASE_DELAY_NANOS = 500_000L;

            /** Cap accept-path delay in the soft-throttle band (nanos). */
            private static final long DEFAULT_MAX_THROTTLE_DELAY_NANOS = 25_000_000L;

            /**
             * Per in-flight pipelined admit, the projector can lead the egress cursor by roughly one
             * {@link com.balh.oms.cluster.OrderAdmittedEvent} plus the matching
             * {@link com.balh.oms.cluster.ExecutionAppliedEvent} before the egress ER offer lands.
             */
            private static final int BYTES_PER_PIPELINED_IN_FLIGHT_ORDER = 512;
            /**
             * Hysteresis for "projector present but egress cursor absent": require this many
             * consecutive poll snapshots before hard-blocking venue admits.
             */
            private static final int DEFAULT_MISSING_EGRESS_BLOCKED_POLLS = 3;
            /**
             * Time-based hysteresis companion for missing egress cursor. The gate blocks once this
             * grace window elapses even if poll cadence drifts and consecutive count is slower.
             */
            private static final long DEFAULT_MISSING_EGRESS_GRACE_MS = 15_000L;

            private boolean enabled = true;
            private long maxLagBytes = DEFAULT_MAX_LAG_BYTES;
            private long throttleExcessLagBytes = DEFAULT_THROTTLE_EXCESS_LAG_BYTES;
            private long throttleBaseDelayNanos = DEFAULT_THROTTLE_BASE_DELAY_NANOS;
            private long maxThrottleDelayNanos = DEFAULT_MAX_THROTTLE_DELAY_NANOS;
            private int missingEgressBlockedPolls = DEFAULT_MISSING_EGRESS_BLOCKED_POLLS;
            private long missingEgressGraceMs = DEFAULT_MISSING_EGRESS_GRACE_MS;

            public boolean isEnabled() { return enabled; }
            public void setEnabled(boolean enabled) { this.enabled = enabled; }

            public long getMaxLagBytes() { return maxLagBytes; }
            public void setMaxLagBytes(long maxLagBytes) {
                this.maxLagBytes = maxLagBytes > 0 ? maxLagBytes : DEFAULT_MAX_LAG_BYTES;
            }

            public long getThrottleExcessLagBytes() { return throttleExcessLagBytes; }
            public void setThrottleExcessLagBytes(long throttleExcessLagBytes) {
                this.throttleExcessLagBytes =
                        throttleExcessLagBytes > 0
                                ? throttleExcessLagBytes
                                : DEFAULT_THROTTLE_EXCESS_LAG_BYTES;
            }

            public long getThrottleBaseDelayNanos() { return throttleBaseDelayNanos; }
            public void setThrottleBaseDelayNanos(long throttleBaseDelayNanos) {
                this.throttleBaseDelayNanos =
                        throttleBaseDelayNanos > 0
                                ? throttleBaseDelayNanos
                                : DEFAULT_THROTTLE_BASE_DELAY_NANOS;
            }

            public long getMaxThrottleDelayNanos() { return maxThrottleDelayNanos; }
            public void setMaxThrottleDelayNanos(long maxThrottleDelayNanos) {
                this.maxThrottleDelayNanos =
                        maxThrottleDelayNanos > 0
                                ? maxThrottleDelayNanos
                                : DEFAULT_MAX_THROTTLE_DELAY_NANOS;
            }

            public int getMissingEgressBlockedPolls() { return missingEgressBlockedPolls; }
            public void setMissingEgressBlockedPolls(int missingEgressBlockedPolls) {
                this.missingEgressBlockedPolls =
                        missingEgressBlockedPolls > 0
                                ? missingEgressBlockedPolls
                                : DEFAULT_MISSING_EGRESS_BLOCKED_POLLS;
            }

            public long getMissingEgressGraceMs() { return missingEgressGraceMs; }
            public void setMissingEgressGraceMs(long missingEgressGraceMs) {
                this.missingEgressGraceMs =
                        missingEgressGraceMs >= 0
                                ? missingEgressGraceMs
                                : DEFAULT_MISSING_EGRESS_GRACE_MS;
            }

            public int getBytesPerPipelinedInFlightOrder() {
                return BYTES_PER_PIPELINED_IN_FLIGHT_ORDER;
            }

            /**
             * Healthy pipelined in-flight window subtracted from raw lag before gate/throttle
             * decisions. Zero for serial egress ({@code venue-route-max-in-flight <= 1}).
             */
            public long pipelinedFloorBytes(int venueRouteMaxInFlight) {
                if (venueRouteMaxInFlight <= 1) {
                    return 0L;
                }
                return (long) venueRouteMaxInFlight * BYTES_PER_PIPELINED_IN_FLIGHT_ORDER;
            }

            /**
             * Raw lag ceiling ({@code pipelinedFloor + maxLagBytes}) — the soak script {@code maxLagB}
             * compares against this when measuring total projector−egress bytes.
             */
            public long hardBlockRawLagBytes(int venueRouteMaxInFlight) {
                return pipelinedFloorBytes(venueRouteMaxInFlight) + getMaxLagBytes();
            }

            /**
             * @deprecated Prefer {@link #hardBlockRawLagBytes(int)} — name kept for call-site clarity.
             */
            @Deprecated
            public long effectiveMaxLagBytes(int venueRouteMaxInFlight) {
                return hardBlockRawLagBytes(venueRouteMaxInFlight);
            }

            /**
             * Bounded accept-path delay when {@code excessLag} sits in the soft-throttle band
             * ({@code throttleExcessLagBytes < excessLag <= maxLagBytes}).
             */
            public long throttleDelayNanos(long excessLagBytes) {
                if (excessLagBytes <= throttleExcessLagBytes || excessLagBytes > maxLagBytes) {
                    return 0L;
                }
                long span = maxLagBytes - throttleExcessLagBytes;
                if (span <= 0) {
                    return maxThrottleDelayNanos;
                }
                long over = excessLagBytes - throttleExcessLagBytes;
                long scaled = throttleBaseDelayNanos * over / span;
                return Math.min(scaled, maxThrottleDelayNanos);
            }
        }
    }

    /**
     * QuickFIX/J initiator / store paths when {@code oms.routing.backend=fix}.
     */
    public static class Fix {
        private boolean autoStart = false;
        private String fileStorePath = "./queues/fix";
        private String socketConnectHost = "127.0.0.1";
        private int socketConnectPort = 9876;
        private String senderCompId = "OMS_INIT";
        private String targetCompId = "BROKER_ACCEPT";
        private int heartBtInt = 30;
        /** Venue id stamped on {@code ExecutionTradeCommand} from inbound ERs. */
        private String venueIdForExecutions = "FIX";
        private boolean useDataDictionary = false;
        /**
         * Slice 4l: QuickFIX/J FileStore sync mode for {@link quickfix.FileStoreFactory}. Default
         * {@code Y} matches QuickFIX/J's documented default (fsync-per-message persistence on the
         * sender's outbound store, giving crash-recovery via the on-disk message stream alone).
         * {@code N} relies on FIX MsgSeqNum + broker resend on logon for crash recovery (still
         * loss-free at the protocol level) and removes the per-NOS fsync from the hot path. Slice
         * 4l hypothesis H1 measured this on Pop! and found the per-message fsync is NOT the
         * dominant per-event cost at Pop! storage speeds (\u224820 \u00b5s vs \u22481.4 ms/event total before
         * H2); see {@code docs/runbooks/local-multi-jvm-bench.md} \u201cSlice 4l evidence\u201d. We keep
         * the safe default and expose the knob for operators on slower storage.
         */
        private String fileStoreSync = "Y";
        /**
         * JSON object: OMS {@code instrument_symbol} (any case) → broker {@code Symbol} on outbound {@code NewOrderSingle}
         * (e.g. {@code {"AAPL":"AAPL.NMS"}}). {@code {}} or empty = identity mapping.
         */
        private String symbolMapJson = "{}";
        /** Logical FIX route key for {@code fix_route_state} (default single route). */
        private String routeKey = "default";
        /**
         * {@code file} — {@link quickfix.FileStoreFactory}; {@code jdbc} — {@link quickfix.JdbcStoreFactory}.
         * Default pool is the application {@link javax.sql.DataSource}; optional dedicated pool via
         * {@link #sessionJdbcDatasourceEnabled} / {@link #sessionJdbcUrl}.
         */
        private String sessionStoreType = "file";
        /** When {@code true}, {@link com.balh.oms.fix.FixRouteStateSodScheduler} sets {@code send_enabled} on all {@code fix_route_state} rows on a cron. */
        private boolean routeStateSodEnabled = false;
        /** Spring 6-field cron; only used when {@link #routeStateSodEnabled} (default from {@code application.yaml}). */
        private String routeStateSodCron = "";
        /**
         * {@code always} (default) — run SOD on every cron fire. {@code weekdays} — Mon–Fri only in
         * {@link #routeStateSodPolicyZoneId}. {@code region_calendar} — JSON in {@link #routeStateSodPolicyCalendarJson}.
         */
        private String routeStateSodPolicyMode = "always";
        /** IANA zone id for weekday / region calendar evaluation (default UTC). */
        private String routeStateSodPolicyZoneId = "UTC";
        /**
         * When {@link #routeStateSodPolicyMode} is {@code region_calendar}, JSON document with {@code activeRegionId}
         * and {@code regions} map (see {@link FixSodPolicyEngine}).
         */
        private String routeStateSodPolicyCalendarJson = "{}";
        /** QuickFIX/J initiator TLS (broker UAT / prod). */
        private boolean socketUseSsl = false;
        private String socketKeyStore = "";
        private String socketKeyStorePassword = "";
        private String socketTrustStore = "";
        private String socketTrustStorePassword = "";
        /** Optional; comma-separated protocols for QuickFIX {@code EnabledProtocols} (e.g. {@code TLSv1.2}). */
        private String enabledSslProtocols = "";
        /**
         * When {@code true} with {@link #sessionStoreType} {@code jdbc}, QuickFIX {@link quickfix.JdbcStoreFactory} uses a
         * dedicated pool ({@link #sessionJdbcUrl}) instead of the application {@link javax.sql.DataSource}.
         */
        private boolean sessionJdbcDatasourceEnabled = false;
        private String sessionJdbcUrl = "";
        private String sessionJdbcUser = "";
        private String sessionJdbcPassword = "";
        private int sessionJdbcPoolMaxSize = 5;
        private int sessionJdbcPoolMinIdle = 1;
        private long sessionJdbcConnectionTimeoutMs = 2000L;
        /** When {@code true}, initiator {@code onLogout} increments {@code oms_fix_mass_cancel_disconnect_signal_total}. */
        private boolean massCancelOnDisconnectEnabled = false;
        /**
         * When {@code true}, {@code POST /internal/v1/fix/mass-cancel-request} is allowed (signal-only unless wire flag is also on).
         */
        private boolean manualMassCancelEnabled = false;
        /**
         * When {@code true} with a logged-on FIX session, {@code OrderMassCancelRequest} may be sent (broker contract required).
         */
        private boolean manualMassCancelWireEnabled = false;
        /** Max UTF-16 length for optional {@code reason} on manual mass-cancel POST. */
        private int manualMassCancelReasonMaxChars = 512;
        public boolean isAutoStart() { return autoStart; }
        public void setAutoStart(boolean autoStart) { this.autoStart = autoStart; }
        public String getFileStorePath() { return fileStorePath; }
        public void setFileStorePath(String fileStorePath) { this.fileStorePath = fileStorePath == null ? "./queues/fix" : fileStorePath; }
        public String getSocketConnectHost() { return socketConnectHost; }
        public void setSocketConnectHost(String socketConnectHost) {
            this.socketConnectHost = socketConnectHost == null ? "127.0.0.1" : socketConnectHost;
        }
        public int getSocketConnectPort() { return socketConnectPort; }
        public void setSocketConnectPort(int socketConnectPort) { this.socketConnectPort = socketConnectPort; }
        public String getSenderCompId() { return senderCompId; }
        public void setSenderCompId(String senderCompId) { this.senderCompId = senderCompId == null ? "OMS_INIT" : senderCompId; }
        public String getTargetCompId() { return targetCompId; }
        public void setTargetCompId(String targetCompId) { this.targetCompId = targetCompId == null ? "BROKER_ACCEPT" : targetCompId; }
        public int getHeartBtInt() { return heartBtInt; }
        public void setHeartBtInt(int heartBtInt) { this.heartBtInt = Math.max(1, heartBtInt); }
        public String getVenueIdForExecutions() { return venueIdForExecutions; }
        public void setVenueIdForExecutions(String venueIdForExecutions) {
            this.venueIdForExecutions = venueIdForExecutions == null ? "FIX" : venueIdForExecutions;
        }
        public boolean isUseDataDictionary() { return useDataDictionary; }
        public void setUseDataDictionary(boolean useDataDictionary) { this.useDataDictionary = useDataDictionary; }

        public String getFileStoreSync() { return fileStoreSync; }
        public void setFileStoreSync(String fileStoreSync) {
            if (fileStoreSync == null) {
                this.fileStoreSync = "Y";
                return;
            }
            String trimmed = fileStoreSync.trim();
            if (trimmed.isEmpty()) {
                this.fileStoreSync = "Y";
                return;
            }
            // QuickFIX/J accepts only Y/N here. Reject anything else with a clear error rather than
            // silently coercing, since this is the on-disk durability flag.
            if (!"Y".equalsIgnoreCase(trimmed) && !"N".equalsIgnoreCase(trimmed)) {
                throw new IllegalArgumentException(
                        "oms.fix.file-store-sync must be 'Y' or 'N' (got '" + fileStoreSync + "')");
            }
            this.fileStoreSync = trimmed.toUpperCase(java.util.Locale.ROOT);
        }

        public String getSymbolMapJson() {
            return symbolMapJson;
        }

        public void setSymbolMapJson(String symbolMapJson) {
            this.symbolMapJson = symbolMapJson == null || symbolMapJson.isBlank() ? "{}" : symbolMapJson.trim();
        }

        public String getRouteKey() {
            return routeKey;
        }

        public void setRouteKey(String routeKey) {
            this.routeKey = routeKey == null || routeKey.isBlank() ? "default" : routeKey;
        }

        public String getSessionStoreType() {
            return sessionStoreType;
        }

        public void setSessionStoreType(String sessionStoreType) {
            this.sessionStoreType =
                    sessionStoreType == null || sessionStoreType.isBlank() ? "file" : sessionStoreType.trim();
        }

        public boolean isJdbcSessionStore() {
            return "jdbc".equalsIgnoreCase(sessionStoreType);
        }

        public boolean isRouteStateSodEnabled() {
            return routeStateSodEnabled;
        }

        public void setRouteStateSodEnabled(boolean routeStateSodEnabled) {
            this.routeStateSodEnabled = routeStateSodEnabled;
        }

        public String getRouteStateSodCron() {
            return routeStateSodCron;
        }

        public void setRouteStateSodCron(String routeStateSodCron) {
            this.routeStateSodCron = routeStateSodCron == null ? "" : routeStateSodCron.trim();
        }

        public String getRouteStateSodPolicyMode() {
            return routeStateSodPolicyMode;
        }

        public void setRouteStateSodPolicyMode(String routeStateSodPolicyMode) {
            this.routeStateSodPolicyMode =
                    routeStateSodPolicyMode == null || routeStateSodPolicyMode.isBlank()
                            ? "always"
                            : routeStateSodPolicyMode.trim().toLowerCase(Locale.ROOT);
        }

        public String getRouteStateSodPolicyZoneId() {
            return routeStateSodPolicyZoneId;
        }

        public void setRouteStateSodPolicyZoneId(String routeStateSodPolicyZoneId) {
            this.routeStateSodPolicyZoneId =
                    routeStateSodPolicyZoneId == null || routeStateSodPolicyZoneId.isBlank()
                            ? "UTC"
                            : routeStateSodPolicyZoneId.trim();
        }

        public String getRouteStateSodPolicyCalendarJson() {
            return routeStateSodPolicyCalendarJson;
        }

        public void setRouteStateSodPolicyCalendarJson(String routeStateSodPolicyCalendarJson) {
            this.routeStateSodPolicyCalendarJson =
                    routeStateSodPolicyCalendarJson == null || routeStateSodPolicyCalendarJson.isBlank()
                            ? "{}"
                            : routeStateSodPolicyCalendarJson.trim();
        }

        public boolean isSocketUseSsl() {
            return socketUseSsl;
        }

        public void setSocketUseSsl(boolean socketUseSsl) {
            this.socketUseSsl = socketUseSsl;
        }

        public String getSocketKeyStore() {
            return socketKeyStore;
        }

        public void setSocketKeyStore(String socketKeyStore) {
            this.socketKeyStore = socketKeyStore == null ? "" : socketKeyStore;
        }

        public String getSocketKeyStorePassword() {
            return socketKeyStorePassword;
        }

        public void setSocketKeyStorePassword(String socketKeyStorePassword) {
            this.socketKeyStorePassword = socketKeyStorePassword == null ? "" : socketKeyStorePassword;
        }

        public String getSocketTrustStore() {
            return socketTrustStore;
        }

        public void setSocketTrustStore(String socketTrustStore) {
            this.socketTrustStore = socketTrustStore == null ? "" : socketTrustStore;
        }

        public String getSocketTrustStorePassword() {
            return socketTrustStorePassword;
        }

        public void setSocketTrustStorePassword(String socketTrustStorePassword) {
            this.socketTrustStorePassword = socketTrustStorePassword == null ? "" : socketTrustStorePassword;
        }

        public String getEnabledSslProtocols() {
            return enabledSslProtocols;
        }

        public void setEnabledSslProtocols(String enabledSslProtocols) {
            this.enabledSslProtocols = enabledSslProtocols == null ? "" : enabledSslProtocols.trim();
        }

        public boolean isSessionJdbcDatasourceEnabled() {
            return sessionJdbcDatasourceEnabled;
        }

        public void setSessionJdbcDatasourceEnabled(boolean sessionJdbcDatasourceEnabled) {
            this.sessionJdbcDatasourceEnabled = sessionJdbcDatasourceEnabled;
        }

        public String getSessionJdbcUrl() {
            return sessionJdbcUrl;
        }

        public void setSessionJdbcUrl(String sessionJdbcUrl) {
            this.sessionJdbcUrl = sessionJdbcUrl == null ? "" : sessionJdbcUrl.trim();
        }

        public String getSessionJdbcUser() {
            return sessionJdbcUser;
        }

        public void setSessionJdbcUser(String sessionJdbcUser) {
            this.sessionJdbcUser = sessionJdbcUser == null ? "" : sessionJdbcUser.trim();
        }

        public String getSessionJdbcPassword() {
            return sessionJdbcPassword;
        }

        public void setSessionJdbcPassword(String sessionJdbcPassword) {
            this.sessionJdbcPassword = sessionJdbcPassword == null ? "" : sessionJdbcPassword;
        }

        public int getSessionJdbcPoolMaxSize() {
            return sessionJdbcPoolMaxSize;
        }

        public void setSessionJdbcPoolMaxSize(int sessionJdbcPoolMaxSize) {
            this.sessionJdbcPoolMaxSize = Math.max(1, sessionJdbcPoolMaxSize);
        }

        public int getSessionJdbcPoolMinIdle() {
            return sessionJdbcPoolMinIdle;
        }

        public void setSessionJdbcPoolMinIdle(int sessionJdbcPoolMinIdle) {
            this.sessionJdbcPoolMinIdle = Math.max(0, sessionJdbcPoolMinIdle);
        }

        public long getSessionJdbcConnectionTimeoutMs() {
            return sessionJdbcConnectionTimeoutMs;
        }

        public void setSessionJdbcConnectionTimeoutMs(long sessionJdbcConnectionTimeoutMs) {
            this.sessionJdbcConnectionTimeoutMs = Math.max(250L, sessionJdbcConnectionTimeoutMs);
        }

        public boolean isMassCancelOnDisconnectEnabled() {
            return massCancelOnDisconnectEnabled;
        }

        public void setMassCancelOnDisconnectEnabled(boolean massCancelOnDisconnectEnabled) {
            this.massCancelOnDisconnectEnabled = massCancelOnDisconnectEnabled;
        }

        public boolean isManualMassCancelEnabled() {
            return manualMassCancelEnabled;
        }

        public void setManualMassCancelEnabled(boolean manualMassCancelEnabled) {
            this.manualMassCancelEnabled = manualMassCancelEnabled;
        }

        public boolean isManualMassCancelWireEnabled() {
            return manualMassCancelWireEnabled;
        }

        public void setManualMassCancelWireEnabled(boolean manualMassCancelWireEnabled) {
            this.manualMassCancelWireEnabled = manualMassCancelWireEnabled;
        }

        public int getManualMassCancelReasonMaxChars() {
            return manualMassCancelReasonMaxChars;
        }

        public void setManualMassCancelReasonMaxChars(int manualMassCancelReasonMaxChars) {
            this.manualMassCancelReasonMaxChars = Math.min(4000, Math.max(32, manualMassCancelReasonMaxChars));
        }
    }

    /** QuickFIX/J acceptor for external FIX-in clients ({@code oms-fix-ingress} profile). */
    public static class FixIn {
        private boolean enabled = false;
        private boolean autoStart = false;
        private String bindHost = "0.0.0.0";
        private int acceptPort = 9877;
        /** Our acceptor {@code SenderCompID} on the wire (client's {@code TargetCompID}). */
        private String acceptorCompId = "BALH_OMS";
        private String fileStorePath = "./queues/fix-in";
        private String sessionStoreType = "file";
        private int heartBtInt = 30;
        private boolean useDataDictionary = false;
        private String symbolMapJson = "{}";
        private boolean requireLogonCredentials = false;
        private String fileStoreSync = "Y";
        private final ReturnPublisher returnPublisher = new ReturnPublisher();
        /** Reject app messages older than this (SendingTime vs wall clock). 0 = disabled. */
        private long maxMessageAgeMs = 120_000L;
        private int maxClientClOrdIdLength = 64;
        /** Per-session app-message rate limit (token bucket). 0 = disabled. */
        private int maxAppMessagesPerSecond = 200;
        /** Retention hint for {@code oms_fix_message_audit} (operator cleanup job). */
        private int messageAuditRetentionDays = 90;
        /**
         * When {@code true} with {@link #sessionStoreType} {@code jdbc}, QuickFIX {@link quickfix.JdbcStoreFactory}
         * uses a dedicated pool ({@link #sessionJdbcUrl}) instead of the application {@link javax.sql.DataSource}.
         */
        private boolean sessionJdbcDatasourceEnabled = false;
        private String sessionJdbcUrl = "";
        private String sessionJdbcUser = "";
        private String sessionJdbcPassword = "";
        private int sessionJdbcPoolMaxSize = 5;
        private int sessionJdbcPoolMinIdle = 1;
        private long sessionJdbcConnectionTimeoutMs = 2000L;

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public boolean isAutoStart() { return autoStart; }
        public void setAutoStart(boolean autoStart) { this.autoStart = autoStart; }
        public String getBindHost() { return bindHost == null ? "0.0.0.0" : bindHost; }
        public void setBindHost(String bindHost) { this.bindHost = bindHost; }
        public int getAcceptPort() { return acceptPort; }
        public void setAcceptPort(int acceptPort) { this.acceptPort = Math.max(1, acceptPort); }
        public String getAcceptorCompId() { return acceptorCompId == null ? "BALH_OMS" : acceptorCompId; }
        public void setAcceptorCompId(String acceptorCompId) { this.acceptorCompId = acceptorCompId; }
        public String getFileStorePath() { return fileStorePath == null ? "./queues/fix-in" : fileStorePath; }
        public void setFileStorePath(String fileStorePath) { this.fileStorePath = fileStorePath; }
        public String getSessionStoreType() { return sessionStoreType == null ? "file" : sessionStoreType; }
        public void setSessionStoreType(String sessionStoreType) { this.sessionStoreType = sessionStoreType; }
        public boolean isJdbcSessionStore() { return "jdbc".equalsIgnoreCase(sessionStoreType); }
        public int getHeartBtInt() { return heartBtInt; }
        public void setHeartBtInt(int heartBtInt) { this.heartBtInt = Math.max(1, heartBtInt); }
        public boolean isUseDataDictionary() { return useDataDictionary; }
        public void setUseDataDictionary(boolean useDataDictionary) { this.useDataDictionary = useDataDictionary; }
        public String getSymbolMapJson() { return symbolMapJson == null ? "{}" : symbolMapJson; }
        public void setSymbolMapJson(String symbolMapJson) { this.symbolMapJson = symbolMapJson; }
        public boolean isRequireLogonCredentials() { return requireLogonCredentials; }
        public void setRequireLogonCredentials(boolean requireLogonCredentials) {
            this.requireLogonCredentials = requireLogonCredentials;
        }
        public String getFileStoreSync() { return fileStoreSync == null ? "Y" : fileStoreSync; }
        public void setFileStoreSync(String fileStoreSync) { this.fileStoreSync = fileStoreSync; }
        public long getMaxMessageAgeMs() { return maxMessageAgeMs; }
        public void setMaxMessageAgeMs(long maxMessageAgeMs) { this.maxMessageAgeMs = Math.max(0L, maxMessageAgeMs); }
        public int getMaxClientClOrdIdLength() { return maxClientClOrdIdLength; }
        public void setMaxClientClOrdIdLength(int maxClientClOrdIdLength) {
            this.maxClientClOrdIdLength = Math.max(1, maxClientClOrdIdLength);
        }
        public int getMaxAppMessagesPerSecond() { return maxAppMessagesPerSecond; }
        public void setMaxAppMessagesPerSecond(int maxAppMessagesPerSecond) {
            this.maxAppMessagesPerSecond = Math.max(0, maxAppMessagesPerSecond);
        }
        public int getMessageAuditRetentionDays() { return messageAuditRetentionDays; }
        public void setMessageAuditRetentionDays(int messageAuditRetentionDays) {
            this.messageAuditRetentionDays = Math.max(1, messageAuditRetentionDays);
        }
        public boolean isSessionJdbcDatasourceEnabled() { return sessionJdbcDatasourceEnabled; }
        public void setSessionJdbcDatasourceEnabled(boolean sessionJdbcDatasourceEnabled) {
            this.sessionJdbcDatasourceEnabled = sessionJdbcDatasourceEnabled;
        }
        public String getSessionJdbcUrl() { return sessionJdbcUrl == null ? "" : sessionJdbcUrl; }
        public void setSessionJdbcUrl(String sessionJdbcUrl) { this.sessionJdbcUrl = sessionJdbcUrl; }
        public String getSessionJdbcUser() { return sessionJdbcUser == null ? "" : sessionJdbcUser; }
        public void setSessionJdbcUser(String sessionJdbcUser) { this.sessionJdbcUser = sessionJdbcUser; }
        public String getSessionJdbcPassword() { return sessionJdbcPassword == null ? "" : sessionJdbcPassword; }
        public void setSessionJdbcPassword(String sessionJdbcPassword) {
            this.sessionJdbcPassword = sessionJdbcPassword;
        }
        public int getSessionJdbcPoolMaxSize() { return sessionJdbcPoolMaxSize; }
        public void setSessionJdbcPoolMaxSize(int sessionJdbcPoolMaxSize) {
            this.sessionJdbcPoolMaxSize = Math.max(1, sessionJdbcPoolMaxSize);
        }
        public int getSessionJdbcPoolMinIdle() { return sessionJdbcPoolMinIdle; }
        public void setSessionJdbcPoolMinIdle(int sessionJdbcPoolMinIdle) {
            this.sessionJdbcPoolMinIdle = Math.max(0, sessionJdbcPoolMinIdle);
        }
        public long getSessionJdbcConnectionTimeoutMs() { return sessionJdbcConnectionTimeoutMs; }
        public void setSessionJdbcConnectionTimeoutMs(long sessionJdbcConnectionTimeoutMs) {
            this.sessionJdbcConnectionTimeoutMs = Math.max(500L, sessionJdbcConnectionTimeoutMs);
        }
        public ReturnPublisher getReturnPublisher() { return returnPublisher; }

        /** Cluster events replay → client FIX 8/9 ({@link com.balh.oms.fixin.OmsFixInReturnService}). */
        public static class ReturnPublisher {
            private static final long DEFAULT_RECORDING_LOOKUP_PARK_MS = 100L;

            private boolean enabled = true;
            private String aeronDirectory = "";
            private String archiveControlRequestChannel = "aeron:ipc?term-length=64k";
            private String archiveControlResponseChannel = "aeron:ipc?term-length=64k";
            private String replayChannel = "aeron:ipc?term-length=64k";
            private int replayStreamId = 4324;
            private long pollParkNanos = 1_000_000L;
            private long recordingLookupParkMs = DEFAULT_RECORDING_LOOKUP_PARK_MS;
            private int fragmentLimit = 64;

            public boolean isEnabled() { return enabled; }
            public void setEnabled(boolean enabled) { this.enabled = enabled; }
            public String getAeronDirectory() { return aeronDirectory == null ? "" : aeronDirectory; }
            public void setAeronDirectory(String aeronDirectory) { this.aeronDirectory = aeronDirectory; }
            public String getArchiveControlRequestChannel() { return archiveControlRequestChannel; }
            public void setArchiveControlRequestChannel(String archiveControlRequestChannel) {
                this.archiveControlRequestChannel = archiveControlRequestChannel;
            }
            public String getArchiveControlResponseChannel() { return archiveControlResponseChannel; }
            public void setArchiveControlResponseChannel(String archiveControlResponseChannel) {
                this.archiveControlResponseChannel = archiveControlResponseChannel;
            }
            public String getReplayChannel() { return replayChannel; }
            public void setReplayChannel(String replayChannel) { this.replayChannel = replayChannel; }
            public int getReplayStreamId() { return replayStreamId; }
            public void setReplayStreamId(int replayStreamId) { this.replayStreamId = replayStreamId; }
            public long getPollParkNanos() { return pollParkNanos; }
            public void setPollParkNanos(long pollParkNanos) { this.pollParkNanos = pollParkNanos; }
            public long getRecordingLookupParkMs() { return recordingLookupParkMs; }
            public void setRecordingLookupParkMs(long recordingLookupParkMs) {
                this.recordingLookupParkMs = Math.max(10L, recordingLookupParkMs);
            }
            public int getFragmentLimit() { return fragmentLimit; }
            public void setFragmentLimit(int fragmentLimit) { this.fragmentLimit = Math.max(1, fragmentLimit); }
        }
    }

    /**
     * Post-trade settlement / custody (slice 6). Defaults align with Flyway {@code V11__settlement_positions.sql}.
     */
    public static class Settlement {
        private String defaultCustodyAccountId = "a0000001-0000-4000-8000-000000000001";
        /**
         * Default market code for stocks when no instrument table is available.
         * Selects the commission schedule and trade currency used by the Ledger
         * settlement outbox (see {@link com.balh.oms.settlement.StockCommissionCalculator}).
         * One of: {@code US}, {@code EU}, {@code UK}.
         */
        private String defaultInstrumentMarket = "US";
        /**
         * Default cash currency for the customer's settlement cash leg when not
         * threaded through from order placement. Phase 1 assumes
         * {@code cashCurrency == tradeCurrency} so the cash leg is a single
         * Ledger transaction; when they differ, Phase 2 splits via {@code @FX-Suspense}.
         */
        private String defaultCashCurrency = "USD";
        private boolean brokerConfirmReconcilerEnabled = false;
        private long brokerConfirmReconcilerIntervalMs = 10_000L;
        private int brokerConfirmReconcilerBatchSize = 50;
        /**
         * When {@code true}, {@link BrokerTradeConfirmBatchLifecycleService} runs immediately after
         * a v2 economic confirm file reaches {@code parsed} (HTTP ingest path).
         */
        private boolean brokerConfirmMatchOnIngestEnabled = false;
        /**
         * When {@code true}, {@link BrokerTradeConfirmMatchScheduler} advances {@code parsed}
         * {@code broker_confirm_batch} rows through matching to {@code applied}.
         */
        private boolean brokerTradeConfirmMatcherSchedulerEnabled = false;
        private long brokerTradeConfirmMatcherIntervalMs = 10_000L;
        /** Max parsed batches the matcher scheduler picks up per tick. */
        private int brokerTradeConfirmMatcherSchedulerBatchSize = 10;
        /** Absolute tolerance when comparing broker {@code grossAmount} to {@code quantity × price}. */
        private java.math.BigDecimal brokerConfirmGrossAmountTolerance = new java.math.BigDecimal("0.01");
        private int brokerConfirmHttpMaxExecutionIds = 100;
        /** Max bytes accepted for {@code POST …/settlement/file-import} body (before SHA + parse). */
        private long fileImportMaxBytes = 10_000_000L;
        /** Max broker fixture rows read from one file (beyond this are counted as skipped-invalid in ingest). */
        private int fileImportMaxRows = 10_000;
        /** Max execution ids registered per DB transaction during file ingest slices. */
        private int fileImportRegisterSliceSize = 50;
        /** Truncation bound for {@code settlement_file_import_batch.error_summary}. */
        private int fileImportErrorSummaryMaxChars = 2000;
        /** Max rows returned by {@code GET /internal/v1/settlement/file-import-batches}. */
        private int fileImportListMaxLimit = 200;
        /** Default page size for file import batch list. */
        private int fileImportListDefaultLimit = 50;
        /** Max rows returned by {@code GET /internal/v1/settlement/reconciliation-breaks/export}. */
        private int reconciliationBreakExportMaxRows = 2_000;
        /** Max chars for reconciliation break workflow notes (assign / resolve / waive). */
        private int reconciliationBreakNoteMaxChars = 2_000;
        /** Max rows returned by {@code GET /internal/v1/settlement/manual-actions}. */
        private int manualActionListMaxLimit = 200;
        /** Default page size for manual settlement action list. */
        private int manualActionListDefaultLimit = 50;
        /** Max {@code action_type} length (UTF-16 code units). */
        private int manualActionTypeMaxLength = 128;
        /** Max JSON text length accepted for {@code payload_json} on create. */
        private int manualActionPayloadJsonMaxChars = 20_000;
        /**
         * When {@code true} (default), {@code POST …/manual-actions/{id}/approve} runs supported
         * {@code action_type} handlers in the same transaction as the approve CAS (see {@link ManualSettlementActionTypes}).
         */
        private boolean manualActionAutoApplyEnabled = true;
        /** When true, {@link com.balh.oms.settlement.SettlementFileDropFolderIngestScheduler} ingests {@code .json} files. */
        private boolean fileImportDropFolderEnabled = false;
        /** Absolute or relative path watched for incoming broker JSON files (see docs/broker-eod-file-contract.md). */
        private String fileImportDropFolderPath = "";
        private long fileImportDropFolderPollIntervalMs = 30_000L;
        private int fileImportDropFolderMaxFilesPerPoll = 20;
        /**
         * When {@code true}, BUY {@code TRADE} fills may append prior unsettled BUY execution ids to
         * {@code executions.unsettled_funded_by_exec_ids} (stub attribution — finance replaces rules later).
         */
        private boolean freeRidingAttributionEnabled = false;
        /** Max prior execution ids merged into {@code unsettled_funded_by_exec_ids} per fill. */
        private int freeRidingAttributionMaxFundingExecutions = 8;
        /**
         * Demo / dev: when {@code true},
         * {@link com.balh.oms.settlement.SettlementAutoStepScheduler} periodically advances any
         * TRADE execution still in a non-terminal {@code settlement_status} one step forward
         * (executed → matched → confirmed → settling → settled). Off by default so production
         * stays driven by real broker confirmation files. Only intended for environments where
         * the broker pipe is the {@code FixRoundTripAcceptorApplication} simulator and no
         * broker EOD file ever arrives.
         */
        private boolean autoStepSchedulerEnabled = false;
        /** Tick interval for the auto-step scheduler. Default 5s gives a visible per-state demo cadence. */
        private long autoStepSchedulerIntervalMs = 5_000L;
        /** Per-tick batch size for the auto-step scheduler. */
        private int autoStepSchedulerBatchSize = 50;
        /**
         * Upper bound on {@code NOW() - created_at} for executions the auto-step scheduler will pick up.
         * Stops the scheduler from churning over stale rows from prior runs. Default 1h.
         */
        private long autoStepSchedulerMaxExecutionAgeSeconds = 3600L;
        /**
         * After this many consecutive {@link com.balh.oms.settlement.SettlementAutoStepScheduler} advance
         * failures, the scheduler calls {@link com.balh.oms.settlement.SettlementConfirmProcessor#markTradeFailed}
         * once and stops selecting the execution until an operator resets it.
         */
        private int autoStepSchedulerMaxAdvanceFailures = 5;
        /**
         * When {@code true}, {@link SettlementOpsMetricsPublisher} polls Postgres for settlement
         * ops gauges (gap plan §5.18).
         */
        private boolean opsMetricsPublisherEnabled = true;
        /** Poll interval for {@link SettlementOpsMetricsPublisher}. */
        private long opsMetricsPollIntervalMs = 30_000L;
        /**
         * Minimum {@code ledger_settlement_outbox.attempts} before a row counts as stuck in
         * {@code oms_ledger_settlement_outbox_stuck_total} (matches beard-admin stuck-outbox default).
         */
        private int stuckOutboxMinAttempts = 3;
        private boolean failCustomerNotificationEnabled = false;
        private long failCustomerNotificationIntervalMs = 3_600_000L;
        private int failCustomerNotificationSlaBusinessDays = 3;
        /** Drains {@code settlement_customer_notification_outbox} to NATS for the customer BFF. */
        private boolean customerNotificationPublisherEnabled = false;
        private long customerNotificationPublisherIntervalMs = 1_000L;
        private int customerNotificationPublisherBatchSize = 50;
        private long customerNotificationPublisherAgeMs = 2_000L;
        private String customerNotificationStreamName = "OMS_CUSTOMER_NOTIFICATIONS";
        private String customerNotificationSubjectPrefix = "oms.customer-notifications";
        /** Enqueue {@code CorporateActionDividendPaid} after dividend ledger leg posts. */
        private boolean corporateActionDividendNotificationEnabled = true;
        /** When true, {@link SettlementDailyCloseJob} runs post-EOD reconcile orchestration. */
        private boolean dailyCloseEnabled = false;
        /** Cron for {@link SettlementDailyCloseJob} (Spring {@code @Scheduled}). */
        private String dailyCloseCron = "0 0 22 * * *";
        /** Lookback window for parsed batches eligible for daily close. */
        private long dailyCloseLookbackHours = 48L;
        /** Max batches per daily-close step per tick. */
        private int dailyCloseBatchLimit = 20;
        /**
         * Ledger nostro indicator template for cash recon; {@code {CCY}} is replaced with batch currency
         * (default {@code @Nostro-{CCY}-Bank} matches pop seed-fx-nostros).
         */
        private String nostroIndicatorTemplate = "@Nostro-{CCY}-Bank";

        public String getDefaultCustodyAccountId() {
            return defaultCustodyAccountId;
        }

        public void setDefaultCustodyAccountId(String defaultCustodyAccountId) {
            String trimmed = defaultCustodyAccountId == null || defaultCustodyAccountId.isBlank()
                    ? "a0000001-0000-4000-8000-000000000001"
                    : defaultCustodyAccountId.trim();
            UUID.fromString(trimmed);
            this.defaultCustodyAccountId = trimmed;
        }

        public String getDefaultInstrumentMarket() {
            return defaultInstrumentMarket;
        }

        public void setDefaultInstrumentMarket(String defaultInstrumentMarket) {
            String m = defaultInstrumentMarket == null || defaultInstrumentMarket.isBlank()
                    ? "US"
                    : defaultInstrumentMarket.trim().toUpperCase();
            this.defaultInstrumentMarket = m;
        }

        public String getDefaultCashCurrency() {
            return defaultCashCurrency;
        }

        public void setDefaultCashCurrency(String defaultCashCurrency) {
            String c = defaultCashCurrency == null || defaultCashCurrency.isBlank()
                    ? "USD"
                    : defaultCashCurrency.trim().toUpperCase();
            this.defaultCashCurrency = c;
        }

        public boolean isBrokerConfirmReconcilerEnabled() {
            return brokerConfirmReconcilerEnabled;
        }

        public void setBrokerConfirmReconcilerEnabled(boolean brokerConfirmReconcilerEnabled) {
            this.brokerConfirmReconcilerEnabled = brokerConfirmReconcilerEnabled;
        }

        public long getBrokerConfirmReconcilerIntervalMs() {
            return brokerConfirmReconcilerIntervalMs;
        }

        public void setBrokerConfirmReconcilerIntervalMs(long brokerConfirmReconcilerIntervalMs) {
            this.brokerConfirmReconcilerIntervalMs = Math.max(100L, brokerConfirmReconcilerIntervalMs);
        }

        public int getBrokerConfirmReconcilerBatchSize() {
            return brokerConfirmReconcilerBatchSize;
        }

        public void setBrokerConfirmReconcilerBatchSize(int brokerConfirmReconcilerBatchSize) {
            this.brokerConfirmReconcilerBatchSize = Math.max(1, brokerConfirmReconcilerBatchSize);
        }

        public boolean isBrokerConfirmMatchOnIngestEnabled() {
            return brokerConfirmMatchOnIngestEnabled;
        }

        public void setBrokerConfirmMatchOnIngestEnabled(boolean brokerConfirmMatchOnIngestEnabled) {
            this.brokerConfirmMatchOnIngestEnabled = brokerConfirmMatchOnIngestEnabled;
        }

        public boolean isBrokerTradeConfirmMatcherSchedulerEnabled() {
            return brokerTradeConfirmMatcherSchedulerEnabled;
        }

        public void setBrokerTradeConfirmMatcherSchedulerEnabled(boolean brokerTradeConfirmMatcherSchedulerEnabled) {
            this.brokerTradeConfirmMatcherSchedulerEnabled = brokerTradeConfirmMatcherSchedulerEnabled;
        }

        public long getBrokerTradeConfirmMatcherIntervalMs() {
            return brokerTradeConfirmMatcherIntervalMs;
        }

        public void setBrokerTradeConfirmMatcherIntervalMs(long brokerTradeConfirmMatcherIntervalMs) {
            this.brokerTradeConfirmMatcherIntervalMs = Math.max(100L, brokerTradeConfirmMatcherIntervalMs);
        }

        public int getBrokerTradeConfirmMatcherSchedulerBatchSize() {
            return brokerTradeConfirmMatcherSchedulerBatchSize;
        }

        public void setBrokerTradeConfirmMatcherSchedulerBatchSize(int brokerTradeConfirmMatcherSchedulerBatchSize) {
            this.brokerTradeConfirmMatcherSchedulerBatchSize = Math.max(1, brokerTradeConfirmMatcherSchedulerBatchSize);
        }

        public java.math.BigDecimal getBrokerConfirmGrossAmountTolerance() {
            return brokerConfirmGrossAmountTolerance;
        }

        public void setBrokerConfirmGrossAmountTolerance(java.math.BigDecimal brokerConfirmGrossAmountTolerance) {
            if (brokerConfirmGrossAmountTolerance == null
                    || brokerConfirmGrossAmountTolerance.signum() < 0) {
                this.brokerConfirmGrossAmountTolerance = new java.math.BigDecimal("0.01");
            } else {
                this.brokerConfirmGrossAmountTolerance = brokerConfirmGrossAmountTolerance;
            }
        }

        public int getBrokerConfirmHttpMaxExecutionIds() {
            return brokerConfirmHttpMaxExecutionIds;
        }

        public void setBrokerConfirmHttpMaxExecutionIds(int brokerConfirmHttpMaxExecutionIds) {
            this.brokerConfirmHttpMaxExecutionIds = Math.min(10_000, Math.max(1, brokerConfirmHttpMaxExecutionIds));
        }

        public long getFileImportMaxBytes() {
            return fileImportMaxBytes;
        }

        public void setFileImportMaxBytes(long fileImportMaxBytes) {
            this.fileImportMaxBytes = Math.min(100_000_000L, Math.max(1024L, fileImportMaxBytes));
        }

        public int getFileImportMaxRows() {
            return fileImportMaxRows;
        }

        public void setFileImportMaxRows(int fileImportMaxRows) {
            this.fileImportMaxRows = Math.min(500_000, Math.max(1, fileImportMaxRows));
        }

        public int getFileImportRegisterSliceSize() {
            return fileImportRegisterSliceSize;
        }

        public void setFileImportRegisterSliceSize(int fileImportRegisterSliceSize) {
            this.fileImportRegisterSliceSize = Math.min(10_000, Math.max(1, fileImportRegisterSliceSize));
        }

        public int getFileImportErrorSummaryMaxChars() {
            return fileImportErrorSummaryMaxChars;
        }

        public void setFileImportErrorSummaryMaxChars(int fileImportErrorSummaryMaxChars) {
            this.fileImportErrorSummaryMaxChars = Math.min(50_000, Math.max(256, fileImportErrorSummaryMaxChars));
        }

        public int getFileImportListMaxLimit() {
            return fileImportListMaxLimit;
        }

        public void setFileImportListMaxLimit(int fileImportListMaxLimit) {
            this.fileImportListMaxLimit = Math.min(500, Math.max(1, fileImportListMaxLimit));
        }

        public int getFileImportListDefaultLimit() {
            return fileImportListDefaultLimit;
        }

        public void setFileImportListDefaultLimit(int fileImportListDefaultLimit) {
            this.fileImportListDefaultLimit = Math.min(getFileImportListMaxLimit(), Math.max(1, fileImportListDefaultLimit));
        }

        public int getReconciliationBreakExportMaxRows() {
            return reconciliationBreakExportMaxRows;
        }

        public void setReconciliationBreakExportMaxRows(int reconciliationBreakExportMaxRows) {
            this.reconciliationBreakExportMaxRows = Math.min(10_000, Math.max(1, reconciliationBreakExportMaxRows));
        }

        public int getReconciliationBreakNoteMaxChars() {
            return reconciliationBreakNoteMaxChars;
        }

        public void setReconciliationBreakNoteMaxChars(int reconciliationBreakNoteMaxChars) {
            this.reconciliationBreakNoteMaxChars = Math.min(10_000, Math.max(64, reconciliationBreakNoteMaxChars));
        }

        public int getManualActionListMaxLimit() {
            return manualActionListMaxLimit;
        }

        public void setManualActionListMaxLimit(int manualActionListMaxLimit) {
            this.manualActionListMaxLimit = Math.min(500, Math.max(1, manualActionListMaxLimit));
        }

        public int getManualActionListDefaultLimit() {
            return manualActionListDefaultLimit;
        }

        public void setManualActionListDefaultLimit(int manualActionListDefaultLimit) {
            this.manualActionListDefaultLimit = Math.min(getManualActionListMaxLimit(), Math.max(1, manualActionListDefaultLimit));
        }

        public int getManualActionTypeMaxLength() {
            return manualActionTypeMaxLength;
        }

        public void setManualActionTypeMaxLength(int manualActionTypeMaxLength) {
            this.manualActionTypeMaxLength = Math.min(512, Math.max(1, manualActionTypeMaxLength));
        }

        public int getManualActionPayloadJsonMaxChars() {
            return manualActionPayloadJsonMaxChars;
        }

        public void setManualActionPayloadJsonMaxChars(int manualActionPayloadJsonMaxChars) {
            this.manualActionPayloadJsonMaxChars = Math.min(500_000, Math.max(256, manualActionPayloadJsonMaxChars));
        }

        public boolean isManualActionAutoApplyEnabled() {
            return manualActionAutoApplyEnabled;
        }

        public void setManualActionAutoApplyEnabled(boolean manualActionAutoApplyEnabled) {
            this.manualActionAutoApplyEnabled = manualActionAutoApplyEnabled;
        }

        public boolean isFileImportDropFolderEnabled() {
            return fileImportDropFolderEnabled;
        }

        public void setFileImportDropFolderEnabled(boolean fileImportDropFolderEnabled) {
            this.fileImportDropFolderEnabled = fileImportDropFolderEnabled;
        }

        public String getFileImportDropFolderPath() {
            return fileImportDropFolderPath;
        }

        public void setFileImportDropFolderPath(String fileImportDropFolderPath) {
            this.fileImportDropFolderPath = fileImportDropFolderPath == null ? "" : fileImportDropFolderPath.trim();
        }

        public long getFileImportDropFolderPollIntervalMs() {
            return fileImportDropFolderPollIntervalMs;
        }

        public void setFileImportDropFolderPollIntervalMs(long fileImportDropFolderPollIntervalMs) {
            this.fileImportDropFolderPollIntervalMs = Math.max(5_000L, fileImportDropFolderPollIntervalMs);
        }

        public int getFileImportDropFolderMaxFilesPerPoll() {
            return fileImportDropFolderMaxFilesPerPoll;
        }

        public void setFileImportDropFolderMaxFilesPerPoll(int fileImportDropFolderMaxFilesPerPoll) {
            this.fileImportDropFolderMaxFilesPerPoll = Math.min(500, Math.max(1, fileImportDropFolderMaxFilesPerPoll));
        }

        public boolean isFreeRidingAttributionEnabled() {
            return freeRidingAttributionEnabled;
        }

        public void setFreeRidingAttributionEnabled(boolean freeRidingAttributionEnabled) {
            this.freeRidingAttributionEnabled = freeRidingAttributionEnabled;
        }

        public int getFreeRidingAttributionMaxFundingExecutions() {
            return freeRidingAttributionMaxFundingExecutions;
        }

        public void setFreeRidingAttributionMaxFundingExecutions(int freeRidingAttributionMaxFundingExecutions) {
            this.freeRidingAttributionMaxFundingExecutions = Math.min(256, Math.max(1, freeRidingAttributionMaxFundingExecutions));
        }

        public boolean isAutoStepSchedulerEnabled() {
            return autoStepSchedulerEnabled;
        }

        public void setAutoStepSchedulerEnabled(boolean autoStepSchedulerEnabled) {
            this.autoStepSchedulerEnabled = autoStepSchedulerEnabled;
        }

        public long getAutoStepSchedulerIntervalMs() {
            return autoStepSchedulerIntervalMs;
        }

        public void setAutoStepSchedulerIntervalMs(long autoStepSchedulerIntervalMs) {
            this.autoStepSchedulerIntervalMs = Math.max(100L, autoStepSchedulerIntervalMs);
        }

        public int getAutoStepSchedulerBatchSize() {
            return autoStepSchedulerBatchSize;
        }

        public void setAutoStepSchedulerBatchSize(int autoStepSchedulerBatchSize) {
            this.autoStepSchedulerBatchSize = Math.max(1, Math.min(500, autoStepSchedulerBatchSize));
        }

        public long getAutoStepSchedulerMaxExecutionAgeSeconds() {
            return autoStepSchedulerMaxExecutionAgeSeconds;
        }

        public void setAutoStepSchedulerMaxExecutionAgeSeconds(long autoStepSchedulerMaxExecutionAgeSeconds) {
            this.autoStepSchedulerMaxExecutionAgeSeconds = Math.max(1L, autoStepSchedulerMaxExecutionAgeSeconds);
        }

        public int getAutoStepSchedulerMaxAdvanceFailures() {
            return autoStepSchedulerMaxAdvanceFailures;
        }

        public void setAutoStepSchedulerMaxAdvanceFailures(int autoStepSchedulerMaxAdvanceFailures) {
            this.autoStepSchedulerMaxAdvanceFailures = Math.max(1, autoStepSchedulerMaxAdvanceFailures);
        }

        public boolean isOpsMetricsPublisherEnabled() {
            return opsMetricsPublisherEnabled;
        }

        public void setOpsMetricsPublisherEnabled(boolean opsMetricsPublisherEnabled) {
            this.opsMetricsPublisherEnabled = opsMetricsPublisherEnabled;
        }

        public long getOpsMetricsPollIntervalMs() {
            return opsMetricsPollIntervalMs;
        }

        public void setOpsMetricsPollIntervalMs(long opsMetricsPollIntervalMs) {
            this.opsMetricsPollIntervalMs = Math.max(5_000L, opsMetricsPollIntervalMs);
        }

        public int getStuckOutboxMinAttempts() {
            return stuckOutboxMinAttempts;
        }

        public void setStuckOutboxMinAttempts(int stuckOutboxMinAttempts) {
            this.stuckOutboxMinAttempts = Math.max(1, stuckOutboxMinAttempts);
        }

        public boolean isFailCustomerNotificationEnabled() {
            return failCustomerNotificationEnabled;
        }

        public void setFailCustomerNotificationEnabled(boolean failCustomerNotificationEnabled) {
            this.failCustomerNotificationEnabled = failCustomerNotificationEnabled;
        }

        public long getFailCustomerNotificationIntervalMs() {
            return failCustomerNotificationIntervalMs;
        }

        public void setFailCustomerNotificationIntervalMs(long failCustomerNotificationIntervalMs) {
            this.failCustomerNotificationIntervalMs = Math.max(60_000L, failCustomerNotificationIntervalMs);
        }

        public int getFailCustomerNotificationSlaBusinessDays() {
            return failCustomerNotificationSlaBusinessDays;
        }

        public void setFailCustomerNotificationSlaBusinessDays(int failCustomerNotificationSlaBusinessDays) {
            this.failCustomerNotificationSlaBusinessDays = Math.max(1, failCustomerNotificationSlaBusinessDays);
        }

        public boolean isCustomerNotificationPublisherEnabled() {
            return customerNotificationPublisherEnabled;
        }

        public void setCustomerNotificationPublisherEnabled(boolean customerNotificationPublisherEnabled) {
            this.customerNotificationPublisherEnabled = customerNotificationPublisherEnabled;
        }

        public long getCustomerNotificationPublisherIntervalMs() {
            return customerNotificationPublisherIntervalMs;
        }

        public void setCustomerNotificationPublisherIntervalMs(long customerNotificationPublisherIntervalMs) {
            this.customerNotificationPublisherIntervalMs = Math.max(100L, customerNotificationPublisherIntervalMs);
        }

        public int getCustomerNotificationPublisherBatchSize() {
            return customerNotificationPublisherBatchSize;
        }

        public void setCustomerNotificationPublisherBatchSize(int customerNotificationPublisherBatchSize) {
            this.customerNotificationPublisherBatchSize = Math.max(1, customerNotificationPublisherBatchSize);
        }

        public long getCustomerNotificationPublisherAgeMs() {
            return customerNotificationPublisherAgeMs;
        }

        public void setCustomerNotificationPublisherAgeMs(long customerNotificationPublisherAgeMs) {
            this.customerNotificationPublisherAgeMs = Math.max(0L, customerNotificationPublisherAgeMs);
        }

        public String getCustomerNotificationStreamName() {
            return customerNotificationStreamName;
        }

        public void setCustomerNotificationStreamName(String customerNotificationStreamName) {
            this.customerNotificationStreamName =
                    customerNotificationStreamName == null || customerNotificationStreamName.isBlank()
                            ? "OMS_CUSTOMER_NOTIFICATIONS"
                            : customerNotificationStreamName.trim();
        }

        public String getCustomerNotificationSubjectPrefix() {
            return customerNotificationSubjectPrefix;
        }

        public void setCustomerNotificationSubjectPrefix(String customerNotificationSubjectPrefix) {
            this.customerNotificationSubjectPrefix =
                    customerNotificationSubjectPrefix == null || customerNotificationSubjectPrefix.isBlank()
                            ? "oms.customer-notifications"
                            : customerNotificationSubjectPrefix.trim();
        }

        public boolean isCorporateActionDividendNotificationEnabled() {
            return corporateActionDividendNotificationEnabled;
        }

        public void setCorporateActionDividendNotificationEnabled(boolean corporateActionDividendNotificationEnabled) {
            this.corporateActionDividendNotificationEnabled = corporateActionDividendNotificationEnabled;
        }

        public boolean isDailyCloseEnabled() {
            return dailyCloseEnabled;
        }

        public void setDailyCloseEnabled(boolean dailyCloseEnabled) {
            this.dailyCloseEnabled = dailyCloseEnabled;
        }

        public String getDailyCloseCron() {
            return dailyCloseCron;
        }

        public void setDailyCloseCron(String dailyCloseCron) {
            this.dailyCloseCron = dailyCloseCron == null || dailyCloseCron.isBlank()
                    ? "0 0 22 * * *"
                    : dailyCloseCron.trim();
        }

        public long getDailyCloseLookbackHours() {
            return dailyCloseLookbackHours;
        }

        public void setDailyCloseLookbackHours(long dailyCloseLookbackHours) {
            this.dailyCloseLookbackHours = Math.max(1L, dailyCloseLookbackHours);
        }

        public int getDailyCloseBatchLimit() {
            return dailyCloseBatchLimit;
        }

        public void setDailyCloseBatchLimit(int dailyCloseBatchLimit) {
            this.dailyCloseBatchLimit = Math.min(500, Math.max(1, dailyCloseBatchLimit));
        }

        public String getNostroIndicatorTemplate() {
            return nostroIndicatorTemplate;
        }

        public void setNostroIndicatorTemplate(String nostroIndicatorTemplate) {
            this.nostroIndicatorTemplate = nostroIndicatorTemplate == null || nostroIndicatorTemplate.isBlank()
                    ? "@Nostro-{CCY}-Bank"
                    : nostroIndicatorTemplate.trim();
        }

        public String nostroIndicatorForCurrency(String currency) {
            String ccy = currency == null ? "" : currency.trim().toUpperCase(java.util.Locale.ROOT);
            return getNostroIndicatorTemplate().replace("{CCY}", ccy);
        }
    }

    /**
     * Desk / attendant read APIs (bounded list — internal key only). See {@code GET /internal/v1/desk/orders/snapshot}.
     */
    public static class Desk {
        private boolean snapshotEnabled = false;
        private int snapshotMaxLimit = 50;
        private int snapshotMaxAgeHours = 24;
        // Active-orders cap on the snapshot endpoint. Independent of snapshotMaxLimit (which
        // applies to terminals only) — actives have no age window, so the operator can be
        // surprised by a large number of GTC LIMITs surfacing. 500 is the same hard cap as
        // snapshotMaxLimit's ceiling; bump per-deploy if a desk has more open orders than that.
        private int snapshotActiveLimit = 500;
        // Historical search endpoint feature flag. Same pattern as snapshotEnabled — off by
        // default; deploys that want operator history search opt-in via OMS_DESK_SEARCH_ENABLED.
        private boolean searchEnabled = false;
        // Per-page cap for the search endpoint. Default lower than snapshotMaxLimit's ceiling so
        // a typo on the wire (limit=10000) doesn't cause unbounded result memory; the operator
        // pages with the cursor when more rows are needed.
        private int searchMaxLimit = 100;

        public boolean isSnapshotEnabled() {
            return snapshotEnabled;
        }

        public void setSnapshotEnabled(boolean snapshotEnabled) {
            this.snapshotEnabled = snapshotEnabled;
        }

        public int getSnapshotMaxLimit() {
            return snapshotMaxLimit;
        }

        public void setSnapshotMaxLimit(int snapshotMaxLimit) {
            this.snapshotMaxLimit = Math.min(500, Math.max(1, snapshotMaxLimit));
        }

        public int getSnapshotMaxAgeHours() {
            return snapshotMaxAgeHours;
        }

        public void setSnapshotMaxAgeHours(int snapshotMaxAgeHours) {
            this.snapshotMaxAgeHours = Math.min(168, Math.max(1, snapshotMaxAgeHours));
        }

        public int getSnapshotActiveLimit() {
            return snapshotActiveLimit;
        }

        public void setSnapshotActiveLimit(int snapshotActiveLimit) {
            this.snapshotActiveLimit = Math.min(2000, Math.max(1, snapshotActiveLimit));
        }

        public boolean isSearchEnabled() {
            return searchEnabled;
        }

        public void setSearchEnabled(boolean searchEnabled) {
            this.searchEnabled = searchEnabled;
        }

        public int getSearchMaxLimit() {
            return searchMaxLimit;
        }

        public void setSearchMaxLimit(int searchMaxLimit) {
            this.searchMaxLimit = Math.min(500, Math.max(1, searchMaxLimit));
        }
    }

    /**
     * FX module (§11.5) — off by default; health endpoint documents rollout state.
     * Sub-tracks are gated independently; see {@code oms/docs/fx-backend-slice-m3.md}.
     */
    public static class Fx {
        private boolean moduleEnabled = false;
        private boolean quoteStubEnabled = false;
        private int quoteStubSchemaVersion = 1;
        private boolean nostroReadEnabled = false;
        /** Comma-separated Ledger {@code balance_id} values for {@code GET /internal/v1/fx/nostro/snapshot}. */
        private String nostroBalanceIdsCsv = "";
        private boolean multiLegAtomicityStubEnabled = false;
        private boolean hedgeHooksEnabled = false;
        private boolean eodFlattenEnabled = false;
        private long eodFlattenIntervalMs = 86_400_000L;
        /** Customer FX netting window before nostro hedge aggregation (§11.5.5). Default 5 min. */
        private long nettingWindowMs = 300_000L;
        /** When true, cross-currency order accepts aggregate into {@code fx_customer_flow_netting_bucket}. */
        private boolean customerFlowNettingEnabled = true;
        /**
         * §8.4 quote-lock at order accept. When {@code true}, the ingress
         * pipeline recalls {@code CreateOrderRequest.fxQuoteId} via
         * {@link com.balh.oms.fx.FxQuoteService#recall(String)} and rejects
         * with {@code RISK_FX_QUOTE_EXPIRED} when the quote is missing or
         * has expired. The BFF mints the id from
         * {@code POST /internal/v1/fx/quote} so the rate the customer was
         * shown pre-confirm is the rate that's honoured at hold time.
         *
         * <p>Default off so a stack without the BFF-side quoteId plumb (or
         * a stack on Phase 1.x without {@code FxQuoteService}) keeps
         * working — single-currency orders never carry a quoteId regardless
         * and so this is also a no-op for them.
         */
        private boolean acceptUseQuoterEnabled = false;
        /**
         * §8.4 cash-hold integrity tolerance, expressed in basis points of
         * the expected source-ccy amount. After a successful quote recall,
         * the accept path recomputes {@code expected_cash = (qty *
         * limitPrice) / lockedRate(side)} and rejects with
         * {@code RISK_FX_QUOTE_EXPIRED} when {@code abs(provided - expected)
         * / expected > tolerance_bps / 10_000}.
         *
         * <p>Default 5 bps (0.05%) absorbs the TypeScript Number → Java
         * BigDecimal rounding drift the BFF picks up when computing
         * {@code (qty * limitPrice) / rate} at six-decimal precision
         * (FX_CASH_AMOUNT_SCALE = 1e-6), while still flagging a BFF that
         * sends a hold ≥0.05% off the locked rate (= operator-visible
         * tampering or a bug worth catching).
         */
        private int acceptQuoteToleranceBps = 5;
        /**
         * When {@code false} and the mid subscriber is enabled but has no fresh
         * tick for a pair, {@link com.balh.oms.fx.FxQuoteService} rejects with
         * {@code RISK_FX_STALE_QUOTE} instead of falling back to {@code STUB_MIDS}.
         * Production stacks should set {@code OMS_FX_STUB_MIDS_ALLOWED=false}.
         */
        private boolean stubMidsAllowed = true;

        private final MarkupOverrides markupOverrides = new MarkupOverrides();
        private final TierKills tierKills = new TierKills();
        private final AutoHedger autoHedger = new AutoHedger();
        private final Suspense suspense = new Suspense();
        private final RetailNostro retailNostro = new RetailNostro();

        public MarkupOverrides getMarkupOverrides() {
            return markupOverrides;
        }

        public TierKills getTierKills() {
            return tierKills;
        }

        public AutoHedger getAutoHedger() {
            return autoHedger;
        }

        public Suspense getSuspense() {
            return suspense;
        }

        public RetailNostro getRetailNostro() {
            return retailNostro;
        }

        /**
         * Tactical markup override knobs (P3.7 + P3.9). The thresholds below
         * decide when a single trader's override is enough vs. when a second
         * approver is required. Defaults are deliberately conservative so a
         * fresh stack is "four-eyes for anything meaningful".
         */
        public static class MarkupOverrides {
            /** Hard cap on additive_bps a single submit can carry (absolute value). */
            private int maxAbsAdditiveBps = 200;
            /** Hard cap on (valid_until - valid_from) per submit. */
            private long maxDurationMs = 6L * 60L * 60L * 1000L;
            /** Self-approve allowed only when |additive_bps| ≤ this. */
            private int autoApproveAbsBps = 5;
            /** Self-approve allowed only when duration ≤ this. */
            private long autoApproveMaxDurationMs = 30L * 60L * 1000L;
            /**
             * When {@code true}, an override row that {@link #autoApproveAbsBps}
             * and {@link #autoApproveMaxDurationMs} approve is written with
             * {@code approved_at = created_at, approved_by = created_by}; when
             * {@code false} (paranoid mode) every row needs a second identity.
             */
            private boolean autoApproveEnabled = true;

            public int getMaxAbsAdditiveBps() { return maxAbsAdditiveBps; }
            public void setMaxAbsAdditiveBps(int v) {
                this.maxAbsAdditiveBps = Math.max(1, Math.min(5000, v));
            }
            public long getMaxDurationMs() { return maxDurationMs; }
            public void setMaxDurationMs(long v) {
                this.maxDurationMs = Math.max(60_000L, v);
            }
            public int getAutoApproveAbsBps() { return autoApproveAbsBps; }
            public void setAutoApproveAbsBps(int v) {
                this.autoApproveAbsBps = Math.max(0, v);
            }
            public long getAutoApproveMaxDurationMs() { return autoApproveMaxDurationMs; }
            public void setAutoApproveMaxDurationMs(long v) {
                this.autoApproveMaxDurationMs = Math.max(0L, v);
            }
            public boolean isAutoApproveEnabled() { return autoApproveEnabled; }
            public void setAutoApproveEnabled(boolean v) { this.autoApproveEnabled = v; }
        }

        /**
         * Per-tier kill-switch knobs (plan A2). A kill removes one
         * (pair, tier) — or every pair for a tier when {@code pair = null}
         * — from the customer-quote publisher's output until the row's
         * window closes or the row is revoked. Defaults sit slightly
         * tighter than {@link MarkupOverrides} because a kill is a
         * blunter instrument: a wildcard kill (no pair scope) always
         * requires a second approver regardless of duration.
         */
        public static class TierKills {
            /** Hard cap on (valid_until - valid_from) per submit. */
            private long maxDurationMs = 6L * 60L * 60L * 1000L;
            /** Self-approve allowed only when duration ≤ this AND pair is scoped (non-null). */
            private long autoApproveMaxDurationMs = 30L * 60L * 1000L;
            /**
             * When {@code true}, scoped-pair kill rows whose duration is
             * within {@link #autoApproveMaxDurationMs} are written with
             * {@code approved_at = created_at, approved_by = created_by};
             * wildcard kills (pair=NULL) still need a second identity
             * regardless of this flag. When {@code false}, every kill
             * goes through four-eyes.
             */
            private boolean autoApproveEnabled = true;

            public long getMaxDurationMs() { return maxDurationMs; }
            public void setMaxDurationMs(long v) { this.maxDurationMs = Math.max(60_000L, v); }
            public long getAutoApproveMaxDurationMs() { return autoApproveMaxDurationMs; }
            public void setAutoApproveMaxDurationMs(long v) {
                this.autoApproveMaxDurationMs = Math.max(0L, v);
            }
            public boolean isAutoApproveEnabled() { return autoApproveEnabled; }
            public void setAutoApproveEnabled(boolean v) { this.autoApproveEnabled = v; }
        }

        /**
         * FxAutoHedger engine knobs (plan B1). Defaults are deliberately
         * conservative: engine off (no scheduled tick), short eval
         * interval when on, auto-fire kill-switch separate from the
         * per-currency mode column so operators have a global
         * panic-stop independent of any row in {@code fx_hedger_policy}.
         */
        public static class AutoHedger {
            /**
             * Master enable for the scheduled engine. When false, the
             * bean is still wired but the {@code @Scheduled} loop
             * no-ops. Lets us deploy the migration + service without
             * any drift evaluation until operators are ready.
             */
            private boolean engineEnabled = false;
            /** Eval cadence in ms. Default 15s, matches plan B1 inputs section. */
            private long evalIntervalMs = 15_000L;
            /** Tier the engine quotes against — must match a tier in fx_pair_markups (V53 seeds 'desk'). */
            private String pricingTier = "desk";
            /**
             * Global auto-fire kill-switch. Even rows in
             * {@code fx_hedger_policy} with {@code mode='auto'} only
             * write a recommendation when this flag is {@code false}.
             * Plan B1.3 (auto-mode hook) flips this to {@code true}
             * after a week of advisory-mode evidence. Until then the
             * scheduler exercises every code path except the
             * {@code FxHedgeService.submit} call.
             */
            private boolean autoFireEnabled = false;
            /** Submitter string stamped on auto-fired hedge rows (plan decision 2). */
            private String autoFireSubmitter = "fx-auto-hedger";
            /** Cache refresh cadence for the policy table. */
            private long policyRefreshMs = 60_000L;

            public boolean isEngineEnabled() { return engineEnabled; }
            public void setEngineEnabled(boolean v) { this.engineEnabled = v; }
            public long getEvalIntervalMs() { return evalIntervalMs; }
            public void setEvalIntervalMs(long v) { this.evalIntervalMs = Math.max(1_000L, v); }
            public String getPricingTier() { return pricingTier; }
            public void setPricingTier(String v) {
                this.pricingTier = (v == null || v.isBlank()) ? "desk" : v.trim().toLowerCase();
            }
            public boolean isAutoFireEnabled() { return autoFireEnabled; }
            public void setAutoFireEnabled(boolean v) { this.autoFireEnabled = v; }
            public String getAutoFireSubmitter() { return autoFireSubmitter; }
            public void setAutoFireSubmitter(String v) {
                this.autoFireSubmitter = (v == null || v.isBlank()) ? "fx-auto-hedger" : v.trim();
            }
            public long getPolicyRefreshMs() { return policyRefreshMs; }
            public void setPolicyRefreshMs(long v) { this.policyRefreshMs = Math.max(5_000L, v); }
        }

        public boolean isModuleEnabled() {
            return moduleEnabled;
        }

        public void setModuleEnabled(boolean moduleEnabled) {
            this.moduleEnabled = moduleEnabled;
        }

        public boolean isQuoteStubEnabled() {
            return quoteStubEnabled;
        }

        public void setQuoteStubEnabled(boolean quoteStubEnabled) {
            this.quoteStubEnabled = quoteStubEnabled;
        }

        public int getQuoteStubSchemaVersion() {
            return quoteStubSchemaVersion;
        }

        public void setQuoteStubSchemaVersion(int quoteStubSchemaVersion) {
            this.quoteStubSchemaVersion = Math.min(99, Math.max(1, quoteStubSchemaVersion));
        }

        public boolean isNostroReadEnabled() {
            return nostroReadEnabled;
        }

        public void setNostroReadEnabled(boolean nostroReadEnabled) {
            this.nostroReadEnabled = nostroReadEnabled;
        }

        public String getNostroBalanceIdsCsv() {
            return nostroBalanceIdsCsv;
        }

        public void setNostroBalanceIdsCsv(String nostroBalanceIdsCsv) {
            this.nostroBalanceIdsCsv = nostroBalanceIdsCsv == null ? "" : nostroBalanceIdsCsv;
        }

        public List<String> nostroBalanceIds() {
            if (nostroBalanceIdsCsv == null || nostroBalanceIdsCsv.isBlank()) {
                return List.of();
            }
            return Arrays.stream(nostroBalanceIdsCsv.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .toList();
        }

        public boolean isMultiLegAtomicityStubEnabled() {
            return multiLegAtomicityStubEnabled;
        }

        public void setMultiLegAtomicityStubEnabled(boolean multiLegAtomicityStubEnabled) {
            this.multiLegAtomicityStubEnabled = multiLegAtomicityStubEnabled;
        }

        public boolean isHedgeHooksEnabled() {
            return hedgeHooksEnabled;
        }

        public void setHedgeHooksEnabled(boolean hedgeHooksEnabled) {
            this.hedgeHooksEnabled = hedgeHooksEnabled;
        }

        public boolean isEodFlattenEnabled() {
            return eodFlattenEnabled;
        }

        public void setEodFlattenEnabled(boolean eodFlattenEnabled) {
            this.eodFlattenEnabled = eodFlattenEnabled;
        }

        public long getEodFlattenIntervalMs() {
            return eodFlattenIntervalMs;
        }

        public void setEodFlattenIntervalMs(long eodFlattenIntervalMs) {
            this.eodFlattenIntervalMs = Math.max(60_000L, eodFlattenIntervalMs);
        }

        public long getNettingWindowMs() {
            return nettingWindowMs;
        }

        public void setNettingWindowMs(long nettingWindowMs) {
            this.nettingWindowMs = Math.max(60_000L, nettingWindowMs);
        }

        public boolean isCustomerFlowNettingEnabled() {
            return customerFlowNettingEnabled;
        }

        public void setCustomerFlowNettingEnabled(boolean customerFlowNettingEnabled) {
            this.customerFlowNettingEnabled = customerFlowNettingEnabled;
        }

        public boolean isAcceptUseQuoterEnabled() {
            return acceptUseQuoterEnabled;
        }

        public void setAcceptUseQuoterEnabled(boolean acceptUseQuoterEnabled) {
            this.acceptUseQuoterEnabled = acceptUseQuoterEnabled;
        }

        public int getAcceptQuoteToleranceBps() {
            return acceptQuoteToleranceBps;
        }

        public void setAcceptQuoteToleranceBps(int acceptQuoteToleranceBps) {
            // Clamp into a sane range so a fat-fingered config can't disable
            // the check entirely (<=0 would pass everything) nor reject the
            // entire flow (>10000 = >100% would still reject everything but
            // semantically meaningless). 5000 bps = 50% is the upper bound.
            this.acceptQuoteToleranceBps = Math.min(5000, Math.max(0, acceptQuoteToleranceBps));
        }

        public boolean isStubMidsAllowed() {
            return stubMidsAllowed;
        }

        public void setStubMidsAllowed(boolean stubMidsAllowed) {
            this.stubMidsAllowed = stubMidsAllowed;
        }
    }

    /**
     * FX suspense exposure visibility + limit monitoring (§11.5 tail). Suspense
     * balances ({@code @FX-Suspense-<CCY>}) hold open FX position pending
     * prime-broker settlement; they're the indicator side of cross-currency
     * customer cash legs and FX hedge legs. Operators need them surfaced
     * because hedge submissions fail at the suspense leg when cumulative
     * customer flow has pushed it past the ledger's overdraft tolerance.
     *
     * <p>Read by {@link com.balh.oms.fx.FxSuspenseSnapshotService} (panel) and
     * {@link com.balh.oms.fx.FxSuspenseLimitMonitor} (gauge + alert source).
     */
    public static class Suspense {
        /**
         * CSV of currencies whose {@code @FX-Suspense-<CCY>} balance is
         * monitored (env {@code OMS_FX_SUSPENSE_CURRENCIES_CSV}). Empty
         * → snapshot is 503 and monitor stays idle.
         */
        private String currenciesCsv = "";

        /**
         * Optional per-currency absolute-value soft limit
         * (env {@code OMS_FX_SUSPENSE_MAX_ABS_CSV}, e.g.
         * {@code USD=1000000,EUR=1000000,GBP=500000}). Triggers
         * {@code overLimit=true} in the snapshot and increments
         * {@code oms_fx_suspense_over_limit_total{currency}} when crossed.
         * A currency without a limit is reported but never marked over.
         */
        private String maxAbsCsv = "";

        /**
         * When {@code true} the {@code @Scheduled} monitor polls suspense
         * balances and publishes gauges + over-limit counters. Off by default
         * so a stack without {@code currenciesCsv} configured stays quiet.
         */
        private boolean limitMonitorEnabled = false;

        /** Monitor poll cadence; clamped to ≥10s. */
        private long limitMonitorPollIntervalMs = 30_000L;

        public String getCurrenciesCsv() { return currenciesCsv; }

        public void setCurrenciesCsv(String csv) {
            this.currenciesCsv = csv == null ? "" : csv;
        }

        public String getMaxAbsCsv() { return maxAbsCsv; }

        public void setMaxAbsCsv(String csv) {
            this.maxAbsCsv = csv == null ? "" : csv;
        }

        public boolean isLimitMonitorEnabled() { return limitMonitorEnabled; }

        public void setLimitMonitorEnabled(boolean v) { this.limitMonitorEnabled = v; }

        public long getLimitMonitorPollIntervalMs() { return limitMonitorPollIntervalMs; }

        public void setLimitMonitorPollIntervalMs(long v) {
            this.limitMonitorPollIntervalMs = Math.max(10_000L, v);
        }

        public List<String> currencies() {
            if (currenciesCsv == null || currenciesCsv.isBlank()) {
                return List.of();
            }
            return Arrays.stream(currenciesCsv.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .map(String::toUpperCase)
                    .toList();
        }

        /**
         * Parse {@link #maxAbsCsv} into a {@code currency → absolute-limit} map.
         * Malformed entries (no {@code =}, blank key/value, unparseable number)
         * are skipped silently so a single bad pair can't kill the snapshot.
         */
        public Map<String, BigDecimal> maxAbsByCurrency() {
            if (maxAbsCsv == null || maxAbsCsv.isBlank()) {
                return Map.of();
            }
            Map<String, BigDecimal> out = new LinkedHashMap<>();
            for (String pair : maxAbsCsv.split(",")) {
                int eq = pair.indexOf('=');
                if (eq <= 0 || eq == pair.length() - 1) continue;
                String key = pair.substring(0, eq).trim().toUpperCase();
                String value = pair.substring(eq + 1).trim();
                if (key.isEmpty() || value.isEmpty()) continue;
                try {
                    out.put(key, new BigDecimal(value));
                } catch (NumberFormatException ignore) {
                    // skip malformed
                }
            }
            return out;
        }
    }

    /**
     * Retail FX conversion pool visibility + limit monitoring. The plain
     * {@code @Nostro-<CCY>} balances (no {@code -Bank} suffix) are the pool
     * the customer-app "move money" cross-currency path debits/credits:
     * a EUR→GBP transfer credits {@code @Nostro-EUR} and debits
     * {@code @Nostro-GBP}, so the signed pool balance per currency is the
     * <em>retail</em> open FX position — distinct from the OMS-routed
     * {@code @FX-Suspense-<CCY>} book ({@link Suspense}) and the
     * correspondent-cash {@code @Nostro-<CCY>-Bank} inventory
     * ({@code nostroBalanceIdsCsv}).
     *
     * <p>Before this block landed nothing read these balances, so retail
     * conversion exposure was unsupervised and un-hedgeable. Surfaced via
     * {@link com.balh.oms.fx.FxRetailNostroSnapshotService} (panel +
     * auto-hedger drift source for {@code exposure_source='retail'}
     * policies) and {@link com.balh.oms.fx.FxRetailNostroLimitMonitor}
     * (gauge + alert source).
     *
     * <p>Configured via {@code oms.fx.retail-nostro.currencies-csv} (env
     * {@code OMS_FX_RETAIL_NOSTRO_CURRENCIES_CSV}, e.g. {@code USD,EUR,GBP,SEK}).
     * Same parse + soft-limit semantics as {@link Suspense}.
     */
    public static class RetailNostro {
        /**
         * CSV of currencies whose retail {@code @Nostro-<CCY>} pool balance
         * is monitored (env {@code OMS_FX_RETAIL_NOSTRO_CURRENCIES_CSV}).
         * Empty → snapshot is 503 and monitor stays idle.
         */
        private String currenciesCsv = "";

        /**
         * Optional per-currency absolute-value soft limit
         * (env {@code OMS_FX_RETAIL_NOSTRO_MAX_ABS_CSV}, e.g.
         * {@code USD=1000000,EUR=1000000,GBP=500000}). Triggers
         * {@code overLimit=true} in the snapshot and increments
         * {@code oms_fx_retail_nostro_over_limit_total{currency}} when crossed.
         */
        private String maxAbsCsv = "";

        /**
         * When {@code true} the {@code @Scheduled} monitor polls retail pool
         * balances and publishes gauges + over-limit counters. Off by default
         * so a stack without {@code currenciesCsv} configured stays quiet.
         */
        private boolean limitMonitorEnabled = false;

        /** Monitor poll cadence; clamped to ≥10s. */
        private long limitMonitorPollIntervalMs = 30_000L;

        public String getCurrenciesCsv() { return currenciesCsv; }

        public void setCurrenciesCsv(String csv) {
            this.currenciesCsv = csv == null ? "" : csv;
        }

        public String getMaxAbsCsv() { return maxAbsCsv; }

        public void setMaxAbsCsv(String csv) {
            this.maxAbsCsv = csv == null ? "" : csv;
        }

        public boolean isLimitMonitorEnabled() { return limitMonitorEnabled; }

        public void setLimitMonitorEnabled(boolean v) { this.limitMonitorEnabled = v; }

        public long getLimitMonitorPollIntervalMs() { return limitMonitorPollIntervalMs; }

        public void setLimitMonitorPollIntervalMs(long v) {
            this.limitMonitorPollIntervalMs = Math.max(10_000L, v);
        }

        public List<String> currencies() {
            if (currenciesCsv == null || currenciesCsv.isBlank()) {
                return List.of();
            }
            return Arrays.stream(currenciesCsv.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .map(String::toUpperCase)
                    .toList();
        }

        /**
         * Parse {@link #maxAbsCsv} into a {@code currency → absolute-limit} map.
         * Malformed entries are skipped silently so a single bad pair can't
         * kill the snapshot.
         */
        public Map<String, BigDecimal> maxAbsByCurrency() {
            if (maxAbsCsv == null || maxAbsCsv.isBlank()) {
                return Map.of();
            }
            Map<String, BigDecimal> out = new LinkedHashMap<>();
            for (String pair : maxAbsCsv.split(",")) {
                int eq = pair.indexOf('=');
                if (eq <= 0 || eq == pair.length() - 1) continue;
                String key = pair.substring(0, eq).trim().toUpperCase();
                String value = pair.substring(eq + 1).trim();
                if (key.isEmpty() || value.isEmpty()) continue;
                try {
                    out.put(key, new BigDecimal(value));
                } catch (NumberFormatException ignore) {
                    // skip malformed
                }
            }
            return out;
        }
    }

    /**
     * Corporate action inbox (slice 8 stub): ingest via internal HTTP; optional processor marks rows processed.
     */
    public static class CorporateAction {
        private boolean processorEnabled = false;
        private long processorIntervalMs = 60_000L;
        private int processorBatchSize = 50;
        private int ingestInstrumentSymbolMaxLength = 64;
        private int ingestActionTypeMaxLength = 64;
        private int ingestPayloadJsonMaxChars = 16_000;
        private int listMaxLimit = 200;
        private int listDefaultLimit = 50;
        private boolean payableDateLedgerEnabled = false;
        private long payableDateLedgerIntervalMs = 60_000L;
        private int payableDateLedgerBatchSize = 50;
        private long ledgerOutboxReconcilerIntervalMs = 1_000L;
        private int ledgerOutboxReconcilerBatchSize = 25;
        private boolean recordDateSnapshotJobEnabled = false;
        private int recordDateSnapshotBatchSize = 50;

        public boolean isProcessorEnabled() {
            return processorEnabled;
        }

        public void setProcessorEnabled(boolean processorEnabled) {
            this.processorEnabled = processorEnabled;
        }

        public long getProcessorIntervalMs() {
            return processorIntervalMs;
        }

        public void setProcessorIntervalMs(long processorIntervalMs) {
            this.processorIntervalMs = Math.max(5_000L, processorIntervalMs);
        }

        public int getProcessorBatchSize() {
            return processorBatchSize;
        }

        public void setProcessorBatchSize(int processorBatchSize) {
            this.processorBatchSize = Math.min(500, Math.max(1, processorBatchSize));
        }

        public int getIngestInstrumentSymbolMaxLength() {
            return ingestInstrumentSymbolMaxLength;
        }

        public void setIngestInstrumentSymbolMaxLength(int ingestInstrumentSymbolMaxLength) {
            this.ingestInstrumentSymbolMaxLength = Math.min(256, Math.max(1, ingestInstrumentSymbolMaxLength));
        }

        public int getIngestActionTypeMaxLength() {
            return ingestActionTypeMaxLength;
        }

        public void setIngestActionTypeMaxLength(int ingestActionTypeMaxLength) {
            this.ingestActionTypeMaxLength = Math.min(256, Math.max(1, ingestActionTypeMaxLength));
        }

        public int getIngestPayloadJsonMaxChars() {
            return ingestPayloadJsonMaxChars;
        }

        public void setIngestPayloadJsonMaxChars(int ingestPayloadJsonMaxChars) {
            this.ingestPayloadJsonMaxChars = Math.min(500_000, Math.max(2, ingestPayloadJsonMaxChars));
        }

        public int getListMaxLimit() {
            return listMaxLimit;
        }

        public void setListMaxLimit(int listMaxLimit) {
            this.listMaxLimit = Math.min(500, Math.max(1, listMaxLimit));
        }

        public int getListDefaultLimit() {
            return listDefaultLimit;
        }

        public void setListDefaultLimit(int listDefaultLimit) {
            this.listDefaultLimit = Math.min(200, Math.max(1, listDefaultLimit));
        }

        public boolean isPayableDateLedgerEnabled() {
            return payableDateLedgerEnabled;
        }

        public void setPayableDateLedgerEnabled(boolean payableDateLedgerEnabled) {
            this.payableDateLedgerEnabled = payableDateLedgerEnabled;
        }

        public long getPayableDateLedgerIntervalMs() {
            return payableDateLedgerIntervalMs;
        }

        public void setPayableDateLedgerIntervalMs(long payableDateLedgerIntervalMs) {
            this.payableDateLedgerIntervalMs = Math.max(5_000L, payableDateLedgerIntervalMs);
        }

        public int getPayableDateLedgerBatchSize() {
            return payableDateLedgerBatchSize;
        }

        public void setPayableDateLedgerBatchSize(int payableDateLedgerBatchSize) {
            this.payableDateLedgerBatchSize = Math.min(500, Math.max(1, payableDateLedgerBatchSize));
        }

        public long getLedgerOutboxReconcilerIntervalMs() {
            return ledgerOutboxReconcilerIntervalMs;
        }

        public void setLedgerOutboxReconcilerIntervalMs(long ledgerOutboxReconcilerIntervalMs) {
            this.ledgerOutboxReconcilerIntervalMs = Math.max(200L, ledgerOutboxReconcilerIntervalMs);
        }

        public int getLedgerOutboxReconcilerBatchSize() {
            return ledgerOutboxReconcilerBatchSize;
        }

        public void setLedgerOutboxReconcilerBatchSize(int ledgerOutboxReconcilerBatchSize) {
            this.ledgerOutboxReconcilerBatchSize = Math.min(200, Math.max(1, ledgerOutboxReconcilerBatchSize));
        }

        public boolean isRecordDateSnapshotJobEnabled() {
            return recordDateSnapshotJobEnabled;
        }

        public void setRecordDateSnapshotJobEnabled(boolean recordDateSnapshotJobEnabled) {
            this.recordDateSnapshotJobEnabled = recordDateSnapshotJobEnabled;
        }

        public int getRecordDateSnapshotBatchSize() {
            return recordDateSnapshotBatchSize;
        }

        public void setRecordDateSnapshotBatchSize(int recordDateSnapshotBatchSize) {
            this.recordDateSnapshotBatchSize = Math.min(500, Math.max(1, recordDateSnapshotBatchSize));
        }
    }

    /** ISK tax / valuation parameters (gap plan §5.10 Phase E). */
    public static class IskTax {
        private BigDecimal defaultFxToSekRate = new BigDecimal("10.00");
        private BigDecimal statslanerantaOverride;
        private java.util.Map<String, BigDecimal> fxToSekRates = new java.util.HashMap<>();
        private boolean quarterlyValuationJobEnabled = false;
        private String quarterlyValuationCron = "0 0 6 1 1,4,7,10 *";
        private boolean pendingPositionCountSyncEnabled = false;
        private long pendingPositionCountSyncIntervalMs = 300_000L;

        public BigDecimal getDefaultFxToSekRate() {
            return defaultFxToSekRate;
        }

        public void setDefaultFxToSekRate(BigDecimal defaultFxToSekRate) {
            if (defaultFxToSekRate != null && defaultFxToSekRate.signum() > 0) {
                this.defaultFxToSekRate = defaultFxToSekRate;
            }
        }

        public BigDecimal getStatslanerantaOverride() {
            return statslanerantaOverride;
        }

        public void setStatslanerantaOverride(BigDecimal statslanerantaOverride) {
            this.statslanerantaOverride = statslanerantaOverride;
        }

        public java.util.Map<String, BigDecimal> getFxToSekRates() {
            if (fxToSekRates == null || fxToSekRates.isEmpty()) {
                java.util.Map<String, BigDecimal> defaults = new java.util.HashMap<>();
                defaults.put("USD", defaultFxToSekRate);
                defaults.put("EUR", new BigDecimal("11.00"));
                return defaults;
            }
            return fxToSekRates;
        }

        public void setFxToSekRates(java.util.Map<String, BigDecimal> fxToSekRates) {
            this.fxToSekRates = fxToSekRates;
        }

        public boolean isQuarterlyValuationJobEnabled() {
            return quarterlyValuationJobEnabled;
        }

        public void setQuarterlyValuationJobEnabled(boolean quarterlyValuationJobEnabled) {
            this.quarterlyValuationJobEnabled = quarterlyValuationJobEnabled;
        }

        public String getQuarterlyValuationCron() {
            return quarterlyValuationCron;
        }

        public void setQuarterlyValuationCron(String quarterlyValuationCron) {
            this.quarterlyValuationCron = quarterlyValuationCron == null || quarterlyValuationCron.isBlank()
                    ? "0 0 6 1 1,4,7,10 *"
                    : quarterlyValuationCron.trim();
        }

        public boolean isPendingPositionCountSyncEnabled() {
            return pendingPositionCountSyncEnabled;
        }

        public void setPendingPositionCountSyncEnabled(boolean pendingPositionCountSyncEnabled) {
            this.pendingPositionCountSyncEnabled = pendingPositionCountSyncEnabled;
        }

        public long getPendingPositionCountSyncIntervalMs() {
            return pendingPositionCountSyncIntervalMs;
        }

        public void setPendingPositionCountSyncIntervalMs(long pendingPositionCountSyncIntervalMs) {
            this.pendingPositionCountSyncIntervalMs = Math.max(60_000L, pendingPositionCountSyncIntervalMs);
        }
    }

    /**
     * Optional HTTP integration to a Marketdata-style service (instruments list + NBBO quotes for evidence).
     * Defaults are safe-off; paths are relative to {@link #baseUrl}.
     */
    public static class Marketdata {
        private boolean enabled = false;
        private String baseUrl = "";
        private String apiKey = "";
        private long connectTimeoutMs = 2000L;
        private long readTimeoutMs = 5000L;
        private String instrumentsHttpPath = "/internal/v0/instruments";
        private String nbboHttpPath = "/internal/v0/nbbo";
        private long instrumentsRefreshIntervalMs = 60_000L;
        /**
         * When {@code true} with {@link com.balh.oms.config.OmsConfig.Routing#isNbboReferenceInMarketContextEnabled()},
         * trade applies try HTTP NBBO
         * before stub bid/ask (symbol query param {@code symbol}).
         */
        private boolean nbboInMarketContextEnabled = false;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getBaseUrl() {
            return baseUrl;
        }

        public void setBaseUrl(String baseUrl) {
            this.baseUrl = baseUrl == null ? "" : baseUrl.trim();
        }

        public String getApiKey() {
            return apiKey;
        }

        public void setApiKey(String apiKey) {
            this.apiKey = apiKey == null ? "" : apiKey;
        }

        public long getConnectTimeoutMs() {
            return connectTimeoutMs;
        }

        public void setConnectTimeoutMs(long connectTimeoutMs) {
            this.connectTimeoutMs = Math.max(250L, connectTimeoutMs);
        }

        public long getReadTimeoutMs() {
            return readTimeoutMs;
        }

        public void setReadTimeoutMs(long readTimeoutMs) {
            this.readTimeoutMs = Math.max(250L, readTimeoutMs);
        }

        public String getInstrumentsHttpPath() {
            return instrumentsHttpPath;
        }

        public void setInstrumentsHttpPath(String instrumentsHttpPath) {
            this.instrumentsHttpPath = instrumentsHttpPath == null ? "" : instrumentsHttpPath.trim();
        }

        public String getNbboHttpPath() {
            return nbboHttpPath;
        }

        public void setNbboHttpPath(String nbboHttpPath) {
            this.nbboHttpPath = nbboHttpPath == null ? "" : nbboHttpPath.trim();
        }

        public long getInstrumentsRefreshIntervalMs() {
            return instrumentsRefreshIntervalMs;
        }

        public void setInstrumentsRefreshIntervalMs(long instrumentsRefreshIntervalMs) {
            this.instrumentsRefreshIntervalMs = Math.max(5_000L, instrumentsRefreshIntervalMs);
        }

        public boolean isNbboInMarketContextEnabled() {
            return nbboInMarketContextEnabled;
        }

        public void setNbboInMarketContextEnabled(boolean nbboInMarketContextEnabled) {
            this.nbboInMarketContextEnabled = nbboInMarketContextEnabled;
        }
    }

    /**
     * OpenTelemetry SDK metrics (optional). When {@code metrics-enabled=false}, only Micrometer
     * {@code /actuator/prometheus} is used (default).
     */
    public static class Otel {
        private boolean metricsEnabled = false;
        private int prometheusPort = 9464;

        public boolean isMetricsEnabled() {
            return metricsEnabled;
        }

        public void setMetricsEnabled(boolean metricsEnabled) {
            this.metricsEnabled = metricsEnabled;
        }

        public int getPrometheusPort() {
            return prometheusPort;
        }

        public void setPrometheusPort(int prometheusPort) {
            this.prometheusPort = Math.min(65_535, Math.max(1, prometheusPort));
        }
    }

    /**
     * Aeron Cluster substrate config (ADR 0001 / plan
     * {@code system-documentation/plans/oms-aeron-cluster-substrate.md}).
     *
     * <p>Phase 1a wires only the cluster <em>client</em> half (Spring bean
     * {@code OmsClusterIngressClient}). The cluster <em>node</em> reads its own
     * config from environment variables in {@code OmsClusterNodeBootstrap} —
     * cluster nodes are plain-Java mains, not Spring apps.
     */
    public static class Cluster {

        private final Client client = new Client();
        private final Projector projector = new Projector();
        private final FixEgress fixEgress = new FixEgress();
        private final VenueEgress venueEgress = new VenueEgress();
        private final VenueResolver venueResolver = new VenueResolver();
        private final VenueReconciler venueReconciler = new VenueReconciler();

        public Client getClient() { return client; }
        public Projector getProjector() { return projector; }
        public FixEgress getFixEgress() { return fixEgress; }
        public VenueEgress getVenueEgress() { return venueEgress; }
        public VenueResolver getVenueResolver() { return venueResolver; }
        public VenueReconciler getVenueReconciler() { return venueReconciler; }

        /**
         * Phase 3 of the Aeron Cluster substrate plan: configuration for the
         * {@code oms-fix-egress} JVM. Mirrors {@link Projector} on the FIX-side: subscribes to
         * the same cluster events recording (Aeron Archive replay), translates admitted /
         * working orders into QuickFIX {@code NewOrderSingle}, and (Phase 3 slice 3d) submits
         * inbound {@code ExecutionReport} back to the cluster as an {@code ApplyExecutionReport}
         * command.
         *
         * <p>Slice 3a shipped only the topology scaffold ({@link #isEnabled()}). Slice 3b-1
         * adds the replay-channel / archive-control fields needed to actually consume cluster
         * events. {@link #replayStreamId} defaults to {@code 4323} so the egress JVM does not
         * collide with the production projector's stream id (4321) or the in-test projector
         * singleton's id (4322); the setter rejects equality with
         * {@link com.balh.oms.cluster.OmsClusterWireFormat#EVENTS_STREAM_ID} for the same
         * reason {@link Projector#setReplayStreamId(int)} does.
         */
        public static class FixEgress {

            private static final long DEFAULT_POLL_PARK_NANOS = 1_000_000L;
            private static final int DEFAULT_FRAGMENT_LIMIT = 64;
            private static final long DEFAULT_RECORDING_LOOKUP_PARK_MS = 100L;
            private static final int DEFAULT_REPLAY_STREAM_ID = 4323;
            /**
             * Default park between {@code Session.sendToTarget} retry attempts while the FIX
             * session is reconnecting / re-logging-on. Same shape as
             * {@code oms.fix.outbound-dedicated-not-ready-park-nanos} on the legacy dispatch
             * worker; named separately so the egress JVM can be tuned independently.
             */
            private static final long DEFAULT_SESSION_NOT_READY_PARK_NANOS = 50_000_000L;
            /**
             * Slice 4l: number of {@code OrderAdmittedEvent} fragments applied between Postgres
             * UPSERTs to {@code oms_fix_egress_cursor}. Default {@code 1} preserves slice 3b-2's
             * per-event advance contract: a crash redelivers exactly the in-flight fragment, the
             * broker rejects with {@code DupClOrdID}, and the at-least-once-at-broker guarantee
             * holds with the smallest possible redelivery window. Values {@code N>1} widen the
             * redelivery window to up to {@code N-1} fragments per crash but trade per-event
             * Postgres round-trips for a single batched advance every {@code N} fragments, which
             * benchmarks (slice 4l on Pop!) showed lifts the egress drain ceiling by ~27\u00d7
             * (\u2248694/s \u2192 \u224818,800/s) because the Postgres UPSERT, not QuickFIX fsync, is the
             * dominant per-event cost. Recommended production setting: 25\u201350 once operators are
             * comfortable with the wider broker-side dedupe burst on crash. See
             * {@code docs/runbooks/local-multi-jvm-bench.md} \u201cSlice 4l evidence\u201d for the
             * benchmark trace and the broker-side {@code DupClOrdID} contract review.
             */
            private static final int DEFAULT_CURSOR_FLUSH_EVERY = 1;

            private boolean enabled = false;
            private String aeronDirectory = "";
            private String archiveControlRequestChannel = "aeron:ipc?term-length=64k";
            private String archiveControlResponseChannel = "aeron:ipc?term-length=64k";
            private String replayChannel = "aeron:ipc?term-length=64k";
            private int replayStreamId = DEFAULT_REPLAY_STREAM_ID;
            private long pollParkNanos = DEFAULT_POLL_PARK_NANOS;
            private int fragmentLimit = DEFAULT_FRAGMENT_LIMIT;
            private long recordingLookupParkMs = DEFAULT_RECORDING_LOOKUP_PARK_MS;
            private long sessionNotReadyParkNanos = DEFAULT_SESSION_NOT_READY_PARK_NANOS;
            private int cursorFlushEvery = DEFAULT_CURSOR_FLUSH_EVERY;

            public boolean isEnabled() { return enabled; }
            public void setEnabled(boolean enabled) { this.enabled = enabled; }

            public String getAeronDirectory() { return aeronDirectory; }
            public void setAeronDirectory(String aeronDirectory) {
                this.aeronDirectory = aeronDirectory == null ? "" : aeronDirectory.trim();
            }

            public String getArchiveControlRequestChannel() { return archiveControlRequestChannel; }
            public void setArchiveControlRequestChannel(String archiveControlRequestChannel) {
                this.archiveControlRequestChannel =
                        archiveControlRequestChannel == null || archiveControlRequestChannel.isBlank()
                                ? "aeron:ipc?term-length=64k"
                                : archiveControlRequestChannel.trim();
            }

            public String getArchiveControlResponseChannel() { return archiveControlResponseChannel; }
            public void setArchiveControlResponseChannel(String archiveControlResponseChannel) {
                this.archiveControlResponseChannel =
                        archiveControlResponseChannel == null || archiveControlResponseChannel.isBlank()
                                ? "aeron:ipc?term-length=64k"
                                : archiveControlResponseChannel.trim();
            }

            public String getReplayChannel() { return replayChannel; }
            public void setReplayChannel(String replayChannel) {
                this.replayChannel =
                        replayChannel == null || replayChannel.isBlank()
                                ? "aeron:ipc?term-length=64k"
                                : replayChannel.trim();
            }

            public int getReplayStreamId() { return replayStreamId; }
            public void setReplayStreamId(int replayStreamId) {
                if (replayStreamId == com.balh.oms.cluster.OmsClusterWireFormat.EVENTS_STREAM_ID) {
                    throw new IllegalArgumentException(
                            "oms.cluster.fix-egress.replay-stream-id must differ from EVENTS_STREAM_ID="
                                    + com.balh.oms.cluster.OmsClusterWireFormat.EVENTS_STREAM_ID);
                }
                this.replayStreamId = replayStreamId;
            }

            public long getPollParkNanos() { return pollParkNanos; }
            public void setPollParkNanos(long pollParkNanos) {
                this.pollParkNanos = Math.max(1_000L, pollParkNanos);
            }

            public int getFragmentLimit() { return fragmentLimit; }
            public void setFragmentLimit(int fragmentLimit) {
                this.fragmentLimit = Math.max(1, fragmentLimit);
            }

            public long getRecordingLookupParkMs() { return recordingLookupParkMs; }
            public void setRecordingLookupParkMs(long recordingLookupParkMs) {
                this.recordingLookupParkMs = Math.max(10L, recordingLookupParkMs);
            }

            public long getSessionNotReadyParkNanos() { return sessionNotReadyParkNanos; }
            public void setSessionNotReadyParkNanos(long sessionNotReadyParkNanos) {
                this.sessionNotReadyParkNanos = Math.max(1_000L, sessionNotReadyParkNanos);
            }

            public int getCursorFlushEvery() { return cursorFlushEvery; }
            public void setCursorFlushEvery(int cursorFlushEvery) {
                this.cursorFlushEvery = Math.max(1, cursorFlushEvery);
            }
        }

        /**
         * Phase 0 of the internal-venue plan: configuration for the {@code oms-venue-egress} JVM.
         * Same replay shape as {@link FixEgress}; distinct replay stream id so FIX and venue egress
         * can co-exist on one host during bench.
         */
        public static class VenueEgress {

            /**
             * Idle park between replay polls when Aeron returns zero fragments. Kept short so the
             * egress re-checks the live tail quickly under sustained admit load (pop @ 400 RPS:
             * 1 ms park capped drain to ~1k polls/s even when fragments were available on the
             * next slice).
             */
            private static final long DEFAULT_POLL_PARK_NANOS = 10_000L;

            /**
             * Fragments per {@code replay.poll} pass. Larger batches amortize poll overhead when
             * draining cluster-log backlog ahead of ingress admit rate.
             */
            private static final int DEFAULT_FRAGMENT_LIMIT = 512;

            private static final long DEFAULT_RECORDING_LOOKUP_PARK_MS = 100L;
            private static final int DEFAULT_REPLAY_STREAM_ID = 4324;
            private static final int DEFAULT_CURSOR_FLUSH_EVERY = 1;
            private static final int DEFAULT_VENUE_ROUTE_MAX_IN_FLIGHT = 512;
            private static final int DEFAULT_BACKLOG_THROTTLE_PENDING_ROUTE_THRESHOLD = 384;
            private static final int DEFAULT_BACKLOG_THROTTLE_ER_OFFER_QUEUE_DEPTH_THRESHOLD = 1024;
            private static final int DEFAULT_BACKLOG_THROTTLE_MAX_IN_FLIGHT = 128;
            private static final int DEFAULT_BACKLOG_THROTTLE_ER_SOFT_CAP_PERMIT_MULTIPLIER = 4;
            private static final int DEFAULT_BACKLOG_THROTTLE_ER_EXHAUSTED_CAP_PERMIT_MULTIPLIER = 3;
            private static final long DEFAULT_BACKLOG_THROTTLE_PARK_NANOS = 50_000L;

            /**
             * Bounded retries when {@code oms-venue-egress} submits a
             * {@link com.balh.oms.cluster.ApplyExecutionReportCommand} with
             * {@code EXEC_TYPE_VENUE_REJECT} after a venue {@code RouteOrder} failure. Without
             * retries a transient cluster-offer blip orphans the order in {@code PENDING_NEW}.
             */
            private static final int DEFAULT_VENUE_REJECT_SUBMIT_MAX_ATTEMPTS = 3;

            /** Park between VENUE_REJECT cluster-submit retries (milliseconds). */
            private static final long DEFAULT_VENUE_REJECT_SUBMIT_RETRY_BACKOFF_MS = 50L;

            /**
             * {@link Thread#setPriority(int)} for {@code oms-venue-egress-replay}. Default
             * {@link Thread#MAX_PRIORITY} keeps the Aeron drain ahead of generic worker pools on
             * a loaded host; operators can lower via env when co-tenancy requires it.
             */
            private static final int DEFAULT_REPLAY_THREAD_PRIORITY = Thread.MAX_PRIORITY;

            /**
             * Spin-yield passes in {@code pollReplayIdleTail} before archive metadata refresh or
             * configured idle park. Raised from 8→16 for pop @ 10k admit/s knee (mirrors projector);
             * 16k soak needs the same headroom to avoid Archive control round-trips on micro-gaps.
             */
            private static final int DEFAULT_REPLAY_IDLE_TAIL_POLLS = 16;

            /**
             * Excess archive byte lag ({@code recordingUpperBound − applied − pipelinedFloor}) above
             * which the replay loop enters lag-adaptive drain: spin-yield instead of park, larger
             * {@code replay.poll} batches, extended idle-tail spins. Aligns with
             * {@link com.balh.oms.ingress.OmsVenueEgressLagPublisher#GAUGE_LAG_BYTES} gate band.
             */
            private static final long DEFAULT_REPLAY_LAG_ADAPTIVE_EXCESS_LAG_BYTES_THRESHOLD = 4_096L;

            /**
             * {@code oms.venue.egress.pipeline.pending.routes} watermark for lag-adaptive drain when
             * byte lag is still below threshold but route completions are backing up.
             */
            private static final int DEFAULT_REPLAY_LAG_ADAPTIVE_PENDING_ROUTES_THRESHOLD = 256;

            /**
             * Raised {@code replay.poll} fragment cap while lag-adaptive drain is active (static floor
             * remains {@link #DEFAULT_FRAGMENT_LIMIT} on bench hosts).
             */
            private static final int DEFAULT_REPLAY_LAG_ADAPTIVE_FRAGMENT_LIMIT = 8_192;

            /**
             * Extended {@code pollReplayIdleTail} spin budget while lag-adaptive drain is active.
             */
            private static final int DEFAULT_REPLAY_LAG_ADAPTIVE_IDLE_TAIL_POLLS = 64;

            private boolean enabled = false;
            private String aeronDirectory = "";
            private String archiveControlRequestChannel = "aeron:ipc?term-length=64k";
            private String archiveControlResponseChannel = "aeron:ipc?term-length=64k";
            private String replayChannel = "aeron:ipc?term-length=64k";
            private int replayStreamId = DEFAULT_REPLAY_STREAM_ID;
            private long pollParkNanos = DEFAULT_POLL_PARK_NANOS;
            private int fragmentLimit = DEFAULT_FRAGMENT_LIMIT;
            private long recordingLookupParkMs = DEFAULT_RECORDING_LOOKUP_PARK_MS;
            private int cursorFlushEvery = DEFAULT_CURSOR_FLUSH_EVERY;
            private int venueRouteMaxInFlight = DEFAULT_VENUE_ROUTE_MAX_IN_FLIGHT;
            private int backlogThrottlePendingRouteThreshold =
                    DEFAULT_BACKLOG_THROTTLE_PENDING_ROUTE_THRESHOLD;
            private int backlogThrottleErOfferQueueDepthThreshold =
                    DEFAULT_BACKLOG_THROTTLE_ER_OFFER_QUEUE_DEPTH_THRESHOLD;
            private int backlogThrottleMaxInFlight = DEFAULT_BACKLOG_THROTTLE_MAX_IN_FLIGHT;
            private int backlogThrottleErSoftCapPermitMultiplier =
                    DEFAULT_BACKLOG_THROTTLE_ER_SOFT_CAP_PERMIT_MULTIPLIER;
            private int backlogThrottleErExhaustedCapPermitMultiplier =
                    DEFAULT_BACKLOG_THROTTLE_ER_EXHAUSTED_CAP_PERMIT_MULTIPLIER;
            private long backlogThrottleParkNanos = DEFAULT_BACKLOG_THROTTLE_PARK_NANOS;
            private int replayThreadPriority = DEFAULT_REPLAY_THREAD_PRIORITY;
            private int replayIdleTailPolls = DEFAULT_REPLAY_IDLE_TAIL_POLLS;
            private long replayLagAdaptiveExcessLagBytesThreshold =
                    DEFAULT_REPLAY_LAG_ADAPTIVE_EXCESS_LAG_BYTES_THRESHOLD;
            private int replayLagAdaptivePendingRoutesThreshold =
                    DEFAULT_REPLAY_LAG_ADAPTIVE_PENDING_ROUTES_THRESHOLD;
            private int replayLagAdaptiveFragmentLimit = DEFAULT_REPLAY_LAG_ADAPTIVE_FRAGMENT_LIMIT;
            private int replayLagAdaptiveIdleTailPolls = DEFAULT_REPLAY_LAG_ADAPTIVE_IDLE_TAIL_POLLS;
            private int venueRejectSubmitMaxAttempts = DEFAULT_VENUE_REJECT_SUBMIT_MAX_ATTEMPTS;
            private long venueRejectSubmitRetryBackoffMs = DEFAULT_VENUE_REJECT_SUBMIT_RETRY_BACKOFF_MS;

            public boolean isEnabled() { return enabled; }
            public void setEnabled(boolean enabled) { this.enabled = enabled; }

            public String getAeronDirectory() { return aeronDirectory; }
            public void setAeronDirectory(String aeronDirectory) {
                this.aeronDirectory = aeronDirectory == null ? "" : aeronDirectory.trim();
            }

            public String getArchiveControlRequestChannel() { return archiveControlRequestChannel; }
            public void setArchiveControlRequestChannel(String archiveControlRequestChannel) {
                this.archiveControlRequestChannel =
                        archiveControlRequestChannel == null || archiveControlRequestChannel.isBlank()
                                ? "aeron:ipc?term-length=64k"
                                : archiveControlRequestChannel.trim();
            }

            public String getArchiveControlResponseChannel() { return archiveControlResponseChannel; }
            public void setArchiveControlResponseChannel(String archiveControlResponseChannel) {
                this.archiveControlResponseChannel =
                        archiveControlResponseChannel == null || archiveControlResponseChannel.isBlank()
                                ? "aeron:ipc?term-length=64k"
                                : archiveControlResponseChannel.trim();
            }

            public String getReplayChannel() { return replayChannel; }
            public void setReplayChannel(String replayChannel) {
                this.replayChannel =
                        replayChannel == null || replayChannel.isBlank()
                                ? "aeron:ipc?term-length=64k"
                                : replayChannel.trim();
            }

            public int getReplayStreamId() { return replayStreamId; }
            public void setReplayStreamId(int replayStreamId) {
                if (replayStreamId == com.balh.oms.cluster.OmsClusterWireFormat.EVENTS_STREAM_ID) {
                    throw new IllegalArgumentException(
                            "oms.cluster.venue-egress.replay-stream-id must differ from EVENTS_STREAM_ID="
                                    + com.balh.oms.cluster.OmsClusterWireFormat.EVENTS_STREAM_ID);
                }
                this.replayStreamId = replayStreamId;
            }

            public long getPollParkNanos() { return pollParkNanos; }
            public void setPollParkNanos(long pollParkNanos) {
                this.pollParkNanos = Math.max(1_000L, pollParkNanos);
            }

            public int getFragmentLimit() { return fragmentLimit; }
            public void setFragmentLimit(int fragmentLimit) {
                this.fragmentLimit = Math.max(1, fragmentLimit);
            }

            public long getRecordingLookupParkMs() { return recordingLookupParkMs; }
            public void setRecordingLookupParkMs(long recordingLookupParkMs) {
                this.recordingLookupParkMs = Math.max(10L, recordingLookupParkMs);
            }

            /**
             * Fragments applied between Postgres UPSERTs to {@code oms_venue_egress_cursor}. Default
             * {@code 1} preserves the per-fragment durability contract (smallest crash replay window;
             * the venue dedupes redelivered {@code RouteOrder}s). Values {@code N>1} batch the cursor
             * advance to cut JDBC pressure on {@code cursorDrainExecutor} during pipelined ER
             * completion storms, widening the replay window to up to {@code N-1} fragments per crash.
             * Env: {@code OMS_CLUSTER_VENUE_EGRESS_CURSOR_FLUSH_EVERY}.
             */
            public int getCursorFlushEvery() { return cursorFlushEvery; }
            public void setCursorFlushEvery(int cursorFlushEvery) {
                this.cursorFlushEvery = Math.max(1, cursorFlushEvery);
            }

            /**
             * Max venue {@code RouteOrder}s the egress keeps in flight concurrently over the ordered
             * {@code RouteOrderStream}. {@code 1} (default) is the exact pre-pipelining serial path
             * (one fragment routed + ER-submitted + cursor-advanced before the next is dispatched);
             * {@code >1} activates the pipeline: dispatch in cluster-log order (preserving venue
             * price-time priority), complete acks asynchronously, and advance the cursor only over
             * the contiguous-completed prefix. Wider value ⇒ higher OMS→venue throughput and a wider
             * crash redelivery window (up to {@code N} duplicate {@code RouteOrder}s, deduped by the
             * venue — same trade-off as {@link #getCursorFlushEvery()}). See
             * {@code system-documentation/plans/oms-venue-egress-pipelining.md}.
             */
            public int getVenueRouteMaxInFlight() { return venueRouteMaxInFlight; }
            public void setVenueRouteMaxInFlight(int venueRouteMaxInFlight) {
                this.venueRouteMaxInFlight = Math.max(1, venueRouteMaxInFlight);
            }

            /**
             * Pending-route watermark that enables backlog throttling in the pipelined egress.
             * When {@link #getVenueRouteMaxInFlight()} is high, this watermark slows new dispatches
             * before queueing pressure reaches replay lag spikes.
             */
            public int getBacklogThrottlePendingRouteThreshold() {
                return backlogThrottlePendingRouteThreshold;
            }
            public void setBacklogThrottlePendingRouteThreshold(int threshold) {
                this.backlogThrottlePendingRouteThreshold = Math.max(1, threshold);
            }

            /**
             * ER-offer queue watermark (see {@link com.balh.oms.cluster.OmsClusterIngressClient}) that
             * enables backlog throttling in the pipelined egress.
             */
            public int getBacklogThrottleErOfferQueueDepthThreshold() {
                return backlogThrottleErOfferQueueDepthThreshold;
            }
            public void setBacklogThrottleErOfferQueueDepthThreshold(int threshold) {
                this.backlogThrottleErOfferQueueDepthThreshold = Math.max(1, threshold);
            }

            /**
             * Effective in-flight cap while backlog throttling is active. Must stay <=
             * {@link #getVenueRouteMaxInFlight()}.
             */
            public int getBacklogThrottleMaxInFlight() {
                return backlogThrottleMaxInFlight;
            }
            public void setBacklogThrottleMaxInFlight(int backlogThrottleMaxInFlight) {
                this.backlogThrottleMaxInFlight = Math.max(1, backlogThrottleMaxInFlight);
            }

            /**
             * When the ER offer queue is deep but venue permits remain, effective dispatch cap is
             * {@code max(backlogThrottleMaxInFlight, min(maxPendingFragments, maxInFlight * this))}.
             */
            public int getBacklogThrottleErSoftCapPermitMultiplier() {
                return backlogThrottleErSoftCapPermitMultiplier;
            }

            public void setBacklogThrottleErSoftCapPermitMultiplier(int multiplier) {
                this.backlogThrottleErSoftCapPermitMultiplier = Math.max(1, multiplier);
            }

            /**
             * When the ER offer queue is deep and venue permits are exhausted but pending routes
             * remain below {@link #getBacklogThrottlePendingRouteThreshold()}, effective dispatch
             * cap is {@code max(backlogThrottleMaxInFlight, min(maxPendingFragments, maxInFlight * this))}.
             * Lower than {@link #getBacklogThrottleErSoftCapPermitMultiplier()} so replay keeps
             * registering admits under ER pressure without the full soft cap that regressed fresh-book
             * 13k soak (split-branch at 4×).
             */
            public int getBacklogThrottleErExhaustedCapPermitMultiplier() {
                return backlogThrottleErExhaustedCapPermitMultiplier;
            }

            public void setBacklogThrottleErExhaustedCapPermitMultiplier(int multiplier) {
                this.backlogThrottleErExhaustedCapPermitMultiplier = Math.max(1, multiplier);
            }

            public long getBacklogThrottleParkNanos() { return backlogThrottleParkNanos; }
            public void setBacklogThrottleParkNanos(long backlogThrottleParkNanos) {
                this.backlogThrottleParkNanos = Math.max(1_000L, backlogThrottleParkNanos);
            }

            public int getReplayThreadPriority() { return replayThreadPriority; }
            public void setReplayThreadPriority(int replayThreadPriority) {
                this.replayThreadPriority =
                        Math.clamp(replayThreadPriority, Thread.MIN_PRIORITY, Thread.MAX_PRIORITY);
            }

            public int getReplayIdleTailPolls() { return replayIdleTailPolls; }
            public void setReplayIdleTailPolls(int replayIdleTailPolls) {
                this.replayIdleTailPolls = Math.max(1, replayIdleTailPolls);
            }

            public long getReplayLagAdaptiveExcessLagBytesThreshold() {
                return replayLagAdaptiveExcessLagBytesThreshold;
            }

            public void setReplayLagAdaptiveExcessLagBytesThreshold(long replayLagAdaptiveExcessLagBytesThreshold) {
                this.replayLagAdaptiveExcessLagBytesThreshold =
                        replayLagAdaptiveExcessLagBytesThreshold > 0L
                                ? replayLagAdaptiveExcessLagBytesThreshold
                                : DEFAULT_REPLAY_LAG_ADAPTIVE_EXCESS_LAG_BYTES_THRESHOLD;
            }

            public int getReplayLagAdaptivePendingRoutesThreshold() {
                return replayLagAdaptivePendingRoutesThreshold;
            }

            public void setReplayLagAdaptivePendingRoutesThreshold(int replayLagAdaptivePendingRoutesThreshold) {
                this.replayLagAdaptivePendingRoutesThreshold =
                        Math.max(1, replayLagAdaptivePendingRoutesThreshold);
            }

            public int getReplayLagAdaptiveFragmentLimit() { return replayLagAdaptiveFragmentLimit; }
            public void setReplayLagAdaptiveFragmentLimit(int replayLagAdaptiveFragmentLimit) {
                this.replayLagAdaptiveFragmentLimit =
                        replayLagAdaptiveFragmentLimit > 0
                                ? replayLagAdaptiveFragmentLimit
                                : DEFAULT_REPLAY_LAG_ADAPTIVE_FRAGMENT_LIMIT;
            }

            public int getReplayLagAdaptiveIdleTailPolls() { return replayLagAdaptiveIdleTailPolls; }
            public void setReplayLagAdaptiveIdleTailPolls(int replayLagAdaptiveIdleTailPolls) {
                this.replayLagAdaptiveIdleTailPolls = Math.max(1, replayLagAdaptiveIdleTailPolls);
            }

            public int getVenueRejectSubmitMaxAttempts() { return venueRejectSubmitMaxAttempts; }
            public void setVenueRejectSubmitMaxAttempts(int venueRejectSubmitMaxAttempts) {
                this.venueRejectSubmitMaxAttempts = Math.max(1, venueRejectSubmitMaxAttempts);
            }

            public long getVenueRejectSubmitRetryBackoffMs() { return venueRejectSubmitRetryBackoffMs; }
            public void setVenueRejectSubmitRetryBackoffMs(long venueRejectSubmitRetryBackoffMs) {
                this.venueRejectSubmitRetryBackoffMs = Math.max(1L, venueRejectSubmitRetryBackoffMs);
            }
        }

        /**
         * Phase B: tails balh-venue cluster {@code VenueResolutionEvent} fragments and submits
         * {@link com.balh.oms.cluster.ApplyVenueResolutionCommand} to the OMS cluster.
         */
        public static class VenueResolver {

            private static final long DEFAULT_POLL_PARK_NANOS = 1_000_000L;
            private static final int DEFAULT_FRAGMENT_LIMIT = 64;
            private static final long DEFAULT_RECORDING_LOOKUP_PARK_MS = 100L;
            private static final int DEFAULT_REPLAY_STREAM_ID = 4326;
            private static final long DEFAULT_OFFER_TIMEOUT_MS = 30_000L;

            private boolean enabled = false;
            /** Aeron directory of the balh-venue MediaDriver (not the OMS cluster driver). */
            private String venueAeronDirectory = "";
            private String archiveControlRequestChannel = "aeron:ipc?term-length=64k";
            private String archiveControlResponseChannel = "aeron:ipc?term-length=64k";
            private String replayChannel = "aeron:ipc?term-length=64k";
            private int replayStreamId = DEFAULT_REPLAY_STREAM_ID;
            private long pollParkNanos = DEFAULT_POLL_PARK_NANOS;
            private int fragmentLimit = DEFAULT_FRAGMENT_LIMIT;
            private long recordingLookupParkMs = DEFAULT_RECORDING_LOOKUP_PARK_MS;
            private long offerTimeoutMs = DEFAULT_OFFER_TIMEOUT_MS;

            public boolean isEnabled() { return enabled; }
            public void setEnabled(boolean enabled) { this.enabled = enabled; }

            public String getVenueAeronDirectory() { return venueAeronDirectory; }
            public void setVenueAeronDirectory(String venueAeronDirectory) {
                this.venueAeronDirectory = venueAeronDirectory == null ? "" : venueAeronDirectory.trim();
            }

            public String getArchiveControlRequestChannel() { return archiveControlRequestChannel; }
            public void setArchiveControlRequestChannel(String channel) {
                this.archiveControlRequestChannel =
                        channel == null || channel.isBlank() ? "aeron:ipc?term-length=64k" : channel.trim();
            }

            public String getArchiveControlResponseChannel() { return archiveControlResponseChannel; }
            public void setArchiveControlResponseChannel(String channel) {
                this.archiveControlResponseChannel =
                        channel == null || channel.isBlank() ? "aeron:ipc?term-length=64k" : channel.trim();
            }

            public String getReplayChannel() { return replayChannel; }
            public void setReplayChannel(String replayChannel) {
                this.replayChannel =
                        replayChannel == null || replayChannel.isBlank()
                                ? "aeron:ipc?term-length=64k"
                                : replayChannel.trim();
            }

            public int getReplayStreamId() { return replayStreamId; }
            public void setReplayStreamId(int replayStreamId) {
                this.replayStreamId = replayStreamId;
            }

            public long getPollParkNanos() { return pollParkNanos; }
            public void setPollParkNanos(long pollParkNanos) {
                this.pollParkNanos = Math.max(1_000L, pollParkNanos);
            }

            public int getFragmentLimit() { return fragmentLimit; }
            public void setFragmentLimit(int fragmentLimit) {
                this.fragmentLimit = Math.max(1, fragmentLimit);
            }

            public long getRecordingLookupParkMs() { return recordingLookupParkMs; }
            public void setRecordingLookupParkMs(long recordingLookupParkMs) {
                this.recordingLookupParkMs = Math.max(10L, recordingLookupParkMs);
            }

            public long getOfferTimeoutMs() { return offerTimeoutMs; }
            public void setOfferTimeoutMs(long offerTimeoutMs) {
                this.offerTimeoutMs = Math.max(1_000L, offerTimeoutMs);
            }
        }

        /**
         * Golden-copy order-state reconciliation against the in-house venue. On the
         * {@code oms-venue-egress} JVM (which already holds the venue gRPC stub and the cluster
         * ingress client), the {@code VenueOrderReconciler} periodically asks the venue — the
         * authoritative source for resting-order state — which of OMS's WORKING venue-routed orders
         * are still live, and terminates the ones the venue has dropped (e.g. rejected on replay
         * after a matching-semantics change), so OMS state self-heals instead of drifting.
         *
         * <p>{@link #minOrderAgeMs} is the race guard: a venue fill propagates to OMS via the
         * resolver tail, so only orders that have been WORKING longer than the resolver's worst-case
         * lag are reconciled. A still-WORKING order older than that, which the venue reports NOT_LIVE,
         * has no in-flight fill and is a genuine orphan.
         */
        public static class VenueReconciler {

            private static final long DEFAULT_POLL_INTERVAL_MS = 60_000L;
            private static final long DEFAULT_INITIAL_DELAY_MS = 15_000L;
            private static final long DEFAULT_MIN_ORDER_AGE_MS = 120_000L;
            private static final int DEFAULT_MAX_ORDERS_PER_PASS = 200;
            private static final long DEFAULT_QUERY_TIMEOUT_MS = 5_000L;

            private boolean enabled = false;
            private long pollIntervalMs = DEFAULT_POLL_INTERVAL_MS;
            private long initialDelayMs = DEFAULT_INITIAL_DELAY_MS;
            private long minOrderAgeMs = DEFAULT_MIN_ORDER_AGE_MS;
            private int maxOrdersPerPass = DEFAULT_MAX_ORDERS_PER_PASS;
            private long queryTimeoutMs = DEFAULT_QUERY_TIMEOUT_MS;

            public boolean isEnabled() { return enabled; }
            public void setEnabled(boolean enabled) { this.enabled = enabled; }

            public long getPollIntervalMs() { return pollIntervalMs; }
            public void setPollIntervalMs(long pollIntervalMs) {
                this.pollIntervalMs = Math.max(1_000L, pollIntervalMs);
            }

            public long getInitialDelayMs() { return initialDelayMs; }
            public void setInitialDelayMs(long initialDelayMs) {
                this.initialDelayMs = Math.max(0L, initialDelayMs);
            }

            public long getMinOrderAgeMs() { return minOrderAgeMs; }
            public void setMinOrderAgeMs(long minOrderAgeMs) {
                this.minOrderAgeMs = Math.max(0L, minOrderAgeMs);
            }

            public int getMaxOrdersPerPass() { return maxOrdersPerPass; }
            public void setMaxOrdersPerPass(int maxOrdersPerPass) {
                this.maxOrdersPerPass = Math.max(1, maxOrdersPerPass);
            }

            public long getQueryTimeoutMs() { return queryTimeoutMs; }
            public void setQueryTimeoutMs(long queryTimeoutMs) {
                this.queryTimeoutMs = Math.max(100L, queryTimeoutMs);
            }
        }

        /**
         * Phase 2 of the Aeron Cluster substrate plan: configuration for {@code OmsPostgresProjector}.
         *
         * <p>Used only by JVMs running the {@code oms-postgres-projector} profile. The projector connects
         * to the cluster member's local Aeron MediaDriver via {@link #aeronDirectory}, opens an
         * {@code AeronArchive} session via {@link #archiveControlRequestChannel} /
         * {@link #archiveControlResponseChannel}, locates the recording for
         * {@code OmsClusterWireFormat.EVENTS_CHANNEL}/{@code EVENTS_STREAM_ID}, and replays from the
         * cursor stored in {@code aeron_projector_cursor}.
         */
        public static class Projector {

            /** Default poll cadence inside the projector replay loop (live tail). */
            private static final long DEFAULT_POLL_PARK_NANOS = 1_000L;

            /**
             * Default fragments per {@code replay.poll} pass. Matches venue-egress (4096 @ 16k RPS):
             * at 5k–10k admits/s a limit of 64 caps drain at ~78 Postgres COMMITs/s and
             * {@code projector_wall_lag_ms} climbs even when {@code ingress_cluster_accept} stays
             * sub-ms. {@link com.balh.oms.projector.OmsPostgresProjector#effectiveFragmentLimit}
             * floors configured values below 4096 on the hot replay path.
             */
            private static final int DEFAULT_FRAGMENT_LIMIT = 4096;

            /**
             * Safety cap on fragments per poll-batch COMMIT at live tail. Larger batches amortise
             * {@code oms_projector_poll_batch_commit_seconds} overhead that otherwise caps drain
             * when ingress outruns replay.
             */
            private static final int DEFAULT_MAX_FRAGMENTS_PER_COMMIT = 8192;

            /**
             * When {@link #catchUpLagThresholdMs} is exceeded, the replay loop uses this larger
             * commit cap, spin-yields instead of park, and skips recording-walk metadata until lag
             * falls below threshold.
             */
            private static final int DEFAULT_CATCH_UP_MAX_FRAGMENTS_PER_COMMIT = 16_384;

            /**
             * Wall-lag ms (cluster admit → projector applied) above which the replay loop enters
             * catch-up fast path: larger commit batches, spin instead of park, skip Archive
             * {@code listRecordings} on idle polls.
             */
            private static final long DEFAULT_CATCH_UP_LAG_THRESHOLD_MS = 1_000L;

            /**
             * Bounded queue of poll batches awaiting the apply thread. Lets the replay thread keep
             * {@code replay.poll}ing while Postgres COMMIT runs — overlap removes poll time from the
             * per-batch critical path at 10k admits/s.
             */
            /** Depth for async apply queue — must cover soak burst (30s @ 16k/s) while COMMIT runs. */
            private static final int DEFAULT_APPLY_QUEUE_BATCH_CAPACITY = 96;

            /**
             * Concurrent apply threads that drain the queue in parallel. Each thread holds
             * its own JDBC connection, so 2 threads double Postgres INSERT throughput when
             * a single connection is bottlenecked on INSERT + index maintenance (~143µs/row).
             * Cursor advance is ordered: the cursor only advances to position N when all
             * batches with positions &lt; N have committed.
             */
            private static final int DEFAULT_APPLY_THREAD_COUNT = 2;

            /**
             * {@link Thread#setPriority(int)} for {@code oms-postgres-projector-replay} and
             * {@code oms-postgres-projector-apply}. Default {@link Thread#MAX_PRIORITY} keeps the
             * Aeron drain ahead of generic worker pools on pop bench hosts.
             */
            private static final int DEFAULT_REPLAY_THREAD_PRIORITY = Thread.MAX_PRIORITY;

            /** Default backoff between {@code listRecordings} retries while waiting for the recording to appear. */
            private static final long DEFAULT_RECORDING_LOOKUP_PARK_MS = 100L;

            private boolean enabled = false;
            private String aeronDirectory = "";
            private String archiveControlRequestChannel = "aeron:ipc?term-length=64k";
            private String archiveControlResponseChannel = "aeron:ipc?term-length=64k";
            private String replayChannel = "aeron:ipc?term-length=64k";
            private int replayStreamId = 4321;
            private long pollParkNanos = DEFAULT_POLL_PARK_NANOS;
            private int fragmentLimit = DEFAULT_FRAGMENT_LIMIT;
            private int maxFragmentsPerCommit = DEFAULT_MAX_FRAGMENTS_PER_COMMIT;
            private int catchUpMaxFragmentsPerCommit = DEFAULT_CATCH_UP_MAX_FRAGMENTS_PER_COMMIT;
            private long catchUpLagThresholdMs = DEFAULT_CATCH_UP_LAG_THRESHOLD_MS;
            private int applyQueueBatchCapacity = DEFAULT_APPLY_QUEUE_BATCH_CAPACITY;
            private int applyThreadCount = DEFAULT_APPLY_THREAD_COUNT;
            private int replayThreadPriority = DEFAULT_REPLAY_THREAD_PRIORITY;
            private long recordingLookupParkMs = DEFAULT_RECORDING_LOOKUP_PARK_MS;

            public boolean isEnabled() { return enabled; }
            public void setEnabled(boolean enabled) { this.enabled = enabled; }

            public String getAeronDirectory() { return aeronDirectory; }
            public void setAeronDirectory(String aeronDirectory) {
                this.aeronDirectory = aeronDirectory == null ? "" : aeronDirectory.trim();
            }

            public String getArchiveControlRequestChannel() { return archiveControlRequestChannel; }
            public void setArchiveControlRequestChannel(String archiveControlRequestChannel) {
                this.archiveControlRequestChannel =
                        archiveControlRequestChannel == null || archiveControlRequestChannel.isBlank()
                                ? "aeron:ipc?term-length=64k"
                                : archiveControlRequestChannel.trim();
            }

            public String getArchiveControlResponseChannel() { return archiveControlResponseChannel; }
            public void setArchiveControlResponseChannel(String archiveControlResponseChannel) {
                this.archiveControlResponseChannel =
                        archiveControlResponseChannel == null || archiveControlResponseChannel.isBlank()
                                ? "aeron:ipc?term-length=64k"
                                : archiveControlResponseChannel.trim();
            }

            public String getReplayChannel() { return replayChannel; }
            public void setReplayChannel(String replayChannel) {
                this.replayChannel =
                        replayChannel == null || replayChannel.isBlank()
                                ? "aeron:ipc?term-length=64k"
                                : replayChannel.trim();
            }

            public int getReplayStreamId() { return replayStreamId; }
            public void setReplayStreamId(int replayStreamId) {
                if (replayStreamId == com.balh.oms.cluster.OmsClusterWireFormat.EVENTS_STREAM_ID) {
                    throw new IllegalArgumentException(
                            "oms.cluster.projector.replay-stream-id must differ from EVENTS_STREAM_ID="
                                    + com.balh.oms.cluster.OmsClusterWireFormat.EVENTS_STREAM_ID);
                }
                this.replayStreamId = replayStreamId;
            }

            public long getPollParkNanos() { return pollParkNanos; }
            public void setPollParkNanos(long pollParkNanos) {
                this.pollParkNanos = Math.max(1_000L, pollParkNanos);
            }

            public int getFragmentLimit() { return fragmentLimit; }
            public void setFragmentLimit(int fragmentLimit) {
                this.fragmentLimit = Math.max(1, fragmentLimit);
            }

            public int getMaxFragmentsPerCommit() { return maxFragmentsPerCommit; }
            public void setMaxFragmentsPerCommit(int maxFragmentsPerCommit) {
                this.maxFragmentsPerCommit = Math.max(1, maxFragmentsPerCommit);
            }

            public int getCatchUpMaxFragmentsPerCommit() { return catchUpMaxFragmentsPerCommit; }
            public void setCatchUpMaxFragmentsPerCommit(int catchUpMaxFragmentsPerCommit) {
                this.catchUpMaxFragmentsPerCommit = Math.max(1, catchUpMaxFragmentsPerCommit);
            }

            public long getCatchUpLagThresholdMs() { return catchUpLagThresholdMs; }
            public void setCatchUpLagThresholdMs(long catchUpLagThresholdMs) {
                this.catchUpLagThresholdMs = Math.max(1L, catchUpLagThresholdMs);
            }

            public int getApplyQueueBatchCapacity() { return applyQueueBatchCapacity; }
            public void setApplyQueueBatchCapacity(int applyQueueBatchCapacity) {
                this.applyQueueBatchCapacity = Math.max(1, applyQueueBatchCapacity);
            }

            public int getApplyThreadCount() { return applyThreadCount; }
            public void setApplyThreadCount(int applyThreadCount) {
                this.applyThreadCount = Math.clamp(applyThreadCount, 1, 8);
            }

            public int getReplayThreadPriority() { return replayThreadPriority; }
            public void setReplayThreadPriority(int replayThreadPriority) {
                this.replayThreadPriority =
                        Math.clamp(replayThreadPriority, Thread.MIN_PRIORITY, Thread.MAX_PRIORITY);
            }

            public long getRecordingLookupParkMs() { return recordingLookupParkMs; }
            public void setRecordingLookupParkMs(long recordingLookupParkMs) {
                this.recordingLookupParkMs = Math.max(10L, recordingLookupParkMs);
            }
        }

        public static class Client {

            /** Default per-call timeout for {@code submitAcceptOrder}. */
            private static final long DEFAULT_SUBMIT_TIMEOUT_MS = 5_000L;
            /** Default Aeron message timeout (single-node startup needs >= 5s in cold-start). */
            private static final long DEFAULT_AERON_MESSAGE_TIMEOUT_MS = 10_000L;
            /** Default initial-connect retry budget. */
            private static final long DEFAULT_CONNECT_TIMEOUT_MS = 30_000L;
            /** Park between back-pressure retries on cluster offer. */
            private static final long DEFAULT_OFFER_BACKPRESSURE_PARK_NANOS = 100_000L;
            /** Park between egress poll passes when waiting for our correlation id. */
            private static final long DEFAULT_EGRESS_POLL_PARK_NANOS = 100_000L;
            /**
             * Default heartbeat interval. Aeron Cluster {@code ConsensusModule} default
             * {@code sessionTimeoutNs} is 5s; we keep the session warm well below that to
             * survive idle periods between {@code submitAcceptOrder} calls (e.g. a quiet
             * trading night, a stalled producer, or a Spring context that loaded the bean
             * but hasn't sent a command yet).
             */
            private static final long DEFAULT_HEARTBEAT_INTERVAL_MS = 1_000L;

            private boolean enabled = false;
            private com.balh.oms.cluster.ClusterClientRole role =
                    com.balh.oms.cluster.ClusterClientRole.FULL;
            private String aeronDirectory = "";
            private String ingressEndpoints = "0=localhost:20110";
            private String ingressChannel = "aeron:udp?endpoint=localhost:0";
            private String egressChannel = "aeron:udp?endpoint=localhost:0";
            private long submitTimeoutMs = DEFAULT_SUBMIT_TIMEOUT_MS;
            private long messageTimeoutMs = DEFAULT_AERON_MESSAGE_TIMEOUT_MS;
            private long connectTimeoutMs = DEFAULT_CONNECT_TIMEOUT_MS;
            private long offerBackpressureParkNanos = DEFAULT_OFFER_BACKPRESSURE_PARK_NANOS;
            private long egressPollParkNanos = DEFAULT_EGRESS_POLL_PARK_NANOS;
            private long heartbeatIntervalMs = DEFAULT_HEARTBEAT_INTERVAL_MS;

            public boolean isEnabled() { return enabled; }
            public void setEnabled(boolean enabled) { this.enabled = enabled; }

            public com.balh.oms.cluster.ClusterClientRole getRole() {
                return role == null ? com.balh.oms.cluster.ClusterClientRole.FULL : role;
            }

            public void setRole(com.balh.oms.cluster.ClusterClientRole role) {
                this.role = role == null ? com.balh.oms.cluster.ClusterClientRole.FULL : role;
            }

            public String getAeronDirectory() { return aeronDirectory; }
            public void setAeronDirectory(String aeronDirectory) {
                this.aeronDirectory = aeronDirectory == null ? "" : aeronDirectory.trim();
            }

            public String getIngressEndpoints() { return ingressEndpoints; }
            public void setIngressEndpoints(String ingressEndpoints) {
                this.ingressEndpoints =
                        ingressEndpoints == null || ingressEndpoints.isBlank()
                                ? "0=localhost:20110"
                                : ingressEndpoints.trim();
            }

            public String getIngressChannel() { return ingressChannel; }
            public void setIngressChannel(String ingressChannel) {
                this.ingressChannel =
                        ingressChannel == null || ingressChannel.isBlank()
                                ? "aeron:udp?endpoint=localhost:0"
                                : ingressChannel.trim();
            }

            public String getEgressChannel() { return egressChannel; }
            public void setEgressChannel(String egressChannel) {
                this.egressChannel =
                        egressChannel == null || egressChannel.isBlank()
                                ? "aeron:udp?endpoint=localhost:0"
                                : egressChannel.trim();
            }

            public long getSubmitTimeoutMs() { return submitTimeoutMs; }
            public void setSubmitTimeoutMs(long submitTimeoutMs) {
                this.submitTimeoutMs = Math.max(100L, submitTimeoutMs);
            }

            public long getMessageTimeoutMs() { return messageTimeoutMs; }
            public void setMessageTimeoutMs(long messageTimeoutMs) {
                this.messageTimeoutMs = Math.max(1_000L, messageTimeoutMs);
            }

            public long getConnectTimeoutMs() { return connectTimeoutMs; }
            public void setConnectTimeoutMs(long connectTimeoutMs) {
                this.connectTimeoutMs = Math.max(1_000L, connectTimeoutMs);
            }

            public long getOfferBackpressureParkNanos() { return offerBackpressureParkNanos; }
            public void setOfferBackpressureParkNanos(long offerBackpressureParkNanos) {
                this.offerBackpressureParkNanos = Math.max(1_000L, offerBackpressureParkNanos);
            }

            public long getEgressPollParkNanos() { return egressPollParkNanos; }
            public void setEgressPollParkNanos(long egressPollParkNanos) {
                this.egressPollParkNanos = Math.max(1_000L, egressPollParkNanos);
            }

            public long getHeartbeatIntervalMs() { return heartbeatIntervalMs; }
            public void setHeartbeatIntervalMs(long heartbeatIntervalMs) {
                this.heartbeatIntervalMs = Math.max(50L, heartbeatIntervalMs);
            }

            private final AdmitBatch admitBatch = new AdmitBatch();
            private final ErOffer erOffer = new ErOffer();

            public AdmitBatch getAdmitBatch() { return admitBatch; }

            public ErOffer getErOffer() { return erOffer; }

            /**
             * Phase 4 Tier 2.5 phase E-3b — per-shard overrides for the few cluster-client values
             * that genuinely differ across the {@code N} clusters one ingress JVM can talk to:
             * {@code aeronDirectory} (each cluster has its own Aeron media driver) and
             * {@code ingressEndpoints} (each cluster's consensus members listen on different
             * ports / hosts). All other knobs ({@code messageTimeoutMs}, {@code admitBatch}, …)
             * are deployment-wide and reuse the flat values above.
             *
             * <p>At {@code oms.shard.count=1} this list is empty and the flat values define the
             * single client &mdash; byte-identical to E-3a / E-2.
             *
             * <p>At {@code oms.shard.count>1} the list <em>must</em> contain {@code N} entries
             * with ids {@code [0, N)} and each entry <em>must</em> set {@code aeronDirectory} and
             * {@code ingressEndpoints} (otherwise two shards would point at the same cluster and
             * silently double-route). {@code OmsClusterClientsConfiguration} validates this at
             * factory build time.
             */
            private final java.util.List<ShardOverride> shards = new java.util.ArrayList<>();

            public java.util.List<ShardOverride> getShards() { return shards; }
            public void setShards(java.util.List<ShardOverride> shards) {
                this.shards.clear();
                if (shards != null) {
                    this.shards.addAll(shards);
                }
            }

            /**
             * Per-shard override for the cluster-client. Only the fields that genuinely differ
             * across clusters are settable; the remainder fall back to the flat
             * {@link Client} values. {@link #id} is required so the factory can validate the
             * full {@code [0, oms.shard.count)} coverage and so YAML order is irrelevant.
             */
            public static class ShardOverride {
                /** Shard id. Required; the factory cross-checks against {@code oms.shard.count}. */
                private int id = -1;
                /**
                 * Aeron media-driver directory for this shard's cluster client. Must be set when
                 * {@code oms.shard.count > 1} so two shards' clients do not share a driver and
                 * silently mix sessions.
                 */
                private String aeronDirectory = "";
                /**
                 * Cluster ingress endpoints for this shard. Must be set when
                 * {@code oms.shard.count > 1}; format mirrors {@link Client#getIngressEndpoints()}
                 * (e.g. {@code 0=host:port[,1=host:port,...]}).
                 */
                private String ingressEndpoints = "";

                public int getId() { return id; }
                public void setId(int id) { this.id = id; }

                public String getAeronDirectory() { return aeronDirectory; }
                public void setAeronDirectory(String aeronDirectory) {
                    this.aeronDirectory = aeronDirectory == null ? "" : aeronDirectory.trim();
                }

                public String getIngressEndpoints() { return ingressEndpoints; }
                public void setIngressEndpoints(String ingressEndpoints) {
                    this.ingressEndpoints =
                            ingressEndpoints == null ? "" : ingressEndpoints.trim();
                }
            }

            /**
             * Phase 4 Tier 2.5 phase D-6: configuration for the
             * {@link com.balh.oms.cluster.OmsClusterIngressClient}'s admit-batcher daemon. When
             * enabled, calls to {@code submitAcceptOrder} enqueue into a bounded MPSC queue and a
             * single daemon thread drains up to {@link #getMaxBatchSize()} entries per pass and
             * offers them as one {@link com.balh.oms.cluster.BatchAcceptOrderCommand}. When
             * disabled (default), {@code submitAcceptOrder} is the unchanged single-message offer
             * path from slice 4n. Per-request {@code CompletableFuture} demux on the response side
             * is identical in both paths — the only thing that changes is how the wire frame is
             * shaped at the cluster ingress.
             *
             * <p>Why this exists: D-9 reduced ingress hot-path Postgres I/O to zero and lifted
             * peak rps from 20 874 → 57 282; the residual wall is the single-leader cluster's
             * {@code onSessionMessage} processing rate, which batching amortises. The D-7 Tomcat
             * thread-bump experiment (see {@code oms/docs/runbooks/local-multi-jvm-bench.md}
             * § Tier 2.5 phase D-7 falsification) confirmed the wall is on the cluster, not on
             * Tomcat.
             */
            public static class AdmitBatch {

                /** Default off — slice 4n single-message path is the safe fallback. */
                private static final boolean DEFAULT_ENABLED = false;
                /**
                 * Default 32 admits per batch. Pop! bench with admit-batch enabled and
                 * {@code ingress_cluster_accept_ms} sub-2 ms showed cluster accept is no longer the
                 * dominant drain; larger batches amortise ingress offer / lock trips without growing
                 * per-batch decode cost materially (~360-byte AcceptOrderCommand bodies).
                 * Tunable via {@code OMS_CLUSTER_CLIENT_ADMIT_BATCH_MAX_SIZE}.
                 */
                private static final int DEFAULT_MAX_BATCH_SIZE = 32;
                /**
                 * Default 50 µs flush interval. Below the 17.5 µs per-admit cluster CPU cost
                 * observed at 57 k rps (so the daemon's tick is fast enough to keep up with peak
                 * arrival), but generous enough that under bursty traffic the daemon can let
                 * batches accumulate to {@link #getMaxBatchSize()} before offering. Lower is more
                 * aggressive (smaller batches, lower latency); higher is more amortising (larger
                 * batches, higher latency).
                 */
                private static final long DEFAULT_FLUSH_INTERVAL_NANOS = 50_000L;
                /**
                 * Default queue capacity. Sized for ~2× the per-ingress in-flight admit count
                 * (Tomcat 200 threads × 2 ingresses = 400 max in flight on Pop! D-9). Bounded so
                 * a stalled daemon back-pressures rather than OOM-ing. The submit thread parks
                 * on {@link #getEnqueueParkNanos()} ns retries when the queue is full.
                 */
                private static final int DEFAULT_QUEUE_CAPACITY = 8_192;
                /** Park duration for the submit thread when the queue is full. */
                private static final long DEFAULT_ENQUEUE_PARK_NANOS = 1_000L;

                private boolean enabled = DEFAULT_ENABLED;
                private int maxBatchSize = DEFAULT_MAX_BATCH_SIZE;
                private long flushIntervalNanos = DEFAULT_FLUSH_INTERVAL_NANOS;
                private int queueCapacity = DEFAULT_QUEUE_CAPACITY;
                private long enqueueParkNanos = DEFAULT_ENQUEUE_PARK_NANOS;

                public boolean isEnabled() { return enabled; }
                public void setEnabled(boolean enabled) { this.enabled = enabled; }

                public int getMaxBatchSize() { return maxBatchSize; }
                public void setMaxBatchSize(int maxBatchSize) {
                    this.maxBatchSize = Math.max(
                            1, Math.min(com.balh.oms.cluster.BatchAcceptOrderCommand.MAX_COUNT, maxBatchSize));
                }

                public long getFlushIntervalNanos() { return flushIntervalNanos; }
                public void setFlushIntervalNanos(long flushIntervalNanos) {
                    this.flushIntervalNanos = Math.max(1_000L, flushIntervalNanos);
                }

                public int getQueueCapacity() { return queueCapacity; }
                public void setQueueCapacity(int queueCapacity) {
                    this.queueCapacity = Math.max(64, queueCapacity);
                }

                public long getEnqueueParkNanos() { return enqueueParkNanos; }
                public void setEnqueueParkNanos(long enqueueParkNanos) {
                    this.enqueueParkNanos = Math.max(100L, enqueueParkNanos);
                }
            }

            /**
             * Configuration for the {@link com.balh.oms.cluster.OmsClusterIngressClient} ER-offer
             * daemon: bounded queue, burst drain per {@code clientLock} hold, and park intervals.
             * Wire format has no batch ER frame — this is transport coalescing only.
             *
             * <p>Tunable via {@code oms.cluster.client.er-offer.*} /
             * {@code OMS_CLUSTER_CLIENT_ER_OFFER_*} env vars on ingress-replica JVMs.
             */
            public static class ErOffer {

                private static final int DEFAULT_QUEUE_CAPACITY = 8_192;
                /**
                 * Default frames offered per lock acquisition. Raised from 64 for bench throughput
                 * when venue-egress virtual-thread completions flood the ingress client at 400+
                 * routes/s.
                 */
                private static final int DEFAULT_MAX_PER_LOCK_PASS = 1_024;
                private static final long DEFAULT_DRAIN_INTERVAL_NANOS = 1_000L;
                private static final long DEFAULT_ENQUEUE_PARK_NANOS = 1_000L;
                /**
                 * ER_OFFER_ONLY: max {@link io.aeron.cluster.client.AeronCluster#pollEgress} rounds
                 * per interleave pass. Clears publication flow-control without a competing poller.
                 */
                private static final int DEFAULT_INTERLEAVE_POLL_CAP = 8;
                /**
                 * ER_OFFER_ONLY: poll egress every N successful offers during a burst (not every offer).
                 * Lower N drains cluster egress more often during ER floods without per-offer poll cost.
                 */
                private static final int DEFAULT_INTERLEAVE_POLL_EVERY_OFFERS = 4;
                private static final boolean DEFAULT_INTERLEAVE_LAG_AWARE_ENABLED = true;
                private static final long DEFAULT_INTERLEAVE_LAG_BYTES_THRESHOLD = 4_096L;
                private static final int DEFAULT_INTERLEAVE_LAG_AWARE_QUEUE_DEPTH_THRESHOLD = 256;
                private static final int DEFAULT_INTERLEAVE_LAG_AWARE_POLL_EVERY_OFFERS = 1;

                private int queueCapacity = DEFAULT_QUEUE_CAPACITY;
                private int maxPerLockPass = DEFAULT_MAX_PER_LOCK_PASS;
                private long drainIntervalNanos = DEFAULT_DRAIN_INTERVAL_NANOS;
                private long enqueueParkNanos = DEFAULT_ENQUEUE_PARK_NANOS;
                private int interleavePollCap = DEFAULT_INTERLEAVE_POLL_CAP;
                private int interleavePollEveryOffers = DEFAULT_INTERLEAVE_POLL_EVERY_OFFERS;
                private boolean interleaveLagAwareEnabled = DEFAULT_INTERLEAVE_LAG_AWARE_ENABLED;
                private long interleaveLagBytesThreshold = DEFAULT_INTERLEAVE_LAG_BYTES_THRESHOLD;
                private int interleaveLagAwareQueueDepthThreshold =
                        DEFAULT_INTERLEAVE_LAG_AWARE_QUEUE_DEPTH_THRESHOLD;
                private int interleaveLagAwarePollEveryOffers =
                        DEFAULT_INTERLEAVE_LAG_AWARE_POLL_EVERY_OFFERS;

                public int getQueueCapacity() { return queueCapacity; }
                public void setQueueCapacity(int queueCapacity) {
                    this.queueCapacity = Math.max(64, queueCapacity);
                }

                public int getMaxPerLockPass() { return maxPerLockPass; }
                public void setMaxPerLockPass(int maxPerLockPass) {
                    this.maxPerLockPass = Math.max(1, maxPerLockPass);
                }

                public long getDrainIntervalNanos() { return drainIntervalNanos; }
                public void setDrainIntervalNanos(long drainIntervalNanos) {
                    this.drainIntervalNanos = Math.max(1_000L, drainIntervalNanos);
                }

                public long getEnqueueParkNanos() { return enqueueParkNanos; }
                public void setEnqueueParkNanos(long enqueueParkNanos) {
                    this.enqueueParkNanos = Math.max(100L, enqueueParkNanos);
                }

                public int getInterleavePollCap() { return interleavePollCap; }
                public void setInterleavePollCap(int interleavePollCap) {
                    this.interleavePollCap = Math.max(1, interleavePollCap);
                }

                public int getInterleavePollEveryOffers() { return interleavePollEveryOffers; }
                public void setInterleavePollEveryOffers(int interleavePollEveryOffers) {
                    this.interleavePollEveryOffers = Math.max(1, interleavePollEveryOffers);
                }

                public boolean isInterleaveLagAwareEnabled() { return interleaveLagAwareEnabled; }
                public void setInterleaveLagAwareEnabled(boolean interleaveLagAwareEnabled) {
                    this.interleaveLagAwareEnabled = interleaveLagAwareEnabled;
                }

                public long getInterleaveLagBytesThreshold() { return interleaveLagBytesThreshold; }
                public void setInterleaveLagBytesThreshold(long interleaveLagBytesThreshold) {
                    this.interleaveLagBytesThreshold =
                            interleaveLagBytesThreshold > 0L
                                    ? interleaveLagBytesThreshold
                                    : DEFAULT_INTERLEAVE_LAG_BYTES_THRESHOLD;
                }

                public int getInterleaveLagAwareQueueDepthThreshold() {
                    return interleaveLagAwareQueueDepthThreshold;
                }
                public void setInterleaveLagAwareQueueDepthThreshold(int threshold) {
                    this.interleaveLagAwareQueueDepthThreshold = Math.max(1, threshold);
                }

                public int getInterleaveLagAwarePollEveryOffers() {
                    return interleaveLagAwarePollEveryOffers;
                }
                public void setInterleaveLagAwarePollEveryOffers(int interleaveLagAwarePollEveryOffers) {
                    this.interleaveLagAwarePollEveryOffers =
                            Math.max(1, interleaveLagAwarePollEveryOffers);
                }
            }
        }
    }
}
