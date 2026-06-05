package com.balh.oms.ingress.bench;

import org.HdrHistogram.Histogram;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Phase 4 slice 4k of {@code system-documentation/plans/oms-aeron-cluster-substrate.md}: a
 * concurrency-aware HTTP burst tool against the OMS ingress-replica
 * {@code POST /internal/v1/orders} endpoint.
 *
 * <p>The slice 4i {@code shoot-ingress-orders.sh} curl loop is serial — it limits its own
 * throughput to ~1 request per ~21 ms because each iteration spawns a curl process and JSON-
 * marshals the body in-shell. That made it impossible to tell whether the FIX-egress's
 * {@code ~21 ms} amortised cost is the egress saturation point or just the curl loop's
 * inter-request gap. This tool fixes that by:
 *
 * <ul>
 *   <li>JDK 21 {@link HttpClient} with a virtual-thread {@link Executors#newVirtualThreadPerTaskExecutor()}
 *       so we can run hundreds of in-flight requests cheaply.</li>
 *   <li>A {@link Semaphore} cap on in-flight requests (the burst tool's "concurrency" knob —
 *       slice 4k experiments at 50/100/200 to find the egress's actual ceiling).</li>
 *   <li>Optional RPS pacing on top of the in-flight cap; useful for long steady-state runs
 *       where you want a fixed offered load instead of as-fast-as-possible.</li>
 *   <li>Per-request HTTP RTT recorded into an {@link Histogram} so the printed
 *       p50/p95/p99/p999/max are lossless tail latencies, not Micrometer-bucket-rounded ones.
 *       Pair this with {@code summarize_cluster_pipeline_deltas.py} pre/post Prometheus scrapes
 *       (slice 4j) to read the per-event admit-to-NOS distribution that this load drives.</li>
 * </ul>
 *
 * <p>Usage (config-and-limits: every knob is an env var with a documented default):
 * <pre>
 *   OMS_INTERNAL_API_KEY=...     required; must match the running ingress-replica's secret
 *   OMS_BURST_URL                http://127.0.0.1:8088/internal/v1/orders
 *   OMS_URL                      http://127.0.0.1:8088  alias; /internal/v1/orders appended when
 *                                OMS_BURST_URL unset (wrapper scripts on pop use this name)
 *   OMS_BURST_URLS               comma-separated list, overrides OMS_BURST_URL when set; the
 *                                burst tool round-robins requests across the URLs. Use this to
 *                                drive N ingress-replicas without standing up an external load
 *                                balancer (slice 4m). Example:
 *                                {@code http://127.0.0.1:8088/internal/v1/orders,http://127.0.0.1:8095/internal/v1/orders}
 *   OMS_BURST_TOTAL              1000   total requests sent (steady-state phase)
 *   OMS_BURST_CONCURRENCY        50     max in-flight requests
 *   OMS_BURST_RPS_CAP            0      optional RPS cap (0 = unlimited / concurrency-bound)
 *   OMS_BURST_ACCOUNT_POOL       1      number of distinct accountId UUIDs to reuse
 *   OMS_BURST_INSTRUMENT         AAPL   FIX symbol
 *   OMS_BURST_QUANTITY           1
 *   OMS_BURST_LIMIT_PRICE        150
 *   OMS_BURST_REQUEST_TIMEOUT_S  30
 *   OMS_BURST_WARMUP             0      number of warmup requests whose latencies are discarded
 *   OMS_BURST_LEDGER_BALANCE_ID  (off)  slice 4p: when set, every burst order carries this
 *                                       ledgerBalanceId so ingress exercises the inflight-hold
 *                                       path (sync HTTP at MAX_SIZE=1, async outbox at
 *                                       OMS_LEDGER_INFLIGHT_ASYNC_ENABLED=true). Requires
 *                                       OMS_BURST_LEDGER_IDENTITY_ID to be set as well.
 *   OMS_BURST_LEDGER_BALANCE_POOL_SIZE
 *                                (off)  optional round-robin pool for ledgerBalanceId.
 *                                       Supports either comma-separated balance IDs directly,
 *                                       or integer size N (uses first N IDs from
 *                                       OMS_BURST_LEDGER_BALANCE_ID, which must then be CSV).
 *   OMS_BURST_LEDGER_IDENTITY_ID (off)  slice 4p: companion to OMS_BURST_LEDGER_BALANCE_ID.
 * </pre>
 *
 * <p>Run via the {@code bootRunBurst} Gradle task or
 * {@code java -cp ... com.balh.oms.ingress.bench.IngressBurstMain}.
 *
 * <p>Output is human-readable on stdout; pre/post Prometheus scrapes are deliberately not done
 * here so this tool can be wrapped by a shell script that already knows the cluster's per-role
 * Prometheus URLs (see {@code scripts/benchmark/burst-ingress-orders.sh}).
 *
 * <p>This is a benchmark utility, not a long-lived service: it does not retry on failure, does
 * not rate-limit retries, and does not back off — the goal is to measure the OMS ingress's
 * actual response under load, not to tolerate the OMS ingress being down.
 */
public final class IngressBurstMain {

    public static final String ENV_API_KEY = "OMS_INTERNAL_API_KEY";
    public static final String ENV_URL = "OMS_BURST_URL";
    /**
     * Slice 4m: comma-separated list of burst targets. When set, requests are distributed
     * round-robin across the URLs (request {@code i} goes to {@code urls[i % urls.size()]}).
     * Empty / unset falls back to the single {@link #ENV_URL} target.
     */
    public static final String ENV_URLS = "OMS_BURST_URLS";
    public static final String ENV_TOTAL = "OMS_BURST_TOTAL";
    public static final String ENV_CONCURRENCY = "OMS_BURST_CONCURRENCY";
    public static final String ENV_RPS_CAP = "OMS_BURST_RPS_CAP";
    public static final String ENV_ACCOUNT_POOL = "OMS_BURST_ACCOUNT_POOL";
    public static final String ENV_INSTRUMENT = "OMS_BURST_INSTRUMENT";
    public static final String ENV_QUANTITY = "OMS_BURST_QUANTITY";
    public static final String ENV_LIMIT_PRICE = "OMS_BURST_LIMIT_PRICE";
    public static final String ENV_REQUEST_TIMEOUT_S = "OMS_BURST_REQUEST_TIMEOUT_S";
    public static final String ENV_WARMUP = "OMS_BURST_WARMUP";
    /**
     * Slice 4p: optional ledgerBalanceId injected into every burst order. When set, ingress
     * exercises the buying-power inflight hold path ({@code OrderIngressService
     * .maybePlaceBuyLedgerInflightHold}) — sync HTTP at MAX_SIZE=1, async outbox at
     * {@code OMS_LEDGER_INFLIGHT_ASYNC_ENABLED=true}. Empty / unset preserves slice-4m behaviour
     * (no hold path; orders skip the hold branch entirely on the {@code ledgerBalanceId == null}
     * guard).
     */
    public static final String ENV_LEDGER_BALANCE_ID = "OMS_BURST_LEDGER_BALANCE_ID";
    public static final String ENV_LEDGER_BALANCE_POOL_SIZE = "OMS_BURST_LEDGER_BALANCE_POOL_SIZE";
    /**
     * Slice 4p: optional ledgerIdentityId. Required by ingress when {@code ledgerBalanceId} is
     * set ({@code OrderIngressService.maybeVerifyLedgerBalanceBinding} returns a 400 otherwise).
     * Operators must pair this with {@link #ENV_LEDGER_BALANCE_ID}.
     */
    public static final String ENV_LEDGER_IDENTITY_ID = "OMS_BURST_LEDGER_IDENTITY_ID";

    public static final String DEFAULT_URL = "http://127.0.0.1:8088/internal/v1/orders";
    public static final int DEFAULT_TOTAL = 1_000;
    public static final int DEFAULT_CONCURRENCY = 50;
    public static final int DEFAULT_RPS_CAP = 0;
    public static final int DEFAULT_ACCOUNT_POOL = 1;
    public static final String DEFAULT_INSTRUMENT = "AAPL";
    public static final String DEFAULT_QUANTITY = "1";
    public static final String DEFAULT_LIMIT_PRICE = "150";
    public static final int DEFAULT_REQUEST_TIMEOUT_S = 30;
    public static final int DEFAULT_WARMUP = 0;
    public static final String DEFAULT_LEDGER_BALANCE_ID = "";
    public static final String DEFAULT_LEDGER_BALANCE_POOL_SIZE = "";
    public static final String DEFAULT_LEDGER_IDENTITY_ID = "";

    /** Lossless histogram covers 100 µs .. 60 s with 3 sig digits — total memory ~1 MB. */
    private static final long HISTOGRAM_LOWEST_DISCERNIBLE_MICROS = 100L;

    private static final long HISTOGRAM_HIGHEST_TRACKABLE_MICROS = TimeUnit.SECONDS.toMicros(60);

    private static final int HISTOGRAM_SIGNIFICANT_DIGITS = 3;

    /** HttpClient connect timeout — generous so a brief TCP backlog does not look like a 5xx. */
    private static final int HTTP_CONNECT_TIMEOUT_S = 10;

    /** Sentinel for the http_status value we record on a transport-level failure (no response). */
    private static final int STATUS_TRANSPORT_FAILURE = -1;

    private IngressBurstMain() {}

    public static void main(String[] args) throws Exception {
        Config cfg = Config.fromEnv();
        System.out.println("OMS ingress burst tool (slice 4k) — " + cfg);
        BurstResult result = run(cfg);
        result.printSummary(System.out);
        // Non-zero exit if no successful request landed; otherwise the wrapper script prints the
        // pre/post Prometheus delta and the operator can read the histograms.
        if (result.successCount == 0) {
            System.err.println("No 2xx responses observed — exiting non-zero so wrapper scripts fail loudly.");
            System.exit(1);
        }
    }

    /**
     * Programmatic entry point. Used by ITs / CI pipelines that drive the burst from a parent
     * JVM rather than env-tuned forks.
     */
    public static BurstResult run(Config cfg) throws InterruptedException {
        HttpClient http = HttpClient.newBuilder()
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .connectTimeout(Duration.ofSeconds(HTTP_CONNECT_TIMEOUT_S))
                .build();

        UUID[] accountIds = new UUID[cfg.accountPoolSize];
        for (int i = 0; i < accountIds.length; i++) {
            accountIds[i] = UUID.randomUUID();
        }

        Histogram steadyHistogram = newHistogram();
        // outcome counters
        AtomicLong submitted = new AtomicLong();
        AtomicLong successes = new AtomicLong();
        AtomicLong created = new AtomicLong();
        AtomicLong duplicates = new AtomicLong();
        AtomicLong failures = new AtomicLong();
        ConcurrentHashMap<Integer, AtomicLong> statusCounts = new ConcurrentHashMap<>();
        // Slice 4m: per-target submitted-request count. Submitted (not 2xx) so an operator can
        // see at the end of a run that the round-robin actually reached every replica even if
        // a subset returned errors. Sized to urls.size() so one entry per target — no overhead
        // when only one URL is configured.
        ConcurrentHashMap<String, AtomicLong> perTargetSubmitted = new ConcurrentHashMap<>();
        for (String u : cfg.urls) {
            perTargetSubmitted.put(u, new AtomicLong());
        }

        Semaphore concurrencyGate = new Semaphore(cfg.concurrency);
        long opIntervalNanos = cfg.rpsCap > 0 ? TimeUnit.SECONDS.toNanos(1) / cfg.rpsCap : 0L;
        long nextScheduledNanos = System.nanoTime();
        LinkedBlockingQueue<CompletableFuture<Void>> inFlight = new LinkedBlockingQueue<>();

        long phaseStartNanos = System.nanoTime();
        for (int i = 0; i < cfg.total; i++) {
            // RPS pacing (optional). Concurrency cap is enforced separately by the semaphore.
            if (opIntervalNanos > 0L) {
                long now = System.nanoTime();
                if (nextScheduledNanos > now) {
                    LockSupport.parkNanos(nextScheduledNanos - now);
                }
                nextScheduledNanos += opIntervalNanos;
            }
            concurrencyGate.acquire();
            final int requestIndex = i;
            final boolean isWarmup = requestIndex < cfg.warmup;
            UUID accountId = accountIds[requestIndex % accountIds.length];
            String body = buildBody(accountId, requestIndex, cfg);
            // Slice 4m: round-robin across the configured targets. With one URL this is a fixed
            // index; with N URLs the burst tool spreads load uniformly without an external LB.
            String targetUrl = cfg.urls.get(requestIndex % cfg.urls.size());
            perTargetSubmitted.get(targetUrl).incrementAndGet();
            HttpRequest request = HttpRequest.newBuilder(URI.create(targetUrl))
                    .timeout(Duration.ofSeconds(cfg.requestTimeoutSeconds))
                    .header("Content-Type", "application/json")
                    .header("X-OMS-Internal-Key", cfg.apiKey)
                    .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                    .build();

            long startNanos = System.nanoTime();
            CompletableFuture<Void> future = http.sendAsync(request, BodyHandlers.discarding())
                    .<Void>handle((response, error) -> {
                        long elapsedNanos = System.nanoTime() - startNanos;
                        submitted.incrementAndGet();
                        if (error != null) {
                            failures.incrementAndGet();
                            countStatus(statusCounts, STATUS_TRANSPORT_FAILURE);
                            return null;
                        }
                        int status = response.statusCode();
                        countStatus(statusCounts, status);
                        if (status >= 200 && status < 300) {
                            successes.incrementAndGet();
                            if (status == 201) {
                                created.incrementAndGet();
                            } else if (status == 200) {
                                duplicates.incrementAndGet();
                            }
                            if (!isWarmup) {
                                long latencyMicros = TimeUnit.NANOSECONDS.toMicros(elapsedNanos);
                                steadyHistogram.recordValue(clamp(latencyMicros,
                                        HISTOGRAM_LOWEST_DISCERNIBLE_MICROS,
                                        HISTOGRAM_HIGHEST_TRACKABLE_MICROS));
                            }
                        } else {
                            failures.incrementAndGet();
                        }
                        return null;
                    })
                    .whenComplete((__, ___) -> concurrencyGate.release());
            inFlight.offer(future);
        }

        // Drain remaining futures.
        for (CompletableFuture<Void> f : inFlight) {
            try {
                f.get();
            } catch (Exception e) {
                // already counted as failure by the .handle stage
            }
        }
        long elapsedNanos = System.nanoTime() - phaseStartNanos;
        return new BurstResult(
                cfg,
                steadyHistogram,
                submitted.get(),
                successes.get(),
                created.get(),
                duplicates.get(),
                failures.get(),
                statusCounts,
                perTargetSubmitted,
                elapsedNanos);
    }

    private static String buildBody(UUID accountId, int requestIndex, Config cfg) {
        // Hand-rolled JSON keeps this benchmark dep-free of Jackson startup. Field order matches
        // CreateOrderRequest's serialization shape (Spring is permissive on order anyway).
        long uniqueSuffix = System.nanoTime();
        String clientIdempotencyKey = "burst-" + uniqueSuffix + "-" + requestIndex;
        StringBuilder sb = new StringBuilder(320);
        sb.append('{')
                .append("\"accountId\":\"").append(accountId).append("\",")
                .append("\"clientIdempotencyKey\":\"").append(clientIdempotencyKey).append("\",")
                .append("\"side\":\"BUY\",")
                .append("\"instrumentSymbol\":\"").append(cfg.instrument).append("\",")
                .append("\"quantity\":\"").append(cfg.quantity).append("\",")
                .append("\"limitPrice\":\"").append(cfg.limitPrice).append("\",")
                .append("\"timeInForce\":\"DAY\"");
        // Slice 4p: opt-in inflight-hold exercise. Both fields are required by ingress when set
        // (Config validates the pairing); leaving them off preserves slice 4m/4n burst shape.
        String ledgerBalanceId = selectLedgerBalanceId(cfg.ledgerBalanceIds, requestIndex);
        if (!ledgerBalanceId.isEmpty()) {
            sb.append(",\"ledgerBalanceId\":\"").append(ledgerBalanceId).append('"')
              .append(",\"ledgerIdentityId\":\"").append(cfg.ledgerIdentityId).append('"');
        }
        sb.append('}');
        return sb.toString();
    }

    private static Histogram newHistogram() {
        return new Histogram(
                HISTOGRAM_LOWEST_DISCERNIBLE_MICROS,
                HISTOGRAM_HIGHEST_TRACKABLE_MICROS,
                HISTOGRAM_SIGNIFICANT_DIGITS);
    }

    private static long clamp(long v, long lo, long hi) {
        if (v < lo) return lo;
        if (v > hi) return hi;
        return v;
    }

    private static void countStatus(ConcurrentHashMap<Integer, AtomicLong> map, int status) {
        map.computeIfAbsent(status, __ -> new AtomicLong()).incrementAndGet();
    }

    /** Configuration object — explicit so {@link #run(Config)} can be driven from tests. */
    public static final class Config {

        public final String apiKey;
        /**
         * Slice 4m: ordered, immutable list of burst target URLs. Always at least one entry.
         * Requests are distributed round-robin: request {@code i} goes to
         * {@code urls.get(i % urls.size())}. Single-URL operators set one entry; multi-replica
         * operators set N entries via {@link #ENV_URLS}.
         */
        public final List<String> urls;
        public final int total;
        public final int concurrency;
        public final int rpsCap;
        public final int accountPoolSize;
        public final String instrument;
        public final String quantity;
        public final String limitPrice;
        public final int requestTimeoutSeconds;
        public final int warmup;
        /**
         * Slice 4p. Empty when no hold path should be exercised; otherwise injected verbatim into
         * every burst body. With {@link #ENV_LEDGER_BALANCE_POOL_SIZE} set, burst requests
         * round-robin over {@link #ledgerBalanceIds} to avoid single-balance OCC serialization.
         */
        public final String ledgerBalanceId;
        public final List<String> ledgerBalanceIds;
        public final String ledgerIdentityId;

        public Config(
                String apiKey,
                String url,
                int total,
                int concurrency,
                int rpsCap,
                int accountPoolSize,
                String instrument,
                String quantity,
                String limitPrice,
                int requestTimeoutSeconds,
                int warmup) {
            this(apiKey,
                    Collections.singletonList(requireUrl(url)),
                    total,
                    concurrency,
                    rpsCap,
                    accountPoolSize,
                    instrument,
                    quantity,
                    limitPrice,
                    requestTimeoutSeconds,
                    warmup,
                    DEFAULT_LEDGER_BALANCE_ID,
                    DEFAULT_LEDGER_BALANCE_POOL_SIZE,
                    DEFAULT_LEDGER_IDENTITY_ID);
        }

        public Config(
                String apiKey,
                List<String> urls,
                int total,
                int concurrency,
                int rpsCap,
                int accountPoolSize,
                String instrument,
                String quantity,
                String limitPrice,
                int requestTimeoutSeconds,
                int warmup) {
            this(apiKey,
                    urls,
                    total,
                    concurrency,
                    rpsCap,
                    accountPoolSize,
                    instrument,
                    quantity,
                    limitPrice,
                    requestTimeoutSeconds,
                    warmup,
                    DEFAULT_LEDGER_BALANCE_ID,
                    DEFAULT_LEDGER_BALANCE_POOL_SIZE,
                    DEFAULT_LEDGER_IDENTITY_ID);
        }

        public Config(
                String apiKey,
                List<String> urls,
                int total,
                int concurrency,
                int rpsCap,
                int accountPoolSize,
                String instrument,
                String quantity,
                String limitPrice,
                int requestTimeoutSeconds,
                int warmup,
                String ledgerBalanceId,
                String ledgerBalancePoolSpec,
                String ledgerIdentityId) {
            if (apiKey == null || apiKey.isBlank()) {
                throw new IllegalArgumentException("apiKey must be set (env " + ENV_API_KEY + ")");
            }
            if (urls == null || urls.isEmpty()) {
                throw new IllegalArgumentException(
                        "urls must contain at least one entry (env " + ENV_URL + " or " + ENV_URLS + ")");
            }
            List<String> sanitised = new ArrayList<>(urls.size());
            for (String u : urls) {
                sanitised.add(requireUrl(u));
            }
            if (total <= 0) {
                throw new IllegalArgumentException("total must be > 0");
            }
            if (concurrency <= 0) {
                throw new IllegalArgumentException("concurrency must be > 0");
            }
            if (rpsCap < 0) {
                throw new IllegalArgumentException("rpsCap must be >= 0 (0 disables pacing)");
            }
            if (accountPoolSize <= 0) {
                throw new IllegalArgumentException("accountPoolSize must be > 0");
            }
            if (warmup < 0 || warmup > total) {
                throw new IllegalArgumentException("warmup must be in [0, total]");
            }
            this.apiKey = apiKey;
            this.urls = Collections.unmodifiableList(sanitised);
            this.total = total;
            this.concurrency = concurrency;
            this.rpsCap = rpsCap;
            this.accountPoolSize = accountPoolSize;
            this.instrument = instrument;
            this.quantity = quantity;
            this.limitPrice = limitPrice;
            this.requestTimeoutSeconds = requestTimeoutSeconds;
            this.warmup = warmup;
            // Slice 4p: a balanceId without an identityId would be rejected at admission with a
            // 400 (ledger_identity_required); fail fast at config build instead of N times in
            // flight.
            String balance = ledgerBalanceId == null ? "" : ledgerBalanceId.trim();
            String identity = ledgerIdentityId == null ? "" : ledgerIdentityId.trim();
            List<String> balancePool = parseLedgerBalancePool(balance, ledgerBalancePoolSpec);
            if (!balancePool.isEmpty() && identity.isEmpty()) {
                throw new IllegalArgumentException(
                        ENV_LEDGER_BALANCE_ID + " was set but " + ENV_LEDGER_IDENTITY_ID
                                + " is empty; ingress requires both when exercising the hold path");
            }
            this.ledgerBalanceId = balancePool.isEmpty() ? "" : balancePool.get(0);
            this.ledgerBalanceIds = Collections.unmodifiableList(new ArrayList<>(balancePool));
            this.ledgerIdentityId = identity;
        }

        public static Config fromEnv() {
            String apiKey = requireEnv(ENV_API_KEY);
            String urlFallback = envOrDefault(ENV_URL, "");
            if (urlFallback.isBlank()) {
                String omsUrl = envOrDefault("OMS_URL", "");
                if (!omsUrl.isBlank()) {
                    urlFallback = omsUrl.trim().replaceAll("/+$", "") + "/internal/v1/orders";
                } else {
                    urlFallback = DEFAULT_URL;
                }
            }
            List<String> urls = parseUrls(System.getenv(ENV_URLS), urlFallback);
            return new Config(
                    apiKey,
                    urls,
                    parseInt(ENV_TOTAL, DEFAULT_TOTAL),
                    parseInt(ENV_CONCURRENCY, DEFAULT_CONCURRENCY),
                    parseInt(ENV_RPS_CAP, DEFAULT_RPS_CAP),
                    parseInt(ENV_ACCOUNT_POOL, DEFAULT_ACCOUNT_POOL),
                    envOrDefault(ENV_INSTRUMENT, DEFAULT_INSTRUMENT),
                    envOrDefault(ENV_QUANTITY, DEFAULT_QUANTITY),
                    envOrDefault(ENV_LIMIT_PRICE, DEFAULT_LIMIT_PRICE),
                    parseInt(ENV_REQUEST_TIMEOUT_S, DEFAULT_REQUEST_TIMEOUT_S),
                    parseInt(ENV_WARMUP, DEFAULT_WARMUP),
                    envOrDefault(ENV_LEDGER_BALANCE_ID, DEFAULT_LEDGER_BALANCE_ID),
                    envOrDefault(ENV_LEDGER_BALANCE_POOL_SIZE, DEFAULT_LEDGER_BALANCE_POOL_SIZE),
                    envOrDefault(ENV_LEDGER_IDENTITY_ID, DEFAULT_LEDGER_IDENTITY_ID));
        }

        @Override
        public String toString() {
            return "Config{urls=" + urls
                    + ", total=" + total
                    + ", concurrency=" + concurrency
                    + ", rpsCap=" + rpsCap
                    + ", accountPool=" + accountPoolSize
                    + ", instrument=" + instrument
                    + ", warmup=" + warmup
                    + ", ledgerBalancePool=" + (ledgerBalanceIds.isEmpty() ? "(unset)" : ledgerBalanceIds)
                    + "}";
        }
    }

    /**
     * Parse {@link #ENV_URLS} (comma-separated, any whitespace around entries is trimmed). When
     * blank, returns a single-element list with {@code fallback}. Empty entries inside the list
     * (e.g. {@code "a,,b"}) are dropped silently rather than failing — operators copy-paste
     * comma-separated lists from runbooks and a stray trailing comma should not block a run.
     */
    static List<String> parseUrls(String raw, String fallback) {
        if (raw == null || raw.isBlank()) {
            return Collections.singletonList(fallback);
        }
        String[] parts = raw.split(",");
        List<String> urls = new ArrayList<>(parts.length);
        for (String p : parts) {
            String trimmed = p.trim();
            if (!trimmed.isEmpty()) {
                urls.add(trimmed);
            }
        }
        if (urls.isEmpty()) {
            return Collections.singletonList(fallback);
        }
        return urls;
    }

    static List<String> parseLedgerBalancePool(String legacyBalanceIds, String poolSpec) {
        String legacy = legacyBalanceIds == null ? "" : legacyBalanceIds.trim();
        if (poolSpec == null || poolSpec.isBlank()) {
            return parseCsvValues(legacy);
        }
        String trimmed = poolSpec.trim();
        if (trimmed.indexOf(',') >= 0) {
            List<String> explicit = parseCsvValues(trimmed);
            if (explicit.isEmpty()) {
                throw new IllegalArgumentException(
                        ENV_LEDGER_BALANCE_POOL_SIZE + " was set but no balance IDs were parsed");
            }
            return explicit;
        }
        int parsedPoolSize = tryParsePositiveInt(trimmed);
        if (parsedPoolSize > 0) {
            List<String> fromLegacy = parseCsvValues(legacy);
            if (fromLegacy.size() < parsedPoolSize) {
                throw new IllegalArgumentException(
                        ENV_LEDGER_BALANCE_POOL_SIZE + "=" + parsedPoolSize
                                + " requires at least that many comma-separated IDs in "
                                + ENV_LEDGER_BALANCE_ID + " (found " + fromLegacy.size() + ")");
            }
            return new ArrayList<>(fromLegacy.subList(0, parsedPoolSize));
        }
        if ("0".equals(trimmed)) {
            throw new IllegalArgumentException(ENV_LEDGER_BALANCE_POOL_SIZE + " must be > 0 when numeric");
        }
        // Non-numeric non-CSV value: treat as a single explicit balance ID.
        return List.of(trimmed);
    }

    static String selectLedgerBalanceId(List<String> balanceIds, int requestIndex) {
        if (balanceIds == null || balanceIds.isEmpty()) {
            return "";
        }
        return balanceIds.get(requestIndex % balanceIds.size());
    }

    private static List<String> parseCsvValues(String raw) {
        if (raw == null || raw.isBlank()) {
            return Collections.emptyList();
        }
        String[] parts = raw.split(",");
        List<String> values = new ArrayList<>(parts.length);
        for (String p : parts) {
            String trimmed = p.trim();
            if (!trimmed.isEmpty()) {
                values.add(trimmed);
            }
        }
        return values;
    }

    private static int tryParsePositiveInt(String value) {
        try {
            int parsed = Integer.parseInt(value);
            return parsed > 0 ? parsed : -1;
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    private static String requireUrl(String url) {
        if (url == null || url.isBlank()) {
            throw new IllegalArgumentException("url must not be blank");
        }
        return url.trim();
    }

    /** Result struct — immutable post-construction; histograms are owned. */
    public static final class BurstResult {

        public final Config config;
        public final Histogram steadyHistogram;
        public final long submitted;
        public final long successCount;
        public final long createdCount;
        public final long duplicateCount;
        public final long failureCount;
        public final ConcurrentHashMap<Integer, AtomicLong> statusCounts;
        /**
         * Slice 4m: per-target submitted-request count. Always populated (one entry per
         * {@link Config#urls} entry) so multi-replica runs print "we did reach every replica"
         * without operators needing to grep ingress-replica access logs.
         */
        public final ConcurrentHashMap<String, AtomicLong> perTargetSubmitted;
        public final long elapsedNanos;

        public BurstResult(
                Config config,
                Histogram steadyHistogram,
                long submitted,
                long successCount,
                long createdCount,
                long duplicateCount,
                long failureCount,
                ConcurrentHashMap<Integer, AtomicLong> statusCounts,
                ConcurrentHashMap<String, AtomicLong> perTargetSubmitted,
                long elapsedNanos) {
            this.config = config;
            this.steadyHistogram = steadyHistogram;
            this.submitted = submitted;
            this.successCount = successCount;
            this.createdCount = createdCount;
            this.duplicateCount = duplicateCount;
            this.failureCount = failureCount;
            this.statusCounts = statusCounts;
            this.perTargetSubmitted = perTargetSubmitted;
            this.elapsedNanos = elapsedNanos;
        }

        public void printSummary(java.io.PrintStream out) {
            double elapsedSeconds = elapsedNanos / (double) TimeUnit.SECONDS.toNanos(1);
            double rps = elapsedSeconds > 0 ? submitted / elapsedSeconds : 0.0;
            out.println();
            out.println("=== Burst summary ===");
            out.printf("submitted=%d  success=%d (created=%d, duplicate=%d)  failed=%d  elapsed=%.3f s  rps=%.1f%n",
                    submitted, successCount, createdCount, duplicateCount, failureCount, elapsedSeconds, rps);
            if (!statusCounts.isEmpty()) {
                out.println("status breakdown:");
                statusCounts.entrySet().stream()
                        .sorted(java.util.Map.Entry.comparingByKey())
                        .forEach(e -> {
                            String label = e.getKey() == STATUS_TRANSPORT_FAILURE
                                    ? "transport-failure"
                                    : String.valueOf(e.getKey());
                            out.printf("  %s = %d%n", label, e.getValue().get());
                        });
            }
            // Slice 4m: print only when we actually have multiple targets — single-URL runs do
            // not need a one-line "100% to localhost" breakdown that would clutter every shoot.
            if (config.urls.size() > 1) {
                out.println("per-target submitted (round-robin):");
                for (String u : config.urls) {
                    long count = perTargetSubmitted.getOrDefault(u, new AtomicLong()).get();
                    out.printf("  %s = %d%n", u, count);
                }
            }
            if (steadyHistogram.getTotalCount() == 0) {
                out.println("(no successful samples — histogram empty)");
                return;
            }
            out.println();
            out.println("HTTP RTT (HdrHistogram, milliseconds; warmup excluded):");
            out.printf("  n=%d  min=%.3f  max=%.3f  mean=%.3f%n",
                    steadyHistogram.getTotalCount(),
                    steadyHistogram.getMinValue() / 1000.0,
                    steadyHistogram.getMaxValue() / 1000.0,
                    steadyHistogram.getMean() / 1000.0);
            out.printf("  p50=%.3f  p95=%.3f  p99=%.3f  p999=%.3f%n",
                    steadyHistogram.getValueAtPercentile(50.0) / 1000.0,
                    steadyHistogram.getValueAtPercentile(95.0) / 1000.0,
                    steadyHistogram.getValueAtPercentile(99.0) / 1000.0,
                    steadyHistogram.getValueAtPercentile(99.9) / 1000.0);
            out.println();
            out.println("Pair this with a pre/post Prometheus scrape via the burst-ingress-orders.sh");
            out.println("wrapper to see oms.pipeline.cluster_admit_to_fix_nos / cluster_admit_to_projector p50/p99");
            out.println("(slice 4j) for the cross-JVM admit\u2192NOS distribution under this load.");
        }
    }

    private static String requireEnv(String key) {
        String v = System.getenv(key);
        if (v == null || v.isBlank()) {
            throw new IllegalStateException("Required env var " + key + " is not set.");
        }
        return v;
    }

    private static String envOrDefault(String key, String fallback) {
        String v = System.getenv(key);
        return (v == null || v.isBlank()) ? fallback : v;
    }

    private static int parseInt(String key, int fallback) {
        String v = System.getenv(key);
        if (v == null || v.isBlank()) {
            return fallback;
        }
        try {
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "env " + key + " is not a valid integer: '" + v + "'", e);
        }
    }
}
