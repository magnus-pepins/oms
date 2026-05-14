package com.balh.oms.ledger;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Objects;

/**
 * Phase 4 Tier 2.5 phase D-8 — caching decorator over {@link LedgerBalanceClient}.
 *
 * <p>Caches only {@link #fetchIdentityIdForBalance(String)}: the
 * {@code (balanceId -> identityId)} binding is durable in Ledger (only changes on operator
 * reassignment), so a JVM-local cache with a short TTL is a correctness-preserving lift on
 * the order-accept hot path. The other methods on {@link LedgerBalanceClient} return
 * volatile state ({@code availableBalance} / {@code balance}) and pass through unchanged.
 *
 * <h2>Why caching this is safe</h2>
 *
 * <p>The verify in
 * {@link com.balh.oms.ingress.OrderIngressService#maybeVerifyLedgerBalanceBinding} only
 * asserts <em>"this {@code ledgerIdentityId} owns this {@code ledgerBalanceId}"</em>. The
 * binding is durable Ledger state. If an operator reassigns a balance to a different
 * identity, ingress JVMs will mis-accept up to {@link
 * com.balh.oms.config.OmsConfig.Ledger.BalanceIdentityCache#getTtlSeconds()} of orders that
 * claim the <em>old</em> identity. That window is bounded and the operator can shrink it
 * by lowering the TTL or restarting the affected ingresses. Reassignment is a manual,
 * rare action — the trade is heavily in favour of caching.
 *
 * <h2>What is NOT cached</h2>
 *
 * <ul>
 *   <li>Exceptions from the delegate — {@link LedgerBalanceClient.LedgerServiceException}
 *       cases (network errors, {@code 5xx}, {@code ledger_balance_not_found}) propagate
 *       directly and are <em>not</em> cached. A transient Ledger blip should not pin a
 *       4xx/5xx for the whole TTL window.</li>
 *   <li>Null / blank {@code identityId} responses — the delegate already throws on these;
 *       we trust that contract. If somehow the loader returns {@code null}, Caffeine's
 *       {@code Cache.get(K, Function)} contract is "remove (or never insert) the mapping",
 *       so a subsequent call re-hits the delegate.</li>
 * </ul>
 *
 * <h2>Concurrency / single-flighting</h2>
 *
 * <p>Uses {@link Cache#get(Object, java.util.function.Function)} which Caffeine documents as
 * atomic per key — only one invocation of the loader runs at a time per {@code balanceId},
 * with the rest blocking on the in-progress load. This is the behaviour we want at
 * burst-start when 1 600 client connections all ask about the same balance: the first
 * thread fetches, the other 1 599 wait, and Ledger sees a single {@code GET}.
 *
 * <p>The checked {@link LedgerBalanceClient.LedgerServiceException} is carried through the
 * loader as an unchecked {@link CacheLoadFailure} wrapper and unwrapped at the boundary; a
 * loader exception removes (does not insert) the mapping per Caffeine's contract, so
 * negative caching policy is naturally enforced.
 *
 * <h2>Metrics</h2>
 *
 * <p>Records a counter under {@code oms.ledger.balance_identity_cache.requests_total} with a
 * {@code result} tag of {@code hit} or {@code miss}. Caffeine's own stats are queryable via
 * {@link #snapshotStats()} for tests + the runbook.
 */
public final class CachingLedgerBalanceClient implements LedgerBalanceClient {

    static final String CACHE_REQUESTS_METRIC = "oms.ledger.balance_identity_cache.requests";

    private final LedgerBalanceClient delegate;
    private final Cache<String, String> identityByBalance;
    private final Counter hitCounter;
    private final Counter missCounter;

    public CachingLedgerBalanceClient(
            LedgerBalanceClient delegate,
            long ttlSeconds,
            long maxSize,
            MeterRegistry meterRegistry) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        Objects.requireNonNull(meterRegistry, "meterRegistry");
        this.identityByBalance = Caffeine.newBuilder()
                .expireAfterWrite(Duration.ofSeconds(Math.max(1L, ttlSeconds)))
                .maximumSize(Math.max(1L, maxSize))
                .recordStats()
                .build();
        this.hitCounter = Counter.builder(CACHE_REQUESTS_METRIC)
                .description("Outcome of OMS-side (balanceId -> identityId) cache lookups (Phase 4 Tier 2.5 D-8)")
                .tags(Tags.of("result", "hit"))
                .register(meterRegistry);
        this.missCounter = Counter.builder(CACHE_REQUESTS_METRIC)
                .description("Outcome of OMS-side (balanceId -> identityId) cache lookups (Phase 4 Tier 2.5 D-8)")
                .tags(Tags.of("result", "miss"))
                .register(meterRegistry);
    }

    @Override
    public String fetchIdentityIdForBalance(String balanceId) throws LedgerServiceException {
        if (balanceId == null) {
            // Preserve the delegate's contract: null balanceId is a programmer error
            // surfaced as the delegate's own LedgerServiceException, not a cached entry.
            return delegate.fetchIdentityIdForBalance(null);
        }
        // Fast path: a hit short-circuits before we enter Caffeine's atomic compute, so the
        // common case (cache warm, single-account bench, steady state) does not block on the
        // per-key lock at all. The miss path goes through Cache.get(K, Function) which is
        // atomic per key — the first thread loads, the rest of the thundering herd wait.
        String cached = identityByBalance.getIfPresent(balanceId);
        if (cached != null) {
            hitCounter.increment();
            return cached;
        }
        String fresh;
        try {
            fresh = identityByBalance.get(balanceId, key -> {
                try {
                    String v = delegate.fetchIdentityIdForBalance(key);
                    if (v == null || v.isBlank()) {
                        // Returning null tells Caffeine "do not cache this" — the next call
                        // re-hits the delegate. Same outcome as the negative caching policy
                        // for exceptions.
                        return null;
                    }
                    return v;
                } catch (LedgerServiceException e) {
                    throw new CacheLoadFailure(e);
                }
            });
        } catch (CacheLoadFailure e) {
            // Caffeine swallows null-loader-returns AND propagates the runtime wrapper.
            // Both paths leave the cache empty for this key, which is the negative caching
            // policy we want: a transient Ledger error must not pin a 4xx/5xx for the TTL.
            throw e.cause;
        }
        if (fresh == null) {
            // Loader returned null (delegate produced a blank id and we mapped it to null
            // to keep the cache empty); surface this as a checked LedgerServiceException
            // matching the delegate's documented contract.
            throw new LedgerServiceException("ledger response missing identityId");
        }
        missCounter.increment();
        return fresh;
    }

    @Override
    public BigDecimal fetchAvailableBalance(String balanceId) throws LedgerServiceException {
        // Volatile — never cache. Pass through.
        return delegate.fetchAvailableBalance(balanceId);
    }

    @Override
    public LedgerBalanceReadModel fetchBalanceReadModel(String balanceId) throws LedgerServiceException {
        // Volatile — never cache. Pass through.
        return delegate.fetchBalanceReadModel(balanceId);
    }

    /**
     * Test-only / runbook helper. Returns a {@link CacheStats} snapshot from Caffeine.
     * Production code should rely on the Micrometer counters; this exists so tests can
     * assert hit/miss counts without leaking the Caffeine type into the {@link
     * LedgerBalanceClient} interface.
     */
    public CacheStats snapshotStats() {
        return identityByBalance.stats();
    }

    /**
     * Visible-for-tests helper. Drops all entries; on the next call the delegate is hit
     * and a fresh value is cached. Production code does not need this — TTL expiry is the
     * release valve — but operators may invoke it via JMX in the future if we add a
     * management bean.
     */
    public void invalidateAll() {
        identityByBalance.invalidateAll();
    }

    /**
     * Carries the checked {@link LedgerServiceException} through Caffeine's loader, which
     * only allows {@link RuntimeException}. Unwrapped at the public method boundary so
     * callers see the original checked exception type.
     */
    private static final class CacheLoadFailure extends RuntimeException {
        private static final long serialVersionUID = 1L;
        private final LedgerServiceException cause;

        CacheLoadFailure(LedgerServiceException cause) {
            super(cause);
            this.cause = cause;
        }
    }
}
