package com.balh.oms.ledger;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link CachingLedgerBalanceClient} (Phase 4 Tier 2.5 phase D-8).
 *
 * <p>Covers the eight invariants the cache must hold:
 * <ul>
 *   <li>Cache miss calls the delegate and increments the {@code miss} counter.</li>
 *   <li>Cache hit returns the stored value, increments the {@code hit} counter, and does
 *       <em>not</em> call the delegate.</li>
 *   <li>Different {@code balanceId}s do not collide (separate cache slots).</li>
 *   <li>{@link LedgerBalanceClient.LedgerServiceException} from the delegate is rethrown
 *       and <em>not</em> cached — the next call hits the delegate again (negative caching
 *       policy).</li>
 *   <li>{@code null} {@code balanceId} bypasses the cache and surfaces the delegate's own
 *       contract.</li>
 *   <li>Volatile pass-through methods ({@code fetchAvailableBalance},
 *       {@code fetchBalanceReadModel}) hit the delegate every time.</li>
 *   <li>{@link CachingLedgerBalanceClient#invalidateAll()} re-arms the cache.</li>
 *   <li>Concurrent loads for the same key single-flight to a single delegate call.</li>
 * </ul>
 */
final class CachingLedgerBalanceClientTest {

    private static final String BALANCE_A = "balance_a";
    private static final String BALANCE_B = "balance_b";
    private static final String IDENTITY_A = "identity_a";
    private static final String IDENTITY_B = "identity_b";

    private LedgerBalanceClient delegate;
    private SimpleMeterRegistry meterRegistry;
    private CachingLedgerBalanceClient cache;

    @BeforeEach
    void setUp() {
        delegate = mock(LedgerBalanceClient.class);
        meterRegistry = new SimpleMeterRegistry();
        cache = new CachingLedgerBalanceClient(delegate, /*ttlSeconds=*/300L, /*maxSize=*/100L, meterRegistry);
    }

    @Test
    void miss_then_hit_returnsCachedValue_andCountersTrack()
            throws LedgerBalanceClient.LedgerServiceException {
        when(delegate.fetchIdentityIdForBalance(BALANCE_A)).thenReturn(IDENTITY_A);

        assertThat(cache.fetchIdentityIdForBalance(BALANCE_A)).isEqualTo(IDENTITY_A);
        assertThat(cache.fetchIdentityIdForBalance(BALANCE_A)).isEqualTo(IDENTITY_A);
        assertThat(cache.fetchIdentityIdForBalance(BALANCE_A)).isEqualTo(IDENTITY_A);

        verify(delegate, times(1)).fetchIdentityIdForBalance(BALANCE_A);
        assertThat(missCount()).isEqualTo(1.0);
        assertThat(hitCount()).isEqualTo(2.0);
    }

    @Test
    void differentBalanceIds_doNotCollide()
            throws LedgerBalanceClient.LedgerServiceException {
        when(delegate.fetchIdentityIdForBalance(BALANCE_A)).thenReturn(IDENTITY_A);
        when(delegate.fetchIdentityIdForBalance(BALANCE_B)).thenReturn(IDENTITY_B);

        assertThat(cache.fetchIdentityIdForBalance(BALANCE_A)).isEqualTo(IDENTITY_A);
        assertThat(cache.fetchIdentityIdForBalance(BALANCE_B)).isEqualTo(IDENTITY_B);
        assertThat(cache.fetchIdentityIdForBalance(BALANCE_A)).isEqualTo(IDENTITY_A);
        assertThat(cache.fetchIdentityIdForBalance(BALANCE_B)).isEqualTo(IDENTITY_B);

        verify(delegate, times(1)).fetchIdentityIdForBalance(BALANCE_A);
        verify(delegate, times(1)).fetchIdentityIdForBalance(BALANCE_B);
        assertThat(missCount()).isEqualTo(2.0);
        assertThat(hitCount()).isEqualTo(2.0);
    }

    @Test
    void delegateException_isRethrown_andNotCached()
            throws LedgerBalanceClient.LedgerServiceException {
        when(delegate.fetchIdentityIdForBalance(BALANCE_A))
                .thenThrow(new LedgerBalanceClient.LedgerServiceException("ledger balance not found"))
                .thenReturn(IDENTITY_A);

        assertThatThrownBy(() -> cache.fetchIdentityIdForBalance(BALANCE_A))
                .isInstanceOf(LedgerBalanceClient.LedgerServiceException.class)
                .hasMessageContaining("not found");

        // Negative caching policy: the same call must hit the delegate again, not surface the
        // first call's exception out of the cache.
        assertThat(cache.fetchIdentityIdForBalance(BALANCE_A)).isEqualTo(IDENTITY_A);
        verify(delegate, times(2)).fetchIdentityIdForBalance(BALANCE_A);
        assertThat(missCount()).isEqualTo(1.0);
    }

    @Test
    void delegateBlankResponse_isRethrownAsLedgerServiceException_andNotCached()
            throws LedgerBalanceClient.LedgerServiceException {
        when(delegate.fetchIdentityIdForBalance(BALANCE_A))
                .thenReturn("   ")
                .thenReturn(IDENTITY_A);

        assertThatThrownBy(() -> cache.fetchIdentityIdForBalance(BALANCE_A))
                .isInstanceOf(LedgerBalanceClient.LedgerServiceException.class);

        assertThat(cache.fetchIdentityIdForBalance(BALANCE_A)).isEqualTo(IDENTITY_A);
        verify(delegate, times(2)).fetchIdentityIdForBalance(BALANCE_A);
    }

    @Test
    void nullBalanceId_bypassesCache_andDelegatesToUnderlying()
            throws LedgerBalanceClient.LedgerServiceException {
        when(delegate.fetchIdentityIdForBalance(null))
                .thenThrow(new LedgerBalanceClient.LedgerServiceException("nope"));

        assertThatThrownBy(() -> cache.fetchIdentityIdForBalance(null))
                .isInstanceOf(LedgerBalanceClient.LedgerServiceException.class);
        assertThatThrownBy(() -> cache.fetchIdentityIdForBalance(null))
                .isInstanceOf(LedgerBalanceClient.LedgerServiceException.class);

        verify(delegate, times(2)).fetchIdentityIdForBalance(null);
    }

    @Test
    void fetchAvailableBalance_neverCached() throws LedgerBalanceClient.LedgerServiceException {
        when(delegate.fetchAvailableBalance(BALANCE_A))
                .thenReturn(BigDecimal.valueOf(10))
                .thenReturn(BigDecimal.valueOf(20));

        assertThat(cache.fetchAvailableBalance(BALANCE_A)).isEqualByComparingTo("10");
        assertThat(cache.fetchAvailableBalance(BALANCE_A)).isEqualByComparingTo("20");
        verify(delegate, times(2)).fetchAvailableBalance(BALANCE_A);
    }

    @Test
    void fetchBalanceReadModel_neverCached() throws LedgerBalanceClient.LedgerServiceException {
        var first = new LedgerBalanceReadModel(BALANCE_A, BigDecimal.ONE, BigDecimal.ONE, "EUR", IDENTITY_A);
        var second = new LedgerBalanceReadModel(BALANCE_A, BigDecimal.TEN, BigDecimal.TEN, "EUR", IDENTITY_A);
        when(delegate.fetchBalanceReadModel(BALANCE_A)).thenReturn(first).thenReturn(second);

        assertThat(cache.fetchBalanceReadModel(BALANCE_A).availableBalance()).isEqualByComparingTo("1");
        assertThat(cache.fetchBalanceReadModel(BALANCE_A).availableBalance()).isEqualByComparingTo("10");
        verify(delegate, times(2)).fetchBalanceReadModel(BALANCE_A);
    }

    @Test
    void invalidateAll_forcesRefresh() throws LedgerBalanceClient.LedgerServiceException {
        when(delegate.fetchIdentityIdForBalance(BALANCE_A)).thenReturn(IDENTITY_A);

        cache.fetchIdentityIdForBalance(BALANCE_A);
        cache.fetchIdentityIdForBalance(BALANCE_A);
        verify(delegate, times(1)).fetchIdentityIdForBalance(BALANCE_A);

        cache.invalidateAll();

        cache.fetchIdentityIdForBalance(BALANCE_A);
        verify(delegate, times(2)).fetchIdentityIdForBalance(BALANCE_A);
    }

    @Test
    void thunderingHerd_singleFlightsToOneDelegateCall() throws Exception {
        // A latch-gated delegate forces every concurrent caller through a single,
        // demonstrably-slow load. With single-flighting, only one delegate call should occur
        // for the same key even under N=64 concurrent fetchers — the rest should block on
        // Caffeine's per-key compute and pick up the result.
        AtomicInteger delegateCalls = new AtomicInteger(0);
        CountDownLatch loadStarted = new CountDownLatch(1);
        CountDownLatch releaseLoad = new CountDownLatch(1);

        LedgerBalanceClient slowDelegate = mock(LedgerBalanceClient.class);
        when(slowDelegate.fetchIdentityIdForBalance(eq(BALANCE_A))).thenAnswer(invocation -> {
            delegateCalls.incrementAndGet();
            loadStarted.countDown();
            // Block until the test releases us so all 64 callers pile up behind us.
            if (!releaseLoad.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("test never released the loader");
            }
            return IDENTITY_A;
        });
        CachingLedgerBalanceClient herd =
                new CachingLedgerBalanceClient(slowDelegate, 300L, 100L, new SimpleMeterRegistry());

        int threadCount = 64;
        ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        try {
            CountDownLatch ready = new CountDownLatch(threadCount);
            CountDownLatch go = new CountDownLatch(1);
            Future<?>[] futures = new Future<?>[threadCount];
            for (int i = 0; i < threadCount; i++) {
                futures[i] = pool.submit(() -> {
                    ready.countDown();
                    go.await();
                    return herd.fetchIdentityIdForBalance(BALANCE_A);
                });
            }
            // Wait until every worker is parked at the go latch, then release them all at
            // once. The first worker enters the loader and trips the loadStarted latch; the
            // rest should serialize on Caffeine's per-key compute.
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            go.countDown();
            assertThat(loadStarted.await(5, TimeUnit.SECONDS)).isTrue();
            // Sanity check: at this point at most one delegate call should have started; any
            // others would have trampled the single-flight contract.
            assertThat(delegateCalls.get()).isEqualTo(1);
            // Release the loader; every caller should now resolve to the same value.
            releaseLoad.countDown();
            for (Future<?> f : futures) {
                assertThat(f.get(5, TimeUnit.SECONDS)).isEqualTo(IDENTITY_A);
            }
        } finally {
            pool.shutdownNow();
        }
        // Single-flighting end-state: regardless of 64 concurrent fetchers, the delegate ran
        // exactly once.
        assertThat(delegateCalls.get()).isEqualTo(1);
        verify(slowDelegate, times(1)).fetchIdentityIdForBalance(BALANCE_A);
    }

    @Test
    void zeroOrNegativeTtl_andMaxSize_areClampedToOne() throws Exception {
        // The OmsConfig setters clamp these too, but the constructor must be defensive
        // because Caffeine rejects 0/negative directly. Smoke check: build with bad inputs
        // and verify the cache still works at all (rather than throwing at construction).
        CachingLedgerBalanceClient defensive =
                new CachingLedgerBalanceClient(delegate, /*ttlSeconds=*/0L, /*maxSize=*/-5L, new SimpleMeterRegistry());
        when(delegate.fetchIdentityIdForBalance(BALANCE_A)).thenReturn(IDENTITY_A);

        assertThat(defensive.fetchIdentityIdForBalance(BALANCE_A)).isEqualTo(IDENTITY_A);
    }

    private double hitCount() {
        return meterRegistry.find(CachingLedgerBalanceClient.CACHE_REQUESTS_METRIC)
                .tag("result", "hit")
                .counter()
                .count();
    }

    private double missCount() {
        return meterRegistry.find(CachingLedgerBalanceClient.CACHE_REQUESTS_METRIC)
                .tag("result", "miss")
                .counter()
                .count();
    }
}
