package com.balh.oms.cluster;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.domain.ShardKey;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/**
 * Phase 4 Tier 2.5 phase E-1 — unit tests for {@link OmsClusterShardRouter}.
 *
 * <p>E-1 only ships the byte-identical-at-{@code N=1} indirection: the router holds exactly one
 * {@link OmsClusterIngressClient} and resolves every admit to it. These tests verify both the
 * Spring-discovered single-bean path and the direct test-friendly constructor (which E-3 will
 * also use to populate {@code N} clients without going through Spring).
 */
class OmsClusterShardRouterTest {

    @Test
    void springConstructor_atShardCount1_resolvesAnyAccountIdToTheSingleClient() {
        OmsConfig config = new OmsConfig();
        config.getShard().setCount(1);
        OmsClusterIngressClient client = mock(OmsClusterIngressClient.class);

        OmsClusterShardRouter router = new OmsClusterShardRouter(config, client);

        assertThat(router.shardCount()).isEqualTo(1);
        assertThat(router.forShard(0)).isSameAs(client);
        assertThat(router.routeAdmit(UUID.randomUUID())).isSameAs(client);
        assertThat(router.routeAdmit(UUID.randomUUID())).isSameAs(client);
    }

    @Test
    void springConstructor_rejectsShardCountAboveE1Cap_withSliceNamingMessage() {
        OmsConfig config = new OmsConfig();
        // Ship the trip-wire that says "this slice does not yet support N>1; that lands in E-3".
        config.getShard().setCount(OmsClusterShardRouter.E1_MAX_SHARD_COUNT + 1);
        OmsClusterIngressClient client = mock(OmsClusterIngressClient.class);

        assertThatThrownBy(() -> new OmsClusterShardRouter(config, client))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("oms.shard.count=" + (OmsClusterShardRouter.E1_MAX_SHARD_COUNT + 1))
                .hasMessageContaining("slice E-3");
    }

    @Test
    void routeAdmit_isStableAcrossCalls_byHashFunction() {
        OmsClusterIngressClient client = mock(OmsClusterIngressClient.class);
        OmsClusterShardRouter router = new OmsClusterShardRouter(1, Map.of(0, client));

        UUID account = UUID.randomUUID();
        // Two calls with the same accountId must resolve to the same client (E-1 trivially true,
        // but pinned here so E-3 cannot regress determinism without the test catching it).
        assertThat(router.routeAdmit(account)).isSameAs(router.routeAdmit(account));
        // And to the client whose shardId matches ShardKey.shardFor(accountId, shardCount).
        int expected = ShardKey.shardFor(account, router.shardCount());
        assertThat(router.routeAdmit(account)).isSameAs(router.forShard(expected));
    }

    @Test
    void forShard_throwsOnOutOfRangeShardId() {
        OmsClusterIngressClient client = mock(OmsClusterIngressClient.class);
        OmsClusterShardRouter router = new OmsClusterShardRouter(1, Map.of(0, client));

        assertThatThrownBy(() -> router.forShard(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("shardId=-1");
        assertThatThrownBy(() -> router.forShard(1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("shardId=1");
        assertThatThrownBy(() -> router.forShard(Integer.MAX_VALUE))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void directConstructor_rejectsNonPositiveShardCount() {
        OmsClusterIngressClient client = mock(OmsClusterIngressClient.class);

        assertThatThrownBy(() -> new OmsClusterShardRouter(0, Map.of(0, client)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("shardCount must be >= 1");
        assertThatThrownBy(() -> new OmsClusterShardRouter(-1, Map.of(0, client)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void directConstructor_rejectsClientMapMismatchedToShardCount() {
        OmsClusterIngressClient client = mock(OmsClusterIngressClient.class);

        // Map covers shard 0 but config asks for 2 shards — this is the shape E-3 will catch when
        // it injects only N-1 clients, so the message must name what is missing rather than just
        // saying "size mismatch".
        assertThatThrownBy(() -> new OmsClusterShardRouter(2, Map.of(0, client)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("clientByShard.size=1")
                .hasMessageContaining("shardCount=2");

        // Map has the right size but missing shard 0 (e.g. an off-by-one shard-id misconfig).
        Map<Integer, OmsClusterIngressClient> wrongIds = Map.of(1, client);
        assertThatThrownBy(() -> new OmsClusterShardRouter(1, wrongIds))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("missing client for shard id 0");
    }

    @Test
    void directConstructor_rejectsNullClient() {
        // Map.of forbids null values, so build the map with an explicit null via a HashMap to
        // exercise the router's defensive null check rather than Map.of's.
        Map<Integer, OmsClusterIngressClient> withNull = new HashMap<>();
        withNull.put(0, null);
        assertThatThrownBy(() -> new OmsClusterShardRouter(1, withNull))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("null client for shard id 0");
    }

    @Test
    void directConstructor_supportsMultiShardForFutureSlices() {
        // The direct constructor is the API E-3 will use once N>1 client beans are injected.
        // Verify the multi-shard path works today (the Spring constructor's E-1 cap is
        // enforced at the Spring entry point, not in the direct constructor).
        OmsClusterIngressClient s0 = mock(OmsClusterIngressClient.class);
        OmsClusterIngressClient s1 = mock(OmsClusterIngressClient.class);
        OmsClusterShardRouter router = new OmsClusterShardRouter(2, Map.of(0, s0, 1, s1));

        assertThat(router.shardCount()).isEqualTo(2);
        assertThat(router.forShard(0)).isSameAs(s0);
        assertThat(router.forShard(1)).isSameAs(s1);

        // routeAdmit must split traffic by the xxh64 hash; verify at least one admit lands on
        // each shard across a small sample (probabilistic but the chance of all 200 mapping to
        // the same shard at a 2-shard split is 2^-199 ≈ 0).
        boolean sawShard0 = false;
        boolean sawShard1 = false;
        for (int i = 0; i < 200; i++) {
            OmsClusterIngressClient picked = router.routeAdmit(UUID.randomUUID());
            if (picked == s0) sawShard0 = true;
            if (picked == s1) sawShard1 = true;
        }
        assertThat(sawShard0).as("at least one admit routed to shard 0").isTrue();
        assertThat(sawShard1).as("at least one admit routed to shard 1").isTrue();
    }
}
