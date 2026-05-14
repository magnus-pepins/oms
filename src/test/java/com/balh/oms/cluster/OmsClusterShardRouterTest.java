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
 * Unit tests for {@link OmsClusterShardRouter}.
 *
 * <p>Phase 4 Tier 2.5 phase E-1 introduced the router as a single-shard indirection. Phase
 * E-3b lifted the cap so the router accepts an {@code N}-element {@code Map} produced by
 * {@link OmsClusterClientsConfiguration} and routes admits across the per-shard clients. These
 * tests exercise both the new Spring constructor (which now takes the qualified map directly)
 * and the direct test-friendly constructor.
 */
class OmsClusterShardRouterTest {

    @Test
    void springConstructor_atShardCount1_resolvesAnyAccountIdToTheSingleClient() {
        OmsConfig config = new OmsConfig();
        config.getShard().setCount(1);
        OmsClusterIngressClient client = mock(OmsClusterIngressClient.class);

        OmsClusterShardRouter router = new OmsClusterShardRouter(config, Map.of(0, client));

        assertThat(router.shardCount()).isEqualTo(1);
        assertThat(router.forShard(0)).isSameAs(client);
        assertThat(router.routeAdmit(UUID.randomUUID())).isSameAs(client);
        assertThat(router.routeAdmit(UUID.randomUUID())).isSameAs(client);
    }

    @Test
    void springConstructor_atShardCount2_routesAcrossInjectedMap() {
        // E-3b: Spring now wires N clients via the qualified map produced by
        // OmsClusterClientsConfiguration. Verify the router uses every entry and that
        // routeAdmit splits by the xxh64 hash (probabilistic; see multi-shard direct-ctor test
        // below for the same assertion at a different entry point).
        OmsConfig config = new OmsConfig();
        config.getShard().setCount(2);
        OmsClusterIngressClient s0 = mock(OmsClusterIngressClient.class);
        OmsClusterIngressClient s1 = mock(OmsClusterIngressClient.class);

        OmsClusterShardRouter router = new OmsClusterShardRouter(config, Map.of(0, s0, 1, s1));

        assertThat(router.shardCount()).isEqualTo(2);
        assertThat(router.forShard(0)).isSameAs(s0);
        assertThat(router.forShard(1)).isSameAs(s1);
    }

    @Test
    void springConstructor_rejectsClientMapMismatchedToShardCount() {
        // E-3b: with the cap lifted, the router still defends against a misconfigured factory
        // that hands it a map missing one of the shards. The error names the specific shard so
        // the operator does not have to count map entries.
        OmsConfig config = new OmsConfig();
        config.getShard().setCount(2);
        OmsClusterIngressClient onlyShard0 = mock(OmsClusterIngressClient.class);

        assertThatThrownBy(() -> new OmsClusterShardRouter(config, Map.of(0, onlyShard0)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("clientByShard.size=1")
                .hasMessageContaining("shardCount=2");
    }

    @Test
    void routeAdmit_isStableAcrossCalls_byHashFunction() {
        OmsClusterIngressClient client = mock(OmsClusterIngressClient.class);
        OmsClusterShardRouter router = new OmsClusterShardRouter(1, Map.of(0, client));

        UUID account = UUID.randomUUID();
        // Two calls with the same accountId must resolve to the same client (E-1 trivially true,
        // but pinned here so E-3b cannot regress determinism without the test catching it).
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

        assertThatThrownBy(() -> new OmsClusterShardRouter(2, Map.of(0, client)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("clientByShard.size=1")
                .hasMessageContaining("shardCount=2");

        Map<Integer, OmsClusterIngressClient> wrongIds = Map.of(1, client);
        assertThatThrownBy(() -> new OmsClusterShardRouter(1, wrongIds))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("missing client for shard id 0");
    }

    @Test
    void directConstructor_rejectsNullClient() {
        Map<Integer, OmsClusterIngressClient> withNull = new HashMap<>();
        withNull.put(0, null);
        assertThatThrownBy(() -> new OmsClusterShardRouter(1, withNull))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("null client for shard id 0");
    }

    @Test
    void directConstructor_atShardCount2_routesAdmitsByXxh64() {
        // E-3b production path: the factory hands the router N clients and routeAdmit must split
        // traffic by ShardKey.shardFor (xxh64 of accountId mod shardCount). The router itself
        // never calls the underlying client; we just check identity routing.
        OmsClusterIngressClient s0 = mock(OmsClusterIngressClient.class);
        OmsClusterIngressClient s1 = mock(OmsClusterIngressClient.class);
        OmsClusterShardRouter router = new OmsClusterShardRouter(2, Map.of(0, s0, 1, s1));

        assertThat(router.shardCount()).isEqualTo(2);
        assertThat(router.forShard(0)).isSameAs(s0);
        assertThat(router.forShard(1)).isSameAs(s1);

        // Probabilistic split sanity check: across 200 random accountIds we must see at least
        // one admit on each shard (chance of a 200-in-a-row monoshard at a 2-shard split is
        // 2^-199 ≈ 0). Pinned so a routing regression that always returns shard 0 is caught.
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
