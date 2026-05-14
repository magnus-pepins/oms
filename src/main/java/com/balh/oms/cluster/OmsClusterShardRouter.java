package com.balh.oms.cluster;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.config.OmsProfiles;
import com.balh.oms.domain.ShardKey;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Picks the {@link OmsClusterIngressClient} that owns a given shard so call sites do not need to
 * know how the cluster topology is laid out. Phase 4 Tier 2.5 phase E-1 introduced the
 * indirection at {@code shardCount=1}; phase E-3b lifted the cap so a single ingress JVM can
 * route across {@code N} clusters via the per-shard client map produced by
 * {@link OmsClusterClientsConfiguration}.
 *
 * <ul>
 *   <li><b>E-1:</b> router holds a single {@link OmsClusterIngressClient} bean — byte-identical
 *       to the pre-E-1 direct injection.</li>
 *   <li><b>E-3b (this slice):</b> Spring constructor takes the
 *       {@code Map<Integer, OmsClusterIngressClient>} produced by
 *       {@link OmsClusterClientsConfiguration} and routes per shard. {@code oms.shard.count > 1}
 *       is now supported inside one ingress JVM.</li>
 *   <li><b>Phase 5:</b> the per-shard client endpoints move from {@code localhost:20110/20120} to
 *       multi-host endpoints. <em>Zero</em> code change here — only operational config.</li>
 * </ul>
 *
 * <h2>Routing function</h2>
 *
 * <p>Routing matches {@code decisions.md} §4 (Sharding): the canonical mapping is
 * {@link ShardKey#shardFor(UUID, int)} (xxh64 of {@code accountId} mod {@code shardCount}). All
 * sharding-relevant call sites — the ingress admit path here, plus the inflight compensator's
 * cancel path (already on the router since E-2) — must derive their shard via this function so
 * that operations on the same {@code accountId} land on the same cluster.
 *
 * <h2>What does <em>not</em> yet live here</h2>
 *
 * <p>{@code FixInboundClusterSink} on the {@code oms-fix-egress} JVM still injects the
 * back-compat singleton {@link OmsClusterIngressClient} bean (shard-0). Making FIX-inbound
 * shard-aware requires an {@code orderId → shardId} lookup that the cluster log does not yet
 * expose to FIX-egress; this is a documented follow-up. {@code oms.shard.count > 1} is therefore
 * supported on ingress JVMs only at E-3b; FIX-egress JVMs must keep {@code oms.shard.count=1}.
 *
 * <h2>Profile gating</h2>
 *
 * <p>Mirrors {@link OmsClusterIngressClient} / {@link OmsClusterClientsConfiguration}: present on
 * JVMs that submit cluster commands (HTTP / gRPC ingress and {@code oms-fix-egress}), absent on
 * pure cluster-internal roles ({@code oms-postgres-projector}, {@code oms-cluster-node}). The
 * {@code @ConditionalOnProperty} matches the underlying factory so when the client beans are
 * disabled, the router is too — no orphan router referencing missing clients.
 */
@Component
@Profile(OmsProfiles.CLUSTER_CLIENT_PROFILE)
@ConditionalOnProperty(prefix = "oms.cluster.client", name = "enabled", havingValue = "true")
public final class OmsClusterShardRouter {

    private final int shardCount;
    private final Map<Integer, OmsClusterIngressClient> clientByShard;

    /**
     * Spring constructor. The {@code @Qualifier("omsClusterIngressClients")} matches the bean
     * name produced by {@link OmsClusterClientsConfiguration#omsClusterIngressClients}; without
     * the qualifier Spring would also see the back-compat singleton bean exposed by
     * {@link OmsClusterClientsConfiguration#omsClusterIngressClient} and fail with a
     * {@code NoUniqueBeanDefinitionException} on multi-shard configurations.
     *
     * @throws IllegalArgumentException if {@code oms.shard.count} is not strictly positive or if
     *                                  the injected map does not cover every shard id in
     *                                  {@code [0, oms.shard.count)}
     */
    @Autowired
    public OmsClusterShardRouter(
            OmsConfig config,
            @Qualifier("omsClusterIngressClients")
                    Map<Integer, OmsClusterIngressClient> clientByShard) {
        this(
                Objects.requireNonNull(config, "config").getShard().getCount(),
                Objects.requireNonNull(clientByShard, "clientByShard"));
    }

    /**
     * Direct constructor for tests (and for E-3's multi-shard wiring once it ships). Accepts a
     * pre-built shard → client mapping; validates that {@code shardCount} is strictly positive and
     * that {@code clientByShard} contains exactly the shard ids {@code [0, shardCount)}.
     *
     * <p>Intentionally package-public so the production Spring constructor can delegate, and tests
     * in the same package can construct multi-shard routers without going through Spring. Marked
     * {@code public} (not {@code package-private}) so unit tests in
     * {@code com.balh.oms.cluster} <em>and</em> service-level tests in
     * {@code com.balh.oms.ingress} can reuse it without reflection tricks.
     */
    public OmsClusterShardRouter(int shardCount, Map<Integer, OmsClusterIngressClient> clientByShard) {
        if (shardCount < 1) {
            throw new IllegalArgumentException(
                    "OmsClusterShardRouter: shardCount must be >= 1 (got " + shardCount + ")");
        }
        Objects.requireNonNull(clientByShard, "clientByShard");
        if (clientByShard.size() != shardCount) {
            throw new IllegalArgumentException(
                    "OmsClusterShardRouter: clientByShard.size="
                            + clientByShard.size()
                            + " does not match shardCount="
                            + shardCount);
        }
        for (int s = 0; s < shardCount; s++) {
            if (!clientByShard.containsKey(s)) {
                throw new IllegalArgumentException(
                        "OmsClusterShardRouter: missing client for shard id " + s);
            }
            if (clientByShard.get(s) == null) {
                throw new IllegalArgumentException(
                        "OmsClusterShardRouter: null client for shard id " + s);
            }
        }
        this.shardCount = shardCount;
        this.clientByShard = Map.copyOf(clientByShard);
    }

    /**
     * Picks the cluster client that owns the shard for {@code accountId}. Used on the ingress
     * admit path; the same {@code accountId} is also passed to {@link ShardKey#shardFor} when
     * stamping {@code Order.shardId}, so the order's recorded shard and its admitting cluster are
     * always the same.
     */
    public OmsClusterIngressClient routeAdmit(UUID accountId) {
        Objects.requireNonNull(accountId, "accountId");
        return forShard(ShardKey.shardFor(accountId, shardCount));
    }

    /**
     * Picks the cluster client that owns {@code shardId}. Used by call sites that already know
     * the shard (e.g. compensator queries that select rows by {@code shard_id}, future FIX
     * inbound paths that resolve the shard from the order). Throws on out-of-range so a stale or
     * corrupt shard id is loud at the call site instead of silently misrouting.
     */
    public OmsClusterIngressClient forShard(int shardId) {
        if (shardId < 0 || shardId >= shardCount) {
            throw new IllegalArgumentException(
                    "OmsClusterShardRouter: shardId="
                            + shardId
                            + " is out of range [0, "
                            + shardCount
                            + ")");
        }
        return clientByShard.get(shardId);
    }

    /** Number of shards currently hosted by this router. Always matches {@code oms.shard.count}. */
    public int shardCount() {
        return shardCount;
    }
}
