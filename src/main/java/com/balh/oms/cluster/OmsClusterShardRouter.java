package com.balh.oms.cluster;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.config.OmsProfiles;
import com.balh.oms.domain.ShardKey;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Picks the {@link OmsClusterIngressClient} that owns a given shard so call sites do not need to
 * know how the cluster topology is laid out. Phase 4 Tier 2.5 phase E-1 introduces this layer as
 * the byte-identical-at-{@code N=1} indirection that later slices replace internally without
 * touching call sites:
 *
 * <ul>
 *   <li><b>E-1 (this slice):</b> {@code shardCount} is pinned to {@code 1} and the router holds a
 *       single {@link OmsClusterIngressClient} bean — every call resolves to that one client.
 *       Behaviour is byte-identical to the pre-E-1 direct injection of
 *       {@code OmsClusterIngressClient}; the only observable change is the new indirection.</li>
 *   <li><b>E-3:</b> the Spring constructor evolves to inject {@code N} qualified
 *       {@code OmsClusterIngressClient} beans (one per shard) and populate
 *       {@link #clientByShard} with all of them.</li>
 *   <li><b>Phase 5:</b> the per-shard client endpoints move from {@code localhost:20110/20120} to
 *       multi-host endpoints. <em>Zero</em> code change here — only operational config.</li>
 * </ul>
 *
 * <h2>Routing function</h2>
 *
 * <p>Routing matches {@code decisions.md} §4 (Sharding): the canonical mapping is
 * {@link ShardKey#shardFor(UUID, int)} (xxh64 of {@code accountId} mod {@code shardCount}). All
 * sharding-relevant call sites — the ingress admit path here, plus the future shard-aware FIX
 * inbound and inflight compensator paths in E-3 — must derive their shard via this function so
 * that operations on the same {@code accountId} land on the same cluster.
 *
 * <h2>What does <em>not</em> live here</h2>
 *
 * <p>E-1 deliberately leaves {@code submitApplyExecutionReport} (FIX inbound) and
 * {@code submitCancelOrder} (compensator) on the singleton client. At {@code N=1} the singleton is
 * <em>the same instance</em> the router would return anyway, so routing those paths is observably
 * a no-op. They move onto the router in E-3 once the cluster log carries an explicit
 * {@code shardId} and the corresponding {@code orderId} → shard lookup is wired through the
 * inflight outbox / FIX inbound queries.
 *
 * <h2>Profile gating</h2>
 *
 * <p>Mirrors {@link OmsClusterIngressClient}: present on JVMs that submit cluster commands (HTTP /
 * gRPC ingress and the future {@code oms-fix-egress} shard-aware paths), absent on pure
 * cluster-internal roles ({@code oms-postgres-projector}, {@code oms-cluster-node}). The
 * {@code @ConditionalOnProperty} matches the underlying client so when the client bean is
 * disabled, the router is too — no orphan router referencing a missing client.
 */
@Component
@Profile(OmsProfiles.CLUSTER_CLIENT_PROFILE)
@ConditionalOnProperty(prefix = "oms.cluster.client", name = "enabled", havingValue = "true")
public final class OmsClusterShardRouter {

    /**
     * Slice E-1 invariant. Until E-3 wires multi-bean injection, the router can only host a
     * single {@link OmsClusterIngressClient}, which means {@code oms.shard.count} must be
     * {@code 1}. Setting it higher fails fast at construction with an explicit message that
     * names the slice that lifts the constraint.
     */
    static final int E1_MAX_SHARD_COUNT = 1;

    private final int shardCount;
    private final Map<Integer, OmsClusterIngressClient> clientByShard;

    /**
     * Spring constructor. At slice E-1 there is exactly one {@link OmsClusterIngressClient} bean
     * in the context, which the router maps to shard {@code 0}. Later slices (E-3) replace this
     * constructor body with multi-bean injection; the {@link #routeAdmit(UUID)} /
     * {@link #forShard(int)} contract stays stable.
     *
     * @throws IllegalStateException if {@link OmsConfig.Shard#getCount()} exceeds
     *                               {@link #E1_MAX_SHARD_COUNT} (E-3 lifts this)
     * @throws IllegalArgumentException if {@code oms.shard.count} is not strictly positive
     */
    @Autowired
    public OmsClusterShardRouter(OmsConfig config, OmsClusterIngressClient singleClient) {
        // Validate the E-1 invariant <em>before</em> delegating to the direct constructor so a
        // multi-shard config produces an operator-actionable error message naming the slice that
        // lifts the constraint, instead of a generic size-mismatch from the delegate.
        this(
                checkE1Invariant(Objects.requireNonNull(config, "config").getShard().getCount()),
                Map.of(0, Objects.requireNonNull(singleClient, "singleClient")));
    }

    private static int checkE1Invariant(int requestedShardCount) {
        if (requestedShardCount > E1_MAX_SHARD_COUNT) {
            throw new IllegalStateException(
                    "OmsClusterShardRouter: oms.shard.count="
                            + requestedShardCount
                            + " is not yet supported on this build (slice E-1 caps shardCount at "
                            + E1_MAX_SHARD_COUNT
                            + "; multi-shard client injection lands in slice E-3).");
        }
        return requestedShardCount;
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
