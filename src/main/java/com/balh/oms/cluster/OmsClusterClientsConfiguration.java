package com.balh.oms.cluster;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.config.OmsProfiles;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Phase 4 Tier 2.5 phase E-3b — produces the {@link OmsClusterIngressClient} bean(s) one ingress
 * JVM exposes. Replaces the previous {@code @Component} on
 * {@link OmsClusterIngressClient} so the same JVM can hold {@code N} clients (one per shard)
 * when {@code oms.shard.count > 1}.
 *
 * <h2>Bean shape</h2>
 *
 * <ul>
 *   <li>{@link #omsClusterIngressClients(OmsConfig, MeterRegistry)} — qualified bean
 *       {@code "omsClusterIngressClients"}, an immutable {@code Map<Integer, OmsClusterIngressClient>}
 *       keyed by shard id and covering exactly {@code [0, oms.shard.count)}. This is the bean
 *       {@link OmsClusterShardRouter} consumes.</li>
 *   <li>{@link #omsClusterIngressClient(Map)} — back-compat singleton bean named
 *       {@code "omsClusterIngressClient"} that returns {@code clients.get(0)}. Kept so call sites
 *       that still {@code @Autowire OmsClusterIngressClient} directly (today: {@code
 *       FixInboundClusterSink}) continue to compile and route to shard 0. At
 *       {@code oms.shard.count > 1} this bean is still wired, but FIX-inbound is not yet
 *       shard-aware (a future slice will read the order's shard from the cluster log and route
 *       on the {@link OmsClusterShardRouter} instead). Documented in the class javadoc on
 *       {@link OmsClusterIngressClient}.</li>
 * </ul>
 *
 * <h2>Per-shard config resolution</h2>
 *
 * <p>For each shard id {@code s ∈ [0, N)} we clone the flat {@code oms.cluster.client} block and
 * apply the matching {@link OmsConfig.Cluster.Client.ShardOverride} (if any). Only
 * {@code aeronDirectory} and {@code ingressEndpoints} are per-shard today; everything else
 * ({@code messageTimeoutMs}, {@code admitBatch}, …) is deployment-wide and reuses the flat
 * values. At {@code N=1} we accept either the flat config alone (no overrides) <em>or</em> a
 * single override with id 0 — the byte-identical-at-N=1 path.
 *
 * <h2>Profile / conditional gating</h2>
 *
 * <p>Mirrors the gating that previously lived on {@link OmsClusterIngressClient}: the
 * {@link Profile} pins to {@link OmsProfiles#CLUSTER_CLIENT_PROFILE} (ingress JVMs and
 * {@code oms-fix-egress}) and {@link ConditionalOnProperty} matches
 * {@code oms.cluster.client.enabled=true}. Worker / projector profiles never load this
 * configuration even if their environment flips the property on, because they fall outside the
 * profile expression.
 */
@Configuration(proxyBeanMethods = false)
@Profile(OmsProfiles.CLUSTER_CLIENT_PROFILE)
@ConditionalOnProperty(prefix = "oms.cluster.client", name = "enabled", havingValue = "true")
public class OmsClusterClientsConfiguration {

    private static final Logger log = LoggerFactory.getLogger(OmsClusterClientsConfiguration.class);

    /**
     * Holds the constructed clients so {@link #close()} can call {@code close()} on each at
     * Spring shutdown — Spring already invokes the per-bean {@link jakarta.annotation.PreDestroy}
     * on {@link OmsClusterIngressClient}, so this map is kept only for diagnostic logging /
     * defensive iteration if a future bean shape stops carrying its own destroy hook.
     */
    private volatile Map<Integer, OmsClusterIngressClient> built = Collections.emptyMap();

    @Bean(name = "omsClusterIngressClients")
    @Profile("!" + OmsProfiles.VENUE_EGRESS)
    public Map<Integer, OmsClusterIngressClient> omsClusterIngressClients(
            OmsConfig config, MeterRegistry meterRegistry) {
        int shardCount = config.getShard().getCount();
        if (shardCount < 1) {
            throw new IllegalStateException(
                    "OmsClusterClientsConfiguration: oms.shard.count must be >= 1 (got "
                            + shardCount + ")");
        }
        OmsConfig.Cluster.Client template = config.getCluster().getClient();

        Map<Integer, OmsConfig.Cluster.Client.ShardOverride> overridesById = new HashMap<>();
        for (OmsConfig.Cluster.Client.ShardOverride override : template.getShards()) {
            if (override.getId() < 0) {
                throw new IllegalStateException(
                        "OmsClusterClientsConfiguration: oms.cluster.client.shards[].id must be set (got "
                                + override.getId() + ")");
            }
            if (override.getId() >= shardCount) {
                throw new IllegalStateException(
                        "OmsClusterClientsConfiguration: oms.cluster.client.shards[].id="
                                + override.getId()
                                + " is out of range [0, " + shardCount + ")");
            }
            if (overridesById.put(override.getId(), override) != null) {
                throw new IllegalStateException(
                        "OmsClusterClientsConfiguration: duplicate shard override for id="
                                + override.getId());
            }
        }

        if (shardCount > 1 && overridesById.size() != shardCount) {
            throw new IllegalStateException(
                    "OmsClusterClientsConfiguration: oms.shard.count="
                            + shardCount
                            + " requires that oms.cluster.client.shards lists every shard id in [0,"
                            + shardCount + "); found ids=" + overridesById.keySet());
        }

        Map<Integer, OmsConfig.Cluster.Client> mergedByShard = new LinkedHashMap<>(shardCount);
        for (int s = 0; s < shardCount; s++) {
            OmsConfig.Cluster.Client.ShardOverride override = overridesById.get(s);
            mergedByShard.put(s, mergeForShard(template, override, shardCount));
        }
        if (shardCount > 1) {
            // Two shards pointing at the same Aeron media driver / ingress endpoints would silently
            // merge sessions and route both shards to the same cluster — the exact failure mode the
            // factory exists to prevent. The default {@code OmsConfig.Cluster.Client} setter
            // back-fills empty fields with {@code 0=localhost:20110} (valid for single-shard) so we
            // verify uniqueness here rather than emptiness in {@link #mergeForShard}.
            Map<String, Integer> aeronDirOwners = new HashMap<>();
            Map<String, Integer> ingressOwners = new HashMap<>();
            for (Map.Entry<Integer, OmsConfig.Cluster.Client> e : mergedByShard.entrySet()) {
                Integer prevAeron = aeronDirOwners.put(e.getValue().getAeronDirectory(), e.getKey());
                if (prevAeron != null) {
                    throw new IllegalStateException(
                            "OmsClusterClientsConfiguration: shards " + prevAeron + " and " + e.getKey()
                                    + " share aeron-directory=" + e.getValue().getAeronDirectory()
                                    + "; each shard's cluster client must have its own Aeron media driver");
                }
                Integer prevIngress = ingressOwners.put(e.getValue().getIngressEndpoints(), e.getKey());
                if (prevIngress != null) {
                    throw new IllegalStateException(
                            "OmsClusterClientsConfiguration: shards " + prevIngress + " and " + e.getKey()
                                    + " share ingress-endpoints=" + e.getValue().getIngressEndpoints()
                                    + "; each shard's cluster client must point at its own consensus endpoints");
                }
            }
        }

        Map<Integer, OmsClusterIngressClient> clients = new LinkedHashMap<>(shardCount);
        for (Map.Entry<Integer, OmsConfig.Cluster.Client> e : mergedByShard.entrySet()) {
            log.info(
                    "OMS cluster client (shard {}/{}): aeronDir={} ingressEndpoints={}",
                    e.getKey(),
                    shardCount,
                    e.getValue().getAeronDirectory(),
                    e.getValue().getIngressEndpoints());
            OmsClusterIngressClient client = createClient(e.getValue(), meterRegistry);
            // Phase 4 Tier 2.5 phase E-3b: when this factory builds clients via {@code new}, Spring
            // does not run the {@link OmsClusterIngressClient#connect() @PostConstruct} on the
            // returned objects (only the back-compat singleton bean exposed below has its lifecycle
            // managed). Without an explicit {@code connect()} here, every shard except shard 0
            // would silently stay unconnected and {@code submit*} would throw
            // {@code IllegalStateException("OMS cluster client is not connected")} at runtime —
            // exactly the failure mode the first 2-shard end-to-end smoke hit (2130/4000 commands
            // routed to shard 1 returned HTTP 503). {@code connect()} is idempotent so re-invoking
            // it on shard 0 (which Spring does call via the singleton bean's @PostConstruct) is a
            // no-op.
            client.connect();
            clients.put(e.getKey(), client);
        }
        Map<Integer, OmsClusterIngressClient> immutable = Collections.unmodifiableMap(clients);
        this.built = immutable;
        return immutable;
    }

    /**
     * Test seam for {@link #omsClusterIngressClients}. Production wiring constructs a real
     * {@link OmsClusterIngressClient}; tests override this to return a spy/mock so they can
     * assert e.g. that {@code connect()} is called on <em>every</em> shard's client (not just
     * shard 0 — the bug Phase 4 Tier 2.5 phase E-3b's first Pop! 2-shard e2e attempt hit).
     */
    OmsClusterIngressClient createClient(
            OmsConfig.Cluster.Client clientConfig, MeterRegistry meterRegistry) {
        return new OmsClusterIngressClient(clientConfig, meterRegistry);
    }

    /**
     * Back-compat shim. Resolves to {@code clientByShard.get(0)} so call sites that still inject
     * {@link OmsClusterIngressClient} directly (today: {@code FixInboundClusterSink} on the
     * {@code oms-fix-egress} JVM) keep compiling. At {@code oms.shard.count > 1} the FIX-inbound
     * path remains shard-0 only; making it shard-aware is a follow-up slice that requires an
     * order-id → shard lookup the cluster log does not yet expose to FIX-egress.
     *
     * <p>{@code @Bean(autowireCandidate = true)} is implicit; the qualified bean above coexists
     * because Spring resolves by type + qualifier, and a {@code Map<Integer, OmsClusterIngressClient>}
     * never matches a {@code OmsClusterIngressClient} injection point.
     */
    @Bean(name = "omsClusterIngressClient")
    @Profile("!" + OmsProfiles.VENUE_EGRESS)
    public OmsClusterIngressClient omsClusterIngressClient(
            @Qualifier("omsClusterIngressClients")
                    Map<Integer, OmsClusterIngressClient> clientByShard) {
        OmsClusterIngressClient shardZero = clientByShard.get(0);
        if (shardZero == null) {
            throw new IllegalStateException(
                    "OmsClusterClientsConfiguration: shard-0 client missing; cannot expose back-compat singleton");
        }
        return shardZero;
    }

    @PreDestroy
    void close() {
        Map<Integer, OmsClusterIngressClient> snapshot = built;
        built = Collections.emptyMap();
        if (snapshot.isEmpty()) {
            return;
        }
        log.info("OMS cluster client factory shutting down: shardCount={}", snapshot.size());
        // Phase 4 Tier 2.5 phase E-3b: same Spring-lifecycle nuance as the {@code connect()} bug
        // fixed in {@link #omsClusterIngressClients}: only shard 0's client is a Spring-managed
        // bean (via the back-compat {@link #omsClusterIngressClient} method) and so only its
        // {@link OmsClusterIngressClient#close() @PreDestroy} runs automatically. Shards 1..N-1
        // were created with {@code new} inside the factory, so we must close them explicitly here
        // or operators see leaked Aeron media-driver subscriptions / publications past JVM
        // shutdown. {@link OmsClusterIngressClient#close()} is idempotent (guards on null
        // {@code client}), so double-closing shard 0 if Spring does call it later is safe.
        for (Map.Entry<Integer, OmsClusterIngressClient> e : snapshot.entrySet()) {
            if (e.getKey() == 0) {
                continue;
            }
            try {
                e.getValue().close();
            } catch (RuntimeException ex) {
                log.warn("Error closing OmsClusterIngressClient for shard {}: {}", e.getKey(), ex.toString(), ex);
            }
        }
    }

    /**
     * Builds an effective per-shard {@link OmsConfig.Cluster.Client} by copying every field from
     * {@code template} and overlaying the {@code aeronDirectory} / {@code ingressEndpoints}
     * values from {@code override} when present. The returned object is detached from
     * {@code template} so each shard's client owns its own settings instance (no aliasing across
     * shards).
     */
    static OmsConfig.Cluster.Client mergeForShard(
            OmsConfig.Cluster.Client template,
            OmsConfig.Cluster.Client.ShardOverride override,
            int shardCount) {
        OmsConfig.Cluster.Client out = new OmsConfig.Cluster.Client();
        out.setEnabled(template.isEnabled());
        out.setIngressChannel(template.getIngressChannel());
        out.setEgressChannel(template.getEgressChannel());
        out.setSubmitTimeoutMs(template.getSubmitTimeoutMs());
        out.setMessageTimeoutMs(template.getMessageTimeoutMs());
        out.setConnectTimeoutMs(template.getConnectTimeoutMs());
        out.setOfferBackpressureParkNanos(template.getOfferBackpressureParkNanos());
        out.setEgressPollParkNanos(template.getEgressPollParkNanos());
        out.setHeartbeatIntervalMs(template.getHeartbeatIntervalMs());
        // AdmitBatch is shared deployment-wide today; copy its values into the per-shard config so
        // each client owns a fresh instance (avoids surprise mutations across shards).
        out.getAdmitBatch().setEnabled(template.getAdmitBatch().isEnabled());
        out.getAdmitBatch().setMaxBatchSize(template.getAdmitBatch().getMaxBatchSize());
        out.getAdmitBatch().setFlushIntervalNanos(template.getAdmitBatch().getFlushIntervalNanos());
        out.getAdmitBatch().setQueueCapacity(template.getAdmitBatch().getQueueCapacity());
        out.getAdmitBatch().setEnqueueParkNanos(template.getAdmitBatch().getEnqueueParkNanos());

        String aeronDir = template.getAeronDirectory();
        String ingressEndpoints = template.getIngressEndpoints();
        if (override != null) {
            if (!override.getAeronDirectory().isEmpty()) {
                aeronDir = override.getAeronDirectory();
            }
            if (!override.getIngressEndpoints().isEmpty()) {
                ingressEndpoints = override.getIngressEndpoints();
            }
        }
        if (shardCount > 1 && (aeronDir == null || aeronDir.isEmpty())) {
            // The flat setter back-fills ingressEndpoints with the localhost default when empty,
            // so collisions across shards are caught by the cross-shard uniqueness check in
            // {@link #omsClusterIngressClients}; aeronDirectory has no such default and is the
            // first place a missing per-shard config surfaces.
            throw new IllegalStateException(
                    "OmsClusterClientsConfiguration: oms.cluster.client.shards[].aeron-directory must be set"
                            + " for every shard when oms.shard.count > 1");
        }
        out.setAeronDirectory(aeronDir);
        out.setIngressEndpoints(ingressEndpoints);
        return out;
    }
}
