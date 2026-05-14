package com.balh.oms.cluster;

import com.balh.oms.config.OmsConfig;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Phase 4 Tier 2.5 phase E-3b — unit tests for {@link OmsClusterClientsConfiguration}'s
 * per-shard config merge and validation. The factory's bean-graph integration is covered by
 * {@code OmsClusterIngressClientIT} (real Aeron) and the existing application IT suites; here we
 * exercise the pure-logic pieces ({@link OmsClusterClientsConfiguration#mergeForShard}) that
 * E-3b adds.
 */
class OmsClusterClientsConfigurationTest {

    @Test
    void mergeForShard_atShardCount1_withoutOverride_returnsTemplateValuesUnchanged() {
        // E-3b byte-identical-at-N=1 path: no shard override list set, mergeForShard with a null
        // override and shardCount=1 must return a fresh client config matching the template.
        OmsConfig.Cluster.Client template = newTemplate("/var/oms/aeron-default", "0=localhost:20110");

        OmsConfig.Cluster.Client merged =
                OmsClusterClientsConfiguration.mergeForShard(template, null, /* shardCount = */ 1);

        assertThat(merged).isNotSameAs(template);
        assertThat(merged.getAeronDirectory()).isEqualTo("/var/oms/aeron-default");
        assertThat(merged.getIngressEndpoints()).isEqualTo("0=localhost:20110");
        assertThat(merged.getIngressChannel()).isEqualTo(template.getIngressChannel());
        assertThat(merged.getEgressChannel()).isEqualTo(template.getEgressChannel());
        assertThat(merged.getMessageTimeoutMs()).isEqualTo(template.getMessageTimeoutMs());
        assertThat(merged.getAdmitBatch().isEnabled()).isEqualTo(template.getAdmitBatch().isEnabled());
    }

    @Test
    void mergeForShard_appliesOverrideForAeronDirectoryAndIngressEndpoints() {
        OmsConfig.Cluster.Client template = newTemplate("/var/oms/aeron-default", "0=localhost:20110");
        OmsConfig.Cluster.Client.ShardOverride override = new OmsConfig.Cluster.Client.ShardOverride();
        override.setId(1);
        override.setAeronDirectory("/var/oms/aeron-shard-1");
        override.setIngressEndpoints("0=localhost:21110");

        OmsConfig.Cluster.Client merged =
                OmsClusterClientsConfiguration.mergeForShard(template, override, /* shardCount = */ 2);

        assertThat(merged.getAeronDirectory()).isEqualTo("/var/oms/aeron-shard-1");
        assertThat(merged.getIngressEndpoints()).isEqualTo("0=localhost:21110");
        // Non-overridable fields still come from the template.
        assertThat(merged.getMessageTimeoutMs()).isEqualTo(template.getMessageTimeoutMs());
        assertThat(merged.getOfferBackpressureParkNanos())
                .isEqualTo(template.getOfferBackpressureParkNanos());
    }

    @Test
    void mergeForShard_failsWhenMultiShardOverrideMissesAeronDirectory() {
        // E-3b: at oms.shard.count > 1 every shard override must explicitly set aeronDirectory
        // and ingressEndpoints (or the template's flat values must be set, but two shards sharing
        // a media driver would silently mix sessions). The factory fails fast here so the
        // operator gets a named error at boot rather than an Aeron Cluster connect timeout.
        OmsConfig.Cluster.Client template = newTemplate("", /* ingressEndpoints = */ "");
        OmsConfig.Cluster.Client.ShardOverride override = new OmsConfig.Cluster.Client.ShardOverride();
        override.setId(0);
        override.setIngressEndpoints("0=localhost:20110");

        assertThatThrownBy(() ->
                OmsClusterClientsConfiguration.mergeForShard(template, override, /* shardCount = */ 2))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("aeron-directory must be set")
                .hasMessageContaining("oms.shard.count > 1");
    }

    @Test
    void omsClusterIngressClients_failsWhenTwoShardsShareAeronDirectory() {
        // Two shards resolving to the same Aeron media-driver directory would silently merge
        // sessions and route both shards to the same cluster. The factory must fail fast at
        // bean build time with a named error so the operator sees this at boot, not at the
        // first cross-shard admit.
        OmsConfig config = new OmsConfig();
        config.getShard().setCount(2);
        config.getCluster().getClient().setAeronDirectory("/var/oms/aeron-shared");
        config.getCluster().getClient().setIngressEndpoints("0=localhost:20110");

        OmsConfig.Cluster.Client.ShardOverride o0 = new OmsConfig.Cluster.Client.ShardOverride();
        o0.setId(0);
        o0.setAeronDirectory("/var/oms/aeron-shared");
        o0.setIngressEndpoints("0=localhost:20110");
        OmsConfig.Cluster.Client.ShardOverride o1 = new OmsConfig.Cluster.Client.ShardOverride();
        o1.setId(1);
        o1.setAeronDirectory("/var/oms/aeron-shared");
        o1.setIngressEndpoints("0=localhost:21110");
        config.getCluster().getClient().setShards(java.util.List.of(o0, o1));

        OmsClusterClientsConfiguration factory = new OmsClusterClientsConfiguration();

        assertThatThrownBy(() -> factory.omsClusterIngressClients(
                config, new io.micrometer.core.instrument.simple.SimpleMeterRegistry()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("share aeron-directory=/var/oms/aeron-shared");
    }

    @Test
    void omsClusterIngressClients_failsWhenTwoShardsShareIngressEndpoints() {
        OmsConfig config = new OmsConfig();
        config.getShard().setCount(2);

        OmsConfig.Cluster.Client.ShardOverride o0 = new OmsConfig.Cluster.Client.ShardOverride();
        o0.setId(0);
        o0.setAeronDirectory("/var/oms/aeron-shard-0");
        o0.setIngressEndpoints("0=localhost:20110");
        OmsConfig.Cluster.Client.ShardOverride o1 = new OmsConfig.Cluster.Client.ShardOverride();
        o1.setId(1);
        o1.setAeronDirectory("/var/oms/aeron-shard-1");
        o1.setIngressEndpoints("0=localhost:20110");
        config.getCluster().getClient().setShards(java.util.List.of(o0, o1));

        OmsClusterClientsConfiguration factory = new OmsClusterClientsConfiguration();

        assertThatThrownBy(() -> factory.omsClusterIngressClients(
                config, new io.micrometer.core.instrument.simple.SimpleMeterRegistry()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("share ingress-endpoints=0=localhost:20110");
    }

    private static OmsConfig.Cluster.Client newTemplate(String aeronDirectory, String ingressEndpoints) {
        OmsConfig.Cluster.Client t = new OmsConfig.Cluster.Client();
        t.setEnabled(true);
        t.setAeronDirectory(aeronDirectory);
        t.setIngressEndpoints(ingressEndpoints);
        // Leave the rest at defaults; we only assert the merge propagates them unchanged.
        return t;
    }
}
