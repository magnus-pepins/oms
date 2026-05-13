package com.balh.oms;

import com.balh.oms.cluster.OmsClusterIngressClient;
import com.balh.oms.config.OmsProfiles;
import com.balh.oms.fixegress.OmsFixEgressService;
import com.balh.oms.ingress.OrderIngressService;
import com.balh.oms.projector.OmsPostgresProjector;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Phase 3 slice 3a/3d: {@value OmsProfiles#FIX_EGRESS} JVM boots without order-accept beans and
 * without {@link OmsPostgresProjector} (mutually exclusive role profiles), but exposes the
 * {@link OmsFixEgressService} skeleton bean.
 *
 * <p><strong>Slice 3d:</strong> {@link OmsClusterIngressClient} now <em>does</em> load on this
 * JVM — the egress translates inbound venue ER into {@link com.balh.oms.cluster.ApplyExecutionReportCommand}
 * and offers it back to the cluster. {@link com.balh.oms.config.FixEgressTopologyValidator} fails
 * fast if {@code oms.cluster.client.enabled=false} on this profile in production. The validator
 * itself is excluded from the {@code test} profile so this IT can probe bean topology without
 * booting a real cluster connection.
 */
@ActiveProfiles({"test", OmsProfiles.FIX_EGRESS})
// Slice 3e: see OmsFixEgressInboundErRoundTripIT — the egress profile keeps an OmsFixEgressService
// replay loop alive against the JVM-wide cluster recording, which would route a sibling test's
// admitted order out and (with slice 3d's inbound ER → cluster path) potentially flip its
// projected status before the sibling assertion polls. Tear down the context after this class.
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class OmsFixEgressApplicationIT extends AbstractPostgresIntegrationTest {

    @DynamicPropertySource
    static void fixEgressProperties(DynamicPropertyRegistry registry) {
        // Slice 3a: skeleton bean is gated on this property; flip it on for the IT so the bean
        // loads. Slice 3b replaced this with full Aeron config (mirrors
        // application-oms-postgres-projector.yaml's slice 2b additions).
        registry.add("oms.cluster.fix-egress.enabled", () -> "true");
        // FIX-egress JVM does not run order-ingress; gRPC must be off.
        registry.add("oms.grpc.enabled", () -> "false");
        // Slice 3d: cluster client is now required on oms-fix-egress (inbound ER -> cluster).
        // AbstractPostgresIntegrationTest already sets enabled=true and points it at the test
        // cluster; we re-state it here so the contract is explicit at the role-test level.
        registry.add("oms.cluster.client.enabled", () -> "true");
        // FIX-egress owns QuickFIX but this IT does not boot it (no broker connection here).
        registry.add("oms.fix.auto-start", () -> "false");
        registry.add("oms.routing.backend", () -> "noop");
    }

    @Autowired ApplicationContext applicationContext;

    @Autowired OmsFixEgressService fixEgressService;

    @Autowired OmsClusterIngressClient clusterIngressClient;

    @Test
    void contextLoads_withFixEgressBeanAndClusterClient_butWithoutOrderIngressOrProjector() {
        assertThat(fixEgressService).isNotNull();
        // Slice 3d: cluster client is part of the egress topology now.
        assertThat(clusterIngressClient).isNotNull();
        assertThatThrownBy(() -> applicationContext.getBean(OrderIngressService.class))
                .isInstanceOf(NoSuchBeanDefinitionException.class);
        assertThatThrownBy(() -> applicationContext.getBean(OmsPostgresProjector.class))
                .isInstanceOf(NoSuchBeanDefinitionException.class);
    }
}
