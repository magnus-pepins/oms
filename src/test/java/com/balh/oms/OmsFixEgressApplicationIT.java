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
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Phase 3 slice 3a: {@value OmsProfiles#FIX_EGRESS} JVM boots without order-accept beans, without
 * {@link OmsClusterIngressClient} (slice 3a/3b — turns back on at slice 3d for inbound ER), and
 * without {@link OmsPostgresProjector} (mutually exclusive role profiles), but exposes the
 * {@link OmsFixEgressService} skeleton bean.
 *
 * <p>Slice 3b will extend this IT to cover the actual Aeron Archive replay path; today it only
 * validates that the new profile produces a healthy Spring context with the expected bean topology.
 */
@ActiveProfiles({"test", OmsProfiles.FIX_EGRESS})
class OmsFixEgressApplicationIT extends AbstractPostgresIntegrationTest {

    @DynamicPropertySource
    static void fixEgressProperties(DynamicPropertyRegistry registry) {
        // Slice 3a: skeleton bean is gated on this property; flip it on for the IT so the bean
        // loads. Slice 3b will replace this with full Aeron config (mirrors
        // application-oms-postgres-projector.yaml's slice 2b additions).
        registry.add("oms.cluster.fix-egress.enabled", () -> "true");
        // FIX-egress JVM does not run order-ingress; gRPC must be off.
        registry.add("oms.grpc.enabled", () -> "false");
        // Slice 3a/3b: cluster client is off (egress reads from Archive, not the cluster client).
        // Slice 3d will flip this back on for ApplyExecutionReportCommand.
        registry.add("oms.cluster.client.enabled", () -> "false");
        // FIX-egress owns QuickFIX but slice 3a does not boot it (no broker connection in the IT).
        registry.add("oms.fix.auto-start", () -> "false");
        registry.add("oms.routing.backend", () -> "noop");
    }

    @Autowired ApplicationContext applicationContext;

    @Autowired OmsFixEgressService fixEgressService;

    @Test
    void contextLoads_withFixEgressBeanAndWithoutOrderIngressOrProjector() {
        assertThat(fixEgressService).isNotNull();
        assertThatThrownBy(() -> applicationContext.getBean(OrderIngressService.class))
                .isInstanceOf(NoSuchBeanDefinitionException.class);
        assertThatThrownBy(() -> applicationContext.getBean(OmsClusterIngressClient.class))
                .isInstanceOf(NoSuchBeanDefinitionException.class);
        assertThatThrownBy(() -> applicationContext.getBean(OmsPostgresProjector.class))
                .isInstanceOf(NoSuchBeanDefinitionException.class);
    }
}
