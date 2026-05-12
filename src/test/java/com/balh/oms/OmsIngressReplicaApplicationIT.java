package com.balh.oms;

import com.balh.oms.config.OmsProfiles;
import com.balh.oms.ingress.OrderIngressService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * P5 prep: {@value OmsProfiles#INGRESS_REPLICA} must still load order-accept (unlike control/fix workers).
 * Production validators {@link com.balh.oms.config.IngressReplicaTopologyValidator} are {@code @Profile("!test")};
 * {@code application-test.yaml} keeps {@code oms.chronicle.enabled=false} (NoOp journal) like other ITs.
 */
@ActiveProfiles(OmsProfiles.INGRESS_REPLICA)
class OmsIngressReplicaApplicationIT extends AbstractPostgresIntegrationTest {

    @Autowired
    ApplicationContext applicationContext;

    @Test
    void contextLoads_withOrderIngress() {
        assertThat(applicationContext.getBean(OrderIngressService.class)).isNotNull();
    }
}
