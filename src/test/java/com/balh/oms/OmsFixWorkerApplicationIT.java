package com.balh.oms;

import com.balh.oms.config.OmsProfiles;
import com.balh.oms.ingress.OrderIngressService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * P4 prep: {@value OmsProfiles#FIX_WORKER} profile must start without order-accept beans.
 * {@link com.balh.oms.config.FixWorkerTopologyValidator} is disabled for {@code test} so QuickFIX need not bind.
 */
@TestPropertySource(properties = {"oms.routing.backend=fix"})
@ActiveProfiles({"test", OmsProfiles.FIX_WORKER})
class OmsFixWorkerApplicationIT extends AbstractPostgresIntegrationTest {

    @Autowired
    ApplicationContext applicationContext;

    @Test
    void contextLoads_withoutOrderIngress() {
        assertThatThrownBy(() -> applicationContext.getBean(OrderIngressService.class))
                .isInstanceOf(NoSuchBeanDefinitionException.class);
    }
}
