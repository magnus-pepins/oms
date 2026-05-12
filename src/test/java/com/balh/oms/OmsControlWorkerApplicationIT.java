package com.balh.oms;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ActiveProfiles;

import com.balh.oms.config.OmsProfiles;
import com.balh.oms.ingress.OrderIngressService;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * P3 prep: {@value OmsProfiles#CONTROL_WORKER} profile must start without order-accept beans.
 */
@ActiveProfiles({"test", OmsProfiles.CONTROL_WORKER})
class OmsControlWorkerApplicationIT extends AbstractPostgresIntegrationTest {

    @Autowired ApplicationContext applicationContext;

    @Test
    void contextLoads_withoutOrderIngress() {
        assertThatThrownBy(() -> applicationContext.getBean(OrderIngressService.class))
                .isInstanceOf(NoSuchBeanDefinitionException.class);
    }
}
