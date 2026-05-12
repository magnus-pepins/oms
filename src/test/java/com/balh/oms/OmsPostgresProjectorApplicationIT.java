package com.balh.oms;

import com.balh.oms.cluster.OmsClusterIngressClient;
import com.balh.oms.config.OmsProfiles;
import com.balh.oms.ingress.OrderIngressService;
import com.balh.oms.projector.AeronProjectorCursorRepository;
import com.balh.oms.projector.OmsPostgresProjector;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Phase 2 slice 2a: {@value OmsProfiles#POSTGRES_PROJECTOR} JVM boots without order-accept / cluster-client
 * beans (those are excluded by the {@code ORDER_ACCEPT_PROFILE} expression) and exposes the projector
 * skeleton + cursor repository.
 */
@ActiveProfiles({"test", OmsProfiles.POSTGRES_PROJECTOR})
class OmsPostgresProjectorApplicationIT extends AbstractPostgresIntegrationTest {

    @Autowired ApplicationContext applicationContext;

    @Autowired OmsPostgresProjector projector;

    @Autowired AeronProjectorCursorRepository cursorRepository;

    @Test
    void contextLoads_withProjectorBeansAndWithoutOrderIngress() {
        assertThat(projector).isNotNull();
        assertThat(cursorRepository).isNotNull();
        assertThatThrownBy(() -> applicationContext.getBean(OrderIngressService.class))
                .isInstanceOf(NoSuchBeanDefinitionException.class);
        assertThatThrownBy(() -> applicationContext.getBean(OmsClusterIngressClient.class))
                .isInstanceOf(NoSuchBeanDefinitionException.class);
    }
}
