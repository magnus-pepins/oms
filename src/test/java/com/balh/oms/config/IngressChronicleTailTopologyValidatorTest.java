package com.balh.oms.config;

import com.balh.oms.ingress.OrderIngressService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class IngressChronicleTailTopologyValidatorTest {

    @Test
    void noIngressBean_noop() {
        @SuppressWarnings("unchecked")
        ObjectProvider<OrderIngressService> provider = mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(null);
        OmsConfig cfg = new OmsConfig();
        assertThatCode(() -> IngressChronicleTailTopologyValidator.validate(provider, cfg)).doesNotThrowAnyException();
    }

    @Test
    void ingress_chronicleDisabled_noop() {
        OrderIngressService ingress = mock(OrderIngressService.class);
        @SuppressWarnings("unchecked")
        ObjectProvider<OrderIngressService> provider = mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(ingress);
        OmsConfig cfg = new OmsConfig();
        cfg.getChronicle().setEnabled(false);
        cfg.getChronicle().setControlTailEnabled(false);
        cfg.getControl().setPostgresWritePath("tail");
        assertThatCode(() -> IngressChronicleTailTopologyValidator.validate(provider, cfg)).doesNotThrowAnyException();
    }

    @Test
    void ingress_tailDisabled_tailWritePath_throws() {
        OrderIngressService ingress = mock(OrderIngressService.class);
        @SuppressWarnings("unchecked")
        ObjectProvider<OrderIngressService> provider = mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(ingress);
        OmsConfig cfg = new OmsConfig();
        cfg.getChronicle().setEnabled(true);
        cfg.getChronicle().setControlTailEnabled(false);
        cfg.getControl().setPostgresWritePath("tail");
        assertThatThrownBy(() -> IngressChronicleTailTopologyValidator.validate(provider, cfg))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("postgres-write-path=ingress");
    }

    @Test
    void ingress_tailDisabled_ingressWritePath_ok() {
        OrderIngressService ingress = mock(OrderIngressService.class);
        @SuppressWarnings("unchecked")
        ObjectProvider<OrderIngressService> provider = mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(ingress);
        OmsConfig cfg = new OmsConfig();
        cfg.getChronicle().setEnabled(true);
        cfg.getChronicle().setControlTailEnabled(false);
        cfg.getControl().setPostgresWritePath("ingress");
        assertThatCode(() -> IngressChronicleTailTopologyValidator.validate(provider, cfg)).doesNotThrowAnyException();
    }
}
