package com.balh.oms.config;

import com.balh.oms.ingress.OrderIngressService;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

/**
 * Fail-fast when order-accept runs on this JVM but the Chronicle control tail is turned off while admission still
 * expects the legacy tail path ({@link ControlPostgresWritePath#TAIL}) — orders would never leave {@code NEW}.
 *
 * <p>Split topology: set {@code oms.chronicle.control-tail-enabled=false} on ingress-only replicas and
 * {@code oms.control.postgres-write-path=ingress} so admission runs in {@link com.balh.oms.ingress.OrderIngressService};
 * control / FIX workers keep the tail enabled.
 */
@Component
public class IngressChronicleTailTopologyValidator {

    private final ObjectProvider<OrderIngressService> orderIngress;
    private final OmsConfig omsConfig;

    public IngressChronicleTailTopologyValidator(
            ObjectProvider<OrderIngressService> orderIngress, OmsConfig omsConfig) {
        this.orderIngress = orderIngress;
        this.omsConfig = omsConfig;
    }

    @PostConstruct
    void validate() {
        validate(orderIngress, omsConfig);
    }

    public static void validate(ObjectProvider<OrderIngressService> orderIngress, OmsConfig omsConfig) {
        if (orderIngress.getIfAvailable() == null) {
            return;
        }
        if (!omsConfig.getChronicle().isEnabled()) {
            return;
        }
        if (omsConfig.getChronicle().isControlTailEnabled()) {
            return;
        }
        if (omsConfig.getControl().getPostgresWritePath() == ControlPostgresWritePath.TAIL) {
            throw new IllegalStateException(
                    "Order ingress is active but oms.chronicle.control-tail-enabled=false with "
                            + "oms.control.postgres-write-path=tail would leave accepted orders stuck in NEW "
                            + "(no local Chronicle tail to apply control). Use postgres-write-path=ingress on this JVM, "
                            + "or set oms.chronicle.control-tail-enabled=true, or run without order-accept (worker profiles).");
        }
    }
}
