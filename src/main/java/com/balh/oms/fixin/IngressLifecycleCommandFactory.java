package com.balh.oms.fixin;

import com.balh.oms.cluster.RequestCancelOrderCommand;
import com.balh.oms.cluster.RequestReplaceOrderCommand;
import com.balh.oms.config.OmsProfiles;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
@Profile(OmsProfiles.FIX_INGRESS)
public class IngressLifecycleCommandFactory {

    public static final String FIXIN_CANCEL_PREFIX = "fixin-cancel:";
    public static final String FIXIN_REPLACE_PREFIX = "fixin-replace:";

    public RequestCancelOrderCommand buildCancel(
            long correlationId, UUID omsOrderId, UUID fixSessionId, String clientClOrdId, String reason) {
        String requestKey = FIXIN_CANCEL_PREFIX + fixSessionId + ":" + clientClOrdId;
        return new RequestCancelOrderCommand(correlationId, omsOrderId, System.nanoTime(), requestKey, reason);
    }

    public RequestReplaceOrderCommand buildReplace(
            long correlationId,
            UUID omsOrderId,
            UUID fixSessionId,
            String clientClOrdId,
            long newQuantityScaled,
            long newLimitPriceScaledOrZero,
            String reason) {
        String requestKey = FIXIN_REPLACE_PREFIX + fixSessionId + ":" + clientClOrdId;
        return new RequestReplaceOrderCommand(
                correlationId,
                omsOrderId,
                newQuantityScaled,
                newLimitPriceScaledOrZero,
                System.nanoTime(),
                requestKey,
                reason);
    }
}
