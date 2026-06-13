package com.balh.oms.fixin;

import com.balh.oms.cluster.AcceptOrderCommand;
import com.balh.oms.cluster.FixInIngressMetadata;
import com.balh.oms.domain.ShardKey;
import com.balh.oms.fixin.persistence.FixInAccountBindingRow;
import com.balh.oms.observability.PiiHash;
import com.balh.oms.config.OmsProfiles;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

@Component
@Profile(OmsProfiles.FIX_INGRESS)
public class IngressAcceptOrderCommandFactory {

    public static final String FIXIN_IDEMPOTENCY_PREFIX = "fixin:";

    private final PiiHash piiHash;

    public IngressAcceptOrderCommandFactory(PiiHash piiHash) {
        this.piiHash = piiHash;
    }

    public AcceptOrderCommand buildNewOrder(
            long correlationId,
            UUID orderId,
            UUID fixSessionId,
            FixInParsedNewOrder parsed,
            FixInAccountBindingRow binding,
            int shardCount,
            Instant now) {
        UUID omsAccountId = binding.omsAccountId();
        long quantityScaled = FixInNewOrderSingleParser.scaleQuantity(parsed.quantity());
        long limitPriceScaled = FixInNewOrderSingleParser.scaleLimitPrice(parsed.limitPriceOrNull(), parsed.ordTypeCode());
        String idempotencyKey = FIXIN_IDEMPOTENCY_PREFIX + fixSessionId + ":" + parsed.clientClOrdId();
        FixInIngressMetadata ingressMetadata = new FixInIngressMetadata(
                fixSessionId, parsed.clientClOrdId(), parsed.fixAccountTagOrEmpty());
        return new AcceptOrderCommand(
                correlationId,
                orderId,
                Math.multiplyExact(now.getEpochSecond(), 1_000_000_000L) + now.getNano(),
                quantityScaled,
                limitPriceScaled,
                ShardKey.shardFor(omsAccountId, shardCount),
                parsed.sideCode(),
                parsed.timeInForceCode(),
                parsed.ordTypeCode(),
                omsAccountId.toString(),
                idempotencyKey,
                piiHash.hash(omsAccountId),
                parsed.instrumentSymbol(),
                binding.ledgerBalanceIdOrNull(),
                ingressMetadata,
                parsed.portfolioIdOrNull());
    }
}
