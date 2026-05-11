package com.balh.oms.config;

import com.balh.oms.fix.FixOutboundOrderDequeue;
import com.balh.oms.fix.FixRouteDispatcher;
import com.balh.oms.fix.QueueFixOutboundOrderDequeue;
import com.balh.oms.observability.otel.IngressToFixNosLatencyRecorder;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Configuration
@ConditionalOnProperty(name = "oms.routing.backend", havingValue = "fix")
public class FixRoutingBeans {

    @Bean
    BlockingQueue<UUID> fixOutboundPendingOrderQueue(OmsConfig omsConfig) {
        int cap = Math.max(1, omsConfig.getFix().getOutboundQueueCapacity());
        return new LinkedBlockingQueue<>(cap);
    }

    @Bean
    FixRouteDispatcher fixRouteDispatcher(
            BlockingQueue<UUID> fixOutboundPendingOrderQueue,
            IngressToFixNosLatencyRecorder ingressToFixNosLatencyRecorder) {
        return new FixRouteDispatcher(fixOutboundPendingOrderQueue, ingressToFixNosLatencyRecorder);
    }

    @Bean
    FixOutboundOrderDequeue fixOutboundOrderDequeue(FixRouteDispatcher fixRouteDispatcher) {
        return new QueueFixOutboundOrderDequeue(fixRouteDispatcher);
    }
}
