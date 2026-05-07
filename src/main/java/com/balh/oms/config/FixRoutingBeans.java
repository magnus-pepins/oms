package com.balh.oms.config;

import com.balh.oms.fix.FixRouteDispatcher;
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
    FixRouteDispatcher fixRouteDispatcher(BlockingQueue<UUID> fixOutboundPendingOrderQueue) {
        return new FixRouteDispatcher(fixOutboundPendingOrderQueue);
    }
}
