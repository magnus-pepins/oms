package com.balh.oms.config;

import com.balh.oms.fix.FixInitiatorManager;
import com.balh.oms.fix.FixNewOrderSingleBuilder;
import com.balh.oms.fix.FixOutboundDispatchWorker;
import com.balh.oms.fix.FixRouteDispatcher;
import com.balh.oms.fix.FixSessionRegistry;
import com.balh.oms.fix.OmsFixApplication;
import com.balh.oms.persistence.OrdersRepository;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * FIX initiator + outbound scheduler when {@code oms.routing.backend=fix} and {@code oms.fix.auto-start=true}.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "oms.routing.backend", havingValue = "fix")
public class FixAutoStartBeans {

    @Bean
    @ConditionalOnProperty(name = "oms.fix.auto-start", havingValue = "true")
    FixInitiatorManager fixInitiatorManager(OmsConfig omsConfig, OmsFixApplication application) {
        return new FixInitiatorManager(omsConfig, application);
    }

    @Bean
    @ConditionalOnProperty(name = "oms.fix.auto-start", havingValue = "true")
    FixOutboundDispatchWorker fixOutboundDispatchWorker(
            FixRouteDispatcher fixRouteDispatcher,
            FixSessionRegistry fixSessionRegistry,
            OrdersRepository ordersRepository,
            FixNewOrderSingleBuilder newOrderSingleBuilder,
            MeterRegistry meterRegistry) {
        return new FixOutboundDispatchWorker(
                fixRouteDispatcher, fixSessionRegistry, ordersRepository, newOrderSingleBuilder, meterRegistry);
    }
}
