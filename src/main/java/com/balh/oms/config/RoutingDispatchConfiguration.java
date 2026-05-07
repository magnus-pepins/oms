package com.balh.oms.config;

import com.balh.oms.routing.RouteDispatcher;
import com.balh.oms.routing.SimulatedFillEngine;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.UUID;

@Configuration
public class RoutingDispatchConfiguration {

    @Bean
    public RouteDispatcher routeDispatcher(
            @Value("${oms.routing.backend:noop}") String backend,
            ObjectProvider<SimulatedFillEngine> simulatedEngine) {
        if ("simulated".equalsIgnoreCase(backend)) {
            SimulatedFillEngine engine = simulatedEngine.getIfAvailable();
            if (engine == null) {
                throw new IllegalStateException(
                        "oms.routing.backend=simulated requires SimulatedFillEngine (check @ConditionalOnProperty wiring)");
            }
            return engine::enqueueWorkingOrder;
        }
        if ("fix".equalsIgnoreCase(backend)) {
            throw new IllegalStateException("oms.routing.backend=fix is not implemented until slice 4");
        }
        return (UUID orderId) -> { };
    }
}
