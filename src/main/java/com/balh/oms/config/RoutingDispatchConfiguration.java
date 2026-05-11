package com.balh.oms.config;

import com.balh.oms.fix.FixRouteDispatcher;
import com.balh.oms.fix.PostgresFixHandoffRouteDispatcher;
import com.balh.oms.routing.RouteDispatcher;
import com.balh.oms.routing.SimulatedBrokerDispatcher;
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
            ObjectProvider<SimulatedBrokerDispatcher> simulatedDispatcher,
            ObjectProvider<PostgresFixHandoffRouteDispatcher> postgresFixHandoffDispatcher,
            ObjectProvider<FixRouteDispatcher> fixRouteDispatcher) {
        if ("simulated".equalsIgnoreCase(backend)) {
            SimulatedBrokerDispatcher dispatcher = simulatedDispatcher.getIfAvailable();
            if (dispatcher == null) {
                throw new IllegalStateException(
                        "oms.routing.backend=simulated requires SimulatedBrokerDispatcher (check SimulatedRoutingBeans wiring)");
            }
            return dispatcher;
        }
        if ("fix".equalsIgnoreCase(backend)) {
            PostgresFixHandoffRouteDispatcher pg = postgresFixHandoffDispatcher.getIfAvailable();
            if (pg != null) {
                return pg;
            }
            FixRouteDispatcher fix = fixRouteDispatcher.getIfAvailable();
            if (fix == null) {
                throw new IllegalStateException(
                        "oms.routing.backend=fix requires FixRouteDispatcher or PostgresFixHandoffRouteDispatcher (check FixRoutingBeans wiring)");
            }
            return fix;
        }
        return (UUID orderId) -> { };
    }
}
