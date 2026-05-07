package com.balh.oms.config;

import com.balh.oms.fix.FixRouteDispatcher;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "oms.routing.backend", havingValue = "fix")
public class FixRoutingBeans {

    @Bean
    FixRouteDispatcher fixRouteDispatcher() {
        return new FixRouteDispatcher();
    }
}
