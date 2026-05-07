package com.balh.oms.fix;

import com.balh.oms.AbstractPostgresIntegrationTest;
import com.balh.oms.routing.RouteDispatcher;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

@TestPropertySource(
        properties = {
                "oms.routing.backend=fix",
                "oms.fix.auto-start=false"
        })
class FixRoutingSpringIntegrationTest extends AbstractPostgresIntegrationTest {

    @Autowired
    private RouteDispatcher routeDispatcher;

    @Test
    void loadsFixRouteDispatcher() {
        assertThat(routeDispatcher).isInstanceOf(FixRouteDispatcher.class);
    }
}
