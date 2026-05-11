package com.balh.oms.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

/**
 * Supplies {@link Clock} for components that inject it (e.g. FX stub controllers). Spring does not
 * register a {@code Clock} bean by default.
 */
@Configuration
public class OmsClockConfiguration {

    @Bean
    public Clock systemUtcClock() {
        return Clock.systemUTC();
    }
}
