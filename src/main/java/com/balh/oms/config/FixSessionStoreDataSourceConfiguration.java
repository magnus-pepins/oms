package com.balh.oms.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.boot.autoconfigure.condition.AllNestedConditions;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

/**
 * Optional second {@link DataSource} for QuickFIX/J {@link quickfix.JdbcStoreFactory} when
 * {@code oms.fix.session-jdbc-datasource-enabled=true} (master plan 6.4: isolate FIX seq/message IO from the
 * application pool). Apply Flyway {@code V9} DDL to that database before enabling in production.
 */
@Configuration(proxyBeanMethods = false)
@Conditional(FixSessionStoreDataSourceConfiguration.OnDedicatedFixSessionJdbc.class)
public class FixSessionStoreDataSourceConfiguration {

    static class OnDedicatedFixSessionJdbc extends AllNestedConditions {

        OnDedicatedFixSessionJdbc() {
            super(ConfigurationPhase.REGISTER_BEAN);
        }

        @ConditionalOnProperty(name = "oms.routing.backend", havingValue = "fix")
        static class RoutingFix {}

        @ConditionalOnProperty(name = "oms.fix.session-store-type", havingValue = "jdbc")
        static class StoreJdbc {}

        @ConditionalOnProperty(name = "oms.fix.session-jdbc-datasource-enabled", havingValue = "true")
        static class DedicatedEnabled {}
    }

    @Bean(name = "fixSessionStoreDataSource", destroyMethod = "close")
    HikariDataSource fixSessionStoreDataSource(OmsConfig omsConfig) {
        OmsConfig.Fix f = omsConfig.getFix();
        if (f.getSessionJdbcUrl().isBlank()) {
            throw new IllegalStateException(
                    "oms.fix.session-jdbc-url is required when oms.fix.session-jdbc-datasource-enabled=true");
        }
        HikariConfig hc = new HikariConfig();
        hc.setJdbcUrl(f.getSessionJdbcUrl());
        hc.setUsername(f.getSessionJdbcUser());
        hc.setPassword(f.getSessionJdbcPassword());
        hc.setPoolName("oms-fix-session-store");
        hc.setMaximumPoolSize(f.getSessionJdbcPoolMaxSize());
        hc.setMinimumIdle(f.getSessionJdbcPoolMinIdle());
        hc.setConnectionTimeout(f.getSessionJdbcConnectionTimeoutMs());
        return new HikariDataSource(hc);
    }
}
