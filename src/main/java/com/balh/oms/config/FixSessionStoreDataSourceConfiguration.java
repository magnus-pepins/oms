package com.balh.oms.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import javax.sql.DataSource;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.AllNestedConditions;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ConfigurationCondition;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * Optional second {@link DataSource} for QuickFIX/J {@link quickfix.JdbcStoreFactory} when
 * {@code oms.fix.session-jdbc-datasource-enabled=true} (master plan 6.4: isolate FIX seq/message IO from the
 * application pool). Apply Flyway {@code V9} DDL to that database before enabling in production.
 *
 * <p>When {@code oms.fix.session-jdbc-url} is identical to {@code spring.datasource.url}, we register the
 * <strong>same</strong> {@code DataSource} bean as {@code fixSessionStoreDataSource} (with {@code destroyMethod=""})
 * instead of opening a second Hikari pool. A second pool to the same JDBC URL is redundant and has caused QuickFIX
 * session rows to be invisible to the application's {@code JdbcTemplate} on some CI runners (connection / pool
 * interaction).
 */
@Configuration(proxyBeanMethods = false)
@AutoConfigureAfter(DataSourceAutoConfiguration.class)
public class FixSessionStoreDataSourceConfiguration {

    static class OnDedicatedFixSessionJdbc extends AllNestedConditions {

        OnDedicatedFixSessionJdbc() {
            super(ConfigurationCondition.ConfigurationPhase.REGISTER_BEAN);
        }

        @ConditionalOnProperty(name = "oms.routing.backend", havingValue = "fix")
        static class RoutingFix {}

        @ConditionalOnProperty(name = "oms.fix.session-store-type", havingValue = "jdbc")
        static class StoreJdbc {}

        @ConditionalOnProperty(name = "oms.fix.session-jdbc-datasource-enabled", havingValue = "true")
        static class DedicatedEnabled {}
    }

    /** FIX session JDBC URL matches the Spring Boot primary datasource URL — reuse that pool. */
    static final class SameJdbcUrlAsSpringDatasource implements Condition {

        @Override
        public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
            Environment env = context.getEnvironment();
            String main = env.getProperty("spring.datasource.url");
            String fix = env.getProperty("oms.fix.session-jdbc-url");
            if (main == null || fix == null) {
                return false;
            }
            return main.trim().equals(fix.trim());
        }
    }

    /** Dedicated FIX JDBC store with a URL different from {@code spring.datasource.url} — separate Hikari pool. */
    static final class NotSameJdbcUrlAsSpringDatasource implements Condition {

        @Override
        public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
            return !new SameJdbcUrlAsSpringDatasource().matches(context, metadata);
        }
    }

    /**
     * Self-contained: nested {@code @Configuration} conditions are evaluated independently of the outer class
     * {@code @Conditional} in some Spring versions.
     */
    static final class DedicatedAndSameJdbcUrl implements Condition {

        private static final OnDedicatedFixSessionJdbc ON_DEDICATED = new OnDedicatedFixSessionJdbc();
        private static final SameJdbcUrlAsSpringDatasource SAME_URL = new SameJdbcUrlAsSpringDatasource();

        @Override
        public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
            return ON_DEDICATED.matches(context, metadata) && SAME_URL.matches(context, metadata);
        }
    }

    static final class DedicatedAndDifferentJdbcUrl implements Condition {

        private static final OnDedicatedFixSessionJdbc ON_DEDICATED = new OnDedicatedFixSessionJdbc();
        private static final NotSameJdbcUrlAsSpringDatasource NOT_SAME_URL = new NotSameJdbcUrlAsSpringDatasource();

        @Override
        public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
            return ON_DEDICATED.matches(context, metadata) && NOT_SAME_URL.matches(context, metadata);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @Conditional(DedicatedAndSameJdbcUrl.class)
    static class ReuseMainJdbcPool {

        @Bean(name = "fixSessionStoreDataSource", destroyMethod = "")
        DataSource fixSessionStoreDataSourceSameUrlAsMain(DataSource dataSource) {
            return dataSource;
        }
    }

    @Configuration(proxyBeanMethods = false)
    @Conditional(DedicatedAndDifferentJdbcUrl.class)
    static class DedicatedHikariPool {

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
}
