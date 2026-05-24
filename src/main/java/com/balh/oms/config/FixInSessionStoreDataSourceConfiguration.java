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
 * Optional dedicated {@link DataSource} for FIX-in QuickFIX/J {@link quickfix.JdbcStoreFactory}
 * ({@code oms.fix-in.session-jdbc-datasource-enabled=true}). Same isolation pattern as
 * {@link FixSessionStoreDataSourceConfiguration} for FIX-out.
 */
@Configuration(proxyBeanMethods = false)
@AutoConfigureAfter(DataSourceAutoConfiguration.class)
public class FixInSessionStoreDataSourceConfiguration {

    static class OnDedicatedFixInSessionJdbc extends AllNestedConditions {

        OnDedicatedFixInSessionJdbc() {
            super(ConfigurationCondition.ConfigurationPhase.REGISTER_BEAN);
        }

        @ConditionalOnProperty(name = "oms.fix-in.session-store-type", havingValue = "jdbc")
        static class StoreJdbc {}

        @ConditionalOnProperty(name = "oms.fix-in.session-jdbc-datasource-enabled", havingValue = "true")
        static class DedicatedEnabled {}
    }

    static final class NotSameJdbcUrlAsSpringDatasource implements Condition {

        @Override
        public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
            Environment env = context.getEnvironment();
            String main = env.getProperty("spring.datasource.url");
            String fixIn = env.getProperty("oms.fix-in.session-jdbc-url");
            if (main == null || fixIn == null || fixIn.isBlank()) {
                return true;
            }
            return !PostgresJdbcUrlEquivalence.isSameLogicalDatabase(main, fixIn);
        }
    }

    static final class DedicatedAndDifferentJdbcUrl implements Condition {

        private static final OnDedicatedFixInSessionJdbc ON_DEDICATED = new OnDedicatedFixInSessionJdbc();
        private static final NotSameJdbcUrlAsSpringDatasource NOT_SAME_URL = new NotSameJdbcUrlAsSpringDatasource();

        @Override
        public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
            return ON_DEDICATED.matches(context, metadata) && NOT_SAME_URL.matches(context, metadata);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @Conditional(DedicatedAndDifferentJdbcUrl.class)
    static class DedicatedHikariPool {

        @Bean(name = "fixInSessionStoreDataSource", destroyMethod = "close")
        HikariDataSource fixInSessionStoreDataSource(OmsConfig omsConfig) {
            OmsConfig.FixIn f = omsConfig.getFixIn();
            if (f.getSessionJdbcUrl().isBlank()) {
                throw new IllegalStateException(
                        "oms.fix-in.session-jdbc-url is required when oms.fix-in.session-jdbc-datasource-enabled=true");
            }
            HikariConfig hc = new HikariConfig();
            hc.setJdbcUrl(f.getSessionJdbcUrl());
            hc.setUsername(f.getSessionJdbcUser());
            hc.setPassword(f.getSessionJdbcPassword());
            hc.setPoolName("oms-fix-in-session-store");
            hc.setMaximumPoolSize(f.getSessionJdbcPoolMaxSize());
            hc.setMinimumIdle(f.getSessionJdbcPoolMinIdle());
            hc.setConnectionTimeout(f.getSessionJdbcConnectionTimeoutMs());
            return new HikariDataSource(hc);
        }
    }
}
