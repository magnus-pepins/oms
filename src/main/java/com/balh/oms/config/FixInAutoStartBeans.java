package com.balh.oms.config;

import com.balh.oms.fixin.FixAcceptorManager;
import com.balh.oms.fixin.FixInApplication;
import com.balh.oms.fixin.persistence.FixInSessionRepository;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;

import javax.sql.DataSource;

@Configuration(proxyBeanMethods = false)
@Profile(OmsProfiles.FIX_INGRESS)
public class FixInAutoStartBeans {

    @Bean
    @ConditionalOnProperty(name = "oms.fix-in.auto-start", havingValue = "true")
    FixAcceptorManager fixAcceptorManager(
            OmsConfig omsConfig,
            FixInApplication application,
            FixInSessionRepository sessionRepository,
            DataSource dataSource,
            Environment environment,
            @Autowired(required = false) @Qualifier("fixInSessionStoreDataSource") DataSource fixInSessionStoreDataSource) {
        DataSource dedicated = fixInSessionStoreDataSource;
        if (dedicated != null && omsConfig.getFixIn().isJdbcSessionStore()) {
            if (dedicated instanceof HikariDataSource hDedicated && dataSource instanceof HikariDataSource hPrimary) {
                String u1 = hDedicated.getJdbcUrl();
                String u2 = hPrimary.getJdbcUrl();
                if (u1 != null && u2 != null && PostgresJdbcUrlEquivalence.isSameLogicalDatabase(u1, u2)) {
                    dedicated = null;
                }
            } else if (PostgresJdbcUrlEquivalence.isSameLogicalDatabase(
                    environment.getProperty("spring.datasource.url"), omsConfig.getFixIn().getSessionJdbcUrl())) {
                dedicated = null;
            }
        }
        DataSource jdbcMessageStoreDataSource =
                omsConfig.getFixIn().isJdbcSessionStore()
                        ? (dedicated != null ? dedicated : dataSource)
                        : dataSource;
        return new FixAcceptorManager(omsConfig, application, sessionRepository, jdbcMessageStoreDataSource);
    }
}
