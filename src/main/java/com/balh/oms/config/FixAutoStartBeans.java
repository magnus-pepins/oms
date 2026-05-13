package com.balh.oms.config;

import com.zaxxer.hikari.HikariDataSource;
import com.balh.oms.fix.FixInitiatorManager;
import com.balh.oms.fix.OmsFixApplication;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import javax.sql.DataSource;

/**
 * QuickFIX/J initiator wiring when {@code oms.routing.backend=fix} and {@code oms.fix.auto-start=true}.
 *
 * <p>Phase 3 slice 3g of the Aeron Cluster substrate plan retired the legacy in-memory outbound
 * queue + {@code FixOutboundDispatchWorker} drain path. The cluster + {@code oms-fix-egress} JVM
 * now own outbound NOS end-to-end via Aeron Archive replay (see {@code OmsFixEgressService}); this
 * configuration only stands up the {@link FixInitiatorManager} (QuickFIX {@code SocketInitiator})
 * so the egress JVM has an open session to send through.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "oms.routing.backend", havingValue = "fix")
public class FixAutoStartBeans {

    @Bean
    @ConditionalOnProperty(name = "oms.fix.auto-start", havingValue = "true")
    FixInitiatorManager fixInitiatorManager(
            OmsConfig omsConfig,
            OmsFixApplication application,
            DataSource dataSource,
            Environment environment,
            @Autowired(required = false) @Qualifier("fixSessionStoreDataSource") DataSource fixSessionStoreDataSource) {
        DataSource dedicated = fixSessionStoreDataSource;
        if (dedicated != null && omsConfig.getFix().isJdbcSessionStore()) {
            if (dedicated instanceof HikariDataSource hDedicated && dataSource instanceof HikariDataSource hPrimary) {
                String u1 = hDedicated.getJdbcUrl();
                String u2 = hPrimary.getJdbcUrl();
                if (u1 != null && u2 != null && PostgresJdbcUrlEquivalence.isSameLogicalDatabase(u1, u2)) {
                    dedicated = null;
                }
            } else if (PostgresJdbcUrlEquivalence.isSameLogicalDatabase(
                    environment.getProperty("spring.datasource.url"), omsConfig.getFix().getSessionJdbcUrl())) {
                dedicated = null;
            }
        }
        DataSource jdbcMessageStoreDataSource =
                omsConfig.getFix().isJdbcSessionStore()
                        ? (dedicated != null ? dedicated : dataSource)
                        : dataSource;
        return new FixInitiatorManager(omsConfig, application, jdbcMessageStoreDataSource);
    }
}
