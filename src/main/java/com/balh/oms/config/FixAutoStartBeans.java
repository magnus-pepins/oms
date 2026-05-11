package com.balh.oms.config;

import com.zaxxer.hikari.HikariDataSource;
import com.balh.oms.fix.FixInitiatorManager;
import com.balh.oms.fix.FixNewOrderSingleBuilder;
import com.balh.oms.fix.FixOutboundDispatchWorker;
import com.balh.oms.fix.FixOutboundDriver;
import com.balh.oms.fix.FixOutboundTokenBucket;
import com.balh.oms.fix.FixRouteDispatcher;
import com.balh.oms.fix.FixSessionRegistry;
import com.balh.oms.fix.OmsFixApplication;
import com.balh.oms.observability.otel.IngressToFixNosLatencyRecorder;
import com.balh.oms.persistence.FixRouteStateRepository;
import com.balh.oms.persistence.OrdersRepository;
import com.balh.oms.returnpath.ExecutionReportApplier;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;

import javax.sql.DataSource;
import java.time.Duration;

/**
 * FIX initiator + outbound scheduler when {@code oms.routing.backend=fix} and {@code oms.fix.auto-start=true}.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "oms.routing.backend", havingValue = "fix")
public class FixAutoStartBeans {

    private static final long MIN_FIX_OUTBOUND_POLL_INTERVAL_MS = 1L;

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

    @Bean
    @ConditionalOnProperty(name = "oms.fix.auto-start", havingValue = "true")
    FixOutboundDispatchWorker fixOutboundDispatchWorker(
            FixRouteDispatcher fixRouteDispatcher,
            FixSessionRegistry fixSessionRegistry,
            OrdersRepository ordersRepository,
            FixNewOrderSingleBuilder newOrderSingleBuilder,
            MeterRegistry meterRegistry,
            OmsConfig omsConfig,
            ExecutionReportApplier executionReportApplier,
            FixRouteStateRepository fixRouteStateRepository,
            FixOutboundTokenBucket fixOutboundTokenBucket,
            IngressToFixNosLatencyRecorder ingressToFixNosLatencyRecorder) {
        return new FixOutboundDispatchWorker(
                fixRouteDispatcher,
                fixSessionRegistry,
                ordersRepository,
                newOrderSingleBuilder,
                meterRegistry,
                omsConfig,
                executionReportApplier,
                fixRouteStateRepository,
                fixOutboundTokenBucket,
                ingressToFixNosLatencyRecorder);
    }

    /**
     * Registers fixed-delay {@link FixOutboundDispatchWorker#drainPendingOutboundOnce()} when
     * {@code oms.fix.outbound-driver=scheduled}. When {@code dedicated}, this configurer is a no-op so we do not
     * register duplicate @{@link org.springframework.boot.autoconfigure.condition.ConditionalOnProperty} annotations
     * (not repeatable on the same element).
     */
    @Bean
    @ConditionalOnProperty(name = "oms.fix.auto-start", havingValue = "true")
    SchedulingConfigurer fixOutboundPollScheduling(FixOutboundDispatchWorker worker, OmsConfig omsConfig) {
        return registrar -> {
            if (omsConfig.getFix().getOutboundDriver() != FixOutboundDriver.SCHEDULED) {
                return;
            }
            long ms = Math.max(MIN_FIX_OUTBOUND_POLL_INTERVAL_MS, omsConfig.getFix().getOutboundPollIntervalMs());
            registrar.addFixedDelayTask(worker::drainPendingOutboundOnce, Duration.ofMillis(ms));
        };
    }
}
