package com.balh.oms.ingress.reconcile;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.config.OmsProfiles;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.util.Optional;

@Configuration(proxyBeanMethods = false)
@Profile(OmsProfiles.ORDER_ACCEPT_PROFILE)
@ConditionalOnProperty(prefix = "oms.cluster.client", name = "enabled", havingValue = "true")
public class OmsReconciliationConfiguration {

    private static final Logger log = LoggerFactory.getLogger(OmsReconciliationConfiguration.class);

    @Bean(destroyMethod = "close")
    public OmsClusterCountsReader omsClusterCountsReader(OmsConfig config) {
        String aeronDir = config.getCluster().getClient().getAeronDirectory();
        log.info("constructing OmsClusterCountsReader: aeronDir='{}'", aeronDir);
        OmsClusterCountsReader reader = new OmsClusterCountsReader(aeronDir);
        reader.start();
        return reader;
    }

    @Bean(destroyMethod = "close")
    public OmsReconciliationService omsReconciliationService(
            OmsClusterCountsReader clusterCountsReader,
            ObjectProvider<OmsProjectorOrderCountsRepository> projectorRepoProvider,
            ObjectProvider<MeterRegistry> meterRegistryProvider) {
        Optional<OmsProjectorOrderCountsRepository> projectorRepo =
                Optional.ofNullable(projectorRepoProvider.getIfAvailable());
        MeterRegistry meterRegistry = meterRegistryProvider.getIfAvailable();
        log.info(
                "constructing OmsReconciliationService: projectorConfigured={}",
                projectorRepo.isPresent());
        OmsReconciliationService service = new OmsReconciliationService(
                clusterCountsReader,
                projectorRepo,
                meterRegistry,
                OmsReconciliationService.DEFAULT_POLL_INTERVAL_MS,
                OmsReconciliationService.DEFAULT_INITIAL_DELAY_MS);
        service.start();
        return service;
    }
}
