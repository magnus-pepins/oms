package com.balh.oms.cluster;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.config.OmsProfiles;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * Venue-egress uses a dedicated {@link ClusterClientRole#ER_OFFER_ONLY} cluster session so ER
 * offers are not serialized behind egress {@code pollEgress} on the same {@code clientLock}.
 */
@Configuration(proxyBeanMethods = false)
@Profile(OmsProfiles.VENUE_EGRESS)
@ConditionalOnProperty(prefix = "oms.cluster.client", name = "enabled", havingValue = "true")
public class VenueEgressClusterClientConfiguration {

    private static final Logger log = LoggerFactory.getLogger(VenueEgressClusterClientConfiguration.class);

    private volatile OmsClusterIngressClient client;

    @Bean(name = "omsClusterIngressClient")
    public OmsClusterIngressClient venueEgressErOfferClusterClient(
            OmsConfig config, MeterRegistry meterRegistry) {
        OmsConfig.Cluster.Client clientConfig = config.getCluster().getClient();
        if (clientConfig.getRole() != ClusterClientRole.ER_OFFER_ONLY) {
            log.info(
                    "venue-egress cluster client: overriding role {} -> ER_OFFER_ONLY",
                    clientConfig.getRole());
            clientConfig.setRole(ClusterClientRole.ER_OFFER_ONLY);
        }
        OmsClusterIngressClient built = new OmsClusterIngressClient(clientConfig, meterRegistry);
        built.connect();
        this.client = built;
        return built;
    }

    @PreDestroy
    void closeClient() {
        OmsClusterIngressClient c = client;
        if (c != null) {
            c.close();
        }
    }
}
