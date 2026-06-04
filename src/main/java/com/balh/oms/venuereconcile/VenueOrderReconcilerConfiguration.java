package com.balh.oms.venuereconcile;

import com.balh.oms.cluster.OmsClusterIngressClient;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.config.OmsProfiles;
import com.balh.oms.persistence.OrdersRepository;
import com.balh.oms.venue.VenueRouteOrderClient;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.time.Clock;

/**
 * Wires {@link VenueOrderReconciler} on the {@code oms-venue-egress} JVM. Gated by
 * {@code oms.cluster.venue-reconciler.enabled=true}; when enabled it requires the internal-venue
 * routing beans ({@link VenueRouteOrderClient}, {@link OmsClusterIngressClient}) that the
 * {@code oms.routing.backend=internal-venue} egress already provides.
 */
@Configuration(proxyBeanMethods = false)
@Profile(OmsProfiles.VENUE_EGRESS)
@ConditionalOnProperty(prefix = "oms.cluster.venue-reconciler", name = "enabled", havingValue = "true")
public class VenueOrderReconcilerConfiguration {

    private static final Logger log = LoggerFactory.getLogger(VenueOrderReconcilerConfiguration.class);

    @Bean(destroyMethod = "close")
    public VenueOrderReconciler venueOrderReconciler(
            OmsConfig config,
            OrdersRepository ordersRepository,
            VenueRouteOrderClient venueRouteOrderClient,
            OmsClusterIngressClient clusterIngressClient,
            ObjectProvider<MeterRegistry> meterRegistryProvider) {
        log.info("constructing VenueOrderReconciler (golden-copy order-state reconciliation)");
        VenueOrderReconciler reconciler = new VenueOrderReconciler(
                config,
                ordersRepository,
                venueRouteOrderClient,
                clusterIngressClient,
                Clock.systemUTC(),
                meterRegistryProvider.getIfAvailable());
        reconciler.start();
        return reconciler;
    }
}
