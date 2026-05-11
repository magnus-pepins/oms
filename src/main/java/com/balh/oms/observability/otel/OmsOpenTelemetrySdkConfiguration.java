package com.balh.oms.observability.otel;

import com.balh.oms.config.OmsConfig;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.exporter.prometheus.PrometheusHttpServer;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.resources.Resource;
import jakarta.annotation.PreDestroy;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

/**
 * Optional OpenTelemetry {@link SdkMeterProvider} with a Prometheus scrape listener (defaults to port {@code 9464}).
 *
 * <p>Micrometer {@code /actuator/prometheus} remains available; this path exposes OTel-native histograms for
 * ingress→FIX NOS latency. Disable in tests and dev unless you intend to scrape the extra port.
 */
@Configuration
@ConditionalOnProperty(prefix = "oms.otel", name = "metrics-enabled", havingValue = "true")
public class OmsOpenTelemetrySdkConfiguration {

    private volatile SdkMeterProvider sdkMeterProvider;

    @Bean(destroyMethod = "")
    public SdkMeterProvider omsSdkMeterProvider(OmsConfig config) {
        int port = config.getOtel().getPrometheusPort();
        PrometheusHttpServer prometheus = PrometheusHttpServer.builder().setPort(port).build();
        Resource resource = Resource.getDefault()
                .merge(Resource.create(Attributes.of(AttributeKey.stringKey("service.name"), "oms")));
        SdkMeterProvider provider = SdkMeterProvider.builder()
                .setResource(resource)
                .registerMetricReader(prometheus)
                .build();
        this.sdkMeterProvider = provider;
        return provider;
    }

    @Bean
    public OpenTelemetry omsOpenTelemetry(SdkMeterProvider omsSdkMeterProvider) {
        return OpenTelemetrySdk.builder().setMeterProvider(omsSdkMeterProvider).build();
    }

    @PreDestroy
    public void shutdownOtelMeterProvider() {
        SdkMeterProvider p = sdkMeterProvider;
        if (p != null) {
            p.shutdown().join(10, TimeUnit.SECONDS);
        }
    }
}
