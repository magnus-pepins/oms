package com.balh.oms.ingress.readiness;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.config.OmsProfiles;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration(proxyBeanMethods = false)
@Profile(OmsProfiles.ORDER_ACCEPT_PROFILE)
@ConditionalOnProperty(prefix = "oms.cluster.client", name = "enabled", havingValue = "true")
public class OmsClusterReadinessConfiguration {

    private static final Logger log = LoggerFactory.getLogger(OmsClusterReadinessConfiguration.class);

    public static final int READINESS_FILTER_ORDER = -1_000;
    public static final int DEFAULT_RETRY_AFTER_SECONDS = 2;

    @Bean(destroyMethod = "close")
    public OmsClusterReadinessReader omsClusterReadinessReader(OmsConfig config) {
        String aeronDir = config.getCluster().getClient().getAeronDirectory();
        log.info("constructing OmsClusterReadinessReader: aeronDir='{}'", aeronDir);
        OmsClusterReadinessReader reader =
                new OmsClusterReadinessReader(aeronDir);
        reader.start();
        return reader;
    }

    @Bean
    public OmsClusterReadinessFilter omsClusterReadinessFilter(
            OmsClusterReadinessReader reader, ObjectMapper objectMapper) {
        return new OmsClusterReadinessFilter(reader, objectMapper, DEFAULT_RETRY_AFTER_SECONDS);
    }

    @Bean
    public FilterRegistrationBean<OmsClusterReadinessFilter> omsClusterReadinessFilterRegistration(
            OmsClusterReadinessFilter filter) {
        FilterRegistrationBean<OmsClusterReadinessFilter> registration =
                new FilterRegistrationBean<>(filter);
        registration.setOrder(READINESS_FILTER_ORDER);
        registration.addUrlPatterns("/*");
        registration.setName("omsClusterReadinessFilter");
        return registration;
    }
}
