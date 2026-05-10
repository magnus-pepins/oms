package com.balh.oms.config;

import com.balh.oms.marketdata.MarketdataPlatformHttpClient;
import com.balh.oms.marketdata.RestMarketdataPlatformHttpClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

@Configuration
@ConditionalOnProperty(prefix = "oms.marketdata", name = "enabled", havingValue = "true")
public class MarketdataConfiguration {

    @Bean
    RestClient omsMarketdataRestClient(OmsConfig config) {
        String base = config.getMarketdata().getBaseUrl().trim();
        if (base.isEmpty()) {
            throw new IllegalStateException("oms.marketdata.base-url is required when oms.marketdata.enabled=true");
        }
        while (base.endsWith("/")) {
            base = base.substring(0, base.length() - 1);
        }
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout((int) config.getMarketdata().getConnectTimeoutMs());
        factory.setReadTimeout((int) config.getMarketdata().getReadTimeoutMs());
        return RestClient.builder().baseUrl(base).requestFactory(factory).build();
    }

    @Bean
    MarketdataPlatformHttpClient marketdataPlatformHttpClient(
            RestClient omsMarketdataRestClient, OmsConfig config, ObjectMapper objectMapper) {
        return new RestMarketdataPlatformHttpClient(omsMarketdataRestClient, config, objectMapper);
    }
}
