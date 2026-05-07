package com.balh.oms.config;

import com.balh.oms.ledger.LedgerBalanceClient;
import com.balh.oms.ledger.RestLedgerBalanceClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

/**
 * Wires a typed {@link RestClient} + {@link LedgerBalanceClient} when
 * {@code oms.ledger.enabled=true}.
 */
@Configuration
@ConditionalOnProperty(prefix = "oms.ledger", name = "enabled", havingValue = "true")
public class LedgerConfiguration {

    @Bean
    RestClient omsLedgerRestClient(OmsConfig config) {
        String base = config.getLedger().getBaseUrl().trim();
        if (base.isEmpty()) {
            throw new IllegalStateException("oms.ledger.base-url is required when oms.ledger.enabled=true");
        }
        while (base.endsWith("/")) {
            base = base.substring(0, base.length() - 1);
        }
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout((int) config.getLedger().getConnectTimeoutMs());
        factory.setReadTimeout((int) config.getLedger().getReadTimeoutMs());
        return RestClient.builder()
                .baseUrl(base)
                .requestFactory(factory)
                .build();
    }

    @Bean
    LedgerBalanceClient ledgerBalanceClient(RestClient omsLedgerRestClient, OmsConfig config, ObjectMapper objectMapper) {
        String key = config.getLedger().getApiKey();
        if (key == null || key.isBlank()) {
            throw new IllegalStateException("oms.ledger.api-key is required when oms.ledger.enabled=true");
        }
        return new RestLedgerBalanceClient(omsLedgerRestClient, key, objectMapper);
    }
}
