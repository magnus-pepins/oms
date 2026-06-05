package com.balh.oms.config;

import org.junit.jupiter.api.Test;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.lang.reflect.Field;

import static org.assertj.core.api.Assertions.assertThat;

class LedgerConfigurationTest {

    @Test
    void omsLedgerRestClient_usesJdkRequestFactoryForConnectionReuse() {
        OmsConfig config = new OmsConfig();
        config.getLedger().setBaseUrl("http://localhost:5001/");
        config.getLedger().setConnectTimeoutMs(1_500L);
        config.getLedger().setReadTimeoutMs(4_000L);

        RestClient restClient = new LedgerConfiguration().omsLedgerRestClient(config);

        ClientHttpRequestFactory requestFactory = extractRequestFactory(restClient);
        assertThat(requestFactory).isInstanceOf(JdkClientHttpRequestFactory.class);
    }

    private static ClientHttpRequestFactory extractRequestFactory(RestClient restClient) {
        Class<?> type = restClient.getClass();
        while (type != null && type != Object.class) {
            for (Field field : type.getDeclaredFields()) {
                if (!ClientHttpRequestFactory.class.isAssignableFrom(field.getType())) {
                    continue;
                }
                field.setAccessible(true);
                try {
                    Object value = field.get(restClient);
                    if (value instanceof ClientHttpRequestFactory factory) {
                        return factory;
                    }
                } catch (IllegalAccessException ignored) {
                    // Continue searching in case another factory field is accessible.
                }
            }
            type = type.getSuperclass();
        }
        throw new IllegalStateException("RestClient implementation did not expose a ClientHttpRequestFactory field");
    }
}
