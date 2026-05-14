package com.balh.oms.config;

import com.balh.oms.ledger.LedgerBalanceClient;
import com.balh.oms.ledger.LedgerInflightBulkDispatcher;
import com.balh.oms.ledger.LedgerInflightCoalescer;
import com.balh.oms.ledger.LedgerInflightReservationClient;
import com.balh.oms.ledger.RestLedgerBalanceClient;
import com.balh.oms.ledger.RestLedgerInflightBulkDispatcher;
import com.balh.oms.ledger.RestLedgerInflightReservationClient;
import com.balh.oms.persistence.LedgerInflightOutboxRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.transaction.PlatformTransactionManager;
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

    @Bean
    @ConditionalOnProperty(
            prefix = "oms.ledger", name = "inflight-reservation-enabled", havingValue = "true")
    LedgerInflightReservationClient ledgerInflightReservationClient(
            RestClient omsLedgerRestClient,
            OmsConfig config,
            ObjectMapper objectMapper) {
        var ledger = config.getLedger();
        String dest = ledger.getInflightHoldDestinationBalanceId();
        if (dest == null || dest.isBlank()) {
            throw new IllegalStateException(
                    "oms.ledger.inflight-hold-destination-balance-id is required when oms.ledger.inflight-reservation-enabled=true");
        }
        int prec = ledger.getInflightReservationPrecision();
        if (prec <= 0) {
            throw new IllegalStateException("oms.ledger.inflight-reservation-precision must be positive");
        }
        String key = ledger.getApiKey();
        if (key == null || key.isBlank()) {
            throw new IllegalStateException("oms.ledger.api-key is required when oms.ledger.inflight-reservation-enabled=true");
        }
        String currency = ledger.getInflightReservationCurrency();
        if (currency == null || currency.isBlank()) {
            throw new IllegalStateException("oms.ledger.inflight-reservation-currency must not be empty");
        }
        return new RestLedgerInflightReservationClient(
                omsLedgerRestClient,
                key,
                objectMapper,
                dest.trim(),
                currency.trim(),
                prec);
    }

    /**
     * Phase 4 slice 4q — coalesces BUY inflight holds onto Ledger
     * {@code POST /transactions/bulk?inflight=true&atomic=false}. Off by default; operators flip
     * {@code oms.ledger.inflight-coalescer-enabled=true} after benching against their Ledger
     * shape. The coalescer reuses the existing {@code RestLedgerInflightBulkDispatcher} (same
     * destination balance / currency / precision as the per-order client above).
     */
    @Bean
    @ConditionalOnProperty(
            prefix = "oms.ledger", name = "inflight-coalescer-enabled", havingValue = "true")
    LedgerInflightBulkDispatcher ledgerInflightBulkDispatcher(
            RestClient omsLedgerRestClient,
            OmsConfig config,
            ObjectMapper objectMapper) {
        var ledger = config.getLedger();
        String dest = ledger.getInflightHoldDestinationBalanceId();
        if (dest == null || dest.isBlank()) {
            throw new IllegalStateException(
                    "oms.ledger.inflight-hold-destination-balance-id is required when oms.ledger.inflight-coalescer-enabled=true");
        }
        int prec = ledger.getInflightReservationPrecision();
        if (prec <= 0) {
            throw new IllegalStateException("oms.ledger.inflight-reservation-precision must be positive");
        }
        String key = ledger.getApiKey();
        if (key == null || key.isBlank()) {
            throw new IllegalStateException("oms.ledger.api-key is required when oms.ledger.inflight-coalescer-enabled=true");
        }
        String currency = ledger.getInflightReservationCurrency();
        if (currency == null || currency.isBlank()) {
            throw new IllegalStateException("oms.ledger.inflight-reservation-currency must not be empty");
        }
        return new RestLedgerInflightBulkDispatcher(
                omsLedgerRestClient,
                key,
                objectMapper,
                dest.trim(),
                currency.trim(),
                prec);
    }

    /**
     * Daemon flush thread starts on bean init ({@code initMethod=start}) and stops on context
     * shutdown ({@code destroyMethod=stop}). Spring orders {@code @PreDestroy}-equivalent
     * destroy methods before the {@code DataSource} closes, so the coalescer's shutdown drain
     * still has Postgres available for outbox fallback inserts.
     */
    @Bean(initMethod = "start", destroyMethod = "stop")
    @ConditionalOnProperty(
            prefix = "oms.ledger", name = "inflight-coalescer-enabled", havingValue = "true")
    LedgerInflightCoalescer ledgerInflightCoalescer(
            OmsConfig config,
            LedgerInflightBulkDispatcher dispatcher,
            LedgerInflightOutboxRepository outbox,
            ObjectMapper objectMapper,
            MeterRegistry meterRegistry,
            PlatformTransactionManager transactionManager) {
        return new LedgerInflightCoalescer(
                config, dispatcher, outbox, objectMapper, meterRegistry, transactionManager);
    }
}
