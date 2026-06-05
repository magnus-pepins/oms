package com.balh.oms.config;

import com.balh.oms.ledger.CachingLedgerBalanceClient;
import com.balh.oms.ledger.LedgerBalanceClient;
import com.balh.oms.ledger.LedgerMetadataClient;
import com.balh.oms.ledger.RestLedgerMetadataClient;
import com.balh.oms.ledger.LedgerInflightBulkDispatcher;
import com.balh.oms.ledger.LedgerInflightCoalescer;
import com.balh.oms.ledger.LedgerInflightReservationClient;
import com.balh.oms.ledger.RestLedgerBalanceClient;
import com.balh.oms.ledger.RestLedgerInflightBulkDispatcher;
import com.balh.oms.ledger.LedgerInflightLifecycleClient;
import com.balh.oms.ledger.RestLedgerInflightLifecycleClient;
import com.balh.oms.ledger.LedgerIskReadClient;
import com.balh.oms.ledger.RestLedgerIskReadClient;
import com.balh.oms.ledger.RestLedgerInflightReservationClient;
import com.balh.oms.persistence.LedgerInflightOutboxRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.web.client.RestClient;

import java.net.http.HttpClient;
import java.time.Duration;

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
        HttpClient httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(config.getLedger().getConnectTimeoutMs()))
                .build();
        JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(httpClient);
        factory.setReadTimeout(Duration.ofMillis(config.getLedger().getReadTimeoutMs()));
        return RestClient.builder()
                .baseUrl(base)
                .requestFactory(factory)
                .build();
    }

    /**
     * Phase 4 Tier 2.5 phase D-8: when
     * {@code oms.ledger.balance-identity-cache.enabled=true}, the {@link RestLedgerBalanceClient}
     * is wrapped in {@link CachingLedgerBalanceClient}. The cache is JVM-local (each ingress
     * has its own) and only caches the durable {@code (balanceId -> identityId)} binding;
     * the volatile {@code availableBalance} / {@code balance} reads pass through unchanged.
     * See {@code OmsConfig.Ledger.BalanceIdentityCache} Javadoc for the operator contract
     * around bounded staleness.
     */
    @Bean
    LedgerBalanceClient ledgerBalanceClient(
            RestClient omsLedgerRestClient,
            OmsConfig config,
            ObjectMapper objectMapper,
            MeterRegistry meterRegistry) {
        String key = config.getLedger().getApiKey();
        if (key == null || key.isBlank()) {
            throw new IllegalStateException("oms.ledger.api-key is required when oms.ledger.enabled=true");
        }
        LedgerBalanceClient delegate = new RestLedgerBalanceClient(omsLedgerRestClient, key, objectMapper);
        var cacheConfig = config.getLedger().getBalanceIdentityCache();
        if (!cacheConfig.isEnabled()) {
            return delegate;
        }
        return new CachingLedgerBalanceClient(
                delegate,
                cacheConfig.getTtlSeconds(),
                cacheConfig.getMaxSize(),
                meterRegistry);
    }

    @Bean
    @ConditionalOnProperty(prefix = "oms.ledger", name = "metadata-sync-enabled", havingValue = "true")
    LedgerMetadataClient ledgerMetadataClient(
            RestClient omsLedgerRestClient, OmsConfig config, ObjectMapper objectMapper) {
        String key = config.getLedger().getApiKey();
        if (key == null || key.isBlank()) {
            throw new IllegalStateException("oms.ledger.api-key is required when oms.ledger.metadata-sync-enabled=true");
        }
        return new RestLedgerMetadataClient(omsLedgerRestClient, key, objectMapper);
    }

    @Bean
    @ConditionalOnProperty(prefix = "oms.ledger", name = "isk-read-enabled", havingValue = "true")
    LedgerIskReadClient ledgerIskReadClient(
            RestClient omsLedgerRestClient, OmsConfig config, ObjectMapper objectMapper) {
        String token = config.getLedger().getElevatedApiKey();
        if (token == null || token.isBlank()) {
            throw new IllegalStateException(
                    "oms.ledger.elevated-api-key is required when oms.ledger.isk-read-enabled=true");
        }
        return new RestLedgerIskReadClient(omsLedgerRestClient, token.trim(), objectMapper);
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
        // §8.4 per-currency dest override: empty map = legacy single-currency behaviour.
        // Operators populate via {@code oms.ledger.inflight-hold-destination-balance-id-by-currency.<CCY>}
        // or {@code OMS_LEDGER_INFLIGHT_HOLD_DESTINATION_BALANCE_ID_BY_CURRENCY_<CCY>=balance_x}.
        return new RestLedgerInflightReservationClient(
                omsLedgerRestClient,
                key,
                objectMapper,
                dest.trim(),
                ledger.getInflightHoldDestinationBalanceIdByCurrency(),
                currency.trim(),
                prec);
    }

    /**
     * Wed-demo (V32): {@code PUT /transactions/inflight/{txID}} client used by
     * {@code LedgerInflightLifecycleReconciler}. Bean is loaded whenever the reservation client
     * is loaded (same {@code oms.ledger.inflight-reservation-enabled} switch — the lifecycle
     * client is meaningful only when we're actually placing holds in the first place); the
     * reconciler itself stays off unless {@code oms.ledger.inflight-lifecycle-reconciler-enabled}
     * is flipped.
     */
    @Bean
    @ConditionalOnProperty(
            prefix = "oms.ledger", name = "inflight-reservation-enabled", havingValue = "true")
    LedgerInflightLifecycleClient ledgerInflightLifecycleClient(
            RestClient omsLedgerRestClient,
            OmsConfig config,
            ObjectMapper objectMapper) {
        String key = config.getLedger().getApiKey();
        if (key == null || key.isBlank()) {
            throw new IllegalStateException(
                    "oms.ledger.api-key is required when oms.ledger.inflight-reservation-enabled=true");
        }
        return new RestLedgerInflightLifecycleClient(omsLedgerRestClient, key, objectMapper);
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
