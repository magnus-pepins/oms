package com.balh.oms.config;

import com.balh.oms.ledger.LedgerSettlementPostingClient;
import com.balh.oms.ledger.RestLedgerSettlementPostingClient;
import com.balh.oms.reconciler.LedgerSettlementOutboxReconciler;
import com.balh.oms.settlement.LedgerSettlementOutboxRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * Wires {@link LedgerSettlementPostingClient} when Ledger HTTP is enabled and settlement outbox delivery is on.
 */
@Configuration
@ConditionalOnProperty(
        prefix = "oms.ledger",
        name = {"enabled", "settlement-outbox-reconciler-enabled"},
        havingValue = "true")
public class LedgerSettlementOutboxConfiguration {

    @Bean
    LedgerSettlementPostingClient ledgerSettlementPostingClient(
            @Qualifier("omsLedgerRestClient") org.springframework.web.client.RestClient omsLedgerRestClient,
            OmsConfig config,
            ObjectMapper objectMapper) {
        String key = config.getLedger().getApiKey();
        if (key == null || key.isBlank()) {
            throw new IllegalStateException("oms.ledger.api-key is required when settlement outbox reconciler is enabled");
        }
        return new RestLedgerSettlementPostingClient(
                omsLedgerRestClient, key, objectMapper, config.getLedger().getSettlementPostingHttpPath());
    }

    @Bean
    LedgerSettlementOutboxReconciler ledgerSettlementOutboxReconciler(
            LedgerSettlementOutboxRepository outbox,
            LedgerSettlementPostingClient postingClient,
            OmsConfig config,
            PlatformTransactionManager transactionManager,
            MeterRegistry meterRegistry) {
        return new LedgerSettlementOutboxReconciler(
                outbox, postingClient, config, transactionManager, meterRegistry);
    }
}
