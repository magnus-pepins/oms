package com.balh.oms.config;

import com.balh.oms.ledger.LedgerSettlementLegPoster;
import com.balh.oms.ledger.LedgerSettlementPostingClient;
import com.balh.oms.reconciler.LedgerSettlementOutboxReconciler;
import com.balh.oms.reconciler.PredictionMarketLedgerOutboxReconciler;
import com.balh.oms.settlement.LedgerSettlementOutboxRepository;
import com.balh.oms.settlement.PredictionMarketLedgerOutboxRepository;
import com.balh.oms.settlement.SettlementTemplateRegistry;
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
/**
 * <p>Deploy only on {@code oms-postgres-projector}: set
 * {@code OMS_LEDGER_SETTLEMENT_OUTBOX_RECONCILER_ENABLED=true} there and {@code false} on
 * ingress / fix-egress (see {@code ecosystem.config.cjs}). There is no Spring
 * {@code @Profile} gate here so integration tests can enable the reconciler via properties.
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
            ObjectMapper objectMapper,
            SettlementTemplateRegistry settlementTemplateRegistry) {
        String key = config.getLedger().getApiKey();
        if (key == null || key.isBlank()) {
            throw new IllegalStateException("oms.ledger.api-key is required when settlement outbox reconciler is enabled");
        }
        // V39 multi-leg poster: posts one Ledger /transactions per outbox row, dispatched by leg_kind.
        // settlement-posting-http-path is no longer consulted; ignore or remove the property.
        return new LedgerSettlementLegPoster(omsLedgerRestClient, key, objectMapper, settlementTemplateRegistry);
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

    @Bean
    PredictionMarketLedgerOutboxReconciler predictionMarketLedgerOutboxReconciler(
            PredictionMarketLedgerOutboxRepository outbox,
            LedgerSettlementPostingClient postingClient,
            OmsConfig config,
            PlatformTransactionManager transactionManager) {
        if (!(postingClient instanceof LedgerSettlementLegPoster poster)) {
            throw new IllegalStateException(
                    "prediction-market reconciler requires LedgerSettlementLegPoster implementation");
        }
        return new PredictionMarketLedgerOutboxReconciler(outbox, poster, config, transactionManager);
    }
}
