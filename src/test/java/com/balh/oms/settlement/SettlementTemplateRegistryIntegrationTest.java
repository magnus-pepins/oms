package com.balh.oms.settlement;

import static org.assertj.core.api.Assertions.assertThat;

import com.balh.oms.AbstractPostgresIntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class SettlementTemplateRegistryIntegrationTest extends AbstractPostgresIntegrationTest {

    @Autowired SettlementTemplateRegistry registry;

    @Test
    void flywaySeeds_predictionMarketAndEquityBrokerTemplates() {
        assertThat(registry.find(SettlementTemplateIds.PREDICTION_MARKET_BINARY_RESOLUTION, 1))
                .isPresent()
                .get()
                .satisfies(
                        def -> {
                            assertThat(def.outboxTable()).isEqualTo(SettlementTemplateRegistry.OUTBOX_PREDICTION_MARKET);
                            assertThat(def.active()).isTrue();
                        });
        assertThat(registry.find(SettlementTemplateIds.EQUITY_BROKER_EOD_V1, 1))
                .isPresent()
                .get()
                .satisfies(
                        def -> {
                            assertThat(def.outboxTable()).isEqualTo(SettlementTemplateRegistry.OUTBOX_LEDGER_SETTLEMENT);
                            assertThat(def.active()).isTrue();
                        });
    }
}
