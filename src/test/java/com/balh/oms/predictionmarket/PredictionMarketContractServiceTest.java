package com.balh.oms.predictionmarket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.Test;

class PredictionMarketContractServiceTest {

    @Test
    void normalizeCurrency_defaultsToUsd() {
        assertEquals("USD", PredictionMarketContractService.normalizeCurrency(null));
        assertEquals("EUR", PredictionMarketContractService.normalizeCurrency("eur"));
    }

    @Test
    void normalizeCurrency_rejectsInvalid() {
        assertThrows(
                IllegalArgumentException.class,
                () -> PredictionMarketContractService.normalizeCurrency("USDD"));
    }

    @Test
    void requiresVenueRegistrySync_falseWhenOnlyRetailCopyChanges() {
        var before = sampleRow("OPEN", "0.01", "old", "old criteria");
        var after =
                new PredictionMarketContractRepository.ContractRow(
                        before.id(),
                        before.slug(),
                        before.title(),
                        before.yesSymbol(),
                        before.noSymbol(),
                        "new description",
                        "new criteria",
                        List.of(new PredictionMarketReferenceLinks.Link("Src", "https://example.com")),
                        before.resolutionSource(),
                        before.status(),
                        before.settlementCurrency(),
                        before.tickSize(),
                        before.payoutPerContract(),
                        before.closesAt(),
                        before.resolvesAt(),
                        before.jurisdictionTags(),
                        before.category(),
                        before.tags(),
                        before.cardImageUrl(),
                        before.displayOrder(),
                        before.feeModelId(),
                        before.feeScheduleVersion(),
                        before.feeParamsJson(),
                        before.retailFeeModelId(),
                        before.eventId(),
                        before.eventSlug(),
                        before.outcomeLabel(),
                        before.outcomeDisplayOrder());
        assertFalse(PredictionMarketContractService.requiresVenueRegistrySync(before, after));
    }

    @Test
    void shouldSyncVenueRegistry_onlyForOpenOrHalted() {
        assertFalse(PredictionMarketContractService.shouldSyncVenueRegistry("DRAFT"));
        assertTrue(PredictionMarketContractService.shouldSyncVenueRegistry("OPEN"));
        assertTrue(PredictionMarketContractService.shouldSyncVenueRegistry("HALTED"));
        assertFalse(PredictionMarketContractService.shouldSyncVenueRegistry("CLOSED"));
    }

    @Test
    void requiresVenueRegistrySync_trueWhenPublishingDraftToOpen() {
        var before = sampleRow("DRAFT", "0.01", null, null);
        var after = sampleRow("OPEN", "0.01", null, null);
        assertTrue(PredictionMarketContractService.requiresVenueRegistrySync(before, after));
    }

    @Test
    void requiresVenueRegistrySync_trueWhenStatusChanges() {
        var before = sampleRow("OPEN", "0.01", null, null);
        var after =
                new PredictionMarketContractRepository.ContractRow(
                        before.id(),
                        before.slug(),
                        before.title(),
                        before.yesSymbol(),
                        before.noSymbol(),
                        before.description(),
                        before.resolutionCriteria(),
                        before.referenceLinks(),
                        before.resolutionSource(),
                        "HALTED",
                        before.settlementCurrency(),
                        before.tickSize(),
                        before.payoutPerContract(),
                        before.closesAt(),
                        before.resolvesAt(),
                        before.jurisdictionTags(),
                        before.category(),
                        before.tags(),
                        before.cardImageUrl(),
                        before.displayOrder(),
                        before.feeModelId(),
                        before.feeScheduleVersion(),
                        before.feeParamsJson(),
                        before.retailFeeModelId(),
                        before.eventId(),
                        before.eventSlug(),
                        before.outcomeLabel(),
                        before.outcomeDisplayOrder());
        assertTrue(PredictionMarketContractService.requiresVenueRegistrySync(before, after));
    }

    private static PredictionMarketContractRepository.ContractRow sampleRow(
            String status, String tick, String description, String criteria) {
        return new PredictionMarketContractRepository.ContractRow(
                1L,
                "test-1",
                "Title",
                "PREDMKT-TEST-1",
                "PREDMKT-TEST-1-NO",
                description,
                criteria,
                List.of(),
                "it-oracle",
                status,
                "USD",
                new BigDecimal(tick),
                new BigDecimal("1.00"),
                null,
                null,
                List.of("SE"),
                null,
                List.of(),
                null,
                0,
                "ZERO",
                1,
                "{}",
                "TAKER_ONLY",
                null,
                null,
                null,
                0);
    }
}
