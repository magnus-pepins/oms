package com.balh.oms.ingress;

import com.balh.oms.predictionmarket.PredictionMarketContractRepository;
import com.balh.oms.predictionmarket.PredictionMarketEventRepository;
import java.util.List;

/** Shared JSON shape for prediction-market event catalog rows. */
public final class PredictionMarketEventDto {

    public record EventResponse(
            long id,
            String slug,
            String title,
            String description,
            String category,
            java.util.List<String> tags,
            String cardImageUrl,
            int displayOrder,
            String status) {}

    public record EventWithContractsResponse(
            long id,
            String slug,
            String title,
            String description,
            String category,
            java.util.List<String> tags,
            String cardImageUrl,
            int displayOrder,
            String status,
            java.util.List<PredictionMarketContractDto.ContractResponse> contracts) {}

    public record EventListResponse(java.util.List<EventResponse> items) {}

    public record EventWithContractsListResponse(java.util.List<EventWithContractsResponse> items) {}

    public record CatalogResponse(
            java.util.List<EventWithContractsResponse> events,
            java.util.List<PredictionMarketContractDto.ContractResponse> standaloneContracts) {}

    private PredictionMarketEventDto() {}

    public static EventResponse toResponse(PredictionMarketEventRepository.EventRow row) {
        return new EventResponse(
                row.id(),
                row.slug(),
                row.title(),
                row.description(),
                row.category(),
                row.tags(),
                row.cardImageUrl(),
                row.displayOrder(),
                row.status());
    }

    public static EventWithContractsResponse toResponseWithContracts(
            PredictionMarketEventRepository.EventRow row,
            List<PredictionMarketContractRepository.ContractRow> contracts) {
        List<PredictionMarketContractDto.ContractResponse> contractResponses =
                contracts.stream().map(PredictionMarketContractDto::toResponse).toList();
        return new EventWithContractsResponse(
                row.id(),
                row.slug(),
                row.title(),
                row.description(),
                row.category(),
                row.tags(),
                row.cardImageUrl(),
                row.displayOrder(),
                row.status(),
                contractResponses);
    }
}
