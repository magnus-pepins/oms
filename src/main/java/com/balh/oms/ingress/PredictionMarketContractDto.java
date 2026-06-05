package com.balh.oms.ingress;

import com.balh.oms.predictionmarket.PredictionMarketContractRepository;
import com.balh.oms.predictionmarket.PredictionMarketReferenceLinks;
import java.time.Instant;
import java.util.List;

/** Shared JSON shape for prediction-market catalog rows. */
public final class PredictionMarketContractDto {

    public record ReferenceLinkResponse(String label, String url) {}

    public record ContractResponse(
            long id,
            String slug,
            String title,
            String yesSymbol,
            String noSymbol,
            String description,
            String resolutionCriteria,
            List<ReferenceLinkResponse> referenceLinks,
            String resolutionSource,
            String status,
            String settlementCurrency,
            String tickSize,
            String payoutPerContract,
            Instant closesAt,
            Instant resolvesAt,
            java.util.List<String> jurisdictionTags,
            String category,
            java.util.List<String> tags,
            String cardImageUrl,
            int displayOrder,
            String feeModelId,
            int feeScheduleVersion,
            String feeParamsJson,
            String retailFeeModelId) {}

    public record ContractListResponse(java.util.List<ContractResponse> items) {}

    private PredictionMarketContractDto() {}

    public static ContractResponse toResponse(PredictionMarketContractRepository.ContractRow row) {
        List<ReferenceLinkResponse> links =
                row.referenceLinks().stream()
                        .map(l -> new ReferenceLinkResponse(l.label(), l.url()))
                        .toList();
        return new ContractResponse(
                row.id(),
                row.slug(),
                row.title(),
                row.yesSymbol(),
                row.noSymbol(),
                row.description(),
                row.resolutionCriteria(),
                links,
                row.resolutionSource(),
                row.status(),
                row.settlementCurrency(),
                row.tickSize().stripTrailingZeros().toPlainString(),
                row.payoutPerContract().stripTrailingZeros().toPlainString(),
                row.closesAt(),
                row.resolvesAt(),
                row.jurisdictionTags(),
                row.category(),
                row.tags(),
                row.cardImageUrl(),
                row.displayOrder(),
                row.feeModelId(),
                row.feeScheduleVersion(),
                row.feeParamsJson(),
                row.retailFeeModelId());
    }
}
