package com.balh.oms.ingress;

import com.balh.oms.predictionmarket.PredictionMarketContractRepository;
import java.time.Instant;

/** Shared JSON shape for prediction-market catalog rows. */
public final class PredictionMarketContractDto {

    public record ContractResponse(
            long id,
            String slug,
            String title,
            String yesSymbol,
            String noSymbol,
            String resolutionSource,
            String status,
            String settlementCurrency,
            String tickSize,
            String payoutPerContract,
            Instant closesAt) {}

    public record ContractListResponse(java.util.List<ContractResponse> items) {}

    private PredictionMarketContractDto() {}

    public static ContractResponse toResponse(PredictionMarketContractRepository.ContractRow row) {
        return new ContractResponse(
                row.id(),
                row.slug(),
                row.title(),
                row.yesSymbol(),
                row.noSymbol(),
                row.resolutionSource(),
                row.status(),
                row.settlementCurrency(),
                row.tickSize().stripTrailingZeros().toPlainString(),
                row.payoutPerContract().stripTrailingZeros().toPlainString(),
                row.closesAt());
    }
}
