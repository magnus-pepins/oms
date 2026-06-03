package com.balh.oms.ingress;

import com.balh.oms.predictionmarket.PredictionMarketContractRepository;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Phase C: contract catalog for Customer API BFF / Markets tab. */
@RestController
@RequestMapping("/internal/v1/prediction-market/contracts")
public class PredictionMarketContractsController {

    public record ContractResponse(
            long id,
            String slug,
            String title,
            String yesSymbol,
            String noSymbol,
            String resolutionSource,
            String status,
            String tickSize,
            String payoutPerContract,
            Instant closesAt) {}

    public record ContractListResponse(List<ContractResponse> items) {}

    private final PredictionMarketContractRepository repository;

    public PredictionMarketContractsController(PredictionMarketContractRepository repository) {
        this.repository = repository;
    }

    @GetMapping
    public ResponseEntity<ContractListResponse> listOpen() {
        List<ContractResponse> items =
                repository.listOpen().stream().map(PredictionMarketContractsController::toResponse).toList();
        return ResponseEntity.ok(new ContractListResponse(items));
    }

    private static ContractResponse toResponse(PredictionMarketContractRepository.ContractRow row) {
        return new ContractResponse(
                row.id(),
                row.slug(),
                row.title(),
                row.yesSymbol(),
                row.noSymbol(),
                row.resolutionSource(),
                row.status(),
                row.tickSize().stripTrailingZeros().toPlainString(),
                row.payoutPerContract().stripTrailingZeros().toPlainString(),
                row.closesAt());
    }
}
