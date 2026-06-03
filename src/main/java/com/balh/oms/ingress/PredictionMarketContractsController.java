package com.balh.oms.ingress;

import com.balh.oms.predictionmarket.PredictionMarketContractRepository;
import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Phase C: contract catalog for Customer API BFF / Markets tab. */
@RestController
@RequestMapping("/internal/v1/prediction-market/contracts")
public class PredictionMarketContractsController {

    private final PredictionMarketContractRepository repository;

    public PredictionMarketContractsController(PredictionMarketContractRepository repository) {
        this.repository = repository;
    }

    @GetMapping
    public ResponseEntity<PredictionMarketContractDto.ContractListResponse> listOpen() {
        List<PredictionMarketContractDto.ContractResponse> items =
                repository.listOpen().stream().map(PredictionMarketContractDto::toResponse).toList();
        return ResponseEntity.ok(new PredictionMarketContractDto.ContractListResponse(items));
    }
}
