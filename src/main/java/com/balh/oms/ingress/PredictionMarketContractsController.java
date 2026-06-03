package com.balh.oms.ingress;

import com.balh.oms.predictionmarket.PredictionMarketContractRepository;
import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
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

    /** Single contract for Markets detail page (any status — detail shows resolved/closed too). */
    @GetMapping("/by-slug/{slug}")
    public ResponseEntity<PredictionMarketContractDto.ContractResponse> getBySlug(@PathVariable String slug) {
        return repository
                .findBySlug(slug)
                .map(row -> ResponseEntity.ok(PredictionMarketContractDto.toResponse(row)))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/{id}")
    public ResponseEntity<PredictionMarketContractDto.ContractResponse> getById(@PathVariable long id) {
        return repository
                .findById(id)
                .map(row -> ResponseEntity.ok(PredictionMarketContractDto.toResponse(row)))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
