package com.balh.oms.ingress;

import com.balh.oms.predictionmarket.PredictionMarketContractRepository;
import com.balh.oms.predictionmarket.PredictionMarketContractService;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Operator catalog for prediction-market contracts (Beard Admin / Ops Console).
 *
 * <p>Auth: same {@code X-OMS-Internal-Key} as other {@code /internal/v1/**} routes.
 */
@RestController
@RequestMapping("/internal/v1/prediction-market/admin/contracts")
public class PredictionMarketContractsAdminController {

    public record CreateBody(
            String slug,
            String title,
            String yesSymbol,
            String noSymbol,
            String resolutionSource,
            String settlementCurrency,
            String tickSize,
            String payoutPerContract,
            Instant closesAt,
            String status) {}

    public record UpdateBody(
            String title,
            String resolutionSource,
            String settlementCurrency,
            String tickSize,
            String payoutPerContract,
            Instant closesAt,
            String status) {}

    private final PredictionMarketContractRepository repository;
    private final PredictionMarketContractService service;

    public PredictionMarketContractsAdminController(
            PredictionMarketContractRepository repository, PredictionMarketContractService service) {
        this.repository = repository;
        this.service = service;
    }

    @GetMapping
    public ResponseEntity<PredictionMarketContractDto.ContractListResponse> list(
            @RequestParam(required = false) String status) {
        String statusFilter =
                status == null || status.isBlank() ? null : status.trim().toUpperCase();
        List<PredictionMarketContractDto.ContractResponse> items =
                repository.listAll(statusFilter).stream().map(PredictionMarketContractDto::toResponse).toList();
        return ResponseEntity.ok(new PredictionMarketContractDto.ContractListResponse(items));
    }

    @PostMapping
    public ResponseEntity<PredictionMarketContractDto.ContractResponse> create(@RequestBody CreateBody body) {
        try {
            var row =
                    service.create(
                            new PredictionMarketContractService.CreateRequest(
                                    body.slug(),
                                    body.title(),
                                    body.yesSymbol(),
                                    body.noSymbol(),
                                    body.resolutionSource(),
                                    body.settlementCurrency(),
                                    parseDecimal(body.tickSize()),
                                    parseDecimal(body.payoutPerContract()),
                                    body.closesAt(),
                                    body.status()));
            return ResponseEntity.status(HttpStatus.CREATED).body(PredictionMarketContractDto.toResponse(row));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().build();
        }
    }

    @PatchMapping("/{id}")
    public ResponseEntity<PredictionMarketContractDto.ContractResponse> update(
            @PathVariable long id, @RequestBody UpdateBody body) {
        try {
            return service.update(
                            id,
                            new PredictionMarketContractService.UpdateRequest(
                                    body.title(),
                                    body.resolutionSource(),
                                    body.settlementCurrency(),
                                    parseDecimal(body.tickSize()),
                                    parseDecimal(body.payoutPerContract()),
                                    body.closesAt(),
                                    body.status()))
                    .map(row -> ResponseEntity.ok(PredictionMarketContractDto.toResponse(row)))
                    .orElse(ResponseEntity.notFound().build());
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().build();
        }
    }

    private static BigDecimal parseDecimal(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        return new BigDecimal(raw.trim());
    }
}
