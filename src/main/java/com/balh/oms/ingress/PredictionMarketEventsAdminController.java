package com.balh.oms.ingress;

import com.balh.oms.predictionmarket.PredictionMarketEventRepository;
import com.balh.oms.predictionmarket.PredictionMarketEventService;
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

/** Operator catalog for prediction-market events (Beard Admin). */
@RestController
@RequestMapping("/internal/v1/prediction-market/admin/events")
public class PredictionMarketEventsAdminController {

    public record CreateBody(
            String slug,
            String title,
            String description,
            String category,
            java.util.List<String> tags,
            String cardImageUrl,
            Integer displayOrder,
            String status) {}

    public record UpdateBody(
            String title,
            String description,
            String category,
            java.util.List<String> tags,
            String cardImageUrl,
            Integer displayOrder,
            String status) {}

    private final PredictionMarketEventRepository repository;
    private final PredictionMarketEventService service;

    public PredictionMarketEventsAdminController(
            PredictionMarketEventRepository repository, PredictionMarketEventService service) {
        this.repository = repository;
        this.service = service;
    }

    @GetMapping
    public ResponseEntity<PredictionMarketEventDto.EventListResponse> list(
            @RequestParam(required = false) String status) {
        String statusFilter =
                status == null || status.isBlank() ? null : status.trim().toUpperCase();
        List<PredictionMarketEventDto.EventResponse> items =
                repository.listAll(statusFilter).stream().map(PredictionMarketEventDto::toResponse).toList();
        return ResponseEntity.ok(new PredictionMarketEventDto.EventListResponse(items));
    }

    @GetMapping("/{id}")
    public ResponseEntity<PredictionMarketEventDto.EventResponse> getById(@PathVariable long id) {
        return repository
                .findById(id)
                .map(row -> ResponseEntity.ok(PredictionMarketEventDto.toResponse(row)))
                .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping
    public ResponseEntity<PredictionMarketEventDto.EventResponse> create(@RequestBody CreateBody body) {
        try {
            var row =
                    service.create(
                            new PredictionMarketEventService.CreateRequest(
                                    body.slug(),
                                    body.title(),
                                    body.description(),
                                    body.category(),
                                    body.tags(),
                                    body.cardImageUrl(),
                                    body.displayOrder(),
                                    body.status()));
            return ResponseEntity.status(HttpStatus.CREATED).body(PredictionMarketEventDto.toResponse(row));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().build();
        }
    }

    @PatchMapping("/{id}")
    public ResponseEntity<PredictionMarketEventDto.EventResponse> update(
            @PathVariable long id, @RequestBody UpdateBody body) {
        try {
            return service.update(
                            id,
                            new PredictionMarketEventService.UpdateRequest(
                                    body.title(),
                                    body.description(),
                                    body.category(),
                                    body.tags(),
                                    body.cardImageUrl(),
                                    body.displayOrder(),
                                    body.status()))
                    .map(row -> ResponseEntity.ok(PredictionMarketEventDto.toResponse(row)))
                    .orElse(ResponseEntity.notFound().build());
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().build();
        }
    }
}
