package com.balh.oms.ingress;

import com.balh.oms.predictionmarket.PredictionMarketContractRepository;
import com.balh.oms.predictionmarket.PredictionMarketEventRepository;
import java.util.ArrayList;
import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Phase H: grouped catalog feed for Customer API BFF / Markets tab. */
@RestController
@RequestMapping("/internal/v1/prediction-market")
public class PredictionMarketCatalogController {

    private final PredictionMarketEventRepository eventRepository;
    private final PredictionMarketContractRepository contractRepository;

    public PredictionMarketCatalogController(
            PredictionMarketEventRepository eventRepository,
            PredictionMarketContractRepository contractRepository) {
        this.eventRepository = eventRepository;
        this.contractRepository = contractRepository;
    }

    @GetMapping("/catalog")
    public ResponseEntity<PredictionMarketEventDto.CatalogResponse> catalog() {
        List<PredictionMarketEventDto.EventWithContractsResponse> events = new ArrayList<>();
        for (PredictionMarketEventRepository.EventRow event : eventRepository.listOpen()) {
            List<PredictionMarketContractRepository.ContractRow> contracts =
                    contractRepository.listOpenByEventId(event.id());
            if (contracts.isEmpty()) {
                continue;
            }
            events.add(PredictionMarketEventDto.toResponseWithContracts(event, contracts));
        }
        List<PredictionMarketContractDto.ContractResponse> standalone =
                contractRepository.listOpenStandalone().stream()
                        .map(PredictionMarketContractDto::toResponse)
                        .toList();
        return ResponseEntity.ok(new PredictionMarketEventDto.CatalogResponse(events, standalone));
    }

    @GetMapping("/events")
    public ResponseEntity<PredictionMarketEventDto.EventWithContractsListResponse> listOpenEvents() {
        List<PredictionMarketEventDto.EventWithContractsResponse> items = new ArrayList<>();
        for (PredictionMarketEventRepository.EventRow event : eventRepository.listOpen()) {
            List<PredictionMarketContractRepository.ContractRow> contracts =
                    contractRepository.listOpenByEventId(event.id());
            if (contracts.isEmpty()) {
                continue;
            }
            items.add(PredictionMarketEventDto.toResponseWithContracts(event, contracts));
        }
        return ResponseEntity.ok(new PredictionMarketEventDto.EventWithContractsListResponse(items));
    }

    @GetMapping("/events/by-slug/{slug}")
    public ResponseEntity<PredictionMarketEventDto.EventWithContractsResponse> getEventBySlug(
            @PathVariable String slug) {
        return eventRepository
                .findBySlug(slug)
                .map(
                        event -> {
                            List<PredictionMarketContractRepository.ContractRow> contracts =
                                    contractRepository.listOpenByEventId(event.id());
                            return ResponseEntity.ok(
                                    PredictionMarketEventDto.toResponseWithContracts(event, contracts));
                        })
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
