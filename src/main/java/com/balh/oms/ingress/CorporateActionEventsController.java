package com.balh.oms.ingress;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.corporateaction.CorporateActionEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

/**
 * Internal ingest for {@code corporate_action_event} (slice 8 stub).
 */
@RestController
@RequestMapping("/internal/v1/corporate-action-events")
public class CorporateActionEventsController {

    private final CorporateActionEventRepository repository;
    private final OmsConfig omsConfig;
    private final ObjectMapper objectMapper;

    public CorporateActionEventsController(
            CorporateActionEventRepository repository, OmsConfig omsConfig, ObjectMapper objectMapper) {
        this.repository = repository;
        this.omsConfig = omsConfig;
        this.objectMapper = objectMapper;
    }

    public record IngestRequest(String instrumentSymbol, String actionType, String effectiveDate, Map<String, Object> payloadJson) {}

    public record IngestResponse(long id) {}

    @GetMapping
    public ResponseEntity<?> list(
            @org.springframework.web.bind.annotation.RequestParam(name = "limit", required = false) Integer limitRaw,
            @org.springframework.web.bind.annotation.RequestParam(name = "offset", required = false) Integer offsetRaw) {
        var ca = omsConfig.getCorporateAction();
        int def = ca.getListDefaultLimit();
        int max = ca.getListMaxLimit();
        int lim = limitRaw == null ? def : Math.min(Math.max(1, limitRaw), max);
        int off = offsetRaw == null ? 0 : Math.max(0, offsetRaw);
        List<CorporateActionEventRepository.ListRow> rows = repository.listPage(lim, off);
        return ResponseEntity.ok(Map.of("items", rows, "limit", lim, "offset", off));
    }

    @GetMapping("/{id}")
    public ResponseEntity<Object> get(@PathVariable long id) {
        return repository
                .findById(id)
                .<ResponseEntity<Object>>map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "not_found")));
    }

    @PostMapping
    public ResponseEntity<?> ingest(@RequestBody IngestRequest body) {
        var ca = omsConfig.getCorporateAction();
        if (body.instrumentSymbol() == null
                || body.instrumentSymbol().isBlank()
                || body.instrumentSymbol().length() > ca.getIngestInstrumentSymbolMaxLength()) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(Map.of("error", "invalid_instrument_symbol"));
        }
        if (body.actionType() == null
                || body.actionType().isBlank()
                || body.actionType().length() > ca.getIngestActionTypeMaxLength()) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", "invalid_action_type"));
        }
        LocalDate effective;
        try {
            effective = LocalDate.parse(body.effectiveDate());
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", "invalid_effective_date"));
        }
        JsonNode payload =
                body.payloadJson() == null
                        ? objectMapper.createObjectNode()
                        : objectMapper.valueToTree(body.payloadJson());
        String payloadText;
        try {
            payloadText = objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", "invalid_payload_json"));
        }
        if (payloadText.length() > ca.getIngestPayloadJsonMaxChars()) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", "payload_json_too_large"));
        }
        long id = repository.insert(body.instrumentSymbol().trim(), body.actionType().trim(), effective, payloadText);
        return ResponseEntity.status(HttpStatus.CREATED).body(new IngestResponse(id));
    }
}
