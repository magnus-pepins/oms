package com.balh.oms.ingress;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.settlement.SettlementConfirmProcessor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Internal settlement controls (slice 6). Secured by {@link ApiKeyFilter} like other
 * {@code /internal/v1/**} routes.
 */
@RestController
@RequestMapping("/internal/v1/settlement")
public class SettlementController {

    public record BrokerConfirmIngestRequest(List<Long> executionIds) {}

    public record BrokerConfirmIngestResponse(int insertedRows) {}

    public record BrokerConfirmProcessResponse(int processedRows) {}

    public record SettlementStepResponse(String settlementStatus) {}

    private final SettlementConfirmProcessor processor;
    private final OmsConfig config;

    public SettlementController(SettlementConfirmProcessor processor, OmsConfig config) {
        this.processor = processor;
        this.config = config;
    }

    @PostMapping("/broker-confirms")
    public ResponseEntity<BrokerConfirmIngestResponse> ingestBrokerConfirms(
            @RequestBody BrokerConfirmIngestRequest body) {
        if (body == null || body.executionIds() == null) {
            return ResponseEntity.badRequest().build();
        }
        int max = config.getSettlement().getBrokerConfirmHttpMaxExecutionIds();
        if (body.executionIds().size() > max) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).build();
        }
        int n = processor.registerBrokerConfirms(body.executionIds());
        return ResponseEntity.ok(new BrokerConfirmIngestResponse(n));
    }

    @PostMapping("/process-pending")
    public ResponseEntity<BrokerConfirmProcessResponse> processPending(
            @RequestParam(name = "maxBatch", required = false) Integer maxBatch) {
        int cap = maxBatch == null
                ? config.getSettlement().getBrokerConfirmReconcilerBatchSize()
                : Math.min(maxBatch, config.getSettlement().getBrokerConfirmReconcilerBatchSize());
        int n = processor.processPendingBatch(cap);
        return ResponseEntity.ok(new BrokerConfirmProcessResponse(n));
    }

    @PostMapping("/executions/{executionId}/advance-one-step")
    public ResponseEntity<SettlementStepResponse> advanceOneStep(@PathVariable long executionId) {
        try {
            String next = processor.advanceOneSettlementStep(executionId);
            if (next == null) {
                return ResponseEntity.notFound().build();
            }
            return ResponseEntity.ok(new SettlementStepResponse(next));
        } catch (IllegalArgumentException ex) {
            return ResponseEntity.badRequest().build();
        }
    }
}
