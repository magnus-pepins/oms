package com.balh.oms.ingress;

import com.balh.oms.settlement.SettlementOpsMetricsRepository;
import java.util.Map;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** JSON ops summary for Beard Admin settlement dashboard (§5.18). */
@RestController
@RequestMapping("/internal/v1/settlement/ops-metrics")
public class SettlementOpsSummaryController {

    public record Summary(
            Map<String, Integer> openBreaksByType,
            long maxOpenBreakAgeSeconds,
            int stuckOutboxRows,
            int openSettlementFails,
            int pendingCorporateActions,
            int positionReconciliationBreaks,
            int cashReconciliationBreaks,
            int lateSettlementExecutions,
            int lateBrokerFileBatches) {}

    private final SettlementOpsMetricsRepository metrics;

    public SettlementOpsSummaryController(SettlementOpsMetricsRepository metrics) {
        this.metrics = metrics;
    }

    @GetMapping("/summary")
    public ResponseEntity<Summary> summary() {
        return ResponseEntity.ok(
                new Summary(
                        metrics.countOpenBreaksByType(),
                        metrics.maxOpenBreakAgeSeconds(),
                        metrics.countStuckOutboxRows(3),
                        metrics.countOpenSettlementFails(),
                        metrics.countPendingCorporateActionEvents(),
                        metrics.countPositionReconciliationBreaks(),
                        metrics.countCashReconciliationBreaks(),
                        metrics.countLateSettlementExecutions(),
                        metrics.countLateBrokerFileBatches()));
    }
}
