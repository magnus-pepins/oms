package com.balh.oms.ingress;

import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Operator summary of {@code prediction_market_ledger_outbox} posting progress by settlement
 * currency (Beard Admin settlement console).
 */
@RestController
@RequestMapping("/internal/v1/prediction-market/admin/ledger-reconciliation")
public class PredictionMarketLedgerReconciliationController {

    private static final String SUMMARY_BY_CURRENCY =
            """
                    SELECT COALESCE(c.settlement_currency, 'UNKNOWN') AS settlement_currency,
                           COUNT(o.id) AS total_legs,
                           COUNT(o.id) FILTER (WHERE o.posted_at IS NOT NULL) AS posted_legs,
                           COUNT(o.id) FILTER (
                               WHERE o.posted_at IS NULL AND o.skipped_at IS NULL
                           ) AS pending_legs,
                           COUNT(o.id) FILTER (WHERE o.skipped_at IS NOT NULL) AS skipped_legs,
                           COUNT(o.id) FILTER (
                               WHERE o.posted_at IS NULL
                                 AND o.skipped_at IS NULL
                                 AND r.posting_paused = TRUE
                           ) AS paused_pending_legs,
                           COUNT(o.id) FILTER (
                               WHERE o.posted_at IS NULL
                                 AND o.skipped_at IS NULL
                                 AND r.posting_paused = FALSE
                                 AND r.dispute_until <= NOW()
                           ) AS eligible_pending_legs
                    FROM prediction_market_ledger_outbox o
                    JOIN venue_contract_resolution r ON r.id = o.resolution_id
                    LEFT JOIN prediction_market_contract c ON c.yes_symbol = r.contract_symbol
                    GROUP BY COALESCE(c.settlement_currency, 'UNKNOWN')
                    ORDER BY settlement_currency
                    """;

    public record CurrencyRow(
            String settlementCurrency,
            long totalLegs,
            long postedLegs,
            long pendingLegs,
            long skippedLegs,
            long pausedPendingLegs,
            long eligiblePendingLegs) {}

    public record SummaryResponse(List<CurrencyRow> byCurrency, long driftLegs) {}

    private final NamedParameterJdbcTemplate jdbc;

    public PredictionMarketLedgerReconciliationController(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @GetMapping
    public ResponseEntity<SummaryResponse> summary() {
        List<CurrencyRow> rows =
                jdbc.query(
                        SUMMARY_BY_CURRENCY,
                        (rs, rowNum) ->
                                new CurrencyRow(
                                        rs.getString("settlement_currency"),
                                        rs.getLong("total_legs"),
                                        rs.getLong("posted_legs"),
                                        rs.getLong("pending_legs"),
                                        rs.getLong("skipped_legs"),
                                        rs.getLong("paused_pending_legs"),
                                        rs.getLong("eligible_pending_legs")));
        long drift =
                rows.stream()
                        .mapToLong(r -> r.pendingLegs() > 0 && r.eligiblePendingLegs() > 0 ? r.eligiblePendingLegs() : 0)
                        .sum();
        return ResponseEntity.ok(new SummaryResponse(rows, drift));
    }
}
