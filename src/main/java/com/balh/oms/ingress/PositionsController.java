package com.balh.oms.ingress;

import com.balh.oms.persistence.PositionsRepository;
import java.util.List;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Read-side positions endpoint for downstream UIs (customer-frontend "My positions",
 * trading-desk per-customer view, beard-admin operator drilldown). Secured by
 * {@link com.balh.oms.security.ApiKeyFilter} like other {@code /internal/v1/**} routes.
 *
 * <p>Wed-demo (2026-05-18 follow-up to MARKET-fill diagnosis): before this controller
 * the customer-frontend BFF read positions exclusively from Supabase
 * {@code customer_order_fills} (Alpaca path) / {@code investment_trades} (manual
 * portfolios). For users on {@code broker_backend='oms'} both tables are empty so
 * "My positions" rendered as zero rows even when OMS Postgres had recorded fills.
 * This endpoint closes that gap without coupling the BFF to OMS Postgres directly.
 */
@RestController
@RequestMapping("/internal/v1/positions")
public class PositionsController {

    private final PositionsRepository positionsRepository;

    public PositionsController(PositionsRepository positionsRepository) {
        this.positionsRepository = positionsRepository;
    }

    public record PositionResponse(
            String instrumentSymbol,
            String custodyAccountId,
            String quantityTotal,
            String quantitySettled,
            String quantityPendingBuySettle,
            String quantityPendingSellSettle,
            String avgCostAmount,
            String currency,
            String updatedAt) {}

    public record PositionsListResponse(List<PositionResponse> items) {}

    @GetMapping
    public ResponseEntity<PositionsListResponse> listByAccount(@RequestParam("accountId") UUID accountId) {
        var rows = positionsRepository.findByAccountId(accountId);
        var items =
                rows.stream()
                        .map(
                                r ->
                                        new PositionResponse(
                                                r.instrumentSymbol(),
                                                r.custodyAccountId() == null ? null : r.custodyAccountId().toString(),
                                                r.quantityTotal() == null ? null : r.quantityTotal().toPlainString(),
                                                r.quantitySettled() == null ? null : r.quantitySettled().toPlainString(),
                                                r.quantityPendingBuySettle() == null
                                                        ? null
                                                        : r.quantityPendingBuySettle().toPlainString(),
                                                r.quantityPendingSellSettle() == null
                                                        ? null
                                                        : r.quantityPendingSellSettle().toPlainString(),
                                                r.avgCostAmount() == null ? null : r.avgCostAmount().toPlainString(),
                                                r.currency(),
                                                r.updatedAt() == null ? null : r.updatedAt().toString()))
                        .toList();
        return ResponseEntity.ok(new PositionsListResponse(items));
    }
}
