package com.balh.oms.ingress;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.persistence.CustomerInvestReadRepository;
import com.balh.oms.persistence.PositionsRepository;
import java.util.List;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Customer invest read models for the BFF (gap plan §5.4 cash availability, §5.13
 * positions + settlement events). Secured by {@link ApiKeyFilter}.
 */
@RestController
@RequestMapping("/internal/v1/invest")
public class InvestReadController {

    private static final int DEFAULT_SETTLEMENT_EVENTS_LIMIT = 20;
    private static final int MAX_SETTLEMENT_EVENTS_LIMIT = 100;

    private final CustomerInvestReadRepository investRead;
    private final PositionsRepository positions;
    private final OmsConfig config;

    public InvestReadController(
            CustomerInvestReadRepository investRead, PositionsRepository positions, OmsConfig config) {
        this.investRead = investRead;
        this.positions = positions;
        this.config = config;
    }

    public record CashAvailabilityResponse(
            String currency,
            String pendingSettlementDebits,
            String pendingSettlementCredits,
            String unsettledSaleProceeds) {}

    public record SettlementEventResponse(
            long executionId,
            String orderId,
            String instrumentSymbol,
            String side,
            String quantity,
            String price,
            String settlementStatus,
            String tradeDate,
            String expectedSettlementDate,
            String venueTs) {}

    public record SettlementEventsListResponse(List<SettlementEventResponse> items) {}

    public record InvestPositionResponse(
            String instrumentSymbol,
            String custodyAccountId,
            String quantityTotal,
            String quantitySettled,
            String quantityPendingBuySettle,
            String quantityPendingSellSettle,
            String currency,
            String updatedAt,
            String averageFillPrice,
            String investedAmount) {}

    public record InvestPositionsListResponse(List<InvestPositionResponse> items) {}

    /**
     * Open {@code TRADE} notionals not yet {@code settled}/{@code failed}. The BFF
     * combines these with Ledger balances for tradeable vs withdrawable cash.
     */
    @GetMapping("/cash-availability")
    public ResponseEntity<CashAvailabilityResponse> cashAvailability(
            @RequestParam("accountId") UUID accountId,
            @RequestParam(name = "currency", required = false) String currency) {
        var amounts = investRead.sumOpenTradeSettlementAmounts(accountId);
        String ccy =
                currency == null || currency.isBlank()
                        ? config.getSettlement().getDefaultCashCurrency()
                        : currency.trim().toUpperCase();
        var sell = amounts.pendingSellCredits() == null ? java.math.BigDecimal.ZERO : amounts.pendingSellCredits();
        var buy = amounts.pendingBuyDebits() == null ? java.math.BigDecimal.ZERO : amounts.pendingBuyDebits();
        return ResponseEntity.ok(
                new CashAvailabilityResponse(
                        ccy,
                        buy.toPlainString(),
                        sell.toPlainString(),
                        sell.toPlainString()));
    }

    /** Recent {@code TRADE} executions for settlement status chips / activity feed. */
    @GetMapping("/settlement-events")
    public ResponseEntity<SettlementEventsListResponse> settlementEvents(
            @RequestParam("accountId") UUID accountId,
            @RequestParam(name = "limit", required = false) Integer limit) {
        int lim = limit == null ? DEFAULT_SETTLEMENT_EVENTS_LIMIT : limit;
        lim = Math.min(Math.max(lim, 1), MAX_SETTLEMENT_EVENTS_LIMIT);
        var rows = investRead.listSettlementEvents(accountId, lim);
        var items =
                rows.stream()
                        .map(
                                r ->
                                        new SettlementEventResponse(
                                                r.executionId(),
                                                r.orderId() == null ? null : r.orderId().toString(),
                                                r.instrumentSymbol(),
                                                r.side(),
                                                plain(r.quantity()),
                                                plain(r.price()),
                                                r.settlementStatus(),
                                                r.tradeDate() == null ? null : r.tradeDate().toString(),
                                                r.expectedSettlementDate() == null
                                                        ? null
                                                        : r.expectedSettlementDate().toString(),
                                                r.venueTs() == null ? null : r.venueTs().toString()))
                        .toList();
        return ResponseEntity.ok(new SettlementEventsListResponse(items));
    }

    /**
     * Positions with settled / pending buy / pending sell quantities (alias of
     * {@code GET /internal/v1/positions} under the invest namespace for BFF §5.13).
     */
    @GetMapping("/positions")
    public ResponseEntity<InvestPositionsListResponse> positions(@RequestParam("accountId") UUID accountId) {
        var rows = positions.findByAccountId(accountId);
        var items =
                rows.stream()
                        .map(
                                r ->
                                        new InvestPositionResponse(
                                                r.instrumentSymbol(),
                                                r.custodyAccountId() == null ? null : r.custodyAccountId().toString(),
                                                plain(r.quantityTotal()),
                                                plain(r.quantitySettled()),
                                                plain(r.quantityPendingBuySettle()),
                                                plain(r.quantityPendingSellSettle()),
                                                r.currency(),
                                                r.updatedAt() == null ? null : r.updatedAt().toString(),
                                                plain(r.averageFillPrice()),
                                                plain(r.investedAmount())))
                        .toList();
        return ResponseEntity.ok(new InvestPositionsListResponse(items));
    }

    private static String plain(java.math.BigDecimal v) {
        return v == null ? null : v.toPlainString();
    }
}
