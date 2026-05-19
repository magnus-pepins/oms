package com.balh.oms.ingress;

import com.balh.oms.config.OmsProfiles;
import com.balh.oms.persistence.OrderFeeSnapshot;
import com.balh.oms.persistence.OrderFeeSnapshotRepository;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;
import java.math.BigDecimal;
import java.util.UUID;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Endpoint the customer-frontend BFF calls after a successful OMS accept to pin
 * the tier-resolved commission (see {@code lib/server/resolveStockFee.ts}) for
 * use at settlement-fee-leg time. See V40 migration file header for the design
 * rationale (avoids touching the Aeron wire format).
 *
 * <p>Profiled the same way as {@link OrdersController} so the projector / FIX
 * egress nodes don't bind it.
 */
@RestController
@RequestMapping("/internal/v1/order-fee-snapshots")
@Profile(OmsProfiles.ORDER_ACCEPT_PROFILE)
public class OrderFeeSnapshotsController {

    public record CreateOrderFeeSnapshotRequest(
            @NotNull UUID orderId,
            @NotNull @PositiveOrZero BigDecimal feeAmount,
            @NotBlank String feeCurrency,
            @NotBlank String feeBalanceIndicator,
            @NotBlank String feeTier,
            @NotBlank String feeSource,
            UUID feeScheduleId,
            UUID userFeeOverrideId,
            // V41 cross-currency pinning — all null when the customer pays in the
            // trade currency (the common case for the demo). When cashCurrency
            // is set and differs from feeCurrency / tradeCurrency, settlement
            // routes the cash leg base/quote via @FX-Suspense-<ccy>.
            String cashCurrency,
            BigDecimal cashAmount,
            BigDecimal fxRate) {}

    public record CreateOrderFeeSnapshotResponse(boolean created) {}

    private final OrderFeeSnapshotRepository repo;

    public OrderFeeSnapshotsController(OrderFeeSnapshotRepository repo) {
        this.repo = repo;
    }

    @PostMapping
    public ResponseEntity<CreateOrderFeeSnapshotResponse> create(
            @Valid @RequestBody CreateOrderFeeSnapshotRequest req) {
        OrderFeeSnapshot snap = new OrderFeeSnapshot(
                req.orderId(),
                req.feeAmount(),
                req.feeCurrency(),
                req.feeBalanceIndicator(),
                req.feeTier(),
                req.feeSource(),
                req.feeScheduleId(),
                req.userFeeOverrideId(),
                req.cashCurrency(),
                req.cashAmount(),
                req.fxRate(),
                null);
        boolean created = repo.insertIgnoreOnConflict(snap);
        // 201 on first insert, 200 on idempotent duplicate — same contract as OrdersController.
        return ResponseEntity.status(created ? HttpStatus.CREATED : HttpStatus.OK)
                .body(new CreateOrderFeeSnapshotResponse(created));
    }
}
