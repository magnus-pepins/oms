package com.balh.oms.ingress;

import com.balh.oms.domain.Side;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

import java.math.BigDecimal;
import java.util.UUID;

public record CreateOrderRequest(
        @NotNull UUID accountId,
        @NotBlank @Size(max = 256) String clientIdempotencyKey,
        @NotNull Side side,
        @NotBlank @Size(max = 64) String instrumentSymbol,
        @NotNull @Positive BigDecimal quantity,
        BigDecimal limitPrice,
        @NotBlank @Size(max = 32) String timeInForce,
        /** Optional Ledger balance id (e.g. {@code balance_...}) for buying-power gate. */
        @Size(max = 128) String ledgerBalanceId,
        /**
         * Required when {@link #ledgerBalanceId} is set: Ledger {@code identity_id} that must
         * own that balance (OMS verifies via Ledger GET). BFF injects this after its own check.
         */
        @Size(max = 256) String ledgerIdentityId
) {
    public CreateOrderRequest {
        ledgerBalanceId = blankToNull(ledgerBalanceId);
        ledgerIdentityId = blankToNull(ledgerIdentityId);
    }

    @AssertTrue(message = "ledgerIdentityId is required when ledgerBalanceId is set and must be omitted otherwise")
    public boolean isLedgerBindingShapeValid() {
        boolean hasBal = ledgerBalanceId != null;
        boolean hasId = ledgerIdentityId != null;
        if (hasBal) {
            return hasId;
        }
        return !hasId;
    }

    private static String blankToNull(String raw) {
        if (raw == null) {
            return null;
        }
        String t = raw.trim();
        return t.isEmpty() ? null : t;
    }
}
