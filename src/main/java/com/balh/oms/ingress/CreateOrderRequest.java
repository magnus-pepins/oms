package com.balh.oms.ingress;

import com.balh.oms.domain.Side;
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
        @Size(max = 128) String ledgerBalanceId
) {}
