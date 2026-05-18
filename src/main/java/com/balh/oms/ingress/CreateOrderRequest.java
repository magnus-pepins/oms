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
        /**
         * For {@code orderType=LIMIT} this is the strict limit price (required, must be {@code > 0}).
         * For {@code orderType=MARKET} this is an <em>optional reference / cap price</em> the
         * BFF can supply (e.g. live ask × slippage cap) so OMS can size the BUY inflight hold
         * and FIX egress can emit a non-zero {@code Price} tag — letting the venue (or the
         * simulator) fill the MARKET at a realistic price rather than a placeholder. {@code null}
         * keeps the legacy "MARKET with no price hint" behaviour (no inflight hold).
         */
        BigDecimal limitPrice,
        @NotBlank @Size(max = 32) String timeInForce,
        /**
         * {@code MARKET} or {@code LIMIT}. Optional for back-compat: when absent, OMS infers
         * {@code MARKET} when {@link #limitPrice} is {@code null} and {@code LIMIT} otherwise
         * (the legacy single-field semantic). New callers SHOULD send this explicitly so a
         * MARKET-with-reference-price (BUY with synthesised cap) is not mis-classified.
         */
        @Size(max = 16) String orderType,
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
        orderType = blankToNull(orderType);
    }

    /**
     * Back-compat constructor that omits {@link #orderType}. Pre-Wed-demo callers (a single
     * unit-test today) keep working; {@link #resolvedOrderType()} then falls back to the
     * legacy "{@code MARKET} unless {@code limitPrice} is set" inference. New callers MUST
     * use the canonical constructor and set {@code orderType} explicitly.
     */
    public CreateOrderRequest(
            UUID accountId,
            String clientIdempotencyKey,
            Side side,
            String instrumentSymbol,
            BigDecimal quantity,
            BigDecimal limitPrice,
            String timeInForce,
            String ledgerBalanceId,
            String ledgerIdentityId) {
        this(accountId, clientIdempotencyKey, side, instrumentSymbol, quantity, limitPrice,
                timeInForce, /* orderType = */ null, ledgerBalanceId, ledgerIdentityId);
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

    /**
     * Canonical resolution of the requested order type, encapsulating the back-compat rule:
     * if the caller did not pass {@code orderType} explicitly, infer {@code LIMIT} when
     * {@link #limitPrice} is set and {@code MARKET} otherwise. New callers (the customer-FE
     * BFF as of Wed-demo) MUST pass {@code orderType} so a MARKET-with-reference-price is
     * classified correctly; the inference branch exists only for older internal callers and
     * a small set of pre-existing integration tests.
     */
    public String resolvedOrderType() {
        if (orderType != null) {
            return orderType.trim().toUpperCase(java.util.Locale.ROOT);
        }
        return limitPrice == null ? "MARKET" : "LIMIT";
    }

    private static String blankToNull(String raw) {
        if (raw == null) {
            return null;
        }
        String t = raw.trim();
        return t.isEmpty() ? null : t;
    }
}
