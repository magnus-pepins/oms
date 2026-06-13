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
        @Size(max = 256) String ledgerIdentityId,
        /**
         * Optional FX quote id (§8.4 quote-lock flow). When non-null AND
         * {@code oms.fx.accept-use-quoter.enabled=true}, the OMS accept path
         * recalls the cached quote via {@code FxQuoteService.recall} and rejects
         * with {@code RISK_FX_QUOTE_EXPIRED} when the quote is missing or
         * expired. The BFF mints the id from {@code POST /internal/v1/fx/quote}
         * and forwards it on every cross-currency order. Single-currency orders
         * leave this null and the recall is skipped.
         */
        @Size(max = 64) String fxQuoteId,
        /**
         * Optional cash hold amount in <em>source-balance currency</em>
         * (§8.4 quote-lock flow). Required iff {@link #fxQuoteId} is set —
         * computed by the BFF as {@code (qty * limitPrice) / fxRate_locked}
         * so OMS does not need to know the FX pair or the rate side, only
         * how much to hold from the customer's source ledger balance. When
         * null, the inflight hold sizer falls back to
         * {@code BuyFundsRequirement.requiredBuyFunds(...)} (single-currency
         * legacy path). Must be {@code > 0} when present.
         */
        @Positive BigDecimal cashHoldAmount,
        /**
         * Optional generic portfolio attribution id (the {@code investment_portfolios.id} the
         * order belongs to). OMS treats it as an opaque string and threads it onto the cluster
         * admit wire → {@code orders.portfolio_id} for attribution; it is <strong>not</strong>
         * validated against a portfolio registry here (the BFF validates ownership before
         * forwarding). {@code null}/blank leaves the order unattributed. Generic across products
         * (equities + prediction markets) — see
         * {@code system-documentation/plans/generic-portfolio-order-attribution.md}.
         */
        @Size(max = 64) String portfolioId
) {
    public CreateOrderRequest {
        ledgerBalanceId = blankToNull(ledgerBalanceId);
        ledgerIdentityId = blankToNull(ledgerIdentityId);
        orderType = blankToNull(orderType);
        fxQuoteId = blankToNull(fxQuoteId);
        portfolioId = blankToNull(portfolioId);
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
                timeInForce, /* orderType = */ null, ledgerBalanceId, ledgerIdentityId,
                /* fxQuoteId = */ null, /* cashHoldAmount = */ null, /* portfolioId = */ null);
    }

    /**
     * Pre-§8 (no FX-quote lock) canonical constructor. Used by paths that
     * already plumb {@code orderType} but predate the quote-lock fields.
     * Defers to the full canonical constructor with both new fields nulled.
     */
    public CreateOrderRequest(
            UUID accountId,
            String clientIdempotencyKey,
            Side side,
            String instrumentSymbol,
            BigDecimal quantity,
            BigDecimal limitPrice,
            String timeInForce,
            String orderType,
            String ledgerBalanceId,
            String ledgerIdentityId) {
        this(accountId, clientIdempotencyKey, side, instrumentSymbol, quantity, limitPrice,
                timeInForce, orderType, ledgerBalanceId, ledgerIdentityId,
                /* fxQuoteId = */ null, /* cashHoldAmount = */ null, /* portfolioId = */ null);
    }

    /**
     * Pre-portfolio canonical constructor (orderType + FX quote-lock fields, no
     * {@code portfolioId}). Keeps callers built before generic portfolio attribution compiling;
     * defers to the full canonical constructor with {@code portfolioId} nulled.
     */
    public CreateOrderRequest(
            UUID accountId,
            String clientIdempotencyKey,
            Side side,
            String instrumentSymbol,
            BigDecimal quantity,
            BigDecimal limitPrice,
            String timeInForce,
            String orderType,
            String ledgerBalanceId,
            String ledgerIdentityId,
            String fxQuoteId,
            BigDecimal cashHoldAmount) {
        this(accountId, clientIdempotencyKey, side, instrumentSymbol, quantity, limitPrice,
                timeInForce, orderType, ledgerBalanceId, ledgerIdentityId,
                fxQuoteId, cashHoldAmount, /* portfolioId = */ null);
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
     * BUY orders bound to a Ledger balance must carry a positive {@link #limitPrice} (strict limit
     * or MARKET reference / cap) so buying-power and inflight holds can be sized.
     */
    @AssertTrue(message = "limitPrice must be positive when ledgerBalanceId is set on a BUY order")
    public boolean isBuyLedgerFundingShapeValid() {
        if (ledgerBalanceId == null || side != Side.BUY) {
            return true;
        }
        return limitPrice != null && limitPrice.signum() > 0;
    }

    /**
     * §8.4 quote-lock pairing: {@link #cashHoldAmount} only carries meaning
     * paired with a {@link #fxQuoteId} (so the recall can validate the rate
     * the BFF used to compute it). A {@code fxQuoteId} on its own is
     * tolerated for forward-compat (e.g. SELL-side flows that don't need a
     * BUY hold) — but a {@code cashHoldAmount} on its own is a
     * mis-configured BFF and we reject the shape at ingress.
     */
    @AssertTrue(message = "cashHoldAmount must be paired with fxQuoteId")
    public boolean isFxQuoteLockShapeValid() {
        return cashHoldAmount == null || fxQuoteId != null;
    }

    /**
     * Explicit LIMIT orders require a positive {@link #limitPrice}.
     */
    @AssertTrue(message = "limitPrice is required and must be positive for LIMIT orders")
    public boolean isLimitOrderPriceShapeValid() {
        if (!"LIMIT".equals(resolvedOrderType())) {
            return true;
        }
        return limitPrice != null && limitPrice.signum() > 0;
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
