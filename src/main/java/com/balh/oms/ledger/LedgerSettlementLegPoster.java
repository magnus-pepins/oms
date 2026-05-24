package com.balh.oms.ledger;

import com.balh.oms.settlement.LedgerSettlementOutboxRepository;
import com.balh.oms.settlement.SettlementFailPenaltyBookingService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Multi-leg implementation of {@link LedgerSettlementPostingClient}.
 *
 * <p>Each {@code ledger_settlement_outbox} row maps to <strong>one</strong> Ledger
 * {@code POST /transactions}. Splitting cash and fee into independent rows
 * (V39 added {@code leg_kind}) means a fee failure does not block the cash
 * leg and the reconciler retries each row independently.
 *
 * <p>Leg routing:
 * <ul>
 *   <li>{@code cash} (BUY): {@code inv-<accountId>-<cashCcy>} → {@code @Nostro-<tradeCcy>-Bank}
 *       for {@code qty * price}. Phase 1 assumes {@code cashCcy == tradeCcy}.
 *   <li>{@code cash} (SELL): the reverse.
 *   <li>{@code cash-base} / {@code cash-quote} (Phase 2): two single-currency legs via
 *       {@code @FX-Suspense-<ccy>} mirroring {@link com.balh.oms.fx.FxHedgeService}.
 *   <li>{@code fee}: {@code inv-<accountId>-<feeCcy>} → {@code feeBalanceIndicator}
 *       (typically {@code @Fees-<feeCcy>}) for the commission amount.
 * </ul>
 *
 * <p>Idempotency uses the Ledger transaction {@code reference} field
 * (deterministic {@code settlement-<outboxId>-<leg>}). Before POST the poster
 * checks {@code GET /transactions?reference=…}; duplicate POSTs that return
 * {@code 409 CONFLICT} are treated as success when the reference already exists.
 */
public final class LedgerSettlementLegPoster implements LedgerSettlementPostingClient {

    private static final Logger log = LoggerFactory.getLogger(LedgerSettlementLegPoster.class);

    private static final String AUTHORIZATION_HEADER = "Authorization";
    private static final String BEARER_PREFIX = "Bearer ";

    /** Legacy stock-fee snapshots used bare {@code @Platform-Revenue} for USD (Bug H). */
    private static final String LEGACY_PLATFORM_REVENUE_INDICATOR = "@Platform-Revenue";

    private final RestClient http;
    private final String apiKey;
    private final ObjectMapper objectMapper;

    /**
     * Cache of {@code "inv-<accountId>-<ccy>" -> balanceId} resolutions. Customer balances
     * are created once per (account, currency) and never renamed, so caching the indicator
     * lookup avoids a Ledger {@code GET /balances?indicator=…} round-trip on every retry of
     * the same execution row. Bank {@code @Nostro-…} / {@code @Fees-…} indicators are
     * resolved server-side by Ledger's own {@code IndicatorResolver} (any string starting
     * with {@code @}); only the {@code inv-…} pattern needs this client-side resolution.
     */
    private final ConcurrentHashMap<String, String> customerBalanceIdCache = new ConcurrentHashMap<>();

    public LedgerSettlementLegPoster(RestClient http, String apiKey, ObjectMapper objectMapper) {
        if (apiKey == null || apiKey.isBlank()) {
            throw new IllegalStateException("oms.ledger.api-key is required for settlement leg posting");
        }
        this.http = http;
        this.apiKey = apiKey;
        this.objectMapper = objectMapper;
    }

    @Override
    public void postSettlementOutbox(
            long outboxId, long executionId, String toSettlementStatus, String legKind, String payloadJson)
            throws LedgerSettlementPostingException {
        JsonNode p;
        try {
            p = objectMapper.readTree(payloadJson == null || payloadJson.isBlank() ? "{}" : payloadJson);
        } catch (JsonProcessingException e) {
            throw new LedgerSettlementPostingException("invalid settlement outbox payload_json", e);
        }
        if (legKind == null || legKind.isBlank()) {
            throw new LedgerSettlementPostingException("legKind required");
        }

        switch (legKind) {
            case LedgerSettlementOutboxRepository.LEG_CASH -> postCashSingleCurrency(outboxId, p);
            case LedgerSettlementOutboxRepository.LEG_CASH_BASE -> postCashCrossCurrency(outboxId, p, true);
            case LedgerSettlementOutboxRepository.LEG_CASH_QUOTE -> postCashCrossCurrency(outboxId, p, false);
            case LedgerSettlementOutboxRepository.LEG_FEE -> postFee(outboxId, p);
            case LedgerSettlementOutboxRepository.LEG_PENALTY -> postPenalty(outboxId, p);
            default -> throw new LedgerSettlementPostingException("unknown legKind: " + legKind);
        }
    }

    /** BUY: customer cash → bank nostro for notional. SELL: reverse. Single Ledger txn (rate=1). */
    private void postCashSingleCurrency(long outboxId, JsonNode p) throws LedgerSettlementPostingException {
        String accountId = required(p, "accountId");
        String side = required(p, "side");
        String cashCurrency = required(p, "cashCurrency");
        String tradeCurrency = required(p, "tradeCurrency");
        if (!cashCurrency.equals(tradeCurrency)) {
            throw new LedgerSettlementPostingException(
                    "cash leg expects single-currency (cashCurrency==tradeCurrency); use cash-base/cash-quote "
                            + "for cross-currency (got cash=" + cashCurrency + " trade=" + tradeCurrency + ")");
        }
        BigDecimal notional = bigDecimal(p, "notional");
        String customerIndicator = "inv-" + accountId + "-" + cashCurrency;
        String customerBalance = resolveCustomerBalanceId(customerIndicator, cashCurrency);
        String nostroBalance = "@Nostro-" + tradeCurrency + "-Bank";

        String src, dst;
        if ("BUY".equalsIgnoreCase(side)) {
            src = customerBalance;
            dst = nostroBalance;
        } else if ("SELL".equalsIgnoreCase(side)) {
            src = nostroBalance;
            dst = customerBalance;
        } else {
            throw new LedgerSettlementPostingException("unknown side: " + side);
        }

        Map<String, Object> body = legBody(
                src, dst, notional, tradeCurrency,
                "settlement-" + outboxId + "-cash",
                "Settlement " + side + " cash leg execution=" + p.get("executionId").asText());
        addLegMetaData(body, p, "cash");
        postLeg(body, outboxId, "cash");
    }

    /**
     * Phase 2 cross-currency cash leg. {@code isBase=true} debits customer in their cash currency to
     * the FX suspense balance in that same currency; {@code isBase=false} credits the bank nostro
     * in the trade currency from the FX suspense balance in that same currency. Both legs use
     * {@code rate=1} per Ledger's single-currency model (mirrors {@link com.balh.oms.fx.FxHedgeService}).
     */
    private void postCashCrossCurrency(long outboxId, JsonNode p, boolean isBase)
            throws LedgerSettlementPostingException {
        String accountId = required(p, "accountId");
        String side = required(p, "side");
        String cashCurrency = required(p, "cashCurrency");
        String tradeCurrency = required(p, "tradeCurrency");
        String customerIndicator = "inv-" + accountId + "-" + cashCurrency;
        String customerBalance = resolveCustomerBalanceId(customerIndicator, cashCurrency);
        String fxSuspenseCash = "@FX-Suspense-" + cashCurrency;
        String fxSuspenseTrade = "@FX-Suspense-" + tradeCurrency;
        String nostro = "@Nostro-" + tradeCurrency + "-Bank";

        String reference;
        String src;
        String dst;
        BigDecimal amount;
        String ccy;
        if (isBase) {
            amount = bigDecimal(p, "cashAmount");
            ccy = cashCurrency;
            reference = "settlement-" + outboxId + "-cash-base";
            if ("BUY".equalsIgnoreCase(side)) {
                src = customerBalance;
                dst = fxSuspenseCash;
            } else {
                src = fxSuspenseCash;
                dst = customerBalance;
            }
        } else {
            amount = bigDecimal(p, "notional");
            ccy = tradeCurrency;
            reference = "settlement-" + outboxId + "-cash-quote";
            if ("BUY".equalsIgnoreCase(side)) {
                src = fxSuspenseTrade;
                dst = nostro;
            } else {
                src = nostro;
                dst = fxSuspenseTrade;
            }
        }
        Map<String, Object> body = legBody(
                src, dst, amount, ccy, reference,
                "Settlement " + side + " cash " + (isBase ? "base" : "quote") + " leg execution="
                        + p.get("executionId").asText());
        addLegMetaData(body, p, isBase ? "cash-base" : "cash-quote");
        postLeg(body, outboxId, isBase ? "cash-base" : "cash-quote");
    }

    /** Fee leg: customer cash → fee revenue balance, always single-currency. */
    private void postFee(long outboxId, JsonNode p) throws LedgerSettlementPostingException {
        String accountId = required(p, "accountId");
        String side = required(p, "side");
        String feeCurrency = required(p, "feeCurrency");
        BigDecimal feeAmount = bigDecimal(p, "feeAmount");
        if (feeAmount.signum() <= 0) {
            log.debug("fee leg skipped (amount<=0) outboxId={}", outboxId);
            return;
        }
        String customerIndicator = "inv-" + accountId + "-" + feeCurrency;
        String customerBalance = resolveCustomerBalanceId(customerIndicator, feeCurrency);
        String feeBalance = normalizeFeeBalanceIndicator(
                p.has("feeBalanceIndicator") && p.get("feeBalanceIndicator").isTextual()
                        ? p.get("feeBalanceIndicator").asText()
                        : "@Fees-" + feeCurrency,
                feeCurrency);

        Map<String, Object> body = legBody(
                customerBalance, feeBalance, feeAmount, feeCurrency,
                "settlement-" + outboxId + "-fee",
                "Settlement " + side + " commission execution=" + p.get("executionId").asText());
        addLegMetaData(body, p, "fee");
        postLeg(body, outboxId, "fee");
    }

    /** Penalty leg: bank nostro → settlement penalty P&L (v1 bank absorbs). */
    private void postPenalty(long outboxId, JsonNode p) throws LedgerSettlementPostingException {
        String currency = required(p, "penaltyCurrency");
        BigDecimal amount = bigDecimal(p, "penaltyAmount");
        if (amount.signum() <= 0) {
            log.debug("penalty leg skipped (amount<=0) outboxId={}", outboxId);
            return;
        }
        String penaltyIndicator =
                p.has("penaltyBalanceIndicator") && p.get("penaltyBalanceIndicator").isTextual()
                        ? p.get("penaltyBalanceIndicator").asText()
                        : SettlementFailPenaltyBookingService.PENALTY_BALANCE_PREFIX + currency;
        String nostro = "@Nostro-" + currency + "-Bank";
        Map<String, Object> body = legBody(
                nostro,
                penaltyIndicator,
                amount,
                currency,
                "settlement-" + outboxId + "-penalty",
                "Settlement fail penalty execution=" + p.get("executionId").asText());
        addLegMetaData(body, p, "penalty");
        postLeg(body, outboxId, "penalty");
    }

    private static Map<String, Object> legBody(
            String src, String dst, BigDecimal amount, String ccy, String reference, String description) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("source", src);
        body.put("destination", dst);
        body.put("amount", amount.doubleValue());
        body.put("currency", ccy);
        body.put("reference", reference);
        body.put("description", description);
        body.put("sync", true);
        // Customer cash and bank nostro/fee balances must be pre-funded for the demo
        // (see oms/scripts/seed-ledger-settlement.sh). Suspense balances are intentionally
        // overdraftable since the matched leg may post first and rebalance later.
        body.put("allowOverdraft", true);
        return body;
    }

    private void addLegMetaData(Map<String, Object> body, JsonNode payload, String legLabel) {
        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("kind", "settlement");
        meta.put("leg", legLabel);
        if (payload.has("executionId")) meta.put("executionId", payload.get("executionId").asText());
        if (payload.has("accountId")) meta.put("accountId", payload.get("accountId").asText());
        if (payload.has("instrumentSymbol")) meta.put("instrument", payload.get("instrumentSymbol").asText());
        if (payload.has("side")) meta.put("side", payload.get("side").asText());
        if (payload.has("quantity")) meta.put("quantity", payload.get("quantity").asText());
        if (payload.has("price")) meta.put("price", payload.get("price").asText());
        if (payload.has("taxWrapper")) meta.put("taxWrapper", payload.get("taxWrapper").asText());
        if (payload.has("iskDepositClass")) meta.put("iskDepositClass", payload.get("iskDepositClass").asText());
        body.put("metaData", meta);
    }

    private void postLeg(Map<String, Object> body, long outboxId, String legLabel)
            throws LedgerSettlementPostingException {
        Object refObj = body.get("reference");
        String reference = refObj instanceof String s ? s : null;
        if (reference != null && !reference.isBlank()) {
            String existingTxnId = findExistingTransactionIdByReference(reference);
            if (existingTxnId != null) {
                log.info(
                        "ledger settlement leg already posted outboxId={} leg={} reference={} txn={}",
                        outboxId,
                        legLabel,
                        reference,
                        existingTxnId);
                return;
            }
        }
        try {
            String resp = http.post()
                    .uri("/transactions")
                    .contentType(MediaType.APPLICATION_JSON)
                    .header(AUTHORIZATION_HEADER, BEARER_PREFIX + apiKey)
                    .body(body)
                    .retrieve()
                    .body(String.class);
            String txnId = parseTransactionIdFromBody(resp);
            if (txnId == null || txnId.isBlank()) {
                throw new LedgerSettlementPostingException(
                        "ledger settlement leg=" + legLabel + " missing transactionId; body=" + resp);
            }
            log.info("ledger settlement leg posted outboxId={} leg={} txn={}", outboxId, legLabel, txnId);
        } catch (RestClientResponseException e) {
            String b = e.getResponseBodyAsString();
            int status = e.getStatusCode().value();
            String snippet = b.substring(0, Math.min(300, b.length()));
            if (status == 409 && reference != null && !reference.isBlank()) {
                String existingTxnId = findExistingTransactionIdByReference(reference);
                if (existingTxnId != null) {
                    log.info(
                            "ledger settlement leg duplicate reference treated as success outboxId={} leg={} reference={} txn={}",
                            outboxId,
                            legLabel,
                            reference,
                            existingTxnId);
                    return;
                }
            }
            if (status == 404 && isIndicatorNotFoundBody(b)) {
                throw new LedgerSettlementPostingException(
                        LedgerSettlementPostingException.Reason.SKIPPED_INDICATOR_NOT_FOUND,
                        "ledger settlement leg=" + legLabel + " HTTP 404: " + snippet);
            }
            throw new LedgerSettlementPostingException(
                    "ledger settlement leg=" + legLabel + " HTTP " + status + ": " + snippet);
        } catch (LedgerSettlementPostingException e) {
            throw e;
        } catch (Exception e) {
            throw new LedgerSettlementPostingException(
                    "ledger settlement leg=" + legLabel + " unexpected: " + e.getMessage(), e);
        }
    }

    /**
     * Returns the Ledger transaction id when {@code reference} already exists, else {@code null}.
     * Used for idempotent settlement leg posting (§5.5).
     */
    private String findExistingTransactionIdByReference(String reference) {
        try {
            String resp = http.get()
                    .uri(b -> b.path("/transactions").queryParam("reference", reference).build())
                    .header(AUTHORIZATION_HEADER, BEARER_PREFIX + apiKey)
                    .retrieve()
                    .body(String.class);
            return parseTransactionIdFromBody(resp);
        } catch (RestClientResponseException e) {
            if (e.getStatusCode().value() == 404) {
                return null;
            }
            log.debug(
                    "ledger settlement reference lookup failed reference={} status={}",
                    reference,
                    e.getStatusCode().value());
            return null;
        } catch (Exception e) {
            log.debug("ledger settlement reference lookup unexpected reference={}: {}", reference, e.getMessage());
            return null;
        }
    }

    private String parseTransactionIdFromBody(String resp) {
        try {
            JsonNode root = objectMapper.readTree(resp == null ? "{}" : resp);
            if (root.has("transactionId")) {
                return root.get("transactionId").asText();
            }
            if (root.has("transaction_id")) {
                return root.get("transaction_id").asText();
            }
            if (root.has("id")) {
                return root.get("id").asText();
            }
        } catch (JsonProcessingException e) {
            log.debug("ledger settlement response parse failed: {}", e.getMessage());
        }
        return null;
    }

    /**
     * Resolves a customer-side balance indicator ({@code inv-<accountId>-<ccy>}) to the
     * concrete Ledger balanceId via {@code GET /balances?indicator=…}. Required because
     * Ledger's server-side {@code IndicatorResolver} only matches indicators that start
     * with {@code @} (see ledger-cluster {@code shim/IndicatorResolver.java}); customer
     * balances ride a different naming convention and would otherwise be passed through
     * as literal balanceIds, triggering {@code BALANCE_NOT_FOUND} on the cluster apply
     * path. Result cached because the binding is durable for the life of the account.
     */
    private String resolveCustomerBalanceId(String indicator, String currency)
            throws LedgerSettlementPostingException {
        String cached = customerBalanceIdCache.get(indicator);
        if (cached != null) {
            return cached;
        }
        try {
            String resp = http.get()
                    .uri(b -> b.path("/balances").queryParam("indicator", indicator).build())
                    .header(AUTHORIZATION_HEADER, BEARER_PREFIX + apiKey)
                    .retrieve()
                    .body(String.class);
            JsonNode arr = objectMapper.readTree(resp == null ? "[]" : resp);
            if (!arr.isArray() || arr.isEmpty()) {
                // Reason=SKIPPED_UNFUNDED_BALANCE so the reconciler can publish this as a
                // skip rather than a failure (see oms_ledger_settlement_outbox_skipped_total).
                // The outbox row stays unposted; once the customer is funded, the next
                // reconciler tick succeeds.
                throw new LedgerSettlementPostingException(
                        LedgerSettlementPostingException.Reason.SKIPPED_UNFUNDED_BALANCE,
                        "customer balance not found in Ledger: indicator=" + indicator
                                + " currency=" + currency
                                + " (customer must be funded before settlement can post)");
            }
            // Multiple matches are not expected (indicator + currency is unique in TS Ledger);
            // prefer the entry whose currency matches if Ledger ever returns several.
            String balanceId = null;
            for (JsonNode row : arr) {
                JsonNode rowCcy = row.get("currency");
                JsonNode rowId = row.get("balanceId");
                if (rowId == null || !rowId.isTextual()) continue;
                if (rowCcy != null && rowCcy.isTextual() && currency.equalsIgnoreCase(rowCcy.asText())) {
                    balanceId = rowId.asText();
                    break;
                }
                if (balanceId == null) balanceId = rowId.asText();
            }
            if (balanceId == null || balanceId.isBlank()) {
                throw new LedgerSettlementPostingException(
                        "ledger /balances?indicator=" + indicator + " returned rows without balanceId");
            }
            customerBalanceIdCache.put(indicator, balanceId);
            return balanceId;
        } catch (LedgerSettlementPostingException e) {
            throw e;
        } catch (RestClientResponseException e) {
            String b = e.getResponseBodyAsString();
            int status = e.getStatusCode().value();
            String snippet = b.substring(0, Math.min(300, b.length()));
            if (status == 404) {
                throw new LedgerSettlementPostingException(
                        LedgerSettlementPostingException.Reason.SKIPPED_INDICATOR_NOT_FOUND,
                        "ledger /balances?indicator lookup HTTP 404: " + snippet);
            }
            throw new LedgerSettlementPostingException(
                    "ledger /balances?indicator lookup HTTP " + status + ": " + snippet);
        } catch (Exception e) {
            throw new LedgerSettlementPostingException(
                    "ledger /balances?indicator lookup unexpected: " + e.getMessage(), e);
        }
    }

    private static String required(JsonNode p, String field) throws LedgerSettlementPostingException {
        if (!p.has(field) || p.get(field).isNull()) {
            throw new LedgerSettlementPostingException("settlement payload missing field: " + field);
        }
        String v = p.get(field).asText();
        if (v == null || v.isBlank()) {
            throw new LedgerSettlementPostingException("settlement payload empty field: " + field);
        }
        return v;
    }

    private static BigDecimal bigDecimal(JsonNode p, String field) throws LedgerSettlementPostingException {
        String s = required(p, field);
        try {
            return new BigDecimal(s);
        } catch (NumberFormatException e) {
            throw new LedgerSettlementPostingException(
                    "settlement payload field not a number: " + field + "=" + s);
        }
    }

    /**
     * Adopt {@code @Platform-Revenue-<CCY>} on the wire while historical outbox rows still
     * carry bare {@code @Platform-Revenue} (Bug H).
     */
    static String normalizeFeeBalanceIndicator(String indicator, String feeCurrency) {
        if (indicator == null || indicator.isBlank()) {
            return "@Fees-" + feeCurrency;
        }
        String trimmed = indicator.trim();
        if (LEGACY_PLATFORM_REVENUE_INDICATOR.equals(trimmed)
                && feeCurrency != null
                && !feeCurrency.isBlank()) {
            return LEGACY_PLATFORM_REVENUE_INDICATOR + "-" + feeCurrency.trim().toUpperCase();
        }
        return trimmed;
    }

    /**
     * Ledger shim 404 bodies for missing {@code @…} indicators include {@code NOT_FOUND} and
     * {@code indicator=} (see {@code IndicatorResolver} / {@code BalanceController}).
     */
    private static boolean isIndicatorNotFoundBody(String body) {
        if (body == null || body.isBlank()) {
            return false;
        }
        String lower = body.toLowerCase();
        return lower.contains("not_found") || lower.contains("balance not found");
    }
}
