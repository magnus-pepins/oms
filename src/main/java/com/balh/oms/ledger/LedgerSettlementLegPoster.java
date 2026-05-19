package com.balh.oms.ledger;

import com.balh.oms.settlement.LedgerSettlementOutboxRepository;
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
 * (deterministic {@code settlement-<outboxId>-<leg>}); Ledger does not enforce
 * uniqueness today, so retries after partial failure may dupe — accepted for
 * demo, deferred follow-up to add a guard via pre-check.
 */
public final class LedgerSettlementLegPoster implements LedgerSettlementPostingClient {

    private static final Logger log = LoggerFactory.getLogger(LedgerSettlementLegPoster.class);

    private static final String LEDGER_KEY_HEADER = "X-Ledger-Key";

    private final RestClient http;
    private final String apiKey;
    private final ObjectMapper objectMapper;

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
        String customerBalance = "inv-" + accountId + "-" + cashCurrency;
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
        String customerBalance = "inv-" + accountId + "-" + cashCurrency;
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
        String customerBalance = "inv-" + accountId + "-" + feeCurrency;
        String feeBalance = p.has("feeBalanceIndicator") && p.get("feeBalanceIndicator").isTextual()
                ? p.get("feeBalanceIndicator").asText()
                : "@Fees-" + feeCurrency;

        Map<String, Object> body = legBody(
                customerBalance, feeBalance, feeAmount, feeCurrency,
                "settlement-" + outboxId + "-fee",
                "Settlement " + side + " commission execution=" + p.get("executionId").asText());
        addLegMetaData(body, p, "fee");
        postLeg(body, outboxId, "fee");
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
        body.put("metaData", meta);
    }

    private void postLeg(Map<String, Object> body, long outboxId, String legLabel)
            throws LedgerSettlementPostingException {
        try {
            String resp = http.post()
                    .uri("/transactions")
                    .contentType(MediaType.APPLICATION_JSON)
                    .header(LEDGER_KEY_HEADER, apiKey)
                    .body(body)
                    .retrieve()
                    .body(String.class);
            JsonNode root = objectMapper.readTree(resp == null ? "{}" : resp);
            String txnId = root.has("transactionId") ? root.get("transactionId").asText()
                    : root.has("transaction_id") ? root.get("transaction_id").asText()
                    : root.has("id") ? root.get("id").asText() : null;
            if (txnId == null || txnId.isBlank()) {
                throw new LedgerSettlementPostingException(
                        "ledger settlement leg=" + legLabel + " missing transactionId; body=" + resp);
            }
            log.info("ledger settlement leg posted outboxId={} leg={} txn={}", outboxId, legLabel, txnId);
        } catch (RestClientResponseException e) {
            String b = e.getResponseBodyAsString();
            throw new LedgerSettlementPostingException(
                    "ledger settlement leg=" + legLabel + " HTTP " + e.getStatusCode().value() + ": "
                            + b.substring(0, Math.min(300, b.length())));
        } catch (LedgerSettlementPostingException e) {
            throw e;
        } catch (Exception e) {
            throw new LedgerSettlementPostingException(
                    "ledger settlement leg=" + legLabel + " unexpected: " + e.getMessage(), e);
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
}
