package com.balh.oms.corporateaction;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.persistence.PositionsRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Locale;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Calculates entitlements and records position/cash impacts for Phase 1 automated corporate actions
 * (gap plan §5.9 Slice 14b).
 */
@Service
public class CorporateActionProcessingService {

    private static final Logger log = LoggerFactory.getLogger(CorporateActionProcessingService.class);

    public static final String ACTION_CASH_DIVIDEND = "CASH_DIVIDEND";
    public static final String ACTION_STOCK_SPLIT = "STOCK_SPLIT";

    private static final int MONEY_SCALE = 2;

    private final CorporateActionImpactRepository impacts;
    private final PositionsRepository positions;
    private final CorporateActionEventRepository events;
    private final ObjectMapper objectMapper;
    private final OmsConfig config;

    public CorporateActionProcessingService(
            CorporateActionImpactRepository impacts,
            PositionsRepository positions,
            CorporateActionEventRepository events,
            ObjectMapper objectMapper,
            OmsConfig config) {
        this.impacts = impacts;
        this.positions = positions;
        this.events = events;
        this.objectMapper = objectMapper;
        this.config = config;
    }

    public void process(CorporateActionEventRepository.ProcessingRow row) {
        String action = row.actionType() == null ? "" : row.actionType().trim().toUpperCase(Locale.ROOT);
        UUID custody = UUID.fromString(config.getSettlement().getDefaultCustodyAccountId());
        JsonNode payload = parsePayload(row.payloadJson());
        switch (action) {
            case ACTION_CASH_DIVIDEND -> processCashDividend(row, custody, payload);
            case ACTION_STOCK_SPLIT -> processStockSplit(row, custody, payload);
            default -> throw new UnsupportedCorporateActionException("unsupported actionType: " + action);
        }
    }

    private void processCashDividend(
            CorporateActionEventRepository.ProcessingRow row, UUID custody, JsonNode payload) {
        BigDecimal dividendPerShare = decimalField(payload, "dividendPerShare");
        if (dividendPerShare == null || dividendPerShare.signum() <= 0) {
            throw new UnsupportedCorporateActionException("CASH_DIVIDEND missing dividendPerShare");
        }
        String currency = textField(payload, "currency");
        if (currency == null || currency.isBlank()) {
            currency = "USD";
        }
        String symbol = row.instrumentSymbol().trim().toUpperCase(Locale.ROOT);
        var holders = positions.listSettledHolders(symbol, custody);
        for (PositionsRepository.SettledHolder holder : holders) {
            BigDecimal gross =
                    holder.quantitySettled().multiply(dividendPerShare).setScale(MONEY_SCALE, RoundingMode.HALF_UP);
            impacts.insertEntitlement(
                    row.id(),
                    holder.accountId(),
                    symbol,
                    holder.quantitySettled(),
                    null,
                    gross,
                    currency);
            impacts.insertCashImpact(
                    row.id(), holder.accountId(), gross, gross, currency, row.payableDate());
        }
        log.info(
                "corporate_action CASH_DIVIDEND processed id={} symbol={} holders={}",
                row.id(),
                symbol,
                holders.size());
    }

    private void processStockSplit(
            CorporateActionEventRepository.ProcessingRow row, UUID custody, JsonNode payload) {
        BigDecimal ratio = splitRatio(payload);
        if (ratio == null || ratio.compareTo(BigDecimal.ZERO) <= 0) {
            throw new UnsupportedCorporateActionException("STOCK_SPLIT missing split ratio");
        }
        String symbol = row.instrumentSymbol().trim().toUpperCase(Locale.ROOT);
        for (PositionsRepository.SettledHolder holder : positions.listSettledHolders(symbol, custody)) {
            BigDecimal before = holder.quantitySettled();
            BigDecimal after = before.multiply(ratio);
            positions.applyCorporateActionSplit(holder.accountId(), symbol, custody, ratio);
            impacts.insertEntitlement(row.id(), holder.accountId(), symbol, before, after, null, null);
            impacts.insertPositionImpact(row.id(), holder.accountId(), symbol, before, after);
        }
        log.info("corporate_action STOCK_SPLIT processed id={} symbol={} ratio={}", row.id(), symbol, ratio);
    }

    static BigDecimal splitRatio(JsonNode payload) {
        BigDecimal newShares = decimalField(payload, "newShares");
        BigDecimal oldShares = decimalField(payload, "oldShares");
        if (newShares != null && oldShares != null && oldShares.signum() > 0) {
            return newShares.divide(oldShares, 10, RoundingMode.HALF_UP);
        }
        return decimalField(payload, "splitRatio");
    }

    private JsonNode parsePayload(String json) {
        try {
            return objectMapper.readTree(json == null || json.isBlank() ? "{}" : json);
        } catch (Exception e) {
            throw new UnsupportedCorporateActionException("invalid payload_json: " + e.getMessage());
        }
    }

    private static BigDecimal decimalField(JsonNode node, String field) {
        if (node == null || !node.has(field) || node.get(field).isNull()) {
            return null;
        }
        return new BigDecimal(node.get(field).asText());
    }

    private static String textField(JsonNode node, String field) {
        if (node == null || !node.has(field) || node.get(field).isNull()) {
            return null;
        }
        return node.get(field).asText();
    }

    public static final class UnsupportedCorporateActionException extends RuntimeException {
        public UnsupportedCorporateActionException(String message) {
            super(message);
        }
    }
}
