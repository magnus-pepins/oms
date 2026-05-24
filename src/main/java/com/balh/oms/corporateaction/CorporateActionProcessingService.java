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
    public static final String ACTION_TENDER_OFFER = "TENDER_OFFER";
    public static final String ACTION_RIGHTS_ISSUE = "RIGHTS_ISSUE";
    public static final String ACTION_STOCK_DIVIDEND = "STOCK_DIVIDEND";
    public static final String ACTION_SYMBOL_CHANGE = "SYMBOL_CHANGE";
    public static final String ACTION_ISIN_CHANGE = "ISIN_CHANGE";

    private static final int MONEY_SCALE = 2;

    private final CorporateActionImpactRepository impacts;
    private final PositionsRepository positions;
    private final CorporateActionRecordDateSnapshotService recordDateSnapshots;
    private final CorporateActionElectionService elections;
    private final CorporateActionElectionRepository electionRepository;
    private final ObjectMapper objectMapper;
    private final OmsConfig config;

    public CorporateActionProcessingService(
            CorporateActionImpactRepository impacts,
            PositionsRepository positions,
            CorporateActionRecordDateSnapshotService recordDateSnapshots,
            CorporateActionElectionService elections,
            CorporateActionElectionRepository electionRepository,
            ObjectMapper objectMapper,
            OmsConfig config) {
        this.impacts = impacts;
        this.positions = positions;
        this.recordDateSnapshots = recordDateSnapshots;
        this.elections = elections;
        this.electionRepository = electionRepository;
        this.objectMapper = objectMapper;
        this.config = config;
    }

    public void process(CorporateActionEventRepository.ProcessingRow row) {
        String action = row.actionType() == null ? "" : row.actionType().trim().toUpperCase(Locale.ROOT);
        UUID custody = UUID.fromString(config.getSettlement().getDefaultCustodyAccountId());
        JsonNode payload = parsePayload(row.payloadJson());
        var holders = recordDateSnapshots.resolveHolders(row);
        if (VoluntaryCorporateActionTypes.requiresElection(action)) {
            elections.requireAllApproved(row.id(), holders);
        }
        switch (action) {
            case ACTION_CASH_DIVIDEND -> processCashDividend(row, holders, payload);
            case ACTION_STOCK_SPLIT -> processStockSplit(row, custody, holders, payload);
            case ACTION_TENDER_OFFER -> processTenderOffer(row, holders, payload);
            case ACTION_RIGHTS_ISSUE -> processRightsIssue(row, holders, payload);
            case ACTION_STOCK_DIVIDEND -> processStockDividend(row, custody, holders, payload);
            case ACTION_SYMBOL_CHANGE, ACTION_ISIN_CHANGE -> processSymbolChange(row, custody, holders, payload);
            default -> throw new UnsupportedCorporateActionException("unsupported actionType: " + action);
        }
    }

    private void processCashDividend(
            CorporateActionEventRepository.ProcessingRow row,
            java.util.List<CorporateActionRecordDateSnapshotService.ResolvedHolder> holders,
            JsonNode payload) {
        BigDecimal dividendPerShare = decimalField(payload, "dividendPerShare");
        if (dividendPerShare == null || dividendPerShare.signum() <= 0) {
            throw new UnsupportedCorporateActionException("CASH_DIVIDEND missing dividendPerShare");
        }
        String currency = textField(payload, "currency");
        if (currency == null || currency.isBlank()) {
            currency = "USD";
        }
        String symbol = row.instrumentSymbol().trim().toUpperCase(Locale.ROOT);
        for (CorporateActionRecordDateSnapshotService.ResolvedHolder holder : holders) {
            BigDecimal gross =
                    holder.quantitySettled().multiply(dividendPerShare).setScale(MONEY_SCALE, RoundingMode.HALF_UP);
            BigDecimal withholding = withholdingAmount(gross, payload);
            BigDecimal net = gross.subtract(withholding).setScale(MONEY_SCALE, RoundingMode.HALF_UP);
            impacts.insertEntitlement(
                    row.id(),
                    holder.accountId(),
                    symbol,
                    holder.quantitySettled(),
                    null,
                    gross,
                    currency);
            impacts.insertCashImpactWithWithholding(
                    row.id(), holder.accountId(), gross, net, withholding, currency, row.payableDate());
        }
        log.info(
                "corporate_action CASH_DIVIDEND processed id={} symbol={} holders={}",
                row.id(),
                symbol,
                holders.size());
    }

    private void processStockSplit(
            CorporateActionEventRepository.ProcessingRow row,
            UUID custody,
            java.util.List<CorporateActionRecordDateSnapshotService.ResolvedHolder> holders,
            JsonNode payload) {
        BigDecimal ratio = splitRatio(payload);
        if (ratio == null || ratio.compareTo(BigDecimal.ZERO) <= 0) {
            throw new UnsupportedCorporateActionException("STOCK_SPLIT missing split ratio");
        }
        String symbol = row.instrumentSymbol().trim().toUpperCase(Locale.ROOT);
        for (CorporateActionRecordDateSnapshotService.ResolvedHolder holder : holders) {
            BigDecimal before = holder.quantitySettled();
            BigDecimal after = before.multiply(ratio);
            positions.applyCorporateActionSplit(holder.accountId(), symbol, custody, ratio);
            impacts.insertEntitlement(row.id(), holder.accountId(), symbol, before, after, null, null);
            impacts.insertPositionImpact(row.id(), holder.accountId(), symbol, before, after);
        }
        log.info("corporate_action STOCK_SPLIT processed id={} symbol={} ratio={}", row.id(), symbol, ratio);
    }

    private void processTenderOffer(
            CorporateActionEventRepository.ProcessingRow row,
            java.util.List<CorporateActionRecordDateSnapshotService.ResolvedHolder> holders,
            JsonNode payload) {
        BigDecimal tenderPrice = decimalField(payload, "tenderPricePerShare");
        if (tenderPrice == null || tenderPrice.signum() <= 0) {
            tenderPrice = decimalField(payload, "offerPricePerShare");
        }
        if (tenderPrice == null || tenderPrice.signum() <= 0) {
            throw new UnsupportedCorporateActionException("TENDER_OFFER missing tenderPricePerShare");
        }
        String currency = textField(payload, "currency");
        if (currency == null || currency.isBlank()) {
            currency = "USD";
        }
        String symbol = row.instrumentSymbol().trim().toUpperCase(Locale.ROOT);
        int processed = 0;
        for (CorporateActionRecordDateSnapshotService.ResolvedHolder holder : holders) {
            String choice =
                    electionRepository
                            .findApprovedChoice(row.id(), holder.accountId())
                            .orElse("");
            if (CorporateActionElectionChoices.isDecline(choice)) {
                continue;
            }
            if (!CorporateActionElectionChoices.isParticipate(choice)) {
                throw new UnsupportedCorporateActionException(
                        "TENDER_OFFER holder missing participate election: " + holder.accountId());
            }
            BigDecimal gross =
                    holder.quantitySettled().multiply(tenderPrice).setScale(MONEY_SCALE, RoundingMode.HALF_UP);
            impacts.insertEntitlement(
                    row.id(), holder.accountId(), symbol, holder.quantitySettled(), null, gross, currency);
            impacts.insertCashImpactWithWithholding(
                    row.id(), holder.accountId(), gross, gross, null, currency, row.payableDate());
            processed++;
        }
        log.info("corporate_action TENDER_OFFER processed id={} symbol={} participants={}", row.id(), symbol, processed);
    }

    private void processRightsIssue(
            CorporateActionEventRepository.ProcessingRow row,
            java.util.List<CorporateActionRecordDateSnapshotService.ResolvedHolder> holders,
            JsonNode payload) {
        BigDecimal rightsPerShare = decimalField(payload, "rightsPerShare");
        if (rightsPerShare == null || rightsPerShare.signum() <= 0) {
            BigDecimal newRights = decimalField(payload, "newRights");
            BigDecimal oldShares = decimalField(payload, "oldShares");
            if (newRights != null && oldShares != null && oldShares.signum() > 0) {
                rightsPerShare = newRights.divide(oldShares, 10, RoundingMode.HALF_UP);
            }
        }
        if (rightsPerShare == null || rightsPerShare.signum() <= 0) {
            throw new UnsupportedCorporateActionException("RIGHTS_ISSUE missing rightsPerShare");
        }
        String symbol = row.instrumentSymbol().trim().toUpperCase(Locale.ROOT);
        String rightsSymbol = textField(payload, "rightsSymbol");
        if (rightsSymbol == null || rightsSymbol.isBlank()) {
            rightsSymbol = symbol + ".RT";
        } else {
            rightsSymbol = rightsSymbol.trim().toUpperCase(Locale.ROOT);
        }
        int processed = 0;
        for (CorporateActionRecordDateSnapshotService.ResolvedHolder holder : holders) {
            String choice =
                    electionRepository
                            .findApprovedChoice(row.id(), holder.accountId())
                            .orElse("");
            if (CorporateActionElectionChoices.isDecline(choice)) {
                continue;
            }
            if (!CorporateActionElectionChoices.isParticipate(choice)) {
                throw new UnsupportedCorporateActionException(
                        "RIGHTS_ISSUE holder missing participate election: " + holder.accountId());
            }
            BigDecimal rightsQty =
                    holder.quantitySettled().multiply(rightsPerShare).setScale(10, RoundingMode.HALF_UP);
            impacts.insertEntitlement(row.id(), holder.accountId(), symbol, holder.quantitySettled(), rightsQty, null, null);
            impacts.insertPositionImpact(row.id(), holder.accountId(), rightsSymbol, BigDecimal.ZERO, rightsQty);
            processed++;
        }
        log.info("corporate_action RIGHTS_ISSUE processed id={} symbol={} participants={}", row.id(), symbol, processed);
    }

    private void processStockDividend(
            CorporateActionEventRepository.ProcessingRow row,
            UUID custody,
            java.util.List<CorporateActionRecordDateSnapshotService.ResolvedHolder> holders,
            JsonNode payload) {
        BigDecimal ratio = stockDividendRatio(payload);
        if (ratio == null || ratio.signum() <= 0) {
            throw new UnsupportedCorporateActionException("STOCK_DIVIDEND missing sharesPerShare or newShares/oldShares");
        }
        String symbol = row.instrumentSymbol().trim().toUpperCase(Locale.ROOT);
        for (CorporateActionRecordDateSnapshotService.ResolvedHolder holder : holders) {
            BigDecimal before = holder.quantitySettled();
            BigDecimal after = before.multiply(ratio).setScale(10, RoundingMode.HALF_UP);
            positions.applyCorporateActionSplit(holder.accountId(), symbol, custody, ratio);
            impacts.insertEntitlement(row.id(), holder.accountId(), symbol, before, after, null, null);
            impacts.insertPositionImpact(row.id(), holder.accountId(), symbol, before, after);
        }
        log.info("corporate_action STOCK_DIVIDEND processed id={} symbol={} ratio={} holders={}",
                row.id(), symbol, ratio, holders.size());
    }

    private void processSymbolChange(
            CorporateActionEventRepository.ProcessingRow row,
            UUID custody,
            java.util.List<CorporateActionRecordDateSnapshotService.ResolvedHolder> holders,
            JsonNode payload) {
        String oldSymbol = row.instrumentSymbol().trim().toUpperCase(Locale.ROOT);
        String newSymbol = textField(payload, "newSymbol");
        if (newSymbol == null || newSymbol.isBlank()) {
            newSymbol = textField(payload, "newInstrumentSymbol");
        }
        if (newSymbol == null || newSymbol.isBlank()) {
            throw new UnsupportedCorporateActionException("SYMBOL_CHANGE missing newSymbol");
        }
        newSymbol = newSymbol.trim().toUpperCase(Locale.ROOT);
        for (CorporateActionRecordDateSnapshotService.ResolvedHolder holder : holders) {
            BigDecimal qty = holder.quantitySettled();
            positions.applyCorporateActionSymbolRename(holder.accountId(), oldSymbol, newSymbol, custody);
            impacts.insertEntitlement(row.id(), holder.accountId(), oldSymbol, qty, qty, null, null);
            impacts.insertPositionImpact(row.id(), holder.accountId(), newSymbol, qty, qty);
        }
        log.info("corporate_action SYMBOL_CHANGE processed id={} {} -> {} holders={}",
                row.id(), oldSymbol, newSymbol, holders.size());
    }

    static BigDecimal stockDividendRatio(JsonNode payload) {
        BigDecimal sharesPerShare = decimalField(payload, "sharesPerShare");
        if (sharesPerShare != null && sharesPerShare.signum() > 0) {
            return BigDecimal.ONE.add(sharesPerShare);
        }
        return splitRatio(payload);
    }

    static BigDecimal splitRatio(JsonNode payload) {
        BigDecimal newShares = decimalField(payload, "newShares");
        BigDecimal oldShares = decimalField(payload, "oldShares");
        if (newShares != null && oldShares != null && oldShares.signum() > 0) {
            return newShares.divide(oldShares, 10, RoundingMode.HALF_UP);
        }
        return decimalField(payload, "splitRatio");
    }

    static BigDecimal withholdingAmount(BigDecimal gross, JsonNode payload) {
        BigDecimal rate = decimalField(payload, "withholdingRate");
        if (rate == null || rate.signum() <= 0) {
            return BigDecimal.ZERO.setScale(MONEY_SCALE);
        }
        return gross.multiply(rate).setScale(MONEY_SCALE, RoundingMode.HALF_UP);
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
