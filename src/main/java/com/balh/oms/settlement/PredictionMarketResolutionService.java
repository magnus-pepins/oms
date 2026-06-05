package com.balh.oms.settlement;

import com.balh.oms.cluster.OmsClusterWireFormat;
import com.balh.oms.cluster.VenueResolutionAppliedEvent;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.domain.OrderStatus;
import com.balh.oms.persistence.DomainEventOutboxRepository;
import com.balh.oms.persistence.PositionsRepository;
import com.balh.oms.predictionmarket.PredictionMarketContractRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Phase B: projector-side effects for {@link VenueResolutionAppliedEvent}. */
@Service
public class PredictionMarketResolutionService {

    private static final BigDecimal DEFAULT_PAYOUT_PER_CONTRACT = BigDecimal.ONE;
    private static final String DEFAULT_SETTLEMENT_CURRENCY = "USD";
    private static final String COLLATERAL_INDICATOR_PREFIX = "@Prediction-Market-Collateral-";

    private final VenueContractResolutionRepository resolutionRepository;
    private final PredictionMarketContractRepository contractRepository;
    private final PredictionMarketLedgerOutboxRepository ledgerOutbox;
    private final com.balh.oms.persistence.ExecutionsRepository executionsRepository;
    private final PositionsRepository positionsRepository;
    private final DomainEventOutboxRepository domainEventOutboxRepository;
    private final NamedParameterJdbcTemplate jdbc;
    private final OmsConfig config;
    private final ObjectMapper objectMapper;

    public PredictionMarketResolutionService(
            VenueContractResolutionRepository resolutionRepository,
            PredictionMarketContractRepository contractRepository,
            PredictionMarketLedgerOutboxRepository ledgerOutbox,
            com.balh.oms.persistence.ExecutionsRepository executionsRepository,
            PositionsRepository positionsRepository,
            DomainEventOutboxRepository domainEventOutboxRepository,
            NamedParameterJdbcTemplate jdbc,
            OmsConfig config,
            ObjectMapper objectMapper) {
        this.resolutionRepository = resolutionRepository;
        this.contractRepository = contractRepository;
        this.ledgerOutbox = ledgerOutbox;
        this.executionsRepository = executionsRepository;
        this.positionsRepository = positionsRepository;
        this.domainEventOutboxRepository = domainEventOutboxRepository;
        this.jdbc = jdbc;
        this.config = config;
        this.objectMapper = objectMapper;
    }

    public void apply(VenueResolutionAppliedEvent ev) {
        String symbol = ev.instrumentSymbol().trim();
        String outcomeLabel = outcomeLabel(ev.outcomeCode());
        Instant resolvedAt = Instant.ofEpochMilli(ev.resolutionTimestampMillis());
        Instant disputeUntil =
                resolvedAt.plusMillis(config.getVenue().getResolutionDisputeWindowMs());

        long resolutionId =
                resolutionRepository
                        .insertIgnoreReturningId(
                                symbol,
                                outcomeLabel,
                                ev.resolutionSource(),
                                resolvedAt,
                                ev.evidenceHash(),
                                ev.venueId(),
                                disputeUntil,
                                ev.ordersResolvedCount())
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "venue_contract_resolution missing after insert: "
                                                        + symbol
                                                        + " "
                                                        + ev.evidenceHash()));

        resolveOpenOrders(symbol);
        enqueuePayouts(resolutionId, symbol, ev.outcomeCode());
        enqueueTradeFees(resolutionId, symbol);
        writeDomainEvent(ev, symbol, outcomeLabel, resolutionId);
    }

    private PredictionMarketContractRepository.ContractRow contractTermsForBase(String baseSymbol) {
        String trimmed = baseSymbol.trim();
        final String yesSymbol =
                trimmed.endsWith("-NO") ? trimmed.substring(0, trimmed.length() - 3) : trimmed;
        return contractRepository
                .findByYesSymbol(yesSymbol)
                .orElseThrow(
                        () ->
                                new IllegalStateException(
                                        "prediction_market_contract missing for yes_symbol=" + yesSymbol));
    }

    private void resolveOpenOrders(String baseSymbol) {
        String noSymbol = noTradeSymbolForBase(baseSymbol);
        jdbc.update(
                """
                        UPDATE orders
                        SET status = CAST(:resolved AS order_status), version = version + 1
                        WHERE instrument_symbol IN (:yesSymbol, :noSymbol)
                          AND status IN (CAST(:working AS order_status), CAST(:partial AS order_status))
                        """,
                new MapSqlParameterSource()
                        .addValue("resolved", OrderStatus.RESOLVED.name())
                        .addValue("yesSymbol", baseSymbol)
                        .addValue("noSymbol", noSymbol)
                        .addValue("working", OrderStatus.WORKING.name())
                        .addValue("partial", OrderStatus.PARTIALLY_FILLED.name()));
    }

    private void enqueuePayouts(long resolutionId, String baseSymbol, byte outcomeCode) {
        PredictionMarketContractRepository.ContractRow terms = contractTermsForBase(baseSymbol);
        BigDecimal payoutPerContract =
                terms.payoutPerContract() != null
                        ? terms.payoutPerContract()
                        : DEFAULT_PAYOUT_PER_CONTRACT;
        String currency =
                terms.settlementCurrency() != null && !terms.settlementCurrency().isBlank()
                        ? terms.settlementCurrency().trim().toUpperCase()
                        : DEFAULT_SETTLEMENT_CURRENCY;
        if (outcomeCode == OmsClusterWireFormat.OUTCOME_YES) {
            enqueuePayoutsForSymbol(resolutionId, baseSymbol, "YES", payoutPerContract, currency);
        } else if (outcomeCode == OmsClusterWireFormat.OUTCOME_NO) {
            enqueuePayoutsForSymbol(
                    resolutionId, noTradeSymbolForBase(baseSymbol), "NO", payoutPerContract, currency);
        }
        zeroContractPositions(baseSymbol);
    }

    private void enqueuePayoutsForSymbol(
            long resolutionId,
            String positionSymbol,
            String outcomeLabel,
            BigDecimal payoutPerContract,
            String currency) {
        List<PositionsRepository.AccountPositionKeyRow> holders =
                positionsRepository.findNonZeroPositionsForSymbol(positionSymbol);
        for (PositionsRepository.AccountPositionKeyRow row : holders) {
            BigDecimal qty = row.quantityTotal();
            if (qty == null || qty.signum() <= 0) {
                continue;
            }
            BigDecimal payout = qty.multiply(payoutPerContract).setScale(2, RoundingMode.HALF_UP);
            ObjectNode payload = objectMapper.createObjectNode();
            payload.put("schemaVersion", 2);
            SettlementTemplatePayload.enrich(
                    payload,
                    SettlementTemplateIds.PREDICTION_MARKET_BINARY_RESOLUTION,
                    SettlementTemplateIds.PREDICTION_MARKET_BINARY_RESOLUTION_VERSION);
            payload.put("accountId", row.accountId().toString());
            payload.put("instrumentSymbol", positionSymbol);
            payload.put("payoutAmount", payout.toPlainString());
            payload.put("currency", currency);
            payload.put("outcome", outcomeLabel);
            payload.put(
                    "collateralIndicator", COLLATERAL_INDICATOR_PREFIX + currency.trim().toUpperCase());
            ledgerOutbox.insertIgnore(
                    resolutionId,
                    row.accountId(),
                    PredictionMarketLedgerOutboxRepository.LEG_PREDICTION_PAYOUT,
                    payload.toString());
        }
    }

    private void enqueueTradeFees(long resolutionId, String baseSymbol) {
        String noSymbol = noTradeSymbolForBase(baseSymbol);
        java.util.List<String> symbols = java.util.List.of(baseSymbol, noSymbol);
        java.time.Instant collectedAt = java.time.Instant.now();
        for (com.balh.oms.persistence.ExecutionsRepository.AccountFeeTotal row :
                executionsRepository.sumUncollectedTradeFeesForSymbols(symbols)) {
            if (row.feeTotal() == null || row.feeTotal().signum() <= 0) {
                continue;
            }
            String currency =
                    row.feeCurrency() != null && !row.feeCurrency().isBlank()
                            ? row.feeCurrency().trim().toUpperCase()
                            : DEFAULT_SETTLEMENT_CURRENCY;
            ObjectNode payload = objectMapper.createObjectNode();
            payload.put("schemaVersion", 1);
            SettlementTemplatePayload.enrich(
                    payload,
                    SettlementTemplateIds.PREDICTION_MARKET_TRADE_FEE,
                    SettlementTemplateIds.PREDICTION_MARKET_TRADE_FEE_VERSION);
            payload.put("accountId", row.accountId().toString());
            payload.put("instrumentSymbol", baseSymbol);
            payload.put("feeAmount", row.feeTotal().toPlainString());
            payload.put("currency", currency);
            payload.put(
                    "collateralIndicator", COLLATERAL_INDICATOR_PREFIX + currency.trim().toUpperCase());
            payload.put("revenueIndicator", "@Platform-Revenue-" + currency);
            ledgerOutbox.insertIgnore(
                    resolutionId,
                    row.accountId(),
                    PredictionMarketLedgerOutboxRepository.LEG_PREDICTION_TRADE_FEE,
                    payload.toString());
        }
        executionsRepository.markTradeFeesCollectedForSymbols(symbols, collectedAt);
    }

    private void zeroContractPositions(String baseSymbol) {
        String noSymbol = noTradeSymbolForBase(baseSymbol);
        zeroPositions(baseSymbol);
        zeroPositions(noSymbol);
    }

    private static String noTradeSymbolForBase(String baseSymbol) {
        String base = baseSymbol.trim();
        if (base.endsWith("-NO")) {
            return base;
        }
        return base + "-NO";
    }

    private void zeroPositions(String symbol) {
        jdbc.update(
                """
                        UPDATE positions
                        SET quantity_total = 0,
                            quantity_settled = 0,
                            quantity_pending_buy_settle = 0,
                            quantity_pending_sell_settle = 0,
                            updated_at = NOW()
                        WHERE instrument_symbol = :symbol
                        """,
                new MapSqlParameterSource("symbol", symbol));
    }

    private void writeDomainEvent(
            VenueResolutionAppliedEvent ev, String symbol, String outcomeLabel, long resolutionId) {
        ObjectNode envelope = objectMapper.createObjectNode();
        envelope.put("type", "VenueContractResolved");
        envelope.put("contractSymbol", symbol);
        envelope.put("outcome", outcomeLabel);
        envelope.put("resolutionSource", ev.resolutionSource());
        envelope.put("evidenceHash", ev.evidenceHash());
        envelope.put("resolutionId", resolutionId);
        envelope.put("ordersResolvedCount", ev.ordersResolvedCount());
        domainEventOutboxRepository.insert(new UUID(0L, 0L), envelope.toString());
    }

    private static String outcomeLabel(byte outcomeCode) {
        if (outcomeCode == OmsClusterWireFormat.OUTCOME_YES) {
            return "YES";
        }
        if (outcomeCode == OmsClusterWireFormat.OUTCOME_NO) {
            return "NO";
        }
        return "UNKNOWN";
    }
}
