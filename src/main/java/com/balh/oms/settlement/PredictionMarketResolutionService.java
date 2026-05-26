package com.balh.oms.settlement;

import com.balh.oms.cluster.OmsClusterWireFormat;
import com.balh.oms.cluster.VenueResolutionAppliedEvent;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.domain.OrderStatus;
import com.balh.oms.persistence.DomainEventOutboxRepository;
import com.balh.oms.persistence.PositionsRepository;
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

    private static final BigDecimal BINARY_PAYOUT_PER_CONTRACT = BigDecimal.ONE;
    private static final String TEMPLATE = "prediction_market_binary_resolution";
    private static final String COLLATERAL_INDICATOR_PREFIX = "@Prediction-Market-Collateral-";

    private final VenueContractResolutionRepository resolutionRepository;
    private final PredictionMarketLedgerOutboxRepository ledgerOutbox;
    private final PositionsRepository positionsRepository;
    private final DomainEventOutboxRepository domainEventOutboxRepository;
    private final NamedParameterJdbcTemplate jdbc;
    private final OmsConfig config;
    private final ObjectMapper objectMapper;

    public PredictionMarketResolutionService(
            VenueContractResolutionRepository resolutionRepository,
            PredictionMarketLedgerOutboxRepository ledgerOutbox,
            PositionsRepository positionsRepository,
            DomainEventOutboxRepository domainEventOutboxRepository,
            NamedParameterJdbcTemplate jdbc,
            OmsConfig config,
            ObjectMapper objectMapper) {
        this.resolutionRepository = resolutionRepository;
        this.ledgerOutbox = ledgerOutbox;
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
        writeDomainEvent(ev, symbol, outcomeLabel, resolutionId);
    }

    private void resolveOpenOrders(String symbol) {
        jdbc.update(
                """
                        UPDATE orders
                        SET status = CAST(:resolved AS order_status), version = version + 1
                        WHERE instrument_symbol = :symbol
                          AND status IN (CAST(:working AS order_status), CAST(:partial AS order_status))
                        """,
                new MapSqlParameterSource()
                        .addValue("resolved", OrderStatus.RESOLVED.name())
                        .addValue("symbol", symbol)
                        .addValue("working", OrderStatus.WORKING.name())
                        .addValue("partial", OrderStatus.PARTIALLY_FILLED.name()));
    }

    private void enqueuePayouts(long resolutionId, String symbol, byte outcomeCode) {
        if (outcomeCode != OmsClusterWireFormat.OUTCOME_YES) {
            zeroPositions(symbol);
            return;
        }
        List<PositionsRepository.AccountPositionKeyRow> holders =
                positionsRepository.findNonZeroPositionsForSymbol(symbol);
        for (PositionsRepository.AccountPositionKeyRow row : holders) {
            BigDecimal qty = row.quantityTotal();
            if (qty == null || qty.signum() <= 0) {
                continue;
            }
            BigDecimal payout = qty.multiply(BINARY_PAYOUT_PER_CONTRACT).setScale(2, RoundingMode.HALF_UP);
            String currency = "USD";
            ObjectNode payload = objectMapper.createObjectNode();
            payload.put("schemaVersion", 2);
            payload.put("template", TEMPLATE);
            payload.put("accountId", row.accountId().toString());
            payload.put("instrumentSymbol", symbol);
            payload.put("payoutAmount", payout.toPlainString());
            payload.put("currency", currency);
            payload.put("outcome", "YES");
            payload.put(
                    "collateralIndicator", COLLATERAL_INDICATOR_PREFIX + currency.trim().toUpperCase());
            ledgerOutbox.insertIgnore(
                    resolutionId,
                    row.accountId(),
                    PredictionMarketLedgerOutboxRepository.LEG_PREDICTION_PAYOUT,
                    payload.toString());
        }
        zeroPositions(symbol);
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
