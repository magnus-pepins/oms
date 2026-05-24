package com.balh.oms.settlement;

import com.balh.oms.config.OmsConfig;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Enqueues broker CSDR/settlement penalty legs to Ledger outbox (gap plan §5.8 Slice 14a).
 *
 * <p>v1: bank absorbs — {@code @Nostro-<ccy>-Bank} → {@code @Settlement-Penalties-<ccy>}.
 */
@Service
public class SettlementFailPenaltyBookingService {

    private static final Logger log = LoggerFactory.getLogger(SettlementFailPenaltyBookingService.class);

    public static final String OUTBOX_STATUS_FAIL_PENALTY = "fail_penalty";
    public static final String PENALTY_BALANCE_PREFIX = "@Settlement-Penalties-";

    private final LedgerSettlementOutboxRepository outbox;
    private final BrokerSettlementFailRowRepository failRows;
    private final ObjectMapper objectMapper;
    private final OmsConfig config;

    public SettlementFailPenaltyBookingService(
            LedgerSettlementOutboxRepository outbox,
            BrokerSettlementFailRowRepository failRows,
            ObjectMapper objectMapper,
            OmsConfig config) {
        this.outbox = outbox;
        this.failRows = failRows;
        this.objectMapper = objectMapper;
        this.config = config;
    }

    /** @return 1 if a new outbox row was inserted, 0 if skipped or duplicate */
    public int enqueueIfPresent(
            BrokerSettlementFailRowRepository.FailRow row, long executionId, UUID accountId) {
        if (!config.getLedger().isSettlementOutboxEnabled()) {
            return 0;
        }
        if (row.penaltyAmount() == null || row.penaltyAmount().signum() <= 0) {
            return 0;
        }
        if (row.penaltyCurrency() == null || row.penaltyCurrency().isBlank()) {
            log.warn(
                    "settlement fail penalty skipped: missing currency failRowId={} executionId={}",
                    row.id(),
                    executionId);
            return 0;
        }
        if (row.penaltyBookedAt() != null) {
            return 0;
        }
        String legKind = penaltyLegKind(row.brokerFailId());
        try {
            ObjectNode payload = objectMapper.createObjectNode();
            payload.put("schemaVersion", 2);
            payload.put("event", "SETTLEMENT_FAIL_PENALTY");
            payload.put("leg", LedgerSettlementOutboxRepository.LEG_PENALTY);
            payload.put("executionId", executionId);
            if (accountId != null) {
                payload.put("accountId", accountId.toString());
            }
            payload.put("failRowId", row.id());
            payload.put("brokerFailId", row.brokerFailId());
            payload.put("penaltyAmount", row.penaltyAmount().toPlainString());
            payload.put("penaltyCurrency", row.penaltyCurrency().trim().toUpperCase());
            payload.put("penaltyBalanceIndicator", PENALTY_BALANCE_PREFIX + row.penaltyCurrency().trim().toUpperCase());
            payload.put("bookedAt", Instant.now().toString());

            int inserted =
                    outbox.insertIgnore(executionId, OUTBOX_STATUS_FAIL_PENALTY, legKind, objectMapper.writeValueAsString(payload));
            if (inserted == 1) {
                failRows.markPenaltyBooked(row.id());
            }
            return inserted;
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("failed to serialize penalty outbox payload failRowId=" + row.id(), e);
        }
    }

    static String penaltyLegKind(String brokerFailId) {
        String suffix = brokerFailId == null || brokerFailId.isBlank() ? "unknown" : brokerFailId.trim();
        return LedgerSettlementOutboxRepository.LEG_PENALTY + "-" + suffix;
    }
}
