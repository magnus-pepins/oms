package com.balh.oms.corporateaction;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.settlement.SettlementCustomerNotificationOutboxRepository;
import com.balh.oms.settlement.SettlementCustomerNotificationTypes;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;

/** Enqueues dividend-paid customer notifications after Ledger credit (§5.9). */
@Service
public class CorporateActionCustomerNotificationService {

    private static final Logger log = LoggerFactory.getLogger(CorporateActionCustomerNotificationService.class);

    private static final String LOAD_EVENT_SYMBOL =
            """
                    SELECT instrument_symbol
                    FROM corporate_action_event
                    WHERE id = :eventId
                    """;

    private static final String MARK_NOTIFIED =
            """
                    UPDATE corporate_action_cash_impact
                    SET customer_notified_at = NOW()
                    WHERE id = :id AND customer_notified_at IS NULL
                    """;

    private static final String IS_ALREADY_NOTIFIED =
            """
                    SELECT customer_notified_at IS NOT NULL
                    FROM corporate_action_cash_impact
                    WHERE id = :id
                    """;

    private final SettlementCustomerNotificationOutboxRepository outbox;
    private final NamedParameterJdbcTemplate jdbc;
    private final ObjectMapper objectMapper;
    private final OmsConfig config;

    public CorporateActionCustomerNotificationService(
            SettlementCustomerNotificationOutboxRepository outbox,
            NamedParameterJdbcTemplate jdbc,
            ObjectMapper objectMapper,
            OmsConfig config) {
        this.outbox = outbox;
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
        this.config = config;
    }

    /**
     * Called after the dividend ledger leg posts successfully.
     *
     * @param payloadJson ledger outbox payload (see {@link CorporateActionCashLedgerBookingService})
     */
    public void enqueueDividendPaidIfEnabled(String payloadJson) {
        if (!config.getSettlement().isCorporateActionDividendNotificationEnabled()) {
            return;
        }
        try {
            var root = objectMapper.readTree(payloadJson);
            if (!CorporateActionCashLedgerBookingService.LEG_DIVIDEND.equals(root.path("leg").asText())) {
                return;
            }
            long cashImpactId = root.path("cashImpactId").asLong(0L);
            if (cashImpactId <= 0L) {
                return;
            }
            Boolean already =
                    jdbc.queryForObject(
                            IS_ALREADY_NOTIFIED,
                            new MapSqlParameterSource("id", cashImpactId),
                            Boolean.class);
            if (Boolean.TRUE.equals(already)) {
                return;
            }
            UUID accountId = UUID.fromString(root.path("accountId").asText());
            long eventId = root.path("corporateActionEventId").asLong();
            String symbol =
                    jdbc.queryForObject(
                            LOAD_EVENT_SYMBOL,
                            new MapSqlParameterSource("eventId", eventId),
                            String.class);
            ObjectNode envelope = objectMapper.createObjectNode();
            envelope.put("schemaVersion", 1);
            envelope.put("type", SettlementCustomerNotificationTypes.CORPORATE_ACTION_DIVIDEND_PAID);
            envelope.put("cashImpactId", cashImpactId);
            envelope.put("corporateActionEventId", eventId);
            envelope.put("accountId", accountId.toString());
            envelope.put("instrumentSymbol", symbol == null ? "" : symbol);
            envelope.put("netAmount", root.path("netAmount").asText());
            envelope.put("currency", root.path("currency").asText());
            int inserted =
                    outbox.insertCorporateActionIgnore(
                            SettlementCustomerNotificationTypes.CORPORATE_ACTION_DIVIDEND_PAID,
                            accountId,
                            cashImpactId,
                            objectMapper.writeValueAsString(envelope));
            if (inserted > 0) {
                jdbc.update(MARK_NOTIFIED, new MapSqlParameterSource("id", cashImpactId));
                log.info(
                        "corporate action dividend customer notification enqueued cashImpactId={} accountId={}",
                        cashImpactId,
                        accountId);
            }
        } catch (Exception e) {
            log.warn("corporate action dividend customer notification enqueue failed: {}", e.toString());
        }
    }
}
