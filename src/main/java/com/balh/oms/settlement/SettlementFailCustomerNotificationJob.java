package com.balh.oms.settlement;

import com.balh.oms.config.OmsConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** Enqueues settlement-delayed customer notifications for unresolved fails (§5.8). */
@Component
@ConditionalOnProperty(name = "oms.settlement.fail-customer-notification-enabled", havingValue = "true")
public class SettlementFailCustomerNotificationJob {

    private static final Logger log = LoggerFactory.getLogger(SettlementFailCustomerNotificationJob.class);

    private static final String LIST_OVERDUE =
            """
                    SELECT f.id AS fail_row_id, f.execution_id, e.account_id, o.instrument_symbol
                    FROM broker_settlement_fail_row f
                    JOIN executions e ON e.id = f.execution_id
                    JOIN orders o ON o.id = e.order_id
                    WHERE f.applied_at IS NOT NULL
                      AND f.customer_notified_at IS NULL
                      AND f.execution_id IS NOT NULL
                      AND f.intended_settlement_date <= :cutoff
                    LIMIT :lim
                    """;

    private static final String MARK_NOTIFIED =
            "UPDATE broker_settlement_fail_row SET customer_notified_at = NOW() WHERE id = :id";

    private final NamedParameterJdbcTemplate jdbc;
    private final SettlementCustomerNotificationOutboxRepository outbox;
    private final ObjectMapper objectMapper;
    private final OmsConfig config;

    public SettlementFailCustomerNotificationJob(
            NamedParameterJdbcTemplate jdbc,
            SettlementCustomerNotificationOutboxRepository outbox,
            ObjectMapper objectMapper,
            OmsConfig config) {
        this.jdbc = jdbc;
        this.outbox = outbox;
        this.objectMapper = objectMapper;
        this.config = config;
    }

    @Scheduled(fixedDelayString = "${oms.settlement.fail-customer-notification-interval-ms:3600000}")
    public void enqueueOverdueFails() {
        int slaDays = config.getSettlement().getFailCustomerNotificationSlaBusinessDays();
        LocalDate cutoff = LocalDate.now().minusDays(Math.max(1, slaDays));
        List<Row> rows =
                jdbc.query(
                        LIST_OVERDUE,
                        new MapSqlParameterSource("cutoff", cutoff).addValue("lim", 50),
                        (rs, rowNum) ->
                                new Row(
                                        rs.getLong("fail_row_id"),
                                        rs.getLong("execution_id"),
                                        UUID.fromString(rs.getString("account_id")),
                                        rs.getString("instrument_symbol")));
        for (Row row : rows) {
            try {
                ObjectNode envelope = objectMapper.createObjectNode();
                envelope.put("schemaVersion", 1);
                envelope.put("type", SettlementCustomerNotificationTypes.SETTLEMENT_DELAYED);
                envelope.put("failRowId", row.failRowId());
                envelope.put("executionId", row.executionId());
                envelope.put("accountId", row.accountId().toString());
                envelope.put("instrumentSymbol", row.instrumentSymbol() == null ? "" : row.instrumentSymbol());
                outbox.insertIgnore(
                        SettlementCustomerNotificationTypes.SETTLEMENT_DELAYED,
                        row.accountId(),
                        row.executionId(),
                        row.failRowId(),
                        objectMapper.writeValueAsString(envelope));
                jdbc.update(MARK_NOTIFIED, new MapSqlParameterSource("id", row.failRowId()));
                log.info(
                        "settlement fail customer notification enqueued failRowId={} executionId={}",
                        row.failRowId(),
                        row.executionId());
            } catch (Exception e) {
                log.warn("settlement fail notification enqueue failed failRowId={}: {}", row.failRowId(), e.toString());
            }
        }
    }

    private record Row(long failRowId, long executionId, UUID accountId, String instrumentSymbol) {}
}
