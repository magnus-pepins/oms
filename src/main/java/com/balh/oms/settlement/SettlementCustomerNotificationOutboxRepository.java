package com.balh.oms.settlement;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class SettlementCustomerNotificationOutboxRepository {

    private static final String INSERT =
            """
                    INSERT INTO settlement_customer_notification_outbox (
                        notification_type, account_id, execution_id, fail_row_id, envelope_json
                    ) VALUES (
                        :type, :accountId, :executionId, :failRowId, CAST(:envelope AS JSONB)
                    )
                    """;

    private final NamedParameterJdbcTemplate jdbc;

    public SettlementCustomerNotificationOutboxRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public int insertIgnore(
            String notificationType,
            UUID accountId,
            Long executionId,
            Long failRowId,
            String envelopeJson) {
        return jdbc.update(
                INSERT,
                new MapSqlParameterSource()
                        .addValue("type", notificationType)
                        .addValue("accountId", accountId)
                        .addValue("executionId", executionId)
                        .addValue("failRowId", failRowId)
                        .addValue("envelope", envelopeJson));
    }
}
