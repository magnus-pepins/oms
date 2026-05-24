package com.balh.oms.settlement;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** Resolves broker fail rows to OMS TRADE executions (gap plan §5.8). */
@Repository
public class BrokerSettlementFailLookupRepository {

    public record TradeTarget(
            long executionId, UUID accountId, BigDecimal lastQuantity, String settlementStatus) {}

    private static final String FIND_BY_BROKER_VENUE_REF =
            """
                    SELECT e.id AS execution_id,
                           e.account_id,
                           e.last_quantity,
                           e.settlement_status::text AS settlement_status
                    FROM executions e
                    WHERE e.venue_exec_ref = :venueRef
                      AND e.exec_type = CAST('TRADE' AS execution_exec_type)
                      AND EXISTS (
                          SELECT 1
                          FROM positions p
                          JOIN custody_accounts ca ON ca.id = p.custody_account_id
                          WHERE p.account_id = e.account_id
                            AND ca.broker_id = :brokerId
                      )
                    LIMIT 2
                    """;

    private final NamedParameterJdbcTemplate jdbc;

    public BrokerSettlementFailLookupRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public Optional<TradeTarget> findTradeByBrokerVenueRef(String brokerId, String venueRef) {
        if (brokerId == null || brokerId.isBlank() || venueRef == null || venueRef.isBlank()) {
            return Optional.empty();
        }
        List<TradeTarget> rows = jdbc.query(
                FIND_BY_BROKER_VENUE_REF,
                new MapSqlParameterSource()
                        .addValue("brokerId", brokerId.trim())
                        .addValue("venueRef", venueRef.trim()),
                (rs, rowNum) ->
                        new TradeTarget(
                                rs.getLong("execution_id"),
                                (UUID) rs.getObject("account_id"),
                                rs.getBigDecimal("last_quantity"),
                                rs.getString("settlement_status")));
        if (rows.size() != 1) {
            return Optional.empty();
        }
        return Optional.of(rows.getFirst());
    }
}
