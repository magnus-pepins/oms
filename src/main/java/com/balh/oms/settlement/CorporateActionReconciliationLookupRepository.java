package com.balh.oms.settlement;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class CorporateActionReconciliationLookupRepository {

    public record OmsEventRow(long id, String actionType, String instrumentSymbol, LocalDate effectiveDate) {}

    private final NamedParameterJdbcTemplate jdbc;

    public CorporateActionReconciliationLookupRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public Optional<OmsEventRow> findByBrokerEvent(String brokerId, String brokerEventId) {
        List<OmsEventRow> rows =
                jdbc.query(
                        """
                                SELECT id, action_type, instrument_symbol, effective_date
                                FROM corporate_action_event
                                WHERE broker_id = :brokerId AND broker_event_id = :brokerEventId
                                LIMIT 1
                                """,
                        new MapSqlParameterSource()
                                .addValue("brokerId", brokerId)
                                .addValue("brokerEventId", brokerEventId),
                        (rs, rowNum) ->
                                new OmsEventRow(
                                        rs.getLong("id"),
                                        rs.getString("action_type"),
                                        rs.getString("instrument_symbol"),
                                        rs.getDate("effective_date").toLocalDate()));
        return rows.stream().findFirst();
    }
}
