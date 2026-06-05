package com.balh.oms.settlement;

import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class VenueParticipantFeeOverrideRepository {

    public static final String TYPE_FIX_COUNTERPARTY = "FIX_COUNTERPARTY";
    public static final String TYPE_OMS_ACCOUNT = "OMS_ACCOUNT";

    public record OverrideRow(UUID id, String feeModelId, String feeParamsJson) {}

    private final NamedParameterJdbcTemplate jdbc;

    public VenueParticipantFeeOverrideRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public Optional<OverrideRow> findEnabled(String participantType, UUID participantId, long contractId) {
        Optional<OverrideRow> exact =
                queryOne(
                        """
                                SELECT id, fee_model_id, fee_params_json::text AS fee_params_json
                                FROM venue_participant_fee_override
                                WHERE participant_type = :type
                                  AND participant_id = :pid
                                  AND contract_id = :cid
                                  AND enabled = TRUE
                                LIMIT 1
                                """,
                        participantType,
                        participantId,
                        contractId);
        if (exact.isPresent()) {
            return exact;
        }
        return queryOne(
                """
                        SELECT id, fee_model_id, fee_params_json::text AS fee_params_json
                        FROM venue_participant_fee_override
                        WHERE participant_type = :type
                          AND participant_id = :pid
                          AND contract_id IS NULL
                          AND enabled = TRUE
                        LIMIT 1
                        """,
                participantType,
                participantId,
                null);
    }

    private Optional<OverrideRow> queryOne(String sql, String type, UUID pid, Long contractId) {
        MapSqlParameterSource params =
                new MapSqlParameterSource().addValue("type", type).addValue("pid", pid);
        if (contractId != null) {
            params.addValue("cid", contractId);
        } else {
            params.addValue("cid", null);
        }
        var rows =
                jdbc.query(
                        sql,
                        params,
                        (rs, rowNum) ->
                                new OverrideRow(
                                        (UUID) rs.getObject("id"),
                                        rs.getString("fee_model_id"),
                                        rs.getString("fee_params_json")));
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.getFirst());
    }
}
