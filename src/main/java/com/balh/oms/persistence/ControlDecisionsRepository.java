package com.balh.oms.persistence;

import com.balh.oms.domain.RejectCode;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.UUID;

/**
 * Persists one row per {@link com.balh.oms.tailer.ControlTailer#apply} outcome (PASS or REJECT).
 */
@Repository
public class ControlDecisionsRepository {

    private static final String INSERT = """
            INSERT INTO control_decisions (order_id, order_version_before, outcome, reject_code, stage, detail)
            VALUES (:order_id, :order_version_before, :outcome,
                    CAST(:reject_code AS reject_code), :stage, CAST(:detail AS JSONB))
            """;

    private final NamedParameterJdbcTemplate jdbc;

    public ControlDecisionsRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void record(
            UUID orderId,
            int orderVersionBefore,
            String outcome,
            RejectCode rejectCode,
            String stage,
            String detailJson) {
        var params = new MapSqlParameterSource()
                .addValue("order_id", orderId)
                .addValue("order_version_before", orderVersionBefore)
                .addValue("outcome", outcome)
                .addValue("reject_code", rejectCode == null ? null : rejectCode.name())
                .addValue("stage", stage)
                .addValue("detail", detailJson == null ? "{}" : detailJson);
        jdbc.update(INSERT, params);
    }
}
