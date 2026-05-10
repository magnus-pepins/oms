package com.balh.oms.persistence;

import com.balh.oms.domain.RejectCode;
import io.micrometer.core.instrument.MeterRegistry;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * Persists one row per {@link com.balh.oms.tailer.ControlTailer#apply} outcome (PASS or REJECT).
 */
@Repository
public class ControlDecisionsRepository {

    /** Micrometer counter mirrored for every successful {@link #record} (Ops Prometheus / W5.2). */
    private static final String METRIC_CONTROL_DECISIONS_RECORDED = "oms_control_decisions_recorded_total";

    private static final String TAG_OUTCOME = "outcome";
    private static final String TAG_REJECT_CODE = "reject_code";
    /** Tag value when {@code rejectCode} is null (PASS rows in Postgres). */
    private static final String REJECT_CODE_TAG_NONE = "NONE";

    private static final String INSERT = """
            INSERT INTO control_decisions (order_id, order_version_before, outcome, reject_code, stage, detail)
            VALUES (:order_id, :order_version_before, :outcome,
                    CAST(:reject_code AS reject_code), :stage, CAST(:detail AS JSONB))
            """;

    private final NamedParameterJdbcTemplate jdbc;
    private final MeterRegistry meterRegistry;

    public ControlDecisionsRepository(NamedParameterJdbcTemplate jdbc, MeterRegistry meterRegistry) {
        this.jdbc = jdbc;
        this.meterRegistry = meterRegistry;
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
        int rows = jdbc.update(INSERT, params);
        if (rows > 0) {
            String rejectTag = rejectCode == null ? REJECT_CODE_TAG_NONE : rejectCode.name();
            meterRegistry
                    .counter(METRIC_CONTROL_DECISIONS_RECORDED, TAG_OUTCOME, outcome, TAG_REJECT_CODE, rejectTag)
                    .increment();
        }
    }

    /**
     * Lists decisions newest-first. Pass {@code null} for unused filters (caller validates that at
     * least one dimension is constrained).
     */
    public List<ControlDecisionRow> findByFilters(UUID orderId, Instant from, Instant to, int limit, int offset) {
        StringBuilder sql = new StringBuilder(
                """
                        SELECT id, order_id, order_version_before, outcome,
                               reject_code::text AS reject_code, stage,
                               COALESCE(detail::text, '{}') AS detail, decided_at
                        FROM control_decisions
                        WHERE 1 = 1
                        """);
        var params = new MapSqlParameterSource();
        if (orderId != null) {
            sql.append(" AND order_id = :order_id");
            params.addValue("order_id", orderId);
        }
        if (from != null) {
            sql.append(" AND decided_at >= :from_ts");
            params.addValue("from_ts", Timestamp.from(from));
        }
        if (to != null) {
            sql.append(" AND decided_at < :to_ts");
            params.addValue("to_ts", Timestamp.from(to));
        }
        sql.append(" ORDER BY decided_at DESC, id DESC LIMIT :lim OFFSET :off");
        params.addValue("lim", limit);
        params.addValue("off", offset);
        return jdbc.query(
                sql.toString(),
                params,
                (rs, rowNum) ->
                        new ControlDecisionRow(
                                rs.getLong("id"),
                                rs.getObject("order_id", UUID.class),
                                rs.getInt("order_version_before"),
                                rs.getString("outcome"),
                                rs.getString("reject_code"),
                                rs.getString("stage"),
                                rs.getString("detail"),
                                rs.getTimestamp("decided_at").toInstant()));
    }
}
