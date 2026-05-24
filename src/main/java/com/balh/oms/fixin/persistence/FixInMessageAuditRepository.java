package com.balh.oms.fixin.persistence;

import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public class FixInMessageAuditRepository {

    private static final String INSERT = """
            INSERT INTO oms_fix_message_audit (
                id, direction, session_role, fix_session_id, msg_type, msg_seq_num,
                cl_ord_id, orig_cl_ord_id, oms_order_id, exec_id, raw_store_ref, summary)
            VALUES (
                :id, :direction, :session_role, :fix_session_id, :msg_type, :msg_seq_num,
                :cl_ord_id, :orig_cl_ord_id, :oms_order_id, :exec_id, :raw_store_ref, :summary)
            """;

    private final NamedParameterJdbcTemplate jdbc;

    public FixInMessageAuditRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    private static final String SEARCH = """
            SELECT id, direction, session_role, fix_session_id, msg_type, msg_seq_num,
                   cl_ord_id, orig_cl_ord_id, oms_order_id, exec_id, raw_store_ref, summary, created_at
              FROM oms_fix_message_audit
             WHERE (:fix_session_id IS NULL OR fix_session_id = :fix_session_id)
               AND (:cl_ord_id IS NULL OR cl_ord_id = :cl_ord_id)
               AND (:oms_order_id IS NULL OR oms_order_id = :oms_order_id)
             ORDER BY created_at DESC
             LIMIT :limit
            """;

    private static final String SELECT_BY_ID = """
            SELECT id, direction, session_role, fix_session_id, msg_type, msg_seq_num,
                   cl_ord_id, orig_cl_ord_id, oms_order_id, exec_id, raw_store_ref, summary, created_at
              FROM oms_fix_message_audit
             WHERE id = :id
            """;

    public List<FixInMessageAuditRow> search(UUID fixSessionIdOrNull, String clOrdIdOrNull, UUID omsOrderIdOrNull, int limit) {
        return jdbc.query(
                SEARCH,
                new MapSqlParameterSource()
                        .addValue("fix_session_id", fixSessionIdOrNull)
                        .addValue("cl_ord_id", clOrdIdOrNull)
                        .addValue("oms_order_id", omsOrderIdOrNull)
                        .addValue("limit", Math.min(500, Math.max(1, limit))),
                (rs, rowNum) -> mapRow(rs));
    }

    public Optional<FixInMessageAuditRow> findById(UUID id) {
        try {
            return Optional.of(jdbc.queryForObject(
                    SELECT_BY_ID, new MapSqlParameterSource("id", id), (rs, rowNum) -> mapRow(rs)));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    private static FixInMessageAuditRow mapRow(java.sql.ResultSet rs) throws java.sql.SQLException {
        return new FixInMessageAuditRow(
                rs.getObject("id", UUID.class),
                rs.getString("direction"),
                rs.getString("session_role"),
                rs.getObject("fix_session_id", UUID.class),
                rs.getString("msg_type"),
                (Integer) rs.getObject("msg_seq_num"),
                rs.getString("cl_ord_id"),
                rs.getString("orig_cl_ord_id"),
                rs.getObject("oms_order_id", UUID.class),
                rs.getString("exec_id"),
                rs.getString("raw_store_ref"),
                rs.getString("summary"),
                rs.getTimestamp("created_at").toInstant());
    }

    public void insert(FixInMessageAuditRow row) {
        jdbc.update(
                INSERT,
                new MapSqlParameterSource()
                        .addValue("id", row.id())
                        .addValue("direction", row.direction())
                        .addValue("session_role", row.sessionRole())
                        .addValue("fix_session_id", row.fixSessionIdOrNull())
                        .addValue("msg_type", row.msgTypeOrNull())
                        .addValue("msg_seq_num", row.msgSeqNumOrNull())
                        .addValue("cl_ord_id", row.clOrdIdOrNull())
                        .addValue("orig_cl_ord_id", row.origClOrdIdOrNull())
                        .addValue("oms_order_id", row.omsOrderIdOrNull())
                        .addValue("exec_id", row.execIdOrNull())
                        .addValue("raw_store_ref", row.rawStoreRefOrNull())
                        .addValue("summary", row.summaryOrNull()));
    }
}
