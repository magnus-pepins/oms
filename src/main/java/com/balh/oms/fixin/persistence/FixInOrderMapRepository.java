package com.balh.oms.fixin.persistence;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public class FixInOrderMapRepository {

    private static final String UPSERT = """
            INSERT INTO oms_fix_in_order_map
                (session_id, client_cl_ord_id, oms_order_id, orig_client_cl_ord_id, account_binding_id)
            VALUES
                (:session_id, :client_cl_ord_id, :oms_order_id, :orig_client_cl_ord_id, :account_binding_id)
            ON CONFLICT (session_id, client_cl_ord_id) DO NOTHING
            """;

    private static final String SELECT_BY_ORDER = """
            SELECT session_id, client_cl_ord_id, oms_order_id, orig_client_cl_ord_id
              FROM oms_fix_in_order_map
             WHERE oms_order_id = :oms_order_id
             ORDER BY created_at
            """;

    private static final String SELECT_BY_SESSION_AND_ORIG = """
            SELECT session_id, client_cl_ord_id, oms_order_id, orig_client_cl_ord_id
              FROM oms_fix_in_order_map
             WHERE session_id = :session_id
               AND client_cl_ord_id = :client_cl_ord_id
            """;

    private static final RowMapper<FixInOrderMapRow> ROW_MAPPER = (rs, rowNum) -> new FixInOrderMapRow(
            rs.getObject("session_id", UUID.class),
            rs.getString("client_cl_ord_id"),
            rs.getObject("oms_order_id", UUID.class),
            rs.getString("orig_client_cl_ord_id"));

    private final NamedParameterJdbcTemplate jdbc;

    public FixInOrderMapRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void insertIfAbsent(
            UUID sessionId, String clientClOrdId, UUID omsOrderId, UUID accountBindingIdOrNull) {
        insertIfAbsent(sessionId, clientClOrdId, omsOrderId, null, accountBindingIdOrNull);
    }

    public void insertIfAbsent(
            UUID sessionId,
            String clientClOrdId,
            UUID omsOrderId,
            String origClientClOrdIdOrNull,
            UUID accountBindingIdOrNull) {
        var params = new MapSqlParameterSource()
                .addValue("session_id", sessionId)
                .addValue("client_cl_ord_id", clientClOrdId)
                .addValue("oms_order_id", omsOrderId)
                .addValue("orig_client_cl_ord_id", origClientClOrdIdOrNull)
                .addValue("account_binding_id", accountBindingIdOrNull);
        jdbc.update(UPSERT, params);
    }

    public List<FixInOrderMapRow> findByOmsOrderId(UUID omsOrderId) {
        return jdbc.query(
                SELECT_BY_ORDER, new MapSqlParameterSource("oms_order_id", omsOrderId), ROW_MAPPER);
    }

    public Optional<FixInOrderMapRow> findBySessionAndClientClOrdId(UUID sessionId, String clientClOrdId) {
        return jdbc.query(
                        SELECT_BY_SESSION_AND_ORIG,
                        new MapSqlParameterSource()
                                .addValue("session_id", sessionId)
                                .addValue("client_cl_ord_id", clientClOrdId),
                        ROW_MAPPER)
                .stream()
                .findFirst();
    }
}
