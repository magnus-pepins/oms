package com.balh.oms.persistence;

import com.balh.oms.domain.Order;
import com.balh.oms.domain.OrderStatus;
import com.balh.oms.domain.RejectCode;
import com.balh.oms.domain.Side;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/**
 * Postgres-backed repository for the {@code orders} table.
 *
 * <p>All authoritative state changes go through here. Mutations use CAS on
 * {@code version}; if the update affects 0 rows, the caller must reload the
 * row (something else won the race).
 *
 * <p>Inserts rely on {@code UNIQUE (account_id, client_idempotency_key)} for
 * idempotency. Duplicates surface as {@link DuplicateOrderException} so the
 * controller can return the existing order rather than creating a new one.
 */
@Repository
public class OrdersRepository {

    private final NamedParameterJdbcTemplate jdbc;

    public OrdersRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public static final class DuplicateOrderException extends RuntimeException {
        public DuplicateOrderException(String message) { super(message); }
    }

    private static final String INSERT_SQL = """
            INSERT INTO orders (
                id, account_id, client_idempotency_key, shard_id, version,
                status, terminal_reason, side, instrument_symbol,
                quantity, limit_price, time_in_force,
                received_at, accepted_at, terminal_at, account_id_hash
            ) VALUES (
                :id, :account_id, :client_idempotency_key, :shard_id, 0,
                CAST(:status AS order_status), CAST(:terminal_reason AS reject_code),
                CAST(:side AS order_side), :instrument_symbol,
                :quantity, :limit_price, :time_in_force,
                :received_at, :accepted_at, :terminal_at, :account_id_hash
            )
            """;

    private static final String SELECT_BY_ID_SQL = """
            SELECT id, account_id, client_idempotency_key, shard_id, version,
                   status::text AS status,
                   terminal_reason::text AS terminal_reason,
                   side::text AS side,
                   instrument_symbol, quantity, limit_price, time_in_force,
                   received_at, accepted_at, terminal_at, account_id_hash
            FROM orders WHERE id = :id
            """;

    private static final String SELECT_BY_IDEMPOTENCY_SQL = """
            SELECT id, account_id, client_idempotency_key, shard_id, version,
                   status::text AS status,
                   terminal_reason::text AS terminal_reason,
                   side::text AS side,
                   instrument_symbol, quantity, limit_price, time_in_force,
                   received_at, accepted_at, terminal_at, account_id_hash
            FROM orders
            WHERE account_id = :account_id AND client_idempotency_key = :key
            """;

    private static final String UPDATE_CAS_SQL = """
            UPDATE orders
               SET status = CAST(:status AS order_status),
                   terminal_reason = CAST(:terminal_reason AS reject_code),
                   accepted_at = COALESCE(:accepted_at, accepted_at),
                   terminal_at = COALESCE(:terminal_at, terminal_at),
                   version = version + 1
             WHERE id = :id AND version = :expected_version
            """;

    public void insert(Order o) {
        try {
            jdbc.update(INSERT_SQL, params(o));
        } catch (DataIntegrityViolationException e) {
            // UNIQUE (account_id, client_idempotency_key) violation.
            throw new DuplicateOrderException(
                    "duplicate order for account=%s key=%s".formatted(
                            o.accountId(), o.clientIdempotencyKey()));
        }
    }

    public Optional<Order> findById(UUID id) {
        var rows = jdbc.query(SELECT_BY_ID_SQL,
                new MapSqlParameterSource("id", id),
                ROW_MAPPER);
        return rows.stream().findFirst();
    }

    public Optional<Order> findByIdempotency(UUID accountId, String idempotencyKey) {
        var params = new MapSqlParameterSource()
                .addValue("account_id", accountId)
                .addValue("key", idempotencyKey);
        var rows = jdbc.query(SELECT_BY_IDEMPOTENCY_SQL, params, ROW_MAPPER);
        return rows.stream().findFirst();
    }

    /**
     * @return {@code true} if the CAS update applied (we hold the latest version).
     *         {@code false} means the row's version moved on; the caller must reload.
     */
    public boolean updateWithCas(
            UUID id,
            int expectedVersion,
            OrderStatus newStatus,
            RejectCode terminalReason,
            Instant acceptedAt,
            Instant terminalAt) {
        var params = new MapSqlParameterSource()
                .addValue("id", id)
                .addValue("expected_version", expectedVersion)
                .addValue("status", newStatus.name())
                .addValue("terminal_reason", terminalReason == null ? null : terminalReason.name())
                .addValue("accepted_at", acceptedAt == null ? null : Timestamp.from(acceptedAt))
                .addValue("terminal_at", terminalAt == null ? null : Timestamp.from(terminalAt));
        return jdbc.update(UPDATE_CAS_SQL, params) == 1;
    }

    private MapSqlParameterSource params(Order o) {
        return new MapSqlParameterSource()
                .addValue("id", o.id())
                .addValue("account_id", o.accountId())
                .addValue("client_idempotency_key", o.clientIdempotencyKey())
                .addValue("shard_id", o.shardId())
                .addValue("status", o.status().name())
                .addValue("terminal_reason", o.terminalReason() == null ? null : o.terminalReason().name())
                .addValue("side", o.side().name())
                .addValue("instrument_symbol", o.instrumentSymbol())
                .addValue("quantity", o.quantity())
                .addValue("limit_price", o.limitPrice())
                .addValue("time_in_force", o.timeInForce())
                .addValue("received_at", Timestamp.from(o.receivedAt()))
                .addValue("accepted_at", o.acceptedAt() == null ? null : Timestamp.from(o.acceptedAt()))
                .addValue("terminal_at", o.terminalAt() == null ? null : Timestamp.from(o.terminalAt()))
                .addValue("account_id_hash", o.accountIdHash());
    }

    private static final RowMapper<Order> ROW_MAPPER = (rs, rowNum) -> {
        BigDecimal limitPrice = rs.getBigDecimal("limit_price");
        Timestamp acceptedAt = rs.getTimestamp("accepted_at");
        Timestamp terminalAt = rs.getTimestamp("terminal_at");
        String terminalReason = rs.getString("terminal_reason");
        return new Order(
                (UUID) rs.getObject("id"),
                (UUID) rs.getObject("account_id"),
                rs.getString("client_idempotency_key"),
                rs.getInt("shard_id"),
                rs.getInt("version"),
                OrderStatus.valueOf(rs.getString("status")),
                terminalReason == null ? null : RejectCode.valueOf(terminalReason),
                Side.valueOf(rs.getString("side")),
                rs.getString("instrument_symbol"),
                rs.getBigDecimal("quantity"),
                limitPrice,
                rs.getString("time_in_force"),
                rs.getTimestamp("received_at").toInstant(),
                acceptedAt == null ? null : acceptedAt.toInstant(),
                terminalAt == null ? null : terminalAt.toInstant(),
                rs.getString("account_id_hash")
        );
    };
}
