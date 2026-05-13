package com.balh.oms.persistence;

import com.balh.oms.cluster.AcceptOrderCommand;
import com.balh.oms.cluster.OrderAdmittedEvent;
import com.balh.oms.domain.Order;
import com.balh.oms.domain.OrderStatus;
import com.balh.oms.domain.RejectCode;
import com.balh.oms.domain.Side;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
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

    // ON CONFLICT DO NOTHING keeps the JDBC transaction healthy on idempotent re-submits so the
    // caller can SELECT the pre-existing row in the same transaction. A raw duplicate-key error
    // would abort the transaction at the Postgres level (every later statement returns
    // "current transaction is aborted, commands ignored until end of transaction block").
    private static final String INSERT_SQL = """
            INSERT INTO orders (
                id, account_id, client_idempotency_key, shard_id, version,
                status, terminal_reason, side, instrument_symbol,
                quantity, limit_price, time_in_force,
                received_at, accepted_at, terminal_at, account_id_hash, ledger_balance_id,
                cum_filled_quantity
            ) VALUES (
                :id, :account_id, :client_idempotency_key, :shard_id, 0,
                CAST(:status AS order_status), CAST(:terminal_reason AS reject_code),
                CAST(:side AS order_side), :instrument_symbol,
                :quantity, :limit_price, :time_in_force,
                :received_at, :accepted_at, :terminal_at, :account_id_hash, :ledger_balance_id,
                :cum_filled_quantity
            )
            ON CONFLICT (account_id, client_idempotency_key) DO NOTHING
            """;

    /**
     * Projector-only insert (slice 2d). Bare {@code ON CONFLICT DO NOTHING} swallows any unique
     * constraint violation — pkey OR {@code (account_id, client_idempotency_key)} — because the
     * projector is purely idempotent on replay and may race a concurrent writer (test
     * {@code TestPostgresProjectorSingleton}, sibling Spring context, restart-from-cursor) that
     * already committed the same row. The legacy ingress path keeps {@link #INSERT_SQL} and its
     * targeted {@code (account_id, client_idempotency_key)} arbiter so callers there can
     * distinguish duplicate idempotency-key submissions from other constraint failures.
     */
    private static final String PROJECTOR_INSERT_SQL = """
            INSERT INTO orders (
                id, account_id, client_idempotency_key, shard_id, version,
                status, terminal_reason, side, instrument_symbol,
                quantity, limit_price, time_in_force,
                received_at, accepted_at, terminal_at, account_id_hash, ledger_balance_id,
                cum_filled_quantity
            ) VALUES (
                :id, :account_id, :client_idempotency_key, :shard_id, 0,
                CAST(:status AS order_status), CAST(:terminal_reason AS reject_code),
                CAST(:side AS order_side), :instrument_symbol,
                :quantity, :limit_price, :time_in_force,
                :received_at, :accepted_at, :terminal_at, :account_id_hash, :ledger_balance_id,
                :cum_filled_quantity
            )
            ON CONFLICT DO NOTHING
            """;

    private static final String SELECT_BY_ID_SQL = """
            SELECT id, account_id, client_idempotency_key, shard_id, version,
                   status::text AS status,
                   terminal_reason::text AS terminal_reason,
                   side::text AS side,
                   instrument_symbol, quantity, limit_price, time_in_force,
                   received_at, accepted_at, terminal_at, account_id_hash, ledger_balance_id,
                   cum_filled_quantity
            FROM orders WHERE id = :id
            """;

    private static final String SELECT_BY_IDEMPOTENCY_SQL = """
            SELECT id, account_id, client_idempotency_key, shard_id, version,
                   status::text AS status,
                   terminal_reason::text AS terminal_reason,
                   side::text AS side,
                   instrument_symbol, quantity, limit_price, time_in_force,
                   received_at, accepted_at, terminal_at, account_id_hash, ledger_balance_id,
                   cum_filled_quantity
            FROM orders
            WHERE account_id = :account_id AND client_idempotency_key = :key
            """;

    private static final String SELECT_DESK_SNAPSHOT_SQL = """
            SELECT id, account_id, instrument_symbol, side::text AS side, status::text AS status,
                   version, received_at
            FROM orders
            WHERE received_at >= :min_received
            ORDER BY received_at DESC
            LIMIT :limit
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

    private static final String UPDATE_FILL_CAS_SQL = """
            UPDATE orders
               SET cum_filled_quantity = :new_cum_filled,
                   status = CAST(:status AS order_status),
                   terminal_reason = CAST(:terminal_reason AS reject_code),
                   terminal_at = COALESCE(:terminal_at, terminal_at),
                   version = version + 1
             WHERE id = :id AND version = :expected_version
            """;

    public void insert(Order o) {
        int affected = jdbc.update(INSERT_SQL, params(o));
        if (affected == 0) {
            // ON CONFLICT (account_id, client_idempotency_key) suppressed the insert — already exists.
            throw new DuplicateOrderException(
                    "duplicate order for account=%s key=%s".formatted(
                            o.accountId(), o.clientIdempotencyKey()));
        }
    }

    /**
     * Phase 2 projector path: idempotent insert from a cluster-emitted {@link OrderAdmittedEvent}.
     *
     * <p>Unlike {@link #insert(Order)}, this method does not throw on duplicates — the projector reaches
     * here on every fresh admission, and a duplicate is the expected outcome whenever the projector
     * replays past its cursor on restart, or when an integration-test
     * {@code TestPostgresProjectorSingleton} has already projected the same event. {@link #INSERT_SQL}
     * arbitrates on {@code (account_id, client_idempotency_key)}, which is the canonical duplicate
     * check, but a concurrent insert that has already committed the matching pkey row first can race
     * past that arbiter and surface as an {@code orders_pkey} violation. We use a separate
     * {@code INSERT ... ON CONFLICT DO NOTHING} (no target) here so any unique-constraint
     * violation — pkey or {@code (account_id, client_idempotency_key)} — is suppressed idempotently.
     *
     * <p>Returns {@code true} when the row was inserted, {@code false} when ON CONFLICT swallowed it.
     */
    public boolean insertFromAdmittedEvent(OrderAdmittedEvent ev) {
        return jdbc.update(PROJECTOR_INSERT_SQL, projectorParams(ev)) == 1;
    }

    private static MapSqlParameterSource projectorParams(OrderAdmittedEvent ev) {
        BigDecimal quantity = BigDecimal.valueOf(ev.quantityScaled())
                .divide(BigDecimal.valueOf(AcceptOrderCommand.QUANTITY_SCALE), 10, RoundingMode.UNNECESSARY);
        BigDecimal limitPrice = ev.limitPriceScaledOrZero() == 0L
                ? null
                : BigDecimal.valueOf(ev.limitPriceScaledOrZero())
                        .divide(BigDecimal.valueOf(AcceptOrderCommand.PRICE_SCALE), 10, RoundingMode.UNNECESSARY);
        Instant receivedAt = nanosToInstant(ev.clientTimestampNanos());
        // ev.acceptedAtMillis() is the Aeron Cluster timestamp (epoch millis; ConsensusModule.Context
        // default TimeUnit.MILLISECONDS — verified in OmsClusterNodeBootstrap.buildConsensusModuleContext
        // which does not override timeUnit). Earlier code mistakenly fed it through nanosToInstant, which
        // collapsed accepted_at to year 1970. Phase 4j slice closes the OrderAdmittedEvent rename and the
        // matching arithmetic fix in one diff.
        Instant acceptedAt = Instant.ofEpochMilli(ev.acceptedAtMillis());
        UUID accountId = UUID.fromString(ev.accountId());
        return new MapSqlParameterSource()
                .addValue("id", ev.orderId())
                .addValue("account_id", accountId)
                .addValue("client_idempotency_key", ev.clientIdempotencyKey())
                .addValue("shard_id", ev.shardId())
                .addValue("status", OrderStatus.NEW.name())
                .addValue("terminal_reason", null)
                .addValue("side", sideName(ev.side()))
                .addValue("instrument_symbol", ev.instrumentSymbol())
                .addValue("quantity", quantity)
                .addValue("limit_price", limitPrice)
                .addValue("time_in_force", timeInForceName(ev.timeInForceCode()))
                .addValue("received_at", Timestamp.from(receivedAt))
                .addValue("accepted_at", Timestamp.from(acceptedAt))
                .addValue("terminal_at", null)
                .addValue("account_id_hash", ev.accountIdHash())
                .addValue("ledger_balance_id", ev.ledgerBalanceIdOrNull())
                .addValue("cum_filled_quantity", BigDecimal.ZERO);
    }

    private static Instant nanosToInstant(long epochNanos) {
        long seconds = Math.floorDiv(epochNanos, 1_000_000_000L);
        long nanoAdjustment = Math.floorMod(epochNanos, 1_000_000_000L);
        return Instant.ofEpochSecond(seconds, nanoAdjustment);
    }

    private static String sideName(byte sideCode) {
        return switch (sideCode) {
            case AcceptOrderCommand.SIDE_BUY -> Side.BUY.name();
            case AcceptOrderCommand.SIDE_SELL -> Side.SELL.name();
            default -> throw new IllegalArgumentException("unknown side code: " + sideCode);
        };
    }

    private static String timeInForceName(byte tif) {
        return switch (tif) {
            case AcceptOrderCommand.TIF_DAY -> "DAY";
            case AcceptOrderCommand.TIF_IOC -> "IOC";
            case AcceptOrderCommand.TIF_FOK -> "FOK";
            case AcceptOrderCommand.TIF_GTC -> "GTC";
            default -> throw new IllegalArgumentException("unknown time-in-force code: " + tif);
        };
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

    /** Bounded recent rows for desk / attendant snapshot (internal API only). */
    public record DeskSnapshotRow(
            UUID id,
            UUID accountId,
            String instrumentSymbol,
            String side,
            String status,
            int version,
            Instant receivedAt) {}

    public List<DeskSnapshotRow> findDeskSnapshot(Instant minReceived, int limit) {
        var params = new MapSqlParameterSource()
                .addValue("min_received", Timestamp.from(minReceived))
                .addValue("limit", limit);
        return jdbc.query(
                SELECT_DESK_SNAPSHOT_SQL,
                params,
                (rs, rowNum) ->
                        new DeskSnapshotRow(
                                (UUID) rs.getObject("id"),
                                (UUID) rs.getObject("account_id"),
                                rs.getString("instrument_symbol"),
                                rs.getString("side"),
                                rs.getString("status"),
                                rs.getInt("version"),
                                rs.getTimestamp("received_at").toInstant()));
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

    /**
     * Applies a venue fill or cancel outcome with CAS on {@code version}.
     *
     * @param terminalAt pass non-null when transitioning to a terminal non-reject status
     *                     ({@link OrderStatus#FILLED}, {@link OrderStatus#CANCELLED}).
     */
    public boolean updateFillOrCancelWithCas(
            UUID id,
            int expectedVersion,
            BigDecimal newCumFilled,
            OrderStatus newStatus,
            RejectCode terminalReason,
            Instant terminalAt) {
        var params = new MapSqlParameterSource()
                .addValue("id", id)
                .addValue("expected_version", expectedVersion)
                .addValue("new_cum_filled", newCumFilled)
                .addValue("status", newStatus.name())
                .addValue("terminal_reason", terminalReason == null ? null : terminalReason.name())
                .addValue("terminal_at", terminalAt == null ? null : Timestamp.from(terminalAt));
        return jdbc.update(UPDATE_FILL_CAS_SQL, params) == 1;
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
                .addValue("account_id_hash", o.accountIdHash())
                .addValue("ledger_balance_id", o.ledgerBalanceId())
                .addValue("cum_filled_quantity", o.cumFilledQuantity());
    }

    private static final RowMapper<Order> ROW_MAPPER = (rs, rowNum) -> {
        BigDecimal limitPrice = rs.getBigDecimal("limit_price");
        Timestamp acceptedAt = rs.getTimestamp("accepted_at");
        Timestamp terminalAt = rs.getTimestamp("terminal_at");
        String terminalReason = rs.getString("terminal_reason");
        String ledgerBalanceId = rs.getString("ledger_balance_id");
        BigDecimal cumFilled = rs.getBigDecimal("cum_filled_quantity");
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
                rs.getString("account_id_hash"),
                ledgerBalanceId,
                cumFilled == null ? BigDecimal.ZERO : cumFilled
        );
    };
}
