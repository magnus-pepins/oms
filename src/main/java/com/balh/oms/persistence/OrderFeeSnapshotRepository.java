package com.balh.oms.persistence;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * Postgres-backed read/write for {@code order_fee_snapshots} (V40).
 *
 * <p>Inserts use {@code ON CONFLICT (order_id) DO NOTHING} so the customer-frontend
 * BFF can safely retry on transient OMS failures without overwriting the original
 * quoted fee. {@link SettlementConfirmProcessor#enqueueSettlementLegs} reads via
 * {@link #findByOrderId(UUID)} when computing the fee leg; absence is non-fatal and
 * falls back to {@link com.balh.oms.settlement.StockCommissionCalculator}.
 */
@Repository
public class OrderFeeSnapshotRepository {

    private final NamedParameterJdbcTemplate jdbc;

    public OrderFeeSnapshotRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    private static final String INSERT_SQL = """
            INSERT INTO order_fee_snapshots (
                order_id, fee_amount, fee_currency, fee_balance_indicator,
                fee_tier, fee_source, fee_schedule_id, user_fee_override_id,
                cash_currency, cash_amount, fx_rate
            ) VALUES (
                :order_id, :fee_amount, :fee_currency, :fee_balance_indicator,
                :fee_tier, :fee_source, :fee_schedule_id, :user_fee_override_id,
                :cash_currency, :cash_amount, :fx_rate
            )
            ON CONFLICT (order_id) DO NOTHING
            """;

    private static final String SELECT_BY_ORDER_ID_SQL = """
            SELECT order_id, fee_amount, fee_currency, fee_balance_indicator,
                   fee_tier, fee_source, fee_schedule_id, user_fee_override_id,
                   cash_currency, cash_amount, fx_rate, created_at
            FROM order_fee_snapshots
            WHERE order_id = :order_id
            """;

    /** @return {@code true} if a new row was inserted, {@code false} on duplicate (idempotent retry). */
    public boolean insertIgnoreOnConflict(OrderFeeSnapshot snapshot) {
        var params = new MapSqlParameterSource()
                .addValue("order_id", snapshot.orderId())
                .addValue("fee_amount", snapshot.feeAmount())
                .addValue("fee_currency", snapshot.feeCurrency())
                .addValue("fee_balance_indicator", snapshot.feeBalanceIndicator())
                .addValue("fee_tier", snapshot.feeTier())
                .addValue("fee_source", snapshot.feeSource())
                .addValue("fee_schedule_id", snapshot.feeScheduleId())
                .addValue("user_fee_override_id", snapshot.userFeeOverrideId())
                .addValue("cash_currency", snapshot.cashCurrency())
                .addValue("cash_amount", snapshot.cashAmount())
                .addValue("fx_rate", snapshot.fxRate());
        return jdbc.update(INSERT_SQL, params) == 1;
    }

    public Optional<OrderFeeSnapshot> findByOrderId(UUID orderId) {
        var rows = jdbc.query(
                SELECT_BY_ORDER_ID_SQL,
                new MapSqlParameterSource("order_id", orderId),
                (rs, rowNum) -> {
                    UUID feeScheduleId = (UUID) rs.getObject("fee_schedule_id");
                    UUID userFeeOverrideId = (UUID) rs.getObject("user_fee_override_id");
                    Timestamp createdAt = rs.getTimestamp("created_at");
                    BigDecimal feeAmount = rs.getBigDecimal("fee_amount");
                    return new OrderFeeSnapshot(
                            (UUID) rs.getObject("order_id"),
                            feeAmount,
                            rs.getString("fee_currency"),
                            rs.getString("fee_balance_indicator"),
                            rs.getString("fee_tier"),
                            rs.getString("fee_source"),
                            feeScheduleId,
                            userFeeOverrideId,
                            rs.getString("cash_currency"),
                            rs.getBigDecimal("cash_amount"),
                            rs.getBigDecimal("fx_rate"),
                            createdAt == null ? null : createdAt.toInstant());
                });
        return rows.stream().findFirst();
    }
}
