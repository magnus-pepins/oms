package com.balh.oms.settlement;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.util.List;

/**
 * Persistence for {@code broker_trade_confirm_fee} (gap plan §5.12 / Flyway V54).
 *
 * <p>Fees have no business unique constraint; a single broker confirm may carry
 * multiple fee rows of the same {@code fee_type}. Re-applying a duplicate batch
 * is prevented at the confirm-row level (see {@link BrokerTradeConfirmRepository}),
 * so the ingest service only inserts fees for newly-created confirms.
 */
@Repository
public class BrokerTradeConfirmFeeRepository {

    private static final String INSERT =
            """
                    INSERT INTO broker_trade_confirm_fee (
                        confirm_id, fee_type, amount, currency, charged_to
                    ) VALUES (
                        :confirmId, :feeType, :amount, :currency, :chargedTo
                    )
                    """;

    private final NamedParameterJdbcTemplate jdbc;

    public BrokerTradeConfirmFeeRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public int insertBatch(long confirmId, List<BrokerTradeConfirmFeeRow> fees) {
        if (fees == null || fees.isEmpty()) {
            return 0;
        }
        SqlParameterSource[] params = new SqlParameterSource[fees.size()];
        for (int i = 0; i < fees.size(); i++) {
            BrokerTradeConfirmFeeRow fee = fees.get(i);
            BigDecimal amount = fee.amount();
            String currency = fee.currency();
            String type = fee.type();
            String chargedTo = fee.chargedTo() == null || fee.chargedTo().isBlank() ? "customer" : fee.chargedTo();
            if (amount == null || currency == null || type == null) {
                throw new IllegalArgumentException(
                        "broker_trade_confirm_fee row missing required fields (type, amount, currency)");
            }
            params[i] = new MapSqlParameterSource()
                    .addValue("confirmId", confirmId)
                    .addValue("feeType", type)
                    .addValue("amount", amount)
                    .addValue("currency", currency)
                    .addValue("chargedTo", chargedTo);
        }
        int[] counts = jdbc.batchUpdate(INSERT, params);
        int total = 0;
        for (int c : counts) {
            total += Math.max(0, c);
        }
        return total;
    }

    /** Sum of customer-charged fee amounts for matcher diff (OMS fee snapshot not wired yet). */
    public java.math.BigDecimal sumCustomerFeeAmount(long confirmId) {
        java.math.BigDecimal sum = jdbc.queryForObject(
                """
                        SELECT COALESCE(SUM(amount), 0)
                        FROM broker_trade_confirm_fee
                        WHERE confirm_id = :confirmId AND charged_to = 'customer'
                        """,
                new MapSqlParameterSource("confirmId", confirmId),
                java.math.BigDecimal.class);
        return sum == null ? java.math.BigDecimal.ZERO : sum;
    }
}
