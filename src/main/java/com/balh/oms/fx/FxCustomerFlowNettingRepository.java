package com.balh.oms.fx;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class FxCustomerFlowNettingRepository {

    private static final int AMOUNT_SCALE = 8;

    public record BucketRow(
            long id,
            String pair,
            String baseCurrency,
            String quoteCurrency,
            Instant windowStart,
            Instant windowEnd,
            BigDecimal netBaseAmount,
            BigDecimal netQuoteAmount,
            int flowCount,
            String status) {}

    private final NamedParameterJdbcTemplate jdbc;

    public FxCustomerFlowNettingRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void addFlow(
            String pair,
            String baseCurrency,
            String quoteCurrency,
            Instant windowStart,
            Instant windowEnd,
            BigDecimal netBaseDelta,
            BigDecimal netQuoteDelta) {
        jdbc.update(
                """
                        INSERT INTO fx_customer_flow_netting_bucket (
                            pair, base_currency, quote_currency, window_start, window_end,
                            net_base_amount, net_quote_amount, flow_count, status
                        ) VALUES (
                            :pair, :baseCcy, :quoteCcy, :windowStart, :windowEnd,
                            :netBase, :netQuote, 1, 'open'
                        )
                        ON CONFLICT (pair, window_start) DO UPDATE SET
                            net_base_amount = fx_customer_flow_netting_bucket.net_base_amount + EXCLUDED.net_base_amount,
                            net_quote_amount = fx_customer_flow_netting_bucket.net_quote_amount + EXCLUDED.net_quote_amount,
                            flow_count = fx_customer_flow_netting_bucket.flow_count + 1
                        """,
                new MapSqlParameterSource()
                        .addValue("pair", pair)
                        .addValue("baseCcy", baseCurrency)
                        .addValue("quoteCcy", quoteCurrency)
                        .addValue("windowStart", Timestamp.from(windowStart))
                        .addValue("windowEnd", Timestamp.from(windowEnd))
                        .addValue("netBase", netBaseDelta.setScale(AMOUNT_SCALE, java.math.RoundingMode.HALF_UP))
                        .addValue("netQuote", netQuoteDelta.setScale(AMOUNT_SCALE, java.math.RoundingMode.HALF_UP)));
    }

    public List<BucketRow> listOpen() {
        return jdbc.query(
                """
                        SELECT id, pair, base_currency, quote_currency, window_start, window_end,
                               net_base_amount, net_quote_amount, flow_count, status
                        FROM fx_customer_flow_netting_bucket
                        WHERE status = 'open'
                        ORDER BY window_end DESC, pair ASC
                        LIMIT 50
                        """,
                new MapSqlParameterSource(),
                (rs, rowNum) ->
                        new BucketRow(
                                rs.getLong("id"),
                                rs.getString("pair"),
                                rs.getString("base_currency"),
                                rs.getString("quote_currency"),
                                rs.getTimestamp("window_start").toInstant(),
                                rs.getTimestamp("window_end").toInstant(),
                                rs.getBigDecimal("net_base_amount"),
                                rs.getBigDecimal("net_quote_amount"),
                                rs.getInt("flow_count"),
                                rs.getString("status")));
    }

    public int closeExpiredWindows(Instant now) {
        return jdbc.update(
                """
                        UPDATE fx_customer_flow_netting_bucket
                        SET status = 'closed'
                        WHERE status = 'open' AND window_end <= :now
                        """,
                new MapSqlParameterSource("now", Timestamp.from(now)));
    }
}
