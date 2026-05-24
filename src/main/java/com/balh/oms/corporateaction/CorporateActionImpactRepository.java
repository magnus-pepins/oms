package com.balh.oms.corporateaction;

import java.math.BigDecimal;
import java.time.LocalDate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

/** Persistence for CA entitlement / impact tables (Flyway V75). */
@Repository
public class CorporateActionImpactRepository {

    private static final String INSERT_ENTITLEMENT =
            """
                    INSERT INTO corporate_action_entitlement (
                        corporate_action_event_id, account_id, instrument_symbol,
                        quantity_held, entitlement_quantity, entitlement_amount, entitlement_currency
                    ) VALUES (
                        :eventId, :accountId, :symbol, :qtyHeld, :entQty, :entAmt, :entCcy
                    )
                    ON CONFLICT (corporate_action_event_id, account_id) DO NOTHING
                    """;

    private static final String INSERT_POSITION_IMPACT =
            """
                    INSERT INTO corporate_action_position_impact (
                        corporate_action_event_id, account_id, instrument_symbol,
                        quantity_before, quantity_after
                    ) VALUES (
                        :eventId, :accountId, :symbol, :qtyBefore, :qtyAfter
                    )
                    ON CONFLICT (corporate_action_event_id, account_id) DO NOTHING
                    """;

    private static final String INSERT_CASH_IMPACT =
            """
                    INSERT INTO corporate_action_cash_impact (
                        corporate_action_event_id, account_id, gross_amount, net_amount,
                        currency, payable_date, withholding_amount
                    ) VALUES (
                        :eventId, :accountId, :gross, :net, :ccy, :payableDate, :withholding
                    )
                    ON CONFLICT (corporate_action_event_id, account_id) DO NOTHING
                    """;

    private final NamedParameterJdbcTemplate jdbc;

    public CorporateActionImpactRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public int insertEntitlement(
            long eventId,
            java.util.UUID accountId,
            String symbol,
            BigDecimal qtyHeld,
            BigDecimal entQty,
            BigDecimal entAmt,
            String currency) {
        return jdbc.update(
                INSERT_ENTITLEMENT,
                new MapSqlParameterSource()
                        .addValue("eventId", eventId)
                        .addValue("accountId", accountId)
                        .addValue("symbol", symbol)
                        .addValue("qtyHeld", qtyHeld)
                        .addValue("entQty", entQty)
                        .addValue("entAmt", entAmt)
                        .addValue("entCcy", currency));
    }

    public int insertPositionImpact(
            long eventId,
            java.util.UUID accountId,
            String symbol,
            BigDecimal qtyBefore,
            BigDecimal qtyAfter) {
        return jdbc.update(
                INSERT_POSITION_IMPACT,
                new MapSqlParameterSource()
                        .addValue("eventId", eventId)
                        .addValue("accountId", accountId)
                        .addValue("symbol", symbol)
                        .addValue("qtyBefore", qtyBefore)
                        .addValue("qtyAfter", qtyAfter));
    }

    public int insertCashImpact(
            long eventId,
            java.util.UUID accountId,
            BigDecimal gross,
            BigDecimal net,
            String currency,
            LocalDate payableDate) {
        return insertCashImpactWithWithholding(eventId, accountId, gross, net, null, currency, payableDate);
    }

    public int insertCashImpactWithWithholding(
            long eventId,
            java.util.UUID accountId,
            BigDecimal gross,
            BigDecimal net,
            BigDecimal withholding,
            String currency,
            LocalDate payableDate) {
        return jdbc.update(
                INSERT_CASH_IMPACT,
                new MapSqlParameterSource()
                        .addValue("eventId", eventId)
                        .addValue("accountId", accountId)
                        .addValue("gross", gross)
                        .addValue("net", net)
                        .addValue("ccy", currency)
                        .addValue("payableDate", payableDate == null ? null : java.sql.Date.valueOf(payableDate))
                        .addValue("withholding", withholding));
    }
}
