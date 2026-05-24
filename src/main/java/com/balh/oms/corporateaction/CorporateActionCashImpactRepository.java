package com.balh.oms.corporateaction;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class CorporateActionCashImpactRepository {

    private static final String LIST_DUE =
            """
                    SELECT c.id, c.corporate_action_event_id, c.account_id, c.gross_amount, c.net_amount,
                           c.withholding_amount, c.currency, c.payable_date,
                           COALESCE(c.ledger_outbox_enqueued_at IS NOT NULL, FALSE) AS ledger_outbox_enqueued,
                           e.action_type
                    FROM corporate_action_cash_impact c
                    JOIN corporate_action_event e ON e.id = c.corporate_action_event_id
                    WHERE c.ledger_outbox_enqueued_at IS NULL
                      AND (c.payable_date IS NULL OR c.payable_date <= CURRENT_DATE)
                    ORDER BY c.id
                    LIMIT :lim
                    """;

    private static final String MARK_ENQUEUED =
            """
                    UPDATE corporate_action_cash_impact
                    SET ledger_outbox_enqueued = TRUE,
                        ledger_outbox_enqueued_at = NOW()
                    WHERE id = :id AND ledger_outbox_enqueued_at IS NULL
                    """;

    private static final String UPDATE_WITHHOLDING =
            """
                    UPDATE corporate_action_cash_impact
                    SET gross_amount = :gross, net_amount = :net, withholding_amount = :withholding
                    WHERE corporate_action_event_id = :eventId AND account_id = :accountId
                    """;

    private final NamedParameterJdbcTemplate jdbc;

    public CorporateActionCashImpactRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public record DueCashImpactRow(
            long id,
            long corporateActionEventId,
            UUID accountId,
            BigDecimal grossAmount,
            BigDecimal netAmount,
            BigDecimal withholdingAmount,
            String currency,
            LocalDate payableDate,
            boolean ledgerOutboxEnqueued,
            String actionType) {}

    public List<DueCashImpactRow> listDueForLedgerBooking(int limit) {
        return jdbc.query(
                LIST_DUE,
                new MapSqlParameterSource("lim", Math.max(1, limit)),
                (rs, rowNum) ->
                        new DueCashImpactRow(
                                rs.getLong("id"),
                                rs.getLong("corporate_action_event_id"),
                                UUID.fromString(rs.getString("account_id")),
                                rs.getBigDecimal("gross_amount"),
                                rs.getBigDecimal("net_amount"),
                                rs.getBigDecimal("withholding_amount"),
                                rs.getString("currency"),
                                rs.getDate("payable_date") == null
                                        ? null
                                        : rs.getDate("payable_date").toLocalDate(),
                                rs.getBoolean("ledger_outbox_enqueued"),
                                rs.getString("action_type")));
    }

    public int markLedgerOutboxEnqueued(long cashImpactId) {
        return jdbc.update(MARK_ENQUEUED, new MapSqlParameterSource("id", cashImpactId));
    }

    public void updateAmountsWithWithholding(
            long eventId, UUID accountId, BigDecimal gross, BigDecimal net, BigDecimal withholding) {
        jdbc.update(
                UPDATE_WITHHOLDING,
                new MapSqlParameterSource()
                        .addValue("eventId", eventId)
                        .addValue("accountId", accountId)
                        .addValue("gross", gross)
                        .addValue("net", net)
                        .addValue("withholding", withholding));
    }
}
