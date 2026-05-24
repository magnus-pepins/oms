package com.balh.oms.settlement;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Repository
public class BrokerPositionSnapshotRowRepository {

    public record SnapshotRow(
            long id,
            long batchId,
            String brokerId,
            String brokerAccount,
            UUID accountId,
            UUID custodyAccountId,
            String instrumentSymbol,
            BigDecimal quantityTotal,
            BigDecimal quantitySettled,
            BigDecimal quantityPendingBuySettle,
            BigDecimal quantityPendingSellSettle) {}

    private static final String INSERT_IGNORE =
            """
                    INSERT INTO broker_position_snapshot_row (
                        batch_id, broker_id, broker_account, account_id, custody_account_id,
                        instrument_symbol, instrument_isin, instrument_currency,
                        quantity_total, quantity_settled,
                        quantity_pending_buy_settle, quantity_pending_sell_settle,
                        as_of, raw_row_json
                    ) VALUES (
                        :batchId, :brokerId, :brokerAccount, :accountId, :custodyAccountId,
                        :instrumentSymbol, :instrumentIsin, :instrumentCurrency,
                        :quantityTotal, :quantitySettled,
                        :quantityPendingBuySettle, :quantityPendingSellSettle,
                        :asOf, CAST(:rawRowJson AS JSONB)
                    )
                    ON CONFLICT DO NOTHING
                    """;

    private static final String LIST_BY_BATCH =
            """
                    SELECT id, batch_id, broker_id, broker_account, account_id, custody_account_id,
                           instrument_symbol, quantity_total, quantity_settled,
                           quantity_pending_buy_settle, quantity_pending_sell_settle
                    FROM broker_position_snapshot_row
                    WHERE batch_id = :batchId
                    ORDER BY id
                    """;

    private final NamedParameterJdbcTemplate jdbc;

    public BrokerPositionSnapshotRowRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public Long insertIgnore(InsertCommand cmd) {
        var kh = new GeneratedKeyHolder();
        jdbc.update(
                INSERT_IGNORE,
                new MapSqlParameterSource()
                        .addValue("batchId", cmd.batchId())
                        .addValue("brokerId", cmd.brokerId())
                        .addValue("brokerAccount", cmd.brokerAccount())
                        .addValue("accountId", cmd.accountId())
                        .addValue("custodyAccountId", cmd.custodyAccountId())
                        .addValue("instrumentSymbol", cmd.instrumentSymbol())
                        .addValue("instrumentIsin", cmd.instrumentIsin())
                        .addValue("instrumentCurrency", cmd.instrumentCurrency())
                        .addValue("quantityTotal", cmd.quantityTotal())
                        .addValue("quantitySettled", cmd.quantitySettled())
                        .addValue("quantityPendingBuySettle", cmd.quantityPendingBuySettle())
                        .addValue("quantityPendingSellSettle", cmd.quantityPendingSellSettle())
                        .addValue("asOf", cmd.asOf() == null ? null : Timestamp.from(cmd.asOf()))
                        .addValue("rawRowJson", cmd.rawRowJson()),
                kh,
                new String[] {"id"});
        Number key = kh.getKey();
        return key == null ? null : key.longValue();
    }

    public List<SnapshotRow> listByBatch(long batchId) {
        return jdbc.query(LIST_BY_BATCH, new MapSqlParameterSource("batchId", batchId), (rs, rowNum) ->
                new SnapshotRow(
                        rs.getLong("id"),
                        rs.getLong("batch_id"),
                        rs.getString("broker_id"),
                        rs.getString("broker_account"),
                        (UUID) rs.getObject("account_id"),
                        (UUID) rs.getObject("custody_account_id"),
                        rs.getString("instrument_symbol"),
                        rs.getBigDecimal("quantity_total"),
                        rs.getBigDecimal("quantity_settled"),
                        rs.getBigDecimal("quantity_pending_buy_settle"),
                        rs.getBigDecimal("quantity_pending_sell_settle")));
    }

    public record InsertCommand(
            long batchId,
            String brokerId,
            String brokerAccount,
            UUID accountId,
            UUID custodyAccountId,
            String instrumentSymbol,
            String instrumentIsin,
            String instrumentCurrency,
            BigDecimal quantityTotal,
            BigDecimal quantitySettled,
            BigDecimal quantityPendingBuySettle,
            BigDecimal quantityPendingSellSettle,
            Instant asOf,
            String rawRowJson) {}
}
