package com.balh.oms.settlement;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

/**
 * Persistence for {@code broker_trade_confirm} (gap plan §5.1 / Flyway V54).
 *
 * <p>Idempotency: unique on {@code (broker_id, broker_trade_id)}. Re-applying the
 * same parsed row is a no-op via {@code ON CONFLICT DO NOTHING}; the generated key
 * is therefore absent for duplicates, and the caller treats that as "already inserted".
 *
 * <p>Matching (gap plan §5.2) lands in Slice 3: this repo deliberately leaves
 * {@code resolved_execution_id} / {@code match_status} at their defaults
 * ({@code NULL} / {@code 'pending'}).
 */
@Repository
public class BrokerTradeConfirmRepository {

    private static final String INSERT_IGNORE =
            """
                    INSERT INTO broker_trade_confirm (
                        batch_id, broker_id, broker_trade_id, venue_exec_ref,
                        account_id, broker_account, custody_account_id,
                        instrument_symbol, instrument_isin, instrument_mic, instrument_currency,
                        side, quantity, price, gross_amount,
                        trade_date, settlement_date, settlement_currency,
                        broker_status, correction_type, raw_row_json
                    ) VALUES (
                        :batchId, :brokerId, :brokerTradeId, :venueExecRef,
                        :accountId, :brokerAccount, :custodyAccountId,
                        :instrumentSymbol, :instrumentIsin, :instrumentMic, :instrumentCurrency,
                        :side, :quantity, :price, :grossAmount,
                        :tradeDate, :settlementDate, :settlementCurrency,
                        :brokerStatus, :correctionType, CAST(:rawRowJson AS JSONB)
                    )
                    ON CONFLICT (broker_id, broker_trade_id) DO NOTHING
                    """;

    private final NamedParameterJdbcTemplate jdbc;

    public BrokerTradeConfirmRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * @return new confirm id when inserted, {@code null} when the unique constraint
     *         {@code (broker_id, broker_trade_id)} already had a row.
     */
    public Long insertIgnore(InsertCommand cmd) {
        SqlParameterSource params = new MapSqlParameterSource()
                .addValue("batchId", cmd.batchId())
                .addValue("brokerId", cmd.brokerId())
                .addValue("brokerTradeId", cmd.brokerTradeId())
                .addValue("venueExecRef", cmd.venueExecRef())
                .addValue("accountId", cmd.accountId())
                .addValue("brokerAccount", cmd.brokerAccount())
                .addValue("custodyAccountId", cmd.custodyAccountId())
                .addValue("instrumentSymbol", cmd.instrumentSymbol())
                .addValue("instrumentIsin", cmd.instrumentIsin())
                .addValue("instrumentMic", cmd.instrumentMic())
                .addValue("instrumentCurrency", cmd.instrumentCurrency())
                .addValue("side", cmd.side())
                .addValue("quantity", cmd.quantity())
                .addValue("price", cmd.price())
                .addValue("grossAmount", cmd.grossAmount())
                .addValue("tradeDate", cmd.tradeDate() == null ? null : Date.valueOf(cmd.tradeDate()))
                .addValue(
                        "settlementDate",
                        cmd.settlementDate() == null ? null : Date.valueOf(cmd.settlementDate()))
                .addValue("settlementCurrency", cmd.settlementCurrency())
                .addValue("brokerStatus", cmd.brokerStatus())
                .addValue("correctionType", cmd.correctionType())
                .addValue("rawRowJson", cmd.rawRowJson());
        var kh = new GeneratedKeyHolder();
        jdbc.update(INSERT_IGNORE, params, kh, new String[] {"id"});
        Number key = kh.getKey();
        return key == null ? null : key.longValue();
    }

    public record InsertCommand(
            long batchId,
            String brokerId,
            String brokerTradeId,
            String venueExecRef,
            UUID accountId,
            String brokerAccount,
            UUID custodyAccountId,
            String instrumentSymbol,
            String instrumentIsin,
            String instrumentMic,
            String instrumentCurrency,
            String side,
            BigDecimal quantity,
            BigDecimal price,
            BigDecimal grossAmount,
            LocalDate tradeDate,
            LocalDate settlementDate,
            String settlementCurrency,
            String brokerStatus,
            String correctionType,
            String rawRowJson) {}

    /**
     * Matcher-facing projection. Carries the economic fields needed to compare a broker
     * confirm row against an OMS {@code executions} row (gap plan §5.2). Raw JSON / fees
     * are not loaded here — the matcher only reads typed fields.
     */
    public record MatchableRow(
            long id,
            String brokerId,
            String brokerTradeId,
            String venueExecRef,
            UUID accountId,
            String instrumentSymbol,
            String instrumentIsin,
            String side,
            BigDecimal quantity,
            BigDecimal price,
            BigDecimal grossAmount,
            LocalDate tradeDate,
            LocalDate settlementDate,
            String correctionType) {}

    private static final String SELECT_MATCHABLE_COLUMNS =
            """
                    SELECT id, broker_id, broker_trade_id, venue_exec_ref,
                           account_id, instrument_symbol, instrument_isin, side,
                           quantity, price, gross_amount,
                           trade_date, settlement_date, correction_type
                    """;

    private static final String LOCK_PENDING_BATCH =
            SELECT_MATCHABLE_COLUMNS
                    + """
                    FROM broker_trade_confirm
                    WHERE match_status = 'pending'
                    ORDER BY created_at, id
                    LIMIT :lim
                    FOR UPDATE SKIP LOCKED
                    """;

    private static final String LOCK_PENDING_BY_ID =
            SELECT_MATCHABLE_COLUMNS
                    + """
                    FROM broker_trade_confirm
                    WHERE id = :id AND match_status = 'pending'
                    FOR UPDATE
                    """;

    private static final String MARK_MATCHED =
            """
                    UPDATE broker_trade_confirm
                    SET match_status = 'matched',
                        resolved_execution_id = :executionId,
                        match_decided_at = NOW(),
                        match_diff_json = CAST(:diffJson AS JSONB)
                    WHERE id = :id AND match_status = 'pending'
                    """;

    private static final String MARK_MISMATCH =
            """
                    UPDATE broker_trade_confirm
                    SET match_status = 'mismatch',
                        resolved_execution_id = :executionId,
                        match_decided_at = NOW(),
                        match_diff_json = CAST(:diffJson AS JSONB)
                    WHERE id = :id AND match_status = 'pending'
                    """;

    private static final String MARK_UNRESOLVED =
            """
                    UPDATE broker_trade_confirm
                    SET match_status = 'unresolved',
                        match_decided_at = NOW(),
                        match_diff_json = CAST(:diffJson AS JSONB)
                    WHERE id = :id AND match_status = 'pending'
                    """;

    public List<MatchableRow> lockPendingBatch(int limit) {
        return jdbc.query(LOCK_PENDING_BATCH, new MapSqlParameterSource("lim", limit), MATCHABLE_ROW_MAPPER);
    }

    public java.util.Optional<MatchableRow> lockPendingById(long id) {
        List<MatchableRow> rows =
                jdbc.query(LOCK_PENDING_BY_ID, new MapSqlParameterSource("id", id), MATCHABLE_ROW_MAPPER);
        return rows.isEmpty() ? java.util.Optional.empty() : java.util.Optional.of(rows.getFirst());
    }

    public int markMatched(long id, long executionId, String diffJson) {
        return jdbc.update(
                MARK_MATCHED,
                new MapSqlParameterSource()
                        .addValue("id", id)
                        .addValue("executionId", executionId)
                        .addValue("diffJson", diffJson));
    }

    public int markMismatch(long id, Long executionId, String diffJson) {
        return jdbc.update(
                MARK_MISMATCH,
                new MapSqlParameterSource()
                        .addValue("id", id)
                        .addValue("executionId", executionId)
                        .addValue("diffJson", diffJson));
    }

    public int markUnresolved(long id, String diffJson) {
        return jdbc.update(
                MARK_UNRESOLVED,
                new MapSqlParameterSource().addValue("id", id).addValue("diffJson", diffJson));
    }

    private static final org.springframework.jdbc.core.RowMapper<MatchableRow> MATCHABLE_ROW_MAPPER = (rs, rowNum) -> {
        java.sql.Date td = rs.getDate("trade_date");
        java.sql.Date sd = rs.getDate("settlement_date");
        Object acct = rs.getObject("account_id");
        return new MatchableRow(
                rs.getLong("id"),
                rs.getString("broker_id"),
                rs.getString("broker_trade_id"),
                rs.getString("venue_exec_ref"),
                acct == null ? null : (UUID) acct,
                rs.getString("instrument_symbol"),
                rs.getString("instrument_isin"),
                rs.getString("side"),
                rs.getBigDecimal("quantity"),
                rs.getBigDecimal("price"),
                rs.getBigDecimal("gross_amount"),
                td == null ? null : td.toLocalDate(),
                sd == null ? null : sd.toLocalDate(),
                rs.getString("correction_type"));
    };
}
