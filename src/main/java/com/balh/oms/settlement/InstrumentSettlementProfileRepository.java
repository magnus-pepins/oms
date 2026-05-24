package com.balh.oms.settlement;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.stereotype.Repository;

import java.sql.Date;
import java.util.List;
import java.util.Optional;

/**
 * Persistence for {@code instrument_settlement_profile} (Flyway V61, gap plan §5.3
 * Slice 2b-1).
 *
 * <p>Read-only on the hot path: {@link #findActiveBySymbol} is called from
 * {@link SettlementDateCalculator#resolveExpectedSettlementDate} at every trade
 * projection, so the symbol+effective-from index added in V61 is the only thing keeping
 * the lookup off the executions write path. Writes ({@link #insert}) are operator-led
 * only (CSV ingest, marketdata-platform sync); the projector never inserts.
 */
@Repository
public class InstrumentSettlementProfileRepository implements SettlementProfileLookup {

    private static final String SELECT_COLUMNS =
            """
                    SELECT id, instrument_id, symbol, isin, primary_mic,
                           settlement_calendar_id, settlement_cycle, settlement_currency,
                           isk_eligible, effective_from, effective_to
                    """;

    /**
     * Returns the single effective-dated row for {@code symbol} that contains {@code asOf}.
     * Half-open upper bound matches the V61 CHECK constraint: {@code effective_to > effective_from},
     * so an EU T+1 migration on {@code 2027-10-11} reads as one closing row with
     * {@code effective_to=2027-10-11} and one opening row with {@code effective_from=2027-10-11}
     * without any overlap.
     *
     * <p>ORDER BY + LIMIT 1 is a defensive guard, not a correctness device: the V61 UNIQUE
     * constraint on {@code (instrument_id, effective_from)} plus the half-open window CHECK
     * already prevent overlap for a given instrument_id. The constraint does not extend to
     * the {@code symbol} column (which is not effective-dated against instrument_id), so we
     * order by {@code effective_from DESC} to deterministically pick the most recently opened
     * row if an operator ever uses the same symbol across two instrument_ids.
     */
    private static final String FIND_ACTIVE_BY_SYMBOL =
            SELECT_COLUMNS
                    + """
                    FROM instrument_settlement_profile
                    WHERE symbol = :symbol
                      AND effective_from <= :asOf
                      AND (effective_to IS NULL OR effective_to > :asOf)
                    ORDER BY effective_from DESC
                    LIMIT 1
                    """;

    private static final String INSERT =
            """
                    INSERT INTO instrument_settlement_profile (
                        instrument_id, symbol, isin, primary_mic,
                        settlement_calendar_id, settlement_cycle, settlement_currency,
                        isk_eligible, effective_from, effective_to
                    ) VALUES (
                        :instrumentId, :symbol, :isin, :primaryMic,
                        :settlementCalendarId, :settlementCycle, :settlementCurrency,
                        :iskEligible, :effectiveFrom, :effectiveTo
                    )
                    """;

    private static final String UPSERT =
            """
                    INSERT INTO instrument_settlement_profile (
                        instrument_id, symbol, isin, primary_mic,
                        settlement_calendar_id, settlement_cycle, settlement_currency,
                        isk_eligible, effective_from, effective_to
                    ) VALUES (
                        :instrumentId, :symbol, :isin, :primaryMic,
                        :settlementCalendarId, :settlementCycle, :settlementCurrency,
                        :iskEligible, :effectiveFrom, :effectiveTo
                    )
                    ON CONFLICT (instrument_id, effective_from) DO UPDATE
                    SET symbol = EXCLUDED.symbol,
                        isin = EXCLUDED.isin,
                        primary_mic = EXCLUDED.primary_mic,
                        settlement_calendar_id = EXCLUDED.settlement_calendar_id,
                        settlement_cycle = EXCLUDED.settlement_cycle,
                        settlement_currency = EXCLUDED.settlement_currency,
                        isk_eligible = EXCLUDED.isk_eligible,
                        effective_to = EXCLUDED.effective_to
                    """;

    private static final String COUNT_BY_KEY =
            """
                    SELECT COUNT(*)::int
                    FROM instrument_settlement_profile
                    WHERE instrument_id = :instrumentId AND effective_from = :effectiveFrom
                    """;

    private final NamedParameterJdbcTemplate jdbc;

    public InstrumentSettlementProfileRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public Optional<InstrumentSettlementProfile> findActiveBySymbol(String symbol, java.time.LocalDate asOf) {
        if (symbol == null || symbol.isBlank() || asOf == null) {
            return Optional.empty();
        }
        List<InstrumentSettlementProfile> rows = jdbc.query(
                FIND_ACTIVE_BY_SYMBOL,
                new MapSqlParameterSource().addValue("symbol", symbol).addValue("asOf", Date.valueOf(asOf)),
                ROW_MAPPER);
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.getFirst());
    }

    /**
     * Operator-only insert (CSV ingest, marketdata-platform sync). Not called by the
     * projector. Returns the surrogate id.
     */
    public long insert(InsertCommand cmd) {
        var params = new MapSqlParameterSource()
                .addValue("instrumentId", cmd.instrumentId())
                .addValue("symbol", cmd.symbol())
                .addValue("isin", cmd.isin())
                .addValue("primaryMic", cmd.primaryMic())
                .addValue("settlementCalendarId", cmd.settlementCalendarId())
                .addValue("settlementCycle", cmd.settlementCycle())
                .addValue("settlementCurrency", cmd.settlementCurrency())
                .addValue("iskEligible", cmd.iskEligible())
                .addValue("effectiveFrom", Date.valueOf(cmd.effectiveFrom()))
                .addValue("effectiveTo", cmd.effectiveTo() == null ? null : Date.valueOf(cmd.effectiveTo()));
        var kh = new GeneratedKeyHolder();
        jdbc.update(INSERT, params, kh, new String[] {"id"});
        Number key = kh.getKey();
        if (key == null) {
            throw new IllegalStateException("instrument_settlement_profile insert returned no id");
        }
        return key.longValue();
    }

    /**
     * Operator-only upsert keyed on the V61 UNIQUE {@code (instrument_id, effective_from)}.
     * Returns {@code true} when a new row was inserted, {@code false} when an existing row
     * was updated. The natural key is checked with a pre-count rather than RETURNING
     * {@code xmax = 0} so the response stays driver-portable.
     *
     * <p>Updates intentionally do <em>not</em> rewrite {@code effective_from} (it's the key
     * half) nor the surrogate {@code id}. Everything else on the row is rewritten from the
     * incoming command — this matches operator expectation when re-importing a corrected
     * CSV ("the file is the source of truth for this key").
     */
    public boolean upsert(InsertCommand cmd) {
        if (cmd == null
                || cmd.instrumentId() == null || cmd.instrumentId().isBlank()
                || cmd.symbol() == null || cmd.symbol().isBlank()
                || cmd.primaryMic() == null || cmd.primaryMic().isBlank()
                || cmd.settlementCalendarId() == null || cmd.settlementCalendarId().isBlank()
                || cmd.settlementCycle() == null || cmd.settlementCycle().isBlank()
                || cmd.settlementCurrency() == null || cmd.settlementCurrency().isBlank()
                || cmd.effectiveFrom() == null) {
            throw new IllegalArgumentException(
                    "instrumentId, symbol, primaryMic, settlementCalendarId, settlementCycle, settlementCurrency, effectiveFrom are required");
        }
        Integer existing = jdbc.queryForObject(
                COUNT_BY_KEY,
                new MapSqlParameterSource()
                        .addValue("instrumentId", cmd.instrumentId())
                        .addValue("effectiveFrom", Date.valueOf(cmd.effectiveFrom())),
                Integer.class);
        var params = new MapSqlParameterSource()
                .addValue("instrumentId", cmd.instrumentId())
                .addValue("symbol", cmd.symbol())
                .addValue("isin", cmd.isin())
                .addValue("primaryMic", cmd.primaryMic())
                .addValue("settlementCalendarId", cmd.settlementCalendarId())
                .addValue("settlementCycle", cmd.settlementCycle())
                .addValue("settlementCurrency", cmd.settlementCurrency())
                .addValue("iskEligible", cmd.iskEligible())
                .addValue("effectiveFrom", Date.valueOf(cmd.effectiveFrom()))
                .addValue("effectiveTo", cmd.effectiveTo() == null ? null : Date.valueOf(cmd.effectiveTo()));
        jdbc.update(UPSERT, params);
        return existing != null && existing == 0;
    }

    public record InsertCommand(
            String instrumentId,
            String symbol,
            String isin,
            String primaryMic,
            String settlementCalendarId,
            String settlementCycle,
            String settlementCurrency,
            boolean iskEligible,
            java.time.LocalDate effectiveFrom,
            java.time.LocalDate effectiveTo) {}

    private static final RowMapper<InstrumentSettlementProfile> ROW_MAPPER = (rs, rowNum) -> {
        java.sql.Date effTo = rs.getDate("effective_to");
        return new InstrumentSettlementProfile(
                rs.getLong("id"),
                rs.getString("instrument_id"),
                rs.getString("symbol"),
                rs.getString("isin"),
                rs.getString("primary_mic"),
                rs.getString("settlement_calendar_id"),
                rs.getString("settlement_cycle"),
                rs.getString("settlement_currency"),
                rs.getBoolean("isk_eligible"),
                rs.getDate("effective_from").toLocalDate(),
                effTo == null ? null : effTo.toLocalDate());
    };
}
