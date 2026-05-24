package com.balh.oms.settlement;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Date;
import java.time.LocalDate;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Persistence for {@code settlement_calendar} (Flyway V63, gap plan §5.3 Slice 2b-3).
 *
 * <p>Read path ({@link #findHolidaysInRange}) is on the trade-projection hot path,
 * indexed by the V63 {@code (calendar_id, holiday_date)} primary key. Write path
 * ({@link #upsertHoliday}, {@link #upsertHolidays}) is operator-led only (JSON ingest
 * endpoint, Slice 2b-4); the projector never writes.
 */
@Repository
public class SettlementCalendarRepository implements SettlementCalendarLookup {

    private static final String FIND_HOLIDAYS_IN_RANGE =
            """
                    SELECT calendar_id, holiday_date, description
                    FROM settlement_calendar
                    WHERE calendar_id = :calendarId
                      AND holiday_date BETWEEN :from AND :to
                    ORDER BY holiday_date
                    """;

    private static final String UPSERT =
            """
                    INSERT INTO settlement_calendar (calendar_id, holiday_date, description)
                    VALUES (:calendarId, :holidayDate, :description)
                    ON CONFLICT (calendar_id, holiday_date) DO UPDATE
                    SET description = EXCLUDED.description
                    """;

    private static final String LIST_BY_CALENDAR =
            """
                    SELECT calendar_id, holiday_date, description
                    FROM settlement_calendar
                    WHERE calendar_id = :calendarId
                    ORDER BY holiday_date
                    """;

    private final NamedParameterJdbcTemplate jdbc;

    public SettlementCalendarRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public Set<LocalDate> findHolidaysInRange(String calendarId, LocalDate from, LocalDate to) {
        if (calendarId == null || calendarId.isBlank() || from == null || to == null || to.isBefore(from)) {
            return Collections.emptySet();
        }
        Set<LocalDate> out = new HashSet<>();
        jdbc.query(
                FIND_HOLIDAYS_IN_RANGE,
                new MapSqlParameterSource()
                        .addValue("calendarId", calendarId)
                        .addValue("from", Date.valueOf(from))
                        .addValue("to", Date.valueOf(to)),
                rs -> {
                    out.add(rs.getDate("holiday_date").toLocalDate());
                });
        return out;
    }

    public List<HolidayRow> listByCalendar(String calendarId) {
        if (calendarId == null || calendarId.isBlank()) {
            return List.of();
        }
        return jdbc.query(
                LIST_BY_CALENDAR,
                new MapSqlParameterSource("calendarId", calendarId),
                ROW_MAPPER);
    }

    /**
     * Operator-only single-row upsert. Returns {@code true} when a new row was inserted,
     * {@code false} when an existing row's description was updated. Used by the JSON
     * ingest endpoint.
     */
    public boolean upsertHoliday(HolidayRow row) {
        if (row == null || row.calendarId() == null || row.calendarId().isBlank() || row.holidayDate() == null) {
            throw new IllegalArgumentException("calendarId and holidayDate are required");
        }
        // Postgres's ON CONFLICT...DO UPDATE returns 1 in either case; we need a pre-check
        // to distinguish insert from update because the ingest response reports both counts.
        Integer existing = jdbc.queryForObject(
                "SELECT COUNT(*)::int FROM settlement_calendar WHERE calendar_id = :calendarId AND holiday_date = :date",
                new MapSqlParameterSource()
                        .addValue("calendarId", row.calendarId())
                        .addValue("date", Date.valueOf(row.holidayDate())),
                Integer.class);
        try {
            jdbc.update(
                    UPSERT,
                    new MapSqlParameterSource()
                            .addValue("calendarId", row.calendarId())
                            .addValue("holidayDate", Date.valueOf(row.holidayDate()))
                            .addValue("description", row.description()));
        } catch (DuplicateKeyException e) {
            // ON CONFLICT should prevent this, but a concurrent ingest could in theory
            // race. Rethrowing keeps the error surface honest for the operator.
            throw e;
        }
        return existing != null && existing == 0;
    }

    /**
     * Bulk upsert. Returns the number of rows that were newly inserted (existing rows are
     * silently updated). All-or-nothing inside a single Spring transaction at the caller.
     */
    public BulkUpsertResult upsertHolidays(Collection<HolidayRow> rows) {
        int inserted = 0;
        int updated = 0;
        for (HolidayRow row : rows) {
            if (upsertHoliday(row)) {
                inserted++;
            } else {
                updated++;
            }
        }
        return new BulkUpsertResult(inserted, updated);
    }

    public record HolidayRow(String calendarId, LocalDate holidayDate, String description) {}

    public record BulkUpsertResult(int inserted, int updated) {}

    private static final RowMapper<HolidayRow> ROW_MAPPER = (rs, rowNum) -> new HolidayRow(
            rs.getString("calendar_id"),
            rs.getDate("holiday_date").toLocalDate(),
            rs.getString("description"));
}
