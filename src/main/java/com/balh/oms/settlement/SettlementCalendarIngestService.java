package com.balh.oms.settlement;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * Operator-led ingest for {@code settlement_calendar} (gap plan §5.3 Slice 2b-4).
 *
 * <p>Accepts a JSON envelope describing one or more holiday rows and upserts them by the
 * V63 primary key {@code (calendar_id, holiday_date)}. Validation is row-by-row; rejection
 * is per-row but commit is all-or-nothing — same trade-off as
 * {@link InstrumentSettlementProfileIngestService} and for the same reason: a half-applied
 * calendar would silently change the calculator's holiday set for some rows but not others.
 */
@Service
public class SettlementCalendarIngestService {

    private static final Logger log = LoggerFactory.getLogger(SettlementCalendarIngestService.class);

    /**
     * Maximum rows per request. Calendars are tiny (a few dozen holidays per market per
     * year) so this is loose by design; the limit primarily defends against an operator
     * pasting the wrong file by accident.
     */
    static final int MAX_ROWS_PER_REQUEST = 2_000;

    private final SettlementCalendarRepository repository;
    private final ObjectMapper objectMapper;

    public SettlementCalendarIngestService(SettlementCalendarRepository repository, ObjectMapper objectMapper) {
        this.repository = repository;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public Result ingest(byte[] body) {
        if (body == null || body.length == 0) {
            throw new IllegalArgumentException("empty body");
        }
        Envelope envelope;
        try {
            envelope = objectMapper.readValue(body, Envelope.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("invalid JSON: " + e.getMessage());
        }
        if (envelope == null || envelope.rows() == null) {
            throw new IllegalArgumentException("rows[] is required");
        }
        if (envelope.rows().size() > MAX_ROWS_PER_REQUEST) {
            throw new IllegalArgumentException(
                    "too many rows: limit is " + MAX_ROWS_PER_REQUEST + ", received " + envelope.rows().size());
        }
        int inserted = 0;
        int updated = 0;
        List<RejectedRow> rejected = new ArrayList<>();
        for (int i = 0; i < envelope.rows().size(); i++) {
            Row row = envelope.rows().get(i);
            String rejection = validate(row);
            if (rejection != null) {
                rejected.add(new RejectedRow(i, rejection));
                continue;
            }
            try {
                boolean isInsert = repository.upsertHoliday(new SettlementCalendarRepository.HolidayRow(
                        row.calendarId().trim(),
                        row.holidayDate(),
                        row.description() == null ? null : row.description().trim()));
                if (isInsert) {
                    inserted++;
                } else {
                    updated++;
                }
            } catch (RuntimeException e) {
                log.warn("settlement_calendar upsert failed for row index {}", i, e);
                throw new IllegalArgumentException(
                        "row " + i + " rejected by database: " + e.getMessage(), e);
            }
        }
        return new Result(inserted, updated, rejected);
    }

    private String validate(Row row) {
        if (row == null) {
            return "row is null";
        }
        if (row.calendarId() == null || row.calendarId().isBlank()) {
            return "calendarId is required";
        }
        if (row.holidayDate() == null) {
            return "holidayDate is required";
        }
        return null;
    }

    public record Envelope(@JsonProperty("rows") List<Row> rows) {}

    public record Row(
            @JsonProperty("calendarId") String calendarId,
            @JsonProperty("holidayDate") LocalDate holidayDate,
            @JsonProperty("description") String description) {}

    public record RejectedRow(int rowIndex, String reason) {}

    public record Result(int inserted, int updated, List<RejectedRow> rejected) {}
}
