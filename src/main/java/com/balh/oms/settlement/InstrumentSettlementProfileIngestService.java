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
import java.util.Set;

/**
 * Operator-led ingest for {@code instrument_settlement_profile} (gap plan §5.3 Slice 2b-4).
 *
 * <p>Accepts a JSON envelope describing one or more profile rows and upserts them by the
 * V61 natural key {@code (instrument_id, effective_from)}. Validation happens row-by-row;
 * an invalid row does not poison the rest of the batch — the operator gets a per-row
 * rejection reason in the response and can re-submit a corrected file.
 *
 * <p>The full call runs inside one transaction because all-or-nothing matches operator
 * intent: if the payload is half-malformed, partial application would leave the calculator
 * routing AAPL via XSTO-CAL because only the first 200 of 500 rows landed. Spring's
 * {@link Transactional} rollback semantics give us that for free; the rejection list is
 * returned so the operator can fix the file and retry without first cleaning state.
 */
@Service
public class InstrumentSettlementProfileIngestService {

    private static final Logger log = LoggerFactory.getLogger(InstrumentSettlementProfileIngestService.class);

    /**
     * Maximum number of rows in a single ingest call. Picked to bound transaction duration
     * (~tens of milliseconds per row including the pre-count COUNT(*)::int and the upsert
     * itself; 5_000 rows ≈ a few seconds inside the transaction). Operators with bigger
     * universes should batch on their side; the marketdata-platform sync will chunk in the
     * service rather than in this endpoint.
     */
    static final int MAX_ROWS_PER_REQUEST = 5_000;

    /**
     * Cycle allowlist mirrors V61's CHECK constraint. We validate here so the rejection
     * reason is human-readable ("expected one of [T+0, T+1, T+2, T+3]") rather than a raw
     * Postgres constraint-violation stack trace.
     */
    static final Set<String> ALLOWED_CYCLES = Set.of("T+0", "T+1", "T+2", "T+3");

    private final InstrumentSettlementProfileRepository repository;
    private final ObjectMapper objectMapper;

    public InstrumentSettlementProfileIngestService(
            InstrumentSettlementProfileRepository repository, ObjectMapper objectMapper) {
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
                boolean isInsert = repository.upsert(toCommand(row));
                if (isInsert) {
                    inserted++;
                } else {
                    updated++;
                }
            } catch (RuntimeException e) {
                // Defensive: validation should catch everything reasonable, but a CHECK
                // violation we didn't pre-validate (e.g. effective_to <= effective_from)
                // surfaces here. Roll back the whole batch to keep state consistent; the
                // operator sees the rejection and fixes the file.
                log.warn("instrument_settlement_profile upsert failed for row index {}", i, e);
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
        if (isBlank(row.instrumentId())) {
            return "instrumentId is required";
        }
        if (isBlank(row.symbol())) {
            return "symbol is required";
        }
        if (isBlank(row.primaryMic())) {
            return "primaryMic is required";
        }
        if (isBlank(row.settlementCalendarId())) {
            return "settlementCalendarId is required";
        }
        if (isBlank(row.settlementCycle())) {
            return "settlementCycle is required";
        }
        if (!ALLOWED_CYCLES.contains(row.settlementCycle())) {
            return "settlementCycle must be one of " + ALLOWED_CYCLES + ", got " + row.settlementCycle();
        }
        if (isBlank(row.settlementCurrency())) {
            return "settlementCurrency is required";
        }
        if (row.effectiveFrom() == null) {
            return "effectiveFrom is required";
        }
        if (row.effectiveTo() != null && !row.effectiveTo().isAfter(row.effectiveFrom())) {
            return "effectiveTo must be after effectiveFrom (half-open window)";
        }
        return null;
    }

    private InstrumentSettlementProfileRepository.InsertCommand toCommand(Row row) {
        return new InstrumentSettlementProfileRepository.InsertCommand(
                row.instrumentId().trim(),
                row.symbol().trim(),
                row.isin() == null ? null : row.isin().trim(),
                row.primaryMic().trim(),
                row.settlementCalendarId().trim(),
                row.settlementCycle().trim(),
                row.settlementCurrency().trim(),
                row.iskEligible() != null && row.iskEligible(),
                row.effectiveFrom(),
                row.effectiveTo());
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }

    public record Envelope(@JsonProperty("rows") List<Row> rows) {}

    public record Row(
            @JsonProperty("instrumentId") String instrumentId,
            @JsonProperty("symbol") String symbol,
            @JsonProperty("isin") String isin,
            @JsonProperty("primaryMic") String primaryMic,
            @JsonProperty("settlementCalendarId") String settlementCalendarId,
            @JsonProperty("settlementCycle") String settlementCycle,
            @JsonProperty("settlementCurrency") String settlementCurrency,
            @JsonProperty("iskEligible") Boolean iskEligible,
            @JsonProperty("effectiveFrom") LocalDate effectiveFrom,
            @JsonProperty("effectiveTo") LocalDate effectiveTo) {}

    public record RejectedRow(int rowIndex, String reason) {}

    public record Result(int inserted, int updated, List<RejectedRow> rejected) {}
}
