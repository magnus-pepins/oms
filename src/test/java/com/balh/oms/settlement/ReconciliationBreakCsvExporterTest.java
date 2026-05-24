package com.balh.oms.settlement;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class ReconciliationBreakCsvExporterTest {

    @Test
    void toCsv_escapesCommasAndQuotesInDiffJson() {
        String csv = ReconciliationBreakCsvExporter.toCsv(List.of(new ReconciliationBreakRepository.BreakRow(
                1L,
                "trade_mismatch",
                "high",
                "broker",
                10L,
                20L,
                UUID.fromString("a0000001-0000-4000-8000-000000000001"),
                LocalDate.of(2026, 5, 23),
                "{\"reason\":\"no_execution_found\",\"note\":\"a,b\"}",
                "open",
                Instant.parse("2026-05-23T12:00:00Z"),
                "system",
                null,
                null,
                null,
                null,
                null,
                null)));
        assertThat(csv).contains("trade_mismatch");
        assertThat(csv).contains("\"{\"\"reason\"\":\"\"no_execution_found\"\",\"\"note\"\":\"\"a,b\"\"}\"");
    }

    @Test
    void csv_plainValueWithoutEscaping() {
        assertThat(ReconciliationBreakCsvExporter.csv("open")).isEqualTo("open");
        assertThat(ReconciliationBreakCsvExporter.csv(null)).isEmpty();
    }
}
