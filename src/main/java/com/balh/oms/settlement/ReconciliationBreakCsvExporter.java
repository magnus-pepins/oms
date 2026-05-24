package com.balh.oms.settlement;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.UUID;

/**
 * CSV export for {@code reconciliation_breaks} (gap plan Phase B — ops report export).
 */
public final class ReconciliationBreakCsvExporter {

    private static final DateTimeFormatter ISO_INSTANT = DateTimeFormatter.ISO_INSTANT;

    private ReconciliationBreakCsvExporter() {}

    public static String toCsv(List<ReconciliationBreakRepository.BreakRow> rows) {
        StringBuilder sb = new StringBuilder();
        sb.append(
                "id,break_type,severity,source_system,confirm_id,execution_id,account_id,business_date,status,opened_at,opened_by,assigned_to,resolved_at,resolved_by,resolution_code,resolution_note,notes,diff_json\n");
        for (ReconciliationBreakRepository.BreakRow row : rows) {
            appendRow(sb, row);
        }
        return sb.toString();
    }

    private static void appendRow(StringBuilder sb, ReconciliationBreakRepository.BreakRow row) {
        sb.append(row.id()).append(',');
        sb.append(csv(row.breakType())).append(',');
        sb.append(csv(row.severity())).append(',');
        sb.append(csv(row.sourceSystem())).append(',');
        sb.append(nullableLong(row.confirmId())).append(',');
        sb.append(nullableLong(row.executionId())).append(',');
        sb.append(nullableUuid(row.accountId())).append(',');
        sb.append(row.businessDate() == null ? "" : row.businessDate()).append(',');
        sb.append(csv(row.status())).append(',');
        sb.append(formatInstant(row.openedAt())).append(',');
        sb.append(csv(row.openedBy())).append(',');
        sb.append(csv(row.assignedTo())).append(',');
        sb.append(formatInstant(row.resolvedAt())).append(',');
        sb.append(csv(row.resolvedBy())).append(',');
        sb.append(csv(row.resolutionCode())).append(',');
        sb.append(csv(row.resolutionNote())).append(',');
        sb.append(csv(row.notes())).append(',');
        sb.append(csv(row.diffJson()));
        sb.append('\n');
    }

    private static String formatInstant(Instant instant) {
        return instant == null ? "" : ISO_INSTANT.format(instant.atOffset(ZoneOffset.UTC));
    }

    private static String nullableLong(Long value) {
        return value == null ? "" : value.toString();
    }

    private static String nullableUuid(UUID value) {
        return value == null ? "" : value.toString();
    }

    static String csv(String value) {
        if (value == null) {
            return "";
        }
        if (!value.contains(",") && !value.contains("\"") && !value.contains("\n") && !value.contains("\r")) {
            return value;
        }
        return "\"" + value.replace("\"", "\"\"") + "\"";
    }
}
