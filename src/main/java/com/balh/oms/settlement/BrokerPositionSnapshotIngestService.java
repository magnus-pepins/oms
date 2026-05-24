package com.balh.oms.settlement;

import com.balh.oms.config.OmsConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.LocalDate;
import java.util.HexFormat;
import java.util.Optional;
import java.util.UUID;

/**
 * Ingest for broker position snapshot files (gap plan §5.6 / Flyway V67).
 *
 * <p>Rows land in {@code broker_position_snapshot_row}; reconciliation is a separate
 * {@link PositionReconciliationService#reconcile(long)} call.
 */
@Service
public class BrokerPositionSnapshotIngestService {

    public record Result(
            boolean duplicate,
            long batchId,
            String status,
            Integer rowCount,
            String errorSummary,
            int insertedRows,
            int skippedDuplicateRows) {}

    private final BrokerPositionSnapshotBatchRepository batches;
    private final BrokerPositionSnapshotRowRepository rows;
    private final ObjectMapper objectMapper;
    private final OmsConfig config;
    private final TransactionTemplate transactionTemplate;

    public BrokerPositionSnapshotIngestService(
            BrokerPositionSnapshotBatchRepository batches,
            BrokerPositionSnapshotRowRepository rows,
            ObjectMapper objectMapper,
            OmsConfig config,
            PlatformTransactionManager transactionManager) {
        this.batches = batches;
        this.rows = rows;
        this.objectMapper = objectMapper;
        this.config = config;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    public Result ingest(String source, String originalFilename, byte[] fileBytes) {
        var st = config.getSettlement();
        if (source == null || source.isBlank()) {
            throw new IllegalArgumentException("source required");
        }
        if (fileBytes.length > st.getFileImportMaxBytes()) {
            throw new IllegalArgumentException("file too large");
        }
        String shaHex = sha256Hex(fileBytes);
        String fileName =
                (originalFilename == null || originalFilename.isBlank()) ? "upload" : originalFilename.trim();

        EnvelopeHeader header;
        try {
            header = parseHeader(fileBytes);
        } catch (Exception e) {
            return new Result(false, -1L, "rejected", null, truncate(e.getMessage(), st), 0, 0);
        }

        Optional<Long> newId = batches.insertReceivedIfNew(
                header.brokerId,
                header.fileId,
                header.businessDate,
                header.schemaVersion,
                shaHex,
                source.trim(),
                fileName,
                header.generatedAt);
        if (newId.isEmpty()) {
            Optional<BrokerPositionSnapshotBatchRepository.BatchRow> existing = batches.findBySha256Hex(shaHex);
            if (existing.isEmpty()) {
                existing = batches.findByBrokerFile(header.brokerId, header.fileId);
            }
            BrokerPositionSnapshotBatchRepository.BatchRow row =
                    existing.orElseThrow(() -> new IllegalStateException("batch row missing after conflict"));
            return new Result(
                    true,
                    row.id(),
                    row.status(),
                    row.rowCount(),
                    row.errorSummary(),
                    0,
                    0);
        }
        long batchId = newId.get();
        try {
            batches.updateStatus(batchId, "parsing", null, null);
            JsonNode tree = objectMapper.readTree(fileBytes);
            JsonNode rowsNode = tree.get("rows");
            if (rowsNode == null || !rowsNode.isArray() || rowsNode.isEmpty()) {
                String summary = truncate("empty rows", st);
                batches.updateStatus(batchId, "failed", null, summary);
                return new Result(false, batchId, "failed", null, summary, 0, 0);
            }
            if (rowsNode.size() > st.getFileImportMaxRows()) {
                String summary = truncate(
                        "row count " + rowsNode.size() + " exceeds max " + st.getFileImportMaxRows(), st);
                batches.updateStatus(batchId, "failed", null, summary);
                return new Result(false, batchId, "failed", null, summary, 0, 0);
            }

            int parsedRowCount = rowsNode.size();
            int totalInserted = 0;
            int totalSkipped = 0;
            int sliceSize = st.getFileImportRegisterSliceSize();

            for (int sliceStart = 0; sliceStart < parsedRowCount; sliceStart += sliceSize) {
                int sliceEnd = Math.min(sliceStart + sliceSize, parsedRowCount);
                final int sliceStartFinal = sliceStart;
                final int sliceEndFinal = sliceEnd;
                int[] insertedAndSkipped = transactionTemplate.execute(
                        status -> insertSlice(batchId, header.brokerId, rowsNode, sliceStartFinal, sliceEndFinal));
                if (insertedAndSkipped == null) {
                    continue;
                }
                totalInserted += insertedAndSkipped[0];
                totalSkipped += insertedAndSkipped[1];
            }

            batches.updateStatus(batchId, "parsed", parsedRowCount, null);
            return new Result(false, batchId, "parsed", parsedRowCount, null, totalInserted, totalSkipped);
        } catch (RuntimeException | java.io.IOException e) {
            String summary = truncate(e.getMessage(), st);
            batches.updateStatus(batchId, "failed", null, summary);
            return new Result(false, batchId, "failed", null, summary, 0, 0);
        }
    }

    private int[] insertSlice(long batchId, String brokerId, JsonNode rowsNode, int from, int to) {
        int inserted = 0;
        int skipped = 0;
        for (int i = from; i < to; i++) {
            JsonNode rowNode = rowsNode.get(i);
            BrokerPositionSnapshotRow typed;
            try {
                typed = objectMapper.treeToValue(rowNode, BrokerPositionSnapshotRow.class);
            } catch (Exception e) {
                throw new IllegalArgumentException("row " + i + ": " + e.getMessage(), e);
            }
            validateRow(i, typed);
            BrokerPositionSnapshotRowRepository.InsertCommand cmd =
                    toInsertCommand(batchId, brokerId, typed, rowNode);
            Long rowId = rows.insertIgnore(cmd);
            if (rowId == null) {
                skipped++;
            } else {
                inserted++;
            }
        }
        return new int[] {inserted, skipped};
    }

    private static void validateRow(int rowIndex, BrokerPositionSnapshotRow row) {
        if (row.instrument() == null
                || row.instrument().symbol() == null
                || row.instrument().symbol().isBlank()) {
            throw new IllegalArgumentException("row " + rowIndex + ": instrument.symbol required");
        }
        if (row.accountId() == null && row.custodyAccountId() == null) {
            throw new IllegalArgumentException("row " + rowIndex + ": accountId or custodyAccountId required");
        }
        if (row.quantityTotal() == null) {
            throw new IllegalArgumentException("row " + rowIndex + ": quantityTotal required");
        }
    }

    private static BrokerPositionSnapshotRowRepository.InsertCommand toInsertCommand(
            long batchId, String brokerId, BrokerPositionSnapshotRow row, JsonNode rawNode) {
        BrokerPositionSnapshotRow.Instrument instrument = row.instrument();
        BigDecimal zero = BigDecimal.ZERO;
        return new BrokerPositionSnapshotRowRepository.InsertCommand(
                batchId,
                brokerId,
                row.brokerAccount(),
                row.accountId(),
                row.custodyAccountId(),
                instrument.symbol().trim(),
                instrument.isin(),
                instrument.currency(),
                row.quantityTotal(),
                row.quantitySettled() == null ? zero : row.quantitySettled(),
                row.quantityPendingBuySettle() == null ? zero : row.quantityPendingBuySettle(),
                row.quantityPendingSellSettle() == null ? zero : row.quantityPendingSellSettle(),
                row.asOf(),
                rawNode.toString());
    }

    private EnvelopeHeader parseHeader(byte[] fileBytes) throws java.io.IOException {
        JsonNode tree = objectMapper.readTree(fileBytes);
        int schemaVersion = requiredInt(tree, "schemaVersion");
        if (schemaVersion != BrokerPositionSnapshotEnvelope.CURRENT_SCHEMA_VERSION) {
            throw new IllegalArgumentException(
                    "unsupported schemaVersion " + schemaVersion + " (current: "
                            + BrokerPositionSnapshotEnvelope.CURRENT_SCHEMA_VERSION + ")");
        }
        String brokerId = requiredText(tree, "brokerId");
        String fileId = requiredText(tree, "fileId");
        LocalDate businessDate = LocalDate.parse(requiredText(tree, "businessDate"));
        Instant generatedAt = null;
        JsonNode generated = tree.get("generatedAt");
        if (generated != null && !generated.isNull()) {
            generatedAt = Instant.parse(generated.asText());
        }
        return new EnvelopeHeader(brokerId, fileId, businessDate, schemaVersion, generatedAt);
    }

    private static String requiredText(JsonNode tree, String field) {
        JsonNode n = tree.get(field);
        if (n == null || n.isNull() || !n.isTextual() || n.asText().isBlank()) {
            throw new IllegalArgumentException("envelope field '" + field + "' required");
        }
        return n.asText();
    }

    private static int requiredInt(JsonNode tree, String field) {
        JsonNode n = tree.get(field);
        if (n == null || n.isNull() || !n.canConvertToInt()) {
            throw new IllegalArgumentException("envelope field '" + field + "' required (int)");
        }
        return n.asInt();
    }

    private static String truncate(String message, OmsConfig.Settlement st) {
        if (message == null) {
            return "";
        }
        int max = st.getFileImportErrorSummaryMaxChars();
        return message.length() <= max ? message : message.substring(0, max);
    }

    private static String sha256Hex(byte[] bytes) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(md.digest(bytes));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    private record EnvelopeHeader(
            String brokerId, String fileId, LocalDate businessDate, int schemaVersion, Instant generatedAt) {}
}
