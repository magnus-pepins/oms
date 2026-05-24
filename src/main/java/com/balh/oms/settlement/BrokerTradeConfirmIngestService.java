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
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Ingest service for the v1 economic broker trade confirm contract
 * (gap plan §5.1 / Flyway V54).
 *
 * <p>Pipeline:
 * <ol>
 *   <li>Hash file bytes (SHA-256).</li>
 *   <li>Parse the JSON envelope shell to read {@code brokerId}, {@code fileId},
 *       {@code businessDate}, {@code schemaVersion}, {@code generatedAt}.</li>
 *   <li>Insert {@code broker_confirm_batch} row with status {@code received};
 *       fall back to the existing row on either {@code (file_sha256_hex)} or
 *       {@code (broker_id, broker_file_id)} collision (duplicate).</li>
 *   <li>Walk the row tree, deserialise each row to {@link BrokerTradeConfirmRow},
 *       preserving the original JSON text for {@code raw_row_json}.</li>
 *   <li>Insert rows + fees in slices of {@code fileImportRegisterSliceSize}; each
 *       slice runs in its own transaction so a partial failure surfaces a status
 *       transition without holding a huge write lock.</li>
 *   <li>On success advance status to {@code parsed} (Slice 3 will continue to
 *       {@code matching}/{@code applied}). On parse / runtime error mark
 *       {@code failed} and capture the truncated error summary.</li>
 * </ol>
 *
 * <p>Matching, execution resolution, and fee posting are out of scope for this
 * slice — confirms land with {@code resolved_execution_id = NULL} and
 * {@code match_status = 'pending'}.
 */
@Service
public class BrokerTradeConfirmIngestService {

    public record Result(
            boolean duplicate,
            long batchId,
            String status,
            Integer rowCount,
            String errorSummary,
            int insertedRows,
            int insertedFees,
            int skippedDuplicateRows) {}

    private final BrokerConfirmBatchRepository batches;
    private final BrokerTradeConfirmRepository confirms;
    private final BrokerTradeConfirmFeeRepository fees;
    private final ObjectMapper objectMapper;
    private final OmsConfig config;
    private final TransactionTemplate transactionTemplate;
    private final BrokerTradeConfirmBatchLifecycleService brokerTradeConfirmBatchLifecycle;

    public BrokerTradeConfirmIngestService(
            BrokerConfirmBatchRepository batches,
            BrokerTradeConfirmRepository confirms,
            BrokerTradeConfirmFeeRepository fees,
            ObjectMapper objectMapper,
            OmsConfig config,
            PlatformTransactionManager transactionManager,
            BrokerTradeConfirmBatchLifecycleService brokerTradeConfirmBatchLifecycle) {
        this.batches = batches;
        this.confirms = confirms;
        this.fees = fees;
        this.objectMapper = objectMapper;
        this.config = config;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.brokerTradeConfirmBatchLifecycle = brokerTradeConfirmBatchLifecycle;
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
            return new Result(false, -1L, "rejected", null, truncate(e.getMessage(), st), 0, 0, 0);
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
            Optional<BrokerConfirmBatchRepository.BatchRow> existing = batches.findBySha256Hex(shaHex);
            if (existing.isEmpty()) {
                existing = batches.findByBrokerFile(header.brokerId, header.fileId);
            }
            BrokerConfirmBatchRepository.BatchRow row =
                    existing.orElseThrow(() -> new IllegalStateException("batch row missing after conflict"));
            return new Result(
                    true,
                    row.id(),
                    row.status(),
                    row.rowCount(),
                    row.errorSummary(),
                    0,
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
                return new Result(false, batchId, "failed", null, summary, 0, 0, 0);
            }
            if (rowsNode.size() > st.getFileImportMaxRows()) {
                String summary = truncate(
                        "row count " + rowsNode.size() + " exceeds max " + st.getFileImportMaxRows(), st);
                batches.updateStatus(batchId, "failed", null, summary);
                return new Result(false, batchId, "failed", null, summary, 0, 0, 0);
            }

            int parsedRowCount = rowsNode.size();
            int totalInsertedRows = 0;
            int totalInsertedFees = 0;
            int totalSkippedDuplicates = 0;
            int sliceSize = st.getFileImportRegisterSliceSize();

            for (int sliceStart = 0; sliceStart < parsedRowCount; sliceStart += sliceSize) {
                int sliceEnd = Math.min(sliceStart + sliceSize, parsedRowCount);
                final int sliceStartFinal = sliceStart;
                final int sliceEndFinal = sliceEnd;
                int[] insertedAndFeesAndSkipped =
                        transactionTemplate.execute(status -> insertSlice(
                                batchId, header.brokerId, rowsNode, sliceStartFinal, sliceEndFinal));
                if (insertedAndFeesAndSkipped == null) {
                    continue;
                }
                totalInsertedRows += insertedAndFeesAndSkipped[0];
                totalInsertedFees += insertedAndFeesAndSkipped[1];
                totalSkippedDuplicates += insertedAndFeesAndSkipped[2];
            }

            batches.updateStatus(batchId, "parsed", parsedRowCount, null);
            if (config.getSettlement().isBrokerConfirmMatchOnIngestEnabled()) {
                brokerTradeConfirmBatchLifecycle.processBatchMatches(batchId);
            }
            String finalStatus =
                    batches.findById(batchId).map(BrokerConfirmBatchRepository.BatchRow::status).orElse("parsed");
            return new Result(
                    false,
                    batchId,
                    finalStatus,
                    parsedRowCount,
                    null,
                    totalInsertedRows,
                    totalInsertedFees,
                    totalSkippedDuplicates);
        } catch (RuntimeException | java.io.IOException e) {
            String summary = truncate(e.getMessage(), st);
            batches.updateStatus(batchId, "failed", null, summary);
            return new Result(false, batchId, "failed", null, summary, 0, 0, 0);
        }
    }

    private int[] insertSlice(long batchId, String brokerId, JsonNode rowsNode, int from, int to) {
        int inserted = 0;
        int insertedFees = 0;
        int skipped = 0;
        for (int i = from; i < to; i++) {
            JsonNode rowNode = rowsNode.get(i);
            BrokerTradeConfirmRow typed;
            try {
                typed = objectMapper.treeToValue(rowNode, BrokerTradeConfirmRow.class);
            } catch (Exception e) {
                throw new IllegalArgumentException("row " + i + ": " + e.getMessage(), e);
            }
            validateRow(i, typed);
            BrokerTradeConfirmRepository.InsertCommand cmd = toInsertCommand(batchId, brokerId, typed, rowNode);
            Long confirmId = confirms.insertIgnore(cmd);
            if (confirmId == null) {
                skipped++;
                continue;
            }
            inserted++;
            insertedFees += fees.insertBatch(confirmId, typed.fees() == null ? List.of() : typed.fees());
        }
        return new int[] {inserted, insertedFees, skipped};
    }

    private static void validateRow(int rowIndex, BrokerTradeConfirmRow row) {
        if (row.brokerTradeId() == null || row.brokerTradeId().isBlank()) {
            throw new IllegalArgumentException("row " + rowIndex + ": brokerTradeId required");
        }
        if (row.instrument() == null
                || row.instrument().symbol() == null
                || row.instrument().symbol().isBlank()) {
            throw new IllegalArgumentException("row " + rowIndex + ": instrument.symbol required");
        }
        if (!"BUY".equals(row.side()) && !"SELL".equals(row.side())) {
            throw new IllegalArgumentException("row " + rowIndex + ": side must be BUY or SELL");
        }
        if (row.quantity() == null) {
            throw new IllegalArgumentException("row " + rowIndex + ": quantity required");
        }
        if (row.price() == null) {
            throw new IllegalArgumentException("row " + rowIndex + ": price required");
        }
        String correction = row.correctionType();
        if (correction == null || correction.isBlank()) {
            correction = "new";
        }
        if (!"new".equals(correction)
                && !"amend".equals(correction)
                && !"cancel".equals(correction)
                && !"bust".equals(correction)) {
            throw new IllegalArgumentException("row " + rowIndex + ": correctionType invalid: " + correction);
        }
        if (!"new".equals(correction)
                && (row.originalBrokerTradeId() == null || row.originalBrokerTradeId().isBlank())) {
            throw new IllegalArgumentException(
                    "row " + rowIndex + ": originalBrokerTradeId required for correctionType " + correction);
        }
    }

    private BrokerTradeConfirmRepository.InsertCommand toInsertCommand(
            long batchId, String brokerId, BrokerTradeConfirmRow row, JsonNode rawNode) {
        BrokerTradeConfirmRow.Instrument instrument = row.instrument();
        String settlementCurrency =
                row.settlementCurrency() == null || row.settlementCurrency().isBlank()
                        ? instrument == null ? null : instrument.currency()
                        : row.settlementCurrency();
        String correction =
                row.correctionType() == null || row.correctionType().isBlank() ? "new" : row.correctionType();
        BigDecimal grossAmount = row.grossAmount();
        return new BrokerTradeConfirmRepository.InsertCommand(
                batchId,
                brokerId,
                row.brokerTradeId(),
                row.venueExecRef(),
                row.accountId(),
                row.brokerAccount(),
                row.custodyAccountId(),
                instrument == null ? null : instrument.symbol(),
                instrument == null ? null : instrument.isin(),
                instrument == null ? null : instrument.mic(),
                instrument == null ? null : instrument.currency(),
                row.side(),
                row.quantity(),
                row.price(),
                grossAmount,
                row.tradeDate(),
                row.settlementDate(),
                settlementCurrency,
                row.status(),
                correction,
                row.originalBrokerTradeId(),
                rawNode.toString());
    }

    private EnvelopeHeader parseHeader(byte[] fileBytes) throws java.io.IOException {
        JsonNode tree = objectMapper.readTree(fileBytes);
        int schemaVersion = requiredInt(tree, "schemaVersion");
        if (schemaVersion != BrokerTradeConfirmEnvelope.CURRENT_SCHEMA_VERSION) {
            throw new IllegalArgumentException(
                    "unsupported schemaVersion " + schemaVersion + " (current: "
                            + BrokerTradeConfirmEnvelope.CURRENT_SCHEMA_VERSION + ")");
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

    /** Exposed for unit tests; not part of the public API surface. */
    static UUID parseUuidOrNull(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return UUID.fromString(raw);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
