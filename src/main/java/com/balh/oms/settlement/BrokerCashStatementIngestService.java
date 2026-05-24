package com.balh.oms.settlement;

import com.balh.oms.config.OmsConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
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

/**
 * Ingest for broker cash statement files (gap plan §5.7 / Flyway V69).
 *
 * <p>Reconciliation is a separate {@link CashReconciliationService#reconcile(long)} call.
 */
@Service
public class BrokerCashStatementIngestService {

    public record Result(
            boolean duplicate,
            long batchId,
            String status,
            Integer movementCount,
            String errorSummary,
            int insertedMovements,
            int skippedDuplicateMovements) {}

    private final BrokerCashStatementBatchRepository batches;
    private final BrokerCashStatementMovementRepository movements;
    private final ObjectMapper objectMapper;
    private final OmsConfig config;
    private final TransactionTemplate transactionTemplate;
    private final MeterRegistry meterRegistry;

    public BrokerCashStatementIngestService(
            BrokerCashStatementBatchRepository batches,
            BrokerCashStatementMovementRepository movements,
            ObjectMapper objectMapper,
            OmsConfig config,
            PlatformTransactionManager transactionManager,
            MeterRegistry meterRegistry) {
        this.batches = batches;
        this.movements = movements;
        this.objectMapper = objectMapper;
        this.config = config;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.meterRegistry = meterRegistry;
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
            recordIngest("rejected");
            return new Result(false, -1L, "rejected", null, truncate(e.getMessage(), st), 0, 0);
        }

        Optional<Long> newId = batches.insertReceivedIfNew(
                new BrokerCashStatementBatchRepository.InsertReceived(
                        header.brokerId,
                        header.fileId,
                        header.businessDate,
                        header.currency,
                        header.schemaVersion,
                        shaHex,
                        source.trim(),
                        fileName,
                        header.generatedAt,
                        header.openingBalance,
                        header.closingBalance));
        if (newId.isEmpty()) {
            Optional<BrokerCashStatementBatchRepository.BatchRow> existing = batches.findBySha256Hex(shaHex);
            if (existing.isEmpty()) {
                existing = batches.findByBrokerFile(header.brokerId, header.fileId);
            }
            BrokerCashStatementBatchRepository.BatchRow row =
                    existing.orElseThrow(() -> new IllegalStateException("batch row missing after conflict"));
            recordIngest("duplicate");
            return new Result(
                    true,
                    row.id(),
                    row.status(),
                    row.movementCount(),
                    row.errorSummary(),
                    0,
                    0);
        }
        long batchId = newId.get();
        try {
            batches.updateStatus(batchId, "parsing", null, null);
            JsonNode tree = objectMapper.readTree(fileBytes);
            JsonNode movementsNode = tree.get("movements");
            if (movementsNode == null || !movementsNode.isArray() || movementsNode.isEmpty()) {
                String summary = truncate("empty movements", st);
                batches.updateStatus(batchId, "failed", null, summary);
                recordIngest("failed");
                return new Result(false, batchId, "failed", null, summary, 0, 0);
            }
            if (movementsNode.size() > st.getFileImportMaxRows()) {
                String summary = truncate(
                        "movement count " + movementsNode.size() + " exceeds max " + st.getFileImportMaxRows(), st);
                batches.updateStatus(batchId, "failed", null, summary);
                recordIngest("failed");
                return new Result(false, batchId, "failed", null, summary, 0, 0);
            }

            int parsedCount = movementsNode.size();
            int totalInserted = 0;
            int totalSkipped = 0;
            int sliceSize = st.getFileImportRegisterSliceSize();

            for (int sliceStart = 0; sliceStart < parsedCount; sliceStart += sliceSize) {
                int sliceEnd = Math.min(sliceStart + sliceSize, parsedCount);
                final int from = sliceStart;
                final int to = sliceEnd;
                int[] counts = transactionTemplate.execute(status -> insertSlice(batchId, header, movementsNode, from, to));
                if (counts != null) {
                    totalInserted += counts[0];
                    totalSkipped += counts[1];
                }
            }

            batches.updateStatus(batchId, "parsed", parsedCount, null);
            recordIngest("parsed");
            return new Result(false, batchId, "parsed", parsedCount, null, totalInserted, totalSkipped);
        } catch (RuntimeException | java.io.IOException e) {
            String summary = truncate(e.getMessage(), st);
            batches.updateStatus(batchId, "failed", null, summary);
            recordIngest("failed");
            return new Result(false, batchId, "failed", null, summary, 0, 0);
        }
    }

    private void recordIngest(String status) {
        BrokerFileIngestMetrics.record(meterRegistry, BrokerFileIngestMetrics.FILE_CASH_STATEMENT, status);
    }

    private int[] insertSlice(long batchId, EnvelopeHeader header, JsonNode movementsNode, int from, int to) {
        int inserted = 0;
        int skipped = 0;
        for (int i = from; i < to; i++) {
            JsonNode rowNode = movementsNode.get(i);
            BrokerCashStatementMovementRow typed;
            try {
                typed = objectMapper.treeToValue(rowNode, BrokerCashStatementMovementRow.class);
            } catch (Exception e) {
                throw new IllegalArgumentException("movement " + i + ": " + e.getMessage(), e);
            }
            validateMovement(i, typed);
            String currency =
                    typed.currency() == null || typed.currency().isBlank() ? header.currency : typed.currency();
            Long id = movements.insertIgnore(new BrokerCashStatementMovementRepository.InsertCommand(
                    batchId,
                    header.brokerId,
                    typed.brokerMovementId(),
                    typed.type(),
                    typed.executionRef(),
                    typed.amount(),
                    currency.trim(),
                    typed.valueDate(),
                    rowNode.toString()));
            if (id == null) {
                skipped++;
            } else {
                inserted++;
            }
        }
        return new int[] {inserted, skipped};
    }

    private static void validateMovement(int index, BrokerCashStatementMovementRow row) {
        if (row.brokerMovementId() == null || row.brokerMovementId().isBlank()) {
            throw new IllegalArgumentException("movement " + index + ": brokerMovementId required");
        }
        if (row.amount() == null) {
            throw new IllegalArgumentException("movement " + index + ": amount required");
        }
    }

    private EnvelopeHeader parseHeader(byte[] fileBytes) throws java.io.IOException {
        JsonNode tree = objectMapper.readTree(fileBytes);
        int schemaVersion = requiredInt(tree, "schemaVersion");
        if (schemaVersion != BrokerCashStatementEnvelope.CURRENT_SCHEMA_VERSION) {
            throw new IllegalArgumentException(
                    "unsupported schemaVersion " + schemaVersion + " (current: "
                            + BrokerCashStatementEnvelope.CURRENT_SCHEMA_VERSION + ")");
        }
        String brokerId = requiredText(tree, "brokerId");
        String fileId = requiredText(tree, "fileId");
        LocalDate businessDate = LocalDate.parse(requiredText(tree, "businessDate"));
        String currency = requiredText(tree, "currency");
        BigDecimal opening = optionalDecimal(tree, "openingBalance");
        BigDecimal closing = optionalDecimal(tree, "closingBalance");
        Instant generatedAt = null;
        JsonNode generated = tree.get("generatedAt");
        if (generated != null && !generated.isNull()) {
            generatedAt = Instant.parse(generated.asText());
        }
        return new EnvelopeHeader(
                brokerId, fileId, businessDate, currency, schemaVersion, generatedAt, opening, closing);
    }

    private static BigDecimal optionalDecimal(JsonNode tree, String field) {
        JsonNode n = tree.get(field);
        if (n == null || n.isNull()) {
            return null;
        }
        return new BigDecimal(n.asText());
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
            String brokerId,
            String fileId,
            LocalDate businessDate,
            String currency,
            int schemaVersion,
            Instant generatedAt,
            BigDecimal openingBalance,
            BigDecimal closingBalance) {}
}
