package com.balh.oms.settlement;

import com.balh.oms.config.OmsConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.LocalDate;
import java.util.HexFormat;
import java.util.Optional;

/**
 * Ingest for broker settlement fail files (gap plan §5.8 / Flyway V70).
 *
 * <p>v1 skeleton: persist raw fail rows only. Lot split, penalty booking, and customer
 * notification are follow-on slices.
 */
@Service
public class BrokerSettlementFailIngestService {

    private static final String METRIC_INGEST = "oms_settlement_fail_file_ingest_total";

    public record Result(
            boolean duplicate,
            long batchId,
            String status,
            Integer failCount,
            String errorSummary,
            int insertedFails,
            int skippedDuplicateFails) {}

    private final BrokerSettlementFailBatchRepository batches;
    private final BrokerSettlementFailRowRepository fails;
    private final ObjectMapper objectMapper;
    private final OmsConfig config;
    private final TransactionTemplate transactionTemplate;
    private final MeterRegistry meterRegistry;

    public BrokerSettlementFailIngestService(
            BrokerSettlementFailBatchRepository batches,
            BrokerSettlementFailRowRepository fails,
            ObjectMapper objectMapper,
            OmsConfig config,
            PlatformTransactionManager transactionManager,
            MeterRegistry meterRegistry) {
        this.batches = batches;
        this.fails = fails;
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
                new BrokerSettlementFailBatchRepository.InsertReceived(
                        header.brokerId,
                        header.fileId,
                        header.businessDate,
                        header.schemaVersion,
                        shaHex,
                        source.trim(),
                        fileName,
                        header.generatedAt));
        if (newId.isEmpty()) {
            Optional<BrokerSettlementFailBatchRepository.BatchRow> existing = batches.findBySha256Hex(shaHex);
            if (existing.isEmpty()) {
                existing = batches.findByBrokerFile(header.brokerId, header.fileId);
            }
            BrokerSettlementFailBatchRepository.BatchRow row =
                    existing.orElseThrow(() -> new IllegalStateException("batch row missing after conflict"));
            recordIngest("duplicate");
            return new Result(
                    true,
                    row.id(),
                    row.status(),
                    row.failCount(),
                    row.errorSummary(),
                    0,
                    0);
        }
        long batchId = newId.get();
        try {
            batches.updateStatus(batchId, "parsing", null, null);
            JsonNode tree = objectMapper.readTree(fileBytes);
            JsonNode failsNode = tree.get("fails");
            if (failsNode == null || !failsNode.isArray() || failsNode.isEmpty()) {
                String summary = truncate("empty fails", st);
                batches.updateStatus(batchId, "failed", null, summary);
                recordIngest("failed");
                return new Result(false, batchId, "failed", null, summary, 0, 0);
            }
            if (failsNode.size() > st.getFileImportMaxRows()) {
                String summary = truncate(
                        "fail count " + failsNode.size() + " exceeds max " + st.getFileImportMaxRows(), st);
                batches.updateStatus(batchId, "failed", null, summary);
                recordIngest("failed");
                return new Result(false, batchId, "failed", null, summary, 0, 0);
            }

            int parsedCount = failsNode.size();
            int totalInserted = 0;
            int totalSkipped = 0;
            int sliceSize = st.getFileImportRegisterSliceSize();

            for (int sliceStart = 0; sliceStart < parsedCount; sliceStart += sliceSize) {
                int sliceEnd = Math.min(sliceStart + sliceSize, parsedCount);
                final int from = sliceStart;
                final int to = sliceEnd;
                int[] counts = transactionTemplate.execute(status -> insertSlice(batchId, header, failsNode, from, to));
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
        meterRegistry.counter(METRIC_INGEST, java.util.List.of(Tag.of("status", status))).increment();
    }

    private int[] insertSlice(long batchId, EnvelopeHeader header, JsonNode failsNode, int from, int to) {
        int inserted = 0;
        int skipped = 0;
        for (int i = from; i < to; i++) {
            JsonNode rowNode = failsNode.get(i);
            BrokerSettlementFailRow typed;
            try {
                typed = objectMapper.treeToValue(rowNode, BrokerSettlementFailRow.class);
            } catch (Exception e) {
                throw new IllegalArgumentException("fail " + i + ": " + e.getMessage(), e);
            }
            validateFail(i, typed);
            Long id = fails.insertIgnore(new BrokerSettlementFailRowRepository.InsertCommand(
                    batchId,
                    header.brokerId,
                    typed.brokerFailId(),
                    typed.brokerTradeId(),
                    typed.executionRef(),
                    typed.instrumentSymbol(),
                    typed.side(),
                    typed.failedQuantity(),
                    typed.intendedSettlementDate(),
                    typed.failReason(),
                    typed.expectedResolutionDate(),
                    typed.penaltyAmount(),
                    typed.penaltyCurrency(),
                    typed.resolutionStatus(),
                    rowNode.toString()));
            if (id == null) {
                skipped++;
            } else {
                inserted++;
            }
        }
        return new int[] {inserted, skipped};
    }

    private static void validateFail(int index, BrokerSettlementFailRow row) {
        if (row.brokerFailId() == null || row.brokerFailId().isBlank()) {
            throw new IllegalArgumentException("fail " + index + ": brokerFailId required");
        }
        if (row.failedQuantity() == null) {
            throw new IllegalArgumentException("fail " + index + ": failedQuantity required");
        }
        if (row.intendedSettlementDate() == null) {
            throw new IllegalArgumentException("fail " + index + ": intendedSettlementDate required");
        }
    }

    private EnvelopeHeader parseHeader(byte[] fileBytes) throws java.io.IOException {
        JsonNode tree = objectMapper.readTree(fileBytes);
        int schemaVersion = requiredInt(tree, "schemaVersion");
        if (schemaVersion != BrokerSettlementFailEnvelope.CURRENT_SCHEMA_VERSION) {
            throw new IllegalArgumentException(
                    "unsupported schemaVersion " + schemaVersion + " (current: "
                            + BrokerSettlementFailEnvelope.CURRENT_SCHEMA_VERSION + ")");
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
