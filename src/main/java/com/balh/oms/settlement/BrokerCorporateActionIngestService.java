package com.balh.oms.settlement;

import com.balh.oms.config.OmsConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.LocalDate;
import java.util.HexFormat;
import java.util.Optional;

/** Ingest for broker corporate-action files (gap plan §5.9 / Flyway V72). */
@Service
public class BrokerCorporateActionIngestService {

    public record Result(
            boolean duplicate,
            long batchId,
            String status,
            Integer eventCount,
            String errorSummary,
            int insertedEvents,
            int skippedDuplicateEvents) {}

    private final BrokerCorporateActionBatchRepository batches;
    private final BrokerCorporateActionRowRepository rows;
    private final ObjectMapper objectMapper;
    private final OmsConfig config;
    private final TransactionTemplate transactionTemplate;

    public BrokerCorporateActionIngestService(
            BrokerCorporateActionBatchRepository batches,
            BrokerCorporateActionRowRepository rows,
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
                new BrokerCorporateActionBatchRepository.InsertReceived(
                        header.brokerId,
                        header.fileId,
                        header.businessDate,
                        header.schemaVersion,
                        shaHex,
                        source.trim(),
                        fileName,
                        header.generatedAt));
        if (newId.isEmpty()) {
            Optional<BrokerCorporateActionBatchRepository.BatchRow> existing = batches.findBySha256Hex(shaHex);
            if (existing.isEmpty()) {
                existing = batches.findByBrokerFile(header.brokerId, header.fileId);
            }
            BrokerCorporateActionBatchRepository.BatchRow row =
                    existing.orElseThrow(() -> new IllegalStateException("batch row missing after conflict"));
            return new Result(
                    true,
                    row.id(),
                    row.status(),
                    row.eventCount(),
                    row.errorSummary(),
                    0,
                    0);
        }
        long batchId = newId.get();
        try {
            batches.updateStatus(batchId, "parsing", null, null);
            JsonNode tree = objectMapper.readTree(fileBytes);
            JsonNode eventsNode = tree.get("events");
            if (eventsNode == null || !eventsNode.isArray() || eventsNode.isEmpty()) {
                String summary = truncate("empty events", st);
                batches.updateStatus(batchId, "failed", null, summary);
                return new Result(false, batchId, "failed", null, summary, 0, 0);
            }
            if (eventsNode.size() > st.getFileImportMaxRows()) {
                String summary = truncate(
                        "event count " + eventsNode.size() + " exceeds max " + st.getFileImportMaxRows(), st);
                batches.updateStatus(batchId, "failed", null, summary);
                return new Result(false, batchId, "failed", null, summary, 0, 0);
            }

            int parsedCount = eventsNode.size();
            int totalInserted = 0;
            int totalSkipped = 0;
            int sliceSize = st.getFileImportRegisterSliceSize();

            for (int sliceStart = 0; sliceStart < parsedCount; sliceStart += sliceSize) {
                int sliceEnd = Math.min(sliceStart + sliceSize, parsedCount);
                final int from = sliceStart;
                final int to = sliceEnd;
                int[] counts = transactionTemplate.execute(status -> insertSlice(batchId, header, eventsNode, from, to));
                if (counts != null) {
                    totalInserted += counts[0];
                    totalSkipped += counts[1];
                }
            }

            batches.updateStatus(batchId, "parsed", parsedCount, null);
            return new Result(false, batchId, "parsed", parsedCount, null, totalInserted, totalSkipped);
        } catch (RuntimeException | java.io.IOException e) {
            String summary = truncate(e.getMessage(), st);
            batches.updateStatus(batchId, "failed", null, summary);
            return new Result(false, batchId, "failed", null, summary, 0, 0);
        }
    }

    private int[] insertSlice(long batchId, EnvelopeHeader header, JsonNode eventsNode, int from, int to) {
        int inserted = 0;
        int skipped = 0;
        for (int i = from; i < to; i++) {
            JsonNode rowNode = eventsNode.get(i);
            BrokerCorporateActionNoticeRow typed;
            try {
                typed = objectMapper.treeToValue(rowNode, BrokerCorporateActionNoticeRow.class);
            } catch (Exception e) {
                throw new IllegalArgumentException("event " + i + ": " + e.getMessage(), e);
            }
            validateEvent(i, typed);
            Long id = rows.insertIgnore(new BrokerCorporateActionRowRepository.InsertCommand(
                    batchId,
                    header.brokerId,
                    typed.brokerEventId(),
                    typed.instrumentSymbol(),
                    typed.actionType(),
                    typed.effectiveDate(),
                    rowNode.toString()));
            if (id == null) {
                skipped++;
            } else {
                inserted++;
            }
        }
        return new int[] {inserted, skipped};
    }

    private static void validateEvent(int index, BrokerCorporateActionNoticeRow row) {
        if (row.brokerEventId() == null || row.brokerEventId().isBlank()) {
            throw new IllegalArgumentException("event " + index + ": brokerEventId required");
        }
        if (row.instrumentSymbol() == null || row.instrumentSymbol().isBlank()) {
            throw new IllegalArgumentException("event " + index + ": instrumentSymbol required");
        }
        if (row.actionType() == null || row.actionType().isBlank()) {
            throw new IllegalArgumentException("event " + index + ": actionType required");
        }
        if (row.effectiveDate() == null) {
            throw new IllegalArgumentException("event " + index + ": effectiveDate required");
        }
    }

    private EnvelopeHeader parseHeader(byte[] fileBytes) throws java.io.IOException {
        JsonNode tree = objectMapper.readTree(fileBytes);
        int schemaVersion = requiredInt(tree, "schemaVersion");
        if (schemaVersion != BrokerCorporateActionEnvelope.CURRENT_SCHEMA_VERSION) {
            throw new IllegalArgumentException(
                    "unsupported schemaVersion " + schemaVersion + " (current: "
                            + BrokerCorporateActionEnvelope.CURRENT_SCHEMA_VERSION + ")");
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
