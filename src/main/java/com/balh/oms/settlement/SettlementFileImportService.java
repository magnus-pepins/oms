package com.balh.oms.settlement;

import com.balh.oms.config.OmsConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.io.IOException;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;

/**
 * Ingest broker confirms file: SHA-256 idempotent batch row, JSON parse (same row model as
 * {@code POST …/broker-confirms/import-json}), transactional {@link SettlementConfirmProcessor#registerBrokerConfirms}
 * slices.
 */
@Service
public class SettlementFileImportService {

    public record Result(
            boolean duplicate,
            long batchId,
            String status,
            Integer rowCount,
            String errorSummary,
            int insertedConfirms,
            int skippedInvalid,
            int skippedUnresolved) {}

    private final SettlementFileImportBatchRepository batches;
    private final SettlementConfirmProcessor processor;
    private final ObjectMapper objectMapper;
    private final OmsConfig config;
    private final TransactionTemplate transactionTemplate;

    public SettlementFileImportService(
            SettlementFileImportBatchRepository batches,
            SettlementConfirmProcessor processor,
            ObjectMapper objectMapper,
            OmsConfig config,
            org.springframework.transaction.PlatformTransactionManager transactionManager) {
        this.batches = batches;
        this.processor = processor;
        this.objectMapper = objectMapper;
        this.config = config;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    public Result ingestMultipart(String source, String originalFilename, byte[] fileBytes) {
        var st = config.getSettlement();
        if (source == null || source.isBlank()) {
            throw new IllegalArgumentException("source required");
        }
        if (fileBytes.length > st.getFileImportMaxBytes()) {
            throw new IllegalArgumentException("file too large");
        }
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA-256");
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
        String shaHex = HexFormat.of().formatHex(md.digest(fileBytes));
        String fileName =
                (originalFilename == null || originalFilename.isBlank()) ? "upload" : originalFilename.trim();

        Optional<Long> newId = batches.insertReceivedIfNew(source.trim(), fileName, shaHex);
        if (newId.isEmpty()) {
            SettlementFileImportBatchRepository.BatchRow row =
                    batches.findBySha256Hex(shaHex).orElseThrow(() -> new IllegalStateException("batch row missing"));
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
            BrokerFixtureFilePayload payload = objectMapper.readValue(fileBytes, BrokerFixtureFilePayload.class);
            if (payload.rows() == null || payload.rows().isEmpty()) {
                String summary = truncate("empty rows", st.getFileImportErrorSummaryMaxChars());
                batches.updateStatus(batchId, "failed", null, summary);
                return new Result(false, batchId, "failed", null, summary, 0, 0, 0);
            }
            BrokerFixtureResolutionOutcome outcome =
                    processor.resolveBrokerFixtureRows(payload.rows(), st.getFileImportMaxRows());
            int insertedTotal = 0;
            List<Long> ids = outcome.resolvedExecutionIds();
            int slice = st.getFileImportRegisterSliceSize();
            for (int i = 0; i < ids.size(); i += slice) {
                int hi = Math.min(i + slice, ids.size());
                List<Long> chunk = ids.subList(i, hi);
                Integer n =
                        transactionTemplate.execute(status -> processor.registerBrokerConfirms(chunk));
                insertedTotal += n == null ? 0 : n;
            }
            int parsedRowCount = payload.rows().size();
            batches.updateStatus(batchId, "applied", parsedRowCount, null);
            return new Result(
                    false,
                    batchId,
                    "applied",
                    parsedRowCount,
                    null,
                    insertedTotal,
                    outcome.skippedInvalidRows(),
                    outcome.skippedUnresolvedRows());
        } catch (IOException e) {
            String summary = truncate(e.getMessage(), st.getFileImportErrorSummaryMaxChars());
            batches.updateStatus(batchId, "failed", null, summary);
            return new Result(false, batchId, "failed", null, summary, 0, 0, 0);
        } catch (RuntimeException e) {
            String summary = truncate(e.getMessage(), st.getFileImportErrorSummaryMaxChars());
            batches.updateStatus(batchId, "failed", null, summary);
            return new Result(false, batchId, "failed", null, summary, 0, 0, 0);
        }
    }

    private static String truncate(String message, int maxChars) {
        if (message == null) {
            return "";
        }
        if (message.length() <= maxChars) {
            return message;
        }
        return message.substring(0, maxChars);
    }
}
