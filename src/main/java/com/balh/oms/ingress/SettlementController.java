package com.balh.oms.ingress;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.persistence.SettlementExecutionsRepository;
import com.balh.oms.settlement.BrokerFixtureRow;
import com.balh.oms.settlement.BrokerTradeConfirmIngestService;
import com.balh.oms.settlement.BrokerTradeConfirmMatcher;
import com.balh.oms.settlement.MarkTradeFailedResult;
import com.balh.oms.settlement.LedgerSettlementOutboxRepository;
import com.balh.oms.settlement.ReconciliationBreakRepository;
import com.balh.oms.settlement.SettlementConfirmProcessor;
import com.balh.oms.settlement.SettlementFileImportBatchRepository;
import com.balh.oms.settlement.SettlementFileImportService;
import com.balh.oms.settlement.SettlementTimelineService;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

/**
 * Internal settlement controls (slice 6). Secured by {@link ApiKeyFilter} like other
 * {@code /internal/v1/**} routes.
 */
@RestController
@RequestMapping("/internal/v1/settlement")
public class SettlementController {

    private static final int DEFAULT_EXECUTION_LIST_LIMIT = 100;
    private static final int MAX_EXECUTION_LIST_LIMIT = 500;
    private static final int MAX_EXECUTION_LIST_OFFSET = 10_000;
    private static final long MAX_EXECUTION_LIST_RANGE_WITHOUT_ORDER_DAYS = 31;

    private static final Set<String> ALLOWED_SETTLEMENT_STATUSES =
            Set.of("executed", "matched", "confirmed", "settling", "settled", "failed");

    public record BrokerConfirmIngestRequest(List<Long> executionIds) {}

    public record BrokerConfirmIngestResponse(int insertedRows) {}

    public record BrokerConfirmProcessResponse(int processedRows) {}

    public record SettlementStepResponse(String settlementStatus) {}

    public record BrokerFixtureImportRequest(List<BrokerFixtureRow> rows) {}

    public record BrokerFixtureImportResponse(int insertedRows, int skippedUnresolvedRows, int skippedInvalidRows) {}

    public record SettlementFileImportResponse(
            boolean duplicate,
            long batchId,
            String status,
            Integer rowCount,
            String errorSummary,
            int insertedConfirms,
            int skippedInvalidRows,
            int skippedUnresolvedRows) {}

    public record SettlementFileImportBatchesResponse(
            List<SettlementFileImportBatchRepository.FileImportBatchListRow> items, int limit, int offset) {}

    public record BrokerTradeConfirmImportResponse(
            boolean duplicate,
            long batchId,
            String status,
            Integer rowCount,
            String errorSummary,
            int insertedRows,
            int insertedFees,
            int skippedDuplicateRows) {}

    public record BrokerTradeMatchResultResponse(
            long confirmId, Long executionId, String outcome, String matchDiffJson) {}

    public record BrokerTradeMatchBatchResponse(List<BrokerTradeMatchResultResponse> items) {}

    public record ReconciliationBreaksListResponse(
            List<ReconciliationBreakRepository.BreakRow> items, String status, int limit, int offset) {}

    public record MarkTradeFailedResponse(String result) {}

    private final SettlementConfirmProcessor processor;
    private final OmsConfig config;
    private final SettlementExecutionsRepository settlementExecutions;
    private final SettlementFileImportService settlementFileImportService;
    private final SettlementFileImportBatchRepository settlementFileImportBatchRepository;
    private final SettlementTimelineService timelineService;
    private final BrokerTradeConfirmIngestService brokerTradeConfirmIngestService;
    private final BrokerTradeConfirmMatcher brokerTradeConfirmMatcher;
    private final ReconciliationBreakRepository reconciliationBreaks;

    private static final int MAX_FILE_IMPORT_LIST_OFFSET = 10_000;

    /** Default min {@code attempts} for {@code GET …/outbox/stuck} (rows below this are omitted). */
    private static final int STUCK_OUTBOX_MIN_ATTEMPTS_DEFAULT = 3;

    private static final int STUCK_OUTBOX_LIST_MAX_LIMIT = 200;

    public record StuckOutboxListResponse(
            List<LedgerSettlementOutboxRepository.StuckOutboxRow> items, int minAttempts, int limit) {}

    private final LedgerSettlementOutboxRepository ledgerSettlementOutbox;

    public SettlementController(
            SettlementConfirmProcessor processor,
            OmsConfig config,
            SettlementExecutionsRepository settlementExecutions,
            SettlementFileImportService settlementFileImportService,
            SettlementFileImportBatchRepository settlementFileImportBatchRepository,
            SettlementTimelineService timelineService,
            LedgerSettlementOutboxRepository ledgerSettlementOutbox,
            BrokerTradeConfirmIngestService brokerTradeConfirmIngestService,
            BrokerTradeConfirmMatcher brokerTradeConfirmMatcher,
            ReconciliationBreakRepository reconciliationBreaks) {
        this.processor = processor;
        this.config = config;
        this.settlementExecutions = settlementExecutions;
        this.settlementFileImportService = settlementFileImportService;
        this.settlementFileImportBatchRepository = settlementFileImportBatchRepository;
        this.timelineService = timelineService;
        this.ledgerSettlementOutbox = ledgerSettlementOutbox;
        this.brokerTradeConfirmIngestService = brokerTradeConfirmIngestService;
        this.brokerTradeConfirmMatcher = brokerTradeConfirmMatcher;
        this.reconciliationBreaks = reconciliationBreaks;
    }

    /**
     * Unposted ledger settlement legs with repeated delivery failures — for beard-admin ops
     * without grepping pm2 logs. Requires {@code attempts >= minAttempts} (default 3).
     */
    @GetMapping("/outbox/stuck")
    public ResponseEntity<StuckOutboxListResponse> listStuckOutbox(
            @RequestParam(name = "minAttempts", required = false) Integer minAttempts,
            @RequestParam(required = false) Integer limit) {
        int min = minAttempts == null ? STUCK_OUTBOX_MIN_ATTEMPTS_DEFAULT : minAttempts;
        if (min < 1) {
            return ResponseEntity.badRequest().build();
        }
        int lim = limit == null ? 50 : limit;
        if (lim < 1 || lim > STUCK_OUTBOX_LIST_MAX_LIMIT) {
            return ResponseEntity.badRequest().build();
        }
        var rows = ledgerSettlementOutbox.findStuckUnposted(min, lim);
        return ResponseEntity.ok(new StuckOutboxListResponse(rows, min, lim));
    }

    /**
     * Paginated read of executions joined to orders. Either {@code orderId} is present, or both
     * {@code from} and {@code to} must be present (half-open {@code [from, to)} on {@code
     * executions.created_at}). Without {@code orderId}, the range must not exceed 31 days and must
     * have {@code to} strictly after {@code from}.
     */
    @GetMapping("/executions")
    public ResponseEntity<SettlementExecutionsPageResponse> listExecutions(
            @RequestParam(required = false) UUID orderId,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to,
            @RequestParam(required = false) String settlementStatus,
            @RequestParam(required = false) Integer limit,
            @RequestParam(required = false) Integer offset) {
        if (orderId == null && (from == null || to == null)) {
            return ResponseEntity.badRequest().build();
        }
        if (settlementStatus != null && !ALLOWED_SETTLEMENT_STATUSES.contains(settlementStatus)) {
            return ResponseEntity.badRequest().build();
        }
        if (from != null ^ to != null) {
            return ResponseEntity.badRequest().build();
        }
        if (from != null && to != null) {
            if (!to.isAfter(from)) {
                return ResponseEntity.badRequest().build();
            }
            if (orderId == null) {
                long days = Duration.between(from, to).toDays();
                if (days > MAX_EXECUTION_LIST_RANGE_WITHOUT_ORDER_DAYS) {
                    return ResponseEntity.badRequest().build();
                }
            }
        }
        int lim = limit == null ? DEFAULT_EXECUTION_LIST_LIMIT : limit;
        if (lim < 1) {
            lim = DEFAULT_EXECUTION_LIST_LIMIT;
        }
        if (lim > MAX_EXECUTION_LIST_LIMIT) {
            lim = MAX_EXECUTION_LIST_LIMIT;
        }
        int off = offset == null ? 0 : offset;
        if (off < 0 || off > MAX_EXECUTION_LIST_OFFSET) {
            return ResponseEntity.badRequest().build();
        }
        var items =
                settlementExecutions
                        .findByFilters(orderId, from, to, settlementStatus, lim, off)
                        .stream()
                        .map(
                                r ->
                                        new SettlementExecutionResponse(
                                                r.id(),
                                                r.orderId(),
                                                r.accountId(),
                                                r.venueId(),
                                                r.venueTs(),
                                                r.venueExecRef(),
                                                r.lastQuantity(),
                                                r.lastPrice(),
                                                r.leavesQuantity(),
                                                r.cumQuantityAfter(),
                                                r.execType(),
                                                r.settlementStatus(),
                                                r.createdAt(),
                                                r.orderStatus(),
                                                r.side(),
                                                r.instrumentSymbol()))
                        .toList();
        return ResponseEntity.ok(new SettlementExecutionsPageResponse(items, lim, off));
    }

    /**
     * Settlement lifecycle for one execution (executed → matched → confirmed → settled, plus
     * each ledger leg's enqueue + posted_at). Used by the beard-admin Detail panel to render
     * a vertical timeline + leg list. See {@link SettlementTimelineService} for sourcing rules.
     */
    @GetMapping("/executions/{executionId}/timeline")
    public ResponseEntity<SettlementTimelineResponse> getTimeline(@PathVariable long executionId) {
        return timelineService
                .loadTimeline(executionId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    /** Single execution joined to order, including raw venue envelope JSON. */
    @GetMapping("/executions/{executionId}")
    public ResponseEntity<SettlementExecutionDetailResponse> getExecution(@PathVariable long executionId) {
        return settlementExecutions
                .findById(executionId)
                .map(
                        r ->
                                new SettlementExecutionDetailResponse(
                                        r.id(),
                                        r.orderId(),
                                        r.accountId(),
                                        r.venueId(),
                                        r.venueTs(),
                                        r.venueExecRef(),
                                        r.lastQuantity(),
                                        r.lastPrice(),
                                        r.leavesQuantity(),
                                        r.cumQuantityAfter(),
                                        r.execType(),
                                        r.settlementStatus(),
                                        r.createdAt(),
                                        r.orderStatus(),
                                        r.side(),
                                        r.instrumentSymbol(),
                                        r.rawEnvelopeJson()))
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping("/broker-confirms")
    public ResponseEntity<BrokerConfirmIngestResponse> ingestBrokerConfirms(
            @RequestBody BrokerConfirmIngestRequest body) {
        if (body == null || body.executionIds() == null) {
            return ResponseEntity.badRequest().build();
        }
        int max = config.getSettlement().getBrokerConfirmHttpMaxExecutionIds();
        if (body.executionIds().size() > max) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).build();
        }
        int n = processor.registerBrokerConfirms(body.executionIds());
        return ResponseEntity.ok(new BrokerConfirmIngestResponse(n));
    }

    @PostMapping("/broker-confirms/import-json")
    public ResponseEntity<BrokerFixtureImportResponse> importBrokerFixture(
            @RequestBody BrokerFixtureImportRequest body) {
        if (body == null || body.rows() == null) {
            return ResponseEntity.badRequest().build();
        }
        var r = processor.registerBrokerConfirmsFromFixture(body.rows());
        return ResponseEntity.ok(
                new BrokerFixtureImportResponse(r.insertedRows(), r.skippedUnresolvedRows(), r.skippedInvalidRows()));
    }

    /**
     * v1 economic broker confirm ingest (gap plan §5.1) — JSON body. Mirrors
     * {@link #importSettlementFile(String, MultipartFile) /file-import} but for the v2 economic
     * envelope ({@code BrokerTradeConfirmEnvelope}). Matching (gap plan §5.2) is **not** wired here;
     * rows land with {@code resolved_execution_id = NULL} and {@code match_status = 'pending'}.
     */
    @PostMapping("/broker-trade-confirms/import-json")
    public ResponseEntity<BrokerTradeConfirmImportResponse> importBrokerTradeConfirms(
            @RequestParam(name = "source", required = false) String source,
            @RequestBody(required = false) byte[] body) {
        String src = (source == null || source.isBlank()) ? "http-json" : source.trim();
        if (body == null || body.length == 0) {
            return ResponseEntity.badRequest().build();
        }
        return runBrokerTradeConfirmIngest(src, "import-json.json", body);
    }

    /** v1 economic broker confirm ingest (gap plan §5.1) — multipart file upload. */
    @PostMapping(value = "/broker-trade-confirms/file-import", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<BrokerTradeConfirmImportResponse> importBrokerTradeConfirmsFile(
            @RequestParam("source") String source, @RequestPart("file") MultipartFile file) {
        if (source == null || source.isBlank() || file == null || file.isEmpty()) {
            return ResponseEntity.badRequest().build();
        }
        long max = config.getSettlement().getFileImportMaxBytes();
        if (file.getSize() > 0 && file.getSize() > max) {
            return ResponseEntity.status(HttpStatus.PAYLOAD_TOO_LARGE).build();
        }
        final byte[] bytes;
        try {
            bytes = file.getBytes();
        } catch (java.io.IOException e) {
            return ResponseEntity.badRequest().build();
        }
        if (bytes.length > max) {
            return ResponseEntity.status(HttpStatus.PAYLOAD_TOO_LARGE).build();
        }
        return runBrokerTradeConfirmIngest(source.trim(), file.getOriginalFilename(), bytes);
    }

    /**
     * Matcher batch trigger (gap plan §5.2). Resolves up to {@code maxBatch} pending
     * {@code broker_trade_confirm} rows to OMS executions, compares economic fields, and
     * writes one of three outcomes per row: {@code matched} (also enqueues v1
     * {@code broker_settlement_confirm} so settlement advances), {@code mismatch} or
     * {@code unresolved} (opens a {@code reconciliation_breaks} row).
     */
    @PostMapping("/broker-trade-confirms/process-pending-matches")
    public ResponseEntity<BrokerTradeMatchBatchResponse> processPendingMatches(
            @RequestParam(name = "maxBatch", required = false) Integer maxBatch) {
        int cap = maxBatch == null ? config.getSettlement().getBrokerConfirmReconcilerBatchSize() : maxBatch;
        var results = brokerTradeConfirmMatcher.processPendingBatch(cap);
        return ResponseEntity.ok(new BrokerTradeMatchBatchResponse(results.stream()
                .map(r -> new BrokerTradeMatchResultResponse(
                        r.confirmId(),
                        r.executionId(),
                        r.outcome().name().toLowerCase(java.util.Locale.ROOT),
                        r.diffJson()))
                .toList()));
    }

    /** Manual single-row match trigger (gap plan §5.2). Returns 404 if the row is not pending. */
    @PostMapping("/broker-trade-confirms/{id}/match")
    public ResponseEntity<BrokerTradeMatchResultResponse> matchOneConfirm(@PathVariable long id) {
        return brokerTradeConfirmMatcher
                .matchById(id)
                .map(r -> new BrokerTradeMatchResultResponse(
                        r.confirmId(),
                        r.executionId(),
                        r.outcome().name().toLowerCase(java.util.Locale.ROOT),
                        r.diffJson()))
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    /** Beard Admin reads the break queue here (gap plan §5.13). */
    @GetMapping("/reconciliation-breaks")
    public ResponseEntity<ReconciliationBreaksListResponse> listReconciliationBreaks(
            @RequestParam(required = false) String status,
            @RequestParam(required = false) Integer limit,
            @RequestParam(required = false) Integer offset) {
        String st = (status == null || status.isBlank()) ? "open" : status;
        int max = config.getSettlement().getFileImportListMaxLimit();
        int def = config.getSettlement().getFileImportListDefaultLimit();
        int lim = limit == null ? def : limit;
        if (lim < 1) {
            lim = def;
        }
        lim = Math.min(lim, max);
        int off = offset == null ? 0 : offset;
        if (off < 0 || off > MAX_FILE_IMPORT_LIST_OFFSET) {
            return ResponseEntity.badRequest().build();
        }
        List<ReconciliationBreakRepository.BreakRow> rows;
        try {
            rows = reconciliationBreaks.listByStatus(st, lim, off);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().build();
        }
        return ResponseEntity.ok(new ReconciliationBreaksListResponse(rows, st, lim, off));
    }

    private ResponseEntity<BrokerTradeConfirmImportResponse> runBrokerTradeConfirmIngest(
            String source, String filename, byte[] bytes) {
        try {
            BrokerTradeConfirmIngestService.Result r =
                    brokerTradeConfirmIngestService.ingest(source, filename, bytes);
            return ResponseEntity.ok(new BrokerTradeConfirmImportResponse(
                    r.duplicate(),
                    r.batchId(),
                    r.status(),
                    r.rowCount(),
                    r.errorSummary(),
                    r.insertedRows(),
                    r.insertedFees(),
                    r.skippedDuplicateRows()));
        } catch (IllegalArgumentException ex) {
            if ("file too large".equals(ex.getMessage())) {
                return ResponseEntity.status(HttpStatus.PAYLOAD_TOO_LARGE).build();
            }
            return ResponseEntity.badRequest().build();
        }
    }

    @PostMapping(value = "/file-import", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<SettlementFileImportResponse> importSettlementFile(
            @RequestParam("source") String source, @RequestPart("file") MultipartFile file) {
        if (source == null || source.isBlank() || file == null || file.isEmpty()) {
            return ResponseEntity.badRequest().build();
        }
        long max = config.getSettlement().getFileImportMaxBytes();
        if (file.getSize() > 0 && file.getSize() > max) {
            return ResponseEntity.status(HttpStatus.PAYLOAD_TOO_LARGE).build();
        }
        final byte[] bytes;
        try {
            bytes = file.getBytes();
        } catch (java.io.IOException e) {
            return ResponseEntity.badRequest().build();
        }
        if (bytes.length > max) {
            return ResponseEntity.status(HttpStatus.PAYLOAD_TOO_LARGE).build();
        }
        try {
            SettlementFileImportService.Result r =
                    settlementFileImportService.ingestMultipart(source, file.getOriginalFilename(), bytes);
            return ResponseEntity.ok(
                    new SettlementFileImportResponse(
                            r.duplicate(),
                            r.batchId(),
                            r.status(),
                            r.rowCount(),
                            r.errorSummary(),
                            r.insertedConfirms(),
                            r.skippedInvalid(),
                            r.skippedUnresolved()));
        } catch (IllegalArgumentException ex) {
            if ("file too large".equals(ex.getMessage())) {
                return ResponseEntity.status(HttpStatus.PAYLOAD_TOO_LARGE).build();
            }
            return ResponseEntity.badRequest().build();
        }
    }

    @GetMapping("/file-import-batches")
    public ResponseEntity<SettlementFileImportBatchesResponse> listFileImportBatches(
            @RequestParam(required = false) Integer limit, @RequestParam(required = false) Integer offset) {
        int max = config.getSettlement().getFileImportListMaxLimit();
        int def = config.getSettlement().getFileImportListDefaultLimit();
        int lim = limit == null ? def : limit;
        if (lim < 1) {
            lim = def;
        }
        lim = Math.min(lim, max);
        int off = offset == null ? 0 : offset;
        if (off < 0 || off > MAX_FILE_IMPORT_LIST_OFFSET) {
            return ResponseEntity.badRequest().build();
        }
        var rows = settlementFileImportBatchRepository.listRecentBatches(lim, off);
        return ResponseEntity.ok(new SettlementFileImportBatchesResponse(rows, lim, off));
    }

    @PostMapping("/process-pending")
    public ResponseEntity<BrokerConfirmProcessResponse> processPending(
            @RequestParam(name = "maxBatch", required = false) Integer maxBatch) {
        int cap = maxBatch == null
                ? config.getSettlement().getBrokerConfirmReconcilerBatchSize()
                : Math.min(maxBatch, config.getSettlement().getBrokerConfirmReconcilerBatchSize());
        int n = processor.processPendingBatch(cap);
        return ResponseEntity.ok(new BrokerConfirmProcessResponse(n));
    }

    @PostMapping("/executions/{executionId}/advance-one-step")
    public ResponseEntity<SettlementStepResponse> advanceOneStep(@PathVariable long executionId) {
        try {
            String next = processor.advanceOneSettlementStep(executionId);
            if (next == null) {
                return ResponseEntity.notFound().build();
            }
            return ResponseEntity.ok(new SettlementStepResponse(next));
        } catch (IllegalArgumentException ex) {
            return ResponseEntity.badRequest().build();
        }
    }

    @PostMapping("/executions/{executionId}/mark-failed")
    public ResponseEntity<?> markTradeFailed(@PathVariable long executionId) {
        MarkTradeFailedResult r = processor.markTradeFailed(executionId);
        return switch (r) {
            case NOT_FOUND -> ResponseEntity.notFound().build();
            case NOT_TRADE -> ResponseEntity.badRequest().build();
            case ALREADY_SETTLED -> ResponseEntity.status(HttpStatus.CONFLICT).build();
            case ALREADY_FAILED, APPLIED -> ResponseEntity.ok(new MarkTradeFailedResponse(r.name()));
        };
    }
}
