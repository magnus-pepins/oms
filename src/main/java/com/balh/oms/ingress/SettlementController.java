package com.balh.oms.ingress;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.persistence.SettlementExecutionsRepository;
import com.balh.oms.settlement.BrokerConfirmBatchRepository;
import com.balh.oms.settlement.BrokerTradeConfirmBatchLifecycleService;
import com.balh.oms.settlement.BrokerFixtureRow;
import com.balh.oms.settlement.BrokerCashStatementBatchRepository;
import com.balh.oms.settlement.BrokerCashStatementIngestService;
import com.balh.oms.settlement.BrokerCorporateActionApplyService;
import com.balh.oms.settlement.BrokerCorporateActionBatchRepository;
import com.balh.oms.settlement.BrokerCorporateActionIngestService;
import com.balh.oms.settlement.BrokerSettlementFailApplyService;
import com.balh.oms.settlement.BrokerSettlementFailBatchRepository;
import com.balh.oms.settlement.BrokerSettlementFailIngestService;
import com.balh.oms.settlement.BrokerPositionSnapshotBatchRepository;
import com.balh.oms.settlement.BrokerPositionSnapshotIngestService;
import com.balh.oms.settlement.BrokerTradeConfirmIngestService;
import com.balh.oms.settlement.BrokerTradeConfirmMatcher;
import com.balh.oms.settlement.InstrumentSettlementProfileIngestService;
import com.balh.oms.settlement.MarkTradeFailedResult;
import com.balh.oms.settlement.LedgerSettlementOutboxRepository;
import com.balh.oms.settlement.CashReconciliationReportRepository;
import com.balh.oms.settlement.CorporateActionReconciliationReportRepository;
import com.balh.oms.settlement.CorporateActionReconciliationService;
import com.balh.oms.settlement.CashReconciliationService;
import com.balh.oms.settlement.PositionReconciliationReportRepository;
import com.balh.oms.settlement.PositionReconciliationService;
import com.balh.oms.settlement.ReconciliationBreakCsvExporter;
import com.balh.oms.settlement.ReconciliationBreakEventRepository;
import com.balh.oms.settlement.ReconciliationBreakRepository;
import com.balh.oms.settlement.ReconciliationBreakWorkflowService;
import com.balh.oms.settlement.SettlementEvidencePackService;
import com.balh.oms.settlement.SettlementCalendarIngestService;
import com.balh.oms.settlement.SettlementConfirmProcessor;
import com.balh.oms.settlement.SettlementFileImportBatchRepository;
import com.balh.oms.settlement.SettlementFileIngestRouter;
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

    public record BrokerConfirmBatchListItemResponse(
            long id,
            String brokerId,
            String brokerFileId,
            java.time.LocalDate businessDate,
            String status,
            Integer rowCount,
            Integer matchedRowCount,
            Integer breakRowCount,
            Instant receivedAt,
            Instant appliedAt,
            String errorSummary) {}

    public record BrokerConfirmBatchListResponse(List<BrokerConfirmBatchListItemResponse> items, int limit, int offset) {}

    public record BrokerConfirmBatchMatchResponse(
            long batchId,
            String status,
            int matchedRows,
            int breakRows,
            List<BrokerTradeMatchResultResponse> lastBatchResults) {}

    public record BrokerCashStatementImportResponse(
            boolean duplicate,
            long batchId,
            String status,
            Integer movementCount,
            String errorSummary,
            int insertedMovements,
            int skippedDuplicateMovements) {}

    public record BrokerCashStatementBatchListItemResponse(
            long id,
            String brokerId,
            String brokerFileId,
            java.time.LocalDate businessDate,
            String currency,
            String status,
            Integer movementCount,
            java.math.BigDecimal openingBalance,
            java.math.BigDecimal closingBalance,
            Instant receivedAt,
            String errorSummary) {}

    public record BrokerCashStatementBatchListResponse(
            List<BrokerCashStatementBatchListItemResponse> items, int limit, int offset) {}

    public record BrokerSettlementFailImportResponse(
            boolean duplicate,
            long batchId,
            String status,
            Integer failCount,
            String errorSummary,
            int insertedFails,
            int skippedDuplicateFails) {}

    public record BrokerSettlementFailBatchListItemResponse(
            long id,
            String brokerId,
            String brokerFileId,
            java.time.LocalDate businessDate,
            String status,
            Integer failCount,
            Instant receivedAt,
            String errorSummary) {}

    public record BrokerSettlementFailBatchListResponse(
            List<BrokerSettlementFailBatchListItemResponse> items, int limit, int offset) {}

    public record BrokerSettlementFailApplyResponse(
            long batchId,
            String status,
            int failRowCount,
            int appliedCount,
            int fullFailCount,
            int partialFailCount,
            int unmatchedCount,
            int quantityMismatchCount,
            int skippedAlreadyApplied) {}

    public record BrokerCorporateActionImportResponse(
            boolean duplicate,
            long batchId,
            String status,
            Integer eventCount,
            String errorSummary,
            int insertedEvents,
            int skippedDuplicateEvents) {}

    public record BrokerCorporateActionBatchListItemResponse(
            long id,
            String brokerId,
            String brokerFileId,
            java.time.LocalDate businessDate,
            String status,
            Integer eventCount,
            Instant receivedAt,
            String errorSummary) {}

    public record BrokerCorporateActionBatchListResponse(
            List<BrokerCorporateActionBatchListItemResponse> items, int limit, int offset) {}

    public record BrokerCorporateActionApplyResponse(
            long batchId,
            String status,
            int eventRowCount,
            int insertedCount,
            int duplicateCount,
            int skippedAlreadyApplied) {}

    public record CashReconciliationResponse(
            long reportId,
            long batchId,
            String status,
            int movementCount,
            int matchedCount,
            int mismatchCount,
            int unmatchedCount,
            int missingInBrokerCount,
            boolean balanceMismatch,
            boolean nostroBalanceMismatch) {}

    public record CorporateActionReconciliationResponse(
            long reportId,
            long batchId,
            String status,
            int eventCount,
            int matchedCount,
            int mismatchCount,
            int unmatchedCount) {}

    public record CorporateActionReconciliationReportListResponse(
            List<CorporateActionReconciliationReportRepository.ReportRow> items, int limit, int offset) {}

    public record CashReconciliationReportListResponse(
            List<CashReconciliationReportRepository.ReportRow> items, int limit, int offset) {}

    public record CashReconciliationReportDetailResponse(
            CashReconciliationReportRepository.ReportRow report,
            List<CashReconciliationReportRepository.ReportDetailRow> rows) {}

    public record BrokerPositionSnapshotImportResponse(
            boolean duplicate,
            long batchId,
            String status,
            Integer rowCount,
            String errorSummary,
            int insertedRows,
            int skippedDuplicateRows) {}

    public record BrokerPositionSnapshotBatchListItemResponse(
            long id,
            String brokerId,
            String brokerFileId,
            java.time.LocalDate businessDate,
            String status,
            Integer rowCount,
            Instant receivedAt,
            String errorSummary) {}

    public record BrokerPositionSnapshotBatchListResponse(
            List<BrokerPositionSnapshotBatchListItemResponse> items, int limit, int offset) {}

    public record PositionReconciliationResponse(
            long reportId,
            long batchId,
            String status,
            int rowCount,
            int matchedCount,
            int mismatchCount,
            int missingInOmsCount,
            int missingInBrokerCount) {}

    public record PositionReconciliationReportListResponse(
            List<PositionReconciliationReportRepository.ReportRow> items, int limit, int offset) {}

    public record PositionReconciliationReportDetailResponse(
            PositionReconciliationReportRepository.ReportRow report,
            List<PositionReconciliationReportRepository.ReportDetailRow> rows) {}

    public record ReconciliationBreaksListResponse(
            List<ReconciliationBreakRepository.BreakRow> items, String status, int limit, int offset) {}

    public record ReconciliationBreakSummaryResponse(
            String status,
            int total,
            java.util.Map<String, Integer> byBreakType,
            java.util.Map<String, Integer> bySeverity) {}

    public record AssignReconciliationBreakRequest(String assignedTo, String actor, String notes) {}

    public record ResolveReconciliationBreakRequest(String resolutionCode, String resolutionNote, String actor) {}

    public record WaiveReconciliationBreakRequest(String resolutionNote, String actor) {}

    public record ReconciliationBreakEventsResponse(
            List<ReconciliationBreakEventRepository.EventRow> items) {}

    public record MarkTradeFailedResponse(String result) {}

    public record SettlementProfileIngestResponse(
            int inserted, int updated, List<InstrumentSettlementProfileIngestService.RejectedRow> rejected) {}

    public record SettlementCalendarIngestResponse(
            int inserted, int updated, List<SettlementCalendarIngestService.RejectedRow> rejected) {}

    private final SettlementConfirmProcessor processor;
    private final OmsConfig config;
    private final SettlementExecutionsRepository settlementExecutions;
    private final SettlementFileIngestRouter settlementFileIngestRouter;
    private final SettlementFileImportBatchRepository settlementFileImportBatchRepository;
    private final SettlementTimelineService timelineService;
    private final SettlementEvidencePackService evidencePackService;
    private final BrokerTradeConfirmIngestService brokerTradeConfirmIngestService;
    private final BrokerTradeConfirmMatcher brokerTradeConfirmMatcher;
    private final BrokerTradeConfirmBatchLifecycleService brokerTradeConfirmBatchLifecycle;
    private final BrokerConfirmBatchRepository brokerConfirmBatches;
    private final BrokerPositionSnapshotIngestService brokerPositionSnapshotIngestService;
    private final BrokerPositionSnapshotBatchRepository brokerPositionSnapshotBatches;
    private final PositionReconciliationService positionReconciliationService;
    private final PositionReconciliationReportRepository positionReconciliationReports;
    private final BrokerCashStatementIngestService brokerCashStatementIngestService;
    private final BrokerCashStatementBatchRepository brokerCashStatementBatches;
    private final CashReconciliationService cashReconciliationService;
    private final CashReconciliationReportRepository cashReconciliationReports;
    private final BrokerSettlementFailIngestService brokerSettlementFailIngestService;
    private final BrokerSettlementFailBatchRepository brokerSettlementFailBatches;
    private final BrokerSettlementFailApplyService brokerSettlementFailApplyService;
    private final BrokerCorporateActionIngestService brokerCorporateActionIngestService;
    private final BrokerCorporateActionBatchRepository brokerCorporateActionBatches;
    private final BrokerCorporateActionApplyService brokerCorporateActionApplyService;
    private final CorporateActionReconciliationService corporateActionReconciliationService;
    private final CorporateActionReconciliationReportRepository corporateActionReconciliationReports;
    private final ReconciliationBreakRepository reconciliationBreaks;
    private final ReconciliationBreakWorkflowService reconciliationBreakWorkflow;
    private final InstrumentSettlementProfileIngestService instrumentSettlementProfileIngestService;
    private final SettlementCalendarIngestService settlementCalendarIngestService;

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
            SettlementFileIngestRouter settlementFileIngestRouter,
            SettlementFileImportBatchRepository settlementFileImportBatchRepository,
            SettlementTimelineService timelineService,
            SettlementEvidencePackService evidencePackService,
            LedgerSettlementOutboxRepository ledgerSettlementOutbox,
            BrokerTradeConfirmIngestService brokerTradeConfirmIngestService,
            BrokerTradeConfirmMatcher brokerTradeConfirmMatcher,
            BrokerTradeConfirmBatchLifecycleService brokerTradeConfirmBatchLifecycle,
            BrokerConfirmBatchRepository brokerConfirmBatches,
            BrokerPositionSnapshotIngestService brokerPositionSnapshotIngestService,
            BrokerPositionSnapshotBatchRepository brokerPositionSnapshotBatches,
            PositionReconciliationService positionReconciliationService,
            PositionReconciliationReportRepository positionReconciliationReports,
            BrokerCashStatementIngestService brokerCashStatementIngestService,
            BrokerCashStatementBatchRepository brokerCashStatementBatches,
            CashReconciliationService cashReconciliationService,
            CashReconciliationReportRepository cashReconciliationReports,
            BrokerSettlementFailIngestService brokerSettlementFailIngestService,
            BrokerSettlementFailBatchRepository brokerSettlementFailBatches,
            BrokerSettlementFailApplyService brokerSettlementFailApplyService,
            BrokerCorporateActionIngestService brokerCorporateActionIngestService,
            BrokerCorporateActionBatchRepository brokerCorporateActionBatches,
            BrokerCorporateActionApplyService brokerCorporateActionApplyService,
            CorporateActionReconciliationService corporateActionReconciliationService,
            CorporateActionReconciliationReportRepository corporateActionReconciliationReports,
            ReconciliationBreakRepository reconciliationBreaks,
            ReconciliationBreakWorkflowService reconciliationBreakWorkflow,
            InstrumentSettlementProfileIngestService instrumentSettlementProfileIngestService,
            SettlementCalendarIngestService settlementCalendarIngestService) {
        this.processor = processor;
        this.config = config;
        this.settlementExecutions = settlementExecutions;
        this.settlementFileIngestRouter = settlementFileIngestRouter;
        this.settlementFileImportBatchRepository = settlementFileImportBatchRepository;
        this.timelineService = timelineService;
        this.evidencePackService = evidencePackService;
        this.ledgerSettlementOutbox = ledgerSettlementOutbox;
        this.brokerTradeConfirmIngestService = brokerTradeConfirmIngestService;
        this.brokerTradeConfirmMatcher = brokerTradeConfirmMatcher;
        this.brokerTradeConfirmBatchLifecycle = brokerTradeConfirmBatchLifecycle;
        this.brokerConfirmBatches = brokerConfirmBatches;
        this.brokerPositionSnapshotIngestService = brokerPositionSnapshotIngestService;
        this.brokerPositionSnapshotBatches = brokerPositionSnapshotBatches;
        this.positionReconciliationService = positionReconciliationService;
        this.positionReconciliationReports = positionReconciliationReports;
        this.brokerCashStatementIngestService = brokerCashStatementIngestService;
        this.brokerCashStatementBatches = brokerCashStatementBatches;
        this.cashReconciliationService = cashReconciliationService;
        this.cashReconciliationReports = cashReconciliationReports;
        this.brokerSettlementFailIngestService = brokerSettlementFailIngestService;
        this.brokerSettlementFailBatches = brokerSettlementFailBatches;
        this.brokerSettlementFailApplyService = brokerSettlementFailApplyService;
        this.brokerCorporateActionIngestService = brokerCorporateActionIngestService;
        this.brokerCorporateActionBatches = brokerCorporateActionBatches;
        this.brokerCorporateActionApplyService = brokerCorporateActionApplyService;
        this.corporateActionReconciliationService = corporateActionReconciliationService;
        this.corporateActionReconciliationReports = corporateActionReconciliationReports;
        this.reconciliationBreaks = reconciliationBreaks;
        this.reconciliationBreakWorkflow = reconciliationBreakWorkflow;
        this.instrumentSettlementProfileIngestService = instrumentSettlementProfileIngestService;
        this.settlementCalendarIngestService = settlementCalendarIngestService;
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
                                                r.instrumentSymbol(),
                                                r.tradeDate(),
                                                r.expectedSettlementDate()))
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

    /**
     * Regulatory / ops evidence bundle for one execution (gap plan §5.17). Aggregates execution,
     * timeline, broker confirms, breaks, and manual actions without log scraping.
     */
    @GetMapping("/executions/{executionId}/evidence-pack")
    public ResponseEntity<SettlementEvidencePackResponse> getEvidencePack(@PathVariable long executionId) {
        return evidencePackService
                .load(executionId)
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
                                        r.tradeDate(),
                                        r.expectedSettlementDate(),
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

    /**
     * Match all pending confirms in one ingest batch and advance {@code broker_confirm_batch}
     * to {@code applied} (gap plan §5.1 batch lifecycle).
     */
    @PostMapping("/broker-trade-confirms/batches/{batchId}/process-matches")
    public ResponseEntity<BrokerConfirmBatchMatchResponse> processBatchMatches(@PathVariable long batchId) {
        return brokerTradeConfirmBatchLifecycle
                .processBatchMatches(batchId)
                .map(summary -> ResponseEntity.ok(new BrokerConfirmBatchMatchResponse(
                        summary.batchId(),
                        summary.finalStatus(),
                        summary.matchedRows(),
                        summary.breakRows(),
                        summary.results().stream()
                                .map(r -> new BrokerTradeMatchResultResponse(
                                        r.confirmId(),
                                        r.executionId(),
                                        r.outcome().name().toLowerCase(java.util.Locale.ROOT),
                                        r.diffJson()))
                                .toList())))
                .orElse(ResponseEntity.notFound().build());
    }

    /** Ops listing for v2 economic confirm ingest batches (gap plan §5.1). */
    @GetMapping("/broker-trade-confirms/batches")
    public ResponseEntity<BrokerConfirmBatchListResponse> listBrokerConfirmBatches(
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
        List<BrokerConfirmBatchListItemResponse> items =
                brokerConfirmBatches.listRecent(lim, off).stream()
                        .map(r -> new BrokerConfirmBatchListItemResponse(
                                r.id(),
                                r.brokerId(),
                                r.brokerFileId(),
                                r.businessDate(),
                                r.status(),
                                r.rowCount(),
                                r.matchedRowCount(),
                                r.breakRowCount(),
                                r.receivedAt(),
                                r.appliedAt(),
                                r.errorSummary()))
                        .toList();
        return ResponseEntity.ok(new BrokerConfirmBatchListResponse(items, lim, off));
    }

    /** Broker position snapshot ingest (gap plan §5.6) — JSON body. */
    @PostMapping("/broker-position-snapshots/import-json")
    public ResponseEntity<BrokerPositionSnapshotImportResponse> importBrokerPositionSnapshots(
            @RequestParam(name = "source", required = false) String source,
            @RequestBody(required = false) byte[] body) {
        String src = (source == null || source.isBlank()) ? "http-json" : source.trim();
        if (body == null || body.length == 0) {
            return ResponseEntity.badRequest().build();
        }
        return runBrokerPositionSnapshotIngest(src, "import-json.json", body);
    }

    /** Broker position snapshot ingest (gap plan §5.6) — multipart file upload. */
    @PostMapping(value = "/broker-position-snapshots/file-import", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<BrokerPositionSnapshotImportResponse> importBrokerPositionSnapshotsFile(
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
        return runBrokerPositionSnapshotIngest(source.trim(), file.getOriginalFilename(), bytes);
    }

    @GetMapping("/broker-position-snapshots/batches")
    public ResponseEntity<BrokerPositionSnapshotBatchListResponse> listBrokerPositionSnapshotBatches(
            @RequestParam(required = false) Integer limit, @RequestParam(required = false) Integer offset) {
        int lim = resolveListLimit(limit);
        int off = resolveListOffset(offset);
        if (off < 0) {
            return ResponseEntity.badRequest().build();
        }
        List<BrokerPositionSnapshotBatchListItemResponse> items =
                brokerPositionSnapshotBatches.listRecent(lim, off).stream()
                        .map(r -> new BrokerPositionSnapshotBatchListItemResponse(
                                r.id(),
                                r.brokerId(),
                                r.brokerFileId(),
                                r.businessDate(),
                                r.status(),
                                r.rowCount(),
                                r.receivedAt(),
                                r.errorSummary()))
                        .toList();
        return ResponseEntity.ok(new BrokerPositionSnapshotBatchListResponse(items, lim, off));
    }

    @PostMapping("/broker-position-snapshots/batches/{batchId}/reconcile")
    public ResponseEntity<PositionReconciliationResponse> reconcileBrokerPositionSnapshotBatch(
            @PathVariable long batchId) {
        try {
            return positionReconciliationService
                    .reconcile(batchId)
                    .map(r -> ResponseEntity.ok(new PositionReconciliationResponse(
                            r.reportId(),
                            r.batchId(),
                            r.status(),
                            r.rowCount(),
                            r.matchedCount(),
                            r.mismatchCount(),
                            r.missingInOmsCount(),
                            r.missingInBrokerCount())))
                    .orElse(ResponseEntity.notFound().build());
        } catch (IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT).build();
        }
    }

    @GetMapping("/position-reconciliation-reports")
    public ResponseEntity<PositionReconciliationReportListResponse> listPositionReconciliationReports(
            @RequestParam(required = false) Integer limit, @RequestParam(required = false) Integer offset) {
        int lim = resolveListLimit(limit);
        int off = resolveListOffset(offset);
        if (off < 0) {
            return ResponseEntity.badRequest().build();
        }
        return ResponseEntity.ok(new PositionReconciliationReportListResponse(
                positionReconciliationReports.listRecent(lim, off), lim, off));
    }

    @GetMapping("/position-reconciliation-reports/{reportId}")
    public ResponseEntity<PositionReconciliationReportDetailResponse> getPositionReconciliationReport(
            @PathVariable long reportId) {
        List<PositionReconciliationReportRepository.ReportRow> reports =
                positionReconciliationReports.listRecent(500, 0);
        PositionReconciliationReportRepository.ReportRow report =
                reports.stream().filter(r -> r.id() == reportId).findFirst().orElse(null);
        if (report == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(new PositionReconciliationReportDetailResponse(
                report, positionReconciliationReports.listDetails(reportId, 500)));
    }

    /** Broker cash statement ingest (gap plan §5.7) — JSON body. */
    @PostMapping("/broker-cash-statements/import-json")
    public ResponseEntity<BrokerCashStatementImportResponse> importBrokerCashStatements(
            @RequestParam(name = "source", required = false) String source,
            @RequestBody(required = false) byte[] body) {
        String src = (source == null || source.isBlank()) ? "http-json" : source.trim();
        if (body == null || body.length == 0) {
            return ResponseEntity.badRequest().build();
        }
        return runBrokerCashStatementIngest(src, "import-json.json", body);
    }

    @PostMapping(value = "/broker-cash-statements/file-import", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<BrokerCashStatementImportResponse> importBrokerCashStatementsFile(
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
        return runBrokerCashStatementIngest(source.trim(), file.getOriginalFilename(), bytes);
    }

    @GetMapping("/broker-cash-statements/batches")
    public ResponseEntity<BrokerCashStatementBatchListResponse> listBrokerCashStatementBatches(
            @RequestParam(required = false) Integer limit, @RequestParam(required = false) Integer offset) {
        int lim = resolveListLimit(limit);
        int off = resolveListOffset(offset);
        if (off < 0) {
            return ResponseEntity.badRequest().build();
        }
        List<BrokerCashStatementBatchListItemResponse> items =
                brokerCashStatementBatches.listRecent(lim, off).stream()
                        .map(r -> new BrokerCashStatementBatchListItemResponse(
                                r.id(),
                                r.brokerId(),
                                r.brokerFileId(),
                                r.businessDate(),
                                r.currency(),
                                r.status(),
                                r.movementCount(),
                                r.openingBalance(),
                                r.closingBalance(),
                                r.receivedAt(),
                                r.errorSummary()))
                        .toList();
        return ResponseEntity.ok(new BrokerCashStatementBatchListResponse(items, lim, off));
    }

    @PostMapping("/broker-cash-statements/batches/{batchId}/reconcile")
    public ResponseEntity<CashReconciliationResponse> reconcileBrokerCashStatementBatch(@PathVariable long batchId) {
        try {
            return cashReconciliationService
                    .reconcile(batchId)
                    .map(r -> ResponseEntity.ok(new CashReconciliationResponse(
                            r.reportId(),
                            r.batchId(),
                            r.status(),
                            r.movementCount(),
                            r.matchedCount(),
                            r.mismatchCount(),
                            r.unmatchedCount(),
                            r.missingInBrokerCount(),
                            r.balanceMismatch(),
                            r.nostroBalanceMismatch())))
                    .orElse(ResponseEntity.notFound().build());
        } catch (IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT).build();
        }
    }

    @GetMapping("/cash-reconciliation-reports")
    public ResponseEntity<CashReconciliationReportListResponse> listCashReconciliationReports(
            @RequestParam(required = false) Integer limit, @RequestParam(required = false) Integer offset) {
        int lim = resolveListLimit(limit);
        int off = resolveListOffset(offset);
        if (off < 0) {
            return ResponseEntity.badRequest().build();
        }
        return ResponseEntity.ok(new CashReconciliationReportListResponse(
                cashReconciliationReports.listRecent(lim, off), lim, off));
    }

    @GetMapping("/cash-reconciliation-reports/{reportId}")
    public ResponseEntity<CashReconciliationReportDetailResponse> getCashReconciliationReport(
            @PathVariable long reportId) {
        List<CashReconciliationReportRepository.ReportRow> all =
                cashReconciliationReports.listRecent(500, 0);
        CashReconciliationReportRepository.ReportRow report =
                all.stream().filter(r -> r.id() == reportId).findFirst().orElse(null);
        if (report == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(new CashReconciliationReportDetailResponse(
                report, cashReconciliationReports.listDetails(reportId, 500)));
    }

    /** Broker settlement fail file ingest (gap plan §5.8) — JSON body. */
    @PostMapping("/broker-settlement-fails/import-json")
    public ResponseEntity<BrokerSettlementFailImportResponse> importBrokerSettlementFails(
            @RequestParam(name = "source", required = false) String source,
            @RequestBody(required = false) byte[] body) {
        String src = (source == null || source.isBlank()) ? "http-json" : source.trim();
        if (body == null || body.length == 0) {
            return ResponseEntity.badRequest().build();
        }
        return runBrokerSettlementFailIngest(src, "import-json.json", body);
    }

    @PostMapping(value = "/broker-settlement-fails/file-import", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<BrokerSettlementFailImportResponse> importBrokerSettlementFailsFile(
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
        return runBrokerSettlementFailIngest(source.trim(), file.getOriginalFilename(), bytes);
    }

    @GetMapping("/broker-settlement-fails/batches")
    public ResponseEntity<BrokerSettlementFailBatchListResponse> listBrokerSettlementFailBatches(
            @RequestParam(required = false) Integer limit, @RequestParam(required = false) Integer offset) {
        int lim = resolveListLimit(limit);
        int off = resolveListOffset(offset);
        if (off < 0) {
            return ResponseEntity.badRequest().build();
        }
        List<BrokerSettlementFailBatchListItemResponse> items =
                brokerSettlementFailBatches.listRecent(lim, off).stream()
                        .map(r -> new BrokerSettlementFailBatchListItemResponse(
                                r.id(),
                                r.brokerId(),
                                r.brokerFileId(),
                                r.businessDate(),
                                r.status(),
                                r.failCount(),
                                r.receivedAt(),
                                r.errorSummary()))
                        .toList();
        return ResponseEntity.ok(new BrokerSettlementFailBatchListResponse(items, lim, off));
    }

    @PostMapping("/broker-settlement-fails/batches/{batchId}/apply")
    public ResponseEntity<BrokerSettlementFailApplyResponse> applyBrokerSettlementFailBatch(
            @PathVariable long batchId) {
        try {
            return brokerSettlementFailApplyService
                    .apply(batchId)
                    .map(r -> ResponseEntity.ok(new BrokerSettlementFailApplyResponse(
                            r.batchId(),
                            r.status(),
                            r.failRowCount(),
                            r.appliedCount(),
                            r.fullFailCount(),
                            r.partialFailCount(),
                            r.unmatchedCount(),
                            r.quantityMismatchCount(),
                            r.skippedAlreadyApplied())))
                    .orElse(ResponseEntity.notFound().build());
        } catch (IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT).build();
        }
    }

    @PostMapping("/broker-corporate-actions/import-json")
    public ResponseEntity<BrokerCorporateActionImportResponse> importBrokerCorporateActions(
            @RequestParam(name = "source", required = false) String source,
            @RequestBody(required = false) byte[] body) {
        String src = (source == null || source.isBlank()) ? "http-json" : source.trim();
        if (body == null || body.length == 0) {
            return ResponseEntity.badRequest().build();
        }
        return runBrokerCorporateActionIngest(src, "import-json.json", body);
    }

    @PostMapping(value = "/broker-corporate-actions/file-import", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<BrokerCorporateActionImportResponse> importBrokerCorporateActionsFile(
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
        return runBrokerCorporateActionIngest(source.trim(), file.getOriginalFilename(), bytes);
    }

    @GetMapping("/broker-corporate-actions/batches")
    public ResponseEntity<BrokerCorporateActionBatchListResponse> listBrokerCorporateActionBatches(
            @RequestParam(required = false) Integer limit, @RequestParam(required = false) Integer offset) {
        int lim = resolveListLimit(limit);
        int off = resolveListOffset(offset);
        if (off < 0) {
            return ResponseEntity.badRequest().build();
        }
        List<BrokerCorporateActionBatchListItemResponse> items =
                brokerCorporateActionBatches.listRecent(lim, off).stream()
                        .map(r -> new BrokerCorporateActionBatchListItemResponse(
                                r.id(),
                                r.brokerId(),
                                r.brokerFileId(),
                                r.businessDate(),
                                r.status(),
                                r.eventCount(),
                                r.receivedAt(),
                                r.errorSummary()))
                        .toList();
        return ResponseEntity.ok(new BrokerCorporateActionBatchListResponse(items, lim, off));
    }

    @PostMapping("/broker-corporate-actions/batches/{batchId}/apply")
    public ResponseEntity<BrokerCorporateActionApplyResponse> applyBrokerCorporateActionBatch(
            @PathVariable long batchId) {
        try {
            return brokerCorporateActionApplyService
                    .apply(batchId)
                    .map(r -> ResponseEntity.ok(new BrokerCorporateActionApplyResponse(
                            r.batchId(),
                            r.status(),
                            r.eventRowCount(),
                            r.insertedCount(),
                            r.duplicateCount(),
                            r.skippedAlreadyApplied())))
                    .orElse(ResponseEntity.notFound().build());
        } catch (IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT).build();
        }
    }

    @PostMapping("/broker-corporate-actions/batches/{batchId}/reconcile")
    public ResponseEntity<CorporateActionReconciliationResponse> reconcileBrokerCorporateActionBatch(
            @PathVariable long batchId) {
        try {
            return corporateActionReconciliationService
                    .reconcile(batchId)
                    .map(r -> ResponseEntity.ok(new CorporateActionReconciliationResponse(
                            r.reportId(),
                            r.batchId(),
                            r.status(),
                            r.eventCount(),
                            r.matchedCount(),
                            r.mismatchCount(),
                            r.unmatchedCount())))
                    .orElse(ResponseEntity.notFound().build());
        } catch (IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT).build();
        }
    }

    @GetMapping("/corporate-action-reconciliation-reports")
    public ResponseEntity<CorporateActionReconciliationReportListResponse> listCorporateActionReconciliationReports(
            @RequestParam(required = false) Integer limit, @RequestParam(required = false) Integer offset) {
        int lim = resolveListLimit(limit);
        int off = resolveListOffset(offset);
        if (off < 0) {
            return ResponseEntity.badRequest().build();
        }
        return ResponseEntity.ok(new CorporateActionReconciliationReportListResponse(
                corporateActionReconciliationReports.listRecent(lim, off), lim, off));
    }

    /** Beard Admin reads the break queue here (gap plan §5.13). */
    @GetMapping("/reconciliation-breaks")
    public ResponseEntity<ReconciliationBreaksListResponse> listReconciliationBreaks(
            @RequestParam(required = false) String status,
            @RequestParam(required = false) String breakType,
            @RequestParam(required = false) String severity,
            @RequestParam(required = false) Integer limit,
            @RequestParam(required = false) Integer offset) {
        String st = (status == null || status.isBlank()) ? "open" : status;
        int lim = resolveListLimit(limit);
        int off = resolveListOffset(offset);
        if (off < 0) {
            return ResponseEntity.badRequest().build();
        }
        List<ReconciliationBreakRepository.BreakRow> rows;
        try {
            rows = reconciliationBreaks.listFiltered(st, breakType, severity, lim, off);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().build();
        }
        return ResponseEntity.ok(new ReconciliationBreaksListResponse(rows, st, lim, off));
    }

    @GetMapping("/reconciliation-breaks/summary")
    public ResponseEntity<ReconciliationBreakSummaryResponse> summarizeReconciliationBreaks(
            @RequestParam(required = false) String status) {
        String st = (status == null || status.isBlank()) ? "open" : status;
        try {
            ReconciliationBreakRepository.BreakSummary summary = reconciliationBreaks.summarizeByStatus(st);
            return ResponseEntity.ok(new ReconciliationBreakSummaryResponse(
                    st, summary.total(), summary.byBreakType(), summary.bySeverity()));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().build();
        }
    }

    @GetMapping("/reconciliation-breaks/{id}")
    public ResponseEntity<ReconciliationBreakRepository.BreakRow> getReconciliationBreak(@PathVariable long id) {
        return reconciliationBreaks.findById(id).map(ResponseEntity::ok).orElse(ResponseEntity.notFound().build());
    }

    @GetMapping(value = "/reconciliation-breaks/export", produces = "text/csv")
    public ResponseEntity<String> exportReconciliationBreaks(
            @RequestParam(required = false) String status,
            @RequestParam(required = false) String breakType,
            @RequestParam(required = false) String severity) {
        String st = (status == null || status.isBlank()) ? "open" : status;
        int exportMax = config.getSettlement().getReconciliationBreakExportMaxRows();
        List<ReconciliationBreakRepository.BreakRow> rows;
        try {
            rows = reconciliationBreaks.listFiltered(st, breakType, severity, exportMax, 0);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().build();
        }
        String csv = ReconciliationBreakCsvExporter.toCsv(rows);
        String filename = "reconciliation-breaks-" + st + "-" + java.time.LocalDate.now(java.time.ZoneOffset.UTC) + ".csv";
        return ResponseEntity.ok()
                .header(
                        org.springframework.http.HttpHeaders.CONTENT_DISPOSITION,
                        "attachment; filename=\"" + filename + "\"")
                .body(csv);
    }

    @PostMapping("/reconciliation-breaks/{id}/assign")
    public ResponseEntity<ReconciliationBreakRepository.BreakRow> assignReconciliationBreak(
            @PathVariable long id, @RequestBody AssignReconciliationBreakRequest body) {
        if (body == null) {
            return ResponseEntity.badRequest().build();
        }
        ReconciliationBreakWorkflowService.Result result =
                reconciliationBreakWorkflow.assign(id, body.assignedTo(), body.actor(), body.notes());
        return mapWorkflowResult(result);
    }

    @PostMapping("/reconciliation-breaks/{id}/resolve")
    public ResponseEntity<ReconciliationBreakRepository.BreakRow> resolveReconciliationBreak(
            @PathVariable long id, @RequestBody ResolveReconciliationBreakRequest body) {
        if (body == null) {
            return ResponseEntity.badRequest().build();
        }
        ReconciliationBreakWorkflowService.Result result = reconciliationBreakWorkflow.resolve(
                id, body.resolutionCode(), body.resolutionNote(), body.actor());
        return mapWorkflowResult(result);
    }

    @PostMapping("/reconciliation-breaks/{id}/waive")
    public ResponseEntity<ReconciliationBreakRepository.BreakRow> waiveReconciliationBreak(
            @PathVariable long id, @RequestBody WaiveReconciliationBreakRequest body) {
        if (body == null) {
            return ResponseEntity.badRequest().build();
        }
        ReconciliationBreakWorkflowService.Result result =
                reconciliationBreakWorkflow.waive(id, body.resolutionNote(), body.actor());
        return mapWorkflowResult(result);
    }

    @GetMapping("/reconciliation-breaks/{id}/events")
    public ResponseEntity<ReconciliationBreakEventsResponse> listReconciliationBreakEvents(@PathVariable long id) {
        if (reconciliationBreaks.findById(id).isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(new ReconciliationBreakEventsResponse(reconciliationBreakWorkflow.listEvents(id)));
    }

    private ResponseEntity<ReconciliationBreakRepository.BreakRow> mapWorkflowResult(
            ReconciliationBreakWorkflowService.Result result) {
        return switch (result.outcome()) {
            case APPLIED -> ResponseEntity.ok(result.breakRow());
            case NOT_FOUND -> ResponseEntity.notFound().build();
            case CONFLICT -> ResponseEntity.status(HttpStatus.CONFLICT).build();
            case INVALID_REQUEST -> ResponseEntity.badRequest().build();
        };
    }

    private int resolveListLimit(Integer limit) {
        int max = config.getSettlement().getFileImportListMaxLimit();
        int def = config.getSettlement().getFileImportListDefaultLimit();
        int lim = limit == null ? def : limit;
        if (lim < 1) {
            lim = def;
        }
        return Math.min(lim, max);
    }

    private int resolveListOffset(Integer offset) {
        int off = offset == null ? 0 : offset;
        if (off < 0 || off > MAX_FILE_IMPORT_LIST_OFFSET) {
            return -1;
        }
        return off;
    }

    private ResponseEntity<BrokerCashStatementImportResponse> runBrokerCashStatementIngest(
            String source, String filename, byte[] bytes) {
        try {
            BrokerCashStatementIngestService.Result r = brokerCashStatementIngestService.ingest(source, filename, bytes);
            return ResponseEntity.ok(new BrokerCashStatementImportResponse(
                    r.duplicate(),
                    r.batchId(),
                    r.status(),
                    r.movementCount(),
                    r.errorSummary(),
                    r.insertedMovements(),
                    r.skippedDuplicateMovements()));
        } catch (IllegalArgumentException ex) {
            if ("file too large".equals(ex.getMessage())) {
                return ResponseEntity.status(HttpStatus.PAYLOAD_TOO_LARGE).build();
            }
            return ResponseEntity.badRequest().build();
        }
    }

    private ResponseEntity<BrokerSettlementFailImportResponse> runBrokerSettlementFailIngest(
            String source, String filename, byte[] bytes) {
        try {
            BrokerSettlementFailIngestService.Result r =
                    brokerSettlementFailIngestService.ingest(source, filename, bytes);
            return ResponseEntity.ok(new BrokerSettlementFailImportResponse(
                    r.duplicate(),
                    r.batchId(),
                    r.status(),
                    r.failCount(),
                    r.errorSummary(),
                    r.insertedFails(),
                    r.skippedDuplicateFails()));
        } catch (IllegalArgumentException ex) {
            if ("file too large".equals(ex.getMessage())) {
                return ResponseEntity.status(HttpStatus.PAYLOAD_TOO_LARGE).build();
            }
            return ResponseEntity.badRequest().build();
        }
    }

    private ResponseEntity<BrokerCorporateActionImportResponse> runBrokerCorporateActionIngest(
            String source, String filename, byte[] bytes) {
        try {
            BrokerCorporateActionIngestService.Result r =
                    brokerCorporateActionIngestService.ingest(source, filename, bytes);
            return ResponseEntity.ok(new BrokerCorporateActionImportResponse(
                    r.duplicate(),
                    r.batchId(),
                    r.status(),
                    r.eventCount(),
                    r.errorSummary(),
                    r.insertedEvents(),
                    r.skippedDuplicateEvents()));
        } catch (IllegalArgumentException ex) {
            if ("file too large".equals(ex.getMessage())) {
                return ResponseEntity.status(HttpStatus.PAYLOAD_TOO_LARGE).build();
            }
            return ResponseEntity.badRequest().build();
        }
    }

    private ResponseEntity<BrokerPositionSnapshotImportResponse> runBrokerPositionSnapshotIngest(
            String source, String filename, byte[] bytes) {
        try {
            BrokerPositionSnapshotIngestService.Result r =
                    brokerPositionSnapshotIngestService.ingest(source, filename, bytes);
            return ResponseEntity.ok(new BrokerPositionSnapshotImportResponse(
                    r.duplicate(),
                    r.batchId(),
                    r.status(),
                    r.rowCount(),
                    r.errorSummary(),
                    r.insertedRows(),
                    r.skippedDuplicateRows()));
        } catch (IllegalArgumentException ex) {
            if ("file too large".equals(ex.getMessage())) {
                return ResponseEntity.status(HttpStatus.PAYLOAD_TOO_LARGE).build();
            }
            return ResponseEntity.badRequest().build();
        }
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
            SettlementFileIngestRouter.RoutedResult r =
                    settlementFileIngestRouter.ingest(source, file.getOriginalFilename(), bytes);
            if ("v2-economic".equals(r.format()) || !"v0-fixture".equals(r.format())) {
                return ResponseEntity.ok(
                        new SettlementFileImportResponse(
                                r.duplicate(),
                                r.batchId(),
                                r.status(),
                                r.rowCount(),
                                r.errorSummary(),
                                0,
                                0,
                                0));
            }
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

    /**
     * Operator-led upsert of {@code instrument_settlement_profile} (gap plan §5.3 Slice 2b-4).
     * JSON body shape: {@code {"rows":[{ "instrumentId":..., "symbol":..., "primaryMic":...,
     * "settlementCalendarId":..., "settlementCycle":"T+1", "settlementCurrency":"USD",
     * "iskEligible":false, "effectiveFrom":"2024-05-28", "effectiveTo":null }, ...]}}.
     * Upsert is keyed on the V61 UNIQUE {@code (instrument_id, effective_from)}. Per-row
     * rejection reasons are returned for invalid rows; the call still 200s so the operator
     * gets full visibility. Hard failures (CHECK constraint, JSON parse, oversized batch)
     * roll back the whole transaction and return 400 — partial application would silently
     * mis-route the calculator and we explicitly do not want that.
     */
    @PostMapping("/instrument-profiles/import-json")
    public ResponseEntity<SettlementProfileIngestResponse> importInstrumentProfiles(
            @RequestBody(required = false) byte[] body) {
        try {
            var r = instrumentSettlementProfileIngestService.ingest(body);
            return ResponseEntity.ok(new SettlementProfileIngestResponse(r.inserted(), r.updated(), r.rejected()));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().build();
        }
    }

    /**
     * Operator-led upsert of {@code settlement_calendar} (gap plan §5.3 Slice 2b-4).
     * JSON body shape: {@code {"rows":[{ "calendarId":"XSTO-CAL", "holidayDate":"2027-01-01",
     * "description":"New Year's Day" }, ...]}}. Upsert is keyed on the V63 primary key
     * {@code (calendar_id, holiday_date)}. Same partial-application stance as the profile
     * ingest above.
     */
    @PostMapping("/settlement-calendars/import-json")
    public ResponseEntity<SettlementCalendarIngestResponse> importSettlementCalendars(
            @RequestBody(required = false) byte[] body) {
        try {
            var r = settlementCalendarIngestService.ingest(body);
            return ResponseEntity.ok(new SettlementCalendarIngestResponse(r.inserted(), r.updated(), r.rejected()));
        } catch (IllegalArgumentException e) {
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
