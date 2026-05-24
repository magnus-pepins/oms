package com.balh.oms.settlement;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;

/**
 * Routes broker JSON files to the appropriate ingest service (gap plan Phase C daily-close /
 * drop-folder multi-envelope routing).
 */
@Service
public class SettlementFileIngestRouter {

    public record RoutedResult(
            String format,
            boolean duplicate,
            long batchId,
            String status,
            Integer rowCount,
            String errorSummary,
            int insertedConfirms,
            int skippedInvalid,
            int skippedUnresolved) {

        static RoutedResult fromFixture(SettlementFileImportService.Result r) {
            return new RoutedResult(
                    "v0-fixture",
                    r.duplicate(),
                    r.batchId(),
                    r.status(),
                    r.rowCount(),
                    r.errorSummary(),
                    r.insertedConfirms(),
                    r.skippedInvalid(),
                    r.skippedUnresolved());
        }

        static RoutedResult fromEconomic(BrokerTradeConfirmIngestService.Result r) {
            return base("v2-economic", r.duplicate(), r.batchId(), r.status(), r.rowCount(), r.errorSummary());
        }

        static RoutedResult fromPosition(BrokerPositionSnapshotIngestService.Result r) {
            return base("position-snapshot", r.duplicate(), r.batchId(), r.status(), r.rowCount(), r.errorSummary());
        }

        static RoutedResult fromCash(BrokerCashStatementIngestService.Result r) {
            return base("cash-statement", r.duplicate(), r.batchId(), r.status(), r.movementCount(), r.errorSummary());
        }

        static RoutedResult fromCorporateAction(BrokerCorporateActionIngestService.Result r) {
            return base("corporate-action", r.duplicate(), r.batchId(), r.status(), r.eventCount(), r.errorSummary());
        }

        static RoutedResult fromSettlementFail(BrokerSettlementFailIngestService.Result r) {
            return base("settlement-fail", r.duplicate(), r.batchId(), r.status(), r.failCount(), r.errorSummary());
        }

        private static RoutedResult base(
                String format,
                boolean duplicate,
                long batchId,
                String status,
                Integer rowCount,
                String errorSummary) {
            return new RoutedResult(format, duplicate, batchId, status, rowCount, errorSummary, 0, 0, 0);
        }
    }

    private final ObjectMapper objectMapper;
    private final SettlementFileImportService fixtureImport;
    private final BrokerTradeConfirmIngestService economicImport;
    private final BrokerPositionSnapshotIngestService positionImport;
    private final BrokerCashStatementIngestService cashImport;
    private final BrokerCorporateActionIngestService corporateActionImport;
    private final BrokerSettlementFailIngestService settlementFailImport;

    public SettlementFileIngestRouter(
            ObjectMapper objectMapper,
            SettlementFileImportService fixtureImport,
            BrokerTradeConfirmIngestService economicImport,
            BrokerPositionSnapshotIngestService positionImport,
            BrokerCashStatementIngestService cashImport,
            BrokerCorporateActionIngestService corporateActionImport,
            BrokerSettlementFailIngestService settlementFailImport) {
        this.objectMapper = objectMapper;
        this.fixtureImport = fixtureImport;
        this.economicImport = economicImport;
        this.positionImport = positionImport;
        this.cashImport = cashImport;
        this.corporateActionImport = corporateActionImport;
        this.settlementFailImport = settlementFailImport;
    }

    public RoutedResult ingest(String source, String originalFilename, byte[] fileBytes) {
        SettlementBrokerFileFormat.Kind kind = SettlementBrokerFileFormat.detect(objectMapper, fileBytes);
        return switch (kind) {
            case V2_ECONOMIC -> RoutedResult.fromEconomic(economicImport.ingest(source, originalFilename, fileBytes));
            case V0_FIXTURE -> RoutedResult.fromFixture(fixtureImport.ingestMultipart(source, originalFilename, fileBytes));
            case POSITION_SNAPSHOT -> RoutedResult.fromPosition(positionImport.ingest(source, originalFilename, fileBytes));
            case CASH_STATEMENT -> RoutedResult.fromCash(cashImport.ingest(source, originalFilename, fileBytes));
            case CORPORATE_ACTION -> RoutedResult.fromCorporateAction(
                    corporateActionImport.ingest(source, originalFilename, fileBytes));
            case SETTLEMENT_FAIL -> RoutedResult.fromSettlementFail(
                    settlementFailImport.ingest(source, originalFilename, fileBytes));
            case INVALID -> throw new IllegalArgumentException("unrecognized broker file envelope");
        };
    }
}
