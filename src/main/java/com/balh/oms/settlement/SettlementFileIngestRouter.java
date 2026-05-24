package com.balh.oms.settlement;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;

/**
 * Routes broker JSON files to the v0 fixture importer or the v2 economic confirm importer
 * (gap plan Phase A tail — drop-folder v2 envelope routing).
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
            int skippedUnresolved,
            int insertedEconomicRows,
            int insertedEconomicFees,
            int skippedDuplicateEconomicRows) {

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
                    r.skippedUnresolved(),
                    0,
                    0,
                    0);
        }

        static RoutedResult fromEconomic(BrokerTradeConfirmIngestService.Result r) {
            return new RoutedResult(
                    "v2-economic",
                    r.duplicate(),
                    r.batchId(),
                    r.status(),
                    r.rowCount(),
                    r.errorSummary(),
                    0,
                    0,
                    0,
                    r.insertedRows(),
                    r.insertedFees(),
                    r.skippedDuplicateRows());
        }
    }

    private final ObjectMapper objectMapper;
    private final SettlementFileImportService fixtureImport;
    private final BrokerTradeConfirmIngestService economicImport;

    public SettlementFileIngestRouter(
            ObjectMapper objectMapper,
            SettlementFileImportService fixtureImport,
            BrokerTradeConfirmIngestService economicImport) {
        this.objectMapper = objectMapper;
        this.fixtureImport = fixtureImport;
        this.economicImport = economicImport;
    }

    public RoutedResult ingest(String source, String originalFilename, byte[] fileBytes) {
        SettlementBrokerFileFormat.Kind kind = SettlementBrokerFileFormat.detect(objectMapper, fileBytes);
        return switch (kind) {
            case V2_ECONOMIC -> RoutedResult.fromEconomic(economicImport.ingest(source, originalFilename, fileBytes));
            case V0_FIXTURE -> RoutedResult.fromFixture(fixtureImport.ingestMultipart(source, originalFilename, fileBytes));
            case INVALID -> throw new IllegalArgumentException("unrecognized broker file envelope");
        };
    }
}
