package com.balh.oms.corporateaction;

import com.balh.oms.config.OmsConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Processes {@code corporate_action_event} rows: entitlements + position/cash impacts (gap plan §5.9).
 */
@Component
@ConditionalOnProperty(name = "oms.corporate-action.processor-enabled", havingValue = "true")
public class CorporateActionProcessorJob {

    private static final Logger log = LoggerFactory.getLogger(CorporateActionProcessorJob.class);
    private static final String METRIC_PROCESSED = "oms_corporate_action_events_processed_total";
    private static final String METRIC_FAILED = "oms_corporate_action_events_processing_failed_total";

    private final CorporateActionEventRepository repository;
    private final CorporateActionProcessingService processingService;
    private final OmsConfig omsConfig;
    private final MeterRegistry meterRegistry;
    private final TransactionTemplate transactionTemplate;
    private final ObjectMapper objectMapper;

    public CorporateActionProcessorJob(
            CorporateActionEventRepository repository,
            CorporateActionProcessingService processingService,
            OmsConfig omsConfig,
            MeterRegistry meterRegistry,
            PlatformTransactionManager transactionManager,
            ObjectMapper objectMapper) {
        this.repository = repository;
        this.processingService = processingService;
        this.omsConfig = omsConfig;
        this.meterRegistry = meterRegistry;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.objectMapper = objectMapper;
    }

    @Scheduled(fixedDelayString = "${oms.corporate-action.processor-interval-ms:60000}")
    public void processBatch() {
        int batch = omsConfig.getCorporateAction().getProcessorBatchSize();
        transactionTemplate.executeWithoutResult(status -> {
            var rows = repository.findUnprocessedForProcessing(batch);
            for (CorporateActionEventRepository.ProcessingRow row : rows) {
                try {
                    processingService.process(row);
                    int n = repository.markProcessedIfPending(row.id());
                    if (n == 1) {
                        meterRegistry.counter(METRIC_PROCESSED).increment();
                        log.info(
                                "corporate_action_event processed id={} symbol={} type={}",
                                row.id(),
                                row.instrumentSymbol(),
                                row.actionType());
                    }
                } catch (CorporateActionProcessingService.UnsupportedCorporateActionException e) {
                    repository.markProcessingError(row.id(), e.getMessage());
                    meterRegistry.counter(METRIC_FAILED).increment();
                    log.warn(
                            "corporate_action_event processing failed id={} symbol={} type={}: {}",
                            row.id(),
                            row.instrumentSymbol(),
                            row.actionType(),
                            e.getMessage());
                }
            }
        });
    }

    public static BrokerDates parseBrokerDates(String rawRowJson, ObjectMapper objectMapper) {
        try {
            JsonNode node = objectMapper.readTree(rawRowJson == null ? "{}" : rawRowJson);
            java.time.LocalDate record =
                    node.has("recordDate") && !node.get("recordDate").isNull()
                            ? java.time.LocalDate.parse(node.get("recordDate").asText())
                            : null;
            java.time.LocalDate payable =
                    node.has("payableDate") && !node.get("payableDate").isNull()
                            ? java.time.LocalDate.parse(node.get("payableDate").asText())
                            : null;
            return new BrokerDates(record, payable);
        } catch (Exception e) {
            return new BrokerDates(null, null);
        }
    }

    public record BrokerDates(java.time.LocalDate recordDate, java.time.LocalDate payableDate) {}
}
