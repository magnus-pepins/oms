package com.balh.oms.corporateaction;

import com.balh.oms.config.OmsConfig;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Marks {@code corporate_action_event} rows processed (stub processor — finance replaces with real apply later).
 * Each wake runs in one DB transaction: rows are claimed with {@code FOR UPDATE SKIP LOCKED} so multiple OMS JVMs
 * do not process the same inbox row concurrently.
 */
@Component
@ConditionalOnProperty(name = "oms.corporate-action.processor-enabled", havingValue = "true")
public class CorporateActionProcessorJob {

    private static final Logger log = LoggerFactory.getLogger(CorporateActionProcessorJob.class);
    private static final String METRIC_CORPORATE_ACTION_PROCESSED = "oms_corporate_action_events_processed_total";

    private final CorporateActionEventRepository repository;
    private final OmsConfig omsConfig;
    private final MeterRegistry meterRegistry;
    private final TransactionTemplate transactionTemplate;

    public CorporateActionProcessorJob(
            CorporateActionEventRepository repository,
            OmsConfig omsConfig,
            MeterRegistry meterRegistry,
            PlatformTransactionManager transactionManager) {
        this.repository = repository;
        this.omsConfig = omsConfig;
        this.meterRegistry = meterRegistry;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    @Scheduled(fixedDelayString = "${oms.corporate-action.processor-interval-ms:60000}")
    public void processBatch() {
        int batch = omsConfig.getCorporateAction().getProcessorBatchSize();
        transactionTemplate.executeWithoutResult(status -> {
            var rows = repository.findUnprocessedForUpdateSkipLocked(batch);
            for (CorporateActionEventRepository.UnprocessedRow row : rows) {
                int n = repository.markProcessedIfPending(row.id());
                if (n == 1) {
                    meterRegistry.counter(METRIC_CORPORATE_ACTION_PROCESSED).increment();
                    log.info(
                            "corporate_action_event marked processed id={} symbol={} type={} effective={}",
                            row.id(),
                            row.instrumentSymbol(),
                            row.actionType(),
                            row.effectiveDate());
                }
            }
        });
    }
}
