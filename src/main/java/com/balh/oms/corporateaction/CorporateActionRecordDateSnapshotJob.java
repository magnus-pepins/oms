package com.balh.oms.corporateaction;

import com.balh.oms.config.OmsConfig;
import java.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** Daily capture of record-date position snapshots for pending corporate actions (gap plan §5.9). */
@Component
@ConditionalOnProperty(name = "oms.corporate-action.record-date-snapshot-job-enabled", havingValue = "true")
public class CorporateActionRecordDateSnapshotJob {

    private static final Logger log = LoggerFactory.getLogger(CorporateActionRecordDateSnapshotJob.class);

    private final CorporateActionRecordDateSnapshotRepository snapshots;
    private final CorporateActionRecordDateSnapshotService snapshotService;
    private final OmsConfig omsConfig;

    public CorporateActionRecordDateSnapshotJob(
            CorporateActionRecordDateSnapshotRepository snapshots,
            CorporateActionRecordDateSnapshotService snapshotService,
            OmsConfig omsConfig) {
        this.snapshots = snapshots;
        this.snapshotService = snapshotService;
        this.omsConfig = omsConfig;
    }

    @Scheduled(cron = "${oms.corporate-action.record-date-snapshot-cron:0 30 23 * * *}")
    public void captureDueToday() {
        LocalDate today = LocalDate.now();
        int batch = omsConfig.getCorporateAction().getRecordDateSnapshotBatchSize();
        var events = snapshots.listEventsNeedingCapture(today, batch);
        for (CorporateActionRecordDateSnapshotRepository.EventCaptureRow event : events) {
            snapshotService.captureForEvent(event.eventId(), event.instrumentSymbol(), event.recordDate());
        }
        if (!events.isEmpty()) {
            log.info("corporate_action record_date snapshot job captured {} event(s) for {}", events.size(), today);
        }
    }
}
