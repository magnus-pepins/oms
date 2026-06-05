package com.balh.oms.predictionmarket;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.config.OmsProfiles;
import com.balh.oms.venue.VenueContractRegistryClient;
import java.util.List;
import java.util.concurrent.locks.LockSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Pushes all OPEN prediction-market catalog rows into the venue in-memory registry when an
 * order-accept or venue-egress JVM becomes ready. After a venue-cluster restart the registry is
 * empty until this runs (or an operator runs {@code sync-pop-prediction-market-venue.sh}).
 */
@Component
@Profile(OmsProfiles.VENUE_REGISTRY_STARTUP_SYNC_PROFILE)
public class PredictionMarketVenueRegistryStartupSync {

    private static final Logger log = LoggerFactory.getLogger(PredictionMarketVenueRegistryStartupSync.class);

    private final OmsConfig omsConfig;
    private final PredictionMarketContractRepository repository;
    private final VenueContractRegistryClient venueRegistry;

    public PredictionMarketVenueRegistryStartupSync(
            OmsConfig omsConfig,
            PredictionMarketContractRepository repository,
            VenueContractRegistryClient venueRegistry) {
        this.omsConfig = omsConfig;
        this.repository = repository;
        this.venueRegistry = venueRegistry;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        if (!omsConfig.getVenue().isRegistrySyncEnabled()
                || !omsConfig.getVenue().isRegistrySyncOnStartupEnabled()) {
            return;
        }
        Thread.startVirtualThread(this::syncOpenContractsWithRetry);
    }

    void syncOpenContractsWithRetry() {
        OmsConfig.Venue venue = omsConfig.getVenue();
        long initialDelayMs = venue.getRegistrySyncOnStartupInitialDelayMs();
        if (initialDelayMs > 0) {
            LockSupport.parkNanos(initialDelayMs * 1_000_000L);
        }

        int maxAttempts = venue.getRegistrySyncOnStartupMaxAttempts();
        long retryBackoffMs = venue.getRegistrySyncOnStartupRetryBackoffMs();

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            SyncBatchResult result = syncOpenContractsOnce();
            if (result.failures() == 0) {
                if (result.synced() == 0) {
                    log.info("prediction-market venue registry startup sync: no OPEN contracts");
                } else {
                    log.info(
                            "prediction-market venue registry startup sync complete: synced={} attempt={}/{}",
                            result.synced(),
                            attempt,
                            maxAttempts);
                }
                return;
            }
            if (attempt < maxAttempts) {
                log.warn(
                        "prediction-market venue registry startup sync: {} failure(s) on attempt {}/{}; retrying",
                        result.failures(),
                        attempt,
                        maxAttempts);
                LockSupport.parkNanos(retryBackoffMs * 1_000_000L);
            } else {
                log.error(
                        "prediction-market venue registry startup sync exhausted {} attempts;"
                                + " synced={} failures={} — run sync-pop-prediction-market-venue.sh",
                        maxAttempts,
                        result.synced(),
                        result.failures());
            }
        }
    }

    SyncBatchResult syncOpenContractsOnce() {
        List<PredictionMarketContractRepository.ContractRow> open = repository.listOpen();
        int synced = 0;
        int failures = 0;
        for (PredictionMarketContractRepository.ContractRow row : open) {
            try {
                venueRegistry.syncContract(row);
                synced++;
            } catch (RuntimeException e) {
                failures++;
                log.warn(
                        "prediction-market venue registry startup sync failed slug={} id={}",
                        row.slug(),
                        row.id(),
                        e);
            }
        }
        return new SyncBatchResult(synced, failures);
    }

    record SyncBatchResult(int synced, int failures) {}
}
