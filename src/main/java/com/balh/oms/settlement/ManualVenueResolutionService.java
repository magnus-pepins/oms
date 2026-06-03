package com.balh.oms.settlement;

import com.balh.oms.cluster.ApplyVenueResolutionCommand;
import com.balh.oms.cluster.OmsClusterIngressClient;
import com.balh.oms.cluster.OmsClusterWireFormat;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.predictionmarket.PredictionMarketContractRepository;
import java.time.Duration;
import java.util.Locale;
import java.util.UUID;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Service;

/** Operator-initiated binary resolution (Beard Admin) when venue oracle is absent or wrong. */
@Service
@ConditionalOnBean(OmsClusterIngressClient.class)
public class ManualVenueResolutionService {

    private final OmsClusterIngressClient clusterIngressClient;
    private final PredictionMarketContractRepository contractRepository;
    private final OmsConfig config;

    public ManualVenueResolutionService(
            OmsClusterIngressClient clusterIngressClient,
            PredictionMarketContractRepository contractRepository,
            OmsConfig config) {
        this.clusterIngressClient = clusterIngressClient;
        this.contractRepository = contractRepository;
        this.config = config;
    }

    public record ManualResolutionRequest(
            String yesSymbol, String outcome, String evidenceHash, String resolutionSource) {}

    public record ManualResolutionResponse(
            String yesSymbol, byte outcomeCode, String evidenceHash, String resolutionSource) {}

    public ManualResolutionResponse submit(ManualResolutionRequest req)
            throws InterruptedException, java.util.concurrent.TimeoutException {
        String yesSymbol = requireYesSymbol(req.yesSymbol());
        if (contractRepository.findByYesSymbol(yesSymbol).isEmpty()) {
            throw new IllegalArgumentException("unknown_contract");
        }
        byte outcomeCode = parseOutcome(req.outcome());
        String evidenceHash = normalizeEvidenceHash(req.evidenceHash());
        String source =
                req.resolutionSource() == null || req.resolutionSource().isBlank()
                        ? "beard-admin-manual"
                        : req.resolutionSource().trim();

        ApplyVenueResolutionCommand cmd =
                new ApplyVenueResolutionCommand(
                        System.nanoTime(),
                        yesSymbol,
                        outcomeCode,
                        source,
                        System.currentTimeMillis(),
                        evidenceHash,
                        config.getVenue().getVenueId());
        clusterIngressClient.submitApplyVenueResolution(
                cmd, Duration.ofMillis(config.getCluster().getVenueResolver().getOfferTimeoutMs()));
        return new ManualResolutionResponse(yesSymbol, outcomeCode, evidenceHash, source);
    }

    private static String requireYesSymbol(String raw) {
        if (raw == null || raw.isBlank()) {
            throw new IllegalArgumentException("yesSymbol_required");
        }
        return raw.trim().toUpperCase(Locale.ROOT);
    }

    private static byte parseOutcome(String raw) {
        if (raw == null || raw.isBlank()) {
            throw new IllegalArgumentException("outcome_required");
        }
        return switch (raw.trim().toUpperCase(Locale.ROOT)) {
            case "YES" -> OmsClusterWireFormat.OUTCOME_YES;
            case "NO" -> OmsClusterWireFormat.OUTCOME_NO;
            default -> throw new IllegalArgumentException("invalid_outcome");
        };
    }

    private static String normalizeEvidenceHash(String raw) {
        if (raw != null && !raw.isBlank()) {
            return raw.trim();
        }
        return "manual-" + UUID.randomUUID();
    }
}
