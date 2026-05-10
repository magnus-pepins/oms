package com.balh.oms.risk;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.domain.RejectCode;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Execution-time sanctions / PEP re-check hook (master plan 5.6). Default permissive implementation
 * records last-pass timestamps keyed by account; {@link OmsConfig.Risk#isSanctionsRecheckStrict()} forces
 * {@link RejectCode#RISK_COMPLIANCE_SANCTIONS} until a real screening client is wired.
 */
@Component
public class SanctionsExecutionGate {

    private final OmsConfig omsConfig;
    private final Clock clock;
    private final Map<UUID, Instant> lastPass = new ConcurrentHashMap<>();

    public SanctionsExecutionGate(OmsConfig omsConfig) {
        this(omsConfig, Clock.systemUTC());
    }

    SanctionsExecutionGate(OmsConfig omsConfig, Clock clock) {
        this.omsConfig = omsConfig;
        this.clock = clock;
    }

    /**
     * @return empty when screening passes; {@link RejectCode#RISK_COMPLIANCE_SANCTIONS} when strict mode or stale
     *     cache policy rejects (v1 stub uses only strict flag).
     */
    public Optional<RejectCode> evaluate(UUID accountId) {
        var risk = omsConfig.getRisk();
        if (!risk.isSanctionsRecheckEnabled()) {
            return Optional.empty();
        }
        if (accountId == null) {
            return Optional.empty();
        }
        if (risk.isSanctionsRecheckStrict()) {
            return Optional.of(RejectCode.RISK_COMPLIANCE_SANCTIONS);
        }
        Instant now = clock.instant();
        Instant prior = lastPass.get(accountId);
        long maxAgeSec = risk.getSanctionsCacheMaxAgeSeconds();
        if (prior != null && maxAgeSec > 0 && prior.plusSeconds(maxAgeSec).isAfter(now)) {
            return Optional.empty();
        }
        lastPass.put(accountId, now);
        return Optional.empty();
    }
}
