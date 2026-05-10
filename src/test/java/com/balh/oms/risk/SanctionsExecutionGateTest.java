package com.balh.oms.risk;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.domain.RejectCode;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class SanctionsExecutionGateTest {

    @Test
    void strictMode_rejects() {
        OmsConfig cfg = new OmsConfig();
        cfg.getRisk().setSanctionsRecheckEnabled(true);
        cfg.getRisk().setSanctionsRecheckStrict(true);
        var gate = new SanctionsExecutionGate(cfg, Clock.fixed(Instant.parse("2026-05-08T12:00:00Z"), ZoneOffset.UTC));
        assertThat(gate.evaluate(UUID.randomUUID())).contains(RejectCode.RISK_COMPLIANCE_SANCTIONS);
    }

    @Test
    void permissiveMode_passesWithinCacheWindow() {
        OmsConfig cfg = new OmsConfig();
        cfg.getRisk().setSanctionsRecheckEnabled(true);
        cfg.getRisk().setSanctionsRecheckStrict(false);
        cfg.getRisk().setSanctionsCacheMaxAgeSeconds(3600);
        UUID account = UUID.randomUUID();
        var clock = Clock.fixed(Instant.parse("2026-05-08T12:00:00Z"), ZoneOffset.UTC);
        var gate = new SanctionsExecutionGate(cfg, clock);
        assertThat(gate.evaluate(account)).isEmpty();
        assertThat(gate.evaluate(account)).isEmpty();
    }
}
