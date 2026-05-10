package com.balh.oms.fix;

import com.balh.oms.config.OmsConfig;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import quickfix.Session;
import quickfix.SessionID;
import quickfix.SessionNotFound;
import quickfix.field.ClOrdID;
import quickfix.field.MassCancelRequestType;
import quickfix.field.TransactTime;
import quickfix.fix44.OrderMassCancelRequest;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;

/**
 * Ops-triggered manual mass cancel (Slice U7+): default is <strong>signal-only</strong> (metric + audit log).
 * Optional wire to venue via QuickFIX {@link OrderMassCancelRequest} when {@code oms.fix.manual-mass-cancel-wire-enabled}
 * and a logged-on session exist — still requires broker contract for semantics (see {@code docs/fix-out.md}).
 */
@Service
@ConditionalOnProperty(name = "oms.routing.backend", havingValue = "fix")
public class FixManualMassCancelService {

    private static final Logger log = LoggerFactory.getLogger(FixManualMassCancelService.class);

    private final OmsConfig omsConfig;
    private final FixSessionRegistry fixSessionRegistry;
    private final MeterRegistry meterRegistry;

    public FixManualMassCancelService(OmsConfig omsConfig, FixSessionRegistry fixSessionRegistry, MeterRegistry meterRegistry) {
        this.omsConfig = omsConfig;
        this.fixSessionRegistry = fixSessionRegistry;
        this.meterRegistry = meterRegistry;
    }

    public record Outcome(String mode, String message, Optional<String> massCancelClOrdId) {}

    /**
     * @param requestedBy non-blank actor (from Ops Console JWT email or internal caller)
     * @param reason optional ops note (bounded by config)
     * @param requestWire when {@code true}, attempt FIX wire if policy + session allow
     */
    public Outcome execute(String requestedBy, String reason, boolean requestWire) {
        var fix = omsConfig.getFix();
        if (!fix.isManualMassCancelEnabled()) {
            throw new IllegalStateException("manual_mass_cancel_disabled");
        }
        String actor = requestedBy == null || requestedBy.isBlank() ? "unknown" : requestedBy.trim();
        String note = reason == null ? "" : reason.trim();
        int maxReason = fix.getManualMassCancelReasonMaxChars();
        if (note.length() > maxReason) {
            throw new IllegalArgumentException("reason_too_long");
        }

        meterRegistry
                .counter("oms_fix_manual_mass_cancel_requests_total", "outcome", "accepted")
                .increment();

        boolean wantWire = requestWire && fix.isManualMassCancelWireEnabled();
        if (!wantWire) {
            log.warn(
                    "manual mass cancel signal-only requestedBy={} reasonLen={} (wire disabled or not requested)",
                    actor,
                    note.length());
            return new Outcome("signal_only", "Recorded signal-only mass cancel intent; venue wire disabled by policy.", Optional.empty());
        }

        SessionID sessionId = fixSessionRegistry.sessionOrNull();
        if (sessionId == null) {
            log.warn("manual mass cancel wire requested but no logged-on FIX session — signal only requestedBy={}", actor);
            meterRegistry
                    .counter("oms_fix_manual_mass_cancel_requests_total", "outcome", "wire_skipped_no_session")
                    .increment();
            return new Outcome(
                    "signal_only",
                    "Wire requested but no active FIX session; recorded intent only.",
                    Optional.empty());
        }

        String clOrdId = UUID.randomUUID().toString();
        OrderMassCancelRequest msg = new OrderMassCancelRequest();
        msg.set(new ClOrdID(clOrdId));
        msg.set(new MassCancelRequestType(MassCancelRequestType.CANCEL_ALL_ORDERS));
        msg.set(new TransactTime(LocalDateTime.now(ZoneOffset.UTC)));
        try {
            Session.sendToTarget(msg, sessionId);
            meterRegistry
                    .counter("oms_fix_manual_mass_cancel_requests_total", "outcome", "wired")
                    .increment();
            log.warn(
                    "manual mass cancel OrderMassCancelRequest sent requestedBy={} clOrdId={} (broker semantics are counterparty-specific)",
                    actor,
                    clOrdId);
            return new Outcome("wired", "OrderMassCancelRequest sent on active FIX session.", Optional.of(clOrdId));
        } catch (SessionNotFound e) {
            meterRegistry
                    .counter("oms_fix_manual_mass_cancel_requests_total", "outcome", "wire_failed_no_session")
                    .increment();
            log.error("manual mass cancel send failed requestedBy={}", actor, e);
            return new Outcome(
                    "signal_only",
                    "Failed to send mass cancel (session not found); intent logged.",
                    Optional.empty());
        }
    }
}
