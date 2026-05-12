package com.balh.oms.fix;

import com.balh.oms.config.OmsConfig;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import quickfix.SessionNotFound;
import quickfix.field.ClOrdID;
import quickfix.field.MassCancelRequestType;
import quickfix.field.TransactTime;
import quickfix.fix44.OrderMassCancelRequest;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * Ops-triggered manual mass cancel (Slice U7+): default is <strong>signal-only</strong> (metric + audit log).
 * Optional wire to venue via QuickFIX {@link OrderMassCancelRequest} when {@code oms.fix.manual-mass-cancel-wire-enabled}
 * and a logged-on session exist — still requires broker contract for semantics (see {@code docs/fix-out.md}).
 *
 * <p>When {@link FixOutboundDispatchWorker} is active ({@code oms.fix.auto-start=true}), wire sends are serialized through
 * the same outbound queue as NOS; otherwise the message is sent directly from this thread.
 */
@Service
@ConditionalOnProperty(name = "oms.routing.backend", havingValue = "fix")
public class FixManualMassCancelService {

    private static final Logger log = LoggerFactory.getLogger(FixManualMassCancelService.class);

    private static final String QUEUE_FULL = "fix_outbound_queue_full";

    private final OmsConfig omsConfig;
    private final FixOutboundSessionSend fixOutboundSessionSend;
    private final FixRouteDispatcher fixRouteDispatcher;
    private final ObjectProvider<FixOutboundDispatchWorker> fixOutboundDispatchWorker;
    private final MeterRegistry meterRegistry;

    public FixManualMassCancelService(
            OmsConfig omsConfig,
            FixOutboundSessionSend fixOutboundSessionSend,
            FixRouteDispatcher fixRouteDispatcher,
            ObjectProvider<FixOutboundDispatchWorker> fixOutboundDispatchWorker,
            MeterRegistry meterRegistry) {
        this.omsConfig = omsConfig;
        this.fixOutboundSessionSend = fixOutboundSessionSend;
        this.fixRouteDispatcher = fixRouteDispatcher;
        this.fixOutboundDispatchWorker = fixOutboundDispatchWorker;
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

        if (!fixOutboundSessionSend.hasActiveSession()) {
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
            if (fixOutboundDispatchWorker.getIfAvailable() != null) {
                fixRouteDispatcher.enqueueMassCancelAndAwait(msg, fix.getManualMassCancelWireQueueWaitMs());
            } else {
                fixOutboundSessionSend.send(msg);
            }
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
        } catch (IllegalStateException e) {
            if (QUEUE_FULL.equals(e.getMessage())) {
                meterRegistry
                        .counter("oms_fix_manual_mass_cancel_requests_total", "outcome", "wire_failed_queue_full")
                        .increment();
                log.error("manual mass cancel queue full requestedBy={}", actor);
                return new Outcome(
                        "signal_only",
                        "Outbound FIX queue full; intent logged.",
                        Optional.empty());
            }
            throw e;
        } catch (TimeoutException e) {
            meterRegistry
                    .counter("oms_fix_manual_mass_cancel_requests_total", "outcome", "wire_failed_timeout")
                    .increment();
            log.error("manual mass cancel wire timed out requestedBy={}", actor, e);
            return new Outcome("signal_only", "Mass cancel wire timed out waiting for outbound worker.", Optional.empty());
        } catch (ExecutionException e) {
            Throwable c = e.getCause();
            if (c instanceof IllegalStateException ise
                    && FixMetrics.FIX_ROUTE_SEND_DISABLED_MESSAGE.equals(ise.getMessage())) {
                meterRegistry
                        .counter("oms_fix_manual_mass_cancel_requests_total", "outcome", "wire_failed_route_disabled")
                        .increment();
                log.warn("manual mass cancel rejected: FIX route send disabled requestedBy={}", actor);
                return new Outcome(
                        "signal_only",
                        "FIX route send disabled; mass cancel not sent.",
                        Optional.empty());
            }
            if (c instanceof SessionNotFound) {
                meterRegistry
                        .counter("oms_fix_manual_mass_cancel_requests_total", "outcome", "wire_failed_no_session")
                        .increment();
                log.error("manual mass cancel send failed requestedBy={}", actor, c);
                return new Outcome(
                        "signal_only",
                        "Failed to send mass cancel (session not found); intent logged.",
                        Optional.empty());
            }
            meterRegistry
                    .counter("oms_fix_manual_mass_cancel_requests_total", "outcome", "wire_failed")
                    .increment();
            log.error("manual mass cancel send failed requestedBy={}", actor, e);
            return new Outcome("signal_only", "Failed to send mass cancel; intent logged.", Optional.empty());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            meterRegistry
                    .counter("oms_fix_manual_mass_cancel_requests_total", "outcome", "wire_failed_interrupted")
                    .increment();
            log.error("manual mass cancel interrupted requestedBy={}", actor, e);
            return new Outcome("signal_only", "Mass cancel wire interrupted.", Optional.empty());
        }
    }
}
