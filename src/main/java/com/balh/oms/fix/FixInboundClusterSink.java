package com.balh.oms.fix;

import com.balh.oms.cluster.ApplyExecutionReportCommand;
import com.balh.oms.cluster.OmsClusterIngressClient;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.returnpath.ExecutionCancelCommand;
import com.balh.oms.returnpath.ExecutionTradeCommand;
import com.balh.oms.returnpath.ExecutionVenueRejectCommand;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import quickfix.FieldNotFound;
import quickfix.Message;
import quickfix.field.CxlRejResponseTo;
import quickfix.field.MsgSeqNum;
import quickfix.field.SenderCompID;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/**
 * Inbound FIX {@code ExecutionReport} (35=8) and {@code OrderCancelReject} (35=9) translator:
 * builds {@link ApplyExecutionReportCommand}s and fire-and-forget submits them via
 * {@link OmsClusterIngressClient#submitApplyExecutionReport}.
 *
 * <p>Introduced in Phase 3 slice 3d of the Aeron Cluster substrate plan; Phase 3 slice 3g
 * deleted the legacy {@code FixInboundHandler} (Postgres-direct applier on
 * {@code oms-fix-worker}) so this is now the only inbound sink on every FIX-routing JVM
 * (monolith and {@code oms-fix-egress}).
 *
 * <p>The cluster service ({@link com.balh.oms.cluster.OmsAdmissionClusteredService}) walks the
 * order state machine and emits {@link com.balh.oms.cluster.ExecutionAppliedEvent} on the side
 * publication; the projector (slice 3e / 3e-2) writes the {@code executions} / {@code orders} /
 * {@code domain_event_outbox} rows from there. No Postgres I/O happens on this path.
 *
 * <h2>Idempotency</h2>
 *
 * <p>Two layers of dedupe protect the cluster from duplicate ERs:
 *
 * <ul>
 *   <li><strong>Wire-level {@code (senderCompId, msgSeqNum)}</strong> — set on every command
 *       built here. The cluster service rejects duplicate {@code (senderCompId, msgSeqNum)}
 *       inside its applier (matches the QuickFIX session's ResendRequest semantics: a broker that
 *       resends the same {@code MsgSeqNum} after a session gap should not re-fill an order).</li>
 *   <li><strong>FIX-level {@code (orderId, venueExecRef)}</strong> — natural FIX dedupe key. The
 *       cluster service maintains the index even when wire-level dedupe is satisfied (e.g. when
 *       two distinct sessions deliver the same {@code ExecID}).</li>
 * </ul>
 *
 * <p>If the inbound message cannot be mapped (no {@code ClOrdID}, no {@code orderId} parseable as
 * UUID, or an {@code ExecType} we do not project) we increment the
 * {@code FixMetrics.METRIC_INBOUND_ER} counter with disposition {@code "ignored"} and return.
 * We do not throw, because QuickFIX would treat that as a session-level reject and broker-side
 * replay quirks should not tear down the FIX session.
 *
 * <h2>Determinism boundary</h2>
 *
 * <p>This bean lives at the cluster <em>edge</em>: it is allowed to call {@code Instant.now()}
 * for {@code venueTs} fallbacks (when the FIX message lacks {@code TransactTime}) and to log on
 * the hot path. The values are then carried into the cluster as fields of
 * {@link ApplyExecutionReportCommand}; the cluster service itself observes only the encoded
 * fields, which is what makes replay deterministic across leader / follower / cold start.
 */
@Service
@ConditionalOnProperty(name = "oms.routing.backend", havingValue = "fix")
public class FixInboundClusterSink {

    private static final Logger log = LoggerFactory.getLogger(FixInboundClusterSink.class);

    private static final BigDecimal QUANTITY_SCALE_BD =
            BigDecimal.valueOf(com.balh.oms.cluster.AcceptOrderCommand.QUANTITY_SCALE);
    private static final BigDecimal PRICE_SCALE_BD =
            BigDecimal.valueOf(com.balh.oms.cluster.AcceptOrderCommand.PRICE_SCALE);
    private static final BigDecimal ZERO = BigDecimal.ZERO;

    private final FixExecutionReportMapper mapper;
    private final OmsClusterIngressClient clusterIngressClient;
    private final OmsConfig omsConfig;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;

    public FixInboundClusterSink(
            FixExecutionReportMapper mapper,
            OmsClusterIngressClient clusterIngressClient,
            OmsConfig omsConfig,
            ObjectMapper objectMapper,
            MeterRegistry meterRegistry) {
        this.mapper = mapper;
        this.clusterIngressClient = clusterIngressClient;
        this.omsConfig = omsConfig;
        this.objectMapper = objectMapper;
        this.meterRegistry = meterRegistry;
    }

    public void handleExecutionReport(Message message) throws FieldNotFound {
        String venueId = omsConfig.getFix().getVenueIdForExecutions();
        Optional<ExecutionTradeCommand> trade = mapper.tryParseTrade(message, venueId);
        if (trade.isPresent()) {
            ApplyExecutionReportCommand cmd = buildTrade(message, trade.get());
            submit(cmd, "trade");
            return;
        }
        Optional<ExecutionCancelCommand> cancel = mapper.tryParseCancel(message, venueId);
        if (cancel.isPresent()) {
            ApplyExecutionReportCommand cmd = buildCancel(message, cancel.get());
            submit(cmd, "cancel");
            return;
        }
        // Wed-demo addition. ER ET=5 REPLACED carries the broker's authoritative new OrderQty
        // and Price; the cluster's apply path on EXEC_TYPE_REPLACE updates the order in place.
        Optional<ExecutionTradeCommand> replace = mapper.tryParseReplace(message, venueId);
        if (replace.isPresent()) {
            ApplyExecutionReportCommand cmd = buildReplace(message, replace.get());
            submit(cmd, "replace");
            return;
        }
        Optional<ExecutionVenueRejectCommand> reject = mapper.tryParseVenueReject(message, venueId);
        if (reject.isPresent()) {
            ApplyExecutionReportCommand cmd = buildVenueReject(message, reject.get());
            submit(cmd, "venue_reject");
            return;
        }
        meterRegistry.counter(FixMetrics.METRIC_INBOUND_ER, FixMetrics.TAG_DISPOSITION, "ignored").increment();
    }

    public void handleOrderCancelReject(Message message) throws FieldNotFound {
        String venueId = omsConfig.getFix().getVenueIdForExecutions();
        Optional<ExecutionVenueRejectCommand> ocr = mapper.tryParseOrderCancelReject(message, venueId);
        if (ocr.isEmpty()) {
            meterRegistry
                    .counter(FixMetrics.METRIC_INBOUND_ER, FixMetrics.TAG_DISPOSITION, "ocr_ignored")
                    .increment();
            return;
        }
        // Wed-demo fix. CxlRejResponseTo=1 → cancel reject; =2 → cancel-replace reject. The two
        // route to distinct execTypeCodes so the cluster's apply path emits the right
        // ExecutionAppliedEvent shape (and the projector writes the right domain envelope kind
        // — OrderCancelRejected vs OrderReplaceRejected). Both must NOT mutate the order's
        // status (the order stays WORKING / PARTIALLY_FILLED), which is what was broken before:
        // {@code buildVenueReject} set {@code EXEC_TYPE_VENUE_REJECT} and the cluster moved the
        // order to REJECTED. Now the cluster sees the dedicated reject discriminator and bumps
        // version only, leaving status / cumQty untouched.
        //
        // CxlRejResponseTo (tag 434) is a {@link CharField} in QuickFIX/J — the constants
        // ({@link CxlRejResponseTo#ORDER_CANCEL_REQUEST} = '1', etc.) are {@code char} values.
        // We MUST read it via {@code getChar} (not {@code getInt}, which would parse the wire
        // value "2" as the integer 2 and never match the char constant promoted to int 50 — the
        // bug that originally collapsed all replace-rejects into cancel-rejects on the venue
        // path; bytecode-verified against {@code build/libs/oms-…-fix-egress.jar}).
        char cxlRejResponseTo = message.isSetField(CxlRejResponseTo.FIELD)
                ? message.getChar(CxlRejResponseTo.FIELD)
                : CxlRejResponseTo.ORDER_CANCEL_REQUEST;
        byte execTypeCode = cxlRejResponseTo == CxlRejResponseTo.ORDER_CANCEL_REPLACE_REQUEST
                ? ApplyExecutionReportCommand.EXEC_TYPE_REPLACE_REJECT
                : ApplyExecutionReportCommand.EXEC_TYPE_CANCEL_REJECT;
        ApplyExecutionReportCommand cmd = buildCancelOrReplaceReject(message, ocr.get(), execTypeCode);
        submit(cmd, cxlRejResponseTo == CxlRejResponseTo.ORDER_CANCEL_REPLACE_REQUEST
                ? "ocr_replace_reject"
                : "ocr_cancel_reject");
    }

    private void submit(ApplyExecutionReportCommand cmd, String disposition) {
        Duration submitTimeout = Duration.ofMillis(omsConfig.getCluster().getClient().getSubmitTimeoutMs());
        try {
            clusterIngressClient.submitApplyExecutionReport(cmd, submitTimeout);
            meterRegistry
                    .counter(FixMetrics.METRIC_INBOUND_ER, FixMetrics.TAG_DISPOSITION, "cluster_" + disposition)
                    .increment();
        } catch (java.util.concurrent.TimeoutException e) {
            // Cluster offer timed out under back-pressure. Re-throwing would tear down the FIX
            // session; the broker would then retransmit on next logon and we'd retry the offer.
            // For now, log + counter so operators can observe; the (sender, msgSeqNum) dedupe
            // guard means re-arriving the same message is safe.
            log.warn(
                    "FixInboundClusterSink submit timed out for orderId={} venueExecRef={}; broker will retry on resend",
                    cmd.orderId(),
                    cmd.venueExecRef(),
                    e);
            meterRegistry
                    .counter(FixMetrics.METRIC_INBOUND_ER, FixMetrics.TAG_DISPOSITION, "cluster_timeout_" + disposition)
                    .increment();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn(
                    "FixInboundClusterSink interrupted while submitting orderId={} venueExecRef={}",
                    cmd.orderId(),
                    cmd.venueExecRef());
            meterRegistry
                    .counter(FixMetrics.METRIC_INBOUND_ER, FixMetrics.TAG_DISPOSITION, "cluster_interrupted_" + disposition)
                    .increment();
        }
    }

    private ApplyExecutionReportCommand buildTrade(Message message, ExecutionTradeCommand t)
            throws FieldNotFound {
        long lastQtyScaled = scaleQty(t.lastQuantity());
        long lastPxScaled = t.lastPrice() != null ? scalePx(t.lastPrice()) : 0L;
        return new ApplyExecutionReportCommand(
                /* correlationId = */ 0L,
                t.orderId(),
                lastQtyScaled,
                lastPxScaled,
                instantToNanos(t.venueTs()),
                msgSeqNum(message),
                ApplyExecutionReportCommand.EXEC_TYPE_TRADE,
                /* rejectCodeOrZero = */ (byte) 0,
                t.venueId(),
                t.venueExecRef(),
                senderCompId(message),
                rawTradeJson(t));
    }

    private ApplyExecutionReportCommand buildCancel(Message message, ExecutionCancelCommand c)
            throws FieldNotFound {
        return new ApplyExecutionReportCommand(
                0L,
                c.orderId(),
                /* lastQtyScaled = */ 0L,
                /* lastPxScaled = */ 0L,
                instantToNanos(c.venueTs()),
                msgSeqNum(message),
                ApplyExecutionReportCommand.EXEC_TYPE_CANCEL,
                (byte) 0,
                c.venueId(),
                c.venueExecRef(),
                senderCompId(message),
                rawCancelJson(c));
    }

    /**
     * Wed-demo addition. Build an ApplyExecutionReportCommand for a 35=8 ER ET=5 REPLACED. The
     * {@code lastQtyScaled} slot carries the broker's new authoritative OrderQty (NOT a fill
     * quantity), and {@code lastPxScaled} carries the new limit price (0 means "unchanged" — the
     * cluster preserves the existing price in that case). See
     * {@link ApplyExecutionReportCommand#EXEC_TYPE_REPLACE} for the overloaded semantic.
     */
    private ApplyExecutionReportCommand buildReplace(Message message, ExecutionTradeCommand r)
            throws FieldNotFound {
        long newOrderQtyScaled = scaleQty(r.lastQuantity());
        long newLimitPxScaled = r.lastPrice() != null && r.lastPrice().signum() != 0
                ? scalePx(r.lastPrice())
                : 0L;
        return new ApplyExecutionReportCommand(
                0L,
                r.orderId(),
                newOrderQtyScaled,
                newLimitPxScaled,
                instantToNanos(r.venueTs()),
                msgSeqNum(message),
                ApplyExecutionReportCommand.EXEC_TYPE_REPLACE,
                (byte) 0,
                r.venueId(),
                r.venueExecRef(),
                senderCompId(message),
                rawReplaceJson(r));
    }

    private ApplyExecutionReportCommand buildVenueReject(Message message, ExecutionVenueRejectCommand v)
            throws FieldNotFound {
        return new ApplyExecutionReportCommand(
                0L,
                v.orderId(),
                0L,
                0L,
                instantToNanos(v.venueTs()),
                msgSeqNum(message),
                ApplyExecutionReportCommand.EXEC_TYPE_VENUE_REJECT,
                (byte) com.balh.oms.domain.RejectCode.VENUE_REJECT.ordinal(),
                v.venueId(),
                v.venueExecRef(),
                senderCompId(message),
                v.rawEnvelopeJson());
    }

    /**
     * Wed-demo addition. Same shape as {@link #buildVenueReject} but parameterised on
     * {@code execTypeCode} so the cluster apply path can distinguish "broker rejected our 35=F"
     * ({@link ApplyExecutionReportCommand#EXEC_TYPE_CANCEL_REJECT}) from "broker rejected our
     * 35=G" ({@link ApplyExecutionReportCommand#EXEC_TYPE_REPLACE_REJECT}). Neither mutates the
     * order's status — the order stays WORKING / PARTIALLY_FILLED.
     */
    private ApplyExecutionReportCommand buildCancelOrReplaceReject(
            Message message, ExecutionVenueRejectCommand v, byte execTypeCode) throws FieldNotFound {
        return new ApplyExecutionReportCommand(
                0L,
                v.orderId(),
                0L,
                0L,
                instantToNanos(v.venueTs()),
                msgSeqNum(message),
                execTypeCode,
                (byte) 0,
                v.venueId(),
                v.venueExecRef(),
                senderCompId(message),
                v.rawEnvelopeJson());
    }

    private static long scaleQty(BigDecimal qty) {
        return qty.multiply(QUANTITY_SCALE_BD).setScale(0, RoundingMode.UNNECESSARY).longValueExact();
    }

    private static long scalePx(BigDecimal px) {
        return px.multiply(PRICE_SCALE_BD).setScale(0, RoundingMode.UNNECESSARY).longValueExact();
    }

    private static long instantToNanos(Instant ts) {
        // Fixed-point packing: epoch seconds * 1e9 + nanos. Bounded by Long.MAX_VALUE / 1e9 ≈
        // year 2262, well past any realistic venue timestamp.
        return Math.multiplyExact(ts.getEpochSecond(), 1_000_000_000L) + ts.getNano();
    }

    private static int msgSeqNum(Message msg) throws FieldNotFound {
        return msg.getHeader().getInt(MsgSeqNum.FIELD);
    }

    private static String senderCompId(Message msg) throws FieldNotFound {
        return msg.getHeader().getString(SenderCompID.FIELD);
    }

    private String rawTradeJson(ExecutionTradeCommand cmd) {
        ObjectNode n = objectMapper.createObjectNode();
        n.put("kind", "ExecutionReport");
        n.put("execType", "TRADE");
        n.put("venueId", cmd.venueId());
        n.put("venueExecRef", cmd.venueExecRef());
        n.put("lastQuantity", cmd.lastQuantity().toPlainString());
        if (cmd.lastPrice() != null) {
            n.put("lastPrice", cmd.lastPrice().toPlainString());
        }
        return writeJson(n);
    }

    private String rawCancelJson(ExecutionCancelCommand cmd) {
        ObjectNode n = objectMapper.createObjectNode();
        n.put("kind", "ExecutionReport");
        n.put("execType", "CANCEL");
        n.put("venueId", cmd.venueId());
        n.put("venueExecRef", cmd.venueExecRef());
        return writeJson(n);
    }

    private String rawReplaceJson(ExecutionTradeCommand cmd) {
        ObjectNode n = objectMapper.createObjectNode();
        n.put("kind", "ExecutionReport");
        n.put("execType", "REPLACE");
        n.put("venueId", cmd.venueId());
        n.put("venueExecRef", cmd.venueExecRef());
        n.put("newOrderQty", cmd.lastQuantity().toPlainString());
        if (cmd.lastPrice() != null) {
            n.put("newLimitPrice", cmd.lastPrice().toPlainString());
        }
        return writeJson(n);
    }

    private String writeJson(ObjectNode n) {
        try {
            return objectMapper.writeValueAsString(n);
        } catch (JsonProcessingException e) {
            // Jackson can serialize a hand-built ObjectNode without I/O — only known cause is a
            // truly broken ObjectMapper config. Wrap as IllegalStateException so the FIX engine
            // surfaces it as a session-level error rather than a silent metric miss.
            throw new IllegalStateException("failed to serialize raw envelope JSON", e);
        }
    }
}
