package com.balh.oms.fix;

import com.balh.oms.cluster.AcceptOrderCommand;
import com.balh.oms.cluster.ApplyExecutionReportCommand;
import com.balh.oms.cluster.OmsClusterIngressClient;
import com.balh.oms.config.OmsConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import quickfix.Message;
import quickfix.field.ClOrdID;
import quickfix.field.CumQty;
import quickfix.field.CxlRejReason;
import quickfix.field.ExecID;
import quickfix.field.ExecType;
import quickfix.field.LastPx;
import quickfix.field.LastQty;
import quickfix.field.LeavesQty;
import quickfix.field.MsgSeqNum;
import quickfix.field.MsgType;
import quickfix.field.OrderID;
import quickfix.field.OrigClOrdID;
import quickfix.field.SenderCompID;
import quickfix.field.TransactTime;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

/**
 * Slice 3d unit coverage: verifies {@link FixInboundClusterSink} maps inbound FIX
 * {@code ExecutionReport} / {@code OrderCancelReject} messages onto well-formed
 * {@link ApplyExecutionReportCommand}s and submits them through {@link OmsClusterIngressClient}.
 *
 * <p>Each scenario asserts:
 *
 * <ol>
 *   <li>The {@code execTypeCode} matches the FIX {@code ExecType} (TRADE / CANCEL / VENUE_REJECT).
 *   <li>{@code lastQtyScaled} / {@code lastPxScaled} use the {@link AcceptOrderCommand#QUANTITY_SCALE}
 *       / {@link AcceptOrderCommand#PRICE_SCALE} fixed-point factors, and zero-out for non-trade
 *       messages.
 *   <li>{@code senderCompId} / {@code msgSeqNum} are pulled from the FIX header (slice 3d's
 *       wire-level dedupe key).
 *   <li>{@code rawEnvelopeJson} carries the same shape the projector consumes.
 * </ol>
 */
class FixInboundClusterSinkTest {

    private static final String VENUE = "FIX";

    private OmsClusterIngressClient ingressClient;
    private FixInboundClusterSink sink;

    @BeforeEach
    void setUp() {
        ObjectMapper objectMapper = new ObjectMapper();
        FixExecutionReportMapper mapper = new FixExecutionReportMapper(objectMapper);
        ingressClient = mock(OmsClusterIngressClient.class);
        OmsConfig cfg = new OmsConfig();
        cfg.getFix().setVenueIdForExecutions(VENUE);
        sink = new FixInboundClusterSink(
                mapper, ingressClient, cfg, objectMapper, new SimpleMeterRegistry());
    }

    @Test
    void partialFill_buildsTradeCommand_withScaledQuantitiesAndWireKeys() throws Exception {
        UUID orderId = UUID.fromString("11111111-1111-1111-1111-111111111111");
        Message m = newExecutionReport();
        m.setChar(ExecType.FIELD, ExecType.PARTIAL_FILL);
        m.setString(ClOrdID.FIELD, orderId.toString());
        m.setString(ExecID.FIELD, "exec-trade-1");
        m.setString(LastQty.FIELD, "5");
        m.setString(LastPx.FIELD, "1.234567");
        m.setString(LeavesQty.FIELD, "5");
        m.setString(CumQty.FIELD, "5");
        m.setString(TransactTime.FIELD, "20260512-12:00:00.000");
        m.getHeader().setString(SenderCompID.FIELD, "BROKER_ACCEPT");
        m.getHeader().setInt(MsgSeqNum.FIELD, 42);

        sink.handleExecutionReport(m);

        ApplyExecutionReportCommand sent = captureSubmitted();
        assertThat(sent.execTypeCode()).isEqualTo(ApplyExecutionReportCommand.EXEC_TYPE_TRADE);
        assertThat(sent.orderId()).isEqualTo(orderId);
        assertThat(sent.venueExecRef()).isEqualTo("exec-trade-1");
        assertThat(sent.venueId()).isEqualTo(VENUE);
        assertThat(sent.lastQtyScaled())
                .as("lastQty=5 * 1e9 quantity scale")
                .isEqualTo(5L * AcceptOrderCommand.QUANTITY_SCALE);
        assertThat(sent.lastPxScaled())
                .as("lastPx=1.234567 * 1e6 price scale")
                .isEqualTo(1_234_567L);
        assertThat(sent.senderCompId()).isEqualTo("BROKER_ACCEPT");
        assertThat(sent.msgSeqNum()).isEqualTo(42);
        assertThat(sent.rejectCodeOrZero()).isEqualTo((byte) 0);
        assertThat(sent.rawEnvelopeJson()).contains("\"execType\":\"TRADE\"");
    }

    @Test
    void canceledExecutionReport_buildsCancelCommand_withZeroQuantities() throws Exception {
        UUID orderId = UUID.fromString("22222222-2222-2222-2222-222222222222");
        Message m = newExecutionReport();
        m.setChar(ExecType.FIELD, ExecType.CANCELED);
        m.setString(ClOrdID.FIELD, orderId.toString());
        m.setString(ExecID.FIELD, "exec-cancel-1");
        m.setString(TransactTime.FIELD, "20260512-12:01:00.000");
        m.getHeader().setString(SenderCompID.FIELD, "BROKER_ACCEPT");
        m.getHeader().setInt(MsgSeqNum.FIELD, 43);

        sink.handleExecutionReport(m);

        ApplyExecutionReportCommand sent = captureSubmitted();
        assertThat(sent.execTypeCode()).isEqualTo(ApplyExecutionReportCommand.EXEC_TYPE_CANCEL);
        assertThat(sent.orderId()).isEqualTo(orderId);
        assertThat(sent.venueExecRef()).isEqualTo("exec-cancel-1");
        assertThat(sent.lastQtyScaled()).isZero();
        assertThat(sent.lastPxScaled()).isZero();
        assertThat(sent.senderCompId()).isEqualTo("BROKER_ACCEPT");
        assertThat(sent.msgSeqNum()).isEqualTo(43);
        assertThat(sent.rawEnvelopeJson()).contains("\"execType\":\"CANCEL\"");
    }

    @Test
    void rejectedExecutionReport_buildsVenueRejectCommand_withRejectCode() throws Exception {
        UUID orderId = UUID.fromString("33333333-3333-3333-3333-333333333333");
        Message m = newExecutionReport();
        m.setChar(ExecType.FIELD, ExecType.REJECTED);
        m.setString(ClOrdID.FIELD, orderId.toString());
        m.setString(ExecID.FIELD, "exec-rej-1");
        m.setString(TransactTime.FIELD, "20260512-12:02:00.000");
        m.getHeader().setString(SenderCompID.FIELD, "BROKER_ACCEPT");
        m.getHeader().setInt(MsgSeqNum.FIELD, 44);

        sink.handleExecutionReport(m);

        ApplyExecutionReportCommand sent = captureSubmitted();
        assertThat(sent.execTypeCode()).isEqualTo(ApplyExecutionReportCommand.EXEC_TYPE_VENUE_REJECT);
        assertThat(sent.orderId()).isEqualTo(orderId);
        assertThat(sent.rejectCodeOrZero())
                .as("VENUE_REJECT ordinal carried so the projector can write executions.exec_type=REJECT")
                .isEqualTo((byte) com.balh.oms.domain.RejectCode.VENUE_REJECT.ordinal());
        assertThat(sent.senderCompId()).isEqualTo("BROKER_ACCEPT");
        assertThat(sent.msgSeqNum()).isEqualTo(44);
    }

    @Test
    void orderCancelReject_noCxlRejResponseTo_defaultsToCancelReject() throws Exception {
        // Wed-demo fix (chunk #2): a 35=9 without explicit CxlRejResponseTo defaults to
        // ORDER_CANCEL_REQUEST → EXEC_TYPE_CANCEL_REJECT. Prior behavior mapped it to
        // EXEC_TYPE_VENUE_REJECT (which moved the order to REJECTED — wrong, the order is still
        // live on the broker). This test pins the corrected default.
        UUID orderId = UUID.fromString("44444444-4444-4444-4444-444444444444");
        Message m = new Message();
        m.getHeader().setString(MsgType.FIELD, MsgType.ORDER_CANCEL_REJECT);
        m.setString(OrigClOrdID.FIELD, orderId.toString());
        m.setString(OrderID.FIELD, "BR-99");
        m.setInt(CxlRejReason.FIELD, CxlRejReason.UNKNOWN_ORDER);
        m.setString(TransactTime.FIELD, "20260512-12:03:00.000");
        m.getHeader().setString(SenderCompID.FIELD, "BROKER_ACCEPT");
        m.getHeader().setInt(MsgSeqNum.FIELD, 45);

        sink.handleOrderCancelReject(m);

        ApplyExecutionReportCommand sent = captureSubmitted();
        assertThat(sent.execTypeCode())
                .as("35=9 with no CxlRejResponseTo ⇒ CANCEL_REJECT, NOT VENUE_REJECT")
                .isEqualTo(ApplyExecutionReportCommand.EXEC_TYPE_CANCEL_REJECT);
        assertThat(sent.orderId()).isEqualTo(orderId);
        assertThat(sent.venueExecRef())
                .as("OCR has no ExecID; mapper synthesises one from broker order id + reason")
                .isEqualTo("ocr-BR-99-" + CxlRejReason.UNKNOWN_ORDER);
        assertThat(sent.senderCompId()).isEqualTo("BROKER_ACCEPT");
        assertThat(sent.msgSeqNum()).isEqualTo(45);
    }

    @Test
    void orderCancelReject_cxlRejResponseToCancel_buildsCancelRejectCommand() throws Exception {
        UUID orderId = UUID.fromString("44444444-4444-4444-4444-444444444444");
        Message m = new Message();
        m.getHeader().setString(MsgType.FIELD, MsgType.ORDER_CANCEL_REJECT);
        m.setString(OrigClOrdID.FIELD, orderId.toString());
        m.setString(OrderID.FIELD, "BR-100");
        m.setInt(CxlRejReason.FIELD, CxlRejReason.UNKNOWN_ORDER);
        m.setInt(quickfix.field.CxlRejResponseTo.FIELD,
                quickfix.field.CxlRejResponseTo.ORDER_CANCEL_REQUEST);
        m.setString(TransactTime.FIELD, "20260512-12:04:00.000");
        m.getHeader().setString(SenderCompID.FIELD, "BROKER_ACCEPT");
        m.getHeader().setInt(MsgSeqNum.FIELD, 46);

        sink.handleOrderCancelReject(m);

        ApplyExecutionReportCommand sent = captureSubmitted();
        assertThat(sent.execTypeCode())
                .isEqualTo(ApplyExecutionReportCommand.EXEC_TYPE_CANCEL_REJECT);
    }

    @Test
    void orderCancelReject_cxlRejResponseToReplace_buildsReplaceRejectCommand() throws Exception {
        UUID orderId = UUID.fromString("44444444-4444-4444-4444-444444444444");
        Message m = new Message();
        m.getHeader().setString(MsgType.FIELD, MsgType.ORDER_CANCEL_REJECT);
        m.setString(OrigClOrdID.FIELD, orderId.toString());
        m.setString(OrderID.FIELD, "BR-101");
        m.setInt(CxlRejReason.FIELD, CxlRejReason.UNKNOWN_ORDER);
        m.setInt(quickfix.field.CxlRejResponseTo.FIELD,
                quickfix.field.CxlRejResponseTo.ORDER_CANCEL_REPLACE_REQUEST);
        m.setString(TransactTime.FIELD, "20260512-12:05:00.000");
        m.getHeader().setString(SenderCompID.FIELD, "BROKER_ACCEPT");
        m.getHeader().setInt(MsgSeqNum.FIELD, 47);

        sink.handleOrderCancelReject(m);

        ApplyExecutionReportCommand sent = captureSubmitted();
        assertThat(sent.execTypeCode())
                .as("35=9 with CxlRejResponseTo=ORDER_CANCEL_REPLACE_REQUEST ⇒ REPLACE_REJECT")
                .isEqualTo(ApplyExecutionReportCommand.EXEC_TYPE_REPLACE_REJECT);
    }

    @Test
    void newExecutionReport_buildsVenueNewCommand_promotesPendingToWorking() throws Exception {
        UUID orderId = UUID.fromString("55555555-5555-5555-5555-555555555555");
        Message m = newExecutionReport();
        m.setChar(ExecType.FIELD, ExecType.NEW); // venue acknowledged our NewOrderSingle (39=0 New).
        m.setString(ClOrdID.FIELD, orderId.toString());
        m.setString(ExecID.FIELD, "exec-new-1");
        m.setString(LastQty.FIELD, "0");
        m.setString(LeavesQty.FIELD, "10");
        m.setString(CumQty.FIELD, "0");
        m.getHeader().setString(SenderCompID.FIELD, "BROKER_ACCEPT");
        m.getHeader().setInt(MsgSeqNum.FIELD, 99);

        sink.handleExecutionReport(m);

        ApplyExecutionReportCommand sent = captureSubmitted();
        assertThat(sent.execTypeCode()).isEqualTo(ApplyExecutionReportCommand.EXEC_TYPE_VENUE_NEW);
        assertThat(sent.orderId()).isEqualTo(orderId);
        assertThat(sent.venueExecRef()).isEqualTo("exec-new-1");
        assertThat(sent.lastQtyScaled()).isZero();
        assertThat(sent.lastPxScaled()).isZero();
        assertThat(sent.senderCompId()).isEqualTo("BROKER_ACCEPT");
        assertThat(sent.msgSeqNum()).isEqualTo(99);
        assertThat(sent.rawEnvelopeJson()).contains("\"execType\":\"NEW\"");
    }

    @Test
    void executionReport_unrecognisedExecType_isDropped_withoutClusterSubmit() throws Exception {
        Message m = newExecutionReport();
        // SUSPENDED is not a state transition we project (not New / fill / cancel / replace / reject).
        m.setChar(ExecType.FIELD, ExecType.SUSPENDED);
        m.setString(ClOrdID.FIELD, UUID.randomUUID().toString());
        m.setString(ExecID.FIELD, "exec-suspended-1");
        m.setString(LastQty.FIELD, "0");
        m.setString(LeavesQty.FIELD, "10");
        m.setString(CumQty.FIELD, "0");
        m.getHeader().setString(SenderCompID.FIELD, "BROKER_ACCEPT");
        m.getHeader().setInt(MsgSeqNum.FIELD, 99);

        sink.handleExecutionReport(m);

        verifyNoInteractions(ingressClient);
    }

    @Test
    void executionReport_clOrdIdNotUuid_isDropped_withoutClusterSubmit() throws Exception {
        Message m = newExecutionReport();
        m.setChar(ExecType.FIELD, ExecType.FILL);
        m.setString(ClOrdID.FIELD, "not-a-uuid");
        m.setString(ExecID.FIELD, "exec-bad-uuid");
        m.setString(LastQty.FIELD, "1");
        m.setString(LastPx.FIELD, "1");
        m.setString(LeavesQty.FIELD, "0");
        m.setString(CumQty.FIELD, "1");
        m.getHeader().setString(SenderCompID.FIELD, "BROKER_ACCEPT");
        m.getHeader().setInt(MsgSeqNum.FIELD, 100);

        sink.handleExecutionReport(m);

        verifyNoInteractions(ingressClient);
    }

    private static Message newExecutionReport() {
        Message m = new Message();
        m.getHeader().setString(MsgType.FIELD, MsgType.EXECUTION_REPORT);
        return m;
    }

    private ApplyExecutionReportCommand captureSubmitted() throws Exception {
        ArgumentCaptor<ApplyExecutionReportCommand> captor =
                ArgumentCaptor.forClass(ApplyExecutionReportCommand.class);
        verify(ingressClient, times(1)).submitApplyExecutionReport(captor.capture(), any(Duration.class));
        return captor.getValue();
    }
}
