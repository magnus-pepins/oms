package com.balh.oms.fix;

import com.balh.oms.returnpath.ExecutionTradeCommand;
import com.balh.oms.returnpath.ExecutionVenueRejectCommand;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import quickfix.Message;
import quickfix.field.ClOrdID;
import quickfix.field.CumQty;
import quickfix.field.CxlRejReason;
import quickfix.field.ExecID;
import quickfix.field.ExecType;
import quickfix.field.LastPx;
import quickfix.field.LastQty;
import quickfix.field.LeavesQty;
import quickfix.field.MsgType;
import quickfix.field.OrderID;
import quickfix.field.OrigClOrdID;
import quickfix.field.TransactTime;

import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class FixExecutionReportMapperTest {

    private final FixExecutionReportMapper mapper = new FixExecutionReportMapper(new ObjectMapper());

    @Test
    void mapsFillExecutionReport() throws Exception {
        UUID orderId = UUID.fromString("11111111-1111-1111-1111-111111111111");
        Message m = new Message();
        m.setChar(ExecType.FIELD, ExecType.FILL);
        m.setString(ClOrdID.FIELD, orderId.toString());
        m.setString(ExecID.FIELD, "exec-1");
        m.setString(LastQty.FIELD, "10");
        m.setString(LastPx.FIELD, "1.25");
        m.setString(LeavesQty.FIELD, "0");
        m.setString(CumQty.FIELD, "10");
        m.setString(TransactTime.FIELD, "20260115-10:00:00.000");

        Optional<ExecutionTradeCommand> cmd = mapper.tryParseTrade(m, "FIX");
        assertThat(cmd).isPresent();
        assertThat(cmd.get().orderId()).isEqualTo(orderId);
        assertThat(cmd.get().venueExecRef()).isEqualTo("exec-1");
        assertThat(cmd.get().lastQuantity().toPlainString()).isEqualTo("10");
        assertThat(cmd.get().lastPrice().toPlainString()).isEqualTo("1.25");
    }

    @Test
    void ignoresNonFillExecTypes() throws Exception {
        Message m = new Message();
        m.setChar(ExecType.FIELD, ExecType.NEW);
        m.setString(ClOrdID.FIELD, UUID.randomUUID().toString());
        m.setString(ExecID.FIELD, "x");
        m.setString(LastQty.FIELD, "1");
        m.setString(LastPx.FIELD, "1");
        m.setString(LeavesQty.FIELD, "9");
        m.setString(CumQty.FIELD, "1");

        assertThat(mapper.tryParseTrade(m, "FIX")).isEmpty();
    }

    @Test
    void mapsRejectedExecutionReport() throws Exception {
        UUID orderId = UUID.fromString("22222222-2222-2222-2222-222222222222");
        Message m = new Message();
        m.setChar(ExecType.FIELD, ExecType.REJECTED);
        m.setString(ClOrdID.FIELD, orderId.toString());
        m.setString(ExecID.FIELD, "rej-1");
        m.setString(TransactTime.FIELD, "20260115-11:00:00.000");

        Optional<ExecutionVenueRejectCommand> cmd = mapper.tryParseVenueReject(m, "FIX");
        assertThat(cmd).isPresent();
        assertThat(cmd.get().orderId()).isEqualTo(orderId);
        assertThat(cmd.get().venueExecRef()).isEqualTo("rej-1");
        assertThat(cmd.get().rawEnvelopeJson()).contains("REJECTED");
    }

    @Test
    void mapsOrderCancelReject() throws Exception {
        UUID orderId = UUID.fromString("33333333-3333-3333-3333-333333333333");
        Message m = new Message();
        m.getHeader().setString(MsgType.FIELD, MsgType.ORDER_CANCEL_REJECT);
        m.setString(OrigClOrdID.FIELD, orderId.toString());
        m.setString(OrderID.FIELD, "BR-42");
        m.setInt(CxlRejReason.FIELD, CxlRejReason.UNKNOWN_ORDER);
        m.setString(TransactTime.FIELD, "20260115-12:00:00.000");

        Optional<ExecutionVenueRejectCommand> cmd = mapper.tryParseOrderCancelReject(m, "FIX");
        assertThat(cmd).isPresent();
        assertThat(cmd.get().orderId()).isEqualTo(orderId);
        assertThat(cmd.get().venueExecRef()).isEqualTo("ocr-BR-42-" + CxlRejReason.UNKNOWN_ORDER);
        assertThat(cmd.get().rawEnvelopeJson()).contains("OrderCancelReject");
    }
}
