package com.balh.oms.fix;

import com.balh.oms.returnpath.ExecutionTradeCommand;
import org.junit.jupiter.api.Test;
import quickfix.Message;
import quickfix.field.ClOrdID;
import quickfix.field.CumQty;
import quickfix.field.ExecID;
import quickfix.field.ExecType;
import quickfix.field.LastPx;
import quickfix.field.LastQty;
import quickfix.field.LeavesQty;
import quickfix.field.TransactTime;

import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class FixExecutionReportMapperTest {

    private final FixExecutionReportMapper mapper = new FixExecutionReportMapper();

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
}
