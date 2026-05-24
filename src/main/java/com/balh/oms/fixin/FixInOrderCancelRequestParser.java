package com.balh.oms.fixin;

import com.balh.oms.cluster.AcceptOrderCommand;
import com.balh.oms.config.OmsProfiles;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import quickfix.FieldNotFound;
import quickfix.Message;
import quickfix.field.ClOrdID;
import quickfix.field.OrigClOrdID;
import quickfix.field.Side;
import quickfix.field.Symbol;

@Component
@Profile(OmsProfiles.FIX_INGRESS)
public class FixInOrderCancelRequestParser {

    private final FixInSymbolMapper symbolMapper;

    public FixInOrderCancelRequestParser(FixInSymbolMapper symbolMapper) {
        this.symbolMapper = symbolMapper;
    }

    public FixInParsedCancel parse(Message message) throws FieldNotFound {
        String clOrdId = requireNonBlank(message.getString(ClOrdID.FIELD), "cl_ord_id_required");
        String origClOrdId = requireNonBlank(message.getString(OrigClOrdID.FIELD), "orig_cl_ord_id_required");
        byte sideCode = mapSide(message.getChar(Side.FIELD));
        String symbol = symbolMapper.toOmsSymbol(message.getString(Symbol.FIELD));
        return new FixInParsedCancel(clOrdId, origClOrdId, sideCode, symbol);
    }

    private static byte mapSide(char side) {
        return switch (side) {
            case Side.BUY -> AcceptOrderCommand.SIDE_BUY;
            case Side.SELL -> AcceptOrderCommand.SIDE_SELL;
            default -> throw new FixInParseException("side_unsupported");
        };
    }

    private static String requireNonBlank(String value, String code) {
        if (value == null || value.isBlank()) {
            throw new FixInParseException(code);
        }
        return value.trim();
    }
}
