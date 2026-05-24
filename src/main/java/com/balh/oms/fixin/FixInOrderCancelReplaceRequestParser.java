package com.balh.oms.fixin;

import com.balh.oms.cluster.AcceptOrderCommand;
import com.balh.oms.config.OmsProfiles;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import quickfix.FieldNotFound;
import quickfix.Message;
import quickfix.field.ClOrdID;
import quickfix.field.OrdType;
import quickfix.field.OrderQty;
import quickfix.field.OrigClOrdID;
import quickfix.field.Price;
import quickfix.field.Side;
import quickfix.field.Symbol;

import java.math.BigDecimal;

@Component
@Profile(OmsProfiles.FIX_INGRESS)
public class FixInOrderCancelReplaceRequestParser {

    private final FixInSymbolMapper symbolMapper;

    public FixInOrderCancelReplaceRequestParser(FixInSymbolMapper symbolMapper) {
        this.symbolMapper = symbolMapper;
    }

    public FixInParsedReplace parse(Message message) throws FieldNotFound {
        String clOrdId = requireNonBlank(message.getString(ClOrdID.FIELD), "cl_ord_id_required");
        String origClOrdId = requireNonBlank(message.getString(OrigClOrdID.FIELD), "orig_cl_ord_id_required");
        byte sideCode = mapSide(message.getChar(Side.FIELD));
        char ordType = message.getChar(OrdType.FIELD);
        byte ordTypeCode = mapOrdType(ordType);
        BigDecimal quantity = FixInNewOrderSingleParser.parsePositiveDecimal(message.getString(OrderQty.FIELD), "quantity_invalid");
        BigDecimal limitPrice = null;
        if (message.isSetField(Price.FIELD)) {
            limitPrice = FixInNewOrderSingleParser.parsePositiveDecimal(message.getString(Price.FIELD), "price_invalid");
        }
        if (ordTypeCode == AcceptOrderCommand.ORD_TYPE_LIMIT && limitPrice == null) {
            throw new FixInParseException("limit_price_required");
        }
        String symbol = symbolMapper.toOmsSymbol(message.getString(Symbol.FIELD));
        return new FixInParsedReplace(clOrdId, origClOrdId, sideCode, ordTypeCode, quantity, limitPrice, symbol);
    }

    private static byte mapSide(char side) {
        return switch (side) {
            case Side.BUY -> AcceptOrderCommand.SIDE_BUY;
            case Side.SELL -> AcceptOrderCommand.SIDE_SELL;
            default -> throw new FixInParseException("side_unsupported");
        };
    }

    private static byte mapOrdType(char ordType) {
        return switch (ordType) {
            case OrdType.MARKET -> AcceptOrderCommand.ORD_TYPE_MARKET;
            case OrdType.LIMIT -> AcceptOrderCommand.ORD_TYPE_LIMIT;
            default -> throw new FixInParseException("ord_type_unsupported");
        };
    }

    private static String requireNonBlank(String value, String code) {
        if (value == null || value.isBlank()) {
            throw new FixInParseException(code);
        }
        return value.trim();
    }
}
