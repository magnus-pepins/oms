package com.balh.oms.fixin;

import com.balh.oms.cluster.AcceptOrderCommand;
import com.balh.oms.config.OmsConfig;
import com.balh.oms.config.OmsProfiles;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import quickfix.FieldNotFound;
import quickfix.Message;
import quickfix.field.Account;
import quickfix.field.ClOrdID;
import quickfix.field.OrdType;
import quickfix.field.OrderQty;
import quickfix.field.Price;
import quickfix.field.Side;
import quickfix.field.Symbol;
import quickfix.field.TimeInForce;

import java.math.BigDecimal;
import java.math.RoundingMode;

@Component
@Profile(OmsProfiles.FIX_INGRESS)
public class FixInNewOrderSingleParser {

    private final FixInSymbolMapper symbolMapper;
    private final int portfolioIdTag;
    private final int maxPortfolioIdLength;

    public FixInNewOrderSingleParser(FixInSymbolMapper symbolMapper, OmsConfig omsConfig) {
        this.symbolMapper = symbolMapper;
        this.portfolioIdTag = omsConfig.getFixIn().getPortfolioIdTag();
        this.maxPortfolioIdLength = omsConfig.getFixIn().getMaxPortfolioIdLength();
    }

    public FixInParsedNewOrder parse(Message message) throws FieldNotFound {
        String clOrdId = requireNonBlank(message.getString(ClOrdID.FIELD), "cl_ord_id_required");
        String accountTag = message.isSetField(Account.FIELD) ? message.getString(Account.FIELD).trim() : "";
        byte sideCode = mapSide(message.getChar(Side.FIELD));
        byte tifCode = mapTimeInForce(message.getChar(TimeInForce.FIELD));
        char ordType = message.getChar(OrdType.FIELD);
        byte ordTypeCode = mapOrdType(ordType);
        BigDecimal quantity = parsePositiveDecimal(message.getString(OrderQty.FIELD), "quantity_invalid");
        BigDecimal limitPrice = null;
        if (message.isSetField(Price.FIELD)) {
            limitPrice = parsePositiveDecimal(message.getString(Price.FIELD), "price_invalid");
        }
        if (ordTypeCode == AcceptOrderCommand.ORD_TYPE_LIMIT && limitPrice == null) {
            throw new FixInParseException("limit_price_required");
        }
        String symbol = symbolMapper.toOmsSymbol(message.getString(Symbol.FIELD));
        String portfolioId = parsePortfolioId(message);
        return new FixInParsedNewOrder(
                clOrdId, accountTag, sideCode, tifCode, ordTypeCode, quantity, limitPrice, symbol, portfolioId);
    }

    /**
     * Optional generic portfolio attribution carried in the configured FIX tag (default
     * {@code 5001 PortfolioID}). Returns {@code null} when the tag is absent or blank. A value
     * exceeding {@link OmsConfig.FixIn#getMaxPortfolioIdLength()} is rejected rather than silently
     * truncated so the counterparty sees an explicit reason.
     */
    private String parsePortfolioId(Message message) throws FieldNotFound {
        if (!message.isSetField(portfolioIdTag)) {
            return null;
        }
        String raw = message.getString(portfolioIdTag).trim();
        if (raw.isEmpty()) {
            return null;
        }
        if (raw.length() > maxPortfolioIdLength) {
            throw new FixInParseException("portfolio_id_too_long");
        }
        return raw;
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

    private static byte mapTimeInForce(char tif) {
        return switch (tif) {
            case TimeInForce.DAY -> AcceptOrderCommand.TIF_DAY;
            case TimeInForce.IMMEDIATE_OR_CANCEL -> AcceptOrderCommand.TIF_IOC;
            case TimeInForce.FILL_OR_KILL -> AcceptOrderCommand.TIF_FOK;
            case TimeInForce.GOOD_TILL_CANCEL -> AcceptOrderCommand.TIF_GTC;
            default -> throw new FixInParseException("tif_unsupported");
        };
    }

    private static String requireNonBlank(String value, String code) {
        if (value == null || value.isBlank()) {
            throw new FixInParseException(code);
        }
        return value.trim();
    }

    static BigDecimal parsePositiveDecimal(String raw, String code) {
        try {
            BigDecimal value = new BigDecimal(raw.trim());
            if (value.signum() <= 0) {
                throw new FixInParseException(code);
            }
            return value.stripTrailingZeros();
        } catch (NumberFormatException | ArithmeticException e) {
            throw new FixInParseException(code);
        }
    }

    static long scaleQuantity(BigDecimal quantity) {
        try {
            return quantity.movePointRight(9).longValueExact();
        } catch (ArithmeticException e) {
            throw new FixInParseException("quantity_unrepresentable");
        }
    }

    static long scaleLimitPrice(BigDecimal priceOrNull, byte ordTypeCode) {
        if (priceOrNull == null) {
            return 0L;
        }
        try {
            return priceOrNull.movePointRight(6).longValueExact();
        } catch (ArithmeticException e) {
            throw new FixInParseException("limit_price_unrepresentable");
        }
    }

    static BigDecimal normalizeDecimal(BigDecimal value) {
        return value.setScale(10, RoundingMode.UNNECESSARY);
    }
}
