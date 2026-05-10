package com.balh.oms.returnpath;

import com.balh.oms.domain.Order;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.Optional;

/**
 * JSON patch (merged into {@code market_context.snapshot_json}) built from an applied venue trade
 * {@link ExecutionTradeCommand} and the {@link Order} row.
 */
public final class MarketContextVenueEvidence {

    /** Version of the {@code snapshot_json} venue-evidence object shape (bump when adding required fields). */
    public static final int SCHEMA_VERSION = 1;

    private static final int QUANTITY_SCALE = 10;

    private MarketContextVenueEvidence() {}

    /** NBBO-class reference merged into {@code snapshot_json} (stub or HTTP-backed). */
    public record NbboQuoteRef(BigDecimal bid, BigDecimal ask, Instant asOf, String quoteClass) {
        public NbboQuoteRef(BigDecimal bid, BigDecimal ask, Instant asOf) {
            this(bid, ask, asOf, "NBBO_STUB");
        }

        public NbboQuoteRef {
            if (quoteClass == null || quoteClass.isBlank()) {
                quoteClass = "NBBO_STUB";
            }
        }
    }

    public static String toJsonPatch(ObjectMapper mapper, Order order, ExecutionTradeCommand cmd)
            throws JsonProcessingException {
        return toJsonPatch(mapper, order, cmd, Optional.empty());
    }

    public static String toJsonPatch(
            ObjectMapper mapper, Order order, ExecutionTradeCommand cmd, Optional<NbboQuoteRef> nbboQuote)
            throws JsonProcessingException {
        ObjectNode n = mapper.createObjectNode();
        n.put("schemaVersion", SCHEMA_VERSION);
        n.put("evidenceSource", "venue_execution_report");
        n.put("instrumentSymbol", order.instrumentSymbol());
        n.put("venueId", cmd.venueId());
        n.put("venueExecRef", cmd.venueExecRef());
        n.put("venueTransactTime", cmd.venueTs().toString());
        n.put("lastQuantity", qty(cmd.lastQuantity()));
        if (cmd.lastPrice() != null) {
            n.put("lastPrice", px(cmd.lastPrice()));
        }
        n.put("leavesQuantity", qty(cmd.leavesQuantity()));
        n.put("cumQuantityAfter", qty(cmd.cumQuantityAfter()));
        nbboQuote.ifPresent(q -> {
            if (q.bid().compareTo(BigDecimal.ZERO) > 0 && q.ask().compareTo(BigDecimal.ZERO) > 0) {
                ObjectNode ref = mapper.createObjectNode();
                ref.put("bid", px(q.bid()));
                ref.put("ask", px(q.ask()));
                ref.put("asOf", q.asOf().toString());
                ref.put("quoteClass", q.quoteClass());
                n.set("nbboClassReference", ref);
            }
        });
        return mapper.writeValueAsString(n);
    }

    private static String qty(BigDecimal q) {
        return q.setScale(QUANTITY_SCALE, RoundingMode.HALF_UP).toPlainString();
    }

    private static String px(BigDecimal p) {
        return p.setScale(QUANTITY_SCALE, RoundingMode.HALF_UP).toPlainString();
    }
}
