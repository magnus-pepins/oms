package com.balh.oms.returnpath;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record ExecutionTradeCommand(
        UUID orderId,
        String venueId,
        Instant venueTs,
        String venueExecRef,
        BigDecimal lastQuantity,
        BigDecimal lastPrice,
        BigDecimal leavesQuantity,
        BigDecimal cumQuantityAfter
) {}
