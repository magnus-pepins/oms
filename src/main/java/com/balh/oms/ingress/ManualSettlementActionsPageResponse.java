package com.balh.oms.ingress;

import java.util.List;

/** Paged list for {@code GET /internal/v1/settlement/manual-actions}. */
public record ManualSettlementActionsPageResponse(
        List<ManualSettlementActionResponse> items, int limit, int offset) {}
