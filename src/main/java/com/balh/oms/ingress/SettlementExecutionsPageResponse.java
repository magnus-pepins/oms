package com.balh.oms.ingress;

import java.util.List;

/** Paged list for {@code GET /internal/v1/settlement/executions}. */
public record SettlementExecutionsPageResponse(
        List<SettlementExecutionResponse> items, int limit, int offset) {}
