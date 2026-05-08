package com.balh.oms.ingress;

import java.util.List;

/** Paged list for {@code GET /internal/v1/control-decisions}. */
public record ControlDecisionsPageResponse(List<ControlDecisionResponse> items, int limit, int offset) {}
