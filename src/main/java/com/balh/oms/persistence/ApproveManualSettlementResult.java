package com.balh.oms.persistence;

/** Outcome of {@link ManualSettlementActionsRepository#approve(long, String)}. */
public enum ApproveManualSettlementResult {
    OK,
    NOT_FOUND,
    ALREADY_APPROVED,
    SAME_ACTOR,
    INVALID_APPROVER,
    CONFLICT
}
