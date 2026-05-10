package com.balh.oms.settlement;

/**
 * Canonical {@code manual_settlement_actions.action_type} values executed after four-eyes approval
 * when {@code oms.settlement.manual-action-auto-apply-enabled} is true.
 */
public final class ManualSettlementActionTypes {

    private ManualSettlementActionTypes() {}

    /** Calls {@link SettlementConfirmProcessor#markTradeFailed(long)} on {@code execution_id}. */
    public static final String MARK_TRADE_FAILED = "MARK_TRADE_FAILED";

    /** Calls {@link SettlementConfirmProcessor#advanceOneSettlementStep(long)} once on {@code execution_id}. */
    public static final String ADVANCE_SETTLEMENT_ONE_STEP = "ADVANCE_SETTLEMENT_ONE_STEP";

    /**
     * Enqueues one pending {@code broker_settlement_confirm} row for {@code execution_id} via {@link
     * SettlementConfirmProcessor#enqueueBrokerSettlementConfirmForTradeOrThrow(long)} (idempotent if already queued).
     */
    public static final String REGISTER_BROKER_CONFIRM = "REGISTER_BROKER_CONFIRM";

    /**
     * Deletes pending {@code broker_settlement_confirm} rows for {@code execution_id} via {@link
     * SettlementConfirmProcessor#clearPendingBrokerConfirmsForTradeOrThrow(long)} (idempotent when queue empty).
     */
    public static final String CLEAR_PENDING_BROKER_CONFIRM = "CLEAR_PENDING_BROKER_CONFIRM";
}
