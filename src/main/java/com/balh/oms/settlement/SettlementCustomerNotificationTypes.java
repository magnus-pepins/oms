package com.balh.oms.settlement;

/** Customer-facing settlement notification envelope {@code type} values (§5.8 / §5.9). */
public final class SettlementCustomerNotificationTypes {

    public static final String SETTLEMENT_DELAYED = "SettlementDelayed";
    public static final String CORPORATE_ACTION_DIVIDEND_PAID = "CorporateActionDividendPaid";

    private SettlementCustomerNotificationTypes() {}
}
