package com.balh.oms.settlement;

/**
 * IL 42 kap. 39 § deposit classification tags for ISK settlement legs (gap plan §5.10).
 *
 * <p>Consumed by Ledger when posting settlement cash/fee legs from OMS outbox payloads.
 */
public final class IskDepositClass {

    /** Sale proceeds per 22 § 1–7 — excluded from kapitalunderlag deposit sum. */
    public static final String SALE_PROCEEDS_EXCLUDED = "sale_proceeds_excluded";

    /** BUY trade funding from ISK cash — not an external deposit. */
    public static final String TRADE_FUNDING = "trade_funding";

    /** Commission debited from ISK cash. */
    public static final String COMMISSION = "commission";

    private IskDepositClass() {}
}
