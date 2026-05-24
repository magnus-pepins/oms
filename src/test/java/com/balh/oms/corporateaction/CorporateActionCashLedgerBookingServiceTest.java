package com.balh.oms.corporateaction;

import com.balh.oms.settlement.IskDepositClass;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CorporateActionCashLedgerBookingServiceTest {

    @Test
    void iskDepositClassFor_tenderOffer_isSaleProceedsExcluded() {
        assertThat(CorporateActionCashLedgerBookingService.iskDepositClassFor("TENDER_OFFER"))
                .isEqualTo(IskDepositClass.SALE_PROCEEDS_EXCLUDED);
    }

    @Test
    void iskDepositClassFor_cashDividend_isDividend() {
        assertThat(CorporateActionCashLedgerBookingService.iskDepositClassFor("CASH_DIVIDEND"))
                .isEqualTo(IskDepositClass.DIVIDEND);
    }
}
