package com.balh.oms.settlement;

import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SettlementFailPenaltyBookingServiceTest {

    @Test
    void penaltyLegKind_includesBrokerFailId() {
        assertThat(SettlementFailPenaltyBookingService.penaltyLegKind("BF-99"))
                .isEqualTo(LedgerSettlementOutboxRepository.LEG_PENALTY + "-BF-99");
    }
}
