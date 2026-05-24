package com.balh.oms.settlement;

import com.balh.oms.domain.Side;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class IskSettlementMetadataServiceTest {

    private static final UUID ACCOUNT_ID = UUID.fromString("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa");
    private static final UUID ISK_ACCOUNT_ID = UUID.fromString("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb");

    @Mock private OmsAccountTaxWrapperRepository accountTaxWrapper;

    @Test
    void depositClassForSellCash_isSaleProceedsExcluded() {
        assertThat(IskSettlementMetadataService.depositClassFor(Side.SELL.name(), LedgerSettlementOutboxRepository.LEG_CASH))
                .isEqualTo(IskDepositClass.SALE_PROCEEDS_EXCLUDED);
    }

    @Test
    void depositClassForBuyCash_isTradeFunding() {
        assertThat(IskSettlementMetadataService.depositClassFor(Side.BUY.name(), LedgerSettlementOutboxRepository.LEG_CASH))
                .isEqualTo(IskDepositClass.TRADE_FUNDING);
    }

    @Test
    void depositClassForFee_isCommission() {
        assertThat(IskSettlementMetadataService.depositClassFor(Side.BUY.name(), LedgerSettlementOutboxRepository.LEG_FEE))
                .isEqualTo(IskDepositClass.COMMISSION);
    }

    @Test
    void enrich_addsIskFieldsForIskAccount() throws Exception {
        when(accountTaxWrapper.findByAccountId(ACCOUNT_ID))
                .thenReturn(
                        Optional.of(
                                new OmsAccountTaxWrapperRepository.AccountTaxWrapperRow(
                                        ACCOUNT_ID, OmsAccountTaxWrapperRepository.TAX_WRAPPER_ISK, ISK_ACCOUNT_ID, "bal-isk-1")));

        IskSettlementMetadataService service = new IskSettlementMetadataService(accountTaxWrapper);
        ObjectNode payload = new ObjectMapper().createObjectNode();

        service.enrich(payload, ACCOUNT_ID, Side.SELL.name(), LedgerSettlementOutboxRepository.LEG_CASH);

        assertThat(payload.get("taxWrapper").asText()).isEqualTo("isk");
        assertThat(payload.get("iskAccountId").asText()).isEqualTo(ISK_ACCOUNT_ID.toString());
        assertThat(payload.get("ledgerBalanceId").asText()).isEqualTo("bal-isk-1");
        assertThat(payload.get("iskDepositClass").asText()).isEqualTo(IskDepositClass.SALE_PROCEEDS_EXCLUDED);
    }
}
