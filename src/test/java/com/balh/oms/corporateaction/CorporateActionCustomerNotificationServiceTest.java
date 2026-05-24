package com.balh.oms.corporateaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.balh.oms.config.OmsConfig;
import com.balh.oms.settlement.SettlementCustomerNotificationOutboxRepository;
import com.balh.oms.settlement.SettlementCustomerNotificationTypes;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

@ExtendWith(MockitoExtension.class)
class CorporateActionCustomerNotificationServiceTest {

    @Mock SettlementCustomerNotificationOutboxRepository outbox;
    @Mock NamedParameterJdbcTemplate jdbc;

    private CorporateActionCustomerNotificationService service;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        OmsConfig config = new OmsConfig();
        config.getSettlement().setCorporateActionDividendNotificationEnabled(true);
        service = new CorporateActionCustomerNotificationService(outbox, jdbc, objectMapper, config);
    }

    @Test
    void enqueueDividendPaid_insertsOutboxAndMarksNotified() throws Exception {
        UUID accountId = UUID.randomUUID();
        String payload =
                """
                {
                  "schemaVersion": 1,
                  "leg": "dividend",
                  "cashImpactId": 10,
                  "corporateActionEventId": 5,
                  "accountId": "%s",
                  "netAmount": "25.00",
                  "currency": "USD"
                }
                """
                        .formatted(accountId);
        when(jdbc.queryForObject(anyString(), any(MapSqlParameterSource.class), eq(Boolean.class)))
                .thenReturn(false);
        when(jdbc.queryForObject(anyString(), any(MapSqlParameterSource.class), eq(String.class)))
                .thenReturn("AAPL");
        when(outbox.insertCorporateActionIgnore(
                        eq(SettlementCustomerNotificationTypes.CORPORATE_ACTION_DIVIDEND_PAID),
                        eq(accountId),
                        eq(10L),
                        anyString()))
                .thenReturn(1);

        service.enqueueDividendPaidIfEnabled(payload);

        ArgumentCaptor<String> envelopeCaptor = ArgumentCaptor.forClass(String.class);
        verify(outbox)
                .insertCorporateActionIgnore(
                        eq(SettlementCustomerNotificationTypes.CORPORATE_ACTION_DIVIDEND_PAID),
                        eq(accountId),
                        eq(10L),
                        envelopeCaptor.capture());
        assertThat(envelopeCaptor.getValue()).contains("CorporateActionDividendPaid");
        verify(jdbc).update(anyString(), any(MapSqlParameterSource.class));
    }

    @Test
    void enqueueDividendPaid_skipsWithholdingLeg() {
        service.enqueueDividendPaidIfEnabled(
                """
                {"leg":"dividend-withholding","cashImpactId":10,"accountId":"%s"}
                """
                        .formatted(UUID.randomUUID()));
        verify(outbox, never()).insertCorporateActionIgnore(anyString(), any(UUID.class), anyLong(), anyString());
    }
}
