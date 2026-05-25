package com.balh.oms.fx;

import com.balh.oms.config.OmsConfig;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class FxCustomerFlowNettingServiceTest {

    private static final Instant FIXED = Instant.parse("2026-05-25T10:07:30Z");

    @Mock FxCustomerFlowNettingRepository buckets;

    private OmsConfig config;
    private FxCustomerFlowNettingService service;

    @BeforeEach
    void setUp() {
        config = new OmsConfig();
        config.getFx().setNettingWindowMs(300_000L);
        config.getFx().setCustomerFlowNettingEnabled(true);
        service = new FxCustomerFlowNettingService(buckets, config, Clock.fixed(FIXED, ZoneOffset.UTC));
    }

    @Test
    void windowStart_alignsToFiveMinuteBoundary() {
        assertThat(service.windowStart(FIXED)).isEqualTo(Instant.parse("2026-05-25T10:05:00Z"));
    }

    @Test
    void recordOrderAcceptFlow_writesNegativeBaseAndPositiveQuote() {
        service.recordOrderAcceptFlow(
                "EURUSD",
                new BigDecimal("100.00"),
                new BigDecimal("108.50"));

        verify(buckets)
                .addFlow(
                        eq("EURUSD"),
                        eq("EUR"),
                        eq("USD"),
                        eq(Instant.parse("2026-05-25T10:05:00Z")),
                        eq(Instant.parse("2026-05-25T10:10:00Z")),
                        eq(new BigDecimal("-100.00000000")),
                        eq(new BigDecimal("108.50000000")));
    }

    @Test
    void recordOrderAcceptFlow_skipsWhenDisabled() {
        config.getFx().setCustomerFlowNettingEnabled(false);
        service.recordOrderAcceptFlow(
                "EURUSD",
                new BigDecimal("1.00"),
                new BigDecimal("1.10"));
        org.mockito.Mockito.verifyNoInteractions(buckets);
    }
}
