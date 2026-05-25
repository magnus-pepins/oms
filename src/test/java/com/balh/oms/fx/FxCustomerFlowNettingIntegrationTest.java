package com.balh.oms.fx;

import com.balh.oms.AbstractPostgresIntegrationTest;
import java.math.BigDecimal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.assertj.core.api.Assertions.assertThat;

class FxCustomerFlowNettingIntegrationTest extends AbstractPostgresIntegrationTest {

    @Autowired JdbcTemplate jdbc;
    @Autowired FxCustomerFlowNettingService netting;

    @BeforeEach
    void truncate() {
        jdbc.update("TRUNCATE fx_customer_flow_netting_bucket RESTART IDENTITY CASCADE");
    }

    @Test
    void addFlow_aggregatesWithinSameWindow() {
        netting.recordOrderAcceptFlow("EURUSD", new BigDecimal("50"), new BigDecimal("54"));
        netting.recordOrderAcceptFlow("EURUSD", new BigDecimal("25"), new BigDecimal("27"));

        Integer count =
                jdbc.queryForObject(
                        "SELECT COUNT(*)::int FROM fx_customer_flow_netting_bucket WHERE pair = 'EURUSD'",
                        Integer.class);
        assertThat(count).isEqualTo(1);

        BigDecimal netBase =
                jdbc.queryForObject(
                        "SELECT net_base_amount FROM fx_customer_flow_netting_bucket WHERE pair = 'EURUSD'",
                        BigDecimal.class);
        BigDecimal netQuote =
                jdbc.queryForObject(
                        "SELECT net_quote_amount FROM fx_customer_flow_netting_bucket WHERE pair = 'EURUSD'",
                        BigDecimal.class);
        Integer flowCount =
                jdbc.queryForObject(
                        "SELECT flow_count FROM fx_customer_flow_netting_bucket WHERE pair = 'EURUSD'",
                        Integer.class);

        assertThat(netBase).isEqualByComparingTo("-75");
        assertThat(netQuote).isEqualByComparingTo("81");
        assertThat(flowCount).isEqualTo(2);
    }

    @Test
    void closeExpiredWindows_marksPastOpenBucketsClosed() {
        jdbc.update(
                """
                        INSERT INTO fx_customer_flow_netting_bucket (
                          pair, base_currency, quote_currency, window_start, window_end,
                          net_base_amount, net_quote_amount, flow_count, status
                        ) VALUES (
                          'EURUSD', 'EUR', 'USD', NOW() - INTERVAL '10 minutes', NOW() - INTERVAL '5 minutes',
                          -10, 11, 1, 'open'
                        )
                        """);

        int closed = netting.closeExpiredWindows();
        assertThat(closed).isEqualTo(1);
        String status =
                jdbc.queryForObject(
                        "SELECT status FROM fx_customer_flow_netting_bucket WHERE pair = 'EURUSD'",
                        String.class);
        assertThat(status).isEqualTo("closed");
    }
}
