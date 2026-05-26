package com.balh.oms.risk;

import static org.assertj.core.api.Assertions.assertThat;

import com.balh.oms.AbstractPostgresIntegrationTest;
import com.balh.oms.domain.Order;
import com.balh.oms.domain.OrderStatus;
import com.balh.oms.domain.RejectCode;
import com.balh.oms.domain.Side;
import com.balh.oms.settlement.OmsAccountTaxWrapperRepository;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.TestPropertySource;

/**
 * ISK Slice ISK-B (gap plan §5.10 / I3): integration coverage for the BUY funding gate
 * driven through the wired {@link ControlRiskEvaluator}. Confirms that with the funding
 * flag enabled, a BUY against an ISK-wrapped account that carries a non-ISK
 * {@code ledgerBalanceId} returns {@link RejectCode#RISK_ISK_FUNDING_MISMATCH} from the
 * evaluator — i.e. before any Ledger admission call would be made by
 * {@code ControlTailer}.
 *
 * <p>Disabled here: {@code isk-instrument-eligibility-check-enabled} (we are not testing
 * instrument routes; the funding gate must reject independently of that gate) and
 * {@code sell-position-check-enabled} (would otherwise trip on the unseeded positions
 * table when we also assert SELL is untouched).
 */
@TestPropertySource(
        properties = {
            "oms.risk.isk-funding-check-enabled=true",
            "oms.risk.sell-position-check-enabled=false"
        })
class IskFundingGateIntegrationTest extends AbstractPostgresIntegrationTest {

    private static final String ISK_SEK_BALANCE = "bal-isk-sek-cash";
    private static final String OTHER_BALANCE = "bal-non-isk";

    @Autowired JdbcTemplate jdbc;
    @Autowired ControlRiskEvaluator controlRiskEvaluator;
    @Autowired OmsAccountTaxWrapperRepository accountTaxWrapper;

    @BeforeEach
    void truncate() {
        jdbc.update(AbstractPostgresIntegrationTest.SQL_TRUNCATE_ORDERS_AND_SETTLEMENT);
    }

    @Test
    void iskBuyWithNonIskFundingBalance_rejectsBeforeLedger() {
        UUID accountId = UUID.randomUUID();
        accountTaxWrapper.upsert(
                accountId,
                OmsAccountTaxWrapperRepository.TAX_WRAPPER_ISK,
                UUID.randomUUID(),
                ISK_SEK_BALANCE);

        Order buy = order(accountId, Side.BUY, OTHER_BALANCE);

        Optional<RejectCode> code = controlRiskEvaluator.evaluate(buy);
        assertThat(code).contains(RejectCode.RISK_ISK_FUNDING_MISMATCH);
    }

    @Test
    void iskBuyWithMatchingIskBalance_passesFundingGate() {
        UUID accountId = UUID.randomUUID();
        accountTaxWrapper.upsert(
                accountId,
                OmsAccountTaxWrapperRepository.TAX_WRAPPER_ISK,
                UUID.randomUUID(),
                ISK_SEK_BALANCE);

        Order buy = order(accountId, Side.BUY, ISK_SEK_BALANCE);

        // No reject — the rest of the cascade (rate-limit, fat-finger, etc.) is off by default
        // so an empty result means the funding gate accepted and downstream had nothing to say.
        assertThat(controlRiskEvaluator.evaluate(buy)).isEmpty();
    }

    @Test
    void iskBuyWithMissingLedgerBalanceId_rejects() {
        UUID accountId = UUID.randomUUID();
        accountTaxWrapper.upsert(
                accountId,
                OmsAccountTaxWrapperRepository.TAX_WRAPPER_ISK,
                UUID.randomUUID(),
                ISK_SEK_BALANCE);

        Order buy = order(accountId, Side.BUY, null);

        assertThat(controlRiskEvaluator.evaluate(buy))
                .contains(RejectCode.RISK_ISK_FUNDING_MISMATCH);
    }

    @Test
    void iskSell_isUntouchedByFundingGate() {
        UUID accountId = UUID.randomUUID();
        accountTaxWrapper.upsert(
                accountId,
                OmsAccountTaxWrapperRepository.TAX_WRAPPER_ISK,
                UUID.randomUUID(),
                ISK_SEK_BALANCE);

        // SELL with deliberately mismatched ledgerBalanceId — funding gate must not fire because
        // SELL generates cash; the wrapper match is not material to a SELL.
        Order sell = order(accountId, Side.SELL, OTHER_BALANCE);

        assertThat(controlRiskEvaluator.evaluate(sell)).isEmpty();
    }

    @Test
    void nonIskAccount_isUntouchedByFundingGate() {
        UUID accountId = UUID.randomUUID();
        accountTaxWrapper.upsert(
                accountId,
                OmsAccountTaxWrapperRepository.TAX_WRAPPER_INVESTMENT,
                null,
                OTHER_BALANCE);

        Order buy = order(accountId, Side.BUY, OTHER_BALANCE);

        assertThat(controlRiskEvaluator.evaluate(buy)).isEmpty();
    }

    private static Order order(UUID accountId, Side side, String ledgerBalanceId) {
        Instant now = Instant.parse("2026-05-20T12:00:00Z");
        return new Order(
                UUID.randomUUID(),
                accountId,
                "idem-" + UUID.randomUUID(),
                0,
                0,
                OrderStatus.NEW,
                null,
                side,
                "AAPL",
                BigDecimal.ONE,
                new BigDecimal("10.00"),
                "DAY",
                now,
                now,
                null,
                "h",
                ledgerBalanceId,
                BigDecimal.ZERO);
    }
}
