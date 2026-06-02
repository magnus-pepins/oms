package com.balh.oms.fx;

import com.balh.oms.config.OmsConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Pure-logic tests for {@link FxAutoHedgerPolicyService}.
 *
 * <p>The cache-load SQL is exercised in the Spring slice tests; here we
 * lock down four-eyes behaviour for the {@code advisory → auto}
 * promotion + the demotion path so a regression cannot silently flip a
 * row to auto without a second approver.
 */
class FxAutoHedgerPolicyServiceTest {

    private static final Instant T = Instant.parse("2026-05-21T12:00:00Z");

    private FxAutoHedgerPolicyService newService(JdbcTemplate jdbc) {
        return new FxAutoHedgerPolicyService(
                jdbc, Clock.fixed(T, ZoneOffset.UTC), new OmsConfig(),
                new SimpleMeterRegistry(), 60_000L);
    }

    private static FxAutoHedgerPolicyService.PolicyRow row(String currency, String mode, String createdBy) {
        return new FxAutoHedgerPolicyService.PolicyRow(
                currency,
                new BigDecimal("1000000.00000000"),
                new BigDecimal("50000.00000000"),
                null,
                "EURSEK",
                new BigDecimal("100000.00000000"),
                300,
                mode,
                "nostro-base", "nostro-quote",
                "suspense",
                createdBy, T.minusSeconds(3600),
                createdBy, T.minusSeconds(3600),
                "auto".equals(mode) ? "approver-x" : null,
                "auto".equals(mode) ? T.minusSeconds(60) : null);
    }

    private static FxAutoHedgerPolicyService.UpsertRequest req(
            String currency, String mode, String updatedBy, String autoApprovedBy) {
        return new FxAutoHedgerPolicyService.UpsertRequest(
                currency,
                new BigDecimal("1000000"),
                new BigDecimal("50000"),
                null,
                "EURSEK",
                new BigDecimal("100000"),
                300,
                mode,
                "nostro-base", "nostro-quote",
                "suspense",
                updatedBy,
                autoApprovedBy);
    }

    @Test
    void validate_requiresThreeLetterCurrency() {
        FxAutoHedgerPolicyService svc = newService(mock(JdbcTemplate.class));
        FxAutoHedgerPolicyService.UpsertRequest bad = new FxAutoHedgerPolicyService.UpsertRequest(
                "EUR1", BigDecimal.ONE, BigDecimal.ONE, null, "EURSEK", BigDecimal.ONE, 1, "off",
                "a", "b", "suspense", "u", null);
        assertThatThrownBy(() -> svc.upsert(bad))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("currency");
    }

    @Test
    void validate_requiresOneThresholdAtLeast() {
        FxAutoHedgerPolicyService svc = newService(mock(JdbcTemplate.class));
        FxAutoHedgerPolicyService.UpsertRequest bad = new FxAutoHedgerPolicyService.UpsertRequest(
                "EUR", BigDecimal.ONE, null, null, "EURSEK", BigDecimal.ONE, 1, "off",
                "a", "b", "suspense", "u", null);
        assertThatThrownBy(() -> svc.upsert(bad))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("threshold");
    }

    @Test
    void promotingToAuto_rejectsWhenApproverMissing() {
        FxAutoHedgerPolicyService svc = newService(mock(JdbcTemplate.class));
        assertThatThrownBy(() -> svc.upsert(req("EUR", "auto", "trader-a", null)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("autoApprovedBy");
    }

    @Test
    void promotingToAuto_rejectsWhenApproverEqualsUpdater() {
        FxAutoHedgerPolicyService svc = newService(mock(JdbcTemplate.class));
        assertThatThrownBy(() -> svc.upsert(req("EUR", "auto", "trader-a", "trader-a")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("four-eyes");
    }

    @Test
    void promotingToAuto_acceptsDistinctIdentities() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        FxAutoHedgerPolicyService svc = newService(jdbc);
        // No existing row: pure insert; two distinct identities are enough.
        doReturn(1).when(jdbc).update(anyString(), any(Object[].class));
        FxAutoHedgerPolicyService.UpsertResult r =
                svc.upsert(req("EUR", "auto", "trader-a", "trader-b"));
        assertThat(r.promotedToAuto()).isTrue();
        assertThat(r.demotedFromAuto()).isFalse();
    }

    @Test
    void promotingToAuto_rejectsWhenApproverEqualsExistingTouchedBy() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        FxAutoHedgerPolicyService svc = newService(jdbc);
        // Existing row was created and updated by trader-a; the same
        // identity now tries to re-update + nominates trader-a as
        // approver via a second submission. Four-eyes is degraded.
        svc.primeCache(List.of(row("EUR", "advisory", "trader-a")));
        assertThatThrownBy(() -> svc.upsert(req("EUR", "auto", "trader-a", "trader-a")))
                .isInstanceOf(IllegalArgumentException.class);
        verify(jdbc, never()).update(anyString(), any(Object[].class));
    }

    @Test
    void demotingFromAuto_doesNotRequireApprover() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        FxAutoHedgerPolicyService svc = newService(jdbc);
        svc.primeCache(List.of(row("EUR", "auto", "trader-a")));
        doReturn(1).when(jdbc).update(anyString(), any(Object[].class));
        FxAutoHedgerPolicyService.UpsertResult r =
                svc.upsert(req("EUR", "advisory", "trader-c", null));
        assertThat(r.demotedFromAuto()).isTrue();
        assertThat(r.promotedToAuto()).isFalse();
    }

    @Test
    void forCurrency_returnsRowFromPrimedCache() {
        FxAutoHedgerPolicyService svc = newService(mock(JdbcTemplate.class));
        svc.primeCache(List.of(row("SEK", "advisory", "trader-a")));
        assertThat(svc.forCurrency("SEK")).isPresent();
        assertThat(svc.forCurrency("sek")).isPresent();
        assertThat(svc.forCurrency("NOK")).isEmpty();
    }
}
