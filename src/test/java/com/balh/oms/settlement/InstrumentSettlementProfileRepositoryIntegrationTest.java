package com.balh.oms.settlement;

import com.balh.oms.AbstractPostgresIntegrationTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.LocalDate;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration test for {@link InstrumentSettlementProfileRepository} (Flyway V61 / gap
 * plan §5.3 Slice 2b-1).
 *
 * <p>Pins three behaviours that are too risky to leave to unit testing alone because they
 * touch the V61 schema:
 *
 * <ul>
 *   <li>the effective-dated window query honours the {@code half-open upper bound}
 *       so the EU T+1 migration can be modelled cleanly;</li>
 *   <li>{@code findActiveBySymbol} returns {@link Optional#empty()} when the lookup falls
 *       outside any active window — the calculator depends on this to drive the default-cycle
 *       fallback path;</li>
 *   <li>the V61 UNIQUE on {@code (instrument_id, effective_from)} stops accidental duplicate
 *       inserts at the same effective date.</li>
 * </ul>
 */
class InstrumentSettlementProfileRepositoryIntegrationTest extends AbstractPostgresIntegrationTest {

    @Autowired
    JdbcTemplate jdbc;

    @Autowired
    InstrumentSettlementProfileRepository repository;

    @BeforeEach
    void truncate() {
        // The shared truncate constant does not list this table — the projector never writes
        // to it and the matcher does not read it, so we own the cleanup here.
        jdbc.update("TRUNCATE TABLE instrument_settlement_profile RESTART IDENTITY CASCADE");
    }

    @Test
    void findActiveBySymbol_returnsEmptyWhenTableIsEmpty() {
        Optional<InstrumentSettlementProfile> result =
                repository.findActiveBySymbol("AAPL", LocalDate.of(2026, 5, 20));
        assertThat(result).isEmpty();
    }

    @Test
    void findActiveBySymbol_returnsRowWhenAsOfWithinEffectiveWindow() {
        long id = repository.insert(new InstrumentSettlementProfileRepository.InsertCommand(
                "AAPL-INST", "AAPL", "US0378331005", "XNAS",
                "XNAS-CAL", "T+1", "USD",
                /* iskEligible = */ false,
                LocalDate.of(2024, 5, 28), /* effectiveTo = */ null));

        Optional<InstrumentSettlementProfile> hit =
                repository.findActiveBySymbol("AAPL", LocalDate.of(2026, 5, 20));
        assertThat(hit).isPresent();
        assertThat(hit.get().id()).isEqualTo(id);
        assertThat(hit.get().symbol()).isEqualTo("AAPL");
        assertThat(hit.get().settlementCycle()).isEqualTo("T+1");
        assertThat(hit.get().settlementCurrency()).isEqualTo("USD");
        assertThat(hit.get().primaryMic()).isEqualTo("XNAS");
        assertThat(hit.get().effectiveTo()).isNull();
    }

    @Test
    void findActiveBySymbol_returnsEmptyWhenAsOfBeforeEffectiveFrom() {
        repository.insert(new InstrumentSettlementProfileRepository.InsertCommand(
                "AAPL-INST", "AAPL", "US0378331005", "XNAS",
                "XNAS-CAL", "T+1", "USD",
                false,
                LocalDate.of(2024, 5, 28), null));

        Optional<InstrumentSettlementProfile> miss =
                repository.findActiveBySymbol("AAPL", LocalDate.of(2024, 5, 27));
        assertThat(miss).isEmpty();
    }

    @Test
    void findActiveBySymbol_honoursHalfOpenUpperBoundForEuT1Migration() {
        // Models the EU T+1 migration on 2027-10-11: one closing T+2 row and one opening
        // T+1 row meeting exactly at the migration date. A trade on 2027-10-10 must hit
        // T+2; a trade on 2027-10-11 must hit T+1.
        repository.insert(new InstrumentSettlementProfileRepository.InsertCommand(
                "ABB-INST", "ABB", "CH0012221716", "XSTO",
                "XSTO-CAL", "T+2", "SEK",
                false,
                LocalDate.of(2024, 1, 1), LocalDate.of(2027, 10, 11)));
        repository.insert(new InstrumentSettlementProfileRepository.InsertCommand(
                "ABB-INST", "ABB", "CH0012221716", "XSTO",
                "XSTO-CAL", "T+1", "SEK",
                false,
                LocalDate.of(2027, 10, 11), null));

        Optional<InstrumentSettlementProfile> before =
                repository.findActiveBySymbol("ABB", LocalDate.of(2027, 10, 10));
        Optional<InstrumentSettlementProfile> onMigrationDay =
                repository.findActiveBySymbol("ABB", LocalDate.of(2027, 10, 11));
        Optional<InstrumentSettlementProfile> well_after =
                repository.findActiveBySymbol("ABB", LocalDate.of(2028, 1, 1));

        assertThat(before).isPresent();
        assertThat(before.get().settlementCycle()).isEqualTo("T+2");
        assertThat(onMigrationDay).isPresent();
        assertThat(onMigrationDay.get().settlementCycle()).isEqualTo("T+1");
        assertThat(well_after).isPresent();
        assertThat(well_after.get().settlementCycle()).isEqualTo("T+1");
    }

    @Test
    void insert_rejectsDuplicateInstrumentIdAndEffectiveFrom() {
        repository.insert(new InstrumentSettlementProfileRepository.InsertCommand(
                "AAPL-INST", "AAPL", null, "XNAS",
                "XNAS-CAL", "T+1", "USD",
                false,
                LocalDate.of(2024, 5, 28), null));
        assertThatThrownBy(() -> repository.insert(new InstrumentSettlementProfileRepository.InsertCommand(
                        "AAPL-INST", "AAPL", null, "XNAS",
                        "XNAS-CAL", "T+1", "USD",
                        false,
                        LocalDate.of(2024, 5, 28), null)))
                .hasMessageContaining("instrument_settlement_profile_instrument_effective_uk");
    }

    @Test
    void insert_rejectsCycleNotInAllowlist() {
        // The V61 CHECK constraint enforces the 4-value allowlist. A migration that wants
        // to extend the allowlist must do so explicitly via a new Flyway version.
        assertThatThrownBy(() -> repository.insert(new InstrumentSettlementProfileRepository.InsertCommand(
                        "WEIRD-INST", "WEIRD", null, "XSTO",
                        "XSTO-CAL", "T+99", "USD",
                        false,
                        LocalDate.of(2024, 1, 1), null)))
                .hasMessageContaining("instrument_settlement_profile_cycle_chk");
    }

    @Test
    void findActiveBySymbol_returnsEmptyOnNullOrBlankInput() {
        // Defensive: the SQL would simply return no rows, but we short-circuit in Java to
        // avoid logging a noisy "no row for symbol=null" trace on every call.
        assertThat(repository.findActiveBySymbol(null, LocalDate.of(2026, 5, 20))).isEmpty();
        assertThat(repository.findActiveBySymbol("", LocalDate.of(2026, 5, 20))).isEmpty();
        assertThat(repository.findActiveBySymbol("   ", LocalDate.of(2026, 5, 20))).isEmpty();
        assertThat(repository.findActiveBySymbol("AAPL", null)).isEmpty();
    }
}
