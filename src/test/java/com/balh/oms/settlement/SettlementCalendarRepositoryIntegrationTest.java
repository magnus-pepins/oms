package com.balh.oms.settlement;

import com.balh.oms.AbstractPostgresIntegrationTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.LocalDate;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for {@link SettlementCalendarRepository} (Flyway V63 / gap plan §5.3
 * Slice 2b-3).
 *
 * <p>Verifies the read path the calculator depends on plus the operator-led write path the
 * JSON ingest endpoint (Slice 2b-4) will exercise:
 *
 * <ul>
 *   <li>seed-row sanity — V63's XSTO-CAL / XNAS-CAL 2026 holidays survive Flyway;</li>
 *   <li>range query semantics — inclusive lower and upper bounds, empty result for
 *       windows outside the seeded data;</li>
 *   <li>defensive input handling — null / blank / inverted-range inputs return empty
 *       rather than throw, since the calculator's hot path cannot tolerate exceptions;</li>
 *   <li>upsert idempotency — re-inserting the same {@code (calendar_id, holiday_date)}
 *       updates {@code description} without throwing on the PK conflict.</li>
 * </ul>
 */
class SettlementCalendarRepositoryIntegrationTest extends AbstractPostgresIntegrationTest {

    @Autowired
    JdbcTemplate jdbc;

    @Autowired
    SettlementCalendarRepository repository;

    @BeforeEach
    void truncate() {
        // V63 seeds 21 rows; tests that depend on a specific window seed their own. Other
        // tests treat the seeded rows as fixtures. We don't blow the table away by default
        // because half the tests check the seed.
    }

    @Test
    void seed_xstoCal2026_includesChristmasAndMidsummer() {
        Set<LocalDate> hols = repository.findHolidaysInRange(
                "XSTO-CAL",
                LocalDate.of(2026, 1, 1),
                LocalDate.of(2026, 12, 31));
        assertThat(hols)
                .contains(
                        LocalDate.of(2026, 1, 1),
                        LocalDate.of(2026, 6, 19),
                        LocalDate.of(2026, 12, 24),
                        LocalDate.of(2026, 12, 25),
                        LocalDate.of(2026, 12, 31));
        assertThat(hols).hasSize(11);
    }

    @Test
    void seed_xnasCal2026_includesObservedIndependenceDay() {
        // 2026-07-04 is a Saturday, so NYSE observes Independence Day on Fri 2026-07-03.
        // The seed encodes the observed date because that's the date settlement is closed.
        Set<LocalDate> hols = repository.findHolidaysInRange(
                "XNAS-CAL",
                LocalDate.of(2026, 7, 1),
                LocalDate.of(2026, 7, 10));
        assertThat(hols).containsExactly(LocalDate.of(2026, 7, 3));
    }

    @Test
    void findHolidaysInRange_inclusiveLowerAndUpperBounds() {
        // Christmas Eve is 2026-12-24 → both bounds inclusive must surface it when the
        // window touches it on either side.
        Set<LocalDate> exact = repository.findHolidaysInRange(
                "XSTO-CAL", LocalDate.of(2026, 12, 24), LocalDate.of(2026, 12, 24));
        assertThat(exact).containsExactly(LocalDate.of(2026, 12, 24));

        Set<LocalDate> beforeOnly = repository.findHolidaysInRange(
                "XSTO-CAL", LocalDate.of(2026, 12, 20), LocalDate.of(2026, 12, 23));
        assertThat(beforeOnly).isEmpty();

        // 2026-12-26 is Saturday (not seeded — weekends aren't in the calendar table) and
        // 2026-12-31 is the next seeded row (New Year's Eve), so the window [26..30] is empty.
        Set<LocalDate> afterOnly = repository.findHolidaysInRange(
                "XSTO-CAL", LocalDate.of(2026, 12, 26), LocalDate.of(2026, 12, 30));
        assertThat(afterOnly).isEmpty();
    }

    @Test
    void findHolidaysInRange_unknownCalendarReturnsEmpty() {
        Set<LocalDate> result = repository.findHolidaysInRange(
                "DOES-NOT-EXIST", LocalDate.of(2026, 1, 1), LocalDate.of(2026, 12, 31));
        assertThat(result).isEmpty();
    }

    @Test
    void findHolidaysInRange_defensiveNullBlankAndInvertedRange_returnEmpty() {
        // Trade-projection hot path cannot afford an exception from the calendar repo.
        assertThat(repository.findHolidaysInRange(null, LocalDate.of(2026, 1, 1), LocalDate.of(2026, 12, 31)))
                .isEmpty();
        assertThat(repository.findHolidaysInRange("", LocalDate.of(2026, 1, 1), LocalDate.of(2026, 12, 31)))
                .isEmpty();
        assertThat(repository.findHolidaysInRange("   ", LocalDate.of(2026, 1, 1), LocalDate.of(2026, 12, 31)))
                .isEmpty();
        assertThat(repository.findHolidaysInRange("XSTO-CAL", null, LocalDate.of(2026, 12, 31))).isEmpty();
        assertThat(repository.findHolidaysInRange("XSTO-CAL", LocalDate.of(2026, 1, 1), null)).isEmpty();
        assertThat(repository.findHolidaysInRange("XSTO-CAL", LocalDate.of(2026, 6, 30), LocalDate.of(2026, 6, 1)))
                .isEmpty();
    }

    @Test
    void upsertHoliday_insertsNewRowAndReturnsTrue() {
        // Pick a 2027 date so we don't collide with the V63 seed.
        boolean inserted = repository.upsertHoliday(new SettlementCalendarRepository.HolidayRow(
                "OPERATOR-TEST",
                LocalDate.of(2027, 3, 15),
                "Operator-supplied test holiday"));
        assertThat(inserted).isTrue();

        Set<LocalDate> hols = repository.findHolidaysInRange(
                "OPERATOR-TEST", LocalDate.of(2027, 1, 1), LocalDate.of(2027, 12, 31));
        assertThat(hols).containsExactly(LocalDate.of(2027, 3, 15));
    }

    @Test
    void upsertHoliday_updatesDescriptionOnPkConflictAndReturnsFalse() {
        var row1 = new SettlementCalendarRepository.HolidayRow(
                "UPSERT-TEST", LocalDate.of(2027, 5, 1), "original");
        var row2 = new SettlementCalendarRepository.HolidayRow(
                "UPSERT-TEST", LocalDate.of(2027, 5, 1), "corrected description");

        assertThat(repository.upsertHoliday(row1)).isTrue();
        assertThat(repository.upsertHoliday(row2)).isFalse();

        List<SettlementCalendarRepository.HolidayRow> all = repository.listByCalendar("UPSERT-TEST");
        assertThat(all).hasSize(1);
        assertThat(all.get(0).description()).isEqualTo("corrected description");
    }

    @Test
    void upsertHolidays_bulkReturnsInsertedAndUpdatedCounts() {
        // First call: 3 inserts.
        var r1 = repository.upsertHolidays(List.of(
                new SettlementCalendarRepository.HolidayRow("BULK-TEST", LocalDate.of(2027, 1, 1), "NYD"),
                new SettlementCalendarRepository.HolidayRow("BULK-TEST", LocalDate.of(2027, 4, 2), "Good Friday"),
                new SettlementCalendarRepository.HolidayRow("BULK-TEST", LocalDate.of(2027, 12, 25), "Christmas")));
        assertThat(r1.inserted()).isEqualTo(3);
        assertThat(r1.updated()).isZero();

        // Second call: 1 update (NYD desc change), 1 new (Memorial Day).
        var r2 = repository.upsertHolidays(List.of(
                new SettlementCalendarRepository.HolidayRow("BULK-TEST", LocalDate.of(2027, 1, 1), "New Year's Day (corrected)"),
                new SettlementCalendarRepository.HolidayRow("BULK-TEST", LocalDate.of(2027, 5, 31), "Memorial Day")));
        assertThat(r2.inserted()).isEqualTo(1);
        assertThat(r2.updated()).isEqualTo(1);

        assertThat(repository.listByCalendar("BULK-TEST")).hasSize(4);
    }
}
