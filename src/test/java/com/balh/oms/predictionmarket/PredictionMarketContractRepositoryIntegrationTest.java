package com.balh.oms.predictionmarket;

import com.balh.oms.IntegrationTestPostgresSupport;
import com.zaxxer.hikari.HikariDataSource;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Postgres-only coverage for {@link PredictionMarketContractRepository#listAll} (no Spring /
 * Aeron — avoids port collisions with the JVM-wide test cluster).
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PredictionMarketContractRepositoryIntegrationTest {

    private HikariDataSource dataSource;
    private PredictionMarketContractRepository repository;

    @BeforeAll
    void startPostgresAndMigrate() {
        dataSource = IntegrationTestPostgresSupport.newDataSource("prediction-market-contract-repo-it");
        repository = new PredictionMarketContractRepository(new NamedParameterJdbcTemplate(dataSource));
    }

    @AfterAll
    void closeDataSource() {
        if (dataSource != null) {
            dataSource.close();
        }
    }

    @Test
    void listAll_nullStatus_returnsAllRows_withoutPostgresTypeError() {
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        repository.insert(
                "halted-" + suffix,
                "Halted contract",
                "PREDMKT-HALT-" + suffix,
                "PREDMKT-HALT-" + suffix + "-NO",
                null,
                null,
                List.of(),
                "it",
                "HALTED",
                "USD",
                new BigDecimal("0.01"),
                new BigDecimal("1.00"),
                Instant.parse("2030-01-01T00:00:00Z"),
                null,
                List.of(),
                null,
                List.of(),
                null,
                0);

        var rows = repository.listAll(null);

        assertThat(rows).isNotEmpty();
        assertThat(rows).anyMatch(r -> "TEST-1".equals(r.slug()));
        assertThat(rows).anyMatch(r -> r.status().equals("HALTED") && r.slug().startsWith("halted-"));
    }

    @Test
    void listAll_withStatus_filtersRows() {
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        String slug = "closed-" + suffix;
        repository.insert(
                slug,
                "Closed contract",
                "PREDMKT-CLS-" + suffix,
                "PREDMKT-CLS-" + suffix + "-NO",
                null,
                null,
                List.of(),
                "it",
                "CLOSED",
                "USD",
                new BigDecimal("0.01"),
                new BigDecimal("1.00"),
                Instant.parse("2030-06-01T00:00:00Z"),
                null,
                List.of(),
                null,
                List.of(),
                null,
                0);

        assertThat(repository.listAll("OPEN")).allMatch(r -> "OPEN".equals(r.status()));
        assertThat(repository.listAll("CLOSED")).anyMatch(r -> slug.equals(r.slug()));
        assertThat(repository.listAll("OPEN")).noneMatch(r -> slug.equals(r.slug()));
    }

    @Test
    void listAll_blankStatus_treatedAsUnfiltered() {
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        String slug = "blank-filter-" + suffix;
        repository.insert(
                slug,
                "Blank filter",
                "PREDMKT-BLK-" + suffix,
                "PREDMKT-BLK-" + suffix + "-NO",
                null,
                null,
                List.of(),
                "it",
                "HALTED",
                "USD",
                new BigDecimal("0.01"),
                new BigDecimal("1.00"),
                null,
                null,
                List.of(),
                null,
                List.of(),
                null,
                0);

        assertThat(repository.listAll("   ")).anyMatch(r -> slug.equals(r.slug()));
    }

    @Test
    void findBySlug_returnsSeededTestContract() {
        assertThat(repository.findBySlug("TEST-1"))
                .isPresent()
                .get()
                .satisfies(r -> {
                    assertThat(r.yesSymbol()).isEqualTo("PREDMKT-TEST-1");
                    assertThat(r.noSymbol()).isEqualTo("PREDMKT-TEST-1-NO");
                });
        assertThat(repository.findBySlug("missing-slug-" + UUID.randomUUID())).isEmpty();
    }

    @Test
    void insert_persistsDescriptionCriteriaLinksAndResolvesAt() {
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        String slug = "content-" + suffix;
        var links =
                List.of(new PredictionMarketReferenceLinks.Link("Source", "https://example.com/article"));
        var inserted =
                repository.insert(
                        slug,
                        "Content contract",
                        "PREDMKT-CNT-" + suffix,
                        "PREDMKT-CNT-" + suffix + "-NO",
                        "Longer market narrative.",
                        "YES if the event occurs by 2030-12-31.",
                        links,
                        "it-oracle",
                        "OPEN",
                        "USD",
                        new BigDecimal("0.01"),
                        new BigDecimal("1.00"),
                        Instant.parse("2030-01-15T12:00:00Z"),
                        Instant.parse("2030-01-16T12:00:00Z"),
                        List.of("SE", "LV"),
                        "Politics",
                        List.of("election", "eu"),
                        "https://cdn.example.com/card.png",
                        42);

        assertThat(repository.findBySlug(slug))
                .isPresent()
                .get()
                .satisfies(r -> {
                    assertThat(r.description()).isEqualTo("Longer market narrative.");
                    assertThat(r.resolutionCriteria()).isEqualTo("YES if the event occurs by 2030-12-31.");
                    assertThat(r.referenceLinks()).hasSize(1);
                    assertThat(r.referenceLinks().getFirst().url()).isEqualTo("https://example.com/article");
                    assertThat(r.resolvesAt()).isEqualTo(inserted.resolvesAt());
                    assertThat(r.jurisdictionTags()).containsExactly("SE", "LV");
                    assertThat(r.category()).isEqualTo("Politics");
                    assertThat(r.tags()).containsExactly("election", "eu");
                    assertThat(r.cardImageUrl()).isEqualTo("https://cdn.example.com/card.png");
                    assertThat(r.displayOrder()).isEqualTo(42);
                });
    }

    @Test
    void listOpen_ordersByDisplayOrderThenId() {
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        repository.insert(
                "sort-b-" + suffix,
                "Sort B",
                "PREDMKT-SB-" + suffix,
                "PREDMKT-SB-" + suffix + "-NO",
                null,
                null,
                List.of(),
                "it",
                "OPEN",
                "USD",
                new BigDecimal("0.01"),
                new BigDecimal("1.00"),
                null,
                null,
                List.of(),
                null,
                List.of(),
                null,
                20);
        repository.insert(
                "sort-a-" + suffix,
                "Sort A",
                "PREDMKT-SA-" + suffix,
                "PREDMKT-SA-" + suffix + "-NO",
                null,
                null,
                List.of(),
                "it",
                "OPEN",
                "USD",
                new BigDecimal("0.01"),
                new BigDecimal("1.00"),
                null,
                null,
                List.of(),
                null,
                List.of(),
                null,
                10);

        var open = repository.listOpen();
        int idxA = -1;
        int idxB = -1;
        for (int i = 0; i < open.size(); i++) {
            if (open.get(i).slug().equals("sort-a-" + suffix)) {
                idxA = i;
            }
            if (open.get(i).slug().equals("sort-b-" + suffix)) {
                idxB = i;
            }
        }
        assertThat(idxA).isGreaterThanOrEqualTo(0);
        assertThat(idxB).isGreaterThanOrEqualTo(0);
        assertThat(idxA).isLessThan(idxB);
    }
}
