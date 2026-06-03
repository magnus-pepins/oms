package com.balh.oms.predictionmarket;

import com.balh.oms.IntegrationTestPostgresSupport;
import com.zaxxer.hikari.HikariDataSource;
import java.math.BigDecimal;
import java.time.Instant;
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
                "it",
                "HALTED",
                "USD",
                new BigDecimal("0.01"),
                new BigDecimal("1.00"),
                Instant.parse("2030-01-01T00:00:00Z"));

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
                "it",
                "CLOSED",
                "USD",
                new BigDecimal("0.01"),
                new BigDecimal("1.00"),
                Instant.parse("2030-06-01T00:00:00Z"));

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
                "it",
                "HALTED",
                "USD",
                new BigDecimal("0.01"),
                new BigDecimal("1.00"),
                null);

        assertThat(repository.listAll("   ")).anyMatch(r -> slug.equals(r.slug()));
    }
}
