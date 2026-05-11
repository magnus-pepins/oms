package com.balh.oms;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Base class for integration tests that need a real Postgres instance.
 *
 * <p><strong>Local / IDE:</strong> starts {@link PostgreSQLContainer} (Testcontainers) unless Docker is
 * unavailable — then tests are skipped ({@code disabledWithoutDocker = true}).
 *
 * <p><strong>GitHub Actions CI:</strong> when {@code OMS_CI_JDBC_URL} is set (see {@code .github/workflows/ci.yml}),
 * the workflow {@code services: postgres} provides the database; this class still starts a tiny
 * no-op {@link GenericContainer} so the Testcontainers JUnit extension remains satisfied for other
 * tests (e.g. NATS) while JDBC points at the service URL instead of Testcontainers Postgres.
 *
 * <p>To run the OMS against Compose Postgres instead, use {@code ./gradlew bootRun} with
 * {@code OMS_PG_URL} / {@code OMS_PG_USER} / {@code OMS_PG_PASSWORD}; see README.
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
public abstract class AbstractPostgresIntegrationTest {

    /** Env var: JDBC URL of CI-managed Postgres (GitHub Actions {@code services: postgres}). */
    static final String ENV_OMS_CI_JDBC_URL = "OMS_CI_JDBC_URL";

    private static final String ENV_OMS_CI_JDBC_USER = "OMS_CI_JDBC_USER";
    private static final String ENV_OMS_CI_JDBC_PASSWORD = "OMS_CI_JDBC_PASSWORD";

    /**
     * Hikari waits for a TCP connection from the Testcontainers-mapped port; GitHub-hosted runners
     * occasionally expose the mapped port before Postgres accepts connections.
     */
    private static final String HIKARI_CONNECTION_TIMEOUT_MS = "60000";

    /**
     * Clears the order graph and slice-6 settlement tables in one statement.
     *
     * <p>{@code TRUNCATE orders CASCADE} alone does not remove {@code positions} (no FK from
     * positions to orders), which can leak state across integration tests that reuse the same
     * container and default custody account.
     */
    public static final String SQL_TRUNCATE_ORDERS_AND_SETTLEMENT =
            "TRUNCATE TABLE corporate_action_event, manual_settlement_actions, ledger_settlement_outbox, broker_settlement_confirm, settlement_file_import_batch, position_history, positions, orders, fx_stub_leg_group CASCADE";

    /** Docker / runner flakes: retry container start before failing the JVM test run (Testcontainers path only). */
    private static final int POSTGRES_CONTAINER_STARTUP_ATTEMPTS = 3;

    /**
     * Either {@link PostgreSQLContainer} (local) or a tiny Alpine container (CI — Postgres comes from
     * {@value #ENV_OMS_CI_JDBC_URL}).
     */
    @Container
    @SuppressWarnings("resource")
    protected static final GenericContainer<?> POSTGRES_TEST_CONTAINER = createPostgresOrCiAuxiliary();

    private static GenericContainer<?> createPostgresOrCiAuxiliary() {
        if (useCiManagedPostgres()) {
            return new GenericContainer<>(DockerImageName.parse("alpine:3.19"))
                    .withCommand("sleep", "3600")
                    .withLabel("oms.test.role", "ci-postgres-placeholder");
        }
        return new PostgreSQLContainer<>("postgres:16-alpine")
                .withDatabaseName("oms")
                .withUsername("oms")
                .withPassword("oms")
                .withStartupAttempts(POSTGRES_CONTAINER_STARTUP_ATTEMPTS);
    }

    private static boolean useCiManagedPostgres() {
        String url = System.getenv(ENV_OMS_CI_JDBC_URL);
        return url != null && !url.isBlank();
    }

    @DynamicPropertySource
    static void registerPgProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", () -> integrationTestJdbcUrl());
        registry.add("spring.datasource.username", () -> integrationTestJdbcUser());
        registry.add("spring.datasource.password", () -> integrationTestJdbcPassword());
        registry.add("spring.datasource.hikari.connection-timeout", () -> HIKARI_CONNECTION_TIMEOUT_MS);
    }

    /** JDBC URL for the shared integration Postgres (CI service or Testcontainers). */
    public static String integrationTestJdbcUrl() {
        if (useCiManagedPostgres()) {
            return requireEnv(ENV_OMS_CI_JDBC_URL);
        }
        return postgresContainer().getJdbcUrl();
    }

    /** Username for {@link #integrationTestJdbcUrl()}. */
    public static String integrationTestJdbcUser() {
        if (useCiManagedPostgres()) {
            return firstNonBlank(System.getenv(ENV_OMS_CI_JDBC_USER), "oms");
        }
        return postgresContainer().getUsername();
    }

    /** Password for {@link #integrationTestJdbcUrl()}. */
    public static String integrationTestJdbcPassword() {
        if (useCiManagedPostgres()) {
            return firstNonBlank(System.getenv(ENV_OMS_CI_JDBC_PASSWORD), "oms");
        }
        return postgresContainer().getPassword();
    }

    private static PostgreSQLContainer<?> postgresContainer() {
        if (!(POSTGRES_TEST_CONTAINER instanceof PostgreSQLContainer<?> pg)) {
            throw new IllegalStateException("Expected PostgreSQLContainer when " + ENV_OMS_CI_JDBC_URL + " is unset");
        }
        return pg;
    }

    private static String requireEnv(String name) {
        String v = System.getenv(name);
        if (v == null || v.isBlank()) {
            throw new IllegalStateException("Missing required environment variable: " + name);
        }
        return v;
    }

    private static String firstNonBlank(String value, String fallback) {
        if (value == null || value.isBlank()) {
            return fallback;
        }
        return value;
    }
}
